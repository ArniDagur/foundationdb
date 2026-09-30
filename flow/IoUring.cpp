/*
 * IoUring.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flow/IoUring.h"
#include "flow/flow.h"
#include "flow/Knobs.h"

#ifdef __linux__

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <linux/io_uring.h>
#include <linux/time_types.h>
#include <poll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

// Newer than the oldest kernel headers FDB builds against; the ring falls back when the kernel rejects them.
#ifndef IORING_SETUP_SUBMIT_ALL
#define IORING_SETUP_SUBMIT_ALL (1U << 7)
#endif
#ifndef IORING_SETUP_COOP_TASKRUN
#define IORING_SETUP_COOP_TASKRUN (1U << 8)
#endif
#ifndef IORING_SETUP_TASKRUN_FLAG
#define IORING_SETUP_TASKRUN_FLAG (1U << 9)
#endif
#ifndef IORING_SETUP_SINGLE_ISSUER
#define IORING_SETUP_SINGLE_ISSUER (1U << 12)
#endif
#ifndef IORING_SETUP_DEFER_TASKRUN
#define IORING_SETUP_DEFER_TASKRUN (1U << 13)
#endif
#ifndef IORING_SQ_TASKRUN
#define IORING_SQ_TASKRUN (1U << 2)
#endif
#ifndef IORING_ENTER_REGISTERED_RING
#define IORING_ENTER_REGISTERED_RING (1U << 4)
#endif
#ifndef IORING_REGISTER_RING_FDS
#define IORING_REGISTER_RING_FDS 20
#endif

namespace iouring {

namespace {
Ring* g_ring = nullptr;
bool g_tried = false;

constexpr uint64_t kKindMask = 7; // Op objects are at least 8-byte aligned; the low bits carry the Kind
static_assert(int(Kind::Count) <= int(kKindMask) + 1);

const char* kindName(int k) {
	static const char* names[] = { "NetRecv", "NetSend", "DiskRead", "DiskWrite", "DiskFsync", "Internal" };
	return names[k];
}
} // namespace

Ring* Ring::existing() {
	return g_ring;
}

Ring* Ring::get() {
	if (g_ring || g_tried) {
		return g_ring;
	}
	g_tried = true;
	if (!g_network || g_network->isSimulated()) {
		return nullptr;
	}
	auto* ring = new Ring();
	if (!ring->init(FLOW_KNOBS->IO_URING_ENTRIES)) {
		delete ring;
		return nullptr;
	}
	g_ring = ring;
	return ring;
}

bool Ring::init(unsigned entries) {
	// Most capable setup first: completions posted only while this (the only submitting) thread is in
	// io_uring_enter (no inter-processor interrupts or task work while it runs Flow tasks) and the whole batch
	// submitted even if one entry fails; then cooperative task running (5.19); then the 5.15 baseline.
	const unsigned base = IORING_SETUP_CQSIZE;
	const unsigned attempts[] = {
		base | IORING_SETUP_SUBMIT_ALL | IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN |
		    IORING_SETUP_TASKRUN_FLAG,
		base | IORING_SETUP_SUBMIT_ALL | IORING_SETUP_COOP_TASKRUN | IORING_SETUP_TASKRUN_FLAG,
		base,
	};
	io_uring_params p;
	int ringFd = -1;
	for (unsigned flags : attempts) {
		if ((flags & IORING_SETUP_DEFER_TASKRUN) && !FLOW_KNOBS->NET_IO_URING) {
			// Without the ring loop, Asio waits in epoll and only reaps; completions must post on their own.
			continue;
		}
		memset(&p, 0, sizeof(p));
		p.flags = flags;
		// Every connection keeps a receive in flight, so completions can far outnumber one batch of submissions.
		p.cq_entries = entries * 4;
		ringFd = syscall(__NR_io_uring_setup, entries, &p);
		if (ringFd >= 0 || errno != EINVAL) {
			break;
		}
	}
	if (ringFd < 0) {
		TraceEvent(SevWarnAlways, "IoUringSetupFailed").GetLastError();
		return false;
	}
	setupFlags = p.flags;
	deferTaskrun = (p.flags & IORING_SETUP_DEFER_TASKRUN) != 0;
	if (!(p.features & IORING_FEAT_EXT_ARG) || !(p.features & IORING_FEAT_NODROP)) {
		TraceEvent(SevWarnAlways, "IoUringSetupFailed").detail("Reason", "kernel lacks EXT_ARG or NODROP");
		::close(ringFd);
		return false;
	}
	const size_t sqBytes = p.sq_off.array + p.sq_entries * sizeof(unsigned);
	const size_t cqBytes = p.cq_off.cqes + p.cq_entries * sizeof(io_uring_cqe);
	void* sq;
	void* cq;
	if (p.features & IORING_FEAT_SINGLE_MMAP) {
		sq = mmap(nullptr,
		          std::max(sqBytes, cqBytes),
		          PROT_READ | PROT_WRITE,
		          MAP_SHARED | MAP_POPULATE,
		          ringFd,
		          IORING_OFF_SQ_RING);
		cq = sq;
	} else {
		sq = mmap(nullptr, sqBytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd, IORING_OFF_SQ_RING);
		cq = mmap(nullptr, cqBytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd, IORING_OFF_CQ_RING);
	}
	void* sqeMem = mmap(nullptr,
	                    p.sq_entries * sizeof(io_uring_sqe),
	                    PROT_READ | PROT_WRITE,
	                    MAP_SHARED | MAP_POPULATE,
	                    ringFd,
	                    IORING_OFF_SQES);
	if (sq == MAP_FAILED || cq == MAP_FAILED || sqeMem == MAP_FAILED) {
		TraceEvent(SevWarnAlways, "IoUringSetupFailed").GetLastError().detail("Reason", "mmap");
		::close(ringFd);
		return false;
	}
	auto* sqBase = static_cast<char*>(sq);
	auto* cqBase = static_cast<char*>(cq);
	sqHead = reinterpret_cast<unsigned*>(sqBase + p.sq_off.head);
	sqTail = reinterpret_cast<unsigned*>(sqBase + p.sq_off.tail);
	sqMask = reinterpret_cast<unsigned*>(sqBase + p.sq_off.ring_mask);
	sqFlags = reinterpret_cast<unsigned*>(sqBase + p.sq_off.flags);
	sqArray = reinterpret_cast<unsigned*>(sqBase + p.sq_off.array);
	sqes = static_cast<io_uring_sqe*>(sqeMem);
	cqHead = reinterpret_cast<unsigned*>(cqBase + p.cq_off.head);
	cqTail = reinterpret_cast<unsigned*>(cqBase + p.cq_off.tail);
	cqMask = reinterpret_cast<unsigned*>(cqBase + p.cq_off.ring_mask);
	cqes = reinterpret_cast<io_uring_cqe*>(cqBase + p.cq_off.cqes);
	for (unsigned i = 0; i < p.sq_entries; i++) {
		sqArray[i] = i;
	}
	sqEntries = p.sq_entries;
	localTail = *sqTail;
	fd = ringFd;

	wakeFd = eventfd(0, EFD_CLOEXEC);
	if (wakeFd < 0) {
		TraceEvent(SevWarnAlways, "IoUringSetupFailed").GetLastError().detail("Reason", "eventfd");
		::close(ringFd);
		fd = -1;
		return false;
	}
	wakeOp.ring = this;
	epollOp.ring = this;

	// A registered ring descriptor spares io_uring_enter the file table lookup (5.18).
	io_uring_rsrc_update reg;
	memset(&reg, 0, sizeof(reg));
	reg.offset = -1U;
	reg.data = static_cast<uint64_t>(fd);
	if (syscall(__NR_io_uring_register, fd, IORING_REGISTER_RING_FDS, &reg, 1) == 1) {
		enterFd = reg.offset;
		enterFlags = IORING_ENTER_REGISTERED_RING;
	} else {
		enterFd = fd;
	}
	TraceEvent("IoUringReady")
	    .detail("SQEntries", p.sq_entries)
	    .detail("CQEntries", p.cq_entries)
	    .detail("Features", p.features)
	    .detail("SetupFlags", setupFlags)
	    .detail("DeferTaskrun", deferTaskrun)
	    .detail("RegisteredRing", enterFlags != 0);
	return true;
}

int Ring::enter(unsigned toSubmit, unsigned minComplete, unsigned flags, const void* arg, unsigned argSize) {
	++counters.enters;
	return syscall(__NR_io_uring_enter, enterFd, toSubmit, minComplete, flags | enterFlags, arg, argSize);
}

bool Ring::taskWorkPending() const {
	return (__atomic_load_n(sqFlags, __ATOMIC_RELAXED) & (IORING_SQ_TASKRUN | IORING_SQ_CQ_OVERFLOW)) != 0;
}

io_uring_sqe* Ring::sqe() {
	while (localTail - __atomic_load_n(sqHead, __ATOMIC_ACQUIRE) >= sqEntries) {
		flush();
	}
	io_uring_sqe* s = &sqes[localTail & *sqMask];
	memset(s, 0, sizeof(*s));
	return s;
}

void Ring::queue(io_uring_sqe* s, Op* op, Kind kind) {
	ASSERT((reinterpret_cast<uintptr_t>(op) & kKindMask) == 0);
	s->user_data = reinterpret_cast<uintptr_t>(op) | uint64_t(kind);
	++localTail;
	++unsubmitted;
	__atomic_store_n(sqTail, localTail, __ATOMIC_RELEASE);
	++counters.submitted[int(kind)];
}

void Ring::flush() {
	if (unsubmitted == 0 && taskWorkPending()) {
		// Completions (deferred task work) are waiting to be posted.
		enter(0, 0, IORING_ENTER_GETEVENTS, nullptr, 0);
		return;
	}
	for (int attempt = 0; unsubmitted > 0 && attempt < 100; attempt++) {
		// GETEVENTS also posts pending completions, so the reap() that follows sees them.
		const int n = enter(unsubmitted, 0, IORING_ENTER_GETEVENTS, nullptr, 0);
		if (n > 0) {
			unsubmitted -= n;
		} else if (n < 0 && (errno == EBUSY || errno == EAGAIN)) {
			// The completion queue backlog is full: make room.
			reap();
		} else if (n < 0 && errno != EINTR) {
			TraceEvent(SevError, "IoUringSubmitFailed").GetLastError().detail("Queued", unsubmitted);
			return;
		}
	}
}

void Ring::armInternal() {
	if (!wakeOp.armed) {
		io_uring_sqe* s = sqe();
		prepRead(s, wakeFd, &wakeValue, sizeof(wakeValue), 0);
		queue(s, &wakeOp, Kind::Internal);
		wakeOp.armed = true;
	}
	if (epollFd >= 0 && !epollOp.armed) {
		io_uring_sqe* s = sqe();
		s->opcode = IORING_OP_POLL_ADD;
		s->fd = epollFd;
		s->poll32_events = POLLIN;
		queue(s, &epollOp, Kind::Internal);
		epollOp.armed = true;
	}
}

void Ring::wait(double timeoutSeconds) {
	armInternal();
	if (timeoutSeconds <= 0 || *cqHead != __atomic_load_n(cqTail, __ATOMIC_ACQUIRE)) {
		flush();
		return;
	}
	__kernel_timespec ts;
	ts.tv_sec = static_cast<int64_t>(timeoutSeconds);
	ts.tv_nsec = static_cast<int64_t>((timeoutSeconds - ts.tv_sec) * 1e9);
	io_uring_getevents_arg arg;
	memset(&arg, 0, sizeof(arg));
	arg.ts = reinterpret_cast<uintptr_t>(&ts);
	++counters.waits;
	const int n = enter(unsubmitted, 1, IORING_ENTER_GETEVENTS | IORING_ENTER_EXT_ARG, &arg, sizeof(arg));
	if (n > 0) {
		unsubmitted -= std::min<unsigned>(n, unsubmitted);
	}
	if (unsubmitted > 0) {
		flush();
	}
}

int Ring::reap() {
	if (reaping) {
		return 0;
	}
	reaping = true;
	int total = 0;
	while (true) {
		unsigned head = *cqHead;
		const unsigned tail = __atomic_load_n(cqTail, __ATOMIC_ACQUIRE);
		if (head == tail) {
			if (taskWorkPending()) {
				// Completions that did not fit (kept by the kernel under IORING_FEAT_NODROP) or deferred task work.
				enter(0, 0, IORING_ENTER_GETEVENTS, nullptr, 0);
				if (*cqHead != __atomic_load_n(cqTail, __ATOMIC_ACQUIRE)) {
					continue;
				}
			}
			break;
		}
		while (head != tail) {
			const io_uring_cqe cqe = cqes[head & *cqMask];
			++head;
			// Released before complete() runs, so it may queue new entries.
			__atomic_store_n(cqHead, head, __ATOMIC_RELEASE);
			const int kind = int(cqe.user_data & kKindMask);
			++counters.completed[kind];
			reinterpret_cast<Op*>(cqe.user_data & ~kKindMask)->complete(cqe.res);
			++total;
		}
	}
	reaping = false;
	return total;
}

void Ring::InternalOp::complete(int32_t) {
	armed = false;
	fired = true;
}

void Ring::wake() {
	const uint64_t one = 1;
	[[maybe_unused]] ssize_t n = ::write(wakeFd, &one, sizeof(one));
}

void Ring::watchEpoll(int fd) {
	epollFd = fd;
}

bool Ring::takeEpollReady() {
	const bool fired = epollOp.fired;
	epollOp.fired = false;
	return fired;
}

bool Ring::takeWoken() {
	const bool fired = wakeOp.fired;
	wakeOp.fired = false;
	return fired;
}

void Ring::maybeLogMetrics(double now, double intervalSeconds) {
	if (now - lastLogTime < intervalSeconds) {
		return;
	}
	const double elapsed = lastLogTime > 0 ? now - lastLogTime : 0;
	lastLogTime = now;
	TraceEvent ev("IoUringMetrics");
	ev.detail("Elapsed", elapsed)
	    .detail("Enters", counters.enters - lastLogged.enters)
	    .detail("Waits", counters.waits - lastLogged.waits);
	for (int k = 0; k < int(Kind::Count); k++) {
		ev.detail(std::string("Submitted") + kindName(k), counters.submitted[k] - lastLogged.submitted[k]);
		ev.detail(std::string("Completed") + kindName(k), counters.completed[k] - lastLogged.completed[k]);
	}
	ev.detail("Queued", unsubmitted);
	lastLogged = counters;
}

void prepRecv(io_uring_sqe* s, int fd, void* buf, unsigned len, int flags) {
	s->opcode = IORING_OP_RECV;
	s->fd = fd;
	s->addr = reinterpret_cast<uintptr_t>(buf);
	s->len = len;
	s->msg_flags = flags;
}

void prepSend(io_uring_sqe* s, int fd, const void* buf, unsigned len, int flags) {
	s->opcode = IORING_OP_SEND;
	s->fd = fd;
	s->addr = reinterpret_cast<uintptr_t>(buf);
	s->len = len;
	s->msg_flags = flags;
}

void prepRead(io_uring_sqe* s, int fd, void* buf, unsigned len, uint64_t offset) {
	s->opcode = IORING_OP_READ;
	s->fd = fd;
	s->addr = reinterpret_cast<uintptr_t>(buf);
	s->len = len;
	s->off = offset;
}

void prepWrite(io_uring_sqe* s, int fd, const void* buf, unsigned len, uint64_t offset) {
	s->opcode = IORING_OP_WRITE;
	s->fd = fd;
	s->addr = reinterpret_cast<uintptr_t>(buf);
	s->len = len;
	s->off = offset;
}

void prepFsync(io_uring_sqe* s, int fd, bool datasync) {
	s->opcode = IORING_OP_FSYNC;
	s->fd = fd;
	s->fsync_flags = datasync ? IORING_FSYNC_DATASYNC : 0;
}

} // namespace iouring

#else // !__linux__

namespace iouring {
Ring* Ring::get() {
	return nullptr;
}
Ring* Ring::existing() {
	return nullptr;
}
} // namespace iouring

#endif
