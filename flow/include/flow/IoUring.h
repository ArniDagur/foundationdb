/*
 * IoUring.h
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

#ifndef FLOW_IOURING_H
#define FLOW_IOURING_H
#pragma once

// The network thread's io_uring (Linux only), shared by socket I/O (FLOW_KNOBS->NET_IO_URING) and KAIO file I/O
// (FLOW_KNOBS->KAIO_IO_URING). It is driven with raw syscalls. Every submission and completion happens on the network
// thread; other threads only call wake().

#include <cstdint>
#include <vector>

struct io_uring_sqe;
struct io_uring_cqe;
struct msghdr;

namespace iouring {

enum class Kind : int { NetRecv, NetSend, DiskRead, DiskWrite, DiskFsync, Internal, Count };

// An operation in flight on the ring. complete() runs on the network thread when its completion is reaped, with the
// kernel's result (bytes transferred or -errno) and the completion's IORING_CQE_F_* flags. The object must stay valid
// until its last completion (a multishot operation completes until one arrives without IORING_CQE_F_MORE).
struct Op {
	virtual void complete(int32_t res, uint32_t flags) = 0;

protected:
	~Op() = default;
};

struct Stats {
	int64_t submitted[int(Kind::Count)] = {};
	int64_t completed[int(Kind::Count)] = {};
	int64_t enters = 0; // io_uring_enter calls
	int64_t waits = 0; // io_uring_enter calls that waited for a completion
	int64_t recvNoBuffers = 0; // multishot receives stopped because the provided buffer ring was empty
};

// Notified when provided receive buffers are returned to an empty buffer ring.
struct BufferWaiter {
	virtual void buffersAvailable() = 0;

protected:
	~BufferWaiter() = default;
};

class Ring {
public:
	// The ring, created on the first call. Null on non-Linux, when the network is simulated, or when setup failed
	// (logged once as IoUringSetupFailed).
	static Ring* get();
	// The ring if it has been created, without creating it.
	static Ring* existing();

	// A zeroed submission queue entry to fill and pass to queue(); flushes the queue first if it is full.
	io_uring_sqe* sqe();
	// Queues a filled entry; op->complete() runs when it completes.
	void queue(io_uring_sqe* sqe, Op* op, Kind kind);

	// Submits queued entries without waiting (no syscall when nothing is queued).
	void flush();
	// Submits queued entries and waits up to timeoutSeconds for a completion; wake() and readiness of the watched
	// epoll descriptor also end the wait. Does nothing more than flush() when completions are already available.
	void wait(double timeoutSeconds);
	// Runs complete() for every available completion; returns how many.
	int reap();

	// Ends a wait(); callable from any thread.
	void wake();

	// Watches an epoll descriptor (Asio's) while waiting: takeEpollReady() then reports whether it became readable,
	// and re-arms the watch.
	void watchEpoll(int epollFd);
	bool takeEpollReady();
	// Whether wake() was called since the last call.
	bool takeWoken();

	// Provided receive buffers (a registered buffer ring, kernel 6.1+ here): multishot receives pick them, and the
	// completion flags name the buffer. Every buffer handed out must come back through recycleBuffer().
	bool hasBufferRing() const { return bufRing != nullptr; }
	uint8_t* bufferData(uint16_t bid) const { return bufMem + size_t(bid) * bufSize; }
	void recycleBuffer(uint16_t bid);
	// For a receive that ran out of buffers: false when some have come back since (the caller may re-arm at once);
	// otherwise calls waiter->buffersAvailable() once, after the next recycleBuffer(), unless cancelled first.
	bool waitForBuffers(BufferWaiter* waiter);
	void cancelBufferWait(BufferWaiter* waiter);
	void noteRecvNoBuffers() { ++counters.recvNoBuffers; }

	unsigned queued() const { return unsubmitted; }
	int descriptor() const { return fd; }
	const Stats& stats() const { return counters; }

	// Logs IoUringMetrics at most every intervalSeconds.
	void maybeLogMetrics(double now, double intervalSeconds);

private:
	Ring() = default;
	bool init(unsigned entries);
	void initBufferRing();
	int enter(unsigned toSubmit, unsigned minComplete, unsigned flags, const void* arg, unsigned argSize);
	bool taskWorkPending() const;
	void armInternal();

	struct InternalOp : Op {
		Ring* ring = nullptr;
		bool armed = false;
		bool fired = false;
		void complete(int32_t res, uint32_t flags) override;
	};

	int fd = -1;
	int enterFd = -1; // registered index with enterFlags = IORING_ENTER_REGISTERED_RING, else fd
	unsigned enterFlags = 0;
	unsigned setupFlags = 0;
	bool deferTaskrun = false;
	unsigned sqEntries = 0;
	unsigned* sqHead = nullptr;
	unsigned* sqTail = nullptr;
	unsigned* sqMask = nullptr;
	unsigned* sqFlags = nullptr;
	unsigned* sqArray = nullptr;
	io_uring_sqe* sqes = nullptr;
	unsigned* cqHead = nullptr;
	unsigned* cqTail = nullptr;
	unsigned* cqMask = nullptr;
	::io_uring_cqe* cqes = nullptr;
	unsigned localTail = 0;
	unsigned unsubmitted = 0;
	bool reaping = false;

	int wakeFd = -1;
	uint64_t wakeValue = 0;
	InternalOp wakeOp;
	int epollFd = -1;
	InternalOp epollOp;

	struct BufRingEntry; // struct io_uring_buf
	BufRingEntry* bufRing = nullptr;
	uint16_t* bufRingTail = nullptr;
	unsigned bufCount = 0;
	unsigned bufSize = 0;
	uint8_t* bufMem = nullptr;
	uint16_t bufTail = 0;
	unsigned bufOutstanding = 0; // handed out in completions and not yet recycled
	std::vector<BufferWaiter*> bufferWaiters;

	Stats counters;
	Stats lastLogged;
	double lastLogTime = 0;
};

// Completion flags (IORING_CQE_F_*), for users that do not include <linux/io_uring.h>.
constexpr uint32_t kCqeBuffer = 1U << 0; // the buffer id is in the upper 16 bits
constexpr uint32_t kCqeMore = 1U << 1; // a multishot operation stays armed
inline uint16_t cqeBufferId(uint32_t flags) {
	return uint16_t(flags >> 16);
}

// Fills a zeroed entry.
void prepRecv(io_uring_sqe* sqe, int fd, void* buf, unsigned len, int flags);
// A receive that stays armed and completes once per chunk of data, each into a provided buffer (hasBufferRing()).
void prepRecvMultishot(io_uring_sqe* sqe, int fd);
void prepSend(io_uring_sqe* sqe, int fd, const void* buf, unsigned len, int flags);
// msg and the iovecs it names must stay valid until the completion.
void prepSendmsg(io_uring_sqe* sqe, int fd, const msghdr* msg, int flags);
void prepRead(io_uring_sqe* sqe, int fd, void* buf, unsigned len, uint64_t offset);
void prepWrite(io_uring_sqe* sqe, int fd, const void* buf, unsigned len, uint64_t offset);
void prepFsync(io_uring_sqe* sqe, int fd, bool datasync);

} // namespace iouring

#endif
