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

struct io_uring_sqe;
struct io_uring_cqe;

namespace iouring {

enum class Kind : int { NetRecv, NetSend, DiskRead, DiskWrite, DiskFsync, Internal, Count };

// An operation in flight on the ring. complete() runs on the network thread when its completion is reaped, with the
// kernel's result (bytes transferred or -errno). The object must stay valid until then.
struct Op {
	virtual void complete(int32_t res) = 0;

protected:
	~Op() = default;
};

struct Stats {
	int64_t submitted[int(Kind::Count)] = {};
	int64_t completed[int(Kind::Count)] = {};
	int64_t enters = 0; // io_uring_enter calls
	int64_t waits = 0; // io_uring_enter calls that waited for a completion
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

	unsigned queued() const { return unsubmitted; }
	int descriptor() const { return fd; }
	const Stats& stats() const { return counters; }

	// Logs IoUringMetrics at most every intervalSeconds.
	void maybeLogMetrics(double now, double intervalSeconds);

private:
	Ring() = default;
	bool init(unsigned entries);
	int enter(unsigned toSubmit, unsigned minComplete, unsigned flags, const void* arg, unsigned argSize);
	bool taskWorkPending() const;
	void armInternal();

	struct InternalOp : Op {
		Ring* ring = nullptr;
		bool armed = false;
		bool fired = false;
		void complete(int32_t res) override;
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

	Stats counters;
	Stats lastLogged;
	double lastLogTime = 0;
};

// Fills a zeroed entry.
void prepRecv(io_uring_sqe* sqe, int fd, void* buf, unsigned len, int flags);
void prepSend(io_uring_sqe* sqe, int fd, const void* buf, unsigned len, int flags);
void prepRead(io_uring_sqe* sqe, int fd, void* buf, unsigned len, uint64_t offset);
void prepWrite(io_uring_sqe* sqe, int fd, const void* buf, unsigned len, uint64_t offset);
void prepFsync(io_uring_sqe* sqe, int fd, bool datasync);

} // namespace iouring

#endif
