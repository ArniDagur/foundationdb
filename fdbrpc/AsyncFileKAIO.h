/*
 * AsyncFileKAIO.h
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

#pragma once
#ifdef __linux__

#include "flow/IoUring.h"
#include "flow/IAsyncFile.h"

#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <linux/fiemap.h>
#include <linux/fs.h>
#include <linux/io_uring.h>
#include <atomic>
#include <type_traits>
#include "linux_kaio.h"
#include "flow/Knobs.h"
#include "fdbrpc/Stats.h"
#include "crc32/crc32c.h"
#include "flow/genericactors.h"

// Set this to true to enable detailed KAIO request logging, which currently is written to a hardcoded location
// /data/v7/fdb/
#define KAIO_LOGGING 0

struct SlowAioSubmit {
	int64_t submitDuration;
	int64_t truncateDuration;
	int64_t numTruncates;
	int64_t truncateBytes;
	int64_t largestTruncate;
};

template <>
struct Descriptor<SlowAioSubmit>
  : DescribeType<SlowAioSubmit,
                 "SlowAioSubmit",
                 DescribeField<&SlowAioSubmit::submitDuration, "submitDuration", "ns">,
                 DescribeField<&SlowAioSubmit::truncateDuration, "truncateDuration", "ns">,
                 DescribeField<&SlowAioSubmit::numTruncates, "numTruncates">,
                 DescribeField<&SlowAioSubmit::truncateBytes, "truncateBytes">,
                 DescribeField<&SlowAioSubmit::largestTruncate, "largestTruncate">> {};

class AsyncFileKAIO final : public IAsyncFile, public ReferenceCounted<AsyncFileKAIO> {
public:
	StringRef getClassName() override { return "AsyncFileKAIO"_sr; }

	struct AsyncFileKAIOMetrics {
		LatencySample readLatencySample = { "AsyncFileKAIOReadLatency",
			                                UID(),
			                                FLOW_KNOBS->KAIO_LATENCY_LOGGING_INTERVAL,
			                                FLOW_KNOBS->KAIO_LATENCY_SKETCH_ACCURACY };
		LatencySample writeLatencySample = { "AsyncFileKAIOWriteLatency",
			                                 UID(),
			                                 FLOW_KNOBS->KAIO_LATENCY_LOGGING_INTERVAL,
			                                 FLOW_KNOBS->KAIO_LATENCY_SKETCH_ACCURACY };
		LatencySample syncLatencySample = { "AsyncFileKAIOSyncLatency",
			                                UID(),
			                                FLOW_KNOBS->KAIO_LATENCY_LOGGING_INTERVAL,
			                                FLOW_KNOBS->KAIO_LATENCY_SKETCH_ACCURACY };
	};

	static AsyncFileKAIOMetrics& getMetrics() {
		static AsyncFileKAIOMetrics metrics;
		return metrics;
	}

#if KAIO_LOGGING
private:
#pragma pack(push, 1)
	struct OpLogEntry {
		OpLogEntry() : result(0) {}
		enum EOperation { READ = 1, WRITE = 2, SYNC = 3, TRUNCATE = 4 };
		enum EStage { START = 1, LAUNCH = 2, REQUEUE = 3, COMPLETE = 4, READY = 5 };
		int64_t timestamp;
		uint32_t id;
		uint32_t checksum;
		uint32_t pageOffset;
		uint8_t pageCount;
		uint8_t op;
		uint8_t stage;
		uint32_t result;

		static uint32_t nextID() {
			static uint32_t last = 0;
			return ++last;
		}

		void log(FILE* file) {
			if (ftell(file) > (int64_t)50 * 1e9)
				fseek(file, 0, SEEK_SET);
			if (!fwrite(this, sizeof(OpLogEntry), 1, file))
				throw io_error();
		}
	};
#pragma pop

	FILE* logFile;
	struct IOBlock;
	static void KAIOLogBlockEvent(IOBlock* ioblock, OpLogEntry::EStage stage, uint32_t result = 0);
	static void KAIOLogBlockEvent(FILE* logFile, IOBlock* ioblock, OpLogEntry::EStage stage, uint32_t result = 0);
	static void KAIOLogEvent(FILE* logFile,
	                         uint32_t id,
	                         OpLogEntry::EOperation op,
	                         OpLogEntry::EStage stage,
	                         uint32_t pageOffset = 0,
	                         uint32_t result = 0);

public:
#else
#define KAIOLogBlockEvent(...)
#define KAIOLogEvent(...)
#endif

	static Future<Reference<IAsyncFile>> open(std::string filename, int flags, int mode, void* ignore) {
		ASSERT(!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO);
		ASSERT(flags & OPEN_UNBUFFERED);

		if (flags & OPEN_LOCK)
			mode |= 02000; // Enable mandatory locking for this file if it is supported by the filesystem

		std::string open_filename = filename;
		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			ASSERT((flags & OPEN_CREATE) && (flags & OPEN_READWRITE) && !(flags & OPEN_EXCLUSIVE));
			open_filename = filename + ".part";
		}

		int fd = ::open(open_filename.c_str(), openFlags(flags), mode);
		if (fd < 0) {
			Error e = errno == ENOENT ? file_not_found() : io_error();
			int ecode = errno; // Save errno in case it is modified before it is used below
			TraceEvent ev("AsyncFileKAIOOpenFailed");
			ev.error(e)
			    .detail("Filename", filename)
			    .detailf("Flags", "%x", flags)
			    .detailf("OSFlags", "%x", openFlags(flags))
			    .detailf("Mode", "0%o", mode)
			    .GetLastError();
			if (ecode == EINVAL)
				ev.detail("Description", "Invalid argument - Does the target filesystem support KAIO?");
			return e;
		} else {
			TraceEvent("AsyncFileKAIOOpen")
			    .detail("Filename", filename)
			    .detail("Flags", flags)
			    .detail("Mode", mode)
			    .detail("Fd", fd);
		}

		Reference<AsyncFileKAIO> r(new AsyncFileKAIO(fd, flags, filename));

		if (flags & OPEN_LOCK) {
			// Acquire a "write" lock for the entire file
			flock lockDesc;
			lockDesc.l_type = F_WRLCK;
			lockDesc.l_whence = SEEK_SET;
			lockDesc.l_start = 0;
			lockDesc.l_len =
			    0; // "Specifying 0 for l_len has the special meaning: lock all bytes starting at the location specified
			       // by l_whence and l_start through to the end of file, no matter how large the file grows."
			lockDesc.l_pid = 0;
			if (fcntl(fd, F_SETLK, &lockDesc) == -1) {
				TraceEvent(SevWarn, "UnableToLockFile").detail("Filename", filename).GetLastError();
				return lock_file_failure();
			}
		}

		struct stat buf;
		if (fstat(fd, &buf)) {
			TraceEvent("AsyncFileKAIOFStatError").detail("Fd", fd).detail("Filename", filename).GetLastError();
			return io_error();
		}
		r->lastFileSize = r->nextFileSize = buf.st_size;
		if (FLOW_KNOBS->KAIO_ZERO_FILL_GROWTH && (flags & OPEN_READWRITE)) {
			return writeUnwrittenRanges(r);
		}
		return Reference<IAsyncFile>(std::move(r));
	}

	// Unwritten extents (from fallocate) read as zeros, but each write into one is converted by a filesystem
	// transaction holding the inode lock exclusively at its completion; meanwhile io_uring's non-blocking attempts on
	// the file fail and go to io-wq worker threads (libaio waits on the lock instead). Writing zeros over them once, in
	// large writes, leaves only written extents, which later page-size writes overwrite without a conversion. Done
	// before the file is handed out, so no other write can overlap.
	static Future<Reference<IAsyncFile>> writeUnwrittenRanges(Reference<AsyncFileKAIO> self) {
		for (const auto& [begin, end] : unwrittenRanges(self->fd, self->lastFileSize)) {
			co_await self->writeZeros(begin, end);
		}
		co_return Reference<IAsyncFile>(self);
	}

	// [begin, end) of the unwritten extents in the first size bytes of fd, 4 KiB aligned.
	static std::vector<std::pair<int64_t, int64_t>> unwrittenRanges(int fd, int64_t size) {
		std::vector<std::pair<int64_t, int64_t>> ranges;
		constexpr int kExtents = 256;
		std::vector<uint8_t> mem(sizeof(fiemap) + kExtents * sizeof(fiemap_extent));
		auto* map = reinterpret_cast<fiemap*>(mem.data());
		uint64_t start = 0;
		while (start < uint64_t(size)) {
			memset(mem.data(), 0, mem.size());
			map->fm_start = start;
			map->fm_length = uint64_t(size) - start;
			map->fm_extent_count = kExtents;
			if (ioctl(fd, FS_IOC_FIEMAP, map) != 0 || map->fm_mapped_extents == 0) {
				break;
			}
			bool last = false;
			for (uint32_t i = 0; i < map->fm_mapped_extents; i++) {
				const fiemap_extent& e = map->fm_extents[i];
				if (e.fe_flags & FIEMAP_EXTENT_UNWRITTEN) {
					const int64_t b = std::max<int64_t>(e.fe_logical, start) & ~int64_t(4095);
					const int64_t en = std::min<int64_t>(e.fe_logical + e.fe_length, size) & ~int64_t(4095);
					if (en > b) {
						ranges.emplace_back(b, en);
					}
				}
				start = e.fe_logical + e.fe_length;
				last = last || (e.fe_flags & FIEMAP_EXTENT_LAST);
			}
			if (last) {
				break;
			}
		}
		return ranges;
	}

	// Writes zeros over [begin, end) (4 KiB aligned) through this file's own I/O path, a few megabytes at a time.
	Future<Void> writeZeros(int64_t begin, int64_t end) {
		static uint8_t* zeros = [] {
			auto* z = static_cast<uint8_t*>(aligned_alloc(4096, kZeroChunk));
			memset(z, 0, kZeroChunk);
			return z;
		}();
		std::vector<Future<Void>> inFlight;
		for (int64_t pos = begin; pos < end; pos += kZeroChunk) {
			const int len = int(std::min<int64_t>(kZeroChunk, end - pos));
			inFlight.push_back(write(zeros, len, pos));
			ctx.zeroFilledBytes += len;
			if (inFlight.size() >= 4) {
				co_await waitForAll(inFlight);
				inFlight.clear();
			}
		}
		co_await waitForAll(inFlight);
	}
	static constexpr int kZeroChunk = 1 << 20;

	static void init(Reference<IEventFD> ev, double ioTimeout) {
		ASSERT(!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO);
		if (!g_network->isSimulated()) {
			ctx.countAIOSubmit.init("AsyncFile.CountAIOSubmit"_sr);
			ctx.countAIOCollect.init("AsyncFile.CountAIOCollect"_sr);
			ctx.submitMetric.init("AsyncFile.Submit"_sr);
			ctx.countPreSubmitTruncate.init("AsyncFile.CountPreAIOSubmitTruncate"_sr);
			ctx.preSubmitTruncateBytes.init("AsyncFile.PreAIOSubmitTruncateBytes"_sr);
			ctx.slowAioSubmitMetric.init("AsyncFile.SlowAIOSubmit"_sr);
		}

		int rc = io_setup(FLOW_KNOBS->MAX_OUTSTANDING, &ctx.iocx);
		if (rc < 0) {
			TraceEvent("IOSetupError").GetLastError();
			throw io_error();
		}
		setTimeout(ioTimeout);
		ctx.evfd = ev->getFD();
		poll(Uncancellable(), ev);

		g_network->setGlobal(INetwork::enRunCycleFunc, (flowGlobalType)&AsyncFileKAIO::launch);
	}

	static int get_eventfd() { return ctx.evfd; }
	static void setTimeout(double ioTimeout) { ctx.setIOTimeout(ioTimeout); }

	void addref() override { ReferenceCounted<AsyncFileKAIO>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileKAIO>::delref(); }
	Future<int> read(void* data, int length, int64_t offset) override {
		++countFileLogicalReads;
		++countLogicalReads;
		// printf("%p Begin logical read\n", getCurrentCoro());

		if (failed) {
			return io_timeout();
		}

		const int chunk = maxIOBytes();
		if (chunk > 0 && length > chunk) {
			std::vector<Future<int>> parts;
			for (int done = 0; done < length; done += chunk) {
				IOBlock* io = new IOBlock(IO_CMD_PREAD, fd);
				io->buf = static_cast<uint8_t*>(data) + done;
				io->nbytes = std::min(chunk, length - done);
				io->offset = offset + done;
				enqueue(io, "read", this);
				parts.push_back(io->result.getFuture());
			}
			return sumReadParts(std::move(parts));
		}

		IOBlock* io = new IOBlock(IO_CMD_PREAD, fd);
		io->buf = data;
		io->nbytes = length;
		io->offset = offset;

		enqueue(io, "read", this);
		Future<int> result = io->result.getFuture();

#if KAIO_LOGGING
		// result = map(result, [=](int r) mutable { KAIOLogBlockEvent(io, OpLogEntry::READY, r); return r; });
#endif

		return result;
	}

	// Effective per-iocb size limit from KAIO_MAX_IO_BYTES, or 0 if reads/writes are not split.
	static int maxIOBytes() {
		const int limit = FLOW_KNOBS->KAIO_MAX_IO_BYTES;
		return limit > 0 ? std::max(4096, limit / 4096 * 4096) : 0;
	}

	// A split read returns the total bytes read. Each part only comes up short at end of file, and every later
	// part then reads 0 bytes, so the sum matches what a single read of the whole range returns. All parts must
	// finish before returning or throwing, because the caller may release the buffer as soon as we do.
	static Future<int> sumReadParts(std::vector<Future<int>> parts) {
		co_await waitForAllReady(parts);
		int total = 0;
		for (auto& part : parts) {
			total += part.get();
		}
		co_return total;
	}
	Future<Void> write(void const* data, int length, int64_t offset) override {
		++countFileLogicalWrites;
		++countLogicalWrites;
		// printf("%p Begin logical write\n", getCurrentCoro());

		if (failed) {
			return io_timeout();
		}

		const int chunk = maxIOBytes();
		if (chunk > 0 && length > chunk) {
			nextFileSize = std::max(nextFileSize, offset + length);
			std::vector<Future<Void>> parts;
			for (int done = 0; done < length; done += chunk) {
				IOBlock* io = new IOBlock(IO_CMD_PWRITE, fd);
				io->buf = (void*)(static_cast<const uint8_t*>(data) + done);
				io->nbytes = std::min(chunk, length - done);
				io->offset = offset + done;
				enqueue(io, "write", this);
				parts.push_back(io->writeResult.getFuture());
			}
			// Not waitForAll(): the caller may release the buffer once this returns, so a failed part must not
			// complete the write while other parts are still in flight.
			return waitForAllReadyThenThrow(parts);
		}

		IOBlock* io = new IOBlock(IO_CMD_PWRITE, fd);
		io->buf = (void*)data;
		io->nbytes = length;
		io->offset = offset;

		nextFileSize = std::max(nextFileSize, offset + length);

		enqueue(io, "write", this);
		Future<Void> result = io->writeResult.getFuture();

#if KAIO_LOGGING
		// result = map(result, [=](int r) mutable { KAIOLogBlockEvent(io, OpLogEntry::READY, r); return r; });
#endif

		// auto& actorLineageSet = IAsyncFileSystem::filesystem()->getActorLineageSet();
		// auto index = actorLineageSet.insert(*currentLineage);
		// ASSERT(index != ActorLineageSet::npos);
		// actorLineageSet.erase(index);
		return result;
	}
// TODO(alexmiller): Remove when we upgrade the dev docker image to >14.10
#ifndef FALLOC_FL_ZERO_RANGE
#define FALLOC_FL_ZERO_RANGE 0x10
#endif
	Future<Void> zeroRange(int64_t offset, int64_t length) override {
		if (FLOW_KNOBS->KAIO_ZERO_FILL_GROWTH && offset % 4096 == 0 && length % 4096 == 0) {
			return writeZeros(offset, offset + length); // FALLOC_FL_ZERO_RANGE would leave unwritten extents
		}
		bool success = false;
		if (ctx.fallocateZeroSupported) {
			int rc = fallocate(fd, FALLOC_FL_ZERO_RANGE, offset, length);
			if (rc == EOPNOTSUPP) {
				ctx.fallocateZeroSupported = false;
			}
			if (rc == 0) {
				success = true;
			}
		}
		return success ? Void() : IAsyncFile::zeroRange(offset, length);
	}
	Future<Void> truncate(int64_t size) override {
		++countFileLogicalWrites;
		++countLogicalWrites;

		if (failed) {
			return io_timeout();
		}

#if KAIO_LOGGING
		uint32_t id = OpLogEntry::nextID();
#endif
		int result = -1;
		KAIOLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::START, size / 4096);
		bool completed = false;
		double begin = timer_monotonic();
		if (FLOW_KNOBS->KAIO_ZERO_FILL_GROWTH && size > lastFileSize && lastFileSize % 4096 == 0 && size % 4096 == 0) {
			// Grow with written zeros instead of unwritten extents (see writeUnwrittenRanges()). The new size is
			// recorded first so that launch() does not fallocate ahead of the zero writes.
			const int64_t from = lastFileSize;
			lastFileSize = nextFileSize = size;
			KAIOLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::COMPLETE, size / 4096, 0);
			return writeZeros(from, size);
		}
		if (ctx.fallocateSupported && size >= lastFileSize) {
			result = fallocate(fd, 0, 0, size);
			if (result != 0) {
				int fallocateErrCode = errno;
				TraceEvent("AsyncFileKAIOAllocateError")
				    .detail("Fd", fd)
				    .detail("Filename", filename)
				    .detail("Size", size)
				    .GetLastError();
				if (fallocateErrCode == EOPNOTSUPP) {
					// Mark fallocate as unsupported. Try again with truncate.
					ctx.fallocateSupported = false;
				} else {
					KAIOLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::COMPLETE, size / 4096, result);
					return io_error();
				}
			} else {
				completed = true;
			}
		}
		if (!completed)
			result = ftruncate(fd, size);

		double end = timer_monotonic();
		if (nondeterministicRandom()->random01() < end - begin) {
			TraceEvent("SlowKAIOTruncate")
			    .detail("TruncateTime", end - begin)
			    .detail("TruncateBytes", size - lastFileSize);
		}
		KAIOLogEvent(logFile, id, OpLogEntry::TRUNCATE, OpLogEntry::COMPLETE, size / 4096, result);

		if (result != 0) {
			TraceEvent("AsyncFileKAIOTruncateError").detail("Fd", fd).detail("Filename", filename).GetLastError();
			return io_error();
		}

		lastFileSize = nextFileSize = size;

		return Void();
	}

	static Future<Void> throwErrorIfFailed(Reference<AsyncFileKAIO> self, Future<Void> sync) {
		co_await sync;
		if (self->failed) {
			throw io_timeout();
		}
	}

	Future<Void> sync() override {
		++countFileLogicalWrites;
		++countLogicalWrites;

		if (failed) {
			return io_timeout();
		}

#if KAIO_LOGGING
		uint32_t id = OpLogEntry::nextID();
#endif

		KAIOLogEvent(logFile, id, OpLogEntry::SYNC, OpLogEntry::START);
		double start_time = timer();

		Future<Void> fsync;
		const int mode = FLOW_KNOBS->KAIO_FDSYNC;
		if (FLOW_KNOBS->KAIO_IO_URING && iouring::Ring::get()) {
			// Queued like reads and writes and submitted to the network thread's ring by launch(). Callers only sync
			// after their writes have completed, so no ordering against in-flight requests is needed.
			IOBlock* io = new IOBlock(IO_CMD_FDSYNC, fd);
			enqueue(io, "sync", this);
			++ctx.fdsyncSubmitted;
			fsync = throwErrorIfFailed(Reference<AsyncFileKAIO>::addRef(this),
			                           fdsyncOrFallback(io->result.getFuture(), fd));
		} else if (mode == 2 && ctx.fdsyncSupported && uringReady()) {
			// Callers only sync after their writes have completed, so no ordering against in-flight iocbs is
			// needed. The completion is posted to the KAIO eventfd and reaped by poll().
			IOBlock* io = new IOBlock(IO_CMD_FDSYNC, fd);
			io->owner = Reference<AsyncFileKAIO>::addRef(this);
			io->prio = int64_t(g_network->getCurrentTask()) << 32;
			Future<int> r = io->result.getFuture();
			++ctx.fdsyncSubmitted;
			uringSubmitFsync(io);
			fsync = throwErrorIfFailed(Reference<AsyncFileKAIO>::addRef(this), fdsyncOrFallback(r, fd));
		} else if (mode == 1 && ctx.fdsyncSupported) {
			// As above; the kernel runs the fdatasync on the submitting CPU's workqueue.
			IOBlock* io = new IOBlock(IO_CMD_FDSYNC, fd);
			enqueue(io, "sync", this);
			++ctx.fdsyncSubmitted;
			fsync = throwErrorIfFailed(Reference<AsyncFileKAIO>::addRef(this),
			                           fdsyncOrFallback(io->result.getFuture(), fd));
		} else {
			fsync = throwErrorIfFailed(
			    Reference<AsyncFileKAIO>::addRef(this),
			    AsyncFileEIO::async_fdatasync(fd)); // Don't close the file until the asynchronous thing is done
		}

		fsync = map(fsync, [=](Void r) mutable {
			KAIOLogEvent(logFile, id, OpLogEntry::SYNC, OpLogEntry::COMPLETE);
			getMetrics().syncLatencySample.addMeasurement(timer() - start_time);
			return r;
		});

		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE) {
			flags &= ~OPEN_ATOMIC_WRITE_AND_CREATE;

			return AsyncFileEIO::waitAndAtomicRename(fsync, filename + ".part", filename);
		}

		return fsync;
	}
	// Result a rejected fdsync is delivered as; a successful one completes with 0.
	static constexpr int kFdsyncUnsupported = 1;

	// Kernels before 4.18, and filesystems without aio fsync, reject IOCB_CMD_FDSYNC with EINVAL; from then on
	// every sync() uses the EIO thread pool.
	static Future<Void> fdsyncOrFallback(Future<int> fdsync, int fd) {
		const int r = co_await fdsync;
		if (r == kFdsyncUnsupported) {
			if (ctx.fdsyncSupported) {
				ctx.fdsyncSupported = false;
				TraceEvent(SevWarnAlways, "AsyncFileKAIOFdsyncUnsupported").detail("Fd", fd);
			}
			++ctx.fdsyncFallbacks;
			co_await AsyncFileEIO::async_fdatasync(fd);
		}
	}

	struct FdsyncStats {
		int64_t submitted;
		int64_t fallbacks;
		bool supported;
	};
	static FdsyncStats getFdsyncStats() {
		return FdsyncStats{ ctx.fdsyncSubmitted, ctx.fdsyncFallbacks, ctx.fdsyncSupported };
	}
	static void resetFdsyncStats() {
		ctx.fdsyncSubmitted = ctx.fdsyncFallbacks = 0;
		ctx.fdsyncSupported = true;
	}

	Future<int64_t> size() const override { return nextFileSize; }
	int64_t debugFD() const override { return fd; }
	std::string getFilename() const override { return filename; }
	~AsyncFileKAIO() override {
		close(fd);

#if KAIO_LOGGING
		if (logFile != nullptr)
			fclose(logFile);
#endif
	}

	static void launch() {
		if (!ctx.queue.empty() && ctx.outstanding < FLOW_KNOBS->MAX_OUTSTANDING - FLOW_KNOBS->MIN_SUBMIT) {
			ctx.submitMetric = true;

			double begin = timer_monotonic();
			if (!ctx.outstanding)
				ctx.ioStallBegin = begin;

			IOBlock* toStart[FLOW_KNOBS->MAX_OUTSTANDING];
			int n = std::min<size_t>(FLOW_KNOBS->MAX_OUTSTANDING - ctx.outstanding, ctx.queue.size());

			int64_t previousTruncateCount = ctx.countPreSubmitTruncate;
			int64_t previousTruncateBytes = ctx.preSubmitTruncateBytes;
			int64_t largestTruncate = 0;

			double start = timer();
			for (int i = 0; i < n; i++) {
				auto io = ctx.queue.top();

				KAIOLogBlockEvent(io, OpLogEntry::LAUNCH);

				ctx.queue.pop();
				toStart[i] = io;
				io->startTime = start;

				if (ctx.ioTimeout > 0) {
					ctx.appendToRequestList(io);
				}

				if (io->owner->lastFileSize != io->owner->nextFileSize) {
					++ctx.countPreSubmitTruncate;
					int64_t truncateSize = io->owner->nextFileSize - io->owner->lastFileSize;
					ASSERT_GT(truncateSize, 0);
					ctx.preSubmitTruncateBytes += truncateSize;
					largestTruncate = std::max(largestTruncate, truncateSize);
					io->owner->truncate(io->owner->nextFileSize);
				}
			}
			double truncateComplete = timer_monotonic();
			int rc = submitToRing(toStart, n);
			if (rc < 0) {
				rc = submitIOCBs(toStart, n);
			}
			double end = timer_monotonic();

			if (end - begin > FLOW_KNOBS->SLOW_LOOP_CUTOFF) {
				ctx.slowAioSubmitMetric->submitDuration = end - truncateComplete;
				ctx.slowAioSubmitMetric->truncateDuration = truncateComplete - begin;
				ctx.slowAioSubmitMetric->numTruncates = ctx.countPreSubmitTruncate - previousTruncateCount;
				ctx.slowAioSubmitMetric->truncateBytes = ctx.preSubmitTruncateBytes - previousTruncateBytes;
				ctx.slowAioSubmitMetric->largestTruncate = largestTruncate;
				ctx.slowAioSubmitMetric->log();

				if (nondeterministicRandom()->random01() < end - begin) {
					TraceEvent("SlowKAIOLaunch")
					    .detail("IOSubmitTime", end - truncateComplete)
					    .detail("TruncateTime", truncateComplete - begin)
					    .detail("TruncateCount", ctx.countPreSubmitTruncate - previousTruncateCount)
					    .detail("TruncateBytes", ctx.preSubmitTruncateBytes - previousTruncateBytes)
					    .detail("LargestTruncate", largestTruncate);
				}
			}

			ctx.submitMetric = false;
			++ctx.countAIOSubmit;

			double elapsed = timer_monotonic() - begin;
			g_network->networkInfo.metrics.secSquaredSubmit += elapsed * elapsed / 2;

			//TraceEvent("Launched").detail("N", rc).detail("Queued", ctx.queue.size()).detail("Elapsed", elapsed).detail("Outstanding", ctx.outstanding+rc);
			// printf("launched: %d/%d in %f us (%d outstanding; lowest prio %d)\n", rc, ctx.queue.size(), elapsed*1e6,
			// ctx.outstanding + rc, toStart[n-1]->getTask());
			if (rc < 0) {
				if (errno == EAGAIN) {
					rc = 0;
				} else {
					KAIOLogBlockEvent(toStart[0], OpLogEntry::COMPLETE, errno ? -errno : -1000000);
					// Other errors are assumed to represent failure to issue the first I/O in the list
					toStart[0]->setResult(errno ? -errno : -1000000);
					rc = 1;
				}
			} else {
				ctx.outstanding += rc;
			}
			// Any unsubmitted I/Os need to be requeued
			for (int i = rc; i < n; i++) {
				KAIOLogBlockEvent(toStart[i], OpLogEntry::REQUEUE);
				ctx.queue.push(toStart[i]);
			}
		}
	}

	static int64_t getRingCompletions() { return ctx.ringCompletions; }
	static int64_t getZeroFilledBytes() { return ctx.zeroFilledBytes; }

	struct SubmitStats {
		int64_t submitCalls;
		int64_t submittedIOCBs;
		int64_t largestSubmittedIOBytes;
		int64_t largestSubmitBatch;
	};
	static SubmitStats getSubmitStats() {
		return SubmitStats{ ctx.submitCalls, ctx.submittedIOCBs, ctx.largestSubmittedIOBytes, ctx.largestSubmitBatch };
	}
	static void resetSubmitStats() {
		ctx.submitCalls = ctx.submittedIOCBs = ctx.largestSubmittedIOBytes = ctx.largestSubmitBatch = 0;
	}

	bool failed;

private:
	int fd, flags;
	int64_t lastFileSize, nextFileSize;
	std::string filename;
	Int64MetricHandle countFileLogicalWrites;
	Int64MetricHandle countFileLogicalReads;

	Int64MetricHandle countLogicalWrites;
	Int64MetricHandle countLogicalReads;

	struct IOBlock : linux_iocb, FastAllocated<IOBlock> {
		// Completion of this request when it was submitted to the network thread's io_uring (KAIO_IO_URING).
		struct UringOp final : iouring::Op {
			IOBlock* io = nullptr;
			void complete(int32_t res, uint32_t) override { AsyncFileKAIO::ringCompleted(io, res); }
		};

		Promise<int> result;
		Promise<Void> writeResult;
		UringOp uringOp;
		Reference<AsyncFileKAIO> owner;
		int64_t prio;
		IOBlock* prev;
		IOBlock* next;
		double startTime;
#if KAIO_LOGGING
		int32_t iolog_id;
#endif

		struct indirect_order_by_priority {
			bool operator()(IOBlock* a, IOBlock* b) { return a->prio < b->prio; }
		};

		IOBlock(int op, int fd)
		  : result(op == IO_CMD_PWRITE ? Promise<int>(nullptr) : Promise<int>()),
		    writeResult(op == IO_CMD_PWRITE ? Promise<Void>() : Promise<Void>(nullptr)), prev(nullptr), next(nullptr),
		    startTime(0) {
			memset((linux_iocb*)this, 0, sizeof(linux_iocb));
			aio_lio_opcode = op;
			aio_fildes = fd;
			uringOp.io = this;
#if KAIO_LOGGING
			iolog_id = 0;
#endif
		}

		TaskPriority getTask() const { return static_cast<TaskPriority>((prio >> 32) + 1); }

		template <class T>
		static coro::DetachedCoroutine deliver(Promise<T> result, bool failed, int r, TaskPriority task) {
			try {
				co_await delay(0, task);
				if (failed)
					result.sendError(io_timeout());
				else if (r < 0)
					result.sendError(io_error());
				else if constexpr (std::is_same_v<T, Void>)
					result.send(Void());
				else
					result.send(r);
			} catch (const Error&) {
				// Typed completion errors have no result consumer.
			} catch (...) {
				(void)unknown_error();
			}
		}

		void setResult(int r) {
			if (aio_lio_opcode == IO_CMD_FDSYNC && r == -EINVAL) {
				deliver(std::move(result), owner->failed, kFdsyncUnsupported, getTask());
				delete this;
				return;
			}
			if (r < 0) {
				struct stat fst;
				fstat(aio_fildes, &fst);

				errno = -r;
				TraceEvent("AsyncFileKAIOIOError")
				    .GetLastError()
				    .detail("Fd", aio_fildes)
				    .detail("Op", aio_lio_opcode)
				    .detail("Nbytes", nbytes)
				    .detail("Offset", offset)
				    .detail("Ptr", int64_t(buf))
				    .detail("Size", fst.st_size)
				    .detail("Filename", owner->filename);
			}
			if (aio_lio_opcode == IO_CMD_PWRITE) {
				deliver(std::move(writeResult), owner->failed, r, getTask());
			} else {
				deliver(std::move(result), owner->failed, r, getTask());
			}
			delete this;
		}

		void timeout(bool warnOnly) {
			TraceEvent(SevWarnAlways, "AsyncFileKAIOTimeout")
			    .detail("Fd", aio_fildes)
			    .detail("Op", aio_lio_opcode)
			    .detail("Nbytes", nbytes)
			    .detail("Offset", offset)
			    .detail("Ptr", int64_t(buf))
			    .detail("Filename", owner->filename);
			g_network->setGlobal(INetwork::enASIOTimedOut, (flowGlobalType) true);

			if (!warnOnly)
				owner->failed = true;
		}
	};

	// Queues toStart[0..n) on the network thread's io_uring when KAIO_IO_URING is set; the run loop submits them.
	// Returns n, or -1 when the ring is not in use.
	static int submitToRing(IOBlock** toStart, int n) {
		if (!FLOW_KNOBS->KAIO_IO_URING) {
			return -1;
		}
		iouring::Ring* ring = iouring::Ring::get();
		if (!ring) {
			return -1;
		}
		for (int i = 0; i < n; i++) {
			IOBlock* io = toStart[i];
			io_uring_sqe* sqe = ring->sqe();
			iouring::Kind kind;
			switch (io->aio_lio_opcode) {
			case IO_CMD_PREAD:
				iouring::prepRead(sqe, io->aio_fildes, io->buf, io->nbytes, io->offset);
				kind = iouring::Kind::DiskRead;
				break;
			case IO_CMD_PWRITE:
				iouring::prepWrite(sqe, io->aio_fildes, io->buf, io->nbytes, io->offset);
				kind = iouring::Kind::DiskWrite;
				break;
			case IO_CMD_FDSYNC:
				iouring::prepFsync(sqe, io->aio_fildes, /*datasync=*/true);
				kind = iouring::Kind::DiskFsync;
				break;
			default:
				UNREACHABLE();
			}
			ring->queue(sqe, &io->uringOp, kind);
			ctx.submittedIOCBs++;
		}
		ctx.submitCalls++;
		ctx.largestSubmitBatch = std::max<int64_t>(ctx.largestSubmitBatch, n);
		return n;
	}

	// The io_uring counterpart of one completion in poll().
	static void ringCompleted(IOBlock* iob, int32_t res) {
		--ctx.outstanding;
		++ctx.ringCompletions;
		if (ctx.ioTimeout > 0) {
			ctx.removeFromRequestList(iob);
		}
		const double currentTime = timer();
		switch (iob->aio_lio_opcode) {
		case IO_CMD_PREAD:
			getMetrics().readLatencySample.addMeasurement(currentTime - iob->startTime);
			break;
		case IO_CMD_PWRITE:
			getMetrics().writeLatencySample.addMeasurement(currentTime - iob->startTime);
			break;
		}
		KAIOLogBlockEvent(iob, OpLogEntry::COMPLETE, res);
		iob->setResult(res);
	}

	// A minimal io_uring used only for fsyncs (KAIO_FDSYNC=2), driven with raw syscalls. Its completions are
	// signalled on the KAIO eventfd so the existing poll() loop reaps them.
	struct URing {
		int fd = -1;
		bool tried = false;
		unsigned* sqHead = nullptr;
		unsigned* sqTail = nullptr;
		unsigned* sqMask = nullptr;
		unsigned* sqArray = nullptr;
		io_uring_sqe* sqes = nullptr;
		unsigned* cqHead = nullptr;
		unsigned* cqTail = nullptr;
		unsigned* cqMask = nullptr;
		io_uring_cqe* cqes = nullptr;
		int inflight = 0;
	};

	static bool uringReady() {
		URing& u = ctx.uring;
		if (u.tried) {
			return u.fd >= 0;
		}
		u.tried = true;
		io_uring_params p;
		memset(&p, 0, sizeof(p));
		const int fd = syscall(__NR_io_uring_setup, FLOW_KNOBS->MAX_OUTSTANDING, &p);
		if (fd < 0) {
			TraceEvent(SevWarnAlways, "AsyncFileKAIOUringSetupFailed").GetLastError();
			return false;
		}
		const size_t sqSize = p.sq_off.array + p.sq_entries * sizeof(unsigned);
		const size_t cqSize = p.cq_off.cqes + p.cq_entries * sizeof(io_uring_cqe);
		const bool single = p.features & IORING_FEAT_SINGLE_MMAP;
		void* sq = mmap(nullptr,
		                single ? std::max(sqSize, cqSize) : sqSize,
		                PROT_READ | PROT_WRITE,
		                MAP_SHARED | MAP_POPULATE,
		                fd,
		                IORING_OFF_SQ_RING);
		void* cq =
		    single ? sq
		           : mmap(nullptr, cqSize, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
		void* sqes = mmap(nullptr,
		                  p.sq_entries * sizeof(io_uring_sqe),
		                  PROT_READ | PROT_WRITE,
		                  MAP_SHARED | MAP_POPULATE,
		                  fd,
		                  IORING_OFF_SQES);
		int evfd = ctx.evfd;
		if (sq == MAP_FAILED || cq == MAP_FAILED || sqes == MAP_FAILED ||
		    syscall(__NR_io_uring_register, fd, IORING_REGISTER_EVENTFD, &evfd, 1) < 0) {
			TraceEvent(SevWarnAlways, "AsyncFileKAIOUringInitFailed").GetLastError();
			close(fd);
			return false;
		}
		char* sqp = static_cast<char*>(sq);
		char* cqp = static_cast<char*>(cq);
		u.sqHead = reinterpret_cast<unsigned*>(sqp + p.sq_off.head);
		u.sqTail = reinterpret_cast<unsigned*>(sqp + p.sq_off.tail);
		u.sqMask = reinterpret_cast<unsigned*>(sqp + p.sq_off.ring_mask);
		u.sqArray = reinterpret_cast<unsigned*>(sqp + p.sq_off.array);
		u.sqes = static_cast<io_uring_sqe*>(sqes);
		u.cqHead = reinterpret_cast<unsigned*>(cqp + p.cq_off.head);
		u.cqTail = reinterpret_cast<unsigned*>(cqp + p.cq_off.tail);
		u.cqMask = reinterpret_cast<unsigned*>(cqp + p.cq_off.ring_mask);
		u.cqes = reinterpret_cast<io_uring_cqe*>(cqp + p.cq_off.cqes);
		u.fd = fd;
		TraceEvent("AsyncFileKAIOUringReady").detail("SQEntries", p.sq_entries).detail("CQEntries", p.cq_entries);
		return true;
	}

	static void uringSubmitFsync(IOBlock* io) {
		URing& u = ctx.uring;
		const unsigned tail = *u.sqTail;
		const unsigned idx = tail & *u.sqMask;
		io_uring_sqe* sqe = &u.sqes[idx];
		memset(sqe, 0, sizeof(*sqe));
		sqe->opcode = IORING_OP_FSYNC;
		sqe->fd = io->aio_fildes;
		sqe->fsync_flags = IORING_FSYNC_DATASYNC;
		sqe->user_data = reinterpret_cast<uint64_t>(io);
		u.sqArray[idx] = idx;
		std::atomic_ref<unsigned>(*u.sqTail).store(tail + 1, std::memory_order_release);
		// At most one fsync per file is outstanding, far below the ring size, so the SQ cannot be full.
		const int rc = syscall(__NR_io_uring_enter, u.fd, 1, 0, 0, nullptr, 0);
		if (rc < 1) {
			// The kernel did not consume the entry; take it back and fail this sync.
			std::atomic_ref<unsigned>(*u.sqTail).store(tail, std::memory_order_release);
			io->setResult(rc < 0 ? -errno : -EAGAIN);
			return;
		}
		++u.inflight;
	}

	static void uringReap() {
		URing& u = ctx.uring;
		if (u.fd < 0 || !u.inflight) {
			return;
		}
		unsigned head = *u.cqHead;
		const unsigned tail = std::atomic_ref<unsigned>(*u.cqTail).load(std::memory_order_acquire);
		while (head != tail) {
			const io_uring_cqe& cqe = u.cqes[head & *u.cqMask];
			IOBlock* io = reinterpret_cast<IOBlock*>(cqe.user_data);
			const int res = cqe.res;
			++head;
			--u.inflight;
			io->setResult(res);
		}
		std::atomic_ref<unsigned>(*u.cqHead).store(head, std::memory_order_release);
	}

	struct Context {
		io_context_t iocx;
		int evfd;
		int outstanding;
		double ioStallBegin;
		bool fallocateSupported;
		bool fallocateZeroSupported;
		bool fdsyncSupported;
		int64_t fdsyncSubmitted;
		int64_t fdsyncFallbacks;
		URing uring;
		std::priority_queue<IOBlock*, std::vector<IOBlock*>, IOBlock::indirect_order_by_priority> queue;
		Int64MetricHandle countAIOSubmit;
		Int64MetricHandle countAIOCollect;
		Int64MetricHandle submitMetric;

		double ioTimeout;
		bool timeoutWarnOnly;
		IOBlock* submittedRequestList;

		Int64MetricHandle countPreSubmitTruncate;
		Int64MetricHandle preSubmitTruncateBytes;

		EventMetricHandle<SlowAioSubmit> slowAioSubmitMetric;

		uint32_t opsIssued;
		int64_t submitCalls;
		int64_t submittedIOCBs;
		int64_t largestSubmittedIOBytes;
		int64_t largestSubmitBatch;
		int64_t ringCompletions = 0;
		int64_t zeroFilledBytes = 0;
		Context()
		  : iocx(0), evfd(-1), outstanding(0), ioStallBegin(0), fallocateSupported(true), fallocateZeroSupported(true),
		    fdsyncSupported(true), fdsyncSubmitted(0), fdsyncFallbacks(0), submittedRequestList(nullptr), opsIssued(0),
		    submitCalls(0), submittedIOCBs(0), largestSubmittedIOBytes(0), largestSubmitBatch(0) {
			setIOTimeout(0);
		}

		void setIOTimeout(double timeout) {
			ioTimeout = fabs(timeout);
			timeoutWarnOnly = timeout < 0;
		}

		void appendToRequestList(IOBlock* io) {
			ASSERT(!io->next && !io->prev);

			if (submittedRequestList) {
				io->prev = submittedRequestList->prev;
				io->prev->next = io;

				submittedRequestList->prev = io;
				io->next = submittedRequestList;
			} else {
				submittedRequestList = io;
				io->next = io->prev = io;
			}
		}

		void removeFromRequestList(IOBlock* io) {
			if (io->next == nullptr) {
				ASSERT(io->prev == nullptr);
				return;
			}

			ASSERT(io->prev != nullptr);

			if (io == io->next) {
				ASSERT(io == submittedRequestList && io == io->prev);
				submittedRequestList = nullptr;
			} else {
				io->next->prev = io->prev;
				io->prev->next = io->next;

				if (submittedRequestList == io) {
					submittedRequestList = io->next;
				}
			}

			io->next = io->prev = nullptr;
		}
	};
	static Context ctx;

	// Submits toStart[0..n) in io_submit() calls of at most KAIO_MAX_IOCBS_PER_SUBMIT iocbs. Returns the number
	// of iocbs accepted, or -1 with errno set if the first iocb could not be submitted, like a single io_submit().
	static int submitIOCBs(IOBlock** toStart, int n) {
		const int batch = FLOW_KNOBS->KAIO_MAX_IOCBS_PER_SUBMIT > 0 ? FLOW_KNOBS->KAIO_MAX_IOCBS_PER_SUBMIT : n;
		int accepted = 0;
		while (accepted < n) {
			const int m = std::min(batch, n - accepted);
			const int rc = io_submit(ctx.iocx, m, (linux_iocb**)(toStart + accepted));
			++ctx.submitCalls;
			ctx.largestSubmitBatch = std::max<int64_t>(ctx.largestSubmitBatch, m);
			if (rc < 0) {
				return accepted > 0 ? accepted : rc;
			}
			for (int i = accepted; i < accepted + rc; i++) {
				++ctx.submittedIOCBs;
				ctx.largestSubmittedIOBytes = std::max<int64_t>(ctx.largestSubmittedIOBytes, toStart[i]->nbytes);
			}
			accepted += rc;
			if (rc < m) {
				break;
			}
		}
		return accepted;
	}

	explicit AsyncFileKAIO(int fd, int flags, std::string const& filename)
	  : failed(false), fd(fd), flags(flags), filename(filename) {
		ASSERT(!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO);
		if (!g_network->isSimulated()) {
			countFileLogicalWrites.init("AsyncFile.CountFileLogicalWrites"_sr, filename);
			countFileLogicalReads.init("AsyncFile.CountFileLogicalReads"_sr, filename);
			countLogicalWrites.init("AsyncFile.CountLogicalWrites"_sr);
			countLogicalReads.init("AsyncFile.CountLogicalReads"_sr);
		}

#if KAIO_LOGGING
		logFile = nullptr;
		// TODO:  Don't do this hacky investigation-specific thing
		StringRef fname(filename);
		if (fname.endsWith(".sqlite"_sr) || fname.endsWith(".sqlite-wal"_sr)) {
			std::string logFileName = basename(filename);
			while (logFileName.find("/") != std::string::npos)
				logFileName = logFileName.substr(logFileName.find("/") + 1);
			if (!logFileName.empty()) {
				// TODO: don't hardcode this path
				std::string logPath("/data/v7/fdb/");
				try {
					platform::createDirectory(logPath);
					logFileName = logPath + format("%s.iolog", logFileName.c_str());
					logFile = fopen(logFileName.c_str(), "r+");
					if (logFile == nullptr)
						logFile = fopen(logFileName.c_str(), "w");
					if (logFile != nullptr)
						TraceEvent("KAIOLogOpened").detail("File", filename).detail("LogFile", logFileName);
					else {
						TraceEvent(SevWarn, "KAIOLogOpenFailure")
						    .detail("File", filename)
						    .detail("LogFile", logFileName)
						    .detail("ErrorCode", errno)
						    .detail("ErrorDesc", strerror(errno));
					}
				} catch (Error& e) {
					TraceEvent(SevError, "KAIOLogOpenFailure").error(e);
				}
			}
		}
#endif
	}

	void enqueue(IOBlock* io, const char* op, AsyncFileKAIO* owner) {
		ASSERT(int64_t(io->buf) % 4096 == 0 && io->offset % 4096 == 0 && io->nbytes % 4096 == 0);

		KAIOLogBlockEvent(owner->logFile, io, OpLogEntry::START);

		io->flags |= 1;
		io->eventfd = ctx.evfd;
		io->prio = (int64_t(g_network->getCurrentTask()) << 32) - (++ctx.opsIssued);
		// io->prio = - (++ctx.opsIssued);
		io->owner = Reference<AsyncFileKAIO>::addRef(owner);

		ctx.queue.push(io);
	}

	static int openFlags(int flags) {
		int oflags = O_DIRECT | O_CLOEXEC;
		ASSERT(bool(flags & OPEN_READONLY) != bool(flags & OPEN_READWRITE)); // readonly xor readwrite
		if (flags & OPEN_EXCLUSIVE)
			oflags |= O_EXCL;
		if (flags & OPEN_CREATE)
			oflags |= O_CREAT;
		if (flags & OPEN_READONLY)
			oflags |= O_RDONLY;
		if (flags & OPEN_READWRITE)
			oflags |= O_RDWR;
		if (flags & OPEN_ATOMIC_WRITE_AND_CREATE)
			oflags |= O_TRUNC;
		return oflags;
	}

	static Future<Void> poll(Uncancellable, Reference<IEventFD> ev) {
		while (true) {
			co_await ev->read();

			co_await delay(0, TaskPriority::DiskIOComplete);

			std::vector<linux_ioresult> events(FLOW_KNOBS->MAX_OUTSTANDING);
			timespec tm;
			tm.tv_sec = 0;
			tm.tv_nsec = 0;

			int n;

			while (true) {
				n = io_getevents(ctx.iocx, 0, FLOW_KNOBS->MAX_OUTSTANDING, events.data(), &tm);
				if (n >= 0 || errno != EINTR)
					break;
			}

			double currentTime = timer();

			++ctx.countAIOCollect;
			// printf("io_getevents: collected %d/%d in %f us (%d queued)\n", n, ctx.outstanding, (timer()-before)*1e6,
			// ctx.queue.size());
			if (n < 0) {
				// printf("io_getevents failed: %d\n", errno);
				TraceEvent("IOGetEventsError").GetLastError();
				throw io_error();
			}
			if (n) {
				double t = timer_monotonic();
				double elapsed = t - ctx.ioStallBegin;
				ctx.ioStallBegin = t;
				g_network->networkInfo.metrics.secSquaredDiskStall += elapsed * elapsed / 2;
			}

			ctx.outstanding -= n;

			if (ctx.ioTimeout > 0) {
				while (ctx.submittedRequestList && currentTime - ctx.submittedRequestList->startTime > ctx.ioTimeout) {
					ctx.submittedRequestList->timeout(ctx.timeoutWarnOnly);
					ctx.removeFromRequestList(ctx.submittedRequestList);
				}
			}

			for (int i = 0; i < n; i++) {
				IOBlock* iob = static_cast<IOBlock*>(events[i].iocb);

				KAIOLogBlockEvent(iob, OpLogEntry::COMPLETE, events[i].result);

				if (ctx.ioTimeout > 0) {
					ctx.removeFromRequestList(iob);
				}

				switch (iob->aio_lio_opcode) {
				case IO_CMD_PREAD:
					getMetrics().readLatencySample.addMeasurement(currentTime - iob->startTime);
					break;
				case IO_CMD_PWRITE:
					getMetrics().writeLatencySample.addMeasurement(currentTime - iob->startTime);
					break;
				}

				iob->setResult(events[i].result);
			}
			uringReap();
		}
	}
};

#if KAIO_LOGGING
// Call from contexts where only an ioblock is available, log if its owner is set
void AsyncFileKAIO::KAIOLogBlockEvent(IOBlock* ioblock, OpLogEntry::EStage stage, uint32_t result) {
	if (ioblock->owner)
		return KAIOLogBlockEvent(ioblock->owner->logFile, ioblock, stage, result);
}

void AsyncFileKAIO::KAIOLogBlockEvent(FILE* logFile, IOBlock* ioblock, OpLogEntry::EStage stage, uint32_t result) {
	if (logFile != nullptr) {
		// Figure out what type of operation this is
		OpLogEntry::EOperation op;
		if (ioblock->aio_lio_opcode == IO_CMD_PREAD)
			op = OpLogEntry::READ;
		else if (ioblock->aio_lio_opcode == IO_CMD_PWRITE)
			op = OpLogEntry::WRITE;
		else
			return;

		// Assign this IO operation an io log id number if it doesn't already have one
		if (ioblock->iolog_id == 0)
			ioblock->iolog_id = OpLogEntry::nextID();

		OpLogEntry e;
		e.timestamp = timer_int();
		e.op = (uint8_t)op;
		e.id = ioblock->iolog_id;
		e.stage = (uint8_t)stage;
		e.pageOffset = (uint32_t)(ioblock->offset / 4096);
		e.pageCount = (uint8_t)(ioblock->nbytes / 4096);
		e.result = result;

		// Log a checksum for Writes up to the Complete stage or Reads starting from the Complete stage
		if ((op == OpLogEntry::WRITE && stage <= OpLogEntry::COMPLETE) ||
		    (op == OpLogEntry::READ && stage >= OpLogEntry::COMPLETE))
			e.checksum = crc32c_append(0xab12fd93, ioblock->buf, ioblock->nbytes);
		else
			e.checksum = 0;

		e.log(logFile);
	}
}

void AsyncFileKAIO::KAIOLogEvent(FILE* logFile,
                                 uint32_t id,
                                 OpLogEntry::EOperation op,
                                 OpLogEntry::EStage stage,
                                 uint32_t pageOffset,
                                 uint32_t result) {
	if (logFile != nullptr) {
		OpLogEntry e;
		e.timestamp = timer_int();
		e.id = id;
		e.op = (uint8_t)op;
		e.stage = (uint8_t)stage;
		e.pageOffset = pageOffset;
		e.pageCount = 0;
		e.checksum = 0;
		e.result = result;
		e.log(logFile);
	}
}
#endif

AsyncFileKAIO::Context AsyncFileKAIO::ctx;

#endif
