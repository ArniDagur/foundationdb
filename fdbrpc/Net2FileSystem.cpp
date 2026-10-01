/*
 * Net2FileSystem.cpp
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

#include "fdbrpc/Net2FileSystem.h"

#include <algorithm>

// Define boost::asio::io_context
#ifndef BOOST_DATE_TIME_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#endif
#ifndef BOOST_REGEX_NO_LIB
#define BOOST_REGEX_NO_LIB
#endif
#include <boost/asio.hpp>

#define FILESYSTEM_IMPL 1

#include "fdbrpc/AsyncFileCached.h"
#include "AsyncFileChaos.h"
#include "AsyncFileEIO.h"
#include "AsyncFileWinASIO.h"
#include "AsyncFileKAIO.h"
#include "flow/AsioReactor.h"
#include "flow/Platform.h"
#include "AsyncFileWriteChecker.h"
#include "flow/UnitTest.h"
#include "flow/IoUring.h"
#ifdef __linux__
#include <dirent.h>
#endif

#ifdef __linux__
namespace {
Future<Void> runAsyncFileKAIOTestOps(Reference<IAsyncFile> f, int numIterations, int fileSize, bool expectedToSucceed) {
	void* buf = FastAllocator<4096>::allocate(); // we leak this if there is an error, but that shouldn't be a big deal

	bool opTimedOut = false;

	for (int iteration = 0; iteration < numIterations; ++iteration) {
		std::vector<Future<Void>> futures;
		for (int numOps = deterministicRandom()->randomInt(1, 20); numOps > 0; --numOps) {
			if (deterministicRandom()->coinflip()) {
				futures.push_back(success(f->read(
				    buf, 4096, static_cast<int64_t>(deterministicRandom()->randomInt(0, fileSize)) / 4096 * 4096)));
			} else {
				futures.push_back(f->write(
				    buf, 4096, static_cast<int64_t>(deterministicRandom()->randomInt(0, fileSize)) / 4096 * 4096));
			}
		}
		for (int fIndex = 0; fIndex < futures.size(); ++fIndex) {
			try {
				co_await futures[fIndex];
			} catch (Error& e) {
				ASSERT(!expectedToSucceed);
				ASSERT(e.code() == error_code_io_timeout);
				opTimedOut = true;
			}
		}

		try {
			co_await (f->sync() && delay(0.1));
			ASSERT(expectedToSucceed);
		} catch (Error& e) {
			ASSERT(!expectedToSucceed && e.code() == error_code_io_timeout);
		}
	}

	FastAllocator<4096>::release(buf);

	ASSERT(expectedToSucceed || opTimedOut);
}
} // namespace

TEST_CASE("/fdbrpc/AsyncFileKAIO/RequestList") {
	// This test does nothing in simulation because simulation doesn't support AsyncFileKAIO
	if (!g_network->isSimulated()) {
		Reference<IAsyncFile> f;
		Optional<Error> err;
		try {
			f = co_await AsyncFileKAIO::open("/tmp/__KAIO_TEST_FILE__",
			                                 IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE |
			                                     IAsyncFile::OPEN_CREATE,
			                                 0666,
			                                 nullptr);
			int fileSize = 2 << 27; // ~100MB
			co_await f->truncate(fileSize);

			// Test that the request list works as intended with default timeout
			AsyncFileKAIO::setTimeout(0.0);
			co_await runAsyncFileKAIOTestOps(f, 100, fileSize, true);
			ASSERT(!((AsyncFileKAIO*)f.getPtr())->failed);

			// Test that the request list works as intended with long timeout
			AsyncFileKAIO::setTimeout(20.0);
			co_await runAsyncFileKAIOTestOps(f, 100, fileSize, true);
			ASSERT(!((AsyncFileKAIO*)f.getPtr())->failed);

			// Test that requests timeout correctly
			AsyncFileKAIO::setTimeout(0.0001);
			co_await runAsyncFileKAIOTestOps(f, 10, fileSize, false);
			ASSERT(((AsyncFileKAIO*)f.getPtr())->failed);
		} catch (Error& e) {
			err = e;
		}
		AsyncFileKAIO::setTimeout(0.0);
		if (err.present()) {
			if (f) {
				co_await AsyncFileEIO::deleteFile(f->getFilename(), true);
			}
			throw err.get();
		}

		co_await AsyncFileEIO::deleteFile(f->getFilename(), true);
	}
}

// KAIO_MAX_IO_BYTES / KAIO_MAX_IOCBS_PER_SUBMIT must split large reads and writes into bounded iocbs and bounded
// io_submit() calls without changing what is read or written, and must leave behavior unchanged when disabled.
TEST_CASE("/fdbrpc/AsyncFileKAIO/SplitLargeIO") {
	if (g_network->isSimulated()) {
		co_return;
	}
	auto* knobs = const_cast<FlowKnobs*>(FLOW_KNOBS);
	const int savedMaxIOBytes = knobs->KAIO_MAX_IO_BYTES;
	const int savedMaxIOCBsPerSubmit = knobs->KAIO_MAX_IOCBS_PER_SUBMIT;
	constexpr int chunk = 128 << 10;
	constexpr int length = (1 << 20) + 3 * 4096; // 8 full chunks and one partial chunk
	constexpr int expectedParts = (length + chunk - 1) / chunk;
	constexpr int64_t offset = 8192;
	uint8_t* wbuf = static_cast<uint8_t*>(allocateFast4kAligned(length));
	uint8_t* rbuf = static_cast<uint8_t*>(allocateFast4kAligned(length));
	for (int i = 0; i < length; i++) {
		wbuf[i] = static_cast<uint8_t>(deterministicRandom()->randomInt(0, 256));
	}
	const std::string filename =
	    joinPath(params.getDataDir(),
	             format("__KAIO_SPLIT_TEST_%s__", deterministicRandom()->randomUniqueID().toString().c_str()));
	Reference<IAsyncFile> f;
	Optional<Error> err;
	try {
		f = co_await AsyncFileKAIO::open(filename,
		                                 IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE |
		                                     IAsyncFile::OPEN_CREATE,
		                                 0666,
		                                 nullptr);

		knobs->KAIO_MAX_IO_BYTES = chunk;
		knobs->KAIO_MAX_IOCBS_PER_SUBMIT = 2;

		AsyncFileKAIO::resetSubmitStats();
		co_await f->write(wbuf, length, offset);
		auto stats = AsyncFileKAIO::getSubmitStats();
		ASSERT_EQ(stats.submittedIOCBs, expectedParts);
		ASSERT_EQ(stats.largestSubmittedIOBytes, chunk);
		ASSERT_LE(stats.largestSubmitBatch, 2);
		ASSERT_GE(stats.submitCalls, (expectedParts + 1) / 2);
		co_await f->sync();
		const int64_t fileSize = co_await f->size();
		ASSERT_EQ(fileSize, offset + length);

		AsyncFileKAIO::resetSubmitStats();
		memset(rbuf, 0, length);
		int n = co_await f->read(rbuf, length, offset);
		stats = AsyncFileKAIO::getSubmitStats();
		ASSERT_EQ(n, length);
		ASSERT(memcmp(rbuf, wbuf, length) == 0);
		ASSERT_EQ(stats.submittedIOCBs, expectedParts);
		ASSERT_LE(stats.largestSubmittedIOBytes, chunk);

		// A split read that runs past end of file returns only the bytes before it, like an unsplit read.
		constexpr int tail = 200 << 10;
		memset(rbuf, 0, length);
		n = co_await f->read(rbuf, 512 << 10, offset + length - tail);
		ASSERT_EQ(n, tail);
		ASSERT(memcmp(rbuf, wbuf + length - tail, tail) == 0);

		// Knob values that are not 4KiB multiples are rounded down to one.
		knobs->KAIO_MAX_IO_BYTES = chunk + 100;
		AsyncFileKAIO::resetSubmitStats();
		co_await f->write(wbuf, length, offset);
		ASSERT_EQ(AsyncFileKAIO::getSubmitStats().largestSubmittedIOBytes, chunk);

		// Disabled: one iocb per request, submitted together.
		knobs->KAIO_MAX_IO_BYTES = 0;
		knobs->KAIO_MAX_IOCBS_PER_SUBMIT = 0;
		AsyncFileKAIO::resetSubmitStats();
		co_await f->write(wbuf, length, offset + length);
		stats = AsyncFileKAIO::getSubmitStats();
		ASSERT_EQ(stats.submittedIOCBs, 1);
		ASSERT_EQ(stats.largestSubmittedIOBytes, length);
		ASSERT_EQ(stats.submitCalls, 1);
		memset(rbuf, 0, length);
		n = co_await f->read(rbuf, length, offset + length);
		ASSERT_EQ(n, length);
		ASSERT(memcmp(rbuf, wbuf, length) == 0);
	} catch (Error& e) {
		err = e;
	}
	knobs->KAIO_MAX_IO_BYTES = savedMaxIOBytes;
	knobs->KAIO_MAX_IOCBS_PER_SUBMIT = savedMaxIOCBsPerSubmit;
	freeFast4kAligned(length, wbuf);
	freeFast4kAligned(length, rbuf);
	if (f) {
		co_await AsyncFileEIO::deleteFile(filename, true);
	}
	if (err.present()) {
		throw err.get();
	}
}

// With KAIO_FDSYNC = 1 (kernel AIO) or 2 (io_uring), sync() must take that path (no fallback on a kernel and
// filesystem that support it) and interleave correctly with writes; with 0 it must stay on the EIO thread pool.
// A rejected fdsync must fall back to the thread pool and disable further attempts.
TEST_CASE("/fdbrpc/AsyncFileKAIO/Fdsync") {
	if (g_network->isSimulated()) {
		co_return;
	}
	auto* knobs = const_cast<FlowKnobs*>(FLOW_KNOBS);
	const int savedFdsync = knobs->KAIO_FDSYNC;
	constexpr int length = 64 << 10;
	constexpr int rounds = 200;
	uint8_t* wbuf = static_cast<uint8_t*>(allocateFast4kAligned(length));
	uint8_t* rbuf = static_cast<uint8_t*>(allocateFast4kAligned(length));
	const std::string filename =
	    joinPath(params.getDataDir(),
	             format("__KAIO_FDSYNC_TEST_%s__", deterministicRandom()->randomUniqueID().toString().c_str()));
	Reference<IAsyncFile> f;
	Optional<Error> err;
	try {
		f = co_await AsyncFileKAIO::open(filename,
		                                 IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE |
		                                     IAsyncFile::OPEN_CREATE,
		                                 0666,
		                                 nullptr);
		std::vector<uint8_t> expected(rounds * length);
		co_await f->truncate(static_cast<int64_t>(rounds) * length);

		for (int mode : { 1, 2 }) {
			knobs->KAIO_FDSYNC = mode;
			AsyncFileKAIO::resetFdsyncStats();
			for (int i = 0; i < rounds; i++) {
				for (int j = 0; j < length; j++) {
					wbuf[j] = static_cast<uint8_t>(deterministicRandom()->randomInt(0, 256));
				}
				const int slot = deterministicRandom()->randomInt(0, rounds);
				co_await f->write(wbuf, length, static_cast<int64_t>(slot) * length);
				memcpy(expected.data() + static_cast<size_t>(slot) * length, wbuf, length);
				co_await f->sync();
			}
			auto st = AsyncFileKAIO::getFdsyncStats();
			ASSERT_EQ(st.submitted, rounds);
			ASSERT_EQ(st.fallbacks, 0);
			ASSERT(st.supported);
			for (int slot = 0; slot < rounds; slot++) {
				const int n = co_await f->read(rbuf, length, static_cast<int64_t>(slot) * length);
				ASSERT_EQ(n, length);
				ASSERT(memcmp(rbuf, expected.data() + static_cast<size_t>(slot) * length, length) == 0);
			}
		}

		// Mode 0: syncs go to the EIO thread pool.
		knobs->KAIO_FDSYNC = 0;
		AsyncFileKAIO::resetFdsyncStats();
		co_await f->write(wbuf, length, 0);
		co_await f->sync();
		ASSERT_EQ(AsyncFileKAIO::getFdsyncStats().submitted, 0);

		// A rejected fdsync (what io_submit reports on kernels before 4.18) falls back and disables fdsync.
		knobs->KAIO_FDSYNC = 1;
		co_await AsyncFileKAIO::fdsyncOrFallback(Future<int>(AsyncFileKAIO::kFdsyncUnsupported),
		                                         static_cast<int>(f->debugFD()));
		auto st = AsyncFileKAIO::getFdsyncStats();
		ASSERT_EQ(st.fallbacks, 1);
		ASSERT(!st.supported);
		co_await f->write(wbuf, length, 0);
		co_await f->sync();
		ASSERT_EQ(AsyncFileKAIO::getFdsyncStats().submitted, 0);
		const int n = co_await f->read(rbuf, length, 0);
		ASSERT_EQ(n, length);
		ASSERT(memcmp(rbuf, wbuf, length) == 0);
	} catch (Error& e) {
		err = e;
	}
	knobs->KAIO_FDSYNC = savedFdsync;
	AsyncFileKAIO::resetFdsyncStats();
	freeFast4kAligned(length, wbuf);
	freeFast4kAligned(length, rbuf);
	if (f) {
		co_await AsyncFileEIO::deleteFile(filename, true);
	}
	if (err.present()) {
		throw err.get();
	}
}

// Many concurrent reads and writes of random sizes and offsets, under random KAIO split settings, must read back
// exactly what an in-memory copy of the file holds. Writes in one round never overlap, so their completion order
// does not matter.
TEST_CASE("/fdbrpc/AsyncFileKAIO/SplitRandomIO") {
	if (g_network->isSimulated()) {
		co_return;
	}
	auto* knobs = const_cast<FlowKnobs*>(FLOW_KNOBS);
	const int savedMaxIOBytes = knobs->KAIO_MAX_IO_BYTES;
	const int savedMaxIOCBsPerSubmit = knobs->KAIO_MAX_IOCBS_PER_SUBMIT;
	constexpr int pageSize = 4096;
	constexpr int fileSize = 64 << 20;
	constexpr int maxOpBytes = 1 << 20;
	const int rounds = params.getInt("rounds").orDefault(40);
	const int opsPerRound = 16;
	std::vector<uint8_t> shadow(fileSize, 0);
	std::vector<uint8_t*> bufs;
	for (int i = 0; i < opsPerRound; i++) {
		bufs.push_back(static_cast<uint8_t*>(allocateFast4kAligned(maxOpBytes)));
	}
	const std::string filename =
	    joinPath(params.getDataDir(),
	             format("__KAIO_SPLIT_RANDOM_TEST_%s__", deterministicRandom()->randomUniqueID().toString().c_str()));
	Reference<IAsyncFile> f;
	Optional<Error> err;
	int64_t splitOps = 0;
	try {
		f = co_await AsyncFileKAIO::open(filename,
		                                 IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE |
		                                     IAsyncFile::OPEN_CREATE,
		                                 0666,
		                                 nullptr);
		co_await f->truncate(fileSize);

		const int maxIOChoices[] = { 0, 4096, 64 << 10, 128 << 10, (128 << 10) + 4096, 300 << 10 };
		for (int round = 0; round < rounds; round++) {
			knobs->KAIO_MAX_IO_BYTES = maxIOChoices[deterministicRandom()->randomInt(0, 6)];
			knobs->KAIO_MAX_IOCBS_PER_SUBMIT = deterministicRandom()->randomInt(0, 4);
			const int chunk = AsyncFileKAIO::maxIOBytes();

			// Non-overlapping writes: split the file into opsPerRound slots and write a random span in each.
			std::vector<Future<Void>> writes;
			const int slotPages = fileSize / pageSize / opsPerRound;
			for (int i = 0; i < opsPerRound; i++) {
				const int pages = deterministicRandom()->randomInt(1, std::min(slotPages, maxOpBytes / pageSize) + 1);
				const int startPage = i * slotPages + deterministicRandom()->randomInt(0, slotPages - pages + 1);
				const int length = pages * pageSize;
				const int64_t offset = int64_t(startPage) * pageSize;
				for (int b = 0; b < length; b++) {
					bufs[i][b] = static_cast<uint8_t>(deterministicRandom()->randomInt(0, 256));
				}
				memcpy(shadow.data() + offset, bufs[i], length);
				splitOps += (chunk > 0 && length > chunk) ? 1 : 0;
				writes.push_back(f->write(bufs[i], length, offset));
			}
			co_await waitForAll(writes);
			if (deterministicRandom()->coinflip()) {
				co_await f->sync();
			}

			// Overlapping reads anywhere in the file, including ranges that end exactly at end of file.
			std::vector<Future<int>> reads;
			std::vector<std::pair<int64_t, int>> ranges;
			for (int i = 0; i < opsPerRound; i++) {
				const int pages = deterministicRandom()->randomInt(1, maxOpBytes / pageSize + 1);
				const int64_t offset =
				    int64_t(deterministicRandom()->randomInt(0, fileSize / pageSize - pages + 1)) * pageSize;
				ranges.emplace_back(offset, pages * pageSize);
				splitOps += (chunk > 0 && pages * pageSize > chunk) ? 1 : 0;
				reads.push_back(f->read(bufs[i], pages * pageSize, offset));
			}
			for (int i = 0; i < opsPerRound; i++) {
				const int n = co_await reads[i];
				ASSERT_EQ(n, ranges[i].second);
				ASSERT(memcmp(bufs[i], shadow.data() + ranges[i].first, n) == 0);
			}
		}
		// The random settings must actually have exercised the split path.
		ASSERT_GT(splitOps, 0);
	} catch (Error& e) {
		err = e;
	}
	knobs->KAIO_MAX_IO_BYTES = savedMaxIOBytes;
	knobs->KAIO_MAX_IOCBS_PER_SUBMIT = savedMaxIOCBsPerSubmit;
	for (auto* b : bufs) {
		freeFast4kAligned(maxOpBytes, b);
	}
	if (f) {
		co_await AsyncFileEIO::deleteFile(filename, true);
	}
	if (err.present()) {
		throw err.get();
	}
}

// With KAIO_IO_URING, reads, writes and syncs must complete through the network thread's io_uring (the ring's disk
// counters and KAIO's ring completions grow by exactly the requests made) and read back what was written, with many
// requests in flight at once.
// io-wq worker threads of this process (named iou-wrk-<pid>).
static int ioWorkerThreads() {
	int n = 0;
	DIR* dir = opendir("/proc/self/task");
	while (dirent* e = dir ? readdir(dir) : nullptr) {
		char comm[32] = {};
		FILE* f = std::fopen((std::string("/proc/self/task/") + e->d_name + "/comm").c_str(), "r");
		if (f && std::fgets(comm, sizeof(comm), f) && std::strncmp(comm, "iou-wrk", 7) == 0) {
			++n;
		}
		if (f) {
			std::fclose(f);
		}
	}
	if (dir) {
		closedir(dir);
	}
	return n;
}

TEST_CASE("/fdbrpc/AsyncFileKAIO/IoUring") {
	if (g_network->isSimulated()) {
		co_return;
	}
	auto* knobs = const_cast<FlowKnobs*>(FLOW_KNOBS);
	const bool saved = knobs->KAIO_IO_URING;
	knobs->KAIO_IO_URING = true;
	iouring::Ring* ring = iouring::Ring::get();
	ASSERT(ring != nullptr);
	constexpr int blockBytes = 16 << 10;
	constexpr int slots = 256;
	constexpr int rounds = 4;
	std::vector<uint8_t*> bufs;
	for (int i = 0; i < slots; i++) {
		bufs.push_back(static_cast<uint8_t*>(allocateFast4kAligned(blockBytes)));
	}
	const std::string filename =
	    joinPath(params.getDataDir(),
	             format("__KAIO_IOURING_TEST_%s__", deterministicRandom()->randomUniqueID().toString().c_str()));
	Reference<IAsyncFile> f;
	Optional<Error> err;
	try {
		f = co_await AsyncFileKAIO::open(filename,
		                                 IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE |
		                                     IAsyncFile::OPEN_CREATE,
		                                 0666,
		                                 nullptr);
		co_await f->truncate(static_cast<int64_t>(slots) * blockBytes);
		std::vector<uint8_t> expected(static_cast<size_t>(slots) * blockBytes);
		const iouring::Stats before = ring->stats();
		const int64_t completionsBefore = AsyncFileKAIO::getRingCompletions();
		const int64_t touchesBefore = AsyncFileKAIO::getRingTimesTouches();
		int reads = 0, writes = 0, syncs = 0, mostWorkers = 0;
		for (int round = 0; round < rounds; round++) {
			// Every slot written concurrently, then synced, then all read back concurrently.
			std::vector<Future<Void>> pending;
			for (int slot = 0; slot < slots; slot++) {
				for (int j = 0; j < blockBytes; j++) {
					bufs[slot][j] = static_cast<uint8_t>(deterministicRandom()->randomInt(0, 256));
				}
				memcpy(expected.data() + static_cast<size_t>(slot) * blockBytes, bufs[slot], blockBytes);
				pending.push_back(f->write(bufs[slot], blockBytes, static_cast<int64_t>(slot) * blockBytes));
				++writes;
			}
			co_await waitForAll(pending);
			mostWorkers = std::max(mostWorkers, ioWorkerThreads());
			co_await f->sync();
			++syncs;
			for (int slot = 0; slot < slots; slot++) {
				memset(bufs[slot], 0, blockBytes);
			}
			std::vector<Future<int>> readsDone;
			for (int slot = 0; slot < slots; slot++) {
				readsDone.push_back(f->read(bufs[slot], blockBytes, static_cast<int64_t>(slot) * blockBytes));
				++reads;
			}
			co_await waitForAll(readsDone);
			for (int slot = 0; slot < slots; slot++) {
				ASSERT_EQ(readsDone[slot].get(), blockBytes);
				ASSERT(memcmp(bufs[slot], expected.data() + static_cast<size_t>(slot) * blockBytes, blockBytes) == 0);
			}
		}
		const iouring::Stats after = ring->stats();
		auto submitted = [&](iouring::Kind k) { return after.submitted[int(k)] - before.submitted[int(k)]; };
		auto completed = [&](iouring::Kind k) { return after.completed[int(k)] - before.completed[int(k)]; };
		ASSERT_EQ(submitted(iouring::Kind::DiskWrite), writes);
		ASSERT_EQ(completed(iouring::Kind::DiskWrite), writes);
		ASSERT_EQ(submitted(iouring::Kind::DiskRead), reads);
		ASSERT_EQ(completed(iouring::Kind::DiskRead), reads);
		ASSERT_EQ(submitted(iouring::Kind::DiskFsync), syncs);
		ASSERT_EQ(completed(iouring::Kind::DiskFsync), syncs);
		ASSERT_EQ(AsyncFileKAIO::getRingCompletions() - completionsBefore, reads + writes + syncs);
		// Each burst of writes refreshed the file's times first (at most once per clock tick), and the io-wq workers
		// that run what cannot be done without blocking stayed within the cap.
		const int64_t touches = AsyncFileKAIO::getRingTimesTouches() - touchesBefore;
		ASSERT(touches >= 1 && touches <= writes);
		ASSERT_LE(mostWorkers, FLOW_KNOBS->IO_URING_MAX_WORKERS);
		printf("KAIO io_uring: %d writes, %d reads, %d syncs through the ring, data verified; %lld time touches, at "
		       "most %d io-wq workers\n",
		       writes,
		       reads,
		       syncs,
		       (long long)touches,
		       mostWorkers);
	} catch (Error& e) {
		err = e;
	}
	knobs->KAIO_IO_URING = saved;
	for (auto* b : bufs) {
		freeFast4kAligned(blockBytes, b);
	}
	if (f) {
		co_await AsyncFileEIO::deleteFile(filename, true);
	}
	if (err.present()) {
		throw err.get();
	}
}
#endif // __linux__

// Opens a file for asynchronous I/O
Future<Reference<class IAsyncFile>> Net2FileSystem::open(const std::string& filename, int64_t flags, int64_t mode) {
#ifdef __linux__
	if (checkFileSystem) {
		dev_t fileDeviceId = getDeviceId(filename);
		if (std::find(this->fileSystemDeviceIds.begin(), this->fileSystemDeviceIds.end(), fileDeviceId) ==
		    this->fileSystemDeviceIds.end()) {
			TraceEvent te(SevError, "DeviceIdMismatched");
			te.detail("FileDeviceId", fileDeviceId);
			for (size_t i = 0; i < this->fileSystemDeviceIds.size(); ++i) {
				te.detail(format("AllowedFileSystemDeviceId%zu", i).c_str(), this->fileSystemDeviceIds[i]);
			}
			throw io_error();
		}
	}
#endif

	if ((flags & IAsyncFile::OPEN_EXCLUSIVE))
		ASSERT(flags & IAsyncFile::OPEN_CREATE);
	if (!(flags & IAsyncFile::OPEN_UNCACHED))
		return AsyncFileCached::open(filename, flags, mode);

	Future<Reference<IAsyncFile>> f;
#ifdef __linux__
	// In the vast majority of cases, we wish to use Kernel AIO. However, some systems
	// don’t properly support kernel async I/O without O_DIRECT or AIO at all. In such
	// cases, DISABLE_POSIX_KERNEL_AIO knob can be enabled to fallback to EIO instead
	// of Kernel AIO. And EIO_USE_ODIRECT can be used to turn on or off O_DIRECT within
	// EIO.
	if ((flags & IAsyncFile::OPEN_UNBUFFERED) && !(flags & IAsyncFile::OPEN_NO_AIO) &&
	    !FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO)
		f = AsyncFileKAIO::open(filename, flags, mode, nullptr);
	else
#endif
	{
		f = Net2AsyncFile::open(
		    filename,
		    flags,
		    mode,
		    static_cast<boost::asio::io_context*>((void*)g_network->global(INetwork::enASIOService)));
	}
	if (FLOW_KNOBS->PAGE_WRITE_CHECKSUM_HISTORY > 0) {
		f = map(f, [=](Reference<IAsyncFile> r) -> Reference<IAsyncFile> {
			return makeReference<AsyncFileWriteChecker>(r);
		});
	}
	if (FLOW_KNOBS->ENABLE_CHAOS_FEATURES)
		f = map(f, [=](Reference<IAsyncFile> r) -> Reference<IAsyncFile> { return makeReference<AsyncFileChaos>(r); });
	return f;
}

// Deletes the given file.  If mustBeDurable, returns only when the file is guaranteed to be deleted even after a power
// failure.
Future<Void> Net2FileSystem::deleteFile(const std::string& filename, bool mustBeDurable) {
	return Net2AsyncFile::deleteFile(filename, mustBeDurable);
}

Future<std::time_t> Net2FileSystem::lastWriteTime(const std::string& filename) {
	return Net2AsyncFile::lastWriteTime(filename);
}

#ifdef ENABLE_SAMPLING
ActorLineageSet& Net2FileSystem::getActorLineageSet() {
	return actorLineageSet;
}
#endif

void Net2FileSystem::newFileSystem(double ioTimeout, const std::vector<std::string>& fileSystemPaths) {
	g_network->setGlobal(INetwork::enFileSystem, (flowGlobalType) new Net2FileSystem(ioTimeout, fileSystemPaths));
}

void Net2FileSystem::newFileSystem(double ioTimeout, const std::string& fileSystemPath) {
	newFileSystem(ioTimeout,
	              fileSystemPath.empty() ? std::vector<std::string>() : std::vector<std::string>{ fileSystemPath });
}

Net2FileSystem::Net2FileSystem(double ioTimeout, const std::string& fileSystemPath)
  : Net2FileSystem(ioTimeout,
                   fileSystemPath.empty() ? std::vector<std::string>() : std::vector<std::string>{ fileSystemPath }) {}

Net2FileSystem::Net2FileSystem(double ioTimeout, const std::vector<std::string>& fileSystemPaths) {
	Net2AsyncFile::init();
#ifdef __linux__
	if (!FLOW_KNOBS->DISABLE_POSIX_KERNEL_AIO)
		AsyncFileKAIO::init(Reference<IEventFD>(N2::ASIOReactor::getEventFD()), ioTimeout);

	if (fileSystemPaths.empty()) {
		checkFileSystem = false;
	} else {
		checkFileSystem = true;

		for (const auto& fileSystemPath : fileSystemPaths) {
			try {
				dev_t fileSystemDeviceId = getDeviceId(fileSystemPath);
				if (fileSystemPath != "/") {
					dev_t fileSystemParentDeviceId = getDeviceId(parentDirectory(fileSystemPath));
					if (fileSystemDeviceId == fileSystemParentDeviceId) {
						criticalError(FDB_EXIT_ERROR,
						              "FileSystemError",
						              format("`%s' is not a mount point", fileSystemPath.c_str()).c_str());
					}
				}
				this->fileSystemDeviceIds.push_back(fileSystemDeviceId);
			} catch (Error&) {
				criticalError(FDB_EXIT_ERROR,
				              "FileSystemError",
				              format("Could not get device id from `%s'", fileSystemPath.c_str()).c_str());
			}
		}

		std::sort(this->fileSystemDeviceIds.begin(), this->fileSystemDeviceIds.end());
		this->fileSystemDeviceIds.erase(std::unique(this->fileSystemDeviceIds.begin(), this->fileSystemDeviceIds.end()),
		                                this->fileSystemDeviceIds.end());
	}
#endif
}

Future<Void> Net2FileSystem::renameFile(const std::string& from, const std::string& to) {
	return Net2AsyncFile::renameFile(from, to);
}

void Net2FileSystem::stop() {
	Net2AsyncFile::stop();
}
