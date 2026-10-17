/*
 * PageChecksum.cpp
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

// Redwood page payload checksums. The build targets AVX, for which xxHash runs its SSE2 loop over long inputs; on
// x86-64 the loop is also compiled for AVX2 and picked at run time where the CPU has it. The hash is unchanged.

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#define PAGE_CHECKSUM_DISPATCH 1
#define XXH_X86DISPATCH
#define XXH_TARGET_AVX512 __attribute__((__target__("avx512f")))
#define XXH_TARGET_AVX2 __attribute__((__target__("avx2")))
#include <immintrin.h> // the AVX2 and AVX-512 types xxHash's dispatch code is written in
#endif
#define XXH_INLINE_ALL
#include "flow/xxhash.h"

#include "PageChecksum.h"

#include "flow/IRandom.h"
#include "flow/UnitTest.h"

#include <string>
#include <vector>

namespace {

#if PAGE_CHECKSUM_DISPATCH
XXH_NO_INLINE XXH_TARGET_AVX2 XXH64_hash_t
hashLongWithSeedAvx2(const void* input, size_t len, XXH64_hash_t seed, const xxh_u8* secret, size_t secretLen) {
	(void)secret;
	(void)secretLen;
	return XXH3_hashLong_64b_withSeed_internal(
	    input, len, seed, XXH3_accumulate_512_avx2, XXH3_scrambleAcc_avx2, XXH3_initCustomSecret_avx2);
}

XXH3_hashLong64_f hashLongWithSeed() {
	static const XXH3_hashLong64_f f = [] {
		__builtin_cpu_init();
		return __builtin_cpu_supports("avx2") ? hashLongWithSeedAvx2 : XXH3_hashLong_64b_withSeed;
	}();
	return f;
}
#endif

} // namespace

XXH64_hash_t pagePayloadChecksum(const void* payload, size_t len, XXH64_hash_t seed) {
#if PAGE_CHECKSUM_DISPATCH
	return XXH3_64bits_internal(payload, len, seed, XXH3_kSecret, sizeof(XXH3_kSecret), hashLongWithSeed());
#else
	return XXH3_64bits_withSeed(payload, len, seed);
#endif
}

bool pagePayloadChecksumUsesAvx2() {
#if PAGE_CHECKSUM_DISPATCH
	return hashLongWithSeed() == hashLongWithSeedAvx2;
#else
	return false;
#endif
}

// Pages are checksummed on write and verified on read, possibly by another build or CPU, so the dispatched hash must
// equal XXH3_64bits_withSeed for every length (each of xxHash's length classes and partial stripes) and seed.
TEST_CASE("/redwood/pageChecksum/matchesXXH3") {
	std::vector<uint8_t> buf(70000);
	for (auto& b : buf) {
		b = deterministicRandom()->randomInt(0, 256);
	}
	std::vector<int> lengths = { 0, 1, 3, 4, 8, 9, 16, 17, 64, 128, 129, 200, 240, 241, 255, 256, 1023, 1024, 1025 };
	for (int len : { 4096, 8192, 16384, 65536 }) {
		for (int d : { -129, -64, -1, 0, 1, 63, 64 }) {
			lengths.push_back(len + d);
		}
	}
	for (int i = 0; i < 200; ++i) {
		lengths.push_back(deterministicRandom()->randomInt(0, buf.size()));
	}
	for (int len : lengths) {
		for (XXH64_hash_t seed : { XXH64_hash_t(0), XXH64_hash_t(1), deterministicRandom()->randomUInt64() }) {
			const int offset = deterministicRandom()->randomInt(0, buf.size() - len + 1);
			const uint8_t* p = buf.data() + offset;
			ASSERT_EQ(pagePayloadChecksum(p, len, seed), XXH3_64bits_withSeed(p, len, seed));
		}
	}
	return Void();
}

// The dispatch is the point of this file: on a CPU with AVX2 the AVX2 loop must be the one in use.
TEST_CASE("/redwood/pageChecksum/usesAvx2") {
#if PAGE_CHECKSUM_DISPATCH
	__builtin_cpu_init();
	ASSERT_EQ(pagePayloadChecksumUsesAvx2(), bool(__builtin_cpu_supports("avx2")));
#endif
	return Void();
}
