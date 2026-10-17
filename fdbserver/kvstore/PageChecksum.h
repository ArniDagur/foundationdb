/*
 * PageChecksum.h
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

#ifndef FDBSERVER_KVSTORE_PAGECHECKSUM_H
#define FDBSERVER_KVSTORE_PAGECHECKSUM_H
#pragma once

#include <cstddef>
#include <cstdint>

// XXH3_64bits_withSeed(payload, len, seed), with the long-input loop in the widest vector width the CPU runs.
uint64_t pagePayloadChecksum(const void* payload, size_t len, uint64_t seed);

// Whether pagePayloadChecksum() runs its AVX2 loop.
bool pagePayloadChecksumUsesAvx2();

#endif
