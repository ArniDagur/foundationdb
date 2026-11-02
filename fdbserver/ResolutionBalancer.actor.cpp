/*
 * ResolutionBalancer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/ResolutionBalancer.actor.h"

#include <functional>
#include <tuple>

#include "fdbclient/KeyRangeMap.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "flow/flow.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

void ResolutionBalancer::setResolvers(const std::vector<ResolverInterface>& v) {
	resolvers = v;
	if (resolvers.size() > 1)
		triggerResolution.trigger();
}

void ResolutionBalancer::setChangesInReply(UID requestingProxy, GetCommitVersionReply& rep) {
	if (resolverNeedingChanges.contains(requestingProxy)) {
		rep.resolverChanges = resolverChanges.get();
		rep.resolverChangesVersion = resolverChangesVersion;
		resolverNeedingChanges.erase(requestingProxy);

		CODE_PROBE(!rep.resolverChanges.empty(), "resolution balancing moves keyranges");
		if (resolverNeedingChanges.empty())
			resolverChanges.set(Standalone<VectorRef<ResolverMoveRef>>());
	}
}

static std::pair<KeyRangeRef, bool> findRange(CoalescedKeyRangeMap<int>& key_resolver,
                                              Standalone<VectorRef<ResolverMoveRef>>& movedRanges,
                                              int src,
                                              int dest) {
	auto ranges = key_resolver.ranges();
	auto prev = ranges.begin();
	auto it = ranges.begin();
	++it;
	if (it == ranges.end()) {
		if (ranges.begin().value() != src ||
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(ranges.begin()->range(), dest)) !=
		        movedRanges.end())
			throw operation_failed();
		return std::make_pair(ranges.begin().range(), true);
	}

	std::set<int> borders;
	// If possible expand an existing boundary between the two resolvers
	for (; it != ranges.end(); ++it) {
		if (it->value() == src && prev->value() == dest &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
		if (it->value() == dest && prev->value() == src &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(prev->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(prev->range(), false);
		}
		if (it->value() == dest)
			borders.insert(prev->value());
		if (prev->value() == dest)
			borders.insert(it->value());
		++prev;
	}

	prev = ranges.begin();
	it = ranges.begin();
	++it;
	// If possible create a new boundary which doesn't exist yet
	for (; it != ranges.end(); ++it) {
		if (it->value() == src && !borders.contains(prev->value()) &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
		if (prev->value() == src && !borders.contains(it->value()) &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(prev->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(prev->range(), false);
		}
		++prev;
	}

	it = ranges.begin();
	for (; it != ranges.end(); ++it) {
		if (it->value() == src &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
	}
	throw operation_failed(); // every range of src is already in movedRanges: being moved, or could not be cut
}

using SplitFn = std::function<Future<ResolutionSplitReply>(ResolutionSplitRequest)>;

// Chooses ranges holding about `amount` of src's load to move to dest; throws operation_failed if nothing should move.
// `key_resolver` must stay alive and unchanged until the returned future is ready.
ACTOR static Future<Standalone<VectorRef<ResolverMoveRef>>> chooseRangesToMove(CoalescedKeyRangeMap<int>* key_resolver,
                                                                              int src,
                                                                              int dest,
                                                                              int64_t amount,
                                                                              SplitFn splitRequest) {
	state Standalone<VectorRef<ResolverMoveRef>> movedRanges;
	state Standalone<VectorRef<ResolverMoveRef>> triedRanges;
	state bool skippedRange = false;
	state std::pair<KeyRangeRef, bool> range;
	// State: the catch block below is compiled into a separate function.
	state bool noRangeLeft = false;
	loop {
		noRangeLeft = false;
		try {
			range = findRange(*key_resolver, triedRanges, src, dest);
		} catch (Error& e) {
			// Out of candidates. src keeps any skipped range, so the moves chosen so far can stand; without a skip they
			// would give away all of src's ranges.
			if (e.code() != error_code_operation_failed || !skippedRange || movedRanges.empty())
				throw;
			noRangeLeft = true;
		}
		if (noRangeLeft) {
			CODE_PROBE(true, "Resolution balancing keeps the moves chosen before a skipped range");
			return movedRanges;
		}

		ResolutionSplitRequest req;
		req.front = range.second;
		req.offset = amount;
		req.range = range.first;

		ResolutionSplitReply reply = wait(splitRequest(req));
		KeyRangeRef moveRange =
		    range.second ? KeyRangeRef(range.first.begin, reply.key) : KeyRangeRef(reply.key, range.first.end);
		triedRanges.push_back_deep(triedRanges.arena(), ResolverMoveRef(range.first, dest));
		if (moveRange.empty()) {
			// Nothing to cut (src has no sampled load before the range's end). The empty move would change nothing and
			// be chosen again every round.
			CODE_PROBE(true, "Resolution balancing skips a range the resolver cannot split");
			TraceEvent("ResolutionRangeNotSplit")
			    .suppressFor(1.0)
			    .detail("Src", src)
			    .detail("Dest", dest)
			    .detail("Amount", amount)
			    .detail("Range", range.first);
			skippedRange = true;
			continue;
		}
		TraceEvent("MovingResolutionRange")
		    .detail("Src", src)
		    .detail("Dest", dest)
		    .detail("Amount", amount)
		    .detail("StartRange", range.first)
		    .detail("MoveRange", moveRange)
		    .detail("Used", reply.used)
		    .detail("KeyResolverRanges", key_resolver->size());
		movedRanges.push_back_deep(movedRanges.arena(), ResolverMoveRef(moveRange, dest));
		amount -= reply.used;
		if (moveRange != range.first || amount <= 0)
			return movedRanges;
	}
}

// Actor state variables cannot be captured by name, so the split callback is built outside the actor.
static SplitFn splitFromResolver(ResolverInterface resolver) {
	return [resolver](ResolutionSplitRequest req) {
		return brokenPromiseToNever(resolver.split.getReply(req, TaskPriority::ResolutionMetrics));
	};
}

// Balance key ranges among resolvers so that their load are evenly distributed.
ACTOR Future<Void> ResolutionBalancer::resolutionBalancing_impl(ResolutionBalancer* self) {
	wait(self->triggerResolution.onTrigger());

	state CoalescedKeyRangeMap<int> key_resolver(
	    0, SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS ? normalKeys.end : allKeys.end);
	key_resolver.insert(SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS ? normalKeys : allKeys, 0);
	loop {
		wait(delay(SERVER_KNOBS->MIN_BALANCE_TIME, TaskPriority::ResolutionMetrics));
		while (self->resolverChanges.get().size())
			wait(self->resolverChanges.onChange());
		state std::vector<Future<ResolutionMetricsReply>> futures;
		for (auto& p : self->resolvers)
			futures.push_back(
			    brokenPromiseToNever(p.metrics.getReply(ResolutionMetricsRequest(), TaskPriority::ResolutionMetrics)));
		wait(waitForAll(futures));
		state IndexedSet<std::pair<int64_t, int>, NoMetric> metrics;

		int64_t total = 0;
		for (int i = 0; i < futures.size(); i++) {
			total += futures[i].get().value;
			metrics.insert(std::make_pair(futures[i].get().value, i), NoMetric());
			//TraceEvent("ResolverMetric").detail("I", i).detail("Metric", futures[i].get());
		}
		if (metrics.lastItem()->first - metrics.begin()->first > SERVER_KNOBS->MIN_BALANCE_DIFFERENCE) {
			try {
				state int src = metrics.lastItem()->second;
				state int dest = metrics.begin()->second;
				state int64_t amount = std::min(metrics.lastItem()->first - total / self->resolvers.size(),
				                                total / self->resolvers.size() - metrics.begin()->first) /
				                       2;
				state Standalone<VectorRef<ResolverMoveRef>> movedRanges;
				wait(store(movedRanges,
				           chooseRangesToMove(&key_resolver, src, dest, amount, splitFromResolver(self->resolvers[src]))));
				for (auto& it : movedRanges)
					key_resolver.insert(it.range, it.dest);
				// for(auto& it : key_resolver.ranges())
				//	TraceEvent("KeyResolver").detail("Range", it.range()).detail("Value", it.value());

				self->resolverChangesVersion = *self->pVersion + 1;
				for (auto& p : self->commitProxies)
					self->resolverNeedingChanges.insert(p.id());
				self->resolverChanges.set(movedRanges);
			} catch (Error& e) {
				if (e.code() != error_code_operation_failed)
					throw;
			}
		}
	}
}

namespace {

Future<ResolutionSplitReply> splitWithSample(const StorageMetricSample& sample, ResolutionSplitRequest req) {
	ResolutionSplitReply reply;
	std::tie(reply.key, reply.used) = sample.splitAndEstimate(req.range, req.offset, req.front);
	return reply;
}

SplitFn splitUsingSample(const StorageMetricSample* sample) {
	return [sample](ResolutionSplitRequest req) { return splitWithSample(*sample, req); };
}

int64_t sampledLoadOf(CoalescedKeyRangeMap<int>& keyResolver, const StorageMetricSample& sample, int resolver) {
	int64_t load = 0;
	for (auto r : keyResolver.ranges()) {
		if (r.value() == resolver) {
			load += sample.getEstimate(r.range());
		}
	}
	return load;
}

} // namespace

TEST_CASE("/fdbserver/ResolutionBalancer/SkipRangeThatCannotBeSplit") {
	state CoalescedKeyRangeMap<int> keyResolver(0, normalKeys.end);
	state StorageMetricSample srcSample(1);
	state Standalone<VectorRef<ResolverMoveRef>> moves;
	state int64_t before = 0;
	state int round = 0;
	keyResolver.insert(normalKeys, 0);
	keyResolver.insert(KeyRangeRef("m"_sr, "t"_sr), 1);
	srcSample.sample.insert("u"_sr, 100);
	srcSample.sample.insert("v"_sr, 100);
	srcSample.sample.insert("w"_sr, 100);
	srcSample.sample.insert("x"_sr, 100);

	for (round = 0; round < 2; ++round) {
		before = sampledLoadOf(keyResolver, srcSample, 0);
		wait(store(moves, chooseRangesToMove(&keyResolver, 0, 1, 150, splitUsingSample(&srcSample))));
		for (const auto& move : moves) {
			ASSERT(!move.range.empty());
			keyResolver.insert(move.range, move.dest);
		}
		ASSERT_LT(sampledLoadOf(keyResolver, srcSample, 0), before);
	}
	return Void();
}

TEST_CASE("/fdbserver/ResolutionBalancer/MoveFromBorderingRange") {
	state CoalescedKeyRangeMap<int> keyResolver(0, normalKeys.end);
	state StorageMetricSample srcSample(1);
	state Standalone<VectorRef<ResolverMoveRef>> moves;
	keyResolver.insert(normalKeys, 1);
	keyResolver.insert(KeyRangeRef(""_sr, "m"_sr), 0);
	srcSample.sample.insert("a"_sr, 100);
	srcSample.sample.insert("k"_sr, 100);
	srcSample.sample.insert("l"_sr, 100);

	wait(store(moves, chooseRangesToMove(&keyResolver, 0, 1, 150, splitUsingSample(&srcSample))));
	ASSERT_EQ(moves.size(), 1);
	ASSERT(moves[0].range == KeyRangeRef("k"_sr, "m"_sr));
	ASSERT_EQ(moves[0].dest, 1);
	return Void();
}

TEST_CASE("/fdbserver/ResolutionBalancer/NothingToMoveWhenNoRangeCanBeSplit") {
	state CoalescedKeyRangeMap<int> keyResolver(0, normalKeys.end);
	state StorageMetricSample srcSample(1);
	state Standalone<VectorRef<ResolverMoveRef>> moves;
	state bool nothingMoved = false;
	keyResolver.insert(normalKeys, 1);
	keyResolver.insert(KeyRangeRef(""_sr, "m"_sr), 0);
	srcSample.sample.insert("u"_sr, 100);

	try {
		wait(store(moves, chooseRangesToMove(&keyResolver, 0, 1, 150, splitUsingSample(&srcSample))));
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_operation_failed);
		nothingMoved = true;
	}
	ASSERT(nothingMoved);
	ASSERT_EQ(keyResolver.size(), 2);
	ASSERT_EQ(keyResolver.rangeContaining("a"_sr).value(), 0);
	return Void();
}

TEST_CASE("/fdbserver/ResolutionBalancer/KeepMovesWhenLaterRangeIsSkipped") {
	state CoalescedKeyRangeMap<int> keyResolver(0, normalKeys.end);
	state StorageMetricSample srcSample(1);
	state Standalone<VectorRef<ResolverMoveRef>> moves;
	keyResolver.insert(normalKeys, 0);
	keyResolver.insert(KeyRangeRef("c"_sr, "m"_sr), 2);
	keyResolver.insert(KeyRangeRef("m"_sr, "p"_sr), 3);
	keyResolver.insert(KeyRangeRef("p"_sr, "t"_sr), 1);
	srcSample.sample.insert("u"_sr, 100);

	wait(store(moves, chooseRangesToMove(&keyResolver, 0, 1, 150, splitUsingSample(&srcSample))));
	ASSERT_EQ(moves.size(), 1);
	ASSERT(moves[0].range == KeyRangeRef("t"_sr, normalKeys.end));
	ASSERT_EQ(moves[0].dest, 1);
	return Void();
}
