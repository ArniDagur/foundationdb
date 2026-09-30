/*
 * fdbrpc_bench.cpp
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

#include <fstream>
#include <iostream>
#include <sstream>
#include <boost/program_options.hpp>
#include <sys/resource.h>

#include "flow/flow.h"
#include "flow/IoUring.h"
#include "flow/Knobs.h"
#include "flow/MkCert.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/FlowTransport.h"

namespace fdbrpc_bench {
NetworkAddress serverAddress;
TaskPriority echoEndpointPriority = TaskPriority::DefaultEndpoint;
int reportSeconds = 10;
int reportCount = 0; // 0: report forever

// This process's CPU seconds (all threads, user and system).
double processCpuSeconds() {
	rusage u;
	getrusage(RUSAGE_SELF, &u);
	return u.ru_utime.tv_sec + u.ru_utime.tv_usec * 1e-6 + u.ru_stime.tv_sec + u.ru_stime.tv_usec * 1e-6;
}

// Prints requests per second, process CPU cores and CPU microseconds per request every reportSeconds, and stops the
// network after reportCount reports when set.
Future<Void> reportLoop(const char* role, const int64_t* requests) {
	int64_t lastRequests = *requests;
	double lastCpu = processCpuSeconds();
	double lastTime = timer_monotonic();
	for (int n = 1; reportCount == 0 || n <= reportCount; n++) {
		co_await delay(reportSeconds);
		const double now = timer_monotonic(), cpu = processCpuSeconds();
		const int64_t done = *requests - lastRequests;
		iouring::Ring* ring = iouring::Ring::existing();
		std::cout << format("%s report %d: %.0f req/s, %.3f cores, %.2f cpu_us/req, io_uring enters %lld\n",
		                    role,
		                    n,
		                    done / (now - lastTime),
		                    (cpu - lastCpu) / (now - lastTime),
		                    done > 0 ? (cpu - lastCpu) * 1e6 / done : 0.0,
		                    ring ? (long long)ring->stats().enters : 0LL)
		          << std::flush;
		lastRequests = *requests;
		lastCpu = cpu;
		lastTime = now;
	}
	g_network->stop();
}

constexpr int MAX_CLIENT_CONCURRENCY = 4096;

enum FdbRpcBenchWellKnownEndpoints {
	WLTOKEN_ECHO_SERVER = WLTOKEN_FIRST_AVAILABLE,
	WLTOKEN_COUNT_ENDPOINTS,
};

struct EchoServerInterface {
	constexpr static FileIdentifier file_identifier = 3152015;
	RequestStream<struct GetInterfaceRequest> getInterface;
	RequestStream<struct EchoRequest> echo;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, echo);
	}
};

struct GetInterfaceRequest {
	constexpr static FileIdentifier file_identifier = 12004156;
	ReplyPromise<EchoServerInterface> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct EchoRequest {
	constexpr static FileIdentifier file_identifier = 10624019;
	std::string message;
	ReplyPromise<std::string> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, message, reply);
	}
};

// A sliding window counter over last `size` seconds. Internally uses a vector
// where each entry counts the number of hits each second.
class StatCounter {
public:
	explicit StatCounter(int size = 10) : vals(size) {}

	// Returns the average number of hits per seconds over last `size` seconds.
	int avg() {
		int now_ts = this->now() / 1000; // Convert ms to second.
		int sum = 0;
		for (auto [ts, v] : vals) {
			if (ts < now_ts - vals.size()) // timestamp older than last `size` seconds.
				continue;
			sum += v;
		}
		return sum / vals.size();
	}

	// Increaments the counter by one for current time.
	void inc() {
		int ts = this->now() / 1000;
		int pos = ts % vals.size();

		auto [old_ts, v] = vals[pos];
		if (old_ts < ts) {
			// Timestamp older than last `size` second, so we reset it back.
			vals[pos] = { ts, 1 };
		} else {
			vals[pos] = { old_ts, v + 1 };
		}
	}

private:
	int64_t now() {
		auto n = std::chrono::system_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(n.time_since_epoch());
		return duration.count();
	}

	std::vector<std::pair<int64_t, int>> vals;
};

class EchoServer {
	static void rethrowUnexpectedError(const Error& e) {
		if (e.code() != error_code_operation_obsolete) {
			fprintf(stderr, "Error: %s\n", e.what());
			throw e;
		}
	}

	Future<Void> serveGetInterfaceReqs() {
		while (true) {
			try {
				GetInterfaceRequest req = co_await interf.getInterface.getFuture();
				req.reply.send(interf);
			} catch (Error& e) {
				rethrowUnexpectedError(e);
			}
		}
	}

	Future<Void> serveEchoReqs() {
		while (true) {
			try {
				EchoRequest req = co_await interf.echo.getFuture();
				req.reply.send(req.message);
				counter.inc();
				++served;
			} catch (Error& e) {
				rethrowUnexpectedError(e);
			}
		}
	}

	EchoServerInterface interf;
	StatCounter counter;

public:
	int64_t served = 0;

	EchoServer() {
		interf.getInterface.makeWellKnownEndpoint(WLTOKEN_ECHO_SERVER, TaskPriority::DefaultEndpoint);
		interf.echo.getEndpoint(echoEndpointPriority);
	}

	Future<Void> run() { co_await race(serveGetInterfaceReqs(), serveEchoReqs(), reportLoop("server", &served)); }
};

Future<Void> echoServer() {
	EchoServer server;
	co_await server.run();
}

int payload_size_bytes = 1024 * 10;

std::string randString(int size) {
	const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	const int charsetLength = charset.length();
	std::string result;

	// Seed the random number generator
	std::srand(static_cast<unsigned int>(std::time(nullptr)));

	for (int i = 0; i < size; ++i) {
		result += charset[std::rand() % charsetLength];
	}

	return result;
}

int64_t clientRequests = 0;

Future<Void> echoClientActor(EchoServerInterface server, std::string payload) {
	while (true) {
		EchoRequest echoRequest;
		echoRequest.message = payload;
		co_await server.echo.getReply(echoRequest);
		++clientRequests;
	}
}

int clientConcurrency = 1;

Future<Void> echoClient() {
	std::cout << "Starting client. Payload size: " << payload_size_bytes << " bytes, concurrency " << clientConcurrency
	          << std::endl;
	EchoServerInterface server;
	server.getInterface =
	    RequestStream<GetInterfaceRequest>(Endpoint::wellKnown({ serverAddress }, WLTOKEN_ECHO_SERVER));
	server = co_await server.getInterface.getReply(GetInterfaceRequest());
	const std::string payload = randString(payload_size_bytes);
	std::vector<Future<Void>> actors;
	for (int i = 0; i < clientConcurrency; i++) {
		actors.push_back(echoClientActor(server, payload));
	}
	co_await race(waitForAll(actors), reportLoop("client", &clientRequests));
}

std::unordered_map<std::string, std::function<Future<Void>()>> actors = {
	{ "server", &echoServer },
	{ "client", &echoClient },
};
} // namespace fdbrpc_bench

int main(int argc, char* argv[]) {
	using namespace fdbrpc_bench;
	namespace po = boost::program_options;

	po::options_description desc("fdbrpc_bench usage");
	// clang-format off
	desc.add_options()
		("help,h","show help message")
		("mode,m", po::value<std::string>(), "process mode [server/client]")
		("payload_size,s", po::value<int>(), "size of payload sent by client (bytes)")
		("endpoint_priority", po::value<std::string>()->default_value("default"), "server echo endpoint priority [default/loadbalanced]")
		("concurrency,c", po::value<int>()->default_value(1), "number of client actors [1/4096]")
	("tls_dir", po::value<std::string>(), "use TLS; the server writes a generated certificate chain to this directory and the client reads it")
	("ktls", "kernel TLS for TLS connections (FLOW_KNOBS->TLS_USE_KTLS)")
	("net_io_uring", "socket I/O through io_uring (FLOW_KNOBS->NET_IO_URING)")
	("report_seconds", po::value<int>()->default_value(10), "seconds between reports")
	("reports", po::value<int>()->default_value(0), "stop after this many reports (0: never)");
	// clang-format on

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	// Check for help option
	if (vm.count("help")) {
		std::cout << desc << std::endl;
		return 0;
	}

	auto errMsg = "invalid arguments provided.\n";
	if (vm.count("mode") == 0) {
		std::cerr << errMsg << desc << std::endl;
		return -1;
	}

	auto mode = vm["mode"].as<std::string>();
	auto endpointPriority = vm["endpoint_priority"].as<std::string>();
	auto concurrency = vm["concurrency"].as<int>();
	if ((mode != "client" && mode != "server") ||
	    (endpointPriority != "default" && endpointPriority != "loadbalanced") || concurrency < 1 ||
	    concurrency > MAX_CLIENT_CONCURRENCY || (vm.count("payload_size") > 0 && vm["payload_size"].as<int>() < 0) ||
	    (mode == "server" && (vm.count("payload_size") > 0 || concurrency != 1)) ||
	    (mode == "client" && endpointPriority != "default")) {
		std::cerr << errMsg << desc << std::endl;
		return -1;
	}

	if (vm.count("payload_size") > 0) {
		payload_size_bytes = vm["payload_size"].as<int>();
	}
	echoEndpointPriority =
	    endpointPriority == "loadbalanced" ? TaskPriority::LoadBalancedEndpoint : TaskPriority::DefaultEndpoint;

	bool isServer = (mode == "server");
	std::vector<std::function<Future<Void>()>> toRun;
	auto actor = actors.find(mode);
	toRun.resize(1, actor->second);
	clientConcurrency = concurrency;
	reportSeconds = vm["report_seconds"].as<int>();
	reportCount = vm["reports"].as<int>();
	if (vm.count("ktls")) {
		const_cast<FlowKnobs*>(FLOW_KNOBS)->TLS_USE_KTLS = true;
	}
	if (vm.count("net_io_uring")) {
		const_cast<FlowKnobs*>(FLOW_KNOBS)->NET_IO_URING = true;
	}

	platformInit();
	TLSConfig tlsConfig(isServer ? TLSEndpointType::SERVER : TLSEndpointType::CLIENT);
	const bool tls = vm.count("tls_dir") > 0;
	if (tls) {
		const std::string dir = vm["tls_dir"].as<std::string>();
		auto readFile = [](const std::string& path) {
			std::ifstream f(path);
			std::stringstream ss;
			ss << f.rdbuf();
			return ss.str();
		};
		if (isServer) {
			Arena arena;
			auto chain = mkcert::makeCertChain(arena, mkcert::makeCertChainSpec(arena, 2, mkcert::ESide::Server), {});
			auto nonRoot = chain;
			nonRoot.pop_back();
			std::ofstream(dir + "/cert.pem") << concatCertChain(arena, nonRoot).toString();
			std::ofstream(dir + "/key.pem") << chain.front().privateKeyPem.toString();
			std::ofstream(dir + "/ca.pem") << chain.back().certPem.toString();
		}
		tlsConfig.setCertificateBytes(readFile(dir + "/cert.pem"));
		tlsConfig.setKeyBytes(readFile(dir + "/key.pem"));
		tlsConfig.setCABytes(readFile(dir + "/ca.pem"));
	}
	g_network = newNet2(tlsConfig, false, true);
	FlowTransport::createInstance(!isServer, 0, WLTOKEN_COUNT_ENDPOINTS);

	serverAddress = NetworkAddress::parse(tls ? "127.0.0.1:9001:tls" : "127.0.0.1:9001");
	NetworkAddress publicAddress = serverAddress;

	try {
		if (isServer) {
			auto listenError = FlowTransport::transport().bind(publicAddress, publicAddress);
			if (listenError.isError()) {
				listenError.get();
			}
		}
	} catch (Error& e) {
		std::cout << format("Error while binding to address (%d): %s\n", e.code(), e.what());
	}

	std::vector<Future<Void>> all;
	all.reserve(toRun.size());
	for (auto& f : toRun) {
		all.emplace_back(f());
	}

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	return 0;
}
