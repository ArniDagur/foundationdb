/*
 * KtlsTest.cpp
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

// Loopback TLS connections through Net2, with FLOW_KNOBS->TLS_USE_KTLS off and on. Payloads of many sizes, sent as
// multi-buffer packet chains in both directions at once, must arrive intact. With the knob on and the kernel tls
// module available, both ends must have the kernel TLS upper-layer protocol with send and receive keys installed;
// with a cipher the kernel cannot handle (--openssl-conf=cbc) the connection must fall back to user-space TLS.
//
//   ktls_unittest [--main-thread-handshakes] [--openssl-conf=tls12|cbc]

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <linux/tls.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "flow/flow.h"
#include "flow/Coroutines.h"
#include "flow/IConnection.h"
#include "flow/Knobs.h"
#include "flow/MkCert.h"
#include "flow/Net2Packet.h"
#include "flow/TLSConfig.h"
#include "flow/network.h"
#include "flow/serialize.h"

namespace {

int failures = 0;

void check(bool ok, const std::string& what) {
	std::printf("%s %s\n", ok ? "PASS" : "FAIL", what.c_str());
	if (!ok) {
		++failures;
	}
}

bool kernelTlsAvailable() {
	return access("/proc/net/tls_stat", R_OK) == 0;
}

struct KernelTlsState {
	std::string ulp;
	bool tx = false;
	bool rx = false;
};

KernelTlsState kernelTlsState(Reference<IConnection> conn) {
	KernelTlsState s;
	const int fd = conn->getSocket().native_handle();
	char name[16] = {};
	socklen_t len = sizeof(name);
	if (getsockopt(fd, IPPROTO_TCP, TCP_ULP, name, &len) == 0) {
		s.ulp = std::string(name, strnlen(name, sizeof(name)));
	}
	// A header-sized buffer asks for just the version and cipher; the kernel fails the call if no key is installed.
	tls_crypto_info info{};
	len = sizeof(info);
	s.tx = getsockopt(fd, SOL_TLS, TLS_TX, &info, &len) == 0 && info.cipher_type != 0;
	info = {};
	len = sizeof(info);
	s.rx = getsockopt(fd, SOL_TLS, TLS_RX, &info, &len) == 0 && info.cipher_type != 0;
	return s;
}

std::string pattern(int size, int seed) {
	std::string s(size, '\0');
	uint32_t x = 2654435761u * (seed + 1);
	for (int i = 0; i < size; i++) {
		x = x * 1664525u + 1013904223u;
		s[i] = static_cast<char>(x >> 24);
	}
	return s;
}

Future<Void> sendAll(Reference<IConnection> conn, std::string data) {
	UnsentPacketQueue packets;
	PacketWriter writer(packets.getWriteBuffer(data.size()), nullptr, Unversioned());
	writer.serializeBytes(StringRef(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
	while (!packets.empty()) {
		const int sent = conn->write(packets.getUnsent(), FLOW_KNOBS->MAX_PACKET_SEND_BYTES);
		if (sent > 0) {
			packets.sent(sent);
		}
		if (!packets.empty()) {
			co_await conn->onWritable();
		}
	}
}

Future<std::string> receiveAll(Reference<IConnection> conn, int size) {
	std::string got(size, '\0');
	int have = 0;
	while (have < size) {
		const int n = conn->read(reinterpret_cast<uint8_t*>(got.data()) + have,
		                         reinterpret_cast<uint8_t*>(got.data()) + std::min(size, have + (1 << 20)));
		have += n;
		if (n == 0) {
			co_await conn->onReadable();
		}
	}
	co_return got;
}

// Sends patterned payloads in both directions at once and checks that both arrive intact.
Future<Void> exchange(Reference<IConnection> client, Reference<IConnection> server, int size, std::string label) {
	const std::string up = pattern(size, size);
	const std::string down = pattern(size, size + 1);
	Future<std::string> atServer = receiveAll(server, size);
	Future<std::string> atClient = receiveAll(client, size);
	co_await (sendAll(client, up) && sendAll(server, down));
	const std::string gotUp = co_await atServer;
	const std::string gotDown = co_await atClient;
	check(gotUp == up && gotDown == down, label + ": " + std::to_string(size) + " bytes each way intact");
}

Future<Void> runOne(bool useKtls, bool expectKernel, std::string mode) {
	const_cast<FlowKnobs*>(FLOW_KNOBS)->TLS_USE_KTLS = useKtls;
	const std::string label = mode + (useKtls ? " knob on" : " knob off");

	Reference<IListener> listener = INetworkConnections::net()->listen(NetworkAddress::parse("127.0.0.1:0:tls"));
	Future<Reference<IConnection>> accepted = listener->accept();
	Reference<IConnection> client = co_await INetworkConnections::net()->connect(listener->getListenAddress());
	Reference<IConnection> server = co_await accepted;
	co_await (client->connectHandshake() && server->acceptHandshake());

	const KernelTlsState c = kernelTlsState(client);
	const KernelTlsState s = kernelTlsState(server);
	if (!useKtls) {
		check(c.ulp.empty() && s.ulp.empty(), label + ": no kernel TLS on either end");
	} else if (expectKernel) {
		check(c.ulp == "tls" && s.ulp == "tls", label + ": kernel TLS protocol on both ends");
		check(c.tx && c.rx && s.tx && s.rx, label + ": kernel send and receive keys on both ends");
	} else {
		check(!c.tx && !c.rx && !s.tx && !s.rx, label + ": user-space TLS when the kernel cannot take the cipher");
	}

	for (int size : { 1, 100, 4000, 16383, 16384, 16385, 65537, 1 << 20, 3 << 20 }) {
		co_await exchange(client, server, size, label);
	}
	// Many small writes in one direction, read as a stream.
	{
		std::string all;
		Future<std::string> got = receiveAll(server, 1000 * 37);
		for (int i = 0; i < 1000; i++) {
			std::string piece = pattern(37, i);
			all += piece;
			co_await sendAll(client, piece);
		}
		check((co_await got) == all, label + ": 1000 small writes intact");
	}

	// A closed peer surfaces as a read error, not a hang.
	client->close();
	bool failed = false;
	try {
		uint8_t b[16];
		while (true) {
			if (server->read(b, b + sizeof(b)) == 0) {
				co_await server->onReadable();
			}
		}
	} catch (Error& e) {
		failed = e.code() == error_code_connection_failed;
	}
	check(failed, label + ": peer close is reported as connection_failed");
	server->close();
}

Future<Void> runAll(std::string mode, bool kernelCipher, int* rc) {
	try {
		co_await runOne(false, false, mode);
		const bool expectKernel = kernelCipher && kernelTlsAvailable();
		if (kernelCipher && !expectKernel) {
			std::printf("NOTE kernel tls module not loaded: knob-on run checks the user-space fallback only\n");
		}
		co_await runOne(true, expectKernel, mode);
	} catch (Error& e) {
		std::printf("FAIL %s: unexpected error %s\n", mode.c_str(), e.what());
		++failures;
	}
	*rc = failures == 0 ? 0 : 1;
	g_network->stop();
}

// An OpenSSL config applied to every new context: TLS 1.2 only, optionally with a cipher that kernel TLS cannot take.
void useOpenSSLConf(const std::string& which) {
	const std::string path = "ktls_unittest_openssl.cnf";
	FILE* f = std::fopen(path.c_str(), "w");
	std::fprintf(f,
	             "openssl_conf = openssl_init\n[openssl_init]\nssl_conf = ssl_sect\n[ssl_sect]\n"
	             "system_default = system_default_sect\n[system_default_sect]\nMaxProtocol = TLSv1.2\n%s",
	             which == "cbc" ? "CipherString = ECDHE-ECDSA-AES256-SHA\n" : "");
	std::fclose(f);
	setenv("OPENSSL_CONF", path.c_str(), 1);
}

} // namespace

int main(int argc, char** argv) {
	std::string mode = "tls13";
	bool kernelCipher = true;
	for (int i = 1; i < argc; i++) {
		const std::string arg = argv[i];
		if (arg == "--main-thread-handshakes") {
			const_cast<FlowKnobs*>(FLOW_KNOBS)->TLS_SERVER_HANDSHAKE_THREADS = 0;
			const_cast<FlowKnobs*>(FLOW_KNOBS)->TLS_CLIENT_HANDSHAKE_THREADS = 0;
			mode += " main-thread handshakes";
		} else if (arg == "--openssl-conf=tls12" || arg == "--openssl-conf=cbc") {
			const std::string which = arg.substr(arg.find('=') + 1);
			useOpenSSLConf(which);
			mode = which == "cbc" ? "tls12 aes-cbc" : "tls12";
			kernelCipher = which != "cbc";
		} else {
			std::fprintf(stderr, "usage: %s [--main-thread-handshakes] [--openssl-conf=tls12|cbc]\n", argv[0]);
			return 2;
		}
	}

	auto arena = Arena();
	auto chain = mkcert::makeCertChain(arena, mkcert::makeCertChainSpec(arena, 2, mkcert::ESide::Server), {});
	auto nonRoot = chain;
	nonRoot.pop_back();
	TLSConfig tlsConfig(TLSEndpointType::SERVER);
	tlsConfig.setCertificateBytes(concatCertChain(arena, nonRoot).toString());
	tlsConfig.setKeyBytes(chain.front().privateKeyPem.toString());
	tlsConfig.setCABytes(chain.back().certPem.toString());

	g_network = newNet2(tlsConfig, false, false);
	openTraceFile({}, 10 << 20, 10 << 20, ".", "ktls_unittest");
	int rc = 1;
	Future<Void> test = runAll(mode, kernelCipher, &rc);
	g_network->run();
	flushTraceFileVoid();
	std::printf("%s\n", rc == 0 ? "ALL PASSED" : "SOME CHECKS FAILED");
	return rc;
}
