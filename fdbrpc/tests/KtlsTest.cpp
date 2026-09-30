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
// with a cipher the kernel cannot handle (--openssl-conf=cbc) the connection must fall back to user-space TLS. A
// plain-OpenSSL client must interoperate with the Net2 server, and peer verification must still reject a client whose
// chain has a different root. Writes to a connection shut down for writing must fail cleanly, not raise SIGPIPE.
// Plain TCP connections get the same data checks, plus a sender that fills the socket while the receiver waits.
//
// With --net-io-uring (FLOW_KNOBS->NET_IO_URING), plain TCP connections and TLS connections whose records the kernel
// handles in both directions must move their data through the network thread's io_uring (its receive and send
// completion counters grow), and every other TLS connection must not touch it.
//
//   ktls_unittest [--net-io-uring] [--main-thread-handshakes] [--openssl-conf=tls12|cbc]

#include <cstdio>
#include <cstdlib>
#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include <linux/tls.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <sys/socket.h>
#include <unistd.h>

#include "flow/flow.h"
#include "flow/Coroutines.h"
#include "flow/IConnection.h"
#include "flow/IoUring.h"
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

struct Creds {
	std::string cert; // the chain without its root
	std::string key;
	std::string ca; // the root
};

Creds makeCreds() {
	auto arena = Arena();
	auto chain = mkcert::makeCertChain(arena, mkcert::makeCertChainSpec(arena, 2, mkcert::ESide::Server), {});
	auto nonRoot = chain;
	nonRoot.pop_back();
	return Creds{ concatCertChain(arena, nonRoot).toString(),
		          chain.front().privateKeyPem.toString(),
		          chain.back().certPem.toString() };
}

// A blocking plain-OpenSSL client on its own thread, presenting `own` and trusting `trusted.ca`: sends "ping" and
// expects "pong" back. Sets done when finished; ok when the exchange completed.
struct RawClient {
	std::atomic<bool> done{ false };
	std::atomic<bool> ok{ false };
	std::thread thread;

	void start(uint16_t port, Creds own, std::string trustedCa) {
		thread = std::thread([this, port, own, trustedCa]() {
			ok = run(port, own, trustedCa);
			done = true;
		});
	}

	static bool run(uint16_t port, const Creds& own, const std::string& trustedCa) {
		SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
		BIO* certBio = BIO_new_mem_buf(own.cert.data(), own.cert.size());
		X509* cert = PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr);
		BIO* keyBio = BIO_new_mem_buf(own.key.data(), own.key.size());
		EVP_PKEY* key = PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr);
		BIO* caBio = BIO_new_mem_buf(trustedCa.data(), trustedCa.size());
		X509* ca = PEM_read_bio_X509(caBio, nullptr, nullptr, nullptr);
		SSL_CTX_use_certificate(ctx, cert);
		SSL_CTX_use_PrivateKey(ctx, key);
		X509_STORE_add_cert(SSL_CTX_get_cert_store(ctx), ca);
		SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
		bool result = false;
		const int fd = socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in addr{};
		addr.sin_family = AF_INET;
		addr.sin_port = htons(port);
		addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		SSL* ssl = SSL_new(ctx);
		if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0 && SSL_set_fd(ssl, fd) == 1 &&
		    SSL_connect(ssl) == 1 && SSL_write(ssl, "ping", 4) == 4) {
			char reply[4];
			int have = 0;
			while (have < 4) {
				const int n = SSL_read(ssl, reply + have, 4 - have);
				if (n <= 0) {
					break;
				}
				have += n;
			}
			result = have == 4 && memcmp(reply, "pong", 4) == 0;
		}
		SSL_free(ssl);
		close(fd);
		X509_free(ca);
		EVP_PKEY_free(key);
		X509_free(cert);
		BIO_free(caBio);
		BIO_free(keyBio);
		BIO_free(certBio);
		SSL_CTX_free(ctx);
		return result;
	}
};

// Counts write() calls that returned 0 (socket full, or a send still in flight) into *blocked when given.
Future<Void> sendAll(Reference<IConnection> conn, std::string data, int* blocked = nullptr) {
	UnsentPacketQueue packets;
	PacketWriter writer(packets.getWriteBuffer(data.size()), nullptr, Unversioned());
	writer.serializeBytes(StringRef(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
	while (!packets.empty()) {
		const int sent = conn->write(packets.getUnsent(), FLOW_KNOBS->MAX_PACKET_SEND_BYTES);
		if (sent > 0) {
			packets.sent(sent);
		} else if (blocked) {
			++*blocked;
		}
		if (!packets.empty()) {
			co_await conn->onWritable();
		}
	}
}

// Receive and send completions on the network thread's io_uring so far.
int64_t ringSocketCompletions() {
	iouring::Ring* ring = iouring::Ring::existing();
	if (!ring) {
		return 0;
	}
	return ring->stats().completed[int(iouring::Kind::NetRecv)] + ring->stats().completed[int(iouring::Kind::NetSend)];
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

// The receiver only starts reading once the sender has filled the socket, so the sender must see write() return 0
// and resume when the receiver drains it.
Future<Void> backpressureCheck(Reference<IConnection> sender, Reference<IConnection> receiver, std::string label) {
	const int size = 32 << 20;
	const std::string data = pattern(size, 99);
	int blocked = 0;
	Future<Void> sent = sendAll(sender, data, &blocked);
	co_await delay(0.3);
	const std::string got = co_await receiveAll(receiver, size);
	co_await sent;
	check(got == data && blocked > 0,
	      label + ": 32 MB into a receiver that waits: the sender blocked " + std::to_string(blocked) +
	          " times, data intact");
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

// A plain-OpenSSL client with this process's chain must interoperate with a Net2 server (one side kernel TLS, the
// other user space); one with a chain from a different root must be rejected by the server's peer verification.
Future<Void> rawClientCheck(Creds trusted, Creds stranger, std::string label) {
	for (bool trustedClient : { true, false }) {
		Reference<IListener> listener = INetworkConnections::net()->listen(NetworkAddress::parse("127.0.0.1:0:tls"));
		Future<Reference<IConnection>> accepted = listener->accept();
		RawClient client;
		client.start(listener->getListenAddress().port, trustedClient ? trusted : stranger, trusted.ca);
		Reference<IConnection> server = co_await accepted;
		bool handshook = false;
		try {
			co_await server->acceptHandshake();
			handshook = true;
		} catch (Error& e) {
			if (e.code() != error_code_connection_failed) {
				throw;
			}
		}
		if (trustedClient) {
			bool exchanged = false;
			if (handshook && server->hasTrustedPeer()) {
				const std::string ping = co_await receiveAll(server, 4);
				co_await sendAll(server, "pong");
				exchanged = ping == "ping";
			}
			while (!client.done) {
				co_await delay(0.01);
			}
			check(exchanged && client.ok, label + ": plain OpenSSL client with a trusted chain interoperates");
		} else {
			check(!handshook, label + ": client with a chain from another root is rejected");
		}
		server->close();
		while (!client.done) {
			co_await delay(0.01);
		}
		client.thread.join();
	}
}

// OpenSSL writes to sockets it owns without MSG_NOSIGNAL. Writing to a connection that is shut down for writing, in the
// handshake or afterwards, must fail with connection_failed instead of killing the process with SIGPIPE.
Future<Void> sigpipeCheck(std::string label, bool tls = true) {
	for (bool duringHandshake : { true, false }) {
		if (duringHandshake && !tls) {
			continue;
		}
		Reference<IListener> listener =
		    INetworkConnections::net()->listen(NetworkAddress::parse(tls ? "127.0.0.1:0:tls" : "127.0.0.1:0"));
		Future<Reference<IConnection>> accepted = listener->accept();
		Reference<IConnection> client = co_await INetworkConnections::net()->connect(listener->getListenAddress());
		Reference<IConnection> server = co_await accepted;
		bool failed = false;
		try {
			if (duringHandshake) {
				::shutdown(client->getSocket().native_handle(), SHUT_WR);
				co_await client->connectHandshake();
			} else {
				co_await (client->connectHandshake() && server->acceptHandshake());
				::shutdown(client->getSocket().native_handle(), SHUT_WR);
				// Several writes: with io_uring a send's error surfaces on the next write.
				co_await sendAll(client, pattern(4 << 20, 7));
			}
		} catch (Error& e) {
			failed = e.code() == error_code_connection_failed;
		}
		check(failed,
		      label + ": a " + (duringHandshake ? "handshake" : "data") +
		          " write to a connection shut down for writing fails without SIGPIPE");
		client->close();
		server->close();
	}
}

// One connection's checks. tls = false: a plain TCP connection. The ring must carry its data exactly when
// expectRing.
Future<Void>
runOne(bool tls, bool useKtls, bool expectKernel, bool expectRing, std::string mode, Creds trusted, Creds stranger) {
	const_cast<FlowKnobs*>(FLOW_KNOBS)->TLS_USE_KTLS = useKtls;
	const std::string label =
	    tls ? mode + (useKtls ? " knob on" : " knob off") : (FLOW_KNOBS->NET_IO_URING ? "tcp io_uring" : "tcp");
	const int64_t ringBefore = ringSocketCompletions();

	Reference<IListener> listener =
	    INetworkConnections::net()->listen(NetworkAddress::parse(tls ? "127.0.0.1:0:tls" : "127.0.0.1:0"));
	Future<Reference<IConnection>> accepted = listener->accept();
	Reference<IConnection> client = co_await INetworkConnections::net()->connect(listener->getListenAddress());
	Reference<IConnection> server = co_await accepted;
	co_await (client->connectHandshake() && server->acceptHandshake());

	const KernelTlsState c = kernelTlsState(client);
	const KernelTlsState s = kernelTlsState(server);
	if (!tls) {
		check(c.ulp.empty() && s.ulp.empty(), label + ": no TLS layer on either end");
	} else if (!useKtls) {
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
	co_await backpressureCheck(client, server, label);

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

	if (tls) {
		co_await rawClientCheck(trusted, stranger, label);
	}
	co_await sigpipeCheck(label, tls);

	const int64_t ringCompletions = ringSocketCompletions() - ringBefore;
	if (expectRing) {
		check(ringCompletions > 0,
		      label + ": data went through io_uring (" + std::to_string(ringCompletions) +
		          " receive and send completions)");
	} else {
		check(ringCompletions == 0, label + ": io_uring not used for this connection's data");
	}
}

Future<Void> runAll(std::string mode, bool kernelCipher, Creds trusted, int* rc) {
	try {
		const Creds stranger = makeCreds();
		const bool ring = FLOW_KNOBS->NET_IO_URING;
		co_await runOne(false, false, false, ring, mode, trusted, stranger);
		co_await runOne(true, false, false, false, mode, trusted, stranger);
		const bool expectKernel = kernelCipher && kernelTlsAvailable();
		if (kernelCipher && !expectKernel) {
			std::printf("NOTE kernel tls module not loaded: knob-on run checks the user-space fallback only\n");
		}
		co_await runOne(true, true, expectKernel, ring && expectKernel, mode, trusted, stranger);
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
		if (arg == "--net-io-uring") {
			const_cast<FlowKnobs*>(FLOW_KNOBS)->NET_IO_URING = true;
			mode += " io_uring";
		} else if (arg == "--main-thread-handshakes") {
			const_cast<FlowKnobs*>(FLOW_KNOBS)->TLS_SERVER_HANDSHAKE_THREADS = 0;
			const_cast<FlowKnobs*>(FLOW_KNOBS)->TLS_CLIENT_HANDSHAKE_THREADS = 0;
			mode += " main-thread handshakes";
		} else if (arg == "--openssl-conf=tls12" || arg == "--openssl-conf=cbc") {
			const std::string which = arg.substr(arg.find('=') + 1);
			useOpenSSLConf(which);
			mode = (which == "cbc" ? "tls12 aes-cbc" : "tls12") + mode.substr(5);
			kernelCipher = which != "cbc";
		} else {
			std::fprintf(
			    stderr, "usage: %s [--net-io-uring] [--main-thread-handshakes] [--openssl-conf=tls12|cbc]\n", argv[0]);
			return 2;
		}
	}

	const Creds creds = makeCreds();
	TLSConfig tlsConfig(TLSEndpointType::SERVER);
	tlsConfig.setCertificateBytes(creds.cert);
	tlsConfig.setKeyBytes(creds.key);
	tlsConfig.setCABytes(creds.ca);

	g_network = newNet2(tlsConfig, false, false);
	openTraceFile({}, 10 << 20, 10 << 20, ".", "ktls_unittest");
	int rc = 1;
	Future<Void> test = runAll(mode, kernelCipher, creds, &rc);
	g_network->run();
	flushTraceFileVoid();
	std::printf("%s\n", rc == 0 ? "ALL PASSED" : "SOME CHECKS FAILED");
	return rc;
}
