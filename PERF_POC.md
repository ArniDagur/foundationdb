# perf-poc: kernel TLS and end-to-end io_uring

This branch, based on `next`, is a proof of concept that moves FoundationDB's
TLS record processing into the kernel (kTLS) and its socket and KAIO file I/O
onto one io_uring per process, which the run loop waits on. Everything is
behind knobs that default to off, so one image serves every arm of an A/B
comparison.

## Knobs

| Knob | Effect |
| --- | --- |
| `TLS_USE_KTLS` | OpenSSL owns the socket and installs the traffic keys in the kernel after the handshake (TLS 1.2 and 1.3 AES-GCM; other ciphers stay in user space). TLS 1.3 session tickets are off so no post-handshake record reaches a kTLS socket, and `TLS_RX_EXPECT_NO_PAD` lets the kernel decrypt straight into the receive buffer. |
| `NET_IO_URING` | Plain TCP connections, and TLS connections whose records the kernel handles in both directions, do their socket I/O through the network thread's io_uring, and the run loop waits on the ring instead of epoll. |
| `KAIO_IO_URING` | `AsyncFileKAIO` reads, writes and fdatasyncs (TLog disk queue, Redwood) go through the same ring instead of libaio and the EIO thread pool. |
| `NET_IO_URING_MULTISHOT`, `NET_IO_URING_RECV_BUFFERS`, `NET_IO_URING_RECV_BYTES`, `NET_IO_URING_MAX_QUEUED_BYTES`, `IO_URING_ENTRIES`, `IO_URING_METRICS_INTERVAL` | Multishot receives into a shared provided-buffer ring (on where the kernel supports it), its size, the per-connection unread-data cap, the ring size, and the `IoUringMetrics` interval. |

## How it works

- **One ring per process** (`flow/IoUring.{h,cpp}`), created on the network
  thread with raw syscalls. Setup tries `SUBMIT_ALL | SINGLE_ISSUER |
  DEFER_TASKRUN | TASKRUN_FLAG | NO_SQARRAY` (6.6+), then without
  `NO_SQARRAY` (6.1+), then `COOP_TASKRUN` (5.19+), then a plain ring (5.15).
  The ring fd is registered, the CQ is 4x the SQ, and idle periods end in an
  `EXT_ARG` timed wait, so completions are only processed inside
  `io_uring_enter` and a whole iteration's submissions go in one call.
- **Run loop**: Asio keeps listeners, connects and handshakes. The ring
  watches Asio's epoll fd with a `POLL_ADD`, and Asio is polled only when that
  fires or its queue has handlers, so there is no `epoll_wait` per iteration.
  Other threads wake the loop through an eventfd read kept armed on the ring.
- **Sockets** (`UringSocket` in `flow/Net2.cpp`): the socket leaves Asio's
  epoll set. On 6.1+ one multishot receive stays armed for the life of the
  connection and fills buffers from a shared provided-buffer ring; `read()`
  copies out and recycles them. A connection holding more than
  `NET_IO_URING_MAX_QUEUED_BYTES` of unread data cancels its receive until the
  reader catches up, so TCP flow control still reaches its sender, and
  running out of buffers (`-ENOBUFS`) re-arms when buffers come back. Sends are
  one `SENDMSG` at a time straight from the caller's `PacketBuffer`s (no copy;
  the buffers are referenced until the send completes), with `MSG_WAITALL` on
  plain TCP. Older kernels get one receive at a time into a per-connection
  buffer. kTLS sockets take the same path once both directions are in the
  kernel; a non-data record (`-EIO` from a plain receive) goes to OpenSSL and
  receiving resumes.
- **KAIO**: each run-loop iteration's queued iocbs become `READ`, `WRITE` and
  `FSYNC(DATASYNC)` entries on the same ring.
- **Observability**: `IoUringReady` (setup flags, features, buffer ring),
  `IoUringMetrics` every 5 s (enters, waits, submissions and completions per
  kind, buffer-ring exhaustion, receive pauses), `N2_TLSKernelOffload` per
  connection, counters `/Net2/IoUring/Connections` and `/Net2/TLS/Ktls*`.

## Tests

- `ktls_unittest` (`fdbrpc/tests/KtlsTest.cpp`, 10 CTest variants): loopback
  connections through Net2 for plain TCP, TLS with kTLS and the user-space
  fallback (TLS 1.3, TLS 1.2, a cipher the kernel cannot take, main-thread
  handshakes), with and without `NET_IO_URING`, with multishot and one-shot
  receives, and with a two-buffer pool. Payloads from 1 byte to 3 MB both
  ways at once, 1000 small writes, a 32 MB transfer into a reader that waits
  (the sender must stall), peer close as `connection_failed`, a plain OpenSSL
  peer, peer verification, and no SIGPIPE. With the ring, the checks assert
  the data went through it (completion counters), the sockets left every
  epoll set, receives were multishot where supported, and a connection paused
  and resumed (or ran out of buffers and resumed).
- `/fdbrpc/AsyncFileKAIO/IoUring`: 1024 concurrent 16 KiB writes, 4 syncs and
  1024 reads through the ring on an O_DIRECT file, with the data compared and
  the ring's per-kind counters checked.
- All pass on kernel 5.15 (fallback paths) and 6.8 (`DEFER_TASKRUN`,
  `NO_SQARRAY`, multishot receives, buffer rings).
- `fdbrpc_transport_bench` gained `--tls_dir`, `--ktls`, `--net_io_uring` and
  CPU-per-request reports.

## Results

Test cluster: the prod-shaped `memy-large` spec on the lapstorage Kubernetes
cluster (158 fdbservers in two regions and a satellite; 1-CPU pods; Redwood;
TLS 1.3 `TLS_AES_256_GCM_SHA384` everywhere; AMD EPYC 9734, kernel 6.8, pod
networking over veth). kTLS runs in software: the NICs are unreachable from
pods, so `TlsTxDevice`/`TlsRxDevice` stay 0. Clients are release-7.3 mako,
60% writes (GRV, get, set, commit) and 40% reads.

### 14K txn/s, A-B-A

Arms: A1 knobs off, B all three knobs on, A2 knobs off; three 60 s runs each
after a 120 s warm-up, 0 client errors. CPU is each process's own
`ProcessMetrics` CPU over its load plateau (the median process of each role
per run; the table shows the median of the three runs), and the same per
FlowTransport message (packets read plus generated): message counts vary about
±20% between runs at the same txn/s in every arm, so the per-message figure is
the steadier measure of the change.

ABA_TABLE_PLACEHOLDER

- TLog CPU is 16–22% lower in B than in either A arm, and every B run is below
  every A run, whether measured as cores or per message. Satellite TLogs and
  resolvers show the same, and remote TLogs, log routers and proxies are lower
  too.
- Storage servers do not improve: their traffic is mostly small client reads,
  where software kTLS on 6.8 costs more than OpenSSL (see the benchmark
  below). With NIC offload or a 6.11+ kernel (VAES AES-GCM) this should
  change; that was not measurable here.
- Latency p99 at 14K is unchanged within noise: commit ABA_COMMIT, GRV
  ABA_GRV. Read and range-read p99 swing 2–5x between runs in every arm.

SAT_PLACEHOLDER

### Controlled benchmark (kernel 6.8, loopback, one pod)

`fdbrpc_transport_bench`: an echo server and a single-threaded client with 64
requests in flight; server CPU per request (µs), mean of two 10 s reports.

MB_PLACEHOLDER

io_uring alone cuts per-message CPU 12–18% for small and 1 KB messages. Software
kTLS on 6.8 is cheaper than OpenSSL for 100-byte messages but 38% more
expensive at 1 KB without io_uring (one `recvmsg` per record); io_uring brings
that to +10%.

ATTRIB_PLACEHOLDER

### Rollouts and correctness

ROLLOUT_PLACEHOLDER

## Caveats

- kTLS here is software-only on kernel 6.8, whose AES-GCM is the older
  `generic-gcm-aesni`; the measured storage-server cost is specific to that.
- The run loop still uses Asio for accept, connect and handshakes, and
  connection setup is rare. Fixed files, registered buffers, `SEND_ZC` and
  SQPOLL were left out: fixed files and registered buffers would save a
  per-operation atomic or page pin at a few thousand operations per second,
  `SEND_ZC` copies anyway over veth and kTLS, and SQPOLL spends a core on a
  1-CPU pod.
- The test cluster shares nodes with other clusters; cores at a fixed txn/s
  vary between runs, which is why the per-message figures are reported.

## Reproducing

The harness is in `contrib/perf-bench/k8s` on the perf branch (not on
`next`): `build-image.sh` packages a build into the fdb-kubernetes-monitor
image, `fastapply.sh` switches overlays, `run.sh` runs the mako load.
Overlays: `poc-image.yaml` (this image, knobs off), `poc-all.yaml` (all three
knobs), `poc-ktls.yaml`, `poc-ktls-net.yaml`, `poc-kaio.yaml`.
