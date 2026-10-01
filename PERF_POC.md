# perf-poc: kernel TLS and end-to-end io_uring

This branch, based on `next`, is a proof of concept that moves FoundationDB's
TLS record processing into the kernel (kTLS) and all of its cluster socket I/O
and KAIO file I/O onto one io_uring per process, which the run loop waits on.
Everything is behind knobs that default to off, so one image serves every arm
of an A/B comparison.

## Knobs

| Knob | Effect |
| --- | --- |
| `TLS_USE_KTLS` | Kernel TLS is mandatory. Only ciphers the kernel can carry are offered (TLS 1.3 AES-GCM and ChaCha20-Poly1305; TLS 1.2 ECDHE with the same), OpenSSL installs the traffic keys in the kernel after the handshake, and a connection the kernel does not take in both directions fails. There is no user-space fallback; the process checks at TLS setup that the kernel has TLS sockets. TLS 1.3 session tickets are off, and `TLS_RX_EXPECT_NO_PAD` lets the kernel decrypt straight into the receive buffer. |
| `NET_IO_URING` | Plain TCP connections and kernel TLS connections are accepted, connected, shake hands (TLS) and move their data through the network thread's io_uring, and the run loop waits on the ring instead of epoll. These sockets are never in Asio's epoll set, and handshakes run on the network thread (no handshake thread pool). External connections (HTTP, blob stores) stay on Asio. |
| `KAIO_IO_URING` | `AsyncFileKAIO` reads, writes and fdatasyncs (TLog disk queue, Redwood) go through the same ring instead of libaio and the EIO thread pool. |
| `KAIO_ZERO_FILL_GROWTH` | KAIO files grow with written zeros instead of `fallocate` (unwritten extents), `zeroRange` writes zeros, and unwritten ranges already in a file are written with zeros when it is opened. |
| `NET_IO_URING_MULTISHOT`, `NET_IO_URING_RECV_BUFFERS`, `NET_IO_URING_RECV_BYTES`, `NET_IO_URING_MAX_QUEUED_BYTES`, `IO_URING_ENTRIES`, `IO_URING_MAX_WORKERS`, `IO_URING_METRICS_INTERVAL` | Multishot receives into a shared provided-buffer ring (on where the kernel supports it), its size, the per-connection unread-data cap, the ring size, the io-wq worker cap, and the `IoUringMetrics` interval. |

## How it works

- **One ring per process** (`flow/IoUring.{h,cpp}`), created on the network
  thread with raw syscalls. Setup tries `SUBMIT_ALL | SINGLE_ISSUER |
  DEFER_TASKRUN | TASKRUN_FLAG | NO_SQARRAY` (6.6+), then without
  `NO_SQARRAY` (6.1+), then `COOP_TASKRUN` (5.19+), then a plain ring (5.15).
  The ring fd is registered, the CQ is 4x the SQ, io-wq workers are capped, and
  idle periods end in an `EXT_ARG` timed wait, so completions are processed
  only inside `io_uring_enter` and a whole run-loop iteration's submissions go
  in one call.
- **Connection setup**: listeners use one multishot accept (5.19+; one accept
  at a time before), connects are `IORING_OP_CONNECT`, and TLS handshakes run
  non-blocking on the network thread, waiting on ring polls. Closing a
  connection that is still being set up cancels its ring operations by
  descriptor. Asio's reactor is left with nothing to watch for these sockets;
  the ring watches Asio's epoll fd only for whatever else uses Asio (external
  connections, DNS resolution) and polls Asio only when it has work.
- **Data** (`UringSocket` in `flow/Net2.cpp`): on 6.1+ one multishot receive
  stays armed for the life of the connection and fills buffers from a shared
  provided-buffer ring; `read()` copies out and recycles them. A connection
  holding more than `NET_IO_URING_MAX_QUEUED_BYTES` of unread data cancels its
  receive until the reader catches up, so TCP flow control still reaches its
  sender, and running out of buffers (`-ENOBUFS`) re-arms when buffers come
  back. Sends are one `SENDMSG` at a time straight from the caller's
  `PacketBuffer`s (no copy; the buffers are referenced until the send
  completes), with `MSG_WAITALL` on plain TCP. Older kernels get one receive at
  a time into a per-connection buffer. On kernel TLS sockets a non-data record
  (`-EIO` from a plain receive) goes to OpenSSL and receiving resumes.
- **KAIO**: each run-loop iteration's queued iocbs become `READ`, `WRITE` and
  `FSYNC(DATASYNC)` entries on the same ring. io_uring first tries each
  O_DIRECT read and write without blocking and hands what would block to io-wq
  worker threads. On XFS, every write into an unwritten (fallocated) extent is
  converted by a transaction that holds the inode lock exclusively, so
  concurrent non-blocking attempts fail: a storage server punted 62% of its
  writes and 16% of its reads at saturation, with ~700 worker threads, because
  Redwood grows its file with `fallocate` in 160 MB steps. libaio waits on the
  lock instead. With `KAIO_ZERO_FILL_GROWTH`, growth and `zeroRange` write
  zeros in 1 MB writes and unwritten ranges are written at open, so page writes
  land on written extents (a probe on the same disks: 4 µs CPU per 8 KB write
  and 1–2 worker threads, against 22 µs and 1792 threads into unwritten
  extents). Refreshing the file times before writes, tried first, made punting
  worse and was dropped. The io-wq worker count is capped as a backstop.
- **Observability**: `IoUringReady` (setup flags, features, buffer ring,
  worker cap), `IoUringMetrics` every 5 s (enters, waits, submissions and
  completions per kind, buffer-ring exhaustion, receive pauses),
  `N2_TLSKernelOffload` per connection, `N2_KernelTLSCheck`, counters
  `/Net2/IoUring/Connections` and `/Net2/TLS/Ktls*`.

## Tests

- `ktls_unittest` (`fdbrpc/tests/KtlsTest.cpp`, 10 CTest variants): loopback
  connections through Net2 for plain TCP and TLS (TLS 1.3, TLS 1.2, an OpenSSL
  configuration that prefers a cipher the kernel cannot take, main-thread
  handshakes), with and without `NET_IO_URING`, with multishot and one-shot
  receives, and with a two-buffer pool. Payloads from 1 byte to 3 MB both ways
  at once, 1000 small writes, a 32 MB transfer into a reader that waits (the
  sender must stall), peer close as `connection_failed`, plain OpenSSL peers
  (refused when they offer only a cipher the kernel cannot take), peer
  verification, and no SIGPIPE. With the ring: accept, connect and handshake
  waits went through it (setup completions), handshakes ran on the network
  thread, no socket of the process is in any epoll set, data went through the
  ring, receives were multishot where supported, and a connection paused and
  resumed (or ran out of buffers and resumed).
- `/fdbrpc/AsyncFileKAIO/IoUring`: 1024 concurrent 16 KiB writes, 4 syncs and
  1024 reads through the ring on an O_DIRECT file, with the data compared, the
  ring's per-kind counters checked, the time refresh exercised, and the io-wq
  workers within the cap.
- All pass on kernel 5.15 (fallback paths) and 6.8 (`DEFER_TASKRUN`,
  `NO_SQARRAY`, multishot accept and receive, buffer rings).
- `fdbrpc_transport_bench` gained `--tls_dir`, `--ktls`, `--net_io_uring` and
  CPU-per-request reports.

## Results

RESULTS_PLACEHOLDER

## Reproducing

The harness is in `contrib/perf-bench/k8s` on the perf branch (not on
`next`): `build-image.sh` packages a build into the fdb-kubernetes-monitor
image, `fastapply.sh` switches overlays, `run.sh` runs the mako load.
Overlays: `poc-image.yaml` (this image, knobs off), `poc-all.yaml` (all three
knobs), `poc-ktls.yaml`, `poc-ktls-net.yaml`, `poc-kaio.yaml`.
