# QUIC Transport

Normative source: `spec.md` §11 and §12.

## Stack

- QUIC (RFC 9000) via `quinn` 0.11.
- Noise handshake via `quinn-hyphae` 0.1: pattern `Noise_XX_25519_ChaChaPoly_BLAKE2s`.
- Persisted X25519 **static keys** on both sides. This is what XX actually authenticates.

**Not in this crate, not in v1:** Noise PSK modifier, a 32-byte cluster `psk` field, QUIC 0-RTT, ALPN (hyphae does not implement ALPN; version is the first Noise payload / preamble: ASCII `arborsync-v1`). Mutating messages are sent only after the handshake completes.

Workspace pin is `quinn-hyphae = "0.1.0-beta.0"`. Quinn is `default-features = false`, `runtime-tokio` only.

## Endpoints

**Master:** `quinn::Endpoint` + hyphae `HandshakeBuilder` with the master static key. Bind UDP `listen_addr` (default `0.0.0.0:8443`). One task per accepted connection.

**Slave:** client endpoint, connect to `master_addr`. Present the slave static key.

After XX:

- Slave disconnects if the peer static key is not in `master_public_keys`.
- Master looks up the peer static key in `[[slaves]]`. `AttemptLimiter::limited` drops the accept before XX. After XX, unknown key records `allow` and disconnects. Known key: that row’s `id` is the only legal `slave_id` on `Subscribe`. `slave_id` mismatch is hangup (the binary also `allow`s).

Preamble `arborsync-v1` is the hyphae Noise prologue (`with_prologue`), not an application frame after XX.

## Streams

| Stream | Use |
|---|---|
| Control (one, slave-opened) | Framed `Envelope` messages |
| Bulk (on demand, one transfer) | Framed `BulkHeader` + chunks of at most 16 MiB that concatenate to `size` |
| Datagram (unreliable, slave to master) | One 25-byte `Gauge`: the slave's own `bottleneck=` verdict. Never a `ProtocolMessage`. A peer without datagram support drops to `hint=absent` and the session is unaffected. |

Quinn gives **byte streams**. Control frame:

```
u32be length || bincode(u16 version) || bincode(ProtocolMessage)
```

That is the field order of `Envelope`. Maximum control frame: 1 MiB. Larger means disconnect (file bodies do not belong here). If `encode_control` of a `SignatureRequest` would overflow that cap, the signature is omitted and the transfer falls back to `Whole` (`spec.md` §9). A `DirListResponse` that would overflow is split. The master sends the longest prefix that still encodes under `MAX_DIR_LIST_PAYLOAD` (`MAX_CONTROL_FRAME / 4`) and sets `more`. One child that exceeds that still goes if `encode_control` succeeds. The slave continues with `DirListRequest.after`. Bulk header uses the same length prefix. The body follows as chunks (`u32be len || bytes`), each at most 16 MiB. `size` in the header is the concatenated length. A longer chunk is refused before allocation. The logical body may exceed 1 GiB. The QUIC apply path writes each chunk to `.arborsync-tmp` and hashes the file. `read_bulk` still concatenates for the in-memory transport.

One message per bulk stream. Close the stream after the body. Control stream stays open for the session.

The master and slave binaries wait on `accept_uni` inside `select!`, then read the body after that arm wins. Putting `accept_bulk` (accept plus the body read) in the same `select!` as the 50 ms outbox tick dropped the `RecvStream` mid-body. Quinn then sent `STOP_SENDING` error 0, which the slave logs as `stream: sending stopped by peer: error 0`.

The slave session does not `write_all` a `Reply::Send` batch on the same task that reads control. A writer task owns the control `SendStream`. Leftover `SignatureRequest`s read the file on `spawn_blocking` and then `write_bulk` on a spawned task. The session keeps `read_control` and interval `status` live during a large leftover send. A read that ran inside `handle` used to mute `status 5s` for the whole file. Up to four small bulks can be in flight. One file larger than 16 MiB uses the large lane. Further asks stay parked. Leftover dir-list pages wait while asks are parked or in flight. Rescan hashing still steps so a leftover send does not stall the rest of the tree. A second ask for the same checkout, path, and `want_hash` is dropped so a leftover walk plus a watcher flood does not send the same file twice. The second send used to land as `unknown_transfer` after the first apply removed pending. `apply_bulk` accepts that late send when the live file already has `want_hash`. Outbound `SignatureRequest`s (a pull after `CasReject`) use a second writer lane that the control task prefers over leftover `FileAnnounce`s. A single FIFO buried those pulls behind the walk, so slave `pending` sat with `apply_ok=0` and `bulk_in=0` while `dir_list` and `bulk_out` still moved. The master session writes control on a writer task. `dispatch_master` and `flush_outbox` enqueue onto that task. `write_control` no longer sits in the same `select!` as `accept_uni`. A slave that paused `read_control` used to block the master write and freeze `pending` with `bulk_in=0`.

`read_control` is also a `select!` arm. Quinn `read_exact` is not cancel-safe. A ready `accept_uni` or tick drops that future and discards bytes already copied into the caller's buffer. The next read then treats leftover payload as a length prefix. The master logs `incoming control length ... exceeds 1 MiB (header 70 69 63 6f)` when those bytes are ASCII `pico`. `ControlReader` stores copied bytes in `pending` and fills with cancel-safe `read`.

Keep-alive: Quinn idle timeout stays at the RFC 9308 default of 30 s. `listen` and `client_endpoint` set `keep_alive_interval` to 10 s so a quiet control stream does not hit that timeout. No `Heartbeat` message.

## Control reader

`decode_control` reads the version first, then the message. Version other than 1 is `FrameError::UnsupportedVersion`, including a future variant index under version 2.

Wire frames use `wire_bincode_config`. On-disk `FileMetadata` uses `meta_bincode_config` plus `u16le META_SCHEMA_VERSION` (`indexing.md`). Do not share one unversioned blob between those two lives.

`read_bulk` decodes the header with `bincode::config::standard()` directly. If `wire_bincode_config` ever diverges from `standard()`, streaming bulk reads must follow `decode_bulk`.

## Message catalog

Exact types: `spec.md` §11.

Direction (normative):

| Message | Who sends |
|---|---|
| `Subscribe` | slave |
| `SubscribeAck` / `SubscribeReject` | master |
| `RootReport` | slave |
| `RootAck` | master |
| `DirListRequest` | slave (walk); master only if a future extension needs it — v1 slave-driven |
| `DirListResponse` | the peer who has that directory listing (master for pull, slave for compare) |
| `FileAnnounce` / `Delete` / `Rename` | either side |
| `CasAccept` / `CasReject` | master |
| `SignatureRequest` | the side that needs bytes (usually the applying slave; master when pulling a slave create) |
| bulk `Whole` / `Delta` | the side that has `want_hash` |
| `Error` / `Disconnect` | either |

`Rename` is handled on both `handle` methods. Same-window same-checkout rename is `fs::rename` plus index update. Unpaired or cross-checkout rename stays Delete plus Create.

`DirListResponse` in v1: when the slave requests a path, the master answers from the global index. When the walk needs the slave’s view, the slave already has it locally and does not need the master to ask. 3-way uses the slave’s index plus the master’s listing. A `DirListRequest` on a file path returns `FileAnnounce`.

## Reconnect

Exponential backoff (1 s, 2 s, 4 s, cap 60 s). Full handshake (no 0-RTT). `Subscribe` plus reconcile. Do not replay in-flight announces from the previous connection.

A new successful session for the same `slave_id` replaces the old one. The binary `close`s the previous `Connection` and signals the old accept task. Roster interest switches when the new session sends `Subscribe`.

On `SubscribeReject` the slave records `denied_centrals` and omits those prefixes from the next `subscribe()`. It stays on the connection when any checkout remains. All denied is hangup.

## Limits

```toml
max_connections = 100
max_connection_attempts_per_minute = 60
```

`quic_max_concurrent_streams`, `quic_idle_timeout_ms`, `quic_initial_mtu`, `reconnect_initial_ms`, and `reconnect_max_ms` appear in older drafts. They are not parsed. Idle timeout and stream limits stay at Quinn defaults. Keep-alive is the 10 s ping above. Reconnect backoff is hardcoded (1 s, doubling, cap 60 s).

Idle timeout closes the QUIC connection. The slave reconnects. A session that ends on a read or write error still drops its roster entry and calls `Master::disconnect`, so the slot does not count toward `max_connections` and fan-out stops. Unknown-key disconnects increment `AttemptLimiter` after XX and still `close`. A limited IP is ignored before XX. Broken frames do not increment the limiter. Prefix deny stays `SubscribeReject`. `slave_id` mismatch is hangup.

## Security

- Mutual static-key authentication (XX).
- Prefix ACL after `Subscribe` and on every slave-originated mutate (`spec.md` §4).
- No shared secret that makes slaves interchangeable.
- Forward secrecy: XX ephemeral DH. Session keys are not the static keys.
- Do not send announces, deletes, or bulk bodies in any 0-RTT space if a future hyphae release grows it.
- Key material: files `0600`, never logged, never put in the environment.

Rotation procedures: `spec.md` §4.

## Errors

- Handshake, pin, or unknown key: disconnect, log at `warn` without printing keys.
- Protocol version other than 1 or prologue other than `arborsync-v1`: disconnect.
- Frame length over the cap or bincode fail: disconnect (do not try to resync a corrupted control stream).
- `Error` on a still-valid session: specified as log, then retry the path at the next reconcile. As built, inbound `Error` is recorded and not echoed; unmatched variants still reply `unsupported`.

## Tests / in-memory

`Transport` is a live session after handshake. `impl Transport for quinn::Connection` is the QUIC path. The master and slave binaries and `core/tests/transport.rs` call the trait. `MemoryTransport::pair` is the in-memory test impl and has no datagrams. `pair_with_datagrams` is the opt-in that carries gauges. Unit tests still call `handle` for CAS and reconcile. The v1 production path is QUIC only.

## Struck from earlier drafts

- `psk = "hex:…"`, `transport = "quic"|"tcp"`.
- 0-RTT resumption “with forward secrecy and replay protection.”
- ALPN `arborsync-v1` as a QUIC-TLS feature.
- Application heartbeats.
- “Seamless, no data loss during brief disconnects” via a push queue. Replace with reconcile.
