# QUIC Transport

Normative source: `spec.md` §11 and §12.

## Stack

- QUIC (RFC 9000) via `quinn` 0.11.
- Noise handshake via `quinn-hyphae` 0.1: pattern `Noise_XX_25519_ChaChaPoly_BLAKE2s`.
- Persisted X25519 **static keys** on both sides. This is what XX actually authenticates.

**Not in this crate, not in v1:** Noise PSK modifier, a 32-byte cluster `psk` field, QUIC 0-RTT, ALPN (hyphae does not implement ALPN; version is the first Noise payload / preamble: ASCII `arborsync-v1`). Mutating messages are sent only after the handshake completes.

## Endpoints

**Master:** `quinn::Endpoint` + hyphae `HandshakeBuilder` with the master static key. Bind UDP `listen_addr` (default `0.0.0.0:8443`). One task per accepted connection.

**Slave:** client endpoint, connect to `master_addr`. Present the slave static key.

After XX:

- Slave disconnects if the peer static key ∉ `master_public_keys`.
- Master looks up the peer static key in `[[slaves]]`. Unknown → disconnect (rate-limited per source IP). Known → that row’s `id` is the only legal `slave_id` on `Subscribe`.

## Streams

| Stream | Use |
|---|---|
| Control (one, slave-opened) | Framed `Envelope` messages |
| Bulk (on demand, one transfer) | Framed `BulkHeader` + exactly `size` raw bytes |

Quinn gives **byte streams**. “No framing needed” was wrong. Control frame:

```
u32be length || bincode(Envelope { version: u16 = 1, msg: ProtocolMessage })
```

Maximum control frame: 1 MiB. Larger → disconnect (file bodies do not belong here). Bulk header uses the same frame prefix; the body is raw and not length-prefixed again (`size` in the header is authoritative).

One message per bulk stream. Close the stream after the body. Control stream stays open for the session.

Keep-alive: QUIC idle timeout + quinn’s native ping. No `Heartbeat` message.

## When implementing the control reader

Both items below land with QUIC. Neither is done in `core` today.

- Decode `Envelope.version` before `ProtocolMessage`. `decode_control` decodes the whole envelope in one step, so bincode rejects an unknown v2 variant index before the version check runs, and the caller sees `FrameError::Bincode` instead of `FrameError::UnsupportedVersion`. Read the version first, then decode the body.
- Do not share one unversioned `bincode_config` between control frames and on-disk `FileMetadata`. The wire format and the stored format have separate lifetimes, so give each its own config, or add a schema version table.

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

`DirListResponse` in v1: when the slave requests a path, the master answers from the global index. When the walk needs the slave’s view, the slave already has it locally and does not need the master to ask — 3-way uses the slave’s index + master’s listing.

## Reconnect

Exponential backoff (1s, 2s, 4s, … cap 60s). Full handshake (no 0-RTT). `Subscribe` + reconcile. Do not replay in-flight announces from the previous connection.

A new successful session for the same `slave_id` replaces the old one (master drops the previous connection).

## Limits

```toml
quic_max_concurrent_streams = 256
quic_idle_timeout_ms = 300000
quic_initial_mtu = 1200
max_connections = 100
max_connection_attempts_per_minute = 60
```

Idle timeout closes the QUIC connection; the slave reconnects. Unknown-key and broken-frame disconnects count toward the per-IP attempt limiter.

## Security

- Mutual static-key authentication (XX).
- Prefix ACL after `Subscribe` and on every slave-originated mutate (`spec.md` §4).
- No shared secret that makes slaves interchangeable.
- Forward secrecy: XX ephemeral DH. Session keys are not the static keys.
- Do not send announces, deletes, or bulk bodies in any 0-RTT space if a future hyphae release grows it.
- Key material: files `0600`, never logged, never put in the environment.

Rotation procedures: `spec.md` §4.

## Errors

- Handshake / pin / unknown key: disconnect, log at `warn` without printing keys.
- Protocol version ≠ 1 or preamble ≠ `arborsync-v1`: disconnect.
- Frame length > cap or bincode fail: disconnect (do not try to resync a corrupted control stream).
- `Error` on a still-valid session: log; the affected path is retried at the next reconcile.

## Tests / in-memory

A `Transport` trait that can be an in-memory pair of control+bulk channels is allowed for unit tests. v1 production path is QUIC only. No TCP fallback.

## Struck from earlier drafts

- `psk = "hex:…"`, `transport = "quic"|"tcp"`.
- 0-RTT resumption “with forward secrecy and replay protection.”
- ALPN `arborsync-v1` as a QUIC-TLS feature.
- Application heartbeats.
- “Seamless, no data loss during brief disconnects” via a push queue — replace with reconcile.
