**Full Specification: ArborSync – Selective Subtree File Synchronization Daemon**

**Version:** 1.0 (April 2026)  

**Purpose:** Provide a complete, LLM/human-implementable blueprint for a lightweight, bidirectional, central-master 
file sync tool that satisfies these requirements:
- one central hierarchy,
- arbitrary subtree checkouts to arbitrary local paths on slaves,
- Merkle-based indexing with custom metadata (mode bits, mtime, size),
- efficient rsync-style block deltas,
- FS watching + rescans,
- PSK-encrypted transport,
- Unix perms preserved (no UID/GID),
- simple conflict handling.

**Provisional Project Name:** *ArborSync* (crate: `arborsync`)

Single binary: `arborsync`

Usage:
- `arborsync master /central`
- `arborsync slave config.yaml`

### 1. Core Requirements (Non-Negotiable)
- Master holds **one** full filesystem hierarchy (`/central`).
- Slaves declare a list of mappings: `central_subtree_path → local_path` (any number, any depth; same subtree can map multiple times on one slave).
- Full-replica backup = one slave mapping root `/` → `/backup/central`.
- Bidirectional propagation (changes anywhere → everywhere interested).
- Efficient: Merkle-tree indexing (per-subtree roots), `notify` watching + periodic rescans, `copia` block-level deltas.
- Transport:
  - Encryption: 32-byte preshared symmetric key (PSK) only.
    - Noise Protocol Framework with preshared symmetric key (PSK) via `snow`.
- Metadata: Preserve Unix mode bits + mtime + size; ignore UID/GID.
- Conflicts: Low-concurrency assumption → “latest mtime wins” (clocks must be NTP-synced within ~1s); optional timestamped `.conflict-YYYYMMDD-HHMMSS.ext` copy on tie.
- Daemonized, minimal resource use, Rust-only (no external processes except optional CLI tools).

### 2. High-Level Architecture
- **Master Daemon** (`arborsync master`):
  - Single process watching entire hierarchy.
  - Maintains global index (DB) of all files + per-subtree Merkle roots.
  - Listens on QUIC endpoint.
  - Accepts slave subscriptions (checkout list).
  - On change: recompute affected subtree Merkle, push deltas only to interested slaves.
- **Slave Daemon** (`arborsync slave`):
  - Runs on any host (including backup server).
  - Reads local config file with checkout mappings.
  - Connects only to master via QUIC; subscribes to mapped subtrees.
  - Local changes → push to master; master pushes → apply locally.
  - Multiple local copies of same central file are allowed (duplicate mappings).
- **Shared Library** (`arborsync-core`): Common types, protocol, Merkle helpers, DB abstraction, delta engine, QUIC 
  transport layer.
- **Communication:**
  - QUIC connections with Noise handshake via `quinn-hyphae`.
  - Native bidirectional streams for all messages (no manual framing).
- **Persistence:** Master uses embedded DB for index. Slaves may keep lightweight local cache (same DB backend).

**Data Flow (example)**
1. Slave A checks out `/central/src/project1` → `/opt/app1/src`.
2. Slave B checks out `/central/` → `/backup/central` (full replica).
3. File `/central/src/project1/foo.rs` changes on Slave A → Slave A computes delta → sends to master → master 
   updates index → pushes delta over separate QUIC streams to Slave B (and any other interested slaves).

### 3. Crate Dependencies (Cargo.toml skeleton)
```toml
[dependencies]
arborsync-core = { path = "core" }  # workspace

# Core
tokio = { version = "1", features = ["full"] }
quinn = "0.11"                    # QUIC transport
quinn-hyphae = "0.1"              # Noise handshake over Quinn (full PSK support)
copia = "0.3"                     # rsync delta-transfer (embeddable)
notify = "8"
notify-debouncer-mini = "0.5"
rs_merkle = "1"
redb = "2"                     # primary (see §5)
bincode = "2"
serde = { version = "1", features = ["derive"] }
blake3 = "1"                   # for Merkle leaves + content hashing
time = { version = "0.3", features = ["serde"] }  # for mtime
clap = { version = "4", features = ["derive"] }
log = "0.4"
env_logger = "0.11"
```

Note on `quinn-hyphae`: Uses the crate `quinn-hyphae` (or the maintained fork `asport-quinn-hyphae` which re-exports as `quinn_hyphae` for API compatibility). It provides full Noise pattern control + PSK injection.

**Workspace structure**
```
arborsync/
├── Cargo.toml
├── core/          # shared types, protocol, merkle, storage trait
├── master/
├── slave/
└── cli/           # optional admin CLI
```

### 4. Database Backend Arbitration & Abstraction
**Recommendation: redb as primary/default backend** (as of April 2026).

**Rationale (research summary):**
- redb: Stable 1.0+ since 2023, actively maintained, pure Rust, B+tree, MVCC, crash-safe, single-file, excellent benchmarks (often beats sled/lmdb/rocksdb on individual writes, random reads, and bulk loads for our workload). Typed tables, multi-table support, transactions out-of-the-box. No on-disk format instability.
- sled: Still in prolonged beta (0.x/alpha releases), on-disk format changes historically, community notes migration risks and slower stabilization. Good read-heavy performance in older comparisons but redb is the clear modern choice for new projects.
- redb fits perfectly: low-to-medium cardinality index (files in a hierarchy), frequent small writes from watcher, concurrent reads from network.

**Abstraction (mandatory for switchability):**
In `core/src/storage.rs` define:
```rust
pub trait Storage: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync;
    fn open(path: &std::path::Path) -> Result<Self, Self::Error>;
    // Table definitions via associated types or generic methods
    fn get_file_metadata(&self, path: &str) -> Result<Option<FileMetadata>, Self::Error>;
    fn put_file_metadata(&self, path: &str, meta: FileMetadata) -> Result<(), Self::Error>;
    fn get_subtree_merkle_root(&self, subtree: &str) -> Result<Option<MerkleRoot>, Self::Error>;
    fn put_subtree_merkle_root(&self, subtree: &str, root: MerkleRoot) -> Result<(), Self::Error>;
    // Transactions, range queries for subtree walks, etc.
    fn transaction<F, R>(&self, f: F) -> Result<R, Self::Error> where F: FnOnce(&mut Transaction) -> Result<R, Self::Error>;
    // ... more methods as needed
}

pub struct RedbStorage { /* ... */ }  // implements Storage
// pub struct SledStorage { /* ... */ } // stub for future
```

Config flag: `--db-backend redb|sled` (default: redb). Master and slaves use the same trait. This adds ~100 lines but future-proofs everything.

### 5. Merkle Tree Design (rs_merkle)
- **Why rs_merkle**: Low-level control (vs high-level `merkle_hash`). Supports custom `Hasher`, transactional updates, proofs, rollback.
- **Leaf format** (custom): Each leaf = BLAKE3 hash of serialized tuple:
  ```
  leaf_data = path_hash (blake3 of canonical path) || content_hash (blake3 of file bytes) || size || mtime (unix ns) || mode (u32)
  ```
  → `rs_merkle::MerkleTree<Blake3Hasher>` where you pre-compute 32-byte leaf hashes.
- Master maintains one global Merkle tree (or one per top-level subtree) + cached subtree roots in DB.
- On change (watcher or push): recompute only affected path upward (efficient with rs_merkle’s API).
- Slaves keep local Merkle cache per checkout mapping.
- Delta request: “My root for subtree X is H1; your root is H2 → give proof + differing blocks via copia”.

### 6. Change Detection
- `notify-debouncer-mini`: Recursive watch on master (full tree) and on each slave (local paths).
- Debounce 200–500 ms.
- Background rescan every 60 s (full Merkle walk on changed subtrees only) for robustness (missed events, NFS, etc.).
- On event: update DB metadata → recompute Merkle → notify subscribers.

### 7. Delta Transfer (copia)
- Use `copia::Sync` (or `SyncBuilder`) directly in memory/streams.
- Master/slave: When sending a file:
  1. Recipient sends signature of its basis file.
  2. Sender computes delta → transmit only changed blocks.
- Fallback: whole-file if < 4 KB or first sync.

### 8. Network / Transport Layer (NEW – QUIC + quinn-hyphae)
**Primary Transport:** QUIC (IETF RFC 9000) via `quinn` + **Noise handshake via `quinn-hyphae`**.  
**Why this combination:**
- Native stream multiplexing (each delta, Merkle update, subscription, heartbeat gets its own QUIC stream → zero head-of-line blocking).
- 0-RTT resumption with PSK.
- Built-in connection migration, better NAT traversal, modern congestion control.
- Exact PSK security model you requested (no certificates).

**Handshake Details:**
- Noise pattern: `Noise_XX_25519_ChaChaPoly_BLAKE2s` (or any supported by hyphae; XX recommended for mutual auth with PSK).
- PSK injected as 32-byte preshared key (config field `psk`).
- ALPN: `arborsync-v1` for version negotiation.
- Post-handshake: All protocol messages flow over QUIC bidirectional streams (no length-prefixing required; Quinn handles framing/reliability).

**Configuration Additions**
```toml
# master.toml / slave.toml
transport = "quic"                  # default; "tcp" optional fallback (future)
listen_addr = "0.0.0.0:8443"        # UDP port for QUIC
psk = "hex:32bytekeyhere..."        # enforced 32 bytes
quic_max_concurrent_streams = 256
quic_idle_timeout_ms = 300000
quic_initial_mtu = 1200
```

**Master Implementation:**
- `quinn::Endpoint::server(...)` + `quinn_hyphae::NoiseConfig` with PSK.
- Accept connections → run hyphae Noise handshake → spawn per-connection handler.

**Slave Implementation:**
- `quinn::Endpoint::client(...)` → `connect` → hyphae handshake with PSK.
- Open control stream for subscriptions; open new streams on-demand for deltas.

**Protocol Messages (unchanged enum):**
```rust
#[derive(Serialize, Deserialize)]
pub enum ProtocolMessage {
    Subscribe { mappings: Vec<(String, String)> },  // central → local
    MerkleUpdate { subtree: String, new_root: [u8; 32], proof: Vec<u8> },
    DeltaRequest { file_path: String, signature: Vec<u8> },
    DeltaResponse { delta: Vec<u8> },
    FileMetadataPush { path: String, meta: FileMetadata },
    // ...
}
```
Messages are sent/received directly on QUIC streams using `bincode`.

### 9. Configuration
**Master** (`/etc/arborsync/master.toml` or `--config`):
```toml
central_root = "/central"
listen_addr = "0.0.0.0:8443"
psk = "hex:32bytekeyhere..."
db_path = "/var/lib/arborsync/index.redb"
log_level = "info"
```

**Slave** (`~/.config/arborsync/slave.toml` or per-host):
```toml
master_addr = "master.example.com:8443"
psk = "hex:32bytekeyhere..."
db_path = "/var/lib/arborsync/cache.redb"
checkouts = [
    { central = "/src", local = "/opt/projects/src" },
    { central = "/", local = "/backup/central" },
]
```

### 10. Conflict & Permission Handling
- On receive: compare local mtime vs incoming.
- If incoming newer → overwrite + set mode.
- If tie → create `.conflict-YYYYMMDD-HHMMSS.ext` (or configurable policy).
- `std::fs::set_permissions` (mode only) after write.
- mtime preserved via `filetime` crate or `std::os::unix::fs::FileExt`.

### 11. Error Handling, Logging, Security
- All ops in `anyhow`/`thiserror` chains.
- Structured logging (JSON option).
- QUIC-specific: `quinn::ConnectionError` handling, automatic reconnect with exponential backoff (Quinn handles most of it).
- PSK must be 32 bytes (enforced); rotate via config reload.
- No root required (run as dedicated user).
- Rate limiting / DoS protection on master (tokio).

### 12. Implementation Roadmap (LLM-friendly chunks)
1. Core crate: Storage trait + redb impl, FileMetadata struct, Merkle helpers, Transport trait (with `QuicHyphaeTransport` impl).
2. Master: watcher + index updater + QUIC endpoint + hyphae handshake.
3. Slave: config parser + QUIC connector + local applicator.
4. Protocol messages over QUIC streams.
5. Delta round-trip with copia.
6. Full bidirectional tests (unit + integration with temp dirs).
7. CLI flags, systemd units, packaging.

Transport Modularity: Define trait Transport in core so you can swap QUIC ↔ TCP later with minimal changes.

**Next Steps for Implementation**
- Start with `cargo new arborsync --bin` + workspace.
- Implement `core` first (Storage + Merkle).
- I can provide skeleton code, exact `Cargo.toml`, or step-by-step module implementations on request.

This specification is self-contained, leverages the requested crates exactly, and is deliberately modular for iterative LLM coding. It will produce a tiny, static, high-performance binary pair that matches your vision with zero bloat. Let me know which part to expand first (e.g., protocol enum definition, Storage trait full impl, or a minimal PoC main.rs).