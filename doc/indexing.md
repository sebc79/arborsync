# Indexing System and Database Backend

## Overview

ArborSync's indexing system provides persistent storage for filesystem metadata, Merkle tree state, and synchronization tracking. The system uses an embedded database with transactional guarantees and efficient querying for subtree operations.

## Database Backend Selection

### Primary Backend: redb v2
- **Architecture**: Pure Rust, B+tree based, MVCC concurrency
- **Features**: Single-file database, crash-safe, ACID transactions
- **Performance**: Excellent for ArborSync's workload (frequent small writes, range queries)
- **Stability**: 1.0+ since 2023, active maintenance

### Abstraction Layer
- **Trait**: `Storage` in `core/src/storage.rs`
- **Purpose**: Backend-agnostic interface for switchability
- **Implementations**: `RedbStorage` (current), `SledStorage` (stub)

## Storage Trait Interface

```rust
pub trait Storage: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync;

    fn open(path: &std::path::Path) -> Result<Self, Self::Error>;

    // Core metadata operations (slaves: checkout_id prefixes keys)
    fn get_file_metadata(&self, checkout_id: Option<&str>, path: &str) -> Result<Option<FileMetadata>, Self::Error>;
    fn put_file_metadata(&self, checkout_id: Option<&str>, path: &str, meta: FileMetadata) -> Result<(), Self::Error>;

    // Merkle tree state (slaves: checkout_id prefixes keys)
    fn get_subtree_merkle_root(&self, checkout_id: Option<&str>, subtree: &str) -> Result<Option<[u8; 32]>, Self::Error>;
    fn put_subtree_merkle_root(&self, checkout_id: Option<&str>, subtree: &str, root: [u8; 32]) -> Result<(), Self::Error>;

    // Advanced operations
    fn transaction<F, R>(&self, f: F) -> Result<R, Self::Error>
    where F: FnOnce(&mut Transaction) -> Result<R, Self::Error>;

    // Range queries for subtree walks (scoped to checkout_id)
    fn get_subtree_files(&self, checkout_id: Option<&str>, prefix: &str) -> Result<Vec<(String, FileMetadata)>, Self::Error>;
    fn delete_subtree_metadata(&self, checkout_id: Option<&str>, prefix: &str) -> Result<(), Self::Error>;
}
```

## Database Schema

### Tables
1. **file_metadata**: Path → FileMetadata
    - Primary key: UTF-8 canonical path (slaves prefix with "checkout_id:")
    - Value: Bincode-serialized FileMetadata

2. **subtree_roots**: Subtree path → Merkle root
    - Primary key: Subtree prefix string (slaves prefix with "checkout_id:")
    - Value: 32-byte BLAKE3 hash

3. **slave_subscriptions**: Slave ID → Subscription mappings
    - Tracks active slave connections and checkouts

### Indexing Strategy
- **Primary Indexes**: B+tree on path strings for range queries
- **Prefix Queries**: Efficient subtree enumeration
- **Secondary Indexes**: Path to slave mappings for notifications

## Transaction Model

### Atomic Operations
- File metadata updates bundled with Merkle recomputation
- Subtree root updates in same transaction
- Rollback on any failure

### Concurrency
- MVCC allows concurrent reads during writes
- Write transactions serialize access
- Read-only snapshots for long-running operations

## Query Patterns

### Subtree Enumeration
```rust
// Master: Find all files under /central/src/
let files = storage.get_subtree_files(None, "/central/src/");
// Slave checkout 1: Find all files under /src/ in checkout
let files = storage.get_subtree_files(Some("1"), "/src/");
// Returns sorted list of (path, metadata) pairs scoped to checkout
```

### Prefix Matching
- Range queries on path keys (prefixed for slaves)
- Efficient for hierarchical operations per checkout
- Supports arbitrary subtree depths

### Change Tracking
- Transaction logs for incremental updates
- Avoids full rescans after interruptions

## Performance Characteristics

### Write Performance
- Optimized for frequent small updates (watcher events)
- B+tree provides O(log n) insertions
- Batch operations for bulk metadata collection

### Read Performance
- Fast range queries for subtree operations
- Memory-mapped for read-heavy workloads
- Concurrent readers don't block writers

### Storage Efficiency
- Single file database (~10-20% overhead)
- Compression for large metadata sets
- Automatic compaction and cleanup

## Crash Safety and Recovery

### ACID Guarantees
- Atomic commits with write-ahead logging
- MVCC prevents partial updates
- Automatic recovery on startup

### Consistency Checks
- Validate Merkle roots against stored metadata
- Rebuild corrupted indexes from filesystem
- Log integrity violations

## Configuration Options

### Master Configuration
```toml
db_path = "/var/lib/arborsync/index.redb"
db_backend = "redb"  # redb | sled
```

### Slave Configuration
```toml
db_path = "/var/cache/arborsync/cache.redb"
db_backend = "redb"
```

## Backend Switchability

### Migration Support
- Export/import functionality for data migration
- Schema compatibility checks
- Zero-downtime backend switches

### Future Backends
- **sled**: Alternative embedded database
- **PostgreSQL/MySQL**: For distributed deployments
- **Custom**: Application-specific optimizations

## Monitoring and Maintenance

### Statistics
- Database size, operation counts
- Query performance metrics
- Background compaction status

### Maintenance Tasks
- Periodic compaction runs
- Index rebuilds for optimization
- Backup and restore procedures

## Error Handling

### Connection Failures
- Automatic reconnection with retries
- Graceful degradation to read-only mode
- Detailed error logging

### Corruption Detection
- Checksum verification on reads
- Automatic repair from filesystem state
- Alert generation for manual intervention

## Integration Points

### Filesystem Watching
- Triggers metadata updates in transactions
- Provides persistence across restarts

### Merkle Trees
- Caches subtree roots for efficiency
- Ensures tree state matches filesystem

### Transport Layer
- Supplies metadata for delta computations
- Tracks synchronization state per slave