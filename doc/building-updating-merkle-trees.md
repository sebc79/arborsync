# Building and Updating Merkle Trees

## Overview

ArborSync uses Merkle trees for efficient change detection and delta synchronization.
The system maintains cryptographic integrity proofs for file hierarchies, enabling selective subtree synchronization with minimal data transfer.

## Merkle Tree Implementation

### Library
- **rs_merkle** v1: Low-level Merkle tree construction with custom hashing
- **Hasher**: BLAKE3 (32-byte outputs)
- **Tree Structure**: Binary Merkle tree with configurable branching

### Leaf Construction

Each file/directory gets one leaf in the Merkle tree:

```rust
// Leaf data format
struct LeafData {
    path_hash: [u8; 32],      // BLAKE3(canonical_path)
    content_hash: [u8; 32],   // BLAKE3(file_bytes) or dir_hash
    size: u64,
    mtime: i128,              // Unix nanoseconds
    mode: u32,
}

// Serialized and hashed to 32-byte leaf
leaf = BLAKE3(serialize(LeafData))
```

The canonical path is the relative path from the _central_ root to the file/directory.

### Directory Handling
- For a directory:
  - content_hash is computed from children hashes (sorted, concatenated, hashed)
  - size is the sum of children sizes
  - mtime and mode are set normally (from filesystem metadata)
- Maintains hierarchical integrity

## Tree Architecture

### Global Tree (Master)
- Single Merkle tree for entire central hierarchy
- Leaves ordered by canonical path (lexicographic)
- Root hash represents complete filesystem state

### Subtree Roots
- Cached in database: `subtree_path → merkle_root`
- Computed on-demand or during updates
- Enables selective synchronization

### Slave Caches
- Per checkout mapping: local Merkle tree
- Mirrors subscribed subtrees from master
- Used for delta computation

## Construction Process

### Initial Build
1. Recursive filesystem scan
2. Collect all file metadata
3. Sort paths lexicographically
4. Build leaves with BLAKE3 hashes
5. Construct Merkle tree bottom-up
6. Store subtree roots in database

### Incremental Updates
1. File change detected (path P)
2. Find leaf index for path P
3. Recompute leaf hash with new metadata
4. Update tree from leaf to root
5. Recompute affected subtree roots
6. Update database cache

## Update Mechanics

### Path to Index Mapping
- Maintain sorted path list
- Binary search for changed path
- Handle insertions/deletions (tree rebuild for major changes)

### Efficient Recomputation
```rust
// rs_merkle API usage
tree.update_leaf(index, new_leaf_hash);
tree.commit();  // Recomputes hashes upward
root = tree.root();
```

### Subtree Root Updates
- Identify affected subtrees (prefix matching)
- Recompute roots for changed subtrees
- Batch database updates

## Delta Synchronization

### Proof Generation
- Client sends current subtree root
- Server generates Merkle proof for differences
- Proof includes changed leaf hashes + path

### Verification
- Client verifies proof against known root
- Identifies exactly which files changed
- Requests deltas only for changed files

### Difference Narrowing for Recovery
When filesystem watchers miss changes, the system uses Merkle proofs for efficient difference detection:

1. **Root Mismatch**: Slave reports local subtree root differing from master's global root
2. **Proof Request**: Master requests Merkle proof from slave for the suspected subtree
3. **Proof Analysis**: Master uses rs_merkle to verify proof and identify changed leaves
4. **Targeted Sync**: Only changed files trigger delta computation and transfer
5. **Integrity Assurance**: Proof verification ensures no spurious changes are propagated

This mechanism provides O(log n) difference detection for large hierarchies without full rescans.

## Performance Optimizations

### Lazy Subtree Computation
- Compute subtree roots on first access
- Cache in database with invalidation
- Avoid full tree recomputation

### Batch Updates
- Group multiple changes in single transaction
- Amortize tree updates across debounced events
- Minimize database writes

### Memory Management
- Streaming leaf construction for large hierarchies
- Tree pruning for unused subtrees
- Configurable tree depth limits

## Database Integration

### Storage Schema
```rust
// Subtree roots cache
fn get_subtree_merkle_root(&self, subtree: &str) -> Result<Option<[u8; 32]>>;
fn put_subtree_merkle_root(&self, subtree: &str, root: [u8; 32]) -> Result<()>;
```

### Transactional Updates
- Merkle updates wrapped in DB transactions
- Atomic tree + metadata updates
- Rollback on failures

## Consistency and Integrity

### Cryptographic Guarantees
- BLAKE3 provides collision resistance
- Tree structure prevents spoofing
- Proofs verify against trusted roots

### Path Ordering
- Consistent lexicographic sorting
- Deterministic tree construction
- Reproducible roots across nodes

## Error Handling

### Tree Corruption
- Detect invalid proofs
- Rebuild tree from filesystem
- Log integrity violations

### Path Conflicts
- Handle duplicate paths (impossible in filesystem)
- Validate path canonicalization
- Reject malformed updates

### Memory Limits
- Stream processing for large trees
- Configurable batch sizes
- Graceful degradation on OOM

## Integration Points

### Change Detection
- Triggers Merkle recomputation on file events
- Provides integrity checking for rescans

### Transport Layer
- Merkle proofs sent over QUIC streams
- Efficient binary serialization

### Delta Engine
- Uses Merkle differences for selective sync
- Minimizes data transfer via proofs