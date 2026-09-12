# Collecting Metadata

## Overview

ArborSync collects and maintains comprehensive metadata for all files in the filesystem hierarchy. This metadata is crucial for change detection, conflict resolution, and efficient synchronization. The system preserves essential Unix attributes while ignoring user/group ownership.

## Metadata Structure

### FileMetadata

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileMetadata {
    pub size: u64,                    // File size in bytes
    pub mtime: i128,                  // Modification time (Unix nanoseconds)
    pub mode: u32,                    // Unix mode bits (permissions + file type)
    pub content_hash: [u8; 32],       // BLAKE3 hash of file contents
}
```

### Path Representation
- Canonical absolute paths within the hierarchy
- UTF-8 encoded strings
- Consistent path separators across platforms

## Collection Process

### Initial Scan
1. Recursive filesystem walk starting from root
2. For each file/directory:
   - Read file attributes using `std::fs::metadata`
   - Compute BLAKE3 content hash
   - Store in database with canonical path

### Incremental Updates
1. Watcher detects file change
2. Read updated metadata immediately
3. Recompute content hash if content changed
4. Update database record

### Content Hashing
- **Algorithm**: BLAKE3 (32-byte output)
- **Input**: Raw file bytes
- **Performance**: Streaming hash for large files
- **Purpose**: Change detection, delta computation

## Metadata Sources

### Primary Attributes (std::fs::Metadata)
- `len()` → size
- `modified()` → mtime (converted to Unix nanoseconds)
- `permissions().mode()` → mode (filtered to Unix permissions)

### Computed Attributes
- Content hash via BLAKE3 hasher
- Path hash for Merkle leaf construction

## Unix Mode Handling

### Preserved Bits
- File type: regular file, directory, symlink, etc.
- Permissions: r/w/x for user/group/other
- Special bits: setuid, setgid, sticky

### Ignored Bits
- UID/GID ownership (always ignored per spec)

### Mode Application
- On slaves: `std::fs::set_permissions` after file write
- Preserves executable bits, directory permissions
- Handles special file types appropriately

## Mtime Precision

### Storage Format
- i128 for Unix nanoseconds (nanosecond precision)
- Compatible with `time` crate v0.3

### Comparison Logic
- Used for conflict resolution ("latest mtime wins")
- NTP synchronization assumed for accurate comparison
- Ties handled via conflict files

## Database Integration

### Storage Trait Methods
```rust
fn get_file_metadata(&self, path: &str) -> Result<Option<FileMetadata>>;
fn put_file_metadata(&self, path: &str, meta: FileMetadata) -> Result<()>;
```

### Indexing
- Primary key: canonical path string
- Secondary indexes: subtree prefix queries
- Transactional updates during scans

## Performance Optimizations

### Lazy Content Hashing
- Only hash when content actually changes
- Use size + mtime as cheap change indicator
- Full hash only when needed for deltas

### Batch Operations
- Collect metadata in batches during rescans
- Transactional database updates
- Minimize I/O during watcher events

## Error Handling

### Missing Files
- Handle ENOENT during metadata read
- Mark as deleted in database
- Clean up orphaned records

### Permission Errors
- Log access denied but continue scan
- Partial hierarchy support
- Graceful degradation

### Corrupt Files
- Skip unreadable files
- Log errors without failing entire scan
- Retry logic for transient issues

## Consistency Guarantees

### Atomic Updates
- Metadata + content hash updated together
- Database transactions ensure consistency
- Rollback on partial failures

### Path Canonicalization
- All paths converted to canonical form
- Symlink resolution handled consistently
- Avoid duplicate entries for same file