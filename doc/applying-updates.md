# Applying Updates on Slaves

## Overview

Slave nodes receive filesystem updates from the master and apply them to their local checkouts. The update application process handles conflict resolution, metadata preservation, and maintains filesystem consistency while ensuring bidirectional synchronization.

## Update Reception

### Message Types
Updates arrive via QUIC streams as protocol messages:
- `MerkleUpdate`: Notifies of subtree changes with proof
- `FileMetadataPush`: Contains updated file metadata
- `DeltaResponse`: Provides file content deltas

### Processing Flow
1. **Receive Notification**: Merkle update indicates subtree change
2. **Verify Proof**: Validate change against local Merkle root
3. **Request Deltas**: Ask for changed file contents
4. **Apply Changes**: Update local files with conflict resolution

## Conflict Resolution

### Mtime Comparison
- **Latest Wins**: Compare local vs incoming modification times
- **NTP Assumption**: Clocks synchronized within ~1 second
- **Resolution Logic**:
  - Incoming mtime > local mtime → Apply update
  - Incoming mtime < local mtime → Ignore (local is newer)
  - Incoming mtime = local mtime → Conflict handling

### Conflict Handling
When mtimes are equal (within tolerance):
1. Create conflict copy: `.conflict-YYYYMMDD-HHMMSS.ext`
2. Apply incoming update to main file
3. Log conflict for user resolution

Example:
```
original: file.txt (mtime: 1234567890)
conflict: file.conflict-20231201-143022.txt
updated:  file.txt (new content, same mtime)
```

## File Application Process

### Content Updates
1. **Receive Delta**: Binary diff from master
2. **Apply to Local**: Use copia to patch local file
3. **Verify Integrity**: Check BLAKE3 hash matches expected
4. **Update Metadata**: Set mode, mtime, permissions

### Metadata Preservation
- **Mode Bits**: Applied via `std::fs::set_permissions`
- **Mtime**: Preserved using `filetime` crate
- **Size**: Verified after application
- **Ownership**: UID/GID ignored (per spec)

### Directory Operations
- **Create**: `std::fs::create_dir_all` with proper permissions
- **Remove**: `std::fs::remove_file` or `remove_dir`
- **Rename**: `std::fs::rename` for moved files

## Error Handling and Recovery

### Application Failures
- **Permission Denied**: Log error, skip file, continue with others
- **Disk Full**: Pause updates, retry after space available
- **File Locked**: Queue for later retry
- **Corrupt Delta**: Request full file retransmission

### Partial Failures
- **Atomic Operations**: Each file update is independent
- **Rollback**: Failed updates don't affect successful ones
- **Logging**: Detailed error reporting for troubleshooting

### Recovery Mechanisms
- **State Validation**: Periodic checks against Merkle roots
- **Resync**: Full subtree resync on persistent failures
- **Manual Intervention**: Commands to force resync specific files

## Performance Optimizations

### Batch Processing
- Group related file updates in single operation
- Minimize filesystem sync calls
- Parallel application where safe

### Memory Management
- Stream delta application for large files
- Limit concurrent file operations
- Garbage collection of temporary files

### I/O Efficiency
- Buffered reads/writes for small files
- Direct I/O for large file patches
- Avoid unnecessary metadata updates

## Mapping Resolution

### Path Translation
For each update, translate central path to local path:
- Central: `/src/project1/main.rs`
- Mapping: `{ central = "/src/project1", local = "/opt/app/src" }`
- Local: `/opt/app/src/main.rs`

### Multiple Mappings
- Same central file may update multiple local copies
- Independent application to each mapped location
- Conflicts resolved per mapping

## Security Considerations

### Path Safety
- Validate all paths prevent directory traversal
- Canonicalize paths before application
- Reject updates outside mapped directories

### Permission Handling
- Respect local filesystem permissions
- Don't escalate privileges
- Log permission failures appropriately

## Monitoring and Logging

### Update Metrics
- Files updated per sync operation
- Conflicts encountered and resolved
- Performance timing for large updates

### Error Reporting
- Structured logging of application failures
- Conflict notifications to administrators
- Debug information for troubleshooting

## Integration Points

### Transport Layer
- Receives updates over QUIC streams
- Handles connection interruptions gracefully

### Merkle Trees
- Validates incoming updates against local state
- Maintains integrity during application

### Configuration
- Respects conflict resolution policies
- Configurable conflict file naming

### Local Changes
- Coordinates with local watcher to avoid conflicts
- Prevents simultaneous local and remote updates

## Configuration Options

### Conflict Resolution
```toml
conflict_resolution = "latest-wins"  # latest-wins | manual | local-wins
conflict_file_template = ".conflict-{timestamp}-{ext}"
```

### Performance Tuning
```toml
max_concurrent_updates = 10
update_batch_size = 100
retry_failed_updates = true
```

### Safety Limits
```toml
max_conflict_files_per_dir = 100
max_update_age_seconds = 3600  # Reject very old updates
```

## Edge Cases

### Symlink Handling
- Preserve symlinks as symlinks
- Update link targets appropriately
- Handle broken symlinks gracefully

### Special Files
- Skip device files, sockets, etc.
- Handle FIFO pipes carefully
- Log unsupported file types

### Filesystem Limits
- Handle path length limits
- Work around filename restrictions
- Graceful degradation on full filesystems