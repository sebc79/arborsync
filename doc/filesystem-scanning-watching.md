# Filesystem Scanning and Watching

## Overview

ArborSync employs a robust filesystem change detection system to monitor file modifications, creations, deletions, and metadata changes across both master and slave nodes. This system ensures that all changes are detected efficiently and propagated bidirectionally through the network.

## Components

### File Watcher

- **Library**: `notify-debouncer-mini` v0.5
- **Purpose**: Provides debounced filesystem event watching with recursive directory monitoring
- **Debounce Window**: 200-500ms to coalesce rapid events (e.g., atomic writes, bulk operations)

### Background Rescan

- **Interval**: Every 60 seconds
- **Purpose**: Robustness against missed events, network filesystems (NFS), or watcher limitations
- **Scope**: Full Merkle tree walk on changed subtrees only, not entire hierarchy

## Master Implementation

### Watcher Setup
- Single process watches the entire central hierarchy (`/central`)
- Uses `notify::Watcher` with recursive flag
- Debounces events to prevent excessive processing

### Event Handling Flow
1. File event detected (create, modify, delete, rename)
2. Debounce period elapses
3. Collect affected paths
4. Update global database metadata for affected files
5. Recompute global Merkle tree roots for affected subtrees
6. Notify subscribed slaves with overlapping checkouts of changes

### Rescan Process
- Runs in background task
- Compares current filesystem state against database index
- Identifies discrepancies (missed changes)
- Triggers same update flow as live events

### Change Recovery Mechanism

In addition to filesystem watching, ArborSync implements a robust recovery mechanism for detecting changes missed by watchers (e.g., NFS issues, permission changes, or filesystem limitations):

#### Root Comparison Protocol
1. **Slave State Reporting**: Slaves periodically send current Merkle tree roots for their checked-out subtrees to the master
2. **Master Verification**: Master compares received roots against its global index
3. **Mismatch Detection**: If roots differ, master initiates difference narrowing

#### Difference Narrowing Process
1. **Subtree Requests**: Master requests slave to provide Merkle proofs for suspected changed subtrees
2. **Proof Analysis**: Master analyzes proofs to identify specific changed files
3. **Delta Requests**: Master requests binary deltas from slave for identified changes
4. **Index Update**: Master applies changes to global database and Merkle tree
5. **Propagation**: Updated changes pushed to all interested slaves

#### Timing and Triggers
- **Periodic Checks**: Every 60 seconds (aligned with rescans)
- **Connection Events**: On slave reconnection after disconnection
- **Manual Triggers**: Administrative commands for forced verification
- **Event-Based**: After batches of filesystem events to catch edge cases

This mechanism ensures bidirectional synchronization remains consistent even when filesystem watching fails.

## Slave Implementation

### Watcher Setup
- Watches each local checkout path (one per checkout)
- Uses `notify::Watcher` per checkout
- Debounces events per checkout

### Event Handling Flow
1. Local file change detected in checkout
2. Debounce period elapses
3. Update checkout's local database metadata and Merkle roots
4. Compute delta against master's global state for canonical prefix
5. Send delta to master via QUIC stream
6. Master updates global index and propagates to other checkouts

### Rescan Process
- Per checkout: compares local filesystem to checkout's index
- Verifies local files match subscribed state
- Updates checkout index on discrepancies
- Triggers sync with master if changes detected

## Event Types Handled

- `Create`: New file/directory
- `Write`: File content modified
- `Remove`: File/directory deleted
- `Rename`: File moved/renamed
- `Chmod`: Permissions/metadata changed (mode, mtime)

## Performance Considerations

- Debouncing prevents excessive I/O during bulk operations
- Rescans are incremental (only changed subtrees)
- Watcher uses efficient inotify/kqueue equivalents
- Minimal resource usage for daemon operation

## Integration Points

- **Database**: Updates file metadata in respective checkout index on changes
- **Merkle Trees**: Triggers recomputation on subtree changes per checkout
- **Transport**: Sends deltas from checkout indexes to master for sync
- **Conflict Resolution**: Compares mtimes across checkouts via master

## Error Handling

- Watcher failures logged but don't crash daemon
- Rescan catches missed events
- Network issues don't affect local watching
- Automatic recovery on filesystem remounts