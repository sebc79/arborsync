# Subscriptions and Mappings

## Overview

ArborSync's subscription system enables flexible, selective synchronization of filesystem subtrees. Slaves declare interest in specific central hierarchy paths and map them to local directories, allowing for customized checkouts and multiple overlapping mappings.

## Subscription Model

### Mapping Structure
Each slave defines a list of mappings from central subtrees to local paths:

```toml
checkouts = [
    { central = "/src/project1", local = "/opt/app1/src" },
    { central = "/src/project2", local = "/opt/app2/src" },
    { central = "/", local = "/backup/central" },  # Full replica
]
```

### Mapping Properties
- **Central Path**: Absolute path in master's hierarchy (e.g., `/src/project1`)
- **Local Path**: Absolute path on slave's filesystem (e.g., `/opt/app1/src`)
- **Arbitrary Depth**: Can map any subtree level
- **Multiple Mappings**: Same central subtree can map to multiple local paths
- **Overlapping**: Mappings can overlap (nested or sibling subtrees)

## Subscription Process

### Connection Establishment
1. Slave connects to master via QUIC
2. Completes Noise handshake with PSK
3. Opens control stream
4. Sends `Subscribe` message with mappings list

### Master Validation
1. Receives subscription request
2. Validates mapping paths (existence, permissions)
3. Registers slave with active mappings
4. Confirms subscription or rejects with error

### Active Subscription
- **Persistent**: Remains active until disconnection
- **Dynamic**: Can be updated during connection
- **Tracked**: Master maintains slave-to-mapping registry

## Mapping Semantics

### Path Resolution
- **Central Paths**: Always absolute from hierarchy root
- **Local Paths**: Always absolute on slave filesystem
- **Canonicalization**: All paths normalized and canonicalized
- **Symlink Handling**: Resolved consistently across platforms

### Overlap Handling
- **Nested Mappings**: Inner mappings take precedence
- **Sibling Conflicts**: Independent synchronization
- **Duplicate Files**: Allowed (same central file in multiple locals)

### Full Replica Mapping
```toml
{ central = "/", local = "/backup/central" }
```
- Maps entire central hierarchy to local directory
- Common for backup slaves
- Receives all changes from master

## Change Propagation

### Interest-Based Notifications
- Master tracks which slaves subscribe to each subtree
- On file change in `/src/project1/file.txt`:
  - Only slaves mapping `/src/project1` or deeper receive update
  - Slaves mapping `/src/project2` are not notified

### Multiple Recipients
- Single change can trigger updates to multiple slaves
- Parallel QUIC streams for efficiency
- Independent delta computation per mapping

### Bidirectional Flow
- **Slave Changes**: Pushed to master, then propagated to interested slaves
- **Master Changes**: Pushed directly to subscribed slaves
- **Conflict Resolution**: Latest mtime wins across all nodes

## Subscription Management

### Dynamic Updates
- Slaves can modify mappings during connection
- Master updates internal registries
- Seamless transition without reconnection

### Connection Recovery
- On reconnect: Slave resends current mappings
- Master verifies and restores subscription state
- Resumes synchronization from last known state

### State Verification
- **Periodic Root Reporting**: Slaves send current Merkle roots for subscribed subtrees every 60 seconds
- **Integrity Checking**: Master compares reported roots against global state
- **Difference Resolution**: Triggers narrowing protocol for mismatches
- **Recovery Assurance**: Ensures bidirectional sync consistency

### Cleanup
- Disconnected slaves automatically unsubscribed
- Stale mappings removed from registries
- Resource cleanup prevents memory leaks

## Validation and Security

### Path Validation
- **Existence**: Central paths must exist (or be creatable)
- **Permissions**: Master validates access rights
- **Sanity Checks**: Prevent directory traversal attacks
- **Canonical Paths**: Normalize to prevent ambiguities

### Access Control
- **PSK Authentication**: Only authenticated slaves can subscribe
- **Path Authorization**: Configurable allowed subtree access
- **Rate Limiting**: Prevent subscription spam

### Conflict Prevention
- **Mapping Conflicts**: Detect and reject conflicting local paths
- **Circular Dependencies**: Prevent self-referential mappings
- **Resource Limits**: Maximum mappings per slave

## Performance Considerations

### Subscription Registry
- **Data Structure**: Efficient path prefix matching
- **Indexing**: Fast lookup of interested slaves per path
- **Memory Usage**: Minimal overhead for large numbers of mappings

### Notification Efficiency
- **Batch Updates**: Group changes for same subtree
- **Parallel Processing**: Multiple slaves updated concurrently
- **Stream Multiplexing**: QUIC handles concurrent notifications

### Scalability
- **Large Hierarchies**: Path trie structures for fast prefix matching
- **Many Slaves**: Distributed registry with sharding if needed
- **High Frequency**: Debounced updates prevent notification storms

## Error Handling

### Subscription Failures
- **Invalid Paths**: Detailed error messages to slave
- **Permission Denied**: Clear access control feedback
- **Resource Limits**: Graceful rejection with retry guidance

### Runtime Issues
- **Network Disconnects**: Automatic resubscription on reconnect
- **Path Changes**: Handle moved/deleted central directories
- **Slave Overload**: Backpressure mechanisms for busy slaves

## Configuration Examples

### Development Slave
```toml
checkouts = [
    { central = "/src/backend", local = "/home/dev/backend" },
    { central = "/src/frontend", local = "/home/dev/frontend" },
    { central = "/docs", local = "/home/dev/docs" },
]
```

### Production Slave
```toml
checkouts = [
    { central = "/src/app", local = "/opt/myapp/src" },
    { central = "/config/prod", local = "/opt/myapp/config" },
]
```

### Backup Slave
```toml
checkouts = [
    { central = "/", local = "/backup/central" },
]
```

## Integration Points

### Transport Layer
- Subscriptions sent over initial QUIC control stream
- Persistent connections maintain subscription state

### Merkle Trees
- Subscriptions determine which subtree roots to track
- Enables selective Merkle proof generation

### Delta Engine
- Mappings determine local vs remote path resolution
- Affects delta computation and application

### Configuration System
- Runtime mapping updates without restart
- Configuration validation at subscription time