# Pushing Updates from Master

## Overview

The master daemon detects filesystem changes and proactively pushes updates to all interested slave nodes. This push-based architecture ensures efficient bidirectional synchronization with minimal polling overhead.

## Change Detection Flow

### Event Trigger
1. **Filesystem Event**: Watcher detects file modification
2. **Debounce Period**: 200-500ms to coalesce rapid changes
3. **Metadata Update**: Refresh file metadata in database
4. **Merkle Recomputation**: Update affected subtree roots

### Impact Assessment
1. **Path Analysis**: Identify changed file path
2. **Subtree Identification**: Find affected subtree prefixes
3. **Subscriber Lookup**: Query which slaves map affected subtrees
4. **Notification Planning**: Prepare update messages per slave

## Subscriber Notification

### Interest Matching
For each changed file, find slaves with relevant mappings:
- Direct mapping: `/src/project1` maps `/central/src/project1/file.txt`
- Parent mapping: `/` maps any file in hierarchy
- Sibling isolation: `/src/project2` doesn't match `/src/project1` changes

### Registry Structure
- **Data Structure**: Path trie for efficient prefix matching
- **Lookup**: O(path_depth) for subscriber identification
- **Caching**: Maintain active connection list per slave

## Update Generation

### Merkle Proof Creation
1. **Current State**: Slave sends known subtree root
2. **Proof Generation**: rs_merkle creates inclusion proof
3. **Delta Computation**: Identify changed files via proof

### Content Preparation
1. **File Selection**: Only files changed since slave's last sync
2. **Delta Generation**: Use copia to create binary diffs
3. **Metadata Bundle**: Include mode, mtime, size updates

### Batch Optimization
- Group multiple changes in single push operation
- Prioritize critical files (config > docs)
- Compress metadata for efficient transfer

## Push Mechanism

### QUIC Stream Allocation
- **Control Stream**: Initial subscription and heartbeats
- **Data Streams**: Dedicated per update operation
- **Multiplexing**: Parallel pushes to multiple slaves

### Message Sequence
1. **MerkleUpdate**: Send proof of changes
2. **DeltaRequest**: Slave requests specific file deltas
3. **DeltaResponse**: Stream binary patches
4. **FileMetadataPush**: Update metadata records

### Flow Control
- Respect QUIC stream limits
- Backpressure on slow slaves
- Prioritize active connections

## Bidirectional Synchronization

### Slave-Initiated Changes

#### Direct Push Mechanism
1. **Local Change**: Slave watcher detects file modification
2. **Delta Computation**: Slave computes binary delta against known master state
3. **Delta Upload**: Slave sends delta to master via QUIC stream
4. **Master Integration**: Master applies delta, updates global index and Merkle tree
5. **Fan-Out**: Push update to all other interested slaves

#### Root Comparison Recovery
For changes missed by watchers, slaves implement periodic state verification:

1. **Root Reporting**: Slave sends current Merkle roots for all checked-out subtrees
2. **Master Comparison**: Master compares against global roots in database
3. **Mismatch Handling**:
   - If roots match: No action needed
   - If roots differ: Initiate difference narrowing protocol

#### Difference Narrowing Protocol
1. **Subtree Proof Request**: Master requests Merkle proof from slave for mismatched subtree
2. **Proof Analysis**: Master identifies specific changed files using rs_merkle verification
3. **Targeted Delta Requests**: Master requests deltas only for identified changed files
4. **Batch Application**: Master applies all changes in transaction
5. **Integrity Verification**: Updated Merkle root computed and stored

#### Push Triggers
- **Watcher Events**: Immediate push on detected local changes
- **Periodic Verification**: Root comparison every 60 seconds
- **Reconnection**: Full state sync after network recovery
- **Administrative**: Manual resync commands

### Conflict Resolution
- **Master Authority**: Acts as central conflict resolver
- **Mtime Arbitration**: Latest modification time wins
- **Notification**: All parties receive resolved state

## Performance Optimizations

### Selective Pushing
- Only notify slaves with relevant mappings
- Skip slaves already up-to-date
- Batch small changes into larger updates

### Parallel Processing
- Concurrent pushes to multiple slaves
- Asynchronous delta generation
- Stream multiplexing for efficiency

### Caching and Reuse
- Cache Merkle proofs for common subtrees
- Reuse deltas across similar slave configurations
- Persistent connection pooling

## Error Handling

### Connection Issues
- **Retry Logic**: Exponential backoff on failed pushes
- **Queue Management**: Buffer updates for offline slaves
- **Timeout Handling**: Abandon stalled transfers

### Slave Failures
- **Detection**: Heartbeat monitoring
- **Cleanup**: Remove failed slaves from active lists
- **Recovery**: Resume pushes when slaves reconnect

### Resource Limits
- **Memory Bounds**: Limit queued updates per slave
- **Rate Limiting**: Prevent update storms
- **Priority Queues**: Critical updates bypass limits

## Scalability Considerations

### Large Deployments
- **Sharding**: Partition subscriber registries
- **Load Balancing**: Distribute master load across instances
- **Hierarchical**: Master-of-masters for global scale

### High-Frequency Changes
- **Debouncing**: Aggregate rapid file changes
- **Snapshotting**: Periodic full state synchronization
- **Incremental**: Avoid redundant delta computation

## Security and Access Control

### Authentication
- Only authenticated slaves receive pushes
- PSK validation on all connections
- Connection state tracking

### Authorization
- Path-based access control
- Subscriber verification per update
- Audit logging of all push operations

## Monitoring and Observability

### Metrics Collection
- Updates pushed per second/minute
- Subscriber counts and active connections
- Delta sizes and transfer times

### Logging
- Structured logs for push operations
- Error tracking for failed deliveries
- Performance monitoring for bottlenecks

### Health Checks
- Connection health monitoring
- Queue depth alerts
- Slave reachability verification

## Configuration Options

### Push Behavior
```toml
push_debounce_ms = 200
max_concurrent_pushes = 50
push_batch_size = 100
push_timeout_seconds = 300
```

### Resource Limits
```toml
max_queued_updates_per_slave = 1000
max_push_attempts = 3
push_retry_delay_ms = 1000
```

### Performance Tuning
```toml
enable_delta_caching = true
max_delta_cache_size_mb = 100
parallel_delta_generation = true
```

## Integration Points

### Filesystem Watching
- Primary trigger for update generation
- Provides real-time change detection

### Database Layer
- Supplies metadata for delta computation
- Tracks subscriber mappings

### Transport Layer
- Handles reliable update delivery
- Manages connection lifecycle

### Merkle System
- Generates proofs for change verification
- Ensures update integrity