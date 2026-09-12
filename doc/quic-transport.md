# QUIC Transport Layer

## Overview

ArborSync uses QUIC as its primary transport protocol, providing secure, multiplexed, and efficient communication between master and slave nodes. The transport layer integrates Noise protocol framework for authenticated encryption using preshared keys (PSK).

## Core Components

### QUIC Implementation
- **Library**: `quinn` v0.11
- **Features**: IETF RFC 9000 compliant, UDP-based, stream multiplexing
- **Benefits**: Zero head-of-line blocking, connection migration, modern congestion control

### Noise Handshake
- **Library**: `quinn-hyphae` v0.1 (Noise over Quinn)
- **Pattern**: `Noise_XX_25519_ChaChaPoly_BLAKE2s`
- **Authentication**: Mutual authentication with 32-byte PSK
- **ALPN**: `arborsync-v1` for protocol negotiation

## Transport Architecture

### Master Side
- **Endpoint**: `quinn::Endpoint::server()` with Noise configuration
- **Listener**: Accepts incoming QUIC connections on UDP port
- **Handler**: Spawns per-connection task for each slave

### Slave Side
- **Endpoint**: `quinn::Endpoint::client()` 
- **Connector**: Initiates connection to master address
- **Streams**: Opens bidirectional streams as needed

## Connection Establishment

### Handshake Process
1. **UDP Connection**: Slave connects to master's UDP endpoint
2. **Noise Handshake**: XX pattern with PSK authentication
3. **ALPN Negotiation**: Confirms `arborsync-v1` protocol
4. **Stream Setup**: Initial control stream for protocol messages

### Security Properties
- **Forward Secrecy**: Ephemeral keys per connection
- **Authentication**: Mutual verification via PSK
- **Encryption**: ChaChaPoly AEAD cipher
- **Replay Protection**: Built into Noise protocol

## Stream Management

### Stream Types
- **Control Stream**: Initial bidirectional stream for subscriptions and heartbeats
- **Data Streams**: Separate streams for file deltas and Merkle updates
- **Unidirectional**: Used for one-way notifications (push updates)

### Multiplexing Benefits
- **Concurrency**: Multiple transfers simultaneously without blocking
- **Prioritization**: Control messages can bypass large data transfers
- **Efficiency**: No connection overhead for parallel operations

### Stream Lifecycle
1. **Open**: Created on-demand for specific operations
2. **Active**: Bidirectional data flow with flow control
3. **Close**: Graceful shutdown with FIN frames
4. **Error**: Immediate termination on protocol violations

## Protocol Messages

### Serialization
- **Format**: Bincode for efficient binary serialization
- **Compatibility**: Versioned message types for protocol evolution

### Message Types
```rust
#[derive(Serialize, Deserialize)]
pub enum ProtocolMessage {
    // Subscription management
    Subscribe { mappings: Vec<(String, String)> },  // central → local paths

    // Merkle tree synchronization
    MerkleUpdate { subtree: String, new_root: [u8; 32], proof: Vec<u8> },

    // Slave state reporting and verification
    SlaveRootReport { subtree: String, current_root: [u8; 32] },
    SubtreeProofRequest { subtree: String },
    SubtreeProofResponse { subtree: String, proof: Vec<u8> },

    // Delta transfer
    DeltaRequest { file_path: String, signature: Vec<u8> },
    DeltaResponse { delta: Vec<u8> },

    // Metadata propagation
    FileMetadataPush { path: String, meta: FileMetadata },

    // Connection management
    Heartbeat,
    Disconnect { reason: String },
}
```

### Message Flow
- **Subscription**: Slave → Master (initial setup)
- **Updates**: Master → Slave (push notifications)
- **Deltas**: Bidirectional (slave changes or master responses)
- **State Verification**:
  - Slave → Master: `SlaveRootReport` (periodic root checks)
  - Master → Slave: `SubtreeProofRequest` (on mismatch detection)
  - Slave → Master: `SubtreeProofResponse` (proof for narrowing)
  - Followed by targeted `DeltaRequest`/`DeltaResponse` exchanges

## Configuration

### Master Configuration
```toml
transport = "quic"
listen_addr = "0.0.0.0:8443"        # UDP port
psk = "hex:32bytekeyhere..."        # 32-byte PSK
quic_max_concurrent_streams = 256
quic_idle_timeout_ms = 300000       # 5 minutes
quic_initial_mtu = 1200
```

### Slave Configuration
```toml
master_addr = "master.example.com:8443"
psk = "hex:32bytekeyhere..."
transport = "quic"
```

### QUIC Parameters
- **Max Streams**: Limits concurrent operations per connection
- **Idle Timeout**: Automatic cleanup of stale connections
- **MTU**: Optimized for various network conditions

## Connection Management

### Lifecycle
1. **Establish**: Noise handshake completes
2. **Active**: Bidirectional communication
3. **Idle**: Keep-alive with heartbeats
4. **Close**: Graceful shutdown or error termination

### Reconnection
- **Automatic**: Exponential backoff on connection loss
- **State Recovery**: Resumes subscriptions after reconnect
- **Seamless**: No data loss during brief disconnections

### Load Balancing
- **Master**: Accepts multiple concurrent slave connections
- **Fairness**: Quinn's built-in flow control prevents starvation
- **Resource Limits**: Configurable connection and stream limits

## Performance Optimizations

### 0-RTT Resumption
- **PSK Sessions**: Resume without full handshake
- **Faster Reconnections**: Reduced latency for frequent connects
- **Security**: Maintains forward secrecy guarantees

### Congestion Control
- **Modern Algorithms**: BBR/CUBIC implementations in Quinn
- **Adaptive**: Adjusts to network conditions
- **Fairness**: Coexists well with other traffic

### Memory Usage
- **Streaming**: Process large files without full buffering
- **Flow Control**: Prevents memory exhaustion
- **Pooling**: Reuse connection resources

## Security Considerations

### Encryption
- **AEAD**: Authenticated encryption for all data
- **Perfect Forward Secrecy**: Ephemeral key agreement
- **Key Rotation**: PSK can be rotated via config reload

### Authentication
- **Mutual**: Both parties verify each other
- **PSK-based**: No certificate management required
- **Session-based**: Per-connection authentication

### DoS Protection
- **Rate Limiting**: Throttle connection attempts
- **Resource Bounds**: Limit memory per connection
- **Timeouts**: Prevent resource exhaustion

## Error Handling

### Connection Errors
- **Network Issues**: Automatic retry with backoff
- **Protocol Violations**: Immediate connection termination
- **Resource Exhaustion**: Graceful degradation

### Recovery Mechanisms
- **State Synchronization**: Merkle trees detect inconsistencies
- **Partial Transfers**: Resume interrupted file transfers
- **Logging**: Detailed error reporting for debugging

## Integration Points

### Filesystem Watching
- Triggers data transmission over established connections
- Maintains persistent connections for efficiency

### Delta Engine
- Uses QUIC streams for efficient block transfer
- Multiplexing enables parallel delta operations

### Configuration System
- Runtime config reload for PSK rotation
- Dynamic parameter adjustment

## Future Extensibility

### Transport Abstraction
- **Trait-based**: `Transport` trait allows TCP fallback
- **Modular**: Easy addition of new transport protocols
- **Configuration-driven**: Runtime transport selection

### Protocol Evolution
- **Version Negotiation**: ALPN for protocol versioning
- **Backward Compatibility**: Graceful handling of older clients
- **Feature Flags**: Negotiate optional capabilities