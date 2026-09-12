# Configuration System

## Overview

ArborSync uses TOML configuration files for both master and slave daemons. Configuration is loaded at startup and can be reloaded dynamically for certain parameters. The system provides sensible defaults while allowing extensive customization.

## Configuration Files

### Master Configuration (`/etc/arborsync/master.toml`)
Primary configuration for the central master daemon.

### Slave Configuration (`~/.config/arborsync/slave.toml`)
Per-host configuration for slave nodes, typically user-specific.

### Loading Behavior
- **Required**: Core parameters must be specified
- **Defaults**: Sensible defaults for optional parameters
- **Validation**: Comprehensive validation at load time
- **Reload**: Runtime reload for non-disruptive changes

## Master Configuration

### Core Parameters
```toml
# Required: Central filesystem hierarchy root
central_root = "/central"

# Required: Network listening address (UDP for QUIC)
listen_addr = "0.0.0.0:8443"

# Required: 32-byte preshared key (hex-encoded)
psk = "hex:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

# Required: Database storage path
db_path = "/var/lib/arborsync/index.redb"

# Optional: Logging verbosity
log_level = "info"  # error | warn | info | debug | trace
```

### Database Options
```toml
# Database backend selection
db_backend = "redb"  # redb | sled

# Database performance tuning
db_max_readers = 10
db_cache_size_mb = 50
```

### Filesystem Options
```toml
# Watcher configuration
watcher_debounce_ms = 200
watcher_recursive = true

# Rescan parameters
rescan_interval_seconds = 60
rescan_concurrent_dirs = 4
```

### Network/Transport Options
```toml
# Transport protocol
transport = "quic"  # quic | tcp (future)

# QUIC-specific parameters
quic_max_concurrent_streams = 256
quic_idle_timeout_ms = 300000
quic_initial_mtu = 1200
quic_send_window = 2097152  # 2MB
quic_recv_window = 2097152  # 2MB

# Connection limits
max_connections = 100
connection_timeout_seconds = 3600
```

### Security Options
```toml
# PSK rotation (runtime reloadable)
psk_rotation_enabled = true

# Rate limiting
max_connection_attempts_per_minute = 60
max_subscriptions_per_connection = 10
```

### Operational Options
```toml
# Daemon behavior
pid_file = "/var/run/arborsync-master.pid"
user = "arborsync"
group = "arborsync"

# Monitoring
metrics_enabled = true
metrics_bind_addr = "127.0.0.1:9090"

# Maintenance
auto_compaction_enabled = true
auto_compaction_interval_hours = 24
```

## Slave Configuration

### Core Parameters
```toml
# Required: Master connection address
master_addr = "master.example.com:8443"

# Required: Matching PSK from master
psk = "hex:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

# Optional: Local database cache
db_path = "/var/cache/arborsync/slave.redb"

# Optional: Logging
log_level = "info"
```

### Checkout Mappings
```toml
# Required: List of central-to-local mappings
checkouts = [
    { central = "/src/project1", local = "/opt/app/src" },
    { central = "/config", local = "/etc/myapp" },
    # Full replica example
    { central = "/", local = "/backup/central" },
]

# Mapping validation
allow_overlapping_mappings = true
max_mappings = 100
```

### Local Filesystem Options
```toml
# Local watcher settings
local_watcher_debounce_ms = 200
local_watcher_recursive = true

# Local rescan
local_rescan_interval_seconds = 60

# Conflict resolution
conflict_resolution = "latest-wins"  # latest-wins | local-wins | manual
conflict_file_template = ".conflict-{timestamp}-{ext}"
max_conflict_files_per_dir = 10
```

### Network Options
```toml
# Connection management
reconnect_delay_ms = 1000
max_reconnect_attempts = 10
connection_timeout_seconds = 30

# Update processing
max_concurrent_updates = 5
update_batch_size = 50
update_timeout_seconds = 300
```

### Performance Tuning
```toml
# Memory limits
max_memory_mb = 512
delta_cache_size_mb = 100

# I/O optimization
io_buffer_size_kb = 64
parallel_file_operations = true
```

## Configuration Validation

### Syntax Validation
- **TOML Format**: Standard TOML parsing with helpful error messages
- **Type Checking**: Ensure correct types for all parameters
- **Range Checking**: Validate numeric ranges and enums

### Semantic Validation
- **Path Existence**: Verify central_root and local paths exist or are creatable
- **Network Addresses**: Validate IP/port formats
- **PSK Format**: Ensure exactly 32 bytes (64 hex characters)
- **Mapping Sanity**: Check for conflicting or invalid mappings

### Runtime Validation
- **Permissions**: Test filesystem access rights
- **Network Connectivity**: Verify master reachability (slave only)
- **Resource Limits**: Check available memory/disk space

## Dynamic Configuration

### Reloadable Parameters
- **PSK**: Runtime key rotation without restart
- **Logging Level**: Change verbosity on-the-fly
- **Rate Limits**: Adjust operational limits
- **Connection Parameters**: Update timeouts and windows

### Reload Process
1. **Signal Reception**: SIGHUP or API call triggers reload
2. **File Re-reading**: Parse updated configuration
3. **Validation**: Check new parameters
4. **Atomic Update**: Apply changes without service interruption
5. **Logging**: Report successful reload or detailed errors

### Non-Reloadable Parameters
- Database path and backend
- Network listening addresses
- Core filesystem paths
- Require daemon restart

## Environment Variables

### Override Support
```bash
# Override config file location
ARBORSYNC_CONFIG=/path/to/custom.toml

# Override specific values
ARBORSYNC_MASTER_ADDR=192.168.1.100:8443
ARBORSYNC_LOG_LEVEL=debug
```

### Security Considerations
- PSK should never be set via environment variables
- Sensitive paths should be validated
- Environment overrides logged for audit

## Configuration Examples

### Minimal Master
```toml
central_root = "/central"
listen_addr = "0.0.0.0:8443"
psk = "hex:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
db_path = "/var/lib/arborsync/index.redb"
```

### Development Slave
```toml
master_addr = "localhost:8443"
psk = "hex:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
checkouts = [
    { central = "/src", local = "/home/dev/project/src" },
    { central = "/docs", local = "/home/dev/project/docs" },
]
log_level = "debug"
```

### Production Slave
```toml
master_addr = "arbor-master.prod.company.com:8443"
psk = "hex:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
db_path = "/var/lib/arborsync/cache.redb"
checkouts = [
    { central = "/app", local = "/opt/myapp" },
]
conflict_resolution = "latest-wins"
```

### High-Performance Master
```toml
central_root = "/central"
listen_addr = "0.0.0.0:8443"
psk = "hex:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
db_path = "/var/lib/arborsync/index.redb"

# Performance tuning
quic_max_concurrent_streams = 512
max_connections = 200
db_cache_size_mb = 200
watcher_debounce_ms = 100
rescan_concurrent_dirs = 8
```

## Configuration Management

### Templating
- Support for variable substitution
- Environment-specific configurations
- Include files for shared settings

### Version Control
- Configuration files tracked in VCS
- Diff checking for changes
- Rollback capabilities

### Backup and Recovery
- Automatic config backups on changes
- Recovery from known-good configurations
- Configuration validation before deployment

## Security Considerations

### File Permissions
- Configuration files contain sensitive PSKs
- Restrict to daemon user only (0600)
- Audit access to config files

### PSK Management
- Generate strong random 32-byte keys
- Rotate regularly via config reload
- Never log or expose in error messages

### Path Validation
- Prevent directory traversal in paths
- Validate all filesystem paths exist
- Check permissions before startup

## Troubleshooting

### Common Issues
- **Invalid PSK**: Must be exactly 64 hex characters
- **Path Not Found**: Ensure directories exist and are accessible
- **Port In Use**: Check for conflicts on listening ports
- **Permission Denied**: Verify filesystem and network permissions

### Validation Tools
- Command-line config validator
- Dry-run mode for testing configurations
- Detailed error reporting with suggestions