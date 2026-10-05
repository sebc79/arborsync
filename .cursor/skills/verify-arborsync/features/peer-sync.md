# Read the peer directory from the slave socket

A co-located client connects to the slave unix socket, reads one snapshot, and the slave closes. The helper omits `peer_socket`, so the socket is `peers.sock` beside the slave db. With only `dev-alice` in the ACL, the snapshot names nobody else. The socket is not a metrics port. Status lines still print `health=` including `failed`.

## Sub-features

- `peer-socket-mode` is mode `660` at `$ROOT/slaves/dev-alice/peers.sock`.
- `peer-directory-empty` names no other slave after connect.
- `peer-query-once` returns one snapshot and then the connection closes.
- `peer-not-metrics` leaves `status ` lines, including `health=`, on the slave log.

## How to get to it (user POV)

- Launch the pair with the helper, which omits `peer_socket`.
- Read the socket next to the slave db. Do not open a metrics port.

## Driving it with control-arborsync

Preconditions:

- ArborSync is healthy from `control-arborsync doctor`.
- `ROOT` and `SLAVE_LOG` come from `control-arborsync paths`.

- **Socket mode.** Run `stat -c '%a' "$ROOT/slaves/dev-alice/peers.sock"`. The mode is `660`.
- **One snapshot.** Run `python3 -c 'import socket,struct,sys; s=socket.socket(socket.AF_UNIX); s.settimeout(2); s.connect(sys.argv[1]); n=struct.unpack(">I", s.recv(4))[0]; body=s.recv(n); assert len(body)==n and body[:4]==b"aspv" and body[4:5]==bytes([1]) and int.from_bytes(body[5:9],"big")==0; assert s.recv(1)==b""' "$ROOT/slaves/dev-alice/peers.sock"`. Exit code `0`. The body is a current directory with count `0`, and the next read is empty because the slave closed.
- **Status line.** Wait 6 s. `grep 'status ' "$SLAVE_LOG"` matches a line that contains `health=`.
- **Proof.** Run `control-arborsync capture peer-sync`. The slave log contains `status `. The socket file is still mode `660`.

## Gotchas

- The helper has one slave. The directory omits `dev-alice`, so it names nobody. That empty directory is current, not a missing socket.
- Before the master has sent a directory the snapshot is waiting. `doctor` already waited for connect, so the recipe sees the empty directory.
- `peer_socket` and `peer_socket_mode` are restart-required. The helper sets neither, so the mode is `660`.
- Staleness is three report periods. This pair cannot show another slave go quiet.
- Do not treat the socket as a metrics port. Queue numbers on the `status ` line are unchanged by the query.
