# Connect a slave

A slave with a matching pin and ACL row opens one QUIC session and logs `connected to` the master's bound address.

## Sub-features

- `slave-connect` reaches a listening master and stays up.
- `slave-missing-key` names the key path it could not read.
- `slave-master-down` keeps running while the master is absent.
- `slave-public-key` logs the public pin `keygen` printed for the slave secret.

## How to get to it (user POV)

- Run `arborsync slave --config PATH` after the master is listening.
- Run `arborsync slave --help`.

## Driving it with control-arborsync

Preconditions:

- `control-arborsync launch` succeeded.
- `control-arborsync doctor` reports `slave_pid` alive, `slave_id=dev-alice`, and `checkout_id=src`.

- **Connect.** Inspect the live pair. Run `control-arborsync doctor`. Stdout includes `slave_pid` and `master_addr`. The slave log contains `connected to ` and `slave dev-alice ready`.
- **Missing key.** Point a slave TOML at a missing `slave_key_path` and run `target/debug/arborsync slave --config` that file. Exit code is non-zero. Stderr contains `slave.key` (or the path you used).
- **Master down.** Start a slave whose `master_addr` is a closed local port (see `tests/cli.rs` `slave_keeps_running_while_the_master_is_down`). The process stays up for at least a second and does not exit on its own.
- **Public key.** After `launch`, read `public_keys` from the `dev-alice` row in `MASTER_CONFIG`. That value is the pin `keygen` printed for `SLAVE_KEY`. The slave log contains that exact string. The pin still prints when the master is down.
- **Proof.** Run `control-arborsync capture slave-connect`. `slave.log` contains `connected to ` the same `master_addr` printed by `doctor`.

## Gotchas

- The slave must use the address from the master log, not `127.0.0.1:0` or a guessed port.
- Both configs must be mode `600`. The slave also rejects insecure mode.
- An unknown public key is a disconnect, not a quiet idle. Do not reuse pins across runs.
- All denied prefixes hang up. A `/` checkout for `dev-alice` (ACL `/src`) is rejected.
- There is no `arborsync slave reconcile` command. Docs mention it as future. Reconnect or wait for the next rescan. SIGHUP reloads config and does not start reconcile.
