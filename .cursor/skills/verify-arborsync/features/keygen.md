# Generate a static key

Keygen writes a 32-byte X25519 secret and prints the matching public pin so a user can paste it into the peer TOML.

## Sub-features

- `keygen-out` writes a 32-byte secret at mode `600` and prints `hex:` plus 64 hex digits.
- `keygen-requires-out` fails when `--out` is omitted.
- `keygen-help` documents `--out`.

## How to get to it (user POV)

- Run `arborsync keygen --out PATH` in a terminal.
- Run `arborsync keygen --help`.
- Run `arborsync keygen` with no `--out`.

## Driving it with control-arborsync

Preconditions:

- `control-arborsync build` has produced `target/debug/arborsync`.
- `/tmp/arborsync-verify-keygen/k` does not exist. `keygen --out` refuses to overwrite.

- **Write secret.** Generate a key. Run `control-arborsync keygen --out /tmp/arborsync-verify-keygen/k`. Exit code `0`. Stdout is one line `hex:` plus 64 hex digits. The file is 32 bytes and mode `600`.
- **Require --out.** Omit the flag. Run `target/debug/arborsync keygen`. Exit code is non-zero. Stderr mentions `--out`.
- **Help.** Ask for usage. Run `target/debug/arborsync keygen --help`. Exit code `0` and stdout contain `--out`.
- **Proof.** Copy stdout and `ls -l /tmp/arborsync-verify-keygen/k` into `/tmp/arborsync-verify-artifacts/keygen/` (create that directory). The listing shows 32 bytes and mode `600`.

## Gotchas

- `--out` is required. A missing flag is not a write to a default path.
- The printed value is the public pin, not the secret. Paste that `hex:…` string into the peer's `public_keys` or `master_public_keys`.
- Do not put key bytes in the environment. There is no `ARBORSYNC_*` for secrets.
- A second `keygen --out` to the same path exits non-zero with `AlreadyExists`. Remove the file first.
