#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
image="${ARBORSYNC_NIX_IMAGE:-nixos/nix}"
nix_cmd=(nix --extra-experimental-features 'nix-command flakes')

if command -v nix >/dev/null 2>&1; then
  system="$(nix eval --raw --impure --expr builtins.currentSystem)"
  (cd "$root" && "${nix_cmd[@]}" flake lock && "${nix_cmd[@]}" build ".#checks.${system}.module-unit")
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "need nix or docker" >&2
  exit 1
fi

if [[ -z "${DOCKER_HOST:-}" && ! -S /var/run/docker.sock && -S "${HOME}/.docker/run/docker.sock" ]]; then
  export DOCKER_HOST="unix://${HOME}/.docker/run/docker.sock"
fi

# A git worktree stores its gitdir under the main repo. Mount both so
# `git+file` flake resolution can follow that pointer.
git_common="$(cd "$root" && git rev-parse --path-format=absolute --git-common-dir)"
main_repo="$(dirname "$git_common")"

run_nix() {
  docker run --rm \
    -v "$main_repo:$main_repo" \
    -v "$root:$root" \
    -w "$root" \
    "$image" \
    "${nix_cmd[@]}" "$@"
}

system="$(docker run --rm "$image" "${nix_cmd[@]}" eval --raw --impure --expr builtins.currentSystem)"
run_nix flake lock
run_nix build ".#checks.${system}.module-unit"
