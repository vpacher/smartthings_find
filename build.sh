#!/bin/bash
# Builds a static Linux binary (x86_64 musl) for deployment.
#
# Quality gate first: clippy with warnings as errors and the tests, on the
# host, before the (slow) cross build. Any failure stops the build.
set -euo pipefail
cd "$(dirname "$0")"

TARGET="${TARGET:-x86_64-unknown-linux-musl}"

cargo clippy --all-targets --locked -- -D warnings
cargo test --locked --quiet

CROSS_CONTAINER_ENGINE="${CROSS_CONTAINER_ENGINE:-podman}" cross build --release --locked --target "$TARGET"
echo "build: target/$TARGET/release/smartthings_find"
