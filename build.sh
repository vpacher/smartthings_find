#!/bin/bash
# Builds a static Linux binary (x86_64 musl) for deployment.
#
# Quality gate first: clippy with warnings as errors and the tests, on the
# host, before the cross build. Any failure stops the build.
#
# Default: native rustc + zig as the musl linker/C compiler (needs
# cargo-zigbuild and zig). BUILD=cross uses the `cross` container build
# instead (needs podman or docker).
set -euo pipefail
cd "$(dirname "$0")"

TARGET="${TARGET:-x86_64-unknown-linux-musl}"

cargo clippy --all-targets --locked -- -D warnings
cargo test --locked --quiet

if [ "${BUILD:-zig}" = cross ]; then
  CROSS_CONTAINER_ENGINE="${CROSS_CONTAINER_ENGINE:-podman}" cross build --release --locked --target "$TARGET"
else
  # rustc asks for -static-pie, which zig ignores ("argument unused") and
  # links plain static (no ASLR); an explicit -pie gives static-pie again.
  RUSTFLAGS="-C link-arg=-pie" cargo zigbuild --release --locked --target "$TARGET"
  # Guard: refuse a binary that lost static-pie (see above).
  file "target/$TARGET/release/smartthings_find" | grep -q "static-pie linked" || {
    echo "build: target/$TARGET/release/smartthings_find is not static-pie" >&2
    exit 1
  }
fi
echo "build: target/$TARGET/release/smartthings_find"
