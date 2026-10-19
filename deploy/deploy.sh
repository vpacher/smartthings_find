#!/bin/bash
# Builds and deploys smartthings_find to an OpenRC host over SSH.
#
#   deploy/deploy.sh root@host
#
# SKIP_BUILD=1 reuses the binary already in target/ (e.g. for an init-script-
# only change). Idempotent: creates the service user and the config/cookie
# directories if they are missing, and never touches existing config, the
# cookie or OpenBao files. Point /etc/smartthings_find at an encrypted volume
# (symlink) before the first deploy if you want the cookie stored there.
set -euo pipefail
cd "$(dirname "$0")/.."

REMOTE="${1:?usage: deploy/deploy.sh user@host}"
TARGET="x86_64-unknown-linux-musl"
BIN=/usr/local/bin/smartthings_find
FETCH=/usr/local/bin/smartthings_find-fetch-secrets.sh
CONF_DIR=/etc/smartthings_find
USER_NAME=smartthings_find

if [ "${SKIP_BUILD:-0}" = 1 ]; then
  echo "SKIP_BUILD=1: reusing target/$TARGET/release/smartthings_find"
else
  ./build.sh
fi

# scp writes in place, which fails with ETXTBSY on a running binary: upload
# to a temp name and rename.
scp "target/$TARGET/release/smartthings_find" "$REMOTE:$BIN.new"
scp deploy/openrc/smartthings_find "$REMOTE:/etc/init.d/smartthings_find"
scp deploy/fetch-secrets.sh "$REMOTE:$FETCH"

# `rc-service restart` can leave the old supervise-daemon (and worker)
# running on some OpenRC versions, so kill by full command line and start
# fresh, then check that exactly one new process runs.
ssh "$REMOTE" "BIN=$BIN FETCH=$FETCH CONF_DIR=$CONF_DIR USER_NAME=$USER_NAME sh -s" <<'EOF'
set -eu
getent group "$USER_NAME" >/dev/null || addgroup -S "$USER_NAME"
id "$USER_NAME" >/dev/null 2>&1 || adduser -S -D -H -h /nonexistent -s /sbin/nologin -G "$USER_NAME" "$USER_NAME"
mkdir -p "$CONF_DIR/cookie"
chmod 711 "$CONF_DIR"
chown -R "$USER_NAME:$USER_NAME" "$CONF_DIR/cookie"
chmod 700 "$CONF_DIR/cookie"
chmod 755 "$BIN.new" "$FETCH" /etc/init.d/smartthings_find
mv "$BIN.new" "$BIN"
rc-update add smartthings_find default >/dev/null 2>&1 || true

pkill -9 -f "^supervise-daemon smartthings_find " || true
pkill -9 -f "^$BIN serve\$" || true
sleep 1
rm -f /run/smartthings_find.pid
rc-service smartthings_find zap >/dev/null 2>&1 || true
rc-service smartthings_find start
sleep 5
pids=$(pgrep -f "^$BIN serve\$" || true)
if [ "$(echo $pids | wc -w)" -ne 1 ] || readlink "/proc/$pids/exe" | grep -q deleted; then
  echo "deploy: expected exactly one fresh smartthings_find, found: '$pids'" >&2
  exit 1
fi
echo "deploy: smartthings_find running as pid $pids (user $(stat -c %U /proc/$pids))"
EOF
