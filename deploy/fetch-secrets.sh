#!/bin/sh
# Fetches the service settings from OpenBao (or Vault) with an AppRole and
# writes them as shell-quoted assignments, for the OpenRC script's start_pre()
# to source. Optional: without it, maintain service.env by hand.
#
#   smartthings_find-fetch-secrets.sh [OUT]     (default /etc/smartthings_find/service.env)
#
# Site settings come from $BAO_CONF (default /etc/smartthings_find/bao.conf),
# so nothing site-specific lives in this script:
#   BAO_NODES="https://bao1:8200 https://bao2:8200"   tried in order, first unsealed wins
#   BAO_ROLE_ID="..."                                 AppRole role_id (not a secret on its own)
#   BAO_SECRET_ID_FILE=/etc/smartthings_find/bao_secret_id
#   BAO_MQTT_PATH=secret/shared/mqtt                  KV v2 path with MQTT_HOST, MQTT_PORT,
#                                                     MQTT_USERNAME, MQTT_PASSWORD
#   BAO_SERVICE_PATH=secret/<host>/smartthings_find   optional KV v2 path; its HEARTBEAT_URL
#                                                     (if present) becomes STF_HEARTBEAT_URL
#   BAO_CACERT=...                                    optional CA bundle for the nodes
#
# POSIX sh (Alpine has no bash). Requires: bao (or vault via BAO_BIN), jq, curl.
set -eu

BAO_CONF="${BAO_CONF:-/etc/smartthings_find/bao.conf}"
OUT="${1:-/etc/smartthings_find/service.env}"

if [ ! -f "$BAO_CONF" ]; then
  echo "fetch-secrets: no $BAO_CONF" >&2
  exit 1
fi
. "$BAO_CONF"
: "${BAO_NODES:?BAO_NODES not set in $BAO_CONF}"
: "${BAO_ROLE_ID:?BAO_ROLE_ID not set in $BAO_CONF}"
: "${BAO_SECRET_ID_FILE:=/etc/smartthings_find/bao_secret_id}"
: "${BAO_MQTT_PATH:=secret/shared/mqtt}"
: "${BAO_BIN:=bao}"

if [ ! -f "$BAO_SECRET_ID_FILE" ]; then
  echo "fetch-secrets: no secret_id file at $BAO_SECRET_ID_FILE" >&2
  exit 1
fi

CURL_CA=""
if [ -n "${BAO_CACERT:-}" ]; then
  CURL_CA="--cacert $BAO_CACERT"
  export BAO_CACERT VAULT_CACERT="$BAO_CACERT"
fi

# A sealed node still answers /sys/health (with sealed:true), so check for
# unsealed, not just reachable.
BAO_ADDR=""
for node in $BAO_NODES; do
  # shellcheck disable=SC2086
  resp=$(curl -s $CURL_CA --max-time 2 "${node}/v1/sys/health" 2>/dev/null) || continue
  case "$resp" in
    *'"sealed":false'*) BAO_ADDR="$node"; break ;;
  esac
done
if [ -z "$BAO_ADDR" ]; then
  echo "fetch-secrets: no unsealed node among ($BAO_NODES)" >&2
  exit 1
fi
export BAO_ADDR VAULT_ADDR="$BAO_ADDR"

login=$("$BAO_BIN" write -f -format=json auth/approle/login \
  role_id="$BAO_ROLE_ID" secret_id="@${BAO_SECRET_ID_FILE}" 2>&1) || {
  echo "fetch-secrets: AppRole login failed: $login" >&2
  exit 1
}
BAO_TOKEN=$(printf '%s' "$login" | jq -r '.auth.client_token')
export BAO_TOKEN VAULT_TOKEN="$BAO_TOKEN"

# Check bao's own exit status before jq: without pipefail a failed read would
# otherwise produce an incomplete file.
data=$("$BAO_BIN" kv get -format=json "$BAO_MQTT_PATH" 2>&1) || {
  echo "fetch-secrets: failed to read $BAO_MQTT_PATH: $data" >&2
  exit 1
}

svc=""
if [ -n "${BAO_SERVICE_PATH:-}" ]; then
  svc=$("$BAO_BIN" kv get -format=json "$BAO_SERVICE_PATH" 2>&1) || {
    echo "fetch-secrets: failed to read $BAO_SERVICE_PATH: $svc" >&2
    exit 1
  }
fi

# Single-quoted, so a $ or ` in a value is never expanded when sourced.
emit() {
  printf "%s='%s'\n" "$1" "$(printf '%s' "$2" | sed "s/'/'\\\\''/g")" >> "$TMP"
}

# Temp file next to the target, so it stays on the same (encrypted) volume.
TMP=$(mktemp "$OUT.XXXXXX")
trap 'rm -f "$TMP"' EXIT
chmod 600 "$TMP"
for key in MQTT_HOST MQTT_PORT MQTT_USERNAME MQTT_PASSWORD; do
  val=$(printf '%s' "$data" | jq -r --arg k "$key" '.data.data[$k] // empty')
  if [ -z "$val" ]; then
    echo "fetch-secrets: $BAO_MQTT_PATH has no $key" >&2
    exit 1
  fi
  emit "$key" "$val"
done
if [ -n "$svc" ]; then
  hb=$(printf '%s' "$svc" | jq -r '.data.data.HEARTBEAT_URL // empty')
  [ -z "$hb" ] || emit STF_HEARTBEAT_URL "$hb"
fi
mv "$TMP" "$OUT"
trap - EXIT
echo "fetch-secrets: [via $BAO_ADDR] wrote $(wc -l < "$OUT" | tr -d ' ') settings to $OUT"
