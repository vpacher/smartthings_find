# smartthings_find

This tool reads the locations and battery levels of Samsung SmartThings Find devices (SmartTags, phones, watches, earbuds). It uses the web backend of smartthingsfind.samsung.com. Samsung does not document this API. The endpoints come from the archived [HA-SmartThings-Find](https://github.com/Vedeneb/HA-SmartThings-Find) integration and its [maintained fork](https://git.jeena.net/jeena/HA-SmartThings-Find).

Status: early but working. `serve` runs the service and publishes to Home Assistant through MQTT discovery.

## Architecture

```
Samsung web API ── session/api/model/roster ── engine ──► Hub ──► integrations
                                                  ▲               ├─ log
                                                  └── Command ────┤
                                                                  └─ mqtt (Home Assistant)
```

- **engine** (`src/engine.rs`) is the only part that talks to Samsung. It reads the last known locations on a schedule, runs commands (Locate, Ring, Refresh), and recovers by itself when the session cookie expires and a new one is saved.
- **hub** (`src/hub.rs`) is what an integration gets. `subscribe()` returns a stream of `Event`s, `snapshot()` returns the current state, and `send(Command)` sends a command to the engine.
- **domain** (`src/domain.rs`) holds the types integrations use: `DeviceState`, `Location`, `Event`, `Command`. They do not depend on Samsung's JSON.
- **integrations** (`src/integrations/`) are independent outputs. `log` is the minimal template. `mqtt` sits behind the `mqtt` cargo feature, which is on by default.

### Adding an integration

To add a REST API or a database writer:

1. Create `src/integrations/<name>.rs` and implement `hub::Integration` (`name()` and `run(hub)`).
2. Gate it behind a cargo feature if it adds dependencies.
3. Add it to `build_integration` in `src/main.rs`.

Your integration uses only `domain` and `hub`. You can also depend on this crate as a library and implement the trait in your own binary.

## Authentication

The tool does not log in. Samsung's login page uses bot protection. You give the tool the `JSESSIONID` cookie of a browser session:

1. Log in at https://smartthingsfind.samsung.com in a browser.
2. Open DevTools > Application (Chrome) or Storage (Firefox) > Cookies > `smartthingsfind.samsung.com`.
3. Copy the value of `JSESSIONID`.
4. Save the value in the cookie file:

   ```sh
   mkdir -p ~/.config/smartthings_find
   pbpaste > ~/.config/smartthings_find/jsessionid
   chmod 600 ~/.config/smartthings_find/jsessionid
   ```

The tool reads the cookie from `STF_JSESSIONID` if that variable is set. If not, it reads the file at `STF_JSESSIONID_FILE` (default: `~/.config/smartthings_find/jsessionid`).

The tool never writes the cookie value to the log. The log shows only a fingerprint (the first 8 hex characters of its SHA-256). If the server sends a new `JSESSIONID`, the tool writes the new value to the cookie file, but only after an authenticated request with it succeeds. When the server rejects a cookie, it sends back a new anonymous one. Without this check, that anonymous value would replace your cookie.

## Usage

```sh
cargo run -- list                       # devices on the account
cargo run -- poll --once                # one cycle: request fresh fixes for tags, wait 20 s, read all
cargo run -- poll                       # loop, every 120 s
cargo run -- poll --active none         # read the last known locations only (no device battery cost)
cargo run -- ring "Backpack"              # ring by part of the name or by dvceID
cargo run -- watch --device S10 --active all   # subscribe to Samsung's push broker, request one fresh fix
cargo run -- tag-location Backpack        # tag-only fallback endpoint (exploration)
cargo run -- --raw poll --once          # also print the raw API responses to stderr
RUST_LOG=smartthings_find=debug cargo run -- poll
```

`--active` controls which devices get a location request in each cycle: `tags` (default), `all` or `none`. A location request costs device battery. For a phone, it can also turn on the screen.

## Home Assistant (MQTT)

```sh
export MQTT_HOST=broker.example MQTT_USERNAME=user MQTT_PASSWORD=...   # MQTT_PORT=1883 by default
cargo run --release -- serve                       # integrations: log,mqtt
cargo run --release -- serve --integrations mqtt   # without the event log
```

Optional settings: `MQTT_TLS=true` (port 8883 by default), `MQTT_CLIENT_ID`, `MQTT_BASE_TOPIC` (default `smartthings_find`) and `MQTT_DISCOVERY_PREFIX` (default `homeassistant`).

Entities for each device:

| Entity | Content |
|---|---|
| `device_tracker` | GPS position, accuracy, fix time, source |
| `sensor` Battery | percent (tags report only FULL/MEDIUM/LOW/VERY_LOW) |
| `sensor` Last fix | when the device was at the shown position |
| `sensor` Locate request | the last Locate press: `idle` (none since the service started), `requested`, `rejected` (with `reason`), `located`, `no_new_fix`. Passive fixes do not change it; see Last fix |
| `button` Locate | requests a fresh fix, then reads the device again at 3/10/30/60/120 s |
| `button` Ring | rings the device |

The "SmartThings Find" bridge device has:

- a `Session` sensor: `ok` or `expired`
- a `Session cookie` text field: paste a new JSESSIONID here (see below)
- a `Refresh` button: reads all devices now instead of at the next poll. It does not change the cookie, and it does nothing while the session is expired. All entities become unavailable when the service stops (last will on `smartthings_find/status`).

When the session expires, the service keeps running. You do not need to restart it. Give it a new JSESSIONID in one of two ways:

- Paste it into the `Session cookie` field in Home Assistant. The service checks it with Samsung first. If it works, the service uses it at once and writes it to the cookie file. If not, the service keeps the current cookie. The field shows the fingerprint of the cookie in use, never the value. Its attributes (`last_set`, `last_set_reason`, `last_set_fingerprint`) show the result of the last attempt. You can also replace a cookie that still works.
- Save it to the cookie file. While the session is expired, the service reads the file again every 30 s.

The cookie goes from Home Assistant to the service over MQTT and is never retained. Use TLS to the broker if the network between them is not trusted. Home Assistant may also record the `text.set_value` service call, with the value, in its database.

## Deploy (OpenRC)

`build.sh` runs clippy and the tests, then cross-compiles a static-pie `x86_64-unknown-linux-musl` binary with [`cargo-zigbuild`](https://github.com/rust-cross/cargo-zigbuild) (needs `zig`). `BUILD=cross ./build.sh` uses [`cross`](https://github.com/cross-rs/cross) instead (needs podman or docker). `deploy/deploy.sh user@host` builds, copies the binary, the OpenRC script and the secrets helper to the host, and restarts the service. It creates the `smartthings_find` system user and the directories if they are missing.

On the host, `/etc/smartthings_find` holds:

| File | Owner, mode | Content |
|---|---|---|
| `service.env` | root, 600 | `MQTT_HOST`, `MQTT_PORT`, `MQTT_USERNAME`, `MQTT_PASSWORD` and optionally `STF_HEARTBEAT_URL`, as shell assignments |
| `cookie/jsessionid` | service user, 600 (dir 700) | the session cookie; the service rewrites it on rotation and when you set a new one from Home Assistant |
| `bao.conf`, `bao_secret_id` | root, 600 | optional, for `deploy/fetch-secrets.sh` |

The service runs as `smartthings_find` and gets the settings as environment variables from `start_pre()`; it cannot read `service.env`. If `/usr/local/bin/smartthings_find-fetch-secrets.sh` exists, `start_pre()` first rewrites `service.env` from OpenBao or Vault with an AppRole (see the comments in `deploy/fetch-secrets.sh`). Without it, write `service.env` by hand.

### Uptime monitoring

Set `STF_HEARTBEAT_URL` to an [Uptime Kuma](https://github.com/louislam/uptime-kuma) push URL (any query string is dropped). Every 60 s the service calls it with `status=up&msg=ok` while the session is valid and a poll succeeded in the last three poll intervals, and with `status=down` and the reason (`session expired`, or `no successful poll since ...`) otherwise. When the service is down, no push arrives and Kuma alerts on the missing heartbeat. Set the monitor's heartbeat interval to a few minutes. The URL contains the monitor's token and is never logged. To keep the cookie on an encrypted volume, make `/etc/smartthings_find` a symlink to it before the first deploy.

## Push (Samsung MQTT)

The web app does not poll for operation results. It gets a push from Samsung's MQTT broker. `watch` does the same:

1. `setLastSelect.do` returns broker credentials for each device: `mqttUrl`, `mqttClientId`, `mquserName`, `mquserCode` (the password) and `topic`.
2. The tool connects to `wss://{mqttUrl}/mqtt` and subscribes to `topic`.
3. Each push payload is an opaque base64 blob. The tool POSTs it unchanged to `dm/getOperationResultRt.do`, which returns the result.

The broker closes a connection that has no traffic for 50 s, so the keep-alive is 25 s. Raw output and trace logs never show `mquserCode`.

## Location requests

The operation depends on the device type, as in the web app:

| Device | Operations | Result |
|---|---|---|
| Phone, watch, other | `CHECK_CONNECTION` + `LOCATION` | Push within about 1 s (battery, then fix) |
| Tag | `CHECK_CONNECTION_WITH_LOCATION` | Push only when there is a new fix. Fallback: `dm/getTagLocation.do` after about 30 s |

If a phone gets `CHECK_CONNECTION_WITH_LOCATION`, the server refuses it with HTTP 200 and `resultCode "02"`. (The Python integration sends this operation to every device type.)

A tag has no GPS. It gets a new position only when a Galaxy device hears it and reports in. A tag that lies still next to your phone keeps its old fix, even after a request. Reading the last known location (`poll --active none`) costs no device battery.

Design: read the last known location on a schedule. Request a fresh fix only when the user asks for it.

## Session lifetime

Samsung does not document how long a `JSESSIONID` lasts. `poll` measures it. When the session ends, or when you press Ctrl-C, the tool logs a line like this:

```
session 1a2b3c4d worked from 2026-10-10 10:40:00 UTC to 2026-10-31 08:12:00 UTC (at least 20d21h)
```

The tool exits with code 2 when the session has expired, and with code 1 for all other errors.

## Limits

- A SmartTag reports a location or rings only when a Galaxy device is near it.
- You cannot stop a SmartTag's ring through this API.
- The tool skips end-to-end encrypted locations (`encLocation.encrypted = true`).
- `accuracy_m` is the horizontal uncertainty only. The Python integration combined the horizontal and vertical values.

## License

MIT, see [LICENSE](LICENSE).
