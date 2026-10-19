//! Home Assistant integration over MQTT discovery.
//!
//! Per device (topic key = short hash of the dvceID):
//! - `device_tracker` from `{base}/{key}/attributes` (latitude, longitude,
//!   gps_accuracy, plus fixed_at/source)
//! - `sensor` battery and last-fix timestamp from `{base}/{key}/state`
//! - `sensor` Locate request: progress of the last Locate press (`idle`
//!   until the first one) from `{base}/{key}/locate`
//! - `button` Locate (`{base}/{key}/locate/set`) and Ring (`{base}/{key}/ring/set`)
//!
//! Service-wide ("SmartThings Find" bridge device):
//! - `sensor` session, `ok` or `expired` (`{base}/session`)
//! - `button` Refresh (`{base}/refresh/set`)
//! - `text` session cookie: paste a new JSESSIONID (`{base}/cookie/set`,
//!   never retained). The state is the fingerprint of the cookie in use
//!   (`{base}/cookie`), never the value, so the cookie does not end up in
//!   HA's state history. Attributes show the last attempt's outcome.
//! - availability `{base}/status` (`online`/`offline`, last will)
//!
//! Discovery and state are retained. Everything is re-published on every
//! (re)connect and when Home Assistant announces `online` on
//! `{discovery_prefix}/status` (HA restart).

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rumqttc::{AsyncClient, Event as MqttEvent, LastWill, MqttOptions, Packet, QoS, Transport};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::domain::{Command, CookieInfo, DeviceId, DeviceKind, DeviceState, Event, LocateStatus, NewCookie, SessionStatus};
use crate::hub::{BoxFuture, Hub, Integration};

const BRIDGE_ID: &str = "smartthings_find_bridge";

#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls: bool,
    pub client_id: String,
    /// Root of this service's own topics.
    pub base_topic: String,
    /// Home Assistant's discovery prefix.
    pub discovery_prefix: String,
}

impl MqttConfig {
    /// `MQTT_HOST` (required), `MQTT_PORT` (default 1883, or 8883 with
    /// `MQTT_TLS=true`), `MQTT_USERNAME`, `MQTT_PASSWORD`,
    /// `MQTT_CLIENT_ID` (default `smartthings_find`), `MQTT_BASE_TOPIC`
    /// (default `smartthings_find`), `MQTT_DISCOVERY_PREFIX` (default
    /// `homeassistant`).
    pub fn from_env() -> Result<Self> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.trim().is_empty());
        let host = var("MQTT_HOST").context("MQTT_HOST is not set")?;
        let tls = matches!(var("MQTT_TLS").as_deref(), Some("1" | "true" | "yes"));
        let port = match var("MQTT_PORT") {
            Some(p) => p.parse().with_context(|| format!("MQTT_PORT {p:?} is not a port"))?,
            None if tls => 8883,
            None => 1883,
        };
        let username = var("MQTT_USERNAME");
        let password = var("MQTT_PASSWORD");
        if password.is_some() && username.is_none() {
            bail!("MQTT_PASSWORD is set but MQTT_USERNAME is not");
        }
        Ok(Self {
            host,
            port,
            username,
            password,
            tls,
            client_id: var("MQTT_CLIENT_ID").unwrap_or_else(|| "smartthings_find".into()),
            base_topic: var("MQTT_BASE_TOPIC").unwrap_or_else(|| "smartthings_find".into()),
            discovery_prefix: var("MQTT_DISCOVERY_PREFIX").unwrap_or_else(|| "homeassistant".into()),
        })
    }
}

pub struct Mqtt {
    config: MqttConfig,
}

impl Mqtt {
    pub fn new(config: MqttConfig) -> Self {
        Self { config }
    }
}

impl Integration for Mqtt {
    fn name(&self) -> &'static str {
        "mqtt"
    }

    fn run(self: Box<Self>, hub: Hub) -> BoxFuture {
        Box::pin(run(self.config, hub))
    }
}

/// Short, topic-safe, stable key for a device. dvceIDs are up to ~100 hex
/// characters, too long for readable topics and entity IDs.
fn key(id: &str) -> String {
    Sha256::digest(id.as_bytes()).iter().take(6).map(|b| format!("{b:02x}")).collect()
}

struct Topics<'a> {
    cfg: &'a MqttConfig,
}

impl Topics<'_> {
    fn status(&self) -> String {
        format!("{}/status", self.cfg.base_topic)
    }
    fn session(&self) -> String {
        format!("{}/session", self.cfg.base_topic)
    }
    fn cookie(&self, leaf: &str) -> String {
        format!("{}/cookie{leaf}", self.cfg.base_topic)
    }
    fn refresh(&self) -> String {
        format!("{}/refresh/set", self.cfg.base_topic)
    }
    fn device(&self, key: &str, leaf: &str) -> String {
        format!("{}/{key}/{leaf}", self.cfg.base_topic)
    }
    fn config(&self, component: &str, node: &str, object: &str) -> String {
        format!("{}/{component}/{node}/{object}/config", self.cfg.discovery_prefix)
    }
    fn ha_status(&self) -> String {
        format!("{}/status", self.cfg.discovery_prefix)
    }
}

/// (component, object id) of every entity created per device, so removal
/// can clear exactly these.
const DEVICE_ENTITIES: &[(&str, &str)] = &[
    ("device_tracker", "location"),
    ("sensor", "battery"),
    ("sensor", "last_fix"),
    ("sensor", "locate_status"),
    ("button", "locate"),
    ("button", "ring"),
];

async fn run(cfg: MqttConfig, hub: Hub) -> Result<()> {
    let topics = Topics { cfg: &cfg };
    let mut opts = MqttOptions::new(&cfg.client_id, &cfg.host, cfg.port);
    opts.set_keep_alive(Duration::from_secs(30))
        .set_last_will(LastWill::new(topics.status(), "offline", QoS::AtLeastOnce, true));
    if let Some(user) = &cfg.username {
        opts.set_credentials(user, cfg.password.clone().unwrap_or_default());
    }
    if cfg.tls {
        opts.set_transport(Transport::tls_with_default_config());
    }
    let (client, mut eventloop) = AsyncClient::new(opts, 256);

    // Poll the MQTT event loop in its own task: publishing from the main
    // loop while it also drives the event loop could deadlock once the
    // request queue is full.
    let (incoming_tx, mut incoming) = mpsc::channel::<Packet>(64);
    let host = format!("{}:{}", cfg.host, cfg.port);
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(MqttEvent::Incoming(p)) => {
                    if incoming_tx.send(p).await.is_err() {
                        return;
                    }
                }
                Ok(MqttEvent::Outgoing(_)) => {}
                Err(e) => {
                    warn!(broker = %host, "MQTT connection error: {e}; retrying in 5s");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });

    let mut events = hub.subscribe();
    let mut out = Publisher { client: &client, topics: &topics, published: BTreeMap::new() };
    loop {
        tokio::select! {
            packet = incoming.recv() => {
                let Some(packet) = packet else { bail!("MQTT event loop stopped") };
                match packet {
                    Packet::ConnAck(_) => {
                        info!(broker = %format!("{}:{}", cfg.host, cfg.port), "connected to MQTT broker");
                        for t in [format!("{}/+/locate/set", cfg.base_topic), format!("{}/+/ring/set", cfg.base_topic), topics.refresh(), topics.cookie("/set"), topics.ha_status()] {
                            client.subscribe(t, QoS::AtLeastOnce).await?;
                        }
                        out.publish_all(&hub).await?;
                    }
                    Packet::Publish(p) => {
                        let payload = String::from_utf8_lossy(&p.payload);
                        if p.topic == topics.ha_status() {
                            if payload == "online" {
                                info!("Home Assistant came online, re-publishing discovery");
                                out.publish_all(&hub).await?;
                            }
                        } else if p.topic == topics.cookie("/set") {
                            // A retained cookie would be re-applied on every
                            // reconnect and sit on the broker; ignore it.
                            if !p.retain {
                                hub.send(Command::SetCookie(NewCookie(payload.into_owned()))).await?;
                            }
                        } else if let Some(cmd) = command_for(&cfg, &hub, &p.topic) {
                            hub.send(cmd).await?;
                        } else {
                            debug!(topic = %p.topic, "ignoring message");
                        }
                    }
                    _ => {}
                }
            }
            event = events.recv() => match event {
                Ok(e) => out.event(&e).await?,
                Err(RecvError::Lagged(n)) => {
                    warn!(skipped = n, "MQTT integration fell behind, re-publishing everything");
                    out.publish_all(&hub).await?;
                }
                Err(RecvError::Closed) => return Ok(()),
            }
        }
    }
}

/// Maps a command topic back to a command. Device topics carry the key,
/// so look the device up in the current snapshot.
fn command_for(cfg: &MqttConfig, hub: &Hub, topic: &str) -> Option<Command> {
    if topic == format!("{}/refresh/set", cfg.base_topic) {
        return Some(Command::Refresh);
    }
    let rest = topic.strip_prefix(&format!("{}/", cfg.base_topic))?;
    let (k, action) = rest.split_once('/')?;
    let id = hub.snapshot().devices.into_keys().find(|id| key(id) == k)?;
    match action {
        "locate/set" => Some(Command::Locate(id)),
        "ring/set" => Some(Command::Ring { id, start: true }),
        _ => None,
    }
}

struct Publisher<'a> {
    client: &'a AsyncClient,
    topics: &'a Topics<'a>,
    /// Device id → name its discovery was last published with. A rename
    /// re-publishes discovery so HA picks up the new device name.
    published: BTreeMap<DeviceId, String>,
}

impl Publisher<'_> {
    async fn send(&self, topic: String, payload: impl Into<Vec<u8>>, retain: bool) -> Result<()> {
        self.client.publish(topic, QoS::AtLeastOnce, retain, payload).await?;
        Ok(())
    }

    async fn json(&self, topic: String, value: &Value) -> Result<()> {
        self.send(topic, value.to_string(), true).await
    }

    async fn publish_all(&mut self, hub: &Hub) -> Result<()> {
        let snap = hub.snapshot();
        self.send(self.topics.status(), "online", true).await?;
        self.bridge_discovery().await?;
        self.session(snap.session).await?;
        if let Some(c) = &snap.cookie {
            self.cookie(c).await?;
        }
        self.published.clear();
        for state in snap.devices.values() {
            self.device(state).await?;
        }
        Ok(())
    }

    async fn event(&mut self, event: &Event) -> Result<()> {
        match event {
            Event::Device(d) => self.device(d).await,
            Event::DeviceRemoved { id } => self.remove(id).await,
            Event::Session { status } => self.session(*status).await,
            Event::Cookie(c) => self.cookie(c).await,
            Event::Locate { id, status } => {
                let attrs = match status {
                    LocateStatus::Rejected { reason } => json!({ "status": "rejected", "reason": reason }),
                    LocateStatus::Located { fixed_at } => json!({ "status": "located", "fixed_at": fixed_at }),
                    LocateStatus::Requested => json!({ "status": "requested" }),
                    LocateStatus::NoNewFix => json!({ "status": "no_new_fix" }),
                    LocateStatus::Idle => json!({ "status": "idle" }),
                };
                self.json(self.topics.device(&key(id), "locate"), &attrs).await
            }
        }
    }

    async fn session(&self, status: SessionStatus) -> Result<()> {
        let payload = match status {
            SessionStatus::Expired => "expired",
            SessionStatus::Valid => "ok",
            SessionStatus::Unknown => return Ok(()),
        };
        self.send(self.topics.session(), payload, true).await
    }

    async fn cookie(&self, c: &CookieInfo) -> Result<()> {
        self.send(self.topics.cookie(""), c.fingerprint.clone(), true).await?;
        let attrs = match &c.last_set {
            Some(s) => json!({
                "last_set": if s.accepted { "accepted" } else { "rejected" },
                "last_set_fingerprint": s.fingerprint,
                "last_set_reason": s.reason,
                "last_set_at": s.at,
            }),
            None => json!({}),
        };
        self.json(self.topics.cookie("/attributes"), &attrs).await
    }

    fn origin() -> Value {
        json!({ "name": "smartthings_find", "sw_version": env!("CARGO_PKG_VERSION") })
    }

    fn availability(&self) -> Value {
        json!([{ "topic": self.topics.status() }])
    }

    async fn bridge_discovery(&self) -> Result<()> {
        let device = json!({
            "identifiers": [BRIDGE_ID],
            "name": "SmartThings Find",
            "manufacturer": "smartthings_find",
            "sw_version": env!("CARGO_PKG_VERSION"),
        });
        // Earlier builds published the session as a `problem` binary_sensor, which HA
        // shows as "Session expired: OK". Clear its retained config so HA
        // drops the old entity.
        self.send(self.topics.config("binary_sensor", BRIDGE_ID, "session_expired"), "", true).await?;
        self.json(
            self.topics.config("sensor", BRIDGE_ID, "session"),
            &json!({
                "name": "Session",
                "unique_id": format!("{BRIDGE_ID}_session"),
                "state_topic": self.topics.session(),
                "device_class": "enum",
                "options": ["ok", "expired"],
                "icon": "mdi:cookie-clock",
                "entity_category": "diagnostic",
                "availability": self.availability(),
                "device": device,
                "origin": Self::origin(),
            }),
        )
        .await?;
        self.json(
            self.topics.config("text", BRIDGE_ID, "cookie"),
            &json!({
                "name": "Session cookie",
                "unique_id": format!("{BRIDGE_ID}_cookie"),
                "command_topic": self.topics.cookie("/set"),
                "state_topic": self.topics.cookie(""),
                "json_attributes_topic": self.topics.cookie("/attributes"),
                "mode": "text",
                "icon": "mdi:cookie-edit",
                "entity_category": "config",
                "availability": self.availability(),
                "device": device,
                "origin": Self::origin(),
            }),
        )
        .await?;
        self.json(
            self.topics.config("button", BRIDGE_ID, "refresh"),
            &json!({
                "name": "Refresh",
                "unique_id": format!("{BRIDGE_ID}_refresh"),
                "command_topic": self.topics.refresh(),
                "payload_press": "PRESS",
                "availability": self.availability(),
                "device": device,
                "origin": Self::origin(),
            }),
        )
        .await
    }

    async fn device(&mut self, d: &DeviceState) -> Result<()> {
        if self.published.get(&d.id) != Some(&d.name) {
            self.discovery(d).await?;
            self.published.insert(d.id.clone(), d.name.clone());
        }
        let k = key(&d.id);
        self.json(
            self.topics.device(&k, "state"),
            &json!({
                "battery_pct": d.battery_pct,
                "fixed_at": d.location.as_ref().map(|l| l.fixed_at),
                "kind": d.kind,
            }),
        )
        .await?;
        if let Some(loc) = &d.location {
            self.json(
                self.topics.device(&k, "attributes"),
                &json!({
                    "latitude": loc.latitude,
                    "longitude": loc.longitude,
                    "gps_accuracy": loc.accuracy_m,
                    "fixed_at": loc.fixed_at,
                    "source": loc.source,
                }),
            )
            .await?;
        }
        Ok(())
    }

    async fn discovery(&self, d: &DeviceState) -> Result<()> {
        let k = key(&d.id);
        let node = format!("stf_{k}");
        let device = json!({
            "identifiers": [format!("smartthings_find_{k}")],
            "name": d.name,
            "manufacturer": "Samsung",
            "model": d.model,
            "via_device": BRIDGE_ID,
        });
        let common = |object: &str| {
            json!({
                "unique_id": format!("{node}_{object}"),
                "availability": self.availability(),
                "device": device,
                "origin": Self::origin(),
            })
        };
        let with = |mut base: Value, extra: Value| {
            if let (Value::Object(b), Value::Object(e)) = (&mut base, extra) {
                b.extend(e);
            }
            base
        };
        let state = self.topics.device(&k, "state");

        let mut entities = vec![
            (
                "device_tracker",
                "location",
                with(
                    common("location"),
                    json!({
                        "name": null,
                        "json_attributes_topic": self.topics.device(&k, "attributes"),
                        "source_type": "gps",
                        "icon": icon(d.kind),
                    }),
                ),
            ),
            (
                "sensor",
                "last_fix",
                with(
                    common("last_fix"),
                    json!({
                        "name": "Last fix",
                        "state_topic": state,
                        "value_template": "{{ value_json.fixed_at }}",
                        "device_class": "timestamp",
                    }),
                ),
            ),
            (
                "sensor",
                "locate_status",
                with(
                    common("locate_status"),
                    json!({
                        "name": "Locate request",
                        "state_topic": self.topics.device(&k, "locate"),
                        "value_template": "{{ value_json.status }}",
                        "json_attributes_topic": self.topics.device(&k, "locate"),
                        "device_class": "enum",
                        "options": ["idle", "requested", "rejected", "located", "no_new_fix"],
                        "entity_category": "diagnostic",
                    }),
                ),
            ),
            (
                "button",
                "locate",
                with(
                    common("locate"),
                    json!({
                        "name": "Locate",
                        "command_topic": self.topics.device(&k, "locate/set"),
                        "payload_press": "PRESS",
                        "icon": "mdi:crosshairs-gps",
                    }),
                ),
            ),
            (
                "button",
                "ring",
                with(
                    common("ring"),
                    json!({
                        "name": "Ring",
                        "command_topic": self.topics.device(&k, "ring/set"),
                        "payload_press": "PRESS",
                        "icon": "mdi:bell-ring",
                    }),
                ),
            ),
        ];
        // Samsung reports no battery for earbuds.
        if d.kind != DeviceKind::Earbuds {
            entities.push((
                "sensor",
                "battery",
                with(
                    common("battery"),
                    json!({
                        "name": "Battery",
                        "state_topic": state,
                        "value_template": "{{ value_json.battery_pct }}",
                        "device_class": "battery",
                        "unit_of_measurement": "%",
                        "state_class": "measurement",
                        "entity_category": "diagnostic",
                    }),
                ),
            ));
        }
        for (component, object, config) in entities {
            self.json(self.topics.config(component, &node, object), &config).await?;
        }
        Ok(())
    }

    /// Empty retained payloads delete the entities in HA and clear the
    /// retained state.
    async fn remove(&mut self, id: &DeviceId) -> Result<()> {
        let k = key(id);
        let node = format!("stf_{k}");
        for (component, object) in DEVICE_ENTITIES {
            self.send(self.topics.config(component, &node, object), "", true).await?;
        }
        for leaf in ["state", "attributes", "locate"] {
            self.send(self.topics.device(&k, leaf), "", true).await?;
        }
        self.published.remove(id);
        Ok(())
    }
}

fn icon(kind: DeviceKind) -> &'static str {
    match kind {
        DeviceKind::Tag => "mdi:tag",
        DeviceKind::Phone => "mdi:cellphone",
        DeviceKind::Tablet => "mdi:tablet",
        DeviceKind::Watch => "mdi:watch",
        DeviceKind::Earbuds => "mdi:earbuds",
        DeviceKind::Other => "mdi:map-marker",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> MqttConfig {
        MqttConfig {
            host: "localhost".into(),
            port: 1883,
            username: None,
            password: None,
            tls: false,
            client_id: "t".into(),
            base_topic: "stf".into(),
            discovery_prefix: "homeassistant".into(),
        }
    }

    #[test]
    fn key_is_short_stable_and_topic_safe() {
        let k = key("0123456789ABCDEF0123456789ABCDEF");
        assert_eq!(k.len(), 12);
        assert_eq!(k, key("0123456789ABCDEF0123456789ABCDEF"));
        assert!(k.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn refresh_topic_maps_to_command() {
        let (hub, _rx) = Hub::new();
        assert_eq!(command_for(&cfg(), &hub, "stf/refresh/set"), Some(Command::Refresh));
        assert_eq!(command_for(&cfg(), &hub, "stf/unknown/locate/set"), None);
        assert_eq!(command_for(&cfg(), &hub, "other/refresh/set"), None);
    }

    #[test]
    fn device_topics_map_to_commands() {
        let (hub, _rx) = Hub::new();
        hub.publish(Event::Device(DeviceState {
            id: "dev1".into(),
            name: "Keys".into(),
            kind: DeviceKind::Tag,
            model: None,
            location: None,
            battery_pct: None,
        }));
        let k = key("dev1");
        assert_eq!(command_for(&cfg(), &hub, &format!("stf/{k}/locate/set")), Some(Command::Locate("dev1".into())));
        assert_eq!(
            command_for(&cfg(), &hub, &format!("stf/{k}/ring/set")),
            Some(Command::Ring { id: "dev1".into(), start: true })
        );
    }
}
