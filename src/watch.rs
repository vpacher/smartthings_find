//! Push mode: subscribe to Samsung's own MQTT broker, the way the
//! SmartThings Find web app does, instead of polling.
//!
//! Flow, as read from the web app's bundle (`connectMqtt`):
//! 1. `setLastSelect.do` returns per-device broker credentials.
//! 2. Connect to `wss://{mqttUrl}/mqtt`, subscribe to `topic`
//!    (`FMM/+/+/+/<usrId>/<dvceId>/operation/result`).
//! 3. Each message payload is POSTed unchanged to `getOperationResultRt.do`,
//!    which returns the `operation[]` list with the new result.

use std::time::Duration;

use anyhow::Result;
use rumqttc::{AsyncClient, Event, LastWill, MqttOptions, Packet, QoS, Transport};
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::{info, warn};

use smartthings_find::api::{Client, is_session_expired};
use smartthings_find::model::{Device, PushCredentials};
use crate::{Active, report};

/// The broker side drops a websocket that is idle for 50 s (measured), so
/// the MQTT keep-alive must be well under that.
const KEEP_ALIVE: Duration = Duration::from_secs(25);

/// One message from a device's subscription.
struct Push {
    device: usize,
    topic: String,
    payload: String,
}

pub async fn watch(client: &mut Client, only: Option<&str>, active: Active, interval: Duration) -> Result<()> {
    let devices = match only {
        Some(q) => vec![crate::find_device(client, q).await?],
        None => {
            let mut roster = smartthings_find::roster::Roster::default();
            for _ in 0..crate::LOOKUP_ATTEMPTS {
                roster.merge(client.devices().await?);
            }
            roster.devices().cloned().collect()
        }
    };
    let (tx, mut rx) = mpsc::channel::<Push>(32);

    for (i, dev) in devices.iter().enumerate() {
        let resp = client.last_select(dev).await?;
        report(dev, &resp.snapshot());
        match resp.push {
            Some(creds) => {
                info!(device = %dev.name(), ?creds, "push credentials received");
                tokio::spawn(subscribe(i, dev.name(), creds, tx.clone()));
            }
            None => warn!(device = %dev.name(), "no push credentials in setLastSelect response"),
        }
    }
    drop(tx);

    // Give the subscriptions a moment before triggering the first request,
    // so its result is not published before anyone listens.
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + Duration::from_secs(5), interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => request_fixes(client, &devices, active).await?,
            msg = rx.recv() => {
                let Some(msg) = msg else {
                    warn!("all subscriptions ended");
                    return Ok(());
                };
                handle_push(client, &devices[msg.device], &msg).await?;
            }
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

async fn request_fixes(client: &mut Client, devices: &[Device], active: Active) -> Result<()> {
    for dev in devices.iter().filter(|d| active.applies_to(d)) {
        match client.request_location(dev).await {
            Ok(()) => info!(device = %dev.name(), "location requested"),
            Err(e) if is_session_expired(&e) => return Err(e),
            Err(e) => warn!(device = %dev.name(), "location request failed: {e:#}"),
        }
    }
    Ok(())
}

async fn handle_push(client: &mut Client, dev: &Device, msg: &Push) -> Result<()> {
    info!(device = %dev.name(), topic = %msg.topic, payload = %summarize(&msg.payload), "push received");
    match client.operation_result_rt(&msg.payload).await {
        Ok(resp) => {
            for op in &resp.operation {
                let s = |k: &str| op.get(k).and_then(Value::as_str).unwrap_or("-").to_string();
                info!(
                    device = %dev.name(),
                    op = %s("oprnType"),
                    status = %s("oprnStsCd"),
                    result = %s("oprnResultCode"),
                    "operation result"
                );
            }
            // A push carries one operation's result; a CHECK_CONNECTION result
            // has battery but no location, which is normal here.
            let snap = resp.snapshot();
            if snap.location.is_some() {
                report(dev, &snap);
            } else if let Some(b) = snap.battery_pct {
                info!(device = %dev.name(), battery = %format!("{b}%"), "battery");
            }
            Ok(())
        }
        Err(e) if is_session_expired(&e) => Err(e),
        Err(e) => {
            warn!(device = %dev.name(), "getOperationResultRt failed: {e:#}");
            Ok(())
        }
    }
}

/// Payload keys only, so the log shows the shape without IDs.
fn summarize(payload: &str) -> String {
    match serde_json::from_str::<Value>(payload) {
        Ok(Value::Object(m)) => format!("{{{}}}", m.keys().cloned().collect::<Vec<_>>().join(", ")),
        _ => format!("<{} bytes, not a JSON object>", payload.len()),
    }
}

async fn subscribe(device: usize, name: String, creds: PushCredentials, tx: mpsc::Sender<Push>) {
    let (host, port) = match creds.url.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(443)),
        None => (creds.url.clone(), 443),
    };
    let mut opts = MqttOptions::new(creds.client_id.clone(), format!("wss://{host}:{port}/mqtt"), port);
    opts.set_transport(Transport::wss_with_default_config())
        .set_credentials(creds.username.clone(), creds.password.clone())
        .set_last_will(LastWill::new(&creds.will_topic, "good-bye", QoS::AtMostOnce, false))
        .set_keep_alive(KEEP_ALIVE)
        .set_max_packet_size(1 << 20, 1 << 20);
    let (mqtt, mut events) = AsyncClient::new(opts, 10);

    loop {
        match events.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                info!(device = %name, code = ?ack.code, "connected to push broker");
                // Re-subscribe on every (re)connect: the session is not persistent.
                if let Err(e) = mqtt.subscribe(&creds.topic, QoS::AtMostOnce).await {
                    warn!(device = %name, "subscribe failed: {e}");
                }
            }
            Ok(Event::Incoming(Packet::SubAck(ack))) => {
                info!(device = %name, codes = ?ack.return_codes, "subscribed");
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                let push = Push {
                    device,
                    topic: p.topic.clone(),
                    payload: String::from_utf8_lossy(&p.payload).into_owned(),
                };
                if tx.send(push).await.is_err() {
                    return;
                }
            }
            Ok(_) => {}
            Err(e) => {
                warn!(device = %name, "push connection error: {e}");
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    }
}
