//! Logs every event. The smallest possible integration, and the template
//! for writing a new one: subscribe, (optionally) read the snapshot, loop
//! over events, send commands through the hub if needed.

use chrono::Utc;
use tokio::sync::broadcast::error::RecvError;
use tracing::{info, warn};

use crate::domain::Event;
use crate::hub::{BoxFuture, Hub, Integration};

pub struct Log;

impl Integration for Log {
    fn name(&self) -> &'static str {
        "log"
    }

    fn run(self: Box<Self>, hub: Hub) -> BoxFuture {
        Box::pin(async move {
            let mut events = hub.subscribe();
            loop {
                match events.recv().await {
                    Ok(event) => log(&event),
                    Err(RecvError::Lagged(n)) => warn!(skipped = n, "log integration fell behind"),
                    Err(RecvError::Closed) => return Ok(()),
                }
            }
        })
    }
}

fn log(event: &Event) {
    match event {
        Event::Device(d) => match &d.location {
            Some(loc) => info!(
                device = %d.name,
                kind = ?d.kind,
                lat = loc.latitude,
                lon = loc.longitude,
                accuracy_m = loc.accuracy_m.map(f64::round),
                fix_age_min = (Utc::now() - loc.fixed_at).num_minutes(),
                battery = d.battery_pct,
                "device"
            ),
            None => info!(device = %d.name, kind = ?d.kind, battery = d.battery_pct, "device (no location yet)"),
        },
        Event::DeviceRemoved { id } => info!(%id, "device removed"),
        Event::Session { status } => info!(?status, "session"),
        Event::Cookie(c) => info!(fingerprint = %c.fingerprint, last_set = ?c.last_set, "session cookie"),
        Event::Locate { id, status } => info!(%id, ?status, "locate"),
    }
}
