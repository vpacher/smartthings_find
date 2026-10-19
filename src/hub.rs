//! The interface between the engine and integrations, plus the
//! `Integration` trait itself.
//!
//! An integration gets a `Hub` and can:
//! - `subscribe()` to a stream of `Event`s,
//! - read a `snapshot()` of the current state (on start or reconnect),
//! - `send()` a `Command` (Locate, Ring, Refresh).
//!
//! Subscribe first, then read the snapshot: an event that arrives in
//! between is then seen twice, never missed. Events carry full state, so
//! handling one twice is harmless.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use tokio::sync::{broadcast, mpsc};

use crate::domain::{Command, CookieInfo, DeviceId, DeviceState, Event, SessionStatus};

/// Events buffered per subscriber. A subscriber that falls further behind
/// gets `RecvError::Lagged` and should re-read the snapshot.
const EVENT_BUFFER: usize = 256;

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub session: SessionStatus,
    pub cookie: Option<CookieInfo>,
    /// End of the last poll cycle that completed without error.
    pub last_poll_ok: Option<DateTime<Utc>>,
    pub devices: BTreeMap<DeviceId, DeviceState>,
}

#[derive(Clone)]
pub struct Hub {
    state: Arc<RwLock<Snapshot>>,
    events: broadcast::Sender<Event>,
    commands: mpsc::Sender<Command>,
}

impl Hub {
    /// Creates the hub and the engine's end of the command channel.
    pub(crate) fn new() -> (Self, mpsc::Receiver<Command>) {
        let (events, _) = broadcast::channel(EVENT_BUFFER);
        let (commands, rx) = mpsc::channel(32);
        let state = Snapshot { session: SessionStatus::Unknown, cookie: None, last_poll_ok: None, devices: BTreeMap::new() };
        (Self { state: Arc::new(RwLock::new(state)), events, commands }, rx)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    pub fn snapshot(&self) -> Snapshot {
        self.state.read().expect("hub state lock poisoned").clone()
    }

    pub async fn send(&self, cmd: Command) -> Result<()> {
        self.commands.send(cmd).await.map_err(|_| anyhow!("engine stopped"))
    }

    /// Engine side: records a completed poll cycle (no event; health checks
    /// read it from the snapshot).
    pub(crate) fn poll_ok(&self) {
        self.state.write().expect("hub state lock poisoned").last_poll_ok = Some(Utc::now());
    }

    /// Engine side: updates the snapshot, then broadcasts. Having no
    /// subscribers is fine.
    pub(crate) fn publish(&self, event: Event) {
        {
            let mut s = self.state.write().expect("hub state lock poisoned");
            match &event {
                Event::Device(d) => {
                    s.devices.insert(d.id.clone(), d.clone());
                }
                Event::DeviceRemoved { id } => {
                    s.devices.remove(id);
                }
                Event::Session { status } => s.session = *status,
                Event::Cookie(c) => s.cookie = Some(c.clone()),
                Event::Locate { .. } => {}
            }
        }
        let _ = self.events.send(event);
    }
}

pub type BoxFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// An output for SmartThings Find data: MQTT, a REST API, a database
/// writer, ... Implement this in one module (or your own crate depending
/// on this library) and register it in the binary.
pub trait Integration: Send {
    /// Short name for logs.
    fn name(&self) -> &'static str;

    /// Runs until the integration stops. Returning `Err` stops the service,
    /// so handle recoverable problems (a broker reconnect) inside.
    fn run(self: Box<Self>, hub: Hub) -> BoxFuture;
}
