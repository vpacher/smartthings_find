//! The engine: the only part that talks to Samsung. It polls last-known
//! locations, runs commands from integrations, and publishes the results
//! to the `Hub` as `Event`s.
//!
//! Locate flow: `addOperation` (operation per device type, see
//! `Client::request_location`), then follow-up reads of that one device at
//! `locate_followups`; for tags also the web app's `getTagLocation`
//! fallback. The first read with a fix newer than the one before the
//! request ends the locate (`Located`); otherwise `NoNewFix` after the
//! last follow-up.
//!
//! Session expiry does not stop the engine: it reports `Expired`, re-reads
//! the cookie source every `cookie_recheck`, and resumes when a new cookie
//! appears.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::api::{Client, OperationRejected, is_session_expired};
use crate::domain::{Command, CookieInfo, CookieSet, DeviceId, DeviceKind, DeviceState, Event, LocateStatus, SessionStatus};
use crate::hub::Hub;
use crate::model::{Device, Snapshot};
use crate::roster::Roster;

/// Device-list reads before a per-device call gives up on the device
/// being in the list.
const PRESENCE_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// How often to read every device's last-known location. Passive reads
    /// cost no device battery.
    pub poll_interval: Duration,
    /// While the session is expired: how often to re-read the cookie source.
    pub cookie_recheck: Duration,
    /// Re-reads of a device after a Locate request, relative to the request.
    pub locate_followups: Vec<Duration>,
    /// When to call the tag-only `getTagLocation` fallback after a Locate.
    pub tag_fallback_after: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(120),
            cookie_recheck: Duration::from_secs(30),
            locate_followups: [3, 10, 30, 60, 120].map(Duration::from_secs).to_vec(),
            // Same as the web app's fallback timer for CHECK_CONNECTION_WITH_LOCATION.
            tag_fallback_after: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FollowupKind {
    Read,
    TagFallback,
}

struct Followup {
    at: Instant,
    id: DeviceId,
    kind: FollowupKind,
}

/// A Locate in progress: done when a fix newer than `baseline` arrives.
struct PendingLocate {
    baseline: Option<DateTime<Utc>>,
}

pub struct Engine {
    client: Client,
    hub: Hub,
    commands: mpsc::Receiver<Command>,
    config: EngineConfig,
    roster: Roster,
    states: BTreeMap<DeviceId, DeviceState>,
    followups: Vec<Followup>,
    locates: BTreeMap<DeviceId, PendingLocate>,
    session: SessionStatus,
    /// For the lifetime log line when the session expires.
    session_first_ok: Option<DateTime<Utc>>,
    session_last_ok: Option<DateTime<Utc>>,
    /// Last published cookie info (fingerprint changes on rotation or
    /// `SetCookie`).
    cookie: Option<CookieInfo>,
}

impl Engine {
    pub fn new(client: Client, config: EngineConfig) -> (Self, Hub) {
        let (hub, commands) = Hub::new();
        let engine = Self {
            client,
            hub: hub.clone(),
            commands,
            config,
            roster: Roster::default(),
            states: BTreeMap::new(),
            followups: Vec::new(),
            locates: BTreeMap::new(),
            session: SessionStatus::Unknown,
            session_first_ok: None,
            session_last_ok: None,
            cookie: None,
        };
        (engine, hub)
    }

    pub async fn run(mut self) -> Result<()> {
        let mut poll = tokio::time::interval(self.config.poll_interval);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut recheck = tokio::time::interval(self.config.cookie_recheck);
        recheck.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            let next_followup = self.followups.iter().map(|f| f.at).min();
            let expired = self.session == SessionStatus::Expired;
            tokio::select! {
                _ = poll.tick(), if !expired => {
                    let r = self.poll_cycle().await;
                    self.handle(r);
                }
                _ = recheck.tick(), if expired => self.try_recover().await,
                cmd = self.commands.recv() => match cmd {
                    Some(cmd) => {
                        let r = self.command(cmd).await;
                        self.handle(r);
                    }
                    None => return Ok(()),
                },
                _ = tokio::time::sleep_until(next_followup.unwrap_or_else(Instant::now)), if next_followup.is_some() && !expired => {
                    let r = self.due_followups().await;
                    self.handle(r);
                }
            }
        }
    }

    /// Central error handling: expiry switches to recovery mode, anything
    /// else is logged and the next tick tries again.
    fn handle(&mut self, r: Result<()>) {
        match r {
            Ok(()) => {}
            Err(e) if is_session_expired(&e) => {
                error!("{e:#}");
                if let (Some(first), Some(last)) = (self.session_first_ok, self.session_last_ok) {
                    error!(
                        "session {} worked from {} to {} ({}m)",
                        self.client.session_fingerprint(),
                        first.format("%F %T UTC"),
                        last.format("%F %T UTC"),
                        (last - first).num_minutes()
                    );
                }
                error!("waiting for a new JSESSIONID in the cookie source (re-checked every {}s)", self.config.cookie_recheck.as_secs());
                self.followups.clear();
                for id in std::mem::take(&mut self.locates).into_keys() {
                    self.hub.publish(Event::Locate { id, status: LocateStatus::Rejected { reason: "session expired".into() } });
                }
                self.set_session(SessionStatus::Expired);
            }
            Err(e) => warn!("{e:#}"),
        }
    }

    fn set_session(&mut self, status: SessionStatus) {
        if status == SessionStatus::Valid {
            let now = Utc::now();
            self.session_first_ok.get_or_insert(now);
            self.session_last_ok = Some(now);
        }
        if self.session != status {
            info!(?status, "session status");
            self.session = status;
            self.hub.publish(Event::Session { status });
        }
        if status == SessionStatus::Valid {
            let fingerprint = self.client.session_fingerprint();
            if self.cookie.as_ref().is_none_or(|c| c.fingerprint != fingerprint) {
                let last_set = self.cookie.take().and_then(|c| c.last_set);
                self.publish_cookie(CookieInfo { fingerprint, last_set });
            }
        }
    }

    fn publish_cookie(&mut self, info: CookieInfo) {
        self.cookie = Some(info.clone());
        self.hub.publish(Event::Cookie(info));
    }

    /// `Command::SetCookie`: checks the new cookie with Samsung before it
    /// replaces the current one. Works while the session is expired too.
    async fn set_cookie(&mut self, raw: &str) {
        let fresh = match self.client.session().with_cookie(raw) {
            Ok(s) => s,
            Err(e) => return self.cookie_result(crate::session::fingerprint(raw.trim()), Some(format!("{e:#}"))),
        };
        let fingerprint = fresh.session_fingerprint();
        info!(%fingerprint, "new session cookie handed in, checking it");
        let old = self.client.replace_session(fresh);
        if let Err(e) = self.client.refresh_csrf().await {
            warn!(%fingerprint, "new cookie not accepted, keeping the current one: {e:#}");
            self.client.replace_session(old);
            let reason = if is_session_expired(&e) {
                "Samsung did not accept this cookie".to_string()
            } else {
                format!("could not check the cookie: {e:#}")
            };
            return self.cookie_result(fingerprint, Some(reason));
        }
        if let Err(e) = self.client.session().save() {
            warn!("new cookie works but was not saved: {e:#}");
        }
        self.session_first_ok = None;
        self.set_session(SessionStatus::Valid);
        self.cookie_result(fingerprint, None);
        let r = self.poll_cycle().await;
        self.handle(r);
    }

    fn cookie_result(&mut self, fingerprint: String, reason: Option<String>) {
        let current = self.client.session_fingerprint();
        let last_set = CookieSet { fingerprint, accepted: reason.is_none(), reason, at: Utc::now() };
        self.publish_cookie(CookieInfo { fingerprint: current, last_set: Some(last_set) });
    }

    async fn try_recover(&mut self) {
        let Some(fresh) = self.client.session().reload_if_changed() else { return };
        info!("new session cookie found, checking it");
        let _old = self.client.replace_session(fresh);
        match self.client.refresh_csrf().await {
            Ok(()) => {
                self.session_first_ok = None;
                self.set_session(SessionStatus::Valid);
                let r = self.poll_cycle().await;
                self.handle(r);
            }
            Err(e) => warn!("new cookie not accepted either: {e:#}"),
        }
    }

    /// Reads the device list and merges it into the roster.
    async fn refresh_devices(&mut self) -> Result<()> {
        let fresh = self.client.devices().await?;
        self.set_session(SessionStatus::Valid);
        for gone in self.roster.merge(fresh) {
            self.states.remove(&gone.id);
            self.locates.remove(&gone.id);
            self.followups.retain(|f| f.id != gone.id);
            self.hub.publish(Event::DeviceRemoved { id: gone.id });
        }
        Ok(())
    }

    /// Per-device calls only work for devices in the session's latest
    /// device list (the backend answers 404 otherwise), and the list omits
    /// devices at random. Re-reads the list until it includes `id`.
    async fn ensure_present(&mut self, id: &str) -> Result<bool> {
        for _ in 0..PRESENCE_ATTEMPTS {
            if self.roster.present().any(|d| d.id == id) {
                return Ok(true);
            }
            self.refresh_devices().await?;
        }
        Ok(self.roster.present().any(|d| d.id == id))
    }

    async fn poll_cycle(&mut self) -> Result<()> {
        self.refresh_devices().await?;
        // Only devices in this response: the backend 404s the others.
        let present: Vec<Device> = self.roster.present().cloned().collect();
        for dev in &present {
            match self.client.last_select(dev).await {
                Ok(resp) => self.apply(dev, &resp.snapshot()),
                Err(e) if is_session_expired(&e) => return Err(e),
                Err(e) => warn!(device = %dev.name(), "read failed: {e:#}"),
            }
        }
        self.hub.poll_ok();
        // Devices known but absent from this response keep their state;
        // publish ones never seen in a read yet so integrations know them.
        for dev in self.roster.devices().cloned().collect::<Vec<_>>() {
            if !self.states.contains_key(&dev.id) {
                self.apply(&dev, &Snapshot::default());
            }
        }
        Ok(())
    }

    /// Merges a read into the device's state and publishes if it changed.
    fn apply(&mut self, dev: &Device, snap: &Snapshot) {
        let state = self.states.entry(dev.id.clone()).or_insert_with(|| DeviceState {
            id: dev.id.clone(),
            name: String::new(),
            kind: DeviceKind::Other,
            model: None,
            location: None,
            battery_pct: None,
        });
        let before = (!state.name.is_empty()).then(|| state.clone());

        state.name = dev.name();
        state.kind = DeviceKind::from_samsung(dev.device_type.as_deref());
        state.model = dev.model_id.clone();
        if let Some(loc) = &snap.location {
            let newer = state.location.as_ref().is_none_or(|cur| loc.fixed_at > cur.fixed_at);
            if newer {
                state.location = Some(crate::domain::Location {
                    latitude: loc.latitude,
                    longitude: loc.longitude,
                    accuracy_m: loc.accuracy_m,
                    fixed_at: loc.fixed_at,
                    source: loc.source.clone(),
                });
            }
        }
        if snap.battery_pct.is_some() {
            state.battery_pct = snap.battery_pct;
        }

        let fixed_at = state.location.as_ref().map(|l| l.fixed_at);
        let first_seen = before.is_none();
        if before.as_ref() != Some(state) {
            self.hub.publish(Event::Device(state.clone()));
        }
        if first_seen {
            self.hub.publish(Event::Locate { id: dev.id.clone(), status: LocateStatus::Idle });
        }

        // A pending Locate ends with the first fix newer than its baseline.
        if let (Some(pending), Some(fixed_at)) = (self.locates.get(&dev.id), fixed_at)
            && pending.baseline.is_none_or(|b| fixed_at > b)
        {
            self.locates.remove(&dev.id);
            self.followups.retain(|f| f.id != dev.id);
            self.hub.publish(Event::Locate { id: dev.id.clone(), status: LocateStatus::Located { fixed_at } });
        }
    }

    async fn command(&mut self, cmd: Command) -> Result<()> {
        info!(?cmd, "command");
        if let Command::SetCookie(c) = &cmd {
            self.set_cookie(&c.0).await;
            return Ok(());
        }
        if self.session == SessionStatus::Expired {
            if let Command::Locate(id) = cmd {
                self.hub.publish(Event::Locate { id, status: LocateStatus::Rejected { reason: "session expired".into() } });
            } else {
                warn!("ignoring command while the session is expired");
            }
            return Ok(());
        }
        match cmd {
            Command::Refresh => self.poll_cycle().await,
            Command::Ring { id, start } => {
                let Some(dev) = self.roster.get(&id).cloned() else {
                    warn!(%id, "ring: unknown device");
                    return Ok(());
                };
                self.ensure_present(&id).await?;
                self.client.ring(&dev, start).await
            }
            Command::Locate(id) => self.locate(id).await,
            Command::SetCookie(_) => unreachable!("handled above"),
        }
    }

    async fn locate(&mut self, id: DeviceId) -> Result<()> {
        let Some(dev) = self.roster.get(&id).cloned() else {
            warn!(%id, "locate: unknown device");
            return Ok(());
        };
        if self.locates.contains_key(&id) {
            info!(device = %dev.name(), "locate already in progress");
            return Ok(());
        }
        self.ensure_present(&id).await?;
        match self.client.request_location(&dev).await {
            Ok(()) => {}
            Err(e) if is_session_expired(&e) => return Err(e),
            Err(e) => {
                let reason = match e.downcast_ref::<OperationRejected>() {
                    Some(r) => format!("rejected by Samsung (resultCode {})", r.result_code),
                    None => format!("{e:#}"),
                };
                self.hub.publish(Event::Locate { id, status: LocateStatus::Rejected { reason } });
                return Ok(());
            }
        }
        let baseline = self.states.get(&id).and_then(|s| s.location.as_ref()).map(|l| l.fixed_at);
        self.locates.insert(id.clone(), PendingLocate { baseline });
        let now = Instant::now();
        for d in &self.config.locate_followups {
            self.followups.push(Followup { at: now + *d, id: id.clone(), kind: FollowupKind::Read });
        }
        if dev.is_tag() {
            self.followups.push(Followup { at: now + self.config.tag_fallback_after, id: id.clone(), kind: FollowupKind::TagFallback });
        }
        self.hub.publish(Event::Locate { id, status: LocateStatus::Requested });
        Ok(())
    }

    async fn due_followups(&mut self) -> Result<()> {
        let now = Instant::now();
        let (due, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut self.followups).into_iter().partition(|f| f.at <= now);
        self.followups = rest;
        for f in due {
            // An earlier follow-up in this batch may have finished the locate.
            if !self.locates.contains_key(&f.id) {
                continue;
            }
            let Some(dev) = self.roster.get(&f.id).cloned() else { continue };
            if !self.ensure_present(&f.id).await? {
                warn!(device = %dev.name(), "device missing from the device list, skipping this follow-up");
            }
            let result = match f.kind {
                FollowupKind::Read => self.client.last_select(&dev).await,
                FollowupKind::TagFallback => {
                    let since = self
                        .states
                        .get(&f.id)
                        .and_then(|s| s.location.as_ref())
                        .map_or_else(|| "00000000".to_string(), |l| l.fixed_at.format("%Y%m%d%H%M%S").to_string());
                    self.client.tag_location(&dev, &since).await
                }
            };
            match result {
                Ok(resp) => self.apply(&dev, &resp.snapshot()),
                Err(e) if is_session_expired(&e) => return Err(e),
                Err(e) => warn!(device = %dev.name(), kind = ?f.kind, "locate follow-up failed: {e:#}"),
            }
            if self.locates.contains_key(&f.id) && !self.followups.iter().any(|g| g.id == f.id) {
                self.locates.remove(&f.id);
                self.hub.publish(Event::Locate { id: f.id, status: LocateStatus::NoNewFix });
            }
        }
        Ok(())
    }
}
