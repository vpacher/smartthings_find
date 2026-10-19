//! Push heartbeat for an uptime monitor, e.g. an Uptime Kuma "Push" monitor.
//!
//! Enabled when `STF_HEARTBEAT_URL` is set. Every `EVERY` it calls that URL
//! with `status=up|down` and `msg=...` query parameters (any query in the
//! configured URL is replaced):
//! - `up`, `ok`: the session is valid and a poll succeeded recently
//! - `down`, `session expired`: Samsung no longer accepts the cookie
//! - `down`, `no successful poll since ...`: the engine is stuck or Samsung
//!   is unreachable
//!
//! If the whole service is down, no push arrives and the monitor alerts on
//! the missing heartbeat. The URL holds the monitor's token, so it is never
//! logged.

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Url;
use tracing::{info, warn};

use crate::domain::SessionStatus;
use crate::hub::{BoxFuture, Hub, Integration, Snapshot};

/// How often to push.
const EVERY: Duration = Duration::from_secs(60);

pub struct Heartbeat {
    url: Url,
    /// A poll older than this counts as "stuck".
    stale_after: chrono::Duration,
}

impl Heartbeat {
    /// `None` when `STF_HEARTBEAT_URL` is not set. `poll_interval` is the
    /// engine's; three missed polls count as stuck.
    pub fn from_env(poll_interval: Duration) -> Result<Option<Self>> {
        let Some(raw) = std::env::var("STF_HEARTBEAT_URL").ok().filter(|v| !v.trim().is_empty()) else {
            return Ok(None);
        };
        // Do not echo the value: it contains the monitor's token.
        let mut url = Url::parse(raw.trim()).context("STF_HEARTBEAT_URL is not a valid URL")?;
        url.set_query(None);
        let stale_after = chrono::Duration::from_std(poll_interval * 3)? + chrono::Duration::seconds(60);
        Ok(Some(Self { url, stale_after }))
    }
}

impl Integration for Heartbeat {
    fn name(&self) -> &'static str {
        "heartbeat"
    }

    fn run(self: Box<Self>, hub: Hub) -> BoxFuture {
        Box::pin(async move {
            let http = reqwest::Client::builder()
                .user_agent(concat!("smartthings_find/", env!("CARGO_PKG_VERSION")))
                .timeout(Duration::from_secs(10))
                .build()?;
            let started = Utc::now();
            let mut last_up: Option<bool> = None;
            let mut tick = tokio::time::interval(EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let Some((up, msg)) = verdict(&hub.snapshot(), started, Utc::now(), self.stale_after) else {
                    continue;
                };
                if last_up != Some(up) {
                    info!(up, %msg, "heartbeat status");
                    last_up = Some(up);
                }
                let mut url = self.url.clone();
                url.query_pairs_mut()
                    .append_pair("status", if up { "up" } else { "down" })
                    .append_pair("msg", &msg)
                    .append_pair("ping", "");
                // A failed push must not stop the service: the monitor
                // reports the gap itself.
                match http.get(url).send().await {
                    Ok(r) if r.status().is_success() => {}
                    Ok(r) => warn!(status = %r.status(), "heartbeat push rejected"),
                    Err(e) => warn!("heartbeat push failed: {}", e.without_url()),
                }
            }
        })
    }
}

/// What to report, or `None` while the service is still starting.
fn verdict(snap: &Snapshot, started: DateTime<Utc>, now: DateTime<Utc>, stale_after: chrono::Duration) -> Option<(bool, String)> {
    if snap.session == SessionStatus::Expired {
        return Some((false, "session expired".into()));
    }
    match snap.last_poll_ok {
        Some(t) if now - t <= stale_after => Some((true, "ok".into())),
        Some(t) => Some((false, format!("no successful poll since {}", t.format("%F %T UTC")))),
        None if now - started <= stale_after => None,
        None => Some((false, "no successful poll since start".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn snap(session: SessionStatus, last_poll_ok: Option<DateTime<Utc>>) -> Snapshot {
        Snapshot { session, cookie: None, last_poll_ok, devices: BTreeMap::new() }
    }

    #[test]
    fn verdicts() {
        let t0 = Utc::now();
        let stale = chrono::Duration::minutes(7);
        let min = chrono::Duration::minutes;
        // Starting: nothing to say yet, then down if no poll ever succeeds.
        assert_eq!(verdict(&snap(SessionStatus::Unknown, None), t0, t0 + min(1), stale), None);
        assert!(!verdict(&snap(SessionStatus::Unknown, None), t0, t0 + min(8), stale).unwrap().0);
        // Healthy, then stuck.
        assert_eq!(verdict(&snap(SessionStatus::Valid, Some(t0)), t0, t0 + min(2), stale), Some((true, "ok".into())));
        let (up, msg) = verdict(&snap(SessionStatus::Valid, Some(t0)), t0, t0 + min(8), stale).unwrap();
        assert!(!up && msg.starts_with("no successful poll since"));
        // Expired wins over a recent poll.
        assert_eq!(
            verdict(&snap(SessionStatus::Expired, Some(t0)), t0, t0 + min(1), stale),
            Some((false, "session expired".into()))
        );
    }

    #[test]
    fn query_is_replaced_and_url_required() {
        // SAFETY: tests in this module are the only ones touching this variable.
        unsafe { std::env::set_var("STF_HEARTBEAT_URL", "https://kuma.example/api/push/abc?status=up&msg=OK&ping=") };
        let hb = Heartbeat::from_env(Duration::from_secs(120)).unwrap().unwrap();
        assert_eq!(hb.url.as_str(), "https://kuma.example/api/push/abc");
        assert_eq!(hb.stale_after, chrono::Duration::seconds(420));
        unsafe { std::env::set_var("STF_HEARTBEAT_URL", "not a url") };
        assert!(Heartbeat::from_env(Duration::from_secs(120)).is_err());
        unsafe { std::env::remove_var("STF_HEARTBEAT_URL") };
        assert!(Heartbeat::from_env(Duration::from_secs(120)).unwrap().is_none());
    }
}
