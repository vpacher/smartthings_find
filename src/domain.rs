//! The types integrations see. They describe devices and what happened to
//! them, independent of how Samsung's API encodes it, so an integration
//! (MQTT, REST, a database writer, ...) never touches `model.rs`.

use chrono::{DateTime, Utc};
use serde::Serialize;

/// Samsung's `dvceID`: stable for a device across renames.
pub type DeviceId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceKind {
    Tag,
    Phone,
    Tablet,
    Watch,
    Earbuds,
    Other,
}

impl DeviceKind {
    /// From the device list's `deviceType` field.
    pub fn from_samsung(device_type: Option<&str>) -> Self {
        match device_type {
            Some("TAG") => Self::Tag,
            Some("PHONE") => Self::Phone,
            Some("TAB") => Self::Tablet,
            Some("WATCH") => Self::Watch,
            Some("BUDS") => Self::Earbuds,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Location {
    pub latitude: f64,
    pub longitude: f64,
    /// Horizontal accuracy radius in metres.
    pub accuracy_m: Option<f64>,
    /// When the device was at this position (not when we read it).
    pub fixed_at: DateTime<Utc>,
    /// Samsung's operation type the fix came from (`LOCATION`, `OFFLINE_LOC`, ...).
    pub source: String,
}

/// Everything known about one device. Integrations get the full state on
/// every change, so they never have to merge partial updates.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DeviceState {
    pub id: DeviceId,
    pub name: String,
    pub kind: DeviceKind,
    pub model: Option<String>,
    pub location: Option<Location>,
    pub battery_pct: Option<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    /// Not checked yet (service just started).
    Unknown,
    Valid,
    /// The JSESSIONID is no longer accepted. The engine re-reads the cookie
    /// source periodically and recovers when a new cookie appears.
    Expired,
}

/// The session cookie in use, by fingerprint only (never the value), and the
/// outcome of the last `Command::SetCookie`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CookieInfo {
    pub fingerprint: String,
    pub last_set: Option<CookieSet>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CookieSet {
    pub fingerprint: String,
    pub accepted: bool,
    /// Why the cookie was not accepted.
    pub reason: Option<String>,
    pub at: DateTime<Utc>,
}

/// A JSESSIONID value handed in by an integration. `Debug` shows only the
/// fingerprint, so logging a command never leaks the cookie.
#[derive(Clone, PartialEq)]
pub struct NewCookie(pub String);

impl std::fmt::Debug for NewCookie {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NewCookie({})", crate::session::fingerprint(self.0.trim()))
    }
}

/// Progress of a `Command::Locate`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LocateStatus {
    /// No Locate since the service started. Published when a device is
    /// first seen, so integrations never show "unknown".
    Idle,
    /// Samsung accepted the request; follow-up reads are scheduled.
    Requested,
    /// Samsung refused the request (`resultCode` other than "00").
    Rejected { reason: String },
    /// A newer fix arrived after the request.
    Located { fixed_at: DateTime<Utc> },
    /// All follow-up reads done, no newer fix. Normal for a tag that no
    /// Galaxy device has heard since.
    NoNewFix,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    /// A device appeared or its state changed (full state).
    Device(DeviceState),
    /// A device was dropped after repeated absence from the device list.
    DeviceRemoved { id: DeviceId },
    Session { status: SessionStatus },
    Cookie(CookieInfo),
    Locate { id: DeviceId, status: LocateStatus },
}

/// What integrations can ask the engine to do. Results come back as events.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Ask for a fresh fix, then re-read the device a few times.
    Locate(DeviceId),
    /// Start ringing (`start: false` stops; phones only).
    Ring { id: DeviceId, start: bool },
    /// Run a poll cycle now instead of waiting for the next tick.
    Refresh,
    /// Check a new JSESSIONID with Samsung. If it works, use it and save it
    /// to the cookie file; if not, keep the current one.
    SetCookie(NewCookie),
}
