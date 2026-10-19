//! Response shapes of the SmartThings Find web API.
//!
//! The API is reverse-engineered and undocumented, so parsing is lenient:
//! numbers may arrive as strings or JSON numbers, and unknown fields are
//! kept (`extra`) so a `--raw` dump can show what else is there.

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct DeviceList {
    #[serde(rename = "deviceList", default)]
    pub devices: Vec<Device>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Device {
    #[serde(rename = "dvceID")]
    pub id: String,
    #[serde(rename = "usrId")]
    pub user_id: String,
    #[serde(rename = "modelName", default)]
    pub model_name: String,
    #[serde(rename = "modelID", default)]
    pub model_id: Option<String>,
    #[serde(rename = "deviceTypeCode", default)]
    pub type_code: Option<String>,
    /// Coarser than `type_code`: `TAG`, `PHONE`, `TAB`, `WATCH`, `BUDS`.
    #[serde(rename = "deviceType", default)]
    pub device_type: Option<String>,
}

impl Device {
    pub fn is_tag(&self) -> bool {
        self.type_code.as_deref() == Some("TAG")
    }

    /// Display name. The API double-escapes HTML entities
    /// (`Alex&amp;#39;s S10` → `Alex's S10`).
    pub fn name(&self) -> String {
        unescape(&unescape(&self.model_name))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct LastSelect {
    #[serde(default)]
    pub operation: Vec<Value>,
    /// Per-device credentials for Samsung's push broker, which the web app
    /// uses to learn when an operation (location, ring, ...) has a result.
    #[serde(flatten)]
    pub push: Option<PushCredentials>,
}

/// Fields of the `setLastSelect.do` response the web app passes to mqtt.js:
/// `connect(wss://{mqttUrl}/mqtt, {clientId, username: mquserName,
/// password: mquserCode, will: {topic: mqttWillTopic}})`, then
/// `subscribe(topic)`.
#[derive(Clone, Deserialize)]
pub struct PushCredentials {
    #[serde(rename = "mqttUrl")]
    pub url: String,
    #[serde(rename = "mqttClientId")]
    pub client_id: String,
    #[serde(rename = "mquserName")]
    pub username: String,
    #[serde(rename = "mquserCode")]
    pub password: String,
    #[serde(rename = "mqttWillTopic")]
    pub will_topic: String,
    pub topic: String,
}

/// Never print the password.
impl std::fmt::Debug for PushCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushCredentials")
            .field("url", &self.url)
            .field("topic", &self.topic)
            .field("will_topic", &self.will_topic)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Location {
    pub source: String,
    pub latitude: f64,
    pub longitude: f64,
    /// Horizontal uncertainty in metres. The Python integration combined
    /// horizontal and vertical (sqrt(h²+v²)); HA's `gps_accuracy` is a
    /// horizontal radius, so vertical is ignored here.
    pub accuracy_m: Option<f64>,
    pub fixed_at: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct Snapshot {
    pub location: Option<Location>,
    pub battery_pct: Option<u8>,
    /// Location entries skipped because the payload is end-to-end encrypted.
    pub encrypted_skipped: usize,
    pub op_types: Vec<String>,
}

impl LastSelect {
    pub fn snapshot(&self) -> Snapshot {
        let mut snap = Snapshot::default();
        for op in &self.operation {
            let kind = op.get("oprnType").and_then(Value::as_str).unwrap_or("?");
            snap.op_types.push(kind.to_string());
            match kind {
                "LOCATION" | "LASTLOC" | "OFFLINE_LOC" => match parse_location(kind, op) {
                    Parsed::Ok(loc) => {
                        let newer = snap.location.as_ref().is_none_or(|cur| loc.fixed_at > cur.fixed_at);
                        if newer {
                            snap.location = Some(loc);
                        }
                    }
                    Parsed::Encrypted => snap.encrypted_skipped += 1,
                    Parsed::Unusable => {}
                },
                "CHECK_CONNECTION" => {
                    if let Some(b) = op.get("battery") {
                        snap.battery_pct = parse_battery(b);
                    }
                }
                _ => {}
            }
        }
        snap
    }
}

enum Parsed {
    Ok(Location),
    Encrypted,
    Unusable,
}

/// Coordinates sit either on the operation itself (date in `extra.gpsUtcDt`)
/// or inside `encLocation` (date in `encLocation.gpsUtcDt`), which may be
/// flagged `encrypted`.
fn parse_location(kind: &str, op: &Value) -> Parsed {
    let (body, date) = if op.get("latitude").is_some() {
        (op, op.pointer("/extra/gpsUtcDt"))
    } else if let Some(enc) = op.get("encLocation") {
        if enc.get("encrypted").and_then(Value::as_bool).unwrap_or(false) {
            return Parsed::Encrypted;
        }
        (enc, enc.get("gpsUtcDt"))
    } else {
        return Parsed::Unusable;
    };
    let (Some(lat), Some(lon), Some(fixed_at)) = (
        body.get("latitude").and_then(num),
        body.get("longitude").and_then(num),
        date.and_then(Value::as_str).and_then(parse_stf_date),
    ) else {
        return Parsed::Unusable;
    };
    Parsed::Ok(Location {
        source: kind.to_string(),
        latitude: lat,
        longitude: lon,
        accuracy_m: body.get("horizontalUncertainty").and_then(num),
        fixed_at,
    })
}

fn parse_battery(v: &Value) -> Option<u8> {
    match v.as_str() {
        Some("FULL") => Some(100),
        Some("MEDIUM") => Some(50),
        Some("LOW") => Some(15),
        Some("VERY_LOW") => Some(5),
        _ => num(v).filter(|n| (0.0..=100.0).contains(n)).map(|n| n as u8),
    }
}

/// A JSON number, or a string holding one.
fn num(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

/// SmartThings Find timestamps are `YYYYMMDDhhmmss` in UTC.
pub fn parse_stf_date(s: &str) -> Option<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(s, "%Y%m%d%H%M%S").ok().map(|n| n.and_utc())
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let Some(end) = rest.find(';').filter(|&e| e <= 10) else {
            out.push('&');
            rest = &rest[1..];
            continue;
        };
        let entity = &rest[1..end];
        let decoded = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => entity
                .strip_prefix("#x")
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .or_else(|| entity.strip_prefix('#').and_then(|d| d.parse().ok()))
                .and_then(char::from_u32),
        };
        match decoded {
            Some(c) => {
                out.push(c);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn double_escaped_name() {
        let d = Device {
            id: "x".into(),
            user_id: "u".into(),
            model_name: "Alex&amp;#39;s S10".into(),
            model_id: None,
            type_code: None,
            device_type: None,
        };
        assert_eq!(d.name(), "Alex's S10");
        assert_eq!(unescape("a & b &bogus; &#x41;"), "a & b &bogus; A");
    }

    #[test]
    fn picks_newest_plain_location_and_skips_encrypted() {
        let resp: LastSelect = serde_json::from_value(json!({
            "operation": [
                { "oprnType": "LASTLOC", "latitude": "48.8", "longitude": "2.3",
                  "horizontalUncertainty": "30", "extra": { "gpsUtcDt": "20261010080000" } },
                { "oprnType": "LOCATION", "latitude": 48.85, "longitude": 2.35,
                  "horizontalUncertainty": 12.0, "extra": { "gpsUtcDt": "20261010090000" } },
                { "oprnType": "OFFLINE_LOC", "encLocation": { "encrypted": true, "gpsUtcDt": "20261010100000" } },
                { "oprnType": "CHECK_CONNECTION", "battery": "MEDIUM" }
            ]
        }))
        .unwrap();
        let s = resp.snapshot();
        let loc = s.location.unwrap();
        assert_eq!(loc.source, "LOCATION");
        assert_eq!(loc.latitude, 48.85);
        assert_eq!(loc.accuracy_m, Some(12.0));
        assert_eq!(s.battery_pct, Some(50));
        assert_eq!(s.encrypted_skipped, 1);
    }

    #[test]
    fn unencrypted_enc_location_is_used() {
        let resp: LastSelect = serde_json::from_value(json!({
            "operation": [
                { "oprnType": "OFFLINE_LOC", "encLocation": {
                    "encrypted": false, "latitude": "1.5", "longitude": "2.5", "gpsUtcDt": "20261010100000" } }
            ]
        }))
        .unwrap();
        let loc = resp.snapshot().location.unwrap();
        assert_eq!((loc.latitude, loc.longitude), (1.5, 2.5));
    }

    #[test]
    fn numeric_battery() {
        assert_eq!(parse_battery(&json!("87")), Some(87));
        assert_eq!(parse_battery(&json!(250)), None);
    }
}
