//! Client for the SmartThings Find web backend (smartthingsfind.samsung.com).
//!
//! Endpoints, as reverse-engineered by the HA-SmartThings-Find integration:
//! - `GET  chkLogin.do`                 → CSRF token in the `_csrf` response header
//! - `POST device/getDeviceList.do`     → all devices on the account
//! - `POST dm/addOperation.do`          → queue an operation (location refresh, ring)
//! - `POST device/setLastSelect.do`     → latest operations/locations for one device
//!
//! Every POST carries `?_csrf=<token>`. The session itself is the
//! `JSESSIONID` cookie (see `session.rs`).

use std::fmt;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use reqwest::header::{ACCEPT, CONTENT_TYPE, COOKIE, HeaderMap};
use reqwest::{Method, StatusCode, redirect};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tracing::{debug, info, trace, warn};

use crate::model::{Device, DeviceList, LastSelect};
use crate::session::Session;

const BASE: &str = "https://smartthingsfind.samsung.com";
const DEVICE_LIST: &str = "device/getDeviceList.do";

/// The session cookie is no longer accepted. Only a new cookie from a
/// browser login fixes this.
#[derive(Debug)]
pub struct SessionExpired(pub String);

impl fmt::Display for SessionExpired {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "session expired: {}", self.0)
    }
}

impl std::error::Error for SessionExpired {}

/// Samsung refused an operation (`addOperation.do` `resultCode` other than
/// "00"), e.g. an operation the device type does not support.
#[derive(Debug)]
pub struct OperationRejected {
    pub result_code: String,
}

impl fmt::Display for OperationRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "operation rejected (resultCode {})", self.result_code)
    }
}

impl std::error::Error for OperationRejected {}

pub fn is_session_expired(e: &anyhow::Error) -> bool {
    e.downcast_ref::<SessionExpired>().is_some()
}

/// Response fields that are credentials and must not reach a log.
const SECRET_FIELDS: &[&str] = &["mquserCode"];

/// Pretty-prints a JSON body with `SECRET_FIELDS` replaced by their length.
fn redacted(body: &str) -> String {
    fn walk(v: &mut Value) {
        match v {
            Value::Object(map) => {
                for (k, v) in map.iter_mut() {
                    if SECRET_FIELDS.contains(&k.as_str()) {
                        let len = v.as_str().map_or(0, str::len);
                        *v = Value::String(format!("<redacted, {len} chars>"));
                    } else {
                        walk(v);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(walk),
            _ => {}
        }
    }
    match serde_json::from_str::<Value>(body) {
        Ok(mut v) => {
            walk(&mut v);
            serde_json::to_string_pretty(&v).unwrap_or_default()
        }
        Err(_) => body.to_string(),
    }
}

pub struct Client {
    http: reqwest::Client,
    session: Session,
    csrf: Option<String>,
    /// Print raw response bodies to stderr (for learning the API shape).
    pub raw: bool,
}

/// A request body. `Raw` is sent as-is with a JSON content type, the way
/// axios posts a string (used to forward MQTT push payloads unchanged).
#[derive(Clone)]
enum Body {
    Json(Value),
    Raw(String),
}

struct Response {
    status: StatusCode,
    headers: HeaderMap,
    body: String,
}

impl Response {
    /// What the backend does with a dead session: a redirect to the login
    /// page, 401, or a 200 with the literal body `Logout`.
    fn looks_logged_out(&self) -> bool {
        self.status.is_redirection()
            || self.status == StatusCode::UNAUTHORIZED
            || self.body.trim() == "Logout"
    }
}

impl Client {
    pub fn new(session: Session) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("smartthings_find/", env!("CARGO_PKG_VERSION")))
            // A dead session redirects to the account login; following it
            // would turn "logged out" into a 200 with an HTML page.
            .redirect(redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { http, session, csrf: None, raw: false })
    }

    pub fn session_fingerprint(&self) -> String {
        self.session.session_fingerprint()
    }

    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Swaps in a new cookie and returns the old session. The CSRF token
    /// belongs to the old session, so it is dropped.
    pub fn replace_session(&mut self, session: Session) -> Session {
        self.csrf = None;
        std::mem::replace(&mut self.session, session)
    }

    async fn send(&mut self, method: Method, path: &str, body: Option<Body>) -> Result<Response> {
        let url = match (&self.csrf, method == Method::POST) {
            (Some(csrf), true) => format!("{BASE}/{path}?_csrf={csrf}"),
            _ => format!("{BASE}/{path}"),
        };
        let mut req = self
            .http
            .request(method.clone(), &url)
            .header(COOKIE, self.session.cookie_header())
            .header(ACCEPT, "application/json");
        req = match body {
            Some(Body::Json(b)) => req.json(&b),
            Some(Body::Raw(b)) => req.header(CONTENT_TYPE, "application/json").body(b),
            // getDeviceList is called with an empty form body by the web app.
            None if method == Method::POST => req.form(&[] as &[(&str, &str)]),
            None => req,
        };
        let resp = req.send().await.with_context(|| format!("{method} {path}"))?;
        let status = resp.status();
        let headers = resp.headers().clone();
        self.session.absorb(&headers);
        let body = resp.text().await.unwrap_or_default();
        debug!(%method, path, %status, bytes = body.len(), "response");
        if self.raw || tracing::enabled!(tracing::Level::TRACE) {
            let shown = redacted(&body);
            trace!(path, body = %shown, "response body");
            if self.raw {
                eprintln!("--- {method} {path} [{status}]\n{shown}");
            }
        }
        Ok(Response { status, headers, body })
    }


    /// Fetches a new CSRF token. Fails with `SessionExpired` if the session
    /// cookie is not accepted.
    pub async fn refresh_csrf(&mut self) -> Result<()> {
        let resp = self.send(Method::GET, "chkLogin.do", None).await?;
        let token = resp
            .headers
            .get("_csrf")
            .and_then(|v| v.to_str().ok())
            .filter(|t| !t.is_empty());
        match (resp.status, token) {
            (StatusCode::OK, Some(t)) => {
                self.csrf = Some(t.to_string());
                self.session.confirm();
                info!("session valid, CSRF token refreshed");
                Ok(())
            }
            (status, _) => Err(SessionExpired(format!(
                "chkLogin.do returned {status} without a _csrf header (body: {:.120})",
                resp.body.trim()
            ))
            .into()),
        }
    }

    /// POSTs with the CSRF token. If the response looks logged out, refreshes
    /// the token once and retries; if that also fails, the session is dead.
    async fn post(&mut self, path: &str, body: Option<Body>) -> Result<Response> {
        if self.csrf.is_none() {
            self.refresh_csrf().await?;
        }
        let resp = self.send(Method::POST, path, body.clone()).await?;
        // getDeviceList answers a stale token with 404. Other endpoints use
        // 404 for "device not in this session's device list" (see
        // roster.rs), which a new token does not fix.
        let stale_token = path == DEVICE_LIST && resp.status == StatusCode::NOT_FOUND;
        if !(resp.looks_logged_out() || stale_token) {
            if resp.status.is_success() {
                self.session.confirm();
            }
            return Ok(resp);
        }
        warn!(path, status = %resp.status, "request rejected, retrying with a new CSRF token");
        self.refresh_csrf().await?;
        let resp = self.send(Method::POST, path, body).await?;
        if resp.looks_logged_out() {
            return Err(SessionExpired(format!("{path} returned {} after CSRF refresh", resp.status)).into());
        }
        if resp.status.is_success() {
            self.session.confirm();
        }
        Ok(resp)
    }

    async fn post_json<T: DeserializeOwned>(&mut self, path: &str, body: Option<Body>) -> Result<T> {
        let resp = self.post(path, body).await?;
        if !resp.status.is_success() {
            return Err(anyhow!("{path} returned {}: {:.200}", resp.status, resp.body.trim()));
        }
        serde_json::from_str(&resp.body).with_context(|| format!("parsing {path} response: {:.200}", resp.body.trim()))
    }

    pub async fn devices(&mut self) -> Result<Vec<Device>> {
        let list: DeviceList = self.post_json(DEVICE_LIST, None).await?;
        Ok(list.devices)
    }

    /// Asks Samsung to get a fresh fix from the device. The result arrives
    /// later (push, or a later `last_select`), not in this response.
    ///
    /// The operation depends on the device type, as in the web app: tags get
    /// `CHECK_CONNECTION_WITH_LOCATION` (`sP`); phones and other devices get
    /// `CHECK_CONNECTION` (battery, reachability) plus `LOCATION` (`sT`).
    /// Sending `CHECK_CONNECTION_WITH_LOCATION` to a phone is refused with
    /// `resultCode "02"`.
    pub async fn request_location(&mut self, dev: &Device) -> Result<()> {
        if dev.is_tag() {
            return self.add_operation(dev, json!({ "operation": "CHECK_CONNECTION_WITH_LOCATION" })).await;
        }
        self.add_operation(dev, json!({ "operation": "CHECK_CONNECTION" })).await?;
        self.add_operation(dev, json!({ "operation": "LOCATION" })).await
    }

    /// Starts (or, for phones, stops) ringing. SmartTags cannot be stopped
    /// through this API; they stop by themselves.
    pub async fn ring(&mut self, dev: &Device, start: bool) -> Result<()> {
        self.add_operation(
            dev,
            json!({
                "operation": "RING",
                "status": if start { "start" } else { "stop" },
                "lockMessage": "Ringing from smartthings_find",
            }),
        )
        .await
    }

    /// Queues an operation. HTTP 200 alone does not mean it was accepted:
    /// the web app also requires `resultCode == "00"` in the body.
    async fn add_operation(&mut self, dev: &Device, mut op: Value) -> Result<()> {
        op["dvceId"] = json!(dev.id);
        op["usrId"] = json!(dev.user_id);
        let resp = self.post("dm/addOperation.do", Some(Body::Json(op))).await?;
        if !resp.status.is_success() {
            return Err(anyhow!("addOperation.do returned {}: {:.200}", resp.status, resp.body.trim()));
        }
        let v: Value = serde_json::from_str(&resp.body).unwrap_or(Value::Null);
        match v.get("resultCode").and_then(Value::as_str) {
            Some("00") => Ok(()),
            code => Err(OperationRejected { result_code: code.unwrap_or("none").to_string() }.into()),
        }
    }

    /// Fetches the result an MQTT push points at. The web app passes the
    /// message payload unchanged as the request body (`t.toString()`).
    pub async fn operation_result_rt(&mut self, payload: &str) -> Result<LastSelect> {
        // The payload is an opaque (base64, likely encrypted) blob, not JSON.
        self.post_json("dm/getOperationResultRt.do", Some(Body::Raw(payload.to_string()))).await
    }

    /// Tag-specific location read the web app falls back to when no push
    /// arrives after `CHECK_CONNECTION_WITH_LOCATION`. `latest_time` is the
    /// newest fix already known (`YYYYMMDDhhmmss`), or `"00000000"`.
    pub async fn tag_location(&mut self, dev: &Device, latest_time: &str) -> Result<LastSelect> {
        self.post_json("dm/getTagLocation.do", Some(Body::Json(json!({ "dvceId": dev.id, "latestTime": latest_time }))))
            .await
    }

    pub async fn last_select(&mut self, dev: &Device) -> Result<LastSelect> {
        self.post_json(
            "device/setLastSelect.do",
            Some(Body::Json(json!({ "dvceId": dev.id, "removeDevice": [] }))),
        )
        .await
    }
}
