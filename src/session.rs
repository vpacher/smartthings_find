//! Cookie handling for the SmartThings Find web session.
//!
//! Cookies are kept in a plain map and sent as one explicit `Cookie` header,
//! instead of a cookie jar. A jar can hold two `JSESSIONID` entries with
//! different domain/path scopes and send the stale one (the upstream Python
//! integration had exactly that bug). Here there is only ever one value per
//! name, and every rotation is visible in the log.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use reqwest::header::{HeaderMap, SET_COOKIE};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

pub const SESSION_COOKIE: &str = "JSESSIONID";

/// Where the session cookie came from. Only a file source can be written back
/// when the server rotates the cookie.
#[derive(Debug, Clone)]
pub enum CookieSource {
    Env,
    File(PathBuf),
}

pub struct Session {
    cookies: BTreeMap<String, String>,
    source: CookieSource,
    /// A new `JSESSIONID` the server sent that has not yet been used for an
    /// authenticated request. The server also hands out a fresh *anonymous*
    /// session when it rejects the current one, so a new value is only saved
    /// once a request with it has succeeded (`confirm`).
    unconfirmed: Option<String>,
}

impl Session {
    /// Loads `JSESSIONID` from `STF_JSESSIONID`, or else from the file at
    /// `STF_JSESSIONID_FILE` (default `~/.config/smartthings_find/jsessionid`).
    pub fn load() -> Result<Self> {
        let (value, source) = match std::env::var("STF_JSESSIONID") {
            Ok(v) if !v.trim().is_empty() => (v, CookieSource::Env),
            _ => {
                let path = cookie_file_path()?;
                check_permissions(&path);
                let v = fs::read_to_string(&path).with_context(|| {
                    format!("reading session cookie from {}", path.display())
                })?;
                (v, CookieSource::File(path))
            }
        };
        let value = validate(&value)?;
        info!(
            source = ?source,
            len = value.len(),
            fingerprint = %fingerprint(&value),
            "loaded session cookie"
        );
        let mut cookies = BTreeMap::new();
        cookies.insert(SESSION_COOKIE.to_string(), value);
        Ok(Self { cookies, source, unconfirmed: None })
    }

    /// A session with a JSESSIONID handed in at runtime, same source as this
    /// one (so `save` writes to the same file).
    pub fn with_cookie(&self, raw: &str) -> Result<Session> {
        let value = validate(raw)?;
        let mut cookies = BTreeMap::new();
        cookies.insert(SESSION_COOKIE.to_string(), value);
        Ok(Self { cookies, source: self.source.clone(), unconfirmed: None })
    }

    /// Writes the current JSESSIONID to the cookie file, so a restart picks
    /// it up. Fails for an `STF_JSESSIONID` source.
    pub fn save(&self) -> Result<()> {
        let CookieSource::File(path) = &self.source else {
            bail!("cookie came from STF_JSESSIONID, so there is no file to save it to");
        };
        let value = self.cookies.get(SESSION_COOKIE).context("no JSESSIONID to save")?;
        write_private(path, value).with_context(|| format!("writing {}", path.display()))?;
        info!(path = %path.display(), fingerprint = %fingerprint(value), "saved JSESSIONID");
        Ok(())
    }

    /// Re-reads the cookie source. Returns a new session only if it holds a
    /// different JSESSIONID than this one (e.g. the user pasted a new
    /// cookie after expiry).
    pub fn reload_if_changed(&self) -> Option<Session> {
        let fresh = match Session::load() {
            Ok(s) => s,
            Err(e) => {
                warn!("re-reading session cookie failed: {e:#}");
                return None;
            }
        };
        (fresh.session_fingerprint() != self.session_fingerprint()).then_some(fresh)
    }

    pub fn cookie_header(&self) -> String {
        self.cookies
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    pub fn session_fingerprint(&self) -> String {
        self.cookies
            .get(SESSION_COOKIE)
            .map(|v| fingerprint(v))
            .unwrap_or_default()
    }

    /// Applies `Set-Cookie` headers from a response. A changed `JSESSIONID`
    /// is logged (fingerprint only, never the value) and saved to the cookie
    /// file only after `confirm`.
    pub fn absorb(&mut self, headers: &HeaderMap) {
        for raw in headers.get_all(SET_COOKIE) {
            let Ok(raw) = raw.to_str() else { continue };
            let Some((name, value)) = raw.split(';').next().and_then(|kv| kv.split_once('=')) else {
                continue;
            };
            let (name, value) = (name.trim(), value.trim());
            let expired = raw.to_ascii_lowercase().contains("max-age=0") || value.is_empty();
            if expired {
                if self.cookies.remove(name).is_some() {
                    warn!(cookie = name, "server cleared cookie");
                }
                continue;
            }
            let old = self.cookies.insert(name.to_string(), value.to_string());
            if old.as_deref() == Some(value) {
                continue;
            }
            if name == SESSION_COOKIE {
                warn!(
                    old = %old.as_deref().map(fingerprint).unwrap_or_default(),
                    new = %fingerprint(value),
                    "server sent a new JSESSIONID (saved once a request with it succeeds)"
                );
                self.unconfirmed = Some(value.to_string());
            } else {
                info!(cookie = name, "server set cookie");
            }
        }
    }

    /// Call after an authenticated request succeeded with the current
    /// cookies. Saves a pending new `JSESSIONID`, so a restart picks it up.
    pub fn confirm(&mut self) {
        let Some(value) = self.unconfirmed.take() else { return };
        if self.cookies.get(SESSION_COOKIE) == Some(&value) {
            self.persist(&value);
        }
    }

    fn persist(&self, value: &str) {
        let CookieSource::File(path) = &self.source else {
            warn!("cookie came from STF_JSESSIONID; rotated value is NOT saved and is lost on restart");
            return;
        };
        if let Err(e) = write_private(path, value) {
            warn!(error = %e, path = %path.display(), "could not save rotated JSESSIONID");
        } else {
            info!(path = %path.display(), "saved rotated JSESSIONID");
        }
    }
}

fn cookie_file_path() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("STF_JSESSIONID_FILE") {
        return Ok(PathBuf::from(p));
    }
    let home = std::env::var("HOME").context("HOME not set")?;
    Ok(Path::new(&home).join(".config/smartthings_find/jsessionid"))
}

/// Normalizes a pasted cookie and rejects values that cannot be a JSESSIONID.
fn validate(raw: &str) -> Result<String> {
    let value = normalize(raw);
    if value.is_empty() {
        bail!("session cookie is empty");
    }
    if value.chars().any(|c| c.is_whitespace() || c == ';') {
        bail!(
            "session cookie contains spaces or ';' ({} chars) - that is not a JSESSIONID value; \
             was something else on the clipboard?",
            value.len()
        );
    }
    Ok(value)
}

/// Accepts either the bare value or a pasted `JSESSIONID=...` pair.
fn normalize(raw: &str) -> String {
    let v = raw.trim();
    let v = v.strip_prefix("JSESSIONID=").unwrap_or(v);
    v.trim_matches('"').to_string()
}

fn check_permissions(path: &Path) {
    if let Ok(meta) = fs::metadata(path) {
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            warn!(path = %path.display(), mode = format!("{mode:o}"), "cookie file is readable by others; chmod 600 it");
        }
    }
}

/// Writes through a temp file in the same directory, then renames.
fn write_private(path: &Path, value: &str) -> Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        f.write_all(value.as_bytes())?;
        f.write_all(b"\n")?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Short, non-reversible tag for a secret, so logs can show "same or
/// different" without leaking the value.
pub fn fingerprint(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .take(4)
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    fn session(v: &str) -> Session {
        let mut cookies = BTreeMap::new();
        cookies.insert(SESSION_COOKIE.to_string(), v.to_string());
        Session { cookies, source: CookieSource::Env, unconfirmed: None }
    }

    #[test]
    fn normalize_strips_name_and_quotes() {
        assert_eq!(normalize(" JSESSIONID=\"abc\"\n"), "abc");
        assert_eq!(normalize("abc"), "abc");
    }

    #[test]
    fn absorb_replaces_not_duplicates() {
        let mut s = session("old");
        let mut h = HeaderMap::new();
        h.append(SET_COOKIE, HeaderValue::from_static("JSESSIONID=new; Path=/; Secure; HttpOnly"));
        h.append(SET_COOKIE, HeaderValue::from_static("AWSALB=lb1; Path=/"));
        s.absorb(&h);
        assert_eq!(s.cookie_header(), "AWSALB=lb1; JSESSIONID=new");
        assert_eq!(s.unconfirmed.as_deref(), Some("new"));
        s.confirm();
        assert_eq!(s.unconfirmed, None);
    }

    #[test]
    fn with_cookie_validates_and_keeps_source() {
        let dir = std::env::temp_dir().join(format!("stf-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("jsessionid");
        let mut s = session("old");
        s.source = CookieSource::File(path.clone());
        assert!(s.with_cookie("two words").is_err());
        assert!(s.with_cookie("  ").is_err());
        let fresh = s.with_cookie(" JSESSIONID=new\n").unwrap();
        assert_eq!(fresh.cookie_header(), "JSESSIONID=new");
        fresh.save().unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "new\n");
        assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn save_fails_for_env_source() {
        assert!(session("v").save().is_err());
    }

    #[test]
    fn new_cookie_debug_hides_value() {
        let shown = format!("{:?}", crate::domain::NewCookie("secretvalue".into()));
        assert!(!shown.contains("secretvalue"));
        assert!(shown.contains(&fingerprint("secretvalue")));
    }

    #[test]
    fn absorb_drops_expired_cookie() {
        let mut s = session("v");
        let mut h = HeaderMap::new();
        h.append(SET_COOKIE, HeaderValue::from_static("JSESSIONID=; Max-Age=0; Path=/"));
        s.absorb(&h);
        assert_eq!(s.cookie_header(), "");
    }
}
