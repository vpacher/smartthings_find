#[cfg(feature = "mqtt")]
mod watch;

use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use smartthings_find::api::{Client, is_session_expired};
use smartthings_find::engine::{Engine, EngineConfig};
use smartthings_find::hub::Integration;
use smartthings_find::model::{self, Device};
use smartthings_find::roster::Roster;
use smartthings_find::session::Session;

/// Exit code when the session cookie is no longer accepted.
const EXIT_SESSION_EXPIRED: u8 = 2;

/// Device-list calls a one-shot command makes before giving up on finding
/// a device (the list sometimes omits devices, see `roster.rs`).
const LOOKUP_ATTEMPTS: u32 = 3;

#[derive(Parser)]
#[command(version, about = "Poll Samsung SmartThings Find for device locations")]
struct Cli {
    /// Print raw API response bodies to stderr.
    #[arg(long, global = true)]
    raw: bool,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List the devices on the account.
    List,
    /// Read locations and battery, once or in a loop.
    Poll {
        /// Run one cycle and exit.
        #[arg(long)]
        once: bool,
        /// Seconds between cycles.
        #[arg(long, default_value_t = 120)]
        interval: u64,
        /// Which devices get an active location request each cycle. Active
        /// requests cost device battery and can wake a phone's screen.
        #[arg(long, value_enum, default_value_t = Active::Tags)]
        active: Active,
        /// Seconds to wait after active requests before reading locations.
        #[arg(long, default_value_t = 20)]
        settle: u64,
    },
    /// Run the service: poll devices and feed the integrations.
    Serve {
        /// Seconds between last-known-location reads.
        #[arg(long, default_value_t = 120)]
        interval: u64,
        /// Integrations to run (comma-separated): log, mqtt.
        #[arg(long, value_delimiter = ',', default_value = "log,mqtt")]
        integrations: Vec<String>,
    },
    /// Subscribe to Samsung's push broker and print results as they arrive.
    #[cfg(feature = "mqtt")]
    Watch {
        /// Only this device (dvceID or part of its name).
        #[arg(long)]
        device: Option<String>,
        /// Seconds between active location requests.
        #[arg(long, default_value_t = 300)]
        interval: u64,
        /// Which devices get an active location request.
        #[arg(long, value_enum, default_value_t = Active::Tags)]
        active: Active,
    },
    /// Call the tag-only getTagLocation endpoint (exploration).
    TagLocation {
        device: String,
        /// Only fixes newer than this (YYYYMMDDhhmmss).
        #[arg(long, default_value = "00000000")]
        since: String,
    },
    /// Ring a device (by dvceID or part of its name).
    Ring {
        device: String,
        /// Stop ringing (phones only; SmartTags stop by themselves).
        #[arg(long)]
        stop: bool,
    },
}

#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Active {
    None,
    Tags,
    All,
}

impl Active {
    pub(crate) fn applies_to(self, dev: &Device) -> bool {
        match self {
            Active::None => false,
            Active::Tags => dev.is_tag(),
            Active::All => true,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("smartthings_find=info")),
        )
        .init();
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) if is_session_expired(&e) => {
            error!("{e:#}");
            error!("log in at https://smartthingsfind.samsung.com and copy a fresh JSESSIONID");
            ExitCode::from(EXIT_SESSION_EXPIRED)
        }
        Err(e) => {
            error!("{e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let mut client = Client::new(Session::load()?)?;
    client.raw = cli.raw;
    match cli.cmd {
        Cmd::List => {
            for d in client.devices().await? {
                println!(
                    "{:<36}  {:<8}  {:<14}  {}",
                    d.id,
                    d.type_code.as_deref().unwrap_or("-"),
                    d.model_id.as_deref().unwrap_or("-"),
                    d.name()
                );
            }
            Ok(())
        }
        Cmd::Ring { device, stop } => {
            let dev = &find_device(&mut client, &device).await?;
            client.ring(dev, !stop).await?;
            info!(device = %dev.name(), "ring {} sent", if stop { "stop" } else { "start" });
            Ok(())
        }
        Cmd::TagLocation { device, since } => {
            let dev = &find_device(&mut client, &device).await?;
            let v = client.tag_location(dev, &since).await?;
            println!("{}", serde_json::to_string_pretty(&v.operation)?);
            Ok(())
        }
        Cmd::Serve { interval, integrations } => serve(client, Duration::from_secs(interval), &integrations).await,
        #[cfg(feature = "mqtt")]
        Cmd::Watch { device, interval, active } => {
            watch::watch(&mut client, device.as_deref(), active, Duration::from_secs(interval)).await
        }
        Cmd::Poll { once, interval, active, settle } => {
            poll(&mut client, once, Duration::from_secs(interval), active, Duration::from_secs(settle)).await
        }
    }
}

fn build_integration(name: &str) -> Result<Box<dyn Integration>> {
    match name {
        "log" => Ok(Box::new(smartthings_find::integrations::log::Log)),
        #[cfg(feature = "mqtt")]
        "mqtt" => {
            use smartthings_find::integrations::mqtt::{Mqtt, MqttConfig};
            Ok(Box::new(Mqtt::new(MqttConfig::from_env()?)))
        }
        other => bail!("unknown integration {other:?} (built with: log{})", if cfg!(feature = "mqtt") { ", mqtt" } else { "" }),
    }
}

/// Runs the engine and the integrations until a signal, or until one of
/// them stops.
async fn serve(client: Client, interval: Duration, names: &[String]) -> Result<()> {
    let mut integrations = names.iter().map(|n| build_integration(n.trim())).collect::<Result<Vec<_>>>()?;
    if let Some(hb) = smartthings_find::integrations::heartbeat::Heartbeat::from_env(interval)? {
        integrations.push(Box::new(hb));
    }
    let config = EngineConfig { poll_interval: interval, ..EngineConfig::default() };
    let (engine, hub) = Engine::new(client, config);

    let mut tasks = tokio::task::JoinSet::new();
    for integration in integrations {
        let name = integration.name();
        info!(integration = name, "starting");
        let hub = hub.clone();
        tasks.spawn(async move { integration.run(hub).await.map_err(|e| e.context(format!("integration {name}"))) });
    }
    // Integrations subscribe inside their tasks; let them start before the
    // engine's first poll so they see its events.
    tokio::task::yield_now().await;
    tasks.spawn(engine.run());

    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("interrupted"),
        _ = term.recv() => info!("terminated"),
        Some(done) = tasks.join_next() => {
            done??;
            bail!("a service task stopped unexpectedly");
        }
    }
    Ok(())
}

/// Looks a device up by dvceID or name, re-reading the device list up to
/// `LOOKUP_ATTEMPTS` times if it is not in the first response.
pub(crate) async fn find_device(client: &mut Client, query: &str) -> Result<Device> {
    let mut roster = Roster::default();
    let mut attempt = 1;
    loop {
        roster.merge(client.devices().await?);
        let devices: Vec<Device> = roster.devices().cloned().collect();
        match pick(&devices, query) {
            Ok(d) => return Ok(d.clone()),
            Err(e) if attempt >= LOOKUP_ATTEMPTS => return Err(e),
            Err(_) => attempt += 1,
        }
    }
}

pub(crate) fn pick<'a>(devices: &'a [Device], query: &str) -> Result<&'a Device> {
    if let Some(d) = devices.iter().find(|d| d.id == query) {
        return Ok(d);
    }
    let q = query.to_lowercase();
    let hits: Vec<_> = devices.iter().filter(|d| d.name().to_lowercase().contains(&q)).collect();
    match hits.as_slice() {
        [one] => Ok(one),
        [] => bail!("no device matches {query:?}"),
        many => bail!(
            "{query:?} matches {} devices: {}",
            many.len(),
            many.iter().map(|d| d.name()).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// Tracks how long the current session cookie keeps working, which is the
/// main unknown of this API (the fork's README says "several weeks").
struct SessionClock {
    fingerprint: String,
    first_ok: DateTime<Utc>,
    last_ok: DateTime<Utc>,
}

impl SessionClock {
    fn summary(&self) -> String {
        format!(
            "session {} worked from {} to {} (at least {})",
            self.fingerprint,
            self.first_ok.format("%F %T UTC"),
            self.last_ok.format("%F %T UTC"),
            human(self.last_ok - self.first_ok)
        )
    }
}

async fn poll(client: &mut Client, once: bool, interval: Duration, active: Active, settle: Duration) -> Result<()> {
    let mut roster = Roster::default();
    roster.merge(client.devices().await?);
    let now = Utc::now();
    let mut clock = SessionClock { fingerprint: client.session_fingerprint(), first_ok: now, last_ok: now };
    info!(count = roster.len(), "devices loaded");

    let mut first = true;
    loop {
        let result = tokio::select! {
            r = poll_cycle(client, &mut roster, first, active, settle) => r,
            _ = tokio::signal::ctrl_c() => {
                info!("{}", clock.summary());
                return Ok(());
            }
        };
        match result {
            Ok(()) => {
                clock.last_ok = Utc::now();
                let fp = client.session_fingerprint();
                if fp != clock.fingerprint {
                    info!("JSESSIONID rotated; {}", clock.summary());
                    clock.fingerprint = fp;
                }
            }
            Err(e) if is_session_expired(&e) => {
                error!("{}", clock.summary());
                return Err(e);
            }
            // A network hiccup or a 5xx: keep going, the next cycle may work.
            Err(e) => warn!("cycle failed: {e:#}"),
        }
        if once {
            return Ok(());
        }
        first = false;
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = tokio::signal::ctrl_c() => {
                info!("{}", clock.summary());
                return Ok(());
            }
        }
    }
}

async fn poll_cycle(
    client: &mut Client,
    roster: &mut Roster,
    first: bool,
    active: Active,
    settle: Duration,
) -> Result<()> {
    // Re-read the list every cycle: it is cheap, and merging means a
    // response that omits a device does not drop it.
    if !first {
        match client.devices().await {
            Ok(fresh) => {
                roster.merge(fresh);
            }
            Err(e) if is_session_expired(&e) => return Err(e),
            Err(e) => warn!("device list refresh failed, using known devices: {e:#}"),
        }
    }
    // Only devices in the latest list: the backend 404s reads for the rest.
    let devices: Vec<Device> = roster.present().cloned().collect();

    // Request fresh fixes first, then wait once for all of them, instead of
    // waiting per device.
    let mut requested = 0;
    for dev in devices.iter().filter(|d| active.applies_to(d)) {
        match client.request_location(dev).await {
            Ok(()) => requested += 1,
            Err(e) if is_session_expired(&e) => return Err(e),
            Err(e) => warn!(device = %dev.name(), "location request failed: {e:#}"),
        }
    }
    if requested > 0 && !settle.is_zero() {
        info!(requested, "waiting {}s for fresh fixes", settle.as_secs());
        tokio::time::sleep(settle).await;
    }

    // One device failing must not hide the others.
    let mut failures = 0;
    for dev in devices.iter() {
        match client.last_select(dev).await {
            Ok(resp) => report(dev, &resp.snapshot()),
            Err(e) if is_session_expired(&e) => return Err(e),
            Err(e) => {
                failures += 1;
                warn!(device = %dev.name(), "read failed: {e:#}");
            }
        }
    }
    if failures > 0 && failures == devices.len() {
        bail!("all {failures} device reads failed");
    }
    Ok(())
}

pub(crate) fn report(dev: &Device, s: &model::Snapshot) {
    let name = dev.name();
    let kind = dev.type_code.as_deref().unwrap_or("-");
    let battery = s.battery_pct.map(|b| format!("{b}%")).unwrap_or_else(|| "-".into());
    match &s.location {
        Some(loc) => info!(
            device = %name,
            kind,
            source = %loc.source,
            lat = loc.latitude,
            lon = loc.longitude,
            accuracy_m = loc.accuracy_m.map(|a| a.round()).unwrap_or(f64::NAN),
            fix_age = %human(Utc::now() - loc.fixed_at),
            battery = %battery,
            encrypted_skipped = s.encrypted_skipped,
            "location"
        ),
        None => warn!(
            device = %name,
            kind,
            battery = %battery,
            encrypted_skipped = s.encrypted_skipped,
            ops = ?s.op_types,
            "no usable location"
        ),
    }
}

fn human(d: chrono::TimeDelta) -> String {
    let s = d.num_seconds().max(0);
    match s {
        0..60 => format!("{s}s"),
        60..3600 => format!("{}m{:02}s", s / 60, s % 60),
        3600..86400 => format!("{}h{:02}m", s / 3600, (s % 3600) / 60),
        _ => format!("{}d{:02}h", s / 86400, (s % 86400) / 3600),
    }
}
