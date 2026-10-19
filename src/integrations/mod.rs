//! Outputs for SmartThings Find data. To add one (REST, a database
//! writer, ...): create a module here implementing
//! [`crate::hub::Integration`], gate it behind a cargo feature if it pulls
//! in dependencies, and register it in `main.rs`'s `serve` command.
//! [`log`] is the minimal template.

pub mod heartbeat;
pub mod log;
#[cfg(feature = "mqtt")]
pub mod mqtt;
