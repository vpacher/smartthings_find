//! SmartThings Find client and service.
//!
//! Layers, from Samsung outwards:
//! - [`session`], [`api`], [`model`], [`roster`]: the reverse-engineered
//!   smartthingsfind.samsung.com web API.
//! - [`engine`]: the only part that talks to Samsung. It polls, runs
//!   commands and publishes [`domain::Event`]s.
//! - [`hub`]: what an integration sees: events, a state snapshot, and a
//!   way to send [`domain::Command`]s. Also the [`hub::Integration`] trait.
//! - [`integrations`]: outputs (log, MQTT for Home Assistant, ...). Each is
//!   self-contained; a new one only needs `domain` and `hub`.

pub mod api;
pub mod domain;
pub mod engine;
pub mod hub;
pub mod integrations;
pub mod model;
pub mod roster;
pub mod session;
