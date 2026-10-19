//! The set of known devices, kept stable across flaky device-list responses.
//!
//! `getDeviceList.do` does not always return every device: after a tag was
//! added, responses alternated between including and omitting it for many
//! minutes (seen 2026-10-10, for both the new and an existing tag). Replacing
//! the list on every response would make devices vanish and reappear, so a
//! device is dropped only after `MISSES_BEFORE_REMOVAL` responses in a row
//! leave it out.

use tracing::{info, warn};

use crate::model::Device;

/// At the default 120 s poll interval this is about one hour.
pub const MISSES_BEFORE_REMOVAL: u32 = 30;

struct Entry {
    device: Device,
    /// Device-list responses in a row that did not contain this device.
    misses: u32,
}

#[derive(Default)]
pub struct Roster {
    entries: Vec<Entry>,
}

impl Roster {
    pub fn devices(&self) -> impl Iterator<Item = &Device> {
        self.entries.iter().map(|e| &e.device)
    }

    /// Devices in the latest device-list response. The backend answers
    /// per-device calls for any other device with 404, so these are the only
    /// ones worth reading this cycle; the rest keep their last state.
    pub fn present(&self) -> impl Iterator<Item = &Device> {
        self.entries.iter().filter(|e| e.misses == 0).map(|e| &e.device)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Merges one device-list response. New devices are added, present ones
    /// updated (names change when renamed in the app), absent ones counted
    /// and removed only after `MISSES_BEFORE_REMOVAL` misses in a row.
    /// Returns the devices removed by this merge.
    pub fn merge(&mut self, fresh: Vec<Device>) -> Vec<Device> {
        for e in &mut self.entries {
            match fresh.iter().find(|d| d.id == e.device.id) {
                Some(d) => {
                    if e.misses > 0 {
                        info!(device = %d.name(), misses = e.misses, "device back in device list");
                    }
                    if d.name() != e.device.name() {
                        info!(old = %e.device.name(), new = %d.name(), "device renamed");
                    }
                    e.device = d.clone();
                    e.misses = 0;
                }
                None => {
                    e.misses += 1;
                    // Expected: with two tags on the account, every second
                    // response omits one of them (alternating). Logged at
                    // info as known noise; removal below is the real event.
                    if e.misses == 1 {
                        info!(device = %e.device.name(), "device missing from this device list (known Samsung flapping); keeping it");
                    } else if e.misses == MISSES_BEFORE_REMOVAL / 2 {
                        warn!(device = %e.device.name(), misses = e.misses, "device missing from device list for a while");
                    }
                }
            }
        }
        let mut removed = Vec::new();
        self.entries.retain(|e| {
            let keep = e.misses < MISSES_BEFORE_REMOVAL;
            if !keep {
                warn!(device = %e.device.name(), misses = e.misses, "device removed after repeated misses");
                removed.push(e.device.clone());
            }
            keep
        });
        for d in fresh {
            if !self.entries.iter().any(|e| e.device.id == d.id) {
                info!(device = %d.name(), kind = d.type_code.as_deref().unwrap_or("-"), "device added");
                self.entries.push(Entry { device: d, misses: 0 });
            }
        }
        removed
    }

    pub fn get(&self, id: &str) -> Option<&Device> {
        self.devices().find(|d| d.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dev(id: &str, name: &str) -> Device {
        Device {
            id: id.into(),
            user_id: "u".into(),
            model_name: name.into(),
            model_id: None,
            type_code: Some("TAG".into()),
            device_type: Some("TAG".into()),
        }
    }

    fn names(r: &Roster) -> Vec<String> {
        r.devices().map(|d| d.name()).collect()
    }

    #[test]
    fn keeps_device_through_flapping_responses() {
        let mut r = Roster::default();
        r.merge(vec![dev("a", "Keys"), dev("b", "Backpack")]);
        r.merge(vec![dev("b", "Backpack")]);
        r.merge(vec![dev("a", "Keys")]);
        assert_eq!(names(&r), ["Keys", "Backpack"]);
        let present: Vec<_> = r.present().map(|d| d.name()).collect();
        assert_eq!(present, ["Keys"]);
    }

    #[test]
    fn removes_after_consecutive_misses_only() {
        let mut r = Roster::default();
        r.merge(vec![dev("a", "Keys"), dev("b", "Backpack")]);
        for _ in 0..MISSES_BEFORE_REMOVAL - 1 {
            r.merge(vec![dev("b", "Backpack")]);
        }
        assert_eq!(r.len(), 2);
        // A sighting resets the count.
        r.merge(vec![dev("a", "Keys"), dev("b", "Backpack")]);
        for _ in 0..MISSES_BEFORE_REMOVAL - 1 {
            r.merge(vec![dev("b", "Backpack")]);
        }
        assert_eq!(r.len(), 2);
        let removed = r.merge(vec![dev("b", "Backpack")]);
        assert_eq!(names(&r), ["Backpack"]);
        assert_eq!(removed.iter().map(|d| d.name()).collect::<Vec<_>>(), ["Keys"]);
    }

    #[test]
    fn rename_updates_in_place() {
        let mut r = Roster::default();
        r.merge(vec![dev("a", "SmartTag 1")]);
        r.merge(vec![dev("a", "Backpack")]);
        assert_eq!(names(&r), ["Backpack"]);
    }

    #[test]
    fn empty_response_does_not_wipe_roster() {
        let mut r = Roster::default();
        r.merge(vec![dev("a", "Keys")]);
        r.merge(vec![]);
        assert_eq!(names(&r), ["Keys"]);
    }
}
