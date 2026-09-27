use std::collections::BTreeMap;

use nft_render::{describe_slot, AppSlot, FIRST_SLOT};
use protocol::AppId;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("all {limit} apps this host is laid out for hold a slot; raise max_apps in config.toml")]
pub struct SlotExhausted {
    pub limit: u32,
}

impl SlotExhausted {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

pub fn read_slot_cursor(value: Option<serde_json::Value>) -> i64 {
    value
        .and_then(|value| value.as_i64())
        .unwrap_or(i64::from(FIRST_SLOT))
}

pub fn assignments_from(records: BTreeMap<String, serde_json::Value>, max_apps: u32) -> BTreeMap<AppId, u32> {
    let mut assignments = BTreeMap::new();
    let mut taken = std::collections::BTreeSet::new();
    for (app_id, slot) in records {
        let Some(slot) = slot.as_u64().and_then(|slot| u32::try_from(slot).ok()) else {
            continue;
        };
        let Ok(app_id) = AppId::parse(app_id) else {
            continue;
        };
        if !(FIRST_SLOT..max_apps).contains(&slot) || taken.contains(&slot) {
            continue;
        }
        assignments.insert(app_id, slot);
        taken.insert(slot);
    }
    assignments
}

pub struct SlotAllocator {
    assignments: BTreeMap<AppId, u32>,
    cursor: i64,
    limit: u32,
}

impl SlotAllocator {
    /// A ring of `max_apps` slots from `FIRST_SLOT`, as the configuration lays the host out.
    pub fn addressing(max_apps: u32) -> Self {
        Self {
            assignments: BTreeMap::new(),
            cursor: i64::from(FIRST_SLOT),
            limit: max_apps,
        }
    }

    /// What an earlier daemon wrote down may have been measured against a wider ring than this
    /// host is laid out for: a slot past the limit is one no app here can hold, and a cursor past
    /// it comes back inside so the next allocation looks where a free slot can be.
    pub fn restore(&mut self, assignments: BTreeMap<AppId, u32>, cursor: i64) {
        self.assignments = assignments
            .into_iter()
            .filter(|(_, slot)| (FIRST_SLOT..self.limit).contains(slot))
            .collect();
        self.cursor = i64::from(self.wrapped(cursor - i64::from(FIRST_SLOT)));
    }

    pub fn assignments(&self) -> &BTreeMap<AppId, u32> {
        &self.assignments
    }

    pub fn cursor(&self) -> i64 {
        self.cursor
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }

    pub(crate) fn grow(&mut self, limit: u32) {
        self.limit = self.limit.max(limit);
    }

    fn span(&self) -> u32 {
        self.limit - FIRST_SLOT
    }

    fn wrapped(&self, offset: i64) -> u32 {
        let span = i64::from(self.span());
        let inside = ((offset % span) + span) % span;
        FIRST_SLOT + inside as u32
    }

    fn next_free(&self) -> Option<u32> {
        let taken: std::collections::BTreeSet<u32> = self.assignments.values().copied().collect();
        (0..self.span())
            .map(|step| self.wrapped(self.cursor - i64::from(FIRST_SLOT) + i64::from(step)))
            .find(|slot| !taken.contains(slot))
    }

    pub fn allocate(&mut self, app_id: &AppId) -> Result<AppSlot, SlotExhausted> {
        if let Some(slot) = self.assignments.get(app_id) {
            return Ok(describe_slot(*slot, app_id.clone()));
        }
        let free = self.next_free().ok_or(SlotExhausted { limit: self.limit })?;
        self.assignments.insert(app_id.clone(), free);
        self.cursor = i64::from(free) + 1;
        Ok(describe_slot(free, app_id.clone()))
    }

    pub fn lookup(&self, app_id: &AppId) -> Option<AppSlot> {
        self.assignments
            .get(app_id)
            .map(|slot| describe_slot(*slot, app_id.clone()))
    }

    pub fn release(&mut self, app_id: &AppId) {
        self.assignments.remove(app_id);
    }

    pub fn slots(&self) -> Vec<AppSlot> {
        self.assignments
            .iter()
            .map(|(app_id, slot)| describe_slot(*slot, app_id.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_APPS: u32 = 1000;

    fn app(name: impl std::fmt::Display) -> AppId {
        AppId::parse(format!("app-{name}")).unwrap()
    }

    fn allocator() -> SlotAllocator {
        SlotAllocator::addressing(MAX_APPS)
    }

    fn full(max_apps: u32) -> SlotAllocator {
        let mut allocator = SlotAllocator::addressing(max_apps);
        for index in 0..max_apps {
            assert_eq!(allocator.allocate(&app(index)).unwrap().slot, index);
        }
        allocator
    }

    #[test]
    fn a_redeploy_keeps_the_host_port_and_distinct_apps_never_share_a_slot() {
        let mut allocator = allocator();
        let first = allocator.allocate(&app(1)).unwrap();
        let again = allocator.allocate(&app(1)).unwrap();
        assert_eq!(first.host_port, again.host_port);
        let ports: std::collections::BTreeSet<u16> = ["alpha", "beta", "gamma"]
            .iter()
            .map(|name| allocator.allocate(&app(name)).unwrap().host_port.get())
            .collect();
        assert_eq!(ports.len(), 3);
    }

    #[test]
    fn a_released_slot_becomes_available_again() {
        let mut allocator = full(MAX_APPS);
        let freed = allocator.allocate(&app(3)).unwrap();
        allocator.release(&app(3));
        assert!(allocator.lookup(&app(3)).is_none());
        assert_eq!(allocator.allocate(&app("next")).unwrap().slot, freed.slot);
    }

    #[test]
    fn a_released_slot_is_not_the_next_one_handed_out() {
        let mut allocator = allocator();
        let first = allocator.allocate(&app(1)).unwrap();
        allocator.release(&app(1));
        assert_ne!(allocator.allocate(&app(2)).unwrap().slot, first.slot);
    }

    #[test]
    fn being_handed_the_slot_an_app_already_holds_does_not_move_the_cursor() {
        let mut allocator = allocator();
        let staying = allocator.allocate(&app("staying")).unwrap();
        allocator.allocate(&app("leaving")).unwrap();
        allocator.release(&app("leaving"));
        allocator.allocate(&app("staying")).unwrap();
        assert_ne!(
            allocator.allocate(&app("arriving")).unwrap().slot,
            staying.slot + 1
        );
    }

    #[test]
    fn running_out_of_slots_is_a_typed_failure_that_assigns_nothing() {
        for max_apps in [1, 63, MAX_APPS] {
            let mut allocator = full(max_apps);
            let cursor = allocator.cursor();
            assert_eq!(
                allocator.allocate(&app("one-too-many")).unwrap_err(),
                SlotExhausted { limit: max_apps }
            );
            assert!(allocator.lookup(&app("one-too-many")).is_none());
            assert_eq!(allocator.assignments().len(), max_apps as usize);
            assert_eq!(allocator.cursor(), cursor, "a refusal does not move the cursor");
        }
    }

    #[test]
    fn the_ring_wraps_at_how_many_apps_the_host_is_laid_out_for() {
        let mut allocator = full(63);
        allocator.release(&app(5));
        assert_eq!(allocator.allocate(&app("next")).unwrap().slot, 5);
    }

    #[test]
    fn a_cursor_read_off_disk_cannot_hand_out_a_slot_somebody_holds() {
        assert_eq!(read_slot_cursor(None), i64::from(FIRST_SLOT));
        assert_eq!(
            read_slot_cursor(Some(serde_json::json!("7"))),
            i64::from(FIRST_SLOT)
        );
        assert_eq!(
            read_slot_cursor(Some(serde_json::json!(1.5))),
            i64::from(FIRST_SLOT)
        );
        assert_eq!(read_slot_cursor(Some(serde_json::json!(3))), 3);
        for cursor in [1000i64, -1000] {
            let mut allocator = SlotAllocator {
                assignments: BTreeMap::new(),
                cursor,
                limit: MAX_APPS,
            };
            let slot = allocator.allocate(&app(1)).unwrap().slot;
            assert!((FIRST_SLOT..MAX_APPS).contains(&slot));
        }
    }

    #[test]
    fn allocation_survives_a_restart_and_a_file_that_lost_its_shape() {
        assert!(assignments_from(BTreeMap::new(), MAX_APPS).is_empty());
        let records = BTreeMap::from([
            ("app-1".to_string(), serde_json::json!("three")),
            ("app-2".to_string(), serde_json::json!(3)),
            ("has.a.dot".to_string(), serde_json::json!(4)),
            ("app-3".to_string(), serde_json::json!(3)),
            ("app-4".to_string(), serde_json::json!(MAX_APPS)),
        ]);
        let assignments = assignments_from(records, MAX_APPS);
        assert_eq!(assignments.get(&app(2)), Some(&3));
        assert_eq!(assignments.get(&app(3)), None);
        assert_eq!(assignments.get(&app(4)), None);
        assert_eq!(assignments.len(), 1);
    }

    #[test]
    fn restoring_replaces_what_was_held_rather_than_merging_with_it() {
        let mut allocator = allocator();
        let stale = allocator.allocate(&app(1)).unwrap();
        assert_eq!(allocator.lookup(&app(1)).map(|slot| slot.slot), Some(stale.slot));

        allocator.restore(BTreeMap::from([(app(2), 7)]), 8);
        assert_eq!(allocator.lookup(&app(1)), None);
        assert_eq!(allocator.lookup(&app(2)).map(|slot| slot.slot), Some(7));
        assert_eq!(allocator.cursor(), 8);
        assert_eq!(allocator.assignments().len(), 1);
    }

    #[test]
    fn what_a_wider_ring_wrote_down_is_brought_back_inside_the_one_this_host_is_laid_out_for() {
        let mut allocator = SlotAllocator::addressing(63);
        let written = BTreeMap::from([
            (app("held"), 62),
            (app("past-63"), 63),
            (app("past-64"), 64),
            (app("past-65"), 65),
            (app("past-66"), 66),
        ]);
        allocator.restore(written, 67);

        assert_eq!(allocator.assignments().len(), 1);
        assert_eq!(allocator.lookup(&app("held")).unwrap().slot, 62);
        assert!(allocator.lookup(&app("past-63")).is_none());
        assert_eq!(
            allocator.cursor(),
            4,
            "the cursor comes back in at the offset it was past"
        );
        assert_eq!(allocator.allocate(&app("arriving")).unwrap().slot, 4);
    }

    #[test]
    fn every_slot_the_host_holds_is_listed_so_the_ruleset_can_be_rendered_from_it() {
        let mut allocator = allocator();
        assert!(allocator.slots().is_empty());
        let first = allocator.allocate(&app(1)).unwrap();
        let second = allocator.allocate(&app(2)).unwrap();
        let slots = allocator.slots();
        assert_eq!(slots.len(), 2);
        let ports: std::collections::BTreeSet<u16> = slots.iter().map(|slot| slot.host_port.get()).collect();
        assert_eq!(
            ports,
            std::collections::BTreeSet::from([first.host_port.get(), second.host_port.get()])
        );
        allocator.release(&app(1));
        assert_eq!(allocator.slots().len(), 1);
    }

    #[test]
    fn the_cursor_follows_the_slot_that_was_just_handed_out() {
        let mut allocator = allocator();
        let handed = allocator.allocate(&app(1)).unwrap();
        assert_eq!(allocator.cursor(), i64::from(handed.slot) + 1);
        allocator.allocate(&app(1)).unwrap();
        assert_eq!(allocator.cursor(), i64::from(handed.slot) + 1);
    }

    #[test]
    fn a_host_with_no_room_left_says_how_many_apps_it_is_laid_out_for_and_where_to_raise_it() {
        let said = SlotExhausted { limit: 63 }.message();
        assert!(said.contains("all 63 apps"), "{said}");
        assert!(said.contains("max_apps"), "{said}");
        assert!(said.contains("config.toml"), "{said}");
    }

    #[test]
    fn a_slot_read_off_disk_is_the_slot_the_app_keeps_being_given() {
        let restored = assignments_from(
            BTreeMap::from([("app-1".to_string(), serde_json::json!(9))]),
            MAX_APPS,
        );
        let mut allocator = allocator();
        allocator.restore(restored, 10);
        assert_eq!(allocator.allocate(&app(1)).unwrap().slot, 9);
        assert_eq!(allocator.lookup(&app(1)).unwrap().slot, 9);
        assert!(allocator.lookup(&app(2)).is_none());
    }

    #[test]
    fn a_slot_number_no_host_could_have_handed_out_is_not_restored() {
        for unusable in [serde_json::json!(-1), serde_json::json!(u64::MAX)] {
            let restored = assignments_from(
                BTreeMap::from([("app-1".to_string(), unusable.clone())]),
                MAX_APPS,
            );
            assert!(restored.is_empty(), "{unusable} was restored as a slot");
        }
    }
}
