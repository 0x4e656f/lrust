//! Response ownership follows the Lua waiter, not a wall-clock TTL.
//! Dropping a lease abandons only the reply; it never cancels or replays SQL.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

pub type Key = (u32, i64);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

struct Slot<T> {
    active: bool,
    value: Option<T>,
}

type Slots<T> = Mutex<HashMap<Key, Weak<Mutex<Slot<T>>>>>;
pub struct Registry<T> {
    // Independent lanes/services should not contend on one process-wide lock.
    // Shards are not a capacity limit; every accepted request still gets a slot.
    slots: [Slots<T>; 32],
}

pub struct Lease<T> {
    registry: Arc<Registry<T>>,
    key: Key,
    slot: Arc<Mutex<Slot<T>>>,
}

impl<T> Registry<T> {
    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    fn shard(&self, key: Key) -> &Slots<T> {
        let hash = (key.0 as u64).wrapping_mul(0x9e37_79b9) ^ key.1 as u64;
        &self.slots[hash as usize & (self.slots.len() - 1)]
    }

    pub fn register(self: &Arc<Self>, key: Key) -> Result<Lease<T>, String> {
        let mut slots = lock(self.shard(key));
        if slots.get(&key).and_then(Weak::upgrade).is_some() {
            return Err("SQLx response session is already in use".to_string());
        }
        let slot = Arc::new(Mutex::new(Slot {
            active: true,
            value: None,
        }));
        slots.insert(key, Arc::downgrade(&slot));
        Ok(Lease {
            registry: self.clone(),
            key,
            slot,
        })
    }

    fn find(&self, key: Key) -> Option<Arc<Mutex<Slot<T>>>> {
        lock(self.shard(key)).get(&key).and_then(Weak::upgrade)
    }

    pub fn publish(&self, key: Key, value: T) -> bool {
        let Some(slot) = self.find(key) else {
            return false;
        };
        let mut slot = lock(&slot);
        if !slot.active || slot.value.is_some() {
            return false;
        }
        slot.value = Some(value);
        true
    }

    pub fn take(&self, key: Key) -> Option<T> {
        let slot = self.find(key)?;
        let mut slot = lock(&slot);
        // One-shot even if a duplicate response arrives before __close.
        slot.active = false;
        slot.value.take()
    }

    pub fn counts(&self) -> (usize, usize) {
        let mut waiting = 0;
        let mut ready = 0;
        for shard in &self.slots {
            let slots = lock(shard);
            for slot in slots.values().filter_map(Weak::upgrade) {
                let slot = lock(&slot);
                if slot.active {
                    if slot.value.is_some() {
                        ready += 1;
                    } else {
                        waiting += 1;
                    }
                }
            }
        }
        (waiting, ready)
    }
}

impl<T> Drop for Lease<T> {
    fn drop(&mut self) {
        // Registry -> slot is the single lock order, also used by counts().
        let mut slots = lock(self.registry.shard(self.key));
        if slots
            .get(&self.key)
            .is_some_and(|slot| Weak::ptr_eq(slot, &Arc::downgrade(&self.slot)))
        {
            slots.remove(&self.key);
        }
        let value = {
            let mut slot = lock(&self.slot);
            slot.active = false;
            slot.value.take()
        };
        drop(slots);
        // Drop potentially large row buffers outside all locks.
        drop(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn consumed_abandoned_and_late_responses() {
        let registry = Arc::new(Registry::new());
        let lease = registry.register((1, 8)).unwrap();
        assert_eq!(registry.counts(), (1, 0));
        assert!(registry.publish((1, 8), vec![1; 1024]));
        assert_eq!(registry.counts(), (0, 1));
        assert!(registry.take((2, 8)).is_none());
        assert_eq!(registry.take((1, 8)).unwrap().len(), 1024);
        assert!(!registry.publish((1, 8), vec![]));
        drop(lease);
        let lease = registry.register((1, 9)).unwrap();
        let payload = Arc::new(());
        drop(lease);
        assert!(!registry.publish((1, 9), vec![]));
        assert_eq!(registry.counts(), (0, 0));
        let other = Arc::new(Registry::new());
        let lease = other.register((1, 1)).unwrap();
        assert!(other.publish((1, 1), payload.clone()));
        drop(lease);
        assert_eq!(Arc::strong_count(&payload), 1);
    }

    #[test]
    fn publish_and_abandon_race_releases_payload() {
        for i in 0..200 {
            let registry = Arc::new(Registry::new());
            let lease = registry.register((1, i)).unwrap();
            let payload = Arc::new(());
            let worker = registry.clone();
            let value = payload.clone();
            let thread = std::thread::spawn(move || worker.publish((1, i), value));
            drop(lease);
            thread.join().unwrap();
            assert_eq!(Arc::strong_count(&payload), 1);
            assert_eq!(registry.counts(), (0, 0));
            assert!(registry.slots.iter().all(|shard| lock(shard).is_empty()));
        }
    }
}
