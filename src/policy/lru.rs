//! The LRU policy implementation.

use super::EvictPolicy;
use hashlink::LruCache;
use parking_lot::Mutex;
use std::hash::Hash;

/// The evict policy based on LRU.
pub struct LruPolicy<K> {
    /// The inner hashlink
    inner: Mutex<LruCache<K, ()>>,
    /// The capacity of this policy
    capacity: usize,
}

impl<K: Hash + Eq> LruPolicy<K> {
    /// Create a new `LruPolicy` with `capacity`.
    pub fn new(capacity: usize) -> Self {
        LruPolicy {
            inner: Mutex::new(LruCache::new(capacity)),
            capacity,
        }
    }
}

impl<K: Hash + Eq> EvictPolicy<K> for LruPolicy<K> {
    fn put(&self, key: K) -> Option<K> {
        let mut lru = self.inner.lock();
        let len = lru.len();

        let evicted = if !lru.contains_key(&key) && len == self.capacity {
            lru.remove_lru()
        } else {
            None
        };

        lru.insert(key, ());
        evicted.map(|(k, _)| k)
    }

    fn touch(&self, key: &K) {
        let _ = self.inner.lock().get(key);
    }

    fn remove(&self, key: &K) {
        let _ = self.inner.lock().remove(key);
    }
}

#[cfg(test)]
#[allow(clippy::default_numeric_fallback)]
mod tests {
    use super::{EvictPolicy, LruPolicy};

    #[test]
    fn test_lru() {
        let cache = LruPolicy::<i32>::new(3);

        let mut res;
        res = cache.put(1);
        assert_eq!(res, None);
        res = cache.put(2);
        assert_eq!(res, None);
        res = cache.put(3);
        assert_eq!(res, None);

        // 1 -> 2 -> 3
        // Evict one
        res = cache.put(4);
        assert_eq!(res, Some(1));

        // 2 -> 3 -> 4
        // Move 2 to front
        cache.touch(&2);

        // 3 -> 4 -> 2
        res = cache.put(5);
        assert_eq!(res, Some(3));

        // 4 -> 2 -> 5
        // Move 4 to front
        res = cache.put(4);
        assert_eq!(res, None);

        // 2 -> 5 -> 4
        res = cache.put(6);
        assert_eq!(res, Some(2));

        // 5 -> 4 -> 6
        cache.remove(&2);
        assert_eq!(cache.inner.lock().len(), 3);

        // 5 -> 4 -> 6
        cache.remove(&5);
        assert_eq!(cache.inner.lock().len(), 2);

        // 4 -> 6
    }
}
