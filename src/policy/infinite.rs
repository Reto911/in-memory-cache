use super::EvictPolicy;

pub struct Infinite;

impl<K> EvictPolicy<K> for Infinite {
    fn put(&self, _: K) -> Option<K> {
        None
    }

    fn touch(&self, _: &K) {}

    fn remove(&self, _: &K) {}
}
