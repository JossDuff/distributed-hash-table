use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct StripedDb<K, V> {
    stripes: Vec<Arc<Mutex<HashMap<K, (V, u64)>>>>,
    num_stripes: usize,
}

impl<K, V> StripedDb<K, V>
where
    K: Hash + Eq + Clone,
    V: Clone,
{
    pub fn new(num_stripes: usize) -> Self {
        let stripes = (0..num_stripes)
            .map(|_| Arc::new(Mutex::new(HashMap::new())))
            .collect();
        Self {
            stripes,
            num_stripes,
        }
    }

    // stripe index for a key is the same across all nodes
    pub fn stripe_index(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_stripes
    }

    pub fn get_stripe_by_index(&self, idx: usize) -> Arc<Mutex<HashMap<K, (V, u64)>>> {
        self.stripes[idx].clone()
    }

    // get a value and its version
    pub async fn get(&self, key: &K) -> Option<(V, u64)> {
        let guard = self.stripes[self.stripe_index(key)].lock().await;
        guard.get(key).cloned()
    }

    // Stores the value only if version >= stored version
    pub async fn put(&self, key: K, value: V, version: u64) -> bool {
        let mut guard = self.stripes[self.stripe_index(&key)].lock().await;
        match guard.get(&key) {
            Some((_, existing_version)) if *existing_version > version => false,
            _ => {
                guard.insert(key, (value, version));
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let db = StripedDb::new(8);
        assert!(db.put(1u64, "hello", 1).await);
        assert_eq!(db.get(&1).await, Some(("hello", 1)));
        assert_eq!(db.get(&2).await, None);
    }

    #[tokio::test]
    async fn test_put_replaces_with_higher_version() {
        let db = StripedDb::new(8);
        assert!(db.put(1u64, "first", 1).await);
        assert!(db.put(1u64, "second", 2).await);
        assert_eq!(db.get(&1).await, Some(("second", 2)));
    }

    #[tokio::test]
    async fn test_put_rejects_lower_version() {
        let db = StripedDb::new(8);
        assert!(db.put(1u64, "first", 5).await);
        assert!(!db.put(1u64, "second", 3).await);
        assert_eq!(db.get(&1).await, Some(("first", 5)));
    }

    #[tokio::test]
    async fn test_put_accepts_equal_version() {
        let db = StripedDb::new(8);
        assert!(db.put(1u64, "first", 5).await);
        assert!(db.put(1u64, "second", 5).await);
        assert_eq!(db.get(&1).await, Some(("second", 5)));
    }
}
