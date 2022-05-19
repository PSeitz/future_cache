use std::hash::Hash;
use std::sync::Mutex;

use futures::future::{BoxFuture, Shared};
use futures::{Future, FutureExt};
use lru::LruCache;

#[derive(Clone, Copy, Debug)]
pub enum Capacity {
    Unlimited,
    InBytes(usize),
}

/// The AsyncCache stores Futures, so that concurrent async request to the same data source can be deduplicated.
///
/// Since we pass the Future potentially to multiple consumer, everything needs to be cloneable. The data and the future.
/// This is reflected on the generic type bounds for the value V: Clone.
///
/// Since most Futures return an Result<V, Error>, this also encompasses the error.
pub struct AsyncCache<K, V: Clone> {
    lru_cache: Mutex<LruCache<K, Shared<BoxFuture<'static, V>>>>,
    num_bytes: usize,
    capacity: Capacity,
}

impl<K: Hash + Eq, V: Clone> AsyncCache<K, V> {
    /// Creates a new NeedMutSliceCache with the given capacity.
    pub fn with_capacity(capacity: Capacity) -> Self {
        AsyncCache {
            // TODO handle capacity
            lru_cache: Mutex::new(LruCache::unbounded()),
            num_bytes: 0,
            capacity,
        }
    }

    /// Instead of the future directly, a constructor to build the future is passed.
    /// In case there is already an existing Future for the passed key, the constructor is not
    /// used.
    pub async fn get_or_create<T, F>(&self, key: K, build_a_future: T) -> V
    where
        T: FnOnce() -> F,
        F: Future<Output = V> + Send + 'static,
    {
        if let Some(future) = self.lru_cache.lock().unwrap().get(&key).cloned() {
            return future.await;
        }
        let fut = Box::pin(build_a_future()) as BoxFuture<'static, V>;
        let fut = fut.shared();
        self.lru_cache.lock().unwrap().put(key, fut.clone());
        fut.await
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Range;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    use super::*;

    use tokio::fs::{self, File};
    use tokio::io::AsyncWriteExt;
    use tokio::sync::OnceCell; // for read_to_end()

    static GLBL_COUNT: OnceCell<AtomicU32> = OnceCell::const_new();

    async fn get_global_count() -> &'static AtomicU32 {
        GLBL_COUNT.get_or_init(|| async { AtomicU32::new(0) }).await
    }

    #[derive(Hash, Debug, Clone, PartialEq, Eq)]
    pub struct SliceAddress {
        pub path: PathBuf,
        pub byte_range: Range<usize>,
    }

    #[tokio::test]
    async fn test_async_slice_cache() {
        //test data

        let temp_dir = tempfile::tempdir().unwrap();
        let test_filepath1 = Arc::new(temp_dir.path().join("f1"));

        let mut file1 = File::create(test_filepath1.as_ref()).await.unwrap();
        file1.write_all("nice cache dude".as_bytes()).await.unwrap();

        let cache: AsyncCache<SliceAddress, Result<String, String>> =
            AsyncCache::with_capacity(Capacity::Unlimited);

        let addr1 = SliceAddress {
            path: test_filepath1.as_ref().clone(),
            byte_range: 10..20,
        };

        // Load via closure
        let val = cache
            .get_or_create(addr1.clone(), || {
                let test_filepath1 = test_filepath1.clone();
                async move {
                    get_global_count().await.fetch_add(1, Ordering::SeqCst);
                    let contents = Box::pin(fs::read_to_string(test_filepath1.as_ref().clone()))
                        .await
                        // to string, so that the error is cloneable
                        .map_err(|err| err.to_string())?;
                    Ok(contents)
                }
            })
            .await
            .unwrap();

        println!("{}", val);

        // Load via function
        let val = cache
            .get_or_create(addr1, || load_via_fn(test_filepath1.as_ref().clone()))
            .await
            .unwrap();

        println!("{}", val);

        assert_eq!(get_global_count().await.load(Ordering::SeqCst), 1);

        // Load via function, new entry
        let addr1 = SliceAddress {
            path: test_filepath1.as_ref().clone(),
            byte_range: 10..30,
        };

        let _val = cache
            .get_or_create(addr1, || load_via_fn(test_filepath1.as_ref().clone()))
            .await
            .unwrap();

        assert_eq!(get_global_count().await.load(Ordering::SeqCst), 2);
    }

    async fn load_via_fn(path: PathBuf) -> Result<String, String> {
        get_global_count().await.fetch_add(1, Ordering::SeqCst);
        let contents = Box::pin(fs::read_to_string(path))
            .await
            .map_err(|err| err.to_string())?;
        Ok(contents)
    }
}
