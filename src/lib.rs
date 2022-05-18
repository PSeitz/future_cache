use std::hash::Hash;

use futures::future::{BoxFuture, Shared};
use futures::{Future, FutureExt};
use lru::LruCache;

/// The AsyncCache stores Futures so that concurrent request to the same data source should be deduplicated.
///
/// Since we pass the Future potentially to multiple consumer, everything needs to be cloneable. The data needs, the future and the error.
/// This is reflected on the generic type bounds for the value V and the error ERR.
pub struct AsyncCache<K, ERR: Clone, V: Clone> {
    lru_cache: LruCache<K, Shared<BoxFuture<'static, Result<V, ERR>>>>,
}

impl<K: Hash + Eq, ERR: Clone, V: Clone> AsyncCache<K, ERR, V> {
    /// Creates a new NeedMutSliceCache with the given capacity.
    pub fn with_capacity() -> Self {
        AsyncCache {
            // TODO handle capacity
            lru_cache: LruCache::unbounded(),
        }
    }

    /// Instead of the future directly, a constructor to build the future is passed.
    /// In case there is already an existing Future for the passed key, the constructor is not
    /// used.
    pub async fn get<T, F>(&mut self, key: K, mut build_a_future: T) -> Result<V, ERR>
    where
        T: FnMut() -> F,
        F: Future<Output = Result<V, ERR>> + Send + 'static,
    {
        if let Some(future) = self.lru_cache.get(&key).cloned() {
            return future.await;
        }
        let fut = Box::pin(build_a_future()) as BoxFuture<'static, Result<V, ERR>>;
        let fut = fut.shared();
        self.lru_cache.put(key, fut.clone());
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

        //let mut cache: AsyncSliceCache<(), String> =
        let mut cache: AsyncCache<SliceAddress, String, String> = AsyncCache::with_capacity();

        let addr1 = SliceAddress {
            path: test_filepath1.as_ref().clone(),
            byte_range: 10..20,
        };

        // Load via closure
        let val = cache
            .get(addr1.clone(), || {
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
            .get(addr1, || load_via_fn(test_filepath1.as_ref().clone()))
            .await
            .unwrap();

        println!("{}", val);

        assert_eq!(get_global_count().await.load(Ordering::SeqCst), 1);

        // Load via function
        let addr1 = SliceAddress {
            path: test_filepath1.as_ref().clone(),
            byte_range: 10..30,
        };

        let _val = cache
            .get(addr1, || load_via_fn(test_filepath1.as_ref().clone()))
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
