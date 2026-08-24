//! Async-safe MoonCake store client.
//!
//! All MoonCake C API calls are synchronous / blocking. They run on tokio's
//! blocking thread pool so the async runtime stays responsive. The inner
//! [`Store`](super::store::Store) is wrapped in `Arc` so the client is cheap
//! to clone.

use std::sync::Arc;

use anyhow::Result;
use tokio::task;

use super::config::NormalizedMoonCakeConfig;
use super::store::Store;

/// Async-safe MoonCake store client.
///
/// Cheap to clone — the underlying store handle is reference-counted.
#[derive(Clone)]
pub struct MoonCakeStoreClient {
    store: Arc<Store>,
    /// Max object size in bytes before automatic chunking.
    chunk_size: u32,
}

impl MoonCakeStoreClient {
    /// Synchronous constructor for use in non-async factory contexts.
    pub fn new_sync(config: &NormalizedMoonCakeConfig) -> Result<Self> {
        let store = Self::init_store(config)?;
        Ok(Self {
            store: Arc::new(store),
            chunk_size: config.max_object_size,
        })
    }

    fn init_store(config: &NormalizedMoonCakeConfig) -> Result<Store> {
        let mut store = Store::create()?;
        store.setup(config)?;
        Ok(store)
    }

    pub async fn put(&self, key: String, value: Vec<u8>) -> Result<()> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || store.put(&key, &value)).await?
    }

    /// Put a key-value pair, removing any existing entry first.
    ///
    /// MoonCake's `PutStart` rejects overwrites of existing keys with
    /// `OBJECT_ALREADY_EXISTS`. This method works around that by issuing a
    /// best-effort `remove` (ignoring `OBJECT_NOT_FOUND` when the key was
    /// never written) before the `put`, so callers can safely update
    /// mutable keys like record blobs and the catalog index.
    pub async fn put_overwrite(&self, key: String, value: Vec<u8>) -> Result<()> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || {
            // Best-effort delete — the key may not exist yet, and that's fine.
            let _ = store.remove(&key, true);
            store.put(&key, &value)
        })
        .await?
    }

    pub async fn get(&self, key: String) -> Result<Vec<u8>> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || store.get(&key)).await?
    }

    pub async fn exists(&self, key: String) -> Result<bool> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || store.exists(&key)).await?
    }

    /// Check whether an object exists, handling both direct and chunked storage.
    ///
    /// Returns `true` if either the direct key or the `{key}/meta` descriptor
    /// (indicating chunked storage) exists.
    pub async fn exists_chunked(&self, key: String) -> Result<bool> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || {
            if store.exists(&key)? {
                return Ok(true);
            }
            let meta_key = format!("{key}/meta");
            store.exists(&meta_key)
        })
        .await?
    }

    /// Delete a single key.
    ///
    /// Always forces removal: every object written through this client is
    /// soft-pinned at put time (and the MoonCake master grants a default KV
    /// lease), so a non-forced remove would fail with `OBJECT_HAS_LEASE`
    /// (-706) for any existing object. Forcing is the only correct behaviour
    /// for explicit deletes in this backend.
    pub async fn remove(&self, key: String) -> Result<()> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || store.remove(&key, true)).await?
    }

    /// Delete all keys matching a regex pattern.
    ///
    /// See [`remove`](Self::remove): removal is always forced because every
    /// object carries a lease.
    pub async fn remove_by_regex(&self, pattern: String) -> Result<i64> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || store.remove_by_regex(&pattern, true)).await?
    }

    /// Read a small object entirely into memory.
    ///
    /// Prefer [`get_chunked`](Self::get_chunked) for objects that may have
    /// been stored in chunked form.
    pub async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.get(key.to_string()).await
    }

    /// Write an object, automatically splitting large values into chunks.
    ///
    /// Uses the `max_object_size` from config as the chunk threshold.
    pub async fn put_chunked(&self, key: String, value: Vec<u8>) -> Result<()> {
        let store = Arc::clone(&self.store);
        let chunk_size = self.chunk_size;
        task::spawn_blocking(move || store.put_chunked(&key, &value, chunk_size)).await?
    }

    /// Read a (possibly chunked) object into memory.
    ///
    /// Transparently handles both direct and chunked objects.
    pub async fn get_chunked(&self, key: String) -> Result<Vec<u8>> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || store.get_chunked(&key)).await?
    }

    /// Download an object to a local file.
    ///
    /// Uses chunked reads so large objects are reconstructed transparently.
    pub async fn get_to_file(&self, key: &str, dest: &std::path::Path) -> Result<u64> {
        let data = self.get_chunked(key.to_string()).await?;
        let len = data.len() as u64;
        tokio::fs::write(dest, &data).await?;
        Ok(len)
    }
}
