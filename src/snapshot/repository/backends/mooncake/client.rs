//! Async-safe MoonCake store client.
//!
//! All MoonCake C API calls are synchronous / blocking. They run on tokio's
//! blocking thread pool so the async runtime stays responsive. The inner
//! [`Store`](super::store::Store) is wrapped in `Arc` so the client is cheap
//! to clone.

use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task;

use super::config::NormalizedMoonCakeConfig;
use super::store::Store;

const WRITE_PERMIT_UNIT: u64 = 4 * 1024;

/// Async-safe MoonCake store client.
///
/// Cheap to clone — the underlying store handle is reference-counted.
#[derive(Clone)]
pub struct MoonCakeStoreClient {
    store: Arc<Store>,
    /// Max object size in bytes before automatic chunking.
    chunk_size: u32,
    /// Admission control for MoonCake's setup-time local transfer buffer.
    write_admission: Arc<Semaphore>,
    local_buffer_size: u64,
}

impl MoonCakeStoreClient {
    /// Synchronous constructor for use in non-async factory contexts.
    pub fn new_sync(config: &NormalizedMoonCakeConfig) -> Result<Self> {
        let store = Self::init_store(config)?;
        Ok(Self {
            store: Arc::new(store),
            chunk_size: config.max_object_size,
            write_admission: Arc::new(Semaphore::new(write_capacity_units(
                config.local_buffer_size,
            ))),
            local_buffer_size: config.local_buffer_size,
        })
    }

    fn init_store(config: &NormalizedMoonCakeConfig) -> Result<Store> {
        let mut store = Store::create()?;
        store.setup(config)?;
        Ok(store)
    }

    /// Put non-evictable control-plane metadata.
    pub async fn put_hard_pinned(&self, key: String, value: Vec<u8>) -> Result<()> {
        let _permit = self.acquire_write_permit(value.len() as u64).await?;
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || store.put_hard_pinned(&key, &value)).await?
    }

    /// Replace non-evictable control-plane metadata.
    pub async fn put_overwrite_hard_pinned(&self, key: String, value: Vec<u8>) -> Result<()> {
        let _permit = self.acquire_write_permit(value.len() as u64).await?;
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || {
            store.remove(&key, true)?;
            store.put_hard_pinned(&key, &value)
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
    /// Always forces removal: repository objects are pinned at put time (data
    /// objects are soft-pinned and catalog objects are hard-pinned), so a
    /// non-forced remove may fail with `OBJECT_HAS_LEASE` (-706). Forcing is
    /// the correct behaviour for explicit repository deletes.
    pub async fn remove(&self, key: String) -> Result<()> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || store.remove(&key, true)).await?
    }

    /// Delete all keys matching a regex pattern.
    ///
    /// See [`remove`](Self::remove): removal is always forced because objects
    /// may carry a lease or hard pin.
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

    /// Write immutable content, accepting a concurrent identical-key writer.
    pub async fn put_immutable(&self, key: String, value: Vec<u8>) -> Result<bool> {
        let _permit = self.acquire_write_permit(value.len() as u64).await?;
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || store.put_immutable(&key, &value)).await?
    }

    /// Write immutable, possibly chunked content with metadata committed last.
    pub async fn put_chunked_immutable(&self, key: String, value: Vec<u8>) -> Result<bool> {
        let reservation = chunked_write_reservation(value.len() as u64, self.chunk_size);
        let _permit = self.acquire_write_permit(reservation).await?;
        let store = Arc::clone(&self.store);
        let chunk_size = self.chunk_size;
        task::spawn_blocking(move || store.put_chunked_immutable(&key, &value, chunk_size)).await?
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

    async fn acquire_write_permit(&self, bytes: u64) -> Result<OwnedSemaphorePermit> {
        ensure!(
            bytes <= self.local_buffer_size,
            "MoonCake write reservation ({bytes} bytes) exceeds local_buffer_size ({} bytes); \
             increase backend.mooncake.local_buffer_size or reduce max_object_size",
            self.local_buffer_size
        );
        let permits = write_permits(bytes, self.local_buffer_size);
        Arc::clone(&self.write_admission)
            .acquire_many_owned(permits)
            .await
            .context("MoonCake write admission semaphore closed")
    }
}

fn write_capacity_units(local_buffer_size: u64) -> usize {
    let units = if local_buffer_size < WRITE_PERMIT_UNIT {
        1
    } else {
        local_buffer_size / WRITE_PERMIT_UNIT
    };
    usize::try_from(units.min(u64::from(u32::MAX))).unwrap_or(usize::MAX)
}

fn write_permits(bytes: u64, local_buffer_size: u64) -> u32 {
    if local_buffer_size < WRITE_PERMIT_UNIT {
        return 1;
    }
    let requested = bytes
        .max(1)
        .div_ceil(WRITE_PERMIT_UNIT)
        .try_into()
        .unwrap_or(u32::MAX);
    let capacity = write_capacity_units(local_buffer_size)
        .try_into()
        .unwrap_or(u32::MAX);
    requested.min(capacity)
}

fn chunked_write_reservation(value_len: u64, chunk_size: u32) -> u64 {
    if chunk_size == 0 {
        value_len
    } else {
        value_len.min(u64::from(chunk_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_chunked_object_only_reserves_one_chunk() {
        assert_eq!(
            chunked_write_reservation(2 * 1024 * 1024 * 1024, 4 * 1024 * 1024),
            4 * 1024 * 1024
        );
    }

    #[test]
    fn admission_rounds_requests_up_and_capacity_down() {
        assert_eq!(write_capacity_units(10 * 1024 + 1), 2);
        assert_eq!(write_permits(4097, 10 * 1024 + 1), 2);
        assert_eq!(write_permits(1, 10 * 1024 + 1), 1);
    }
}
