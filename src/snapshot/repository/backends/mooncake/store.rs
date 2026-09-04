//! Safe Rust wrapper around the raw MoonCake C FFI.
//!
//! The [`Store`] handle manages CString lifetimes for `preferred_segments` and
//! provides typed, error-propagating methods for each C API call. All methods
//! are synchronous — use [`super::client::MoonCakeStoreClient`] for async access
//! via `spawn_blocking`.

use std::{
    ffi::{c_void, CString},
    thread,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context, Result};
use overlaybd::backend::mc_buffer_pool::RegisteredReadBufferPool;
use rand::RngExt;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::config::NormalizedMoonCakeConfig;
use super::ffi::*;

pub(crate) const MOONCAKE_NO_AVAILABLE_HANDLE: i64 = -200;
pub(crate) const MOONCAKE_OBJECT_NOT_FOUND: i64 = -704;
pub(crate) const MOONCAKE_OBJECT_ALREADY_EXISTS: i64 = -705;

/// Error returned by the MoonCake C API.
///
/// Keeping the numeric status as structured data lets higher layers handle
/// expected outcomes such as immutable-object deduplication without parsing
/// display strings.
#[derive(Debug, thiserror::Error)]
#[error("{operation}({target}) failed: ret={code}")]
pub(crate) struct MoonCakeStoreError {
    operation: &'static str,
    target: String,
    code: i64,
}

impl MoonCakeStoreError {
    fn new(operation: &'static str, target: impl Into<String>, code: i64) -> Self {
        Self {
            operation,
            target: target.into(),
            code,
        }
    }

    pub(crate) fn code(&self) -> i64 {
        self.code
    }
}

pub(crate) fn mooncake_error_code(error: &anyhow::Error) -> Option<i64> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<MoonCakeStoreError>()
            .map(MoonCakeStoreError::code)
    })
}

fn store_error(operation: &'static str, target: impl Into<String>, code: i64) -> anyhow::Error {
    MoonCakeStoreError::new(operation, target, code).into()
}

/// Metadata stored under `{key}/meta` for chunked objects.
///
/// When an object exceeds [`super::config::DEFAULT_MAX_OBJECT_SIZE`], it is
/// split into fixed-size chunks and this descriptor is written so readers can
/// reconstruct the original byte stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChunkMetadata {
    /// Total size of the original object in bytes.
    pub total_size: u64,
    /// Number of data chunks.
    pub chunk_count: u32,
    /// Size of each chunk in bytes (last chunk may be smaller).
    pub chunk_size: u32,
}

/// Safe Rust wrapper around a MoonCake store handle.
///
/// MoonCake is internally thread-safe (the C++ implementation uses internal
/// mutexes for concurrent access). All C API calls are synchronous — call them
/// from `tokio::task::spawn_blocking`.
pub struct Store {
    handle: MoonCakeStore,
    /// Preferred segment names, kept alive for FFI pointer stability across
    /// repeated `put()` calls that reference them in the replicate config.
    preferred_segments: Vec<CString>,
    /// UB/RDMA reads use reusable page-aligned registered memory. TCP reads
    /// continue to use the caller-provided buffer directly.
    registered_read_pool: Option<RegisteredReadBufferPool>,
    /// Bounded retry policy for transient remote allocation pressure.
    put_no_space_retry: PutNoSpaceRetryPolicy,
}

#[derive(Clone, Copy, Debug)]
struct PutNoSpaceRetryPolicy {
    max_retries: u32,
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
}

// Safety: MoonCake is thread-safe (all C API calls can be made from any
// thread). The C++ implementation uses internal mutexes for concurrent access.
unsafe impl Send for Store {}
unsafe impl Sync for Store {}

impl Store {
    /// Create a new uninitialized MoonCake store handle.
    pub fn create() -> Result<Self> {
        let handle = unsafe { mooncake_store_create() };
        if handle.is_null() {
            anyhow::bail!("mooncake_store_create returned NULL");
        }
        Ok(Self {
            handle,
            preferred_segments: Vec::new(),
            registered_read_pool: None,
            put_no_space_retry: PutNoSpaceRetryPolicy {
                max_retries: 0,
                initial_backoff_ms: 0,
                max_backoff_ms: 0,
            },
        })
    }

    /// Connect to the MoonCake master and initialise the client.
    ///
    /// The local transfer buffer is an independent shared staging pool;
    /// `preferred_segments` provides allocation affinity for the local node's
    /// disk segment.
    pub fn setup(&mut self, config: &NormalizedMoonCakeConfig) -> Result<()> {
        let c_host = CString::new(config.local_hostname.as_str()).context("local_hostname")?;
        let c_meta = CString::new(config.metadata_server.as_str()).context("metadata_server")?;
        let c_proto = CString::new(config.protocol.as_str()).context("protocol")?;
        let c_dev = CString::new(config.device_name.as_str()).context("device_name")?;
        let c_master =
            CString::new(config.master_server_addr.as_str()).context("master_server_addr")?;

        self.preferred_segments = config
            .preferred_segments
            .iter()
            .map(|s| CString::new(s.as_str()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("preferred_segments")?;
        self.put_no_space_retry = PutNoSpaceRetryPolicy {
            max_retries: config.put_no_space_max_retries,
            initial_backoff_ms: config.put_no_space_retry_initial_backoff_ms,
            max_backoff_ms: config.put_no_space_retry_max_backoff_ms,
        };

        let ret = unsafe {
            mooncake_store_setup(
                self.handle,
                c_host.as_ptr(),
                c_meta.as_ptr(),
                config.global_segment_size,
                config.local_buffer_size,
                c_proto.as_ptr(),
                c_dev.as_ptr(),
                c_master.as_ptr(),
            )
        };
        if ret != 0 {
            return Err(store_error("mooncake_store_setup", "client", ret.into()));
        }

        if matches!(config.protocol.to_ascii_lowercase().as_str(), "ub" | "rdma") {
            // SAFETY: the pool is owned by this Store and explicitly dropped
            // before the Mooncake handle is destroyed.
            self.registered_read_pool = Some(unsafe {
                RegisteredReadBufferPool::new(
                    self.handle,
                    mooncake_store_register_buffer,
                    mooncake_store_unregister_buffer,
                )
            });
        }
        Ok(())
    }

    /// Put raw bytes under a key.
    ///
    /// Mooncake's copy-style `put` API copies `value` into its setup-time
    /// registered allocator before submitting the transfer, so arbitrary Rust
    /// slices are valid for TCP, RDMA, and UB.
    pub fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.put_with_pinning(key, value, true, false)
    }

    /// Put control-plane metadata that must not be removed by memory eviction.
    ///
    /// Explicit repository deletion still works because it uses forced remove.
    pub fn put_hard_pinned(&self, key: &str, value: &[u8]) -> Result<()> {
        self.put_with_pinning(key, value, false, true)
    }

    fn put_with_pinning(
        &self,
        key: &str,
        value: &[u8],
        with_soft_pin: bool,
        with_hard_pin: bool,
    ) -> Result<()> {
        let c_key = CString::new(key).context("key")?;

        let seg_ptrs: Vec<*const std::ffi::c_char> = self
            .preferred_segments
            .iter()
            .map(|cs| cs.as_ptr())
            .collect();

        let config = MoonCakeReplicateConfig {
            replica_num: 1,
            with_soft_pin: i32::from(with_soft_pin),
            with_hard_pin: i32::from(with_hard_pin),
            preferred_segments: if seg_ptrs.is_empty() {
                std::ptr::null()
            } else {
                seg_ptrs.as_ptr()
            },
            preferred_segments_count: seg_ptrs.len(),
        };

        let retry_started = Instant::now();
        let mut retries = 0;
        let mut base_backoff_ms = self.put_no_space_retry.initial_backoff_ms;

        loop {
            let ret = unsafe {
                mooncake_store_put(
                    self.handle,
                    c_key.as_ptr(),
                    value.as_ptr() as *const std::ffi::c_void,
                    value.len(),
                    &config,
                )
            };
            if ret == 0 {
                if retries > 0 {
                    info!(
                        key,
                        size_bytes = value.len() as u64,
                        retries,
                        elapsed_ms = retry_started.elapsed().as_millis() as u64,
                        "MoonCake PUT recovered after waiting for available space"
                    );
                }
                return Ok(());
            }

            if i64::from(ret) != MOONCAKE_NO_AVAILABLE_HANDLE
                || retries >= self.put_no_space_retry.max_retries
            {
                let error = store_error(
                    "mooncake_store_put",
                    format!("'{key}', {}B", value.len()),
                    ret.into(),
                );
                if i64::from(ret) == MOONCAKE_NO_AVAILABLE_HANDLE && retries > 0 {
                    return Err(error).with_context(|| {
                        format!(
                            "MoonCake PUT still has no available handle after {retries} retries over {}ms",
                            retry_started.elapsed().as_millis()
                        )
                    });
                }
                return Err(error);
            }

            retries += 1;
            let delay_ms = jittered_backoff_ms(base_backoff_ms);
            if retries == 1 {
                warn!(
                    key,
                    size_bytes = value.len() as u64,
                    retry = retries,
                    max_retries = self.put_no_space_retry.max_retries,
                    delay_ms,
                    "MoonCake PUT has no available handle; waiting for eviction before retry"
                );
            } else {
                debug!(
                    key,
                    size_bytes = value.len() as u64,
                    retry = retries,
                    max_retries = self.put_no_space_retry.max_retries,
                    delay_ms,
                    "MoonCake PUT still has no available handle; retrying after backoff"
                );
            }
            thread::sleep(Duration::from_millis(delay_ms));
            base_backoff_ms = base_backoff_ms
                .saturating_mul(2)
                .min(self.put_no_space_retry.max_backoff_ms);
        }
    }

    /// Read bytes for a key into a pre-allocated buffer.
    ///
    /// Returns the number of bytes actually read.
    pub fn get_into(&self, key: &str, buf: &mut [u8]) -> Result<i64> {
        let c_key = CString::new(key).context("key")?;
        let ret = if let Some(pool) = self
            .registered_read_pool
            .as_ref()
            .filter(|_| !buf.is_empty())
        {
            let mut registered = pool.acquire(buf.len())?;
            let ret = unsafe {
                mooncake_store_get_into(
                    self.handle,
                    c_key.as_ptr(),
                    registered.as_mut_ptr(),
                    buf.len(),
                )
            };
            if ret >= 0 {
                let bytes_read = ret as usize;
                ensure!(
                    bytes_read <= buf.len(),
                    "mooncake_store_get_into('{key}') returned {bytes_read}B for a {}B buffer",
                    buf.len()
                );
                buf[..bytes_read].copy_from_slice(registered.as_slice(bytes_read));
            }
            ret
        } else {
            unsafe {
                mooncake_store_get_into(
                    self.handle,
                    c_key.as_ptr(),
                    buf.as_mut_ptr() as *mut c_void,
                    buf.len(),
                )
            }
        };
        if ret < 0 {
            return Err(store_error(
                "mooncake_store_get_into",
                format!("'{key}'"),
                ret,
            ));
        }
        Ok(ret)
    }

    /// Convenience: read a key into a `Vec<u8>`.
    ///
    /// First queries the object size, then allocates and reads.
    pub fn get(&self, key: &str) -> Result<Vec<u8>> {
        const MAX_READ_ATTEMPTS: usize = 3;

        for attempt in 0..MAX_READ_ATTEMPTS {
            let Some(size) = self.get_size(key)? else {
                return Ok(Vec::new());
            };
            if size == 0 {
                return Ok(Vec::new());
            }

            let mut buf = vec![0u8; size];
            match self.get_into(key, &mut buf) {
                Ok(n) => {
                    buf.truncate(n as usize);
                    return Ok(buf);
                }
                Err(error)
                    if attempt + 1 < MAX_READ_ATTEMPTS
                        && mooncake_error_code(&error) == Some(MOONCAKE_OBJECT_NOT_FOUND) =>
                {
                    // A mutable catalog object may have been replaced between
                    // get_size and get_into. Re-query its size and retry.
                }
                Err(error) => return Err(error),
            }
        }

        unreachable!("the read loop always returns on its last attempt")
    }

    /// Check whether a key exists.
    pub fn exists(&self, key: &str) -> Result<bool> {
        let c_key = CString::new(key).context("key")?;
        let ret = unsafe { mooncake_store_is_exist(self.handle, c_key.as_ptr()) };
        if ret < 0 {
            return Err(store_error(
                "mooncake_store_is_exist",
                format!("'{key}'"),
                ret.into(),
            ));
        }
        Ok(ret == 1)
    }

    /// Get the stored object size for a key.
    ///
    /// Returns `None` if the key does not exist.
    pub fn get_size(&self, key: &str) -> Result<Option<usize>> {
        let c_key = CString::new(key).context("key")?;
        let ret = unsafe { mooncake_store_get_size(self.handle, c_key.as_ptr()) };
        if ret == MOONCAKE_OBJECT_NOT_FOUND {
            return Ok(None);
        }
        if ret < 0 {
            return Err(store_error(
                "mooncake_store_get_size",
                format!("'{key}'"),
                ret,
            ));
        }
        let size = usize::try_from(ret).context("MoonCake object size does not fit usize")?;
        Ok(Some(size))
    }

    /// Delete a single key.
    ///
    /// Returns `Ok(())` even if the key does not exist (idempotent).
    pub fn remove(&self, key: &str, force: bool) -> Result<()> {
        let c_key = CString::new(key).context("key")?;
        let ret = unsafe {
            mooncake_store_remove(self.handle, c_key.as_ptr(), if force { 1 } else { 0 })
        };
        if i64::from(ret) == MOONCAKE_OBJECT_NOT_FOUND {
            return Ok(());
        }
        if ret != 0 {
            return Err(store_error(
                "mooncake_store_remove",
                format!("'{key}'"),
                ret.into(),
            ));
        }
        Ok(())
    }

    /// Delete all keys matching a regex pattern.
    ///
    /// Returns the number of keys removed, or -1 on error.
    pub fn remove_by_regex(&self, pattern: &str, force: bool) -> Result<i64> {
        let c_pattern = CString::new(pattern).context("pattern")?;
        let ret = unsafe {
            mooncake_store_remove_by_regex(
                self.handle,
                c_pattern.as_ptr(),
                if force { 1 } else { 0 },
            )
        };
        if ret < 0 {
            return Err(store_error(
                "mooncake_store_remove_by_regex",
                format!("'{pattern}'"),
                ret,
            ));
        }
        Ok(ret)
    }

    /// Put immutable content and treat a concurrent identical-key winner as
    /// successful deduplication.
    ///
    /// Callers must only use this for keys whose bytes can never change.
    pub fn put_immutable(&self, key: &str, value: &[u8]) -> Result<bool> {
        match self.put(key, value) {
            Ok(()) => Ok(true),
            Err(error) if mooncake_error_code(&error) == Some(MOONCAKE_OBJECT_ALREADY_EXISTS) => {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    // ── Chunked object I/O ──────────────────────────────────────────────

    /// Write immutable content with chunk-level concurrent deduplication.
    ///
    /// Returns `true` when this call published the direct key or chunk
    /// metadata commit marker, and `false` when another writer won the race.
    pub fn put_chunked_immutable(&self, key: &str, data: &[u8], chunk_size: u32) -> Result<bool> {
        if chunk_size == 0 || data.len() <= chunk_size as usize {
            return self.put_immutable(key, data);
        }

        let chunk_count = data.len().div_ceil(chunk_size as usize) as u32;
        let meta = ChunkMetadata {
            total_size: data.len() as u64,
            chunk_count,
            chunk_size,
        };

        for i in 0..chunk_count {
            let start = i as usize * chunk_size as usize;
            let end = std::cmp::min(start + chunk_size as usize, data.len());
            let chunk_key = format!("{key}/chunk-{i:08x}");
            self.put_immutable(&chunk_key, &data[start..end])?;
        }

        let meta_key = format!("{key}/meta");
        let meta_json = serde_json::to_vec(&meta).context("serialize chunk metadata")?;
        self.put_immutable(&meta_key, &meta_json)
    }

    /// Read a (possibly chunked) object into a `Vec<u8>`.
    ///
    /// Tries a direct read first for backward compatibility. If the key does
    /// not exist, checks for a `{key}/meta` descriptor and reassembles from
    /// chunks.
    pub fn get_chunked(&self, key: &str) -> Result<Vec<u8>> {
        // Fast path: direct read (small object or legacy data).
        if self.exists(key)? {
            return self.get(key);
        }

        // Chunked path.
        let meta_key = format!("{key}/meta");
        ensure!(
            self.exists(&meta_key)?,
            "object '{key}' not found (neither direct nor chunked)"
        );

        let meta_json = self.get(&meta_key)?;
        let meta: ChunkMetadata =
            serde_json::from_slice(&meta_json).context("deserialize chunk metadata")?;

        let mut buf = Vec::with_capacity(meta.total_size as usize);
        for i in 0..meta.chunk_count {
            let chunk_key = format!("{key}/chunk-{i:08x}");
            let mut chunk_data = self.get(&chunk_key).map_err(|e| {
                anyhow::anyhow!(
                    "read chunk {i}/{count} of '{key}': {e}",
                    count = meta.chunk_count
                )
            })?;
            buf.append(&mut chunk_data);
        }

        Ok(buf)
    }
}

/// Equal jitter in `[base / 2, base]` prevents concurrent publishers that hit
/// the same water mark from retrying in lockstep while preserving the
/// configured maximum delay.
fn jittered_backoff_ms(base_backoff_ms: u64) -> u64 {
    if base_backoff_ms <= 1 {
        return base_backoff_ms;
    }
    rand::rng().random_range(base_backoff_ms.div_ceil(2)..=base_backoff_ms)
}

impl Drop for Store {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            // Registered regions must be released while the Mooncake handle is
            // still valid.
            drop(self.registered_read_pool.take());
            unsafe { mooncake_store_destroy(self.handle) };
            self.handle = std::ptr::null_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jittered_backoff_stays_within_equal_jitter_window() {
        for _ in 0..100 {
            let delay_ms = jittered_backoff_ms(100);
            assert!((50..=100).contains(&delay_ms));
        }
        assert_eq!(jittered_backoff_ms(0), 0);
        assert_eq!(jittered_backoff_ms(1), 1);
    }

    #[test]
    fn preserves_mooncake_error_code_through_context() {
        let error =
            store_error("mooncake_store_put", "'key', 4B", -600).context("upload managed layer");

        assert_eq!(mooncake_error_code(&error), Some(-600));
        assert!(error.to_string().contains("upload managed layer"));
    }
}
