//! Safe Rust wrapper around the raw MoonCake C FFI.
//!
//! The [`Store`] handle manages CString lifetimes for `preferred_segments` and
//! provides typed, error-propagating methods for each C API call. All methods
//! are synchronous — use [`super::client::MoonCakeStoreClient`] for async access
//! via `spawn_blocking`.

use std::ffi::{c_void, CString};

use anyhow::{ensure, Context, Result};
use overlaybd::backend::mc_buffer_pool::RegisteredReadBufferPool;
use serde::{Deserialize, Serialize};

use super::config::NormalizedMoonCakeConfig;
use super::ffi::*;

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
        })
    }

    /// Connect to the MoonCake master and initialise the client.
    ///
    /// The local transfer buffer is sized to the largest direct object;
    /// `preferred_segments` provides allocation affinity for the local
    /// node's disk segment.
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

        let ret = unsafe {
            mooncake_store_setup(
                self.handle,
                c_host.as_ptr(),
                c_meta.as_ptr(),
                config.global_segment_size,
                config.max_object_size as u64,
                c_proto.as_ptr(),
                c_dev.as_ptr(),
                c_master.as_ptr(),
            )
        };
        if ret != 0 {
            anyhow::bail!("mooncake_store_setup failed: ret={ret}");
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
        let c_key = CString::new(key).context("key")?;

        let seg_ptrs: Vec<*const std::ffi::c_char> = self
            .preferred_segments
            .iter()
            .map(|cs| cs.as_ptr())
            .collect();

        let config = MoonCakeReplicateConfig {
            replica_num: 1,
            with_soft_pin: 1,
            with_hard_pin: 0,
            preferred_segments: if seg_ptrs.is_empty() {
                std::ptr::null()
            } else {
                seg_ptrs.as_ptr()
            },
            preferred_segments_count: seg_ptrs.len(),
        };

        let ret = unsafe {
            mooncake_store_put(
                self.handle,
                c_key.as_ptr(),
                value.as_ptr() as *const std::ffi::c_void,
                value.len(),
                &config,
            )
        };
        if ret != 0 {
            anyhow::bail!(
                "mooncake_store_put('{key}', {}B) failed: ret={ret}",
                value.len()
            );
        }
        Ok(())
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
            anyhow::bail!("mooncake_store_get_into('{key}') failed: ret={ret}");
        }
        Ok(ret)
    }

    /// Convenience: read a key into a `Vec<u8>`.
    ///
    /// First queries the object size, then allocates and reads.
    pub fn get(&self, key: &str) -> Result<Vec<u8>> {
        let size = self.get_size(key)?;
        if size <= 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; size as usize];
        let n = self.get_into(key, &mut buf)?;
        if n >= 0 {
            buf.truncate(n as usize);
        }
        Ok(buf)
    }

    /// Check whether a key exists.
    pub fn exists(&self, key: &str) -> Result<bool> {
        let c_key = CString::new(key).context("key")?;
        let ret = unsafe { mooncake_store_is_exist(self.handle, c_key.as_ptr()) };
        if ret < 0 {
            anyhow::bail!("mooncake_store_is_exist('{key}') failed: ret={ret}");
        }
        Ok(ret == 1)
    }

    /// Get the stored object size for a key.
    ///
    /// Returns -1 if the key does not exist (consistent with the C API).
    pub fn get_size(&self, key: &str) -> Result<i64> {
        let c_key = CString::new(key).context("key")?;
        let ret = unsafe { mooncake_store_get_size(self.handle, c_key.as_ptr()) };
        Ok(ret)
    }

    /// Delete a single key.
    ///
    /// Returns `Ok(())` even if the key does not exist (idempotent).
    pub fn remove(&self, key: &str, force: bool) -> Result<()> {
        let c_key = CString::new(key).context("key")?;
        let ret = unsafe {
            mooncake_store_remove(self.handle, c_key.as_ptr(), if force { 1 } else { 0 })
        };
        if ret != 0 {
            anyhow::bail!("mooncake_store_remove('{key}') failed: ret={ret}");
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
            anyhow::bail!("mooncake_store_remove_by_regex('{pattern}') failed: ret={ret}");
        }
        Ok(ret)
    }

    // ── Chunked object I/O ──────────────────────────────────────────────

    /// Write an object, automatically chunking it if it exceeds `chunk_size`.
    ///
    /// Small objects (`data.len() <= chunk_size`) are stored directly under
    /// `key` — this preserves backward compatibility.  Large objects are split
    /// into `chunk_size`-byte pieces stored as `{key}/chunk-NNNNNNNN` with a
    /// JSON metadata descriptor at `{key}/meta`.
    pub fn put_chunked(&self, key: &str, data: &[u8], chunk_size: u32) -> Result<()> {
        if chunk_size == 0 || data.len() <= chunk_size as usize {
            return self.put(key, data);
        }

        let chunk_count = data.len().div_ceil(chunk_size as usize) as u32;
        let meta = ChunkMetadata {
            total_size: data.len() as u64,
            chunk_count,
            chunk_size,
        };

        // 1. Write metadata descriptor.
        let meta_key = format!("{key}/meta");
        let meta_json = serde_json::to_vec(&meta).context("serialize chunk metadata")?;
        self.put(&meta_key, &meta_json)?;

        // 2. Write data chunks sequentially.
        for i in 0..chunk_count {
            let start = i as usize * chunk_size as usize;
            let end = std::cmp::min(start + chunk_size as usize, data.len());
            let chunk_key = format!("{key}/chunk-{i:08x}");
            self.put(&chunk_key, &data[start..end])?;
        }

        Ok(())
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
