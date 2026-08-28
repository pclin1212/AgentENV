//! MoonCake remote backend — reads overlaybd layer blobs from a MoonCake
//! distributed KV store.
//!
//! The backend is modelled on [`super::oss::OssBackend`]: it implements
//! [`VirtualFile`] so the overlaybd runtime (ublk-daemon) can read layer data
//! through the same code paths used for S3/OSS object storage.
//!
//! # Chunked storage
//!
//! MoonCake may impose a per-object size limit (typically tied to
//! `global_segment_size`).  Objects larger than `max_object_size` are stored
//! in fixed-size chunks:
//!
//! ```text
//! {key}/meta           — ChunkMetadata JSON descriptor
//! {key}/chunk-NNNNNNNN — data chunk (0-padded hex index)
//! ```
//!
//! Small objects (≤ max_object_size) are stored directly under `{key}` for
//! backward compatibility.  The read path detects which format is in use by
//! checking for the presence of `{key}/meta` — this is deterministic and does
//! not rely on catching errors from a failed direct read.
//!
//! # Range-read optimisation
//!
//! [`VirtualFile::read_at(offset, len)`] computes exactly which chunks cover
//! `[offset, offset+len)` and reads **only those chunks**.  This is critical
//! for the Direct (no-file-cache) path.  When the file-cache backend wraps
//! `McFile` the full object is downloaded once and subsequent range reads are
//! served from local disk.
//!
//! # URL format
//!
//! ```text
//! mc://{segment}/{key}
//! ```
//!
//! The *segment* (URL host) is informational; the key is the flat KV-store
//! key (e.g. `managed-layers/sha256:abc...`).

use std::cmp::{max, min};
use std::ffi::{c_char, c_int, c_void, CString};
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::backend::mc_buffer_pool::RegisteredReadBufferPool;
use crate::config::McConfig;
use crate::io::virtual_file::VirtualFile;

// ── Inline MoonCake C FFI ─────────────────────────────────────────────────

type MoonCakeStore = *mut c_void;

#[link(name = "mooncake_store")]
extern "C" {
    fn mooncake_store_create() -> MoonCakeStore;
    fn mooncake_store_destroy(store: MoonCakeStore);
    fn mooncake_store_setup(
        store: MoonCakeStore,
        local_hostname: *const c_char,
        metadata_server: *const c_char,
        global_segment_size: u64,
        local_buffer_size: u64,
        protocol: *const c_char,
        device_name: *const c_char,
        master_server_addr: *const c_char,
    ) -> c_int;
    fn mooncake_store_get_into(
        store: MoonCakeStore,
        key: *const c_char,
        buf: *mut c_void,
        buf_len: usize,
    ) -> i64;
    fn mooncake_store_get_size(store: MoonCakeStore, key: *const c_char) -> i64;
    /// Returns 1 if the key exists, 0 if not, < 0 on error.
    fn mooncake_store_is_exist(store: MoonCakeStore, key: *const c_char) -> c_int;
    fn mooncake_store_register_buffer(
        store: MoonCakeStore,
        buffer: *mut c_void,
        size: usize,
    ) -> c_int;
    fn mooncake_store_unregister_buffer(store: MoonCakeStore, buffer: *mut c_void) -> c_int;
}

// ── Store wrapper ──────────────────────────────────────────────────────────

/// Safe wrapper around a raw MoonCake store handle.
///
/// MoonCake is internally thread-safe (the C++ implementation uses internal
/// mutexes).  All methods are synchronous — the caller (`McFile`) invokes them
/// from the tokio blocking thread pool when necessary.
#[derive(Debug)]
struct Store {
    handle: MoonCakeStore,
    registered_read_pool: Option<RegisteredReadBufferPool>,
}

// Safety: MoonCake C API is thread-safe (internal mutexes).
unsafe impl Send for Store {}
unsafe impl Sync for Store {}

impl Store {
    fn create() -> Result<Self> {
        let handle = unsafe { mooncake_store_create() };
        if handle.is_null() {
            bail!("mooncake_store_create returned NULL");
        }
        Ok(Self {
            handle,
            registered_read_pool: None,
        })
    }

    fn setup(&mut self, config: &McConfig) -> Result<()> {
        let c_host = CString::new(config.local_hostname.as_str()).context("local_hostname")?;
        let c_meta = CString::new(config.metadata_server.as_str()).context("metadata_server")?;
        let c_proto = CString::new(config.protocol.as_str()).context("protocol")?;
        let c_dev = CString::new(config.device_name.as_str()).context("device_name")?;
        let c_master =
            CString::new(config.master_server_addr.as_str()).context("master_server_addr")?;

        let ret = unsafe {
            mooncake_store_setup(
                self.handle,
                c_host.as_ptr(),
                c_meta.as_ptr(),
                config.global_segment_size,
                config.max_object_size as u64, // local_buffer_size for transfer buffer
                c_proto.as_ptr(),
                c_dev.as_ptr(),
                c_master.as_ptr(),
            )
        };
        if ret != 0 {
            bail!("mooncake_store_setup failed: ret={ret}");
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

    fn get_into(&self, key: &str, buf: &mut [u8]) -> Result<i64> {
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
            bail!("mooncake_store_get_into('{key}') failed: ret={ret}");
        }
        Ok(ret)
    }

    fn get_size(&self, key: &str) -> Result<i64> {
        let c_key = CString::new(key).context("key")?;
        let ret = unsafe { mooncake_store_get_size(self.handle, c_key.as_ptr()) };
        Ok(ret)
    }

    /// Returns `true` when the key exists in MoonCake.
    fn exists(&self, key: &str) -> Result<bool> {
        let c_key = CString::new(key).context("key")?;
        let ret = unsafe { mooncake_store_is_exist(self.handle, c_key.as_ptr()) };
        if ret < 0 {
            bail!("mooncake_store_is_exist('{key}') failed: ret={ret}");
        }
        Ok(ret == 1)
    }

    /// Read a small KV entry entirely into a `Vec<u8>`.
    ///
    /// This is used for metadata descriptors and small direct objects.  For
    /// large chunked data chunks use [`get_into`](Self::get_into) directly.
    fn get(&self, key: &str) -> Result<Vec<u8>> {
        let size = self.get_size(key)?;
        if size < 0 {
            bail!("key '{key}' not found (get_size returned {size})");
        }
        if size == 0 {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; size as usize];
        let n = self.get_into(key, &mut buf)?;
        if n >= 0 {
            buf.truncate(n as usize);
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

// ── Parsed URL ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ParsedMcUrl {
    /// The flat KV-store key (e.g. `managed-layers/sha256:abc...`).
    key: String,
}

impl ParsedMcUrl {
    fn parse(raw: &str) -> Result<Self> {
        let url = Url::parse(raw).context(format!("invalid mc url {raw}"))?;
        ensure!(
            url.scheme() == "mc",
            "unsupported mc url scheme {}",
            url.scheme()
        );
        let key = url.path().trim_start_matches('/').to_string();
        ensure!(!key.is_empty(), "mc url object key is missing");
        Ok(Self { key })
    }
}

// ── Chunk metadata ────────────────────────────────────────────────────────

/// Stored under `{key}/meta` as JSON.  Describes a chunked object so
/// readers know how to reassemble it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChunkMetadata {
    total_size: u64,
    chunk_count: u32,
    chunk_size: u32,
}

/// Cached classification of a MoonCake object.
#[derive(Debug, Clone)]
enum McObjectKind {
    /// Small object — stored directly as a single KV entry.
    Direct,
    /// Large object split into fixed-size chunks.
    Chunked(ChunkMetadata),
}

#[derive(Debug, Clone)]
struct McObjectInfo {
    total_size: u64,
    kind: McObjectKind,
}

// ── Chunk-range helpers ───────────────────────────────────────────────────

/// Given a byte range `[offset, offset+len)` and the chunk layout, return the
/// (inclusive) range of chunk indices that cover the request.
///
/// Returns `None` when `offset` is past the end of the file.
fn covering_chunks(
    offset: u64,
    len: usize,
    chunk_size: u32,
    total_size: u64,
) -> Option<(u32, u32)> {
    if offset >= total_size {
        return None;
    }
    let cs = chunk_size as u64;
    let first = (offset / cs) as u32;
    if len == 0 {
        return Some((first, first));
    }
    let last_byte = min(offset.saturating_add(len as u64), total_size) - 1;
    let last = (last_byte / cs) as u32;
    Some((first, last))
}

/// For a given chunk in the covering range, compute the byte range *inside
/// the chunk data* that should be extracted.
///
/// Returns `(start_in_chunk, end_in_chunk)` — both offsets relative to the
/// decompressed chunk buffer.
fn chunk_slice_range(
    offset: u64,
    len: usize,
    chunk_size: u32,
    chunk_idx: u32,
    total_size: u64,
) -> (usize, usize) {
    let cs = chunk_size as u64;
    let chunk_start = chunk_idx as u64 * cs;
    let chunk_end = min(chunk_start + cs, total_size);

    let req_start = max(offset, chunk_start);
    let req_end = min(offset.saturating_add(len as u64), chunk_end);

    let take_start = (req_start - chunk_start) as usize;
    let take_end = (req_end - chunk_start) as usize;
    (take_start, take_end)
}

// ── Backend ───────────────────────────────────────────────────────────────

/// Read-only overlaybd backend that fetches layer blobs from a MoonCake
/// distributed KV store.
#[derive(Clone, Debug)]
pub struct McBackend {
    inner: Arc<McBackendInner>,
}

#[derive(Debug)]
struct McBackendInner {
    store: Store,
}

impl McBackend {
    /// Create a new MoonCake backend.
    ///
    /// Initialises the store handle and connects to the MoonCake master.
    pub fn new(config: &McConfig) -> Result<Self> {
        let mut store = Store::create()?;
        store.setup(config)?;
        Ok(Self {
            inner: Arc::new(McBackendInner { store }),
        })
    }

    /// Open a MoonCake object as a read-only [`VirtualFile`].
    ///
    /// `size_hint` is cached internally so that the first [`VirtualFile::size`]
    /// call can skip the C API round-trip when the caller already knows the
    /// object size (e.g. from an image config).
    ///
    /// The returned `McFile` transparently handles both direct (small) and
    /// chunked (large) objects — the caller does not need to know which
    /// storage format was used.
    pub fn open_with_size_hint(
        &self,
        url: impl AsRef<str>,
        size_hint: Option<u64>,
    ) -> Result<Arc<dyn VirtualFile>> {
        let location = ParsedMcUrl::parse(url.as_ref())?;

        Ok(Arc::new(McFile {
            backend: Arc::clone(&self.inner),
            location,
            size_cache: Mutex::new(size_hint),
            object_info: Mutex::new(None),
            full_data: Mutex::new(None),
        }))
    }
}

// ── VirtualFile impl ─────────────────────────────────────────────────────

/// A read-only [`VirtualFile`] backed by a single MoonCake KV object.
///
/// Two internal paths:
///
/// **Direct** (small objects ≤ `max_object_size`): the full payload is fetched
/// on first access and cached in `full_data`.  Range reads slice from the
/// cached buffer.
///
/// **Chunked** (large objects > `max_object_size`): object metadata is read
/// once and cached.  Each [`VirtualFile::read_at`] call reads only the chunks
/// that cover the requested byte range — no full-object buffer is kept.
///
/// In production the overlaybd file-cache backend wraps every `mc://` blob,
/// downloads the full object once, and serves subsequent range reads from its
/// own persisted blocks.  The chunked path here is primarily exercised in
/// Direct mode (no file cache).
pub struct McFile {
    backend: Arc<McBackendInner>,
    location: ParsedMcUrl,
    /// Cached object size (may be seeded from the image config's size hint).
    size_cache: Mutex<Option<u64>>,
    /// Lazily resolved object layout: Direct vs Chunked + total_size.
    object_info: Mutex<Option<McObjectInfo>>,
    /// Full-data cache for **Direct** objects only.  Chunked objects do not
    /// populate this — instead each `read_at` reads only the covering chunks.
    full_data: Mutex<Option<Arc<Vec<u8>>>>,
}

impl McFile {
    /// Determine how this object is stored (Direct vs Chunked).
    ///
    /// Checks for `{key}/meta` first — this is a **deterministic** check, not
    /// an error-driven fallback.  Result is cached in `object_info`.
    async fn ensure_info(&self) -> Result<McObjectInfo> {
        // 1. Check cached info.
        {
            let guard = self.object_info.lock().await;
            if let Some(ref info) = *guard {
                return Ok(info.clone());
            }
        }

        // 2. Probe for chunked meta.
        let meta_key = format!("{}/meta", self.location.key);
        let is_chunked = self
            .backend
            .store
            .exists(&meta_key)
            .context("check chunk meta existence")?;

        let info = if is_chunked {
            let meta_json = self
                .backend
                .store
                .get(&meta_key)
                .context("read chunk metadata")?;
            let meta: ChunkMetadata =
                serde_json::from_slice(&meta_json).context("deserialize chunk metadata")?;
            McObjectInfo {
                total_size: meta.total_size,
                kind: McObjectKind::Chunked(meta),
            }
        } else {
            // 3. Small (direct) object — use get_size.
            let size = self
                .backend
                .store
                .get_size(&self.location.key)
                .context("get_size for direct object")?;
            if size < 0 {
                bail!(
                    "mc object '{}' not found (get_size returned {size})",
                    self.location.key
                );
            }
            McObjectInfo {
                total_size: size as u64,
                kind: McObjectKind::Direct,
            }
        };

        // 4. Cache and return.
        let mut size_guard = self.size_cache.lock().await;
        *size_guard = Some(info.total_size);
        drop(size_guard);

        let mut info_guard = self.object_info.lock().await;
        *info_guard = Some(info.clone());
        Ok(info)
    }

    /// Fetch and cache the full payload for a **Direct** (small) object.
    ///
    /// Only called for `McObjectKind::Direct`.  Chunked objects never
    /// populate `full_data`.
    async fn ensure_full_data_direct(&self) -> Result<Arc<Vec<u8>>> {
        {
            let guard = self.full_data.lock().await;
            if let Some(data) = guard.as_ref() {
                return Ok(Arc::clone(data));
            }
        }

        let mut buf = vec![0u8; self.size().await? as usize];
        if !buf.is_empty() {
            let n = self
                .backend
                .store
                .get_into(&self.location.key, &mut buf)
                .context("read direct object")?;
            if n >= 0 {
                buf.truncate(n as usize);
            }
        }

        let data = Arc::new(buf);
        let mut guard = self.full_data.lock().await;
        *guard = Some(Arc::clone(&data));
        Ok(data)
    }

    /// Read a single chunk from MoonCake.
    ///
    /// The last chunk may be smaller than `meta.chunk_size`.
    async fn read_chunk(&self, index: u32, meta: &ChunkMetadata) -> Result<Vec<u8>> {
        let chunk_key = format!("{}/chunk-{index:08x}", self.location.key);

        let expected_size = if index == meta.chunk_count.saturating_sub(1) {
            // Last chunk — may be smaller.
            meta.total_size as usize - (index as usize * meta.chunk_size as usize)
        } else {
            meta.chunk_size as usize
        };

        let mut buf = vec![0u8; expected_size];
        let n = self
            .backend
            .store
            .get_into(&chunk_key, &mut buf)
            .with_context(|| format!("read chunk {index} of '{}'", self.location.key))?;
        if n >= 0 {
            buf.truncate(n as usize);
        }
        Ok(buf)
    }
}

#[async_trait]
impl VirtualFile for McFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        if len == 0 {
            return Ok(Bytes::new());
        }

        let info = self.ensure_info().await?;

        // Boundary check.
        if offset >= info.total_size {
            return Ok(Bytes::new());
        }
        let effective_len = min(len as u64, info.total_size - offset) as usize;

        match &info.kind {
            // ── Small object: full-data slice ──
            McObjectKind::Direct => {
                let data = self.ensure_full_data_direct().await?;
                let start = offset as usize;
                Ok(Bytes::copy_from_slice(&data[start..start + effective_len]))
            }
            // ── Chunked object: read only covering chunks ──
            McObjectKind::Chunked(meta) => {
                let (first, last) =
                    covering_chunks(offset, effective_len, meta.chunk_size, meta.total_size)
                        .expect("offset already checked against total_size");

                // Fast path: single chunk.
                if first == last {
                    let chunk_data = self.read_chunk(first, meta).await?;
                    let (take_start, take_end) = chunk_slice_range(
                        offset,
                        effective_len,
                        meta.chunk_size,
                        first,
                        meta.total_size,
                    );
                    return Ok(Bytes::copy_from_slice(&chunk_data[take_start..take_end]));
                }

                // Multi-chunk: read and stitch.
                let mut result = Vec::with_capacity(effective_len);
                for i in first..=last {
                    let chunk_data = self.read_chunk(i, meta).await?;
                    let (take_start, take_end) = chunk_slice_range(
                        offset,
                        effective_len,
                        meta.chunk_size,
                        i,
                        meta.total_size,
                    );
                    result.extend_from_slice(&chunk_data[take_start..take_end]);
                }
                Ok(Bytes::from(result))
            }
        }
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }

        let info = self.ensure_info().await?;

        // Boundary check.
        if offset >= info.total_size {
            return Ok(0);
        }
        let effective_len = min(dst.len() as u64, info.total_size - offset) as usize;

        match &info.kind {
            McObjectKind::Direct => {
                let data = self.ensure_full_data_direct().await?;
                let start = offset as usize;
                let n = min(dst.len(), data.len().saturating_sub(start));
                dst[..n].copy_from_slice(&data[start..start + n]);
                Ok(n)
            }
            McObjectKind::Chunked(meta) => {
                let (first, last) =
                    covering_chunks(offset, effective_len, meta.chunk_size, meta.total_size)
                        .expect("offset already checked against total_size");

                let mut written: usize = 0;
                for i in first..=last {
                    let chunk_data = self.read_chunk(i, meta).await?;
                    let (take_start, take_end) = chunk_slice_range(
                        offset,
                        effective_len,
                        meta.chunk_size,
                        i,
                        meta.total_size,
                    );
                    let n = take_end - take_start;
                    dst[written..written + n].copy_from_slice(&chunk_data[take_start..take_end]);
                    written += n;
                }
                Ok(written)
            }
        }
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("mc file backend is read-only; use MoonCakeStoreClient for writes")
    }

    async fn size(&self) -> Result<u64> {
        // Use cached size when available.
        if let Some(size) = *self.size_cache.lock().await {
            return Ok(size);
        }

        // ensure_info populates size_cache as a side effect.
        let info = self.ensure_info().await?;
        Ok(info.total_size)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── URL parsing ───────────────────────────────────────────────────

    #[test]
    fn parse_mc_url_extracts_key() {
        let parsed = ParsedMcUrl::parse("mc://default/managed-layers/sha256:abcdef1234567890")
            .expect("parse mc url");
        assert_eq!(parsed.key, "managed-layers/sha256:abcdef1234567890");
    }

    #[test]
    fn parse_mc_url_rejects_non_mc_scheme() {
        let err = ParsedMcUrl::parse("s3://bucket/key").unwrap_err();
        assert!(err.to_string().contains("unsupported mc url scheme"));
    }

    #[test]
    fn parse_mc_url_rejects_empty_key() {
        let err = ParsedMcUrl::parse("mc://default/").unwrap_err();
        assert!(err.to_string().contains("object key is missing"));
    }

    #[test]
    fn parse_mc_url_accepts_nested_keys() {
        let parsed = ParsedMcUrl::parse("mc://seg-a/catalog/records/snap-001.json")
            .expect("parse nested mc url");
        assert_eq!(parsed.key, "catalog/records/snap-001.json");
    }

    // ── Chunk-range arithmetic ────────────────────────────────────────

    #[test]
    fn covering_chunks_single_chunk() {
        // 10MB file, 4MB chunks, read 64KB at offset 5MB
        let (first, last) =
            covering_chunks(5_000_000, 65536, 4_194_304, 10_000_000).expect("should cover");
        assert_eq!(first, 1); // chunk index 1 (bytes 4_194_304 .. 8_388_608)
        assert_eq!(last, 1); // same chunk
    }

    #[test]
    fn covering_chunks_crosses_boundary() {
        // 10MB file, 4MB chunks, read 2MB starting 1 byte before chunk boundary
        let (first, last) =
            covering_chunks(4_194_303, 2_000_000, 4_194_304, 10_000_000).expect("should cover");
        assert_eq!(first, 0);
        assert_eq!(last, 1); // spans chunk 0 and chunk 1
    }

    #[test]
    fn covering_chunks_past_end() {
        let result = covering_chunks(10_000_000, 100, 4_194_304, 10_000_000);
        assert!(result.is_none());
    }

    #[test]
    fn covering_chunks_zero_len() {
        let (first, last) =
            covering_chunks(0, 0, 4_194_304, 10_000_000).expect("should cover zero-length range");
        assert_eq!(first, 0);
        assert_eq!(last, 0);
    }

    #[test]
    fn chunk_slice_range_middle_of_file() {
        // Read bytes [3_000_000 .. 5_000_000) — chunk_size = 4_194_304
        let (start, end) = chunk_slice_range(3_000_000, 2_000_000, 4_194_304, 0, 10_000_000);
        // chunk-0 covers [0, 4_194_304)
        // request overlaps [3_000_000, 4_194_304)
        assert_eq!(start, 3_000_000);
        assert_eq!(end, 4_194_304);
    }

    #[test]
    fn chunk_slice_range_last_chunk() {
        // 10MB file, chunk_size = 4MB
        // chunk-2 covers [8_388_608, 10_000_000) = 1_611_392 bytes
        // read bytes [9_000_000 .. 10_000_000)
        let (start, end) = chunk_slice_range(9_000_000, 1_000_000, 4_194_304, 2, 10_000_000);
        assert_eq!(start, 9_000_000 - 8_388_608); // 611_392
        assert_eq!(end, 10_000_000 - 8_388_608); // 1_611_392
    }

    #[test]
    fn chunk_slice_range_full_first_chunk() {
        // read_at(0, 4_194_304) on a 4MB chunk
        let (start, end) = chunk_slice_range(0, 4_194_304, 4_194_304, 0, 10_000_000);
        assert_eq!(start, 0);
        assert_eq!(end, 4_194_304);
    }
}
