//! Async-safe MoonCake store client.
//!
//! All MoonCake C API calls are synchronous / blocking. They run on tokio's
//! blocking thread pool so the async runtime stays responsive. The inner
//! [`Store`](super::store::Store) is wrapped in `Arc` so the client is cheap
//! to clone.

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{ensure, Context, Result};
use tokio::io::AsyncReadExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::{self, JoinSet};

use super::config::NormalizedMoonCakeConfig;
use super::store::{ChunkMetadata, Store};

const WRITE_PERMIT_UNIT: u64 = 4 * 1024;

/// Timings for a file streamed through the bounded chunk upload pipeline.
///
/// `read_ms` is cumulative time spent in file reads, while `upload_work_ms`
/// is cumulative worker time across chunk PUTs. Those values overlap when the
/// pipeline concurrency is greater than one and therefore must not be added;
/// `pipeline_ms` is the actual wall-clock transfer time.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ChunkedFilePutStats {
    pub published: bool,
    pub read_ms: u64,
    pub upload_work_ms: u64,
    pub pipeline_ms: u64,
    pub chunk_count: u32,
    pub concurrency: usize,
}

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
    /// Per-object in-flight chunk limit. The client-wide byte semaphore above
    /// is still authoritative across all concurrently published objects.
    chunk_upload_concurrency: usize,
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
            chunk_upload_concurrency: config.chunk_upload_concurrency,
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
        let permit = self.acquire_write_permit(value.len() as u64).await?;
        self.put_immutable_admitted(key, value, permit).await
    }

    async fn put_immutable_admitted(
        &self,
        key: String,
        value: Vec<u8>,
        permit: OwnedSemaphorePermit,
    ) -> Result<bool> {
        let store = Arc::clone(&self.store);
        task::spawn_blocking(move || {
            let _permit = permit;
            store.put_immutable(&key, &value)
        })
        .await?
    }

    /// Write immutable, possibly chunked content with metadata committed last.
    pub async fn put_chunked_immutable(&self, key: String, value: Vec<u8>) -> Result<bool> {
        let reservation = chunked_write_reservation(value.len() as u64, self.chunk_size);
        let _permit = self.acquire_write_permit(reservation).await?;
        let store = Arc::clone(&self.store);
        let chunk_size = self.chunk_size;
        task::spawn_blocking(move || store.put_chunked_immutable(&key, &value, chunk_size)).await?
    }

    /// Stream an immutable file into MoonCake using bounded read/upload
    /// pipelining.
    ///
    /// Large files are never loaded into one layer-sized `Vec`. The reader
    /// allocates one chunk at a time and continues reading while earlier chunk
    /// PUTs execute on the blocking pool. At most `chunk_upload_concurrency`
    /// chunk buffers are retained for this object, and every PUT additionally
    /// participates in the repository-client-wide byte admission semaphore.
    ///
    /// The `{key}/meta` commit marker is written only after every chunk has
    /// completed successfully. Partial chunks are intentionally retained on
    /// failure: immutable retry or a concurrent writer can safely reuse them,
    /// while readers ignore them until the metadata marker exists.
    pub(crate) async fn put_chunked_file_immutable(
        &self,
        key: String,
        path: &Path,
        total_size: u64,
    ) -> Result<ChunkedFilePutStats> {
        let pipeline_started = Instant::now();

        if self.chunk_size == 0 || total_size <= u64::from(self.chunk_size) {
            let permit = self.acquire_write_permit(total_size).await?;
            let read_started = Instant::now();
            let data = tokio::fs::read(path)
                .await
                .with_context(|| format!("read immutable MoonCake object '{}'", path.display()))?;
            let read_ms = elapsed_ms(read_started);
            ensure!(
                data.len() as u64 == total_size,
                "immutable MoonCake object '{}' changed size while reading: expected {} bytes, got {}",
                path.display(),
                total_size,
                data.len()
            );

            let upload_started = Instant::now();
            let published = self.put_immutable_admitted(key, data, permit).await?;
            return Ok(ChunkedFilePutStats {
                published,
                read_ms,
                upload_work_ms: elapsed_ms(upload_started),
                pipeline_ms: elapsed_ms(pipeline_started),
                chunk_count: 1,
                concurrency: 1,
            });
        }

        let chunk_size = u64::from(self.chunk_size);
        let chunk_count = u32::try_from(total_size.div_ceil(chunk_size))
            .context("MoonCake chunk count exceeds u32")?;
        let concurrency = self
            .chunk_upload_concurrency
            .min(chunk_count as usize)
            .max(1);
        let mut file = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("open immutable MoonCake object '{}'", path.display()))?;
        let mut uploads: JoinSet<Result<Duration>> = JoinSet::new();
        let mut read_elapsed = Duration::ZERO;
        let mut upload_work_elapsed = Duration::ZERO;
        let mut first_error = None;

        for index in 0..chunk_count {
            while uploads.len() >= concurrency {
                collect_one_chunk_upload(&mut uploads, &mut upload_work_elapsed, &mut first_error)
                    .await;
                if first_error.is_some() {
                    break;
                }
            }
            if first_error.is_some() {
                break;
            }

            let offset = u64::from(index) * chunk_size;
            let len = (total_size - offset).min(chunk_size) as usize;
            // Reserve staging capacity before allocating and reading the
            // chunk. This keeps aggregate read-ahead memory bounded by the
            // same client-wide budget as MoonCake's transfer buffers.
            let permit = match self.acquire_write_permit(len as u64).await {
                Ok(permit) => permit,
                Err(error) => {
                    first_error = Some(error.context(format!(
                        "reserve MoonCake upload capacity for chunk {index}/{chunk_count}"
                    )));
                    break;
                }
            };
            let mut chunk = vec![0_u8; len];
            let read_started = Instant::now();
            if let Err(error) = file.read_exact(&mut chunk).await {
                first_error = Some(anyhow::Error::new(error).context(format!(
                    "read chunk {index}/{chunk_count} from '{}'",
                    path.display()
                )));
                break;
            }
            read_elapsed = read_elapsed.saturating_add(read_started.elapsed());

            let client = self.clone();
            let chunk_key = format!("{key}/chunk-{index:08x}");
            uploads.spawn(async move {
                let upload_started = Instant::now();
                client
                    .put_immutable_admitted(chunk_key, chunk, permit)
                    .await
                    .with_context(|| format!("upload MoonCake chunk {index}/{chunk_count}"))?;
                Ok(upload_started.elapsed())
            });
        }

        if first_error.is_none() {
            let mut trailing = [0_u8; 1];
            let read_started = Instant::now();
            match file.read(&mut trailing).await {
                Ok(0) => {}
                Ok(_) => {
                    first_error = Some(anyhow::anyhow!(
                        "immutable MoonCake object '{}' grew while reading beyond declared size {}",
                        path.display(),
                        total_size
                    ));
                }
                Err(error) => {
                    first_error = Some(anyhow::Error::new(error).context(format!(
                        "verify immutable MoonCake object '{}' size",
                        path.display()
                    )));
                }
            }
            read_elapsed = read_elapsed.saturating_add(read_started.elapsed());
        }

        while !uploads.is_empty() {
            collect_one_chunk_upload(&mut uploads, &mut upload_work_elapsed, &mut first_error)
                .await;
        }
        if let Some(error) = first_error {
            return Err(error);
        }

        let metadata = ChunkMetadata {
            total_size,
            chunk_count,
            chunk_size: self.chunk_size,
        };
        let metadata_bytes = serde_json::to_vec(&metadata).context("serialize chunk metadata")?;
        let metadata_started = Instant::now();
        let published = self
            .put_immutable(format!("{key}/meta"), metadata_bytes)
            .await
            .context("commit MoonCake chunk metadata")?;
        upload_work_elapsed = upload_work_elapsed.saturating_add(metadata_started.elapsed());

        Ok(ChunkedFilePutStats {
            published,
            read_ms: duration_ms(read_elapsed),
            upload_work_ms: duration_ms(upload_work_elapsed),
            pipeline_ms: elapsed_ms(pipeline_started),
            chunk_count,
            concurrency,
        })
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

fn elapsed_ms(started: Instant) -> u64 {
    duration_ms(started.elapsed())
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis() as u64
}

async fn collect_one_chunk_upload(
    uploads: &mut JoinSet<Result<Duration>>,
    upload_work_elapsed: &mut Duration,
    first_error: &mut Option<anyhow::Error>,
) {
    let Some(joined) = uploads.join_next().await else {
        if first_error.is_none() {
            *first_error = Some(anyhow::anyhow!(
                "MoonCake chunk upload set became empty unexpectedly"
            ));
        }
        return;
    };

    match joined.context("join MoonCake chunk upload task") {
        Ok(Ok(elapsed)) => {
            *upload_work_elapsed = upload_work_elapsed.saturating_add(elapsed);
        }
        Ok(Err(error)) | Err(error) => {
            if first_error.is_none() {
                *first_error = Some(error);
            }
        }
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
