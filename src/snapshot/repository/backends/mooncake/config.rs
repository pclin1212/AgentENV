//! Normalised configuration for the MoonCake backend.
//!
//! Resolves optional fields to concrete values (auto-detected hostname,
//! default protocol, minimum segment size, etc.) so the rest of the backend
//! does not need to handle missing config at every call site.

use anyhow::{ensure, Result};

use crate::cfg::MoonCakeBackendConfig;

fn detect_hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/proc/sys/kernel/hostname")
                .ok()
                .map(|h| h.trim().to_string())
                .filter(|h| !h.is_empty())
        })
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|h| h.trim().to_string())
                .filter(|h| !h.is_empty())
        })
        .unwrap_or_else(|| "localhost".to_string())
}

/// Default max object size for direct (non-chunked) storage: 4 MiB.
/// Objects larger than this will be split into chunks.
pub(crate) const DEFAULT_MAX_OBJECT_SIZE: u32 = 4 * 1024 * 1024; // 4 MiB

/// Default total transfer staging buffer owned by each MoonCake client.
pub(crate) const DEFAULT_LOCAL_BUFFER_SIZE: u64 = 128 * 1024 * 1024; // 128 MiB

/// Default retry budget for a PUT rejected while MoonCake is reclaiming space.
pub(crate) const DEFAULT_PUT_NO_SPACE_MAX_RETRIES: u32 = 12;

/// Default initial backoff for a PUT rejected with NO_AVAILABLE_HANDLE.
pub(crate) const DEFAULT_PUT_NO_SPACE_RETRY_INITIAL_BACKOFF_MS: u64 = 100;

/// Default maximum base backoff for a PUT rejected with NO_AVAILABLE_HANDLE.
pub(crate) const DEFAULT_PUT_NO_SPACE_RETRY_MAX_BACKOFF_MS: u64 = 2_000;

#[derive(Debug, Clone)]
pub(crate) struct NormalizedMoonCakeConfig {
    pub local_hostname: String,
    pub metadata_server: String,
    pub master_server_addr: String,
    pub global_segment_size: u64,
    pub protocol: String,
    pub device_name: String,
    pub preferred_segments: Vec<String>,
    /// Max object size in bytes before chunking. Objects ≤ this size are stored
    /// directly; larger objects are split into fixed-size chunks with a /meta
    /// descriptor.
    pub max_object_size: u32,
    /// Total client-side transfer staging buffer. Concurrent operations share
    /// this pool, so it must not be coupled to the per-object chunk size.
    pub local_buffer_size: u64,
    /// Number of retries after the initial PUT receives NO_AVAILABLE_HANDLE.
    pub put_no_space_max_retries: u32,
    /// Initial retry backoff in milliseconds.
    pub put_no_space_retry_initial_backoff_ms: u64,
    /// Maximum base retry backoff in milliseconds.
    pub put_no_space_retry_max_backoff_ms: u64,
}

impl NormalizedMoonCakeConfig {
    pub fn new(config: &MoonCakeBackendConfig) -> Result<Self> {
        let local_hostname = config
            .local_hostname
            .clone()
            .unwrap_or_else(detect_hostname);
        let max_object_size = config.max_object_size.unwrap_or(DEFAULT_MAX_OBJECT_SIZE);
        let local_buffer_size = config
            .local_buffer_size
            .unwrap_or(DEFAULT_LOCAL_BUFFER_SIZE);
        let put_no_space_max_retries = config
            .put_no_space_max_retries
            .unwrap_or(DEFAULT_PUT_NO_SPACE_MAX_RETRIES);
        let put_no_space_retry_initial_backoff_ms = config
            .put_no_space_retry_initial_backoff_ms
            .unwrap_or(DEFAULT_PUT_NO_SPACE_RETRY_INITIAL_BACKOFF_MS);
        let put_no_space_retry_max_backoff_ms = config
            .put_no_space_retry_max_backoff_ms
            .unwrap_or(DEFAULT_PUT_NO_SPACE_RETRY_MAX_BACKOFF_MS);

        ensure!(
            local_buffer_size > 0,
            "backend.mooncake.local_buffer_size must be greater than zero"
        );
        ensure!(
            max_object_size == 0 || local_buffer_size >= u64::from(max_object_size),
            "backend.mooncake.local_buffer_size ({local_buffer_size}) must be at least \
             max_object_size ({max_object_size})"
        );
        ensure!(
            put_no_space_max_retries == 0 || put_no_space_retry_initial_backoff_ms > 0,
            "backend.mooncake.put_no_space_retry_initial_backoff_ms must be greater than zero \
             when put_no_space_max_retries is non-zero"
        );
        ensure!(
            put_no_space_max_retries == 0
                || put_no_space_retry_max_backoff_ms >= put_no_space_retry_initial_backoff_ms,
            "backend.mooncake.put_no_space_retry_max_backoff_ms \
             ({put_no_space_retry_max_backoff_ms}) must be at least \
             put_no_space_retry_initial_backoff_ms \
             ({put_no_space_retry_initial_backoff_ms})"
        );

        Ok(Self {
            local_hostname,
            metadata_server: config.metadata_server.clone(),
            master_server_addr: config.master_server_addr.clone(),
            global_segment_size: config.global_segment_size.unwrap_or(0),
            protocol: config.protocol.clone().unwrap_or_else(|| "tcp".to_string()),
            device_name: config.device_name.clone().unwrap_or_default(),
            preferred_segments: config.preferred_segments.clone().unwrap_or_default(),
            max_object_size,
            local_buffer_size,
            put_no_space_max_retries,
            put_no_space_retry_initial_backoff_ms,
            put_no_space_retry_max_backoff_ms,
        })
    }

    /// Generates the `mc://` repoBlobUrl for managed overlaybd layers.
    ///
    /// This URL is written into resolved image config JSON, so that the
    /// overlaybd runtime (ublk-daemon) can read layers through the MoonCake
    /// `mc://` backend.  Uses the first preferred segment as the namespace;
    /// falls back to `"default"` when no preferred segments are configured.
    pub(crate) fn managed_layers_repo_blob_url(&self) -> String {
        let segment = self
            .preferred_segments
            .first()
            .map(|s| s.as_str())
            .unwrap_or("default");
        format!("mc://{segment}/managed-layers")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> MoonCakeBackendConfig {
        MoonCakeBackendConfig {
            metadata_server: "http://127.0.0.1:8080/metadata".to_string(),
            master_server_addr: "127.0.0.1:50051".to_string(),
            local_hostname: Some("test-node".to_string()),
            global_segment_size: None,
            protocol: None,
            device_name: None,
            preferred_segments: None,
            max_object_size: None,
            local_buffer_size: None,
            put_no_space_max_retries: None,
            put_no_space_retry_initial_backoff_ms: None,
            put_no_space_retry_max_backoff_ms: None,
        }
    }

    #[test]
    fn defaults_chunk_size_and_local_buffer_independently() {
        let normalized = NormalizedMoonCakeConfig::new(&config()).expect("normalize config");

        assert_eq!(normalized.max_object_size, DEFAULT_MAX_OBJECT_SIZE);
        assert_eq!(normalized.local_buffer_size, DEFAULT_LOCAL_BUFFER_SIZE);
        assert_eq!(
            normalized.put_no_space_max_retries,
            DEFAULT_PUT_NO_SPACE_MAX_RETRIES
        );
        assert_eq!(
            normalized.put_no_space_retry_initial_backoff_ms,
            DEFAULT_PUT_NO_SPACE_RETRY_INITIAL_BACKOFF_MS
        );
        assert_eq!(
            normalized.put_no_space_retry_max_backoff_ms,
            DEFAULT_PUT_NO_SPACE_RETRY_MAX_BACKOFF_MS
        );
    }

    #[test]
    fn accepts_local_buffer_larger_than_chunk_size() {
        let mut config = config();
        config.max_object_size = Some(8 * 1024 * 1024);
        config.local_buffer_size = Some(256 * 1024 * 1024);

        let normalized = NormalizedMoonCakeConfig::new(&config).expect("normalize config");
        assert_eq!(normalized.max_object_size, 8 * 1024 * 1024);
        assert_eq!(normalized.local_buffer_size, 256 * 1024 * 1024);
    }

    #[test]
    fn rejects_local_buffer_smaller_than_chunk_size() {
        let mut config = config();
        config.max_object_size = Some(8 * 1024 * 1024);
        config.local_buffer_size = Some(4 * 1024 * 1024);

        let error = NormalizedMoonCakeConfig::new(&config).expect_err("invalid config");
        assert!(error
            .to_string()
            .contains("must be at least max_object_size"));
    }

    #[test]
    fn accepts_custom_no_space_retry_policy() {
        let mut config = config();
        config.put_no_space_max_retries = Some(5);
        config.put_no_space_retry_initial_backoff_ms = Some(250);
        config.put_no_space_retry_max_backoff_ms = Some(4_000);

        let normalized = NormalizedMoonCakeConfig::new(&config).expect("normalize config");
        assert_eq!(normalized.put_no_space_max_retries, 5);
        assert_eq!(normalized.put_no_space_retry_initial_backoff_ms, 250);
        assert_eq!(normalized.put_no_space_retry_max_backoff_ms, 4_000);
    }

    #[test]
    fn rejects_invalid_no_space_retry_backoff() {
        let mut config = config();
        config.put_no_space_max_retries = Some(1);
        config.put_no_space_retry_initial_backoff_ms = Some(2_000);
        config.put_no_space_retry_max_backoff_ms = Some(1_000);

        let error = NormalizedMoonCakeConfig::new(&config).expect_err("invalid config");
        assert!(error
            .to_string()
            .contains("put_no_space_retry_max_backoff_ms"));
    }
}
