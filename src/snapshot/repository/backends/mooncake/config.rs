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

        ensure!(
            local_buffer_size > 0,
            "backend.mooncake.local_buffer_size must be greater than zero"
        );
        ensure!(
            max_object_size == 0 || local_buffer_size >= u64::from(max_object_size),
            "backend.mooncake.local_buffer_size ({local_buffer_size}) must be at least \
             max_object_size ({max_object_size})"
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
        }
    }

    #[test]
    fn defaults_chunk_size_and_local_buffer_independently() {
        let normalized = NormalizedMoonCakeConfig::new(&config()).expect("normalize config");

        assert_eq!(normalized.max_object_size, DEFAULT_MAX_OBJECT_SIZE);
        assert_eq!(normalized.local_buffer_size, DEFAULT_LOCAL_BUFFER_SIZE);
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
}
