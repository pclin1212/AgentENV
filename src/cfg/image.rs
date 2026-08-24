use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Result};
use confique::Config;
use overlaybd::config::lexically_normalize_path;

const IMAGE_CACHE_COMMIT_DIR: &str = "commits";
const IMAGE_CACHE_REMOTE_BLOCKS_DIR: &str = "remote-blocks";
const BYTES_PER_GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Config, Clone)]
pub struct ImageConfig {
    #[config(nested)]
    pub resolver: ImageResolverConfig,
    #[config(nested)]
    pub cache: ImageCacheConfig,
}

#[derive(Debug, Config, Clone)]
pub struct ImageResolverConfig {
    #[config(default = "ubuntu:24.04")]
    pub default_image: String,
    #[config(default = ["docker.io", "ghcr.io"])]
    pub search_registries: Vec<String>,
    /// Allow AgentENV to convert standard OCI tar layers into local overlaybd
    /// commits. When false, non-overlaybd OCI images fail unless an
    /// overlaybd-native referrer is selected first.
    #[config(default = true)]
    pub convert_standard_oci: bool,
    /// Whitelist of registry hosts (e.g. `docker.io`, `ghcr.io`,
    /// `registry.example.com:5000`). `None` (key omitted) imposes no
    /// restriction; an explicit empty list `[]` rejects every registry.
    /// Otherwise only image references whose registry host is in this list are
    /// resolvable; anything else is rejected with an `ImageError::InvalidReference`.
    /// This gate is applied to both fully-qualified references and the
    /// candidates produced by expanding short references via
    /// `search_registries`.
    pub allowed_registries: Option<Vec<String>>,
    /// Reference prefixes for which standard OCI images are checked for OCI
    /// referrers with an overlaybd-native artifact before falling back to local
    /// layer conversion. Prefixes are matched with `starts_with`; include the
    /// trailing slash yourself, e.g. `registry.example.com/` or
    /// `registry.example.com/team/`.
    #[config(default = [])]
    pub try_referrers_overlaybd_prefixes: Vec<String>,
}

#[derive(Debug, Config, Clone)]
pub struct ImageCacheConfig {
    #[config(default = "$AENV_HOME/image-cache")]
    pub root_dir: PathBuf,
    /// Budget for capacity-driven eviction of local commit bytes. Enforced only
    /// when `[image.cache.gc].enabled` is set: the GC evicts least-recently-used
    /// source configs once usage crosses the high watermark. Unset = no cap.
    pub capacity_gb: Option<u64>,
    #[config(nested)]
    pub remote_blocks: ImageRemoteBlocksCacheConfig,
    #[config(nested)]
    pub gc: ImageCacheGcConfig,
}

#[derive(Debug, Config, Clone)]
pub struct ImageRemoteBlocksCacheConfig {
    /// Optional override for the rootfs registryfs_v2 block cache. Other
    /// generated OverlayBD configs derive isolated sibling directories from
    /// this path so their independent eviction lifecycles cannot interfere.
    pub cache_dir: Option<PathBuf>,
    #[config(default = 10u32)]
    pub max_size_gb: u32,
}

/// Background hard-commit GC scheduling. When enabled it runs the
/// reconcile/plan/verify/delete pass on each interval.
#[derive(Debug, Config, Clone)]
pub struct ImageCacheGcConfig {
    #[config(default = true)]
    pub enabled: bool,
    #[config(default = 1800u64)]
    pub interval_secs: u64,
    /// Min time since last use before a source config is eligible for
    /// capacity-driven eviction (the LRU floor).
    #[config(default = 600u64)]
    pub min_age_secs: u64,
    /// Start evicting when local commit bytes exceed `capacity_gb` * this ratio.
    #[config(default = 0.95)]
    pub high_watermark_ratio: f64,
    /// Evict down to `capacity_gb` * this ratio once the high watermark trips.
    #[config(default = 0.70)]
    pub low_watermark_ratio: f64,
}

/// Derived image-cache paths and limits used by runtime components.
#[derive(Clone, Debug)]
pub struct ResolvedImageCacheConfig {
    pub root_dir: PathBuf,
    pub commit_store: PathBuf,
    pub remote_blocks_dir: PathBuf,
    pub remote_blocks_size_gb: u32,
    pub capacity_bytes: Option<u64>,
}

impl ResolvedImageCacheConfig {
    /// Derive an isolated cache directory beside the configured rootfs remote
    /// block cache while keeping all OverlayBD caches on the same filesystem.
    pub(crate) fn isolated_cache_dir(&self, name: &str) -> PathBuf {
        let parent = self
            .remote_blocks_dir
            .parent()
            .unwrap_or(self.root_dir.as_path());
        match self.remote_blocks_dir.file_name().and_then(|v| v.to_str()) {
            Some(IMAGE_CACHE_REMOTE_BLOCKS_DIR) => parent.join(name),
            Some(file_name) if !file_name.is_empty() => parent.join(format!("{file_name}-{name}")),
            _ => self.root_dir.join(name),
        }
    }
}

/// Resolved background hard-commit GC schedule.
#[derive(Clone, Debug)]
pub struct ResolvedImageCacheGcConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub min_age: Duration,
    /// Validated to `0 < low_watermark_ratio <= high_watermark_ratio <= 1`.
    pub high_watermark_ratio: f64,
    pub low_watermark_ratio: f64,
}

impl ResolvedImageCacheGcConfig {
    /// Watermark byte budgets derived from the cache `capacity_bytes`. `None`
    /// when capacity is unset (capacity-driven eviction is then disabled).
    pub(crate) fn watermark_bytes(&self, capacity_bytes: Option<u64>) -> Option<(u64, u64)> {
        let capacity = capacity_bytes? as f64;
        let high = (capacity * self.high_watermark_ratio) as u64;
        let low = (capacity * self.low_watermark_ratio) as u64;
        Some((high, low))
    }
}

impl ImageConfig {
    pub(crate) fn normalize(config: &mut Self, config_dir: &Path, home_path: &Path) {
        config.resolver.normalize();
        config.cache.gc.normalize();
        if let Some(cache_dir) = config.cache.remote_blocks.cache_dir.as_mut() {
            let raw = super::resolve_path(home_path, config_dir, cache_dir);
            *cache_dir = lexically_normalize_path(&raw);
        }
        let raw = super::resolve_path(home_path, config_dir, &config.cache.root_dir);
        config.cache.root_dir = lexically_normalize_path(&raw);
    }
}

impl ImageCacheConfig {
    pub(crate) fn layout(&self) -> ResolvedImageCacheConfig {
        let root_dir = lexically_normalize_path(&self.root_dir);
        let remote_blocks_dir = self
            .remote_blocks
            .cache_dir
            .as_ref()
            .map(|path| lexically_normalize_path(path))
            .unwrap_or_else(|| root_dir.join(IMAGE_CACHE_REMOTE_BLOCKS_DIR));
        let remote_blocks_size_gb = self.remote_blocks.max_size_gb;
        let capacity_bytes = self.capacity_gb.map(|gb| gb.saturating_mul(BYTES_PER_GIB));
        ResolvedImageCacheConfig {
            commit_store: root_dir.join(IMAGE_CACHE_COMMIT_DIR),
            remote_blocks_dir,
            root_dir,
            remote_blocks_size_gb,
            capacity_bytes,
        }
    }

    pub(crate) fn gc_schedule(&self) -> ResolvedImageCacheGcConfig {
        ResolvedImageCacheGcConfig {
            enabled: self.gc.enabled,
            interval: Duration::from_secs(self.gc.interval_secs),
            min_age: Duration::from_secs(self.gc.min_age_secs),
            high_watermark_ratio: self.gc.high_watermark_ratio,
            low_watermark_ratio: self.gc.low_watermark_ratio,
        }
    }
}

impl ImageCacheGcConfig {
    fn normalize(&mut self) {
        if self.interval_secs == 0 {
            self.interval_secs = Self::default().interval_secs;
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_ratio(
            "image.cache.gc.high_watermark_ratio",
            self.high_watermark_ratio,
        )?;
        validate_ratio(
            "image.cache.gc.low_watermark_ratio",
            self.low_watermark_ratio,
        )?;
        if self.low_watermark_ratio > self.high_watermark_ratio {
            bail!(
                "image.cache.gc.low_watermark_ratio ({}) must be <= image.cache.gc.high_watermark_ratio ({})",
                self.low_watermark_ratio,
                self.high_watermark_ratio
            );
        }
        Ok(())
    }
}

fn validate_ratio(name: &str, value: f64) -> Result<()> {
    if !value.is_finite() || value <= 0.0 || value > 1.0 {
        bail!("{name} must be > 0 and <= 1, got {value}");
    }
    Ok(())
}

super::impl_config_default!(
    ImageConfig,
    ImageResolverConfig,
    ImageCacheConfig,
    ImageRemoteBlocksCacheConfig,
    ImageCacheGcConfig,
);

impl ImageResolverConfig {
    fn normalize(&mut self) {
        self.default_image = self.default_image.trim().to_string();
        if self.default_image.is_empty() {
            self.default_image = Self::default().default_image;
        }
        self.search_registries = self
            .search_registries
            .iter()
            .map(|registry| registry.trim().trim_end_matches('/').to_string())
            .collect();
        // Keep `None` (unset = no restriction) distinct from `Some([])`
        // (explicit empty list = deny all). Only trim/drop empty entries.
        self.allowed_registries = self.allowed_registries.as_ref().map(|registries| {
            registries
                .iter()
                .filter_map(|registry| {
                    let host = registry.trim().trim_end_matches('/');
                    (!host.is_empty()).then(|| host.to_string())
                })
                .collect()
        });
        self.try_referrers_overlaybd_prefixes = self
            .try_referrers_overlaybd_prefixes
            .iter()
            .filter_map(|prefix| {
                let prefix = prefix.trim();
                (!prefix.is_empty()).then(|| prefix.to_string())
            })
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_cache_layout_defaults_remote_blocks_under_root() {
        let cache = ImageCacheConfig {
            root_dir: PathBuf::from("/var/lib/aenv/image-cache"),
            ..Default::default()
        };

        let layout = cache.layout();
        assert_eq!(
            layout.remote_blocks_dir,
            Path::new("/var/lib/aenv/image-cache/remote-blocks")
        );
        assert_eq!(
            layout.isolated_cache_dir("memory-blocks"),
            Path::new("/var/lib/aenv/image-cache/memory-blocks")
        );
    }

    #[test]
    fn image_cache_normalizes_explicit_remote_blocks_cache_dir() {
        let mut config = ImageConfig::default();
        config.cache.root_dir = "$AENV_HOME/image-cache".into();
        config.cache.remote_blocks.cache_dir = Some("$AENV_HOME/ssd/remote-blocks".into());

        ImageConfig::normalize(
            &mut config,
            Path::new("/etc/agentenv"),
            Path::new("/data/aenv"),
        );

        let layout = config.cache.layout();
        assert_eq!(
            layout.remote_blocks_dir,
            Path::new("/data/aenv/ssd/remote-blocks")
        );
        assert_eq!(
            layout.isolated_cache_dir("memory-blocks"),
            Path::new("/data/aenv/ssd/memory-blocks")
        );
    }

    #[test]
    fn isolated_cache_dirs_prefix_a_custom_cache_basename() {
        let mut cache = ImageCacheConfig::default();
        cache.remote_blocks.cache_dir = Some("/mnt/nvme/agentenv-cache".into());

        let layout = cache.layout();
        assert_eq!(
            layout.isolated_cache_dir("convert-blocks"),
            Path::new("/mnt/nvme/agentenv-cache-convert-blocks")
        );
    }

    #[test]
    fn validate_rejects_invalid_gc_watermarks() {
        let cases = [
            (
                ImageCacheGcConfig {
                    high_watermark_ratio: 1.2,
                    ..Default::default()
                },
                "high_watermark_ratio",
            ),
            (
                ImageCacheGcConfig {
                    high_watermark_ratio: 0.5,
                    low_watermark_ratio: 0.7,
                    ..Default::default()
                },
                "low_watermark_ratio",
            ),
        ];

        for (config, expected_error) in cases {
            let err = config.validate().unwrap_err();
            assert!(
                err.to_string().contains(expected_error),
                "expected error containing {expected_error:?}, got: {err}"
            );
        }
    }
}
