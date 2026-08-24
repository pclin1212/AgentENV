mod client;
mod config;
mod ffi;
mod layout;
mod repository;
mod resolver;
mod store;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cfg::MoonCakeBackendConfig;
use crate::image::cache::{local_image_services_from_global_config, OverlaybdLayerStore};
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};

pub(crate) use self::client::MoonCakeStoreClient;
pub(crate) use self::config::NormalizedMoonCakeConfig;
pub(crate) use self::layout::MoonCakeArtifactLayout;
pub(crate) use self::repository::MoonCakeSnapshotRepository;
use self::resolver::MoonCakeRuntimeResolver;

/// MoonCake-backed snapshot backend.
///
/// Combines the durable committed-state repository (stored in a MoonCake
/// distributed KV store) with a node-local runtime resolver that materializes
/// runnable overlaybd configs from cached artifacts.
pub struct MoonCakeBackend {
    repository: Arc<dyn SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
}

impl MoonCakeBackend {
    /// Build the MoonCake backend from config.
    ///
    /// This convenience constructor remains available for tests and direct
    /// callers. The main backend factory constructs a shared cache once and
    /// uses [`MoonCakeBackend::from_parts`] instead.
    pub fn new(config: &MoonCakeBackendConfig, cache_root: PathBuf) -> Result<Self> {
        let cache = LocalArtifactCache::new(cache_root.clone(), None)?;
        Self::from_parts(
            config,
            cache,
            cache_root.join("runtime"),
            local_image_services_from_global_config().overlaybd_layers,
        )
    }

    /// Build the MoonCake backend from config plus a shared node-local cache.
    pub(crate) fn from_parts(
        config: &MoonCakeBackendConfig,
        cache: Arc<LocalArtifactCache>,
        runtime_root: PathBuf,
        store: Arc<dyn OverlaybdLayerStore>,
    ) -> Result<Self> {
        let config = NormalizedMoonCakeConfig::new(config)?;
        let client = MoonCakeStoreClient::new_sync(&config)?;

        let repository: Arc<dyn SnapshotRepository> =
            Arc::new(MoonCakeSnapshotRepository::new(client.clone()));

        let managed_layers_repo_blob_url = config.managed_layers_repo_blob_url();
        std::fs::create_dir_all(&runtime_root).with_context(|| {
            format!("create mooncake runtime root '{}'", runtime_root.display())
        })?;
        let runtime_resolver: Arc<dyn SnapshotRuntimeResolver> =
            Arc::new(MoonCakeRuntimeResolver::new(
                Arc::new(client),
                cache,
                runtime_root,
                store,
                managed_layers_repo_blob_url,
            )?);

        Ok(Self {
            repository,
            runtime_resolver,
        })
    }

    /// Splits the backend into its repository and runtime-resolution components.
    pub fn into_parts(
        self,
    ) -> (
        Arc<dyn SnapshotRepository>,
        Arc<dyn SnapshotRuntimeResolver>,
    ) {
        (self.repository, self.runtime_resolver)
    }
}
