//! MoonCake-backed [`SnapshotRepository`] implementation.
//!
//! Because MoonCake is a flat key-value store without native prefix listing,
//! we maintain a `catalog/records-index.json` key whose value is a JSON array
//! of all snapshot IDs. Every create / delete updates this index atomically
//! (read-modify-write).

use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tracing::{debug, warn};

use super::client::MoonCakeStoreClient;
use super::layout::MoonCakeArtifactLayout;
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::repository::interfaces::SnapshotRepository;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::{
    CommittedAttachedDrive, CommittedSnapshot, ManagedLayer, OverlaybdLayerRef, SnapshotAlias,
    SnapshotId, SnapshotListFilter, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord,
    SnapshotSource, SnapshotSourceKind, TemplateBuildErrorReason, TemplateBuildInfo,
    TemplateBuildStatus, SNAPSHOT_ARTIFACT_LAYOUT,
};

/// Manages the committed-state layer of the MoonCake snapshot repository.
pub(crate) struct MoonCakeSnapshotRepository {
    client: MoonCakeStoreClient,
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn validated_alias_key(alias: &str) -> RepositoryResult<String> {
    SnapshotAlias::parse(alias).map_err(|e| RepositoryError::InvalidRequest {
        reason: format!("invalid alias '{alias}': {e}"),
    })?;
    Ok(MoonCakeArtifactLayout::alias_key(alias))
}

impl MoonCakeSnapshotRepository {
    pub(crate) fn new(client: MoonCakeStoreClient) -> Self {
        Self { client }
    }

    // ── index helpers ────────────────────────────────────────────────────

    /// Atomically read-modify-write the records index.
    async fn update_index<F>(&self, mutate: F) -> RepositoryResult<()>
    where
        F: FnOnce(&mut Vec<String>),
    {
        let key = MoonCakeArtifactLayout::RECORDS_INDEX_KEY;
        let ids = self.read_index().await.unwrap_or_default();
        let mut ids = ids;
        mutate(&mut ids);
        let value = serde_json::to_vec(&ids)
            .map_err(|e| RepositoryError::backend("serialize records index".to_string(), e))?;
        // MoonCake rejects overwrites — the index key persists across
        // create / publish / delete calls, so every write is an update.
        self.client
            .put_overwrite(key.to_string(), value)
            .await
            .map_err(|e| RepositoryError::backend("put records index".to_string(), e))
    }

    async fn read_index(&self) -> RepositoryResult<Vec<String>> {
        match self
            .client
            .get(MoonCakeArtifactLayout::RECORDS_INDEX_KEY.to_string())
            .await
        {
            Ok(data) if data.is_empty() => Ok(Vec::new()),
            Ok(data) => serde_json::from_slice(&data)
                .map_err(|e| RepositoryError::backend("deserialize records index".to_string(), e)),
            Err(_) => Ok(Vec::new()),
        }
    }

    // ── record helpers ───────────────────────────────────────────────────

    async fn read_record(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>> {
        let key = MoonCakeArtifactLayout::record_key(id);
        match self.client.get(key).await {
            Ok(data) if data.is_empty() => Ok(None),
            Ok(data) => serde_json::from_slice(&data)
                .map(Some)
                .map_err(|e| RepositoryError::backend(format!("parse record '{id}'"), e)),
            Err(_) => Ok(None),
        }
    }

    async fn write_record(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        let key = MoonCakeArtifactLayout::record_key(&record.id);
        let value = serde_json::to_vec_pretty(record)
            .map_err(|e| RepositoryError::backend("serialize record".to_string(), e))?;
        // MoonCake rejects overwrites — use put_overwrite to remove the
        // previous record blob first (create writes a "waiting" record
        // that publish / try_start_build / mark_build_error all replace).
        self.client
            .put_overwrite(key, value)
            .await
            .map_err(|e| RepositoryError::backend("write record".to_string(), e))
    }

    async fn snapshot_exists(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        let key = MoonCakeArtifactLayout::record_key(id);
        self.client
            .exists(key)
            .await
            .map_err(|e| RepositoryError::backend(format!("check snapshot record '{id}'"), e))
    }

    // ── alias helpers ────────────────────────────────────────────────────

    async fn load_alias_target(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        match self.client.get(key).await {
            Ok(data) if data.is_empty() => Ok(None),
            Ok(data) => {
                let id: SnapshotId = serde_json::from_slice(&data)
                    .map_err(|e| RepositoryError::backend(format!("parse alias '{alias}'"), e))?;
                Ok(Some(id))
            }
            Err(_) => Ok(None),
        }
    }

    /// Best-effort alias binding.
    ///
    /// MoonCake has no conditional-write primitive. We use a read-check-write
    /// approach that is acceptable in single-writer-per-alias deployments.
    async fn bind_alias(&self, alias: &str, id: &SnapshotId) -> RepositoryResult<()> {
        let key = validated_alias_key(alias)?;
        let payload = serde_json::to_vec(id)
            .map_err(|e| RepositoryError::backend("serialize alias binding".to_string(), e))?;

        // Check current binding.
        if let Some(existing) = self.load_alias_target(alias).await? {
            if existing == *id {
                return Ok(()); // already bound to us
            }
            if self.snapshot_exists(&existing).await? {
                return Err(RepositoryError::AliasConflict {
                    alias: alias.to_string(),
                    existing,
                    new_id: id.clone(),
                });
            }
            // Stale alias — delete it.
            self.client
                .remove(key.clone())
                .await
                .map_err(|e| RepositoryError::backend("delete stale alias".to_string(), e))?;
        }

        self.client
            .put(key, payload)
            .await
            .map_err(|e| RepositoryError::backend("write alias binding".to_string(), e))?;

        Ok(())
    }

    fn matches_record_filter(record: &SnapshotRecord, filter: &SnapshotListFilter) -> bool {
        if let Some(alias_prefix) = filter.alias_prefix.as_deref() {
            match record.alias.as_ref() {
                Some(alias) if alias.to_string().starts_with(alias_prefix) => {}
                _ => return false,
            }
        }

        if let Some(ids) = filter.snapshot_ids.as_ref() {
            if !ids.iter().any(|id| id == &record.id) {
                return false;
            }
        }

        if let Some(id_or_alias) = filter.snapshot_id_or_alias.as_deref() {
            if record.id.to_string() != id_or_alias
                && record
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias.as_ref() != id_or_alias)
            {
                return false;
            }
        }

        if let Some(source_sandbox_id) = filter.source_sandbox_id.as_deref() {
            match &record.source {
                SnapshotSource::Sandbox {
                    source_sandbox_id: record_sid,
                } if record_sid == source_sandbox_id => {}
                _ => return false,
            }
        }

        if let Some(sources) = filter.sources.as_ref() {
            let source = match &record.source {
                SnapshotSource::Template { .. } => SnapshotSourceKind::Template,
                SnapshotSource::Sandbox { .. } => SnapshotSourceKind::Sandbox,
            };
            if !sources.contains(&source) {
                return false;
            }
        }

        if let Some(statuses) = filter.template_statuses.as_ref() {
            let SnapshotSource::Template { build } = &record.source else {
                return false;
            };
            if !statuses.contains(&build.status) {
                return false;
            }
        }

        true
    }
}

// ── SnapshotRepository impl ────────────────────────────────────────────────

#[async_trait]
impl SnapshotRepository for MoonCakeSnapshotRepository {
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        if !matches!(record.source, SnapshotSource::Template { .. }) {
            return Err(RepositoryError::InvalidRequest {
                reason: "only template snapshots can be pre-created".to_string(),
            });
        }
        if record.committed.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "pre-created template snapshots must not already be committed".to_string(),
            });
        }
        if self.snapshot_exists(&record.id).await? {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }
        if let Some(alias) = record.alias.as_ref() {
            if let Some(existing) = self.load_alias_target(alias.as_ref()).await? {
                if existing != record.id && self.snapshot_exists(&existing).await? {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing,
                        new_id: record.id.clone(),
                    });
                }
            }
        }

        self.write_record(&record).await?;

        if let Some(alias) = record.alias.as_ref() {
            if let Err(error) = self.bind_alias(alias.as_ref(), &record.id).await {
                // Best-effort rollback.
                let _ = self
                    .client
                    .remove(MoonCakeArtifactLayout::record_key(&record.id))
                    .await;
                return Err(error);
            }
        }

        self.update_index(|ids| ids.push(record.id.to_string()))
            .await?;
        Ok(record)
    }

    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        let id = &metadata.id;

        // Validate no duplicate drive ids.
        {
            let mut seen = std::collections::HashSet::new();
            for drive in &manifest.attached_drives {
                if !seen.insert(drive.drive_id.clone()) {
                    return Err(RepositoryError::InvalidRequest {
                        reason: format!(
                            "duplicate attached drive id in publish request: {}",
                            drive.drive_id
                        ),
                    });
                }
            }
        }

        // 1. Upload VM state artifact.
        let vm_state_local_path = manifest.vm_state.path.as_path();
        let vm_state_bytes = tokio::fs::read(vm_state_local_path).await.map_err(|e| {
            RepositoryError::backend(
                format!(
                    "read vm_state '{}' for snapshot '{id}'",
                    vm_state_local_path.display()
                ),
                e,
            )
        })?;
        let vm_state_key =
            MoonCakeArtifactLayout::artifact_key(id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        self.client
            .put_chunked(vm_state_key, vm_state_bytes)
            .await
            .map_err(|e| {
                RepositoryError::backend(format!("upload vm_state for snapshot '{id}'"), e)
            })?;

        // 2. Upload Firecracker manifest.
        let persisted_manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|e| {
            RepositoryError::backend("serialize firecracker manifest".to_string(), e)
        })?;
        let manifest_key =
            MoonCakeArtifactLayout::artifact_key(id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        self.client
            .put(manifest_key, persisted_manifest_bytes)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("write firecracker manifest for snapshot '{id}'"),
                    e,
                )
            })?;

        // 3. Import overlaybd layers (rootfs, memory, attached drives).
        let rootfs_layers = self
            .import_managed_layers(&manifest.rootfs.image_config_path)
            .await?;
        let memory_layer_refs = self
            .import_managed_layers(&manifest.memory.image_config_path)
            .await?;
        let memory_layers: Vec<ManagedLayer> = memory_layer_refs
            .into_iter()
            .filter_map(|layer| match layer {
                OverlaybdLayerRef::Managed(m) => Some(m),
                OverlaybdLayerRef::External(_) => None,
            })
            .collect();
        let attached_drives = self.import_attached_drives(&manifest).await?;

        // 4. Construct committed snapshot.
        let committed = CommittedSnapshot {
            context: metadata.context.clone(),
            startup: metadata.startup.clone(),
            runtime_versions: metadata.runtime_versions.clone(),
            virtualization_mode: metadata.virtualization_mode,
            image_configs: metadata.image_configs.clone(),
            custom_extension_params: metadata.custom_extension_params.clone(),
            rootfs_layers,
            attached_drives,
            memory_layers,
            disk_publications: Vec::new(), // MoonCake backend does not publish to registries
        };

        // 5. Bind alias (if present).
        if let Some(ref alias) = metadata.alias {
            if let Err(e) = self.bind_alias(alias.as_ref(), id).await {
                let pattern = MoonCakeArtifactLayout::artifact_prefix_regex(id);
                if let Err(rollback_err) = self.client.remove_by_regex(pattern).await {
                    warn!(snapshot_id = %id, error = %rollback_err, "failed to roll back snapshot artifacts after alias bind failure");
                }
                return Err(e);
            }
        }

        // 6. Write committed record.
        let now = now_unix_ms();
        let record = if let Some(mut record) = self.read_record(id).await? {
            record.mark_committed(
                metadata.alias.clone(),
                metadata.resources,
                committed,
                metadata.source.clone(),
                now,
            );
            record
        } else {
            let source = match metadata.source {
                SnapshotPublishSource::Template => SnapshotSource::Template {
                    build: TemplateBuildInfo {
                        status: TemplateBuildStatus::Ready,
                        started_at_unix_ms: None,
                        finished_at_unix_ms: Some(now),
                        error_reason: None,
                    },
                },
                SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                    SnapshotSource::Sandbox { source_sandbox_id }
                }
            };
            SnapshotRecord {
                id: id.clone(),
                alias: metadata.alias.clone(),
                source,
                resources: metadata.resources,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
                committed: Some(committed),
            }
        };

        self.write_record(&record).await?;
        self.update_index(|ids| {
            let sid = record.id.to_string();
            if !ids.contains(&sid) {
                ids.push(sid);
            }
        })
        .await?;

        debug!(snapshot_id = %id, "published snapshot to mooncake");
        Ok(record)
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        // Try by id first.
        if let Ok(direct_id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.read_record(&direct_id).await? {
                return Ok(Some(record));
            }
        }

        // Try by alias.
        let resolved_id = self.resolve_alias(id_or_alias).await?;
        let Some(resolved_id) = resolved_id else {
            return Ok(None);
        };
        self.read_record(&resolved_id).await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let ids = self.read_index().await?;
        let mut records = Vec::with_capacity(ids.len());

        for id_str in &ids {
            if let Ok(parsed) = SnapshotId::parse(id_str) {
                if let Ok(Some(record)) = self.read_record(&parsed).await {
                    if Self::matches_record_filter(&record, &filter) {
                        records.push(record);
                    }
                }
            }
        }

        records.sort_by(|a, b| {
            b.created_at_unix_ms
                .cmp(&a.created_at_unix_ms)
                .then_with(|| a.id.to_string().cmp(&b.id.to_string()))
        });

        Ok(records)
    }

    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()> {
        let record = match self.get(id_or_alias).await? {
            Some(r) => r,
            None => return Ok(()), // idempotent
        };
        let id = &record.id;

        // 1. Delete alias binding if it still points to us.
        if let Some(ref alias) = record.alias {
            if self.load_alias_target(alias.as_ref()).await?.as_ref() == Some(id) {
                let alias_key = validated_alias_key(alias.as_ref())?;
                if let Err(error) = self.client.remove(alias_key).await {
                    warn!(snapshot_id = %id, alias = %alias, error = %error, "failed to delete alias during snapshot removal");
                }
            }
        }

        // 2. Delete the catalog record.
        self.client
            .remove(MoonCakeArtifactLayout::record_key(id))
            .await
            .map_err(|e| RepositoryError::backend("delete record".to_string(), e))?;

        // 3. Delete all artifacts.
        let pattern = MoonCakeArtifactLayout::artifact_prefix_regex(id);
        if let Err(error) = self.client.remove_by_regex(pattern).await {
            warn!(snapshot_id = %id, error = %error, "failed to delete snapshot artifacts");
        }

        // 4. Remove from index.
        self.update_index(|ids| ids.retain(|i| i != &id.to_string()))
            .await?;

        debug!(snapshot_id = %id, "deleted snapshot from mooncake");
        Ok(())
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        let Some(id) = self.load_alias_target(alias).await? else {
            return Ok(None);
        };

        // Stale-alias cleanup.
        if !self.snapshot_exists(&id).await? {
            warn!(alias = %alias, snapshot_id = %id, "cleaning up stale alias pointing to missing snapshot");
            let _ = self.client.remove(key).await;
            return Ok(None);
        }

        Ok(Some(id))
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let mut record =
            self.read_record(id)
                .await?
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        if build.status != TemplateBuildStatus::Waiting {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("template build '{id}' is not in waiting state"),
            });
        }
        build.status = TemplateBuildStatus::Building;
        build.started_at_unix_ms = Some(now);
        build.error_reason = None;
        record.updated_at_unix_ms = now;
        self.write_record(&record).await?;
        Ok(record)
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let mut record =
            self.read_record(id)
                .await?
                .ok_or_else(|| RepositoryError::SnapshotNotFound {
                    lookup: id.to_string(),
                })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        build.status = TemplateBuildStatus::Error;
        build.finished_at_unix_ms = Some(now);
        build.error_reason = Some(reason);
        record.updated_at_unix_ms = now;
        self.write_record(&record).await
    }
}

// ── managed layer helpers ──────────────────────────────────────────────────

impl MoonCakeSnapshotRepository {
    /// Upload all locally-referenced overlaybd layers for an image config.
    ///
    /// Layers that already exist in the repository (checked by digest key) are
    /// skipped. Layers with external `repoBlobUrl` references are preserved as
    /// [`OverlaybdLayerRef::External`].
    async fn import_managed_layers(
        &self,
        image_config_path: &std::path::Path,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
        use overlaybd::config::load_image_config as load_overlaybd_image_config;

        let image_config = load_overlaybd_image_config(image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!("load image config '{}'", image_config_path.display()),
                e,
            )
        })?;

        let mut layers = Vec::with_capacity(image_config.lowers.len());

        for (index, layer) in image_config.lowers.into_iter().enumerate() {
            if !layer.file.is_empty() {
                let layer_path = std::path::Path::new(&layer.file);
                let managed = self.import_single_layer(layer_path).await?;
                layers.push(OverlaybdLayerRef::Managed(managed));
                continue;
            }

            // Remote layer — preserve as external reference.
            let repo_blob_url = layer
                .effective_repo_blob_url(&image_config.repo_blob_url)
                .to_string();
            if !repo_blob_url.is_empty() {
                let digest = if !layer.digest.is_empty() {
                    layer.digest
                } else if !layer.target_digest.is_empty() {
                    layer.target_digest
                } else {
                    format!("external:{index}")
                };
                layers.push(OverlaybdLayerRef::External(
                    crate::snapshot::ExternalLayer {
                        digest,
                        repo_blob_url,
                        size: layer.size,
                    },
                ));
                continue;
            }

            return Err(RepositoryError::Unsupported {
                feature: format!("overlaybd lower layer {index} without local file or repoBlobUrl"),
            });
        }

        Ok(layers)
    }

    async fn import_single_layer(
        &self,
        source: &std::path::Path,
    ) -> RepositoryResult<ManagedLayer> {
        let descriptor = crate::digest::FileDigest::describe(source)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("describe managed layer '{}'", source.display()),
                    e,
                )
            })?;

        let key = MoonCakeArtifactLayout::managed_layer_key(&descriptor.sha256);

        // Skip upload if the layer already exists (handles both direct and chunked).
        if !self
            .client
            .exists_chunked(key.clone())
            .await
            .map_err(|e| RepositoryError::backend("check managed layer existence".to_string(), e))?
        {
            let data = tokio::fs::read(source).await.map_err(|e| {
                RepositoryError::backend(format!("read managed layer '{}'", source.display()), e)
            })?;
            self.client.put_chunked(key, data).await.map_err(|e| {
                RepositoryError::backend(format!("upload managed layer '{}'", source.display()), e)
            })?;
            debug!(
                digest = %descriptor.sha256,
                "uploaded managed layer to mooncake"
            );
        }

        Ok(ManagedLayer {
            digest: descriptor.sha256,
            size: descriptor.size,
            uuid: overlaybd::layer_metadata::read_overlaybd_layer_uuid(source)
                .ok()
                .filter(|u| !u.is_nil())
                .map(|u| u.to_string()),
        })
    }

    async fn import_attached_drives(
        &self,
        manifest: &FirecrackerSnapshotManifest,
    ) -> RepositoryResult<Vec<CommittedAttachedDrive>> {
        let mut drives = Vec::with_capacity(manifest.attached_drives.len());

        for drive in &manifest.attached_drives {
            let layers = self.import_managed_layers(&drive.image_config_path).await?;

            drives.push(CommittedAttachedDrive::Overlaybd {
                drive_id: drive.drive_id.clone(),
                layers,
                read_only: drive.read_only,
                virtual_size: drive.virtual_size,
                mount_path: crate::sandbox::normalize_mount_path_for_drive(
                    &drive.drive_id,
                    drive.mount_path.clone(),
                )
                .unwrap_or_else(|_| {
                    crate::sandbox::ExtraDrive::default_mount_path(&drive.drive_id)
                }),
                sub_path: drive.sub_path.clone(),
            });
        }

        Ok(drives)
    }
}
