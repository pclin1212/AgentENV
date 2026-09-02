//! MoonCake-backed [`SnapshotRepository`] implementation.
//!
//! Because MoonCake is a flat key-value store without native prefix listing,
//! we maintain a `catalog/records-index.json` key whose value is a JSON array
//! of all snapshot IDs. Mutable catalog operations are serialized within one
//! AgentENV process; coordinating this index across AgentENV processes remains
//! a separate multi-node concern.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use overlaybd::config::{load_image_config as load_overlaybd_image_config, LayerConfig};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use super::client::MoonCakeStoreClient;
use super::layout::MoonCakeArtifactLayout;
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::repository::interfaces::SnapshotRepository;
use crate::snapshot::repository::{RepositoryError, RepositoryResult};
use crate::snapshot::timing::time_publish_stage;
use crate::snapshot::{
    CommittedAttachedDrive, CommittedSnapshot, ManagedLayer, OverlaybdLayerRef, SnapshotAlias,
    SnapshotId, SnapshotListFilter, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord,
    SnapshotSource, SnapshotSourceKind, TemplateBuildErrorReason, TemplateBuildInfo,
    TemplateBuildStatus, SNAPSHOT_ARTIFACT_LAYOUT,
};

/// Manages the committed-state layer of the MoonCake snapshot repository.
pub(crate) struct MoonCakeSnapshotRepository {
    client: MoonCakeStoreClient,
    /// Serializes catalog read-modify-write transactions within this AgentENV
    /// process. Layer transfers remain concurrent.
    catalog_lock: Mutex<()>,
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
        Self {
            client,
            catalog_lock: Mutex::new(()),
        }
    }

    // ── index helpers ────────────────────────────────────────────────────

    /// Read-modify-write the records index while `catalog_lock` is held.
    async fn update_index_unlocked<F>(&self, mutate: F) -> RepositoryResult<()>
    where
        F: FnOnce(&mut Vec<String>),
    {
        let key = MoonCakeArtifactLayout::RECORDS_INDEX_KEY;
        let mut ids = self.read_index_unlocked().await?;
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

    async fn read_index_unlocked(&self) -> RepositoryResult<Vec<String>> {
        match self
            .client
            .get(MoonCakeArtifactLayout::RECORDS_INDEX_KEY.to_string())
            .await
        {
            Ok(data) if data.is_empty() => Ok(Vec::new()),
            Ok(data) => serde_json::from_slice(&data)
                .map_err(|e| RepositoryError::backend("deserialize records index".to_string(), e)),
            Err(e) => Err(RepositoryError::backend(
                "read records index".to_string(),
                e,
            )),
        }
    }

    // ── record helpers ───────────────────────────────────────────────────

    async fn read_record_unlocked(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        let key = MoonCakeArtifactLayout::record_key(id);
        match self.client.get(key).await {
            Ok(data) if data.is_empty() => Ok(None),
            Ok(data) => serde_json::from_slice(&data)
                .map(Some)
                .map_err(|e| RepositoryError::backend(format!("parse record '{id}'"), e)),
            Err(e) => Err(RepositoryError::backend(format!("read record '{id}'"), e)),
        }
    }

    async fn write_record_unlocked(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
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

    async fn snapshot_exists_unlocked(&self, id: &SnapshotId) -> RepositoryResult<bool> {
        let key = MoonCakeArtifactLayout::record_key(id);
        self.client
            .exists(key)
            .await
            .map_err(|e| RepositoryError::backend(format!("check snapshot record '{id}'"), e))
    }

    // ── alias helpers ────────────────────────────────────────────────────

    async fn load_alias_target_unlocked(
        &self,
        alias: &str,
    ) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        match self.client.get(key).await {
            Ok(data) if data.is_empty() => Ok(None),
            Ok(data) => {
                let id: SnapshotId = serde_json::from_slice(&data)
                    .map_err(|e| RepositoryError::backend(format!("parse alias '{alias}'"), e))?;
                Ok(Some(id))
            }
            Err(e) => Err(RepositoryError::backend(format!("read alias '{alias}'"), e)),
        }
    }

    /// Best-effort alias binding.
    ///
    /// MoonCake has no conditional-write primitive. We use a read-check-write
    /// approach that is acceptable in single-writer-per-alias deployments.
    async fn bind_alias_unlocked(&self, alias: &str, id: &SnapshotId) -> RepositoryResult<()> {
        let key = validated_alias_key(alias)?;
        let payload = serde_json::to_vec(id)
            .map_err(|e| RepositoryError::backend("serialize alias binding".to_string(), e))?;

        // Check current binding.
        if let Some(existing) = self.load_alias_target_unlocked(alias).await? {
            if existing == *id {
                return Ok(()); // already bound to us
            }
            if self.snapshot_exists_unlocked(&existing).await? {
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

    async fn resolve_alias_unlocked(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let key = validated_alias_key(alias)?;
        let Some(id) = self.load_alias_target_unlocked(alias).await? else {
            return Ok(None);
        };

        if !self.snapshot_exists_unlocked(&id).await? {
            warn!(alias = %alias, snapshot_id = %id, "cleaning up stale alias pointing to missing snapshot");
            let _ = self.client.remove(key).await;
            return Ok(None);
        }

        Ok(Some(id))
    }

    async fn get_unlocked(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        if let Ok(direct_id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.read_record_unlocked(&direct_id).await? {
                return Ok(Some(record));
            }
        }

        let Some(resolved_id) = self.resolve_alias_unlocked(id_or_alias).await? else {
            return Ok(None);
        };
        self.read_record_unlocked(&resolved_id).await
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

        let _catalog_guard = self.catalog_lock.lock().await;
        if self.snapshot_exists_unlocked(&record.id).await? {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }
        if let Some(alias) = record.alias.as_ref() {
            if let Some(existing) = self.load_alias_target_unlocked(alias.as_ref()).await? {
                if existing != record.id && self.snapshot_exists_unlocked(&existing).await? {
                    return Err(RepositoryError::AliasConflict {
                        alias: alias.to_string(),
                        existing,
                        new_id: record.id.clone(),
                    });
                }
            }
        }

        self.write_record_unlocked(&record).await?;

        if let Some(alias) = record.alias.as_ref() {
            if let Err(error) = self.bind_alias_unlocked(alias.as_ref(), &record.id).await {
                // Best-effort rollback.
                let _ = self
                    .client
                    .remove(MoonCakeArtifactLayout::record_key(&record.id))
                    .await;
                return Err(error);
            }
        }

        self.update_index_unlocked(|ids| ids.push(record.id.to_string()))
            .await?;
        Ok(record)
    }

    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord> {
        let snapshot_id = metadata.id.clone();
        time_publish_stage(&snapshot_id, "mooncake", "total", async {
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
        let vm_state_bytes = time_publish_stage(&snapshot_id, "mooncake", "vm_state_read", async {
            tokio::fs::read(vm_state_local_path).await.map_err(|e| {
                RepositoryError::backend(
                    format!(
                        "read vm_state '{}' for snapshot '{id}'",
                        vm_state_local_path.display()
                    ),
                    e,
                )
            })
        })
        .await?;
        let vm_state_size = vm_state_bytes.len();
        let vm_state_key =
            MoonCakeArtifactLayout::artifact_key(id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        time_publish_stage(
            &snapshot_id,
            "mooncake",
            "vm_state_upload",
            async {
                self.client
                    .put_chunked_immutable(vm_state_key, vm_state_bytes)
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        RepositoryError::backend(
                            format!("upload vm_state for snapshot '{id}'"),
                            e,
                        )
                    })
            },
        )
        .await?;
        info!(
            snapshot_id = %id,
            operation = "publish",
            component = "mooncake",
            artifact = "vm_state",
            bytes = vm_state_size as u64,
            "snapshot publish artifact uploaded"
        );

        // 2. Upload Firecracker manifest.
        let persisted_manifest_bytes = time_publish_stage(
            &snapshot_id,
            "mooncake",
            "manifest_serialize",
            async {
                serde_json::to_vec_pretty(&manifest).map_err(|e| {
                    RepositoryError::backend("serialize firecracker manifest".to_string(), e)
                })
            },
        )
        .await?;
        let manifest_size = persisted_manifest_bytes.len();
        let manifest_key =
            MoonCakeArtifactLayout::artifact_key(id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        time_publish_stage(
            &snapshot_id,
            "mooncake",
            "manifest_upload",
            async {
                self.client
                    .put_immutable(manifest_key, persisted_manifest_bytes)
                    .await
                    .map(|_| ())
                    .map_err(|e| {
                        RepositoryError::backend(
                            format!("write firecracker manifest for snapshot '{id}'"),
                            e,
                        )
                    })
            },
        )
        .await?;
        info!(
            snapshot_id = %id,
            operation = "publish",
            component = "mooncake",
            artifact = "firecracker_manifest",
            bytes = manifest_size as u64,
            "snapshot publish artifact uploaded"
        );

        // 3. Import overlaybd layers (rootfs, memory, attached drives).
        let rootfs_layers = time_publish_stage(
            &snapshot_id,
            "mooncake",
            "rootfs_layers",
            self.import_managed_layers(id, "rootfs", &manifest.rootfs.image_config_path),
        )
        .await?;
        let memory_layers = time_publish_stage(
            &snapshot_id,
            "mooncake",
            "memory_layers",
            self.import_memory_layers(id, &manifest.memory.image_config_path),
        )
        .await?;
        let attached_drives = time_publish_stage(
            &snapshot_id,
            "mooncake",
            "attached_drives",
            self.import_attached_drives(id, &manifest),
        )
        .await?;

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

        // Serialize the mutable catalog transaction. Large immutable artifact
        // uploads above remain fully concurrent across snapshots.
        let catalog_guard = self.catalog_lock.lock().await;

        // 5. Bind alias (if present).
        if let Some(ref alias) = metadata.alias {
            let bind_result = time_publish_stage(
                &snapshot_id,
                "mooncake",
                "alias_bind",
                self.bind_alias_unlocked(alias.as_ref(), id),
            )
            .await;
            if let Err(e) = bind_result {
                drop(catalog_guard);
                let pattern = MoonCakeArtifactLayout::artifact_prefix_regex(id);
                if let Err(rollback_err) = self.client.remove_by_regex(pattern).await {
                    warn!(snapshot_id = %id, error = %rollback_err, "failed to roll back snapshot artifacts after alias bind failure");
                }
                return Err(e);
            }
        }

        // 6. Write committed record.
        let now = now_unix_ms();
        let existing_record = time_publish_stage(
            &snapshot_id,
            "mooncake",
            "catalog_read",
            self.read_record_unlocked(id),
        )
        .await?;
        let record = if let Some(mut record) = existing_record {
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

        time_publish_stage(
            &snapshot_id,
            "mooncake",
            "catalog_record_write",
            self.write_record_unlocked(&record),
        )
        .await?;
        time_publish_stage(&snapshot_id, "mooncake", "catalog_index_update", async {
            self.update_index_unlocked(|ids| {
                let sid = record.id.to_string();
                if !ids.contains(&sid) {
                    ids.push(sid);
                }
            })
            .await
        })
        .await?;
        drop(catalog_guard);

        debug!(snapshot_id = %id, "published snapshot to mooncake");
        Ok(record)
        })
        .await
    }

    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        let _catalog_guard = self.catalog_lock.lock().await;
        self.get_unlocked(id_or_alias).await
    }

    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        let _catalog_guard = self.catalog_lock.lock().await;
        let ids = self.read_index_unlocked().await?;
        let mut records = Vec::with_capacity(ids.len());

        for id_str in &ids {
            if let Ok(parsed) = SnapshotId::parse(id_str) {
                if let Some(record) = self.read_record_unlocked(&parsed).await? {
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
        let id = {
            let _catalog_guard = self.catalog_lock.lock().await;
            let record = match self.get_unlocked(id_or_alias).await? {
                Some(record) => record,
                None => return Ok(()),
            };
            let id = record.id.clone();

            // Delete the alias binding if it still points to this record.
            if let Some(ref alias) = record.alias {
                if self
                    .load_alias_target_unlocked(alias.as_ref())
                    .await?
                    .as_ref()
                    == Some(&id)
                {
                    let alias_key = validated_alias_key(alias.as_ref())?;
                    if let Err(error) = self.client.remove(alias_key).await {
                        warn!(snapshot_id = %id, alias = %alias, error = %error, "failed to delete alias during snapshot removal");
                    }
                }
            }

            self.client
                .remove(MoonCakeArtifactLayout::record_key(&id))
                .await
                .map_err(|e| RepositoryError::backend("delete record".to_string(), e))?;

            self.update_index_unlocked(|ids| ids.retain(|item| item != &id.to_string()))
                .await?;
            id
        };

        // Artifact cleanup is outside the catalog lock because it may be
        // expensive and cannot make the already-removed record visible again.
        let pattern = MoonCakeArtifactLayout::artifact_prefix_regex(&id);
        if let Err(error) = self.client.remove_by_regex(pattern).await {
            warn!(snapshot_id = %id, error = %error, "failed to delete snapshot artifacts");
        }

        debug!(snapshot_id = %id, "deleted snapshot from mooncake");
        Ok(())
    }

    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let _catalog_guard = self.catalog_lock.lock().await;
        self.resolve_alias_unlocked(alias).await
    }

    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let _catalog_guard = self.catalog_lock.lock().await;
        let mut record = self.read_record_unlocked(id).await?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
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
        self.write_record_unlocked(&record).await?;
        Ok(record)
    }

    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let _catalog_guard = self.catalog_lock.lock().await;
        let mut record = self.read_record_unlocked(id).await?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
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
        self.write_record_unlocked(&record).await
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
        snapshot_id: &SnapshotId,
        layer_group: &str,
        image_config_path: &std::path::Path,
    ) -> RepositoryResult<Vec<OverlaybdLayerRef>> {
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
                let declared_descriptor = (!layer.digest.is_empty() && layer.size > 0)
                    .then_some((layer.digest.as_str(), layer.size));
                let managed = self
                    .import_single_layer(
                        snapshot_id,
                        layer_group,
                        index,
                        layer_path,
                        declared_descriptor,
                    )
                    .await?;
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
                info!(
                    %snapshot_id,
                    operation = "publish",
                    component = "mooncake",
                    stage = "managed_layer",
                    layer_group,
                    layer_index = index as u64,
                    size_bytes = layer.size,
                    uploaded = false,
                    external = true,
                    "snapshot publish layer processed"
                );
                continue;
            }

            return Err(RepositoryError::Unsupported {
                feature: format!("overlaybd lower layer {index} without local file or repoBlobUrl"),
            });
        }

        Ok(layers)
    }

    /// Import memory lowers without dropping inherited MoonCake-managed layers.
    ///
    /// A sandbox restored from MoonCake has remote `mc://` lowers with no
    /// local `file` path. Those lowers are still managed by this repository
    /// and must remain in the next incremental snapshot's memory chain.
    async fn import_memory_layers(
        &self,
        snapshot_id: &SnapshotId,
        image_config_path: &std::path::Path,
    ) -> RepositoryResult<Vec<ManagedLayer>> {
        let image_config = load_overlaybd_image_config(image_config_path).map_err(|e| {
            RepositoryError::backend(
                format!("load memory image config '{}'", image_config_path.display()),
                e,
            )
        })?;

        let mut layers = Vec::with_capacity(image_config.lowers.len());
        for (index, layer) in image_config.lowers.into_iter().enumerate() {
            if !layer.file.is_empty() {
                let layer_path = std::path::Path::new(&layer.file);
                let declared_descriptor = (!layer.digest.is_empty() && layer.size > 0)
                    .then_some((layer.digest.as_str(), layer.size));
                layers.push(
                    self.import_single_layer(
                        snapshot_id,
                        "memory",
                        index,
                        layer_path,
                        declared_descriptor,
                    )
                    .await?,
                );
                continue;
            }

            let repo_blob_url = layer
                .effective_repo_blob_url(&image_config.repo_blob_url)
                .to_string();
            let managed = managed_memory_layer_from_remote_lower(index, layer, &repo_blob_url)?;
            let key = MoonCakeArtifactLayout::managed_layer_key(&managed.digest);
            let exists = self.client.exists_chunked(key.clone()).await.map_err(|e| {
                RepositoryError::backend(format!("check inherited memory layer '{key}'"), e)
            })?;
            if !exists {
                return Err(RepositoryError::ArtifactNotFound {
                    artifact: format!(
                        "memory: inherited managed layer {index} '{}' is missing",
                        managed.digest
                    ),
                });
            }

            info!(
                %snapshot_id,
                operation = "publish",
                component = "mooncake",
                stage = "managed_layer",
                layer_group = "memory",
                layer_index = index as u64,
                digest = %managed.digest,
                size_bytes = managed.size,
                uploaded = false,
                external = false,
                remote_reused = true,
                "snapshot publish layer processed"
            );
            layers.push(managed);
        }

        Ok(layers)
    }

    async fn import_single_layer(
        &self,
        snapshot_id: &SnapshotId,
        layer_group: &str,
        layer_index: usize,
        source: &std::path::Path,
        declared_descriptor: Option<(&str, u64)>,
    ) -> RepositoryResult<ManagedLayer> {
        let layer_start = Instant::now();
        let digest_start = Instant::now();
        let (descriptor, descriptor_reused) =
            resolve_managed_layer_descriptor(source, declared_descriptor).await?;
        let digest_ms = digest_start.elapsed().as_millis() as u64;

        let key = MoonCakeArtifactLayout::managed_layer_key(&descriptor.sha256);

        // Skip upload if the layer already exists (handles both direct and chunked).
        let existence_check_start = Instant::now();
        let exists = self.client.exists_chunked(key.clone()).await.map_err(|e| {
            RepositoryError::backend("check managed layer existence".to_string(), e)
        })?;
        let existence_check_ms = existence_check_start.elapsed().as_millis() as u64;
        let mut read_ms = 0;
        let mut upload_ms = 0;
        let mut uploaded = false;
        if !exists {
            let read_start = Instant::now();
            let data = tokio::fs::read(source).await.map_err(|e| {
                RepositoryError::backend(format!("read managed layer '{}'", source.display()), e)
            })?;
            read_ms = read_start.elapsed().as_millis() as u64;
            let upload_start = Instant::now();
            uploaded = self
                .client
                .put_chunked_immutable(key, data)
                .await
                .map_err(|e| {
                    RepositoryError::backend(
                        format!("upload managed layer '{}'", source.display()),
                        e,
                    )
                })?;
            upload_ms = upload_start.elapsed().as_millis() as u64;
            debug!(
                digest = %descriptor.sha256,
                "uploaded managed layer to mooncake"
            );
        }

        info!(
            %snapshot_id,
            operation = "publish",
            component = "mooncake",
            stage = "managed_layer",
            layer_group,
            layer_index = layer_index as u64,
            source = %source.display(),
            digest = %descriptor.sha256,
            size_bytes = descriptor.size,
            uploaded,
            external = false,
            descriptor_reused,
            digest_ms,
            existence_check_ms,
            read_ms,
            upload_ms,
            elapsed_ms = layer_start.elapsed().as_millis() as u64,
            "snapshot publish layer processed"
        );

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
        snapshot_id: &SnapshotId,
        manifest: &FirecrackerSnapshotManifest,
    ) -> RepositoryResult<Vec<CommittedAttachedDrive>> {
        let mut drives = Vec::with_capacity(manifest.attached_drives.len());

        for drive in &manifest.attached_drives {
            let layer_group = format!("attached_drive:{}", drive.drive_id);
            let layers = self
                .import_managed_layers(snapshot_id, &layer_group, &drive.image_config_path)
                .await?;

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

fn managed_memory_layer_from_remote_lower(
    index: usize,
    layer: LayerConfig,
    repo_blob_url: &str,
) -> RepositoryResult<ManagedLayer> {
    let is_mooncake_managed_layer = repo_blob_url
        .strip_prefix("mc://")
        .and_then(|remainder| remainder.split_once('/'))
        .is_some_and(|(segment, path)| {
            !segment.is_empty() && path.trim_end_matches('/') == "managed-layers"
        });
    if !is_mooncake_managed_layer {
        return Err(RepositoryError::Unsupported {
            feature: format!(
                "memory layer {index} uses non-MoonCake managed repoBlobUrl '{repo_blob_url}'"
            ),
        });
    }

    let digest = if !layer.digest.is_empty() {
        layer.digest
    } else if !layer.target_digest.is_empty() {
        layer.target_digest
    } else {
        return Err(RepositoryError::Unsupported {
            feature: format!("memory layer {index} without digest"),
        });
    };

    Ok(ManagedLayer {
        digest,
        size: layer.size,
        uuid: (!layer.uuid.is_empty()).then_some(layer.uuid),
    })
}

async fn resolve_managed_layer_descriptor(
    source: &std::path::Path,
    declared_descriptor: Option<(&str, u64)>,
) -> RepositoryResult<(crate::digest::FileDigest, bool)> {
    let Some((digest, size)) = declared_descriptor else {
        let descriptor = crate::digest::FileDigest::describe(source)
            .await
            .map_err(|e| {
                RepositoryError::backend(
                    format!("describe managed layer '{}'", source.display()),
                    e,
                )
            })?;
        return Ok((descriptor, false));
    };

    let source_size = tokio::fs::metadata(source)
        .await
        .map_err(|e| {
            RepositoryError::backend(
                format!("read managed layer metadata '{}'", source.display()),
                e,
            )
        })?
        .len();
    if source_size != size {
        return Err(RepositoryError::Backend {
            message: format!(
                "managed layer descriptor size mismatch for '{}': descriptor says {}, file has {}",
                source.display(),
                size,
                source_size
            ),
            source: None,
        });
    }

    Ok((
        crate::digest::FileDigest {
            size,
            sha256: digest.to_string(),
        },
        true,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inherited_mooncake_memory_layer_remains_managed() {
        let layer = LayerConfig {
            digest: "sha256:parent".to_string(),
            size: 76_750_848,
            uuid: "parent-uuid".to_string(),
            ..Default::default()
        };

        let managed =
            managed_memory_layer_from_remote_lower(0, layer, "mc://default/managed-layers/")
                .expect("preserve inherited MoonCake layer");

        assert_eq!(managed.digest, "sha256:parent");
        assert_eq!(managed.size, 76_750_848);
        assert_eq!(managed.uuid.as_deref(), Some("parent-uuid"));
    }

    #[test]
    fn inherited_memory_layer_from_another_repository_is_rejected() {
        let layer = LayerConfig {
            digest: "sha256:parent".to_string(),
            size: 76_750_848,
            ..Default::default()
        };

        let error =
            managed_memory_layer_from_remote_lower(0, layer, "s3://other-bucket/managed-layers")
                .expect_err("reject foreign memory repository");

        assert!(error
            .to_string()
            .contains("uses non-MoonCake managed repoBlobUrl"));
    }

    #[test]
    fn inherited_mooncake_memory_layer_without_digest_is_rejected() {
        let error = managed_memory_layer_from_remote_lower(
            0,
            LayerConfig::default(),
            "mc://default/managed-layers",
        )
        .expect_err("reject inherited layer without content identity");

        assert!(error.to_string().contains("memory layer 0 without digest"));
    }

    #[tokio::test]
    async fn managed_layer_descriptor_reuses_declared_digest() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layer = temp.path().join("layer.commit");
        tokio::fs::write(&layer, b"sealed layer")
            .await
            .expect("write layer");

        let (descriptor, reused) = resolve_managed_layer_descriptor(
            &layer,
            Some(("sha256:declared", b"sealed layer".len() as u64)),
        )
        .await
        .expect("reuse declared descriptor");

        assert!(reused);
        assert_eq!(descriptor.sha256, "sha256:declared");
        assert_eq!(descriptor.size, b"sealed layer".len() as u64);
    }

    #[tokio::test]
    async fn managed_layer_descriptor_hashes_descriptorless_layer() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layer = temp.path().join("layer.commit");
        tokio::fs::write(&layer, b"legacy layer")
            .await
            .expect("write layer");

        let (descriptor, reused) = resolve_managed_layer_descriptor(&layer, None)
            .await
            .expect("hash legacy layer");

        assert!(!reused);
        assert_eq!(descriptor.size, b"legacy layer".len() as u64);
        assert_ne!(descriptor.sha256, "sha256:declared");
    }

    #[tokio::test]
    async fn managed_layer_descriptor_rejects_size_mismatch() {
        let temp = tempfile::tempdir().expect("tempdir");
        let layer = temp.path().join("layer.commit");
        tokio::fs::write(&layer, b"sealed layer")
            .await
            .expect("write layer");

        let error = resolve_managed_layer_descriptor(&layer, Some(("sha256:declared", 1)))
            .await
            .expect_err("reject stale descriptor");

        assert!(error.to_string().contains("descriptor size mismatch"));
    }
}
