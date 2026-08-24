//! MoonCake key layout conventions.
//!
//! MoonCake is a flat key-value store without native directory hierarchy.
//! We use `/`-delimited key prefixes to organise objects, mirroring the OSS
//! backend layout.

use crate::snapshot::SnapshotId;

/// Committed object layout for the MoonCake snapshot backend.
///
/// Keys follow the pattern established by the POSIX-fs and OSS backends:
///
/// ```text
/// catalog/records/{id}.json       — SnapshotRecord JSON
/// catalog/aliases/{name}.json     — Alias → SnapshotId mapping
/// catalog/records-index.json      — JSON array of all snapshot IDs
/// artifacts/{id}/vm_state.bin     — Firecracker VM state
/// artifacts/{id}/firecracker-manifest.json
/// artifacts/{id}/mem_image.json
/// managed-layers/{digest}         — Content-addressed overlaybd layer blob
/// ```
pub(crate) struct MoonCakeArtifactLayout;

impl MoonCakeArtifactLayout {
    /// Single-key workaround for MoonCake's lack of native key listing.
    pub const RECORDS_INDEX_KEY: &'static str = "catalog/records-index.json";

    pub fn record_key(id: &SnapshotId) -> String {
        format!("catalog/records/{id}.json")
    }

    pub fn alias_key(alias: &str) -> String {
        format!("catalog/aliases/{alias}.json")
    }

    pub fn artifact_key(snapshot_id: &SnapshotId, relative_path: &str) -> String {
        format!("artifacts/{snapshot_id}/{relative_path}")
    }

    /// Regex pattern that matches all artifact keys for a given snapshot.
    /// The leading `^` ensures we only match the exact prefix.
    pub fn artifact_prefix_regex(snapshot_id: &SnapshotId) -> String {
        format!(r"^artifacts/{}/", regex_escape(&snapshot_id.to_string()))
    }

    pub fn managed_layer_key(digest: &str) -> String {
        format!("managed-layers/{digest}")
    }
}

fn regex_escape(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '\\' => {
                format!("\\{c}")
            }
            other => other.to_string(),
        })
        .collect()
}
