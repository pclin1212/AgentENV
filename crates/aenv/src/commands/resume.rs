use crate::client::Client;
use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::time::Duration;

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv resume <sandbox-id>
  aenv resume <target-node-url> <sandbox-id>

When a target node is provided, the CLI resumes the existing sandbox there if present.
Otherwise it creates a new sandbox from the source sandbox's latest Mooncake snapshot.")]
pub struct Args {
    /// Sandbox ID, or the target AgentENV node URL when SANDBOX_ID is also supplied.
    #[arg(add = crate::commands::completion::add_paused_sandbox_candidates())]
    node_or_sandbox_id: String,
    /// Sandbox to restore on the target AgentENV node.
    #[arg(add = crate::commands::completion::add_paused_sandbox_candidates())]
    sandbox_id: Option<String>,
    /// TTL in seconds from now. Must be longer than the sandbox's current TTL.
    #[arg(long, default_value_t = super::DEFAULT_TIMEOUT_SECS)]
    timeout: u32,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let (sandbox_id, target_node) = resolve_target(&args);
    let Some(target_node) = target_node else {
        client.connect_sandbox(sandbox_id, args.timeout)?;
        println!("Resumed {sandbox_id}");
        return Ok(());
    };

    let target_url = normalize_node_url(target_node);
    let target_client = client
        .with_base_url(&target_url)
        .with_context(|| format!("configure target node {target_url}"))?;
    if target_client
        .sandbox_state_with_timeout(sandbox_id, Duration::from_secs(5))
        .with_context(|| format!("query sandbox {sandbox_id} on target node {target_url}"))?
        .is_some()
    {
        target_client
            .connect_sandbox(sandbox_id, args.timeout)
            .with_context(|| format!("resume sandbox {sandbox_id} on target node {target_url}"))?;
        println!("Resumed {sandbox_id} on {target_url}");
        return Ok(());
    }

    let snapshots = target_client
        .list_snapshots(Some(sandbox_id))
        .with_context(|| {
            format!("list snapshots for sandbox {sandbox_id} through target node {target_url}")
        })?;
    let snapshot = snapshots.first().with_context(|| {
        format!(
            "sandbox {sandbox_id} does not exist on {target_url} and has no associated snapshot; pause it while the snapshot repository backend is Mooncake first"
        )
    })?;
    let restored = target_client
        .create_sandbox(&snapshot.snapshot_id, Some(args.timeout), false)
        .with_context(|| {
            format!(
                "restore sandbox {sandbox_id} from snapshot {} on target node {target_url}",
                snapshot.snapshot_id
            )
        })?;
    println!(
        "Restored {sandbox_id} on {target_url} as {} from snapshot {}",
        restored.sandbox_id,
        snapshot.snapshot_id
    );
    Ok(())
}

fn resolve_target(args: &Args) -> (&str, Option<&str>) {
    match args.sandbox_id.as_deref() {
        Some(sandbox_id) => (sandbox_id, Some(args.node_or_sandbox_id.as_str())),
        None => (args.node_or_sandbox_id.as_str(), None),
    }
}

fn normalize_node_url(node: &str) -> String {
    let node = node.trim_end_matches('/');
    if node.contains("://") {
        node.to_string()
    } else {
        format!("http://{node}")
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_node_url, resolve_target, Args};

    #[test]
    fn resolves_legacy_resume_form() {
        let args = Args {
            node_or_sandbox_id: "sandbox-1".to_string(),
            sandbox_id: None,
            timeout: 300,
        };

        assert_eq!(resolve_target(&args), ("sandbox-1", None));
    }

    #[test]
    fn resolves_target_node_resume_form() {
        let args = Args {
            node_or_sandbox_id: "192.168.25.65:8000".to_string(),
            sandbox_id: Some("sandbox-1".to_string()),
            timeout: 300,
        };

        assert_eq!(
            resolve_target(&args),
            ("sandbox-1", Some("192.168.25.65:8000"))
        );
    }

    #[test]
    fn normalizes_target_node_url() {
        assert_eq!(
            normalize_node_url("192.168.25.65:8000/"),
            "http://192.168.25.65:8000"
        );
        assert_eq!(
            normalize_node_url("https://node.example/"),
            "https://node.example"
        );
    }
}
