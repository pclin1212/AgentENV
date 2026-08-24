use crate::client::Client;
use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use std::time::Duration;

#[derive(ClapArgs)]
#[command(after_help = "Examples:
  aenv resume <sandbox-id>
  aenv resume <target-node-url> <sandbox-id>
  aenv resume --node <node-id> <sandbox-id>

When a target node URL or node ID is provided, the CLI resumes the existing sandbox there if present.
Otherwise it creates a new sandbox from the source sandbox's latest Mooncake snapshot.")]
pub struct Args {
    /// Sandbox ID, or the target AgentENV node URL when SANDBOX_ID is also supplied.
    #[arg(add = crate::commands::completion::add_paused_sandbox_candidates())]
    node_or_sandbox_id: String,
    /// Sandbox to restore on the target AgentENV node.
    #[arg(add = crate::commands::completion::add_paused_sandbox_candidates())]
    sandbox_id: Option<String>,
    /// Target scheduler node ID. Requires the configured server URL to point at the gateway.
    #[arg(long, value_name = "NODE_ID")]
    node: Option<String>,
    /// TTL in seconds from now. Must be longer than the sandbox's current TTL.
    #[arg(long, default_value_t = super::DEFAULT_TIMEOUT_SECS)]
    timeout: u32,
}

pub fn run(args: Args) -> Result<()> {
    let client = Client::from_env()?;
    let (sandbox_id, target) = resolve_target(&args)?;
    let Some(target) = target else {
        client.connect_sandbox(sandbox_id, args.timeout)?;
        println!("Resumed {sandbox_id}");
        return Ok(());
    };

    let (target_client, target_description) = match target {
        ResumeTarget::NodeId(node_id) => (
            client
                .with_target_node_id(node_id)
                .with_context(|| format!("configure target node {node_id}"))?,
            format!("node {node_id}"),
        ),
        ResumeTarget::Url(url) => {
            let target_url = normalize_node_url(url);
            (
                client
                    .with_base_url(&target_url)
                    .with_context(|| format!("configure target node {target_url}"))?,
                target_url,
            )
        }
    };
    if target_client
        .sandbox_state_with_timeout(sandbox_id, Duration::from_secs(5))
        .with_context(|| format!("query sandbox {sandbox_id} on target {target_description}"))?
        .is_some()
    {
        target_client
            .connect_sandbox(sandbox_id, args.timeout)
            .with_context(|| format!("resume sandbox {sandbox_id} on {target_description}"))?;
        println!("Resumed {sandbox_id} on {target_description}");
        return Ok(());
    }

    let snapshots = target_client
        .list_snapshots(Some(sandbox_id))
        .with_context(|| {
            format!("list snapshots for sandbox {sandbox_id} through {target_description}")
        })?;
    let snapshot = snapshots.first().with_context(|| {
        format!(
            "sandbox {sandbox_id} does not exist on {target_description} and has no associated snapshot; pause it while the snapshot repository backend is Mooncake first"
        )
    })?;
    let restored = target_client
        .create_sandbox(&snapshot.snapshot_id, Some(args.timeout), false)
        .with_context(|| {
            format!(
                "restore sandbox {sandbox_id} from snapshot {} on {target_description}",
                snapshot.snapshot_id
            )
        })?;
    println!(
        "Restored {sandbox_id} on {target_description} as {} from snapshot {}",
        restored.sandbox_id, snapshot.snapshot_id
    );
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum ResumeTarget<'a> {
    NodeId(&'a str),
    Url(&'a str),
}

fn resolve_target(args: &Args) -> Result<(&str, Option<ResumeTarget<'_>>)> {
    match (args.node.as_deref(), args.sandbox_id.as_deref()) {
        (Some(node_id), None) => Ok((
            args.node_or_sandbox_id.as_str(),
            Some(ResumeTarget::NodeId(node_id)),
        )),
        (None, Some(sandbox_id)) => Ok((
            sandbox_id,
            Some(ResumeTarget::Url(args.node_or_sandbox_id.as_str())),
        )),
        (None, None) => Ok((args.node_or_sandbox_id.as_str(), None)),
        (Some(_), Some(_)) => anyhow::bail!(
            "--node cannot be combined with the legacy <target-node-url> <sandbox-id> form"
        ),
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
    use super::{normalize_node_url, resolve_target, Args, ResumeTarget};

    #[test]
    fn resolves_legacy_resume_form() {
        let args = Args {
            node_or_sandbox_id: "sandbox-1".to_string(),
            sandbox_id: None,
            node: None,
            timeout: 300,
        };

        assert_eq!(resolve_target(&args).unwrap(), ("sandbox-1", None));
    }

    #[test]
    fn resolves_target_node_resume_form() {
        let args = Args {
            node_or_sandbox_id: "192.168.25.65:8000".to_string(),
            sandbox_id: Some("sandbox-1".to_string()),
            node: None,
            timeout: 300,
        };

        assert_eq!(
            resolve_target(&args).unwrap(),
            ("sandbox-1", Some(ResumeTarget::Url("192.168.25.65:8000")))
        );
    }

    #[test]
    fn resolves_node_id_resume_form() {
        let args = Args {
            node_or_sandbox_id: "sandbox-1".to_string(),
            sandbox_id: None,
            node: Some("node-65".to_string()),
            timeout: 300,
        };

        assert_eq!(
            resolve_target(&args).unwrap(),
            ("sandbox-1", Some(ResumeTarget::NodeId("node-65")))
        );
    }

    #[test]
    fn rejects_node_id_with_legacy_target_url_form() {
        let args = Args {
            node_or_sandbox_id: "192.168.25.65:8000".to_string(),
            sandbox_id: Some("sandbox-1".to_string()),
            node: Some("node-65".to_string()),
            timeout: 300,
        };

        assert!(resolve_target(&args).is_err());
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
