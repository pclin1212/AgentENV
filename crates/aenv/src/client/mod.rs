pub mod files;
pub mod sandboxes;
pub mod snapshots;
pub mod templates;

use crate::auth::Credentials;
use crate::grpc::Transport;
use anyhow::{anyhow, bail, Result};
use std::time::Duration;
use ureq::Agent;

pub(crate) const TARGET_NODE_ID_HEADER: &str = "x-agentenv-target-node-id";

#[derive(Clone)]
pub struct Client {
    agent: Agent,
    base: String,
    api_key: String,
    target_node_id: Option<String>,
}

impl Client {
    pub fn from_env() -> Result<Self> {
        let creds = Credentials::load()?;
        Self::new(&creds.url, &creds.api_key)
    }

    pub fn new(url: &str, api_key: &str) -> Result<Self> {
        Self::new_with_timeouts(
            url,
            api_key,
            Duration::from_secs(5),
            Duration::from_secs(120),
        )
    }

    pub fn new_with_timeouts(
        url: &str,
        api_key: &str,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self> {
        let base = url.trim_end_matches('/').to_string();
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(connect_timeout)
            .timeout(request_timeout)
            .build();
        Ok(Self {
            agent,
            base,
            api_key: api_key.to_string(),
            target_node_id: None,
        })
    }

    pub fn with_base_url(&self, url: &str) -> Result<Self> {
        Self::new_with_timeouts(
            url,
            &self.api_key,
            Duration::from_secs(5),
            Duration::from_secs(120),
        )
    }

    pub fn with_target_node_id(&self, node_id: &str) -> Result<Self> {
        let node_id = node_id.trim();
        if node_id.is_empty() {
            bail!("target node id cannot be empty");
        }
        let mut client = self.clone();
        client.target_node_id = Some(node_id.to_string());
        Ok(client)
    }

    pub fn transport(
        &self,
        sandbox_id: &str,
        envd_access_token: Option<&str>,
    ) -> Result<Transport> {
        Transport::new(&self.base, sandbox_id, envd_access_token)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    pub fn get(&self, path: &str) -> ureq::Request {
        let request = self
            .agent
            .get(&self.url(path))
            .set("X-API-Key", &self.api_key);
        self.with_routing_headers(request)
    }

    pub fn post(&self, path: &str) -> ureq::Request {
        let request = self
            .agent
            .post(&self.url(path))
            .set("X-API-Key", &self.api_key);
        self.with_routing_headers(request)
    }

    pub fn delete(&self, path: &str) -> ureq::Request {
        let request = self
            .agent
            .delete(&self.url(path))
            .set("X-API-Key", &self.api_key);
        self.with_routing_headers(request)
    }

    fn with_routing_headers(&self, request: ureq::Request) -> ureq::Request {
        match self.target_node_id.as_deref() {
            Some(node_id) => request.set(TARGET_NODE_ID_HEADER, node_id),
            None => request,
        }
    }
}

impl Credentials {
    pub fn load() -> Result<Self> {
        crate::auth::load()
    }
}

pub fn handle_status(resp: Result<ureq::Response, ureq::Error>) -> Result<ureq::Response> {
    match resp {
        Ok(r) => Ok(r),
        Err(ureq::Error::Status(code, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            Err(format_status_error(code, &body))
        }
        Err(ureq::Error::Transport(t)) => Err(anyhow!(t).context("transport error")),
    }
}

fn format_status_error(code: u16, body: &str) -> anyhow::Error {
    let detail = parse_api_error(body)
        .or_else(|| (!body.trim().is_empty()).then(|| body.trim().to_string()));

    if code == 401 {
        return match detail {
            Some(detail) => anyhow!(
                "authentication failed (HTTP 401 Unauthorized): {detail}; run `aenv auth` to update your API key"
            ),
            None => anyhow!(
                "authentication failed (HTTP 401 Unauthorized); run `aenv auth` to update your API key"
            ),
        };
    }

    anyhow!("HTTP {}: {}", code, detail.unwrap_or_default())
}

fn parse_api_error(body: &str) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct ApiError {
        message: Option<String>,
    }
    serde_json::from_str::<ApiError>(body)
        .ok()
        .and_then(|e| e.message)
}
#[cfg(test)]
mod tests {
    use super::{format_status_error, Client, TARGET_NODE_ID_HEADER};

    #[test]
    fn unauthorized_error_explains_how_to_reauthenticate() {
        assert_eq!(
            format_status_error(401, "").to_string(),
            "authentication failed (HTTP 401 Unauthorized); run `aenv auth` to update your API key"
        );
    }

    #[test]
    fn target_node_client_adds_routing_header() {
        let client = Client::new("http://gateway.test", "secret")
            .unwrap()
            .with_target_node_id(" node-65 ")
            .unwrap();

        let request = client.get("/snapshots");
        assert_eq!(request.header(TARGET_NODE_ID_HEADER), Some("node-65"));
        assert_eq!(request.header("X-API-Key"), Some("secret"));
    }

    #[test]
    fn target_node_id_cannot_be_empty() {
        let client = Client::new("http://gateway.test", "secret").unwrap();
        let error = client.with_target_node_id("  ").err().unwrap();
        assert!(error.to_string().contains("cannot be empty"));
    }
}
