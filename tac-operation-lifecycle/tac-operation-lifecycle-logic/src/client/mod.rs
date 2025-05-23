use std::collections::HashMap;
use std::time::Duration;

use anyhow::Error;
use reqwest::{Request, Response, Method};
use settings::RpcSettings;
use tower::ServiceExt;
use tracing::{debug, error};
use tower::{Service, ServiceBuilder, limit::RateLimit};
use reqwest::Client as HttpClient;
use tokio;

pub mod settings;

pub mod models;
use models::operations::{Operations, OperationIdsApiResponse};
use models::profiling::{OperationData, StageProfilingApiResponse};

#[derive(Debug)]
pub struct Client {
    rpc: RpcSettings,
    service: RateLimit<HttpClient>,
}

impl Client {
    pub fn new(settings: RpcSettings) -> Self {
        let http_client = HttpClient::new();
        
        // Create rate limited service
        let service = ServiceBuilder::new()
            .rate_limit(settings.request_per_second as u64, Duration::from_secs(1))
            .service(http_client);

        Self {
            rpc: settings,
            service,
        }
    }

    pub async fn get_operations(&mut self, start: u64, end: u64) -> Result<Operations, Error> {
        let url = format!("{}/operation-ids?from={}&till={}", self.url(), start, end);
            
        let request = Request::new(Method::GET, url.parse()?);
        let response = self.make_request(request).await?;
        if !response.status().is_success() {
            let status = response.status();
            tracing::error!("Bad response status: {} from {url}", status);
            tracing::error!("Response body: {}", response.text().await?);
            
            return Err(anyhow::anyhow!("HTTP error {}: {}", status.as_u16(), status.as_str()));
        }

        let text = response.text().await?;
        
        if text.is_empty() {
            tracing::error!("Received empty response from {url}");
            return Ok(Vec::new());
        }
        
        match serde_json::from_str::<OperationIdsApiResponse>(&text) {
            Ok(response) => Ok(response.response.operations),
            Err(e) => {
                tracing::error!("Failed to parse response: {}", e);
                Err(e.into())
            }
        }
    }

    pub async fn get_operations_stages(&mut self, id: Vec<&str>) -> Result<HashMap<String, OperationData>, Error> {
        let request_body = serde_json::json!({
            "operationIds": id
        });

        let url = format!("{}/stage-profiling", self.url());
        let mut request = Request::new(Method::POST, url.parse()?);
        
        // Set headers
        request.headers_mut().insert("accept", "application/json".parse()?);
        request.headers_mut().insert("Content-Type", "application/json".parse()?);
        
        // Set body
        request.body_mut().replace(serde_json::to_vec(&request_body)?.into());

        let response = self.make_request(request).await?;
        
        if response.status().is_success() {
            let text = response.text().await?;
            debug!("Raw response body: {}", text);
            
            if text.is_empty() {
                tracing::error!("Received empty response from {url}");
                return Err(anyhow::anyhow!("Received empty response from {url}"));
            }
            
            match serde_json::from_str::<StageProfilingApiResponse>(&text) {
                Ok(response) => Ok(response.response),
                Err(e) => {
                    error!("Failed to parse response: {}", e);
                    Err(e.into())
                }
            }
        } else {
            Err(anyhow::anyhow!("HTTP error {}: {}", response.status().as_u16(), response.status().as_str()))
        }
    }

    fn url(&self) -> &str {
        self.rpc.url.strip_suffix("/").unwrap_or(self.rpc.url.as_str())
    }

    async fn make_request(&mut self, request: Request) -> anyhow::Result<Response> {
        let mut retries = 0;
        const MAX_RETRIES: u32 = 10;
        const RETRY_DELAY_MS: u64 = 100;

        while retries < MAX_RETRIES {
            match self.service.ready().await {
                Ok(service) => {
                    return service.call(request).await.map_err(|e| anyhow::anyhow!("HTTP request error: {}", e));
                }
                Err(e) => {
                    retries += 1;
                    if retries < MAX_RETRIES {
                        debug!("Rate limit exceeded, retrying in {}ms (attempt {}/{})", RETRY_DELAY_MS, retries, MAX_RETRIES);
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                    } else {
                        return Err(anyhow::anyhow!("Rate limit exceeded after {} retries: {}", MAX_RETRIES, e));
                    }
                }
            }
        }

        Err(anyhow::anyhow!("Rate limit exceeded after {} retries, dropping request", MAX_RETRIES))
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new(RpcSettings::default())
    }
}
