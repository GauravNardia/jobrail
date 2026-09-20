use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateJobRequest {
    pub name: String,
    pub payload: Value,

    #[serde(default)]
    pub priority: i32,

    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,

    #[serde(default)]
    pub delay_ms: u64,

    #[serde(default)]
    pub run_at: Option<u64>,

    #[serde(default)]
    pub idempotency_key: Option<String>,
}

fn default_max_attempts() -> u32 {
    3
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobResponse {
    pub id: String,
    pub name: String,
    pub state: String,
    pub attempts_made: u32,
    pub attempts_started: u32,
}
