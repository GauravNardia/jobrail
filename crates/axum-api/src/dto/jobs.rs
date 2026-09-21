use jobrail_core::job::JobState;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobAttemptResponse {
    pub attempt: u32,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobAttemptsResponse {
    pub attempts: Vec<JobAttemptResponse>,
}

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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListJobsQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,

    #[serde(default)]
    pub cursor: Option<String>,

    #[serde(default)]
    pub state: Option<JobState>,
}

fn default_limit() -> usize {
    20
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobPageResponse {
    pub jobs: Vec<JobResponse>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
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
    pub run_at: Option<u64>,
}
