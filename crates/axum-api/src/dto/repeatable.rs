use jobrail_core::repeat::RepeatSchedule;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateRepeatableJobRequest {
    pub name: String,
    pub payload: Value,
    pub schedule: RepeatSchedule,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepeatableJobResponse {
    pub id: String,
    pub name: String,
    pub payload: Value,
    pub schedule: RepeatSchedule,
    pub enabled: bool,
    pub next_run_at: Option<u64>,
}

impl From<jobrail_core::repeat::RepeatableJob> for RepeatableJobResponse {
    fn from(job: jobrail_core::repeat::RepeatableJob) -> Self {
        Self {
            id: job.id.0.to_string(),
            name: job.name,
            payload: job.payload,
            schedule: job.schedule,
            enabled: job.enabled,
            next_run_at: job.next_run_at,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RepeatableJobsResponse {
    pub jobs: Vec<RepeatableJobResponse>,
}
