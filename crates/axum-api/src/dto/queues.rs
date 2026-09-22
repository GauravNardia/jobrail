use serde::Serialize;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueueResponse {
    pub name: String,
    pub waiting: u64,
    pub active: u64,
    pub delayed: u64,
    pub scheduled: u64,
    pub processing: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuesResponse {
    pub queues: Vec<QueueResponse>,
}
