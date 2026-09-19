use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RepeatableJobId(pub Uuid);

impl RepeatableJobId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RepeatSchedule {
    EveryMillis(u64),
}

impl RepeatSchedule {
    pub fn next_run_at(&self, now_ms: u64) -> u64 {
        match self {
            Self::EveryMillis(interval_ms) => now_ms + interval_ms,
        }
    }

    pub fn next_run_at_from_now(&self) -> Result<u64, String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock is before UNIX epoch: {error}"))?;

        Ok(self.next_run_at(now.as_millis() as u64))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepeatableJob {
    pub id: RepeatableJobId,
    pub name: String,
    pub payload: serde_json::Value,
    pub schedule: RepeatSchedule,
    pub enabled: bool,
}

impl RepeatableJob {
    pub fn new(
        name: impl Into<String>,
        payload: serde_json::Value,
        schedule: RepeatSchedule,
    ) -> Self {
        Self {
            id: RepeatableJobId::new(),
            name: name.into(),
            payload,
            schedule,
            enabled: true,
        }
    }
}
