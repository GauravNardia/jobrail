use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub Uuid);

impl JobId {
    pub fn new() -> Self {
        JobId(Uuid::new_v4())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JobState {
    Waiting,
    Prioritized,
    Delayed,
    Active,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateTransitionError {
    pub from: JobState,
    pub to: JobState,
}

pub struct Job {
    pub id: JobId,
    pub name: String,
    pub payload: serde_json::Value,
    pub state: JobState,
    pub priority: i32,
    pub attempts_made: u32,
    pub attempts_started: u32,
    pub max_attempts: u32,
    pub delay_ms: u64,
}

impl Job {
    pub fn transition_to(&mut self, next: JobState) -> Result<(), StateTransitionError> {
        if !self.state.can_transition_to(next) {
            return Err(StateTransitionError {
                from: self.state,
                to: next,
            });
        }
        self.state = next;

        Ok(())
    }
}

#[cfg(test)]

mod tests {
    use super::*;

    fn test_job() -> Job {
        Job {
            id: JobId::new(),
            name: "Test Job".to_string(),
            payload: serde_json::json!({"key": "value"}),
            state: JobState::Waiting,
            priority: 0,
            attempts_made: 0,
            attempts_started: 0,
            max_attempts: 3,
            delay_ms: 0,
        }
    }

    #[test]
    fn job_can_transition_to_active() {
        let mut job = test_job();

        let result = job.transition_to(JobState::Active);

        assert!(result.is_ok());
        assert_eq!(job.state, JobState::Active);
    }

    #[test]
    fn job_cannot_transition_from_completed_to_active() {
        let mut job = test_job();

        job.state = JobState::Completed;

        let result = job.transition_to(JobState::Active);

        assert!(result.is_err());
        assert_eq!(job.state, JobState::Completed);
    }
}
