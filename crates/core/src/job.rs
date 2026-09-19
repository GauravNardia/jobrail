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
    Scheduled,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobOptions {
    pub priority: i32,
    pub max_attempts: u32,
    pub delay_ms: u64,
    pub run_at: Option<u64>,
    pub idempotency_key: Option<String>,
}

impl Default for JobOptions {
    fn default() -> Self {
        Self {
            priority: 0,
            max_attempts: 3,
            delay_ms: 0,
            run_at: None,
            idempotency_key: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub run_at: Option<u64>,
    pub idempotency_key: Option<String>,
}

impl Job {
    pub fn new(name: impl Into<String>, payload: serde_json::Value, options: JobOptions) -> Self {
        let state = if options.run_at.is_some() {
            JobState::Scheduled
        } else if options.delay_ms > 0 {
            JobState::Delayed
        } else if options.priority > 0 {
            JobState::Prioritized
        } else {
            JobState::Waiting
        };

        Self {
            id: JobId::new(),
            name: name.into(),
            payload,
            state,
            priority: options.priority,
            attempts_made: 0,
            attempts_started: 0,
            max_attempts: options.max_attempts,
            delay_ms: options.delay_ms,
            run_at: options.run_at,
            idempotency_key: options.idempotency_key,
        }
    }

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

    pub fn retry_delay_ms(&self) -> u64 {
        let base_delay = 1000_u64;

        base_delay * 2_u64.pow(self.attempts_made.saturating_sub(1))
    }
}

#[cfg(test)]

mod tests {
    use super::*;

    #[test]
    fn job_can_transition_to_active() {
        let mut job = Job::new(
            "send_email",
            serde_json::json!({  "email": "test@example.com" }),
            JobOptions::default(),
        );

        let result = job.transition_to(JobState::Active);

        assert!(result.is_ok());
        assert_eq!(job.state, JobState::Active);
    }

    #[test]
    fn job_cannot_transition_from_completed_to_active() {
        let mut job = Job::new(
            "send_email",
            serde_json::json!({  "email": "test@example.com" }),
            JobOptions::default(),
        );

        job.state = JobState::Completed;

        let result = job.transition_to(JobState::Active);

        assert!(result.is_err());
        assert_eq!(job.state, JobState::Completed);
    }

    #[test]
    fn new_job_starts_waiting() {
        let job = Job::new(
            "send_email",
            serde_json::json!({  "email": "test@example.com" }),
            JobOptions::default(),
        );

        assert_eq!(job.state, JobState::Waiting);
        assert_eq!(job.priority, 0);
        assert_eq!(job.attempts_made, 0);
        assert_eq!(job.attempts_started, 0);
        assert_eq!(job.max_attempts, 3);
        assert_eq!(job.delay_ms, 0);
    }

    #[test]
    fn priority_job_starts_prioritized() {
        let options = JobOptions {
            priority: 10,
            ..Default::default()
        };

        let job = Job::new("send_email", serde_json::json!({}), options);

        assert_eq!(job.state, JobState::Prioritized);
    }

    #[test]
    fn delayed_job_starts_delayed() {
        let options = JobOptions {
            delay_ms: 5_000,
            ..Default::default()
        };

        let job = Job::new("send_email", serde_json::json!({}), options);

        assert_eq!(job.state, JobState::Delayed);
    }

    #[test]
    fn retry_delay_uses_exponential_backoff() {
        let mut job = Job::new("send_email", serde_json::json!({}), JobOptions::default());

        job.attempts_made = 1;
        assert_eq!(job.retry_delay_ms(), 1_000);

        job.attempts_made = 2;
        assert_eq!(job.retry_delay_ms(), 2_000);

        job.attempts_made = 3;
        assert_eq!(job.retry_delay_ms(), 4_000);

        job.attempts_made = 4;
        assert_eq!(job.retry_delay_ms(), 8_000);
    }
}
