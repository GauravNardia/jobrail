use crate::job::JobState;

impl JobState {
    pub fn can_transition_to(&self, next: JobState) -> bool {
        match (self, next) {
            // job can enter normal waiting
            (JobState::Waiting, JobState::Active) => true,
            // A prioritized job can become active.
            (JobState::Prioritized, JobState::Active) => true,
            // A delayed job becomes waiting once its delay expires.
            (JobState::Delayed, JobState::Waiting) => true,

            (JobState::Active, JobState::Waiting) => true,
            // A running job can finish successfully.
            (JobState::Active, JobState::Completed) => true,
            // A running job can fail.
            (JobState::Active, JobState::Failed) => true,
            // A running job can be delayed.
            (JobState::Active, JobState::Delayed) => true,

            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waiting_can_become_active() {
        assert!(JobState::Waiting.can_transition_to(JobState::Active));
    }

    #[test]
    fn active_can_become_completed() {
        assert!(JobState::Active.can_transition_to(JobState::Completed));
    }

    #[test]
    fn active_can_become_failed() {
        assert!(JobState::Active.can_transition_to(JobState::Failed));
    }

    #[test]
    fn active_can_become_delayed() {
        assert!(JobState::Active.can_transition_to(JobState::Delayed));
    }

    #[test]
    fn delayed_can_become_waiting() {
        assert!(JobState::Delayed.can_transition_to(JobState::Waiting));
    }

    #[test]
    fn completed_cannot_become_active() {
        assert!(!JobState::Completed.can_transition_to(JobState::Active));
    }

    #[test]
    fn failed_cannot_become_completed() {
        assert!(!JobState::Failed.can_transition_to(JobState::Completed));
    }

    #[test]
    fn active_can_return_to_waiting_after_recovery() {
        assert!(JobState::Active.can_transition_to(JobState::Waiting));
    }
}
