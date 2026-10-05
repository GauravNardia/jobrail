use crate::job::JobState;

impl JobState {
    pub fn can_transition_to(&self, next: JobState) -> bool {
        matches!(
            (self, next),
            (JobState::Waiting, JobState::Active)
                | (JobState::Prioritized, JobState::Active)
                | (JobState::Scheduled, JobState::Waiting)
                | (JobState::Delayed, JobState::Waiting)
                | (JobState::Active, JobState::Completed)
                | (JobState::Active, JobState::Failed)
                | (JobState::Active, JobState::Delayed)
                | (JobState::Active, JobState::Waiting)
                | (JobState::Waiting, JobState::Cancelled)
                | (JobState::Prioritized, JobState::Cancelled)
                | (JobState::Delayed, JobState::Cancelled)
                | (JobState::Scheduled, JobState::Cancelled)
                | (JobState::Active, JobState::Cancelled)
                | (JobState::Failed, JobState::Waiting)
        )
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
