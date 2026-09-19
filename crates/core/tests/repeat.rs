use jobrail_core::repeat::{RepeatSchedule, RepeatableJob};

#[test]
fn creates_enabled_repeatable_job() {
    let job = RepeatableJob::new(
        "send_report",
        serde_json::json!({
            "user_id": 123
        }),
        RepeatSchedule::EveryMillis(60_000),
    );

    assert!(job.enabled);

    assert_eq!(job.name, "send_report");

    assert!(matches!(job.schedule, RepeatSchedule::EveryMillis(60_000)));
}

#[test]
fn calculates_next_run_for_interval() {
    let schedule = RepeatSchedule::EveryMillis(10_000);

    let now = 1_000_000;

    let next = schedule.next_run_at(now);

    assert_eq!(next, 1_010_000);
}
