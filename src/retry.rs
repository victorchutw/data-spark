//! The retry engine of the write phase (ADR-0048, ADR-0049, ADR-0050): one
//! orchestrator-owned loop that re-attempts a failed retry unit — `begin` or
//! one `write_chunk` — while its failure is classified transient by its
//! originator and the per-unit attempt budget allows, waiting through an
//! injectable sleeper between attempts and recording every failed attempt of
//! a retried unit for the load report. Terminal failures and exhausted
//! budgets surface the last failure unchanged; the terminal `commit` never
//! passes through here, because committing consumes the writer and leaves
//! nothing to re-call.

use crate::connector::{DestinationWriteFailure, Transience};
use serde_json::{json, Map, Value};
use std::num::NonZeroU64;

/// The default retry policy (ADR-0049): three total attempts per unit, a
/// 200ms first wait, and a 5000ms wait cap.
const DEFAULT_MAX_ATTEMPTS: u64 = 3;
const DEFAULT_INITIAL_DELAY_MS: u64 = 200;
const DEFAULT_MAX_DELAY_MS: u64 = 5000;

/// The effective retry policy of a load (ADR-0049): how many total attempts
/// each retry unit is allowed — including the first, so `1` disables retry —
/// and the fixed exponential backoff bounds. The wait before attempt `n`
/// (`n >= 2`) is `min(initial_delay_ms * 2^(n-2), max_delay_ms)`: clamp
/// semantics, well-defined even when `max_delay_ms < initial_delay_ms`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetryPolicy {
    pub(crate) max_attempts: NonZeroU64,
    pub(crate) initial_delay_ms: u64,
    pub(crate) max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: NonZeroU64::new(DEFAULT_MAX_ATTEMPTS).expect("nonzero default"),
            initial_delay_ms: DEFAULT_INITIAL_DELAY_MS,
            max_delay_ms: DEFAULT_MAX_DELAY_MS,
        }
    }
}

/// The wait the engine performs between attempts. Production wires the real
/// thread sleep; tests wire a recording fake that returns instantly, so the
/// whole suite performs zero real sleeping and asserts delays as recorded
/// values. `Sync` because concurrent write slots share one sleeper and each
/// waits on its own thread (ADR-0051): a wait occupies only the slot that
/// performs it.
pub(crate) trait Sleeper: Sync {
    fn sleep_ms(&self, delay_ms: u64);
}

/// The production sleeper: a real thread sleep. Provably idle in the shipped
/// connector matrix, where no failure is classified transient.
pub(crate) struct ThreadSleeper;

impl Sleeper for ThreadSleeper {
    fn sleep_ms(&self, delay_ms: u64) {
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
    }
}

/// The bounded operation a load re-attempts as a whole after a transient
/// failure, with the same input (ADR-0048): the `begin` call, or one
/// `write_chunk` call identified by its 0-based chunk ordinal in the stream.
/// Each unit holds its own attempt budget.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RetryUnit {
    Begin,
    WriteChunk { chunk_index: u64 },
}

/// One recorded failed attempt of a retried unit (ADR-0050): entries exist
/// only for units the engine actually retried, and then every failed attempt
/// is recorded — including the final failure, terminal or exhausted, so the
/// report counts how many times the unit was tried. `delay_before_retry_ms`
/// is the wait applied after this failure, absent on a final failed attempt
/// that no retry followed. Successful attempts are not recorded.
#[derive(Debug)]
pub(crate) struct RetryAttempt {
    unit: RetryUnit,
    attempt: u64,
    code: &'static str,
    message: String,
    delay_before_retry_ms: Option<u64>,
}

/// Runs one retry unit to completion under the policy: re-attempts the
/// operation while its failure is transient and budget remains, sleeping the
/// backoff wait between attempts, and surfaces a terminal or
/// budget-exhausting failure unchanged — same code and message, no wrapper.
/// Failed attempts of a retried unit accumulate onto `attempts`.
pub(crate) fn run_unit<T>(
    policy: &RetryPolicy,
    unit: RetryUnit,
    attempts: &mut Vec<RetryAttempt>,
    sleeper: &dyn Sleeper,
    mut operation: impl FnMut() -> Result<T, DestinationWriteFailure>,
) -> Result<T, DestinationWriteFailure> {
    let mut attempt = 1_u64;
    loop {
        let failure = match operation() {
            Ok(value) => return Ok(value),
            Err(failure) => failure,
        };
        if failure.transience == Transience::Transient && attempt < policy.max_attempts.get() {
            let delay_ms = delay_before_attempt_ms(policy, attempt + 1);
            attempts.push(RetryAttempt {
                unit,
                attempt,
                code: failure.failure.code,
                message: failure.failure.message.clone(),
                delay_before_retry_ms: Some(delay_ms),
            });
            sleeper.sleep_ms(delay_ms);
            attempt += 1;
        } else {
            // A unit that was never retried tells no retry story: an
            // immediately-terminal or single-budget failure surfaces with an
            // empty log, keeping today's failure reports byte-identical
            // (ADR-0050). Once a retry happened, the final failure joins the
            // log too, so the entries count every attempt the unit was
            // tried.
            if attempt > 1 {
                attempts.push(RetryAttempt {
                    unit,
                    attempt,
                    code: failure.failure.code,
                    message: failure.failure.message.clone(),
                    delay_before_retry_ms: None,
                });
            }
            return Err(failure);
        }
    }
}

/// The fixed exponential backoff of ADR-0049: the wait before attempt `n`
/// (`n >= 2`) is `min(initial_delay_ms * 2^(n-2), max_delay_ms)`. Saturating
/// arithmetic keeps the product well-defined at any attempt count; the
/// clamp keeps it well-defined even when `max_delay_ms < initial_delay_ms`.
fn delay_before_attempt_ms(policy: &RetryPolicy, attempt: u64) -> u64 {
    let doublings = u32::try_from(attempt.saturating_sub(2)).unwrap_or(u32::MAX);
    2_u64
        .saturating_pow(doublings)
        .saturating_mul(policy.initial_delay_ms)
        .min(policy.max_delay_ms)
}

/// The conditional retry story of the `not_started` report posture
/// (ADR-0050): `Some` exactly when attempts were recorded, so a
/// never-retried failure report keeps its established shape while an
/// exhausted transient `begin` still tells the whole story. Boxed for the
/// failure types that carry it.
pub(crate) fn report_when_attempted(
    policy: &RetryPolicy,
    attempts: &[RetryAttempt],
) -> Option<Box<Value>> {
    (!attempts.is_empty()).then(|| Box::new(report_value(policy, attempts)))
}

/// Renders the report's `execution.retry` object (ADR-0050): the effective
/// policy echo plus the attempts array — empty when nothing was retried.
/// The array is ordered deterministically at assembly (ADR-0053): `begin`
/// entries first, then ascending chunk index, then ascending attempt —
/// byte-identical to the appending order of a serial load, and independent
/// of the wall-clock completion order of concurrent slots.
pub(crate) fn report_value(policy: &RetryPolicy, attempts: &[RetryAttempt]) -> Value {
    let mut ordered: Vec<&RetryAttempt> = attempts.iter().collect();
    ordered.sort_by_key(|attempt| attempt.unit_order());
    json!({
        "max_attempts": policy.max_attempts.get(),
        "initial_delay_ms": policy.initial_delay_ms,
        "max_delay_ms": policy.max_delay_ms,
        "attempts": ordered
            .into_iter()
            .map(RetryAttempt::report_value)
            .collect::<Vec<_>>(),
    })
}

impl RetryAttempt {
    /// The deterministic report position of this entry: `begin` before
    /// every chunk, chunks by ascending index, attempts ascending within a
    /// unit.
    fn unit_order(&self) -> (u8, u64, u64) {
        match self.unit {
            RetryUnit::Begin => (0, 0, self.attempt),
            RetryUnit::WriteChunk { chunk_index } => (1, chunk_index, self.attempt),
        }
    }

    /// One attempts-array entry: `operation`, the 0-based `chunk_index`
    /// (absent for `begin`), the 1-based `attempt` within the unit, the
    /// failure mirrored in the `error_summary` shape, and the wait that
    /// followed (absent when no retry followed).
    pub(crate) fn report_value(&self) -> Value {
        let mut entry = Map::new();
        let (operation, chunk_index) = match self.unit {
            RetryUnit::Begin => ("begin", None),
            RetryUnit::WriteChunk { chunk_index } => ("write_chunk", Some(chunk_index)),
        };
        entry.insert("operation".to_string(), json!(operation));
        if let Some(chunk_index) = chunk_index {
            entry.insert("chunk_index".to_string(), json!(chunk_index));
        }
        entry.insert("attempt".to_string(), json!(self.attempt));
        entry.insert(
            "error".to_string(),
            json!({ "code": self.code, "message": self.message }),
        );
        if let Some(delay_ms) = self.delay_before_retry_ms {
            entry.insert("delay_before_retry_ms".to_string(), json!(delay_ms));
        }
        Value::Object(entry)
    }
}

/// A sleeper that records the requested waits instead of performing them,
/// so tests assert the backoff sequence without any real sleep. The record
/// sits behind a lock because concurrent slots share one sleeper; recorded
/// order across slots is scheduling order, so concurrency tests assert the
/// sorted multiset while single-slot tests keep asserting the sequence.
#[cfg(test)]
pub(crate) struct RecordingSleeper {
    slept_ms: std::sync::Mutex<Vec<u64>>,
}

#[cfg(test)]
impl RecordingSleeper {
    pub(crate) fn new() -> Self {
        RecordingSleeper {
            slept_ms: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn slept_ms(&self) -> Vec<u64> {
        self.slept_ms.lock().expect("sleeper record lock").clone()
    }
}

#[cfg(test)]
impl Sleeper for RecordingSleeper {
    fn sleep_ms(&self, delay_ms: u64) {
        self.slept_ms
            .lock()
            .expect("sleeper record lock")
            .push(delay_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::DestinationWriteFacts;
    use crate::LoadFailure;

    /// A scripted operation: each call consumes the next outcome, so tests
    /// drive exact transient/terminal sequences.
    struct ScriptedOperation {
        outcomes: Vec<ScriptedOutcome>,
        calls: u64,
    }

    enum ScriptedOutcome {
        Succeed,
        Fail {
            message: &'static str,
            transience: Transience,
        },
    }

    impl ScriptedOperation {
        fn new(outcomes: Vec<ScriptedOutcome>) -> Self {
            ScriptedOperation { outcomes, calls: 0 }
        }

        fn invoke(&mut self) -> Result<u64, DestinationWriteFailure> {
            let outcome = self.outcomes.remove(0);
            self.calls += 1;
            match outcome {
                ScriptedOutcome::Succeed => Ok(self.calls),
                ScriptedOutcome::Fail {
                    message,
                    transience,
                } => Err(DestinationWriteFailure {
                    failure: LoadFailure {
                        code: "destination_write_failed",
                        message: message.to_string(),
                    },
                    facts: DestinationWriteFacts::not_applicable(),
                    written_records: 0,
                    committed_chunks: 0,
                    transience,
                }),
            }
        }
    }

    fn transient(message: &'static str) -> ScriptedOutcome {
        ScriptedOutcome::Fail {
            message,
            transience: Transience::Transient,
        }
    }

    #[test]
    fn transient_failures_retry_through_the_sleeper_and_record_the_backoff_sequence() {
        // Two transient failures, then success, under the default policy:
        // the waits are 200 (before attempt 2) and 400 (before attempt 3),
        // and the attempt log records exactly the two failed attempts, each
        // carrying the wait that followed it.
        let mut operation = ScriptedOperation::new(vec![
            transient("first shortage"),
            transient("second shortage"),
            ScriptedOutcome::Succeed,
        ]);
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        let value = run_unit(
            &RetryPolicy::default(),
            RetryUnit::WriteChunk { chunk_index: 4 },
            &mut attempts,
            &sleeper,
            || operation.invoke(),
        )
        .expect("the third attempt succeeds");

        assert_eq!(value, 3, "the operation ran exactly three times");
        assert_eq!(sleeper.slept_ms(), vec![200, 400]);
        let entries: Vec<Value> = attempts.iter().map(RetryAttempt::report_value).collect();
        assert_eq!(
            entries,
            vec![
                json!({
                    "operation": "write_chunk",
                    "chunk_index": 4,
                    "attempt": 1,
                    "error": {
                        "code": "destination_write_failed",
                        "message": "first shortage"
                    },
                    "delay_before_retry_ms": 200
                }),
                json!({
                    "operation": "write_chunk",
                    "chunk_index": 4,
                    "attempt": 2,
                    "error": {
                        "code": "destination_write_failed",
                        "message": "second shortage"
                    },
                    "delay_before_retry_ms": 400
                }),
            ]
        );
    }

    #[test]
    fn exhaustion_surfaces_the_last_failure_unchanged_and_records_the_final_attempt() {
        // Three transient failures under a budget of three: the engine stops
        // sleeping after the last attempt, surfaces the third failure's code
        // and message unchanged — no wrapper code — and the log holds all
        // three failed attempts, the final one without a delay.
        let mut operation = ScriptedOperation::new(vec![
            transient("first shortage"),
            transient("second shortage"),
            transient("third shortage"),
        ]);
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        let failure = run_unit(
            &RetryPolicy::default(),
            RetryUnit::Begin,
            &mut attempts,
            &sleeper,
            || operation.invoke(),
        )
        .expect_err("an exhausted budget surfaces the failure");

        assert_eq!(failure.failure.code, "destination_write_failed");
        assert_eq!(failure.failure.message, "third shortage");
        assert_eq!(sleeper.slept_ms(), vec![200, 400]);
        let entries: Vec<Value> = attempts.iter().map(RetryAttempt::report_value).collect();
        assert_eq!(
            entries,
            vec![
                json!({
                    "operation": "begin",
                    "attempt": 1,
                    "error": {
                        "code": "destination_write_failed",
                        "message": "first shortage"
                    },
                    "delay_before_retry_ms": 200
                }),
                json!({
                    "operation": "begin",
                    "attempt": 2,
                    "error": {
                        "code": "destination_write_failed",
                        "message": "second shortage"
                    },
                    "delay_before_retry_ms": 400
                }),
                json!({
                    "operation": "begin",
                    "attempt": 3,
                    "error": {
                        "code": "destination_write_failed",
                        "message": "third shortage"
                    }
                }),
            ]
        );
    }

    #[test]
    fn a_terminal_failure_after_a_transient_one_stops_the_unit_and_joins_the_log() {
        // The second attempt fails terminally with budget remaining: no
        // further retry, the terminal failure surfaces unchanged, and — the
        // unit having been retried once — the final failure is recorded
        // without a delay.
        let mut operation = ScriptedOperation::new(vec![
            transient("first shortage"),
            ScriptedOutcome::Fail {
                message: "storage detached",
                transience: Transience::Terminal,
            },
        ]);
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        let failure = run_unit(
            &RetryPolicy::default(),
            RetryUnit::WriteChunk { chunk_index: 0 },
            &mut attempts,
            &sleeper,
            || operation.invoke(),
        )
        .expect_err("a terminal failure surfaces");

        assert_eq!(failure.failure.message, "storage detached");
        assert_eq!(sleeper.slept_ms(), vec![200]);
        let entries: Vec<Value> = attempts.iter().map(RetryAttempt::report_value).collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1]["attempt"], 2);
        assert_eq!(entries[1]["error"]["message"], "storage detached");
        assert!(
            entries[1].get("delay_before_retry_ms").is_none(),
            "no retry followed the final failure"
        );
    }

    #[test]
    fn an_immediately_terminal_failure_retries_zero_times_and_records_nothing() {
        // Terminal on the first attempt: the engine performs no sleep and
        // leaves the attempt log empty, so a never-retried unit tells no
        // retry story and today's failure reports stay byte-identical.
        let mut operation = ScriptedOperation::new(vec![ScriptedOutcome::Fail {
            message: "table is missing",
            transience: Transience::Terminal,
        }]);
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        let failure = run_unit(
            &RetryPolicy::default(),
            RetryUnit::Begin,
            &mut attempts,
            &sleeper,
            || operation.invoke(),
        )
        .expect_err("a terminal failure surfaces");

        assert_eq!(failure.failure.message, "table is missing");
        assert_eq!(operation.calls, 1, "terminal failures retry zero times");
        assert!(sleeper.slept_ms().is_empty());
        assert!(attempts.is_empty());
    }

    #[test]
    fn max_attempts_one_disables_retry_even_for_a_transient_failure() {
        // A budget of one is the disable form (ADR-0049): the transient
        // failure surfaces unchanged after a single attempt, with no sleep
        // and no recorded attempts.
        let policy = RetryPolicy {
            max_attempts: NonZeroU64::new(1).expect("nonzero"),
            ..RetryPolicy::default()
        };
        let mut operation = ScriptedOperation::new(vec![transient("first shortage")]);
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        let failure = run_unit(
            &policy,
            RetryUnit::WriteChunk { chunk_index: 2 },
            &mut attempts,
            &sleeper,
            || operation.invoke(),
        )
        .expect_err("a single-attempt budget surfaces the first failure");

        assert_eq!(failure.failure.message, "first shortage");
        assert_eq!(operation.calls, 1);
        assert!(sleeper.slept_ms().is_empty());
        assert!(attempts.is_empty());
    }

    #[test]
    fn each_unit_holds_its_own_attempt_budget() {
        // Two units driven through the same log and sleeper, each failing
        // twice before succeeding under a budget of three: neither unit's
        // retries consume the other's budget, so both recover, and the
        // backoff sequence restarts per unit.
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        for chunk_index in [0, 1] {
            let mut operation = ScriptedOperation::new(vec![
                transient("first shortage"),
                transient("second shortage"),
                ScriptedOutcome::Succeed,
            ]);
            run_unit(
                &RetryPolicy::default(),
                RetryUnit::WriteChunk { chunk_index },
                &mut attempts,
                &sleeper,
                || operation.invoke(),
            )
            .unwrap_or_else(|_| panic!("unit {chunk_index} recovers within its own budget"));
        }

        assert_eq!(sleeper.slept_ms(), vec![200, 400, 200, 400]);
        let entries: Vec<Value> = attempts.iter().map(RetryAttempt::report_value).collect();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0]["chunk_index"], 0);
        assert_eq!(entries[1]["chunk_index"], 0);
        assert_eq!(entries[2]["chunk_index"], 1);
        assert_eq!(entries[3]["chunk_index"], 1);
        assert_eq!(
            entries[2]["attempt"], 1,
            "the second unit's attempts start back at 1"
        );
        assert_eq!(entries[2]["delay_before_retry_ms"], 200);
    }

    #[test]
    fn the_backoff_wait_doubles_from_the_initial_delay_and_clamps_at_the_cap() {
        // The waits before attempts 2..=6 under 100/700: 100, 200, 400, then
        // the cap holds 700 from the fifth wait on — min(100 * 2^(n-2), 700).
        let policy = RetryPolicy {
            max_attempts: NonZeroU64::new(6).expect("nonzero"),
            initial_delay_ms: 100,
            max_delay_ms: 700,
        };
        let outcomes = vec![
            transient("shortage"),
            transient("shortage"),
            transient("shortage"),
            transient("shortage"),
            transient("shortage"),
            ScriptedOutcome::Succeed,
        ];
        let mut operation = ScriptedOperation::new(outcomes);
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        run_unit(&policy, RetryUnit::Begin, &mut attempts, &sleeper, || {
            operation.invoke()
        })
        .expect("the sixth attempt succeeds");

        assert_eq!(sleeper.slept_ms(), vec![100, 200, 400, 700, 700]);
    }

    #[test]
    fn a_cap_below_the_initial_delay_clamps_every_wait() {
        // max_delay_ms < initial_delay_ms is legal, not a config error: the
        // clamp makes every wait the cap, including the first.
        let policy = RetryPolicy {
            max_attempts: NonZeroU64::new(3).expect("nonzero"),
            initial_delay_ms: 500,
            max_delay_ms: 300,
        };
        let mut operation = ScriptedOperation::new(vec![
            transient("shortage"),
            transient("shortage"),
            ScriptedOutcome::Succeed,
        ]);
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        run_unit(&policy, RetryUnit::Begin, &mut attempts, &sleeper, || {
            operation.invoke()
        })
        .expect("the third attempt succeeds");

        assert_eq!(sleeper.slept_ms(), vec![300, 300]);
    }

    #[test]
    fn a_first_attempt_success_touches_neither_the_sleeper_nor_the_log() {
        let mut operation = ScriptedOperation::new(vec![ScriptedOutcome::Succeed]);
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        run_unit(
            &RetryPolicy::default(),
            RetryUnit::Begin,
            &mut attempts,
            &sleeper,
            || operation.invoke(),
        )
        .expect("the first attempt succeeds");

        assert_eq!(operation.calls, 1);
        assert!(sleeper.slept_ms().is_empty());
        assert!(attempts.is_empty());
    }

    #[test]
    fn the_not_started_retry_story_exists_exactly_when_attempts_were_recorded() {
        // The conditional presence rule of ADR-0050: an empty log yields no
        // retry story — the never-retried `not_started` report stays
        // byte-identical — while a recorded log yields the full policy echo
        // with its entries.
        let policy = RetryPolicy::default();
        assert!(report_when_attempted(&policy, &[]).is_none());

        let mut operation = ScriptedOperation::new(vec![
            transient("connection shortage"),
            ScriptedOutcome::Succeed,
        ]);
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();
        run_unit(&policy, RetryUnit::Begin, &mut attempts, &sleeper, || {
            operation.invoke()
        })
        .expect("the second attempt succeeds");

        let story =
            *report_when_attempted(&policy, &attempts).expect("recorded attempts tell their story");
        assert_eq!(story["max_attempts"], 3);
        assert_eq!(
            story["attempts"].as_array().expect("attempts array").len(),
            1
        );
        assert_eq!(story["attempts"][0]["operation"], "begin");
    }

    #[test]
    fn the_report_value_orders_attempts_begin_first_then_chunk_index_then_attempt() {
        // The deterministic attempts order of ADR-0053: report assembly
        // sorts the log — `begin` entries first, then ascending chunk
        // index, then ascending attempt — so concurrent slots completing in
        // any wall-clock order yield one report shape, byte-identical to
        // the serial appending order.
        let policy = RetryPolicy::default();
        let sleeper = RecordingSleeper::new();
        let mut attempts = Vec::new();

        // Record real entries per unit, then interleave them in a scrambled
        // completion order: chunk 4 first, then begin, then chunk 1.
        let mut scrambled = Vec::new();
        for chunk_index in [4, 1] {
            let mut operation = ScriptedOperation::new(vec![
                transient("first shortage"),
                transient("second shortage"),
                ScriptedOutcome::Succeed,
            ]);
            run_unit(
                &policy,
                RetryUnit::WriteChunk { chunk_index },
                &mut attempts,
                &sleeper,
                || operation.invoke(),
            )
            .expect("the unit recovers");
            scrambled.append(&mut attempts);
            if chunk_index == 4 {
                let mut operation = ScriptedOperation::new(vec![
                    transient("connection shortage"),
                    ScriptedOutcome::Succeed,
                ]);
                run_unit(&policy, RetryUnit::Begin, &mut attempts, &sleeper, || {
                    operation.invoke()
                })
                .expect("begin recovers");
                scrambled.append(&mut attempts);
            }
        }

        let entries = report_value(&policy, &scrambled)["attempts"]
            .as_array()
            .expect("attempts array")
            .clone();
        let keys: Vec<(Value, Value, Value)> = entries
            .iter()
            .map(|entry| {
                (
                    entry["operation"].clone(),
                    entry.get("chunk_index").cloned().unwrap_or(Value::Null),
                    entry["attempt"].clone(),
                )
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                (json!("begin"), Value::Null, json!(1)),
                (json!("write_chunk"), json!(1), json!(1)),
                (json!("write_chunk"), json!(1), json!(2)),
                (json!("write_chunk"), json!(4), json!(1)),
                (json!("write_chunk"), json!(4), json!(2)),
            ]
        );
    }

    #[test]
    fn the_report_value_echoes_the_effective_policy_around_the_attempts() {
        // The retry object is the policy echo plus the attempts array — an
        // always-present empty array when nothing was retried.
        let policy = RetryPolicy {
            max_attempts: NonZeroU64::new(5).expect("nonzero"),
            initial_delay_ms: 50,
            max_delay_ms: 900,
        };
        assert_eq!(
            report_value(&policy, &[]),
            json!({
                "max_attempts": 5,
                "initial_delay_ms": 50,
                "max_delay_ms": 900,
                "attempts": []
            })
        );
    }
}
