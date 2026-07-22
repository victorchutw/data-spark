//! The write-phase dispatcher (ADR-0046, ADR-0047, ADR-0048, ADR-0051):
//! one orchestrator-owned loop that streams the source chunks into one
//! destination write session — begin, up to the effective parallelism of
//! concurrent `write_chunk` operations in flight at once, then the
//! terminal commit. Source chunk pulls stay strictly sequential and in
//! stream order — only destination writes run concurrently — and the
//! dispatcher pulls the next chunk only when a write slot is free, so at
//! most `parallelism` chunks are materialized simultaneously. At effective
//! parallelism 1 — every load against the shipped connector matrix, whose
//! declared limits are all 1 — the dispatcher is today's sequential loop.
//! The retry engine wraps `begin` and each `write_chunk` — the two retry
//! units (ADR-0048) — re-attempting transient failures under the policy
//! through the sleeper; each in-flight unit retries independently inside
//! its slot, and failed attempts of retried units accumulate onto the
//! orchestrator-held attempt log, ordered deterministically at report
//! assembly. The terminal `commit` is never engine-retried, structurally:
//! committing consumes the writer, so nothing remains to re-call.

use crate::connector::{
    Destination, DestinationWriteFacts, DestinationWriteFailure, DestinationWriter, LoadMode,
    Transience,
};
use crate::retry;
use crate::LoadFailure;
use arrow_array::RecordBatch;
use std::num::NonZeroU64;
use std::sync::mpsc;

/// The write-phase outcome of a load whose every chunk committed: the chunk
/// count the report states as `batch_count`, plus the destination-owned
/// write result.
pub(crate) struct WritePhaseOutcome {
    pub(crate) bytes_written: Option<u64>,
    pub(crate) facts: DestinationWriteFacts,
    pub(crate) chunk_count: u64,
}

/// A write-phase failure, split at the session boundary (ADR-0047): before
/// the destination session opened no record batch was ever exchanged, so
/// the load keeps the pre-write report posture; once it opened, failures
/// report the committed execution posture.
#[derive(Debug)]
pub(crate) enum WritePhaseFailure {
    BeforeSession(DestinationWriteFailure),
    InSession(DestinationWriteFailure),
}

/// Runs the write phase of a load under the effective parallelism — the
/// already-resolved `min(configured, connector limit)` for the load's mode
/// (ADR-0052), which the dispatcher enforces as the in-flight window bound
/// and never reasons about beyond that; commit-semantics safety above 1 is
/// the declaring connector's obligation (ADR-0051). A destination failure
/// in the serial path carries its own committed-state facts; a source
/// failure mid-stream — the mutation guard, or a chunk that fails to
/// build — abandons the session and reports the destination state at
/// abandonment, so append's committed chunk prefix stays honestly visible
/// (ADR-0047).
pub(crate) fn run_write_phase(
    chunks: impl Iterator<Item = Result<RecordBatch, LoadFailure>>,
    destination: &dyn Destination,
    mode: LoadMode,
    parallelism: NonZeroU64,
    retry_policy: &retry::RetryPolicy,
    sleeper: &dyn retry::Sleeper,
    retry_attempts: &mut Vec<retry::RetryAttempt>,
) -> Result<WritePhaseOutcome, WritePhaseFailure> {
    let writer = retry::run_unit(
        retry_policy,
        retry::RetryUnit::Begin,
        retry_attempts,
        sleeper,
        || destination.begin(mode),
    )
    .map_err(WritePhaseFailure::BeforeSession)?;
    if parallelism.get() == 1 {
        run_serial_write_phase(chunks, writer, retry_policy, sleeper, retry_attempts)
    } else {
        run_windowed_write_phase(
            chunks,
            writer,
            parallelism,
            retry_policy,
            sleeper,
            retry_attempts,
        )
    }
}

/// The sequential write phase — effective parallelism 1, every load in the
/// shipped connector matrix: one `write_chunk` at a time, in stream order,
/// observably today's loop end to end.
fn run_serial_write_phase(
    chunks: impl Iterator<Item = Result<RecordBatch, LoadFailure>>,
    writer: Box<dyn DestinationWriter>,
    retry_policy: &retry::RetryPolicy,
    sleeper: &dyn retry::Sleeper,
    retry_attempts: &mut Vec<retry::RetryAttempt>,
) -> Result<WritePhaseOutcome, WritePhaseFailure> {
    let mut chunk_count = 0_u64;
    for chunk in chunks {
        match chunk {
            Ok(batch) => {
                retry::run_unit(
                    retry_policy,
                    retry::RetryUnit::WriteChunk {
                        chunk_index: chunk_count,
                    },
                    retry_attempts,
                    sleeper,
                    || writer.write_chunk(&batch),
                )
                .map_err(WritePhaseFailure::InSession)?;
                chunk_count += 1;
            }
            Err(source_failure) => {
                return Err(abandon_in_session(writer, source_failure));
            }
        }
    }
    let write = writer.commit().map_err(WritePhaseFailure::InSession)?;
    Ok(WritePhaseOutcome {
        bytes_written: write.bytes_written,
        facts: write.facts,
        chunk_count,
    })
}

/// Ends a halted session and surfaces the given failure — code and message
/// unchanged, no wrapper — joined with the destination-owned committed
/// state at abandonment (ADR-0047), so append's committed prefix stays
/// honestly visible whichever path halted the load.
fn abandon_in_session(
    writer: Box<dyn DestinationWriter>,
    failure: LoadFailure,
) -> WritePhaseFailure {
    let abandoned = writer.abandon();
    WritePhaseFailure::InSession(DestinationWriteFailure {
        failure,
        facts: abandoned.facts,
        written_records: abandoned.written_records,
        committed_chunks: abandoned.committed_chunks,
        transience: Transience::Terminal,
    })
}

/// What one write slot reports back to the dispatcher when its unit
/// completes: the unit's chunk index, the failed attempts its retries
/// recorded, and the terminal outcome after those retries — or the panic
/// a misbehaving writer escaped with, which the dispatcher re-raises on
/// the dispatching thread after the drain instead of hanging on a
/// completion that would never arrive.
struct SlotCompletion {
    chunk_index: u64,
    attempts: Vec<retry::RetryAttempt>,
    outcome: Result<(), SlotFailure>,
}

/// How a write slot's unit ended short of success: the unit's terminal
/// write failure, or a writer panic caught at the slot boundary.
enum SlotFailure {
    Write(DestinationWriteFailure),
    Panicked(Box<dyn std::any::Any + Send>),
}

/// The bounded-window write phase (ADR-0051) — effective parallelism above
/// 1, reachable only through a connector that declared a limit above 1: up
/// to `parallelism` chunk writes run concurrently on scoped worker
/// threads, the dispatching thread pulls the source only when a slot is
/// free — the pulled chunk immediately occupies it, bounding materialized
/// chunks at the window size — and each slot runs its own retry unit with
/// its own budget and backoff, waiting inside its slot without pausing
/// siblings. On any terminal unit failure or source pull failure the
/// dispatcher stops dispatching, lets the in-flight writes run to
/// completion (no preemption), then abandons the session so the committed
/// facts stay destination-owned; the surfaced failure is the terminal
/// failure with the lowest chunk index, never wall-clock order.
fn run_windowed_write_phase(
    mut chunks: impl Iterator<Item = Result<RecordBatch, LoadFailure>>,
    writer: Box<dyn DestinationWriter>,
    parallelism: NonZeroU64,
    retry_policy: &retry::RetryPolicy,
    sleeper: &dyn retry::Sleeper,
    retry_attempts: &mut Vec<retry::RetryAttempt>,
) -> Result<WritePhaseOutcome, WritePhaseFailure> {
    let window = usize::try_from(parallelism.get()).unwrap_or(usize::MAX);
    let mut write_failures: Vec<(u64, DestinationWriteFailure)> = Vec::new();
    let mut source_failure: Option<LoadFailure> = None;
    let mut writer_panic: Option<Box<dyn std::any::Any + Send>> = None;
    let mut dispatched = 0_u64;

    std::thread::scope(|scope| {
        let writer = writer.as_ref();
        let (completion_sender, completion_receiver) = mpsc::channel::<SlotCompletion>();
        let mut in_flight = 0_usize;
        let mut source_done = false;
        // Once a unit fails terminally or a pull fails, no further chunk is
        // dispatched; the loop below keeps receiving until the in-flight
        // writes have drained.
        let mut halted = false;
        loop {
            while !halted && !source_done && in_flight < window {
                match chunks.next() {
                    None => source_done = true,
                    Some(Ok(batch)) => {
                        let chunk_index = dispatched;
                        dispatched += 1;
                        in_flight += 1;
                        let completion_sender = completion_sender.clone();
                        scope.spawn(move || {
                            let mut attempts = Vec::new();
                            // A panicking writer is a connector-contract
                            // violation; catching it at the slot boundary
                            // keeps the completion protocol whole — one
                            // send per dispatched unit — so the dispatcher
                            // drains and re-raises instead of waiting
                            // forever on a completion that died.
                            let outcome =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    retry::run_unit(
                                        retry_policy,
                                        retry::RetryUnit::WriteChunk { chunk_index },
                                        &mut attempts,
                                        sleeper,
                                        || writer.write_chunk(&batch),
                                    )
                                    .map(drop)
                                }));
                            let outcome = match outcome {
                                Ok(unit_outcome) => unit_outcome.map_err(SlotFailure::Write),
                                Err(payload) => Err(SlotFailure::Panicked(payload)),
                            };
                            // The dispatcher receives once per dispatched
                            // unit, so the receiver outlives every send.
                            let _ = completion_sender.send(SlotCompletion {
                                chunk_index,
                                attempts,
                                outcome,
                            });
                        });
                    }
                    Some(Err(failure)) => {
                        source_failure = Some(failure);
                        halted = true;
                    }
                }
            }
            if in_flight == 0 {
                break;
            }
            let completion = completion_receiver
                .recv()
                .expect("every dispatched unit completes");
            in_flight -= 1;
            retry_attempts.extend(completion.attempts);
            match completion.outcome {
                Ok(()) => {}
                Err(SlotFailure::Write(failure)) => {
                    write_failures.push((completion.chunk_index, failure));
                    halted = true;
                }
                Err(SlotFailure::Panicked(payload)) => {
                    if writer_panic.is_none() {
                        writer_panic = Some(payload);
                    }
                    halted = true;
                }
            }
        }
    });

    // Re-raise a caught writer panic on the dispatching thread once the
    // drain is done, before touching the writer again — its state is
    // suspect, and unwinding drops it through the session's own cleanup.
    if let Some(payload) = writer_panic {
        std::panic::resume_unwind(payload);
    }

    if write_failures.is_empty() && source_failure.is_none() {
        let write = writer.commit().map_err(WritePhaseFailure::InSession)?;
        return Ok(WritePhaseOutcome {
            bytes_written: write.bytes_written,
            facts: write.facts,
            chunk_count: dispatched,
        });
    }

    // The in-flight writes drained inside the scope; the session is
    // abandoned now, so the committed facts stay destination-owned through
    // the same path a serial source failure takes. Every dispatched write
    // failure has a chunk index below the pull position a source failure
    // halted at, so taking the lowest-indexed write failure — and the
    // source failure only when no write failed — is the lowest-chunk-index
    // rule.
    let failure = write_failures
        .into_iter()
        .min_by_key(|(chunk_index, _)| *chunk_index)
        .map(|(_, failure)| failure.failure)
        .or(source_failure)
        .expect("a halted window holds a failure");
    Err(abandon_in_session(writer, failure))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::{AbandonedWrite, DestinationWrite, DestinationWriter};
    use serde_json::{json, Value};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    fn int64_batch(values: &[i64]) -> RecordBatch {
        let field = arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false);
        let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![field]));
        RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(arrow_array::Int64Array::from(
                values.to_vec(),
            ))],
        )
        .expect("test batch")
    }

    fn batch_values(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("int64 column")
            .values()
            .to_vec()
    }

    fn chunk_stream(chunks: &[&[i64]]) -> crate::connector::SourceChunks {
        let batches: Vec<Result<RecordBatch, LoadFailure>> = chunks
            .iter()
            .map(|values| Ok(int64_batch(values)))
            .collect();
        Box::new(batches.into_iter())
    }

    enum ScriptedWriteOutcome {
        Succeed,
        Fail {
            message: &'static str,
            transience: Transience,
        },
        /// A writer panic mid-write: the connector-contract violation the
        /// dispatcher must drain and re-raise rather than hang on.
        Panic {
            message: &'static str,
        },
    }

    fn scripted_transient(message: &'static str) -> ScriptedWriteOutcome {
        ScriptedWriteOutcome::Fail {
            message,
            transience: Transience::Transient,
        }
    }

    /// The scripted in-crate destination of the write-phase retry tests
    /// (ADR-0048): models per-chunk commits like an append session, scripts
    /// each `begin` and `write_chunk` call's outcome in call order — an
    /// exhausted script succeeds — and observes every write invocation, so
    /// tests prove the engine re-submits the same chunk batch and the
    /// committed prefix stays honest. Transient failures are constructed
    /// here and nowhere else: no shipped connector classifies any failure
    /// transient.
    struct ScriptedDestination {
        begin_outcomes: Mutex<VecDeque<ScriptedWriteOutcome>>,
        write_outcomes: Arc<Mutex<VecDeque<ScriptedWriteOutcome>>>,
        begin_calls: AtomicU64,
        observed_writes: Arc<Mutex<Vec<Vec<i64>>>>,
    }

    impl ScriptedDestination {
        fn new(
            begin_outcomes: Vec<ScriptedWriteOutcome>,
            write_outcomes: Vec<ScriptedWriteOutcome>,
        ) -> Self {
            ScriptedDestination {
                begin_outcomes: Mutex::new(begin_outcomes.into()),
                write_outcomes: Arc::new(Mutex::new(write_outcomes.into())),
                begin_calls: AtomicU64::new(0),
                observed_writes: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn observed_writes(&self) -> Vec<Vec<i64>> {
            self.observed_writes.lock().expect("observed lock").clone()
        }

        fn begin_calls(&self) -> u64 {
            self.begin_calls.load(Ordering::SeqCst)
        }
    }

    impl Destination for ScriptedDestination {
        fn supported_load_modes(&self) -> &'static [LoadMode] {
            &[LoadMode::Append]
        }

        fn begin(
            &self,
            _mode: LoadMode,
        ) -> Result<Box<dyn DestinationWriter>, DestinationWriteFailure> {
            self.begin_calls.fetch_add(1, Ordering::SeqCst);
            match self.begin_outcomes.lock().expect("script lock").pop_front() {
                Some(ScriptedWriteOutcome::Panic { message }) => panic!("{message}"),
                None | Some(ScriptedWriteOutcome::Succeed) => Ok(Box::new(ScriptedWriter {
                    write_outcomes: Arc::clone(&self.write_outcomes),
                    observed_writes: Arc::clone(&self.observed_writes),
                    progress: Mutex::new(ScriptedProgress {
                        committed_chunks: 0,
                        written_records: 0,
                    }),
                })),
                Some(ScriptedWriteOutcome::Fail {
                    message,
                    transience,
                }) => Err(DestinationWriteFailure {
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

    struct ScriptedProgress {
        committed_chunks: u64,
        written_records: u64,
    }

    struct ScriptedWriter {
        write_outcomes: Arc<Mutex<VecDeque<ScriptedWriteOutcome>>>,
        observed_writes: Arc<Mutex<Vec<Vec<i64>>>>,
        progress: Mutex<ScriptedProgress>,
    }

    impl DestinationWriter for ScriptedWriter {
        fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
            self.observed_writes
                .lock()
                .expect("observed lock")
                .push(batch_values(batch));
            let mut progress = self.progress.lock().expect("progress lock");
            match self.write_outcomes.lock().expect("script lock").pop_front() {
                Some(ScriptedWriteOutcome::Panic { message }) => panic!("{message}"),
                None | Some(ScriptedWriteOutcome::Succeed) => {
                    progress.committed_chunks += 1;
                    progress.written_records += batch.num_rows() as u64;
                    Ok(())
                }
                Some(ScriptedWriteOutcome::Fail {
                    message,
                    transience,
                }) => Err(DestinationWriteFailure {
                    failure: LoadFailure {
                        code: "destination_write_failed",
                        message: message.to_string(),
                    },
                    facts: DestinationWriteFacts::best_effort("scripted_append"),
                    written_records: progress.written_records,
                    committed_chunks: progress.committed_chunks,
                    transience,
                }),
            }
        }

        fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
            Ok(DestinationWrite {
                bytes_written: None,
                facts: DestinationWriteFacts::best_effort("scripted_append"),
            })
        }

        fn abandon(self: Box<Self>) -> AbandonedWrite {
            let progress = self.progress.lock().expect("progress lock");
            AbandonedWrite {
                committed_chunks: progress.committed_chunks,
                written_records: progress.written_records,
                facts: DestinationWriteFacts::best_effort("scripted_append"),
            }
        }
    }

    // ---- Bounded-window dispatch (issue #53: ADR-0051, ADR-0052, ADR-0053) ----
    //
    // Every window test synchronizes through explicit rendezvous — barriers
    // and single-use gates — with no sleeps and no timing assertions: a
    // window that cannot reach its bound deadlocks instead of passing, and
    // every asserted order is forced by a happens-before edge, not by
    // scheduling luck.

    fn window(parallelism: u64) -> NonZeroU64 {
        NonZeroU64::new(parallelism).expect("nonzero window")
    }

    /// A chunk stream wrapper counting the chunks the dispatcher pulled, so
    /// tests prove pull-on-free-slot: the source is not pulled while every
    /// slot is occupied.
    struct CountedChunks {
        inner: crate::connector::SourceChunks,
        pulled: Arc<AtomicU64>,
    }

    impl Iterator for CountedChunks {
        type Item = Result<RecordBatch, LoadFailure>;

        fn next(&mut self) -> Option<Self::Item> {
            let item = self.inner.next();
            if item.is_some() {
                self.pulled.fetch_add(1, Ordering::SeqCst);
            }
            item
        }
    }

    fn counted_chunks(chunks: &[&[i64]]) -> (CountedChunks, Arc<AtomicU64>) {
        let pulled = Arc::new(AtomicU64::new(0));
        (
            CountedChunks {
                inner: chunk_stream(chunks),
                pulled: Arc::clone(&pulled),
            },
            pulled,
        )
    }

    /// What a rendezvous wave does after its members meet: release
    /// immediately, or hold every member at a second barrier until each
    /// has recorded the pull count — frozen, because no completion can
    /// reach the dispatcher while the wave holds every slot open.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum WaveObservation {
        MeetAndRelease,
        RecordFrozenPulls,
    }

    /// The rendezvous destination of the window-bound tests: `write_chunk`
    /// tracks the in-flight count, then meets its wave at a barrier sized
    /// to the window. Reaching the barrier requires the full wave to be in
    /// flight simultaneously, and the tracked maximum proves the window
    /// never exceeded it.
    struct RendezvousProbe {
        rendezvous: std::sync::Barrier,
        in_flight: AtomicU64,
        max_in_flight: AtomicU64,
        pulled: Arc<AtomicU64>,
        observed_pulls: Mutex<Vec<u64>>,
        observation: WaveObservation,
    }

    struct RendezvousDestination {
        limit: NonZeroU64,
        probe: Arc<RendezvousProbe>,
    }

    impl RendezvousDestination {
        fn new(wave: usize, pulled: Arc<AtomicU64>, observation: WaveObservation) -> Self {
            RendezvousDestination {
                limit: window(wave as u64),
                probe: Arc::new(RendezvousProbe {
                    rendezvous: std::sync::Barrier::new(wave),
                    in_flight: AtomicU64::new(0),
                    max_in_flight: AtomicU64::new(0),
                    pulled,
                    observed_pulls: Mutex::new(Vec::new()),
                    observation,
                }),
            }
        }
    }

    impl Destination for RendezvousDestination {
        fn supported_load_modes(&self) -> &'static [LoadMode] {
            &[LoadMode::Append]
        }

        fn parallelism_limit(&self, _mode: LoadMode) -> NonZeroU64 {
            self.limit
        }

        fn begin(
            &self,
            _mode: LoadMode,
        ) -> Result<Box<dyn DestinationWriter>, DestinationWriteFailure> {
            Ok(Box::new(RendezvousWriter {
                probe: Arc::clone(&self.probe),
                progress: Mutex::new(0),
            }))
        }
    }

    struct RendezvousWriter {
        probe: Arc<RendezvousProbe>,
        progress: Mutex<u64>,
    }

    impl DestinationWriter for RendezvousWriter {
        fn write_chunk(&self, _batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
            let now = self.probe.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.probe.max_in_flight.fetch_max(now, Ordering::SeqCst);
            self.probe.rendezvous.wait();
            if self.probe.observation == WaveObservation::RecordFrozenPulls {
                self.probe
                    .observed_pulls
                    .lock()
                    .expect("observed lock")
                    .push(self.probe.pulled.load(Ordering::SeqCst));
                self.probe.rendezvous.wait();
            }
            self.probe.in_flight.fetch_sub(1, Ordering::SeqCst);
            *self.progress.lock().expect("progress lock") += 1;
            Ok(())
        }

        fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
            Ok(DestinationWrite {
                bytes_written: None,
                facts: DestinationWriteFacts::best_effort("windowed_append"),
            })
        }

        fn abandon(self: Box<Self>) -> AbandonedWrite {
            let committed = *self.progress.lock().expect("progress lock");
            AbandonedWrite {
                committed_chunks: committed,
                written_records: committed,
                facts: DestinationWriteFacts::best_effort("windowed_append"),
            }
        }
    }

    #[test]
    fn the_window_reaches_and_never_exceeds_the_effective_parallelism() {
        // Four chunks under a window of 2 against a rendezvous of 2: each
        // wave's two writes must be in flight simultaneously before either
        // may return — a window unable to reach 2 deadlocks instead of
        // passing — and the tracked maximum proves the window never held
        // more than 2 writes in flight.
        let (chunks, _pulled) = counted_chunks(&[&[1], &[2], &[3], &[4]]);
        let destination = RendezvousDestination::new(
            2,
            Arc::clone(&chunks.pulled),
            WaveObservation::MeetAndRelease,
        );
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let outcome = run_write_phase(
            chunks,
            &destination,
            LoadMode::Append,
            window(2),
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        )
        .expect("all four chunks commit");

        assert_eq!(outcome.chunk_count, 4);
        assert_eq!(
            destination.probe.max_in_flight.load(Ordering::SeqCst),
            2,
            "the window reached 2 and never exceeded it"
        );
        assert!(retry_attempts.is_empty());
        assert!(sleeper.slept_ms().is_empty());
    }

    #[test]
    fn the_source_is_pulled_only_when_a_slot_frees_and_chunks_stay_window_bounded() {
        // Two-phase rendezvous: while a wave of 2 holds both slots open, no
        // completion can reach the dispatcher, so the pull count each wave
        // member observes is frozen — exactly 2 pulls during the first
        // wave, exactly 4 during the second. Each pulled chunk immediately
        // occupies the slot that freed, so the observation is also the
        // materialization bound: never more than window-many chunks exist.
        let (chunks, pulled) = counted_chunks(&[&[1], &[2], &[3], &[4]]);
        let destination =
            RendezvousDestination::new(2, Arc::clone(&pulled), WaveObservation::RecordFrozenPulls);
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let outcome = run_write_phase(
            chunks,
            &destination,
            LoadMode::Append,
            window(2),
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        )
        .expect("all four chunks commit");

        assert_eq!(outcome.chunk_count, 4);
        assert_eq!(pulled.load(Ordering::SeqCst), 4, "every chunk pulled once");
        assert_eq!(
            *destination.probe.observed_pulls.lock().expect("observed"),
            vec![2, 2, 4, 4],
            "the source was never pulled while every slot was occupied"
        );
    }

    /// One scripted step of the gated destination, popped per `write_chunk`
    /// call of its chunk value: an optional wait on the shared single-use
    /// gate before acting, the outcome, and an optional gate release
    /// before returning — the happens-before edges the drain and ordering
    /// tests are built from.
    struct GatedStep {
        wait_for_gate: bool,
        release_gate: bool,
        outcome: ScriptedWriteOutcome,
    }

    /// A plain ungated step.
    fn step(outcome: ScriptedWriteOutcome) -> GatedStep {
        GatedStep {
            wait_for_gate: false,
            release_gate: false,
            outcome,
        }
    }

    /// A step that acts only once the gate has been released.
    fn wait_then(outcome: ScriptedWriteOutcome) -> GatedStep {
        GatedStep {
            wait_for_gate: true,
            release_gate: false,
            outcome,
        }
    }

    /// A step that releases the gate on its way out.
    fn release_after(outcome: ScriptedWriteOutcome) -> GatedStep {
        GatedStep {
            wait_for_gate: false,
            release_gate: true,
            outcome,
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum GatedEvent {
        Entered(i64),
        Exited(i64),
        Abandoned,
    }

    struct GatedState {
        steps: Mutex<std::collections::HashMap<i64, VecDeque<GatedStep>>>,
        gate_sender: Mutex<Option<std::sync::mpsc::Sender<()>>>,
        gate_receiver: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        events: Mutex<Vec<GatedEvent>>,
        committed_chunks: AtomicU64,
        written_records: AtomicU64,
    }

    /// The gated scripted destination of the drain, ordering, and
    /// concurrent-retry tests: each chunk value carries its own step queue,
    /// and one single-use gate sequences the slots — the waiting step
    /// cannot act before the releasing step acted, whatever the scheduler
    /// does.
    struct GatedDestination {
        limit: NonZeroU64,
        state: Arc<GatedState>,
    }

    impl GatedDestination {
        fn new(scripts: Vec<(i64, Vec<GatedStep>)>) -> Self {
            let (gate_sender, gate_receiver) = std::sync::mpsc::channel();
            GatedDestination {
                limit: window(2),
                state: Arc::new(GatedState {
                    steps: Mutex::new(
                        scripts
                            .into_iter()
                            .map(|(value, steps)| (value, steps.into()))
                            .collect(),
                    ),
                    gate_sender: Mutex::new(Some(gate_sender)),
                    gate_receiver: Mutex::new(Some(gate_receiver)),
                    events: Mutex::new(Vec::new()),
                    committed_chunks: AtomicU64::new(0),
                    written_records: AtomicU64::new(0),
                }),
            }
        }

        fn events(&self) -> Vec<GatedEvent> {
            self.state.events.lock().expect("events lock").clone()
        }
    }

    impl Destination for GatedDestination {
        fn supported_load_modes(&self) -> &'static [LoadMode] {
            &[LoadMode::Append]
        }

        fn parallelism_limit(&self, _mode: LoadMode) -> NonZeroU64 {
            self.limit
        }

        fn begin(
            &self,
            _mode: LoadMode,
        ) -> Result<Box<dyn DestinationWriter>, DestinationWriteFailure> {
            Ok(Box::new(GatedWriter {
                state: Arc::clone(&self.state),
            }))
        }
    }

    struct GatedWriter {
        state: Arc<GatedState>,
    }

    impl DestinationWriter for GatedWriter {
        fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
            let value = batch_values(batch)[0];
            let state = &*self.state;
            state
                .events
                .lock()
                .expect("events lock")
                .push(GatedEvent::Entered(value));
            let step = state
                .steps
                .lock()
                .expect("steps lock")
                .get_mut(&value)
                .and_then(VecDeque::pop_front)
                .unwrap_or_else(|| step(ScriptedWriteOutcome::Succeed));
            if step.wait_for_gate {
                let receiver = state
                    .gate_receiver
                    .lock()
                    .expect("gate lock")
                    .take()
                    .expect("the gate is waited on once");
                receiver.recv().expect("the gate is released");
            }
            let result = match step.outcome {
                ScriptedWriteOutcome::Panic { message } => panic!("{message}"),
                ScriptedWriteOutcome::Succeed => {
                    state.committed_chunks.fetch_add(1, Ordering::SeqCst);
                    state
                        .written_records
                        .fetch_add(batch.num_rows() as u64, Ordering::SeqCst);
                    Ok(())
                }
                ScriptedWriteOutcome::Fail {
                    message,
                    transience,
                } => Err(DestinationWriteFailure {
                    failure: LoadFailure {
                        code: "destination_write_failed",
                        message: message.to_string(),
                    },
                    facts: DestinationWriteFacts::best_effort("windowed_append"),
                    written_records: state.written_records.load(Ordering::SeqCst),
                    committed_chunks: state.committed_chunks.load(Ordering::SeqCst),
                    transience,
                }),
            };
            state
                .events
                .lock()
                .expect("events lock")
                .push(GatedEvent::Exited(value));
            if step.release_gate {
                let sender = state
                    .gate_sender
                    .lock()
                    .expect("gate lock")
                    .take()
                    .expect("the gate is released once");
                sender.send(()).expect("the gate waiter is alive");
            }
            result
        }

        fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
            Ok(DestinationWrite {
                bytes_written: None,
                facts: DestinationWriteFacts::best_effort("windowed_append"),
            })
        }

        fn abandon(self: Box<Self>) -> AbandonedWrite {
            self.state
                .events
                .lock()
                .expect("events lock")
                .push(GatedEvent::Abandoned);
            let committed_chunks = self.state.committed_chunks.load(Ordering::SeqCst);
            AbandonedWrite {
                committed_chunks,
                written_records: self.state.written_records.load(Ordering::SeqCst),
                facts: if committed_chunks > 0 {
                    DestinationWriteFacts::best_effort("windowed_append")
                } else {
                    DestinationWriteFacts::not_applicable()
                },
            }
        }
    }

    fn terminal(message: &'static str) -> ScriptedWriteOutcome {
        ScriptedWriteOutcome::Fail {
            message,
            transience: Transience::Terminal,
        }
    }

    #[test]
    fn a_terminal_failure_drains_the_in_flight_write_then_abandons_with_its_commit() {
        // Chunk 0's write waits on the gate; chunk 1 fails terminally and
        // releases it on the way out. The engine must let chunk 0 run to
        // completion — its commit joins the abandoned session's counts, so
        // the surfaced failure reports one committed chunk even though the
        // failing write saw zero at raise time — and abandon happens only
        // after every in-flight write exited.
        let destination = GatedDestination::new(vec![
            (10, vec![wait_then(ScriptedWriteOutcome::Succeed)]),
            (20, vec![release_after(terminal("value 20 detached"))]),
        ]);
        let (chunks, pulled) = counted_chunks(&[&[10], &[20]]);
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let Err(WritePhaseFailure::InSession(failure)) = run_write_phase(
            chunks,
            &destination,
            LoadMode::Append,
            window(2),
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        ) else {
            panic!("the terminal unit failure fails the load in session")
        };

        assert_eq!(failure.failure.code, "destination_write_failed");
        assert_eq!(failure.failure.message, "value 20 detached");
        assert_eq!(
            failure.committed_chunks, 1,
            "the drained write's commit is reported through the abandon path"
        );
        assert_eq!(failure.written_records, 1);
        assert_eq!(
            failure.facts.report_value(),
            serde_json::json!({ "atomicity": "best_effort", "strategy": "windowed_append" })
        );
        assert_eq!(pulled.load(Ordering::SeqCst), 2);
        let events = destination.events();
        assert_eq!(
            events.last(),
            Some(&GatedEvent::Abandoned),
            "abandon happens only after the drain"
        );
        assert!(events.contains(&GatedEvent::Exited(10)));
        assert!(events.contains(&GatedEvent::Exited(20)));
        assert!(retry_attempts.is_empty());
    }

    #[test]
    fn concurrent_terminal_failures_surface_the_lowest_chunk_index_not_wall_clock_order() {
        // Chunk 1 fails first in wall clock — its exit is recorded before
        // the gate release that lets chunk 0 fail at all — yet the
        // surfaced failure is chunk 0's, the lowest failing index. The
        // first received failure also halts dispatch, so chunk 2 is never
        // pulled or written.
        let destination = GatedDestination::new(vec![
            (10, vec![wait_then(terminal("value 10 detached"))]),
            (20, vec![release_after(terminal("value 20 detached"))]),
        ]);
        let (chunks, pulled) = counted_chunks(&[&[10], &[20], &[30]]);
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let Err(WritePhaseFailure::InSession(failure)) = run_write_phase(
            chunks,
            &destination,
            LoadMode::Append,
            window(2),
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        ) else {
            panic!("concurrent terminal failures fail the load in session")
        };

        assert_eq!(failure.failure.message, "value 10 detached");
        assert_eq!(failure.committed_chunks, 0);
        assert_eq!(
            failure.facts.report_value(),
            serde_json::json!({ "atomicity": "not_applicable" }),
            "nothing committed, so the abandoned session states no strategy"
        );
        assert_eq!(
            pulled.load(Ordering::SeqCst),
            2,
            "the halt stops dispatch: chunk 2 is never pulled"
        );
        let events = destination.events();
        let exited_20 = events
            .iter()
            .position(|event| *event == GatedEvent::Exited(20))
            .expect("chunk 1 exited");
        let exited_10 = events
            .iter()
            .position(|event| *event == GatedEvent::Exited(10))
            .expect("chunk 0 exited");
        assert!(
            exited_20 < exited_10,
            "chunk 1's failure preceded chunk 0's in wall clock"
        );
        assert!(!events.contains(&GatedEvent::Entered(30)));
        assert_eq!(events.last(), Some(&GatedEvent::Abandoned));
    }

    #[test]
    fn a_source_pull_failure_mid_stream_drains_and_abandons_the_windowed_session() {
        // The pull of the third chunk fails while both slots are occupied:
        // the dispatcher stops pulling, lets both dispatched writes run to
        // completion, then abandons — the committed writes stay
        // destination-owned through the abandon path and the source
        // failure surfaces unchanged, no write unit having failed.
        let destination = GatedDestination::new(Vec::new());
        let chunks: crate::connector::SourceChunks = Box::new(
            vec![
                Ok(int64_batch(&[10])),
                Ok(int64_batch(&[20])),
                Err(LoadFailure {
                    code: "source_changed_during_load",
                    message: "source changed during the load".to_string(),
                }),
            ]
            .into_iter(),
        );
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let Err(WritePhaseFailure::InSession(failure)) = run_write_phase(
            chunks,
            &destination,
            LoadMode::Append,
            window(2),
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        ) else {
            panic!("a mid-stream pull failure fails the load in session")
        };

        assert_eq!(failure.failure.code, "source_changed_during_load");
        assert_eq!(failure.failure.message, "source changed during the load");
        assert_eq!(
            failure.committed_chunks, 2,
            "both dispatched writes drained to completion before the abandon"
        );
        assert_eq!(failure.written_records, 2);
        let events = destination.events();
        assert_eq!(events.last(), Some(&GatedEvent::Abandoned));
        assert!(events.contains(&GatedEvent::Exited(10)));
        assert!(events.contains(&GatedEvent::Exited(20)));
        assert!(retry_attempts.is_empty());
    }

    #[test]
    #[should_panic(expected = "value 10 writer panic")]
    fn a_panicking_writer_drains_the_window_and_re_raises_on_the_dispatching_thread() {
        // A writer panic is a connector-contract violation, not a failure
        // the engine can report: the slot boundary catches it, the
        // dispatcher stops dispatching, drains the sibling write, and
        // re-raises the panic on the dispatching thread — deterministically,
        // instead of hanging on a completion that would never arrive.
        let destination = GatedDestination::new(vec![
            (
                10,
                vec![wait_then(ScriptedWriteOutcome::Panic {
                    message: "value 10 writer panic",
                })],
            ),
            (20, vec![release_after(ScriptedWriteOutcome::Succeed)]),
        ]);
        let (chunks, _pulled) = counted_chunks(&[&[10], &[20]]);
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let _ = run_write_phase(
            chunks,
            &destination,
            LoadMode::Append,
            window(2),
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        );
    }

    #[test]
    fn concurrent_slots_retry_independently_and_the_report_orders_their_attempts() {
        // Two chunks in flight together: chunk 0's first attempt waits on
        // the gate until chunk 1 has already failed twice, slept its own
        // backoff sequence, and succeeded — chunk 1's whole retry
        // lifecycle ran while chunk 0's slot sat occupied, so no slot's
        // waits paused a sibling. Both units recover inside their own
        // three-attempt budgets, each restarting the backoff sequence at
        // 200ms, and report assembly orders the interleaved log
        // deterministically by chunk index then attempt (the begin-first
        // rule is pinned in the retry module's ordering test).
        let destination = GatedDestination::new(vec![
            (
                10,
                vec![
                    wait_then(scripted_transient("value 10 first shortage")),
                    step(scripted_transient("value 10 second shortage")),
                    step(ScriptedWriteOutcome::Succeed),
                ],
            ),
            (
                20,
                vec![
                    step(scripted_transient("value 20 first shortage")),
                    step(scripted_transient("value 20 second shortage")),
                    release_after(ScriptedWriteOutcome::Succeed),
                ],
            ),
        ]);
        let (chunks, _pulled) = counted_chunks(&[&[10], &[20]]);
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let outcome = run_write_phase(
            chunks,
            &destination,
            LoadMode::Append,
            window(2),
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        )
        .expect("both units recover inside their own budgets");

        assert_eq!(outcome.chunk_count, 2);
        let mut slept = sleeper.slept_ms();
        slept.sort_unstable();
        assert_eq!(
            slept,
            vec![200, 200, 400, 400],
            "each slot slept its own restarted backoff sequence"
        );
        let entries = retry::report_value(&retry::RetryPolicy::default(), &retry_attempts)
            ["attempts"]
            .as_array()
            .expect("attempts array")
            .clone();
        let summarized: Vec<(Value, Value, Value)> = entries
            .iter()
            .map(|entry| {
                (
                    entry["chunk_index"].clone(),
                    entry["attempt"].clone(),
                    entry["delay_before_retry_ms"].clone(),
                )
            })
            .collect();
        assert_eq!(
            summarized,
            vec![
                (json!(0), json!(1), json!(200)),
                (json!(0), json!(2), json!(400)),
                (json!(1), json!(1), json!(200)),
                (json!(1), json!(2), json!(400)),
            ],
            "report assembly orders attempts by chunk index then attempt, \
             not by completion order"
        );
    }

    #[test]
    fn a_transient_write_chunk_failure_resubmits_the_same_chunk_batch() {
        // Chunk 0 fails transiently once: the engine re-invokes write_chunk
        // with the identical batch — the retry unit's same-input rule
        // (ADR-0048) — and the load completes with one recorded attempt.
        let destination =
            ScriptedDestination::new(Vec::new(), vec![scripted_transient("first chunk shortage")]);
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let outcome = run_write_phase(
            chunk_stream(&[&[1, 2], &[3, 4]]),
            &destination,
            LoadMode::Append,
            NonZeroU64::MIN,
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        )
        .expect("the retried chunk commits");

        assert_eq!(outcome.chunk_count, 2);
        assert_eq!(
            destination.observed_writes(),
            vec![vec![1, 2], vec![1, 2], vec![3, 4]],
            "the failed chunk is re-submitted with the same records"
        );
        assert_eq!(sleeper.slept_ms(), vec![200]);
        let entries: Vec<Value> = retry_attempts
            .iter()
            .map(retry::RetryAttempt::report_value)
            .collect();
        assert_eq!(
            entries,
            vec![json!({
                "operation": "write_chunk",
                "chunk_index": 0,
                "attempt": 1,
                "error": {
                    "code": "destination_write_failed",
                    "message": "first chunk shortage"
                },
                "delay_before_retry_ms": 200
            })]
        );
    }

    #[test]
    fn a_transient_begin_failure_reopens_the_session() {
        // begin is its own retry unit: a transient session-open failure is
        // re-attempted, and the chunks stream into the second session.
        let destination =
            ScriptedDestination::new(vec![scripted_transient("connection shortage")], Vec::new());
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let outcome = run_write_phase(
            chunk_stream(&[&[1]]),
            &destination,
            LoadMode::Append,
            NonZeroU64::MIN,
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        )
        .expect("the retried begin opens the session");

        assert_eq!(destination.begin_calls(), 2);
        assert_eq!(outcome.chunk_count, 1);
        assert_eq!(destination.observed_writes(), vec![vec![1]]);
        assert_eq!(sleeper.slept_ms(), vec![200]);
        let entries: Vec<Value> = retry_attempts
            .iter()
            .map(retry::RetryAttempt::report_value)
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["operation"], "begin");
        assert!(entries[0].get("chunk_index").is_none());
    }

    #[test]
    fn an_exhausted_chunk_unit_keeps_the_committed_prefix_and_the_full_history() {
        // Chunk 0 commits; chunk 1 exhausts its three-attempt budget. The
        // in-session failure carries the honest committed prefix and the
        // last failure unchanged, and the log holds every failed attempt of
        // the exhausted unit (ADR-0050).
        let destination = ScriptedDestination::new(
            Vec::new(),
            vec![
                ScriptedWriteOutcome::Succeed,
                scripted_transient("first shortage"),
                scripted_transient("second shortage"),
                scripted_transient("third shortage"),
            ],
        );
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let Err(WritePhaseFailure::InSession(failure)) = run_write_phase(
            chunk_stream(&[&[1], &[2], &[3]]),
            &destination,
            LoadMode::Append,
            NonZeroU64::MIN,
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        ) else {
            panic!("an exhausted chunk unit fails in session")
        };

        assert_eq!(failure.failure.code, "destination_write_failed");
        assert_eq!(failure.failure.message, "third shortage");
        assert_eq!(failure.committed_chunks, 1);
        assert_eq!(failure.written_records, 1);
        assert_eq!(
            destination.observed_writes(),
            vec![vec![1], vec![2], vec![2], vec![2]],
            "every re-attempt of the exhausted unit re-submitted the same batch"
        );
        assert_eq!(sleeper.slept_ms(), vec![200, 400]);
        let entries: Vec<Value> = retry_attempts
            .iter()
            .map(retry::RetryAttempt::report_value)
            .collect();
        assert_eq!(entries.len(), 3);
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(entry["operation"], "write_chunk");
            assert_eq!(entry["chunk_index"], 1);
            assert_eq!(entry["attempt"], index as u64 + 1);
        }
        assert_eq!(entries[0]["delay_before_retry_ms"], 200);
        assert_eq!(entries[1]["delay_before_retry_ms"], 400);
        assert!(entries[2].get("delay_before_retry_ms").is_none());
    }

    #[test]
    fn an_exhausted_begin_unit_fails_before_the_session_with_its_history() {
        // begin never opens: the failure stays before the session — the
        // not_started report posture — while the attempt log still tells
        // the whole retry story (ADR-0050's conditional presence rule).
        let destination = ScriptedDestination::new(
            vec![
                scripted_transient("first shortage"),
                scripted_transient("second shortage"),
                scripted_transient("third shortage"),
            ],
            Vec::new(),
        );
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();

        let Err(WritePhaseFailure::BeforeSession(failure)) = run_write_phase(
            chunk_stream(&[&[1]]),
            &destination,
            LoadMode::Append,
            NonZeroU64::MIN,
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        ) else {
            panic!("an exhausted begin unit fails before the session")
        };

        assert_eq!(failure.failure.message, "third shortage");
        assert_eq!(destination.begin_calls(), 3);
        assert!(destination.observed_writes().is_empty());
        assert_eq!(sleeper.slept_ms(), vec![200, 400]);
        let entries: Vec<Value> = retry_attempts
            .iter()
            .map(retry::RetryAttempt::report_value)
            .collect();
        assert_eq!(entries.len(), 3);
        assert!(entries.iter().all(|entry| entry["operation"] == "begin"));
    }
}
