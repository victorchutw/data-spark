//! Bounded-memory proof for chunked execution (issue #52, ADR-0045): loading
//! twice the data must not move the peak allocation by anything close to the
//! added data volume — multiple logical batches alone would not prove a
//! bounded peak, so the proof measures the allocator itself.
//!
//! The binary installs a counting global allocator and drives the full
//! pipeline in-process through `data_spark::run_from` (the ADR-0028 library
//! target), so every Rust-side allocation of the load — both source passes,
//! schema state, chunk buffers, the Parquet writer — is measured. The
//! destination is Parquet by design: DuckDB's bundled engine allocates
//! outside the Rust global allocator, so a DuckDB destination would evade
//! this accounting while exercising the identical source and chunk path.
//! The file holds this one test so no parallel test can pollute the peak.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingAllocator;

static CURRENT_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = System.alloc(layout);
        if !pointer.is_null() {
            let current = CURRENT_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK_BYTES.fetch_max(current, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        System.dealloc(pointer, layout);
        CURRENT_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn reset_peak() {
    PEAK_BYTES.store(CURRENT_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
}

fn peak_bytes() -> usize {
    PEAK_BYTES.load(Ordering::Relaxed)
}

/// One prepared load: the definition to execute and the artifacts directory
/// its report lands in. Source generation happens here, outside the measured
/// region, streamed to disk so the test itself never holds the source.
struct PreparedLoad {
    definition_path: std::path::PathBuf,
    artifacts_dir: std::path::PathBuf,
    source_bytes: u64,
    rows: usize,
}

fn prepare_load(work: &Path, label: &str, rows: usize) -> PreparedLoad {
    use std::io::Write;

    let source_path = work.join(format!("customers-{label}.csv"));
    let mut source = std::io::BufWriter::new(fs::File::create(&source_path).expect("create csv"));
    source
        .write_all(b"customer_id,name,total\n")
        .expect("write header");
    for row in 0..rows {
        writeln!(source, "{row},customer-{row},{row}.25").expect("write record");
    }
    source.flush().expect("flush csv");
    drop(source);
    let source_bytes = fs::metadata(&source_path).expect("source metadata").len();

    let definition_path = work.join(format!("load-{label}.yml"));
    fs::write(
        &definition_path,
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: csv\n\
             destination:\n\
             \x20 connector: parquet\n\
             \x20 path: {}\n\
             dataset: customers\n\
             load_mode: full_refresh\n\
             execution:\n\
             \x20 chunk_rows: 4096\n",
            source_path.display(),
            work.join(format!("customers-{label}-dataset")).display(),
        ),
    )
    .expect("write load definition");

    PreparedLoad {
        artifacts_dir: work.join(format!("artifacts-{label}")),
        definition_path,
        source_bytes,
        rows,
    }
}

/// Executes one prepared load in-process — the measured region — and then
/// asserts it succeeded via its report.
fn execute_load(load: &PreparedLoad) {
    data_spark::run_from([
        "data-spark".to_string(),
        "load".to_string(),
        "--output-dir".to_string(),
        load.artifacts_dir.display().to_string(),
        load.definition_path.display().to_string(),
    ]);

    let run_dirs = fs::read_dir(&load.artifacts_dir)
        .expect("artifact root")
        .collect::<Result<Vec<_>, _>>()
        .expect("artifact entries");
    assert_eq!(run_dirs.len(), 1, "one load run");
    let report: serde_json::Value = serde_json::from_slice(
        &fs::read(run_dirs[0].path().join("load-report.json")).expect("load report"),
    )
    .expect("json report");
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["written"], load.rows);
    assert_eq!(report["execution"]["batch_count"], load.rows.div_ceil(4096));
}

#[test]
fn peak_allocation_stays_bounded_when_the_source_doubles() {
    const ROWS: usize = 150_000;

    let work = tempfile::TempDir::new().expect("tempdir");
    let warmup = prepare_load(work.path(), "warmup", ROWS);
    let single = prepare_load(work.path(), "single", ROWS);
    let double = prepare_load(work.path(), "double", ROWS * 2);

    // Warm allocator pools, thread-locals, and lazily initialized state so
    // the measured runs compare like against like.
    execute_load(&warmup);

    reset_peak();
    execute_load(&single);
    let single_peak = peak_bytes();

    reset_peak();
    execute_load(&double);
    let double_peak = peak_bytes();

    let added_bytes = double.source_bytes - single.source_bytes;
    let peak_delta = double_peak.saturating_sub(single_peak);

    // The added source volume is megabytes; a bounded pipeline's peak may
    // wiggle by buffer-sized noise, never by anything proportional to it.
    const PEAK_DELTA_BOUND: usize = 2_000_000;
    assert!(
        added_bytes > 4_000_000,
        "the doubled source must add real volume, added {added_bytes} bytes"
    );
    assert!(
        peak_delta < PEAK_DELTA_BOUND,
        "peak allocation must stay bounded: single-source peak {single_peak}, \
         double-source peak {double_peak}, delta {peak_delta} exceeds \
         {PEAK_DELTA_BOUND} against {added_bytes} added source bytes"
    );
}
