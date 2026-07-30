# Tuning how a load executes

The `execution` block governs how a load runs rather than what it moves: how
many records a chunk holds, how many chunk writes may be in flight at once,
and how a failed write is re-attempted. Every knob defaults on its own, and
the defaults are deliberately dull — the reason to reach for the block is a
source too large for the memory you have, or a destination you expect to
stumble.

This guide tunes all three from a runnable example and reads back the report
fields that state what actually took effect, which is not always what the
definition asked for.

## Start from the example

[examples/chunked-execution](../../examples/chunked-execution/) appends five
records under a chunk bound of two. Copy it somewhere writable and load it
([why](../../examples/README.md#running-them)):

```bash
cp -r examples/chunked-execution /tmp/
cd /tmp/chunked-execution
data-spark load load.yml
```

Every command below was run, and every YAML fragment and report excerpt is
taken from a real load against the release binary. The `execution` block is the
whole subject:

```yaml
execution:
  chunk_rows: 2
  parallelism: 4
  retry:
    max_attempts: 5
    initial_delay_ms: 100
    max_delay_ms: 1000
```

`readings-dataset` ends up holding three `part-*.parquet` files and five
records, and the report states how the load got there:

```json
{
  "record_format": "arrow_record_batch",
  "batch_count": 3,
  "chunk_rows": 2,
  "parallelism": 1,
  "connector_parallelism_limit": 1,
  "retry": {
    "max_attempts": 5,
    "initial_delay_ms": 100,
    "max_delay_ms": 1000,
    "attempts": []
  }
}
```

Three of those numbers are echoes of the definition. `parallelism: 1` is not —
that one is the clamp, and the last section explains it.

## `chunk_rows`: the bound that keeps memory flat

A load reads, validates, writes, and commits in chunks, and `chunk_rows` is
the most records one chunk may hold
([ADR-0046](../adr/0046-resolve-then-stream-connector-ports-with-chunked-sessions.md)).
The default is `65536`, which is large enough that a small load still moves as
a single chunk and small enough to bound peak memory on a large one. Peak
memory follows the bound, not the size of the source — a 10 GB CSV file and a
10 KB one hold the same records in flight
([ADR-0045](../adr/0045-resolve-loads-in-two-streaming-source-passes.md)).

The bound counts records, not bytes, because records are the unit that
validation, rejection, and Arrow materialization already speak. Rejected
records never occupy a chunk: the bound is on surviving records.

`batch_count` in the report counts the chunks the destination **committed**,
so it is the observable side of the bound. Load the same five records with the
bound left at its default and the same work arrives as one chunk:

```json
{
  "record_format": "arrow_record_batch",
  "batch_count": 1,
  "chunk_rows": 65536,
  "parallelism": 1,
  "connector_parallelism_limit": 1,
  "retry": {
    "max_attempts": 3,
    "initial_delay_ms": 200,
    "max_delay_ms": 5000,
    "attempts": []
  }
}
```

Same five records in the destination dataset, one part file instead of three.
The bound changes how records travel, never which records land.

**Choosing a value.** Leave it alone until memory is the problem. Lower it
when records are wide — many fields, long text — or when the machine is
small; a few thousand is a reasonable floor to try. Raising it above the
default buys little, because the cost it controls is already amortized.
`chunk_rows: 0`, a negative value, or a non-integer fails the load at YAML
parsing.

## Commit boundaries follow the load mode

How a chunked load becomes visible is decided by the load mode, not by the
chunk count ([ADR-0047](../adr/0047-commit-full-refresh-terminally-and-append-per-chunk.md)).

**`full_refresh` commits once, terminally.** However many chunks the load
splits into, nothing becomes visible until the single commit at the end, and a
failure before it leaves the destination dataset untouched. A DuckDB full
refresh of the same five records under `chunk_rows: 2` reports three committed
chunks and one atomic write:

```json
{
  "destination_write": {
    "atomicity": "atomic",
    "strategy": "transactional_replace"
  },
  "execution": {
    "record_format": "arrow_record_batch",
    "batch_count": 3,
    "chunk_rows": 2,
    "parallelism": 1,
    "connector_parallelism_limit": 1,
    "retry": {
      "max_attempts": 3,
      "initial_delay_ms": 200,
      "max_delay_ms": 5000,
      "attempts": []
    }
  }
}
```

A full refresh that fails before that commit reports `batch_count: 0` and
`atomicity: not_applicable` — the posture the second load of
[examples/pinned-schema-fail-on-drift](../../examples/pinned-schema-fail-on-drift/)
shows.

**`append` commits per chunk.** The three part files the first example wrote
are three commits, and a failure after `k` of them leaves exactly that prefix
visible — `row_counts.written` and `batch_count` state how much, and
`destination_write` reports `best_effort` rather than pretending nothing
happened.

| Destination | Load mode | `atomicity` | `strategy` |
| --- | --- | --- | --- |
| `duckdb` | `full_refresh` | `atomic` | `transactional_replace` |
| `duckdb` | `append` | `best_effort` | `insert` |
| `parquet` | `full_refresh` | `best_effort` | `staging_then_replace` |
| `parquet` | `append` | `best_effort` | `staged_part_append` |

Those four are the contract, stated in full — including what each value means
and what a failure reports — under
[`destination_write`](../reference/load-report.md#destination_write).

The practical consequence: a `full_refresh` that fails costs you nothing but
time, while a failed `append` leaves a committed prefix that loading the same
source again would duplicate. Read `batch_count` and `row_counts.written`
from the failed load's report before re-loading an append.

## `retry`: what happens to a failed write

The retry policy has three knobs, all optional
([ADR-0049](../adr/0049-configure-retry-as-per-unit-attempts-with-fixed-exponential-backoff.md),
and documented as a contract under
[`execution.retry`](../reference/load-definition.md#executionretry)):

| Key | Default | Meaning |
| --- | --- | --- |
| `max_attempts` | `3` | Total attempts per retry unit, **the first included**. `1` disables retry. |
| `initial_delay_ms` | `200` | The wait before the second attempt. |
| `max_delay_ms` | `5000` | The ceiling on every wait. |

Backoff is fixed exponential ×2 with that ceiling and no jitter: the wait
before attempt `n` is `min(initial_delay_ms × 2^(n-2), max_delay_ms)`. The
example's policy — five attempts from 100 ms, capped at 1000 ms — therefore
waits 100 ms, 200 ms, 400 ms, and 800 ms between its five attempts.

The budget is **per retry unit**, and a load has two kinds: opening the
destination session, and writing one chunk. Each gets its own `max_attempts`,
so a five-chunk load may spend attempts on chunk 3 without touching chunk 4's
budget.

What gets retried is deliberately narrow. Only a failure the connector that
raised it classifies as **transient** is re-attempted, and a connector may
classify one that way only when the failed attempt provably committed nothing
and the session can accept the same unit again; any uncertainty is terminal
([ADR-0048](../adr/0048-classify-transience-at-the-originator-and-retry-only-provably-uncommitted-units.md)).
The terminal commit itself is never retried by the engine.

**No shipped connector classifies any failure as transient**, so on today's
local CSV, JSONL, DuckDB, and Parquet loads the policy changes nothing: a
local disk that refuses a write once refuses it again. Configure it anyway if
you want to — the report echoes the policy with its resolved defaults on every
load that reached the write phase, and `attempts` stays the empty array until
a connector arrives whose failures are genuinely temporary
([ADR-0050](../adr/0050-report-retry-as-a-policy-echo-with-per-failed-attempt-entries.md)).

## `parallelism`: one number, clamped by the destination

`execution.parallelism` bounds how many chunk writes a load may have in
flight at once ([ADR-0051](../adr/0051-dispatch-chunk-writes-through-a-bounded-window-capped-by-per-mode-connector-limits.md)).
Reading the source stays strictly sequential; only destination writes run
concurrently.

Each destination connector declares a **connector parallelism limit** per load
mode, and that limit is both the default when a definition configures nothing
and a hard cap on what a definition may ask for. Configuring more is not an
error — the effective value is `min(configured, limit)`, so one definition
stays valid against destinations with different limits
([ADR-0052](../adr/0052-configure-parallelism-as-one-scalar-clamped-to-the-connector-limit.md)).

That is why the example reports `parallelism: 1` beside
`connector_parallelism_limit: 1` while its definition asks for `4`: both
shipped destinations declare `1` for every load mode
([ADR-0023](../adr/0023-conservative-parallelism-defaults.md)), so every load
ships serial today and the report shows both numbers so the clamp needs no
explaining ([ADR-0053](../adr/0053-report-parallelism-as-the-effective-value-beside-the-connector-limit.md)).

Two things to keep in mind before that changes. Peak memory scales with
`parallelism × chunk_rows`, and nothing validates the product for you — it is
the definition author's to own. And a load's report stays deterministic at any
parallelism: the retry attempts are ordered by unit rather than by wall clock,
and a halted load surfaces the failure of the lowest chunk index, not of
whichever slot lost the race.

## What the load report says

All of it lands in the report's `execution` object
([reference](../reference/load-report.md#execution)):

| Key | What to read it for |
| --- | --- |
| `record_format` | `arrow_record_batch` once the write phase started, `not_started` when the load failed before it. |
| `batch_count` | Chunks the destination committed — every chunk on a success, the committed prefix on a failed append, `0` for a full refresh that failed before its commit. |
| `chunk_rows` | The effective bound, default included. |
| `parallelism` | What actually ran, after the clamp. |
| `connector_parallelism_limit` | The destination's limit for this load mode. |
| `retry` | The policy echo, plus one entry per failed attempt. |

The `not_started` posture omits `chunk_rows`, `parallelism`, and the limit
rather than reporting values that never took effect, so a report with no
`chunk_rows` is a load that never reached its write phase.

## Where to look next

- [Load Definition Reference: `execution`](../reference/load-definition.md#execution)
  — every key, its default, and its validation.
- [Load Report Reference: `execution`](../reference/load-report.md#execution)
  — both postures, and the retry attempt entry shape.
- [Load Report Reference: `destination_write`](../reference/load-report.md#destination_write)
  — what each destination can promise about visibility.
- [Rejected records](rejected-records.md) — the other gate in front of the
  write phase.
