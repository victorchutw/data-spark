---
status: accepted
---

# Add an Internal Library Target for Testing

Data Spark will split `src/main.rs` into a library target (`src/lib.rs`) holding all load logic plus a thin `src/main.rs` binary entry that only calls `data_spark::run()`. The library exists to give in-process unit tests a conventional home and to keep a clean boundary between the binary entry and the load logic; its public surface is deliberately a single `run()` function, with all other items private. This library API is internal and is not a public, semver-stable interface — Data Spark remains a CLI product distributed as a single binary (ADR-0001, ADR-0025), and the library target compiles into that same binary rather than becoming a separately consumed dependency. We did not expose the core as a public in-process API — the alternative that would let external code or `tests/` call the load logic directly — to avoid taking on a library compatibility commitment before v1's data movement contract is settled.
