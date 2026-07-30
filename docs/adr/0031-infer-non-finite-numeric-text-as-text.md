---
status: accepted
---

# Infer Non-Finite Numeric Text as Text

Data Spark's default text-type inference will observe a value as Float64 only when it parses to a finite `f64`; text whose parse yields a non-finite float — the `inf` / `infinity` / `nan` spellings Rust accepts in any casing with an optional sign, and magnitude overflow such as `1e400` that saturates to ±infinity — will observe as text instead. Under the existing merge rules, a numeric-looking column containing one such value therefore infers as a text column rather than becoming a Float64 column that silently stores non-finite values, which round-trip poorly through Parquet and warehouse destinations (ADR-0003) and poison BI aggregates such as `SUM` and `AVG`. This keeps default inference aligned with its existing conservative observation rules (exact boolean spellings only, no whitespace trimming, type disagreements fall to text) and with the fail-loud posture of ADR-0020, while ADR-0006's schema overrides and pinning remain the escape hatch for datasets that genuinely carry non-finite floats. Finite parses keep their current behavior: integer-range overflow such as `99999999999999999999999` still observes Float64 (it parses to the finite `1e23`), and subnormal underflow such as `1e-400` parses to the finite `0.0` and stays Float64. JSON-value inference is unaffected because JSON numbers cannot carry non-finite values.

Accepted consequences:

- A column that legitimately carries non-finite floats infers as text by default; loading it as Float64 requires a schema override or pinned schema (ADR-0006).
- Text that saturates `f64` to infinity, such as `1e400`, degrades its column to text instead of silently becoming an infinite float.
- How non-finite text behaves under a pinned or overridden Float64 schema — stored non-finite value versus a Rejected Record per `CONTEXT.md` — was out of scope here; ADR-0035 resolved it for pinned schemas and ADR-0038 for overrides, in both cases as a rejected record rather than a stored non-finite value.
