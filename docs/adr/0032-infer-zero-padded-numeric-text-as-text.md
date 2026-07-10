---
status: accepted
---

# Infer Zero-Padded Numeric Text as Text

Data Spark's default text-type inference will observe a value whose integer part is zero-padded — after an optional leading `+`/`-` sign, a `0` immediately followed by another digit, as in `007`, `0042`, or `007.5` — as text, even though it parses numerically (#22). Coercing such text to a number silently and irreversibly drops the leading zeros (`00501` loads as `501`) on the very first load, before any pinned schema exists, corrupting identifier-like columns such as zip codes, zero-padded IDs, and account numbers; the reverse cost — a genuine numeric column that happens to contain a zero-padded value widening to text — is lossless and recoverable downstream. The harm is asymmetric, so the default favors text. This refines the infer-schemas-by-default baseline of ADR-0006 in the same text-favoring direction as ADR-0031, which restricted the same observation rule: a numeric reading that would lose information is not taken. It also aligns CSV/text inference with JSONL, where a JSON string `"007"` already stays text by design. Unpadded values keep their numeric reading — `0`, `-0`, and `42` still observe Int64, and `0.5` and `1e10` still observe Float64. Forcing numeric typing onto a zero-padded column remains the job of schema override and pinning, the escape hatch ADR-0006 designates (#7).

Accepted consequences:

- A column mixing zero-padded and plain numeric text (such as `007` and `1234`) disagrees under the merge lattice and falls to text, so every value in it round-trips as text unchanged.
- A column can legitimately flip int64 → utf8 between loads when zero-padded values first appear; pinned schemas and drift policy (#7) are the mechanism that will make such flips loud and controllable.
- A genuine integer column containing a zero-padded value loses numeric typing by default; loading it as Int64 requires a schema override or pinned schema (ADR-0006, #7), and the coercion semantics of forcing int64 over text like `007` land with #7.
