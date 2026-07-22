---
status: accepted
---

# License the Project Under MIT OR Apache-2.0

Data Spark is dual-licensed under **MIT OR Apache-2.0**, with Victor Chu as
the copyright holder. The full texts live at the repository root as
`LICENSE-MIT` and `LICENSE-APACHE` — the filenames GitHub's license detection
recognizes for a dual grant — and `Cargo.toml` declares the grant as the SPDX
expression `license = "MIT OR Apache-2.0"` alongside the `description` and
`repository` fields that describe the package to cargo tooling. Until now the
repository had no license file at all, so the source and the release binaries
published since the first tagged release were all-rights-reserved by default;
an explicit grant
is what makes using, copying, and redistributing them legal for anyone other
than the maintainer.

The disjunctive `OR` is the Rust ecosystem norm: downstream users pick
whichever license suits them, MIT keeps the work compatible with the broadest
set of licenses including GPLv2, and Apache-2.0 adds an explicit patent grant
and contribution terms for users who need them. Contributions follow the usual
inbound-equals-outbound convention — submitted work is accepted under the same
dual license. Release automation is untouched: the next tag simply packages a
tree that carries the license files, and the `Cargo.toml` metadata rides along
with no workflow change.

Alternatives rejected: single MIT (no explicit patent grant, a gap the dual
grant closes at no cost); single Apache-2.0 (incompatible with GPLv2-only
downstreams that MIT accommodates); a source-available license (the project is
meant to be openly usable, and its dependency tree is permissively licensed
open source); `license-file` pointing at a single custom text (the SPDX
expression is machine-readable by cargo, crates.io, and license scanners,
while `license-file` is an escape hatch for nonstandard terms this project
does not have).
