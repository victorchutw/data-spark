---
status: accepted
---

# Use GitHub Release Binaries as the Primary Distribution

Data Spark v1 will be distributed primarily as single-file native binaries published through GitHub Releases for Linux x86_64/aarch64 and macOS x86_64/aarch64, with Windows treated as beta. `cargo install` may be supported for Rust users, but it is not the primary installation path because the product goal is a portable binary that can be downloaded and used without requiring a Rust toolchain.
