---
status: accepted
---

# Use YAML Load Definitions as the Canonical Repeatable Interface

Data Spark will use YAML load definitions as the canonical interface for repeatable loads, while still supporting command-line flags for one-off loads. CLI flags are an on-ramp rather than a second full configuration language: complex behavior belongs in YAML, credentials are not accepted directly in flags, and exploratory commands may generate a YAML skeleton so they can graduate into versioned load definitions without requiring users to rewrite the task by hand.
