---
status: accepted
---

# Use YAML Load Definitions as the Canonical Repeatable Interface

Data Spark will use YAML load definitions as the canonical interface for repeatable loads, while still supporting command-line flags for one-off loads. CLI flags may generate a YAML skeleton so exploratory commands can graduate into versioned load definitions without requiring users to rewrite the task by hand.
