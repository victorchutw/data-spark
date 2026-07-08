---
status: accepted
---

# Defer Encrypted Connection Profile Storage

Data Spark v1 will support environment variables, secure prompts, and local connection profiles, but local profiles will store non-sensitive settings and credential references rather than secret values. Cross-platform encrypted profile storage is deferred beyond v1 because keychain and secret-service integration would add portability and support burden before the core data movement paths are proven.
