---
status: accepted
---

# Guarantee Secret-Free Load Reports by Contract Shape

No versioned YAML contract in this repository — the load definition today, any future contract file — may define a key that carries a secret value. Secrets enter configuration only as references: today, the name of an environment variable (`password_env`, ADR-0060), which is a credential reference in the glossary's sense and safe to display anywhere. Because the contracts cannot hold a secret, every echo surface — `destination_summary`, `source_summary`, and any report field that reproduces definition content — stays a verbatim echo with no redaction layer: the load report's "no secret is ever echoed" guarantee is structural, achieved by the shape of the contract rather than by runtime filtering.

The alternative — accept secret-bearing keys and redact them on output — is a denylist, and a denylist fails open: it must enumerate every secret-bearing key and every surface (reports, error messages, logs, future artifact paths, future debug output), and the one surface or key it misses becomes a leak discovered in production. A shape guarantee fails closed: adding an inline `password:`-style key is a contract-design violation a reviewer can see in the diff, not a leak an operator finds in a report. The cost is real but intended — a user must place the secret in an environment variable rather than paste it into YAML — and it is exactly the cost ADR-0011 committed to when it kept credentials out of load definitions. The rule applies repo-wide, beyond the `sqlserver` connector that occasioned it.
