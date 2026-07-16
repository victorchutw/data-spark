---
status: accepted
---

# Reject Unknown Fields in Versioned YAML Contracts

Data Spark will parse its versioned YAML contracts strictly: a load definition (ADR-0010, ADR-0026) and a pinned schema file (ADR-0033) reject keys the contract does not declare, recursively through the nested `source`, `destination`, `schema`, and `artifacts` blocks and each pinned-schema field entry, under the existing `invalid_load_definition_yaml` and `invalid_pinned_schema` classifications with error text naming the rejected key. A misspelled key or a future-looking key such as `schema.overrides` or `parallelism` therefore fails the load before any source reading or destination writing, instead of silently running with behavior the author did not intend or making a deferred capability look implemented. A new key enters a contract only through an explicit contract change: the change that implements the capability declares the key in the parser and states, per ADR-0026, whether the declared `version` still covers it — so an older binary that meets a newer definition fails on the key it cannot honor rather than ignoring it.
