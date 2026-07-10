---
status: accepted
---

# Persist Pinned Schemas as Versioned YAML Files Bootstrapped by the First Load

Data Spark will persist a pinned schema as a YAML file that declares `version: 1` and the dataset's fields (name, type, nullable), stored at the path a load definition names under `schema.pinned_path`. When that file does not exist yet, the first load persists the schema it inferred, so pinning needs no separate authoring step; when it exists, later loads validate observed records against it, and a load explicitly allowed to accept additive nullable drift rewrites the file with the added fields so a field that later disappears again is caught as drift instead of silently matching the older pin. The pinned schema file works like a lockfile: the tool maintains it, git versions it alongside the load definition, and hand edits stay possible because loads validate against the file rather than trust past runs.
