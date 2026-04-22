---
'@tanstack/db': minor
---

Expose tracked source record subscriptions on all collections.

Base collections now expose `utils.getTrackedSourceRecords()` and
`utils.subscribeTrackedSourceRecords()`, aggregating tracked source-record
membership across active live queries that depend on them. Live query
collections keep their existing per-query tracked-source behavior.
