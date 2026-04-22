---
'@tanstack/db': minor
---

Add live query utilities for observing tracked source records.

Live query collections now expose `utils.getTrackedSourceRecords()` and
`utils.subscribeTrackedSourceRecords()` so consumers can observe which source
collection records are currently contributing to an active live query. The API
returns stable `{ collectionId, key }` identities and emits add/remove deltas
as tracked source-record membership changes.
