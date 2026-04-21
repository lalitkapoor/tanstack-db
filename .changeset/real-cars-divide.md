---
"@tanstack/db": minor
"@tanstack/db-sqlite-persistence-core": minor
---

Add collection-level index declarations for persisted and backend indexes, including composite index declarations.

Collections can now call `declareIndex(...)` to register index metadata without creating an in-memory runtime index. Persisted SQLite bootstrap now materializes all declared index expressions instead of assuming a single expression, so composite declarations can create composite SQLite indexes without manual adapter calls.
