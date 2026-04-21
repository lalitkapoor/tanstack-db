---
"@tanstack/db": minor
"@tanstack/db-sqlite-persistence-core": minor
---

Allow `collection.createIndex(...)` to declare composite indexes by returning an array of expressions. Runtime collection indexing continues to use the first expression, while persisted SQLite bootstrap now materializes all declared expressions as a composite backend index.
