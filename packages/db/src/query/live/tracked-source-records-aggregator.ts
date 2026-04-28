import type { Collection } from '../../collection/index.js'

type Entry = { refCount: number }

/**
 * Per-live-query aggregator for tracked source records.
 *
 * Lives on a single sync session — dies with it. Refcounts over aliases
 * within one query (a self-join references the same base collection under
 * multiple aliases, so the same (collectionId, key) pair can be added
 * multiple times). Net 0↔1 transitions are propagated to each source
 * collection's `_trackedSourceRecords` manager, where end users observe
 * them via `collection.subscribeTrackedSourceRecords`.
 *
 * `exposed` gates propagation: only push to source collections while the
 * live query has active subscribers. Flipping `exposed` replays the
 * current snapshot as added/removed so the source-collection view stays
 * consistent.
 */
export class LiveQueryTrackedSourceRecordsAggregator {
  // Nested map avoids serializing (collectionId, key) composites. Outer key
  // is collectionId; inner key is the source record's key (primitive).
  private readonly entries = new Map<string, Map<string | number, Entry>>()
  private exposed = false

  constructor(
    private readonly sourceCollections: Record<
      string,
      Collection<any, any, any>
    >,
  ) {}

  /**
   * Record a membership change from one `CollectionSubscriber`. All keys in
   * a single call share the same collectionId, so propagation to the source
   * collection's manager is a direct call — no grouping needed.
   */
  apply(
    collectionId: string,
    added: Iterable<string | number>,
    removed: Iterable<string | number>,
  ): void {
    let byKey = this.entries.get(collectionId)
    if (!byKey) {
      byKey = new Map()
      this.entries.set(collectionId, byKey)
    }

    const netAdded: Array<string | number> = []
    const netRemoved: Array<string | number> = []

    for (const key of added) {
      const existing = byKey.get(key)
      if (existing) {
        existing.refCount++
      } else {
        byKey.set(key, { refCount: 1 })
        netAdded.push(key)
      }
    }

    for (const key of removed) {
      const existing = byKey.get(key)
      if (!existing) continue
      if (existing.refCount === 1) {
        byKey.delete(key)
        netRemoved.push(key)
      } else {
        existing.refCount--
      }
    }

    // Drop an emptied bucket so `entries.size === 0` correctly reflects
    // "nothing tracked" for setExposed.
    if (byKey.size === 0) {
      this.entries.delete(collectionId)
    }

    if (!this.exposed) return
    if (netAdded.length === 0 && netRemoved.length === 0) return

    this.sourceCollections[collectionId]?._trackedSourceRecords.apply(
      netAdded,
      netRemoved,
    )
  }

  setExposed(exposed: boolean): void {
    if (this.exposed === exposed) return
    this.exposed = exposed
    if (this.entries.size === 0) return

    for (const [collectionId, byKey] of this.entries) {
      const keys = Array.from(byKey.keys())
      const collection = this.sourceCollections[collectionId]
      if (exposed) {
        collection?._trackedSourceRecords.apply(keys, [])
      } else {
        collection?._trackedSourceRecords.apply([], keys)
      }
    }
  }
}
