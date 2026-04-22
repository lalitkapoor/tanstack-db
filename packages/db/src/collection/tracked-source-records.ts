import type { Collection } from './index.js'
import type {
  SubscribeTrackedSourceRecordsOptions,
  TrackedSourceRecord,
  TrackedSourceRecordsChange,
} from '../types.js'

type TrackedSourceRecordEntry<TKey extends string | number> = {
  key: TKey
  refCount: number
}

class CollectionTrackedSourceRecordsManager<
  TKey extends string | number = string | number,
> {
  private readonly trackedSourceRecords = new Map<
    string,
    TrackedSourceRecordEntry<TKey>
  >()
  private readonly listeners = new Set<
    (changes: TrackedSourceRecordsChange) => void
  >()

  constructor(private readonly collectionId: string) {}

  getTrackedSourceRecords(): Array<TrackedSourceRecord> {
    return this.getTrackedSourceRecordsSnapshot()
  }

  subscribeTrackedSourceRecords(
    callback: (changes: TrackedSourceRecordsChange) => void,
    options?: SubscribeTrackedSourceRecordsOptions,
  ): () => void {
    this.listeners.add(callback)

    if (options?.includeInitialState) {
      const added = this.getTrackedSourceRecordsSnapshot()

      if (added.length > 0) {
        callback({ added, removed: [] })
      }
    }

    return () => {
      this.listeners.delete(callback)
    }
  }

  applyTrackedSourceRecordChanges(changes: {
    added: Iterable<TKey>
    removed: Iterable<TKey>
  }): void {
    const added: Array<TrackedSourceRecord> = []
    const removed: Array<TrackedSourceRecord> = []

    for (const key of changes.added) {
      const serializedKey = this.serializeKey(key)
      const existing = this.trackedSourceRecords.get(serializedKey)

      if (existing) {
        existing.refCount += 1
        continue
      }

      this.trackedSourceRecords.set(serializedKey, {
        key,
        refCount: 1,
      })
      added.push(this.toTrackedSourceRecord(key))
    }

    for (const key of changes.removed) {
      const serializedKey = this.serializeKey(key)
      const existing = this.trackedSourceRecords.get(serializedKey)

      if (!existing) {
        continue
      }

      if (existing.refCount > 1) {
        existing.refCount -= 1
        continue
      }

      this.trackedSourceRecords.delete(serializedKey)
      removed.push(this.toTrackedSourceRecord(existing.key))
    }

    this.emitChanges({ added, removed })
  }

  private emitChanges(changes: TrackedSourceRecordsChange): void {
    if (changes.added.length === 0 && changes.removed.length === 0) {
      return
    }

    this.listeners.forEach((listener) => {
      listener(changes)
    })
  }

  private getTrackedSourceRecordsSnapshot(): Array<TrackedSourceRecord> {
    return Array.from(this.trackedSourceRecords.values(), ({ key }) =>
      this.toTrackedSourceRecord(key),
    )
  }

  private toTrackedSourceRecord(key: TKey): TrackedSourceRecord {
    return {
      collectionId: this.collectionId,
      key,
    }
  }

  private serializeKey(key: TKey): string {
    return JSON.stringify(key)
  }
}

const collectionTrackedSourceRecordsManagers = new WeakMap<
  Collection<any, any, any, any, any>,
  CollectionTrackedSourceRecordsManager<any>
>()

function getOrCreateCollectionTrackedSourceRecordsManager<
  TKey extends string | number,
>(collection: Collection<any, TKey, any, any, any>) {
  let manager = collectionTrackedSourceRecordsManagers.get(collection)

  if (!manager) {
    manager = new CollectionTrackedSourceRecordsManager<TKey>(collection.id)
    collectionTrackedSourceRecordsManagers.set(collection, manager)
  }

  return manager as CollectionTrackedSourceRecordsManager<TKey>
}

export function getTrackedSourceRecords<TKey extends string | number>(
  collection: Collection<any, TKey, any, any, any>,
): Array<TrackedSourceRecord> {
  return getOrCreateCollectionTrackedSourceRecordsManager(
    collection,
  ).getTrackedSourceRecords()
}

export function subscribeTrackedSourceRecords<TKey extends string | number>(
  collection: Collection<any, TKey, any, any, any>,
  callback: (changes: TrackedSourceRecordsChange) => void,
  options?: SubscribeTrackedSourceRecordsOptions,
): () => void {
  return getOrCreateCollectionTrackedSourceRecordsManager(
    collection,
  ).subscribeTrackedSourceRecords(callback, options)
}

export function applyTrackedSourceRecordChanges<TKey extends string | number>(
  collection: Collection<any, TKey, any, any, any>,
  changes: {
    added: Iterable<TKey>
    removed: Iterable<TKey>
  },
): void {
  getOrCreateCollectionTrackedSourceRecordsManager(
    collection,
  ).applyTrackedSourceRecordChanges(changes)
}
