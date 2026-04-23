import { serializeValue } from '@tanstack/db-ivm'
import { RefCountedKeyedSet } from './ref-counted-keyed-set.js'
import type {
	SubscribeTrackedSourceRecordsOptions,
	TrackedSourceRecord,
	TrackedSourceRecordsChange,
} from '../types.js'

/**
 * Per-base-collection tracked source records manager.
 *
 * Refcounts over active live queries that depend on this collection. Each
 * live query's `LiveQueryTrackedSourceRecordsAggregator` pushes its net
 * alias-level transitions here; this manager dedupes across queries and
 * emits to subscribers only on true 0↔1 transitions.
 *
 * Lives as a real field on `CollectionImpl` — not a WeakMap lookup, not
 * utils-injection.
 */
export class TrackedSourceRecordsManager<
	TKey extends string | number = string | number,
> {
	private readonly set = new RefCountedKeyedSet<TKey>((key) =>
		serializeValue(key),
	)

	constructor(private readonly collectionId: string) {}

	apply(added: Iterable<TKey>, removed: Iterable<TKey>): void {
		this.set.apply(added, removed)
	}

	get(): Array<TrackedSourceRecord> {
		return this.set.snapshot().map((key) => this.toRecord(key))
	}

	subscribe(
		callback: (changes: TrackedSourceRecordsChange) => void,
		options?: SubscribeTrackedSourceRecordsOptions,
	): () => void {
		return this.set.subscribe(
			({ added, removed }) => {
				callback({
					added: added.map((key) => this.toRecord(key)),
					removed: removed.map((key) => this.toRecord(key)),
				})
			},
			options,
		)
	}

	private toRecord(key: TKey): TrackedSourceRecord {
		return { collectionId: this.collectionId, key }
	}
}
