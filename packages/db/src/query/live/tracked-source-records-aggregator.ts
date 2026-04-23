import { serializeValue } from '@tanstack/db-ivm'
import { RefCountedKeyedSet } from '../../collection/ref-counted-keyed-set.js'
import type { Collection } from '../../collection/index.js'
import type {
	SubscribeTrackedSourceRecordsOptions,
	TrackedSourceRecord,
	TrackedSourceRecordsChange,
} from '../../types.js'

/**
 * Per-live-query aggregator for tracked source records.
 *
 * Lives on a single sync session — dies with it. Refcounts over aliases
 * within one query (a self-join references the same base collection under
 * multiple aliases, so the same (collectionId, key) pair can be added
 * multiple times).
 *
 * `exposed` controls whether state transitions are visible to external
 * listeners and propagated to source-collection managers. A live query
 * only exposes its tracked records while it has subscribers; flipping
 * `exposed` emits the current snapshot as added/removed so downstream
 * views stay consistent.
 */
export class LiveQueryTrackedSourceRecordsAggregator {
	private readonly set = new RefCountedKeyedSet<TrackedSourceRecord>((record) =>
		serializeValue([record.collectionId, record.key]),
	)
	private readonly listeners = new Set<
		(change: TrackedSourceRecordsChange) => void
	>()
	private exposed = false

	constructor(
		private readonly sourceCollections: Record<
			string,
			Collection<any, any, any>
		>,
	) {}

	/**
	 * Record a membership change from one `CollectionSubscriber`. Keys are
	 * raw; the caller supplies the source collectionId.
	 */
	apply(
		collectionId: string,
		added: Iterable<string | number>,
		removed: Iterable<string | number>,
	): void {
		const addedRecords = Array.from(added, (key) => ({ collectionId, key }))
		const removedRecords = Array.from(removed, (key) => ({
			collectionId,
			key,
		}))
		const net = this.set.apply(addedRecords, removedRecords)

		if (!this.exposed) return
		this.propagate(net.added, net.removed)
		this.emit(net)
	}

	setExposed(exposed: boolean): void {
		if (this.exposed === exposed) return
		this.exposed = exposed

		const snapshot = this.set.snapshot()
		if (snapshot.length === 0) return

		const change = exposed
			? { added: snapshot, removed: [] as Array<TrackedSourceRecord> }
			: { added: [] as Array<TrackedSourceRecord>, removed: snapshot }

		this.propagate(change.added, change.removed)
		this.emit(change)
	}

	snapshot(): Array<TrackedSourceRecord> {
		return this.exposed ? this.set.snapshot() : []
	}

	subscribe(
		callback: (change: TrackedSourceRecordsChange) => void,
		options?: SubscribeTrackedSourceRecordsOptions,
	): () => void {
		this.listeners.add(callback)

		if (options?.includeInitialState && this.exposed) {
			const added = this.set.snapshot()
			if (added.length > 0) callback({ added, removed: [] })
		}

		return () => {
			this.listeners.delete(callback)
		}
	}

	private emit(change: TrackedSourceRecordsChange): void {
		if (change.added.length === 0 && change.removed.length === 0) return
		for (const listener of this.listeners) listener(change)
	}

	/**
	 * Forward the net transitions to each source collection's tracked-source
	 * records manager. Groups by collectionId so mixed batches (during a
	 * setExposed snapshot replay) apply per-collection in one pass.
	 */
	private propagate(
		added: Array<TrackedSourceRecord>,
		removed: Array<TrackedSourceRecord>,
	): void {
		if (added.length === 0 && removed.length === 0) return

		const byId = new Map<
			string,
			{ added: Array<string | number>; removed: Array<string | number> }
		>()
		const bucket = (id: string) => {
			let entry = byId.get(id)
			if (!entry) {
				entry = { added: [], removed: [] }
				byId.set(id, entry)
			}
			return entry
		}
		for (const record of added) bucket(record.collectionId).added.push(record.key)
		for (const record of removed) bucket(record.collectionId).removed.push(record.key)

		for (const [collectionId, delta] of byId) {
			const collection = this.sourceCollections[collectionId]
			if (!collection) continue
			collection._trackedSourceRecords.apply(delta.added, delta.removed)
		}
	}
}
