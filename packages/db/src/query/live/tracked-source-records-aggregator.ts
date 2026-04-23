import { serializeValue } from '@tanstack/db-ivm'
import type { Collection } from '../../collection/index.js'
import type {
	TrackedSourceRecord,
	TrackedSourceRecordsChange,
} from '../../types.js'

type Entry = {
	collectionId: string
	key: string | number
	refCount: number
}

/**
 * Per-live-query aggregator for tracked source records.
 *
 * Lives on a single sync session — dies with it. Refcounts over aliases
 * within one query (a self-join references the same base collection under
 * multiple aliases, so the same (collectionId, key) pair can be added
 * multiple times).
 *
 * `exposed` gates whether net 0↔1 transitions are visible to the outside:
 * we only propagate to source collections and call `onChange` while the
 * live query has subscribers. Flipping `exposed` replays the current
 * snapshot as added/removed so downstream views stay consistent.
 */
export class LiveQueryTrackedSourceRecordsAggregator {
	private readonly entries = new Map<string, Entry>()
	private exposed = false

	constructor(
		private readonly sourceCollections: Record<
			string,
			Collection<any, any, any>
		>,
		private readonly onChange: (change: TrackedSourceRecordsChange) => void,
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
		const netAdded: Array<string | number> = []
		const netRemoved: Array<string | number> = []

		for (const key of added) {
			const serialized = serializeValue([collectionId, key])
			const existing = this.entries.get(serialized)
			if (existing) {
				existing.refCount++
			} else {
				this.entries.set(serialized, { collectionId, key, refCount: 1 })
				netAdded.push(key)
			}
		}

		for (const key of removed) {
			const serialized = serializeValue([collectionId, key])
			const existing = this.entries.get(serialized)
			if (!existing) continue
			if (existing.refCount === 1) {
				this.entries.delete(serialized)
				netRemoved.push(existing.key)
			} else {
				existing.refCount--
			}
		}

		if (!this.exposed) return
		if (netAdded.length === 0 && netRemoved.length === 0) return

		this.sourceCollections[collectionId]?._trackedSourceRecords.apply(
			netAdded,
			netRemoved,
		)
		this.onChange({
			added: netAdded.map((key) => ({ collectionId, key })),
			removed: netRemoved.map((key) => ({ collectionId, key })),
		})
	}

	setExposed(exposed: boolean): void {
		if (this.exposed === exposed) return
		this.exposed = exposed
		if (this.entries.size === 0) return

		// Group current entries by source collectionId so each source
		// collection's manager sees its keys in one call.
		const grouped = new Map<string, Array<string | number>>()
		for (const entry of this.entries.values()) {
			let bucket = grouped.get(entry.collectionId)
			if (!bucket) {
				bucket = []
				grouped.set(entry.collectionId, bucket)
			}
			bucket.push(entry.key)
		}

		const added: Array<TrackedSourceRecord> = []
		const removed: Array<TrackedSourceRecord> = []
		for (const [collectionId, keys] of grouped) {
			const collection = this.sourceCollections[collectionId]
			if (exposed) {
				collection?._trackedSourceRecords.apply(keys, [])
				for (const key of keys) added.push({ collectionId, key })
			} else {
				collection?._trackedSourceRecords.apply([], keys)
				for (const key of keys) removed.push({ collectionId, key })
			}
		}

		this.onChange({ added, removed })
	}

	snapshot(): Array<TrackedSourceRecord> {
		if (!this.exposed) return []
		return Array.from(this.entries.values(), ({ collectionId, key }) => ({
			collectionId,
			key,
		}))
	}
}
