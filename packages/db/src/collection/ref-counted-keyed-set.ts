export type RefCountedKeyedSetChange<T> = {
	added: Array<T>
	removed: Array<T>
}

type Entry<T> = {
	value: T
	refCount: number
}

/**
 * A set with reference counting. Tracks how many times each element has been
 * added; emits deltas only on 0↔1 transitions.
 *
 * Used as the shared primitive behind both the per-base-collection
 * `TrackedSourceRecordsManager` (refcounts over live queries that depend on
 * the collection) and the per-live-query
 * `LiveQueryTrackedSourceRecordsAggregator` (refcounts over aliases within
 * one query, e.g. self-joins).
 */
export class RefCountedKeyedSet<T> {
	private readonly entries = new Map<string, Entry<T>>()
	private readonly listeners = new Set<
		(change: RefCountedKeyedSetChange<T>) => void
	>()

	constructor(private readonly serialize: (value: T) => string) {}

	/**
	 * Apply a batch of adds/removes. Returns the net 0↔1 transitions. Also
	 * emits the same delta to listeners when non-empty.
	 */
	apply(
		added: Iterable<T>,
		removed: Iterable<T>,
	): RefCountedKeyedSetChange<T> {
		const netAdded: Array<T> = []
		const netRemoved: Array<T> = []

		for (const value of added) {
			const key = this.serialize(value)
			const existing = this.entries.get(key)
			if (existing) {
				existing.refCount++
			} else {
				this.entries.set(key, { value, refCount: 1 })
				netAdded.push(value)
			}
		}

		for (const value of removed) {
			const key = this.serialize(value)
			const existing = this.entries.get(key)
			if (!existing) continue

			if (existing.refCount === 1) {
				this.entries.delete(key)
				netRemoved.push(existing.value)
			} else {
				existing.refCount--
			}
		}

		if (netAdded.length > 0 || netRemoved.length > 0) {
			const change = { added: netAdded, removed: netRemoved }
			for (const listener of this.listeners) listener(change)
		}

		return { added: netAdded, removed: netRemoved }
	}

	snapshot(): Array<T> {
		return Array.from(this.entries.values(), (entry) => entry.value)
	}

	get size(): number {
		return this.entries.size
	}

	subscribe(
		listener: (change: RefCountedKeyedSetChange<T>) => void,
		options?: { includeInitialState?: boolean },
	): () => void {
		this.listeners.add(listener)

		if (options?.includeInitialState && this.entries.size > 0) {
			listener({ added: this.snapshot(), removed: [] })
		}

		return () => {
			this.listeners.delete(listener)
		}
	}
}
