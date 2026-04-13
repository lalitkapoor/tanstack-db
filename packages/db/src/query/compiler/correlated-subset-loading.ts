import { inArray } from '../builder/functions.js'
import type { PropRef } from '../ir.js'
import type { CollectionSubscription } from '../../collection/subscription.js'

export function collectDistinctNonNullKeys<TEntry, TKey>(
  entries: Array<[TEntry, number]>,
  getKey: (entry: TEntry) => TKey | null | undefined,
): Array<TKey> {
  const keys = new Set<TKey>()

  for (const [entry] of entries) {
    const key = getKey(entry)
    if (key != null) {
      keys.add(key)
    }
  }

  return [...keys]
}

export function requestCorrelatedSubsetSnapshot(
  subscription: CollectionSubscription,
  targetRef: PropRef,
  correlationKeys: Array<unknown>,
  onOptimizedFailure: () => void,
): void {
  if (correlationKeys.length === 0) {
    return
  }

  const loaded = subscription.requestSnapshot({
    where: inArray(targetRef, correlationKeys),
    optimizedOnly: true,
  })

  if (!loaded) {
    onOptimizedFailure()
  }
}
