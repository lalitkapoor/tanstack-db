import { inArray } from '../builder/functions.js'
import type { PropRef } from '../ir.js'
import type { CollectionSubscription } from '../../collection/subscription.js'

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
