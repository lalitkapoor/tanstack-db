import { useCallback, useRef, useSyncExternalStore } from 'react'
import type {
  Collection,
  CollectionStatus,
  UtilsRecord,
} from '@tanstack/db'

type CollectionRecordSnapshot<TRecord extends object> = {
  data: TRecord | undefined
  status: CollectionStatus
  version: number
}

export type UseCollectionRecordResult<
  TRecord extends object,
  TKey extends string | number,
  TUtils extends UtilsRecord = UtilsRecord,
> = {
  data: TRecord | undefined
  collection: Collection<TRecord, TKey, TUtils>
  key: TKey
  status: CollectionStatus
  isLoading: boolean
  isReady: boolean
  isIdle: boolean
  isError: boolean
  isCleanedUp: boolean
}

/**
 * Subscribe to one collection record by key without compiling a live query graph.
 *
 * This is the point-read counterpart to useLiveQuery: useLiveQuery should own
 * list/query/projection semantics, while this hook owns row rendering by key.
 */
export function useCollectionRecord<
  TRecord extends object,
  TKey extends string | number,
  TUtils extends UtilsRecord = UtilsRecord,
>(
  collection: Collection<TRecord, TKey, TUtils>,
  key: TKey,
): UseCollectionRecordResult<TRecord, TKey, TUtils> {
  const versionRef = useRef(0)
  const sourceRef = useRef<{
    collection: Collection<TRecord, TKey, TUtils>
    key: TKey
  } | null>(null)
  const snapshotRef =
    useRef<CollectionRecordSnapshot<TRecord> | null>(null)
  const returnedSnapshotRef =
    useRef<CollectionRecordSnapshot<TRecord> | null>(null)
  const returnedRef =
    useRef<UseCollectionRecordResult<TRecord, TKey, TUtils> | null>(null)

  if (
    sourceRef.current?.collection !== collection ||
    sourceRef.current.key !== key
  ) {
    sourceRef.current = { collection, key }
    snapshotRef.current = null
    returnedSnapshotRef.current = null
    returnedRef.current = null
    versionRef.current += 1
  }

  const subscribe = useCallback(
    (onStoreChange: () => void) => {
      const notify = () => {
        versionRef.current += 1
        onStoreChange()
      }

      const subscription = collection.subscribeKeyChanges(key, notify)
      const unsubscribeStatus = collection.on(`status:change`, notify)

      // Refresh once after subscribing so changes that land between the
      // render-time read and the subscription attach are reflected.
      notify()

      return () => {
        subscription.unsubscribe()
        unsubscribeStatus()
      }
    },
    [collection, key],
  )

  const getSnapshot = useCallback((): CollectionRecordSnapshot<TRecord> => {
    const version = versionRef.current
    const data = collection.get(key)
    const status = collection.status

    if (
      !snapshotRef.current ||
      snapshotRef.current.version !== version ||
      snapshotRef.current.data !== data ||
      snapshotRef.current.status !== status
    ) {
      snapshotRef.current = {
        data,
        status,
        version,
      }
    }

    return snapshotRef.current
  }, [collection, key])

  const snapshot = useSyncExternalStore(subscribe, getSnapshot, getSnapshot)

  if (
    returnedSnapshotRef.current !== snapshot ||
    returnedRef.current === null
  ) {
    returnedRef.current = {
      data: snapshot.data,
      collection,
      key,
      status: snapshot.status,
      isLoading: snapshot.status === `loading`,
      isReady: snapshot.status === `ready`,
      isIdle: snapshot.status === `idle`,
      isError: snapshot.status === `error`,
      isCleanedUp: snapshot.status === `cleaned-up`,
    }
    returnedSnapshotRef.current = snapshot
  }

  return returnedRef.current
}
