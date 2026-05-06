import { describe, expect, it } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import { createCollection } from '@tanstack/db'
import { useCollectionRecord } from '../src/useCollectionRecord'
import { mockSyncCollectionOptions } from '../../db/tests/utils'

type Person = {
  id: string
  name: string
  age: number
}

const initialPersons: Array<Person> = [
  {
    id: `1`,
    name: `John Doe`,
    age: 30,
  },
  {
    id: `2`,
    name: `Jane Doe`,
    age: 25,
  },
  {
    id: `3`,
    name: `John Smith`,
    age: 35,
  },
]

function createPersonsCollection() {
  return createCollection(
    mockSyncCollectionOptions<Person>({
      id: `test-persons`,
      getKey: (person: Person) => person.id,
      initialData: initialPersons,
    }),
  )
}

describe(`useCollectionRecord`, () => {
  it(`returns the current record for a collection key`, async () => {
    const collection = createPersonsCollection()

    const { result } = renderHook(() =>
      useCollectionRecord(collection, `2`),
    )

    await waitFor(() => {
      expect(result.current.data?.id).toBe(`2`)
    })

    expect(result.current.data?.name).toBe(`Jane Doe`)
    expect(result.current.collection).toBe(collection)
    expect(result.current.key).toBe(`2`)
    expect(result.current.isReady).toBe(true)
  })

  it(`does not rerender when a different key changes`, async () => {
    const collection = createPersonsCollection()
    let renderCount = 0

    const { result } = renderHook(() => {
      renderCount += 1
      return useCollectionRecord(collection, `2`)
    })

    await waitFor(() => {
      expect(result.current.data?.name).toBe(`Jane Doe`)
    })

    const renderCountAfterInitialLoad = renderCount

    act(() => {
      collection.update(`1`, (draft) => {
        draft.name = `Changed John`
      })
    })

    expect(result.current.data?.name).toBe(`Jane Doe`)
    expect(renderCount).toBe(renderCountAfterInitialLoad)

    act(() => {
      collection.update(`2`, (draft) => {
        draft.name = `Changed Jane`
      })
    })

    await waitFor(() => {
      expect(result.current.data?.name).toBe(`Changed Jane`)
    })

    expect(renderCount).toBeGreaterThan(renderCountAfterInitialLoad)
  })

  it(`resubscribes when the key changes`, async () => {
    const collection = createPersonsCollection()
    let renderCount = 0

    const { result, rerender } = renderHook(
      ({ personId }: { personId: string }) => {
        renderCount += 1
        return useCollectionRecord(collection, personId)
      },
      { initialProps: { personId: `1` } },
    )

    await waitFor(() => {
      expect(result.current.data?.name).toBe(`John Doe`)
    })

    rerender({ personId: `3` })

    await waitFor(() => {
      expect(result.current.data?.name).toBe(`John Smith`)
    })

    const renderCountAfterKeyChange = renderCount

    act(() => {
      collection.update(`1`, (draft) => {
        draft.name = `Changed John`
      })
    })

    expect(result.current.data?.name).toBe(`John Smith`)
    expect(renderCount).toBe(renderCountAfterKeyChange)

    act(() => {
      collection.update(`3`, (draft) => {
        draft.name = `Changed Smith`
      })
    })

    await waitFor(() => {
      expect(result.current.data?.name).toBe(`Changed Smith`)
    })
  })
})
