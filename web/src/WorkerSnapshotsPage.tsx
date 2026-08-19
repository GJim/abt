import { useCallback, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import './Console.css'

export type SnapshotSummary = {
  snapshot_id: string
  server: string | null
  login: string | number | null
  balance: number | string | null
  equity: number | string | null
  trade_allowed: boolean | null
  trade_expert: boolean | null
  tradeapi_disabled: boolean | null
  timestamp: string
}

type SnapshotResponse = {
  items: SnapshotSummary[]
  next_cursor: string | null
}

type SnapshotRequest = {
  cursor: string
  append: boolean
}

export function WorkerSnapshotsPage() {
  const [search, setSearch] = useState('')
  const [appliedSearch, setAppliedSearch] = useState('')
  const [snapshots, setSnapshots] = useState<SnapshotSummary[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [rawSnapshots, setRawSnapshots] = useState<Record<string, unknown>>({})
  const [detailError, setDetailError] = useState<Record<string, string>>({})
  const [loadingDetailId, setLoadingDetailId] = useState<string | null>(null)
  const listAbortControllerRef = useRef<AbortController | null>(null)
  const detailAbortControllerRef = useRef<AbortController | null>(null)
  const failedRequestRef = useRef<SnapshotRequest>({ cursor: '', append: false })

  const loadSnapshots = useCallback(async (request: SnapshotRequest, query: string) => {
    listAbortControllerRef.current?.abort()
    const controller = new AbortController()
    listAbortControllerRef.current = controller
    failedRequestRef.current = request
    setError(null)

    if (request.append) {
      setIsLoadingMore(true)
    } else {
      setIsLoading(true)
    }

    const parameters = new URLSearchParams({
      limit: '50',
      q: query,
      cursor: request.cursor,
    })

    try {
      const response = await fetch(`/api/admin/worker-snapshots?${parameters}`, {
        credentials: 'same-origin',
        signal: controller.signal,
      })
      if (!response.ok) {
        throw new Error(`Worker snapshots could not be loaded (${response.status}).`)
      }

      const payload = await response.json() as SnapshotResponse
      if (!Array.isArray(payload.items) || (payload.next_cursor !== null && typeof payload.next_cursor !== 'string')) {
        throw new Error('Worker snapshots returned an invalid response.')
      }

      setSnapshots((current) => request.append ? [...current, ...payload.items] : payload.items)
      setNextCursor(payload.next_cursor)
    } catch (caughtError) {
      if (controller.signal.aborted) {
        return
      }
      setError(caughtError instanceof Error ? caughtError.message : 'Worker snapshots could not be loaded.')
    } finally {
      if (listAbortControllerRef.current === controller) {
        setIsLoading(false)
        setIsLoadingMore(false)
      }
    }
  }, [])

  useEffect(() => {
    void loadSnapshots({ cursor: '', append: false }, appliedSearch)
    return () => {
      listAbortControllerRef.current?.abort()
      detailAbortControllerRef.current?.abort()
    }
  }, [appliedSearch, loadSnapshots])

  function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setAppliedSearch(search.trim())
  }

  function retry() {
    void loadSnapshots(failedRequestRef.current, appliedSearch)
  }

  async function loadRawSnapshot(snapshotId: SnapshotSummary['snapshot_id']) {
    detailAbortControllerRef.current?.abort()
    const controller = new AbortController()
    detailAbortControllerRef.current = controller
    setLoadingDetailId(snapshotId)
    setDetailError((current) => ({ ...current, [snapshotId]: '' }))

    try {
      const response = await fetch(`/api/admin/worker-snapshots/${encodeURIComponent(snapshotId)}`, {
        credentials: 'same-origin',
        signal: controller.signal,
      })
      if (!response.ok) {
        throw new Error(`Snapshot details could not be loaded (${response.status}).`)
      }

      const payload = await response.json() as unknown
      setRawSnapshots((current) => ({ ...current, [snapshotId]: payload }))
    } catch (caughtError) {
      if (!controller.signal.aborted) {
        setDetailError((current) => ({
          ...current,
          [snapshotId]: caughtError instanceof Error ? caughtError.message : 'Snapshot details could not be loaded.',
        }))
      }
    } finally {
      if (detailAbortControllerRef.current === controller) {
        setLoadingDetailId(null)
      }
    }
  }

  return (
    <section aria-labelledby="worker-snapshots-heading">
      <header className="console-page-header">
        <div>
          <h1 id="worker-snapshots-heading">Worker snapshots</h1>
          <p>Review the latest account state reported by each worker.</p>
        </div>
      </header>

      <form className="console-table-actions" role="search" onSubmit={submitSearch}>
        <label htmlFor="worker-snapshots-search">Search worker snapshots</label>
        <input
          id="worker-snapshots-search"
          name="q"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Server or login"
        />
        <button type="submit" disabled={isLoading || isLoadingMore}>Search</button>
      </form>

      {isLoading && snapshots.length === 0 ? (
        <p className="console-empty-state" role="status">Loading worker snapshots…</p>
      ) : null}
      {isLoading && snapshots.length > 0 ? (
        <p className="console-table-actions" role="status">Updating worker snapshots…</p>
      ) : null}

      {error ? (
        <div className="console-empty-state" role="alert">
          <strong>Worker snapshots are unavailable.</strong>
          <span>{error}</span>
          <button type="button" onClick={retry}>Try again</button>
        </div>
      ) : null}

      {!isLoading && !error && snapshots.length === 0 ? (
        <div className="console-empty-state">
          <strong>No worker snapshots found.</strong>
          <span>{appliedSearch ? 'Try a different search term.' : 'New worker reports will appear here.'}</span>
        </div>
      ) : null}

      {snapshots.length > 0 ? (
        <>
          <div className="console-table-scroll">
            <table className="console-table">
              <caption className="visually-hidden">Worker snapshots</caption>
              <thead>
                <tr>
                  <th scope="col" title="Trading server">Server</th>
                  <th scope="col" title="MT5 account login">Login</th>
                  <th scope="col" title="Account balance">Bal.</th>
                  <th scope="col" title="Account equity">Eq.</th>
                  <th scope="col" title="Trading allowed">Allowed</th>
                  <th scope="col" title="Expert trading enabled">Expert</th>
                  <th scope="col" title="Trade API disabled">API off</th>
                  <th scope="col" title="Snapshot timestamp">Time</th>
                </tr>
              </thead>
              <tbody>
                {snapshots.map((snapshot) => {
                  const snapshotId = snapshot.snapshot_id
                  const rawSnapshot = rawSnapshots[snapshotId]
                  const rawError = detailError[snapshotId]
                  const isLoadingDetail = loadingDetailId === snapshotId

                  return (
                    <tr key={snapshotId}>
                      <td>{displayValue(snapshot.server)}</td>
                      <td>{displayValue(snapshot.login)}</td>
                      <td>{displayNumber(snapshot.balance)}</td>
                      <td>{displayNumber(snapshot.equity)}</td>
                      <td><BooleanTag value={snapshot.trade_allowed} /></td>
                      <td><BooleanTag value={snapshot.trade_expert} /></td>
                      <td><BooleanTag value={snapshot.tradeapi_disabled} /></td>
                      <td>
                        <time dateTime={snapshot.timestamp}>{formatDateTime(snapshot.timestamp)}</time>
                        {rawSnapshot === undefined ? (
                          <div>
                            <button
                              type="button"
                              onClick={() => void loadRawSnapshot(snapshot.snapshot_id)}
                              disabled={isLoadingDetail}
                            >
                              {isLoadingDetail ? 'Loading raw JSON…' : 'View raw JSON'}
                            </button>
                            {rawError ? <span role="alert">{rawError}</span> : null}
                          </div>
                        ) : (
                          <details open>
                            <summary>Raw JSON</summary>
                            <pre className="console-raw-detail">{JSON.stringify(rawSnapshot, null, 2)}</pre>
                          </details>
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {nextCursor ? (
            <div className="console-table-actions">
              <button
                type="button"
                onClick={() => void loadSnapshots({ cursor: nextCursor, append: true }, appliedSearch)}
                disabled={isLoadingMore}
              >
                {isLoadingMore ? 'Loading more snapshots…' : 'Load more'}
              </button>
              {isLoadingMore ? <span role="status">Loading more worker snapshots…</span> : null}
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  )
}

function BooleanTag({ value }: { value: boolean | null }) {
  const state = value === null ? 'unknown' : value ? 'healthy' : 'error'
  return <span className="console-status" data-state={state}>{value === null ? 'Unknown' : value ? 'Yes' : 'No'}</span>
}

function displayValue(value: string | number | null) {
  return value === null || value === '' ? '—' : value
}

function displayNumber(value: number | string | null) {
  if (value === null || value === '') {
    return '—'
  }

  const number = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(number) ? number.toLocaleString(undefined, { maximumFractionDigits: 2 }) : value
}

function formatDateTime(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

export default WorkerSnapshotsPage
