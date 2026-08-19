import { useCallback, useEffect, useRef, useState } from 'react'
import { Pagination } from '@astryxdesign/core/Pagination'
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
  pageIndex: number
  query: string
}

const PAGE_SIZE = 50

export function WorkerSnapshotsPage() {
  const [search, setSearch] = useState('')
  const [pages, setPages] = useState<SnapshotResponse[]>([])
  const [pageIndex, setPageIndex] = useState(0)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [rawSnapshots, setRawSnapshots] = useState<Record<string, unknown>>({})
  const [detailError, setDetailError] = useState<Record<string, string>>({})
  const [loadingDetailId, setLoadingDetailId] = useState<string | null>(null)
  const [selectedSnapshotId, setSelectedSnapshotId] = useState<string | null>(null)
  const listAbortControllerRef = useRef<AbortController | null>(null)
  const detailAbortControllerRef = useRef<AbortController | null>(null)
  const failedRequestRef = useRef<SnapshotRequest | null>(null)

  const loadPage = useCallback(async (request: SnapshotRequest) => {
    listAbortControllerRef.current?.abort()
    const controller = new AbortController()
    listAbortControllerRef.current = controller
    failedRequestRef.current = request
    setError(null)
    setIsLoading(true)

    const parameters = new URLSearchParams({ limit: String(PAGE_SIZE) })
    if (request.query) {
      parameters.set('q', request.query)
    }
    if (request.cursor) {
      parameters.set('cursor', request.cursor)
    }

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

      setPages((current) => [...current.slice(0, request.pageIndex), payload])
      setPageIndex(request.pageIndex)
    } catch (caughtError) {
      if (!controller.signal.aborted) {
        setError(caughtError instanceof Error ? caughtError.message : 'Worker snapshots could not be loaded.')
      }
    } finally {
      if (listAbortControllerRef.current === controller) {
        setIsLoading(false)
      }
    }
  }, [])

  const normalizedSearch = search.trim()
  useEffect(() => {
    const delay = window.setTimeout(() => {
      setPages([])
      setPageIndex(0)
      void loadPage({ cursor: '', pageIndex: 0, query: normalizedSearch })
    }, 250)
    return () => {
      window.clearTimeout(delay)
      listAbortControllerRef.current?.abort()
      detailAbortControllerRef.current?.abort()
    }
  }, [loadPage, normalizedSearch])

  function retry() {
    if (failedRequestRef.current) {
      void loadPage(failedRequestRef.current)
    }
  }

  function changePage(nextPage: number) {
    const nextPageIndex = nextPage - 1
    if (nextPageIndex === pageIndex) {
      return
    }
    if (pages[nextPageIndex]) {
      setPageIndex(nextPageIndex)
      return
    }
    const cursor = pages[pageIndex]?.next_cursor
    if (cursor) {
      setPageIndex(nextPageIndex)
      void loadPage({ cursor, pageIndex: nextPageIndex, query: normalizedSearch })
    }
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

  function openRawSnapshot(snapshotId: SnapshotSummary['snapshot_id']) {
    setSelectedSnapshotId(snapshotId)
    if (rawSnapshots[snapshotId] === undefined) {
      void loadRawSnapshot(snapshotId)
    }
  }

  const currentPage = pages[pageIndex]
  const snapshots = currentPage?.items ?? []
  const hasNextPage = Boolean(currentPage?.next_cursor || pages[pageIndex + 1])
  const selectedRawSnapshot = selectedSnapshotId === null ? undefined : rawSnapshots[selectedSnapshotId]
  const selectedRawError = selectedSnapshotId === null ? undefined : detailError[selectedSnapshotId]
  const isSelectedRawSnapshotLoading = selectedSnapshotId !== null && loadingDetailId === selectedSnapshotId

  return (
    <section aria-label="Worker snapshots">
      <div className="console-table-actions analysis-history-filters" role="search">
        <label htmlFor="worker-snapshots-search">Search</label>
        <input
          id="worker-snapshots-search"
          name="q"
          type="search"
          value={search}
          onChange={(event) => {
            setSearch(event.target.value)
            setIsLoading(true)
          }}
          placeholder="Server or login"
        />
      </div>

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
          <span>{normalizedSearch ? 'Try a different search term.' : 'New worker reports will appear here.'}</span>
        </div>
      ) : null}

      {snapshots.length > 0 ? (
        <>
          <div className="console-table-scroll">
            <table aria-label="Worker snapshots" className="console-table">
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
                        <button
                          type="button"
                          onClick={() => openRawSnapshot(snapshot.snapshot_id)}
                        >
                          View raw JSON
                        </button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          <div className="console-pagination">
            <Pagination
              hasMore={hasNextPage}
              isDisabled={isLoading}
              label="Worker snapshot pages"
              onChange={changePage}
              page={pageIndex + 1}
              pageSize={PAGE_SIZE}
              size="sm"
            />
          </div>
        </>
      ) : null}
      {selectedSnapshotId !== null && (
        <div className="snapshot-json-dialog-backdrop" role="presentation">
          <section
            aria-labelledby="snapshot-json-dialog-title"
            aria-modal="true"
            className="snapshot-json-dialog"
            role="dialog"
          >
            <div className="snapshot-json-dialog-header">
              <h2 id="snapshot-json-dialog-title">Raw snapshot JSON</h2>
              <button aria-label="Close raw snapshot JSON" onClick={() => setSelectedSnapshotId(null)} type="button">Close</button>
            </div>
            {isSelectedRawSnapshotLoading && <p role="status">Loading raw JSON…</p>}
            {selectedRawError && (
              <div role="alert">
                <p>{selectedRawError}</p>
                <button type="button" onClick={() => void loadRawSnapshot(selectedSnapshotId)}>Try again</button>
              </div>
            )}
            {selectedRawSnapshot !== undefined && <pre className="console-raw-detail">{JSON.stringify(selectedRawSnapshot, null, 2)}</pre>}
          </section>
        </div>
      )}
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
  if (Number.isNaN(date.getTime())) {
    return value
  }
  const pad = (part: number) => String(part).padStart(2, '0')
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`
}

export default WorkerSnapshotsPage
