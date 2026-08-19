import { useCallback, useEffect, useRef, useState } from 'react'
import { Pagination } from '@astryxdesign/core/Pagination'
import './Console.css'

export type AuditEvent = {
  event_id: number
  event_type: string
  occurred_at: string
  payload?: Record<string, unknown>
}

type AuditEventsResponse = {
  items: AuditEvent[]
  next_cursor: string | null
}

type AuditEventsPageProps = {
  csrfToken?: string
}

type EventRequest = {
  cursor: string
  pageIndex: number
  query: string
}

const PAGE_SIZE = 50

export function AuditEventsPage({ csrfToken }: AuditEventsPageProps) {
  const [search, setSearch] = useState('')
  const [pages, setPages] = useState<AuditEventsResponse[]>([])
  const [pageIndex, setPageIndex] = useState(0)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const abortControllerRef = useRef<AbortController | null>(null)
  const failedRequestRef = useRef<EventRequest | null>(null)

  void csrfToken

  const loadPage = useCallback(async (request: EventRequest) => {
    abortControllerRef.current?.abort()
    const controller = new AbortController()
    abortControllerRef.current = controller
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
      const response = await fetch(`/api/admin/events?${parameters}`, {
        credentials: 'same-origin',
        signal: controller.signal,
      })
      if (!response.ok) {
        throw new Error(`Audit events could not be loaded (${response.status}).`)
      }

      const payload = await response.json() as AuditEventsResponse
      if (!Array.isArray(payload.items) || (payload.next_cursor !== null && typeof payload.next_cursor !== 'string')) {
        throw new Error('Audit events returned an invalid response.')
      }

      setPages((current) => [...current.slice(0, request.pageIndex), payload])
      setPageIndex(request.pageIndex)
    } catch (caughtError) {
      if (!controller.signal.aborted) {
        setError(caughtError instanceof Error ? caughtError.message : 'Audit events could not be loaded.')
      }
    } finally {
      if (abortControllerRef.current === controller) {
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
      abortControllerRef.current?.abort()
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

  const currentPage = pages[pageIndex]
  const events = currentPage?.items ?? []
  const hasNextPage = Boolean(currentPage?.next_cursor || pages[pageIndex + 1])

  return (
    <section aria-label="Audit events">
      <div className="console-table-actions analysis-history-filters" role="search">
        <label htmlFor="audit-events-search">Search</label>
        <input
          id="audit-events-search"
          name="q"
          type="search"
          value={search}
          onChange={(event) => {
            setSearch(event.target.value)
            setIsLoading(true)
          }}
          placeholder="Event type, reason, or identifier"
        />
      </div>

      {isLoading && events.length === 0 ? (
        <p className="console-empty-state" role="status">Loading audit events…</p>
      ) : null}
      {isLoading && events.length > 0 ? (
        <p className="console-table-actions" role="status">Updating audit events…</p>
      ) : null}

      {error ? (
        <div className="console-empty-state" role="alert">
          <strong>Audit events are unavailable.</strong>
          <span>{error}</span>
          <button type="button" onClick={retry}>Try again</button>
        </div>
      ) : null}

      {!isLoading && !error && events.length === 0 ? (
        <div className="console-empty-state">
          <strong>No audit events found.</strong>
          <span>{normalizedSearch ? 'Try a different search term.' : 'New operational activity will appear here.'}</span>
        </div>
      ) : null}

      {events.length > 0 ? (
        <>
          <div className="console-table-scroll">
            <table aria-label="Audit events" className="console-table">
              <thead>
                <tr>
                  <th scope="col">Timestamp</th>
                  <th scope="col">Event type</th>
                  <th scope="col">Summary</th>
                </tr>
              </thead>
              <tbody>
                {events.map((event) => (
                  <tr key={event.event_id}>
                    <td><time dateTime={event.occurred_at}>{formatDateTime(event.occurred_at)}</time></td>
                    <td><span className="console-tag">{humanize(event.event_type)}</span></td>
                    <td>
                      <span>{eventSummary(event)}</span>
                      {event.payload ? (
                        <details>
                          <summary>View raw payload</summary>
                          <pre className="console-raw-detail">{JSON.stringify(event.payload, null, 2)}</pre>
                        </details>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="console-pagination">
            <Pagination
              hasMore={hasNextPage}
              isDisabled={isLoading}
              label="Audit event pages"
              onChange={changePage}
              page={pageIndex + 1}
              pageSize={PAGE_SIZE}
              size="sm"
            />
          </div>
        </>
      ) : null}
    </section>
  )
}

function formatDateTime(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) {
    return value
  }
  const pad = (part: number) => String(part).padStart(2, '0')
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`
}

function humanize(value: string) {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (character) => character.toUpperCase())
}

function eventSummary(event: AuditEvent) {
  const payload = event.payload
  if (!payload) {
    return humanize(event.event_type)
  }

  for (const key of ['reason', 'message', 'stage', 'analysis_id', 'worker_id', 'enrollment_id']) {
    const value = payload[key]
    if (typeof value === 'string' && value.trim()) {
      return key === 'stage' ? humanize(value) : value
    }
  }

  return humanize(event.event_type)
}

export default AuditEventsPage
