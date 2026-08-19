import { useCallback, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
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
  append: boolean
}

export function AuditEventsPage({ csrfToken }: AuditEventsPageProps) {
  const [search, setSearch] = useState('')
  const [appliedSearch, setAppliedSearch] = useState('')
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abortControllerRef = useRef<AbortController | null>(null)
  const failedRequestRef = useRef<EventRequest>({ cursor: '', append: false })

  void csrfToken

  const loadEvents = useCallback(async (request: EventRequest, query: string) => {
    abortControllerRef.current?.abort()
    const controller = new AbortController()
    abortControllerRef.current = controller
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

      setEvents((current) => request.append ? [...current, ...payload.items] : payload.items)
      setNextCursor(payload.next_cursor)
    } catch (caughtError) {
      if (controller.signal.aborted) {
        return
      }
      setError(caughtError instanceof Error ? caughtError.message : 'Audit events could not be loaded.')
    } finally {
      if (abortControllerRef.current === controller) {
        setIsLoading(false)
        setIsLoadingMore(false)
      }
    }
  }, [])

  useEffect(() => {
    void loadEvents({ cursor: '', append: false }, appliedSearch)
    return () => abortControllerRef.current?.abort()
  }, [appliedSearch, loadEvents])

  function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setAppliedSearch(search.trim())
  }

  function retry() {
    void loadEvents(failedRequestRef.current, appliedSearch)
  }

  return (
    <section aria-labelledby="audit-events-heading">
      <header className="console-page-header">
        <div>
          <h1 id="audit-events-heading">Audit events</h1>
          <p>Review the immutable operational record, newest events first.</p>
        </div>
      </header>

      <form className="console-table-actions" role="search" onSubmit={submitSearch}>
        <label htmlFor="audit-events-search">Search audit events</label>
        <input
          id="audit-events-search"
          name="q"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Event type, reason, or identifier"
        />
        <button type="submit" disabled={isLoading || isLoadingMore}>Search</button>
      </form>

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
          <span>{appliedSearch ? 'Try a different search term.' : 'New operational activity will appear here.'}</span>
        </div>
      ) : null}

      {events.length > 0 ? (
        <>
          <div className="console-table-scroll">
            <table className="console-table">
              <caption className="visually-hidden">Audit events</caption>
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

          {nextCursor ? (
            <div className="console-table-actions">
              <button
                type="button"
                onClick={() => void loadEvents({ cursor: nextCursor, append: true }, appliedSearch)}
                disabled={isLoadingMore}
              >
                {isLoadingMore ? 'Loading more events…' : 'Load more'}
              </button>
              {isLoadingMore ? <span role="status">Loading more audit events…</span> : null}
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  )
}

function formatDateTime(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
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
