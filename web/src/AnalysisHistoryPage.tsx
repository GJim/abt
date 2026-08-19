import { useCallback, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import './Console.css'

export type AnalysisSummary = {
  analysis_id: string
  requested_by: string
  first_worker: {
    worker_id: string
    login: number
    server: string
  }
  second_worker: {
    worker_id: string
    login: number
    server: string
  }
  policy_label: string | null
  status: string
  current_stage: string
  retry_count: number
  requested_at: string
  completed_at: string | null
}

type AnalysisHistoryResponse = {
  items: AnalysisSummary[]
  next_cursor: string | null
}

type AnalysisHistoryPageProps = {
  onOpenAnalysis: (analysisId: string) => void
}

type AnalysisRequest = {
  cursor: string
  append: boolean
}

const statusOptions = [
  { value: '', label: 'All statuses' },
  { value: 'running', label: 'Running' },
  { value: 'succeeded', label: 'Succeeded' },
  { value: 'failed', label: 'Failed' },
]

export function AnalysisHistoryPage({ onOpenAnalysis }: AnalysisHistoryPageProps) {
  const [search, setSearch] = useState('')
  const [appliedSearch, setAppliedSearch] = useState('')
  const [status, setStatus] = useState('')
  const [analyses, setAnalyses] = useState<AnalysisSummary[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abortControllerRef = useRef<AbortController | null>(null)
  const failedRequestRef = useRef<AnalysisRequest>({ cursor: '', append: false })

  const loadAnalyses = useCallback(async (request: AnalysisRequest, query: string, statusFilter: string) => {
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

    const parameters = new URLSearchParams({ limit: '50' })
    if (query) {
      parameters.set('q', query)
    }
    if (statusFilter) {
      parameters.set('status', statusFilter)
    }
    if (request.cursor) {
      parameters.set('cursor', request.cursor)
    }

    try {
      const response = await fetch(`/api/admin/product-catalog-analyses?${parameters}`, {
        credentials: 'same-origin',
        signal: controller.signal,
      })
      if (!response.ok) {
        throw new Error(`Analysis history could not be loaded (${response.status}).`)
      }

      const payload = await response.json() as AnalysisHistoryResponse
      if (!Array.isArray(payload.items) || (payload.next_cursor !== null && typeof payload.next_cursor !== 'string')) {
        throw new Error('Analysis history returned an invalid response.')
      }

      setAnalyses((current) => request.append ? [...current, ...payload.items] : payload.items)
      setNextCursor(payload.next_cursor)
    } catch (caughtError) {
      if (controller.signal.aborted) {
        return
      }
      setError(caughtError instanceof Error ? caughtError.message : 'Analysis history could not be loaded.')
    } finally {
      if (abortControllerRef.current === controller) {
        setIsLoading(false)
        setIsLoadingMore(false)
      }
    }
  }, [])

  useEffect(() => {
    void loadAnalyses({ cursor: '', append: false }, appliedSearch, status)
    return () => abortControllerRef.current?.abort()
  }, [appliedSearch, loadAnalyses, status])

  function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setAppliedSearch(search.trim())
  }

  function retry() {
    void loadAnalyses(failedRequestRef.current, appliedSearch, status)
  }

  return (
    <section aria-labelledby="analysis-history-heading">
      <header className="console-page-header">
        <div>
          <h1 id="analysis-history-heading">Analysis history</h1>
          <p>Search and review product catalog analysis requests across worker pairs.</p>
        </div>
      </header>

      <form className="console-table-actions" role="search" onSubmit={submitSearch}>
        <label htmlFor="analysis-history-search">Search analyses</label>
        <input
          id="analysis-history-search"
          name="q"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Worker, server, policy, or ID"
        />
        <label htmlFor="analysis-history-status">Status</label>
        <select
          id="analysis-history-status"
          name="status"
          value={status}
          onChange={(event) => setStatus(event.target.value)}
          disabled={isLoadingMore}
        >
          {statusOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
        </select>
        <button type="submit" disabled={isLoading || isLoadingMore}>Search</button>
      </form>

      {isLoading && analyses.length === 0 ? (
        <p className="console-empty-state" role="status">Loading analysis history…</p>
      ) : null}
      {isLoading && analyses.length > 0 ? (
        <p className="console-table-actions" role="status">Updating analysis history…</p>
      ) : null}

      {error ? (
        <div className="console-empty-state" role="alert">
          <strong>Analysis history is unavailable.</strong>
          <span>{error}</span>
          <button type="button" onClick={retry}>Try again</button>
        </div>
      ) : null}

      {!isLoading && !error && analyses.length === 0 ? (
        <div className="console-empty-state">
          <strong>No analyses found.</strong>
          <span>{appliedSearch || status ? 'Try changing the search or status filter.' : 'New analysis requests will appear here.'}</span>
        </div>
      ) : null}

      {analyses.length > 0 ? (
        <>
          <div className="console-table-scroll">
            <table className="console-table">
              <caption className="visually-hidden">Product catalog analysis history</caption>
              <thead>
                <tr>
                  <th scope="col">Requested</th>
                  <th scope="col">Worker pair</th>
                  <th scope="col">Policy</th>
                  <th scope="col">Status</th>
                  <th scope="col">Stage</th>
                  <th scope="col">Retries</th>
                  <th scope="col">Action</th>
                </tr>
              </thead>
              <tbody>
                {analyses.map((analysis) => (
                  <tr key={analysis.analysis_id}>
                    <td><time dateTime={analysis.requested_at}>{formatDateTime(analysis.requested_at)}</time></td>
                    <td>{workerLabel(analysis.first_worker)} / {workerLabel(analysis.second_worker)}</td>
                    <td>{analysis.policy_label || '—'}</td>
                    <td><span className="console-status" data-state={analysis.status}>{humanize(analysis.status)}</span></td>
                    <td><span className="console-tag">{humanize(analysis.current_stage)}</span></td>
                    <td>{analysis.retry_count}</td>
                    <td><button type="button" onClick={() => onOpenAnalysis(analysis.analysis_id)}>Open</button></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {nextCursor ? (
            <div className="console-table-actions">
              <button
                type="button"
                onClick={() => void loadAnalyses({ cursor: nextCursor, append: true }, appliedSearch, status)}
                disabled={isLoadingMore}
              >
                {isLoadingMore ? 'Loading more analyses…' : 'Load more'}
              </button>
              {isLoadingMore ? <span role="status">Loading more analyses…</span> : null}
            </div>
          ) : null}
        </>
      ) : null}
    </section>
  )
}

function workerLabel(worker: AnalysisSummary['first_worker']) {
  return `${worker.server} · ${worker.login}`
}

function formatDateTime(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

function humanize(value: string) {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (character) => character.toUpperCase())
}

export default AnalysisHistoryPage
