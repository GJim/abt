import { useCallback, useEffect, useRef, useState } from 'react'
import type { KeyboardEvent } from 'react'
import { Pagination } from '@astryxdesign/core/Pagination'
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
  total_items: number
}

type AnalysisHistoryPageProps = {
  onOpenAnalysis: (analysisId: string) => void
}

type AnalysisPage = AnalysisHistoryResponse

type AnalysisRequest = {
  pageIndex: number
  query: string
  status: string
}

const PAGE_SIZE = 20

const statusOptions = [
  { value: '', label: 'All statuses' },
  { value: 'running', label: 'Running' },
  { value: 'succeeded', label: 'Succeeded' },
  { value: 'failed', label: 'Failed' },
]

export function AnalysisHistoryPage({ onOpenAnalysis }: AnalysisHistoryPageProps) {
  const [search, setSearch] = useState('')
  const [status, setStatus] = useState('')
  const [pages, setPages] = useState<AnalysisPage[]>([])
  const [pageIndex, setPageIndex] = useState(0)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const abortControllerRef = useRef<AbortController | null>(null)
  const failedRequestRef = useRef<AnalysisRequest | null>(null)

  const loadPage = useCallback(async (request: AnalysisRequest) => {
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
    if (request.status) {
      parameters.set('status', request.status)
    }
    parameters.set('page', String(request.pageIndex + 1))

    try {
      const response = await fetch(`/api/admin/product-catalog-analyses?${parameters}`, {
        credentials: 'same-origin',
        signal: controller.signal,
      })
      if (!response.ok) {
        throw new Error(`Analysis history could not be loaded (${response.status}).`)
      }

      const payload = await response.json() as AnalysisHistoryResponse
      if (
        !Array.isArray(payload.items)
        || (payload.next_cursor !== null && typeof payload.next_cursor !== 'string')
        || !Number.isSafeInteger(payload.total_items)
        || payload.total_items < 0
      ) {
        throw new Error('Analysis history returned an invalid response.')
      }

      setPages((current) => {
        const next = [...current]
        next[request.pageIndex] = payload
        return next
      })
    } catch (caughtError) {
      if (controller.signal.aborted) {
        return
      }
      setError(caughtError instanceof Error ? caughtError.message : 'Analysis history could not be loaded.')
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
      void loadPage({ pageIndex: 0, query: normalizedSearch, status })
    }, 250)
    return () => window.clearTimeout(delay)
  }, [loadPage, normalizedSearch, status])

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
    setPageIndex(nextPageIndex)
    void loadPage({ pageIndex: nextPageIndex, query: normalizedSearch, status })
  }

  function openRow(analysisId: string, event: KeyboardEvent<HTMLTableRowElement>) {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      onOpenAnalysis(analysisId)
    }
  }

  const currentPage = pages[pageIndex]
  const analyses = currentPage?.items ?? []
  const totalItems = pages[0]?.total_items ?? 0

  return (
    <section aria-label="Analysis history">
      <div className="console-table-actions analysis-history-filters" role="search">
        <div className="analysis-history-filter analysis-history-search-filter">
          <label htmlFor="analysis-history-search">Search</label>
          <input
            id="analysis-history-search"
            name="q"
            type="search"
            value={search}
            onChange={(event) => {
              setSearch(event.target.value)
              setIsLoading(true)
            }}
            placeholder="Worker, server, policy, ID, or YYYYMMDD"
          />
        </div>
        <div className="analysis-history-filter">
          <label htmlFor="analysis-history-status">Status</label>
          <select
            id="analysis-history-status"
            name="status"
            value={status}
            onChange={(event) => {
              setStatus(event.target.value)
              setIsLoading(true)
            }}
            disabled={isLoading}
          >
            {statusOptions.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
          </select>
        </div>
      </div>

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
          <span>{normalizedSearch || status ? 'Try changing the search or status filter.' : 'New analysis requests will appear here.'}</span>
        </div>
      ) : null}

      {analyses.length > 0 ? (
        <>
          <div className="console-table-scroll">
            <table aria-label="Analysis history" className="console-table">
              <thead>
                <tr>
                  <th scope="col">Requested</th>
                  <th scope="col">Worker pair</th>
                  <th scope="col">Policy</th>
                  <th scope="col">Status</th>
                  <th scope="col">Stage</th>
                  <th scope="col">Retries</th>
                </tr>
              </thead>
              <tbody>
                {analyses.map((analysis) => (
                  <tr
                    aria-label={`Open analysis ${analysis.analysis_id}`}
                    className="console-clickable-row"
                    key={analysis.analysis_id}
                    onClick={() => onOpenAnalysis(analysis.analysis_id)}
                    onKeyDown={(event) => openRow(analysis.analysis_id, event)}
                    role="link"
                    tabIndex={0}
                  >
                    <td><time dateTime={analysis.requested_at}>{formatDateTime(analysis.requested_at)}</time></td>
                    <td>{workerLabel(analysis.first_worker)} / {workerLabel(analysis.second_worker)}</td>
                    <td>{analysis.policy_label || '—'}</td>
                    <td><span className="console-status" data-state={analysis.status}>{humanize(analysis.status)}</span></td>
                    <td><span className="console-tag">{humanize(analysis.current_stage)}</span></td>
                    <td>{analysis.retry_count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="console-pagination">
            <Pagination
              isDisabled={isLoading}
              label="Analysis history pages"
              onChange={changePage}
              page={pageIndex + 1}
              pageSize={PAGE_SIZE}
              size="sm"
              totalItems={totalItems}
            />
          </div>
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
  if (Number.isNaN(date.getTime())) {
    return value
  }
  const pad = (part: number) => String(part).padStart(2, '0')
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`
}

function humanize(value: string) {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (character) => character.toUpperCase())
}

export default AnalysisHistoryPage
