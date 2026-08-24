import { useCallback, useEffect, useRef, useState } from 'react'
import { Pagination } from '@astryxdesign/core/Pagination'
import './Console.css'

export type ProductPairSummary = {
  product_pair_id: string
  status: string
  endpoints: Array<{
    server: string
    symbol: string
  }>
  lot_relationship: {
    ratio: string
  }
  built_from_analysis_id: string
  created_at: string
  retired_reason: string | null
}

type ProductPairsResponse = {
  items: ProductPairSummary[]
  next_cursor: string | null
  total_items: number
}

type ProductPairsListPageProps = {
  status: 'active' | 'retired'
  onOpenPair?: (productPairId: string) => void
}

type ProductPairsRequest = {
  pageIndex: number
  query: string
  status: ProductPairsListPageProps['status']
}

const PAGE_SIZE = 50

export function ProductPairsListPage({ status }: ProductPairsListPageProps) {
  const [search, setSearch] = useState('')
  const [pages, setPages] = useState<ProductPairsResponse[]>([])
  const [pageIndex, setPageIndex] = useState(0)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const abortControllerRef = useRef<AbortController | null>(null)
  const failedRequestRef = useRef<ProductPairsRequest | null>(null)

  const loadPage = useCallback(async (request: ProductPairsRequest) => {
    abortControllerRef.current?.abort()
    const controller = new AbortController()
    abortControllerRef.current = controller
    failedRequestRef.current = request
    setError(null)
    setIsLoading(true)

    const parameters = new URLSearchParams({
      limit: String(PAGE_SIZE),
      status: request.status,
    })
    if (request.query) {
      parameters.set('q', request.query)
    }
    parameters.set('page', String(request.pageIndex + 1))

    try {
      const response = await fetch(`/api/admin/product-pairs?${parameters}`, {
        credentials: 'same-origin',
        signal: controller.signal,
      })
      if (!response.ok) {
        throw new Error(`Product pairs could not be loaded (${response.status}).`)
      }

      const payload = await response.json() as ProductPairsResponse
      if (
        !Array.isArray(payload.items)
        || (payload.next_cursor !== null && typeof payload.next_cursor !== 'string')
        || !Number.isSafeInteger(payload.total_items)
        || payload.total_items < 0
      ) {
        throw new Error('Product pairs returned an invalid response.')
      }

      setPages((current) => {
        const next = [...current]
        next[request.pageIndex] = payload
        return next
      })
    } catch (caughtError) {
      if (!controller.signal.aborted) {
        setError(caughtError instanceof Error ? caughtError.message : 'Product pairs could not be loaded.')
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
      void loadPage({ pageIndex: 0, query: normalizedSearch, status })
    }, 250)
    return () => {
      window.clearTimeout(delay)
      abortControllerRef.current?.abort()
    }
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

  const currentPage = pages[pageIndex]
  const productPairs = currentPage?.items ?? []
  const totalItems = pages[0]?.total_items ?? 0

  return (
    <section aria-label={`${status === 'active' ? 'Active' : 'Retired'} product pairs`}>
      <div className="console-table-actions analysis-history-filters" role="search">
        <label htmlFor="product-pairs-search">Search</label>
        <input
          id="product-pairs-search"
          name="q"
          type="search"
          value={search}
          onChange={(event) => {
            setSearch(event.target.value)
            setIsLoading(true)
          }}
          placeholder="Server, symbol, or analysis ID"
        />
      </div>

      {isLoading && productPairs.length === 0 ? (
        <p className="console-empty-state" role="status">Loading {status} product pairs…</p>
      ) : null}
      {isLoading && productPairs.length > 0 ? (
        <p className="console-table-actions" role="status">Updating product pairs…</p>
      ) : null}

      {error ? (
        <div className="console-empty-state" role="alert">
          <strong>Product pairs are unavailable.</strong>
          <span>{error}</span>
          <button type="button" onClick={retry}>Try again</button>
        </div>
      ) : null}

      {!isLoading && !error && productPairs.length === 0 ? (
        <div className="console-empty-state">
          <strong>No {status} product pairs found.</strong>
          <span>{normalizedSearch ? 'Try a different search term.' : `New ${status} product pairs will appear here.`}</span>
        </div>
      ) : null}

      {productPairs.length > 0 ? (
        <>
          <div className="console-table-scroll">
            <table aria-label={`${status === 'active' ? 'Active' : 'Retired'} product pairs`} className="console-table">
              <thead>
                <tr>
                  <th scope="col">Created</th>
                  <th scope="col">Endpoint pair</th>
                  <th scope="col">Lot ratio</th>
                  <th scope="col">Lifecycle status</th>
                  <th scope="col">Source analysis</th>
                  {status === 'retired' ? <th scope="col">Retired reason</th> : null}
                </tr>
              </thead>
              <tbody>
                {productPairs.map((productPair) => (
                  <tr key={productPair.product_pair_id}>
                    <td><time dateTime={productPair.created_at}>{formatDateTime(productPair.created_at)}</time></td>
                    <td>{endpointPairLabel(productPair.endpoints)}</td>
                    <td>{productPair.lot_relationship?.ratio || '—'}</td>
                    <td><span className="console-status" data-state={productPair.status}>{humanize(productPair.status)}</span></td>
                    <td>{productPair.built_from_analysis_id || '—'}</td>
                    {status === 'retired' ? <td>{productPair.retired_reason ? humanize(productPair.retired_reason) : '—'}</td> : null}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="console-pagination">
            <Pagination
              isDisabled={isLoading}
              label="Product pair pages"
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

function endpointPairLabel(endpoints: ProductPairSummary['endpoints']) {
  return endpoints.map(({ server, symbol }) => `${server} · ${symbol}`).join(' / ') || '—'
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

export default ProductPairsListPage
