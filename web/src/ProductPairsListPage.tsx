import { useCallback, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
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
}

type ProductPairsListPageProps = {
  status: 'active' | 'retired'
  onOpenPair: (productPairId: string) => void
}

type ProductPairsRequest = {
  cursor: string
  append: boolean
}

export function ProductPairsListPage({ status, onOpenPair }: ProductPairsListPageProps) {
  const [search, setSearch] = useState('')
  const [appliedSearch, setAppliedSearch] = useState('')
  const [productPairs, setProductPairs] = useState<ProductPairSummary[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isLoadingMore, setIsLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const abortControllerRef = useRef<AbortController | null>(null)
  const failedRequestRef = useRef<ProductPairsRequest>({ cursor: '', append: false })
  const title = status === 'active' ? 'Active product pairs' : 'Retired product pairs'

  const loadProductPairs = useCallback(async (request: ProductPairsRequest, query: string, statusFilter: ProductPairsListPageProps['status']) => {
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
      status: statusFilter,
      cursor: request.cursor,
    })

    try {
      const response = await fetch(`/api/admin/product-pairs?${parameters}`, {
        credentials: 'same-origin',
        signal: controller.signal,
      })
      if (!response.ok) {
        throw new Error(`Product pairs could not be loaded (${response.status}).`)
      }

      const payload = await response.json() as ProductPairsResponse
      if (!Array.isArray(payload.items) || (payload.next_cursor !== null && typeof payload.next_cursor !== 'string')) {
        throw new Error('Product pairs returned an invalid response.')
      }

      setProductPairs((current) => request.append ? [...current, ...payload.items] : payload.items)
      setNextCursor(payload.next_cursor)
    } catch (caughtError) {
      if (controller.signal.aborted) {
        return
      }
      setError(caughtError instanceof Error ? caughtError.message : 'Product pairs could not be loaded.')
    } finally {
      if (abortControllerRef.current === controller) {
        setIsLoading(false)
        setIsLoadingMore(false)
      }
    }
  }, [])

  useEffect(() => {
    void loadProductPairs({ cursor: '', append: false }, appliedSearch, status)
    return () => abortControllerRef.current?.abort()
  }, [appliedSearch, loadProductPairs, status])

  function submitSearch(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setAppliedSearch(search.trim())
  }

  function retry() {
    void loadProductPairs(failedRequestRef.current, appliedSearch, status)
  }

  return (
    <section aria-labelledby="product-pairs-heading">
      <header className="console-page-header">
        <div>
          <h1 id="product-pairs-heading">{title}</h1>
          <p>Review the approved cross-server mappings and the analysis that established them.</p>
        </div>
      </header>

      <form className="console-table-actions" role="search" onSubmit={submitSearch}>
        <label htmlFor="product-pairs-search">Search product pairs</label>
        <input
          id="product-pairs-search"
          name="q"
          type="search"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Server, symbol, or analysis ID"
        />
        <button type="submit" disabled={isLoading || isLoadingMore}>Search</button>
      </form>

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
          <span>{appliedSearch ? 'Try a different search term.' : `New ${status} product pairs will appear here.`}</span>
        </div>
      ) : null}

      {productPairs.length > 0 ? (
        <>
          <div className="console-table-scroll">
            <table className="console-table">
              <caption className="visually-hidden">{title}</caption>
              <thead>
                <tr>
                  <th scope="col">Created</th>
                  <th scope="col">Endpoint pair</th>
                  <th scope="col">Lot ratio</th>
                  <th scope="col">Lifecycle status</th>
                  <th scope="col">Source analysis</th>
                  {status === 'retired' ? <th scope="col">Retired reason</th> : null}
                  <th scope="col">Action</th>
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
                    <td><button type="button" onClick={() => onOpenPair(productPair.product_pair_id)}>Open</button></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {nextCursor ? (
            <div className="console-table-actions">
              <button
                type="button"
                onClick={() => void loadProductPairs({ cursor: nextCursor, append: true }, appliedSearch, status)}
                disabled={isLoadingMore}
              >
                {isLoadingMore ? 'Loading more product pairs…' : 'Load more'}
              </button>
              {isLoadingMore ? <span role="status">Loading more product pairs…</span> : null}
            </div>
          ) : null}
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
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

function humanize(value: string) {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (character) => character.toUpperCase())
}

export default ProductPairsListPage
