import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import type { AccountWorker } from './App'
import { formatTimestamp } from './formatting'

type Endpoint = { server: string; symbol: string }
type LiveQuote = {
  symbol: string
  bid: number
  ask: number
  last?: number
  broker_time: string
}
type ApplicableWorker = { worker_id: string; server?: string; applicability_status?: string }
type ProductPair = {
  product_pair_id: string
  status: string
  endpoints: Endpoint[]
  worker_applicability: ApplicableWorker[]
}
type Target = {
  pair: ProductPair
  workers: Array<AccountWorker & { endpoint: Endpoint }>
  buy_worker_id: string
  sell_worker_id: string
  leg_order: 'buy_to_sell' | 'sell_to_buy'
  interval_seconds: number
  revision: number
}
type ManualTradePlan = {
  pair_id: string
  target_revision: number
  leg_order: string
  interval_seconds: number
  legs: Array<{
    worker_id: string
    symbol: string
    direction: string
    lots: string
    estimated_entry_price: string
    estimated_stop_loss: string
    estimated_take_profit: string
  }>
}
type ManualTrade = {
  manual_trade_id: string
  pair_id: string
  status: 'scheduled' | 'dispatching' | 'active' | 'needs_human'
  created_at: string
  legs: Array<{
    worker_id: string
    login: number | null
    server: string | null
    symbol: string
    direction: string
    lots: string
    market_order_ticket: string | null
    position_ticket: string | null
    position_status: 'pending' | 'open' | 'closed'
  }>
}
type EntryDraft = {
  base_lots: string
  stop_loss_pips: string
  take_profit_pips: string
}
type ActiveOperationPlan = {
  manual_trade_id: string
  operation: 'exit' | 'protection'
  legs: Array<{
    worker_id: string
    symbol: string
    direction: string
    lots: string
    position: string
    fill_price: string
    stop_loss?: string
    take_profit?: string
  }>
}

export function ManualTradingPage({
  csrfToken,
  onChanged,
  productPairs,
  workers,
}: {
  csrfToken: string
  onChanged: () => Promise<void>
  productPairs: ProductPair[]
  workers: AccountWorker[]
}) {
  const [target, setTarget] = useState<Target | null>(null)
  const [trades, setTrades] = useState<ManualTrade[]>([])
  const [pairId, setPairId] = useState('')
  const [buyWorkerId, setBuyWorkerId] = useState('')
  const [sellWorkerId, setSellWorkerId] = useState('')
  const [legOrder, setLegOrder] = useState<Target['leg_order']>('buy_to_sell')
  const [intervalSeconds, setIntervalSeconds] = useState('0')
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [entry, setEntry] = useState<EntryDraft>({ base_lots: '', stop_loss_pips: '', take_profit_pips: '' })
  const [preview, setPreview] = useState<{ commandId: string; plan: ManualTradePlan } | null>(null)
  const [activePreview, setActivePreview] = useState<{ commandId: string; plan: ActiveOperationPlan; payload: Record<string, string> } | null>(null)
  const [protection, setProtection] = useState({ stop_loss_pips: '', take_profit_pips: '' })
  const [editingProtectionTradeId, setEditingProtectionTradeId] = useState<string | null>(null)
  const [quoteSearch, setQuoteSearch] = useState('')
  const [showAllQuotes, setShowAllQuotes] = useState(false)

  function applyTargetDraft(loaded: Target | null) {
    if (!loaded) return
    setPairId(loaded.pair.product_pair_id)
    setBuyWorkerId(loaded.buy_worker_id ?? loaded.workers[0]?.worker_id ?? '')
    setSellWorkerId(loaded.sell_worker_id ?? loaded.workers[1]?.worker_id ?? '')
    setLegOrder(loaded.leg_order)
    setIntervalSeconds(String(loaded.interval_seconds))
  }

  async function refreshManualState() {
    const [targetResponse, tradesResponse] = await Promise.all([
      fetch('/api/admin/manual-trading-target', { credentials: 'same-origin' }),
      fetch('/api/admin/manual-trades', { credentials: 'same-origin' }),
    ])
    if (!targetResponse.ok || !tradesResponse.ok) {
      throw new Error('Manual-trading state could not be loaded.')
    }
    const loadedTarget = await targetResponse.json() as Target | null
    setTarget(loadedTarget)
    setTrades(await tradesResponse.json() as ManualTrade[])
    applyTargetDraft(loadedTarget)
  }

  useEffect(() => {
    void refreshManualState().catch((error: unknown) => {
      setMessage(error instanceof Error ? error.message : 'Manual-trading state could not be loaded.')
    })
  }, [])

  const pair = useMemo(
    () => productPairs.find((candidate) => candidate.product_pair_id === pairId && candidate.status === 'active') ?? null,
    [pairId, productPairs],
  )
  const selectedPair = pair ?? target?.pair ?? null
  const eligibleWorkers = useMemo(() => selectedPair === null ? [] : selectedPair.worker_applicability
    .filter((candidate) => candidate.applicability_status === 'applicable')
    .map((candidate) => workers.find((worker) => worker.worker_id === candidate.worker_id))
    .filter((worker): worker is AccountWorker => worker !== undefined)
    .filter((worker) => worker.connectivity === 'connected' && worker.safety_state === 'connected' && worker.live_state?.connectivity === true),
  [selectedPair, workers])

  async function saveTarget(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!pair || !buyWorkerId || !sellWorkerId) return
    setBusy(true)
    setMessage(null)
    try {
      const response = await fetch('/api/admin/manual-trading-target', {
        method: 'PUT',
        credentials: 'same-origin',
        headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({
          pair_id: pair.product_pair_id,
          buy_worker_id: buyWorkerId,
          sell_worker_id: sellWorkerId,
          leg_order: legOrder,
          interval_seconds: Number(intervalSeconds),
          expected_revision: target?.revision ?? 0,
        }),
      })
      const payload = await response.json() as Target | { detail?: string }
      if (!response.ok) throw new Error('detail' in payload ? payload.detail : 'Manual-trading target was rejected.')
      const configured = payload as Target
      setTarget(configured)
      applyTargetDraft(configured)
      setMessage(`Current target saved at revision ${configured.revision}.`)
      await refreshManualState()
      await onChanged()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Manual-trading target was rejected.')
    } finally {
      setBusy(false)
    }
  }

  async function previewEntry(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!target) return
    setBusy(true)
    setMessage(null)
    const commandId = crypto.randomUUID()
    const payload = { command_id: commandId, target_revision: target.revision, ...entry }
    try {
      const response = await fetch('/api/admin/manual-trades/preview', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify(payload),
      })
      const result = await response.json() as ManualTradePlan | { detail?: string }
      if (!response.ok) throw new Error('detail' in result ? result.detail : 'Manual-trade preview was rejected.')
      setPreview({ commandId, plan: result as ManualTradePlan })
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Manual-trade preview was rejected.')
    } finally {
      setBusy(false)
    }
  }

  async function confirmEntry() {
    if (!preview || !target) return
    setBusy(true)
    setMessage(null)
    try {
      const response = await fetch('/api/admin/manual-trades', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({ command_id: preview.commandId, target_revision: target.revision, ...entry }),
      })
      const result = await response.json() as { manual_trade_id?: string; detail?: string }
      if (!response.ok) throw new Error(result.detail ?? 'Manual trade was rejected.')
      setPreview(null)
      setMessage(`Manual trade ${result.manual_trade_id ?? ''} was scheduled.`)
      await refreshManualState()
      await onChanged()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Manual trade was rejected.')
    } finally {
      setBusy(false)
    }
  }

  async function previewTradeOperation(manualTradeId: string, operation: ActiveOperationPlan['operation']) {
    setBusy(true)
    setMessage(null)
    const commandId = crypto.randomUUID()
    const payload = operation === 'protection' ? protection : {}
    try {
      const response = await fetch(`/api/admin/manual-trades/${encodeURIComponent(manualTradeId)}/${operation}/preview`, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({ command_id: commandId, ...payload }),
      })
      const result = await response.json() as ActiveOperationPlan | { detail?: string }
      if (!response.ok) throw new Error('detail' in result ? result.detail : 'Manual-trade operation preview was rejected.')
      setActivePreview({ commandId, plan: result as ActiveOperationPlan, payload })
      setEditingProtectionTradeId(null)
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Manual-trade operation preview was rejected.')
    } finally {
      setBusy(false)
    }
  }

  async function confirmTradeOperation() {
    if (!activePreview) return
    setBusy(true)
    setMessage(null)
    try {
      const { manual_trade_id: manualTradeId, operation } = activePreview.plan
      const response = await fetch(`/api/admin/manual-trades/${encodeURIComponent(manualTradeId)}/${operation}`, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({ command_id: activePreview.commandId, ...activePreview.payload }),
      })
      const result = await response.json() as { operation_id?: string; detail?: string }
      if (!response.ok) throw new Error(result.detail ?? 'Manual-trade operation was rejected.')
      setActivePreview(null)
      setMessage(`Manual-trade ${operation} operation ${result.operation_id ?? ''} was scheduled.`)
      await refreshManualState()
      await onChanged()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Manual-trade operation was rejected.')
    } finally {
      setBusy(false)
    }
  }

  async function discardUnconfirmedTrade(manualTradeId: string) {
    if (!window.confirm('Discard this unconfirmed trade record? It will leave the Paired trades list but remain in the audit trail.')) {
      return
    }
    setBusy(true)
    setMessage(null)
    try {
      const response = await fetch(`/api/admin/manual-trades/${encodeURIComponent(manualTradeId)}`, {
        method: 'DELETE',
        credentials: 'same-origin',
        headers: { 'X-CSRF-Token': csrfToken },
      })
      const result = await response.json() as { detail?: string }
      if (!response.ok) throw new Error(result.detail ?? 'Manual-trade record could not be discarded.')
      setMessage(`Manual trade ${manualTradeId} was discarded from the active list.`)
      await refreshManualState()
      await onChanged()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Manual-trade record could not be discarded.')
    } finally {
      setBusy(false)
    }
  }

  const observedWorkers = useMemo(() => {
    if (selectedPair === null) return []
    return [buyWorkerId, sellWorkerId].flatMap((workerId) => {
      const worker = workers.find((candidate) => candidate.worker_id === workerId)
      const endpoint = selectedPair.endpoints.find((candidate) => candidate.server === worker?.server)
      return worker && endpoint ? [{ ...worker, endpoint }] : []
    })
  }, [buyWorkerId, selectedPair, sellWorkerId, workers])
  const quoteRows = useMemo(() => {
    const rows = new Map<string, { symbol: string; first?: LiveQuote; second?: LiveQuote }>()
    observedWorkers.slice(0, 2).forEach((worker, index) => {
      for (const quote of worker.live_state?.quotes ?? []) {
        const liveQuote = quote as LiveQuote
        const row = rows.get(liveQuote.symbol) ?? { symbol: liveQuote.symbol }
        if (index === 0) row.first = liveQuote
        else row.second = liveQuote
        rows.set(liveQuote.symbol, row)
      }
    })
    return [...rows.values()].sort((left, right) => left.symbol.localeCompare(right.symbol))
  }, [observedWorkers])
  const targetSymbols = useMemo(
    () => new Set(selectedPair?.endpoints.map((endpoint) => endpoint.symbol) ?? []),
    [selectedPair],
  )
  const normalizedQuoteSearch = quoteSearch.trim().toLocaleLowerCase()
  const visibleQuoteRows = quoteRows.filter((row) => (
    normalizedQuoteSearch
      ? row.symbol.toLocaleLowerCase().includes(normalizedQuoteSearch)
      : showAllQuotes || targetSymbols.has(row.symbol)
  ))
  const additionalQuoteCount = quoteRows.filter((row) => !targetSymbols.has(row.symbol)).length

  return <section aria-labelledby="manual-trading-heading" className="manual-trading-page">
    {message ? <p className="manual-trading-message" role="status">{message}</p> : null}

    <form className="analysis-form launch-form manual-target-form" onSubmit={(event) => void saveTarget(event)}>
      <div>
        <h2>Current target</h2>
        <p>Save a new draft at any time. Existing paired trades retain their own recorded legs.</p>
      </div>
      <div className="manual-form-grid">
        <label>Active product pair
          <select required value={pairId} onChange={(event) => {
            setPairId(event.target.value)
            setBuyWorkerId('')
            setSellWorkerId('')
          }}>
            <option value="">Select an active product pair</option>
            {productPairs.filter((candidate) => candidate.status === 'active').map((candidate) => (
              <option key={candidate.product_pair_id} value={candidate.product_pair_id}>
                {candidate.endpoints.map((endpoint) => `${endpoint.server}:${endpoint.symbol}`).join(' / ')}
              </option>
            ))}
          </select>
        </label>
        <label>Buy worker
          <select required value={buyWorkerId} onChange={(event) => setBuyWorkerId(event.target.value)}>
            <option value="">Select a connected applicable worker</option>
            {eligibleWorkers.map((worker) => <option key={worker.worker_id} value={worker.worker_id}>
              {worker.login} on {worker.server}
            </option>)}
          </select>
        </label>
        <label>Sell worker
          <select required value={sellWorkerId} onChange={(event) => setSellWorkerId(event.target.value)}>
            <option value="">Select the worker on the other endpoint</option>
            {eligibleWorkers.map((worker) => <option key={worker.worker_id} value={worker.worker_id}>
              {worker.login} on {worker.server}
            </option>)}
          </select>
        </label>
        <label>Dispatch order
          <select value={legOrder} onChange={(event) => setLegOrder(event.target.value as Target['leg_order'])}>
            <option value="buy_to_sell">Buy to Sell</option>
            <option value="sell_to_buy">Sell to Buy</option>
          </select>
        </label>
        <label>Leg interval (seconds)
          <input min="0" required step="1" type="number" value={intervalSeconds} onChange={(event) => setIntervalSeconds(event.target.value)} />
        </label>
      </div>
      <div className="manual-form-actions">
        <button disabled={busy} type="submit">{busy ? 'Saving…' : 'Save current target'}</button>
      </div>
    </form>

    {target ? <form className="analysis-form launch-form manual-entry-form" onSubmit={(event) => void previewEntry(event)}>
      <div>
        <h2>Enter protected paired trade</h2>
        <p>The target supplies the Buy and Sell workers. Both legs require broker-managed TP and SL.</p>
      </div>
      <div className="manual-form-grid">
        <label>Base lots<input min="0.00001" required step="any" type="number" value={entry.base_lots} onChange={(event) => setEntry({ ...entry, base_lots: event.target.value })} /></label>
        <label>Stop loss (pips)<input min="0.00001" required step="any" type="number" value={entry.stop_loss_pips} onChange={(event) => setEntry({ ...entry, stop_loss_pips: event.target.value })} /></label>
        <label>Take profit (pips)<input min="0.00001" required step="any" type="number" value={entry.take_profit_pips} onChange={(event) => setEntry({ ...entry, take_profit_pips: event.target.value })} /></label>
      </div>
      <div className="manual-form-actions">
        <button disabled={busy} type="submit">Preview protected entry</button>
      </div>
    </form> : null}

    <section aria-labelledby="manual-trades-heading" className="manual-trades-section">
      <div className="manual-section-header">
        <div>
          <h2 id="manual-trades-heading">Paired trades</h2>
          <p>Each record retains its own broker tickets and verified position state.</p>
        </div>
      </div>
      {trades.length === 0 ? <p className="console-empty-state">No scheduled, active, or human-review manual trades.</p> : (
        <ul className="manual-trade-list">
          {trades.map((trade) => {
            const canClose = trade.status === 'active' || trade.status === 'needs_human'
            const canDiscard = trade.status === 'needs_human' && trade.legs.every(
              (leg) => leg.market_order_ticket === null && leg.position_ticket === null,
            )
            return <li className="manual-trade-record" key={trade.manual_trade_id}>
              <header>
                <div>
                  <h3>{trade.pair_id}</h3>
                  <p>Trade {trade.manual_trade_id} · scheduled {formatTimestamp(trade.created_at)}</p>
                </div>
                <span className="console-status" data-state={trade.status}>{trade.status.replace('_', ' ')}</span>
              </header>
              <dl className="manual-trade-legs">
                {trade.legs.map((leg) => <div key={leg.worker_id}>
                  <dt>{leg.direction} · {leg.symbol}</dt>
                  <dd>
                    {leg.login ?? leg.worker_id} on {leg.server ?? 'unknown server'} · {leg.lots} lots
                    <span>Market order {leg.market_order_ticket ?? 'pending'} · position {leg.position_ticket ?? 'pending'} · {leg.position_status}</span>
                  </dd>
                </div>)}
              </dl>
              <div className="manual-trade-actions">
                {canClose ? <button disabled={busy} onClick={() => void previewTradeOperation(trade.manual_trade_id, 'exit')} type="button">
                  {trade.status === 'active' ? 'Close trade' : 'Close verified remaining positions'}
                </button> : null}
                {trade.status === 'active' ? <button className="snapshot-json-button" disabled={busy} onClick={() => setEditingProtectionTradeId(
                  editingProtectionTradeId === trade.manual_trade_id ? null : trade.manual_trade_id,
                )} type="button">Update protections</button> : null}
                {canDiscard ? <button className="manual-trade-discard" disabled={busy} onClick={() => void discardUnconfirmedTrade(
                  trade.manual_trade_id,
                )} type="button">Discard unconfirmed record</button> : null}
              </div>
              {editingProtectionTradeId === trade.manual_trade_id ? <div className="manual-protection-fields">
                <label>Updated stop loss (pips)<input min="0.00001" required step="any" type="number" value={protection.stop_loss_pips} onChange={(event) => setProtection({ ...protection, stop_loss_pips: event.target.value })} /></label>
                <label>Updated take profit (pips)<input min="0.00001" required step="any" type="number" value={protection.take_profit_pips} onChange={(event) => setProtection({ ...protection, take_profit_pips: event.target.value })} /></label>
                <button disabled={busy || !protection.stop_loss_pips || !protection.take_profit_pips} onClick={() => void previewTradeOperation(trade.manual_trade_id, 'protection')} type="button">Preview protection update</button>
              </div> : null}
            </li>
          })}
        </ul>
      )}
    </section>

    <section aria-labelledby="manual-quotes-heading" className="manual-live-state">
      <div className="manual-section-header">
        <div>
          <h2 id="manual-quotes-heading">Live target quotes</h2>
          <p>Quotes are reference context for the editable current target, not a trade ledger.</p>
        </div>
      </div>
      {observedWorkers.length === 0 ? <p className="console-empty-state">Configure a current target to inspect its live quotes.</p> : (
        <section className="manual-quote-panel" aria-label="Live quotes">
          <div className="manual-quote-toolbar">
            <label>Search symbols
              <input onChange={(event) => setQuoteSearch(event.target.value)} placeholder="Search symbols" type="search" value={quoteSearch} />
            </label>
            {additionalQuoteCount > 0 && !normalizedQuoteSearch ? (
              <button onClick={() => setShowAllQuotes((visible) => !visible)} type="button">
                {showAllQuotes ? 'Show target symbols only' : `Show ${additionalQuoteCount} more symbols`}
              </button>
            ) : null}
          </div>
          <div className="console-table-scroll">
            <table className="console-table manual-quote-table">
              <thead>
                <tr>
                  <th rowSpan={2}>Symbol</th>
                  {observedWorkers.slice(0, 2).map((worker, index) => (
                    <th className={`manual-quote-account-${index + 1}`} colSpan={4} key={worker.worker_id}>{worker.login} on {worker.server}</th>
                  ))}
                </tr>
                <tr>
                  {observedWorkers.slice(0, 2).flatMap((worker, index) => [
                    <th className={`manual-quote-account-${index + 1}`} key={`${worker.worker_id}-bid`}>Bid</th>,
                    <th className={`manual-quote-account-${index + 1}`} key={`${worker.worker_id}-ask`}>Ask</th>,
                    <th className={`manual-quote-account-${index + 1}`} key={`${worker.worker_id}-last`}>Last</th>,
                    <th className={`manual-quote-account-${index + 1}`} key={`${worker.worker_id}-broker-time`}>Broker quote time</th>,
                  ])}
                </tr>
              </thead>
              <tbody>
                {visibleQuoteRows.length === 0 ? <tr><td colSpan={9}>No matching live quotes.</td></tr> : visibleQuoteRows.map((row) => (
                  <tr className={targetSymbols.has(row.symbol) ? 'manual-quote-target' : undefined} key={row.symbol}>
                    <td>{row.symbol}</td>
                    <QuoteCells quote={row.first} account={1} />
                    <QuoteCells quote={row.second} account={2} />
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </section>

    {preview ? <div className="snapshot-json-dialog-backdrop"><section aria-modal="true" className="snapshot-json-dialog" role="dialog">
      <h2>Confirm protected paired trade</h2>
      <p>{preview.plan.leg_order === 'buy_to_sell' ? 'Buy then Sell' : 'Sell then Buy'} with a {preview.plan.interval_seconds}-second interval.</p>
      <div className="console-table-scroll"><table className="console-table"><thead><tr><th>Direction</th><th>Worker</th><th>Lots</th><th>Quote estimate</th><th>SL estimate</th><th>TP estimate</th></tr></thead>
        <tbody>{preview.plan.legs.map((leg) => <tr key={leg.worker_id}><td>{leg.direction}</td><td>{leg.symbol}</td><td>{leg.lots}</td><td>{leg.estimated_entry_price}</td><td>{leg.estimated_stop_loss}</td><td>{leg.estimated_take_profit}</td></tr>)}</tbody>
      </table></div>
      <p>TP {entry.take_profit_pips} pips and SL {entry.stop_loss_pips} pips are required for both legs.</p>
      <button disabled={busy} type="button" onClick={() => void confirmEntry()}>Confirm protected entry</button>{' '}
      <button disabled={busy} type="button" onClick={() => setPreview(null)}>Back</button>
    </section></div> : null}
    {activePreview ? <div className="snapshot-json-dialog-backdrop"><section aria-modal="true" className="snapshot-json-dialog" role="dialog">
      <h2>Confirm manual-trade {activePreview.plan.operation === 'exit' ? 'exit' : 'protection update'}</h2>
      <div className="console-table-scroll"><table className="console-table"><thead><tr><th>Direction</th><th>Symbol</th><th>Position</th><th>Actual fill</th>{activePreview.plan.operation === 'protection' ? <><th>SL</th><th>TP</th></> : null}</tr></thead>
        <tbody>{activePreview.plan.legs.map((leg) => <tr key={leg.worker_id}><td>{leg.direction}</td><td>{leg.symbol}</td><td>{leg.position}</td><td>{leg.fill_price}</td>{activePreview.plan.operation === 'protection' ? <><td>{leg.stop_loss}</td><td>{leg.take_profit}</td></> : null}</tr>)}</tbody>
      </table></div>
      <button disabled={busy} type="button" onClick={() => void confirmTradeOperation()}>Confirm {activePreview.plan.operation === 'exit' ? 'exit' : 'protection update'}</button>{' '}
      <button disabled={busy} type="button" onClick={() => setActivePreview(null)}>Back</button>
    </section></div> : null}
  </section>
}

function QuoteCells({ quote, account }: { quote?: LiveQuote; account: 1 | 2 }) {
  const className = `manual-quote-account-${account}`
  return <>
    <td className={className}>{quote?.bid ?? '—'}</td>
    <td className={className}>{quote?.ask ?? '—'}</td>
    <td className={className}>{quote?.last || '—'}</td>
    <td className={className}>{quote ? formatTimestamp(quote.broker_time) : '—'}</td>
  </>
}
