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
  leg_order: 'buy_to_sell' | 'sell_to_buy'
  interval_seconds: number
  revision: number
  active_manual_trade_id: string | null
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
type EntryDraft = {
  buy_worker_id: string
  sell_worker_id: string
  base_lots: string
  stop_loss_pips: string
  take_profit_pips: string
}
type ActiveOperationPlan = {
  manual_trade_id: string
  operation: 'exit' | 'protection'
  legs: Array<{ worker_id: string; symbol: string; direction: string; lots: string; position: string; fill_price: string; stop_loss?: string; take_profit?: string }>
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
  const [pairId, setPairId] = useState('')
  const [firstWorkerId, setFirstWorkerId] = useState('')
  const [secondWorkerId, setSecondWorkerId] = useState('')
  const [legOrder, setLegOrder] = useState<Target['leg_order']>('buy_to_sell')
  const [intervalSeconds, setIntervalSeconds] = useState('0')
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const [entry, setEntry] = useState<EntryDraft>({ buy_worker_id: '', sell_worker_id: '', base_lots: '', stop_loss_pips: '', take_profit_pips: '' })
  const [preview, setPreview] = useState<{ commandId: string; plan: ManualTradePlan } | null>(null)
  const [activePreview, setActivePreview] = useState<{ commandId: string; plan: ActiveOperationPlan; payload: Record<string, string> } | null>(null)
  const [protection, setProtection] = useState({ stop_loss_pips: '', take_profit_pips: '' })
  const [quoteSearch, setQuoteSearch] = useState('')
  const [showAllQuotes, setShowAllQuotes] = useState(false)

  useEffect(() => {
    void fetch('/api/admin/manual-trading-target', { credentials: 'same-origin' })
      .then(async (response) => {
        if (!response.ok) throw new Error('Manual-trading target could not be loaded.')
        const loaded = await response.json() as Target | null
        setTarget(loaded)
        if (loaded) {
          setPairId(loaded.pair.product_pair_id)
          setFirstWorkerId(loaded.workers[0]?.worker_id ?? '')
          setSecondWorkerId(loaded.workers[1]?.worker_id ?? '')
          setLegOrder(loaded.leg_order)
          setIntervalSeconds(String(loaded.interval_seconds))
          setEntry((current) => ({
            ...current,
            buy_worker_id: current.buy_worker_id || loaded.workers[0]?.worker_id || '',
            sell_worker_id: current.sell_worker_id || loaded.workers[1]?.worker_id || '',
          }))
        }
      })
      .catch((error: unknown) => setMessage(error instanceof Error ? error.message : 'Manual-trading target could not be loaded.'))
  }, [])

  const pair = useMemo(
    () => productPairs.find((candidate) => candidate.product_pair_id === pairId && candidate.status === 'active') ?? null,
    [pairId, productPairs],
  )
  const eligibleWorkers = (endpoint: Endpoint) => pair === null ? [] : pair.worker_applicability
    .filter((candidate) => candidate.server === endpoint.server && candidate.applicability_status === 'applicable')
    .map((candidate) => workers.find((worker) => worker.worker_id === candidate.worker_id))
    .filter((worker): worker is AccountWorker => worker !== undefined)
    .filter((worker) => worker.connectivity === 'connected' && worker.safety_state === 'connected' && worker.live_state?.connectivity === true)

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!pair || !firstWorkerId || !secondWorkerId) return
    setBusy(true)
    setMessage(null)
    try {
      const response = await fetch('/api/admin/manual-trading-target', {
        method: 'PUT',
        credentials: 'same-origin',
        headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({
          pair_id: pair.product_pair_id,
          first_worker_id: firstWorkerId,
          second_worker_id: secondWorkerId,
          leg_order: legOrder,
          interval_seconds: Number(intervalSeconds),
          expected_revision: target?.revision ?? 0,
        }),
      })
      const payload = await response.json() as Target | { detail?: string }
      if (!response.ok) throw new Error('detail' in payload ? payload.detail : 'Manual-trading target was rejected.')
      const configured = payload as Target
      setTarget(configured)
      setEntry((current) => ({
        ...current,
        buy_worker_id: configured.workers[0]?.worker_id ?? '',
        sell_worker_id: configured.workers[1]?.worker_id ?? '',
      }))
      setMessage(`Shared target saved at revision ${configured.revision}.`)
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
    const payload = {
      command_id: commandId,
      target_revision: target.revision,
      ...entry,
    }
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
      const refreshed = await fetch('/api/admin/manual-trading-target', { credentials: 'same-origin' })
      if (!refreshed.ok) throw new Error('Manual-trading target could not be refreshed.')
      setTarget(await refreshed.json() as Target)
      await onChanged()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Manual trade was rejected.')
    } finally {
      setBusy(false)
    }
  }

  async function previewActiveOperation(operation: ActiveOperationPlan['operation']) {
      if (!target) return
      setBusy(true)
      setMessage(null)
      const commandId = crypto.randomUUID()
      const payload = operation === 'protection' ? protection : {}
      try {
        const response = await fetch(`/api/admin/manual-trades/active/${operation}/preview`, {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
          body: JSON.stringify({ command_id: commandId, ...payload }),
        })
        const result = await response.json() as ActiveOperationPlan | { detail?: string }
        if (!response.ok) throw new Error('detail' in result ? result.detail : 'Manual-trade operation preview was rejected.')
        setActivePreview({ commandId, plan: result as ActiveOperationPlan, payload })
      } catch (error) {
        setMessage(error instanceof Error ? error.message : 'Manual-trade operation preview was rejected.')
      } finally {
        setBusy(false)
      }
    }

  async function confirmActiveOperation() {
      if (!activePreview) return
      setBusy(true)
      setMessage(null)
      try {
        const response = await fetch(`/api/admin/manual-trades/active/${activePreview.plan.operation}`, {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
          body: JSON.stringify({ command_id: activePreview.commandId, ...activePreview.payload }),
        })
        const result = await response.json() as { operation_id?: string; detail?: string }
        if (!response.ok) throw new Error(result.detail ?? 'Manual-trade operation was rejected.')
        setActivePreview(null)
        setMessage(`Manual-trade ${activePreview.plan.operation} operation ${result.operation_id ?? ''} was scheduled.`)
        const refreshed = await fetch('/api/admin/manual-trading-target', { credentials: 'same-origin' })
        if (!refreshed.ok) throw new Error('Manual-trading target could not be refreshed.')
        setTarget(await refreshed.json() as Target)
        await onChanged()
      } catch (error) {
        setMessage(error instanceof Error ? error.message : 'Manual-trade operation was rejected.')
      } finally {
        setBusy(false)
    }
  }

  const observedWorkers = useMemo(() => {
    const selected = pair === null ? [] : [
      { workerId: firstWorkerId, endpoint: pair.endpoints[0] },
      { workerId: secondWorkerId, endpoint: pair.endpoints[1] },
    ]
    if (selected.every(({ workerId }) => workerId)) {
      return selected.flatMap(({ workerId, endpoint }) => {
        const worker = workers.find((candidate) => candidate.worker_id === workerId)
        return worker ? [{ ...worker, endpoint }] : []
      })
    }
    return target?.workers.map((targetWorker) => ({
      ...(workers.find((worker) => worker.worker_id === targetWorker.worker_id) ?? targetWorker),
      endpoint: targetWorker.endpoint,
    })) ?? []
  }, [firstWorkerId, pair, secondWorkerId, target, workers])
  const quoteRows = useMemo(() => {
    const rows = new Map<string, { symbol: string; first?: LiveQuote; second?: LiveQuote }>()
    observedWorkers.slice(0, 2).forEach((worker, index) => {
      const quotes = worker.live_state?.quotes ?? []
      quotes.forEach((quote) => {
        const liveQuote = quote as LiveQuote
        const row = rows.get(liveQuote.symbol) ?? { symbol: liveQuote.symbol }
        if (index === 0) row.first = liveQuote
        else row.second = liveQuote
        rows.set(liveQuote.symbol, row)
      })
    })
    return [...rows.values()].sort((left, right) => left.symbol.localeCompare(right.symbol))
  }, [observedWorkers])
  const targetSymbols = useMemo(
    () => new Set((pair ?? target?.pair)?.endpoints.map((endpoint) => endpoint.symbol) ?? []),
    [pair, target],
  )
  const normalizedQuoteSearch = quoteSearch.trim().toLocaleLowerCase()
  const visibleQuoteRows = quoteRows.filter((row) => (
    normalizedQuoteSearch
      ? row.symbol.toLocaleLowerCase().includes(normalizedQuoteSearch)
      : showAllQuotes || targetSymbols.has(row.symbol)
  ))
  const additionalQuoteCount = quoteRows.filter((row) => !targetSymbols.has(row.symbol)).length
  return <section aria-labelledby="manual-trading-heading">
    {message ? <p role="status">{message}</p> : null}
    <form className="analysis-form launch-form" onSubmit={(event) => void save(event)}>
      <h2>Current target</h2>
      <label>Active product pair
        <select required value={pairId} onChange={(event) => {
          setPairId(event.target.value)
          setFirstWorkerId('')
          setSecondWorkerId('')
        }}>
          <option value="">Select an active product pair</option>
          {productPairs.filter((candidate) => candidate.status === 'active').map((candidate) => (
            <option key={candidate.product_pair_id} value={candidate.product_pair_id}>
              {candidate.endpoints.map((endpoint) => `${endpoint.server}:${endpoint.symbol}`).join(' / ')}
            </option>
          ))}
        </select>
      </label>
      {pair ? <>
        <label>Buy endpoint worker
          <select required value={firstWorkerId} onChange={(event) => setFirstWorkerId(event.target.value)}>
            <option value="">Select a connected applicable worker</option>
            {eligibleWorkers(pair.endpoints[0]).map((worker) => <option key={worker.worker_id} value={worker.worker_id}>{worker.login} on {worker.server}</option>)}
          </select>
        </label>
        <label>Sell endpoint worker
          <select required value={secondWorkerId} onChange={(event) => setSecondWorkerId(event.target.value)}>
            <option value="">Select a connected applicable worker</option>
            {eligibleWorkers(pair.endpoints[1]).map((worker) => <option key={worker.worker_id} value={worker.worker_id}>{worker.login} on {worker.server}</option>)}
          </select>
        </label>
      </> : null}
      <label>Dispatch order
        <select value={legOrder} onChange={(event) => setLegOrder(event.target.value as Target['leg_order'])}>
          <option value="buy_to_sell">Buy to Sell</option>
          <option value="sell_to_buy">Sell to Buy</option>
        </select>
      </label>
      <label>Leg interval (seconds)
        <input min="0" required step="1" type="number" value={intervalSeconds} onChange={(event) => setIntervalSeconds(event.target.value)} />
      </label>
      <button disabled={busy || (target !== null && target.active_manual_trade_id !== null)} type="submit">
        {busy ? 'Saving…' : 'Save shared target'}
      </button>
      {target?.active_manual_trade_id ? <p>The target is locked while its manual trade is active.</p> : null}
    </form>
    {target ? <form className="analysis-form launch-form" onSubmit={(event) => void previewEntry(event)}>
      <h2>Enter protected paired trade</h2>
      <p>Both legs require broker-managed TP and SL. The displayed prices are quote estimates; protections use each actual fill.</p>
      <label>Buy worker
        <select required value={entry.buy_worker_id} onChange={(event) => setEntry({ ...entry, buy_worker_id: event.target.value })}>
          {target.workers.map((worker) => <option key={worker.worker_id} value={worker.worker_id}>{worker.login} on {worker.server} — {worker.endpoint.symbol}</option>)}
        </select>
      </label>
      <label>Sell worker
        <select required value={entry.sell_worker_id} onChange={(event) => setEntry({ ...entry, sell_worker_id: event.target.value })}>
          {target.workers.map((worker) => <option key={worker.worker_id} value={worker.worker_id}>{worker.login} on {worker.server} — {worker.endpoint.symbol}</option>)}
        </select>
      </label>
      <label>Base lots<input min="0.00001" required step="any" type="number" value={entry.base_lots} onChange={(event) => setEntry({ ...entry, base_lots: event.target.value })} /></label>
      <label>Stop loss (pips)<input min="0.00001" required step="any" type="number" value={entry.stop_loss_pips} onChange={(event) => setEntry({ ...entry, stop_loss_pips: event.target.value })} /></label>
      <label>Take profit (pips)<input min="0.00001" required step="any" type="number" value={entry.take_profit_pips} onChange={(event) => setEntry({ ...entry, take_profit_pips: event.target.value })} /></label>
      <button disabled={busy || target.active_manual_trade_id !== null} type="submit">Preview protected entry</button>
    </form> : null}
    {target?.active_manual_trade_id ? <section className="analysis-form launch-form">
      <h2>Manage active paired trade</h2>
      <p>Review the broker-mapped values before operating both confirmed positions.</p>
      <button disabled={busy} type="button" onClick={() => void previewActiveOperation('exit')}>Preview full exit</button>
      <label>Updated stop loss (pips)<input min="0.00001" required step="any" type="number" value={protection.stop_loss_pips} onChange={(event) => setProtection({ ...protection, stop_loss_pips: event.target.value })} /></label>
      <label>Updated take profit (pips)<input min="0.00001" required step="any" type="number" value={protection.take_profit_pips} onChange={(event) => setProtection({ ...protection, take_profit_pips: event.target.value })} /></label>
      <button disabled={busy || !protection.stop_loss_pips || !protection.take_profit_pips} type="button" onClick={() => void previewActiveOperation('protection')}>Preview protection update</button>
    </section> : null}
    <h2>Live target state</h2>
    {observedWorkers.length === 0 ? <p>No manual-trading target is configured.</p> : <>
      <section className="manual-quote-panel" aria-label="Live quotes">
        <div className="manual-quote-toolbar">
          <label>Search symbols
            <input
              onChange={(event) => setQuoteSearch(event.target.value)}
              placeholder="Search symbols"
              type="search"
              value={quoteSearch}
            />
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
                  <th className={`manual-quote-account-${index + 1}`} colSpan={4} key={worker.worker_id}>
                    {worker.login} on {worker.server}
                  </th>
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
      {observedWorkers.map((worker) => (
      <section className="worker" key={worker.worker_id}>
        <h3>{worker.login} on {worker.server} — {worker.endpoint.symbol}</h3>
        <p>{worker.connectivity === 'connected' && worker.live_state?.connectivity ? 'Connected' : 'Not connected'}; safety state: {worker.safety_state}.</p>
        <Exposure activeManualTradeId={target?.active_manual_trade_id ?? null} title="Open orders" rows={worker.live_state?.orders ?? []} targetSymbol={worker.endpoint.symbol} />
        <Exposure activeManualTradeId={target?.active_manual_trade_id ?? null} title="Open positions" rows={worker.live_state?.positions ?? []} targetSymbol={worker.endpoint.symbol} />
      </section>
      ))}
    </>}
    {preview ? <div className="snapshot-json-dialog-backdrop"><section aria-modal="true" className="snapshot-json-dialog" role="dialog">
      <h2>Confirm protected paired trade</h2>
      <p>{preview.plan.leg_order === 'buy_to_sell' ? 'Buy then Sell' : 'Sell then Buy'} with a {preview.plan.interval_seconds}-second interval.</p>
      <table className="console-table"><thead><tr><th>Direction</th><th>Worker</th><th>Lots</th><th>Quote estimate</th><th>SL estimate</th><th>TP estimate</th></tr></thead>
        <tbody>{preview.plan.legs.map((leg) => <tr key={leg.worker_id}><td>{leg.direction}</td><td>{leg.symbol}</td><td>{leg.lots}</td><td>{leg.estimated_entry_price}</td><td>{leg.estimated_stop_loss}</td><td>{leg.estimated_take_profit}</td></tr>)}</tbody>
      </table>
      <p>TP {entry.take_profit_pips} pips and SL {entry.stop_loss_pips} pips are required for both legs.</p>
      <button disabled={busy} type="button" onClick={() => void confirmEntry()}>Confirm protected entry</button>{' '}
      <button disabled={busy} type="button" onClick={() => setPreview(null)}>Back</button>
    </section></div> : null}
    {activePreview ? <div className="snapshot-json-dialog-backdrop"><section aria-modal="true" className="snapshot-json-dialog" role="dialog">
      <h2>Confirm manual-trade {activePreview.plan.operation === 'exit' ? 'full exit' : 'protection update'}</h2>
      <table className="console-table"><thead><tr><th>Direction</th><th>Symbol</th><th>Position</th><th>Actual fill</th>{activePreview.plan.operation === 'protection' ? <><th>SL</th><th>TP</th></> : null}</tr></thead>
        <tbody>{activePreview.plan.legs.map((leg) => <tr key={leg.worker_id}><td>{leg.direction}</td><td>{leg.symbol}</td><td>{leg.position}</td><td>{leg.fill_price}</td>{activePreview.plan.operation === 'protection' ? <><td>{leg.stop_loss}</td><td>{leg.take_profit}</td></> : null}</tr>)}</tbody>
      </table>
      <button disabled={busy} type="button" onClick={() => void confirmActiveOperation()}>Confirm {activePreview.plan.operation === 'exit' ? 'full exit' : 'protection update'}</button>{' '}
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

function Exposure({
  activeManualTradeId,
  rows,
  targetSymbol,
  title,
}: {
  activeManualTradeId: string | null
  rows: Record<string, unknown>[]
  targetSymbol: string
  title: string
}) {
  return <section>
    <h4>{title}</h4>
    {rows.length === 0 ? <p>No {title.toLowerCase()}.</p> : <ul>
      {rows.map((row, index) => <li key={`${String(row.ticket ?? index)}`}>
        {String(row.ticket ?? 'Unticketed record')}: {String(row.symbol ?? 'unknown symbol')}
        {row.symbol === targetSymbol ? ' (current target product pair)' : ''}
        {activeManualTradeId !== null && row.manual_trade_id === activeManualTradeId ? ' (active manual trade)' : ''}
      </li>)}
    </ul>}
  </section>
}
