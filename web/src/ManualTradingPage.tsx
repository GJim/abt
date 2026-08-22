import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import type { AccountWorker } from './App'

type Endpoint = { server: string; symbol: string }
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
  return <section aria-labelledby="manual-trading-heading">
    <header className="console-page-header">
      <div>
        <h1 id="manual-trading-heading">Manual trading</h1>
        <p>Configure the shared target, then review both accounts before any paired-trade command.</p>
      </div>
    </header>
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
    <h2>Live target state</h2>
    {observedWorkers.length === 0 ? <p>No manual-trading target is configured.</p> : observedWorkers.map((worker) => (
      <section className="worker" key={worker.worker_id}>
        <h3>{worker.login} on {worker.server} — {worker.endpoint.symbol}</h3>
        <p>{worker.connectivity === 'connected' && worker.live_state?.connectivity ? 'Connected' : 'Not connected'}; safety state: {worker.safety_state}.</p>
        <table className="console-table">
          <thead><tr><th>Symbol</th><th>Bid</th><th>Ask</th><th>Broker quote time</th><th>Controller receipt time</th></tr></thead>
          <tbody>{worker.live_state?.quotes.map((quote) => <tr key={quote.symbol}><td>{quote.symbol}</td><td>{quote.bid}</td><td>{quote.ask}</td><td>{quote.broker_time}</td><td>{(quote as { controller_received_at?: string }).controller_received_at ?? '—'}</td></tr>) ?? <tr><td colSpan={5}>No live quotes.</td></tr>}</tbody>
        </table>
        <Exposure activeManualTradeId={target?.active_manual_trade_id ?? null} title="Open orders" rows={worker.live_state?.orders ?? []} targetSymbol={worker.endpoint.symbol} />
        <Exposure activeManualTradeId={target?.active_manual_trade_id ?? null} title="Open positions" rows={worker.live_state?.positions ?? []} targetSymbol={worker.endpoint.symbol} />
      </section>
    ))}
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
  </section>
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
