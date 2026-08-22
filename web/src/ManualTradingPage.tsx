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
