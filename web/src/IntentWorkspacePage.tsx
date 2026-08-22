import { useEffect, useState } from 'react'
import type { FormEvent } from 'react'

type ProductPair = { product_pair_id: string; endpoints: Array<{ server: string; symbol: string }> }
type Intent = {
  intent_id: string; origin: string; originator: string; pair_id: string; status: string; accepted_at: string; has_fill: boolean
  intent: { primary_direction: string; lots: string; entry_price: string; stop_loss_pips: string; take_profit_pips: string; filling_mode: string; expires_at: string }
}
type IntentRecord = Intent & { execution_records: Array<{ event_id: number; event_type: string; occurred_at: string; payload: Record<string, unknown> }> }
type PreflightOutcome = {
  worker_id: string; server?: string; status: string; order: { symbol: string; direction: string }
  response?: { diagnostics?: { retcode: number; comment?: string; quote?: { bid?: number; ask?: number; time?: number } } }
}
type RejectedPreflight = { status: 'rejected_preflight'; reason: string; preflight: PreflightOutcome[] }

type Draft = Omit<Intent['intent'], 'expires_at'> & { pair_id: string; expires_at: string }
const emptyDraft: Draft = { pair_id: '', primary_direction: 'LONG', lots: '', entry_price: '', stop_loss_pips: '', take_profit_pips: '', filling_mode: 'FOK', expires_at: '' }

export function IntentWorkspacePage({ csrfToken }: { csrfToken: string }) {
  const [intents, setIntents] = useState<Intent[]>([])
  const [pairs, setPairs] = useState<ProductPair[]>([])
  const [history, setHistory] = useState(false)
  const [statusFilter, setStatusFilter] = useState('')
  const [draft, setDraft] = useState<Draft>(emptyDraft)
  const [preview, setPreview] = useState<{ commandId: string; kind: 'create' | 'cancel'; intent?: Intent; draft?: Draft } | null>(null)
  const [rejectedPreflight, setRejectedPreflight] = useState<RejectedPreflight | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [timeline, setTimeline] = useState<IntentRecord | null>(null)

  const load = async () => {
    const [intentResponse, pairResponse] = await Promise.all([
      fetch(`/api/admin/intents?${new URLSearchParams({ active_only: String(!history), ...(statusFilter ? { status: statusFilter } : {}) })}`, { credentials: 'same-origin' }),
      fetch('/api/admin/product-pairs?status=active', { credentials: 'same-origin' }),
    ])
    if (!intentResponse.ok || !pairResponse.ok) throw new Error('Intent workspace data could not be loaded.')
    const pairPayload = await pairResponse.json() as ProductPair[] | { items: ProductPair[] }
    setIntents(await intentResponse.json() as Intent[])
    setPairs(Array.isArray(pairPayload) ? pairPayload : pairPayload.items)
  }
  useEffect(() => { void load().catch((caught) => setError(caught instanceof Error ? caught.message : 'Intent workspace data could not be loaded.')) }, [history, statusFilter])

  const submit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setPreview({ commandId: crypto.randomUUID(), kind: 'create', draft })
  }
  const confirm = async () => {
    if (!preview) return
    setError(null)
    setRejectedPreflight(null)
    const target = preview.kind === 'create' ? '/api/admin/intents' : `/api/admin/intents/${encodeURIComponent(preview.intent!.intent_id)}/cancel`
    const body = preview.kind === 'create'
      ? { type: 'intent', ...preview.draft, expires_at: new Date(preview.draft!.expires_at).toISOString(), command_id: preview.commandId }
      : { command_id: preview.commandId }
    const response = await fetch(target, { method: 'POST', credentials: 'same-origin', headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken }, body: JSON.stringify(body) })
    const payload = await response.json().catch(() => null) as (RejectedPreflight & { detail?: string }) | null
    if (!response.ok) {
      setError(payload?.detail ?? 'The management command was rejected.')
      return
    }
    if (preview.kind === 'create' && payload?.status === 'rejected_preflight') {
      setPreview(null)
      setRejectedPreflight(payload)
      return
    }
    setPreview(null)
    if (preview.kind === 'create') setDraft(emptyDraft)
    await load()
  }

  return <section aria-label="Intent workspace">
    <header className="console-page-header"><div><h1>Intents</h1><p>Create and intervene on Trader- and management-originated paired intents.</p></div><label><input checked={history} onChange={(event) => setHistory(event.target.checked)} type="checkbox" /> Show complete history</label><label>Status <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}><option value="">All statuses</option><option value="accepted">Accepted</option><option value="working">Working</option><option value="needs_human">Frozen</option><option value="cancelled">Cancelled</option></select></label></header>
    {error ? <p className="error" role="alert">{error}</p> : null}
    {rejectedPreflight ? <section className="intent-preflight-result" role="alert"><h2>Intent preflight rejected</h2><p>{rejectedPreflight.reason}</p><table className="console-table"><thead><tr><th>Endpoint</th><th>Order</th><th>Result</th><th>Broker diagnostic</th></tr></thead><tbody>{rejectedPreflight.preflight.map((outcome) => <tr key={outcome.worker_id}><td>{outcome.server ?? outcome.worker_id}</td><td>{outcome.order.direction} {outcome.order.symbol}</td><td>{outcome.status === 'accepted' ? 'Accepted' : 'Rejected'}</td><td>{outcome.response?.diagnostics ? <>{`Retcode ${outcome.response.diagnostics.retcode}`}{outcome.response.diagnostics.comment ? `: ${outcome.response.diagnostics.comment}` : ''}{outcome.response.diagnostics.quote ? <><br />Bid {outcome.response.diagnostics.quote.bid ?? '—'} / Ask {outcome.response.diagnostics.quote.ask ?? '—'}</> : null}</> : 'No broker diagnostic returned.'}</td></tr>)}</tbody></table><details><summary>View complete preflight evidence</summary><pre className="console-raw-detail">{JSON.stringify(rejectedPreflight.preflight, null, 2)}</pre></details><button type="button" onClick={() => setRejectedPreflight(null)}>Dismiss preflight result</button></section> : null}
    <form className="analysis-form launch-form" onSubmit={submit}>
      <h2>Create management intent</h2>
      <label>Active pair<select required value={draft.pair_id} onChange={(event) => setDraft({ ...draft, pair_id: event.target.value })}><option value="">Select an active pair</option>{pairs.map((pair) => <option key={pair.product_pair_id} value={pair.product_pair_id}>{pair.endpoints.map((endpoint) => `${endpoint.server}:${endpoint.symbol}`).join(' / ')}</option>)}</select></label>
      <label>Primary direction<select value={draft.primary_direction} onChange={(event) => setDraft({ ...draft, primary_direction: event.target.value })}><option>LONG</option><option>SHORT</option></select></label>
      <label>Lots<input min="0.00001" required step="any" type="number" value={draft.lots} onChange={(event) => setDraft({ ...draft, lots: event.target.value })} /></label>
      <label>Entry price<input min="0.00001" required step="any" type="number" value={draft.entry_price} onChange={(event) => setDraft({ ...draft, entry_price: event.target.value })} /></label>
      <label>Stop loss (pips)<input min="0.00001" required step="any" type="number" value={draft.stop_loss_pips} onChange={(event) => setDraft({ ...draft, stop_loss_pips: event.target.value })} /></label>
      <label>Take profit (pips)<input min="0.00001" required step="any" type="number" value={draft.take_profit_pips} onChange={(event) => setDraft({ ...draft, take_profit_pips: event.target.value })} /></label>
      <label>Filling mode<select value={draft.filling_mode} onChange={(event) => setDraft({ ...draft, filling_mode: event.target.value })}><option>FOK</option><option>IOC</option></select></label>
      <label>Absolute expiry<input required type="datetime-local" value={draft.expires_at} onChange={(event) => setDraft({ ...draft, expires_at: event.target.value })} /></label>
      <button type="submit">Preview intent</button>
    </form>
    <h2>{history ? 'Intent history' : 'Active intents'}</h2>
    {intents.length === 0 ? <p>No intents match this view.</p> : <table className="console-table"><thead><tr><th>Origin</th><th>Pair</th><th>Order</th><th>Status</th><th>Accepted</th><th>Action</th></tr></thead><tbody>{intents.map((intent) => <tr key={intent.intent_id}><td>{intent.origin}: {intent.originator}</td><td>{intent.pair_id}</td><td>{intent.intent.primary_direction} {intent.intent.lots} @ {intent.intent.entry_price}<br />{intent.intent.filling_mode}, SL {intent.intent.stop_loss_pips}, TP {intent.intent.take_profit_pips}</td><td>{intent.status}</td><td>{formatDateTime(intent.accepted_at)}</td><td><button type="button" onClick={() => void fetch(`/api/admin/intents/${encodeURIComponent(intent.intent_id)}`, { credentials: 'same-origin' }).then(async (response) => { if (!response.ok) throw new Error('Intent timeline could not be loaded.'); setTimeline(await response.json() as IntentRecord) }).catch((caught) => setError(caught instanceof Error ? caught.message : 'Intent timeline could not be loaded.'))}>Timeline</button>{' '}{!intent.has_fill && ['accepted', 'dispatching', 'working'].includes(intent.status) ? <button type="button" onClick={() => setPreview({ commandId: crypto.randomUUID(), kind: 'cancel', intent })}>Preview cancellation</button> : null}</td></tr>)}</tbody></table>}
    {preview ? <div className="snapshot-json-dialog-backdrop"><section aria-modal="true" className="snapshot-json-dialog" role="dialog"><h2>Confirm {preview.kind.replace('_', ' ')}</h2><p>Review the exact data below. This action is sent only after confirmation.</p><pre className="console-raw-detail">{JSON.stringify(preview.draft ?? preview.intent?.intent, null, 2)}</pre><button type="button" onClick={() => void confirm()}>Confirm {preview.kind.replace('_', ' ')}</button>{' '}<button type="button" onClick={() => setPreview(null)}>Back</button></section></div> : null}
    {timeline ? <div className="snapshot-json-dialog-backdrop"><section aria-modal="true" className="snapshot-json-dialog" role="dialog"><h2>Immutable intent timeline</h2>{timeline.execution_records.length === 0 ? <p>No execution events have been recorded.</p> : <ol>{timeline.execution_records.map((record) => <li key={record.event_id}><strong>{record.event_type}</strong> <time dateTime={record.occurred_at}>{formatDateTime(record.occurred_at)}</time><pre className="console-raw-detail">{JSON.stringify(record.payload, null, 2)}</pre></li>)}</ol>}<button type="button" onClick={() => setTimeline(null)}>Close</button></section></div> : null}
  </section>
}

function formatDateTime(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toISOString().slice(0, 19).replace('T', ' ')
}
