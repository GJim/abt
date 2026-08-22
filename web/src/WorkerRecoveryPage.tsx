import { useMemo, useState } from 'react'
import type { AccountWorker } from './App'

type WorkerRecoveryPageProps = {
  csrfToken: string
  onChanged: () => Promise<void>
  workers: AccountWorker[]
}

export function WorkerRecoveryPage({ csrfToken, onChanged, workers }: WorkerRecoveryPageProps) {
  const [source, setSource] = useState('')
  const [candidate, setCandidate] = useState<AccountWorker | null>(null)
  const [operation, setOperation] = useState<'cleanup' | 'release' | null>(null)
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState<string | null>(null)
  const frozenWorkers = useMemo(
    () => workers.filter((worker) => worker.safety_state === 'frozen' && (!source || worker.freeze?.source === source)),
    [source, workers],
  )
  const sources = useMemo(
    () => [...new Set(workers.flatMap((worker) => worker.safety_state === 'frozen' && worker.freeze ? [worker.freeze.source] : []))],
    [workers],
  )

  async function confirm() {
    if (!candidate || !operation) return
    setBusy(true)
    setMessage(null)
    try {
      const response = await fetch(`/api/admin/workers/${encodeURIComponent(candidate.worker_id)}/${operation}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
        credentials: 'same-origin',
        body: JSON.stringify({ command_id: crypto.randomUUID() }),
      })
      if (!response.ok) throw new Error((await response.json() as { detail?: string }).detail ?? 'Recovery request failed.')
      const result = await response.json() as { operation_id: string }
      setMessage(`${operation === 'cleanup' ? 'Cleanup' : 'Release'} requested for ${candidate.login} on ${candidate.server} (${result.operation_id}).`)
      setCandidate(null)
      setOperation(null)
      await onChanged()
    } catch (error) {
      setMessage(error instanceof Error ? error.message : 'Recovery request failed.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <section aria-labelledby="recovery-heading">
      <header className="console-page-header">
        <div>
          <h1 id="recovery-heading">Worker recovery</h1>
          <p>Cancel every pending order, prove cancellation, close positions, and independently release only empty accounts.</p>
        </div>
      </header>
      {message ? <p role="status">{message}</p> : null}
      <label>
        Freeze source
        <select aria-label="Freeze source" onChange={(event) => setSource(event.target.value)} value={source}>
          <option value="">All freeze sources</option>
          {sources.map((value) => <option key={value} value={value}>{value.replaceAll('_', ' ')}</option>)}
        </select>
      </label>
      {frozenWorkers.length === 0 ? <p>No frozen workers match this filter.</p> : (
        <ul className="worker-list" aria-label="Frozen workers">
          {frozenWorkers.map((worker) => {
            const state = worker.live_state ?? null
            const empty = state !== null && state.orders.length === 0 && state.positions.length === 0
            return (
              <li className="worker" key={worker.worker_id}>
                <strong>{worker.login} on {worker.server}</strong>
                <p>Frozen by {worker.freeze?.source.replaceAll('_', ' ')}.</p>
                <p>{state ? `${state.orders.length} pending orders; ${state.positions.length} positions.` : 'Awaiting broker-visible state.'}</p>
                <pre className="console-raw-detail">{JSON.stringify({ orders: state?.orders ?? [], positions: state?.positions ?? [] }, null, 2)}</pre>
                <div className="action-row">
                  <button onClick={() => { setCandidate(worker); setOperation('cleanup') }} type="button">Preview cleanup</button>
                  <button disabled={!empty} onClick={() => { setCandidate(worker); setOperation('release') }} type="button">Release worker</button>
                </div>
              </li>
            )
          })}
        </ul>
      )}
      {candidate && operation ? (
        <section aria-label="Confirm worker recovery" className="worker-revoke-confirmation">
          <h2>{operation === 'cleanup' ? 'Confirm account cleanup' : 'Confirm independent release'} for {candidate.login} on {candidate.server}?</h2>
          <p>{operation === 'cleanup'
            ? 'All pending orders will be cancelled before every broker position is closed. The worker remains frozen if any step is uncertain.'
            : 'A fresh broker reconciliation must prove zero pending orders and zero positions before this worker becomes available.'}</p>
          <div className="action-row">
            <button disabled={busy} onClick={() => void confirm()} type="button">{busy ? 'Submitting…' : `Confirm ${operation}`}</button>
            <button disabled={busy} onClick={() => { setCandidate(null); setOperation(null) }} type="button">Back</button>
          </div>
        </section>
      ) : null}
    </section>
  )
}
