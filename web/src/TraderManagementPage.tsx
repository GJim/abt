import { useEffect, useState } from 'react'

type PendingTrader = {
  registration_id: string
  strategy_name: string
  claimed_public_ip: string
  created_at: string
  expires_at: string
}

type Trader = {
  trader_id: string
  strategy_name: string
  status: string
  approved_at: string
  revoked_at: string | null
}

export function TraderManagementPage({ csrfToken }: { csrfToken: string }) {
  const [pending, setPending] = useState<PendingTrader[]>([])
  const [traders, setTraders] = useState<Trader[]>([])
  const [error, setError] = useState<string | null>(null)

  const load = async () => {
    setError(null)
    try {
      const [pendingResponse, tradersResponse] = await Promise.all([
        fetch('/api/admin/traders/enrollments', { credentials: 'same-origin' }),
        fetch('/api/admin/traders', { credentials: 'same-origin' }),
      ])
      if (!pendingResponse.ok || !tradersResponse.ok) throw new Error('Trader records could not be loaded.')
      setPending(await pendingResponse.json() as PendingTrader[])
      setTraders(await tradersResponse.json() as Trader[])
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Trader records could not be loaded.')
    }
  }

  useEffect(() => { void load() }, [])

  const act = async (path: string, fallback: string) => {
    setError(null)
    const response = await fetch(path, { method: 'POST', credentials: 'same-origin', headers: { 'X-CSRF-Token': csrfToken } })
    if (!response.ok) {
      const payload = await response.json().catch(() => null) as { detail?: string } | null
      setError(payload?.detail ?? fallback)
      return
    }
    await load()
  }

  return (
    <section aria-label="Trader management">
      {error ? <p className="error" role="alert">{error}</p> : null}
      <section aria-labelledby="pending-traders-heading">
        <h2 id="pending-traders-heading">Pending enrollment review</h2>
        {pending.length === 0 ? <p>No Trader registrations are awaiting review.</p> : <table className="console-table"><thead><tr><th>Strategy</th><th>Claimed public IP</th><th>Expires</th><th>Actions</th></tr></thead><tbody>
          {pending.map((trader) => <tr key={trader.registration_id}><td>{trader.strategy_name}</td><td>{trader.claimed_public_ip}</td><td>{formatDateTime(trader.expires_at)}</td><td>
            <button type="button" onClick={() => void act(`/api/admin/traders/enrollments/${encodeURIComponent(trader.registration_id)}/approve`, 'Trader approval failed.')}>Approve</button>{' '}
            <button className="reject-button" type="button" onClick={() => void act(`/api/admin/traders/enrollments/${encodeURIComponent(trader.registration_id)}/reject`, 'Trader rejection failed.')}>Reject</button>
          </td></tr>)}
        </tbody></table>}
      </section>
      <section aria-labelledby="trader-identities-heading">
        <h2 id="trader-identities-heading">Trader identities</h2>
        {traders.length === 0 ? <p>No Trader identities have been approved.</p> : <table className="console-table"><thead><tr><th>Strategy</th><th>Status</th><th>Approved</th><th>Action</th></tr></thead><tbody>
          {traders.map((trader) => <tr key={trader.trader_id}><td>{trader.strategy_name}</td><td><span className="console-status" data-state={trader.status}>{trader.status}</span></td><td>{formatDateTime(trader.approved_at)}</td><td>
            {trader.status === 'active' ? <button className="reject-button" type="button" onClick={() => void act(`/api/admin/traders/${encodeURIComponent(trader.trader_id)}/revoke`, 'Trader certificate revocation failed.')}>Revoke certificate</button> : '—'}
          </td></tr>)}
        </tbody></table>}
      </section>
    </section>
  )
}

function formatDateTime(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toISOString().slice(0, 19).replace('T', ' ')
}
