import { useEffect, useState } from 'react'

type RegistrationInvite = {
  invite_id: string
  role: 'trader' | 'worker'
  issued_by: string
  issued_at: string
  expires_at: string
  status: string
  used_at: string | null
  revoked_at: string | null
}

export function RegistrationInvitesPage({ csrfToken }: { csrfToken: string }) {
  const [invites, setInvites] = useState<RegistrationInvite[]>([])
  const [role, setRole] = useState<RegistrationInvite['role']>('worker')
  const [revealedInvite, setRevealedInvite] = useState<string | null>(null)
  const [copyStatus, setCopyStatus] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = async () => {
    setError(null)
    try {
      const response = await fetch('/api/admin/registration-invites', { credentials: 'same-origin' })
      if (!response.ok) throw new Error('Registration invites could not be loaded.')
      setInvites(await response.json() as RegistrationInvite[])
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Registration invites could not be loaded.')
    }
  }

  useEffect(() => { void load() }, [])

  const issue = async () => {
    setError(null)
    try {
      const response = await fetch('/api/admin/registration-invites', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'content-type': 'application/json', 'X-CSRF-Token': csrfToken },
        body: JSON.stringify({ role }),
      })
      if (!response.ok) throw new Error(await responseDetail(response, 'Invite issuance failed.'))
      const created = await response.json() as RegistrationInvite & { invite: string }
      setRevealedInvite(created.invite)
      setCopyStatus(null)
      await load()
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Invite issuance failed.')
    }
  }

  const copyInvite = async () => {
    if (!revealedInvite) return
    try {
      await navigator.clipboard.writeText(revealedInvite)
      setCopyStatus('Invite copied to clipboard.')
    } catch {
      setCopyStatus('Copy failed. Select the invite value and copy it manually.')
    }
  }

  const revoke = async (invite: RegistrationInvite) => {
    setError(null)
    try {
      const response = await fetch(`/api/admin/registration-invites/${encodeURIComponent(invite.invite_id)}/revoke`, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'X-CSRF-Token': csrfToken },
      })
      if (!response.ok) throw new Error(await responseDetail(response, 'Invite revocation failed.'))
      await load()
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Invite revocation failed.')
    }
  }

  return (
    <section aria-label="Registration invite management">
      {error ? <p className="error" role="alert">{error}</p> : null}
      <section aria-labelledby="issue-registration-invite" className="registration-invite-issue">
        <h2 id="issue-registration-invite">Issue invite</h2>
        <label>Role
          <select value={role} onChange={(event) => setRole(event.target.value as RegistrationInvite['role'])}>
            <option value="worker">Worker</option>
            <option value="trader">Trader</option>
          </select>
        </label>{' '}
        <button type="button" onClick={() => void issue()}>Issue invite</button>
      </section>
      <section aria-labelledby="registration-invite-history" className="registration-invite-history">
        <h2 id="registration-invite-history">Invite history</h2>
        {invites.length === 0 ? <p>No registration invites have been issued.</p> : (
          <table className="console-table">
            <thead><tr><th>Role</th><th>Status</th><th>Issued</th><th>Expires</th><th>Action</th></tr></thead>
            <tbody>{invites.map((invite) => <tr key={invite.invite_id}>
              <td>{invite.role}</td><td>{invite.status}</td><td>{formatDateTime(invite.issued_at)}</td><td>{formatDateTime(invite.expires_at)}</td>
              <td>{invite.status === 'active' ? <button type="button" className="reject-button" aria-label={`Revoke invite for ${invite.role}`} onClick={() => void revoke(invite)}>Revoke</button> : '—'}</td>
            </tr>)}</tbody>
          </table>
        )}
      </section>
      {revealedInvite ? (
        <div className="snapshot-json-dialog-backdrop">
          <section aria-modal="true" className="snapshot-json-dialog" role="dialog" aria-label="Registration invite issued">
            <h2>Registration invite issued</h2>
            <p>Copy this value now. It cannot be displayed again.</p>
            <div className="invite-secret">
              <pre className="console-raw-detail">{revealedInvite}</pre>
              <button aria-label="Copy invite code" className="console-icon-button" onClick={() => void copyInvite()} title="Copy invite code" type="button">
                <svg aria-hidden="true" fill="none" height="18" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" width="18">
                  <rect height="13" rx="2" width="13" x="8" y="8" />
                  <path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3" />
                </svg>
              </button>
            </div>
            {copyStatus ? <p role="status">{copyStatus}</p> : null}
            <button type="button" onClick={() => { setRevealedInvite(null); setCopyStatus(null) }}>I have saved this invite</button>
          </section>
        </div>
      ) : null}
    </section>
  )
}

async function responseDetail(response: Response, fallback: string): Promise<string> {
  const payload = await response.json().catch(() => null) as { detail?: string } | null
  return payload?.detail ?? fallback
}

function formatDateTime(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toISOString().slice(0, 19).replace('T', ' ')
}
