import { useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import './App.css'

type LoginResponse = {
  csrf_token: string
}

type AuditEvent = {
  event_id: number
  event_type: string
  occurred_at: string
}

type EnrollmentEvidence = Record<string, unknown>

type Enrollment = {
  enrollment_id: string
  login: number
  server: string
  pairing_code: string
  created_at: string
  expires_at: string
  account_info: EnrollmentEvidence
  terminal_info: EnrollmentEvidence
}

type EnrollmentAction = 'approve' | 'reject'

type ReconciliationDelta = {
  cursor: number
  observed_at: string
  entity: string
  ticket: string
  change: string
  record: Record<string, unknown>
}

type AccountWorker = {
  worker_id: string
  login: number
  server: string
  connectivity: string
  safety_state: string
  latest_snapshot: {
    cursor: number
    observed_at: string
    account: EnrollmentEvidence
    terminal: EnrollmentEvidence
    orders: EnrollmentEvidence[]
    positions: EnrollmentEvidence[]
  } | null
  deltas: ReconciliationDelta[]
}

type WorkerAlert = {
  alert_id: number
  worker_id: string
  priority: string
  alert_type: string
  reason: string
  occurred_at: string
}

function App() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [csrfToken, setCsrfToken] = useState<string | null>(null)
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [enrollments, setEnrollments] = useState<Enrollment[]>([])
  const [workers, setWorkers] = useState<AccountWorker[]>([])
  const [alerts, setAlerts] = useState<WorkerAlert[]>([])
  const [processingEnrollmentId, setProcessingEnrollmentId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function refreshManagementData() {
    const [eventsResponse, enrollmentsResponse, workersResponse, alertsResponse] = await Promise.all([
      fetch('/api/admin/events', { credentials: 'same-origin' }),
      fetch('/api/admin/enrollments', { credentials: 'same-origin' }),
      fetch('/api/admin/workers', { credentials: 'same-origin' }),
      fetch('/api/admin/alerts', { credentials: 'same-origin' }),
    ])
    if (!eventsResponse.ok || !enrollmentsResponse.ok || !workersResponse.ok || !alertsResponse.ok) {
      throw new Error('Management data could not be loaded.')
    }

    const [eventPayload, enrollmentPayload, workerPayload, alertPayload] = await Promise.all([
      eventsResponse.json() as Promise<AuditEvent[]>,
      enrollmentsResponse.json() as Promise<Enrollment[]>,
      workersResponse.json() as Promise<AccountWorker[]>,
      alertsResponse.json() as Promise<WorkerAlert[]>,
    ])
    setEvents(eventPayload)
    setEnrollments(enrollmentPayload)
    setWorkers(workerPayload)
    setAlerts(alertPayload)
  }

  useEffect(() => {
    async function resumeSession() {
      try {
        const response = await fetch('/api/admin/session', { credentials: 'same-origin' })
        if (!response.ok) {
          return
        }
        const payload = (await response.json()) as LoginResponse
        setCsrfToken(payload.csrf_token)
        await refreshManagementData()
      } catch {
        setError('Your session could not be restored. Please sign in again.')
      }
    }

    void resumeSession()
  }, [])

  async function revokeWorker(workerId: string) {
    if (!csrfToken) {
      return
    }
    setError(null)
    try {
      const response = await fetch(`/api/admin/workers/${workerId}/revoke`, {
        method: 'POST',
        headers: { 'X-CSRF-Token': csrfToken },
        credentials: 'same-origin',
      })
      if (!response.ok) {
        throw new Error('Certificate revocation failed.')
      }
      await refreshManagementData()
    } catch {
      setError('Could not revoke this worker certificate. Please try again.')
    }
  }

  async function submitLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)

    try {
      const response = await fetch('/api/admin/login', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ username, password }),
      })
      if (!response.ok) {
        setError('Sign-in failed. Check your credentials or contact a console-host operator.')
        return
      }
      const payload = (await response.json()) as LoginResponse
      setCsrfToken(payload.csrf_token)
      await refreshManagementData()
    } catch {
      setError('Signed in, but management data could not be loaded.')
    }
  }

  async function reviewEnrollment(enrollmentId: string, action: EnrollmentAction) {
    if (!csrfToken) {
      return
    }

    setError(null)
    setProcessingEnrollmentId(enrollmentId)
    try {
      const response = await fetch(`/api/admin/enrollments/${enrollmentId}/${action}`, {
        method: 'POST',
        headers: { 'X-CSRF-Token': csrfToken },
        credentials: 'same-origin',
      })
      if (!response.ok) {
        const payload = await response.json() as { detail?: unknown }
        throw new Error(typeof payload.detail === 'string' ? payload.detail : 'Enrollment review failed.')
      }
      await refreshManagementData()
    } catch (error) {
      const detail = error instanceof Error ? error.message : 'Enrollment review failed.'
      setError(`Could not ${action} this worker registration: ${detail}`)
    } finally {
      setProcessingEnrollmentId(null)
    }
  }

  if (csrfToken) {
    return (
      <main className="management">
        <div className="management-content">
          <header>
            <p className="eyebrow">ABT control plane</p>
            <h1>Audit events</h1>
            <p>Your management session is active.</p>
          </header>
          {error && <p className="error" role="alert">{error}</p>}
          <section aria-labelledby="worker-alerts-heading">
            <h2 id="worker-alerts-heading">Worker alerts</h2>
            {alerts.length === 0 ? <p>No worker alerts.</p> : (
              <ul className="worker-alerts" aria-label="Worker alerts">
                {alerts.map((alert) => (
                  <li key={alert.alert_id}>
                    <strong>{alert.priority}: {alert.alert_type}</strong>
                    <span>{alert.reason}</span>
                    <time dateTime={alert.occurred_at}>{alert.occurred_at}</time>
                  </li>
                ))}
              </ul>
            )}
          </section>
          <section aria-labelledby="pending-enrollments-heading">
            <h2 id="pending-enrollments-heading">Pending worker registrations</h2>
            {enrollments.length === 0 ? (
              <p>No worker registrations are awaiting review.</p>
            ) : (
              <ul className="enrollment-list" aria-label="Pending worker registrations">
                {enrollments.map((enrollment) => {
                  const isProcessing = processingEnrollmentId === enrollment.enrollment_id
                  const registrationName = `${enrollment.login} on ${enrollment.server}`

                  return (
                    <li key={enrollment.enrollment_id} className="enrollment">
                      <dl>
                        <div><dt>Login</dt><dd>{enrollment.login}</dd></div>
                        <div><dt>Server</dt><dd>{enrollment.server}</dd></div>
                        <div><dt>Pairing code</dt><dd>{enrollment.pairing_code}</dd></div>
                        <div><dt>Created</dt><dd><time dateTime={enrollment.created_at}>{enrollment.created_at}</time></dd></div>
                        <div><dt>Expires</dt><dd><time dateTime={enrollment.expires_at}>{enrollment.expires_at}</time></dd></div>
                      </dl>
                      <div className="enrollment-evidence">
                        <section aria-label={`Account information for ${registrationName}`}>
                          <h3>Account information</h3>
                          <pre>{JSON.stringify(enrollment.account_info, null, 2)}</pre>
                        </section>
                        <section aria-label={`Terminal information for ${registrationName}`}>
                          <h3>Terminal information</h3>
                          <pre>{JSON.stringify(enrollment.terminal_info, null, 2)}</pre>
                        </section>
                      </div>
                      <div className="enrollment-actions">
                        <button
                          aria-label={`Approve registration for ${registrationName}`}
                          disabled={isProcessing}
                          onClick={() => void reviewEnrollment(enrollment.enrollment_id, 'approve')}
                          type="button"
                        >
                          Approve
                        </button>
                        <button
                          aria-label={`Reject registration for ${registrationName}`}
                          className="reject-button"
                          disabled={isProcessing}
                          onClick={() => void reviewEnrollment(enrollment.enrollment_id, 'reject')}
                          type="button"
                        >
                          Reject
                        </button>
                      </div>
                    </li>
                  )
                })}
              </ul>
            )}
          </section>
          <section aria-labelledby="account-workers-heading">
            <h2 id="account-workers-heading">Account workers</h2>
            {workers.length === 0 ? (
              <p>No approved account workers have reported yet.</p>
            ) : (
              <ul className="worker-list" aria-label="Account workers">
                {workers.map((worker) => (
                  <li key={worker.worker_id} className="worker">
                    <dl>
                      <div><dt>Account</dt><dd>{worker.login} on {worker.server}</dd></div>
                      <div><dt>Connectivity</dt><dd>{worker.connectivity}</dd></div>
                      <div><dt>Safety state</dt><dd>{worker.safety_state}</dd></div>
                    </dl>
                    {worker.connectivity !== 'revoked' && (
                      <button
                        aria-label={`Revoke certificate for ${worker.login} on ${worker.server}`}
                        className="reject-button"
                        onClick={() => void revokeWorker(worker.worker_id)}
                        type="button"
                      >
                        Revoke certificate
                      </button>
                    )}
                    {worker.latest_snapshot && (
                      <section aria-label={`Latest snapshot for ${worker.login} on ${worker.server}`}>
                        <h3>Latest snapshot</h3>
                        <time dateTime={worker.latest_snapshot.observed_at}>{worker.latest_snapshot.observed_at}</time>
                        <pre>{JSON.stringify({
                          account: worker.latest_snapshot.account,
                          terminal: worker.latest_snapshot.terminal,
                          orders: worker.latest_snapshot.orders,
                          positions: worker.latest_snapshot.positions,
                        }, null, 2)}</pre>
                      </section>
                    )}
                    <section aria-label={`Lifecycle deltas for ${worker.login} on ${worker.server}`}>
                      <h3>Lifecycle deltas</h3>
                      {worker.deltas.length === 0 ? <p>No lifecycle or volume changes reported.</p> : (
                        <ul className="worker-deltas">
                          {worker.deltas.map((delta) => (
                            <li key={delta.cursor}>
                              <strong>{delta.change}</strong> {delta.entity} {delta.ticket}
                              <time dateTime={delta.observed_at}>{delta.observed_at}</time>
                            </li>
                          ))}
                        </ul>
                      )}
                    </section>
                  </li>
                ))}
              </ul>
            )}
          </section>
          <section aria-label="Audit events">
            <ul className="audit-events" aria-label="Audit events">
              {events.map((event) => (
                <li key={event.event_id}>
                  <strong>{event.event_type}</strong>
                  <time dateTime={event.occurred_at}>{event.occurred_at}</time>
                </li>
              ))}
            </ul>
          </section>
        </div>
      </main>
    )
  }

  return (
    <main className="management">
      <section className="login-card" aria-labelledby="management-heading">
        <p className="eyebrow">ABT control plane</p>
        <h1 id="management-heading">Management access</h1>
        <p className="description">Sign in with a console-host generated administrator account.</p>
        <form onSubmit={submitLogin}>
          <label>
            Administrator account
            <input
              autoCapitalize="characters"
              autoComplete="username"
              maxLength={6}
              onChange={(event) => setUsername(event.target.value.toUpperCase())}
              pattern="[A-Z]{6}"
              required
              value={username}
            />
          </label>
          <label>
            Password
            <input
              autoComplete="current-password"
              minLength={20}
              onChange={(event) => setPassword(event.target.value)}
              required
              type="password"
              value={password}
            />
          </label>
          {error && <p className="error" role="alert">{error}</p>}
          <button type="submit">Sign in</button>
        </form>
      </section>
    </main>
  )
}

export default App
