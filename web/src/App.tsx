import { useState } from 'react'
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
  source_ip: string
  created_at: string
  expires_at: string
  account_info: EnrollmentEvidence
  terminal_info: EnrollmentEvidence
}

type EnrollmentAction = 'approve' | 'reject'

function App() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [csrfToken, setCsrfToken] = useState<string | null>(null)
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [enrollments, setEnrollments] = useState<Enrollment[]>([])
  const [processingEnrollmentId, setProcessingEnrollmentId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function refreshManagementData() {
    const [eventsResponse, enrollmentsResponse] = await Promise.all([
      fetch('/api/admin/events', { credentials: 'same-origin' }),
      fetch('/api/admin/enrollments', { credentials: 'same-origin' }),
    ])
    if (!eventsResponse.ok || !enrollmentsResponse.ok) {
      throw new Error('Management data could not be loaded.')
    }

    const [eventPayload, enrollmentPayload] = await Promise.all([
      eventsResponse.json() as Promise<AuditEvent[]>,
      enrollmentsResponse.json() as Promise<Enrollment[]>,
    ])
    setEvents(eventPayload)
    setEnrollments(enrollmentPayload)
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
        throw new Error('Enrollment review failed.')
      }
      await refreshManagementData()
    } catch {
      setError(`Could not ${action} this worker registration. Please try again.`)
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
                        <div><dt>Source IP</dt><dd>{enrollment.source_ip}</dd></div>
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
