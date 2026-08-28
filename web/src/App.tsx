import { useCallback, useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { AuditEventsPage } from './AuditEventsPage'
import { RegistrationInvitesPage } from './RegistrationInvitesPage'
import { TraderManagementPage } from './TraderManagementPage'
import { WorkerManagementPage } from './WorkerManagementPage'
import './App.css'
import './Console.css'

type LoginResponse = {
  csrf_token: string
}

type EnrollmentEvidence = Record<string, unknown>

export type Enrollment = {
  enrollment_id: string
  login: number
  server: string
  created_at: string
  expires_at: string
  account_info: EnrollmentEvidence
  terminal_info: EnrollmentEvidence
}

export type AccountWorker = {
  worker_id: string
  login: number
  server: string
  connectivity: string
  last_seen_at: string | null
}

export type WorkerAlert = {
  alert_id: number
  worker_id: string | null
  enrollment_id?: string | null
  priority: string
  alert_type: string
  reason: string
  occurred_at: string
}

export type InterventionItem =
  | {
    id: string
    kind: string
    reason: string
    occurredAt: string
    priorityRank: number
    enrollment: Enrollment
  }
  | {
    id: string
    kind: string
    reason: string
    occurredAt: string
    priorityRank: number
    alert: WorkerAlert
  }

type ConsolePage = 'main' | 'workers' | 'invites' | 'traders' | 'audit'

function App() {
  const location = useLocation()
  const navigate = useNavigate()
  const page = readConsolePage(location.pathname)
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [csrfToken, setCsrfToken] = useState<string | null>(null)
  const [enrollments, setEnrollments] = useState<Enrollment[]>([])
  const [workers, setWorkers] = useState<AccountWorker[]>([])
  const [alerts, setAlerts] = useState<WorkerAlert[]>([])
  const [processingEnrollmentId, setProcessingEnrollmentId] = useState<string | null>(null)
  const [isRestoringSession, setIsRestoringSession] = useState(true)
  const [isSigningIn, setIsSigningIn] = useState(false)
  const [isSigningOut, setIsSigningOut] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const endSession = useCallback((message: string | null) => {
    setCsrfToken(null)
    setEnrollments([])
    setWorkers([])
    setAlerts([])
    setError(message)
  }, [])

  const refreshManagementData = useCallback(async () => {
    const responses = await Promise.all([
      fetch('/api/admin/enrollments', { credentials: 'same-origin' }),
      fetch('/api/admin/workers', { credentials: 'same-origin' }),
      fetch('/api/admin/alerts', { credentials: 'same-origin' }),
    ])
    if (responses.some((response) => response.status === 401)) {
      endSession('Your session has expired. Please sign in again.')
      return
    }
    if (responses.some((response) => !response.ok)) {
      throw new Error('Control-plane identity and Worker data could not be loaded.')
    }
    const [enrollmentPayload, workerPayload, alertPayload] = await Promise.all(
      responses.map((response) => response.json()),
    )
    if (!Array.isArray(enrollmentPayload) || !Array.isArray(workerPayload) || !Array.isArray(alertPayload)) {
      throw new Error('The control plane returned invalid management data.')
    }
    setEnrollments(enrollmentPayload as Enrollment[])
    setWorkers(workerPayload as AccountWorker[])
    setAlerts(alertPayload as WorkerAlert[])
  }, [endSession])

  useEffect(() => {
    let cancelled = false
    async function restore() {
      try {
        const response = await fetch('/api/admin/session', { credentials: 'same-origin' })
        if (!response.ok) {
          if (!cancelled) endSession(null)
          return
        }
        const payload = await response.json() as LoginResponse
        if (!cancelled) {
          setCsrfToken(payload.csrf_token)
          await refreshManagementData()
        }
      } catch {
        if (!cancelled) endSession('The console could not restore your session. Please sign in again.')
      } finally {
        if (!cancelled) setIsRestoringSession(false)
      }
    }
    void restore()
    return () => {
      cancelled = true
    }
  }, [endSession, refreshManagementData])

  useEffect(() => {
    if (!csrfToken) return
    const interval = window.setInterval(() => {
      if (document.visibilityState === 'visible') {
        void refreshManagementData().catch(() => setError('Control-plane data refresh failed.'))
      }
    }, 10_000)
    return () => window.clearInterval(interval)
  }, [csrfToken, refreshManagementData])

  const interventionQueue = useMemo<InterventionItem[]>(() => {
    const pending = enrollments.map((enrollment) => ({
      id: `enrollment-${enrollment.enrollment_id}`,
      kind: 'Pending worker registration',
      reason: `${enrollment.login} on ${enrollment.server} requires review.`,
      occurredAt: enrollment.created_at,
      priorityRank: 1,
      enrollment,
    }))
    const workerAlerts = alerts.map((alert) => ({
      id: `alert-${alert.alert_id}`,
      kind: alert.alert_type,
      reason: alert.reason,
      occurredAt: alert.occurred_at,
      priorityRank: alert.priority === 'high' ? 0 : 2,
      alert,
    }))
    return [...pending, ...workerAlerts].sort(
      (first, second) => first.priorityRank - second.priorityRank
        || second.occurredAt.localeCompare(first.occurredAt),
    )
  }, [alerts, enrollments])

  async function signIn(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)
    setIsSigningIn(true)
    try {
      const response = await fetch('/api/admin/login', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ username, password }),
      })
      if (!response.ok) throw new Error('Invalid administrator credentials.')
      const payload = await response.json() as LoginResponse
      setCsrfToken(payload.csrf_token)
      setPassword('')
      await refreshManagementData()
    } catch (caught) {
      endSession(caught instanceof Error ? caught.message : 'Sign in failed.')
    } finally {
      setIsSigningIn(false)
    }
  }

  async function signOut() {
    if (!csrfToken) {
      endSession(null)
      return
    }
    setIsSigningOut(true)
    try {
      const response = await fetch('/api/admin/logout', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'X-CSRF-Token': csrfToken },
      })
      if (!response.ok && response.status !== 401) throw new Error('Could not sign out.')
      endSession(null)
      navigate('/')
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Could not sign out.')
    } finally {
      setIsSigningOut(false)
    }
  }

  async function reviewEnrollment(enrollmentId: string, action: 'approve' | 'reject') {
    if (!csrfToken) return
    setProcessingEnrollmentId(enrollmentId)
    setError(null)
    try {
      const response = await fetch(`/api/admin/enrollments/${encodeURIComponent(enrollmentId)}/${action}`, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'X-CSRF-Token': csrfToken },
      })
      if (!response.ok) throw new Error(`Worker registration ${action} failed.`)
      await refreshManagementData()
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : `Worker registration ${action} failed.`)
    } finally {
      setProcessingEnrollmentId(null)
    }
  }

  async function revokeWorker(workerId: string): Promise<boolean> {
    if (!csrfToken) return false
    setError(null)
    try {
      const response = await fetch(`/api/admin/workers/${encodeURIComponent(workerId)}/revoke`, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'X-CSRF-Token': csrfToken },
      })
      if (!response.ok) throw new Error('Certificate revocation failed.')
      await refreshManagementData()
      return true
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Certificate revocation failed.')
      return false
    }
  }

  if (isRestoringSession) {
    return <main className="login-shell"><p>Restoring control-plane session…</p></main>
  }

  if (!csrfToken) {
    return (
      <main className="login-shell">
        <form className="login-card" onSubmit={(event) => void signIn(event)}>
          <h1>ABT control plane</h1>
          <p>Identity, connectivity, relay, and audit administration.</p>
          {error ? <p className="error" role="alert">{error}</p> : null}
          <label>Administrator account
            <input autoComplete="username" onChange={(event) => setUsername(event.target.value)} required value={username} />
          </label>
          <label>Password
            <input autoComplete="current-password" onChange={(event) => setPassword(event.target.value)} required type="password" value={password} />
          </label>
          <button disabled={isSigningIn} type="submit">{isSigningIn ? 'Signing in…' : 'Sign in'}</button>
        </form>
      </main>
    )
  }

  return (
    <div className="console-app-frame">
      <aside className="console-sidebar">
        <strong>ABT console</strong>
        <nav aria-label="Console sections" className="console-nav">
          <Link aria-current={page === 'main' ? 'page' : undefined} to="/">Overview</Link>
          <Link aria-current={page === 'workers' ? 'page' : undefined} to="/workers">Workers</Link>
          <Link aria-current={page === 'invites' ? 'page' : undefined} to="/registration-invites">Registration invites</Link>
          <Link aria-current={page === 'traders' ? 'page' : undefined} to="/traders">Traders</Link>
          <Link aria-current={page === 'audit' ? 'page' : undefined} to="/audit">Audit events</Link>
        </nav>
        <button disabled={isSigningOut} onClick={() => void signOut()} type="button">
          {isSigningOut ? 'Signing out…' : 'Sign out'}
        </button>
      </aside>
      <main className="console-main management-content">
        {error ? <p className="error" role="alert">{error}</p> : null}
        {page === 'workers' ? (
          <WorkerManagementPage
            enrollments={enrollments}
            interventionQueue={interventionQueue}
            isProcessingEnrollment={(enrollmentId) => processingEnrollmentId === enrollmentId}
            onReviewEnrollment={(enrollmentId, action) => void reviewEnrollment(enrollmentId, action)}
            onRevokeWorker={revokeWorker}
            workers={workers}
          />
        ) : page === 'invites' ? (
          <RegistrationInvitesPage csrfToken={csrfToken} />
        ) : page === 'traders' ? (
          <TraderManagementPage csrfToken={csrfToken} />
        ) : page === 'audit' ? (
          <AuditEventsPage csrfToken={csrfToken} />
        ) : (
          <section aria-labelledby="overview-heading">
            <h1 id="overview-heading">Control-plane overview</h1>
            <p>The controller authenticates and connects Strategy Runtimes and Workers. Trading lifecycle is not operated here.</p>
            <dl className="console-summary-grid">
              <div><dt>Workers</dt><dd>{workers.length}</dd></div>
              <div><dt>Connected</dt><dd>{workers.filter((worker) => worker.connectivity === 'connected').length}</dd></div>
              <div><dt>Pending registrations</dt><dd>{enrollments.length}</dd></div>
              <div><dt>Open alerts</dt><dd>{alerts.length}</dd></div>
            </dl>
          </section>
        )}
      </main>
    </div>
  )
}

function readConsolePage(pathname: string): ConsolePage {
  if (pathname === '/workers') return 'workers'
  if (pathname === '/registration-invites') return 'invites'
  if (pathname === '/traders') return 'traders'
  if (pathname === '/audit') return 'audit'
  return 'main'
}

export default App
