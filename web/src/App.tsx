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

function App() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [csrfToken, setCsrfToken] = useState<string | null>(null)
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [error, setError] = useState<string | null>(null)

  async function submitLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(null)

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
    const eventsResponse = await fetch('/api/admin/events', { credentials: 'same-origin' })
    if (!eventsResponse.ok) {
      setError('Signed in, but audit events could not be loaded.')
      return
    }
    setEvents((await eventsResponse.json()) as AuditEvent[])
  }

  if (csrfToken) {
    return (
      <main className="management">
        <header>
          <p className="eyebrow">ABT control plane</p>
          <h1>Audit events</h1>
          <p>Your management session is active.</p>
          <ul aria-label="Audit events">
            {events.map((event) => (
              <li key={event.event_id}>
                <strong>{event.event_type}</strong>
                <time dateTime={event.occurred_at}>{event.occurred_at}</time>
              </li>
            ))}
          </ul>
        </header>
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
