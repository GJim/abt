import { useState } from 'react'
import { Collapsible } from '@astryxdesign/core/Collapsible'
import { Icon } from '@astryxdesign/core/Icon'
import { Link } from 'react-router-dom'
import type { AccountWorker, Enrollment, InterventionItem } from './App'
import { formatTimestamp as formatDateTime } from './formatting'

type WorkerManagementPageProps = {
  enrollments: Enrollment[]
  interventionQueue: InterventionItem[]
  isProcessingEnrollment: (enrollmentId: string) => boolean
  onReviewEnrollment: (enrollmentId: string, action: 'approve' | 'reject') => void
  onRevokeWorker: (workerId: string) => Promise<boolean>
  workers: AccountWorker[]
}

export function WorkerManagementPage({
  enrollments,
  interventionQueue,
  isProcessingEnrollment,
  onReviewEnrollment,
  onRevokeWorker,
  workers,
}: WorkerManagementPageProps) {
  const [revokeCandidate, setRevokeCandidate] = useState<AccountWorker | null>(null)
  const [isRevoking, setIsRevoking] = useState(false)
  const [revokeStatus, setRevokeStatus] = useState<string | null>(null)
  async function confirmRevoke() {
    if (!revokeCandidate) {
      return
    }

    setIsRevoking(true)
    try {
      const revoked = await onRevokeWorker(revokeCandidate.worker_id)
      if (revoked) {
        setRevokeStatus(`Certificate revoked for ${revokeCandidate.login} on ${revokeCandidate.server}.`)
        setRevokeCandidate(null)
      } else {
        setRevokeStatus('Certificate revocation failed. Confirm the worker state and try again.')
      }
    } finally {
      setIsRevoking(false)
    }
  }

  return (
    <section aria-labelledby="workers-heading" className="worker-management">
      <header className="console-page-header">
        <div>
          <h1 id="workers-heading">Workers</h1>
        </div>
        <Link className="secondary-button" to="/registration-invites">Registration invites</Link>
      </header>
      {revokeStatus ? <p className="worker-action-status" role="status">{revokeStatus}</p> : null}

      {interventionQueue.length > 0 ? <section aria-labelledby="worker-attention-heading" className="worker-attention">
        <div className="section-header">
          <h2 id="worker-attention-heading"><Icon aria-hidden="true" icon="warning" size="sm" />Action required</h2>
          <span className="status-badge status-failed">{interventionQueue.length} open</span>
        </div>
        <ul aria-label="Worker intervention queue" className="intervention-list">
          {interventionQueue.map((item) => (
            <li key={item.id}>
              <div>
                <strong>{item.kind}</strong>
                <p>{item.reason}</p>
                <time dateTime={item.occurredAt}>{formatDateTime(item.occurredAt)}</time>
              </div>
              {'alert' in item ? <a href={`#worker-${item.alert.worker_id ?? ''}`}>Review worker</a> : null}
            </li>
          ))}
        </ul>
      </section> : null}

      {enrollments.length > 0 ? <section aria-labelledby="pending-enrollments-heading" className="worker-enrollments">
        <div className="section-header">
          <h2 id="pending-enrollments-heading"><Icon aria-hidden="true" icon="clock" size="sm" />Pending registrations</h2>
          <span className="console-tag">{enrollments.length} pending</span>
        </div>
        <ul className="enrollment-list" aria-label="Pending worker registrations">
            {enrollments.map((enrollment) => {
              const registrationName = `${enrollment.login} on ${enrollment.server}`
              const isProcessing = isProcessingEnrollment(enrollment.enrollment_id)

              return (
                <li id={`enrollment-${enrollment.enrollment_id}`} key={enrollment.enrollment_id} className="enrollment">
                  <dl>
                    <div><dt>Login</dt><dd>{enrollment.login}</dd></div>
                    <div><dt>Server</dt><dd>{enrollment.server}</dd></div>
                    <div><dt>Created</dt><dd><time dateTime={enrollment.created_at}>{formatDateTime(enrollment.created_at)}</time></dd></div>
                    <div><dt>Expires</dt><dd><time dateTime={enrollment.expires_at}>{formatDateTime(enrollment.expires_at)}</time></dd></div>
                  </dl>
                  <Collapsible defaultIsOpen={false} trigger="View registration evidence">
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
                  </Collapsible>
                  <div className="enrollment-actions">
                    <button aria-label={`Approve registration for ${registrationName}`} disabled={isProcessing} onClick={() => onReviewEnrollment(enrollment.enrollment_id, 'approve')} type="button">Approve</button>
                    <button aria-label={`Reject registration for ${registrationName}`} className="reject-button" disabled={isProcessing} onClick={() => onReviewEnrollment(enrollment.enrollment_id, 'reject')} type="button">Reject</button>
                  </div>
                </li>
              )
            })}
        </ul>
      </section> : null}

      <section aria-labelledby="fleet-heading" className="fleet-health">
        <div className="section-header">
          <h2 id="fleet-heading"><Icon aria-hidden="true" icon="checkDouble" size="sm" />Fleet</h2>
          <span className="fleet-count">{workers.length} reporting</span>
        </div>
        <WorkerRoster
          onRevoke={(worker) => {
            setRevokeStatus(null)
            setRevokeCandidate(worker)
          }}
          workers={workers}
        />
      </section>

      {revokeCandidate ? (
        <section aria-labelledby="revoke-certificate-heading" className="worker-revoke-confirmation">
          <h2 id="revoke-certificate-heading">Revoke certificate for {revokeCandidate.login} on {revokeCandidate.server}?</h2>
          <p>This blocks future connections that use the Worker certificate. Strategy Runtime lifecycle decisions are not made by the control plane.</p>
          <div className="action-row">
            <button className="reject-button" disabled={isRevoking} onClick={() => void confirmRevoke()} type="button">
              {isRevoking ? 'Revoking…' : 'Confirm emergency revocation'}
            </button>
            <button disabled={isRevoking} onClick={() => setRevokeCandidate(null)} type="button">Cancel</button>
          </div>
        </section>
      ) : null}
    </section>
  )
}

function WorkerRoster({
  onRevoke,
  workers,
}: {
  onRevoke: (worker: AccountWorker) => void
  workers: AccountWorker[]
}) {
  const sortedWorkers = [...workers].sort((first, second) => {
    const priority = workerPriority(first) - workerPriority(second)
    return priority || `${first.server}:${first.login}`.localeCompare(`${second.server}:${second.login}`)
  })

  if (sortedWorkers.length === 0) {
    return <p className="empty-state">No reports yet.</p>
  }

  return (
    <ul className="worker-list" aria-label="Workers by account">
      {sortedWorkers.map((worker) => {
        const health = describeWorkerHealth(worker)
        const accountName = `${worker.login} on ${worker.server}`

        return (
          <li id={`worker-${worker.worker_id}`} key={worker.worker_id} className="worker">
            <div className="worker-summary">
              <span className="worker-account" dir="auto">
                {accountName}
              </span>
              <span>{worker.last_seen_at ? formatDateTime(worker.last_seen_at) : 'Never connected'}</span>
              <span className={`status-badge worker-health-${health.tone}`}>{health.label}</span>
            </div>
            {worker.connectivity !== 'revoked' ? (
              <button className="reject-button" onClick={() => onRevoke(worker)} type="button">Revoke certificate</button>
            ) : null}
          </li>
        )
      })}
    </ul>
  )
}

function describeWorkerHealth(worker: AccountWorker) {
  if (worker.connectivity === 'revoked') {
    return { description: 'Certificate revoked; connection is blocked.', label: 'Revoked', tone: 'human-action' }
  }
  if (worker.connectivity === 'disconnected') {
    return { description: 'Requires operator investigation before further use.', label: 'Action needed', tone: 'human-action' }
  }
  if (worker.connectivity === 'stale') {
    return { description: 'The authenticated Worker connection is stale.', label: 'Stale', tone: 'stale' }
  }
  return { description: 'The authenticated Worker connection is current.', label: 'Healthy', tone: 'healthy' }
}

function workerPriority(worker: AccountWorker) {
  const health = describeWorkerHealth(worker)
  return health.tone === 'human-action' ? 0 : health.tone === 'stale' ? 1 : 2
}
