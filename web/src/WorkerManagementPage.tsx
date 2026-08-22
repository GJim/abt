import { useMemo, useState } from 'react'
import { Collapsible } from '@astryxdesign/core/Collapsible'
import { Link } from 'react-router-dom'
import { WorkerSnapshotsPage } from './WorkerSnapshotsPage'
import type { AccountWorker, Enrollment, InterventionItem, WorkerAlert } from './App'

type WorkerManagementPageProps = {
  alerts: WorkerAlert[]
  enrollments: Enrollment[]
  interventionQueue: InterventionItem[]
  isProcessingEnrollment: (enrollmentId: string) => boolean
  onReviewEnrollment: (enrollmentId: string, action: 'approve' | 'reject') => void
  onRevokeWorker: (workerId: string) => Promise<boolean>
  workers: AccountWorker[]
}

export function WorkerManagementPage({
  alerts,
  enrollments,
  interventionQueue,
  isProcessingEnrollment,
  onReviewEnrollment,
  onRevokeWorker,
  workers,
}: WorkerManagementPageProps) {
  const [selectedWorkerId, setSelectedWorkerId] = useState<string | null>(null)
  const [revokeCandidate, setRevokeCandidate] = useState<AccountWorker | null>(null)
  const [isRevoking, setIsRevoking] = useState(false)
  const [revokeStatus, setRevokeStatus] = useState<string | null>(null)
  const workerIdsWithAlerts = useMemo(
    () => new Set(alerts.map((alert) => alert.worker_id).filter((workerId): workerId is string => workerId !== null)),
    [alerts],
  )

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
          <p>Approve registrations, assess the latest account state, and contain workers that need intervention.</p>
        </div>
        <Link className="secondary-button" to="/registration-invites">Registration invites</Link>
      </header>
      {revokeStatus ? <p className="worker-action-status" role="status">{revokeStatus}</p> : null}

      <section aria-labelledby="worker-attention-heading" className="worker-attention">
        <div className="section-header">
          <div>
            <h2 id="worker-attention-heading">Needs attention</h2>
            <p>Open items are limited to worker registrations and operating conditions that require an accountable action.</p>
          </div>
          <span className="status-badge status-failed">{interventionQueue.length} open</span>
        </div>
        {interventionQueue.length === 0 ? (
          <p>No operator action is needed.</p>
        ) : (
          <ul aria-label="Worker intervention queue" className="intervention-list">
            {interventionQueue.map((item) => (
              <li key={item.id}>
                <div>
                  <strong>{item.kind}</strong>
                  <p>{item.reason}</p>
                  <time dateTime={item.occurredAt}>{formatDateTime(item.occurredAt)}</time>
                </div>
                {'enrollment' in item ? (
                  <div className="enrollment-actions">
                    <button
                      aria-label={`Approve registration for ${item.enrollment.login} on ${item.enrollment.server}`}
                      disabled={isProcessingEnrollment(item.enrollment.enrollment_id)}
                      onClick={() => onReviewEnrollment(item.enrollment.enrollment_id, 'approve')}
                      type="button"
                    >
                      Approve
                    </button>
                    <button
                      aria-label={`Reject registration for ${item.enrollment.login} on ${item.enrollment.server}`}
                      className="reject-button"
                      disabled={isProcessingEnrollment(item.enrollment.enrollment_id)}
                      onClick={() => onReviewEnrollment(item.enrollment.enrollment_id, 'reject')}
                      type="button"
                    >
                      Reject
                    </button>
                  </div>
                ) : (
                  <a href={`#worker-${item.alert.worker_id ?? ''}`}>Review worker</a>
                )}
              </li>
            ))}
          </ul>
        )}
      </section>

      <section aria-labelledby="pending-enrollments-heading" className="worker-enrollments">
        <div className="section-header">
          <div>
            <h2 id="pending-enrollments-heading">Pending registrations</h2>
            <p>Verify account, terminal, and pairing evidence before issuing a device certificate.</p>
          </div>
          <span className="console-tag">{enrollments.length} pending</span>
        </div>
        {enrollments.length === 0 ? (
          <p>No worker registrations are awaiting review.</p>
        ) : (
          <ul className="enrollment-list" aria-label="Pending worker registrations">
            {enrollments.map((enrollment) => {
              const registrationName = `${enrollment.login} on ${enrollment.server}`
              const isProcessing = isProcessingEnrollment(enrollment.enrollment_id)

              return (
                <li id={`enrollment-${enrollment.enrollment_id}`} key={enrollment.enrollment_id} className="enrollment">
                  <dl>
                    <div><dt>Login</dt><dd>{enrollment.login}</dd></div>
                    <div><dt>Server</dt><dd>{enrollment.server}</dd></div>
                    <div><dt>Pairing code</dt><dd>{enrollment.pairing_code}</dd></div>
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
        )}
      </section>

      <section aria-labelledby="fleet-heading" className="fleet-health">
        <div className="section-header">
          <div>
            <h2 id="fleet-heading">Fleet</h2>
            <p>Latest report only. Open a worker to review its reconciliation evidence or historical snapshots.</p>
          </div>
          <span className="fleet-count">{workers.length} reporting</span>
        </div>
        <WorkerRoster
          onRevoke={(worker) => {
            setRevokeStatus(null)
            setRevokeCandidate(worker)
          }}
          onSelect={setSelectedWorkerId}
          selectedWorkerId={selectedWorkerId}
          workerIdsWithAlerts={workerIdsWithAlerts}
          workers={workers}
        />
      </section>

      {revokeCandidate ? (
        <section aria-labelledby="revoke-certificate-heading" className="worker-revoke-confirmation">
          <h2 id="revoke-certificate-heading">Revoke certificate for {revokeCandidate.login} on {revokeCandidate.server}?</h2>
          <p>This is an emergency containment action. The worker will receive <code>REVOKE_AND_FLATTEN</code>; its related pairs remain frozen until manually resolved.</p>
          <div className="action-row">
            <button className="reject-button" disabled={isRevoking} onClick={() => void confirmRevoke()} type="button">
              {isRevoking ? 'Revoking…' : 'Confirm revoke and flatten'}
            </button>
            <button disabled={isRevoking} onClick={() => setRevokeCandidate(null)} type="button">Cancel</button>
          </div>
        </section>
      ) : null}

      <section aria-labelledby="worker-history-heading" className="worker-history">
        <div className="section-header">
          <div>
            <h2 id="worker-history-heading">Snapshot history</h2>
            <p>Use this diagnostic archive only when the latest worker state needs investigation.</p>
          </div>
        </div>
        <Collapsible defaultIsOpen={false} trigger="Search snapshot history">
          <WorkerSnapshotsPage />
        </Collapsible>
      </section>
    </section>
  )
}

function WorkerRoster({
  onRevoke,
  onSelect,
  selectedWorkerId,
  workerIdsWithAlerts,
  workers,
}: {
  onRevoke: (worker: AccountWorker) => void
  onSelect: (workerId: string | null) => void
  selectedWorkerId: string | null
  workerIdsWithAlerts: Set<string>
  workers: AccountWorker[]
}) {
  const sortedWorkers = [...workers].sort((first, second) => {
    const priority = workerPriority(first, workerIdsWithAlerts) - workerPriority(second, workerIdsWithAlerts)
    return priority || `${first.server}:${first.login}`.localeCompare(`${second.server}:${second.login}`)
  })

  if (sortedWorkers.length === 0) {
    return <p className="empty-state">No approved account workers have reported yet. Approved workers will appear here after their first connection.</p>
  }

  return (
    <ul className="worker-list" aria-label="Workers by account">
      {sortedWorkers.map((worker) => {
        const isSelected = selectedWorkerId === worker.worker_id
        const health = describeWorkerHealth(worker, workerIdsWithAlerts.has(worker.worker_id))
        const accountName = `${worker.login} on ${worker.server}`
        const snapshot = worker.latest_snapshot
        const liveState = worker.live_state

        return (
          <li id={`worker-${worker.worker_id}`} key={worker.worker_id} className="worker">
            <button
              aria-controls={`worker-detail-${worker.worker_id}`}
              aria-expanded={isSelected}
              className="worker-summary"
              onClick={() => onSelect(isSelected ? null : worker.worker_id)}
              type="button"
            >
              <span dir="auto">
                <strong>{accountName}</strong>
                <span>{health.description}</span>
              </span>
              <span className={`status-badge worker-health-${health.tone}`}>{health.label}</span>
            </button>
            <dl className="worker-facts">
              <div><dt>Freshness</dt><dd>{snapshot ? <time dateTime={snapshot.observed_at}>{formatSnapshotFreshness(snapshot.observed_at)}</time> : 'No snapshot received'}</dd></div>
              <div><dt>Account</dt><dd>{snapshot ? formatAccountSummary(snapshot.account) : 'Awaiting snapshot'}</dd></div>
              <div><dt>Reconciliation</dt><dd>{worker.deltas.length === 0 ? 'No recent changes' : `${worker.deltas.length} recent changes`}</dd></div>
            </dl>
            {isSelected ? (
              <div id={`worker-detail-${worker.worker_id}`} className="worker-evidence">
                <section aria-label={`Live market state for ${accountName}`}>
                  <h3>Live market state</h3>
                  {liveState ? (
                    <>
                      <p>
                        Terminal {liveState.connectivity ? 'connected' : 'disconnected'}; controller received{' '}
                        <time dateTime={liveState.received_at}>{formatSnapshotFreshness(liveState.received_at)}</time>.
                      </p>
                      {liveState.quotes.length === 0 ? <p>No watched-symbol quotes are available.</p> : (
                        <dl className="worker-live-quotes">
                          {liveState.quotes.map((quote) => (
                            <div key={quote.symbol}>
                              <dt>{quote.symbol}</dt>
                              <dd>Bid {quote.bid}; ask {quote.ask}</dd>
                              <dd>Broker <time dateTime={quote.broker_time}>{formatDateTime(quote.broker_time)}</time></dd>
                            </div>
                          ))}
                        </dl>
                      )}
                      <p>{liveState.orders.length} open orders; {liveState.positions.length} open positions.</p>
                    </>
                  ) : <p>Awaiting the worker’s live-state snapshot.</p>}
                </section>
                <section aria-label={`Latest snapshot for ${accountName}`}>
                  <h3>Latest report</h3>
                  {snapshot ? (
                    <>
                      <time dateTime={snapshot.observed_at}>{formatDateTime(snapshot.observed_at)}</time>
                      <pre className="console-raw-detail">{JSON.stringify({
                        account: snapshot.account,
                        terminal: snapshot.terminal,
                        orders: snapshot.orders,
                        positions: snapshot.positions,
                      }, null, 2)}</pre>
                    </>
                  ) : <p>No snapshot has been received.</p>}
                </section>
                <section aria-label={`Lifecycle deltas for ${accountName}`}>
                  <h3>Recent lifecycle changes</h3>
                  {worker.deltas.length === 0 ? <p>No lifecycle or volume changes reported.</p> : (
                    <ul className="worker-deltas">
                      {worker.deltas.map((delta) => (
                        <li key={delta.cursor}>
                          <strong>{delta.change}</strong> {delta.entity} {delta.ticket}
                          <time dateTime={delta.observed_at}>{formatDateTime(delta.observed_at)}</time>
                        </li>
                      ))}
                    </ul>
                  )}
                </section>
                {worker.connectivity !== 'revoked' ? (
                  <button className="reject-button" onClick={() => onRevoke(worker)} type="button">Revoke certificate</button>
                ) : null}
              </div>
            ) : null}
          </li>
        )
      })}
    </ul>
  )
}

function formatDateTime(value: string) {
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toISOString().slice(0, 19).replace('T', ' ') + ' UTC'
}

function formatSnapshotFreshness(value: string) {
  const timestamp = Date.parse(value)
  if (!Number.isFinite(timestamp)) {
    return 'Unknown'
  }
  const ageMinutes = Math.max(0, Math.floor((Date.now() - timestamp) / 60_000))
  return ageMinutes === 0 ? 'Reported just now' : `Reported ${ageMinutes} min ago`
}

function formatAccountSummary(account: Record<string, unknown>) {
  const balance = typeof account.balance === 'number' ? account.balance.toLocaleString() : '—'
  const equity = typeof account.equity === 'number' ? account.equity.toLocaleString() : '—'
  return `Balance ${balance}; equity ${equity}`
}

function describeWorkerHealth(worker: AccountWorker, hasAlert: boolean) {
  if (worker.connectivity === 'revoked') {
    return { description: 'Certificate revoked; connection is blocked.', label: 'Revoked', tone: 'human-action' }
  }
  if (hasAlert || worker.safety_state !== 'connected' || worker.connectivity === 'disconnected') {
    return { description: 'Requires operator investigation before further use.', label: 'Action needed', tone: 'human-action' }
  }
  if (worker.connectivity === 'stale' || !worker.latest_snapshot || isSnapshotStale(worker.latest_snapshot.observed_at)) {
    return { description: 'Latest account report is stale or unavailable.', label: 'Stale', tone: 'stale' }
  }

  function isSnapshotStale(value: string) {
    const timestamp = Date.parse(value)
    return !Number.isFinite(timestamp) || Date.now() - timestamp > 15 * 60_000
  }
  return { description: 'Connected with a current account report.', label: 'Healthy', tone: 'healthy' }
}

function workerPriority(worker: AccountWorker, workerIdsWithAlerts: Set<string>) {
  const health = describeWorkerHealth(worker, workerIdsWithAlerts.has(worker.worker_id))
  return health.tone === 'human-action' ? 0 : health.tone === 'stale' ? 1 : 2
}
