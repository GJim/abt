export type WorkerSummarySnapshot = {
  observed_at: string
  account: Record<string, unknown>
  terminal: Record<string, unknown>
}

export type WorkerSummaryWorker = {
  worker_id: string
  login: number
  server: string
  connectivity: string
  safety_state: string
  latest_snapshot: WorkerSummarySnapshot | null
}

export type WorkerSummaryTableProps = {
  workers: WorkerSummaryWorker[]
  onOpenSnapshots?: () => void
}

const SNAPSHOT_STALE_AFTER_MS = 15 * 60 * 1000

export function WorkerSummaryTable({ workers, onOpenSnapshots }: WorkerSummaryTableProps) {
  return (
    <section aria-label="Worker summary">
      {onOpenSnapshots ? (
        <div className="console-table-actions">
          <button type="button" onClick={onOpenSnapshots}>Open snapshots</button>
        </div>
      ) : null}
      <div className="console-table-scroll">
        <table className="console-table">
          <caption className="visually-hidden">Worker account summary</caption>
          <thead>
            <tr>
              <th scope="col" title="Trading server">Server</th>
              <th scope="col" title="MT5 account login">Login</th>
              <th scope="col" title="Account balance">Bal.</th>
              <th scope="col" title="Account equity">Eq.</th>
              <th scope="col" title="Trading allowed">Allowed</th>
              <th scope="col" title="Expert trading enabled">Expert</th>
              <th scope="col" title="Trade API disabled">API off</th>
              <th scope="col" title="Worker connectivity and snapshot freshness">Health</th>
            </tr>
          </thead>
          <tbody>
            {workers.map((worker) => {
              const snapshot = worker.latest_snapshot
              const account = snapshot?.account
              const terminal = snapshot?.terminal
              const health = getWorkerHealth(worker)

              return (
                <tr key={worker.worker_id}>
                  <td>{worker.server || '—'}</td>
                  <td>{displayValue(worker.login)}</td>
                  <td>{displayNumber(account?.balance)}</td>
                  <td>{displayNumber(account?.equity)}</td>
                  <td><BooleanStatus value={terminal?.trade_allowed} /></td>
                  <td><BooleanStatus value={terminal?.trade_expert} /></td>
                  <td><BooleanStatus value={terminal?.tradeapi_disabled} /></td>
                  <td><span className="console-status" data-state={health.state}>{health.label}</span></td>
                </tr>
              )
            })}
            {workers.length === 0 ? (
              <tr>
                <td colSpan={8}>No account workers have reported yet.</td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
    </section>
  )
}

function BooleanStatus({ value }: { value: unknown }) {
  const boolean = typeof value === 'boolean' ? value : null
  const state = boolean === null ? 'unknown' : boolean ? 'healthy' : 'error'

  return <span className="console-status" data-state={state}>{boolean === null ? '—' : boolean ? 'Yes' : 'No'}</span>
}

function displayValue(value: unknown) {
  return typeof value === 'string' || typeof value === 'number' ? value : '—'
}

function displayNumber(value: unknown) {
  if (typeof value !== 'number' && typeof value !== 'string') {
    return '—'
  }

  const number = typeof value === 'number' ? value : Number(value)
  return Number.isFinite(number) ? number.toLocaleString(undefined, { maximumFractionDigits: 2 }) : value
}

function getWorkerHealth(worker: WorkerSummaryWorker) {
  if (worker.connectivity === 'revoked' || worker.safety_state !== 'connected' || worker.connectivity === 'disconnected') {
    return { label: 'Action needed', state: 'error' }
  }
  if (worker.connectivity === 'stale' || isSnapshotStale(worker.latest_snapshot?.observed_at)) {
    return { label: 'Stale', state: 'warning' }
  }
  if (worker.latest_snapshot === null) {
    return { label: 'No snapshot', state: 'warning' }
  }
  return { label: 'Healthy', state: 'healthy' }
}

function isSnapshotStale(observedAt: string | undefined) {
  if (!observedAt) {
    return false
  }

  const timestamp = Date.parse(observedAt)
  return !Number.isFinite(timestamp) || Date.now() - timestamp > SNAPSHOT_STALE_AFTER_MS
}

export default WorkerSummaryTable
