import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import './App.css'

type LoginResponse = {
  csrf_token: string
}

type AuditEventPayload = {
  analysis_id?: string
  stage?: string
  reason?: string
  [key: string]: unknown
}

type AuditEvent = {
  event_id: number
  event_type: string
  occurred_at: string
  payload?: AuditEventPayload
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
  product_pair_id?: string | null
  priority: string
  alert_type: string
  reason: string
  occurred_at: string
}

type ProductCatalogAnalysisPolicy = {
  label: string
  require_equal_base_currency: boolean
  require_equal_profit_currency: boolean
  minimum_m15_common_coverage: number
  minimum_m1_common_coverage: number
  minimum_m15_return_correlation: number
  minimum_m1_return_correlation: number
  maximum_m1_median_price_difference_points: number
}

type WorkerReference = {
  worker_id: string
  login: number
  server: string
}

type AnalysisSourceWorkers = {
  first_worker: WorkerReference
  second_worker: WorkerReference
}

type AnalysisPeriod = {
  timeframe: string
  started_at_utc: string
  ended_at_utc: string
}

type CatalogEvidence = {
  collected_at?: string
  symbols: Record<string, unknown>[]
}

type AnalysisCandidate = {
  first_symbol: string
  second_symbol: string
  currency_base: string
  currency_profit: string
  first_point: number
  second_point: number
}

type AnalysisException = AnalysisCandidate & {
  reason: string
  first_trade_calc_mode: string
  second_trade_calc_mode: string
}

type AnalysisDifference = {
  field: string
  first_value: unknown
  second_value: unknown
}

type MarketStatistics = {
  aligned_bar_count: number
  first_bar_count: number
  second_bar_count: number
  coverage_ratio: number
  return_correlation: number | null
  median_price_difference_points: number | null
  p99_price_difference_points: number | null
  target_point: number
}

type MarketDataSummary = {
  symbol: string
  bar_count: number
  first_raw_epoch: number
  last_raw_epoch: number
  first_utc: string
  last_utc: string
  content_hash: string
  time_metadata: Record<string, unknown>
}

type PolicyEvaluation = Record<string, boolean | number>

type ScreeningResult = AnalysisCandidate & {
  screening_status: string
  statistics: MarketStatistics
  policy_evaluation: PolicyEvaluation
  first_market_data: MarketDataSummary
  second_market_data: MarketDataSummary
}

type VerificationResult = ScreeningResult & {
  verification_status: string
  hard_block_differences: AnalysisDifference[]
  warning_differences: AnalysisDifference[]
}

type ProductCatalogAnalysis = {
  analysis_id: string
  requested_by: string
  first_worker: WorkerReference
  second_worker: WorkerReference
  policy: ProductCatalogAnalysisPolicy
  status: string
  failure_reason: string | null
  current_stage: string
  retry_count: number
  analysis_period: AnalysisPeriod
  first_catalog_evidence: CatalogEvidence | null
  second_catalog_evidence: CatalogEvidence | null
  eligible_candidates: AnalysisCandidate[]
  exceptions: AnalysisException[]
  m15_screening_results: ScreeningResult[]
  m1_verification_results: VerificationResult[]
  requested_at: string
  catalog_completed_at: string | null
  m15_screened_at: string | null
  m1_verified_at: string | null
  completed_at: string | null
}

type ProductPairReferenceSpecification = {
  server: string
  symbol: string
  specification: Record<string, unknown>
}

type ProductPairBuildConfirmation = {
  confirmation_id: string
  analysis_id: string
  requested_by: string
  analysis_period: AnalysisPeriod
  policy_snapshot: ProductCatalogAnalysisPolicy
  lot_relationship: {
    version: string
    ratio: string
    first_lots: number
    second_lots: number
  }
  source_workers: AnalysisSourceWorkers
  endpoints: Array<{
    server: string
    symbol: string
  }>
  reference_specifications: ProductPairReferenceSpecification[]
  approval_evidence: VerificationResult
}

type ProductPairRetest = {
  retest_id: string
  product_pair_id: string
  requested_by: string
  source_workers: AnalysisSourceWorkers
  policy_snapshot: ProductCatalogAnalysisPolicy
  analysis_period: AnalysisPeriod
  reference_specifications: ProductPairReferenceSpecification[]
  status: string
  current_stage: string
  retry_count: number
  failure_reason: string | null
  requested_at: string
  completed_at: string | null
  verification_result?: VerificationResult | null
  statistics?: MarketStatistics | null
  alert?: ProductPairRetestAlert | null
  latest_alert?: ProductPairRetestAlert | null
  result?: { statistics?: MarketStatistics | null } | null
  [key: string]: unknown
}

type ProductPairRetestAlert = {
  product_pair_id?: string | null
  alert_type: string
  priority: string
  reason: string
  occurred_at: string
}

type WorkerCompatibilityResult = {
  product_pair_id?: string
  worker_id?: string
  login?: number
  server?: string
  applicability_status?: string
  inspection_status?: string
  compatibility_check_id?: string
  reference_symbol: string
  live_specification: Record<string, unknown> | null
  reference_specification: Record<string, unknown>
  hard_block_differences: AnalysisDifference[]
  warning_differences: AnalysisDifference[]
  checked_at?: string | null
  checked_by?: string | null
  [key: string]: unknown
}

type WorkerExclusion = {
  excluded_at?: string | null
  excluded_by?: string | null
  compatibility_check_id?: string | null
  [key: string]: unknown
}

type WorkerApplicability = {
  worker_id: string
  login?: number
  server?: string
  applicability_status?: string
  inspection_status?: string
  latest_compatibility_check?: WorkerCompatibilityResult | null
  exclusion?: WorkerExclusion | null
  compatibility_result?: WorkerCompatibilityResult | null
  compatibility?: WorkerCompatibilityResult | null
  [key: string]: unknown
}

type ApplicabilityState = 'applicable_uninspected' | 'checked' | 'excluded'

type ProductPair = {
  product_pair_id: string
  status: string
  endpoints: Array<{
    server: string
    symbol: string
  }>
  lot_relationship: {
    version: string
    ratio: string
    first_lots: number
    second_lots: number
  }
  policy_snapshot: ProductCatalogAnalysisPolicy
  analysis_period: AnalysisPeriod
  reference_specifications: ProductPairReferenceSpecification[]
  approval_evidence: VerificationResult
  source_workers: AnalysisSourceWorkers
  built_from_analysis_id: string
  built_from_confirmation_id: string
  built_by: string
  created_at: string
  retired_at: string | null
  retired_by: string | null
  retired_reason: string | null
  replaced_by_product_pair_id: string | null
  replaces_product_pair_id: string | null
  latest_retest: ProductPairRetest | null
  worker_applicability: WorkerApplicability[]
}

const DEFAULT_POLICY: ProductCatalogAnalysisPolicy = {
  label: 'FX catalog v1',
  require_equal_base_currency: true,
  require_equal_profit_currency: true,
  minimum_m15_common_coverage: 1,
  minimum_m1_common_coverage: 0.98,
  minimum_m15_return_correlation: 0.97,
  minimum_m1_return_correlation: 0.95,
  maximum_m1_median_price_difference_points: 2,
}

const BOOLEAN_POLICY_FIELDS: Array<{
  key: keyof ProductCatalogAnalysisPolicy
  label: string
  description: string
}> = [
  {
    key: 'require_equal_base_currency',
    label: 'Require equal base currency',
    description: 'Keep pairs on the same base currency.',
  },
  {
    key: 'require_equal_profit_currency',
    label: 'Require equal profit currency',
    description: 'Keep pairs on the same profit currency.',
  },
]

const NUMBER_POLICY_FIELDS: Array<{
  key: keyof ProductCatalogAnalysisPolicy
  label: string
  description: string
  min: number
  max?: number
  step: number
}> = [
  {
    key: 'minimum_m15_common_coverage',
    label: 'Minimum M15 common coverage',
    description: 'Minimum overlapping M15 bar coverage ratio.',
    min: 0,
    max: 1,
    step: 0.01,
  },
  {
    key: 'minimum_m1_common_coverage',
    label: 'Minimum M1 common coverage',
    description: 'Minimum overlapping M1 bar coverage ratio.',
    min: 0,
    max: 1,
    step: 0.01,
  },
  {
    key: 'minimum_m15_return_correlation',
    label: 'Minimum M15 return correlation',
    description: 'Minimum return correlation during M15 screening.',
    min: -1,
    max: 1,
    step: 0.01,
  },
  {
    key: 'minimum_m1_return_correlation',
    label: 'Minimum M1 return correlation',
    description: 'Minimum return correlation during M1 verification.',
    min: -1,
    max: 1,
    step: 0.01,
  },
  {
    key: 'maximum_m1_median_price_difference_points',
    label: 'Maximum M1 median price difference',
    description: 'Median price difference threshold in points.',
    min: 0,
    step: 0.1,
  },
]

const STAGE_LABELS: Record<string, string> = {
  catalog: 'Collecting product catalogs',
  m15_screening: 'Screening M15 returns',
  m1_verification: 'Verifying M1 candidates',
  completed: 'Completed',
  catalog_failed: 'Catalog collection failed',
  m15_failed: 'M15 screening failed',
  m1_failed: 'M1 verification failed',
}

const EVENT_LABELS: Record<string, string> = {
  product_catalog_analysis_requested: 'Queued',
  product_catalog_analysis_catalog_completed: 'Catalog evidence recorded',
  product_catalog_analysis_m15_completed: 'M15 screening completed',
  product_catalog_analysis_m1_completed: 'M1 verification completed',
  product_catalog_analysis_retry: 'Retry requested',
  product_catalog_analysis_succeeded: 'Succeeded',
  product_catalog_analysis_failed: 'Failed',
}

const POLICY_EVALUATION_LABELS: Record<string, string> = {
  minimum_m15_common_coverage: 'Minimum M15 common coverage',
  minimum_m1_common_coverage: 'Minimum M1 common coverage',
  minimum_m15_return_correlation: 'Minimum M15 return correlation',
  minimum_m1_return_correlation: 'Minimum M1 return correlation',
  maximum_m1_median_price_difference_points: 'Maximum M1 median difference',
  coverage_passed: 'Coverage passed',
  return_correlation_passed: 'Return correlation passed',
  median_price_difference_passed: 'Median difference passed',
  hard_block_differences_passed: 'Hard-block differences passed',
}

function App() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [csrfToken, setCsrfToken] = useState<string | null>(null)
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [enrollments, setEnrollments] = useState<Enrollment[]>([])
  const [workers, setWorkers] = useState<AccountWorker[]>([])
  const [alerts, setAlerts] = useState<WorkerAlert[]>([])
  const [productPairs, setProductPairs] = useState<ProductPair[]>([])
  const [processingEnrollmentId, setProcessingEnrollmentId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [policy, setPolicy] = useState<ProductCatalogAnalysisPolicy>(DEFAULT_POLICY)
  const [firstWorkerId, setFirstWorkerId] = useState('')
  const [secondWorkerId, setSecondWorkerId] = useState('')
  const [initialAnalysisLookupId] = useState(() => readAnalysisQuery())
  const [analysisLookupId, setAnalysisLookupId] = useState(initialAnalysisLookupId)
  const [analysis, setAnalysis] = useState<ProductCatalogAnalysis | null>(null)
  const [analysisError, setAnalysisError] = useState<string | null>(null)
  const [isLaunchingAnalysis, setIsLaunchingAnalysis] = useState(false)
  const [isLoadingAnalysis, setIsLoadingAnalysis] = useState(false)

  const eligibleWorkers = useMemo(
    () => workers.filter((worker) => workerEligibilityReason(worker) === null),
    [workers],
  )
  const ineligibleWorkers = useMemo(
    () => workers
      .map((worker) => ({ worker, reason: workerEligibilityReason(worker) }))
      .filter((entry): entry is { worker: AccountWorker; reason: string } => entry.reason !== null),
    [workers],
  )

  const selectedFirstWorker = useMemo(
    () => eligibleWorkers.find((worker) => worker.worker_id === firstWorkerId) ?? null,
    [eligibleWorkers, firstWorkerId],
  )
  const availableSecondWorkers = useMemo(
    () => selectedFirstWorker === null
      ? eligibleWorkers
      : eligibleWorkers.filter(
          (worker) => worker.worker_id !== selectedFirstWorker.worker_id && worker.server !== selectedFirstWorker.server,
        ),
    [eligibleWorkers, selectedFirstWorker],
  )
  const selectedSecondWorker = useMemo(
    () => availableSecondWorkers.find((worker) => worker.worker_id === secondWorkerId)
      ?? eligibleWorkers.find((worker) => worker.worker_id === secondWorkerId)
      ?? null,
    [availableSecondWorkers, eligibleWorkers, secondWorkerId],
  )
  const pairValidationError = useMemo(
    () => validateAnalysisPair(selectedFirstWorker, selectedSecondWorker),
    [selectedFirstWorker, selectedSecondWorker],
  )
  const analysisEvents = useMemo(
    () => analysis === null
      ? []
      : events.filter((event) => event.payload?.analysis_id === analysis.analysis_id),
    [analysis, events],
  )

  useEffect(() => {
    if (eligibleWorkers.length === 0) {
      setFirstWorkerId('')
      setSecondWorkerId('')
      return
    }
    if (!eligibleWorkers.some((worker) => worker.worker_id === firstWorkerId)) {
      setFirstWorkerId(eligibleWorkers[0]?.worker_id ?? '')
    }
  }, [eligibleWorkers, firstWorkerId])

  useEffect(() => {
    if (!selectedFirstWorker) {
      setSecondWorkerId('')
      return
    }
    if (!availableSecondWorkers.some((worker) => worker.worker_id === secondWorkerId)) {
      setSecondWorkerId(availableSecondWorkers[0]?.worker_id ?? '')
    }
  }, [availableSecondWorkers, secondWorkerId, selectedFirstWorker])

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
        if (initialAnalysisLookupId) {
          await loadAnalysisById(initialAnalysisLookupId, { replaceUrl: false })
        }
      } catch {
        setError('Your session could not be restored. Please sign in again.')
      }
    }

    void resumeSession()
  }, [initialAnalysisLookupId])

  useEffect(() => {
    if (analysis?.status !== 'running') {
      return
    }
    const timer = window.setTimeout(() => {
      void loadAnalysisById(analysis.analysis_id, { replaceUrl: false, silent: true })
    }, 3000)
    return () => window.clearTimeout(timer)
  }, [analysis?.analysis_id, analysis?.status])

  async function refreshManagementData() {
    const [eventsResponse, enrollmentsResponse, workersResponse, alertsResponse, productPairsResponse] = await Promise.all([
      fetch('/api/admin/events', { credentials: 'same-origin' }),
      fetch('/api/admin/enrollments', { credentials: 'same-origin' }),
      fetch('/api/admin/workers', { credentials: 'same-origin' }),
      fetch('/api/admin/alerts', { credentials: 'same-origin' }),
      fetch('/api/admin/product-pairs', { credentials: 'same-origin' }),
    ])
    if (!eventsResponse.ok || !enrollmentsResponse.ok || !workersResponse.ok || !alertsResponse.ok || !productPairsResponse.ok) {
      throw new Error('Management data could not be loaded.')
    }

    const [eventPayload, enrollmentPayload, workerPayload, alertPayload, productPairPayload] = await Promise.all([
      eventsResponse.json() as Promise<AuditEvent[]>,
      enrollmentsResponse.json() as Promise<Enrollment[]>,
      workersResponse.json() as Promise<AccountWorker[]>,
      alertsResponse.json() as Promise<WorkerAlert[]>,
      productPairsResponse.json() as Promise<ProductPair[]>,
    ])
    setEvents(eventPayload)
    setEnrollments(enrollmentPayload)
    setWorkers(workerPayload)
    setAlerts(alertPayload)
    setProductPairs(productPairPayload)
  }

  async function loadAnalysisById(
    analysisId: string,
    options?: { replaceUrl?: boolean; silent?: boolean },
  ) {
    const trimmedId = analysisId.trim()
    if (!trimmedId) {
      setAnalysisError('Enter an analysis ID to view its lifecycle and evidence.')
      return
    }
    if (!options?.silent) {
      setAnalysisError(null)
    }
    setIsLoadingAnalysis(true)
    try {
      const response = await fetch(`/api/admin/product-catalog-analyses/${encodeURIComponent(trimmedId)}`, {
        credentials: 'same-origin',
      })
      if (!response.ok) {
        throw new Error(await readResponseDetail(response, 'The requested analysis could not be loaded.'))
      }
      const payload = (await response.json()) as ProductCatalogAnalysis
      setAnalysis(payload)
      setAnalysisLookupId(payload.analysis_id)
      if (options?.replaceUrl !== false) {
        writeAnalysisQuery(payload.analysis_id)
      }
    } catch (loadError) {
      if (!options?.silent) {
        setAnalysisError(loadError instanceof Error ? loadError.message : 'The requested analysis could not be loaded.')
      }
    } finally {
      setIsLoadingAnalysis(false)
    }
  }

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
      if (analysisLookupId) {
        await loadAnalysisById(analysisLookupId, { replaceUrl: false })
      }
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
        const detail = await readResponseDetail(response, 'Enrollment review failed.')
        throw new Error(detail)
      }
      await refreshManagementData()
    } catch (reviewError) {
      const detail = reviewError instanceof Error ? reviewError.message : 'Enrollment review failed.'
      setError(`Could not ${action} this worker registration: ${detail}`)
    } finally {
      setProcessingEnrollmentId(null)
    }
  }

  async function submitAnalysis(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!csrfToken) {
      return
    }
    if (pairValidationError) {
      setAnalysisError(pairValidationError)
      return
    }
    if (!selectedFirstWorker || !selectedSecondWorker) {
      setAnalysisError('Pick two eligible workers on different exact MT5 servers before launching.')
      return
    }

    setAnalysisError(null)
    setIsLaunchingAnalysis(true)
    try {
      const response = await fetch('/api/admin/product-catalog-analyses', {
        method: 'POST',
        headers: {
          'X-CSRF-Token': csrfToken,
          'content-type': 'application/json',
        },
        credentials: 'same-origin',
        body: JSON.stringify({
          first_worker_id: selectedFirstWorker.worker_id,
          second_worker_id: selectedSecondWorker.worker_id,
          policy,
        }),
      })
      if (!response.ok) {
        throw new Error(await readResponseDetail(response, 'The analysis could not be launched.'))
      }
      const payload = (await response.json()) as ProductCatalogAnalysis
      setAnalysis(payload)
      setAnalysisLookupId(payload.analysis_id)
      writeAnalysisQuery(payload.analysis_id)
      await refreshManagementData()
    } catch (launchError) {
      setAnalysisError(launchError instanceof Error ? launchError.message : 'The analysis could not be launched.')
    } finally {
      setIsLaunchingAnalysis(false)
    }
  }

  async function submitAnalysisLookup(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    await loadAnalysisById(analysisLookupId)
  }

  if (csrfToken) {
    return (
      <main className="management">
        <div className="management-content">
          <header>
            <p className="eyebrow">ABT control plane</p>
            <h1>Management console</h1>
            <p>Launch and inspect cross-server product-pair analyses alongside worker health and audit events.</p>
          </header>
          {error && <p className="error" role="alert">{error}</p>}

          <section aria-labelledby="analysis-heading" className="analysis-section">
            <div className="section-header">
              <div>
                <h2 id="analysis-heading">Cross-server product-pair analyses</h2>
                <p>Select two eligible workers, capture the policy snapshot, and inspect lifecycle evidence.</p>
              </div>
              {(isLaunchingAnalysis || isLoadingAnalysis) && (
                <span className="status-badge status-queued" role="status">
                  {isLaunchingAnalysis ? 'Queued request' : 'Loading analysis'}
                </span>
              )}
            </div>
            {analysisError && <p className="error" role="alert">{analysisError}</p>}
            <div className="analysis-layout">
              <article className="panel">
                <h3>Eligible workers</h3>
                {eligibleWorkers.length === 0 ? (
                  <p>No healthy, connected workers are currently eligible for analysis.</p>
                ) : (
                  <ul className="worker-status-list" aria-label="Eligible analysis workers">
                    {eligibleWorkers.map((worker) => (
                      <li key={worker.worker_id}>
                        <strong>{worker.login} on {worker.server}</strong>
                        <span>{worker.connectivity} / {worker.safety_state}</span>
                      </li>
                    ))}
                  </ul>
                )}
                {ineligibleWorkers.length > 0 && (
                  <details>
                    <summary>Show blocked workers</summary>
                    <ul className="worker-status-list" aria-label="Blocked analysis workers">
                      {ineligibleWorkers.map(({ worker, reason }) => (
                        <li key={worker.worker_id}>
                          <strong>{worker.login} on {worker.server}</strong>
                          <span>{reason}</span>
                        </li>
                      ))}
                    </ul>
                  </details>
                )}
              </article>

              <article className="panel">
                <h3>Launch analysis</h3>
                <form className="analysis-form" onSubmit={submitAnalysis}>
                  <label>
                    First worker
                    <select
                      aria-label="First analysis worker"
                      onChange={(event) => setFirstWorkerId(event.target.value)}
                      value={firstWorkerId}
                    >
                      <option value="">Select a worker</option>
                      {eligibleWorkers.map((worker) => (
                        <option key={worker.worker_id} value={worker.worker_id}>
                          {worker.login} on {worker.server}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Second worker
                    <select
                      aria-label="Second analysis worker"
                      onChange={(event) => setSecondWorkerId(event.target.value)}
                      value={secondWorkerId}
                    >
                      <option value="">Select a worker on another exact server</option>
                      {availableSecondWorkers.map((worker) => (
                        <option key={worker.worker_id} value={worker.worker_id}>
                          {worker.login} on {worker.server}
                        </option>
                      ))}
                    </select>
                  </label>
                  {pairValidationError ? (
                    <p className="hint error" id="analysis-pair-hint">{pairValidationError}</p>
                  ) : (
                    <p className="hint" id="analysis-pair-hint">
                      Only approved, healthy, connected workers on different exact MT5 servers can be submitted.
                    </p>
                  )}

                  <fieldset className="policy-fieldset">
                    <legend>Policy</legend>
                    <label>
                      Policy label
                      <input
                        aria-label="Policy label"
                        maxLength={128}
                        onChange={(event) => setPolicy((current) => ({ ...current, label: event.target.value }))}
                        required
                        value={policy.label}
                      />
                    </label>
                    <div className="policy-grid">
                      {BOOLEAN_POLICY_FIELDS.map((field) => (
                        <label key={field.key} className="checkbox-field">
                          <input
                            checked={Boolean(policy[field.key])}
                            onChange={(event) => {
                              setPolicy((current) => ({ ...current, [field.key]: event.target.checked }))
                            }}
                            type="checkbox"
                          />
                          <span>
                            <strong>{field.label}</strong>
                            <small>{field.description}</small>
                          </span>
                        </label>
                      ))}
                    </div>
                    <div className="policy-grid">
                      {NUMBER_POLICY_FIELDS.map((field) => (
                        <label key={field.key}>
                          {field.label}
                          <input
                            aria-label={field.label}
                            max={field.max}
                            min={field.min}
                            onChange={(event) => {
                              const nextValue = Number(event.target.value)
                              setPolicy((current) => ({ ...current, [field.key]: Number.isFinite(nextValue) ? nextValue : 0 }))
                            }}
                            required
                            step={field.step}
                            type="number"
                            value={String(policy[field.key])}
                          />
                          <small>{field.description}</small>
                        </label>
                      ))}
                    </div>
                  </fieldset>

                  <button
                    aria-describedby="analysis-pair-hint"
                    disabled={isLaunchingAnalysis || pairValidationError !== null}
                    type="submit"
                  >
                    {isLaunchingAnalysis ? 'Launching analysis…' : 'Launch analysis'}
                  </button>
                </form>
              </article>

              <article className="panel">
                <h3>View analysis</h3>
                <form className="analysis-form compact-form" onSubmit={submitAnalysisLookup}>
                  <label>
                    Analysis ID
                    <input
                      aria-label="Analysis ID"
                      onChange={(event) => setAnalysisLookupId(event.target.value)}
                      placeholder="Paste an analysis ID"
                      value={analysisLookupId}
                    />
                  </label>
                  <button disabled={isLoadingAnalysis} type="submit">
                    {isLoadingAnalysis ? 'Loading analysis…' : 'Load analysis'}
                  </button>
                </form>
                <p className="hint">
                  Use this when another operator has already launched an analysis and you want to inspect its live state.
                </p>
              </article>
            </div>

            {analysis && (
              <AnalysisDetails
                analysis={analysis}
                csrfToken={csrfToken}
                events={analysisEvents}
                onProductPairsChanged={refreshManagementData}
                productPairs={productPairs}
              />
            )}
          </section>

          <ProductPairsSection
            alerts={alerts}
            csrfToken={csrfToken}
            onProductPairsChanged={refreshManagementData}
            productPairs={productPairs}
            workers={workers}
          />

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
            <h2>Audit events</h2>
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

function AnalysisDetails({
  analysis,
  csrfToken,
  events,
  onProductPairsChanged,
  productPairs,
}: {
  analysis: ProductCatalogAnalysis
  csrfToken: string
  events: AuditEvent[]
  onProductPairsChanged: () => Promise<void>
  productPairs: ProductPair[]
}) {
  const passingCandidates = analysis.m1_verification_results.filter((result) => result.verification_status === 'passed')
  const failingCandidates = analysis.m1_verification_results.filter((result) => result.verification_status !== 'passed')
  const failedScreening = analysis.m15_screening_results.filter((result) => result.screening_status !== 'passed')
  const runtimeStatus = analysis.status === 'running' ? 'running' : analysis.status

  return (
    <section aria-labelledby="analysis-results-heading" className="analysis-results">
      <div className="section-header">
        <div>
          <h3 id="analysis-results-heading">Analysis {analysis.analysis_id}</h3>
          <p>
            {analysis.first_worker.login} on {analysis.first_worker.server} ↔ {analysis.second_worker.login} on {analysis.second_worker.server}
          </p>
        </div>
        <span className={`status-badge status-${runtimeStatus}`}>
          {humanizeLifecycleStatus(runtimeStatus)}
        </span>
      </div>

      <div className="summary-grid">
        <article className="panel">
          <h4>Lifecycle</h4>
          <dl className="compact-list">
            <div><dt>Status</dt><dd>{humanizeLifecycleStatus(runtimeStatus)}</dd></div>
            <div><dt>Current stage</dt><dd>{humanizeStage(analysis.current_stage)}</dd></div>
            <div><dt>Retry count</dt><dd>{analysis.retry_count}</dd></div>
            <div><dt>Requested</dt><dd><time dateTime={analysis.requested_at}>{formatDateTime(analysis.requested_at)}</time></dd></div>
            <div><dt>Completed</dt><dd>{analysis.completed_at ? <time dateTime={analysis.completed_at}>{formatDateTime(analysis.completed_at)}</time> : '—'}</dd></div>
            <div><dt>Failure reason</dt><dd>{analysis.failure_reason ?? '—'}</dd></div>
          </dl>
        </article>

        <article className="panel">
          <h4>UTC period</h4>
          <dl className="compact-list">
            <div><dt>Timeframe</dt><dd>{analysis.analysis_period.timeframe}</dd></div>
            <div><dt>Started</dt><dd><time dateTime={analysis.analysis_period.started_at_utc}>{formatDateTime(analysis.analysis_period.started_at_utc)}</time></dd></div>
            <div><dt>Ended</dt><dd><time dateTime={analysis.analysis_period.ended_at_utc}>{formatDateTime(analysis.analysis_period.ended_at_utc)}</time></dd></div>
          </dl>
        </article>

        <article className="panel">
          <h4>Policy snapshot</h4>
          <dl className="compact-list">
            <div><dt>Label</dt><dd>{analysis.policy.label}</dd></div>
            {BOOLEAN_POLICY_FIELDS.map((field) => (
              <div key={field.key}>
                <dt>{field.label}</dt>
                <dd>{analysis.policy[field.key] ? 'Yes' : 'No'}</dd>
              </div>
            ))}
            {NUMBER_POLICY_FIELDS.map((field) => (
              <div key={field.key}>
                <dt>{field.label}</dt>
                <dd>{String(analysis.policy[field.key])}</dd>
              </div>
            ))}
          </dl>
        </article>

        <article className="panel">
          <h4>Candidate totals</h4>
          <dl className="compact-list">
            <div><dt>Eligible catalog candidates</dt><dd>{analysis.eligible_candidates.length}</dd></div>
            <div><dt>Calculation-mode exceptions</dt><dd>{analysis.exceptions.length}</dd></div>
            <div><dt>M15 passed</dt><dd>{analysis.m15_screening_results.filter((result) => result.screening_status === 'passed').length}</dd></div>
            <div><dt>Final passing candidates</dt><dd>{passingCandidates.length}</dd></div>
            <div><dt>Final failing candidates</dt><dd>{failingCandidates.length}</dd></div>
          </dl>
        </article>
      </div>

      <section aria-labelledby="analysis-timeline-heading">
        <h4 id="analysis-timeline-heading">Lifecycle and retry events</h4>
        {events.length === 0 ? (
          <p>No analysis-specific audit events have been recorded yet.</p>
        ) : (
          <ol className="timeline" aria-label="Analysis lifecycle events">
            {events.map((event) => (
              <li key={event.event_id}>
                <div>
                  <strong>{EVENT_LABELS[event.event_type] ?? humanizeToken(event.event_type)}</strong>
                  <p>{describeAnalysisEvent(event)}</p>
                </div>
                <time dateTime={event.occurred_at}>{formatDateTime(event.occurred_at)}</time>
              </li>
            ))}
          </ol>
        )}
      </section>

      <section aria-labelledby="catalog-evidence-heading">
        <h4 id="catalog-evidence-heading">Catalog evidence</h4>
        <div className="summary-grid">
          <CatalogEvidenceCard evidence={analysis.first_catalog_evidence} worker={analysis.first_worker} />
          <CatalogEvidenceCard evidence={analysis.second_catalog_evidence} worker={analysis.second_worker} />
        </div>
      </section>

      <section aria-labelledby="m15-results-heading">
        <h4 id="m15-results-heading">M15 screening results</h4>
        {analysis.m15_screening_results.length === 0 ? (
          <p>No M15 screening results were recorded.</p>
        ) : (
          <div className="result-grid">
            {analysis.m15_screening_results.map((result) => (
              <AnalysisResultCard
                key={`${result.first_symbol}:${result.second_symbol}:m15`}
                firstWorker={analysis.first_worker}
                result={result}
                secondWorker={analysis.second_worker}
              />
            ))}
          </div>
        )}
        {failedScreening.length > 0 && (
          <p className="hint">{failedScreening.length} candidate(s) stopped at M15 and never reached final verification.</p>
        )}
      </section>

      <section aria-labelledby="final-passing-heading">
        <h4 id="final-passing-heading">Final passing candidates</h4>
        {passingCandidates.length === 0 ? (
          <p>No final passing candidates were recorded.</p>
        ) : (
          <div className="result-grid">
            {passingCandidates.map((result) => (
              <BuildableVerificationCard
                analysis={analysis}
                csrfToken={csrfToken}
                key={`${result.first_symbol}:${result.second_symbol}:passed`}
                firstWorker={analysis.first_worker}
                onProductPairsChanged={onProductPairsChanged}
                productPairs={productPairs}
                result={result}
                secondWorker={analysis.second_worker}
              />
            ))}
          </div>
        )}
      </section>

      <section aria-labelledby="final-failing-heading">
        <h4 id="final-failing-heading">Final failing candidates</h4>
        {failingCandidates.length === 0 ? (
          <p>No final failing candidates were recorded.</p>
        ) : (
          <div className="result-grid">
            {failingCandidates.map((result) => (
              <AnalysisResultCard
                key={`${result.first_symbol}:${result.second_symbol}:failed`}
                firstWorker={analysis.first_worker}
                result={result}
                secondWorker={analysis.second_worker}
              />
            ))}
          </div>
        )}
      </section>

      <section aria-labelledby="analysis-exceptions-heading">
        <h4 id="analysis-exceptions-heading">Calculation-mode exceptions</h4>
        {analysis.exceptions.length === 0 ? (
          <p>No catalog exceptions were recorded.</p>
        ) : (
          <ul className="exception-list" aria-label="Analysis exceptions">
            {analysis.exceptions.map((exception) => (
              <li key={`${exception.first_symbol}:${exception.second_symbol}`}>
                <strong>{exception.first_symbol} ↔ {exception.second_symbol}</strong>
                <span>{humanizeToken(exception.reason)}</span>
                <span>{exception.first_trade_calc_mode} / {exception.second_trade_calc_mode}</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </section>
  )
}

function BuildableVerificationCard({
  analysis,
  csrfToken,
  firstWorker,
  onProductPairsChanged,
  productPairs,
  result,
  secondWorker,
}: {
  analysis: ProductCatalogAnalysis
  csrfToken: string
  firstWorker: WorkerReference
  onProductPairsChanged: () => Promise<void>
  productPairs: ProductPair[]
  result: VerificationResult
  secondWorker: WorkerReference
}) {
  const [confirmation, setConfirmation] = useState<ProductPairBuildConfirmation | null>(null)
  const [isPreparing, setIsPreparing] = useState(false)
  const [isApplying, setIsApplying] = useState(false)
  const [confirmationAccepted, setConfirmationAccepted] = useState(false)
  const [feedback, setFeedback] = useState<{ error: string | null; success: string | null }>({
    error: null,
    success: null,
  })
  const conflictPair = useMemo(
    () => confirmation === null ? null : findMatchingActiveProductPair(productPairs, confirmation.endpoints),
    [confirmation, productPairs],
  )
  const candidateId = `${result.first_symbol}:${result.second_symbol}`

  async function requestBuildConfirmation() {
    setFeedback({ error: null, success: null })
    setIsPreparing(true)
    try {
      const response = await fetch(
        `/api/admin/product-catalog-analyses/${encodeURIComponent(analysis.analysis_id)}/product-pair-build-confirmations`,
        {
          method: 'POST',
          headers: {
            'X-CSRF-Token': csrfToken,
            'content-type': 'application/json',
          },
          credentials: 'same-origin',
          body: JSON.stringify({
            first_symbol: result.first_symbol,
            second_symbol: result.second_symbol,
          }),
        },
      )
      if (!response.ok) {
        throw new Error(await readResponseDetail(response, 'The Build confirmation could not be prepared.'))
      }
      const payload = (await response.json()) as ProductPairBuildConfirmation
      setConfirmation(payload)
      setConfirmationAccepted(false)
    } catch (buildError) {
      setFeedback({
        error: buildError instanceof Error ? buildError.message : 'The Build confirmation could not be prepared.',
        success: null,
      })
    } finally {
      setIsPreparing(false)
    }
  }

  async function applyBuildAction(mode: 'build' | 'replace') {
    if (confirmation === null || !confirmationAccepted) {
      return
    }
    const targetPair = mode === 'replace' ? conflictPair : null
    if (mode === 'replace' && targetPair === null) {
      setFeedback({
        error: 'Reload product pairs before replacing the conflicting active pair.',
        success: null,
      })
      return
    }

    setFeedback({ error: null, success: null })
    setIsApplying(true)
    try {
      const response = await fetch(
        mode === 'build'
          ? '/api/admin/product-pairs'
          : `/api/admin/product-pairs/${encodeURIComponent(targetPair!.product_pair_id)}/replace`,
        {
          method: 'POST',
          headers: {
            'X-CSRF-Token': csrfToken,
            'content-type': 'application/json',
          },
          credentials: 'same-origin',
          body: JSON.stringify({ confirmation_id: confirmation.confirmation_id }),
        },
      )
      if (!response.ok) {
        throw new Error(await readResponseDetail(
          response,
          mode === 'build' ? 'The product pair could not be built.' : 'The active product pair could not be replaced.',
        ))
      }
      const payload = (await response.json()) as ProductPair
      await onProductPairsChanged()
      setFeedback({
        error: null,
        success: mode === 'build'
          ? `Built active product pair ${payload.product_pair_id}.`
          : `Replaced the active pair with ${payload.product_pair_id}.`,
      })
    } catch (buildError) {
      setFeedback({
        error: buildError instanceof Error
          ? buildError.message
          : mode === 'build'
            ? 'The product pair could not be built.'
            : 'The active product pair could not be replaced.',
        success: null,
      })
    } finally {
      setIsApplying(false)
    }
  }

  return (
    <div className="buildable-result-card">
      <AnalysisResultCard
        firstWorker={firstWorker}
        result={result}
        secondWorker={secondWorker}
      />
      <article className="panel build-panel" aria-labelledby={`build-panel-${candidateId}`}>
        <div className="section-header">
          <div>
            <h6 id={`build-panel-${candidateId}`}>Build candidate confirmation</h6>
            <p>Build only records a control-plane product pair. It never places, modifies, or closes broker orders.</p>
          </div>
          <button disabled={isPreparing || isApplying} onClick={() => void requestBuildConfirmation()} type="button">
            {isPreparing ? 'Preparing confirmation…' : 'Prepare Build confirmation'}
          </button>
        </div>

        {feedback.error && <p className="error" role="alert">{feedback.error}</p>}
        {feedback.success && <p className="success" role="status">{feedback.success}</p>}

        {confirmation ? (
          <>
            <div className="summary-grid">
              <article className="panel">
                <h6>Approval summary</h6>
                <dl className="compact-list">
                  <div><dt>Lot relationship</dt><dd>{confirmation.lot_relationship.ratio}</dd></div>
                  <div><dt>UTC timeframe</dt><dd>{confirmation.analysis_period.timeframe}</dd></div>
                  <div><dt>UTC started</dt><dd><time dateTime={confirmation.analysis_period.started_at_utc}>{formatDateTime(confirmation.analysis_period.started_at_utc)}</time></dd></div>
                  <div><dt>UTC ended</dt><dd><time dateTime={confirmation.analysis_period.ended_at_utc}>{formatDateTime(confirmation.analysis_period.ended_at_utc)}</time></dd></div>
                  <div><dt>Coverage</dt><dd>{formatRatio(confirmation.approval_evidence.statistics.coverage_ratio)}</dd></div>
                  <div><dt>M1 return correlation</dt><dd>{formatDecimal(confirmation.approval_evidence.statistics.return_correlation)}</dd></div>
                </dl>
              </article>

              <article className="panel">
                <h6>Source workers</h6>
                <dl className="compact-list">
                  <div><dt>First source</dt><dd>{formatWorkerReference(confirmation.source_workers.first_worker)}</dd></div>
                  <div><dt>Second source</dt><dd>{formatWorkerReference(confirmation.source_workers.second_worker)}</dd></div>
                  <div><dt>Endpoints</dt><dd>{formatEndpointPair(confirmation.endpoints)}</dd></div>
                  <div><dt>Policy label</dt><dd>{confirmation.policy_snapshot.label}</dd></div>
                </dl>
              </article>

              <article className="panel">
                <h6>Policy snapshot</h6>
                <dl className="compact-list">
                  {BOOLEAN_POLICY_FIELDS.map((field) => (
                    <div key={field.key}>
                      <dt>{field.label}</dt>
                      <dd>{confirmation.policy_snapshot[field.key] ? 'Yes' : 'No'}</dd>
                    </div>
                  ))}
                  {NUMBER_POLICY_FIELDS.map((field) => (
                    <div key={field.key}>
                      <dt>{field.label}</dt>
                      <dd>{String(confirmation.policy_snapshot[field.key])}</dd>
                    </div>
                  ))}
                </dl>
              </article>
            </div>

            <div className="summary-grid">
              {confirmation.reference_specifications.map((reference) => (
                <ReferenceSpecificationCard key={`${reference.server}:${reference.symbol}`} reference={reference} />
              ))}
            </div>

            <details>
              <summary>View immutable approval evidence</summary>
              <pre>{JSON.stringify({
                approval_evidence: confirmation.approval_evidence,
                policy_snapshot: confirmation.policy_snapshot,
                reference_specifications: confirmation.reference_specifications,
              }, null, 2)}</pre>
            </details>

            {conflictPair && (
              <div className="conflict-panel" role="status">
                <strong>Build conflict:</strong> an active product pair already exists for {formatEndpointPair(conflictPair.endpoints)}.
              </div>
            )}

            <label className="checkbox-field confirmation-check">
              <input
                checked={confirmationAccepted}
                onChange={(event) => setConfirmationAccepted(event.target.checked)}
                type="checkbox"
              />
              <span>
                <strong>Explicit Build confirmation</strong>
                <small>I verified the immutable evidence and want this candidate to become the active cross-server product pair.</small>
              </span>
            </label>

            <div className="action-row">
              <button
                disabled={isApplying || !confirmationAccepted}
                onClick={() => void applyBuildAction('build')}
                type="button"
              >
                {isApplying ? 'Saving…' : 'Build product pair'}
              </button>
              {conflictPair && (
                <button
                  className="secondary-button"
                  disabled={isApplying || !confirmationAccepted}
                  onClick={() => void applyBuildAction('replace')}
                  type="button"
                >
                  {isApplying ? 'Saving…' : 'Replace active pair'}
                </button>
              )}
            </div>
          </>
        ) : (
          <p className="hint">Prepare the Build confirmation to review immutable reference specifications, source workers, and the 1:1 lot relationship before Build is enabled.</p>
        )}
      </article>
    </div>
  )
}

function ProductPairsSection({
  alerts,
  csrfToken,
  onProductPairsChanged,
  productPairs,
  workers,
}: {
  alerts: WorkerAlert[]
  csrfToken: string
  onProductPairsChanged: () => Promise<void>
  productPairs: ProductPair[]
  workers: AccountWorker[]
}) {
  const activePairs = useMemo(
    () => productPairs.filter((pair) => pair.status === 'active'),
    [productPairs],
  )
  const retiredPairs = useMemo(
    () => productPairs.filter((pair) => pair.status !== 'active'),
    [productPairs],
  )

  return (
    <section aria-labelledby="product-pairs-heading" className="analysis-section">
      <div className="section-header">
        <div>
          <h2 id="product-pairs-heading">Active product pairs</h2>
          <p>Inspect active and retired pairs, their approval evidence, and retirement history without any broker-write path.</p>
        </div>
        <span className="status-badge status-queued">{activePairs.length} active / {retiredPairs.length} retired</span>
      </div>

      {productPairs.length === 0 ? (
        <p>No product pairs have been built yet.</p>
      ) : (
        <>
          <div className="product-pair-list" aria-label="Active product pairs">
            {activePairs.length === 0 ? <p>No active product pairs.</p> : activePairs.map((pair) => (
              <ProductPairCard
                alerts={alerts}
                csrfToken={csrfToken}
                key={pair.product_pair_id}
                onProductPairsChanged={onProductPairsChanged}
                pair={pair}
                workers={workers}
              />
            ))}
          </div>

          <section aria-labelledby="retired-product-pairs-heading">
            <h3 id="retired-product-pairs-heading">Retirement history</h3>
            {retiredPairs.length === 0 ? (
              <p>No retired product pairs.</p>
            ) : (
              <div className="product-pair-list" aria-label="Retired product pairs">
                {retiredPairs.map((pair) => (
                  <ProductPairCard
                    alerts={alerts}
                    csrfToken={csrfToken}
                    key={pair.product_pair_id}
                    onProductPairsChanged={onProductPairsChanged}
                    pair={pair}
                    workers={workers}
                  />
                ))}
              </div>
            )}
          </section>
        </>
      )}
    </section>
  )
}

function ProductPairCard({
  alerts,
  csrfToken,
  onProductPairsChanged,
  pair,
  workers,
}: {
  alerts: WorkerAlert[]
  csrfToken: string
  onProductPairsChanged: () => Promise<void>
  pair: ProductPair
  workers: AccountWorker[]
}) {
  const [isRetiring, setIsRetiring] = useState(false)
  const [isSubmittingRetest, setIsSubmittingRetest] = useState(false)
  const [retestError, setRetestError] = useState<string | null>(null)
  const [retestSuccess, setRetestSuccess] = useState<string | null>(null)
  const [compatibilityError, setCompatibilityError] = useState<string | null>(null)
  const [compatibilitySuccess, setCompatibilitySuccess] = useState<string | null>(null)
  const [checkingWorkerId, setCheckingWorkerId] = useState<string | null>(null)
  const [excludingWorkerId, setExcludingWorkerId] = useState<string | null>(null)
  const [compatibilityResults, setCompatibilityResults] = useState<Record<string, WorkerCompatibilityResult>>({})
  const pairWorkers = useMemo(
    () => workers.filter((worker) => pair.endpoints.some((endpoint) => endpoint.server === worker.server)),
    [pair.endpoints, workers],
  )
  const pairWorkerRows = useMemo(
    () => pairWorkers.map((worker) => {
      const applicability = pair.worker_applicability.find((entry) => entry.worker_id === worker.worker_id) ?? null
      return {
        worker,
        applicability,
        compatibility: compatibilityResults[worker.worker_id]
          ?? getApplicabilityCompatibilityResult(applicability)
          ?? null,
        state: deriveApplicabilityState(applicability),
      }
    }),
    [compatibilityResults, pair.worker_applicability, pairWorkers],
  )
  const eligibleRetestWorkers = useMemo(() => ({
    first: workers.filter((worker) => worker.server === pair.source_workers.first_worker.server && workerEligibilityReason(worker) === null),
    second: workers.filter((worker) => worker.server === pair.source_workers.second_worker.server && workerEligibilityReason(worker) === null),
  }), [pair.source_workers.first_worker.server, pair.source_workers.second_worker.server, workers])
  const [firstRetestWorkerId, setFirstRetestWorkerId] = useState('')
  const [secondRetestWorkerId, setSecondRetestWorkerId] = useState('')
  const [error, setError] = useState<string | null>(null)
  const relatedRetestAlert = useMemo(
    () => findRetestAlert(alerts, pair),
    [alerts, pair],
  )
  const latestRetest = pair.latest_retest
  const retestValidationError = useMemo(
    () => validateRetestSelection(pair, eligibleRetestWorkers.first, eligibleRetestWorkers.second, firstRetestWorkerId, secondRetestWorkerId),
    [eligibleRetestWorkers.first, eligibleRetestWorkers.second, firstRetestWorkerId, pair, secondRetestWorkerId],
  )

  useEffect(() => {
    if (!eligibleRetestWorkers.first.some((worker) => worker.worker_id === firstRetestWorkerId)) {
      setFirstRetestWorkerId(eligibleRetestWorkers.first[0]?.worker_id ?? '')
    }
  }, [eligibleRetestWorkers.first, firstRetestWorkerId])

  useEffect(() => {
    if (!eligibleRetestWorkers.second.some((worker) => worker.worker_id === secondRetestWorkerId)) {
      setSecondRetestWorkerId(eligibleRetestWorkers.second[0]?.worker_id ?? '')
    }
  }, [eligibleRetestWorkers.second, secondRetestWorkerId])

  async function retirePair() {
    setError(null)
    setIsRetiring(true)
    try {
      const response = await fetch(`/api/admin/product-pairs/${encodeURIComponent(pair.product_pair_id)}/retire`, {
        method: 'POST',
        headers: { 'X-CSRF-Token': csrfToken },
        credentials: 'same-origin',
      })
      if (!response.ok) {
        throw new Error(await readResponseDetail(response, 'The product pair could not be retired.'))
      }
      await onProductPairsChanged()
    } catch (retireError) {
      setError(retireError instanceof Error ? retireError.message : 'The product pair could not be retired.')
    } finally {
      setIsRetiring(false)
    }
  }

  async function runCompatibilityCheck(worker: AccountWorker) {
    setCompatibilityError(null)
    setCompatibilitySuccess(null)
    setCheckingWorkerId(worker.worker_id)
    try {
      const response = await fetch(
        `/api/admin/product-pairs/${encodeURIComponent(pair.product_pair_id)}/workers/${encodeURIComponent(worker.worker_id)}/compatibility-check`,
        {
          method: 'POST',
          headers: { 'X-CSRF-Token': csrfToken },
          credentials: 'same-origin',
        },
      )
      if (!response.ok) {
        throw new Error(await readResponseDetail(response, 'The compatibility check could not be completed.'))
      }
      const payload = (await response.json()) as WorkerCompatibilityResult
      setCompatibilityResults((current) => ({ ...current, [worker.worker_id]: payload }))
      setCompatibilitySuccess(`Compatibility check recorded for ${worker.login} on ${worker.server}.`)
    } catch (checkError) {
      setCompatibilityError(checkError instanceof Error ? checkError.message : 'The compatibility check could not be completed.')
    } finally {
      setCheckingWorkerId(null)
    }
  }

  async function excludeWorker(worker: AccountWorker) {
    setCompatibilityError(null)
    setCompatibilitySuccess(null)
    setExcludingWorkerId(worker.worker_id)
    try {
      const response = await fetch(
        `/api/admin/product-pairs/${encodeURIComponent(pair.product_pair_id)}/workers/${encodeURIComponent(worker.worker_id)}/exclude`,
        {
          method: 'POST',
          headers: { 'X-CSRF-Token': csrfToken },
          credentials: 'same-origin',
        },
      )
      if (!response.ok) {
        throw new Error(await readResponseDetail(response, 'The worker could not be excluded from this pair.'))
      }
      await onProductPairsChanged()
      setCompatibilityResults((current) => {
        const next = { ...current }
        delete next[worker.worker_id]
        return next
      })
      setCompatibilitySuccess(`Excluded ${worker.login} on ${worker.server} from product pair ${pair.product_pair_id}.`)
    } catch (excludeError) {
      setCompatibilityError(excludeError instanceof Error ? excludeError.message : 'The worker could not be excluded from this pair.')
    } finally {
      setExcludingWorkerId(null)
    }
  }

  async function submitRetest(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (retestValidationError !== null) {
      setRetestError(retestValidationError)
      return
    }

    setRetestError(null)
    setRetestSuccess(null)
    setIsSubmittingRetest(true)
    try {
      const response = await fetch(`/api/admin/product-pairs/${encodeURIComponent(pair.product_pair_id)}/retests`, {
        method: 'POST',
        headers: {
          'X-CSRF-Token': csrfToken,
          'content-type': 'application/json',
        },
        credentials: 'same-origin',
        body: JSON.stringify({
          first_worker_id: firstRetestWorkerId,
          second_worker_id: secondRetestWorkerId,
        }),
      })
      if (!response.ok) {
        throw new Error(await readResponseDetail(response, 'The re-test could not be started.'))
      }
      await onProductPairsChanged()
      setRetestSuccess(`Recorded a re-test request for ${pair.product_pair_id}.`)
    } catch (submitError) {
      setRetestError(submitError instanceof Error ? submitError.message : 'The re-test could not be started.')
    } finally {
      setIsSubmittingRetest(false)
    }
  }

  return (
    <article className="result-card product-pair-card">
      <div className="section-header">
        <div>
          <h3>{formatEndpointPair(pair.endpoints)}</h3>
          <p>{pair.product_pair_id}</p>
        </div>
        <span className={`status-badge ${pair.status === 'active' ? 'status-succeeded' : 'status-retired'}`}>
          {humanizePairStatus(pair.status)}
        </span>
      </div>

      {error && <p className="error" role="alert">{error}</p>}
      {compatibilityError && <p className="error" role="alert">{compatibilityError}</p>}
      {compatibilitySuccess && <p className="success" role="status">{compatibilitySuccess}</p>}
      {retestError && <p className="error" role="alert">{retestError}</p>}
      {retestSuccess && <p className="success" role="status">{retestSuccess}</p>}

      <div className="summary-grid">
        <article className="panel">
          <h4>Approval evidence</h4>
          <dl className="compact-list">
            <div><dt>Built from analysis</dt><dd>{pair.built_from_analysis_id}</dd></div>
            <div><dt>Lot relationship</dt><dd>{pair.lot_relationship.ratio}</dd></div>
            <div><dt>Coverage</dt><dd>{formatRatio(pair.approval_evidence.statistics.coverage_ratio)}</dd></div>
            <div><dt>M1 return correlation</dt><dd>{formatDecimal(pair.approval_evidence.statistics.return_correlation)}</dd></div>
            <div><dt>Hard-block differences</dt><dd>{pair.approval_evidence.hard_block_differences.length}</dd></div>
            <div><dt>Warning differences</dt><dd>{pair.approval_evidence.warning_differences.length}</dd></div>
          </dl>
        </article>

        <article className="panel">
          <h4>Source workers</h4>
          <dl className="compact-list">
            <div><dt>First source</dt><dd>{formatWorkerReference(pair.source_workers.first_worker)}</dd></div>
            <div><dt>Second source</dt><dd>{formatWorkerReference(pair.source_workers.second_worker)}</dd></div>
            <div><dt>Built by</dt><dd>{pair.built_by}</dd></div>
            <div><dt>Built at</dt><dd><time dateTime={pair.created_at}>{formatDateTime(pair.created_at)}</time></dd></div>
          </dl>
        </article>

        <article className="panel">
          <h4>Latest re-test</h4>
          {latestRetest === null ? (
            <p>No re-test recorded.</p>
          ) : (
            <RetestSummary retest={latestRetest} />
          )}
        </article>

        <article className="panel">
          <h4>Retirement history</h4>
          <dl className="compact-list">
            <div><dt>Status</dt><dd>{humanizePairStatus(pair.status)}</dd></div>
            <div><dt>Retired at</dt><dd>{pair.retired_at ? <time dateTime={pair.retired_at}>{formatDateTime(pair.retired_at)}</time> : '—'}</dd></div>
            <div><dt>Retired by</dt><dd>{pair.retired_by ?? '—'}</dd></div>
            <div><dt>Reason</dt><dd>{describeRetiredReason(pair.retired_reason)}</dd></div>
            <div><dt>Replaced by</dt><dd>{pair.replaced_by_product_pair_id ?? '—'}</dd></div>
            <div><dt>Replaces</dt><dd>{pair.replaces_product_pair_id ?? '—'}</dd></div>
          </dl>
        </article>
      </div>

      <details>
        <summary>View reference specifications</summary>
        <div className="summary-grid">
          {pair.reference_specifications.map((reference) => (
            <ReferenceSpecificationCard key={`${pair.product_pair_id}:${reference.server}:${reference.symbol}`} reference={reference} />
          ))}
        </div>
      </details>

      <section aria-labelledby={`pair-workers-${pair.product_pair_id}`}>
        <div className="section-header">
          <div>
            <h4 id={`pair-workers-${pair.product_pair_id}`}>Worker applicability</h4>
            <p>Applicability stays server-wide by default until you explicitly exclude one worker from this one pair.</p>
          </div>
        </div>
        {pairWorkerRows.length === 0 ? (
          <p>No workers from this pair&apos;s endpoint servers are currently connected to the console.</p>
        ) : (
          <div className="pair-worker-list" aria-label={`Worker applicability for ${pair.product_pair_id}`}>
            {pairWorkerRows.map(({ applicability, compatibility, state, worker }) => (
              <article className="panel pair-worker-card" key={worker.worker_id}>
                <div className="section-header">
                  <div>
                    <h5>{worker.login} on {worker.server}</h5>
                    <p>{worker.connectivity} / {worker.safety_state}</p>
                  </div>
                  <span className={`status-badge ${applicabilityStatusClassName(state)}`}>
                    {humanizeApplicabilityStatus(state)}
                  </span>
                </div>

                <dl className="compact-list">
                  <div><dt>Pair scope</dt><dd>{pair.product_pair_id}</dd></div>
                  <div><dt>Status meaning</dt><dd>{describeApplicabilityState(state)}</dd></div>
                  <div><dt>Inspection</dt><dd>{humanizeInspectionStatus(applicability?.inspection_status, compatibility)}</dd></div>
                  <div><dt>Checked at</dt><dd>{compatibility?.checked_at ? <time dateTime={compatibility.checked_at}>{formatDateTime(compatibility.checked_at)}</time> : '—'}</dd></div>
                  <div><dt>Excluded at</dt><dd>{applicability?.exclusion?.excluded_at ? <time dateTime={applicability.exclusion.excluded_at}>{formatDateTime(applicability.exclusion.excluded_at)}</time> : '—'}</dd></div>
                  <div><dt>Excluded by</dt><dd>{applicability?.exclusion?.excluded_by ?? '—'}</dd></div>
                </dl>

                <div className="action-row">
                  <button
                    disabled={checkingWorkerId === worker.worker_id || excludingWorkerId === worker.worker_id}
                    onClick={() => void runCompatibilityCheck(worker)}
                    type="button"
                  >
                    {checkingWorkerId === worker.worker_id ? 'Checking compatibility…' : `Run compatibility check for ${worker.login} on ${worker.server}`}
                  </button>
                </div>

                {compatibility ? (
                  <WorkerCompatibilityPanel
                    compatibility={compatibility}
                    onExclude={state === 'excluded' ? null : () => void excludeWorker(worker)}
                    pairId={pair.product_pair_id}
                    worker={worker}
                    excluding={excludingWorkerId === worker.worker_id}
                  />
                ) : (
                  <p className="hint">No compatibility evidence recorded yet for this worker on this pair.</p>
                )}
              </article>
            ))}
          </div>
        )}
      </section>

      <section aria-labelledby={`retest-${pair.product_pair_id}`} className="panel">
        <div className="section-header">
          <div>
            <h4 id={`retest-${pair.product_pair_id}`}>Manual re-test</h4>
            <p>Re-testing uses the original policy snapshot and never retires the pair automatically.</p>
          </div>
        </div>
        <div className="summary-grid">
          <article className="panel">
            <h5>Original policy snapshot</h5>
            <dl className="compact-list">
              <div><dt>Policy label</dt><dd>{pair.policy_snapshot.label}</dd></div>
              {BOOLEAN_POLICY_FIELDS.map((field) => (
                <div key={field.key}>
                  <dt>{field.label}</dt>
                  <dd>{pair.policy_snapshot[field.key] ? 'Yes' : 'No'}</dd>
                </div>
              ))}
              {NUMBER_POLICY_FIELDS.map((field) => (
                <div key={field.key}>
                  <dt>{field.label}</dt>
                  <dd>{String(pair.policy_snapshot[field.key])}</dd>
                </div>
              ))}
            </dl>
          </article>

          <article className="panel">
            <h5>Submit re-test</h5>
            <form className="analysis-form" onSubmit={submitRetest}>
              <label>
                Re-test worker for {pair.source_workers.first_worker.server}:{pair.endpoints.find((endpoint) => endpoint.server === pair.source_workers.first_worker.server)?.symbol ?? pair.endpoints[0]?.symbol}
                <select
                  aria-label={`Re-test worker for ${pair.source_workers.first_worker.server}`}
                  onChange={(event) => setFirstRetestWorkerId(event.target.value)}
                  value={firstRetestWorkerId}
                >
                  <option value="">Select a healthy connected worker</option>
                  {eligibleRetestWorkers.first.map((worker) => (
                    <option key={worker.worker_id} value={worker.worker_id}>
                      {worker.login} on {worker.server}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Re-test worker for {pair.source_workers.second_worker.server}:{pair.endpoints.find((endpoint) => endpoint.server === pair.source_workers.second_worker.server)?.symbol ?? pair.endpoints[1]?.symbol}
                <select
                  aria-label={`Re-test worker for ${pair.source_workers.second_worker.server}`}
                  onChange={(event) => setSecondRetestWorkerId(event.target.value)}
                  value={secondRetestWorkerId}
                >
                  <option value="">Select a healthy connected worker</option>
                  {eligibleRetestWorkers.second.map((worker) => (
                    <option key={worker.worker_id} value={worker.worker_id}>
                      {worker.login} on {worker.server}
                    </option>
                  ))}
                </select>
              </label>
              {retestValidationError ? (
                <p className="hint error" id={`retest-hint-${pair.product_pair_id}`}>{retestValidationError}</p>
              ) : (
                <p className="hint" id={`retest-hint-${pair.product_pair_id}`}>
                  Only healthy connected workers on the pair&apos;s two exact endpoint servers can be selected.
                </p>
              )}
              <button
                aria-describedby={`retest-hint-${pair.product_pair_id}`}
                disabled={isSubmittingRetest || retestValidationError !== null}
                type="submit"
              >
                {isSubmittingRetest ? 'Submitting re-test…' : 'Run manual re-test'}
              </button>
            </form>
          </article>
        </div>

        {(latestRetest?.status === 'failed' || relatedRetestAlert) && (
          <div className="conflict-panel" role="status">
            <strong>Latest failed re-test:</strong>{' '}
            {relatedRetestAlert
              ? `${relatedRetestAlert.priority}: ${relatedRetestAlert.alert_type} — ${relatedRetestAlert.reason}`
              : 'The pair remains active until an administrator explicitly retires it.'}
          </div>
        )}
      </section>

      {pair.status === 'active' && (
        <div className="action-row">
          <button disabled={isRetiring} onClick={() => void retirePair()} type="button">
            {isRetiring ? 'Retiring…' : 'Retire product pair'}
          </button>
        </div>
      )}
    </article>
  )
}

function ReferenceSpecificationCard({ reference }: { reference: ProductPairReferenceSpecification }) {
  return (
    <article className="panel">
      <h6>{reference.server} · {reference.symbol}</h6>
      <details open>
        <summary>View immutable reference specification</summary>
        <pre>{JSON.stringify(reference.specification, null, 2)}</pre>
      </details>
    </article>
  )
}

function WorkerCompatibilityPanel({
  compatibility,
  excluding,
  onExclude,
  pairId,
  worker,
}: {
  compatibility: WorkerCompatibilityResult
  excluding: boolean
  onExclude: (() => void) | null
  pairId: string
  worker: AccountWorker
}) {
  return (
    <section aria-label={`Compatibility evidence for ${worker.login} on ${worker.server}`} className="compatibility-panel">
      <div className="summary-grid">
        <article className="panel">
          <h6>Compatibility summary</h6>
          <dl className="compact-list">
            <div><dt>Reference symbol</dt><dd>{compatibility.reference_symbol}</dd></div>
            <div><dt>Inspection</dt><dd>{humanizeInspectionStatus(compatibility.inspection_status, compatibility)}</dd></div>
            <div><dt>Checked at</dt><dd>{compatibility.checked_at ? <time dateTime={compatibility.checked_at}>{formatDateTime(compatibility.checked_at)}</time> : '—'}</dd></div>
            <div><dt>Checked by</dt><dd>{compatibility.checked_by ?? '—'}</dd></div>
            <div><dt>Hard-block differences</dt><dd>{compatibility.hard_block_differences.length}</dd></div>
            <div><dt>Warning differences</dt><dd>{compatibility.warning_differences.length}</dd></div>
          </dl>
        </article>

        <article className="panel">
          <h6>Live specification</h6>
          <pre>{JSON.stringify(compatibility.live_specification, null, 2)}</pre>
        </article>

        <article className="panel">
          <h6>Reference specification</h6>
          <pre>{JSON.stringify(compatibility.reference_specification, null, 2)}</pre>
        </article>
      </div>

      <div className="summary-grid">
        <DifferenceCard differences={compatibility.hard_block_differences} title="Hard-block differences" />
        <DifferenceCard differences={compatibility.warning_differences} title="Warning differences" />
      </div>

      {onExclude ? (
        <div className="action-row">
          <button
            className="reject-button"
            disabled={excluding}
            onClick={onExclude}
            type="button"
          >
            {excluding ? 'Excluding worker…' : `Exclude ${worker.login} on ${worker.server} from ${pairId}`}
          </button>
        </div>
      ) : (
        <p className="hint">This worker is already explicitly excluded from this pair.</p>
      )}
    </section>
  )
}

function RetestSummary({ retest }: { retest: ProductPairRetest }) {
  const statistics = getRetestStatistics(retest)
  const alert = retest.alert ?? retest.latest_alert ?? null

  return (
    <>
      <dl className="compact-list">
        <div><dt>Status</dt><dd>{humanizeRetestStatus(retest.status)}</dd></div>
        <div><dt>Current stage</dt><dd>{humanizeStage(retest.current_stage)}</dd></div>
        <div><dt>Requested</dt><dd><time dateTime={retest.requested_at}>{formatDateTime(retest.requested_at)}</time></dd></div>
        <div><dt>Completed</dt><dd>{retest.completed_at ? <time dateTime={retest.completed_at}>{formatDateTime(retest.completed_at)}</time> : '—'}</dd></div>
        <div><dt>Failure reason</dt><dd>{retest.failure_reason ?? '—'}</dd></div>
        <div><dt>First source</dt><dd>{formatWorkerReference(retest.source_workers.first_worker)}</dd></div>
        <div><dt>Second source</dt><dd>{formatWorkerReference(retest.source_workers.second_worker)}</dd></div>
        <div><dt>Coverage</dt><dd>{statistics ? formatRatio(statistics.coverage_ratio) : '—'}</dd></div>
        <div><dt>M1 return correlation</dt><dd>{statistics ? formatDecimal(statistics.return_correlation) : '—'}</dd></div>
      </dl>
      {alert && (
        <div className="conflict-panel" role="status">
          <strong>{alert.priority}: {alert.alert_type}</strong> — {alert.reason}
        </div>
      )}
    </>
  )
}

function CatalogEvidenceCard({ evidence, worker }: { evidence: CatalogEvidence | null; worker: WorkerReference }) {
  return (
    <article className="panel">
      <h5>{worker.login} on {worker.server}</h5>
      {evidence === null ? (
        <p>Catalog evidence is not available yet.</p>
      ) : (
        <>
          <dl className="compact-list">
            <div><dt>Collected</dt><dd>{evidence.collected_at ? <time dateTime={evidence.collected_at}>{formatDateTime(evidence.collected_at)}</time> : '—'}</dd></div>
            <div><dt>Symbols</dt><dd>{evidence.symbols.length}</dd></div>
            <div><dt>Sample</dt><dd>{evidence.symbols.slice(0, 3).map((symbol) => String(symbol.symbol ?? 'unknown')).join(', ') || '—'}</dd></div>
          </dl>
          <details>
            <summary>View raw catalog evidence</summary>
            <pre>{JSON.stringify(evidence, null, 2)}</pre>
          </details>
        </>
      )}
    </article>
  )
}

function AnalysisResultCard({
  result,
  firstWorker,
  secondWorker,
}: {
  result: ScreeningResult | VerificationResult
  firstWorker: WorkerReference
  secondWorker: WorkerReference
}) {
  const verificationStatus = 'verification_status' in result ? result.verification_status : null
  const badgeStatus = verificationStatus ?? result.screening_status

  return (
    <article className="result-card">
      <div className="section-header">
        <div>
          <h5>{result.first_symbol} ↔ {result.second_symbol}</h5>
          <p>{result.currency_base}/{result.currency_profit} · calibration {result.first_point} / {result.second_point}</p>
        </div>
        <span className={`status-badge status-${badgeStatus === 'passed' ? 'succeeded' : 'failed'}`}>
          {verificationStatus ? `Final ${humanizeToken(verificationStatus)}` : `M15 ${humanizeToken(result.screening_status)}`}
        </span>
      </div>

      <div className="summary-grid">
        <article className="panel">
          <h6>Statistics</h6>
          <dl className="compact-list">
            <div><dt>Aligned bars</dt><dd>{result.statistics.aligned_bar_count}</dd></div>
            <div><dt>First bars</dt><dd>{result.statistics.first_bar_count}</dd></div>
            <div><dt>Second bars</dt><dd>{result.statistics.second_bar_count}</dd></div>
            <div><dt>Coverage</dt><dd>{formatRatio(result.statistics.coverage_ratio)}</dd></div>
            <div><dt>Return correlation</dt><dd>{formatDecimal(result.statistics.return_correlation)}</dd></div>
            <div><dt>Median diff (points)</dt><dd>{formatDecimal(result.statistics.median_price_difference_points)}</dd></div>
            <div><dt>P99 diff (points)</dt><dd>{formatDecimal(result.statistics.p99_price_difference_points)}</dd></div>
            <div><dt>Target point</dt><dd>{formatDecimal(result.statistics.target_point)}</dd></div>
          </dl>
        </article>

        <article className="panel">
          <h6>Policy evaluation</h6>
          <dl className="compact-list">
            {Object.entries(result.policy_evaluation).map(([key, value]) => (
              <div key={key}>
                <dt>{POLICY_EVALUATION_LABELS[key] ?? humanizeToken(key)}</dt>
                <dd>{typeof value === 'boolean' ? (value ? 'Passed' : 'Failed') : formatDecimal(value)}</dd>
              </div>
            ))}
          </dl>
        </article>
      </div>

      <div className="summary-grid">
        <MarketDataCard marketData={result.first_market_data} worker={firstWorker} />
        <MarketDataCard marketData={result.second_market_data} worker={secondWorker} />
      </div>

      {'verification_status' in result && (
        <div className="summary-grid">
          <DifferenceCard differences={result.hard_block_differences} title="Hard-block differences" />
          <DifferenceCard differences={result.warning_differences} title="Warning differences" />
        </div>
      )}
    </article>
  )
}

function MarketDataCard({ marketData, worker }: { marketData: MarketDataSummary; worker: WorkerReference }) {
  return (
    <article className="panel">
      <h6>{worker.server} market data</h6>
      <dl className="compact-list">
        <div><dt>Symbol</dt><dd>{marketData.symbol}</dd></div>
        <div><dt>Bars</dt><dd>{marketData.bar_count}</dd></div>
        <div><dt>First UTC</dt><dd><time dateTime={marketData.first_utc}>{formatDateTime(marketData.first_utc)}</time></dd></div>
        <div><dt>Last UTC</dt><dd><time dateTime={marketData.last_utc}>{formatDateTime(marketData.last_utc)}</time></dd></div>
        <div><dt>Content hash</dt><dd className="mono">{marketData.content_hash}</dd></div>
      </dl>
    </article>
  )
}

function DifferenceCard({ differences, title }: { differences: AnalysisDifference[]; title: string }) {
  return (
    <article className="panel">
      <h6>{title}</h6>
      {differences.length === 0 ? (
        <p>None.</p>
      ) : (
        <ul className="difference-list">
          {differences.map((difference) => (
            <li key={difference.field}>
              <strong>{difference.field}</strong>
              <span>{formatUnknown(difference.first_value)} ↔ {formatUnknown(difference.second_value)}</span>
            </li>
          ))}
        </ul>
      )}
    </article>
  )
}

function findMatchingActiveProductPair(
  productPairs: ProductPair[],
  endpoints: Array<{ server: string; symbol: string }>,
) {
  const targetKey = endpointPairKey(endpoints)
  return productPairs.find((pair) => pair.status === 'active' && endpointPairKey(pair.endpoints) === targetKey) ?? null
}

function getApplicabilityCompatibilityResult(applicability: WorkerApplicability | null) {
  return applicability?.latest_compatibility_check ?? applicability?.compatibility_result ?? applicability?.compatibility ?? null
}

function deriveApplicabilityState(applicability: WorkerApplicability | null): ApplicabilityState {
  if (applicability?.exclusion?.excluded_at || applicability?.applicability_status === 'excluded') {
    return 'excluded'
  }
  if (
    getApplicabilityCompatibilityResult(applicability)
    || (applicability?.inspection_status !== undefined && applicability.inspection_status !== 'uninspected')
    || applicability?.applicability_status === 'checked'
  ) {
    return 'checked'
  }
  return 'applicable_uninspected'
}

function humanizeInspectionStatus(
  inspectionStatus: string | null | undefined,
  compatibility: WorkerCompatibilityResult | null = null,
) {
  const resolvedStatus = inspectionStatus
    ?? compatibility?.inspection_status
    ?? (
      compatibility
        ? compatibility.hard_block_differences.length > 0 || compatibility.warning_differences.length > 0
          ? 'differences_detected'
          : 'compatible'
        : 'uninspected'
    )
  switch (resolvedStatus) {
    case 'compatible':
      return 'Compatible'
    case 'differences_detected':
      return 'Differences detected'
    case 'uninspected':
      return 'Uninspected'
    default:
      return humanizeToken(resolvedStatus)
  }
}

function describeApplicabilityState(state: ApplicabilityState) {
  switch (state) {
    case 'checked':
      return 'Compatibility evidence exists. A mismatch remains evidence until you explicitly exclude the worker.'
    case 'excluded':
      return 'This worker is explicitly excluded from this one product pair.'
    default:
      return 'This worker is applicable by default and has not been inspected yet.'
  }
}

function findRetestAlert(alerts: WorkerAlert[], pair: ProductPair) {
  return alerts.find((alert) =>
    alert.alert_type.includes('retest')
    && alert.product_pair_id === pair.product_pair_id,
  ) ?? null
}

function validateRetestSelection(
  pair: ProductPair,
  firstWorkers: AccountWorker[],
  secondWorkers: AccountWorker[],
  firstWorkerId: string,
  secondWorkerId: string,
) {
  if (firstWorkers.length === 0) {
    return `No healthy connected workers are available on ${pair.source_workers.first_worker.server}.`
  }
  if (secondWorkers.length === 0) {
    return `No healthy connected workers are available on ${pair.source_workers.second_worker.server}.`
  }
  if (!firstWorkers.some((worker) => worker.worker_id === firstWorkerId)) {
    return `Choose a healthy connected worker on ${pair.source_workers.first_worker.server}.`
  }
  if (!secondWorkers.some((worker) => worker.worker_id === secondWorkerId)) {
    return `Choose a healthy connected worker on ${pair.source_workers.second_worker.server}.`
  }
  return null
}

function getRetestStatistics(retest: ProductPairRetest) {
  if (retest.statistics) {
    return retest.statistics
  }
  if (retest.verification_result) {
    return retest.verification_result.statistics
  }
  const nestedResult = retest.result
  if (nestedResult && typeof nestedResult === 'object' && 'statistics' in nestedResult) {
    return nestedResult.statistics as MarketStatistics
  }
  return null
}

function endpointPairKey(endpoints: Array<{ server: string; symbol: string }>) {
  return endpoints
    .map((endpoint) => `${endpoint.server}\u0000${endpoint.symbol}`)
    .sort()
    .join('\u0001')
}

function readAnalysisQuery() {
  const params = new URLSearchParams(window.location.search)
  return params.get('analysis') ?? ''
}

function writeAnalysisQuery(analysisId: string) {
  const url = new URL(window.location.href)
  if (analysisId) {
    url.searchParams.set('analysis', analysisId)
  } else {
    url.searchParams.delete('analysis')
  }
  window.history.replaceState({}, '', url)
}

function workerEligibilityReason(worker: AccountWorker) {
  if (worker.connectivity === 'revoked') {
    return 'Certificate revoked.'
  }
  if (worker.connectivity !== 'connected') {
    return 'Worker is not currently connected.'
  }
  if (worker.safety_state !== 'connected') {
    return `Safety state is ${humanizeToken(worker.safety_state)}.`
  }
  return null
}

function validateAnalysisPair(firstWorker: AccountWorker | null, secondWorker: AccountWorker | null) {
  if (!firstWorker || !secondWorker) {
    return 'Pick two eligible workers on different exact MT5 servers before launching.'
  }
  if (firstWorker.worker_id === secondWorker.worker_id) {
    return 'Pick two distinct workers before launching.'
  }
  if (firstWorker.server === secondWorker.server) {
    return 'Pick workers on different exact MT5 servers before launching.'
  }
  return null
}

async function readResponseDetail(response: Response, fallback: string) {
  try {
    const payload = await response.json() as { detail?: unknown }
    return typeof payload.detail === 'string' ? payload.detail : fallback
  } catch {
    return fallback
  }
}

function describeAnalysisEvent(event: AuditEvent) {
  const stage = typeof event.payload?.stage === 'string' ? humanizeStage(event.payload.stage) : null
  const reason = typeof event.payload?.reason === 'string' ? event.payload.reason : null
  if (stage && reason) {
    return `${stage}: ${reason}`
  }
  if (stage) {
    return stage
  }
  if (reason) {
    return reason
  }
  return 'Analysis event recorded.'
}

function humanizeLifecycleStatus(status: string) {
  switch (status) {
    case 'queued':
      return 'Queued'
    case 'running':
      return 'Running'
    case 'succeeded':
      return 'Succeeded'
    case 'failed':
      return 'Failed'
    default:
      return humanizeToken(status)
  }
}

function humanizePairStatus(status: string) {
  switch (status) {
    case 'active':
      return 'Active'
    case 'retired':
      return 'Retired'
    default:
      return humanizeToken(status)
  }
}

function humanizeRetestStatus(status: string) {
  switch (status) {
    case 'passed':
      return 'Passed'
    case 'failed':
      return 'Failed'
    case 'running':
      return 'Running'
    default:
      return humanizeToken(status)
  }
}

function humanizeApplicabilityStatus(status: ApplicabilityState) {
  switch (status) {
    case 'checked':
      return 'Checked'
    case 'excluded':
      return 'Excluded'
    default:
      return 'Applicable (uninspected)'
  }
}

function applicabilityStatusClassName(status: ApplicabilityState) {
  switch (status) {
    case 'checked':
      return 'status-queued'
    case 'excluded':
      return 'status-failed'
    default:
      return 'status-succeeded'
  }
}

function humanizeStage(stage: string) {
  return STAGE_LABELS[stage] ?? humanizeToken(stage)
}

function humanizeToken(value: string) {
  return value
    .replaceAll('_', ' ')
    .replace(/\b\w/g, (character) => character.toUpperCase())
}

function formatDateTime(value: string) {
  return value.replace('T', ' ')
}

function formatRatio(value: number | null) {
  if (value === null) {
    return '—'
  }
  return `${(value * 100).toFixed(2)}%`
}

function formatDecimal(value: number | null) {
  if (value === null) {
    return '—'
  }
  return Number.isInteger(value) ? String(value) : value.toFixed(6).replace(/0+$/, '').replace(/\.$/, '')
}

function formatUnknown(value: unknown) {
  if (typeof value === 'string') {
    return value
  }
  if (typeof value === 'number' || typeof value === 'boolean' || value === null) {
    return String(value)
  }
  return JSON.stringify(value)
}

function formatEndpointPair(endpoints: Array<{ server: string; symbol: string }>) {
  return endpoints.map((endpoint) => `${endpoint.server}:${endpoint.symbol}`).join(' ↔ ')
}

function formatWorkerReference(worker: WorkerReference) {
  return `${worker.login} on ${worker.server}`
}

function describeRetiredReason(reason: string | null) {
  if (reason === null) {
    return '—'
  }
  if (reason === 'manual_retirement') {
    return 'Manual retirement'
  }
  if (reason === 'replaced') {
    return 'Replaced'
  }
  return humanizeToken(reason)
}

export default App
