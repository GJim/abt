import { useEffect, useMemo, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { AppShell } from '@astryxdesign/core/AppShell'
import { Badge } from '@astryxdesign/core/Badge'
import { Collapsible } from '@astryxdesign/core/Collapsible'
import { StatusDot } from '@astryxdesign/core/StatusDot'
import { Tab, TabList } from '@astryxdesign/core/TabList'
import { TopNav } from '@astryxdesign/core/TopNav'
import { useToast } from '@astryxdesign/core/Toast'
import { Tooltip } from '@astryxdesign/core/Tooltip'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { AuditEventsPage } from './AuditEventsPage'
import { AnalysisHistoryPage } from './AnalysisHistoryPage'
import { ProductPairsListPage } from './ProductPairsListPage'
import { WorkerSnapshotsPage } from './WorkerSnapshotsPage'
import { WorkerSummaryTable } from './WorkerSummaryTable'
import './App.css'
import './Console.css'

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
  worker_id: string | null
  product_pair_id?: string | null
  enrollment_id?: string | null
  priority: string
  alert_type: string
  reason: string
  occurred_at: string
}

type InterventionItem =
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
  supported_filling_modes: string[]
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

type PairedTradeLifecycle = {
  availability: 'available' | 'unavailable'
  reason: string
}

type OperationsDashboard = {
  paired_trade_lifecycle: PairedTradeLifecycle
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

function shortPolicyFieldLabel(key: keyof ProductCatalogAnalysisPolicy) {
  const labels: Partial<Record<keyof ProductCatalogAnalysisPolicy, string>> = {
    minimum_m15_common_coverage: 'M15 coverage',
    minimum_m15_return_correlation: 'M15 correlation',
    minimum_m1_common_coverage: 'M1 coverage',
    minimum_m1_return_correlation: 'M1 correlation',
    maximum_m1_median_price_difference_points: 'Max M1 delta',
  }
  return labels[key] ?? String(key)
}

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
    key: 'minimum_m15_return_correlation',
    label: 'Minimum M15 return correlation',
    description: 'Minimum return correlation during M15 screening.',
    min: -1,
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

const LIVE_REFRESH_INTERVAL_MS = 30_000
const LIVE_REFRESH_TIMEOUT_MS = 10_000
type ConsolePage = 'main' | 'launch' | 'audit' | 'snapshots' | 'history' | 'active-pairs' | 'retired-pairs'

function App() {
  const location = useLocation()
  const navigate = useNavigate()
  const analysisRouteId = readAnalysisRouteId(location.pathname)
  const isAnalysisRoute = location.pathname === '/analysis' || analysisRouteId !== null
  const consolePage = isAnalysisRoute ? 'launch' : readConsolePage(location.pathname)
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [csrfToken, setCsrfToken] = useState<string | null>(null)
  const [events, setEvents] = useState<AuditEvent[]>([])
  const [enrollments, setEnrollments] = useState<Enrollment[]>([])
  const [notificationEnrollments, setNotificationEnrollments] = useState<Enrollment[]>([])
  const [isNotificationListOpen, setIsNotificationListOpen] = useState(false)
  const [selectedNotificationEnrollment, setSelectedNotificationEnrollment] = useState<Enrollment | null>(null)
  const [notificationConnection, setNotificationConnection] = useState<'connecting' | 'live' | 'reconnecting'>('connecting')
  const [workers, setWorkers] = useState<AccountWorker[]>([])
  const [alerts, setAlerts] = useState<WorkerAlert[]>([])
  const [productPairs, setProductPairs] = useState<ProductPair[]>([])
  const [refreshError, setRefreshError] = useState<string | null>(null)
  const [processingEnrollmentId, setProcessingEnrollmentId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [policy, setPolicy] = useState<ProductCatalogAnalysisPolicy>(DEFAULT_POLICY)
  const [firstWorkerId, setFirstWorkerId] = useState('')
  const [secondWorkerId, setSecondWorkerId] = useState('')
  const [analysis, setAnalysis] = useState<ProductCatalogAnalysis | null>(null)
  const [analysisError, setAnalysisError] = useState<string | null>(null)
  const [isLaunchingAnalysis, setIsLaunchingAnalysis] = useState(false)
  const [isLoadingAnalysis, setIsLoadingAnalysis] = useState(false)
  const refreshInFlight = useRef(false)
  const operatorActionInProgress = useRef(false)
  const notificationRetryTimer = useRef<number | null>(null)

  const eligibleWorkers = useMemo(
    () => workers.filter((worker) => workerEligibilityReason(worker) === null),
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
  const interventionQueue = useMemo<InterventionItem[]>(
    () => [
      ...alerts
        .filter((alert) => alert.priority === 'high' || alert.priority === 'critical')
        .map((alert) => {
          const enrollment = alert.enrollment_id
            ? enrollments.find((candidate) => candidate.enrollment_id === alert.enrollment_id)
            : undefined
          return enrollment
            ? {
              id: `alert-${alert.alert_id}`,
              kind: 'Pending approval',
              reason: 'Worker registration needs operator approval before it can receive a device certificate.',
              occurredAt: alert.occurred_at,
              priorityRank: alert.priority === 'critical' ? 3 : 2,
              enrollment,
            }
            : {
              id: `alert-${alert.alert_id}`,
              kind: `${alert.priority} priority alert`,
              reason: alert.reason,
              occurredAt: alert.occurred_at,
              priorityRank: alert.priority === 'critical' ? 3 : 2,
              alert,
            }
        }),
    ].sort((first, second) => second.priorityRank - first.priorityRank || second.occurredAt.localeCompare(first.occurredAt)),
    [alerts, enrollments],
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
        if (analysisRouteId) {
          await loadAnalysisById(analysisRouteId, { replaceUrl: false })
        }
      } catch {
        setError('Your session could not be restored. Please sign in again.')
      }
    }

    void resumeSession()
  }, [analysisRouteId])

  useEffect(() => {
    if (!csrfToken) {
      return
    }

    let socket: WebSocket | null = null
    let disposed = false
    let reconnectDelay = 1_000

    const mergeEnrollment = (incoming: Enrollment) => {
      setNotificationEnrollments((current) => [
        incoming,
        ...current.filter((enrollment) => enrollment.enrollment_id !== incoming.enrollment_id),
      ])
    }
    const connect = () => {
      if (disposed) return
      setNotificationConnection(socket ? 'reconnecting' : 'connecting')
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
      const nextSocket = new WebSocket(`${protocol}//${window.location.host}/api/admin/notifications`)
      socket = nextSocket
      nextSocket.onopen = () => {
        reconnectDelay = 1_000
        setNotificationConnection('live')
      }
      nextSocket.onmessage = (event) => {
        try {
          const message = JSON.parse(String(event.data)) as { type?: string; items?: Enrollment[]; item?: Enrollment }
          if (message.type === 'pending_enrollments' && Array.isArray(message.items)) {
            setNotificationEnrollments(message.items)
          } else if (message.type === 'pending_enrollment' && message.item) {
            mergeEnrollment(message.item)
          }
        } catch {
          // REST-seeded data remains available when a push message is malformed.
        }
      }
      nextSocket.onclose = () => {
        if (disposed || socket !== nextSocket) return
        setNotificationConnection('reconnecting')
        notificationRetryTimer.current = window.setTimeout(() => {
          reconnectDelay = Math.min(reconnectDelay * 2, 30_000)
          connect()
        }, reconnectDelay)
      }
      nextSocket.onerror = () => nextSocket.close()
    }
    connect()

    return () => {
      disposed = true
      if (notificationRetryTimer.current !== null) window.clearTimeout(notificationRetryTimer.current)
      socket?.close()
    }
  }, [csrfToken])

  useEffect(() => {
    document.title = isAnalysisRoute ? 'Analysis | ABT control plane' : 'ABT control plane'
  }, [isAnalysisRoute])

  useEffect(() => {
    if (analysis?.status !== 'running') {
      return
    }
    const timer = window.setTimeout(() => {
      void loadAnalysisById(analysis.analysis_id, { replaceUrl: false, silent: true })
    }, 3000)
    return () => window.clearTimeout(timer)
  }, [analysis?.analysis_id, analysis?.status])

  useEffect(() => {
    operatorActionInProgress.current = processingEnrollmentId !== null || isLaunchingAnalysis
  }, [isLaunchingAnalysis, processingEnrollmentId])

  useEffect(() => {
    if (!csrfToken) {
      return
    }

    const refreshWhenSafe = () => {
      if (document.visibilityState === 'hidden' || operatorActionInProgress.current) {
        return
      }
      void refreshManagementData().catch(() => undefined)
    }
    const timer = window.setInterval(refreshWhenSafe, LIVE_REFRESH_INTERVAL_MS)
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        refreshWhenSafe()
      }
    }

    document.addEventListener('visibilitychange', handleVisibilityChange)
    return () => {
      window.clearInterval(timer)
      document.removeEventListener('visibilitychange', handleVisibilityChange)
    }
  }, [csrfToken])

  async function refreshManagementData() {
    if (refreshInFlight.current) {
      return
    }
    refreshInFlight.current = true
    setRefreshError(null)
    const controller = new AbortController()
    const timeout = window.setTimeout(() => controller.abort(), LIVE_REFRESH_TIMEOUT_MS)
    try {
      const [eventsResponse, enrollmentsResponse, workersResponse, alertsResponse, productPairsResponse] = await Promise.all([
        fetch('/api/admin/events', { credentials: 'same-origin', signal: controller.signal }),
        fetch('/api/admin/enrollments', { credentials: 'same-origin', signal: controller.signal }),
        fetch('/api/admin/workers', { credentials: 'same-origin', signal: controller.signal }),
        fetch('/api/admin/alerts', { credentials: 'same-origin', signal: controller.signal }),
        fetch('/api/admin/product-pairs', { credentials: 'same-origin', signal: controller.signal }),
      ])
      if ([eventsResponse, enrollmentsResponse, workersResponse, alertsResponse, productPairsResponse].some((response) => response.status === 401)) {
        setCsrfToken(null)
        throw new Error('Your session has expired. Please sign in again.')
      }
      if (!eventsResponse.ok || !enrollmentsResponse.ok || !workersResponse.ok || !alertsResponse.ok || !productPairsResponse.ok) {
        throw new Error('Management data could not be loaded. Your previous data is still shown.')
      }

      const [eventPayload, enrollmentPayload, workerPayload, alertPayload, productPairPayload] = await Promise.all([
        eventsResponse.json() as Promise<AuditEvent[] | { items: AuditEvent[] }>,
        enrollmentsResponse.json() as Promise<Enrollment[]>,
        workersResponse.json() as Promise<AccountWorker[]>,
        alertsResponse.json() as Promise<WorkerAlert[]>,
        productPairsResponse.json() as Promise<ProductPair[] | { items: ProductPair[] }>,
      ])
      setEvents(Array.isArray(eventPayload) ? eventPayload : eventPayload.items)
      setEnrollments(enrollmentPayload)
      setNotificationEnrollments(enrollmentPayload)
      setWorkers(workerPayload)
      setAlerts(alertPayload)
      setProductPairs(Array.isArray(productPairPayload) ? productPairPayload : productPairPayload.items)
    } catch (refreshFailure) {
      setRefreshError(
        refreshFailure instanceof DOMException && refreshFailure.name === 'AbortError'
          ? 'Live refresh timed out. Your previous data is still shown.'
          : refreshFailure instanceof Error
            ? refreshFailure.message
            : 'Management data could not be refreshed.',
      )
      throw refreshFailure
    } finally {
      window.clearTimeout(timeout)
      refreshInFlight.current = false
    }
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
      if (options?.replaceUrl !== false) {
        navigate(`/analysis/${encodeURIComponent(payload.analysis_id)}`)
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
      if (analysisRouteId) {
        await loadAnalysisById(analysisRouteId, { replaceUrl: false })
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
    operatorActionInProgress.current = true
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
      setEnrollments((current) => current.filter((enrollment) => enrollment.enrollment_id !== enrollmentId))
      setNotificationEnrollments((current) => current.filter((enrollment) => enrollment.enrollment_id !== enrollmentId))
      setSelectedNotificationEnrollment((current) => current?.enrollment_id === enrollmentId ? null : current)
      await refreshManagementData()
    } catch (reviewError) {
      const detail = reviewError instanceof Error ? reviewError.message : 'Enrollment review failed.'
      setError(`Could not ${action} this worker registration: ${detail}`)
    } finally {
      operatorActionInProgress.current = false
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
    operatorActionInProgress.current = true
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
      navigate(`/analysis/${encodeURIComponent(payload.analysis_id)}`)
      await refreshManagementData()
    } catch (launchError) {
      setAnalysisError(launchError instanceof Error ? launchError.message : 'The analysis could not be launched.')
    } finally {
      operatorActionInProgress.current = false
      setIsLaunchingAnalysis(false)
    }
  }

  if (csrfToken) {
    return (
      <AppShell
        contentPadding={4}
        height="auto"
        topNav={
          <TopNav
            endContent={
              <div className="console-notifications">
                <button
                  aria-controls="pending-enrollment-notifications"
                  aria-expanded={isNotificationListOpen}
                  aria-label={`Pending enrollment notifications: ${notificationEnrollments.length}`}
                  className="console-notification-button"
                  onClick={() => setIsNotificationListOpen((isOpen) => !isOpen)}
                  type="button"
                >
                  <svg aria-hidden="true" fill="none" viewBox="0 0 24 24"><path d="M18 9a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.75" /></svg>
                  {notificationEnrollments.length > 0 && <span className="console-notification-badge">{notificationEnrollments.length}</span>}
                </button>
                <span className="console-notification-connection" role="status">
                  {notificationConnection === 'live' ? 'Updates live' : 'Updates reconnecting — showing latest loaded registrations.'}
                </span>
                {isNotificationListOpen && (
                  <section aria-label="Pending enrollment notifications" className="console-notification-popover" id="pending-enrollment-notifications">
                    <header><strong>Pending enrollments</strong><span>{notificationEnrollments.length}</span></header>
                    {notificationEnrollments.length === 0 ? <p>No pending registrations.</p> : (
                      <ul>
                        {notificationEnrollments.map((enrollment) => (
                          <li key={enrollment.enrollment_id}>
                            <button onClick={() => setSelectedNotificationEnrollment(enrollment)} type="button">
                              <strong>{enrollment.login} on {enrollment.server}</strong>
                              <span>Review registration</span>
                            </button>
                          </li>
                        ))}
                      </ul>
                    )}
                  </section>
                )}
              </div>
            }
            heading={<strong>ABT control plane</strong>}
            label="Control-plane navigation"
          />
        }
        variant="section"
      >
        <div className="console-app-frame">
          <aside className="console-sidebar">
            <strong>ABT console</strong>
            <nav aria-label="Console sections" className="console-nav">
              <Link aria-current={consolePage === 'main' ? 'page' : undefined} to="/">Main</Link>
              <Link aria-current={isAnalysisRoute ? 'page' : undefined} to="/analysis">Analysis</Link>
              <Link aria-current={consolePage === 'history' ? 'page' : undefined} to="/analysis/history">Analysis history</Link>
              <Link aria-current={consolePage === 'active-pairs' ? 'page' : undefined} to="/pairs/active">Current pairs</Link>
              <Link aria-current={consolePage === 'retired-pairs' ? 'page' : undefined} to="/pairs/retired">Retired pairs</Link>
              <Link aria-current={consolePage === 'snapshots' ? 'page' : undefined} to="/workers">Workers</Link>
              <Link aria-current={consolePage === 'audit' ? 'page' : undefined} to="/audit">Audit events</Link>
            </nav>
          </aside>
          <main className="console-main management-content">
          <header className={isAnalysisRoute ? 'analysis-page-header' : undefined}>
            {consolePage === 'main' && <h1>Management console</h1>}
            {consolePage === 'main' && <p>Launch and inspect cross-server product-pair analyses alongside worker health and audit events.</p>}
            {isAnalysisRoute && analysisRouteId && <Link className="new-analysis-button" to="/analysis">+ New</Link>}
          </header>
          {error && <p className="error" role="alert">{error}</p>}
          {refreshError && <p className="error" role="alert">{refreshError}</p>}
          {selectedNotificationEnrollment && (
            <div className="snapshot-json-dialog-backdrop" onMouseDown={() => setSelectedNotificationEnrollment(null)}>
              <section
                aria-labelledby="enrollment-notification-dialog-title"
                aria-modal="true"
                className="snapshot-json-dialog enrollment-notification-dialog"
                onMouseDown={(event) => event.stopPropagation()}
                role="dialog"
              >
                <header className="snapshot-json-dialog-header">
                  <h2 id="enrollment-notification-dialog-title">
                    Registration for {selectedNotificationEnrollment.login} on {selectedNotificationEnrollment.server}
                  </h2>
                  <button aria-label="Close enrollment notification" className="console-icon-button" onClick={() => setSelectedNotificationEnrollment(null)} type="button">×</button>
                </header>
                <section aria-label="Account parameters">
                  <h3>Account parameters</h3>
                  <pre className="console-raw-detail">{JSON.stringify(selectedNotificationEnrollment.account_info, null, 2)}</pre>
                </section>
                <section aria-label="Terminal parameters">
                  <h3>Terminal parameters</h3>
                  <pre className="console-raw-detail">{JSON.stringify(selectedNotificationEnrollment.terminal_info, null, 2)}</pre>
                </section>
                <div className="enrollment-actions">
                  <button disabled={processingEnrollmentId === selectedNotificationEnrollment.enrollment_id} onClick={() => void reviewEnrollment(selectedNotificationEnrollment.enrollment_id, 'approve')} type="button">Approve</button>
                  <button className="reject-button" disabled={processingEnrollmentId === selectedNotificationEnrollment.enrollment_id} onClick={() => void reviewEnrollment(selectedNotificationEnrollment.enrollment_id, 'reject')} type="button">Reject</button>
                </div>
              </section>
            </div>
          )}
          {consolePage === 'audit' ? <AuditEventsPage /> : consolePage === 'snapshots' ? <WorkerSnapshotsPage /> : consolePage === 'history' ? (
            <AnalysisHistoryPage onOpenAnalysis={(analysisId) => {
              navigate(`/analysis/${encodeURIComponent(analysisId)}`)
            }} />
          ) : consolePage === 'active-pairs' || consolePage === 'retired-pairs' ? (
            <ProductPairsListPage
              status={consolePage === 'active-pairs' ? 'active' : 'retired'}
              onOpenPair={() => {
                navigate('/pairs/active')
              }}
            />
          ) : <>

          {consolePage !== 'launch' && <>
          <section aria-labelledby="intervention-queue-heading" className="intervention-queue">
            <div className="section-header">
              <div>
                <h2 id="intervention-queue-heading">Needs attention</h2>
                <p>Only control-plane conditions that need an operator action appear here.</p>
              </div>
              <span className="status-badge status-failed">{interventionQueue.length} open</span>
            </div>
            {interventionQueue.length === 0 ? (
              <p>No operator action is needed.</p>
            ) : (
              <ul aria-label="Intervention queue">
                {interventionQueue.map((item) => (
                  <li key={item.id} className="intervention-item">
                    <div>
                      <strong>{item.kind}</strong>
                      <p>{item.reason}</p>
                      <time dateTime={item.occurredAt}>{formatDateTime(item.occurredAt)}</time>
                    </div>
                    {'enrollment' in item ? (
                      <div className="enrollment-actions">
                        <button
                          aria-label={`Approve registration for ${item.enrollment.login} on ${item.enrollment.server}`}
                          disabled={processingEnrollmentId === item.enrollment.enrollment_id}
                          onClick={() => void reviewEnrollment(item.enrollment!.enrollment_id, 'approve')}
                          type="button"
                        >
                          Approve
                        </button>
                        <button
                          aria-label={`Reject registration for ${item.enrollment.login} on ${item.enrollment.server}`}
                          className="reject-button"
                          disabled={processingEnrollmentId === item.enrollment.enrollment_id}
                          onClick={() => void reviewEnrollment(item.enrollment!.enrollment_id, 'reject')}
                          type="button"
                        >
                          Reject
                        </button>
                      </div>
                    ) : (
                      <a href="#account-workers-heading">Review worker</a>
                    )}
                    {'enrollment' in item && <a href={`#enrollment-${item.enrollment.enrollment_id}`}>Review evidence</a>}
                  </li>
                ))}
              </ul>
            )}
          </section>
          </>}

          {isAnalysisRoute && <section aria-label={analysisRouteId ? 'Analysis result' : 'Launch'} className="analysis-section">
            {!analysisRouteId && <div className="section-header">
              <div>
                <h2 id="analysis-heading">Launch</h2>
              </div>
              {(isLaunchingAnalysis || isLoadingAnalysis) && (
                <span className="status-badge status-queued" role="status">
                  {isLaunchingAnalysis ? 'Queued request' : 'Loading analysis'}
                </span>
              )}
            </div>}
            {analysisError && <p className="error" role="alert">{analysisError}</p>}
            {!analysisRouteId && <div className="analysis-layout">
              <article className="panel launch-panel">
                <h3>Launch</h3>
                <form className="analysis-form launch-form" onSubmit={submitAnalysis}>
                  <label>
                    <Tooltip content="The first healthy worker whose symbol catalog and market data will be compared.">
                      <span>Source A</span>
                    </Tooltip>
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
                    <Tooltip content="A healthy worker on a different MT5 server. It is compared against Source A.">
                      <span>Source B</span>
                    </Tooltip>
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
                  ) : null}

                  <section className="policy-summary" aria-label="Analysis policy">
                    <label>
                      <Tooltip content="The name stored with this analysis and its immutable policy snapshot.">
                        <span>Policy</span>
                      </Tooltip>
                      <input
                        aria-label="Policy label"
                        maxLength={128}
                        onChange={(event) => setPolicy((current) => ({ ...current, label: event.target.value }))}
                        required
                        value={policy.label}
                      />
                    </label>
                    <Collapsible defaultIsOpen={false} trigger="Advanced policy">
                      <fieldset className="policy-fieldset">
                        <legend>Advanced policy</legend>
                        <div className="policy-grid">
                          {NUMBER_POLICY_FIELDS.map((field) => (
                            <label key={field.key}>
                              <Tooltip content={field.description}><span>{shortPolicyFieldLabel(field.key)}</span></Tooltip>
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
                            </label>
                          ))}
                        </div>
                      </fieldset>
                    </Collapsible>
                  </section>

                  <button
                    aria-describedby="analysis-pair-hint"
                    disabled={isLaunchingAnalysis || pairValidationError !== null}
                    type="submit"
                  >
                    {isLaunchingAnalysis && <span aria-hidden="true" className="button-spinner" />}
                    {isLaunchingAnalysis ? 'Launching…' : 'Launch'}
                  </button>
                </form>
              </article>
            </div>
            }

            {analysisRouteId && analysis && (
              <AnalysisDetails
                analysis={analysis}
                csrfToken={csrfToken}
                onProductPairsChanged={refreshManagementData}
                productPairs={productPairs}
              />
            )}
          </section>
          }

          {consolePage !== 'launch' && <>
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
                    <time dateTime={alert.occurred_at}>{formatDateTime(alert.occurred_at)}</time>
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
          <section aria-labelledby="account-workers-heading" className="fleet-health">
            <div className="section-header">
              <div>
                <h2 id="account-workers-heading">Fleet health</h2>
                <p>Scan worker state and snapshot freshness before opening reconciliation evidence.</p>
              </div>
              <span className="fleet-count">{workers.length} {workers.length === 1 ? 'worker' : 'workers'}</span>
            </div>
            <WorkerSummaryTable workers={workers} onOpenSnapshots={() => navigate('/workers')} />
            <Collapsible defaultIsOpen={false} trigger="Show worker actions and reconciliation">
            {workers.length === 0 ? (
              <p className="empty-state">No approved account workers have reported yet. Approved workers will appear here after their first connection.</p>
            ) : (
              <ul className="worker-list" aria-label="Fleet health by account">
                {[...workers].sort((first, second) => {
                  const priority = workerHealthPriority(first) - workerHealthPriority(second)
                  return priority || `${first.server}:${first.login}`.localeCompare(`${second.server}:${second.login}`)
                }).map((worker) => {
                  const health = describeWorkerHealth(worker)
                  const accountName = `${worker.login} on ${worker.server}`
                  const snapshot = worker.latest_snapshot
                  return (
                    <li key={worker.worker_id} className="worker">
                      <div className="worker-summary">
                        <div>
                          <h3>{accountName}</h3>
                          <p>{health.description}</p>
                        </div>
                        <span className={`status-badge worker-health-${health.tone}`}>{health.label}</span>
                      </div>
                      <dl className="worker-facts">
                        <div><dt>Freshness</dt><dd>{snapshot ? <time dateTime={snapshot.observed_at}>{formatSnapshotFreshness(snapshot.observed_at)}</time> : 'No snapshot received'}</dd></div>
                        <div><dt>Account</dt><dd>{snapshot ? formatAccountSummary(snapshot.account) : 'Awaiting snapshot'}</dd></div>
                        <div><dt>Reconciliation</dt><dd>{formatReconciliationSummary(worker)}</dd></div>
                      </dl>
                      <div className="worker-actions">
                        <Collapsible defaultIsOpen={false} trigger={`View snapshot and reconciliation evidence for ${accountName}`}>
                          <div className="worker-evidence">
                            {snapshot ? (
                              <section aria-label={`Latest snapshot for ${accountName}`}>
                                <h4>Latest snapshot</h4>
                                <time dateTime={snapshot.observed_at}>{formatDateTime(snapshot.observed_at)}</time>
                                <pre>{JSON.stringify({
                                  account: snapshot.account,
                                  terminal: snapshot.terminal,
                                  orders: snapshot.orders,
                                  positions: snapshot.positions,
                                }, null, 2)}</pre>
                              </section>
                            ) : <p>No raw snapshot has been received.</p>}
                            <section aria-label={`Lifecycle deltas for ${accountName}`}>
                              <h4>Lifecycle deltas</h4>
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
                          </div>
                        </Collapsible>
                        {worker.connectivity !== 'revoked' && (
                          <button
                            aria-label={`Revoke certificate for ${accountName}`}
                            className="reject-button"
                            onClick={() => void revokeWorker(worker.worker_id)}
                            type="button"
                          >
                            Revoke certificate
                          </button>
                        )}
                      </div>
                    </li>
                  )
                })}
              </ul>
            )}
            </Collapsible>
          </section>
          <section aria-labelledby="audit-events-heading">
            <h2 id="audit-events-heading">Audit events</h2>
            <ul className="audit-events" aria-label="Audit events">
              {events.map((event) => (
                <li key={event.event_id}>
                  <strong>{event.event_type}</strong>
                  <time dateTime={event.occurred_at}>{formatDateTime(event.occurred_at)}</time>
                </li>
              ))}
            </ul>
          </section>
          </>}
          </>}
          </main>
        </div>
      </AppShell>
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
  onProductPairsChanged,
  productPairs,
}: {
  analysis: ProductCatalogAnalysis
  csrfToken: string
  onProductPairsChanged: () => Promise<void>
  productPairs: ProductPair[]
}) {
  const passingCandidates = analysis.m1_verification_results.filter((result) => result.verification_status === 'passed')
  const failingCandidates = analysis.m1_verification_results.filter((result) => result.verification_status !== 'passed')
  const failedScreening = analysis.m15_screening_results.filter((result) => result.screening_status !== 'passed')
  const passedScreening = analysis.m15_screening_results.filter((result) => result.screening_status === 'passed')
  const runtimeStatus = analysis.status === 'running' ? 'running' : analysis.status
  const [activeView, setActiveView] = useState('overview')
  return (
    <section aria-labelledby="analysis-results-heading" className="analysis-results">
      <header className="analysis-workspace-header">
        <div>
          <h3 id="analysis-results-heading">Analysis {analysis.analysis_id}</h3>
          <p>
            {analysis.first_worker.login} on {analysis.first_worker.server} ↔ {analysis.second_worker.login} on {analysis.second_worker.server}
          </p>
        </div>
        <div className="analysis-status">
          <StatusDot
            isPulsing={runtimeStatus === 'running'}
            label={humanizeLifecycleStatus(runtimeStatus)}
            variant={analysisStatusDotVariant(runtimeStatus)}
          />
          <Badge label={humanizeLifecycleStatus(runtimeStatus)} variant={analysisStatusBadgeVariant(runtimeStatus)} />
        </div>
      </header>

      <div className="analysis-stage-strip" aria-label="Analysis decision summary">
        <div><span>Catalog</span><strong>{analysis.eligible_candidates.length} matched</strong></div>
        <div><span>M15 screen</span><strong>{passedScreening.length} passed</strong></div>
        <div><span>Final verification</span><strong>{passingCandidates.length} passed</strong></div>
      </div>
      <div className="analysis-tabs">
        <TabList aria-label="Analysis workspace" hasDivider value={activeView} onChange={setActiveView}>
          <Tab label="Overview" value="overview" />
          <Tab endContent={<Badge label={passingCandidates.length + failingCandidates.length} variant="neutral" />} label="Candidates" value="candidates" />
          <Tab endContent={<Badge label={analysis.m15_screening_results.length} variant="neutral" />} label="M15 screen" value="screening" />
        </TabList>
      </div>

      {activeView === 'overview' && (
        <section className="analysis-overview" aria-label="Analysis overview">
          <dl className="analysis-facts">
            <div><dt>Stage</dt><dd>{humanizeStage(analysis.current_stage)}</dd></div>
            <div><dt>Requested</dt><dd><time dateTime={analysis.requested_at}>{formatDateTime(analysis.requested_at)}</time></dd></div>
            <div><dt>Policy</dt><dd>{analysis.policy.label}</dd></div>
            {NUMBER_POLICY_FIELDS.map((field) => (
              <div key={field.key}><dt>{shortPolicyFieldLabel(field.key)}</dt><dd>{String(analysis.policy[field.key])}</dd></div>
            ))}
          </dl>
          {analysis.failure_reason && <p className="error" role="alert">Analysis failed: {analysis.failure_reason}</p>}
        </section>
      )}

      {activeView === 'screening' && <section aria-labelledby="m15-results-heading">
        <h4 id="m15-results-heading">M15 screening results</h4>
        {analysis.m15_screening_results.length === 0 ? (
          <p>No M15 screening results were recorded.</p>
        ) : (
          <>
            <div className="analysis-candidate-list">
              {passedScreening.map((result) => (
                <CandidateDisclosure key={`${result.first_symbol}:${result.second_symbol}:m15`} result={result}>
                  <AnalysisResultCard result={result} />
                </CandidateDisclosure>
              ))}
            </div>
            {failedScreening.length > 0 && (
              <details>
              <summary>Show {failedScreening.length} failed M15 candidate(s)</summary>
              <div className="analysis-candidate-list">
                {failedScreening.map((result) => (
                  <CandidateDisclosure key={`${result.first_symbol}:${result.second_symbol}:m15`} result={result}>
                    <AnalysisResultCard result={result} />
                  </CandidateDisclosure>
                ))}
              </div>
              </details>
            )}
          </>
        )}
        {failedScreening.length > 0 && (
          <p className="hint">{failedScreening.length} candidate(s) stopped at M15 and never reached final verification.</p>
        )}
      </section>}

      {activeView === 'candidates' && <>
      <section aria-labelledby="final-passing-heading">
        <h4 id="final-passing-heading">Final passing candidates</h4>
        {passingCandidates.length === 0 ? (
          <p>No final passing candidates are available to build.</p>
        ) : (
          <div className="analysis-candidate-list">
            {passingCandidates.map((result) => (
              <BuildableVerificationCard
                analysis={analysis}
                csrfToken={csrfToken}
                key={`${result.first_symbol}:${result.second_symbol}:passed`}
                onProductPairsChanged={onProductPairsChanged}
                productPairs={productPairs}
                result={result}
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
          <details>
            <summary>Show {failingCandidates.length} failed final candidate(s)</summary>
            <div className="analysis-candidate-list">
              {failingCandidates.map((result) => (
                <CandidateDisclosure key={`${result.first_symbol}:${result.second_symbol}:failed`} result={result}>
                  <AnalysisResultCard result={result} />
                </CandidateDisclosure>
              ))}
            </div>
          </details>
        )}
      </section>
      </>}

    </section>
  )
}

function CandidateDisclosure({
  children,
  result,
}: {
  children: React.ReactNode
  result: ScreeningResult | VerificationResult
}) {
  const status = 'verification_status' in result ? result.verification_status : result.screening_status
  return (
    <Collapsible
      defaultIsOpen={false}
      trigger={
        <span className="analysis-candidate-summary">
          <strong>{result.first_symbol} ↔ {result.second_symbol}</strong>
          <span>{formatRatio(result.statistics.coverage_ratio)} coverage</span>
          <span>{formatDecimal(result.statistics.return_correlation)} correlation</span>
          <Badge label={humanizeLifecycleStatus(status)} variant={analysisStatusBadgeVariant(status)} />
        </span>
      }
    >
      {children}
    </Collapsible>
  )
}

function BuildableVerificationCard({
  analysis,
  csrfToken,
  onProductPairsChanged,
  productPairs,
  result,
}: {
  analysis: ProductCatalogAnalysis
  csrfToken: string
  onProductPairsChanged: () => Promise<void>
  productPairs: ProductPair[]
  result: VerificationResult
}) {
  const [pendingReplacement, setPendingReplacement] = useState<{
    confirmation: ProductPairBuildConfirmation
    pair: ProductPair
  } | null>(null)
  const [isPreparing, setIsPreparing] = useState(false)
  const [isApplying, setIsApplying] = useState(false)
  const toast = useToast()
  const candidateId = `${result.first_symbol}:${result.second_symbol}`

  async function createProductPair() {
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
      const conflictPair = findMatchingActiveProductPair(productPairs, payload.endpoints)
      if (conflictPair) {
        setPendingReplacement({ confirmation: payload, pair: conflictPair })
        return
      }
      await applyBuildAction(payload, 'build')
    } catch (buildError) {
      toast({
        body: buildError instanceof Error ? buildError.message : 'The Build confirmation could not be prepared.',
        type: 'error',
        uniqueID: `product-pair-${candidateId}`,
      })
    } finally {
      setIsPreparing(false)
    }
  }

  async function applyBuildAction(
    confirmation: ProductPairBuildConfirmation,
    mode: 'build' | 'replace',
    targetPair: ProductPair | null = null,
  ) {
    if (mode === 'replace' && targetPair === null) {
      toast({
        body: 'The active product pair to replace is no longer available. Refresh and try again.',
        type: 'error',
        uniqueID: `product-pair-${candidateId}`,
      })
      return
    }
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
      toast({
        body: mode === 'build'
          ? `Created active product pair ${payload.product_pair_id}.`
          : `Replaced the active pair with ${payload.product_pair_id}.`,
        uniqueID: `product-pair-${candidateId}`,
      })
      setPendingReplacement(null)
    } catch (buildError) {
      toast({
        body: buildError instanceof Error
          ? buildError.message
          : mode === 'build'
            ? 'The product pair could not be built.'
            : 'The active product pair could not be replaced.',
        type: 'error',
        uniqueID: `product-pair-${candidateId}`,
      })
    } finally {
      setIsApplying(false)
    }
  }

  return (
    <div className="buildable-candidate">
      <CandidateDisclosure result={result}>
        <div className="buildable-result-card">
          <AnalysisResultCard result={result} />
        </div>
      </CandidateDisclosure>
      <button
        aria-label={`Create product pair for ${result.first_symbol} and ${result.second_symbol}`}
        className="candidate-create-action"
        disabled={isPreparing || isApplying}
        onClick={() => void createProductPair()}
        type="button"
      >
        {isPreparing || isApplying ? 'Creating…' : 'Create'}
      </button>
      {pendingReplacement && (
        <div className="confirmation-dialog-backdrop" role="presentation">
          <section
            aria-describedby={`replace-pair-description-${candidateId}`}
            aria-labelledby={`replace-pair-title-${candidateId}`}
            aria-modal="true"
            className="confirmation-dialog"
            role="dialog"
          >
            <h6 id={`replace-pair-title-${candidateId}`}>Replace active product pair?</h6>
            <p id={`replace-pair-description-${candidateId}`}>
              This retires {pendingReplacement.pair.product_pair_id} and creates a new pair for {formatEndpointPair(pendingReplacement.confirmation.endpoints)}.
            </p>
            <div className="action-row">
              <button disabled={isApplying} onClick={() => setPendingReplacement(null)} type="button">Cancel</button>
              <button
                className="secondary-button"
                disabled={isApplying}
                onClick={() => void applyBuildAction(pendingReplacement.confirmation, 'replace', pendingReplacement.pair)}
                type="button"
              >
                {isApplying ? 'Replacing…' : 'Retire and create pair'}
              </button>
            </div>
          </section>
        </div>
      )}
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
  const [dashboard, setDashboard] = useState<OperationsDashboard | null>(null)
  const [dashboardError, setDashboardError] = useState<string | null>(null)
  const activePairs = useMemo(
    () => productPairs.filter((pair) => pair.status === 'active'),
    [productPairs],
  )
  const retiredPairs = useMemo(
    () => productPairs.filter((pair) => pair.status !== 'active'),
    [productPairs],
  )

  useEffect(() => {
    let cancelled = false

    async function loadDashboard() {
      setDashboardError(null)
      try {
        const response = await fetch('/api/admin/operations-dashboard', { credentials: 'same-origin' })
        if (!response.ok) {
          throw new Error(await readResponseDetail(response, 'The operational overview could not be loaded.'))
        }
        const payload = (await response.json()) as OperationsDashboard
        if (!cancelled) {
          setDashboard(payload)
        }
      } catch (loadError) {
        if (!cancelled) {
          setDashboard(null)
          setDashboardError(loadError instanceof Error ? loadError.message : 'The operational overview could not be loaded.')
        }
      }
    }

    void loadDashboard()
    return () => {
      cancelled = true
    }
  }, [productPairs])

  return (
    <section aria-labelledby="product-pairs-heading" className="analysis-section">
      <div className="section-header">
        <div>
          <h2 id="product-pairs-heading">Hedge and product-pair overview</h2>
          <p>Configuration lifecycle is visible here; product-pair records are not paired-trade activity.</p>
        </div>
        <span className="status-badge status-queued">{activePairs.length} active / {retiredPairs.length} retired</span>
      </div>

      <div className="hedge-overview" aria-label="Hedge lifecycle availability">
        <article className="panel">
          <h3>Current product-pair state</h3>
          <p className="state-summary">
            {activePairs.length === 0
              ? 'No active product-pair records are currently available.'
              : `${activePairs.length} active product-pair ${activePairs.length === 1 ? 'record is' : 'records are'} currently available.`}
          </p>
          <p className="hint">Active means the control-plane mapping is current. It does not report an order, position, or trade.</p>
        </article>
        <article className={`panel lifecycle-availability ${dashboardError ? 'lifecycle-availability-error' : ''}`}>
          <h3>Paired-trade lifecycle records</h3>
          {dashboardError ? (
            <>
              <p className="state-summary">Availability could not be confirmed.</p>
              <p className="hint" role="status">{dashboardError}</p>
            </>
          ) : dashboard === null ? (
            <p className="state-summary" role="status">Checking record availability…</p>
          ) : (
            <>
              <p className="state-summary">
                {dashboard.paired_trade_lifecycle.availability === 'unavailable'
                  ? 'Unavailable in the control-plane ledger.'
                  : 'Available in the control-plane ledger.'}
              </p>
              <p className="hint">{dashboard.paired_trade_lifecycle.reason}</p>
            </>
          )}
          <p className="hint">This console cannot infer paired-trade activity from product-pair state.</p>
        </article>
      </div>

      <div className="section-header product-pair-management-heading">
        <div>
          <h3>Active product pairs</h3>
          <p>Inspect lifecycle details, re-test evidence, and retirement history without any broker-write path.</p>
        </div>
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
              {NUMBER_POLICY_FIELDS.map((field) => (
                <div key={field.key}>
                  <dt>{shortPolicyFieldLabel(field.key)}</dt>
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

function AnalysisResultCard({
  result,
}: {
  result: ScreeningResult | VerificationResult
}) {
  const verificationStatus = 'verification_status' in result ? result.verification_status : null
  const badgeStatus = verificationStatus ?? result.screening_status

  return (
    <article className="result-card">
      <div className="section-header">
        <div>
          <div className="candidate-title">
            <h5>{result.first_symbol} ↔ {result.second_symbol}</h5>
            {'supported_filling_modes' in result && result.supported_filling_modes.map((mode) => (
              <Badge key={mode} label={mode} variant="neutral" />
            ))}
          </div>
          <p>{result.currency_base}/{result.currency_profit} · calibration {result.first_point} / {result.second_point}</p>
        </div>
        <span className={`status-badge status-${badgeStatus === 'passed' ? 'succeeded' : 'failed'}`}>
          {verificationStatus ? `Final ${humanizeToken(verificationStatus)}` : `M15 ${humanizeToken(result.screening_status)}`}
        </span>
      </div>

      <div className="candidate-summary-evidence">
        <section className="candidate-statistics" aria-label="Candidate statistics">
          <h6>Statistics</h6>
          <dl className="compact-list">
            <div><dt>Bars</dt><dd>{result.statistics.first_bar_count} / {result.statistics.second_bar_count}</dd></div>
            <div><dt>Aligned</dt><dd>{result.statistics.aligned_bar_count}</dd></div>
            <div><dt>Coverage</dt><dd>{formatRatio(result.statistics.coverage_ratio)}</dd></div>
            <div><dt>Correlation</dt><dd>{formatDecimal(result.statistics.return_correlation)}</dd></div>
            <div><dt>Median Δ</dt><dd>{formatDecimal(result.statistics.median_price_difference_points)}</dd></div>
            <div><dt>P99 Δ</dt><dd>{formatDecimal(result.statistics.p99_price_difference_points)}</dd></div>
          </dl>
        </section>
        {'verification_status' in result && (
          <DifferenceCard differences={result.warning_differences} title="Warning differences" />
        )}
      </div>
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

function readAnalysisRouteId(pathname: string) {
  if (pathname === '/analysis' || pathname === '/analysis/history') {
    return null
  }
  const match = /^\/analysis\/([^/]+)\/?$/.exec(pathname)
  return match ? decodeURIComponent(match[1]) : null
}

function readConsolePage(pathname: string): ConsolePage {
  switch (pathname) {
    case '/audit':
      return 'audit'
    case '/workers':
      return 'snapshots'
    case '/analysis/history':
      return 'history'
    case '/pairs/active':
      return 'active-pairs'
    case '/pairs/retired':
      return 'retired-pairs'
    default:
      return 'main'
  }
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

function analysisStatusBadgeVariant(status: string): 'success' | 'warning' | 'error' | 'info' {
  if (status === 'succeeded') {
    return 'success'
  }
  if (status === 'failed') {
    return 'error'
  }
  if (status === 'running') {
    return 'info'
  }
  return 'warning'
}

function analysisStatusDotVariant(status: string): 'success' | 'warning' | 'error' | 'accent' {
  if (status === 'succeeded') {
    return 'success'
  }
  if (status === 'failed') {
    return 'error'
  }
  if (status === 'running') {
    return 'accent'
  }
  return 'warning'
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
  const timestamp = Date.parse(value)
  if (!Number.isFinite(timestamp)) {
    return value
  }
  return new Date(timestamp).toISOString().slice(0, 19).replace('T', ' ')
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

const SNAPSHOT_STALE_AFTER_MS = 15 * 60 * 1000

function describeWorkerHealth(worker: AccountWorker) {
  if (worker.connectivity === 'revoked') {
    return {
      label: 'Revoked — human action',
      tone: 'human-action',
      description: 'Its certificate is revoked. Re-enrol the worker before it can report again.',
    }
  }
  if (worker.safety_state !== 'connected' || worker.connectivity === 'disconnected') {
    return {
      label: 'Human action needed',
      tone: 'human-action',
      description: `Connectivity is ${humanizeToken(worker.connectivity)} and safety state is ${humanizeToken(worker.safety_state)}.`,
    }
  }
  if (worker.connectivity === 'stale' || isSnapshotStale(worker.latest_snapshot?.observed_at)) {
    return {
      label: 'Stale data',
      tone: 'stale',
      description: 'The latest snapshot is too old to use for live operational decisions.',
    }
  }
  if (worker.latest_snapshot === null) {
    return {
      label: 'Idle — no snapshot',
      tone: 'idle',
      description: 'Connected, but waiting for its first account snapshot.',
    }
  }
  return {
    label: 'Healthy',
    tone: 'healthy',
    description: 'Connected with a current account snapshot.',
  }
}

function workerHealthPriority(worker: AccountWorker) {
  const tone = describeWorkerHealth(worker).tone
  return { 'human-action': 0, stale: 1, idle: 2, healthy: 3 }[tone] ?? 3
}

function isSnapshotStale(observedAt: string | undefined) {
  if (!observedAt) {
    return false
  }
  const timestamp = Date.parse(observedAt)
  return !Number.isFinite(timestamp) || Date.now() - timestamp > SNAPSHOT_STALE_AFTER_MS
}

function formatSnapshotFreshness(observedAt: string) {
  const timestamp = Date.parse(observedAt)
  if (!Number.isFinite(timestamp)) {
    return 'Snapshot time unavailable'
  }
  const ageInMinutes = Math.max(0, Math.floor((Date.now() - timestamp) / 60_000))
  const age = ageInMinutes === 0 ? 'just now' : `${ageInMinutes} ${ageInMinutes === 1 ? 'minute' : 'minutes'} ago`
  return `${isSnapshotStale(observedAt) ? 'Stale' : 'Current'}: ${age} (${formatDateTime(observedAt)})`
}

function formatAccountSummary(account: EnrollmentEvidence) {
  const balance = account.balance
  const currency = account.currency
  if (typeof balance === 'number') {
    return `Balance ${balance.toLocaleString()}${typeof currency === 'string' ? ` ${currency}` : ''}`
  }
  return 'Account snapshot received'
}

function formatReconciliationSummary(worker: AccountWorker) {
  const snapshot = worker.latest_snapshot
  if (snapshot === null) {
    return 'Awaiting reconciliation'
  }
  const counts = `${snapshot.positions.length} positions, ${snapshot.orders.length} orders`
  if (worker.deltas.length === 0) {
    return `${counts}; no changes`
  }
  const latestDelta = worker.deltas.reduce((latest, delta) => (
    delta.observed_at > latest.observed_at ? delta : latest
  ))
  return `${counts}; ${worker.deltas.length} change${worker.deltas.length === 1 ? '' : 's'}, latest ${humanizeToken(latestDelta.change)}`
}

export default App
