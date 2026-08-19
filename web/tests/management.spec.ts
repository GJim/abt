import { expect, test } from '@playwright/test'

test('administrator can reach the management login', async ({ page }) => {
  await page.goto('http://127.0.0.1:4173/')

  await expect(page.getByRole('heading', { name: 'Management access' })).toBeVisible()
  await expect(page.getByLabel('Administrator account')).toBeVisible()
  await expect(page.getByLabel('Password')).toBeVisible()
})

test('administrator resumes a cookie-backed session after refresh', async ({ page }) => {
  await mockSessionResume(page)

  await page.goto('http://127.0.0.1:4173/')

  await expect(page.getByRole('heading', { name: 'Cross-server product-pair analyses' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Audit events' })).toBeVisible()
})

test('administrator can sign in and view audit events', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    events: [
      { event_id: 1, event_type: 'admin_login_succeeded', payload: {}, occurred_at: '2026-08-15T00:00:00Z' },
    ],
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByRole('heading', { name: 'Audit events' })).toBeVisible()
  await expect(page.getByText('admin_login_succeeded')).toBeVisible()
})

test('administrator can review and approve a pending worker registration', async ({ page }) => {
  let enrollmentRequests = 0
  let alertRequests = 0

  await page.setViewportSize({ width: 900, height: 800 })
  await mockLogin(page)
  await page.route('**/api/admin/events', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify([
        { event_id: 1, event_type: 'worker_enrollment_approved', payload: {}, occurred_at: '2026-08-15T00:00:00Z' },
      ]),
    })
  })
  await page.route('**/api/admin/enrollments/enrollment-1/approve', async (route) => {
    expect(route.request().method()).toBe('POST')
    expect(route.request().headers()['x-csrf-token']).toBe('csrf-token')
    await route.fulfill({ status: 200 })
  })
  await page.route('**/api/admin/enrollments', async (route) => {
    enrollmentRequests += 1
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(enrollmentRequests === 1
        ? [buildEnrollment()]
        : []),
    })
  })
  await page.route('**/api/admin/workers', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/alerts', async (route) => {
    alertRequests += 1
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(alertRequests === 1 ? [{
        alert_id: 1,
        worker_id: null,
        enrollment_id: 'enrollment-1',
        priority: 'high',
        alert_type: 'worker_enrollment_pending_approval',
        reason: 'administrator_approval_required',
        occurred_at: '2026-08-15T00:00:00Z',
      }] : []),
    })
  })
  await page.route('**/api/admin/product-pairs', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByRole('heading', { name: 'Needs attention' })).toBeVisible()
  await expect(page.getByText('Pending approval')).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Pending worker registrations' })).toBeVisible()
  await expect(page.getByText('12345678')).toBeVisible()
  await expect(page.getByText('Broker-Demo')).toBeVisible()
  await expect(page.getByText('87654321')).toBeVisible()
  await page.getByRole('button', { name: 'View registration evidence' }).click()
  await expect(page.getByRole('region', { name: 'Account information for 12345678 on Broker-Demo' })
    .getByText('"currency": "USD"')).toBeVisible()
  await expect(page.getByRole('region', { name: 'Terminal information for 12345678 on Broker-Demo' })
    .getByText('"platform": "MetaTrader 5"')).toBeVisible()
  const enrollment = page.getByRole('listitem').filter({ hasText: '12345678' })
  const metadata = enrollment.locator('dl')
  const evidence = enrollment.locator('.enrollment-evidence')
  const actions = enrollment.locator('.enrollment-actions')
  const metadataBox = await metadata.boundingBox()
  const evidenceBox = await evidence.boundingBox()
  const actionsBox = await actions.boundingBox()
  expect(metadataBox).not.toBeNull()
  expect(evidenceBox).not.toBeNull()
  expect(actionsBox).not.toBeNull()
  expect(evidenceBox!.y).toBeGreaterThan(metadataBox!.y + metadataBox!.height)
  expect(actionsBox!.y).toBeGreaterThan(evidenceBox!.y + evidenceBox!.height)
  await page.getByRole('button', { name: 'Approve registration for 12345678 on Broker-Demo' }).first().click()

  await expect(page.getByText('No operator action is needed.')).toBeVisible()
  await expect(page.getByText('No worker registrations are awaiting review.')).toBeVisible()
  await expect(page.getByText('worker_enrollment_approved')).toBeVisible()
  expect(enrollmentRequests).toBe(2)
})

test('administrator can view connected worker health and reconciliation', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    workers: [{
      worker_id: 'worker-1',
      login: 123456,
      server: 'Broker-Demo',
      connectivity: 'connected',
      safety_state: 'connected',
      latest_snapshot: {
        cursor: 1,
        observed_at: '2026-08-16T00:00:00Z',
        account: { balance: 1000 },
        terminal: { connected: true },
        orders: [],
        positions: [],
      },
      deltas: [{ cursor: 2, observed_at: '2026-08-16T00:01:00Z', entity: 'position', ticket: '51',
        change: 'volume_changed', record: { volume: 1 } }],
    }],
    alerts: [{
      alert_id: 1,
      worker_id: 'worker-1',
      priority: 'high',
      alert_type: 'external_broker_change',
      reason: 'unattributed_broker_change',
      occurred_at: '2026-08-16T00:01:00Z',
    }],
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByRole('heading', { name: 'Account workers' })).toBeVisible()
  const workerCard = page.getByRole('listitem').filter({ hasText: '123456 on Broker-Demo' })
  await expect(workerCard.getByText('connected', { exact: true }).first()).toBeVisible()
  await expect(page.getByText('volume_changed')).toBeVisible()
  await expect(page.getByText('"balance": 1000')).toBeVisible()
  await expect(page.getByText('high: external_broker_change')).toBeVisible()
})

test('administrator can launch an analysis with CSRF protection and inspect passing, failing, and exception evidence', async ({ page }) => {
  let receivedHeaders: Record<string, string> | null = null
  let receivedBody: Record<string, unknown> | null = null

  await mockLogin(page)
  await mockManagementData(page, {
    events: [
      {
        event_id: 10,
        event_type: 'product_catalog_analysis_requested',
        payload: { analysis_id: 'analysis-1' },
        occurred_at: '2026-08-17T00:00:00Z',
      },
      {
        event_id: 11,
        event_type: 'product_catalog_analysis_retry',
        payload: { analysis_id: 'analysis-1', stage: 'm15_screening', reason: 'Worker timed out once.' },
        occurred_at: '2026-08-17T00:03:00Z',
      },
      {
        event_id: 12,
        event_type: 'product_catalog_analysis_succeeded',
        payload: { analysis_id: 'analysis-1' },
        occurred_at: '2026-08-17T00:08:00Z',
      },
    ],
    workers: [
      buildWorker({ worker_id: 'worker-a', login: 111111, server: 'Broker-A' }),
      buildWorker({ worker_id: 'worker-b', login: 222222, server: 'Broker-B' }),
      buildWorker({ worker_id: 'worker-c', login: 333333, server: 'Broker-A' }),
      buildWorker({ worker_id: 'worker-d', login: 444444, server: 'Broker-D', connectivity: 'stale' }),
    ],
  })

  await page.route('**/api/admin/product-catalog-analyses', async (route) => {
    receivedHeaders = route.request().headers()
    receivedBody = route.request().postDataJSON() as Record<string, unknown>
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(buildAnalysis()),
    })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await page.getByLabel('First analysis worker').selectOption('worker-a')
  await page.getByLabel('Second analysis worker').selectOption('worker-b')
  await page.getByLabel('Policy label').fill('FX catalog v2')
  await page.getByRole('button', { name: 'Launch analysis' }).click()

  expect(receivedHeaders?.['x-csrf-token']).toBe('csrf-token')
  expect(receivedBody).toMatchObject({
    first_worker_id: 'worker-a',
    second_worker_id: 'worker-b',
    policy: {
      label: 'FX catalog v2',
      require_equal_base_currency: true,
      require_equal_profit_currency: true,
      minimum_m15_common_coverage: 1,
      minimum_m1_common_coverage: 0.98,
      minimum_m15_return_correlation: 0.97,
      minimum_m1_return_correlation: 0.95,
      maximum_m1_median_price_difference_points: 2,
    },
  })

  await expect(page.getByRole('heading', { name: 'Analysis analysis-1' })).toBeVisible()
  const finalPassingSection = page.getByRole('heading', { name: 'Final passing candidates' }).locator('..')
  const finalFailingSection = page.getByRole('heading', { name: 'Final failing candidates' }).locator('..')
  await expect(finalPassingSection).toBeVisible()
  await expect(finalPassingSection.getByRole('heading', { name: 'EURUSD.a ↔ EURUSD' })).toBeVisible()
  await expect(finalPassingSection.getByRole('heading', { name: 'Warning differences' })).toBeVisible()
  const failedCandidates = finalFailingSection.locator('details')
  await expect(failedCandidates).not.toHaveAttribute('open', '')
  await failedCandidates.locator('summary').click()
  await expect(finalFailingSection.getByRole('heading', { name: 'Hard-block differences' })).toBeVisible()
  await expect(page.getByText('volume_step')).toBeVisible()
  await expect(page.getByText('volume_max')).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Calculation-mode exceptions' })).toBeVisible()
  await expect(page.getByText('Trade Calc Mode Mismatch')).toBeVisible()
  await expect(page.getByText('Worker timed out once.')).toBeVisible()
})

test('administrator sees the worker-provided analysis failure reason as an alert', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    workers: [
      buildWorker({ worker_id: 'worker-a', login: 111111, server: 'Broker-A' }),
      buildWorker({ worker_id: 'worker-b', login: 222222, server: 'Broker-B' }),
    ],
  })
  await page.route('**/api/admin/product-catalog-analyses', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(buildAnalysis({
        status: 'failed',
        current_stage: 'm15_failed',
        failure_reason: 'AUDNZDC M15 evidence is unavailable.',
      })),
    })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await page.getByLabel('First analysis worker').selectOption('worker-a')
  await page.getByLabel('Second analysis worker').selectOption('worker-b')
  await page.getByRole('button', { name: 'Launch analysis' }).click()

  await expect(page.getByRole('alert')).toHaveText('Analysis failed: AUDNZDC M15 evidence is unavailable.')
})

test('administrator can inspect immutable evidence, build a pair, and retire it without any broker-write path', async ({ page }) => {
  let buildHeaders: Record<string, string> | null = null
  let confirmationHeaders: Record<string, string> | null = null
  let currentPairs: Record<string, unknown>[] = []

  await mockLogin(page)
  await mockManagementData(page, {
    workers: [
      buildWorker({ worker_id: 'worker-a', login: 111111, server: 'Broker-A' }),
      buildWorker({ worker_id: 'worker-b', login: 222222, server: 'Broker-B' }),
    ],
  })
  await page.route('**/api/admin/product-catalog-analyses', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(buildAnalysis()),
    })
  })
  await page.route('**/api/admin/product-catalog-analyses/analysis-1/product-pair-build-confirmations', async (route) => {
    confirmationHeaders = route.request().headers()
    await route.fulfill({
      status: 201,
      contentType: 'application/json',
      body: JSON.stringify(buildProductPairConfirmation()),
    })
  })
  await page.route('**/api/admin/product-pairs/pair-1/retire', async (route) => {
    expect(route.request().method()).toBe('POST')
    expect(route.request().headers()['x-csrf-token']).toBe('csrf-token')
    currentPairs = [buildProductPair({ status: 'retired', retired_at: '2026-08-17T00:12:00Z', retired_by: 'ABCDEF', retired_reason: 'manual_retirement' })]
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(currentPairs[0]),
    })
  })
  await page.route('**/api/admin/product-pairs', async (route) => {
    if (route.request().method() === 'GET') {
      await route.fulfill({
        contentType: 'application/json',
        body: JSON.stringify(currentPairs),
      })
      return
    }
    buildHeaders = route.request().headers()
    currentPairs = [buildProductPair()]
    await route.fulfill({
      status: 201,
      contentType: 'application/json',
      body: JSON.stringify(currentPairs[0]),
    })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await page.getByLabel('First analysis worker').selectOption('worker-a')
  await page.getByLabel('Second analysis worker').selectOption('worker-b')
  await page.getByRole('button', { name: 'Launch analysis' }).click()

  await expect(page.getByText('It never places, modifies, or closes broker orders.')).toBeVisible()
  const candidateCard = page.locator('.buildable-result-card').first()
  await candidateCard.getByRole('button', { name: 'Prepare Build confirmation' }).click()

  expect(confirmationHeaders?.['x-csrf-token']).toBe('csrf-token')
  await expect(candidateCard.getByText('Lot relationship')).toBeVisible()
  await expect(candidateCard.getByText('1:1')).toBeVisible()
  await expect(candidateCard.getByText('111111 on Broker-A')).toBeVisible()
  await expect(candidateCard.getByText('222222 on Broker-B')).toBeVisible()
  await expect(candidateCard.getByText('Broker-A · EURUSD.a')).toBeVisible()
  await expect(candidateCard.getByText('Broker-B · EURUSD')).toBeVisible()

  const buildButton = candidateCard.getByRole('button', { name: 'Build product pair' })
  await expect(buildButton).toBeDisabled()
  await candidateCard.getByRole('checkbox', { name: /Explicit Build confirmation/i }).check()
  await expect(buildButton).toBeEnabled()
  await buildButton.click()

  expect(buildHeaders?.['x-csrf-token']).toBe('csrf-token')
  await expect(page.getByRole('heading', { name: 'Active product pairs' })).toBeVisible()
  const pairCard = page.locator('.product-pair-card').filter({ hasText: 'pair-1' })
  await expect(pairCard.getByRole('heading', { name: 'Broker-A:EURUSD.a ↔ Broker-B:EURUSD' })).toBeVisible()
  await expect(pairCard).toContainText('Original policy snapshot')
  await expect(pairCard).toContainText('Passed')
  await expect(page.locator('.buildable-result-card')).toHaveCount(0)
  await expect(page.getByText('1 final passing candidate(s) already have an active product pair')).toBeVisible()

  await page.getByRole('button', { name: 'Retire product pair' }).click()

  await expect(page.getByText('No active product pairs.')).toBeVisible()
  await expect(page.locator('#retired-product-pairs-heading')).toBeVisible()
  await expect(page.locator('.product-pair-card').first()).toContainText('Manual retirement')
})

test('administrator hides a final candidate already represented by an active pair', async ({ page }) => {
  const initialPair = buildProductPair({
    product_pair_id: 'pair-existing',
    built_from_analysis_id: 'analysis-old',
    built_from_confirmation_id: 'confirmation-old',
    policy_snapshot: {
      ...buildAnalysis().policy,
      label: 'FX catalog v1',
    },
    created_at: '2026-08-16T00:00:00Z',
  })
  const currentPairs: Record<string, unknown>[] = [initialPair]

  await mockLogin(page)
  await mockManagementData(page, {
    workers: [
      buildWorker({ worker_id: 'worker-a', login: 111111, server: 'Broker-A' }),
      buildWorker({ worker_id: 'worker-b', login: 222222, server: 'Broker-B' }),
    ],
    productPairs: currentPairs,
  })
  await page.route('**/api/admin/product-catalog-analyses', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(buildAnalysis({
        policy: {
          ...buildAnalysis().policy,
          label: 'FX catalog v3',
          maximum_m1_p99_price_difference_points: 20,
        },
      })),
    })
  })
  await page.route('**/api/admin/product-pairs', async (route) => {
    if (route.request().method() === 'GET') {
      await route.fulfill({
        contentType: 'application/json',
        body: JSON.stringify(currentPairs),
      })
      return
    }
    await route.fulfill({
      status: 409,
      contentType: 'application/json',
      body: JSON.stringify({ detail: 'An active product pair already exists for this unordered endpoint pair.' }),
    })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await page.getByRole('button', { name: 'Launch analysis' }).click()

  await expect(page.locator('.buildable-result-card')).toHaveCount(0)
  await expect(page.getByText('1 final passing candidate(s) already have an active product pair')).toBeVisible()
  await expect(page.locator('.product-pair-card').first()).toContainText('pair-existing')
})

test('administrator can inspect compatibility evidence and explicitly exclude one worker from one pair', async ({ page }) => {
  let compatibilityHeaders: Record<string, string> | null = null
  let exclusionHeaders: Record<string, string> | null = null
  const compatibilityResult = buildCompatibilityResult({
    product_pair_id: 'pair-1',
    worker_id: 'worker-c',
    login: 333333,
    server: 'Broker-A',
    reference_symbol: 'EURUSD',
    inspection_status: 'differences_detected',
    live_specification: {
      symbol: 'EURUSD',
      digits: 5,
      point: 0.00001,
      volume_max: 50,
    },
    reference_specification: {
      symbol: 'EURUSD',
      digits: 5,
      point: 0.00001,
      volume_max: 100,
    },
    hard_block_differences: [],
    warning_differences: [{ field: 'volume_max', first_value: 50, second_value: 100 }],
  })
  let currentPairs = [buildProductPair({
    worker_applicability: [
      buildWorkerApplicability({
        worker_id: 'worker-a',
        login: 111111,
        server: 'Broker-A',
        applicability_status: 'applicable',
        inspection_status: 'differences_detected',
        latest_compatibility_check: buildCompatibilityResult({
          product_pair_id: 'pair-1',
          worker_id: 'worker-a',
          login: 111111,
          server: 'Broker-A',
          reference_symbol: 'EURUSD',
          inspection_status: 'differences_detected',
          warning_differences: [{ field: 'volume_max', first_value: 200, second_value: 250 }],
        }),
      }),
      buildWorkerApplicability({
        worker_id: 'worker-b',
        login: 222222,
        server: 'Broker-B',
        applicability_status: 'excluded',
        inspection_status: 'differences_detected',
        latest_compatibility_check: buildCompatibilityResult({
          product_pair_id: 'pair-1',
          worker_id: 'worker-b',
          login: 222222,
          server: 'Broker-B',
          reference_symbol: 'EURUSD.a',
          inspection_status: 'differences_detected',
          hard_block_differences: [{ field: 'volume_step', first_value: 0.01, second_value: 0.1 }],
        }),
        exclusion: {
          excluded_at: '2026-08-17T00:15:00Z',
          excluded_by: 'ABCDEF',
          compatibility_check_id: 'compatibility-check-b',
        },
      }),
    ],
  })]
  const workers = [
    buildWorker({ worker_id: 'worker-a', login: 111111, server: 'Broker-A' }),
    buildWorker({ worker_id: 'worker-b', login: 222222, server: 'Broker-B' }),
    buildWorker({ worker_id: 'worker-c', login: 333333, server: 'Broker-A' }),
  ]

  await mockLogin(page)
  await page.route('**/api/admin/events', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/enrollments', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/workers', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(workers) })
  })
  await page.route('**/api/admin/alerts', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/product-pairs/pair-1/workers/worker-c/compatibility-check', async (route) => {
    compatibilityHeaders = route.request().headers()
    expect(route.request().method()).toBe('POST')
    expect(route.request().postData()).toBeNull()
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(compatibilityResult),
    })
  })
  await page.route('**/api/admin/product-pairs/pair-1/workers/worker-c/exclude', async (route) => {
    exclusionHeaders = route.request().headers()
    expect(route.request().method()).toBe('POST')
    expect(route.request().postData()).toBeNull()
    currentPairs = [buildProductPair({
      worker_applicability: [
        ...currentPairs[0].worker_applicability,
        buildWorkerApplicability({
          worker_id: 'worker-c',
          login: 333333,
          server: 'Broker-A',
          applicability_status: 'excluded',
          inspection_status: 'differences_detected',
          latest_compatibility_check: compatibilityResult,
          exclusion: {
            excluded_at: '2026-08-17T00:17:00Z',
            excluded_by: 'ABCDEF',
            compatibility_check_id: 'compatibility-check-c',
          },
        }),
      ],
    })]
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(currentPairs[0].worker_applicability[currentPairs[0].worker_applicability.length - 1]),
    })
  })
  await page.route('**/api/admin/product-pairs', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(currentPairs) })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  const checkedWorker = page.locator('.pair-worker-card').filter({ hasText: '111111 on Broker-A' })
  const excludedWorker = page.locator('.pair-worker-card').filter({ hasText: '222222 on Broker-B' })
  const uninspectedWorker = page.locator('.pair-worker-card').filter({ hasText: '333333 on Broker-A' })
  await expect(checkedWorker).toContainText('Checked')
  await expect(excludedWorker).toContainText('Excluded')
  await expect(uninspectedWorker).toContainText('Applicable (uninspected)')

  await uninspectedWorker.getByRole('button', { name: 'Run compatibility check for 333333 on Broker-A' }).click()

  expect(compatibilityHeaders?.['x-csrf-token']).toBe('csrf-token')
  await expect(uninspectedWorker.getByText('"volume_max": 50')).toBeVisible()
  await expect(uninspectedWorker.getByText('"volume_max": 100')).toBeVisible()
  await expect(uninspectedWorker.getByRole('heading', { name: 'Warning differences' })).toBeVisible()
  await expect(uninspectedWorker).toContainText('Reference symbol')
  await expect(uninspectedWorker).toContainText('Differences detected')
  await expect(uninspectedWorker.getByLabel('Compatibility evidence for 333333 on Broker-A').getByText('volume_max', { exact: true })).toBeVisible()
  await expect(checkedWorker).toContainText('Checked')

  await uninspectedWorker.getByRole('button', { name: 'Exclude 333333 on Broker-A from pair-1' }).click()

  expect(exclusionHeaders?.['x-csrf-token']).toBe('csrf-token')
  await expect(uninspectedWorker).toContainText('Excluded')
  await expect(uninspectedWorker).toContainText('Excluded at')
})

test('administrator sees invalid manual re-test selection when an endpoint has no healthy connected workers', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    workers: [
      buildWorker({ worker_id: 'worker-a', login: 111111, server: 'Broker-A' }),
      buildWorker({ worker_id: 'worker-b', login: 222222, server: 'Broker-B', connectivity: 'stale' }),
    ],
    productPairs: [buildProductPair({ latest_retest: null })],
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByText('No healthy connected workers are available on Broker-B.')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Run manual re-test' })).toBeDisabled()
  await expect(page.getByLabel('Re-test worker for Broker-B').locator('option')).toHaveCount(1)
})

test('administrator can submit a manual re-test with valid endpoint workers and see failed alert state', async ({ page }) => {
  let retestHeaders: Record<string, string> | null = null
  let currentAlerts: Record<string, unknown>[] = []
  let currentPairs = [buildProductPair({ latest_retest: null })]
  const workers = [
    buildWorker({ worker_id: 'worker-a', login: 111111, server: 'Broker-A' }),
    buildWorker({ worker_id: 'worker-c', login: 333333, server: 'Broker-A' }),
    buildWorker({ worker_id: 'worker-b', login: 222222, server: 'Broker-B', connectivity: 'stale' }),
    buildWorker({ worker_id: 'worker-d', login: 444444, server: 'Broker-B' }),
  ]

  await mockLogin(page)
  await page.route('**/api/admin/events', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/enrollments', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/workers', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(workers) })
  })
  await page.route('**/api/admin/alerts', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(currentAlerts) })
  })
  await page.route('**/api/admin/product-pairs/pair-1/retests', async (route) => {
    retestHeaders = route.request().headers()
    expect(route.request().method()).toBe('POST')
    expect(route.request().postDataJSON()).toEqual({
      first_worker_id: 'worker-c',
      second_worker_id: 'worker-d',
    })
    currentAlerts = [
      {
        alert_id: 8,
        worker_id: 'worker-c',
        product_pair_id: 'pair-other',
        priority: 'high',
        alert_type: 'product_pair_retest_failed',
        reason: 'Latest re-test failed for pair-1',
        occurred_at: '2026-08-17T00:17:30Z',
      },
      {
        alert_id: 9,
        worker_id: 'worker-c',
        product_pair_id: 'pair-1',
        priority: 'high',
        alert_type: 'product_pair_retest_failed',
        reason: 'Pair pair-1 exceeded drift threshold.',
        occurred_at: '2026-08-17T00:18:00Z',
      },
    ]
    currentPairs = [buildProductPair({
      latest_retest: buildRetest({
        status: 'failed',
        failure_reason: 'Reference drift exceeded threshold.',
        source_workers: {
          first_worker: { worker_id: 'worker-c', login: 333333, server: 'Broker-A' },
          second_worker: { worker_id: 'worker-d', login: 444444, server: 'Broker-B' },
        },
        verification_result: buildStageResult({
          verification_status: 'failed',
          statistics: {
            aligned_bar_count: 4,
            first_bar_count: 4,
            second_bar_count: 4,
            coverage_ratio: 0.97,
            return_correlation: 0.95,
            median_price_difference_points: 2.5,
            p99_price_difference_points: 18,
            target_point: 0.00001,
          },
          hard_block_differences: [{ field: 'volume_step', first_value: 0.01, second_value: 0.1 }],
          warning_differences: [{ field: 'swap_long', first_value: -4.2, second_value: -3.7 }],
        }),
        alert: {
          product_pair_id: 'pair-1',
          priority: 'high',
          alert_type: 'product_pair_retest_failed',
          reason: 'Pair pair-1 exceeded drift threshold.',
          occurred_at: '2026-08-17T00:18:00Z',
        },
        requested_at: '2026-08-17T00:16:00Z',
        completed_at: '2026-08-17T00:18:00Z',
      }),
    })]
    await route.fulfill({
      status: 201,
      contentType: 'application/json',
      body: JSON.stringify(currentPairs[0].latest_retest),
    })
  })
  await page.route('**/api/admin/product-pairs', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(currentPairs) })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByRole('button', { name: 'Run manual re-test' })).toBeEnabled()
  await page.getByLabel('Re-test worker for Broker-A').selectOption('worker-c')
  await page.getByLabel('Re-test worker for Broker-B').selectOption('worker-d')
  await page.getByRole('button', { name: 'Run manual re-test' }).click()

  expect(retestHeaders?.['x-csrf-token']).toBe('csrf-token')
  const pairCard = page.locator('.product-pair-card').filter({ hasText: 'pair-1' })
  await expect(pairCard).toContainText('Recorded a re-test request for pair-1.')
  await expect(pairCard).toContainText('Original policy snapshot')
  await expect(pairCard).toContainText('Failed')
  await expect(pairCard).toContainText('Reference drift exceeded threshold.')
  await expect(pairCard).toContainText('333333 on Broker-A')
  await expect(pairCard).toContainText('444444 on Broker-B')
  await expect(pairCard).toContainText('97.00%')
  await expect(pairCard).toContainText('0.95')
  await expect(pairCard).toContainText('Latest failed re-test')
  await expect(pairCard).toContainText('high: product_pair_retest_failed')
  await expect(pairCard).toContainText('Pair pair-1 exceeded drift threshold.')
  await expect(pairCard.locator('.conflict-panel').filter({ hasText: 'Latest failed re-test' })).not.toContainText('Latest re-test failed for pair-1')
  await expect(pairCard).toContainText('Active')
  await expect(page.getByRole('heading', { name: 'Worker alerts' }).locator('..')).toContainText('product_pair_retest_failed')
})

test('administrator sees validation feedback when no cross-server pair is available', async ({ page }) => {
  let launchRequests = 0

  await mockLogin(page)
  await mockManagementData(page, {
    workers: [
      buildWorker({ worker_id: 'worker-a', login: 111111, server: 'Broker-A' }),
      buildWorker({ worker_id: 'worker-c', login: 333333, server: 'Broker-A' }),
      buildWorker({ worker_id: 'worker-d', login: 444444, server: 'Broker-D', connectivity: 'stale' }),
    ],
  })
  await page.route('**/api/admin/product-catalog-analyses', async (route) => {
    launchRequests += 1
    await route.fulfill({ status: 500 })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByText('Pick two eligible workers on different exact MT5 servers before launching.')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Launch analysis' })).toBeDisabled()
  expect(launchRequests).toBe(0)
})

test('administrator sees terminal analysis failures with lifecycle details', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    events: [
      {
        event_id: 20,
        event_type: 'product_catalog_analysis_requested',
        payload: { analysis_id: 'analysis-failed' },
        occurred_at: '2026-08-17T00:00:00Z',
      },
      {
        event_id: 21,
        event_type: 'product_catalog_analysis_retry',
        payload: { analysis_id: 'analysis-failed', stage: 'm1_verification', reason: 'Worker returned incomplete evidence.' },
        occurred_at: '2026-08-17T00:05:00Z',
      },
      {
        event_id: 22,
        event_type: 'product_catalog_analysis_failed',
        payload: { analysis_id: 'analysis-failed', stage: 'm1_failed', reason: 'Worker returned incomplete evidence.' },
        occurred_at: '2026-08-17T00:06:00Z',
      },
    ],
    workers: [
      buildWorker({ worker_id: 'worker-a', login: 111111, server: 'Broker-A' }),
      buildWorker({ worker_id: 'worker-b', login: 222222, server: 'Broker-B' }),
    ],
  })
  await page.route('**/api/admin/product-catalog-analyses', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(buildAnalysis({
        analysis_id: 'analysis-failed',
        status: 'failed',
        current_stage: 'm1_failed',
        retry_count: 1,
        failure_reason: 'Worker returned incomplete evidence.',
        completed_at: '2026-08-17T00:06:00Z',
      })),
    })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await page.getByRole('button', { name: 'Launch analysis' }).click()

  await expect(page.locator('.analysis-results .status-badge').first()).toHaveText('Failed')
  await expect(page.getByText('M1 verification failed', { exact: true })).toBeVisible()
  const lifecycleCard = page.locator('.analysis-results .summary-grid .panel').first()
  await expect(lifecycleCard).toContainText('Worker returned incomplete evidence.')
  await expect(lifecycleCard).toContainText('Retry count')
  await expect(lifecycleCard).toContainText('1')
})

async function mockSessionResume(page: Parameters<typeof test>[0]['page']) {
  await page.route('**/api/admin/session', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({ csrf_token: 'resumed-csrf-token' }) })
  })
  await mockManagementData(page)
}

async function mockLogin(page: Parameters<typeof test>[0]['page']) {
  await page.route('**/api/admin/login', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ csrf_token: 'csrf-token' }),
    })
  })
}

async function mockManagementData(
  page: Parameters<typeof test>[0]['page'],
  overrides?: {
    events?: object[]
    enrollments?: object[]
    workers?: object[]
    alerts?: object[]
    productPairs?: object[]
  },
) {
  await page.route('**/api/admin/events', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(overrides?.events ?? []),
    })
  })
  await page.route('**/api/admin/enrollments', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(overrides?.enrollments ?? []),
    })
  })
  await page.route('**/api/admin/workers', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(overrides?.workers ?? []),
    })
  })
  await page.route('**/api/admin/alerts', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(overrides?.alerts ?? []),
    })
  })
  await page.route('**/api/admin/product-pairs', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(overrides?.productPairs ?? []),
    })
  })
}

function buildEnrollment() {
  return {
    enrollment_id: 'enrollment-1',
    login: 12345678,
    server: 'Broker-Demo',
    pairing_code: '87654321',
    created_at: '2026-08-15T00:00:00Z',
    expires_at: '2026-08-15T00:15:00Z',
    account_info: {
      currency: 'USD',
      leverage: 100,
    },
    terminal_info: {
      platform: 'MetaTrader 5',
      version: '5.0.0',
    },
  }
}

function buildWorker(overrides: Record<string, unknown>) {
  return {
    worker_id: 'worker-1',
    login: 123456,
    server: 'Broker-Demo',
    connectivity: 'connected',
    safety_state: 'connected',
    latest_snapshot: null,
    deltas: [],
    ...overrides,
  }
}

function buildAnalysis(overrides?: Record<string, unknown>) {
  return {
    analysis_id: 'analysis-1',
    requested_by: 'ABCDEF',
    first_worker: { worker_id: 'worker-a', login: 111111, server: 'Broker-A' },
    second_worker: { worker_id: 'worker-b', login: 222222, server: 'Broker-B' },
    policy: {
      label: 'FX catalog v2',
      require_equal_base_currency: true,
      require_equal_profit_currency: true,
      minimum_common_coverage: 0.99,
      minimum_m15_return_correlation: 0.98,
      minimum_m1_return_correlation: 0.97,
      maximum_m1_median_price_difference_points: 2,
      maximum_m1_p99_price_difference_points: 15,
    },
    status: 'succeeded',
    failure_reason: null,
    current_stage: 'completed',
    retry_count: 1,
    analysis_period: {
      timeframe: 'M15',
      started_at_utc: '2026-08-10T00:00:00Z',
      ended_at_utc: '2026-08-17T00:00:00Z',
    },
    first_catalog_evidence: {
      collected_at: '2026-08-17T00:01:00Z',
      symbols: [{ symbol: 'EURUSD.a' }, { symbol: 'GBPUSD.a' }],
    },
    second_catalog_evidence: {
      collected_at: '2026-08-17T00:01:30Z',
      symbols: [{ symbol: 'EURUSD' }, { symbol: 'GBPUSD' }],
    },
    eligible_candidates: [
      {
        first_symbol: 'EURUSD.a',
        second_symbol: 'EURUSD',
        currency_base: 'EUR',
        currency_profit: 'USD',
        first_point: 0.00001,
        second_point: 0.00001,
      },
      {
        first_symbol: 'GBPUSD.a',
        second_symbol: 'GBPUSD',
        currency_base: 'GBP',
        currency_profit: 'USD',
        first_point: 0.00001,
        second_point: 0.00001,
      },
    ],
    exceptions: [{
      first_symbol: 'XAUUSD',
      second_symbol: 'XAUUSD',
      currency_base: 'XAU',
      currency_profit: 'USD',
      first_point: 0.01,
      second_point: 0.01,
      reason: 'trade_calc_mode_mismatch',
      first_trade_calc_mode: 1,
      second_trade_calc_mode: 0,
    }],
    m15_screening_results: [
      buildStageResult({
        first_symbol: 'EURUSD.a',
        second_symbol: 'EURUSD',
        screening_status: 'passed',
      }),
      buildStageResult({
        first_symbol: 'GBPUSD.a',
        second_symbol: 'GBPUSD',
        screening_status: 'passed',
        statistics: {
          aligned_bar_count: 4,
          first_bar_count: 4,
          second_bar_count: 4,
          coverage_ratio: 1,
          return_correlation: 0.99,
          median_price_difference_points: 1.5,
          p99_price_difference_points: 3,
          target_point: 0.00001,
        },
      }),
    ],
    m1_verification_results: [
      buildStageResult({
        first_symbol: 'EURUSD.a',
        second_symbol: 'EURUSD',
        verification_status: 'passed',
        hard_block_differences: [],
        warning_differences: [{ field: 'volume_max', first_value: 200, second_value: 250 }],
      }),
      buildStageResult({
        first_symbol: 'GBPUSD.a',
        second_symbol: 'GBPUSD',
        verification_status: 'failed',
        hard_block_differences: [{ field: 'volume_step', first_value: 0.01, second_value: 0.1 }],
        warning_differences: [],
      }),
    ],
    requested_at: '2026-08-17T00:00:00Z',
    catalog_completed_at: '2026-08-17T00:02:00Z',
    m15_screened_at: '2026-08-17T00:04:00Z',
    m1_verified_at: '2026-08-17T00:08:00Z',
    completed_at: '2026-08-17T00:08:00Z',
    ...overrides,
  }
}

function buildProductPairConfirmation(overrides?: Record<string, unknown>) {
  return {
    confirmation_id: 'confirmation-1',
    analysis_id: 'analysis-1',
    requested_by: 'ABCDEF',
    analysis_period: buildAnalysis().analysis_period,
    policy_snapshot: buildAnalysis().policy,
    lot_relationship: {
      version: 'FX_V1',
      ratio: '1:1',
      first_lots: 1,
      second_lots: 1,
    },
    source_workers: {
      first_worker: buildAnalysis().first_worker,
      second_worker: buildAnalysis().second_worker,
    },
    endpoints: [
      { server: 'Broker-A', symbol: 'EURUSD.a' },
      { server: 'Broker-B', symbol: 'EURUSD' },
    ],
    reference_specifications: [
      {
        server: 'Broker-A',
        symbol: 'EURUSD.a',
        specification: {
          symbol: 'EURUSD.a',
          digits: 5,
          point: 0.00001,
          trade_calc_mode: 'FOREX',
        },
      },
      {
        server: 'Broker-B',
        symbol: 'EURUSD',
        specification: {
          symbol: 'EURUSD',
          digits: 5,
          point: 0.00001,
          trade_calc_mode: 'FOREX',
        },
      },
    ],
    approval_evidence: buildAnalysis().m1_verification_results[0],
    ...overrides,
  }
}

function buildProductPair(overrides?: Record<string, unknown>) {
  return {
    product_pair_id: 'pair-1',
    status: 'active',
    endpoints: buildProductPairConfirmation().endpoints,
    lot_relationship: buildProductPairConfirmation().lot_relationship,
    policy_snapshot: buildProductPairConfirmation().policy_snapshot,
    analysis_period: buildProductPairConfirmation().analysis_period,
    reference_specifications: buildProductPairConfirmation().reference_specifications,
    approval_evidence: buildProductPairConfirmation().approval_evidence,
    source_workers: buildProductPairConfirmation().source_workers,
    built_from_analysis_id: 'analysis-1',
    built_from_confirmation_id: 'confirmation-1',
    built_by: 'ABCDEF',
    created_at: '2026-08-17T00:10:00Z',
    retired_at: null,
    retired_by: null,
    retired_reason: null,
    replaced_by_product_pair_id: null,
    replaces_product_pair_id: null,
    latest_retest: buildRetest(),
    worker_applicability: [],
    ...overrides,
  }
}

function buildRetest(overrides?: Record<string, unknown>) {
  return {
    retest_id: 'retest-1',
    product_pair_id: 'pair-1',
    requested_by: 'ABCDEF',
    source_workers: buildProductPairConfirmation().source_workers,
    policy_snapshot: buildProductPairConfirmation().policy_snapshot,
    analysis_period: buildProductPairConfirmation().analysis_period,
    reference_specifications: buildProductPairConfirmation().reference_specifications,
    status: 'passed',
    current_stage: 'completed',
    retry_count: 0,
    failure_reason: null,
    requested_at: '2026-08-17T00:11:00Z',
    completed_at: '2026-08-17T00:12:00Z',
    verification_result: buildStageResult({
      verification_status: 'passed',
      hard_block_differences: [],
      warning_differences: [{ field: 'volume_max', first_value: 200, second_value: 250 }],
    }),
    ...overrides,
  }
}

function buildWorkerApplicability(overrides?: Record<string, unknown>) {
  return {
    worker_id: 'worker-a',
    login: 111111,
    server: 'Broker-A',
    applicability_status: 'applicable',
    inspection_status: 'differences_detected',
    latest_compatibility_check: buildCompatibilityResult(),
    exclusion: null,
    ...overrides,
  }
}

function buildCompatibilityResult(overrides?: Record<string, unknown>) {
  return {
    product_pair_id: 'pair-1',
    worker_id: 'worker-a',
    login: 111111,
    server: 'Broker-A',
    applicability_status: 'applicable',
    inspection_status: 'differences_detected',
    compatibility_check_id: 'compatibility-check-a',
    reference_symbol: 'EURUSD',
    live_specification: {
      symbol: 'EURUSD',
      digits: 5,
      point: 0.00001,
      trade_calc_mode: 'FOREX',
      volume_max: 200,
    },
    reference_specification: {
      symbol: 'EURUSD',
      digits: 5,
      point: 0.00001,
      trade_calc_mode: 'FOREX',
      volume_max: 250,
    },
    hard_block_differences: [],
    warning_differences: [{ field: 'volume_max', first_value: 200, second_value: 250 }],
    checked_at: '2026-08-17T00:13:00Z',
    checked_by: 'ABCDEF',
    ...overrides,
  }
}

function buildStageResult(overrides: Record<string, unknown>) {
  return {
    first_symbol: 'EURUSD.a',
    second_symbol: 'EURUSD',
    currency_base: 'EUR',
    currency_profit: 'USD',
    first_point: 0.00001,
    second_point: 0.00001,
    screening_status: 'passed',
    statistics: {
      aligned_bar_count: 4,
      first_bar_count: 4,
      second_bar_count: 4,
      coverage_ratio: 1,
      return_correlation: 1,
      median_price_difference_points: 1,
      p99_price_difference_points: 2,
      target_point: 0.00001,
    },
    policy_evaluation: {
      minimum_common_coverage: 0.99,
      minimum_m15_return_correlation: 0.98,
      minimum_m1_return_correlation: 0.97,
      maximum_m1_median_price_difference_points: 2,
      maximum_m1_p99_price_difference_points: 15,
      coverage_passed: true,
      return_correlation_passed: true,
      median_price_difference_passed: true,
      p99_price_difference_passed: true,
      hard_block_differences_passed: true,
    },
    first_market_data: {
      symbol: 'EURUSD.a',
      bar_count: 4,
      first_raw_epoch: 1000,
      last_raw_epoch: 1180,
      first_utc: '2026-08-10T00:00:00Z',
      last_utc: '2026-08-10T00:03:00Z',
      content_hash: 'first-hash',
      time_metadata: { source_family: 'market_data' },
    },
    second_market_data: {
      symbol: 'EURUSD',
      bar_count: 4,
      first_raw_epoch: 1000,
      last_raw_epoch: 1180,
      first_utc: '2026-08-10T00:00:00Z',
      last_utc: '2026-08-10T00:03:00Z',
      content_hash: 'second-hash',
      time_metadata: { source_family: 'market_data' },
    },
    ...overrides,
  }
}
