import { expect, test } from '@playwright/test'

test('administrator can reach the management login', async ({ page }) => {
  await page.goto('http://127.0.0.1:4173/')

  await expect(page.getByRole('heading', { name: 'Management access' })).toBeVisible()
  await expect(page.getByLabel('Administrator account')).toBeVisible()
  await expect(page.getByLabel('Password')).toBeVisible()
})

test('analysis history route is not treated as an analysis ID', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/product-catalog-analyses**', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ items: [], next_cursor: null, total_items: 0 }),
    })
  })

  await page.goto('http://127.0.0.1:4173/analysis/history')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.locator('h1')).toHaveCount(0)
  await expect(page.getByRole('heading', { name: 'Analysis result' })).toHaveCount(0)
})

test('disabled pagination controls do not imply an active load', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/product-catalog-analyses?**', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        items: [{
          analysis_id: 'analysis-1',
          requested_by: 'ABCDEF',
          first_worker: { worker_id: 'worker-a', login: 111111, server: 'Broker-A' },
          second_worker: { worker_id: 'worker-b', login: 222222, server: 'Broker-B' },
          policy_label: 'FX catalog v2',
          status: 'succeeded',
          current_stage: 'completed',
          retry_count: 0,
          requested_at: '2026-08-16T00:00:00Z',
          completed_at: '2026-08-16T00:08:00Z',
        }],
        next_cursor: 'next-page',
        total_items: 21,
      }),
    })
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Analysis history' }).click()

  const previous = page.getByRole('button', { name: 'Previous' })
  await expect(previous).toBeDisabled()
  await expect(previous).not.toHaveCSS('cursor', 'wait')
})

test('administrator resumes a cookie-backed session after refresh', async ({ page }) => {
  await mockSessionResume(page)

  await page.goto('http://127.0.0.1:4173/')

  await expect(page.getByRole('heading', { name: 'Management console' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Audit events' })).toHaveCount(0)
})

test('resuming a session does not flash the sign-in screen', async ({ page }) => {
  await page.addInitScript(() => {
    const markLoginScreen = () => {
      if (document.body?.textContent?.includes('Management access')) {
        window.sessionStorage.setItem('sign-in-screen-seen', 'true')
      }
    }
    new MutationObserver(markLoginScreen).observe(document, { childList: true, subtree: true })
    markLoginScreen()
  })
  await page.route('**/api/admin/session', async (route) => {
    await new Promise((resolve) => setTimeout(resolve, 600))
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({ csrf_token: 'resumed-csrf-token' }) })
  })
  await mockManagementData(page)

  await page.goto('http://127.0.0.1:4173/', { waitUntil: 'domcontentloaded' })
  await expect(page.getByRole('heading', { name: 'Management console' })).toBeVisible()
  await expect(page.evaluate(() => window.sessionStorage.getItem('sign-in-screen-seen'))).resolves.toBeNull()
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

test('administrator can sign out from the console header', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/logout', async (route) => {
    expect(route.request().method()).toBe('POST')
    expect(route.request().headers()['x-csrf-token']).toBe('csrf-token')
    await route.fulfill({ status: 204 })
  })

  await signIn(page)
  await page.getByRole('button', { name: 'Sign out' }).click()

  await expect(page.getByRole('heading', { name: 'Management access' })).toBeVisible()
})

test('expired sessions return to the sign-in screen without management data', async ({ page }) => {
  await page.route('**/api/admin/session', async (route) => {
    await route.fulfill({ status: 401, contentType: 'application/json', body: JSON.stringify({ detail: 'Administrator login is required.' }) })
  })

  await page.goto('http://127.0.0.1:4173/workers')

  await expect(page.getByRole('heading', { name: 'Management access' })).toBeVisible()
  await expect(page.getByText('Your session has expired. Please sign in again.')).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Workers' })).toHaveCount(0)
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
  await page.getByRole('link', { name: 'Workers' }).click()

  await expect(page.getByRole('heading', { name: 'Action required' })).toHaveCount(0)
  await expect(page.getByRole('heading', { name: 'Pending registrations' })).toBeVisible()
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

  await expect(page.getByRole('heading', { name: 'Action required' })).toHaveCount(0)
  await expect(page.getByRole('heading', { name: 'Pending registrations' })).toHaveCount(0)
  expect(enrollmentRequests).toBe(2)
})

test('administrator reviews pending enrollments from the notification list', async ({ page }) => {
  let approved = false
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/enrollments', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(approved ? [] : [buildEnrollment()]),
    })
  })
  await page.route('**/api/admin/enrollments/enrollment-1/approve', async (route) => {
    approved = true
    await route.fulfill({ status: 200 })
  })

  await signIn(page)

  await expect(page.getByRole('heading', { level: 1, name: 'Management console' })).toBeVisible()
  const notifications = page.getByRole('button', { name: 'Pending enrollment notifications: 1' })
  await expect(notifications).toBeVisible()
  await notifications.click()
  await page.getByRole('button', { name: 'Review registration' }).click()
  const dialog = page.getByRole('dialog', { name: 'Registration for 12345678 on Broker-Demo' })
  await expect(dialog.getByText('"currency": "USD"')).toBeVisible()
  await expect(dialog.getByText('"platform": "MetaTrader 5"')).toBeVisible()
  await dialog.getByRole('button', { name: 'Approve' }).click()

  await expect(page.getByRole('button', { name: 'Pending enrollment notifications: 0' })).toBeVisible()
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
  await page.getByRole('link', { name: 'Workers' }).click()

  await expect(page.getByRole('heading', { name: 'Fleet' })).toBeVisible()
  const workerCard = page.getByLabel('Workers by account').getByRole('listitem').filter({ hasText: '123456 on Broker-Demo' })
  await expect(workerCard.getByText('Action needed', { exact: true })).toBeVisible()
  await expect(workerCard.getByText('"balance": 1000')).not.toBeVisible()
  await workerCard.getByRole('button', { name: /123456 on Broker-Demo/ }).click()
  await expect(page.getByText('volume_changed')).toBeVisible()
  await expect(page.getByText('"balance": 1000')).toBeVisible()
  await expect(page.getByText('unattributed_broker_change')).toBeVisible()
})

test('administrator keeps the revoke confirmation open when certificate containment fails', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    workers: [buildWorker({ worker_id: 'worker-1', login: 123456, server: 'Broker-Demo' })],
  })
  await page.route('**/api/admin/workers/worker-1/revoke', async (route) => {
    expect(route.request().method()).toBe('POST')
    expect(route.request().headers()['x-csrf-token']).toBe('csrf-token')
    await route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ detail: 'unavailable' }) })
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Workers' }).click()
  const workerCard = page.getByLabel('Workers by account').getByRole('listitem').filter({ hasText: '123456 on Broker-Demo' })
  await workerCard.getByRole('button', { name: /123456 on Broker-Demo/ }).click()
  await workerCard.getByRole('button', { name: 'Revoke certificate' }).click()
  await page.getByRole('button', { name: 'Confirm revoke and flatten' }).click()

  await expect(page.getByRole('alert')).toContainText('Could not revoke this worker certificate.')
  await expect(page.getByRole('heading', { name: 'Revoke certificate for 123456 on Broker-Demo?' })).toBeVisible()
  await expect(page.locator('.worker-action-status')).toContainText('Certificate revocation failed. Confirm the worker state and try again.')
})

test('administrator can scan a realistic fleet, including stale and human-action workers, on a narrow screen', async ({ page }) => {
  const currentSnapshot = {
    cursor: 1,
    observed_at: new Date().toISOString(),
    account: { balance: 1000, currency: 'USD' },
    terminal: { connected: true },
    orders: [],
    positions: [],
  }
  const workers = [
    buildWorker({ worker_id: 'healthy-1', login: 100001, server: 'Broker-A', latest_snapshot: currentSnapshot }),
    buildWorker({ worker_id: 'healthy-2', login: 100002, server: 'Broker-B', latest_snapshot: currentSnapshot }),
    buildWorker({ worker_id: 'idle', login: 100003, server: 'Broker-C' }),
    buildWorker({ worker_id: 'stale', login: 100004, server: 'Broker-D', latest_snapshot: { ...currentSnapshot, observed_at: new Date(Date.now() - 16 * 60_000).toISOString() } }),
    buildWorker({ worker_id: 'revoked', login: 100005, server: 'Broker-E', connectivity: 'revoked' }),
    buildWorker({ worker_id: 'paused', login: 100006, server: 'Broker-F', safety_state: 'paused', latest_snapshot: currentSnapshot }),
    buildWorker({ worker_id: 'healthy-3', login: 100007, server: 'Broker-G', latest_snapshot: currentSnapshot }),
    buildWorker({ worker_id: 'healthy-4', login: 100008, server: 'Broker-H', latest_snapshot: currentSnapshot }),
  ]

  await page.setViewportSize({ width: 390, height: 844 })
  await mockLogin(page)
  await mockManagementData(page, { workers })
  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await page.getByRole('link', { name: 'Workers' }).click()

  const fleet = page.getByLabel('Workers by account')
  await expect(fleet.getByRole('listitem')).toHaveCount(8)
  await expect(fleet.getByText('Healthy', { exact: true })).toHaveCount(4)
  await expect(fleet.getByText('Stale', { exact: true })).toHaveCount(2)
  await expect(fleet.getByText('Revoked', { exact: true })).toBeVisible()
  await expect(fleet.getByText('Action needed', { exact: true })).toBeVisible()
  const firstWorker = fleet.getByRole('listitem').first()
  await expect(firstWorker.locator('.worker-signal')).toHaveCount(3)
  await expect(firstWorker.getByRole('button', { name: /Latest report: .*Account: .*Reconciliation:/ })).toBeVisible()
  await firstWorker.locator('.worker-signal').first().hover()
  await expect(page.getByRole('tooltip')).toHaveText(/Latest report:/)
  await expect(page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).resolves.toBe(true)
})

test('administrator can inspect a frozen worker and its immutable isolation record', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    workers: [buildWorker({
      worker_id: 'worker-frozen',
      login: 100008,
      server: 'Broker-H',
      safety_state: 'frozen',
      freeze: {
        source: 'execution_anomaly',
        affected_worker_ids: ['worker-frozen', 'worker-counterpart'],
        audit: { reason: 'Broker response could not be verified.' },
        frozen_at: '2026-08-22T09:00:00Z',
      },
    })],
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Workers' }).click()

  const workerCard = page.getByLabel('Workers by account').getByRole('listitem')
  await expect(workerCard.getByText('Frozen', { exact: true })).toBeVisible()
  await workerCard.getByRole('button', { name: /100008 on Broker-H/ }).click()
  await expect(workerCard.getByText('Frozen by execution anomaly.')).toBeVisible()
  await expect(workerCard.getByText('Affected workers: worker-frozen, worker-counterpart.')).toBeVisible()
  await expect(workerCard.getByText('Broker response could not be verified.')).toBeVisible()
})

test('administrator sees a useful fleet-health empty state', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, { workers: [] })
  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await page.getByRole('link', { name: 'Workers' }).click()

  await expect(page.getByRole('heading', { name: 'Fleet' })).toBeVisible()
  await expect(page.getByText('No reports yet.')).toBeVisible()
})

test('administrator can launch an analysis with CSRF protection and inspect passing and failing evidence', async ({ page }) => {
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
  await page.getByRole('link', { name: 'Analysis', exact: true }).click()

  await page.getByLabel('First analysis worker').selectOption('worker-a')
  await page.getByLabel('Second analysis worker').selectOption('worker-b')
  await page.getByLabel('Policy label').fill('FX catalog v2')
  await page.getByRole('button', { name: 'Launch' }).click()

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
  await page.getByRole('button', { name: /Candidates/ }).click()
  const finalPassingSection = page.getByRole('heading', { name: 'Final passing candidates' }).locator('..')
  const finalFailingSection = page.getByRole('heading', { name: 'Final failing candidates' }).locator('..')
  await expect(finalPassingSection).toBeVisible()
  await finalPassingSection.getByRole('button', { name: /EURUSD.a ↔ EURUSD/ }).click()
  await expect(finalPassingSection.getByRole('heading', { name: 'EURUSD.a ↔ EURUSD' })).toBeVisible()
  await expect(finalPassingSection.getByRole('heading', { name: 'Warning differences' })).toBeVisible()
  const failedCandidates = finalFailingSection.locator('details')
  await expect(failedCandidates).not.toHaveAttribute('open', '')
  await failedCandidates.locator('summary').click()
  await finalFailingSection.getByRole('button', { name: /GBPUSD.a ↔ GBPUSD/ }).click()
  await expect(finalFailingSection.getByRole('heading', { name: 'Warning differences' })).toBeVisible()
  await expect(page.getByText('volume_step')).toHaveCount(0)
  await expect(page.getByRole('heading', { name: 'Calculation-mode exceptions' })).toHaveCount(0)
  await expect(page.getByRole('button', { name: /Evidence|Activity/ })).toHaveCount(0)
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
  await page.getByRole('link', { name: 'Analysis', exact: true }).click()
  await page.getByLabel('First analysis worker').selectOption('worker-a')
  await page.getByLabel('Second analysis worker').selectOption('worker-b')
  await page.getByRole('button', { name: 'Launch' }).click()

  await expect(page.getByRole('alert').filter({ hasText: 'Analysis failed:' })).toHaveText('Analysis failed: AUDNZDC M15 evidence is unavailable.')
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
  await page.getByRole('link', { name: 'Analysis', exact: true }).click()
  await page.getByLabel('First analysis worker').selectOption('worker-a')
  await page.getByLabel('Second analysis worker').selectOption('worker-b')
  await page.getByRole('button', { name: 'Launch' }).click()

  await page.getByRole('button', { name: /Candidates/ }).click()
  await page.getByRole('button', { name: /EURUSD.a ↔ EURUSD/ }).first().click()
  await page.getByRole('button', { name: 'Create product pair for EURUSD.a and EURUSD' }).click()

  await expect.poll(() => buildHeaders?.['x-csrf-token']).toBe('csrf-token')
  expect(confirmationHeaders?.['x-csrf-token']).toBe('csrf-token')
  expect(buildHeaders?.['x-csrf-token']).toBe('csrf-token')
  await expect(page.getByLabel('Notifications').getByText('Created active product pair pair-1.')).toBeVisible()
  await page.getByRole('link', { name: 'Main' }).click()
  await expect(page.getByRole('heading', { name: 'Active product pairs' })).toBeVisible()
  const pairCard = page.locator('.product-pair-card').filter({ hasText: 'pair-1' })
  await expect(pairCard.getByRole('heading', { name: 'Broker-A:EURUSD.a ↔ Broker-B:EURUSD' })).toBeVisible()
  await expect(pairCard).toContainText('Original policy snapshot')
  await expect(pairCard).toContainText('Passed')

  await page.getByRole('button', { name: 'Retire product pair' }).click()

  await expect(page.getByText('No active product pairs.')).toBeVisible()
  await expect(page.locator('#retired-product-pairs-heading')).toBeVisible()
  await expect(page.locator('.product-pair-card').first()).toContainText('Manual retirement')
})

test('administrator sees product-pair state without fabricated paired-trade lifecycle data', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    productPairs: [buildProductPair()],
    pairedTradeLifecycle: {
      availability: 'unavailable',
      reason: 'Paired-trade lifecycle records are not available in the control-plane ledger.',
    },
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  const overview = page.getByLabel('Hedge lifecycle availability')
  await expect(overview).toContainText('1 active product-pair record is currently available.')
  await expect(overview).toContainText('Unavailable in the control-plane ledger.')
  await expect(overview).toContainText('cannot infer paired-trade activity')
  await expect(page.getByRole('button', { name: 'Retire product pair' })).toBeVisible()
})

test('administrator sees inactive product-pair state separately from paired-trade lifecycle availability', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    productPairs: [buildProductPair({ status: 'retired', retired_at: '2026-08-17T00:12:00Z' })],
    pairedTradeLifecycle: {
      availability: 'unavailable',
      reason: 'Paired-trade lifecycle records are not available in the control-plane ledger.',
    },
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  const overview = page.getByLabel('Hedge lifecycle availability')
  await expect(overview).toContainText('No active product-pair records are currently available.')
  await expect(overview).toContainText('Unavailable in the control-plane ledger.')
  await expect(page.locator('#retired-product-pairs-heading')).toBeVisible()
})

test('administrator sees when paired-trade lifecycle availability cannot be loaded', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/operations-dashboard', async (route) => {
    await route.fulfill({
      status: 503,
      contentType: 'application/json',
      body: JSON.stringify({ detail: 'The MT5 credential mediator is unavailable.' }),
    })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  const overview = page.getByLabel('Hedge lifecycle availability')
  await expect(overview).toContainText('Availability could not be confirmed.')
  await expect(overview.getByRole('status')).toContainText('The MT5 credential mediator is unavailable.')
})

test('administrator can review a final candidate that would replace an active pair', async ({ page }) => {
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
  await page.getByRole('link', { name: 'Analysis', exact: true }).click()
  await page.getByRole('button', { name: 'Launch' }).click()

  await page.getByRole('button', { name: /Candidates/ }).click()
  await expect(page.locator('.buildable-result-card')).toHaveCount(1)
  await expect(page.getByRole('button', { name: 'Create product pair for EURUSD.a and EURUSD' })).toBeVisible()
  await page.getByRole('link', { name: 'Main' }).click()
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
  await page.getByRole('link', { name: 'Workers' }).click()
  await expect(page.getByLabel('Worker intervention queue')).toContainText('Pair pair-1 exceeded drift threshold.')
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
  await page.getByRole('link', { name: 'Analysis', exact: true }).click()

  await expect(page.getByText('Pick two eligible workers on different exact MT5 servers before launching.')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Launch' })).toBeDisabled()
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
  await page.getByRole('link', { name: 'Analysis', exact: true }).click()

  await page.getByRole('button', { name: 'Launch' }).click()

  await expect(page.getByRole('heading', { name: 'Analysis analysis-failed' })).toBeVisible()
  await expect(page.getByText('Analysis failed: Worker returned incomplete evidence.', { exact: true })).toBeVisible()
})

test('administrator receives bounded live updates without losing the current console', async ({ page }) => {
  let eventRequests = 0

  await page.clock.install({ time: new Date('2026-08-19T00:00:00Z') })
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/events', async (route) => {
    eventRequests += 1
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(eventRequests === 1 ? [] : [{
        event_id: 99,
        event_type: 'worker_reconciliation_snapshot',
        payload: {},
        occurred_at: '2026-08-19T00:00:30Z',
      }]),
    })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await page.clock.fastForward(30_000)
  await expect(page.getByText('worker_reconciliation_snapshot')).toBeVisible()
  expect(eventRequests).toBe(2)
})

test('administrator sees stale-connection feedback and retains prior management data after a live refresh fails', async ({ page }) => {
  let eventRequests = 0
  const worker = buildWorker({ worker_id: 'worker-1', login: 123456, server: 'Broker-Demo' })

  await page.clock.install({ time: new Date('2026-08-19T00:00:00Z') })
  await mockLogin(page)
  await mockManagementData(page, { workers: [worker] })
  await page.route('**/api/admin/events', async (route) => {
    eventRequests += 1
    if (eventRequests === 1) {
      await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
      return
    }
    await route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ detail: 'unavailable' }) })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await page.clock.fastForward(30_000)
  await expect(page.getByRole('alert')).toContainText('Management data could not be loaded. Your previous data is still shown.')
  await page.getByRole('link', { name: 'Workers' }).click()
  await expect(page.getByLabel('Workers by account').getByRole('listitem').filter({ hasText: '123456 on Broker-Demo' })).toBeVisible()
})

test('administrator actions defer live refresh until the action is complete', async ({ page }) => {
  let enrollmentRequests = 0
  let releaseApproval: (() => void) | undefined

  await page.clock.install({ time: new Date('2026-08-19T00:00:00Z') })
  await mockLogin(page)
  await mockManagementData(page, {
    enrollments: [buildEnrollment()],
    alerts: [{
      alert_id: 1,
      worker_id: null,
      enrollment_id: 'enrollment-1',
      priority: 'high',
      alert_type: 'worker_enrollment_pending_approval',
      reason: 'administrator_approval_required',
      occurred_at: '2026-08-19T00:00:00Z',
    }],
  })
  await page.route('**/api/admin/enrollments', async (route) => {
    enrollmentRequests += 1
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([buildEnrollment()]) })
  })
  await page.route('**/api/admin/enrollments/enrollment-1/approve', async (route) => {
    await new Promise<void>((resolve) => {
      releaseApproval = resolve
    })
    await route.fulfill({ status: 200 })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await page.getByRole('link', { name: 'Workers' }).click()

  const approveButton = page.getByRole('button', { name: 'Approve registration for 12345678 on Broker-Demo' }).first()
  await approveButton.click()
  await expect(approveButton).toBeDisabled()
  await page.clock.fastForward(30_000)
  expect(enrollmentRequests).toBe(1)

  releaseApproval?.()
  await expect(approveButton).toBeEnabled()
  expect(enrollmentRequests).toBe(2)
})

test('administrator can search, paginate, and disclose audit evidence', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/events?**', async (route) => {
    const query = new URL(route.request().url()).searchParams
    expect(query.get('limit')).toBe('50')
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(query.get('page') === '2' ? {
        items: [{ event_id: 2, event_type: 'worker_reconciled', payload: {}, occurred_at: '2026-08-16T00:01:00Z' }],
        next_cursor: null,
        total_items: 51,
      } : {
        items: [{ event_id: 1, event_type: 'worker_enrollment_approved', payload: { reason: 'administrator_approval_required' }, occurred_at: '2026-08-16T00:00:00Z' }],
        next_cursor: 'next-events',
        total_items: 51,
      }),
    })
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Audit events' }).click()
  await expect(page.locator('h1')).toHaveCount(0)
  await expect(page.getByText('administrator_approval_required', { exact: true })).toBeVisible()
  await page.getByText('View raw payload').click()
  await expect(page.getByText('"reason": "administrator_approval_required"')).toBeVisible()
  await page.getByRole('button', { name: 'Next' }).click()
  await expect(page.getByText('Worker Reconciled', { exact: true }).first()).toBeVisible()
  await page.getByLabel('Search').fill('reconciled')
  await expect.poll(() => page.evaluate(() => performance.getEntriesByType('resource')
    .some((entry) => entry.name.includes('q=reconciled')))).toBe(true)
  await expect(page.getByText('Worker Enrollment Approved', { exact: true }).first()).toBeVisible()
})

test('administrator can search worker snapshots and retrieve raw evidence on demand', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/worker-snapshots/snapshot-1', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        account: { currency: 'USD' },
        positions: Array.from({ length: 100 }, (_, index) => ({ symbol: 'EURUSD', ticket: index })),
      }),
    })
  })
  await page.route('**/api/admin/worker-snapshots?**', async (route) => {
    const query = new URL(route.request().url()).searchParams
    expect(query.get('limit')).toBe('50')
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        items: [{
          snapshot_id: 'snapshot-1',
          server: 'Broker-A',
          login: 123456,
          balance: 1000,
          equity: 1005,
          trade_allowed: true,
          trade_expert: false,
          tradeapi_disabled: false,
          timestamp: '2026-08-16T00:00:00Z',
        }],
        next_cursor: null,
        total_items: 1,
      }),
    })
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Workers' }).click()
  await expect(page.getByRole('heading', { level: 1, name: 'Workers' })).toBeVisible()
  await page.getByRole('button', { name: 'Open archive' }).click()
  await expect(page.getByText('Broker-A')).toBeVisible()
  await expect(page.getByText('1,000')).toBeVisible()
  const openJson = page.getByRole('button', { name: 'View raw JSON for Broker-A 123456' })
  await expect(openJson).toHaveText('JSON')
  await expect(openJson).toHaveCSS('border-radius', '999px')
  await openJson.click()
  const rawSnapshotDialog = page.getByRole('dialog', { name: 'Raw snapshot JSON' })
  await expect(rawSnapshotDialog.getByText('"currency": "USD"')).toBeVisible()
  await expect.poll(() => rawSnapshotDialog.locator('.console-raw-detail').evaluate((element) => element.scrollHeight > element.clientHeight)).toBe(true)
  await page.getByRole('button', { name: 'Close raw snapshot JSON' }).click()
  await expect(page.getByRole('dialog', { name: 'Raw snapshot JSON' })).toHaveCount(0)
  await page.getByLabel('Search').fill('Broker-A')
  await expect.poll(() => page.evaluate(() => performance.getEntriesByType('resource')
    .some((entry) => entry.name.includes('q=Broker-A')))).toBe(true)
  await expect(page.getByText('Broker-A')).toBeVisible()
})

test('administrator can filter, paginate, and open analysis history from its row', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/product-catalog-analyses/analysis-1', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(buildAnalysis()) })
  })
  await page.route('**/api/admin/product-catalog-analyses?**', async (route) => {
    const query = new URL(route.request().url()).searchParams
    expect(query.get('limit')).toBe('20')
    const isNextPage = query.get('page') === '2'
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        items: [{
          analysis_id: isNextPage ? 'analysis-2' : 'analysis-1',
          requested_by: 'ABCDEF',
          first_worker: { worker_id: 'worker-a', login: 111111, server: 'Broker-A' },
          second_worker: { worker_id: 'worker-b', login: 222222, server: 'Broker-B' },
          policy_label: 'FX catalog v2',
          status: 'succeeded',
          current_stage: 'completed',
          retry_count: 0,
          requested_at: '2026-08-16T00:00:00Z',
          completed_at: '2026-08-16T00:08:00Z',
        }],
        next_cursor: isNextPage ? null : 'next-page',
        total_items: 21,
      }),
    })
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Analysis history' }).click()
  await expect(page.locator('h1')).toHaveCount(0)
  await page.getByLabel('Search').fill('20260819')
  await expect.poll(() => page.evaluate(() => performance.getEntriesByType('resource')
    .some((entry) => entry.name.includes('q=20260819')))).toBe(true)
  await expect(page.getByRole('link', { name: 'Open analysis analysis-1' })).toBeVisible()
  await page.getByLabel('Status').selectOption('succeeded')
  await expect(page.getByText('FX catalog v2')).toBeVisible()
  await page.getByRole('button', { name: 'Next' }).click()
  await expect(page.getByRole('link', { name: 'Open analysis analysis-2' })).toBeVisible()
  await page.getByRole('button', { name: 'Previous' }).click()
  await page.getByRole('link', { name: 'Open analysis analysis-1' }).click()
  await expect(page.locator('h1')).toHaveCount(0)
  await expect(page).toHaveURL(/\/analysis\/analysis-1$/)
  await expect(page.getByRole('heading', { name: 'Fleet health' })).not.toBeVisible()
})

test('administrator can browse current and retired product-pair lists', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/product-pairs?**', async (route) => {
    const query = new URL(route.request().url()).searchParams
    const retired = query.get('status') === 'retired'
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        items: [buildProductPair(retired ? { product_pair_id: 'pair-retired', status: 'retired', retired_reason: 'policy_replaced' } : {})],
        next_cursor: null,
        total_items: 1,
      }),
    })
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Current pairs' }).click()
  await expect(page.locator('h1')).toHaveCount(0)
  await expect(page.getByText('Broker-A · EURUSD.a / Broker-B · EURUSD')).toBeVisible()
  await page.getByLabel('Search').fill('EURUSD')
  await expect.poll(() => page.evaluate(() => performance.getEntriesByType('resource')
    .some((entry) => entry.name.includes('q=EURUSD')))).toBe(true)
  await page.getByRole('link', { name: 'Retired pairs' }).click()
  await expect(page.locator('h1')).toHaveCount(0)
  await expect(page.getByText('Policy Replaced')).toBeVisible()
})

test('administrator can issue, reveal once, and revoke an unused registration invite', async ({ page }) => {
  let invites = [{
    invite_id: 'invite-1',
    role: 'trader',
    issued_by: 'ABCDEF',
    issued_at: '2026-08-20T00:00:00Z',
    expires_at: '2026-08-20T01:00:00Z',
    status: 'active',
    used_at: null,
    revoked_at: null,
  }]
  let createHeaders: Record<string, string> | undefined
  let revokeHeaders: Record<string, string> | undefined

  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/registration-invites', async (route) => {
    if (route.request().method() === 'GET') {
      await route.fulfill({ contentType: 'application/json', body: JSON.stringify(invites) })
      return
    }
    createHeaders = route.request().headers()
    const created = {
      ...invites[0],
      invite: 'one-time-trader-invite',
    }
    await route.fulfill({ status: 201, contentType: 'application/json', body: JSON.stringify(created) })
  })
  await page.route('**/api/admin/registration-invites/invite-1/revoke', async (route) => {
    revokeHeaders = route.request().headers()
    invites = [{ ...invites[0], status: 'revoked', revoked_at: '2026-08-20T00:01:00Z' }]
    await route.fulfill({ status: 204 })
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Registration invites' }).click()

  await expect(page.getByRole('heading', { name: 'Registration invites' })).toBeVisible()
  await page.getByLabel('Role').selectOption('trader')
  await page.getByRole('button', { name: 'Issue invite' }).click()
  const disclosure = page.getByRole('dialog', { name: 'Registration invite issued' })
  await expect(disclosure.getByText('one-time-trader-invite')).toBeVisible()
  await page.context().grantPermissions(['clipboard-read', 'clipboard-write'])
  await disclosure.getByRole('button', { name: 'Copy invite code' }).click()
  await expect(disclosure.getByRole('status')).toContainText('Invite copied to clipboard.')
  await expect.poll(() => page.evaluate(() => navigator.clipboard.readText())).toBe('one-time-trader-invite')
  await disclosure.getByRole('button', { name: 'I have saved this invite' }).click()
  await expect(disclosure).toHaveCount(0)
  expect(createHeaders?.['x-csrf-token']).toBe('csrf-token')

  await page.getByRole('button', { name: 'Revoke invite for trader' }).click()
  expect(revokeHeaders?.['x-csrf-token']).toBe('csrf-token')
  await expect(page.getByText('revoked', { exact: true })).toBeVisible()
})

test('administrator can review Trader identities with CSRF-protected actions', async ({ page }) => {
  let pending = [{
    registration_id: 'trader-registration-1',
    strategy_name: 'mean-reversion',
    claimed_public_ip: '203.0.113.4',
    created_at: '2026-08-20T00:00:00Z',
    expires_at: '2026-08-20T00:15:00Z',
  }]
  let traders = [{
    trader_id: 'trader-1',
    strategy_name: 'spread-capture',
    status: 'active',
    approved_at: '2026-08-20T00:00:00Z',
    revoked_at: null,
  }]
  let actionHeaders: Record<string, string>[] = []

  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/traders/enrollments', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(pending) })
  })
  await page.route('**/api/admin/traders', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(traders) })
  })
  await page.route('**/api/admin/traders/enrollments/trader-registration-1/approve', async (route) => {
    actionHeaders.push(route.request().headers())
    pending = []
    await route.fulfill({ status: 200 })
  })
  await page.route('**/api/admin/traders/trader-1/revoke', async (route) => {
    actionHeaders.push(route.request().headers())
    traders = [{ ...traders[0], status: 'revoked', revoked_at: '2026-08-20T00:01:00Z' }]
    await route.fulfill({ status: 204 })
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Traders' }).click()
  await expect(page.getByText('mean-reversion')).toBeVisible()
  await page.getByRole('button', { name: 'Approve' }).click()
  await expect(page.getByText('No Trader registrations are awaiting review.')).toBeVisible()
  await page.getByRole('button', { name: 'Revoke certificate' }).click()
  await expect(page.getByText('revoked', { exact: true })).toBeVisible()
  expect(actionHeaders).toHaveLength(2)
  expect(actionHeaders.every((headers) => headers['x-csrf-token'] === 'csrf-token')).toBe(true)
})

test('administrator previews intent actions, reads immutable events, and sees command errors', async ({ page }) => {
  const zeroFillIntent = buildIntent({ intent_id: 'intent-zero', status: 'working', has_fill: false })
  const filledIntent = buildIntent({ intent_id: 'intent-filled', status: 'working', has_fill: true })
  let intents = [zeroFillIntent, filledIntent]
  const createBodies: Record<string, unknown>[] = []
  let cancelHeaders: Record<string, string> | undefined
  const intentQueries: string[] = []

  await mockLogin(page)
  await mockManagementData(page)
  await page.route('**/api/admin/intents?**', async (route) => {
    intentQueries.push(new URL(route.request().url()).search)
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(intents) })
  })
  await page.route('**/api/admin/product-pairs?status=active', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([{
      product_pair_id: 'pair-1',
      endpoints: [{ server: 'Broker-A', symbol: 'EURUSD.a' }, { server: 'Broker-B', symbol: 'EURUSD' }],
    }]) })
  })
  await page.route('**/api/admin/intents', async (route) => {
    createBodies.push(route.request().postDataJSON() as Record<string, unknown>)
    if (createBodies.length === 1) {
      await route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ detail: 'The controller timed out before replying.' }) })
      return
    }
    await route.fulfill({
      status: 201,
      contentType: 'application/json',
      body: JSON.stringify({
        status: 'rejected_preflight',
        reason: 'A broker rejected the intent order check.',
        preflight: [
          { worker_id: 'worker-a', server: 'Broker-A', status: 'accepted', order: { symbol: 'EURUSD', direction: 'LONG' }, response: { diagnostics: { retcode: 0 } } },
          { worker_id: 'worker-b', server: 'Broker-B', status: 'rejected', order: { symbol: 'EURUSD', direction: 'SHORT' }, response: { diagnostics: { retcode: 10015, comment: 'Invalid price', quote: { bid: 1.2344, ask: 1.2346 } } } },
        ],
      }),
    })
  })
  await page.route('**/api/admin/intents/intent-zero', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({
      ...zeroFillIntent,
      execution_records: [{ event_id: 1, event_type: 'intent_accepted', occurred_at: '2026-08-20T00:00:00Z', payload: { preflight: 'passed' } }],
    }) })
  })
  await page.route('**/api/admin/intents/intent-zero/cancel', async (route) => {
    cancelHeaders = route.request().headers()
    await route.fulfill({ status: 409, contentType: 'application/json', body: JSON.stringify({ detail: 'A fill was observed while cancelling.' }) })
  })
  await signIn(page)
  await page.getByRole('link', { name: 'Intents' }).click()
  await page.getByLabel('Active pair').selectOption('pair-1')
  await page.getByLabel('Lots').fill('0.1')
  await page.getByLabel('Entry price').fill('1.2345')
  await page.getByLabel('Stop loss (pips)').fill('10')
  await page.getByLabel('Take profit (pips)').fill('20')
  await page.getByLabel('Absolute expiry').fill('2026-08-20T01:00')
  await page.getByRole('button', { name: 'Preview intent' }).click()
  const createPreview = page.locator('[role="dialog"]').filter({ hasText: 'Confirm create' })
  await expect(createPreview).toContainText('"pair_id": "pair-1"')
  await createPreview.getByRole('button', { name: 'Confirm create' }).click()
  await expect(page.getByRole('alert')).toContainText('The controller timed out before replying.')
  await createPreview.getByRole('button', { name: 'Confirm create' }).click()
  expect(createBodies).toHaveLength(2)
  expect(createBodies[0]).toMatchObject({ type: 'intent', pair_id: 'pair-1', filling_mode: 'FOK' })
  expect(createBodies[0].command_id).toEqual(createBodies[1].command_id)
  await expect(page.getByRole('alert')).toContainText('Intent preflight rejected')
  await expect(page.getByRole('alert')).toContainText('Broker-B')
  await expect(page.getByRole('alert')).toContainText('Retcode 10015: Invalid price')
  await page.getByRole('button', { name: 'Dismiss preflight result' }).click()

  await page.getByRole('button', { name: 'Timeline' }).first().click()
  await expect(page.locator('[role="dialog"]').filter({ hasText: 'Immutable intent timeline' }).getByText('intent_accepted')).toBeVisible()
  await page.getByRole('button', { name: 'Close' }).click()
  await page.getByRole('button', { name: 'Preview cancellation' }).click()
  await page.locator('[role="dialog"]').filter({ hasText: 'Confirm cancel' }).getByRole('button', { name: 'Confirm cancel' }).click()
  await expect(page.getByRole('alert')).toContainText('A fill was observed while cancelling.')
  expect(cancelHeaders?.['x-csrf-token']).toBe('csrf-token')
  await page.locator('[role="dialog"]').filter({ hasText: 'Confirm cancel' }).getByRole('button', { name: 'Back' }).click()

  await expect(page.getByRole('button', { name: 'Preview emergency flatten' })).toHaveCount(0)
  await page.getByLabel('Show complete history').check()
  await expect.poll(() => intentQueries.some((query) => query.includes('active_only=false'))).toBe(true)
})

test('launch analysis is isolated from main-page operational summaries', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, { workers: [buildWorker({})] })

  await signIn(page)
  await page.getByRole('link', { name: 'Analysis', exact: true }).click()

  await expect(page.getByRole('heading', { name: 'Launch' }).first()).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Fleet health' })).not.toBeVisible()
  await expect(page.getByRole('heading', { name: 'Needs attention' })).not.toBeVisible()
})

test('administrator configures and observes the shared manual-trading target', async ({ page }) => {
  let target: object | null = null
  const workers = [
    buildWorker({
      worker_id: 'worker-a',
      login: 123456,
      server: 'Broker-A',
      live_state: {
        observed_at: '2026-08-22T00:00:00Z',
        received_at: '2026-08-22T00:00:00Z',
        connectivity: true,
        quotes: [{ symbol: 'EURUSD', bid: 1.1, ask: 1.1002, broker_time: '2026-08-22T00:00:00Z', controller_received_at: '2026-08-22T00:00:01Z' }],
        orders: [{ ticket: 101, symbol: 'EURUSD' }],
        positions: [],
      },
    }),
    buildWorker({
      worker_id: 'worker-b',
      login: 654321,
      server: 'Broker-B',
      live_state: {
        observed_at: '2026-08-22T00:00:00Z',
        received_at: '2026-08-22T00:00:00Z',
        connectivity: true,
        quotes: [{ symbol: 'EURUSD.a', bid: 1.0998, ask: 1.1, broker_time: '2026-08-22T00:00:00Z', controller_received_at: '2026-08-22T00:00:01Z' }],
        orders: [],
        positions: [{ ticket: 202, symbol: 'EURUSD.a' }],
      },
    }),
  ]
  await mockLogin(page)
  await mockManagementData(page, {
    workers,
    productPairs: [{
      product_pair_id: 'pair-1',
      status: 'active',
      endpoints: [{ server: 'Broker-A', symbol: 'EURUSD' }, { server: 'Broker-B', symbol: 'EURUSD.a' }],
      worker_applicability: [
        { worker_id: 'worker-a', server: 'Broker-A', applicability_status: 'applicable' },
        { worker_id: 'worker-b', server: 'Broker-B', applicability_status: 'applicable' },
      ],
    }],
  })
  await page.route('**/api/admin/session', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({ csrf_token: 'resumed-csrf-token' }) })
  })
  await page.route('**/api/admin/manual-trading-target', async (route) => {
    if (route.request().method() === 'PUT') {
      expect(route.request().headers()['x-csrf-token']).toBe('resumed-csrf-token')
      expect(route.request().postDataJSON()).toMatchObject({
        pair_id: 'pair-1', first_worker_id: 'worker-a', second_worker_id: 'worker-b',
        leg_order: 'buy_to_sell', interval_seconds: 7, expected_revision: 0,
      })
      target = {
        pair: {
          product_pair_id: 'pair-1',
          status: 'active',
          endpoints: [{ server: 'Broker-A', symbol: 'EURUSD' }, { server: 'Broker-B', symbol: 'EURUSD.a' }],
          worker_applicability: [],
        },
        workers: [
          { ...workers[0], endpoint: { server: 'Broker-A', symbol: 'EURUSD' } },
          { ...workers[1], endpoint: { server: 'Broker-B', symbol: 'EURUSD.a' } },
        ],
        leg_order: 'buy_to_sell',
        interval_seconds: 7,
        revision: 1,
        active_manual_trade_id: null,
      }
      await route.fulfill({ contentType: 'application/json', body: JSON.stringify(target) })
      return
    }
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(target) })
  })

  await page.goto('http://127.0.0.1:4173/manual-trading')
  await page.getByLabel('Active product pair').selectOption('pair-1')
  await page.getByLabel('Buy endpoint worker').selectOption('worker-a')
  await page.getByLabel('Sell endpoint worker').selectOption('worker-b')
  await page.getByLabel('Leg interval (seconds)').fill('7')
  await page.getByRole('button', { name: 'Save shared target' }).click()

  await expect(page.getByText('Shared target saved at revision 1.')).toBeVisible()
  await expect(page.getByRole('heading', { name: '123456 on Broker-A — EURUSD' })).toBeVisible()
  await expect(page.getByText('current target product pair')).toHaveCount(2)
  await expect(page.getByText('2026-08-22T00:00:01Z').first()).toBeVisible()
})

test('administrator previews and confirms a protected scheduled manual paired trade', async ({ page }) => {
  const workers = [
    buildWorker({ worker_id: 'worker-a', login: 123456, server: 'Broker-A', live_state: { observed_at: '2026-08-22T00:00:00Z', received_at: '2026-08-22T00:00:00Z', connectivity: true, quotes: [{ symbol: 'EURUSD', bid: 1.1, ask: 1.1002, broker_time: '2026-08-22T00:00:00Z' }], orders: [], positions: [] } }),
    buildWorker({ worker_id: 'worker-b', login: 654321, server: 'Broker-B', live_state: { observed_at: '2026-08-22T00:00:00Z', received_at: '2026-08-22T00:00:00Z', connectivity: true, quotes: [{ symbol: 'EURUSD.a', bid: 1.0998, ask: 1.1, broker_time: '2026-08-22T00:00:00Z' }], orders: [], positions: [] } }),
  ]
  const target = {
    pair: { product_pair_id: 'pair-1', status: 'active', endpoints: [{ server: 'Broker-A', symbol: 'EURUSD' }, { server: 'Broker-B', symbol: 'EURUSD.a' }], worker_applicability: [] },
    workers: [{ ...workers[0], endpoint: { server: 'Broker-A', symbol: 'EURUSD' } }, { ...workers[1], endpoint: { server: 'Broker-B', symbol: 'EURUSD.a' } }],
    leg_order: 'buy_to_sell', interval_seconds: 7, revision: 1, active_manual_trade_id: null,
  }
  await mockLogin(page)
  await page.route('**/api/admin/session', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({ csrf_token: 'csrf-token' }) })
  })
  await mockManagementData(page, { workers, productPairs: [target.pair] })
  await page.route('**/api/admin/manual-trading-target', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(target) })
  })
  await page.route('**/api/admin/manual-trades/preview', async (route) => {
    expect(route.request().headers()['x-csrf-token']).toBe('csrf-token')
    expect(route.request().postDataJSON()).toMatchObject({ target_revision: 1, buy_worker_id: 'worker-a', sell_worker_id: 'worker-b', base_lots: '0.1', stop_loss_pips: '10', take_profit_pips: '20' })
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({ pair_id: 'pair-1', target_revision: 1, leg_order: 'buy_to_sell', interval_seconds: 7, legs: [
      { worker_id: 'worker-a', symbol: 'EURUSD', direction: 'BUY', lots: '0.1', estimated_entry_price: '1.1002', estimated_stop_loss: '1.0992', estimated_take_profit: '1.1022' },
      { worker_id: 'worker-b', symbol: 'EURUSD.a', direction: 'SELL', lots: '0.2', estimated_entry_price: '1.0998', estimated_stop_loss: '1.1008', estimated_take_profit: '1.0978' },
    ] }) })
  })
  await page.route('**/api/admin/manual-trades', async (route) => {
    expect(route.request().headers()['x-csrf-token']).toBe('csrf-token')
    expect(route.request().postDataJSON()).toMatchObject({ target_revision: 1, buy_worker_id: 'worker-a', sell_worker_id: 'worker-b', base_lots: '0.1', stop_loss_pips: '10', take_profit_pips: '20' })
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({ manual_trade_id: 'manual-1', status: 'scheduled' }) })
  })

  await page.goto('http://127.0.0.1:4173/manual-trading')
  await page.getByLabel('Base lots').fill('0.1')
  await page.getByLabel('Stop loss (pips)').fill('10')
  await page.getByLabel('Take profit (pips)').fill('20')
  await page.getByRole('button', { name: 'Preview protected entry' }).click()
  await expect(page.getByRole('dialog')).toContainText('Confirm protected paired trade')
  await expect(page.getByRole('dialog')).toContainText('0.2')
  await page.getByRole('dialog').getByRole('button', { name: 'Confirm protected entry' }).click()
  await expect(page.getByText('Manual trade manual-1 was scheduled.')).toBeVisible()
})

async function signIn(page: Parameters<typeof test>[0]['page']) {
  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()
}

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
    pairedTradeLifecycle?: {
      availability: 'available' | 'unavailable'
      reason: string
    }
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
  await page.route('**/api/admin/operations-dashboard', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({
        paired_trade_lifecycle: overrides?.pairedTradeLifecycle ?? {
          availability: 'unavailable',
          reason: 'Paired-trade lifecycle records are not available in the control-plane ledger.',
        },
      }),
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

function buildIntent(overrides?: Record<string, unknown>) {
  return {
    intent_id: 'intent-1',
    origin: 'trader',
    originator: 'trader-1',
    pair_id: 'pair-1',
    status: 'accepted',
    accepted_at: '2026-08-20T00:00:00Z',
    has_fill: false,
    intent: {
      primary_direction: 'LONG',
      lots: '0.1',
      entry_price: '1.2345',
      stop_loss_pips: '10',
      take_profit_pips: '20',
      filling_mode: 'FOK',
      expires_at: '2026-08-20T01:00:00Z',
    },
    ...overrides,
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
