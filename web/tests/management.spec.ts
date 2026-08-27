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
  await page.getByRole('link', { name: 'Audit events' }).click()

  const auditEvents = page.getByRole('table', { name: 'Audit events' })
  await expect(auditEvents).toBeVisible()
  await expect(auditEvents.getByRole('row').filter({ hasText: 'Admin Login Succeeded' })).toBeVisible()
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
      latest_snapshot: {
        cursor: 1,
        observed_at: new Date().toISOString(),
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
  await expect(workerCard.getByText('Healthy', { exact: true })).toBeVisible()
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
  await page.getByRole('button', { name: 'Confirm emergency revocation' }).click()

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
    buildWorker({ worker_id: 'disconnected', login: 100006, server: 'Broker-F', connectivity: 'disconnected', latest_snapshot: currentSnapshot }),
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

test('administrator can inspect an automatic recovery lifecycle', async ({ page }) => {
  await mockLogin(page)
  await mockManagementData(page, {
    workers: [buildWorker({
      worker_id: 'worker-recovering',
      login: 100008,
      server: 'Broker-H',
      recovery: {
        lifecycle_state: 'CONVERGING_EMPTY',
        desired_state: 'EMPTY',
        revision: 2,
        incident_id: 'incident-1',
        directive: {
          kind: 'CLOSE_POSITIONS',
          reason: 'The desired account state is empty.',
          revision: 2,
          tickets: ['51'],
        },
        updated_at: '2026-08-22T09:00:00Z',
      },
    })],
  })

  await signIn(page)
  await page.getByRole('link', { name: 'Workers' }).click()

  const workerCard = page.getByLabel('Workers by account').getByRole('listitem')
  await expect(workerCard.getByText('Recovering', { exact: true })).toBeVisible()
  await workerCard.getByRole('button', { name: /100008 on Broker-H/ }).click()
  await expect(workerCard.getByRole('heading', { name: 'Recovery lifecycle' })).toBeVisible()
  await expect(workerCard.getByText('Converging Empty toward Empty.')).toBeVisible()
  await expect(workerCard.getByText('The desired account state is empty.')).toBeVisible()
  await expect(workerCard.getByText('Current directive: Close Positions.')).toBeVisible()
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
  await page.route('**/api/admin/session', async (route) => {
    await route.fulfill({
      status: 401,
      contentType: 'application/json',
      body: JSON.stringify({ detail: 'Administrator login is required.' }),
    })
  })
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
  await page.route('**/api/admin/events**', async (route) => {
    const requestUrl = new URL(route.request().url())
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(requestUrl.search
        ? {
            items: overrides?.events ?? [],
            next_cursor: null,
            total_items: (overrides?.events ?? []).length,
          }
        : overrides?.events ?? []),
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
