import { expect, test } from '@playwright/test'

test('administrator can reach the management login', async ({ page }) => {
  await page.goto('http://127.0.0.1:4173/')

  await expect(page.getByRole('heading', { name: 'Management access' })).toBeVisible()
  await expect(page.getByLabel('Administrator account')).toBeVisible()
  await expect(page.getByLabel('Password')).toBeVisible()
})

test('administrator can sign in and view audit events', async ({ page }) => {
  await page.route('**/api/admin/login', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ csrf_token: 'csrf-token' }),
    })
  })
  await page.route('**/api/admin/events', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify([
        { event_id: 1, event_type: 'admin_login_succeeded', payload: {}, occurred_at: '2026-08-15T00:00:00Z' },
      ]),
    })
  })
  await page.route('**/api/admin/enrollments', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify([]),
    })
  })
  await page.route('**/api/admin/workers', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/alerts', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
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

  await page.setViewportSize({ width: 900, height: 800 })
  await page.route('**/api/admin/login', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ csrf_token: 'csrf-token' }),
    })
  })
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
        ? [{
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
          }]
        : []),
    })
  })
  await page.route('**/api/admin/workers', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/alerts', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByRole('heading', { name: 'Pending worker registrations' })).toBeVisible()
  await expect(page.getByText('12345678')).toBeVisible()
  await expect(page.getByText('Broker-Demo')).toBeVisible()
  await expect(page.getByText('87654321')).toBeVisible()
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
  await page.getByRole('button', { name: 'Approve registration for 12345678 on Broker-Demo' }).click()

  await expect(page.getByText('No worker registrations are awaiting review.')).toBeVisible()
  await expect(page.getByText('worker_enrollment_approved')).toBeVisible()
  expect(enrollmentRequests).toBe(2)
})

test('administrator can view connected worker health and reconciliation', async ({ page }) => {
  await page.route('**/api/admin/login', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify({ csrf_token: 'csrf-token' }) })
  })
  await page.route('**/api/admin/events', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/enrollments', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify([]) })
  })
  await page.route('**/api/admin/workers', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify([{
        worker_id: 'worker-1',
        login: 123456,
        server: 'Broker-Demo',
        connectivity: 'connected',
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
      }]),
    })
  })
  await page.route('**/api/admin/alerts', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify([{
        alert_id: 1,
        worker_id: 'worker-1',
        priority: 'high',
        alert_type: 'external_broker_change',
        reason: 'unattributed_broker_change',
        occurred_at: '2026-08-16T00:01:00Z',
      }]),
    })
  })

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByRole('heading', { name: 'Account workers' })).toBeVisible()
  await expect(page.getByText('connected', { exact: true })).toBeVisible()
  await expect(page.getByText('volume_changed')).toBeVisible()
  await expect(page.getByText('"balance": 1000')).toBeVisible()
  await expect(page.getByText('high: external_broker_change')).toBeVisible()
})
