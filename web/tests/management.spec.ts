import { expect, test } from '@playwright/test'
import type { Page } from '@playwright/test'

test('administrator can reach the control-plane login', async ({ page }) => {
  await mockLoggedOut(page)
  await page.goto('/')

  await expect(page.getByRole('heading', { name: 'ABT control plane' })).toBeVisible()
  await expect(page.getByLabel('Administrator account')).toBeVisible()
  await expect(page.getByLabel('Password')).toBeVisible()
})

test('management navigation exposes only control-plane responsibilities', async ({ page }) => {
  await mockLoggedOut(page)
  await mockManagementData(page)
  await signIn(page)

  const navigation = page.getByRole('navigation', { name: 'Console sections' })
  await expect(navigation.getByRole('link')).toHaveText([
    'Overview',
    'Workers',
    'Registration invites',
    'Traders',
    'Audit events',
  ])
  await expect(page.getByText('Trading lifecycle is not operated here.')).toBeVisible()
  await expect(page.getByRole('link', { name: /analysis|product pair|intent/i })).toHaveCount(0)
})

test('removed trading routes render no trading workspace or trading request', async ({ page }) => {
  const requestedAdminPaths: string[] = []
  page.on('request', (request) => {
    const url = new URL(request.url())
    if (url.pathname.startsWith('/api/admin/')) requestedAdminPaths.push(url.pathname)
  })
  await mockSession(page)
  await mockManagementData(page)

  for (const route of ['/analysis', '/analysis/history', '/product-pairs', '/intents']) {
    await page.goto(route)
    await expect(page.getByRole('heading', { name: 'Control-plane overview' })).toBeVisible()
  }

  expect(requestedAdminPaths).not.toContain('/api/admin/product-pairs')
  expect(requestedAdminPaths.some((path) => path.includes('product-catalog-analyses'))).toBe(false)
  expect(requestedAdminPaths.some((path) => path.includes('intents'))).toBe(false)
})

test('Worker administration does not expose trading state', async ({ page }) => {
  await mockLoggedOut(page)
  await mockManagementData(page, {
    workers: [{
      worker_id: 'worker-1',
      login: 123456,
      server: 'Broker-Demo',
      connectivity: 'connected',
      last_seen_at: new Date().toISOString(),
      opaque_state: { positions: [{ ticket: 'POSITION-MUST-STAY-OPAQUE' }] },
    }],
  })
  await signIn(page)
  await page.getByRole('link', { name: 'Workers' }).click()

  const worker = page.getByRole('listitem').filter({ hasText: '123456 on Broker-Demo' })
  await expect(worker.getByText('Healthy')).toBeVisible()
  await expect(page.getByText(/MUST-STAY-OPAQUE/)).toHaveCount(0)
})

test('administrator can approve a Worker registration', async ({ page }) => {
  let approved = false
  await mockLoggedOut(page)
  await mockManagementData(page, {
    enrollments: [buildEnrollment()],
  })
  await page.route('**/api/admin/enrollments/enrollment-1/approve', async (route) => {
    approved = true
    await route.fulfill({ status: 200, contentType: 'application/json', body: '{}' })
  })
  await page.route('**/api/admin/enrollments', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify(approved ? [] : [buildEnrollment()]),
    })
  })
  await signIn(page)
  await page.getByRole('link', { name: 'Workers' }).click()

  await page.getByRole('button', { name: 'Approve registration for 12345678 on Broker-Demo' }).click()
  await expect(page.getByRole('heading', { name: 'Pending registrations' })).toHaveCount(0)
})

test('administrator can manage Strategy Runtime identities', async ({ page }) => {
  await mockLoggedOut(page)
  await mockManagementData(page)
  await page.route('**/api/admin/traders/enrollments', async (route) => {
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify([{
        registration_id: 'registration-1',
        strategy_name: 'realtime-arbitrage',
        claimed_public_ip: '203.0.113.10',
        created_at: '2026-08-28T00:00:00Z',
        expires_at: '2026-08-28T00:15:00Z',
      }]),
    })
  })
  await page.route('**/api/admin/traders', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: '[]' })
  })
  await signIn(page)
  await page.getByRole('link', { name: 'Traders' }).click()

  await expect(page.getByRole('heading', { name: 'Pending enrollment review' })).toBeVisible()
  await expect(page.getByText('realtime-arbitrage')).toBeVisible()
})

test('administrator can view opaque audit events', async ({ page }) => {
  await mockLoggedOut(page)
  await mockManagementData(page, {
    events: [{
      event_id: 1,
      event_type: 'relay_delivery_acknowledged',
      payload: { opaque_envelope_id: 'envelope-1' },
      occurred_at: '2026-08-28T00:00:00Z',
    }],
  })
  await signIn(page)
  await page.getByRole('link', { name: 'Audit events' }).click()

  const table = page.getByRole('table', { name: 'Audit events' })
  await expect(table).toBeVisible()
  await expect(table.getByText('Relay Delivery Acknowledged').first()).toBeVisible()
})

test('administrator can sign out', async ({ page }) => {
  await mockLoggedOut(page)
  await mockManagementData(page)
  await page.route('**/api/admin/logout', async (route) => {
    expect(route.request().headers()['x-csrf-token']).toBe('csrf-token')
    await route.fulfill({ status: 204 })
  })
  await signIn(page)

  await page.getByRole('button', { name: 'Sign out' }).click()
  await expect(page.getByRole('heading', { name: 'ABT control plane' })).toBeVisible()
})

async function signIn(page: Page) {
  await page.goto('/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('heading', { name: 'Control-plane overview' })).toBeVisible()
}

async function mockLoggedOut(page: Page) {
  await page.route('**/api/admin/session', async (route) => {
    await route.fulfill({ status: 401, contentType: 'application/json', body: '{"detail":"login required"}' })
  })
  await page.route('**/api/admin/login', async (route) => {
    expect(route.request().method()).toBe('POST')
    await route.fulfill({ contentType: 'application/json', body: '{"csrf_token":"csrf-token"}' })
  })
}

async function mockSession(page: Page) {
  await page.route('**/api/admin/session', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: '{"csrf_token":"csrf-token"}' })
  })
}

async function mockManagementData(
  page: Page,
  overrides: { events?: object[]; enrollments?: object[]; workers?: object[]; alerts?: object[] } = {},
) {
  await page.route('**/api/admin/enrollments', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(overrides.enrollments ?? []) })
  })
  await page.route('**/api/admin/workers', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(overrides.workers ?? []) })
  })
  await page.route('**/api/admin/alerts', async (route) => {
    await route.fulfill({ contentType: 'application/json', body: JSON.stringify(overrides.alerts ?? []) })
  })
  await page.route('**/api/admin/events?**', async (route) => {
    const events = overrides.events ?? []
    await route.fulfill({
      contentType: 'application/json',
      body: JSON.stringify({ items: events, next_cursor: null, total_items: events.length }),
    })
  })
}

function buildEnrollment() {
  return {
    enrollment_id: 'enrollment-1',
    login: 12345678,
    server: 'Broker-Demo',
    created_at: '2026-08-28T00:00:00Z',
    expires_at: '2026-08-28T00:15:00Z',
    account_info: { currency: 'USD' },
    terminal_info: { platform: 'MetaTrader 5' },
  }
}
