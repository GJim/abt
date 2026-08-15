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

  await page.goto('http://127.0.0.1:4173/')
  await page.getByLabel('Administrator account').fill('ABCDEF')
  await page.getByLabel('Password').fill('A-secure-admin-password!')
  await page.getByRole('button', { name: 'Sign in' }).click()

  await expect(page.getByRole('heading', { name: 'Audit events' })).toBeVisible()
  await expect(page.getByText('admin_login_succeeded')).toBeVisible()
})
