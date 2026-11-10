// Authoritative tests for src/lib/notices.ts — the pure logic behind both pages.
// Run from frontend/:  node --test tests/
import { test } from 'node:test'
import assert from 'node:assert/strict'

import type { Meta, MonthTotal, Notice } from '../src/types.ts'
import {
  filterNotices,
  formatNumber,
  monthRange,
  paginate,
  sortNotices,
  stateFreshness,
} from '../src/lib/notices.ts'

function n(over: Partial<Notice>): Notice {
  return {
    key: `${over.state ?? 'TX'}::${(over.employer ?? 'acme').toLowerCase()}::${over.notice_date ?? 'x'}`,
    state: 'TX',
    employer: 'Acme Corp',
    notice_date: '2026-01-02',
    employees: 100,
    city: 'Austin',
    county: 'Travis',
    notice_type: 'Layoff',
    source_url: 'https://example.test/tx',
    first_seen: '2026-11-09',
    ...over,
  }
}

const NOTICES: Notice[] = [
  n({ state: 'TX', employer: 'Acme Corp', notice_date: '2026-01-02', employees: 100, city: 'Austin' }),
  n({ state: 'CA', employer: 'Beta LLC', notice_date: '2026-03-01', employees: 1000, city: 'Oakland', county: 'Alameda County' }),
  n({ state: 'CA', employer: 'alpha inc', notice_date: null, employees: 5, city: 'Fresno' }),
  n({ state: 'NY', employer: 'Gamma Co', notice_date: '2026-02-10', employees: null, city: 'Brooklyn', county: 'Kings' }),
  n({ state: 'TX', employer: 'Delta Co', notice_date: '2025-12-31', employees: 50, city: 'Waco' }),
]

// --- filterNotices -------------------------------------------------------------

test('filterNotices with no criteria returns everything in the same order', () => {
  assert.deepEqual(filterNotices(NOTICES, {}), NOTICES)
  assert.deepEqual(filterNotices(NOTICES, { state: '', query: '  ' }), NOTICES)
})

test('filterNotices by state', () => {
  assert.deepEqual(
    filterNotices(NOTICES, { state: 'CA' }).map((x) => x.employer),
    ['Beta LLC', 'alpha inc'],
  )
})

test('filterNotices query matches employer, city or county, ignoring case and outer spaces', () => {
  assert.deepEqual(filterNotices(NOTICES, { query: ' ACME ' }).map((x) => x.employer), ['Acme Corp'])
  assert.deepEqual(filterNotices(NOTICES, { query: 'brook' }).map((x) => x.employer), ['Gamma Co'])
  assert.deepEqual(filterNotices(NOTICES, { query: 'alameda' }).map((x) => x.employer), ['Beta LLC'])
  assert.deepEqual(filterNotices(NOTICES, { query: 'zzz' }), [])
})

test('filterNotices combines state and query', () => {
  assert.deepEqual(
    filterNotices(NOTICES, { state: 'TX', query: 'co' }).map((x) => x.employer),
    ['Acme Corp', 'Delta Co'],
  )
})

test('filterNotices does not mutate its input', () => {
  const copy = [...NOTICES]
  filterNotices(NOTICES, { state: 'TX' })
  assert.deepEqual(NOTICES, copy)
})

// --- sortNotices ---------------------------------------------------------------

test('sortNotices by date descending puts undated notices last', () => {
  assert.deepEqual(
    sortNotices(NOTICES, 'notice_date', 'desc').map((x) => x.notice_date),
    ['2026-03-01', '2026-02-10', '2026-01-02', '2025-12-31', null],
  )
})

test('sortNotices by date ascending still puts undated notices last', () => {
  assert.deepEqual(
    sortNotices(NOTICES, 'notice_date', 'asc').map((x) => x.notice_date),
    ['2025-12-31', '2026-01-02', '2026-02-10', '2026-03-01', null],
  )
})

test('sortNotices by employees puts unknown counts last in both directions', () => {
  assert.deepEqual(
    sortNotices(NOTICES, 'employees', 'desc').map((x) => x.employees),
    [1000, 100, 50, 5, null],
  )
  assert.deepEqual(
    sortNotices(NOTICES, 'employees', 'asc').map((x) => x.employees),
    [5, 50, 100, 1000, null],
  )
})

test('sortNotices by employer is case-insensitive', () => {
  assert.deepEqual(
    sortNotices(NOTICES, 'employer', 'asc').map((x) => x.employer),
    ['Acme Corp', 'alpha inc', 'Beta LLC', 'Delta Co', 'Gamma Co'],
  )
})

test('sortNotices by state keeps the original order within a state', () => {
  assert.deepEqual(
    sortNotices(NOTICES, 'state', 'asc').map((x) => `${x.state}:${x.employer}`),
    ['CA:Beta LLC', 'CA:alpha inc', 'NY:Gamma Co', 'TX:Acme Corp', 'TX:Delta Co'],
  )
})

test('sortNotices returns a new array and leaves the input untouched', () => {
  const copy = [...NOTICES]
  const sorted = sortNotices(NOTICES, 'employees', 'desc')
  assert.notEqual(sorted, NOTICES)
  assert.deepEqual(NOTICES, copy)
})

// --- paginate ------------------------------------------------------------------

test('paginate returns the requested page with totals', () => {
  const items = Array.from({ length: 120 }, (_, i) => i)
  const p = paginate(items, 2, 50)
  assert.deepEqual(p.items, items.slice(50, 100))
  assert.equal(p.page, 2)
  assert.equal(p.pageCount, 3)
  assert.equal(p.total, 120)
})

test('paginate last page may be short', () => {
  const p = paginate(Array.from({ length: 120 }, (_, i) => i), 3, 50)
  assert.equal(p.items.length, 20)
})

test('paginate clamps out-of-range pages', () => {
  const items = [1, 2, 3]
  assert.equal(paginate(items, 0, 2).page, 1)
  assert.equal(paginate(items, -5, 2).page, 1)
  assert.equal(paginate(items, 99, 2).page, 2)
  assert.deepEqual(paginate(items, 99, 2).items, [3])
})

test('paginate of an empty list is one empty page', () => {
  assert.deepEqual(paginate([], 1, 50), { items: [], page: 1, pageCount: 1, total: 0 })
})

// --- monthRange ----------------------------------------------------------------

const BY_MONTH: MonthTotal[] = [
  { month: '2025-11', notices: 2, employees: 20 },
  { month: '2026-01', notices: 5, employees: 500 },
  { month: '2026-02', notices: 1, employees: 10 },
  { month: '2027-03', notices: 9, employees: 900 },
]

test('monthRange returns consecutive months ending at endMonth, filling gaps with zeros', () => {
  assert.deepEqual(monthRange(BY_MONTH, 4, '2026-02'), [
    { month: '2025-11', notices: 2, employees: 20 },
    { month: '2025-12', notices: 0, employees: 0 },
    { month: '2026-01', notices: 5, employees: 500 },
    { month: '2026-02', notices: 1, employees: 10 },
  ])
})

test('monthRange crosses year boundaries and ignores months after endMonth', () => {
  const r = monthRange(BY_MONTH, 14, '2026-03')
  assert.equal(r.length, 14)
  assert.equal(r[0].month, '2025-02')
  assert.equal(r[13].month, '2026-03')
  assert.deepEqual(r[13], { month: '2026-03', notices: 0, employees: 0 })
  assert.ok(!r.some((m) => m.month === '2027-03'))
})

test('monthRange with empty input still returns zero-filled months', () => {
  assert.deepEqual(monthRange([], 2, '2026-01'), [
    { month: '2025-12', notices: 0, employees: 0 },
    { month: '2026-01', notices: 0, employees: 0 },
  ])
})

// --- stateFreshness ------------------------------------------------------------

const META: Meta = {
  generated_at: '2026-11-09T12:00:00+00:00',
  total_notices: 10,
  total_employees: 100,
  latest_notice_date: '2026-11-01',
  states: {
    TX: { ok: true, fetched: 8, added: 0, error: null, last_attempt: '2026-11-09', last_success: '2026-11-09', degraded: false },
    CA: { ok: true, fetched: 2, added: 1, error: null, last_attempt: '2026-11-09', last_success: '2026-11-09', degraded: true },
    NY: { ok: false, fetched: 5, added: 0, error: 'HTTP 503', last_attempt: '2026-11-09', last_success: '2026-11-02', degraded: false },
  },
}

test('stateFreshness lists states alphabetically with a single status word', () => {
  assert.deepEqual(stateFreshness(META), [
    { state: 'CA', status: 'degraded', lastSuccess: '2026-11-09', fetched: 2, error: null },
    { state: 'NY', status: 'failed', lastSuccess: '2026-11-02', fetched: 5, error: 'HTTP 503' },
    { state: 'TX', status: 'ok', lastSuccess: '2026-11-09', fetched: 8, error: null },
  ])
})

test('stateFreshness tolerates a meta file with no states', () => {
  assert.deepEqual(stateFreshness({ ...META, states: {} }), [])
})

// --- formatNumber --------------------------------------------------------------

test('formatNumber adds thousands separators and shows a dash for missing values', () => {
  assert.equal(formatNumber(1234567), '1,234,567')
  assert.equal(formatNumber(0), '0')
  assert.equal(formatNumber(null), '—')
  assert.equal(formatNumber(undefined), '—')
})
