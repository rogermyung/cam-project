import { test } from 'node:test'
import assert from 'node:assert/strict'

import type { CfpbCompany, CfpbMonth } from '../src/types.ts'
import { filterCompanies, formatChange, sparkPoints, sortCompanies } from '../src/lib/cfpb.ts'
import type { SortDir } from '../src/lib/notices.ts'

function makeCompany(over: Partial<CfpbCompany>): CfpbCompany {
  return {
    company: 'EXAMPLE COMPANY',
    total: 0,
    latest: 0,
    baseline: 1,
    ratio: null,
    spike: false,
    top_product: 'EXAMPLE PRODUCT',
    months: [],
    products: [],
    ...over,
  }
}

const COMPANIES: CfpbCompany[] = [
  makeCompany({ company: 'BETA BANK', total: 300, latest: 70, baseline: 50, ratio: 1.4, spike: false, top_product: 'CARD' }),
  makeCompany({ company: 'alpha credit', total: 200, latest: 90, baseline: 60, ratio: 1.5, spike: true, top_product: 'LOAN' }),
  makeCompany({ company: 'Gamma Finance', total: 250, latest: 80, baseline: 0, ratio: null, spike: false, top_product: 'DEBT' }),
  makeCompany({ company: 'DELTA SERVICES', total: 400, latest: 70, baseline: 50, ratio: 1.4, spike: true, top_product: 'MORTGAGE' }),
]

test('filterCompanies with empty criteria returns everything in the same order', () => {
  assert.deepEqual(filterCompanies(COMPANIES, {}), COMPANIES)
})

test('filterCompanies trims query and matches company name case-insensitively as substring', () => {
  assert.deepEqual(
    filterCompanies(COMPANIES, { query: '  alpha  ' }).map((c) => c.company),
    ['alpha credit'],
  )

  assert.deepEqual(
    filterCompanies(COMPANIES, { query: 'bank' }).map((c) => c.company),
    ['BETA BANK'],
  )
})

test('filterCompanies combines query and spikesOnly', () => {
  assert.deepEqual(
    filterCompanies(COMPANIES, { query: 'delta', spikesOnly: true }).map((c) => c.company),
    ['DELTA SERVICES'],
  )
})

test('sortCompanies by company is case-insensitive and stable on ties', () => {
  const input = [
    makeCompany({ company: 'zeta', total: 1, latest: 1, baseline: 1, ratio: 1 }),
    makeCompany({ company: 'ALPHA', total: 2, latest: 2, baseline: 1, ratio: 1 }),
    makeCompany({ company: 'alpha', total: 3, latest: 3, baseline: 1, ratio: 1 }),
  ]

  const sorted = sortCompanies(input, 'company', 'asc')
  assert.deepEqual(sorted.map((c) => c.company), ['ALPHA', 'alpha', 'zeta'])
})

test('sortCompanies keeps input order within equal values in both directions (stable)', () => {
  const input = [
    makeCompany({ company: 'A', latest: 5, baseline: 1, ratio: 2, spike: false }),
    makeCompany({ company: 'B', latest: 5, baseline: 1, ratio: 2, spike: true }),
    makeCompany({ company: 'C', latest: 5, baseline: 1, ratio: 2, spike: false }),
  ]

  for (const dir of ['asc', 'desc'] as SortDir[]) {
    const sorted = sortCompanies(input, 'latest', dir)
    assert.deepEqual(sorted.map((c) => c.company), ['A', 'B', 'C'])
  }
})

test('sortCompanies by ratio sorts null LAST for both directions', () => {
  const input = [
    makeCompany({ company: 'A', ratio: null, latest: 1, baseline: 0, spike: false }),
    makeCompany({ company: 'B', ratio: 0.5, latest: 1, baseline: 2, spike: false }),
    makeCompany({ company: 'C', ratio: 2.0, latest: 4, baseline: 2, spike: false }),
  ]

  assert.deepEqual(sortCompanies(input, 'ratio', 'asc').map((c) => c.company), ['B', 'C', 'A'])
  assert.deepEqual(sortCompanies(input, 'ratio', 'desc').map((c) => c.company), ['C', 'B', 'A'])
})

test('formatChange shows an em dash for null ratio', () => {
  assert.equal(formatChange(null), '—')
})

test('formatChange formats rounded percentage deltas with the correct sign', () => {
  assert.equal(formatChange(1.6), '+60%')
  assert.equal(formatChange(0.9), '-10%')
  assert.equal(formatChange(1), '0%')
  assert.equal(formatChange(0), '-100%')
})

test('sparkPoints returns empty string for an empty series', () => {
  const months: CfpbMonth[] = []
  assert.equal(sparkPoints(months, 100, 20), '')
})

test('sparkPoints maps complaints to an SVG polyline with evenly spaced x coordinates', () => {
  const months: CfpbMonth[] = [
    { month: '2024-11', complaints: 0 },
    { month: '2024-12', complaints: 5 },
    { month: '2025-01', complaints: 10 },
  ]
  assert.equal(sparkPoints(months, 100, 20), '0,20 50,10 100,0')
})

test('sparkPoints uses x=0 for a single point', () => {
  const months: CfpbMonth[] = [{ month: '2024-11', complaints: 7 }]
  assert.equal(sparkPoints(months, 100, 20), '0,0')
})

test('sparkPoints rounds to 1 decimal and omits .0 for integer outputs', () => {
  const months: CfpbMonth[] = [
    { month: '2024-11', complaints: 1 },
    { month: '2024-12', complaints: 7 },
  ]

  assert.equal(sparkPoints(months, 100, 20), '0,17.1 100,0')
})
