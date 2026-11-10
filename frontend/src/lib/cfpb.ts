import type { CfpbCompany, CfpbMonth } from '@/types'
import type { SortDir } from '@/lib/notices'

export type CfpbSortKey = 'company' | 'total' | 'latest' | 'ratio'

export function filterCompanies(
  companies: CfpbCompany[],
  criteria: { query?: string; spikesOnly?: boolean },
): CfpbCompany[] {
  const query = criteria.query?.trim() ?? ''
  const spikesOnly = criteria.spikesOnly === true

  if (query === '' && !spikesOnly) return companies

  const q = query.toLowerCase()
  return companies.filter((c) => {
    if (spikesOnly && !c.spike) return false
    if (q === '') return true
    return c.company.toLowerCase().includes(q)
  })
}

export function sortCompanies(companies: CfpbCompany[], key: CfpbSortKey, dir: SortDir): CfpbCompany[] {
  const withIdx = companies.map((c, idx) => ({ c, idx }))

  withIdx.sort((a, b) => {
    const sign = dir === 'asc' ? 1 : -1

    if (key === 'company') {
      const ak = a.c.company.toLowerCase()
      const bk = b.c.company.toLowerCase()
      if (ak < bk) return -1 * sign
      if (ak > bk) return 1 * sign
      return a.idx - b.idx
    }

    if (key === 'total') {
      const av = a.c.total
      const bv = b.c.total
      if (av < bv) return -1 * sign
      if (av > bv) return 1 * sign
      return a.idx - b.idx
    }

    if (key === 'latest') {
      const av = a.c.latest
      const bv = b.c.latest
      if (av < bv) return -1 * sign
      if (av > bv) return 1 * sign
      return a.idx - b.idx
    }

    // ratio
    const ar = a.c.ratio
    const br = b.c.ratio

    if (ar == null && br == null) return a.idx - b.idx
    if (ar == null) return 1
    if (br == null) return -1
    if (ar < br) return -1 * sign
    if (ar > br) return 1 * sign
    return a.idx - b.idx
  })

  return withIdx.map(({ c }) => c)
}

export function formatChange(ratio: number | null): string {
  if (ratio == null) return '—'
  const pct = (ratio - 1) * 100
  const rounded = Math.round(pct)
  if (rounded === 0) return '0%'
  if (rounded > 0) return `+${rounded}%`
  return `${rounded}%`
}

function fmt1(n: number): string {
  const r = Math.round(n * 10) / 10
  const fixed = r.toFixed(1).replace(/\.0$/, '')
  if (fixed === '-0') return '0'
  return fixed
}

export function sparkPoints(months: CfpbMonth[], width: number, height: number): string {
  if (months.length === 0) return ''

  const max = Math.max(...months.map((m) => m.complaints))
  const n = months.length
  const denom = n === 1 ? 1 : n - 1

  const pts: string[] = []
  for (let i = 0; i < n; i++) {
    const x = n === 1 ? 0 : (width * i) / denom
    const y = max === 0 ? height : height - (months[i].complaints / max) * height
    pts.push(`${fmt1(x)},${fmt1(y)}`)
  }

  return pts.join(' ')
}
