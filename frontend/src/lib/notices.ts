import type { Meta, MonthTotal, Notice } from '@/types'

export type SortKey = 'notice_date' | 'employees' | 'employer' | 'state'
export type SortDir = 'asc' | 'desc'

export function filterNotices(
  notices: Notice[],
  criteria: { state?: string; query?: string },
): Notice[] {
  const state = criteria.state
  const rawQuery = criteria.query

  const query = rawQuery?.trim().toLowerCase() ?? ''
  const stateAll = state == null || state === ''

  return notices.filter((n) => {
    if (!stateAll && n.state !== state) return false
    if (query === '') return true
    const hay = `${n.employer} ${n.city} ${n.county}`.toLowerCase()
    return hay.includes(query)
  })
}

export function sortNotices(
  notices: Notice[],
  key: SortKey,
  dir: SortDir,
): Notice[] {
  const withIdx = notices.map((n, i) => ({ n, idx: i }))

  const sign = dir === 'asc' ? 1 : -1

  withIdx.sort((a, b) => {
    // Put nulls last for both directions.
    if (key === 'notice_date') {
      const ad = a.n.notice_date
      const bd = b.n.notice_date
      if (ad == null && bd == null) return a.idx - b.idx
      if (ad == null) return 1
      if (bd == null) return -1
      if (ad < bd) return -1 * sign
      if (ad > bd) return 1 * sign
      return a.idx - b.idx
    }

    if (key === 'employees') {
      const ae = a.n.employees
      const be = b.n.employees
      if (ae == null && be == null) return a.idx - b.idx
      if (ae == null) return 1
      if (be == null) return -1
      if (ae < be) return -1 * sign
      if (ae > be) return 1 * sign
      return a.idx - b.idx
    }

    if (key === 'employer') {
      const aKey = a.n.employer.toLowerCase()
      const bKey = b.n.employer.toLowerCase()
      if (aKey < bKey) return -1 * sign
      if (aKey > bKey) return 1 * sign
      return a.idx - b.idx
    }

    if (key === 'state') {
      if (a.n.state < b.n.state) return -1 * sign
      if (a.n.state > b.n.state) return 1 * sign
      return a.idx - b.idx
    }

    return a.idx - b.idx
  })

  return withIdx.map(({ n }) => n)
}

export function paginate<T>(
  items: T[],
  page: number,
  pageSize: number,
): { items: T[]; page: number; pageCount: number; total: number } {
  const total = items.length
  const pageCount = Math.max(1, Math.ceil(total / pageSize))
  const clampedPage = Math.min(Math.max(page, 1), pageCount)
  const start = (clampedPage - 1) * pageSize
  const end = start + pageSize

  return {
    items: items.slice(start, end),
    page: clampedPage,
    pageCount,
    total,
  }
}

function monthToIndex(month: string): number {
  const [y, m] = month.split('-')
  const year = Number(y)
  const month1 = Number(m)
  return year * 12 + (month1 - 1)
}

function indexToMonth(index: number): string {
  const year = Math.floor(index / 12)
  const month1 = (index % 12) + 1
  return `${year}-${String(month1).padStart(2, '0')}`
}

export function monthRange(
  byMonth: MonthTotal[],
  count: number,
  endMonth: string,
): MonthTotal[] {
  if (count <= 0) return []

  const endIdx = monthToIndex(endMonth)
  const startIdx = endIdx - (count - 1)

  const map = new Map<string, MonthTotal>()
  for (const m of byMonth) map.set(m.month, m)

  const out: MonthTotal[] = []
  for (let i = 0; i < count; i++) {
    const idx = startIdx + i
    const month = indexToMonth(idx)
    const found = map.get(month)
    if (found) {
      out.push({ month, notices: found.notices, employees: found.employees })
    } else {
      out.push({ month, notices: 0, employees: 0 })
    }
  }

  return out
}

export function stateFreshness(meta: Meta): {
  state: string
  status: 'ok' | 'degraded' | 'failed'
  lastSuccess: string | null
  fetched: number
  error: string | null
}[] {
  const keys = Object.keys(meta.states)
  keys.sort()

  return keys.map((state) => {
    const st = meta.states[state]
    const status: 'ok' | 'degraded' | 'failed' = !st.ok
      ? 'failed'
      : st.degraded
        ? 'degraded'
        : 'ok'

    return {
      state,
      status,
      lastSuccess: st.last_success,
      fetched: st.fetched,
      error: st.error,
    }
  })
}

export function formatNumber(value: number | null | undefined): string {
  if (value == null) return '—'
  return value.toLocaleString('en-US')
}
