import { useEffect, useMemo, useState, type ChangeEvent } from 'react'

import { Layout } from '@/components/Layout'
import { CompanyTable } from '@/components/cfpb/CompanyTable'
import { StatCard } from '@/components/overview/StatCard'
import { Pagination } from '@/components/notices/Pagination'
import { fetchCfpbCompanies, fetchCfpbMeta } from '@/lib/data'
import { filterCompanies, sortCompanies, type CfpbSortKey } from '@/lib/cfpb'

import { formatNumber, paginate, type SortDir } from '@/lib/notices'
import type { CfpbCompany, CfpbMeta } from '@/types'

export function CfpbPage() {
  const [companies, setCompanies] = useState<CfpbCompany[]>([])
  const [meta, setMeta] = useState<CfpbMeta | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [searchText, setSearchText] = useState('')
  const [spikesOnly, setSpikesOnly] = useState(false)

  const [sortKey, setSortKey] = useState<CfpbSortKey>('ratio')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [page, setPage] = useState(1)

  useEffect(() => {
    Promise.all([fetchCfpbCompanies(), fetchCfpbMeta()])
      .then(([cs, m]) => {
        setCompanies(cs)
        setMeta(m)
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  const minComplaints = meta?.status?.min_complaints

  const filtered = useMemo(() => {
    return filterCompanies(companies, { query: searchText, spikesOnly })
  }, [companies, searchText, spikesOnly])

  const sorted = useMemo(() => {
    return sortCompanies(filtered, sortKey, sortDir)
  }, [filtered, sortKey, sortDir])

  const pagination = useMemo(() => {
    return paginate(sorted, page, 50)
  }, [sorted, page])

  const shownCount = filtered.length
  const totalCount = companies.length

  const handleSearchChange = (e: ChangeEvent<HTMLInputElement>) => {
    setSearchText(e.target.value)
    setPage(1)
  }

  const handleSpikesToggle = (e: ChangeEvent<HTMLInputElement>) => {
    setSpikesOnly(e.target.checked)
    setPage(1)
  }

  const handleSort = (key: CfpbSortKey) => {
    setPage(1)
    if (key === sortKey) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
      return
    }

    setSortKey(key)
    setSortDir(key === 'company' ? 'asc' : 'desc')
  }

  if (loading) {
    return (
      <Layout>
        <p className="text-gray-400">Loading…</p>
      </Layout>
    )
  }

  if (error) {
    return (
      <Layout>
        <p className="text-red-600">Error: {error}</p>
      </Layout>
    )
  }

  if (!meta) {
    return (
      <Layout>
        <p className="text-red-600">Error: missing CFPB meta</p>
      </Layout>
    )
  }

  const first10 = meta.generated_at.slice(0, 10)
  const minPhrase =
    typeof minComplaints === 'number'
      ? `for companies with at least ${formatNumber(minComplaints)} complaints in the last 24 months`
      : 'for companies in the last 24 months'

  return (
    <Layout>
      <div className="space-y-6">
        <div>
          <h1 className="text-xl font-semibold text-gray-900">CFPB consumer complaints</h1>
          <p className="mt-2 text-sm text-gray-600">
            Monthly complaints to the Consumer Financial Protection Bureau {minPhrase}.{' '}
            <a
              href="https://www.consumerfinance.gov/data-research/consumer-complaints/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:underline"
            >
              source
            </a>
          </p>
        </div>

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <StatCard label="Companies tracked" value={formatNumber(meta.companies)} />
          <StatCard label="Complaints in window" value={formatNumber(meta.total_complaints)} />
          <StatCard label={`Spikes in ${meta.latest_month}`} value={formatNumber(meta.spikes)} />
          <StatCard label="Data updated" value={first10} />
        </div>

        {meta.status?.ok === false ? (
          <p className="text-sm text-amber-700">
            The last CFPB fetch failed ({meta.status.error}). Showing data from{' '}
            {meta.status.last_success ?? 'an earlier run'}.
          </p>
        ) : null}
        {meta.status?.degraded ? (
          <p className="text-sm text-amber-700">The last fetch found far fewer companies than before.</p>
        ) : null}

        <div className="flex flex-wrap items-center gap-3">
          <div className="flex-1 min-w-[240px]">
            <label className="sr-only" htmlFor="cfpb-search">
              Search company
            </label>
            <input
              id="cfpb-search"
              type="search"
              aria-label="Search company"
              placeholder="Search company"
              className="h-9 w-full rounded-lg border border-gray-200 bg-white px-3 text-sm text-gray-900"
              value={searchText}
              onChange={handleSearchChange}
            />
          </div>

          <label className="flex items-center gap-2 text-sm text-gray-700">
            <input
              type="checkbox"
              checked={spikesOnly}
              onChange={handleSpikesToggle}
              className="h-4 w-4 rounded border-gray-300"
            />
            Spikes only
          </label>

          <div className="text-sm text-gray-500 tabular-nums">
            {formatNumber(shownCount)} of {formatNumber(totalCount)} companies
          </div>
        </div>

        <div className="overflow-x-auto">
          <div className="min-w-[980px] bg-white rounded-lg border border-gray-200">
            <CompanyTable companies={pagination.items} sortKey={sortKey} sortDir={sortDir} onSort={handleSort} />
          </div>
        </div>

        <Pagination
          page={pagination.page}
          pageCount={pagination.pageCount}
          onPrev={() => setPage((p) => Math.max(1, p - 1))}
          onNext={() => setPage((p) => Math.min(pagination.pageCount, p + 1))}
        />

        <p className="text-xs text-gray-400">
          A spike means the latest full month is more than 50% above the average of the 12 months before it. Complaints are
          counted by the date the CFPB received them; the most recent month can still grow as complaints are published.
        </p>
      </div>
    </Layout>
  )
}
