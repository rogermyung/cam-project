import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Layout } from '@/components/Layout'
import { NoticeFilters } from '@/components/notices/NoticeFilters'
import { NoticeTable } from '@/components/notices/NoticeTable'
import { Pagination } from '@/components/notices/Pagination'
import { fetchNotices } from '@/lib/data'
import type { Notice } from '@/types'
import {
  filterNotices,
  formatNumber,
  paginate,
  sortNotices,
  type SortDir,
  type SortKey,
} from '@/lib/notices'

export function NoticesPage() {
  const [searchParams, setSearchParams] = useSearchParams()

  const [notices, setNotices] = useState<Notice[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [searchText, setSearchText] = useState('')
  const [sortKey, setSortKey] = useState<SortKey>('notice_date')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [page, setPage] = useState(1)

  useEffect(() => {
    fetchNotices()
      .then(setNotices)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  const activeState = searchParams.get('state') ?? ''

  const stateOptions = useMemo(() => {
    const s = new Set<string>()
    for (const n of notices) s.add(n.state)
    return Array.from(s).sort()
  }, [notices])

  const filtered = useMemo(() => {
    return filterNotices(notices, { state: activeState, query: searchText })
  }, [notices, activeState, searchText])

  const sorted = useMemo(() => {
    return sortNotices(filtered, sortKey, sortDir)
  }, [filtered, sortKey, sortDir])

  const pagination = useMemo(() => {
    return paginate(sorted, page, 50)
  }, [sorted, page])

  const shownCount = filtered.length
  const totalCount = notices.length

  const handleStateChange = (nextState: string) => {
    const next = new URLSearchParams(searchParams)
    if (nextState === '') next.delete('state')
    else next.set('state', nextState)
    setSearchParams(next)
    setPage(1)
  }

  const handleClear = () => {
    const next = new URLSearchParams(searchParams)
    next.delete('state')
    setSearchParams(next)
    setSearchText('')
    setPage(1)
  }

  const handleSearchChange = (next: string) => {
    setSearchText(next)
    setPage(1)
  }

  const handleSort = (key: SortKey) => {
    setPage(1)
    if (key === sortKey) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
      return
    }

    setSortKey(key)
    setSortDir(key === 'notice_date' || key === 'employees' ? 'desc' : 'asc')
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

  return (
    <Layout>
      <h1 className="text-xl font-semibold text-gray-900">All notices</h1>
      <p className="mt-1 text-sm text-gray-600 tabular-nums">
        {formatNumber(shownCount)} of {formatNumber(totalCount)} notices
      </p>

      <NoticeFilters
        stateOptions={stateOptions}
        activeState={activeState}
        searchText={searchText}
        onStateChange={handleStateChange}
        onSearchChange={handleSearchChange}
        onClear={handleClear}
      />

      <div className="mt-4 overflow-x-auto">
        <div className="min-w-[980px] bg-white rounded-lg border border-gray-200">
          <NoticeTable
            notices={pagination.items}
            sortKey={sortKey}
            sortDir={sortDir}
            onSort={handleSort}
          />
        </div>
      </div>

      <Pagination
        page={pagination.page}
        pageCount={pagination.pageCount}
        onPrev={() => setPage((p) => Math.max(1, p - 1))}
        onNext={() => setPage((p) => Math.min(pagination.pageCount, p + 1))}
      />
    </Layout>
  )
}
