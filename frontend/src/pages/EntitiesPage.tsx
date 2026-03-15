import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { fetchEntities } from '@/lib/data'
import type { EntitySummary, AlertLevel } from '@/types'
import { Layout } from '@/components/Layout'
import { AlertBadge } from '@/components/AlertBadge'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { naicsLabel } from '@/lib/naics'
import { pct, relativeDate } from '@/lib/utils'

type SortKey = 'name' | 'score' | 'date'

const LEVEL_ORDER: Record<string, number> = { critical: 0, elevated: 1, watch: 2 }

function levelOrder(level: AlertLevel) {
  return LEVEL_ORDER[level ?? ''] ?? 99
}

interface SortBtnProps {
  value: SortKey
  label: string
  active: boolean
  onSort: (v: SortKey) => void
}

function SortBtn({ value, label, active, onSort }: SortBtnProps) {
  return (
    <Button size="sm" variant={active ? 'default' : 'outline'} onClick={() => onSort(value)}>
      {label}
    </Button>
  )
}

export function EntitiesPage() {
  const [entities, setEntities] = useState<EntitySummary[]>([])
  const [search, setSearch] = useState('')
  const [sort, setSort] = useState<SortKey>('score')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchEntities()
      .then(setEntities)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  const filtered = entities
    .filter((e) => !search || e.canonical_name.toLowerCase().includes(search.toLowerCase()))
    .slice()
    .sort((a, b) => {
      if (sort === 'score') {
        const la = levelOrder(a.alert_level)
        const lb = levelOrder(b.alert_level)
        if (la !== lb) return la - lb
        return (b.composite_score ?? -1) - (a.composite_score ?? -1)
      }
      if (sort === 'date') {
        return (b.score_date ?? '').localeCompare(a.score_date ?? '')
      }
      return a.canonical_name.localeCompare(b.canonical_name)
    })

  return (
    <Layout>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">All Entities</h1>
        <p className="text-sm text-gray-500 mt-1">{entities.length} monitored companies</p>
      </div>

      {/* Controls */}
      <div className="flex flex-wrap gap-3 mb-6 items-center">
        <div className="flex gap-1">
          <SortBtn value="score" label="By Risk" active={sort === 'score'} onSort={setSort} />
          <SortBtn value="name" label="By Name" active={sort === 'name'} onSort={setSort} />
          <SortBtn value="date" label="By Date" active={sort === 'date'} onSort={setSort} />
        </div>
        <div className="flex-1 min-w-40 max-w-xs">
          <Input
            placeholder="Search entities…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 text-sm"
          />
        </div>
      </div>

      {loading && <p className="text-gray-400">Loading…</p>}
      {error && <p className="text-red-600">Error: {error}</p>}

      {!loading && !error && (
        <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-xs text-gray-400 bg-gray-50 border-b border-gray-200">
                <th className="px-4 py-3 font-medium">Company</th>
                <th className="px-4 py-3 font-medium">Industry</th>
                <th className="px-4 py-3 font-medium text-right">Score</th>
                <th className="px-4 py-3 font-medium">Alert</th>
                <th className="px-4 py-3 font-medium">Updated</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {filtered.map((e) => (
                <tr key={e.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3">
                    <Link
                      to={`/entity/${e.id}`}
                      className="font-medium text-indigo-600 hover:underline"
                    >
                      {e.canonical_name}
                    </Link>
                    {e.ticker && (
                      <span className="text-gray-400 text-xs ml-1 font-mono">({e.ticker})</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-gray-500">{naicsLabel(e.naics_code)}</td>
                  <td className="px-4 py-3 text-right tabular-nums text-gray-700">
                    {e.composite_score != null ? pct(e.composite_score) : '—'}
                  </td>
                  <td className="px-4 py-3">
                    {e.alert_level ? (
                      <AlertBadge level={e.alert_level} />
                    ) : (
                      <span className="text-gray-300 text-xs">none</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-gray-400 text-xs">
                    {e.score_date ? relativeDate(e.score_date) : '—'}
                  </td>
                </tr>
              ))}
              {filtered.length === 0 && (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-gray-400 italic">
                    No entities match the current filter.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </Layout>
  )
}
