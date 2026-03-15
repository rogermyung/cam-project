import { useEffect, useState } from 'react'
import { fetchAlerts, fetchMeta } from '@/lib/data'
import type { Alert, AlertLevel, Meta } from '@/types'
import { AlertCard } from '@/components/AlertCard'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Layout } from '@/components/Layout'

type Filter = 'all' | AlertLevel

const FILTER_OPTIONS: { value: Filter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'critical', label: 'Critical' },
  { value: 'elevated', label: 'Elevated' },
  { value: 'watch', label: 'Watch' },
]

function exportCsv(alerts: Alert[]) {
  const header = 'entity_id,canonical_name,alert_level,composite_score,score_date,naics_code'
  const rows = alerts.map((a) =>
    [
      a.entity_id,
      `"${a.canonical_name.replace(/"/g, '""')}"`,
      a.alert_level ?? '',
      a.composite_score,
      a.score_date,
      a.naics_code ?? '',
    ].join(','),
  )
  const csv = [header, ...rows].join('\n')
  const blob = new Blob([csv], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `cam-alerts-${new Date().toISOString().slice(0, 10)}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

export function AlertsPage() {
  const [alerts, setAlerts] = useState<Alert[]>([])
  const [meta, setMeta] = useState<Meta | null>(null)
  const [filter, setFilter] = useState<Filter>('all')
  const [search, setSearch] = useState('')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    Promise.all([fetchAlerts(), fetchMeta()])
      .then(([a, m]) => {
        setAlerts(a)
        setMeta(m)
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  const filtered = alerts
    .filter((a) => filter === 'all' || a.alert_level === filter)
    .filter((a) => !search || a.canonical_name.toLowerCase().includes(search.toLowerCase()))

  return (
    <Layout>
      {/* Page header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Active Alerts</h1>
        {meta && (
          <p className="text-sm text-gray-500 mt-1">
            {meta.alert_count} alert{meta.alert_count !== 1 ? 's' : ''} across{' '}
            {meta.entity_count} monitored entities — as of{' '}
            {new Date(meta.exported_at).toLocaleDateString('en-US', {
              month: 'long',
              day: 'numeric',
              year: 'numeric',
            })}
          </p>
        )}
      </div>

      {/* Controls row */}
      <div className="flex flex-wrap gap-3 mb-6 items-center">
        {/* Level filter buttons */}
        <div className="flex gap-1 flex-wrap">
          {FILTER_OPTIONS.map(({ value, label }) => (
            <Button
              key={value}
              size="sm"
              variant={filter === value ? 'default' : 'outline'}
              onClick={() => setFilter(value)}
            >
              {label}
              {value !== 'all' && (
                <span className="ml-1 text-xs opacity-70">
                  ({alerts.filter((a) => a.alert_level === value).length})
                </span>
              )}
            </Button>
          ))}
        </div>

        {/* Search */}
        <div className="flex-1 min-w-40 max-w-xs">
          <Input
            placeholder="Search entities…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 text-sm"
          />
        </div>

        {/* CSV export */}
        <Button size="sm" variant="outline" onClick={() => exportCsv(filtered)}>
          ↓ Export CSV
        </Button>
      </div>

      {/* Content */}
      {loading && <p className="text-gray-400">Loading alerts…</p>}
      {error && <p className="text-red-600">Error: {error}</p>}
      {!loading && !error && filtered.length === 0 && (
        <p className="text-gray-400 italic">No alerts match the current filter.</p>
      )}
      <div className="space-y-3">
        {filtered.map((alert) => (
          <AlertCard key={alert.entity_id} alert={alert} />
        ))}
      </div>
    </Layout>
  )
}
