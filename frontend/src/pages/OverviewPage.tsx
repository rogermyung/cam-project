import { useEffect, useMemo, useState, type ReactNode } from 'react'

import { Layout } from '@/components/Layout'
import { MonthChart } from '@/components/overview/MonthChart'
import { LargestNoticesTable } from '@/components/overview/LargestNoticesTable'
import { SectionCard } from '@/components/overview/SectionCard'
import { SourceStatus } from '@/components/overview/SourceStatus'
import { StatCard } from '@/components/overview/StatCard'
import { ByStateTable } from '@/components/overview/ByStateTable'
import { RepeatEmployersTable } from '@/components/overview/RepeatEmployersTable'
import { fetchMeta, fetchSummary } from '@/lib/data'
import { formatNumber, monthRange } from '@/lib/notices'
import type { Meta, MonthTotal, Summary } from '@/types'

function CardSpacer({ children }: { children: ReactNode }) {
  return <div className="space-y-6">{children}</div>
}

export function OverviewPage() {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [summary, setSummary] = useState<Summary | null>(null)
  const [meta, setMeta] = useState<Meta | null>(null)

  useEffect(() => {
    Promise.all([fetchSummary(), fetchMeta()])
      .then(([s, m]) => {
        setSummary(s)
        setMeta(m)
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  const endMonth = useMemo(() => new Date().toISOString().slice(0, 7), [])

  const monthData = useMemo<MonthTotal[]>(() => {
    if (!summary) return []
    return monthRange(summary.by_month, 24, endMonth)
  }, [summary, endMonth])

  const byStateSorted = useMemo(() => {
    if (!summary) return []
    return [...summary.by_state].sort((a, b) => b.notices - a.notices)
  }, [summary])

  const largest = useMemo(() => summary?.largest ?? [], [summary])
  const repeatEmployers = useMemo(() => summary?.repeat_employers.slice(0, 20) ?? [], [summary])

  return (
    <Layout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">WARN layoff notices</h1>
          <p className="mt-2 text-sm text-gray-600">
            Mass-layoff and closure notices that employers file with state governments under the WARN Act.
          </p>
        </div>

        {loading ? <p className="text-gray-400">Loading…</p> : null}
        {error ? <p className="text-red-600">Error: {error}</p> : null}

        {!loading && !error && meta && summary ? (
          <CardSpacer>
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
              <StatCard label="Notices" value={formatNumber(meta.total_notices)} />
              <StatCard
                label="Workers affected"
                value={formatNumber(meta.total_employees)}
              />
              <StatCard
                label="Latest notice date"
                value={meta.latest_notice_date ?? '—'}
              />
              <StatCard label="Data updated" value={meta.generated_at.slice(0, 10)} />
            </div>

            <SectionCard title="Notices per month">
              <MonthChart data={monthData} />
            </SectionCard>

            <SectionCard title="Source status">
              <SourceStatus meta={meta} />
            </SectionCard>

            <SectionCard title="By state">
              <ByStateTable byState={byStateSorted} />
              <p className="mt-3 text-xs text-gray-500">
                Tip: click a state code to view matching notices.
              </p>
            </SectionCard>

            <SectionCard title="Largest notices">
              <LargestNoticesTable largest={largest} />
            </SectionCard>

            <SectionCard title="Employers with repeat notices">
              <RepeatEmployersTable repeatEmployers={repeatEmployers} />
              <p className="mt-3 text-xs text-gray-500">
                Includes multiple notices by the same employer in the same state.
              </p>
            </SectionCard>
          </CardSpacer>
        ) : null}
      </div>
    </Layout>
  )
}
