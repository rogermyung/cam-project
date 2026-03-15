import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { fetchEntity } from '@/lib/data'
import type { EntityDetail } from '@/types'
import { Layout } from '@/components/Layout'
import { AlertBadge } from '@/components/AlertBadge'
import { ScoreGauge } from '@/components/ScoreGauge'
import { ComponentBreakdown } from '@/components/ComponentBreakdown'
import { ScoreHistoryChart } from '@/components/ScoreHistoryChart'
import { EvidenceTable } from '@/components/EvidenceTable'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { naicsLabel } from '@/lib/naics'
import { relativeDate } from '@/lib/utils'

export function EntityPage() {
  const { id } = useParams<{ id: string }>()
  const [entity, setEntity] = useState<EntityDetail | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!id) return
    fetchEntity(id)
      .then(setEntity)
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [id])

  if (loading) {
    return (
      <Layout>
        <p className="text-gray-400">Loading entity…</p>
      </Layout>
    )
  }

  if (error || !entity) {
    return (
      <Layout>
        <div className="text-center py-16">
          <p className="text-red-600 mb-4">{error ?? 'Entity not found.'}</p>
          <Link to="/" className="text-indigo-600 hover:underline text-sm">
            ← Back to alerts
          </Link>
        </div>
      </Layout>
    )
  }

  const cs = entity.current_score

  return (
    <Layout>
      {/* Breadcrumb */}
      <div className="mb-4">
        <Link to="/" className="text-sm text-gray-400 hover:text-gray-700">
          ← Alerts
        </Link>
      </div>

      {/* Entity header */}
      <div className="flex items-start gap-4 mb-8 flex-wrap">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3 flex-wrap">
            <h1 className="text-2xl font-bold text-gray-900 truncate">
              {entity.canonical_name}
            </h1>
            {entity.ticker && (
              <span className="text-gray-400 font-mono text-sm">({entity.ticker})</span>
            )}
            {cs && <AlertBadge level={cs.alert_level} />}
          </div>
          <p className="text-sm text-gray-500 mt-1">
            {naicsLabel(entity.naics_code)}
            {entity.naics_code && ` · NAICS ${entity.naics_code}`}
            {cs && ` · Updated ${relativeDate(cs.score_date)}`}
          </p>
        </div>

        {/* Score gauge */}
        {cs && (
          <div className="flex-shrink-0">
            <ScoreGauge score={cs.composite_score} level={cs.alert_level} size={180} />
          </div>
        )}
      </div>

      {/* Two-column grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-6">
        {/* Component breakdown */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Score Components</CardTitle>
          </CardHeader>
          <CardContent>
            {cs ? (
              <ComponentBreakdown scores={cs.component_scores} />
            ) : (
              <p className="text-sm text-gray-400 italic">No scores available.</p>
            )}
          </CardContent>
        </Card>

        {/* Score history */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Score History</CardTitle>
          </CardHeader>
          <CardContent>
            <ScoreHistoryChart history={entity.score_history} />
          </CardContent>
        </Card>
      </div>

      {/* Evidence table */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Top Evidence Signals</CardTitle>
        </CardHeader>
        <CardContent>
          <EvidenceTable evidence={entity.top_evidence} />
        </CardContent>
      </Card>
    </Layout>
  )
}
