import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { fetchAlerts, fetchEntities } from '@/lib/data'
import type { Alert, EntitySummary } from '@/types'
import { Layout } from '@/components/Layout'
import { AlertBadge } from '@/components/AlertBadge'
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from '@/components/ui/accordion'
import { naicsTwoDigit, NAICS_LABELS } from '@/lib/naics'
import { pct, relativeDate } from '@/lib/utils'

interface SectorGroup {
  code: string
  label: string
  entities: EntitySummary[]
  alerts: Alert[]
}

function groupBySector(entities: EntitySummary[], alerts: Alert[]): SectorGroup[] {
  const alertsByEntity = new Map<string, Alert>()
  for (const a of alerts) alertsByEntity.set(a.entity_id, a)

  const sectorMap = new Map<string, SectorGroup>()

  for (const e of entities) {
    const code = naicsTwoDigit(e.naics_code)
    const label = NAICS_LABELS[code] ?? `Sector ${code}`
    if (!sectorMap.has(code)) {
      sectorMap.set(code, { code, label, entities: [], alerts: [] })
    }
    const group = sectorMap.get(code)!
    group.entities.push(e)
    if (alertsByEntity.has(e.id)) group.alerts.push(alertsByEntity.get(e.id)!)
  }

  return [...sectorMap.values()].sort((a, b) => b.alerts.length - a.alerts.length || a.label.localeCompare(b.label))
}

const LEVEL_ORDER = { critical: 0, elevated: 1, watch: 2 } as const

function alertOrder(level: string | null): number {
  return (LEVEL_ORDER as Record<string, number>)[level ?? ''] ?? 99
}

export function IndustriesPage() {
  const [entities, setEntities] = useState<EntitySummary[]>([])
  const [alerts, setAlerts] = useState<Alert[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    Promise.all([fetchEntities(), fetchAlerts()])
      .then(([e, a]) => {
        setEntities(e)
        setAlerts(a)
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false))
  }, [])

  const sectors = groupBySector(entities, alerts)

  return (
    <Layout>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Industries</h1>
        <p className="text-sm text-gray-500 mt-1">
          {entities.length} entities across {sectors.length} NAICS sectors
        </p>
      </div>

      {loading && <p className="text-gray-400">Loading…</p>}
      {error && <p className="text-red-600">Error: {error}</p>}

      {!loading && !error && (
        <Accordion className="space-y-2">
          {sectors.map((sector) => (
            <AccordionItem
              key={sector.code}
              value={sector.code}
              className="border border-gray-200 rounded-lg overflow-hidden bg-white"
            >
              <AccordionTrigger className="px-4 py-3 hover:no-underline hover:bg-gray-50">
                <div className="flex items-center gap-3 flex-1 min-w-0 text-left">
                  <span className="font-medium text-gray-900 truncate">{sector.label}</span>
                  <span className="text-xs text-gray-400 font-mono shrink-0">NAICS {sector.code}</span>
                  <div className="ml-auto flex items-center gap-2 mr-2 shrink-0">
                    <span className="text-xs text-gray-500">
                      {sector.entities.length} co.
                    </span>
                    {sector.alerts.length > 0 && (
                      <span className="text-xs font-medium text-red-600 bg-red-50 px-2 py-0.5 rounded-full">
                        {sector.alerts.length} alert{sector.alerts.length !== 1 ? 's' : ''}
                      </span>
                    )}
                  </div>
                </div>
              </AccordionTrigger>
              <AccordionContent className="px-4 pb-3">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs text-gray-400 border-b border-gray-100">
                      <th className="pb-2 font-medium">Company</th>
                      <th className="pb-2 font-medium text-right">Score</th>
                      <th className="pb-2 font-medium">Level</th>
                      <th className="pb-2 font-medium">Updated</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-50">
                    {sector.entities
                      .slice()
                      .sort((a, b) => alertOrder(a.alert_level) - alertOrder(b.alert_level))
                      .map((e) => (
                        <tr key={e.id} className="hover:bg-gray-50">
                          <td className="py-2 pr-4">
                            <Link
                              to={`/entity/${e.id}`}
                              className="text-indigo-600 hover:underline font-medium"
                            >
                              {e.canonical_name}
                            </Link>
                            {e.ticker && (
                              <span className="text-gray-400 text-xs ml-1 font-mono">
                                ({e.ticker})
                              </span>
                            )}
                          </td>
                          <td className="py-2 pr-4 text-right tabular-nums text-gray-700">
                            {e.composite_score != null ? pct(e.composite_score) : '—'}
                          </td>
                          <td className="py-2 pr-4">
                            {e.alert_level ? (
                              <AlertBadge level={e.alert_level} />
                            ) : (
                              <span className="text-gray-400 text-xs">—</span>
                            )}
                          </td>
                          <td className="py-2 text-gray-400 text-xs">
                            {e.score_date ? relativeDate(e.score_date) : '—'}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </AccordionContent>
            </AccordionItem>
          ))}
        </Accordion>
      )}
    </Layout>
  )
}
