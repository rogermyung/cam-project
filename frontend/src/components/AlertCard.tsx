import { Link } from 'react-router-dom'
import { AlertBadge, levelBorderClass } from '@/components/AlertBadge'
import { pct, relativeDate } from '@/lib/utils'
import { naicsLabel } from '@/lib/naics'
import type { Alert } from '@/types'

interface AlertCardProps {
  alert: Alert
}

export function AlertCard({ alert }: AlertCardProps) {
  return (
    <div className={`border-l-4 rounded-r-md px-4 py-3 ${levelBorderClass(alert.alert_level)}`}>
      <div className="flex items-center gap-2 flex-wrap">
        <Link
          to={`/entity/${alert.entity_id}`}
          className="font-semibold text-base hover:underline text-gray-900"
        >
          {alert.canonical_name}
        </Link>
        <AlertBadge level={alert.alert_level} />
      </div>
      <div className="text-sm text-gray-500 mt-1 flex gap-3 flex-wrap">
        <span>Score: <strong>{pct(alert.composite_score)}</strong></span>
        <span>·</span>
        <span>{relativeDate(alert.score_date)}</span>
        <span>·</span>
        <span>{naicsLabel(alert.naics_code)} (NAICS {alert.naics_code ?? 'N/A'})</span>
      </div>
    </div>
  )
}
