import type { ScoreHistory } from '@/types'

interface ScoreHistoryChartProps {
  history: ScoreHistory[]
}

// Alert level thresholds (0–100 display scale)
const THRESHOLDS = [
  { y: 80, label: 'Critical', color: '#dc2626' },
  { y: 65, label: 'Elevated', color: '#f97316' },
  { y: 40, label: 'Watch',    color: '#eab308' },
]

function levelColor(score: number): string {
  if (score >= 80) return '#dc2626'
  if (score >= 65) return '#f97316'
  if (score >= 40) return '#eab308'
  return '#6366f1'
}

function shortDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

export function ScoreHistoryChart({ history }: ScoreHistoryChartProps) {
  if (!history || history.length < 2) {
    return <p className="text-sm text-gray-400 italic">Not enough score history to display chart.</p>
  }

  const sorted = [...history]
    .sort((a, b) => a.score_date.localeCompare(b.score_date))
    .map((h) => ({ date: h.score_date, score: Math.round(h.composite_score * 100) }))

  const W = 500
  const H = 120
  const PAD = { top: 12, right: 44, bottom: 20, left: 32 }
  const cW = W - PAD.left - PAD.right
  const cH = H - PAD.top - PAD.bottom

  const xPos = (i: number) => PAD.left + (i / (sorted.length - 1)) * cW
  const yPos = (v: number) => PAD.top + cH - (v / 100) * cH

  const polyline = sorted.map((d, i) => `${xPos(i)},${yPos(d.score)}`).join(' ')

  const labelIdx =
    sorted.length > 3 ? [0, Math.floor(sorted.length / 2), sorted.length - 1] : [0, sorted.length - 1]

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', height: 'auto' }}
      role="img" aria-label="Score history sparkline">

      {/* Y-axis ticks */}
      {[0, 50, 100].map((t) => (
        <text key={t} x={PAD.left - 4} y={yPos(t) + 3} fontSize={9} fill="#9ca3af" textAnchor="end">
          {t}%
        </text>
      ))}

      {/* Threshold reference lines */}
      {THRESHOLDS.map((t) => (
        <g key={t.label}>
          <line x1={PAD.left} y1={yPos(t.y)} x2={W - PAD.right} y2={yPos(t.y)}
            stroke={t.color} strokeWidth={1} strokeDasharray="4 2" opacity={0.65} />
          <text x={W - PAD.right + 3} y={yPos(t.y) + 3} fontSize={8} fill={t.color} opacity={0.85}>
            {t.label}
          </text>
        </g>
      ))}

      {/* Sparkline */}
      <polyline points={polyline} fill="none" stroke="#6366f1"
        strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" />

      {/* Data points colored by alert level */}
      {sorted.map((d, i) => (
        <circle key={i} cx={xPos(i)} cy={yPos(d.score)} r={3.5}
          fill={levelColor(d.score)} stroke="#fff" strokeWidth={1}>
          <title>{shortDate(d.date)}: {d.score}%</title>
        </circle>
      ))}

      {/* X-axis date labels */}
      {labelIdx.map((i) => (
        <text key={i} x={xPos(i)} y={H - 4} fontSize={9} fill="#9ca3af"
          textAnchor={i === 0 ? 'start' : i === sorted.length - 1 ? 'end' : 'middle'}>
          {shortDate(sorted[i].date)}
        </text>
      ))}
    </svg>
  )
}
