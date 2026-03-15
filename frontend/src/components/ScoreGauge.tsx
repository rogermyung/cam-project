import { levelColor } from '@/components/AlertBadge'
import { pct } from '@/lib/utils'
import type { AlertLevel } from '@/types'

interface ScoreGaugeProps {
  score: number | null
  level: AlertLevel
  size?: number
}

// Thresholds as angles on a 180° semicircle (0 = left, 180 = right)
const WATCH = 0.40
const ELEVATED = 0.65
const CRITICAL = 0.80

function scoreToAngle(score: number): number {
  return score * 180
}

function polarToXY(angleDeg: number, r: number, cx: number, cy: number) {
  const rad = ((angleDeg - 180) * Math.PI) / 180
  return {
    x: cx + r * Math.cos(rad),
    y: cy + r * Math.sin(rad),
  }
}

function arcPath(startDeg: number, endDeg: number, r: number, cx: number, cy: number): string {
  const start = polarToXY(startDeg, r, cx, cy)
  const end = polarToXY(endDeg, r, cx, cy)
  const large = endDeg - startDeg > 90 ? 1 : 0
  return `M ${start.x} ${start.y} A ${r} ${r} 0 ${large} 1 ${end.x} ${end.y}`
}

export function ScoreGauge({ score, level, size = 200 }: ScoreGaugeProps) {
  const cx = size / 2
  const cy = size * 0.6
  const r = size * 0.38
  const strokeWidth = size * 0.07

  const filledAngle = score != null ? scoreToAngle(score) : 0
  const fillColor = levelColor(level)

  const thresholds = [
    { pct: WATCH, label: '40%' },
    { pct: ELEVATED, label: '65%' },
    { pct: CRITICAL, label: '80%' },
  ]

  return (
    <svg width={size} height={size * 0.65} aria-label={`Risk score gauge: ${pct(score)}`}>
      {/* Background arc */}
      <path
        d={arcPath(0, 180, r, cx, cy)}
        fill="none"
        stroke="#e5e7eb"
        strokeWidth={strokeWidth}
        strokeLinecap="round"
      />

      {/* Filled arc */}
      {score != null && score > 0 && (
        <path
          d={arcPath(0, filledAngle, r, cx, cy)}
          fill="none"
          stroke={fillColor}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
        />
      )}

      {/* Threshold tick marks */}
      {thresholds.map(({ pct: tPct, label }) => {
        const angle = scoreToAngle(tPct)
        const inner = polarToXY(angle, r - strokeWidth * 0.8, cx, cy)
        const outer = polarToXY(angle, r + strokeWidth * 0.3, cx, cy)
        const textPt = polarToXY(angle, r + strokeWidth * 1.2, cx, cy)
        return (
          <g key={label}>
            <line
              x1={inner.x} y1={inner.y}
              x2={outer.x} y2={outer.y}
              stroke="#6b7280" strokeWidth={1.5}
            />
            <text
              x={textPt.x} y={textPt.y}
              textAnchor="middle" dominantBaseline="middle"
              fontSize={size * 0.065} fill="#9ca3af"
            >
              {label}
            </text>
          </g>
        )
      })}

      {/* Score text */}
      <text
        x={cx} y={cy - r * 0.15}
        textAnchor="middle" dominantBaseline="middle"
        fontSize={size * 0.18} fontWeight="700"
        fill={score != null ? fillColor : '#9ca3af'}
      >
        {score != null ? pct(score, 0) : 'N/A'}
      </text>
    </svg>
  )
}
