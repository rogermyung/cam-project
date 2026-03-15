import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceLine,
  ResponsiveContainer,
} from 'recharts'
import type { ScoreHistory } from '@/types'

interface ScoreHistoryChartProps {
  history: ScoreHistory[]
}

function formatDate(dateStr: string): string {
  const d = new Date(dateStr)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

export function ScoreHistoryChart({ history }: ScoreHistoryChartProps) {
  if (!history || history.length === 0) {
    return <p className="text-sm text-gray-400 italic">No score history available.</p>
  }

  const data = [...history]
    .sort((a, b) => a.score_date.localeCompare(b.score_date))
    .map((h) => ({
      date: formatDate(h.score_date),
      score: Math.round(h.composite_score * 100),
    }))

  return (
    <ResponsiveContainer width="100%" height={220}>
      <LineChart data={data} margin={{ top: 8, right: 12, left: -20, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f3f4f6" />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 11, fill: '#9ca3af' }}
          tickLine={false}
          axisLine={false}
        />
        <YAxis
          domain={[0, 100]}
          tick={{ fontSize: 11, fill: '#9ca3af' }}
          tickLine={false}
          axisLine={false}
          tickFormatter={(v) => `${v}%`}
        />
        <Tooltip
          formatter={(value) => [`${value ?? 0}%`, 'Score']}
          contentStyle={{ fontSize: 12, borderRadius: 6, border: '1px solid #e5e7eb' }}
        />
        {/* Threshold reference lines */}
        <ReferenceLine y={40} stroke="#eab308" strokeDasharray="4 2" strokeWidth={1.5} label={{ value: 'Watch', position: 'insideTopRight', fontSize: 10, fill: '#eab308' }} />
        <ReferenceLine y={65} stroke="#f97316" strokeDasharray="4 2" strokeWidth={1.5} label={{ value: 'Elevated', position: 'insideTopRight', fontSize: 10, fill: '#f97316' }} />
        <ReferenceLine y={80} stroke="#dc2626" strokeDasharray="4 2" strokeWidth={1.5} label={{ value: 'Critical', position: 'insideTopRight', fontSize: 10, fill: '#dc2626' }} />
        <Line
          type="monotone"
          dataKey="score"
          stroke="#6366f1"
          strokeWidth={2}
          dot={{ r: 3, fill: '#6366f1' }}
          activeDot={{ r: 5 }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
