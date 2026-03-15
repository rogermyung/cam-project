import { Progress } from '@/components/ui/progress'
import { pct } from '@/lib/utils'
import { signalLabel } from '@/lib/utils'

interface ComponentBreakdownProps {
  scores: Record<string, number>
}

export function ComponentBreakdown({ scores }: ComponentBreakdownProps) {
  const entries = Object.entries(scores).sort(([, a], [, b]) => b - a)

  if (entries.length === 0) {
    return <p className="text-sm text-gray-400 italic">No component scores available.</p>
  }

  return (
    <div className="space-y-3">
      {entries.map(([key, value]) => (
        <div key={key}>
          <div className="flex justify-between text-sm mb-1">
            <span className="text-gray-700 font-medium">{signalLabel(key)}</span>
            <span className="text-gray-500 tabular-nums">{pct(value)}</span>
          </div>
          <Progress value={value * 100} className="h-2" />
        </div>
      ))}
    </div>
  )
}
