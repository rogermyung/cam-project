import { useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

import type { MonthTotal } from '@/types'

const BAR_COLOR = '#4f46e5'

type Metric = 'notices' | 'employees'

export function MonthChart({ data }: { data: MonthTotal[] }) {
  const [metric, setMetric] = useState<Metric>('notices')
  const activeLabel = metric === 'notices' ? 'Notices' : 'Workers'

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <button
          type="button"
          onClick={() => setMetric('notices')}
          aria-pressed={metric === 'notices'}
          className={
            metric === 'notices'
              ? 'px-3 py-1.5 rounded-md border border-gray-900 bg-gray-900 text-white text-xs font-medium'
              : 'px-3 py-1.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium hover:bg-gray-50'
          }
        >
          Notices
        </button>
        <button
          type="button"
          onClick={() => setMetric('employees')}
          aria-pressed={metric === 'employees'}
          className={
            metric === 'employees'
              ? 'px-3 py-1.5 rounded-md border border-gray-900 bg-gray-900 text-white text-xs font-medium'
              : 'px-3 py-1.5 rounded-md border border-gray-200 bg-white text-gray-700 text-xs font-medium hover:bg-gray-50'
          }
        >
          Workers
        </button>
      </div>

      <ResponsiveContainer width="100%" height={260}>
        <BarChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis dataKey="month" tick={{ fontSize: 12 }} />
          <YAxis tick={{ fontSize: 12 }} />
          <Tooltip
            formatter={(value) => [value as number, activeLabel]}
            labelStyle={{ color: '#6b7280' }}
          />
          <Bar dataKey={metric} fill={BAR_COLOR} />
        </BarChart>
      </ResponsiveContainer>

      <p className="mt-3 text-xs text-gray-500">
        Some states report the layoff date rather than the filing date, so recent months can still
        grow.
      </p>
    </div>
  )
}
