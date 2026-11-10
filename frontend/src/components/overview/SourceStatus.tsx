import type { Meta } from '@/types'
import { stateFreshness, formatNumber } from '@/lib/notices'

type AttentionRow = {
  state: string
  status: 'ok' | 'degraded' | 'failed'
  lastSuccess: string | null
  fetched: number
  error: string | null
}

function problemText(row: AttentionRow) {
  if (row.status === 'failed') return row.error ?? '—'
  if (row.status === 'degraded') return 'Fetched far fewer notices than last time'
  return '—'
}

function pillClasses(status: AttentionRow['status']) {
  if (status === 'ok') return 'bg-green-100 text-green-800'
  if (status === 'degraded') return 'bg-amber-100 text-amber-800'
  return 'bg-red-100 text-red-800'
}

export function SourceStatus({ meta }: { meta: Meta }) {
  const freshness = stateFreshness(meta) as AttentionRow[]

  const attentionCount = freshness.filter((r) => r.status !== 'ok').length
  const totalCount = freshness.length

  return (
    <div>
      {attentionCount > 0 ? (
        <p className="mb-3 text-sm text-amber-700">
          {attentionCount} of {totalCount} sources need attention.
        </p>
      ) : null}

      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200">
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">State</th>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">Status</th>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">
                Last successful fetch
              </th>
              <th className="px-3 py-2 text-right text-xs font-medium text-gray-400">
                Notices fetched
              </th>
              <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">Problem</th>
            </tr>
          </thead>
          <tbody>
            {freshness.map((row) => (
              <tr key={row.state} className="border-b border-gray-100 last:border-b-0">
                <td className="px-3 py-2 text-sm text-gray-900 font-medium">{row.state}</td>
                <td className="px-3 py-2">
                  <span
                    className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ${pillClasses(
                      row.status
                    )}`}
                  >
                    {row.status}
                  </span>
                </td>
                <td className="px-3 py-2 text-sm text-gray-700">
                  {row.lastSuccess ? row.lastSuccess.slice(0, 10) : '—'}
                </td>
                <td className="px-3 py-2 text-right text-sm text-gray-700 tabular-nums">
                  {formatNumber(row.fetched)}
                </td>
                <td className="px-3 py-2 text-sm text-gray-700">{problemText(row)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
