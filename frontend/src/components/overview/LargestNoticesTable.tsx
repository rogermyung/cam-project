import type { Notice } from '@/types'
import { formatNumber } from '@/lib/notices'

export function LargestNoticesTable({ largest }: { largest: Notice[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200">
            <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">Employer</th>
            <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">State</th>
            <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">Notice date</th>
            <th className="px-3 py-2 text-right text-xs font-medium text-gray-400">Workers</th>
            <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">
              Location (city)
            </th>
          </tr>
        </thead>
        <tbody>
          {largest.map((n) => (
            <tr key={n.key} className="border-b border-gray-100 last:border-b-0">
              <td className="px-3 py-2 text-sm text-gray-900 font-medium">{n.employer}</td>
              <td className="px-3 py-2 text-sm text-gray-700">{n.state}</td>
              <td className="px-3 py-2 text-sm text-gray-700">
                {n.notice_date ? n.notice_date : '—'}
              </td>
              <td className="px-3 py-2 text-right text-sm text-gray-700 tabular-nums">
                {formatNumber(n.employees)}
              </td>
              <td className="px-3 py-2 text-sm text-gray-700">{n.city}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
