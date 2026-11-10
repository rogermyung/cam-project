import { Link } from 'react-router-dom'

import type { StateTotal } from '@/types'
import { formatNumber } from '@/lib/notices'

export function ByStateTable({ byState }: { byState: StateTotal[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200">
            <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">State</th>
            <th className="px-3 py-2 text-right text-xs font-medium text-gray-400">Notices</th>
            <th className="px-3 py-2 text-right text-xs font-medium text-gray-400">
              Workers affected
            </th>
          </tr>
        </thead>
        <tbody>
          {byState.map((row) => (
            <tr key={row.state} className="border-b border-gray-100 last:border-b-0">
              <td className="px-3 py-2 text-sm text-gray-900 font-medium">
                <Link to={`/notices?state=${row.state}`} className="text-gray-900 hover:underline">
                  {row.state}
                </Link>
              </td>
              <td className="px-3 py-2 text-right text-sm text-gray-700 tabular-nums">
                {formatNumber(row.notices)}
              </td>
              <td className="px-3 py-2 text-right text-sm text-gray-700 tabular-nums">
                {formatNumber(row.employees)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
