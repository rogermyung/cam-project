import type { RepeatEmployer } from '@/types'
import { formatNumber } from '@/lib/notices'

export function RepeatEmployersTable({
  repeatEmployers,
}: {
  repeatEmployers: RepeatEmployer[]
}) {
  if (repeatEmployers.length === 0) {
    return (
      <p className="italic text-gray-400">
        No employer has more than one notice in a state.
      </p>
    )
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200">
            <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">Employer</th>
            <th className="px-3 py-2 text-left text-xs font-medium text-gray-400">State</th>
            <th className="px-3 py-2 text-right text-xs font-medium text-gray-400">Notices</th>
            <th className="px-3 py-2 text-right text-xs font-medium text-gray-400">
              Workers affected
            </th>
          </tr>
        </thead>
        <tbody>
          {repeatEmployers.map((row) => (
            <tr key={`${row.state}-${row.employer}`} className="border-b border-gray-100 last:border-b-0">
              <td className="px-3 py-2 text-sm text-gray-900 font-medium">{row.employer}</td>
              <td className="px-3 py-2 text-sm text-gray-700">{row.state}</td>
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
