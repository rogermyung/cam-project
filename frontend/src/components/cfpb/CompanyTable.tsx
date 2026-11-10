import type { CfpbCompany } from '@/types'
import type { CfpbSortKey } from '@/lib/cfpb'
import type { SortDir } from '@/lib/notices'

import { formatChange } from '@/lib/cfpb'
import { formatNumber } from '@/lib/notices'
import { Sparkline } from '@/components/cfpb/Sparkline'

function SortHeaderButton({
  label,
  column,
  align,
  sortKey,
  sortDir,
  onSort,
}: {
  label: string
  column: CfpbSortKey
  align: 'left' | 'right'
  sortKey: CfpbSortKey
  sortDir: SortDir
  onSort: (key: CfpbSortKey) => void
}) {
  const active = sortKey === column
  const ariaSort = active ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'
  const arrow = active ? (sortDir === 'asc' ? '↑' : '↓') : null

  return (
    <th
      aria-sort={ariaSort}
      className={`px-4 py-2 whitespace-nowrap ${align === 'right' ? 'text-right' : 'text-left'}`}
    >
      <button
        type="button"
        className="inline-flex items-center gap-1 text-xs font-medium text-gray-400 hover:text-gray-700"
        onClick={() => onSort(column)}
      >
        {label}
        {arrow}
      </button>
    </th>
  )
}

export function CompanyTable({
  companies,
  sortKey,
  sortDir,
  onSort,
}: {
  companies: CfpbCompany[]
  sortKey: CfpbSortKey
  sortDir: SortDir
  onSort: (key: CfpbSortKey) => void
}) {
  return (
    <table className="w-full border-collapse">
      <thead>
        <tr>
          <SortHeaderButton
            label="Company"
            column="company"
            align="left"
            sortKey={sortKey}
            sortDir={sortDir}
            onSort={onSort}
          />

          <th className="px-4 py-2 whitespace-nowrap text-left">
            <span className="text-xs font-medium text-gray-400">Trend</span>
          </th>

          <SortHeaderButton
            label="Latest month"
            column="latest"
            align="right"
            sortKey={sortKey}
            sortDir={sortDir}
            onSort={onSort}
          />

          <th className="px-4 py-2 whitespace-nowrap text-right">
            <span className="text-xs font-medium text-gray-400">12-mo average</span>
          </th>

          <SortHeaderButton label="Change" column="ratio" align="right" sortKey={sortKey} sortDir={sortDir} onSort={onSort} />

          <th className="px-4 py-2 whitespace-nowrap text-left">
            <span className="text-xs font-medium text-gray-400">Flag</span>
          </th>

          <th className="px-4 py-2 whitespace-nowrap text-left">
            <span className="text-xs font-medium text-gray-400">Top product</span>
          </th>

          <SortHeaderButton
            label="Window total"
            column="total"
            align="right"
            sortKey={sortKey}
            sortDir={sortDir}
            onSort={onSort}
          />
        </tr>
      </thead>

      <tbody>
        {companies.length === 0 ? (
          <tr>
            <td colSpan={8} className="px-4 py-6 text-center italic text-gray-400">
              No companies match the current filters.
            </td>
          </tr>
        ) : (
          companies.map((c) => (
            <tr key={c.company} className="border-t border-gray-200">
              <td className="px-4 py-3 text-sm text-gray-900">{c.company}</td>
              <td className="px-4 py-3 text-sm text-gray-900">
                <Sparkline months={c.months} />
              </td>
              <td className="px-4 py-3 text-sm text-gray-900 text-right tabular-nums whitespace-nowrap">
                {formatNumber(c.latest)}
              </td>
              <td className="px-4 py-3 text-sm text-gray-900 text-right tabular-nums whitespace-nowrap">
                {c.baseline.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 })}
              </td>
              <td className="px-4 py-3 text-sm text-gray-900 text-right tabular-nums whitespace-nowrap">
                {formatChange(c.ratio)}
              </td>
              <td className="px-4 py-3 text-sm text-gray-900">
                {c.spike ? (
                  <span className="inline-flex items-center rounded-full bg-red-100 px-2 py-0.5 text-xs font-medium text-red-800">
                    Spike
                  </span>
                ) : null}
              </td>
              <td className="px-4 py-3 text-sm text-gray-900">{c.top_product}</td>
              <td className="px-4 py-3 text-sm text-gray-900 text-right tabular-nums whitespace-nowrap">
                {formatNumber(c.total)}
              </td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  )
}
