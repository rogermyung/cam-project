import type { Notice } from '@/types'
import type { SortDir, SortKey } from '@/lib/notices'
import { formatNumber } from '@/lib/notices'

interface NoticeTableProps {
  notices: Notice[]
  sortKey: SortKey
  sortDir: SortDir
  onSort: (key: SortKey) => void
}

function SortHeaderButton({
  label,
  sortKey,
  column,
  sortDir,
  onSort,
}: {
  label: string
  sortKey: SortKey
  column: SortKey
  sortDir: SortDir
  onSort: (key: SortKey) => void
}) {
  const active = sortKey === column
  const ariaSort = active ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'
  const arrow = active ? (sortDir === 'asc' ? '↑' : '↓') : null

  return (
    <th aria-sort={ariaSort} className="whitespace-nowrap text-left">
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

export function NoticeTable({ notices, sortKey, sortDir, onSort }: NoticeTableProps) {
  return (
    <table className="w-full border-collapse">
      <thead>
        <tr>
          <SortHeaderButton
            label="Employer"
            sortKey={sortKey}
            column="employer"
            sortDir={sortDir}
            onSort={onSort}
          />
          <SortHeaderButton
            label="State"
            sortKey={sortKey}
            column="state"
            sortDir={sortDir}
            onSort={onSort}
          />
          <SortHeaderButton
            label="Notice date"
            sortKey={sortKey}
            column="notice_date"
            sortDir={sortDir}
            onSort={onSort}
          />
          <SortHeaderButton
            label="Workers"
            sortKey={sortKey}
            column="employees"
            sortDir={sortDir}
            onSort={onSort}
          />
          <th className="whitespace-nowrap text-left">
            <span className="text-xs font-medium text-gray-400">City</span>
          </th>
          <th className="whitespace-nowrap text-left">
            <span className="text-xs font-medium text-gray-400">County</span>
          </th>
          <th className="whitespace-nowrap text-left">
            <span className="text-xs font-medium text-gray-400">Type</span>
          </th>
          <th className="whitespace-nowrap text-left">
            <span className="text-xs font-medium text-gray-400">Source</span>
          </th>
        </tr>
      </thead>

      <tbody>
        {notices.length === 0 ? (
          <tr>
            <td colSpan={8} className="px-4 py-6 text-center italic text-gray-400">
              No notices match the current filters.
            </td>
          </tr>
        ) : (
          notices.map((n) => (
            <tr key={n.key} className="border-t border-gray-200">
              <td className="px-4 py-3 text-sm text-gray-900 break-words">
                {n.employer}
              </td>
              <td className="px-4 py-3 text-sm text-gray-900 whitespace-nowrap">{n.state}</td>
              <td className="px-4 py-3 text-sm text-gray-900 whitespace-nowrap">{n.notice_date ?? '—'}</td>
              <td className="px-4 py-3 text-sm text-gray-900 text-right tabular-nums whitespace-nowrap">
                {formatNumber(n.employees)}
              </td>
              <td className="px-4 py-3 text-sm text-gray-900 break-words">{n.city}</td>
              <td className="px-4 py-3 text-sm text-gray-900 break-words">{n.county}</td>
              <td className="px-4 py-3 text-sm text-gray-900 whitespace-nowrap">{n.notice_type}</td>
              <td className="px-4 py-3 text-sm">
                <a
                  href={n.source_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-600 hover:underline"
                >
                  source
                </a>
              </td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  )
}
