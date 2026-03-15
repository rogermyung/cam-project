import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table'
import { pct, signalLabel } from '@/lib/utils'
import type { Evidence } from '@/types'

interface EvidenceTableProps {
  evidence: Evidence[]
}

function formatDate(dateStr: string | null): string {
  if (!dateStr) return '—'
  return new Date(dateStr).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  })
}

export function EvidenceTable({ evidence }: EvidenceTableProps) {
  if (!evidence || evidence.length === 0) {
    return <p className="text-sm text-gray-400 italic">No evidence signals available.</p>
  }

  return (
    <div className="overflow-x-auto rounded-md border border-gray-200">
      <Table>
        <TableHeader>
          <TableRow className="bg-gray-50">
            <TableHead className="text-xs font-semibold text-gray-600 w-40">Signal Type</TableHead>
            <TableHead className="text-xs font-semibold text-gray-600 w-20 text-right">Score</TableHead>
            <TableHead className="text-xs font-semibold text-gray-600 w-28">Date</TableHead>
            <TableHead className="text-xs font-semibold text-gray-600">Evidence</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {evidence.map((ev, i) => (
            <TableRow key={i} className="hover:bg-gray-50">
              <TableCell className="text-sm font-medium text-gray-900">
                {signalLabel(ev.signal_type)}
              </TableCell>
              <TableCell className="text-sm text-right tabular-nums text-gray-700">
                {pct(ev.score)}
              </TableCell>
              <TableCell className="text-sm text-gray-500">
                {formatDate(ev.signal_date)}
              </TableCell>
              <TableCell className="text-sm text-gray-600 max-w-xs">
                {ev.document_url ? (
                  <a
                    href={ev.document_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-indigo-600 hover:underline line-clamp-2"
                    title={ev.evidence ?? undefined}
                  >
                    {ev.evidence ?? 'View document'}
                  </a>
                ) : (
                  <span className="line-clamp-2" title={ev.evidence ?? undefined}>
                    {ev.evidence ?? '—'}
                  </span>
                )}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}
