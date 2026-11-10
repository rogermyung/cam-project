import type { CfpbMonth } from '@/types'

import { sparkPoints } from '@/lib/cfpb'

export function Sparkline({ months }: { months: CfpbMonth[] }) {
  const first = months[0]?.month ?? '—'
  const last = months[months.length - 1]?.month ?? '—'

  return (
    <svg width={120} height={28} role="img" aria-label={`Monthly complaints, ${first} to ${last}`}>
      <polyline points={sparkPoints(months, 120, 28)} fill="none" stroke="#4f46e5" strokeWidth={1.5} />
    </svg>
  )
}
