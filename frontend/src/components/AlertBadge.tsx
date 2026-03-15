import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/utils'
import type { AlertLevel } from '@/types'

interface AlertBadgeProps {
  level: AlertLevel
  className?: string
}

const LEVEL_CONFIG = {
  critical: { label: 'Critical', className: 'bg-red-600 hover:bg-red-600 text-white' },
  elevated: { label: 'Elevated', className: 'bg-orange-500 hover:bg-orange-500 text-white' },
  watch: { label: 'Watch', className: 'bg-yellow-500 hover:bg-yellow-500 text-black' },
} as const

export function AlertBadge({ level, className }: AlertBadgeProps) {
  if (!level) return null
  const cfg = LEVEL_CONFIG[level]
  return (
    <Badge className={cn(cfg.className, className)}>
      {cfg.label}
    </Badge>
  )
}

export function levelColor(level: AlertLevel): string {
  switch (level) {
    case 'critical': return '#dc2626' // red-600
    case 'elevated': return '#f97316' // orange-500
    case 'watch':    return '#eab308' // yellow-500
    default:         return '#9ca3af' // gray-400
  }
}

export function levelBorderClass(level: AlertLevel): string {
  switch (level) {
    case 'critical': return 'border-l-red-600 bg-red-50'
    case 'elevated': return 'border-l-orange-500 bg-orange-50'
    case 'watch':    return 'border-l-yellow-500 bg-yellow-50'
    default:         return 'border-l-gray-300 bg-white'
  }
}
