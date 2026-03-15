import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/** Format a decimal 0–1 as a percentage string: 0.874 → "87.4%" */
export function pct(value: number | null | undefined, decimals = 1): string {
  if (value == null) return 'N/A'
  return `${(value * 100).toFixed(decimals)}%`
}

/** Format an ISO date string as a relative timestamp or the date itself. */
export function relativeDate(isoDate: string | null | undefined): string {
  if (!isoDate) return '—'
  const d = new Date(isoDate)
  if (isNaN(d.getTime())) return isoDate
  const diffMs = Date.now() - d.getTime()
  const diffDays = Math.floor(diffMs / 86_400_000)
  if (diffDays === 0) return 'today'
  if (diffDays === 1) return 'yesterday'
  if (diffDays <= 30) return `${diffDays} days ago`
  return isoDate
}

/** Convert snake_case signal type to a human label. */
export function signalLabel(key: string): string {
  return key
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase())
}
