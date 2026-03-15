// Two-digit NAICS sector names (top-level sectors)
export const NAICS_LABELS: Record<string, string> = {
  '11': 'Agriculture, Forestry, Fishing & Hunting',
  '21': 'Mining, Quarrying & Oil/Gas Extraction',
  '22': 'Utilities',
  '23': 'Construction',
  '31': 'Manufacturing',
  '32': 'Manufacturing',
  '33': 'Manufacturing',
  '42': 'Wholesale Trade',
  '44': 'Retail Trade',
  '45': 'Retail Trade',
  '48': 'Transportation & Warehousing',
  '49': 'Transportation & Warehousing',
  '51': 'Information',
  '52': 'Finance & Insurance',
  '53': 'Real Estate & Rental',
  '54': 'Professional, Scientific & Technical Services',
  '55': 'Management of Companies',
  '56': 'Administrative & Support Services',
  '61': 'Educational Services',
  '62': 'Health Care & Social Assistance',
  '71': 'Arts, Entertainment & Recreation',
  '72': 'Accommodation & Food Services',
  '81': 'Other Services',
  '92': 'Public Administration',
}

export function naicsLabel(code: string | null | undefined): string {
  if (!code) return 'Unknown Sector'
  const twoDigit = String(code).slice(0, 2)
  return NAICS_LABELS[twoDigit] ?? `NAICS ${twoDigit}`
}

export function naicsTwoDigit(code: string | null | undefined): string {
  if (!code) return 'N/A'
  return String(code).slice(0, 2)
}
