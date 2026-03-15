// TypeScript interfaces mirroring the JSON schemas written by cam/output/exporter.py

export type AlertLevel = 'critical' | 'elevated' | 'watch' | null

export interface Meta {
  exported_at: string
  entity_count: number
  alert_count: number
  version: string
}

export interface Alert {
  entity_id: string
  canonical_name: string
  alert_level: AlertLevel
  composite_score: number
  score_date: string
  component_scores: Record<string, number>
  naics_code: string | null
}

export interface EntitySummary {
  id: string
  canonical_name: string
  ticker: string | null
  naics_code: string | null
  composite_score: number | null
  alert_level: AlertLevel
  score_date: string | null
}

export interface ScoreHistory {
  score_date: string
  composite_score: number
  alert_level: AlertLevel
}

export interface Evidence {
  signal_type: string
  score: number
  evidence: string | null
  signal_date: string | null
  document_url: string | null
}

export interface EntityDetail {
  id: string
  canonical_name: string
  ticker: string | null
  naics_code: string | null
  current_score: {
    composite_score: number
    alert_level: AlertLevel
    score_date: string
    component_scores: Record<string, number>
  } | null
  score_history: ScoreHistory[]
  top_evidence: Evidence[]
}
