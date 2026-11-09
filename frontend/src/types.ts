// TypeScript interfaces mirroring the JSON written by cam/warn/build.py.
// This file is the data contract between the Python tracker and the site.

/** One WARN notice (a row of data/warn/notices.csv). */
export interface Notice {
  key: string
  state: string
  employer: string
  /** ISO date. For MI and PA this is the layoff effective date and may be in the future. */
  notice_date: string | null
  employees: number | null
  city: string
  county: string
  notice_type: string
  source_url: string
  /** ISO date the tracker first saw this notice. */
  first_seen: string
}

export interface StateTotal {
  state: string
  notices: number
  employees: number
}

export interface MonthTotal {
  /** 'YYYY-MM' */
  month: string
  notices: number
  employees: number
}

export interface RepeatEmployer {
  state: string
  employer: string
  notices: number
  employees: number
}

/** data/summary.json */
export interface Summary {
  by_state: StateTotal[]
  by_month: MonthTotal[]
  largest: Notice[]
  repeat_employers: RepeatEmployer[]
}

/** Result of the tracker's last attempt at one state's source. */
export interface StateStatus {
  ok: boolean
  fetched: number
  added: number
  error: string | null
  last_attempt: string
  last_success: string | null
  degraded: boolean
}

/** data/meta.json */
export interface Meta {
  generated_at: string
  total_notices: number
  total_employees: number
  latest_notice_date: string | null
  states: Record<string, StateStatus>
}
