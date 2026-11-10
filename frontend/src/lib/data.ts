import type { Meta, Notice, Summary } from '@/types'

// BASE_URL is '/cam-project/' in production (GitHub Pages) and '/' in dev.
const B = import.meta.env.BASE_URL

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${B}${path}`)
  if (!res.ok) throw new Error(`Failed to fetch ${path}: ${res.status}`)
  return res.json() as Promise<T>
}

export const fetchMeta = () => getJson<Meta>('data/meta.json')

export const fetchNotices = () => getJson<Notice[]>('data/notices.json')
export const fetchSummary = () => getJson<Summary>('data/summary.json')
