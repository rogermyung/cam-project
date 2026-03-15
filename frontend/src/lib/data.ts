import type { Alert, EntityDetail, EntitySummary, Meta } from '@/types'

// BASE_URL is '/cam-project/' in production (GitHub Pages) and '/' in dev.
const B = import.meta.env.BASE_URL

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${B}${path}`)
  if (!res.ok) throw new Error(`Failed to fetch ${path}: ${res.status}`)
  return res.json() as Promise<T>
}

export const fetchMeta = () => getJson<Meta>('data/meta.json')
export const fetchAlerts = () => getJson<Alert[]>('data/alerts.json')
export const fetchEntities = () => getJson<EntitySummary[]>('data/entities.json')
export const fetchEntity = (id: string) =>
  getJson<EntityDetail>(`data/entities/${encodeURIComponent(id)}.json`)
