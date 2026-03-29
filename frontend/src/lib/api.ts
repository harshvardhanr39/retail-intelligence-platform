const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

// ── TypeScript interfaces matching Pydantic models ────────

export interface DailyRevenue {
  date: string
  total_revenue: number
  order_count: number
  avg_order_value: number
  revenue_7day_avg: number | null
  revenue_growth_pct: number | null
}

export interface RevenueSummary {
  total_revenue_mtd: number
  order_count_mtd: number
  aov: number
  revenue_growth_pct: number | null
  last_updated: string
}

export interface CategoryPerformance {
  date: string
  category: string
  revenue: number
  units_sold: number
  order_count: number
  revenue_share_pct: number
}

export interface InventoryItem {
  sku_id: string
  product_name: string | null
  product_id: string | null
  warehouse_id: string | null
  quantity_on_hand: number
  reorder_point: number | null
  days_of_stock: number | null
  status_flag: string
  avg_daily_units_sold: number | null
}

export interface TopProduct {
  rank: number
  product_id: string
  product_name: string | null
  category: string | null
  total_revenue: number
  units_sold: number
  order_count: number
  avg_selling_price: number | null
}

export interface PipelineRun {
  run_id: string
  source_name: string
  status: string
  rows_extracted: number | null
  rows_written: number | null
  started_at: string
  finished_at: string | null
  duration_seconds: number | null
  error_message: string | null
}

export interface DataFreshness {
  source_name: string
  last_run_at: string | null
  hours_since_update: number | null
  status: 'green' | 'amber' | 'red'
}

// ── Typed fetch helper ────────────────────────────────────

async function fetchAPI<T>(path: string): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    next: { revalidate: 300 }, // cache for 5 minutes
  })
  if (!res.ok) {
    throw new Error(`API error ${res.status}: ${path}`)
  }
  return res.json()
}

// ── API functions ─────────────────────────────────────────

export const getDailyRevenue = (days = 30) =>
  fetchAPI<DailyRevenue[]>(`/api/v1/revenue/daily?days=${days}`)

export const getRevenueSummary = () =>
  fetchAPI<RevenueSummary>('/api/v1/revenue/summary')

export const getCategoryPerformance = (days = 30) =>
  fetchAPI<CategoryPerformance[]>(`/api/v1/revenue/by-category?days=${days}`)

export const getInventoryHealth = (status?: string) =>
  fetchAPI<InventoryItem[]>(
    `/api/v1/inventory/health${status ? `?status=${status}` : ''}`
  )

export const getInventoryAlerts = () =>
  fetchAPI<InventoryItem[]>('/api/v1/inventory/alerts')

export const getTopProducts = (limit = 10) =>
  fetchAPI<TopProduct[]>(`/api/v1/products/top?limit=${limit}`)

export const getPipelineRuns = () =>
  fetchAPI<PipelineRun[]>('/api/v1/pipeline/runs')

export const getDataFreshness = () =>
  fetchAPI<DataFreshness[]>('/api/v1/pipeline/freshness')

export const triggerPipeline = async () => {
  const res = await fetch(`${API_URL}/api/v1/pipeline/trigger`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
  })
  if (!res.ok) throw new Error('Trigger failed')
  return res.json()
}
