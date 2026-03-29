import {
  getDailyRevenue, getRevenueSummary, getCategoryPerformance,
  getTopProducts, getInventoryAlerts, getDataFreshness
} from '@/lib/api'
import KPICard from '@/components/KPICard'
import RevenueChart from '@/components/RevenueChart'
import CategoryChart from '@/components/CategoryChart'
import TopProductsTable from '@/components/TopProductsTable'
import InventoryTable from '@/components/InventoryTable'
import DataFreshnessBar from '@/components/DataFreshnessBar'
import TriggerButton from '@/components/TriggerButton'
import Link from 'next/link'

export default async function DashboardPage() {
  // Fetch all data in parallel on the server
  const [summary, dailyRevenue, categories, topProducts, alerts, freshness] =
    await Promise.allSettled([
      getRevenueSummary(),
      getDailyRevenue(30),
      getCategoryPerformance(30),
      getTopProducts(10),
      getInventoryAlerts(),
      getDataFreshness(),
    ])

  const s  = summary.status        === 'fulfilled' ? summary.value        : null
  const dr = dailyRevenue.status   === 'fulfilled' ? dailyRevenue.value   : []
  const ca = categories.status     === 'fulfilled' ? categories.value     : []
  const tp = topProducts.status    === 'fulfilled' ? topProducts.value    : []
  const al = alerts.status         === 'fulfilled' ? alerts.value         : []
  const fr = freshness.status      === 'fulfilled' ? freshness.value      : []

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      {/* Header */}
      <header className="border-b border-gray-800 bg-gray-900/50 backdrop-blur">
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold text-white">⬡ Retail Intelligence</h1>
            <p className="text-xs text-gray-400 mt-0.5">Real-time analytics platform</p>
          </div>
          <div className="flex items-center gap-4">
            <Link
              href="/pipeline"
              className="text-sm text-gray-400 hover:text-white transition-colors"
            >
              Pipeline Monitor →
            </Link>
            <TriggerButton />
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-8 space-y-8">

        {/* Data Freshness */}
        {fr.length > 0 && <DataFreshnessBar data={fr} />}

        {/* KPI Cards */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <KPICard
            title="Revenue MTD"
            value={s ? `$${s.total_revenue_mtd.toLocaleString()}` : '—'}
            growth={s?.revenue_growth_pct}
            icon="💰"
          />
          <KPICard
            title="Orders MTD"
            value={s ? s.order_count_mtd.toLocaleString() : '—'}
            icon="📦"
          />
          <KPICard
            title="Avg Order Value"
            value={s ? `$${s.aov.toFixed(2)}` : '—'}
            icon="🛒"
          />
          <KPICard
            title="Inventory Alerts"
            value={al.length.toString()}
            subtitle="items need attention"
            icon="⚠️"
          />
        </div>

        {/* Charts */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {dr.length > 0 && <RevenueChart data={dr} />}
          {ca.length > 0 && <CategoryChart data={ca} />}
        </div>

        {/* Tables */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {tp.length > 0 && <TopProductsTable data={tp} />}
          {al.length > 0 && <InventoryTable data={al} />}
        </div>

      </main>
    </div>
  )
}
