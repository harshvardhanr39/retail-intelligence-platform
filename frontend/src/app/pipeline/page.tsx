import { getPipelineRuns, getDataFreshness } from '@/lib/api'
import PipelineRunsTable from '@/components/PipelineRunsTable'
import DataFreshnessBar from '@/components/DataFreshnessBar'
import TriggerButton from '@/components/TriggerButton'
import Link from 'next/link'

export default async function PipelinePage() {
  const [runs, freshness] = await Promise.allSettled([
    getPipelineRuns(),
    getDataFreshness(),
  ])

  const r = runs.status     === 'fulfilled' ? runs.value     : []
  const f = freshness.status === 'fulfilled' ? freshness.value : []

  return (
    <div className="min-h-screen bg-gray-950 text-white">
      <header className="border-b border-gray-800 bg-gray-900/50 backdrop-blur">
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <div>
            <h1 className="text-xl font-bold text-white">Pipeline Monitor</h1>
            <p className="text-xs text-gray-400 mt-0.5">Track pipeline runs and data freshness</p>
          </div>
          <div className="flex items-center gap-4">
            <Link
              href="/"
              className="text-sm text-gray-400 hover:text-white transition-colors"
            >
              ← Dashboard
            </Link>
            <TriggerButton />
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-8 space-y-6">
        {f.length > 0 && <DataFreshnessBar data={f} />}
        {r.length > 0 && <PipelineRunsTable data={r} />}

        {r.length === 0 && (
          <div className="bg-gray-900 border border-gray-800 rounded-xl p-12 text-center">
            <p className="text-gray-400">No pipeline runs found.</p>
            <p className="text-gray-600 text-sm mt-2">
              Run an extractor to see data here.
            </p>
          </div>
        )}
      </main>
    </div>
  )
}
