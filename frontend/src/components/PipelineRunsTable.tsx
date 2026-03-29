import { PipelineRun } from '@/lib/api'
import { CheckCircle, XCircle, Clock, Loader } from 'lucide-react'
import { format } from 'date-fns'

interface Props {
  data: PipelineRun[]
}

function StatusIcon({ status }: { status: string }) {
  switch (status) {
    case 'success': return <CheckCircle className="w-4 h-4 text-emerald-400" />
    case 'failed':  return <XCircle className="w-4 h-4 text-red-400" />
    case 'running': return <Loader className="w-4 h-4 text-blue-400 animate-spin" />
    default:        return <Clock className="w-4 h-4 text-gray-400" />
  }
}

export default function PipelineRunsTable({ data }: Props) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <h2 className="text-white font-semibold mb-4">Recent Pipeline Runs</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-400 text-xs uppercase border-b border-gray-800">
              <th className="text-left pb-3 pr-4">Status</th>
              <th className="text-left pb-3 pr-4">Source</th>
              <th className="text-right pb-3 pr-4">Rows</th>
              <th className="text-right pb-3 pr-4">Duration</th>
              <th className="text-left pb-3">Started</th>
            </tr>
          </thead>
          <tbody>
            {data.slice(0, 20).map((run) => (
              <tr key={run.run_id} className="border-b border-gray-800/50">
                <td className="py-3 pr-4">
                  <div className="flex items-center gap-2">
                    <StatusIcon status={run.status} />
                    <span className="text-gray-300">{run.status}</span>
                  </div>
                </td>
                <td className="py-3 pr-4 text-white font-mono text-xs">{run.source_name}</td>
                <td className="py-3 pr-4 text-right text-gray-400">
                  {run.rows_extracted != null ? run.rows_extracted.toLocaleString() : '—'}
                </td>
                <td className="py-3 pr-4 text-right text-gray-400">
                  {run.duration_seconds != null ? `${run.duration_seconds}s` : '—'}
                </td>
                <td className="py-3 text-gray-500 text-xs">
                  {format(new Date(run.started_at), 'MMM d, HH:mm')}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
