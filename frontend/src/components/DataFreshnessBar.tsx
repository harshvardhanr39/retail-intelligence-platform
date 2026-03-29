import { DataFreshness } from '@/lib/api'

interface Props {
  data: DataFreshness[]
}

const DOT_COLORS: Record<string, string> = {
  green: 'bg-emerald-400',
  amber: 'bg-amber-400',
  red:   'bg-red-400',
}

export default function DataFreshnessBar({ data }: Props) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-4">
      <div className="flex items-center gap-2 mb-3">
        <span className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
          Data Freshness
        </span>
      </div>
      <div className="flex flex-wrap gap-4">
        {data.map((source) => (
          <div key={source.source_name} className="flex items-center gap-2">
            <span className={`w-2 h-2 rounded-full ${DOT_COLORS[source.status]} animate-pulse`} />
            <span className="text-xs text-gray-300">{source.source_name}</span>
            <span className="text-xs text-gray-500">
              {source.hours_since_update != null
                ? `${source.hours_since_update}h ago`
                : 'never'}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}
