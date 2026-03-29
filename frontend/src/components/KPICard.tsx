interface KPICardProps {
  title: string
  value: string
  subtitle?: string
  growth?: number | null
  icon: string
}

export default function KPICard({ title, value, subtitle, growth, icon }: KPICardProps) {
  const growthColor = growth == null
    ? 'text-gray-400'
    : growth >= 0 ? 'text-emerald-400' : 'text-red-400'

  const growthPrefix = growth != null && growth >= 0 ? '+' : ''

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <div className="flex items-center justify-between mb-4">
        <span className="text-2xl">{icon}</span>
        {growth != null && (
          <span className={`text-sm font-medium ${growthColor}`}>
            {growthPrefix}{growth.toFixed(1)}%
          </span>
        )}
      </div>
      <div className="text-2xl font-bold text-white mb-1">{value}</div>
      <div className="text-sm text-gray-400">{title}</div>
      {subtitle && <div className="text-xs text-gray-600 mt-1">{subtitle}</div>}
    </div>
  )
}
