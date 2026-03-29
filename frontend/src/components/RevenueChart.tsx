'use client'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer
} from 'recharts'
import { DailyRevenue } from '@/lib/api'
import { format } from 'date-fns'

interface Props {
  data: DailyRevenue[]
}

export default function RevenueChart({ data }: Props) {
  const sorted = [...data].sort((a, b) => a.date.localeCompare(b.date))

  const formatted = sorted.map(d => ({
    ...d,
    date: format(new Date(d.date), 'MMM d'),
    total_revenue: Number(d.total_revenue.toFixed(2)),
    revenue_7day_avg: d.revenue_7day_avg ? Number(d.revenue_7day_avg.toFixed(2)) : null,
  }))

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <h2 className="text-white font-semibold mb-4">Daily Revenue (30 days)</h2>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={formatted}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
          <XAxis
            dataKey="date"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={(v) => `$${(Number(v) / 1000).toFixed(0)}k`}
          />
          <Tooltip
            contentStyle={{ backgroundColor: '#111827', border: '1px solid #374151', borderRadius: 8 }}
            labelStyle={{ color: '#f9fafb' }}
            formatter={(v) => [`$${Number(v ?? 0).toLocaleString()}`, '']}
          />
          <Legend wrapperStyle={{ color: '#9ca3af' }} />
          <Line
            type="monotone"
            dataKey="total_revenue"
            stroke="#10b981"
            strokeWidth={2}
            dot={false}
            name="Daily Revenue"
          />
          <Line
            type="monotone"
            dataKey="revenue_7day_avg"
            stroke="#6366f1"
            strokeWidth={2}
            strokeDasharray="5 5"
            dot={false}
            name="7-Day Avg"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
