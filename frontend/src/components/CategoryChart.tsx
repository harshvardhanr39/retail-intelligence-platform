'use client'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Cell
} from 'recharts'
import { CategoryPerformance } from '@/lib/api'

interface Props {
  data: CategoryPerformance[]
}

const COLORS = ['#10b981', '#6366f1', '#f59e0b', '#ef4444', '#8b5cf6']

export default function CategoryChart({ data }: Props) {
  // Aggregate by category across all days
  const byCategory = data.reduce((acc, row) => {
    if (!acc[row.category]) acc[row.category] = { category: row.category, revenue: 0, units_sold: 0 }
    acc[row.category].revenue += row.revenue
    acc[row.category].units_sold += row.units_sold
    return acc
  }, {} as Record<string, { category: string; revenue: number; units_sold: number }>)

  const chartData = Object.values(byCategory)
    .sort((a, b) => b.revenue - a.revenue)
    .map(d => ({ ...d, revenue: Number(d.revenue.toFixed(2)) }))

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <h2 className="text-white font-semibold mb-4">Revenue by Category</h2>
      <ResponsiveContainer width="100%" height={300}>
        <BarChart data={chartData} layout="vertical">
          <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" horizontal={false} />
          <XAxis
            type="number"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            tickFormatter={(v) => `$${(v / 1000).toFixed(0)}k`}
          />
          <YAxis
            type="category"
            dataKey="category"
            tick={{ fill: '#9ca3af', fontSize: 11 }}
            width={120}
          />
          <Tooltip
            contentStyle={{ backgroundColor: '#111827', border: '1px solid #374151', borderRadius: 8 }}
            formatter={(v) => [`$${Number(v ?? 0).toLocaleString()}`, 'Revenue']}
          />
          <Bar dataKey="revenue" radius={[0, 4, 4, 0]}>
            {chartData.map((_, index) => (
              <Cell key={index} fill={COLORS[index % COLORS.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
