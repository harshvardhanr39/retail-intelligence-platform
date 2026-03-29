import { TopProduct } from '@/lib/api'

interface Props {
  data: TopProduct[]
}

export default function TopProductsTable({ data }: Props) {
  const maxRevenue = Math.max(...data.map(p => p.total_revenue))

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <h2 className="text-white font-semibold mb-4">Top Products</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-400 text-xs uppercase border-b border-gray-800">
              <th className="text-left pb-3 pr-4">#</th>
              <th className="text-left pb-3 pr-4">Product</th>
              <th className="text-left pb-3 pr-4">Category</th>
              <th className="text-right pb-3 pr-4">Revenue</th>
              <th className="text-right pb-3">Units</th>
            </tr>
          </thead>
          <tbody>
            {data.map((product) => (
              <tr key={product.product_id} className="border-b border-gray-800/50">
                <td className="py-3 pr-4 text-gray-400">{product.rank}</td>
                <td className="py-3 pr-4">
                  <div className="text-white">{product.product_name ?? product.product_id}</div>
                  <div className="w-full bg-gray-800 rounded-full h-1 mt-1">
                    <div
                      className="bg-emerald-500 h-1 rounded-full"
                      style={{ width: `${(product.total_revenue / maxRevenue) * 100}%` }}
                    />
                  </div>
                </td>
                <td className="py-3 pr-4">
                  <span className="bg-gray-800 text-gray-300 text-xs px-2 py-1 rounded">
                    {product.category ?? '—'}
                  </span>
                </td>
                <td className="py-3 pr-4 text-right text-white">
                  ${product.total_revenue.toLocaleString()}
                </td>
                <td className="py-3 text-right text-gray-400">
                  {product.units_sold.toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
