import { InventoryItem } from '@/lib/api'

interface Props {
  data: InventoryItem[]
}

const STATUS_STYLES: Record<string, string> = {
  out_of_stock: 'bg-red-900/50 text-red-400 border border-red-800',
  low_stock:    'bg-amber-900/50 text-amber-400 border border-amber-800',
  healthy:      'bg-emerald-900/50 text-emerald-400 border border-emerald-800',
  overstock:    'bg-blue-900/50 text-blue-400 border border-blue-800',
}

export default function InventoryTable({ data }: Props) {
  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
      <h2 className="text-white font-semibold mb-4">Inventory Health</h2>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-gray-400 text-xs uppercase border-b border-gray-800">
              <th className="text-left pb-3 pr-4">SKU</th>
              <th className="text-left pb-3 pr-4">Warehouse</th>
              <th className="text-right pb-3 pr-4">Qty</th>
              <th className="text-right pb-3 pr-4">Days of Stock</th>
              <th className="text-left pb-3">Status</th>
            </tr>
          </thead>
          <tbody>
            {data.slice(0, 20).map((item) => (
              <tr key={item.sku_id} className="border-b border-gray-800/50">
                <td className="py-3 pr-4 text-white font-mono text-xs">{item.sku_id}</td>
                <td className="py-3 pr-4 text-gray-400">{item.warehouse_id ?? '—'}</td>
                <td className="py-3 pr-4 text-right text-white">{item.quantity_on_hand}</td>
                <td className="py-3 pr-4 text-right text-gray-400">
                  {item.days_of_stock != null ? `${item.days_of_stock}d` : '—'}
                </td>
                <td className="py-3">
                  <span className={`text-xs px-2 py-1 rounded font-medium ${STATUS_STYLES[item.status_flag] ?? ''}`}>
                    {item.status_flag.replace('_', ' ')}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
