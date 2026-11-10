export function StatCard({
  label,
  value,
}: {
  label: string
  value: string
}) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="text-xs text-gray-500">{label}</div>
      <div className="mt-2 text-xl font-semibold text-gray-900 tabular-nums">{value}</div>
    </div>
  )
}
