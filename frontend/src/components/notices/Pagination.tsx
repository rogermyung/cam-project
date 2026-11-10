interface PaginationProps {
  page: number
  pageCount: number
  onPrev: () => void
  onNext: () => void
}

export function Pagination({ page, pageCount, onPrev, onNext }: PaginationProps) {
  return (
    <div className="mt-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
      <div className="text-sm text-gray-600 tabular-nums">Page {page} of {pageCount}</div>

      <div className="flex gap-2">
        <button
          type="button"
          className="rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 disabled:cursor-not-allowed disabled:opacity-50"
          onClick={onPrev}
          disabled={page <= 1}
        >
          Previous
        </button>
        <button
          type="button"
          className="rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 disabled:cursor-not-allowed disabled:opacity-50"
          onClick={onNext}
          disabled={page >= pageCount}
        >
          Next
        </button>
      </div>
    </div>
  )
}
