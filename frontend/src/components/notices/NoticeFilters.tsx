import { type ChangeEvent } from 'react'

interface NoticeFiltersProps {
  stateOptions: string[]
  activeState: string
  searchText: string
  onStateChange: (nextState: string) => void
  onSearchChange: (next: string) => void
  onClear: () => void
}

export function NoticeFilters({
  stateOptions,
  activeState,
  searchText,
  onStateChange,
  onSearchChange,
  onClear,
}: NoticeFiltersProps) {
  const showClear = activeState !== '' || searchText.trim() !== ''

  const handleState = (e: ChangeEvent<HTMLSelectElement>) => {
    onStateChange(e.target.value)
  }

  const handleSearch = (e: ChangeEvent<HTMLInputElement>) => {
    onSearchChange(e.target.value)
  }

  return (
    <div className="mt-4 flex flex-wrap items-end gap-3">
      <div>
        <label className="sr-only" htmlFor="notice-state">
          State
        </label>
        <select
          id="notice-state"
          aria-label="State"
          className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm text-gray-900"
          value={activeState}
          onChange={handleState}
        >
          <option value="">All states</option>
          {stateOptions.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </div>

      <div className="flex-1 min-w-[240px]">
        <label className="sr-only" htmlFor="notice-search">
          Search employer, city or county
        </label>
        <input
          id="notice-search"
          type="search"
          aria-label="Search employer, city or county"
          placeholder="Search employer, city or county"
          className="h-9 w-full rounded-lg border border-gray-200 bg-white px-3 text-sm text-gray-900"
          value={searchText}
          onChange={handleSearch}
        />
      </div>

      {showClear ? (
        <button
          type="button"
          className="h-9 rounded-lg border border-gray-200 bg-white px-3 text-sm font-medium text-gray-700 hover:bg-gray-50"
          onClick={onClear}
        >
          Clear
        </button>
      ) : null}
    </div>
  )
}
