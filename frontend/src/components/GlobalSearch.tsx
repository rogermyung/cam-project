import { useEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { fetchEntities } from '@/lib/data'
import type { EntitySummary } from '@/types'
import { pct } from '@/lib/utils'

export function GlobalSearch() {
  const [query, setQuery] = useState('')
  const [entities, setEntities] = useState<EntitySummary[]>([])
  const [open, setOpen] = useState(false)
  const [activeIdx, setActiveIdx] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const navigate = useNavigate()

  // Load entity list once
  useEffect(() => {
    fetchEntities()
      .then(setEntities)
      .catch(() => {/* silently ignore in header context */})
  }, [])

  // '/' keyboard shortcut focuses the search input
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      const tag = (e.target as HTMLElement).tagName
      if (e.key === '/' && tag !== 'INPUT' && tag !== 'TEXTAREA') {
        e.preventDefault()
        inputRef.current?.focus()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  const results = query.trim().length > 0
    ? entities
        .filter((e) =>
          e.canonical_name.toLowerCase().includes(query.toLowerCase()) ||
          (e.ticker ?? '').toLowerCase().includes(query.toLowerCase()),
        )
        .slice(0, 10)
    : []

  function handleInputChange(val: string) {
    setQuery(val)
    setActiveIdx(0)
    setOpen(val.trim().length > 0)
  }

  function selectResult(entity: EntitySummary) {
    setQuery('')
    setOpen(false)
    navigate(`/entity/${entity.id}`)
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (!open || results.length === 0) {
      if (e.key === 'Escape') {
        setQuery('')
        inputRef.current?.blur()
      }
      return
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActiveIdx((i) => Math.min(i + 1, results.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActiveIdx((i) => Math.max(i - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      selectResult(results[activeIdx])
    } else if (e.key === 'Escape') {
      setOpen(false)
      setQuery('')
      inputRef.current?.blur()
    }
  }

  return (
    <div className="relative">
      <div className="relative flex items-center">
        <input
          ref={inputRef}
          type="search"
          value={query}
          onChange={(e) => handleInputChange(e.target.value)}
          onKeyDown={handleKeyDown}
          onFocus={() => { if (query.trim()) setOpen(true) }}
          onBlur={() => setTimeout(() => setOpen(false), 150)}
          placeholder="Search entities…"
          aria-label="Search entities (press / to focus)"
          className="h-7 w-44 rounded-md border border-gray-200 bg-gray-50 px-2 pr-8 text-xs text-gray-700 placeholder-gray-400 focus:outline-none focus:ring-1 focus:ring-indigo-400 focus:bg-white transition-all"
        />
        {/* / hint badge */}
        {!query && (
          <kbd className="pointer-events-none absolute right-2 select-none rounded border border-gray-200 bg-white px-1 text-[10px] text-gray-400">
            /
          </kbd>
        )}
      </div>

      {open && results.length > 0 && (
        <ul
          role="listbox"
          className="absolute right-0 top-full z-50 mt-1 w-72 rounded-md border border-gray-200 bg-white shadow-lg py-1 text-sm"
        >
          {results.map((entity, idx) => (
            <li
              key={entity.id}
              role="option"
              aria-selected={idx === activeIdx}
              onMouseDown={() => selectResult(entity)}
              onMouseEnter={() => setActiveIdx(idx)}
              className={`flex items-center justify-between px-3 py-2 cursor-pointer ${
                idx === activeIdx ? 'bg-indigo-50 text-indigo-900' : 'text-gray-700 hover:bg-gray-50'
              }`}
            >
              <span className="truncate font-medium">{entity.canonical_name}</span>
              <span className="ml-2 shrink-0 flex items-center gap-2 text-xs text-gray-400">
                {entity.ticker && <span className="font-mono">{entity.ticker}</span>}
                {entity.composite_score != null && <span>{pct(entity.composite_score)}</span>}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
