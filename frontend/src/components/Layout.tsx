import { Link, useLocation } from 'react-router-dom'
import { cn } from '@/lib/utils'

const NAV_LINKS = [
  { to: '/', label: 'Overview' },
  { to: '/notices', label: 'Notices' },
]

interface LayoutProps {
  children: React.ReactNode
}

export function Layout({ children }: LayoutProps) {
  const { pathname } = useLocation()

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
            <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8">
              <div className="flex items-center justify-between h-14">
                {/* Logo / brand */}
                <Link to="/" className="flex items-center gap-2">
                  <span className="text-red-600 font-bold text-lg leading-none">WARN</span>
                  <span className="text-gray-500 text-sm font-medium">Layoff Notice Tracker</span>
                </Link>

              {/* Nav */}
                <div className="flex items-center gap-3">
                  <nav className="flex items-center gap-1">
                    {NAV_LINKS.map(({ to, label }) => (
                  <Link
                    key={to}
                    to={to}
                    className={cn(
                      'px-3 py-1.5 rounded-md text-sm font-medium transition-colors',
                      pathname === to
                        ? 'bg-gray-100 text-gray-900'
                        : 'text-gray-500 hover:text-gray-900 hover:bg-gray-50',
                    )}
                  >
                    {label}
                  </Link>
                ))}
                  </nav>
                </div>
              </div>
            </div>
          </header>

      {/* Page content */}
      <main className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {children}
        <footer className="mt-6 text-sm text-gray-400">
          Data comes from state WARN Act notice lists.
        </footer>
      </main>
    </div>
  )
}
