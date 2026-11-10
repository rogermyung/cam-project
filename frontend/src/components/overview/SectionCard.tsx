import { type ReactNode } from 'react'

import { cn } from '@/lib/utils'

export function SectionCard({
  title,
  children,
  className,
}: {
  title: string
  children: ReactNode
  className?: string
}) {
  return (
    <section className={cn('bg-white rounded-lg border border-gray-200', className)}>
      <div className="p-6">
        <h2 className="text-sm font-semibold text-gray-900">{title}</h2>
        <div className="mt-4">{children}</div>
      </div>
    </section>
  )
}
