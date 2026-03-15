import { HashRouter, Routes, Route } from 'react-router-dom'
import { AlertsPage } from '@/pages/AlertsPage'
import { EntitiesPage } from '@/pages/EntitiesPage'
import { EntityPage } from '@/pages/EntityPage'
import { IndustriesPage } from '@/pages/IndustriesPage'

export default function App() {
  return (
    <HashRouter>
      <Routes>
        <Route path="/" element={<AlertsPage />} />
        <Route path="/entities" element={<EntitiesPage />} />
        <Route path="/entity/:id" element={<EntityPage />} />
        <Route path="/industries" element={<IndustriesPage />} />
      </Routes>
    </HashRouter>
  )
}
