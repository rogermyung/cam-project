import { HashRouter, Routes, Route } from 'react-router-dom'
import { OverviewPage } from '@/pages/OverviewPage'
import { NoticesPage } from '@/pages/NoticesPage'
import { CfpbPage } from '@/pages/CfpbPage'

export default function App() {
  return (
    <HashRouter>
      <Routes>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/notices" element={<NoticesPage />} />
        <Route path="/cfpb" element={<CfpbPage />} />
        <Route path="*" element={<OverviewPage />} />
      </Routes>
    </HashRouter>
  )
}
