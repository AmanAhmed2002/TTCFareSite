import { BrowserRouter, Routes, Route } from 'react-router-dom';
import NavBar from './components/NavBar.jsx';
import Footer from './components/footer.jsx';
import Home from './pages/Home.jsx';
import Tool from './pages/Tool.jsx';
import Install from './pages/Install.jsx';
import Chat from './pages/Chat.jsx'; // NEW

function NotFound() {
  return (
    <div className="container-narrow py-16">
      <h1 className="text-2xl font-bold mb-2">Page not found</h1>
      <p className="text-slate-600">The page you’re looking for doesn’t exist.</p>
    </div>
  );
}

export default function AppShell() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gradient-to-b from-white to-slate-50">
        <NavBar />
        <main>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/tool" element={<Tool />} />
            <Route path="/install" element={<Install />} />
            <Route path="/chat" element={<Chat />} /> {/* NEW */}
            <Route path="*" element={<NotFound />} />
          </Routes>
        </main>
        <Footer />
      </div>
    </BrowserRouter>
  );
}

