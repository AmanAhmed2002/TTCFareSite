import { Link, NavLink } from 'react-router-dom';
import { useEffect, useState } from 'react';

export default function NavBar() {
  const [dark, setDark] = useState(() => typeof window !== 'undefined' && localStorage.getItem('theme') === 'dark');

  useEffect(() => {
    const el = document.documentElement;
    if (dark) { el.classList.add('dark'); localStorage.setItem('theme', 'dark'); }
    else { el.classList.remove('dark'); localStorage.setItem('theme', 'light'); }
  }, [dark]);

  const linkCls = (isActive) =>
    [
      'inline-flex items-center rounded-xl px-3 py-2 text-sm font-medium transition-colors',
      'text-slate-700 dark:text-slate-100',
      'hover:bg-slate-100 dark:hover:bg-slate-800',
      isActive ? 'bg-brand-600 text-white hover:bg-brand-600' : '',
    ].filter(Boolean).join(' ');

  return (
    <header className="sticky top-0 z-40 border-b bg-white/95 text-slate-900 shadow-sm backdrop-blur supports-[backdrop-filter]:bg-white/75 dark:border-slate-700/60 dark:bg-slate-900/90 dark:text-slate-100">
      <div className="container-narrow h-14 flex items-center justify-between">
        <Link to="/" className="flex items-center gap-2 font-semibold">
          <span className="inline-flex h-6 w-6 items-center justify-center rounded-md bg-brand-600 text-white">OF</span>
          <span>One-Fare Helper</span>
        </Link>

        <nav className="flex items-center gap-1">
          <NavLink to="/" end className={({ isActive }) => linkCls(isActive)}>Home</NavLink>
          <NavLink to="/tool" className={({ isActive }) => linkCls(isActive)}>Open the Tool</NavLink>
          <NavLink to="/chat" className={({ isActive }) => linkCls(isActive)}>Transit Chat</NavLink>
          <NavLink to="/install" className={({ isActive }) => linkCls(isActive)}>Install app</NavLink>
          <button type="button" className="inline-flex items-center rounded-xl px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:text-slate-100 dark:hover:bg-slate-800 transition-colors"
            onClick={() => setDark((d) => !d)} aria-pressed={dark}
            title={dark ? 'Switch to light mode' : 'Switch to dark mode'}>
            {dark ? 'Light mode' : 'Dark mode'}
          </button>
        </nav>
      </div>
    </header>
  );
}

