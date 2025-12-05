// frontend/src/apiBase.js

const explicit = (import.meta.env.VITE_API_BASE || '').trim();

export const API_BASE = import.meta.env.DEV ? '' : explicit;






/*
const explicit = (import.meta.env.VITE_API_BASE || '').trim();

const isDev = import.meta.env.DEV;
// When built locally (vite preview), you’re on localhost:4173
const isPreviewLocalhost =
  !isDev && typeof window !== 'undefined' && window.location.hostname === 'localhost';

// Prefer explicit base if provided (works in dev, preview, prod)
export const API_BASE = import.meta.env.VITE_API_BASE;



  explicit ||
  (isDev
    ? '' // dev uses Vite proxy to http://localhost:4000
    : isPreviewLocalhost
      ? 'http://localhost:4000'
      : ''); // otherwise require VITE_API_BASE to be set

*/