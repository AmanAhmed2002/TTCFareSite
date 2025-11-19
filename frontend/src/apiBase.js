// frontend/src/apiBase.js
const explicit = (import.meta.env.VITE_API_BASE || '').trim();

const isDev = import.meta.env.DEV;
// When built locally (vite preview), you’re on localhost:4173
const isPreviewLocalhost =
  !isDev && typeof window !== 'undefined' && window.location.hostname === 'localhost';

// Prefer explicit base if provided (works in dev, preview, prod)
// frontend/src/apiBase.js
export const API_BASE = "ttcbackend-gacpdqemdbgsgdg2.canadacentral-01.azurewebsites.net";


