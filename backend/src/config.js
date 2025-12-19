// backend/src/config.js
import 'dotenv/config';

export const PORT = Number(process.env.PORT || 4000);

// Read CORS_ALLOWLIST from env as a comma-separated list
// Example in Azure:  https://ttc-fare-site.vercel.app,http://localhost:5173
const raw = process.env.CORS_ALLOWLIST || '';
export const CORS_ALLOWLIST = raw
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

export const DATABASE_URL = process.env.DATABASE_URL || null;
