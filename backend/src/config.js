// backend/src/config.js
import 'dotenv/config';

export const PORT = Number(process.env.PORT || 4000);

// Azure PostgreSQL (set in Azure + in local .env)
export const DATABASE_URL = process.env.DATABASE_URL ?? null;

// CORS_ALLOWLIST env format (Azure):
//   CORS_ALLOWLIST=https://ttc-fare-site.vercel.app,http://localhost:5173
export const CORS_ALLOWLIST = (process.env.CORS_ALLOWLIST || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

