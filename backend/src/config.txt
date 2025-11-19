import 'dotenv/config';

export const PORT = process.env.PORT ? Number(process.env.PORT) : 4000;

export const CORS_ALLOWLIST = (process.env.CORS_ALLOWLIST || 'http://localhost:5173')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

export const DATABASE_URL = process.env.DATABASE_URL || null;

