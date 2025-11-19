// backend/src/config.js
import 'dotenv/config';

export const PORT = process.env.PORT ? Number(process.env.PORT) : 4000;

// CORS: Only allow your frontend + local dev
export const CORS_ALLOWLIST = [
  "http://localhost:5173",
  "https://ttc-fare-site.vercel.app"
  // DO NOT add your Azure backend URL here
];

// Azure PostgreSQL connection string
export const DATABASE_URL = process.env.DATABASE_URL || null;
