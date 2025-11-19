// backend/src/db.js
import fs from 'fs';
import pkg from 'pg';
const { Pool } = pkg;

let pool = null;

export function getPool() {
  if (pool) return pool;

  const connectionString = process.env.DATABASE_URL;
  if (!connectionString) return null;

  const cfg = {
    connectionString,
    max: Number(process.env.DB_POOL_MAX || 5),
  };

  // SSL config: prefer explicit CA path, then CA contents, else default TLS
  const caPath = process.env.DATABASE_CA_CERT_PATH;
  const caText = process.env.DATABASE_CA_CERT;

  if (caPath && fs.existsSync(caPath)) {
    cfg.ssl = { ca: fs.readFileSync(caPath, 'utf8'), rejectUnauthorized: true };
  } else if (caText) {
    cfg.ssl = { ca: caText, rejectUnauthorized: true };
  } else {
    // Cockroach Cloud usually works with verify-full in the URL, but keep TLS on.
    cfg.ssl = { rejectUnauthorized: false };
  }

  // Create pool
  pool = new Pool(cfg);
  pool.on('error', (err) => console.error('[db] idle client error', err));

  // One-time visibility: where are we connecting?
  try {
    const u = new URL(connectionString);
    const host = u.hostname;
    const db   = u.pathname.replace(/^\//,'');
    console.log(`[db] pool created → host=${host} db=${db} ssl=${!!cfg.ssl}`);
  } catch {}

  // Fire-and-forget a tiny ping so errors show up in logs instead of being swallowed later
  pool.query('select current_database() as db, current_user as usr, version() as ver')
    .then(r => {
      const row = r.rows?.[0] || {};
      console.log(`[db] connected → db=${row.db} user=${row.usr}`);
    })
    .catch(e => {
      console.error('[db] connection test failed:', e.message);
    });

  return pool;
}

