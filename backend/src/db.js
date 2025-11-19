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
    ssl: { rejectUnauthorized: false } // Azure Postgres SSL
  };

  pool = new Pool(cfg);

  pool.on('error', (err) => console.error('[db] idle client error', err));

  // optional: quick ping
  pool.query('SELECT current_database() as db, current_user as usr')
    .then(r => {
      const row = r.rows?.[0] || {};
      console.log(`[db] connected → db=${row.db} user=${row.usr}`);
    })
    .catch(e => {
      console.error('[db] connection test failed:', e.message);
    });

  return pool;
}
