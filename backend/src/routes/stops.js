import { Router } from 'express';
import { getPool } from '../db.js';

const router = Router();

router.get('/', async (req, res) => {
  const q = (req.query.query || '').toString().trim();
  const pool = getPool();
  if (!pool) {
    return res.status(501).json({ error: 'Stops search not configured (no DATABASE_URL).' });
  }
  if (!q) return res.json({ items: [] });

  // Simple case-insensitive search
  const { rows } = await pool.query(
    `SELECT stop_id, stop_name, stop_lat, stop_lon, agency
     FROM stops
     WHERE name ILIKE $1
     ORDER BY name ASC
     LIMIT 20`,
    [`%${q}%`]
  );
  return res.json({ items: rows });
});

export default router;

