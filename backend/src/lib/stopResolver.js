// backend/src/lib/stopResolver.js
import { getPool } from '../db.js';
import { openTtcSqlite, isSqliteReady } from './sqlite.js';

// Heuristic: if it looks like a raw GTFS stop_id, just return it.
function looksLikeId(s) {
  return /^[A-Za-z0-9_-]+$/.test(String(s || '').trim());
}

/**
 * Find candidate stop ids by name for a given agency.
 * Returns array of { id, name } sorted by relevance.
 * Order of attempts:
 *   1) Postgres (Neon) if configured
 *   2) TTC-only: SQLite stops table (file-backed, low-RAM)
 */
export async function findCandidateStopIds(agencyKey, stopRef, { limit = 10 } = {}) {
  if (!stopRef) return [];
  const q = String(stopRef).trim();
  if (looksLikeId(q)) return [{ id: q, name: q }];

  const agency = String(agencyKey || '').toLowerCase();

  // (1) Try your Postgres 'stops' table if available
  const pool = getPool();
  if (pool) {
    const { rows } = await pool.query(
      `SELECT id, name
       FROM stops
       WHERE agency = $1 AND name ILIKE $2
       ORDER BY name ASC
       LIMIT $3`,
      [agency, `%${q}%`, Math.max(1, limit)]
    );
    if (rows?.length) return rows.map(r => ({ id: r.id, name: r.name }));
  }

  // (2) TTC fallback via SQLite (file-backed) if ready
  if (agency === 'ttc' && isSqliteReady()) {
    const db = await openTtcSqlite();

    // Simple token LIKE match; prefer station rows (location_type=1)
    const tokens = q.toLowerCase().split(/\s+/).filter(Boolean);
    const like = `%${tokens.join('%')}%`;

    const rows = db.prepare(
      `SELECT stop_id AS id, stop_name AS name, location_type
       FROM stops
       WHERE lower(stop_name) LIKE ?
       ORDER BY (CASE WHEN location_type = 1 THEN 0 ELSE 1 END), stop_name
       LIMIT ?`
    ).all(like, Math.max(1, limit));

    if (rows?.length) return rows.map(r => ({ id: String(r.id), name: r.name }));
  }

  return [];
}

/** Single best stop id or null. */
export async function getStopId(agencyKey, stopRef) {
  const cands = await findCandidateStopIds(agencyKey, stopRef, { limit: 1 });
  return cands[0]?.id || null;
}

