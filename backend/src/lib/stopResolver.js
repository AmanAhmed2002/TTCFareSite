// backend/src/lib/stopResolver.js
import { getPool } from '../db.js';

/** If the string already looks like an id, just return it. */
export async function getStopId(agencyKey, stopRef) {
  if (!stopRef) return null;
  const s = String(stopRef).trim();
  if (!s) return null;

  const pool = getPool();
  if (!pool) return null;

  const ag = String(agencyKey || '').toUpperCase();

  // ✅ If it's digits, treat as TTC stop number (stop_code) first
  if (/^\d{3,6}$/.test(s)) {
    try {
      const { rows } = await pool.query(
        `SELECT id FROM stops WHERE agency=$1 AND stop_code=$2 LIMIT 1`,
        [ag, s]
      );
      if (rows?.[0]?.id) return String(rows[0].id);
    } catch {
      // if stop_code column doesn't exist or query fails, ignore and fall through
    }
    // If not found by stop_code, it might actually be a stop_id; allow it
    return s;
  }

  // If it looks like an id (non-numeric tokens), return it
  if (/^[A-Za-z0-9_-]+$/.test(s)) return s;

  // Otherwise fuzzy lookup by name
  const cands = await findCandidateStopIds(agencyKey, s, 8);
  return cands[0]?.id || null;
}


/** Return candidate stop ids by name (token-aware). */
export async function findCandidateStopIds(agencyKey, nameLike, max = 12) {
  const q = String(nameLike || '').trim();
  if (!q) return [];

  const pool = getPool(); if (!pool) return [];
  const ag = String(agencyKey || '').toUpperCase();

  const { rows } = await pool.query(
    `SELECT id, name, coalesce(location_type,0) AS location_type
     FROM stops
     WHERE agency=$1 AND name ILIKE $2
     LIMIT 200`,
    [ag, `%${q}%`]
  );

  const tokens = q
    .toLowerCase()
    .replace(/[–—-]/g, ' ')
    .replace(/\bstn\b/g, 'station')
    .split(/\s+/)
    .filter(Boolean);

  const filtered = rows.filter(r => {
    const n = String(r.name || '').toLowerCase();
    return tokens.every(t => n.includes(t));
  });

  const wantsStationish = /\bstation\b/i.test(q);

  // ✅ If user said "station", and we have any "platform" rows, prefer ONLY those.
  let poolRows = filtered;
  if (wantsStationish) {
    const platforms = filtered.filter(r => /\bplatform\b/i.test(String(r.name)));
    if (platforms.length) poolRows = platforms;
  }

  const scored = poolRows
    .map(r => ({ ...r, _score: wantsStationish ? stationScore(r) : defaultScore(r, q) }))
    .sort((a, b) => b._score - a._score || String(a.name).localeCompare(String(b.name)));

  return scored.slice(0, max).map(r => ({ id: String(r.id), name: String(r.name) }));
}

function stationScore(r) {
  const name = String(r.name || '').toLowerCase();
  let s = 0;
  if (/\bplatform\b/.test(name)) s += 10;               // stronger bias to platforms
  if (/\b(east|west|north|south)bound\b/.test(name)) s += 3;
  if (Number(r.location_type) !== 1) s += 1;            // platforms often 0
  return s;
}
function defaultScore(r, q) {
  const name = String(r.name || '');
  let s = 0;
  if (name.toLowerCase().startsWith(q.toLowerCase())) s += 2;
  s += Math.max(0, 40 - Math.abs(name.length - q.length)) / 40;
  return s;
}

