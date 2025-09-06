// backend/src/lib/stopResolver.js
import { getPool } from '../db.js';

/**
 * If stopRef looks like an exact id (digits/alnum), return it directly.
 * Otherwise, try to find a matching stop_id by name (returns the first best match).
 */
export async function getStopId(agencyKey, stopRef) {
  if (!stopRef) return null;
  const s = String(stopRef).trim();

  // Basic "it looks like an id" heuristic
  if (/^[A-Za-z0-9_-]+$/.test(s)) return s;

  const pool = getPool();
  if (!pool) return null;

  const { rows } = await pool.query(
    `
    SELECT id
    FROM stops
    WHERE agency = $1 AND name ILIKE $2
    ORDER BY name ASC
    LIMIT 1
    `,
    [String(agencyKey).toUpperCase(), `%${s}%`]
  );
  return rows[0]?.id || null;
}

/**
 * Tokenize the free-text stop name to help disambiguate.
 * We look for "station/stn/subway", "go", "terminal/loop", and street-type tokens.
 */
function extractTokens(q) {
  const t = String(q || '').toLowerCase();
  const has = (re) => re.test(t);
  return {
    station: has(/\b(station|stn|subway)\b/),
    go: has(/\bgo\b/),
    terminal: has(/\b(terminal|loop|exchange)\b/),
    street: has(/\b(st|street|rd|road|ave|avenue|blvd|boulevard|dr|drive|ct|court|cres|crescent)\b/),
  };
}

function nameHasAnyToken(name, tok) {
  const s = String(name || '').toLowerCase();
  if (tok.station && /\b(station|stn|subway)\b/.test(s)) return true;
  if (tok.go && /\bgo\b/.test(s)) return true;
  if (tok.terminal && /\b(terminal|loop|exchange)\b/.test(s)) return true;
  if (tok.street && /\b(st|street|rd|road|ave|avenue|blvd|boulevard|dr|drive|ct|court|cres|crescent)\b/.test(s)) return true;
  return false;
}

/**
 * Return up to `max` candidate stop_ids matching the name for this agency.
 * Applies token-aware filtering so "Warden Station" does not match "Warden Ave".
 */
export async function findCandidateStopIds(agencyKey, nameLike, max = 12) {
  const q = String(nameLike || '').trim();
  if (!q) return [];

  const pool = getPool();
  if (!pool) return [];

  const sql = `
    SELECT id, name
    FROM stops
    WHERE agency = $1 AND name ILIKE $2
    ORDER BY name ASC
    LIMIT $3
  `;
  const { rows } = await pool.query(sql, [
    String(agencyKey).toUpperCase(),
    `%${q}%`,
    Math.max(1, Math.min(50, max)),
  ]);

  if (!rows?.length) return [];

  // Apply token filters only if the query clearly indicates a type
  const tok = extractTokens(q);
  const wantsType =
    tok.station || tok.go || tok.terminal || tok.street;

  let filtered = rows;
  if (wantsType) {
    const f = rows.filter(r => nameHasAnyToken(r.name, tok));
    if (f.length) filtered = f;
  }

  // Final pass: prioritize those with all query words present (loose)
  const words = q.toLowerCase().split(/\s+/).filter(Boolean);
  filtered.sort((a, b) => {
    const as = a.name.toLowerCase();
    const bs = b.name.toLowerCase();
    const ac = words.reduce((n,w)=> n + (as.includes(w)?1:0), 0);
    const bc = words.reduce((n,w)=> n + (bs.includes(w)?1:0), 0);
    if (ac !== bc) return bc - ac;
    return String(a.name).localeCompare(String(b.name));
  });

  return filtered.map(r => ({ id: r.id, name: r.name }));
}

