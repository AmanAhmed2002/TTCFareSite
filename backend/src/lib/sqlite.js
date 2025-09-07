// backend/src/lib/sqlite.js
import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import Database from 'better-sqlite3';

let DB = null;

const SQLITE_URL = process.env.TTC_SQLITE_URL || '';
const TTL_MS = 24 * 60 * 60 * 1000; // 24h
const fileFor = (url) => path.join(os.tmpdir(), `ttc-${sha12(url)}.sqlite`);
function sha12(s) { return crypto.createHash('sha1').update(String(s)).digest('hex').slice(0,12); }

async function ensureFile() {
  if (!SQLITE_URL) throw new Error('TTC_SQLITE_URL not set');
  const file = fileFor(SQLITE_URL);
  try {
    const st = fs.statSync(file);
    if (st.size > 0 && (Date.now() - st.mtimeMs) < TTL_MS) return file;
  } catch {}
  const res = await fetch(SQLITE_URL);
  if (!res.ok) throw new Error(`download sqlite failed: ${res.status}`);
  const tmp = `${file}.downloading`;
  await fs.promises.mkdir(path.dirname(file), { recursive: true });
  await streamToFile(res.body, tmp);
  fs.renameSync(tmp, file);
  return file;
}
function streamToFile(readable, filename) {
  return new Promise((resolve, reject) => {
    const w = fs.createWriteStream(filename);
    readable.pipe(w);
    readable.on('error', reject);
    w.on('finish', resolve);
    w.on('error', reject);
  });
}

export async function openTtcSqlite() {
  if (DB) return DB;
  const file = await ensureFile();
  DB = new Database(file, { readonly: true, fileMustExist: true });
  return DB;
}

// --- helpers the adapters/schedule can use ---

export function expandStationStopIds(db, stopId) {
  const row = db.prepare('SELECT location_type, parent_station FROM stops WHERE stop_id = ?').get(String(stopId));
  if (row && Number(row.location_type) === 1) {
    const kids = db.prepare('SELECT stop_id FROM stops WHERE parent_station = ?').all(String(stopId));
    if (kids.length) return kids.map(x => String(x.stop_id));
  }
  return [String(stopId)];
}

export function routeShortNamesForRouteIds(db, routeIds) {
  if (!routeIds.length) return new Map();
  const q = `SELECT route_id, route_short_name, route_long_name FROM routes WHERE route_id IN (${routeIds.map(()=>'?').join(',')})`;
  const map = new Map();
  for (const r of db.prepare(q).all(...routeIds)) {
    const s = (r.route_short_name || r.route_long_name || '').trim();
    map.set(String(r.route_id), s);
  }
  return map;
}

export function mapTripIdsToRouteShort(db, tripIds) {
  if (!tripIds.length) return new Map();
  const q = `SELECT t.trip_id, t.route_id, r.route_short_name, r.route_long_name
             FROM trips t JOIN routes r USING(route_id)
             WHERE t.trip_id IN (${tripIds.map(()=>'?').join(',')})`;
  const map = new Map();
  for (const r of db.prepare(q).all(...tripIds)) {
    const s = (r.route_short_name || r.route_long_name || '').trim();
    map.set(String(r.trip_id), s);
  }
  return map;
}

