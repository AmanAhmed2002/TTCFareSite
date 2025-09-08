// src/lib/sqlite.js
import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import Database from 'better-sqlite3';
import unzipper from 'unzipper';
import { parse } from 'csv-parse';
import { Readable } from 'stream';
import { pipeline } from 'stream/promises';

const SQLITE_URL   = process.env.TTC_SQLITE_URL || '';
const GTFS_ZIP_URL = process.env.TTC_GTFS_STATIC_URL || '';

const TTL_SQLITE_MS = 24 * 60 * 60 * 1000; // 24h cache
const TTL_ZIP_MS    = 6  * 60 * 60 * 1000; // 6h cache
const SQLITE_RETRY_DELAY_MS = 60_000;      // 60s between retries

let DB = null;
let sqliteReady = false;

const sha12 = (s) => crypto.createHash('sha1').update(String(s)).digest('hex').slice(0,12);
const sqlitePathFor = (url) => path.join(os.tmpdir(), `ttc-${sha12(url)}.sqlite`);
const zipPathFor    = (url) => path.join(os.tmpdir(), `ttc-gtfs-${sha12(url)}.zip`);

/* --------------------- robust HTTP download helpers --------------------- */

async function httpHeadSize(url) {
  // Ask for the first byte to elicit Content-Range: bytes 0-0/NNN (works behind redirects)
  const res = await fetch(url, { redirect: 'follow', headers: { Range: 'bytes=0-0' } });
  if (res.status === 206) {
    const cr = res.headers.get('content-range'); // e.g. "bytes 0-0/905793536"
    const total = cr && Number(cr.split('/')[1]);
    if (Number.isFinite(total)) return total;
  }
  // Fallback: plain GET headers
  const res2 = await fetch(url, { redirect: 'follow', method: 'HEAD' });
  const len = Number(res2.headers.get('content-length'));
  return Number.isFinite(len) ? len : null;
}

async function streamToFileAppend(res, filePath, flags = 'a') {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.part`;
  const nodeReadable = Readable.fromWeb(res.body);
  await pipeline(nodeReadable, fs.createWriteStream(tmp, { flags }));
  // Append tmp → final (atomic rename for append is not safe, do append manually)
  const dest = fs.createWriteStream(filePath, { flags: 'a' });
  await pipeline(fs.createReadStream(tmp), dest);
  fs.unlinkSync(tmp);
}

async function downloadWithResume(url, filePath, remoteSize) {
  let wrote = 0;
  try {
    const st = fs.statSync(filePath);
    wrote = st.size >>> 0;
  } catch {}

  // If we already have the full file and it's fresh enough, keep it.
  if (remoteSize && wrote === remoteSize) return;

  const maxAttempts = 8;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const headers = wrote ? { Range: `bytes=${wrote}-` } : {};
      const res = await fetch(url, { redirect: 'follow', headers });
      if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);

      // If server ignored Range and sends the whole file (200), start over
      if (res.status === 200 && wrote !== 0) {
        fs.unlinkSync(filePath);
        wrote = 0;
      }
      await fs.promises.mkdir(path.dirname(filePath), { recursive: true });

      // Stream directly to file (append)
      const tmp = `${filePath}.downloading`;
      const nodeReadable = Readable.fromWeb(res.body);
      await pipeline(nodeReadable, fs.createWriteStream(tmp, { flags: wrote ? 'a' : 'w' }));
      // Move/cat tmp to final
      if (wrote) {
        const out = fs.createWriteStream(filePath, { flags: 'a' });
        await pipeline(fs.createReadStream(tmp), out);
        fs.unlinkSync(tmp);
      } else {
        fs.renameSync(tmp, filePath);
      }

      // Verify size if known
      if (remoteSize) {
        const st2 = fs.statSync(filePath);
        if (st2.size >= remoteSize) return;
        wrote = st2.size >>> 0;
        continue; // loop to fetch remaining bytes
      } else {
        return;
      }
    } catch (e) {
      const backoff = Math.min(30_000 * attempt, 180_000);
      console.warn(`[ttc-sqlite] resume attempt ${attempt} failed: ${e.message}. Retrying in ${Math.round(backoff/1000)}s`);
      await new Promise(r => setTimeout(r, backoff));
    }
  }
  throw new Error('exhausted retries');
}

/* --------------------- ensure local cached files --------------------- */

async function ensureSqliteLocal() {
  if (!SQLITE_URL) throw new Error('TTC_SQLITE_URL not set');
  const file = sqlitePathFor(SQLITE_URL);

  // Freshness check
  try {
    const st = fs.statSync(file);
    if (st.size > 0 && (Date.now() - st.mtimeMs) < TTL_SQLITE_MS) return file;
  } catch {}

  console.log(`[ttc-sqlite] downloading: ${SQLITE_URL}`);
  const size = await httpHeadSize(SQLITE_URL);
  await downloadWithResume(SQLITE_URL, file, size || null);
  console.log(`[ttc-sqlite] saved → ${file}`);
  return file;
}

async function ensureZipLocal() {
  if (!GTFS_ZIP_URL) throw new Error('TTC_GTFS_STATIC_URL not set');
  const file = zipPathFor(GTFS_ZIP_URL);
  try {
    const st = fs.statSync(file);
    if (st.size > 0 && (Date.now() - st.mtimeMs) < TTL_ZIP_MS) return file;
  } catch {}
  console.log(`[ttc-gtfszip] downloading: ${GTFS_ZIP_URL}`);
  const res = await fetch(GTFS_ZIP_URL, { redirect: 'follow' });
  if (!res.ok || !res.body) throw new Error(`gtfs zip fetch failed: ${res.status}`);
  const nodeReadable = Readable.fromWeb(res.body);
  await fs.promises.mkdir(path.dirname(file), { recursive: true });
  await pipeline(nodeReadable, fs.createWriteStream(file));
  console.log(`[ttc-gtfszip] saved → ${file}`);
  return file;
}

async function openZipEntry(entryName) {
  const zipFile = await ensureZipLocal();
  const dir = await unzipper.Open.file(zipFile);
  const ent = dir.files.find(f => f.path.toLowerCase() === entryName.toLowerCase());
  if (!ent) throw new Error(`${entryName} not found in GTFS zip`);
  return ent.stream();
}

/* --------------------- SQLite open + status --------------------- */

export async function openTtcSqlite() {
  if (DB) return DB;
  const file = await ensureSqliteLocal();
  DB = new Database(file, { readonly: true, fileMustExist: true });
  sqliteReady = true;
  console.log(`[ttc-sqlite] opened`);
  return DB;
}

export function isSqliteReady() { return sqliteReady; }

/**
 * Start a background open that keeps retrying until success.
 * Never throws; logs failures and keeps trying.
 */
export async function primeSQLite() {
  const loop = async () => {
    for (;;) {
      try {
        await openTtcSqlite();
        return;
      } catch (e) {
        console.warn(`[ttc-sqlite] prime failed, will retry: ${e.message}`);
        await new Promise(r => setTimeout(r, SQLITE_RETRY_DELAY_MS));
      }
    }
  };
  loop(); // fire-and-forget
}

/* --------------------- Queries used by schedule/RT adapters --------------------- */

export function expandStationStopIds(db, stopId) {
  const row = db.prepare('SELECT location_type FROM stops WHERE stop_id = ?').get(String(stopId));
  if (row && Number(row.location_type) === 1) {
    const kids = db.prepare('SELECT stop_id FROM stops WHERE parent_station = ?').all(String(stopId));
    if (kids.length) return kids.map(x => String(x.stop_id));
  }
  return [String(stopId)];
}

export function mapTripIdsToRouteShort(db, tripIds) {
  if (!tripIds.length) return new Map();
  const q = `SELECT t.trip_id, r.route_short_name, r.route_long_name
             FROM trips t JOIN routes r USING(route_id)
             WHERE t.trip_id IN (${tripIds.map(()=>'?').join(',')})`;
  const m = new Map();
  for (const r of db.prepare(q).all(...tripIds)) {
    m.set(String(r.trip_id), (r.route_short_name || r.route_long_name || '').trim());
  }
  return m;
}

export function routeShortNamesForRouteIds(db, routeIds) {
  if (!routeIds.length) return new Map();
  const q = `SELECT route_id, route_short_name, route_long_name
             FROM routes WHERE route_id IN (${routeIds.map(()=>'?').join(',')})`;
  const m = new Map();
  for (const r of db.prepare(q).all(...routeIds)) {
    m.set(String(r.route_id), (r.route_short_name || r.route_long_name || '').trim());
  }
  return m;
}

/* --------------------- Light-weight fallbacks from GTFS zip --------------------- */

export async function loadRoutesMapFromZip() {
  const map = new Map();
  const s = await openZipEntry('routes.txt');
  await new Promise((resolve, reject) => {
    const parser = parse({ columns: true, trim: true });
    s.pipe(parser);
    parser.on('data', r => {
      const id = String(r.route_id || '').trim();
      const short = (r.route_short_name || r.route_long_name || '').trim();
      if (id && short) map.set(id, short);
    });
    parser.on('error', reject);
    parser.on('end', resolve);
  });
  return map;
}

export async function expandStationFromZip(stopId) {
  const id = String(stopId);
  let isStation = false;
  const kids = [];

  const s = await openZipEntry('stops.txt');
  await new Promise((resolve, reject) => {
    const parser = parse({ columns: true, trim: true });
    s.pipe(parser);
    parser.on('data', r => {
      const sid = String(r.stop_id || '').trim();
      if (!sid) return;
      const lt = Number(r.location_type || 0);
      const parent = String(r.parent_station || '').trim();
      if (sid === id && lt === 1) isStation = true;
      if (parent === id) kids.push(sid);
    });
    parser.on('error', reject);
    parser.on('end', resolve);
  });

  if (isStation && kids.length) return kids;
  return [id];
}

