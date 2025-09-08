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

// ---------- env & TTLs ----------
const SQLITE_URL   = process.env.TTC_SQLITE_URL || '';
const GTFS_ZIP_URL = process.env.TTC_GTFS_STATIC_URL || '';
const TTL_SQLITE_MS = 24 * 60 * 60 * 1000; // 24h
const TTL_ZIP_MS    = 6  * 60 * 60 * 1000; // 6h

// ---------- state ----------
let DB = null;
let sqliteReady = false;

// ---------- small utils ----------
const sha12 = (s) => crypto.createHash('sha1').update(String(s)).digest('hex').slice(0,12);
const sqlitePathFor = (url) => path.join(os.tmpdir(), `ttc-${sha12(url)}.sqlite`);
const zipPathFor    = (url) => path.join(os.tmpdir(), `ttc-gtfs-${sha12(url)}.zip`);

async function streamHttpToFile(res, filePath) {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.downloading`;
  const nodeReadable = Readable.fromWeb(res.body);
  await pipeline(nodeReadable, fs.createWriteStream(tmp));
  fs.renameSync(tmp, filePath);
}

// ---------- ensure local SQLite file (no buffering) ----------
async function ensureSqliteLocal() {
  if (!SQLITE_URL) throw new Error('TTC_SQLITE_URL not set');
  const file = sqlitePathFor(SQLITE_URL);
  try {
    const st = fs.statSync(file);
    if (st.size > 0 && (Date.now() - st.mtimeMs) < TTL_SQLITE_MS) return file;
  } catch {}
  console.log(`[ttc-sqlite] downloading: ${SQLITE_URL}`);
  const res = await fetch(SQLITE_URL, { redirect: 'follow' });
  if (!res.ok || !res.body) throw new Error(`download sqlite failed: ${res.status}`);
  await streamHttpToFile(res, file);
  console.log(`[ttc-sqlite] saved → ${file}`);
  return file;
}

// ---------- public: open sqlite (file-backed, low RAM) ----------
export async function openTtcSqlite() {
  if (DB) return DB;
  const file = await ensureSqliteLocal();
  DB = new Database(file, { readonly: true, fileMustExist: true });
  sqliteReady = true;
  console.log(`[ttc-sqlite] opened`);
  return DB;
}

export function isSqliteReady() {
  return sqliteReady;
}

// Kick off background download/open at boot (non-blocking)
export async function primeSQLite() {
  try {
    await openTtcSqlite();
  } catch (e) {
    console.warn(`[ttc-sqlite] prime failed: ${e.message}`);
  }
}

// ---------- tiny queries used by adapters/schedule ----------
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

// ---------- GTFS zip: cache to /tmp and stream entries (no buffering) ----------
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
  await streamHttpToFile(res, file);
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

// Build Map(route_id → short_name) by streaming routes.txt
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

// Expand station to platform ids by streaming stops.txt
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

