// backend/src/lib/gtfsZipSchedule.js
// Low-RAM GTFS static (schedule) utilities using streaming unzip + CSV.
// Env: TTC_GTFS_STATIC_URL must point to the TTC static GTFS zip.

import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import unzipper from 'unzipper';
import { parse } from 'csv-parse';

const GTFS_URL = process.env.TTC_GTFS_STATIC_URL || '';
const ZIP_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const TZ = 'America/Toronto';

// --- tiny LRU for per-query results ---
const LRU = new Map();
const LRU_MAX = 80;
const LRU_TTL_MS = 60 * 1000;

function lruGet(key) {
  const v = LRU.get(key);
  if (!v) return null;
  if (Date.now() - v.t > LRU_TTL_MS) { LRU.delete(key); return null; }
  LRU.delete(key); LRU.set(key, v);
  return v.data;
}
function lruSet(key, data) {
  LRU.set(key, { t: Date.now(), data });
  while (LRU.size > LRU_MAX) LRU.delete(LRU.keys().next().value);
}

// --- basic helpers ---
function todayKeyLocal(d) {
  const z = toLocal(d, TZ);
  return `${z.getFullYear()}-${String(z.getMonth()+1).padStart(2,'0')}-${String(z.getDate()).padStart(2,'0')}`;
}
function toLocal(date, timeZone) {
  return new Date(date.toLocaleString('en-CA', { timeZone }));
}
function localMidnight(dateLocal) {
  return new Date(dateLocal.getFullYear(), dateLocal.getMonth(), dateLocal.getDate(), 0, 0, 0, 0);
}
function timeToSeconds(t) {
  const m = String(t || '').trim().match(/^(\d+):(\d{2})(?::(\d{2}))?$/);
  if (!m) return null;
  const h = parseInt(m[1],10), min = parseInt(m[2],10), s = parseInt(m[3]||'0',10);
  return h*3600 + min*60 + s; // HH>=24 allowed (next day)
}
function secondsToWhenLocal(dayStartLocal, sec) {
  return new Date(dayStartLocal.getTime() + sec*1000).toISOString();
}

async function ensureZipLocal() {
  if (!GTFS_URL) throw new Error('TTC_GTFS_STATIC_URL not set');
  const hash = crypto.createHash('sha1').update(GTFS_URL).digest('hex').slice(0,12);
  const file = path.join(os.tmpdir(), `ttc-gtfs-${hash}.zip`);
  try {
    const st = fs.statSync(file);
    if (st.size > 0 && (Date.now() - st.mtimeMs) < ZIP_TTL_MS) return file;
  } catch {}
  const res = await fetch(GTFS_URL);
  if (!res.ok) throw new Error(`GTFS zip fetch failed: ${res.status}`);
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

async function openEntry(zipPath, entryName) {
  const dir = await unzipper.Open.file(zipPath);
  const f = dir.files.find(x => x.path.toLowerCase() === entryName.toLowerCase());
  if (!f) throw new Error(`${entryName} not found in GTFS zip`);
  return f.stream();
}

async function streamCsv(entryStream, onRow) {
  return new Promise((resolve, reject) => {
    const parser = parse({ columns: true, relax_quotes: true, relax_column_count: true, trim: true });
    entryStream.pipe(parser);
    parser.on('data', onRow);
    parser.on('error', reject);
    parser.on('end', resolve);
  });
}

function serviceActiveTodaySets(calendarRows, calendarDatesRows, dateLocal) {
  const y = dateLocal.getFullYear();
  const m = dateLocal.getMonth() + 1;
  const d = dateLocal.getDate();
  const iso = `${y}${String(m).padStart(2,'0')}${String(d).padStart(2,'0')}`;

  const dow = dateLocal.getDay(); // 0..6, 0=Sun, local
  const dayCol = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'][dow];

  const active = new Set();
  for (const r of calendarRows) {
    if (r.start_date && r.end_date && iso >= r.start_date && iso <= r.end_date) {
      if (String(r[dayCol] || '0') === '1') active.add(r.service_id);
    }
  }
  for (const r of calendarDatesRows) {
    if (r.date !== iso || !r.service_id) continue;
    if (String(r.exception_type) === '1') active.add(r.service_id);
    if (String(r.exception_type) === '2') active.delete(r.service_id);
  }
  return active;
}

// ===== stops index for station→platform expansion (small, built once per zip) =====
let STOPS_INDEX = null;
/**
 * Builds:
 *  - idToType: stop_id -> Number(location_type)
 *  - stationChildren: station_id -> string[] child stop_ids
 */
async function buildStopsIndex(zipPath) {
  if (STOPS_INDEX) return STOPS_INDEX;
  const idToType = new Map();
  const stationChildren = new Map();

  await streamCsv(await openEntry(zipPath, 'stops.txt'), (r) => {
    const id = String(r.stop_id || '').trim();
    if (!id) return;
    const lt = Number(r.location_type || 0);
    idToType.set(id, lt);
    const parent = String(r.parent_station || '').trim();
    if (parent) {
      const arr = stationChildren.get(parent) || [];
      arr.push(id);
      stationChildren.set(parent, arr);
    }
  });

  STOPS_INDEX = { idToType, stationChildren };
  return STOPS_INDEX;
}

/**
 * Expand a station stop_id to its platform stop_ids.
 * If stop_id is not a station or unknown, returns [stop_id].
 */
export async function expandStopIdsIfStationFromZip(stopId) {
  const zipPath = await ensureZipLocal();
  const { idToType, stationChildren } = await buildStopsIndex(zipPath);
  const id = String(stopId);
  const lt = idToType.get(id);
  if (lt === 1) {
    const kids = stationChildren.get(id);
    if (kids?.length) return kids.slice();
  }
  return [id];
}

// ===== schedule fallback: next departures at stop(s) =====
export async function nextArrivalsFromZipStreaming({ stopIds = [], routeRef = null, limit = 10, now = new Date(), horizonMin = 360 } = {}) {
  if (!GTFS_URL) throw new Error('GTFS url missing');
  if (!stopIds.length) return [];

  const key = `arr:${todayKeyLocal(now)}:${stopIds.slice().sort().join(',')}:${routeRef || ''}:${limit}:${horizonMin}`;
  const cached = lruGet(key);
  if (cached) return cached;

  const zipPath = await ensureZipLocal();

  // calendar + dates (small)
  const calendarRows = [];
  const calendarDatesRows = [];
  await streamCsv(await openEntry(zipPath, 'calendar.txt'), (r)=> calendarRows.push(r));
  try {
    await streamCsv(await openEntry(zipPath, 'calendar_dates.txt'), (r)=> calendarDatesRows.push(r));
  } catch {}

  const nowLocal = toLocal(now, TZ);
  const dayStartLocal = localMidnight(nowLocal);
  const nowDaySec = nowLocal.getHours()*3600 + nowLocal.getMinutes()*60 + nowLocal.getSeconds();
  const horizonSec = nowDaySec + horizonMin*60;

  const activeServices = serviceActiveTodaySets(calendarRows, calendarDatesRows, nowLocal);

  // stop_times slice for our stopIds within horizon
  const wantedTrips = new Map(); // trip_id -> { depSec, stop_id }
  await streamCsv(await openEntry(zipPath, 'stop_times.txt'), (r) => {
    const sid = String(r.stop_id || '').trim();
    if (!sid || !stopIds.includes(sid)) return;
    const depSec = timeToSeconds(r.departure_time);
    if (depSec == null) return;
    if (depSec < nowDaySec || depSec > horizonSec) return;
    const tid = String(r.trip_id || '').trim();
    if (!tid) return;
    const prev = wantedTrips.get(tid);
    if (!prev || depSec < prev.depSec) wantedTrips.set(tid, { depSec, stop_id: sid });
  });
  if (wantedTrips.size === 0) { lruSet(key, []); return []; }

  // trips for those trip_ids (and active today)
  const wantedTripIds = new Set(wantedTrips.keys());
  const tripInfo = new Map(); // trip_id -> { route_id, headsign }
  const wantedRouteIds = new Set();
  await streamCsv(await openEntry(zipPath, 'trips.txt'), (r) => {
    const tid = String(r.trip_id || '').trim();
    if (!tid || !wantedTripIds.has(tid)) return;
    const svc = String(r.service_id || '').trim();
    if (!svc || !activeServices.has(svc)) return;
    const rid = String(r.route_id || '').trim();
    if (!rid) return;
    tripInfo.set(tid, { route_id: rid, headsign: r.trip_headsign || '' });
    wantedRouteIds.add(rid);
  });
  if (tripInfo.size === 0) { lruSet(key, []); return []; }

  // routes -> short_name
  const routeNames = new Map();
  await streamCsv(await openEntry(zipPath, 'routes.txt'), (r) => {
    const rid = String(r.route_id || '').trim();
    if (!rid || !wantedRouteIds.has(rid)) return;
    routeNames.set(rid, (r.route_short_name || r.route_long_name || '').trim());
  });

  const want = routeRef ? String(routeRef).toLowerCase() : null;
  const out = [];
  for (const [tid, meta] of wantedTrips) {
    const ti = tripInfo.get(tid);
    if (!ti) continue;
    const short = routeNames.get(ti.route_id) || '';
    if (want) {
      const s = short.toLowerCase();
      if (!(s === want || s.startsWith(want))) continue; // allow 83/83A
    }
    out.push({
      routeShortName: short,
      headsign: ti.headsign || '',
      when: secondsToWhenLocal(dayStartLocal, meta.depSec),
      realtime: false
    });
  }

  out.sort((a,b)=> new Date(a.when) - new Date(b.when));
  const capped = out.slice(0, Math.max(1, limit));
  lruSet(key, capped);
  return capped;
}

// ===== which lines serve these stop(s) within a window =====
export async function linesAtStopFromZipStreaming({ stopIds = [], windowMin = 1440, now = new Date() } = {}) {
  if (!GTFS_URL) throw new Error('GTFS url missing');
  if (!stopIds.length) return [];

  const key = `lines:${todayKeyLocal(now)}:${stopIds.slice().sort().join(',')}:${windowMin}`;
  const cached = lruGet(key);
  if (cached) return cached;

  const zipPath = await ensureZipLocal();

  // calendar
  const calendarRows = [];
  const calendarDatesRows = [];
  await streamCsv(await openEntry(zipPath, 'calendar.txt'), (r)=> calendarRows.push(r));
  try {
    await streamCsv(await openEntry(zipPath, 'calendar_dates.txt'), (r)=> calendarDatesRows.push(r));
  } catch {}

  const nowLocal = toLocal(now, TZ);
  const nowDaySec = nowLocal.getHours()*3600 + nowLocal.getMinutes()*60 + nowLocal.getSeconds();
  const horizonSec = nowDaySec + windowMin*60;
  const activeServices = serviceActiveTodaySets(calendarRows, calendarDatesRows, nowLocal);

  const wantedTripIds = new Set();
  await streamCsv(await openEntry(zipPath, 'stop_times.txt'), (r) => {
    const sid = String(r.stop_id || '').trim();
    if (!sid || !stopIds.includes(sid)) return;
    const depSec = timeToSeconds(r.departure_time);
    if (depSec == null) return;
    if (depSec < nowDaySec || depSec > horizonSec) return;
    const tid = String(r.trip_id || '').trim();
    if (tid) wantedTripIds.add(tid);
  });
  if (wantedTripIds.size === 0) { lruSet(key, []); return []; }

  const wantedRouteIds = new Set();
  await streamCsv(await openEntry(zipPath, 'trips.txt'), (r) => {
    const tid = String(r.trip_id || '').trim();
    if (!tid || !wantedTripIds.has(tid)) return;
    const svc = String(r.service_id || '').trim();
    if (!svc || !activeServices.has(svc)) return;
    const rid = String(r.route_id || '').trim();
    if (rid) wantedRouteIds.add(rid);
  });

  const shortNames = new Set();
  await streamCsv(await openEntry(zipPath, 'routes.txt'), (r) => {
    const rid = String(r.route_id || '').trim();
    if (!rid || !wantedRouteIds.has(rid)) return;
    const s = (r.route_short_name || r.route_long_name || '').trim();
    if (s) shortNames.add(s);
  });

  const out = Array.from(shortNames).sort((a,b)=> String(a).localeCompare(String(b), undefined, { numeric:true }));
  lruSet(key, out);
  return out;
}

