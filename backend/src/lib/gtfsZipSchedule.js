// backend/lib/gtfsZipSchedule.js
// Low-RAM schedule fallback by streaming the TTC GTFS ZIP on demand.
// Requires: npm i unzipper csv-parse
//
// Uses process.env.TTC_GTFS_STATIC_URL (public GTFS static zip URL).
// Caches the downloaded zip in os.tmpdir() for 24h, and caches per-stop results briefly in memory.

import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import unzipper from 'unzipper';
import { parse } from 'csv-parse';

const GTFS_URL = process.env.TTC_GTFS_STATIC_URL || '';
const ZIP_TTL_MS = 24 * 60 * 60 * 1000; // 24h on-disk cache

// very small per-process cache (stop+route+horizon -> results) to avoid rescans on bursts
const LRU = new Map();
const LRU_MAX = 50;
const LRU_TTL_MS = 60 * 1000; // 60s

function lruGet(key) {
  const v = LRU.get(key);
  if (!v) return null;
  if (Date.now() - v.t > LRU_TTL_MS) { LRU.delete(key); return null; }
  // touch
  LRU.delete(key); LRU.set(key, v);
  return v.data;
}
function lruSet(key, data) {
  LRU.set(key, { t: Date.now(), data });
  while (LRU.size > LRU_MAX) {
    const k = LRU.keys().next().value;
    LRU.delete(k);
  }
}

function todayKey(d) {
  const z = new Date(d);
  return `${z.getUTCFullYear()}-${String(z.getUTCMonth()+1).padStart(2,'0')}-${String(z.getUTCDate()).padStart(2,'0')}`;
}

async function ensureZipLocal() {
  if (!GTFS_URL) throw new Error('TTC_GTFS_STATIC_URL not set');
  const hash = crypto.createHash('sha1').update(GTFS_URL).digest('hex').slice(0,12);
  const file = path.join(os.tmpdir(), `ttc-gtfs-${hash}.zip`);
  try {
    const st = fs.statSync(file);
    if (Date.now() - st.mtimeMs < ZIP_TTL_MS && st.size > 0) return file;
  } catch {}
  // download
  const r = await fetch(GTFS_URL);
  if (!r.ok) throw new Error(`GTFS zip fetch failed: ${r.status}`);
  const tmp = `${file}.downloading`;
  await fs.promises.mkdir(path.dirname(file), { recursive: true });
  const w = fs.createWriteStream(tmp);
  await new Promise((resolve, reject) => {
    r.body.pipe(w);
    r.body.on('error', reject);
    w.on('finish', resolve);
    w.on('error', reject);
  });
  fs.renameSync(tmp, file);
  return file;
}

async function openEntry(zipPath, entryName) {
  const d = await unzipper.Open.file(zipPath);
  const e = d.files.find(f => f.path.toLowerCase() === entryName.toLowerCase());
  if (!e) throw new Error(`${entryName} not found in GTFS zip`);
  return e.stream();
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

function serviceActiveTodaySets(calendarRows, calendarDatesRows, theDate) {
  // Build a set of service_ids active on theDate (UTC date).
  const active = new Set();
  const y = theDate.getUTCFullYear();
  const m = theDate.getUTCMonth() + 1;
  const d = theDate.getUTCDate();
  const iso = `${y}${String(m).padStart(2,'0')}${String(d).padStart(2,'0')}`;

  const dow = theDate.getUTCDay(); // 0..6, 0=Sun
  const dayCol = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'][dow];

  for (const r of calendarRows) {
    if (r.start_date && r.end_date && iso >= r.start_date && iso <= r.end_date) {
      if (String(r[dayCol] || '0') === '1') active.add(r.service_id);
    }
  }
  // exceptions
  for (const r of calendarDatesRows) {
    if (!r.service_id || !r.date) continue;
    if (r.date !== iso) continue;
    if (String(r.exception_type) === '1') active.add(r.service_id);
    if (String(r.exception_type) === '2') active.delete(r.service_id);
  }
  return active;
}

function timeToSeconds(t) {
  // GTFS HH:MM:SS possibly >= 24:00:00 (next day)
  const m = String(t || '').trim().match(/^(\d+):(\d{2})(?::(\d{2}))?$/);
  if (!m) return null;
  const h = parseInt(m[1],10), min = parseInt(m[2],10), s = parseInt(m[3]||'0',10);
  return h*3600 + min*60 + s;
}

function secondsToWhen(tsSeconds, baseDate) {
  // build Date from today's midnight UTC + tsSeconds
  const dayStart = Date.UTC(baseDate.getUTCFullYear(), baseDate.getUTCMonth(), baseDate.getUTCDate(), 0, 0, 0);
  return new Date(dayStart + tsSeconds*1000).toISOString();
}

// ---- Public: schedule fallback (next departures) ----
export async function nextArrivalsFromZipStreaming({ gtfsUrl = GTFS_URL, stopIds = [], routeRef = null, limit = 10, now = new Date(), horizonMin = 360 }) {
  if (!gtfsUrl) throw new Error('GTFS url missing');
  if (!stopIds.length) return [];

  const key = `arr:${todayKey(now)}:${stopIds.sort().join(',')}:${routeRef || ''}:${limit}:${horizonMin}`;
  const cached = lruGet(key);
  if (cached) return cached;

  const zipPath = await ensureZipLocal();

  // 1) read calendar + calendar_dates (small)
  const calendarRows = [];
  const calendarDatesRows = [];
  await streamCsv(await openEntry(zipPath, 'calendar.txt'), (r)=> calendarRows.push(r));
  await streamCsv(await openEntry(zipPath, 'calendar_dates.txt').catch(()=>Promise.resolve({pipe:()=>{}})), (r)=> calendarDatesRows.push(r)).catch(()=>{}); // optional
  const activeServices = serviceActiveTodaySets(calendarRows, calendarDatesRows, now);

  // 2) scan stop_times for rows matching our stopIds and within horizon
  const nowDaySec = ((now.getUTCHours()*60)+now.getUTCMinutes())*60 + now.getUTCSeconds();
  const horizonSec = nowDaySec + horizonMin*60;

  const wantedTrips = new Map(); // trip_id -> { depSec, stop_id, pickup_type, drop_off_type }
  await streamCsv(await openEntry(zipPath, 'stop_times.txt'), (r) => {
    const sid = r.stop_id;
    if (!sid || !stopIds.includes(String(sid))) return;
    const depSec = timeToSeconds(r.departure_time);
    if (depSec == null) return;
    if (depSec < nowDaySec || depSec > horizonSec) return;
    const trip_id = r.trip_id;
    if (!trip_id) return;
    // Keep earliest departure per trip at this stop
    const prev = wantedTrips.get(trip_id);
    if (!prev || depSec < prev.depSec) wantedTrips.set(trip_id, { depSec, stop_id: sid, headsign: null });
  });

  if (wantedTrips.size === 0) { lruSet(key, []); return []; }

  const wantedTripIds = new Set(wantedTrips.keys());
  const tripInfo = new Map(); // trip_id -> { route_id, service_id, headsign }
  const wantedRouteIds = new Set();

  // 3) read trips for those trip_ids only
  await streamCsv(await openEntry(zipPath, 'trips.txt'), (r) => {
    const tid = r.trip_id;
    if (!tid || !wantedTripIds.has(tid)) return;
    const route_id = r.route_id;
    const service_id = r.service_id;
    const headsign = r.trip_headsign || '';
    if (!route_id || !service_id) return;
    if (!activeServices.has(service_id)) return; // not today
    tripInfo.set(tid, { route_id, headsign });
    wantedRouteIds.add(route_id);
  });

  if (tripInfo.size === 0) { lruSet(key, []); return []; }

  // 4) map route_id -> route_short_name/long_name
  const routeNames = new Map();
  await streamCsv(await openEntry(zipPath, 'routes.txt'), (r) => {
    const id = r.route_id;
    if (!id || !wantedRouteIds.has(id)) return;
    routeNames.set(id, { short: r.route_short_name || '', long: r.route_long_name || '' });
  });

  // 5) build results, apply optional routeRef filter, sort & cap
  const out = [];
  for (const [trip_id, meta] of wantedTrips) {
    const ti = tripInfo.get(trip_id);
    if (!ti) continue;
    const name = routeNames.get(ti.route_id) || { short: '', long: '' };
    const routeShort = (name.short || name.long || '').trim();
    if (routeRef && String(routeShort).toLowerCase() !== String(routeRef).toLowerCase()) continue;
    out.push({
      routeShortName: routeShort,
      headsign: ti.headsign || '',
      when: secondsToWhen(meta.depSec, now),
      realtime: false
    });
  }
  out.sort((a,b)=> new Date(a.when) - new Date(b.when));
  const capped = out.slice(0, Math.max(1, limit));
  lruSet(key, capped);
  return capped;
}

// ---- Public: enumerate routes serving stop today (chips) ----
export async function linesAtStopFromZipStreaming({ gtfsUrl = GTFS_URL, stopIds = [], windowMin = 1440, now = new Date() }) {
  if (!gtfsUrl) throw new Error('GTFS url missing');
  if (!stopIds.length) return [];

  const key = `lines:${todayKey(now)}:${stopIds.sort().join(',')}:${windowMin}`;
  const cached = lruGet(key);
  if (cached) return cached;

  const zipPath = await ensureZipLocal();

  // calendar
  const calendarRows = [];
  const calendarDatesRows = [];
  await streamCsv(await openEntry(zipPath, 'calendar.txt'), (r)=> calendarRows.push(r));
  await streamCsv(await openEntry(zipPath, 'calendar_dates.txt').catch(()=>Promise.resolve({pipe:()=>{}})), (r)=> calendarDatesRows.push(r)).catch(()=>{});
  const activeServices = serviceActiveTodaySets(calendarRows, calendarDatesRows, now);

  // collect trips hitting our stopIds within window
  const nowDaySec = ((now.getUTCHours()*60)+now.getUTCMinutes())*60 + now.getUTCSeconds();
  const horizonSec = nowDaySec + windowMin*60;

  const wantedTripIds = new Set();
  await streamCsv(await openEntry(zipPath, 'stop_times.txt'), (r) => {
    const sid = r.stop_id;
    if (!sid || !stopIds.includes(String(sid))) return;
    const depSec = timeToSeconds(r.departure_time);
    if (depSec == null) return;
    if (depSec < nowDaySec || depSec > horizonSec) return;
    if (r.trip_id) wantedTripIds.add(r.trip_id);
  });

  if (wantedTripIds.size === 0) { lruSet(key, []); return []; }

  const wantedRouteIds = new Set();
  await streamCsv(await openEntry(zipPath, 'trips.txt'), (r) => {
    if (!r.trip_id || !wantedTripIds.has(r.trip_id)) return;
    if (!activeServices.has(r.service_id)) return;
    if (r.route_id) wantedRouteIds.add(r.route_id);
  });

  const shortNames = new Set();
  await streamCsv(await openEntry(zipPath, 'routes.txt'), (r) => {
    if (!r.route_id || !wantedRouteIds.has(r.route_id)) return;
    const s = (r.route_short_name || r.route_long_name || '').trim();
    if (s) shortNames.add(s);
  });

  const out = Array.from(shortNames).sort((a,b)=> String(a).localeCompare(String(b), undefined, { numeric:true }));
  lruSet(key, out);
  return out;
}

