// src/lib/gtfsZipSchedule.js
import unzipper from 'unzipper';
import fs from 'fs';
import os from 'os';
import path from 'path';
import crypto from 'crypto';
import { parse } from 'csv-parse';
import { Readable } from 'stream';
import { pipeline } from 'stream/promises';

const GTFS_ZIP_URL = process.env.TTC_GTFS_STATIC_URL || '';
const TTL_ZIP_MS = 6 * 60 * 60 * 1000;
const TZ = 'America/Toronto';

const sha12 = (s) => crypto.createHash('sha1').update(String(s)).digest('hex').slice(0,12);
const zipPathFor = (url) => path.join(os.tmpdir(), `ttc-gtfs-${sha12(url)}.zip`);

async function ensureZipLocal() {
  if (!GTFS_ZIP_URL) throw new Error('TTC_GTFS_STATIC_URL not set');
  const file = zipPathFor(GTFS_ZIP_URL);
  try {
    const st = fs.statSync(file);
    if (st.size > 0 && (Date.now() - st.mtimeMs) < TTL_ZIP_MS) return file;
  } catch {}
  const res = await fetch(GTFS_ZIP_URL, { redirect: 'follow' });
  if (!res.ok || !res.body) throw new Error(`gtfs zip fetch failed: ${res.status}`);
  await fs.promises.mkdir(path.dirname(file), { recursive: true });
  const nodeReadable = Readable.fromWeb(res.body);
  await pipeline(nodeReadable, fs.createWriteStream(file));
  return file;
}

async function openEntry(entryName) {
  const zip = await ensureZipLocal();
  const dir = await unzipper.Open.file(zip);
  const ent = dir.files.find(f => f.path.toLowerCase() === entryName.toLowerCase());
  if (!ent) throw new Error(`${entryName} not found in GTFS zip`);
  return ent.stream();
}

async function streamCsv(entryStream, onRow) {
  return new Promise((resolve, reject) => {
    const parser = parse({ columns: true, trim: true, relax_column_count: true, relax_quotes: true });
    entryStream.pipe(parser);
    parser.on('data', onRow);
    parser.on('error', reject);
    parser.on('end', resolve);
  });
}

function toLocal(d) { return new Date(d.toLocaleString('en-CA', { timeZone: TZ })); }
function localMidnight(d) { return new Date(d.getFullYear(), d.getMonth(), d.getDate()); }
function hmsToSec(t) { const m = String(t||'').match(/^(\d+):(\d{2})(?::(\d{2}))?$/); if(!m) return null; return (+m[1])*3600+(+m[2])*60+(+m[3]||0); }
function secToIso(dayStart, sec) { return new Date(dayStart.getTime() + sec*1000).toISOString(); }

function activeServiceIds(calendarRows, calendarDatesRows, dLoc) {
  const y=dLoc.getFullYear(), m=dLoc.getMonth()+1, d=dLoc.getDate();
  const iso = `${y}${String(m).padStart(2,'0')}${String(d).padStart(2,'0')}`;
  const dow = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'][dLoc.getDay()];
  const act = new Set();
  for (const r of calendarRows) {
    if (iso>=r.start_date && iso<=r.end_date && String(r[dow]||'0')==='1') act.add(r.service_id);
  }
  for (const r of calendarDatesRows) {
    if (r.date !== iso) continue;
    if (String(r.exception_type)==='1') act.add(r.service_id);
    if (String(r.exception_type)==='2') act.delete(r.service_id);
  }
  return act;
}

export async function nextArrivalsFromZipStreaming({ stopIds = [], routeRef = null, limit = 10, now = new Date(), horizonMin = 360 } = {}) {
  if (!stopIds.length) return [];
  const calendarRows = [], calDates = [];
  await streamCsv(await openEntry('calendar.txt'), (r)=> calendarRows.push(r));
  try { await streamCsv(await openEntry('calendar_dates.txt'), (r)=> calDates.push(r)); } catch {}
  const nowLoc = toLocal(now);
  const dayStart = localMidnight(nowLoc);
  const nowSec = nowLoc.getHours()*3600 + nowLoc.getMinutes()*60 + nowLoc.getSeconds();
  const horizonSec = nowSec + horizonMin*60;
  const services = activeServiceIds(calendarRows, calDates, nowLoc);

  const wantedTrips = new Map(); // trip_id -> depSec
  await streamCsv(await openEntry('stop_times.txt'), (r) => {
    const sid = String(r.stop_id || '').trim();
    if (!stopIds.includes(sid)) return;
    const sec = hmsToSec(r.departure_time);
    if (sec == null || sec < nowSec || sec > horizonSec) return;
    const tid = String(r.trip_id || '').trim();
    if (!tid) return;
    const prev = wantedTrips.get(tid);
    if (!prev || sec < prev) wantedTrips.set(tid, sec);
  });
  if (wantedTrips.size === 0) return [];

  const wantedTripIds = new Set(wantedTrips.keys());
  const trips = [];
  await streamCsv(await openEntry('trips.txt'), (r) => {
    const tid = String(r.trip_id || '').trim();
    if (!wantedTripIds.has(tid)) return;
    if (!services.has(String(r.service_id || ''))) return;
    trips.push({ trip_id: tid, route_id: String(r.route_id || '').trim(), headsign: r.trip_headsign || '' });
  });
  if (!trips.length) return [];

  const routeNames = new Map();
  const neededRoutes = new Set(trips.map(t => t.route_id));
  await streamCsv(await openEntry('routes.txt'), (r) => {
    const rid = String(r.route_id || '').trim();
    if (!neededRoutes.has(rid)) return;
    routeNames.set(rid, (r.route_short_name || r.route_long_name || '').trim());
  });

  const ref = routeRef ? String(routeRef).toLowerCase() : null;
  const out = [];
  for (const t of trips) {
    const short = routeNames.get(t.route_id) || '';
    if (ref) {
      const s = short.toLowerCase();
      if (!(s === ref || s.startsWith(ref))) continue;
    }
    const sec = wantedTrips.get(t.trip_id);
    out.push({ routeShortName: short, headsign: t.headsign || '', when: secToIso(dayStart, sec), realtime: false });
  }
  out.sort((a,b)=> new Date(a.when) - new Date(b.when));
  return out.slice(0, Math.max(1, limit));
}

export async function linesAtStopFromZipStreaming({ stopIds = [], windowMin = 60, now = new Date() } = {}) {
  if (!stopIds.length) return [];
  const calendarRows = [], calDates = [];
  await streamCsv(await openEntry('calendar.txt'), (r)=> calendarRows.push(r));
  try { await streamCsv(await openEntry('calendar_dates.txt'), (r)=> calDates.push(r)); } catch {}
  const nowLoc = toLocal(now);
  const nowSec = nowLoc.getHours()*3600 + nowLoc.getMinutes()*60 + nowLoc.getSeconds();
  const horizonSec = nowSec + Math.max(5, Number(windowMin||60))*60;
  const services = activeServiceIds(calendarRows, calDates, nowLoc);

  const wantedTripIds = new Set();
  await streamCsv(await openEntry('stop_times.txt'), (r) => {
    const sid = String(r.stop_id || '').trim();
    if (!stopIds.includes(sid)) return;
    const sec = hmsToSec(r.departure_time); if (sec == null) return;
    if (sec < nowSec || sec > horizonSec) return;
    const tid = String(r.trip_id || '').trim();
    if (tid) wantedTripIds.add(tid);
  });
  if (!wantedTripIds.size) return [];

  const routeIds = new Set();
  await streamCsv(await openEntry('trips.txt'), (r) => {
    const tid = String(r.trip_id || '').trim();
    if (!wantedTripIds.has(tid)) return;
    if (!services.has(String(r.service_id || ''))) return;
    const rid = String(r.route_id || '').trim();
    if (rid) routeIds.add(rid);
  });

  const names = new Set();
  await streamCsv(await openEntry('routes.txt'), (r) => {
    const rid = String(r.route_id || '').trim();
    if (!routeIds.has(rid)) return;
    const s = (r.route_short_name || r.route_long_name || '').trim();
    if (s) names.add(s);
  });

  return Array.from(names).sort((a,b)=> String(a).localeCompare(String(b), undefined, { numeric: true }));
}

