// backend/src/lib/schedule-sqlite.js
import { openTtcSqlite, isSqliteReady, expandStationStopIds, routeShortNamesForRouteIds, expandStationFromZip } from './sqlite.js';

const TZ = 'America/Toronto';
const toLocal = (d) => new Date(d.toLocaleString('en-CA', { timeZone: TZ }));
const localMidnight = (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate());
const hmsToSec = (t) => { const m = String(t||'').match(/^(\d+):(\d{2})(?::(\d{2}))?$/); if(!m) return null; return (+m[1])*3600+(+m[2])*60+(+m[3]||0); };
const secToIso = (dayStart, sec) => new Date(dayStart.getTime() + sec*1000).toISOString();

function activeServiceIds(db, dLoc) {
  const y=dLoc.getFullYear(), m=dLoc.getMonth()+1, d=dLoc.getDate();
  const iso = `${y}${String(m).padStart(2,'0')}${String(d).padStart(2,'0')}`;
  const dowCol = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'][dLoc.getDay()];
  const cal = db.prepare(`SELECT service_id,start_date,end_date,${dowCol} AS dow FROM calendar`).all();
  const act = new Set();
  for (const r of cal) if (iso>=r.start_date && iso<=r.end_date && String(r.dow||'0')==='1') act.add(String(r.service_id));
  for (const r of db.prepare('SELECT service_id FROM calendar_dates WHERE date = ? AND exception_type = 1').all(iso)) act.add(String(r.service_id));
  for (const r of db.prepare('SELECT service_id FROM calendar_dates WHERE date = ? AND exception_type = 2').all(iso)) act.delete(String(r.service_id));
  return act;
}

export async function expandStopIdsIfStation(agencyKey, stopId) {
  if (String(agencyKey||'').toLowerCase() !== 'ttc') return [String(stopId)];
  if (isSqliteReady()) {
    const db = await openTtcSqlite();
    return expandStationStopIds(db, stopId);
  }
  // fallback: stream stops.txt from TTC zip
  return await expandStationFromZip(stopId);
}

export async function nextArrivalsFromSchedule(agencyKey, stopId, { limit = 10, routeRef = null, fromTime } = {}) {
  if (String(agencyKey||'').toLowerCase() !== 'ttc') return [];
  if (!isSqliteReady()) {
    // If DB isn't ready yet, we can't compute schedule fallback safely → return empty; RT will have been tried first.
    return [];
  }
  const db = await openTtcSqlite();

  const now = fromTime ? new Date(fromTime) : new Date();
  const nowLoc = toLocal(now);
  const dayStart = localMidnight(nowLoc);
  const nowSec = nowLoc.getHours()*3600 + nowLoc.getMinutes()*60 + nowLoc.getSeconds();
  const horizonSec = nowSec + 360*60; // 6h

  const services = activeServiceIds(db, nowLoc);
  const stopIds = expandStationStopIds(db, stopId);
  const rows = db.prepare(
    `SELECT st.trip_id, st.departure_time
     FROM stop_times st
     WHERE st.stop_id IN (${stopIds.map(()=>'?').join(',')})
       AND st.departure_time IS NOT NULL
     ORDER BY st.departure_time ASC`
  ).all(...stopIds);

  const wanted = [];
  for (const r of rows) {
    const sec = hmsToSec(r.departure_time);
    if (sec == null || sec < nowSec || sec > horizonSec) continue;
    wanted.push({ tid: String(r.trip_id), sec });
  }
  if (!wanted.length) return [];

  const tids = Array.from(new Set(wanted.map(x=>x.tid)));
  const trips = db.prepare(`SELECT trip_id, service_id, route_id, trip_headsign FROM trips WHERE trip_id IN (${tids.map(()=>'?').join(',')})`).all(...tids)
                  .filter(t => services.has(String(t.service_id)));

  if (!trips.length) return [];
  const routeIds = Array.from(new Set(trips.map(t=>String(t.route_id))));
  const names = routeShortNamesForRouteIds(db, routeIds);

  const ref = routeRef ? String(routeRef).toLowerCase() : null;
  const tripsById = new Map(trips.map(t=>[String(t.trip_id), t]));
  const out = [];
  for (const w of wanted) {
    const t = tripsById.get(w.tid); if (!t) continue;
    const short = (names.get(String(t.route_id)) || '').trim();
    if (ref) {
      const s = short.toLowerCase();
      if (!(s === ref || s.startsWith(ref))) continue;
    }
    out.push({ routeShortName: short, headsign: t.trip_headsign || '', when: secToIso(dayStart, w.sec), realtime: false });
  }
  out.sort((a,b)=> new Date(a.when) - new Date(b.when));
  return out.slice(0, Math.max(1, limit));
}

export async function linesAtStopWindow(agencyKey, stopId, { windowMin = 60 } = {}) {
  if (String(agencyKey||'').toLowerCase() !== 'ttc') return [];
  if (!isSqliteReady()) return []; // until DB is ready
  const db = await openTtcSqlite();

  const nowLoc = toLocal(new Date());
  const nowSec = nowLoc.getHours()*3600 + nowLoc.getMinutes()*60 + nowLoc.getSeconds();
  const horizonSec = nowSec + Math.max(5, Number(windowMin||60))*60;
  const services = activeServiceIds(db, nowLoc);

  const stopIds = expandStationStopIds(db, stopId);
  const rows = db.prepare(
    `SELECT st.departure_time, t.route_id, t.service_id
     FROM stop_times st JOIN trips t USING(trip_id)
     WHERE st.stop_id IN (${stopIds.map(()=>'?').join(',')})
       AND st.departure_time IS NOT NULL
     ORDER BY st.departure_time ASC`
  ).all(...stopIds);

  const routeIds = new Set();
  for (const r of rows) {
    const sec = hmsToSec(r.departure_time); if (sec == null) continue;
    if (sec < nowSec || sec > horizonSec) continue;
    if (!services.has(String(r.service_id))) continue;
    routeIds.add(String(r.route_id));
  }
  const names = routeShortNamesForRouteIds(db, Array.from(routeIds));
  return Array.from(names.values()).sort((a,b)=> String(a).localeCompare(String(b), undefined, { numeric: true }));
}

