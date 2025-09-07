// backend/src/lib/schedule-sqlite.js
import { openTtcSqlite, expandStationStopIds, routeShortNamesForRouteIds } from './sqlite.js';

const TZ = 'America/Toronto';

function toLocal(d) { return new Date(d.toLocaleString('en-CA', { timeZone: TZ })); }
function localMidnight(dLoc) { return new Date(dLoc.getFullYear(), dLoc.getMonth(), dLoc.getDate()); }
function hmsToSec(hms) {
  const m = String(hms || '').match(/^(\d+):(\d{2})(?::(\d{2}))?$/); if (!m) return null;
  return (+m[1])*3600 + (+m[2])*60 + (+m[3]||0);
}
function secToIso(dayStartLocal, sec) { return new Date(dayStartLocal.getTime() + sec*1000).toISOString(); }

function activeServiceIds(db, dLoc) {
  const y = dLoc.getFullYear(), m = dLoc.getMonth()+1, d = dLoc.getDate();
  const iso = `${y}${String(m).padStart(2,'0')}${String(d).padStart(2,'0')}`;
  const dow = ['sunday','monday','tuesday','wednesday','thursday','friday','saturday'][dLoc.getDay()];
  const cal = db.prepare(`SELECT service_id,start_date,end_date,${dow} as dow FROM calendar`).all();
  const active = new Set();
  for (const r of cal) {
    if (iso >= r.start_date && iso <= r.end_date && String(r.dow||'0') === '1') active.add(String(r.service_id));
  }
  const adds = db.prepare('SELECT service_id FROM calendar_dates WHERE date = ? AND exception_type = 1').all(iso);
  const rems = db.prepare('SELECT service_id FROM calendar_dates WHERE date = ? AND exception_type = 2').all(iso);
  for (const a of adds) active.add(String(a.service_id));
  for (const r of rems) active.delete(String(r.service_id));
  return active;
}

export async function expandStopIdsIfStation(agencyKey, stopId) {
  if (String(agencyKey||'').toLowerCase() !== 'ttc') return [String(stopId)];
  const db = await openTtcSqlite();
  return expandStationStopIds(db, stopId);
}

export async function nextArrivalsFromSchedule(agencyKey, stopId, { limit = 10, routeRef = null, fromTime = undefined } = {}) {
  if (String(agencyKey||'').toLowerCase() !== 'ttc') return [];
  const db = await openTtcSqlite();

  const now = fromTime ? new Date(fromTime) : new Date();
  const nowLoc = toLocal(now);
  const dayStartLoc = localMidnight(nowLoc);
  const nowSec = nowLoc.getHours()*3600 + nowLoc.getMinutes()*60 + nowLoc.getSeconds();
  const horizonSec = nowSec + 360*60; // 6h window

  const services = activeServiceIds(db, nowLoc);

  const stopIds = expandStationStopIds(db, stopId);
  const placeholders = stopIds.map(()=>'?').join(',');
  const rows = db.prepare(
    `SELECT trip_id, departure_time
     FROM stop_times
     WHERE stop_id IN (${placeholders})
       AND departure_time IS NOT NULL
     ORDER BY departure_time ASC`
  ).all(...stopIds);

  // Filter in JS by seconds (handles HH>=24)
  const wanted = [];
  for (const r of rows) {
    const sec = hmsToSec(r.departure_time);
    if (sec == null) continue;
    if (sec < nowSec || sec > horizonSec) continue;
    wanted.push({ trip_id: String(r.trip_id), sec });
  }
  if (!wanted.length) return [];

  // Trips meta
  const tripIds = Array.from(new Set(wanted.map(x=>x.trip_id)));
  const qTrips = `SELECT trip_id, service_id, route_id, trip_headsign FROM trips WHERE trip_id IN (${tripIds.map(()=>'?').join(',')})`;
  const trips = db.prepare(qTrips).all(...tripIds)
    .filter(t => services.has(String(t.service_id)));

  if (!trips.length) return [];

  const routeIds = Array.from(new Set(trips.map(t=>String(t.route_id))));
  const routeNames = routeShortNamesForRouteIds(db, routeIds);

  // Assemble, apply route filter (prefix match like "83" matches "83A"), sort and cap
  const ref = routeRef ? String(routeRef).toLowerCase() : null;
  const out = [];
  const tripsById = new Map(trips.map(t => [String(t.trip_id), t]));
  for (const w of wanted) {
    const t = tripsById.get(w.trip_id); if (!t) continue;
    const short = (routeNames.get(String(t.route_id)) || '').trim();
    if (ref) {
      const s = short.toLowerCase();
      if (!(s === ref || s.startsWith(ref))) continue;
    }
    out.push({
      routeShortName: short,
      headsign: t.trip_headsign || '',
      when: secToIso(dayStartLoc, w.sec),
      realtime: false
    });
  }
  out.sort((a,b)=> new Date(a.when) - new Date(b.when));
  return out.slice(0, Math.max(1, limit));
}

export async function linesAtStopWindow(agencyKey, stopId, { windowMin = 60 } = {}) {
  if (String(agencyKey||'').toLowerCase() !== 'ttc') return [];
  const db = await openTtcSqlite();

  const nowLoc = toLocal(new Date());
  const nowSec = nowLoc.getHours()*3600 + nowLoc.getMinutes()*60 + nowLoc.getSeconds();
  const horizonSec = nowSec + Math.max(5, Number(windowMin||60))*60;
  const services = activeServiceIds(db, nowLoc);

  const stopIds = expandStationStopIds(db, stopId);
  const placeholders = stopIds.map(()=>'?').join(',');
  const rows = db.prepare(
    `SELECT st.trip_id, st.departure_time, t.route_id, t.service_id
     FROM stop_times st JOIN trips t USING(trip_id)
     WHERE st.stop_id IN (${placeholders})
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

