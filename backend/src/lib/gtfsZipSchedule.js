// backend/src/lib/gtfsZipSchedule.js
// Zero-DB GTFS schedule fallback + station-aware utilities for TTC.
// Loads TTC GTFS zip -> computes next departures & lines in memory.

import unzipper from 'unzipper';
import { parse } from 'csv-parse/sync';
import { DateTime } from 'luxon';

// ---------------- CSV helpers ----------------
function readCsv(buf) {
  return parse(buf, { columns: true, skip_empty_lines: true, trim: true });
}
function hmsToSec(hms) {
  if (!hms) return null;
  const m = String(hms).trim().match(/^(\d+):(\d{1,2}):(\d{1,2})$/);
  if (!m) return null;
  const H = Number(m[1]), M = Number(m[2]), S = Number(m[3]);
  return (H * 3600) + (M * 60) + S; // supports H >= 24
}
function normalizeRouteKey(s) {
  return String(s ?? '').toLowerCase().replace(/[^a-z0-9]/g, '').replace(/^0+/, '');
}

// ---------------- In-memory state (TTC) ----------------
const S = {
  loaded: false,
  routesById: new Map(),        // route_id -> { shortName, longName }
  tripsById: new Map(),         // trip_id  -> { route_id, service_id, headsign }
  stopTimesByStop: new Map(),   // stop_id  -> Array<{ sec, trip_id }>
  stopsById: new Map(),         // stop_id  -> { name, parent_station, location_type }
  childrenByParent: new Map(),  // parent_station_id -> Set(child_stop_id)
  calendar: [],
  calendarDates: [],
};

// ---------------- Loader ----------------
export async function loadTtcGtfsFromZip(zipArrayBuffer) {
  // reset
  S.loaded = false;
  S.routesById.clear();
  S.tripsById.clear();
  S.stopTimesByStop.clear();
  S.stopsById.clear();
  S.childrenByParent.clear();
  S.calendar = [];
  S.calendarDates = [];

  const directory = await unzipper.Open.buffer(Buffer.from(zipArrayBuffer));
  async function bufOf(nameVariants) {
    for (const v of nameVariants) {
      const entry = directory.files.find(f => f.path.toLowerCase() === v.toLowerCase());
      if (entry) return entry.buffer();
    }
    return null;
  }

  const routesBuf     = await bufOf(['routes.txt']);
  const tripsBuf      = await bufOf(['trips.txt']);
  const stopTimesBuf  = await bufOf(['stop_times.txt']);
  const stopsBuf      = await bufOf(['stops.txt']);
  const calBuf        = await bufOf(['calendar.txt']);
  const calDatesBuf   = await bufOf(['calendar_dates.txt']);

  if (!routesBuf || !tripsBuf || !stopTimesBuf || !stopsBuf) {
    throw new Error('GTFS zip missing one of routes/trips/stop_times/stops');
  }

  const routes     = readCsv(routesBuf);
  const trips      = readCsv(tripsBuf);
  const stopTimes  = readCsv(stopTimesBuf);
  const stops      = readCsv(stopsBuf);
  const calendar   = calBuf ? readCsv(calBuf) : [];
  const calDates   = calDatesBuf ? readCsv(calDatesBuf) : [];

  for (const r of routes) {
    S.routesById.set(r.route_id, {
      shortName: r.route_short_name || '',
      longName: r.route_long_name || '',
    });
  }
  for (const t of trips) {
    S.tripsById.set(t.trip_id, {
      route_id: t.route_id,
      service_id: t.service_id,
      headsign: t.trip_headsign || '',
    });
  }
  for (const st of stopTimes) {
    const stop_id = st.stop_id;
    const sec = hmsToSec(st.departure_time || st.arrival_time);
    if (sec == null) continue;
    const arr = S.stopTimesByStop.get(stop_id) || [];
    arr.push({ sec, trip_id: st.trip_id });
    S.stopTimesByStop.set(stop_id, arr);
  }
  for (const [k, arr] of S.stopTimesByStop) arr.sort((a, b) => a.sec - b.sec);

  for (const s of stops) {
    const locType = Number(s.location_type || '0');
    const parent = s.parent_station || '';
    S.stopsById.set(s.stop_id, {
      name: s.stop_name || '',
      parent_station: parent || null,
      location_type: locType, // 1=station, 0=stop, (others exist in spec)
    });
    if (parent) {
      const set = S.childrenByParent.get(parent) || new Set();
      set.add(s.stop_id);
      S.childrenByParent.set(parent, set);
    }
  }

  S.calendar = calendar;
  S.calendarDates = calDates;
  S.loaded = true;
}

export async function loadTtcGtfsFromUrl(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch GTFS zip: HTTP ${res.status}`);
  const buf = await res.arrayBuffer();
  await loadTtcGtfsFromZip(buf);
}

// ---------------- Calendar helpers ----------------
function ymd(dt) { return dt.toFormat('yyyyLLdd'); }
function weekdayKey(dt) {
  return ['monday','tuesday','wednesday','thursday','friday','saturday','sunday'][dt.weekday - 1];
}
function getActiveServiceIds(dt) {
  const y = ymd(dt);
  const wk = weekdayKey(dt);
  const active = new Set();
  for (const row of S.calendar) {
    const start = String(row.start_date || '');
    const end = String(row.end_date || '');
    if (start && end && y >= start && y <= end) {
      if (String(row[wk] || '0') === '1') active.add(row.service_id);
    }
  }
  for (const ex of S.calendarDates) {
    if (String(ex.date) !== y) continue;
    if (String(ex.exception_type) === '1') active.add(ex.service_id);
    else if (String(ex.exception_type) === '2') active.delete(ex.service_id);
  }
  return active;
}

// ---------------- Station expansion ----------------
export function expandStopIdsStationAware(stopId) {
  if (!S.loaded) return [String(stopId)];
  const s = S.stopsById.get(String(stopId));
  if (!s) return [String(stopId)];
  // If it's a station (location_type=1), include all children + the station record.
  if (s.location_type === 1) {
    const kids = Array.from(S.childrenByParent.get(String(stopId)) || []);
    return [String(stopId), ...kids];
  }
  // If it's a platform with a parent_station, include all siblings + parent + itself.
  if (s.parent_station) {
    const parent = s.parent_station;
    const sibs = Array.from(S.childrenByParent.get(parent) || []);
    return [parent, ...sibs];
  }
  return [String(stopId)];
}

// ---------------- Schedule queries ----------------
export function ttcNextArrivalsFromSchedule(stopId, { limit = 10, routeRef = null, fromTime = undefined } = {}) {
  if (!S.loaded) return [];
  const now = fromTime instanceof Date
    ? DateTime.fromJSDate(fromTime, { zone: 'America/Toronto' })
    : DateTime.now().setZone('America/Toronto');

  const secNow = now.hour * 3600 + now.minute * 60 + now.second;

  const out = [];
  function scanDay(dt, secCutoff) {
    const active = getActiveServiceIds(dt);
    const sec0 = dt.startOf('day').toSeconds();
    const arr = S.stopTimesByStop.get(String(stopId)) || [];
    for (const row of arr) {
      const trip = S.tripsById.get(row.trip_id);
      if (!trip || !active.has(trip.service_id)) continue;
      const route = S.routesById.get(trip.route_id);
      const rsn = route?.shortName || '';
      if (routeRef) {
        const A = normalizeRouteKey(rsn), B = normalizeRouteKey(String(routeRef));
        if (!(A === B || A.startsWith(B) || B.startsWith(A))) continue;
      }
      if (row.sec < secCutoff) continue;
      const whenIso = DateTime.fromSeconds(sec0 + row.sec, { zone: 'America/Toronto' }).toISO();
      out.push({ when: whenIso, realtime: false, routeShortName: rsn, headsign: trip.headsign || '' });
      if (out.length >= limit) break;
    }
  }
  scanDay(now, secNow);
  if (out.length < limit) {
    const tmr = now.plus({ days: 1 }).startOf('day');
    scanDay(tmr, 0);
  }
  return out.slice(0, limit);
}

/**
 * Compute distinct route_short_name values serving a stop in the next `windowMin` minutes.
 * Used to build accurate "lines at this station" suggestions.
 */
export function ttcLinesAtStopInWindow(stopId, { windowMin = 60, fromTime = undefined } = {}) {
  if (!S.loaded) return [];
  const now = fromTime instanceof Date
    ? DateTime.fromJSDate(fromTime, { zone: 'America/Toronto' })
    : DateTime.now().setZone('America/Toronto');

  const active = getActiveServiceIds(now);
  const secNow = now.hour * 3600 + now.minute * 60 + now.second;
  const secLimit = secNow + windowMin * 60;
  const sec0 = now.startOf('day').toSeconds();

  const arr = S.stopTimesByStop.get(String(stopId)) || [];
  const set = new Set();
  for (const row of arr) {
    if (row.sec < secNow || row.sec > secLimit) continue;
    const trip = S.tripsById.get(row.trip_id);
    if (!trip || !active.has(trip.service_id)) continue;
    const route = S.routesById.get(trip.route_id);
    const rsn = route?.shortName || '';
    if (rsn) set.add(String(rsn));
  }
  // Return sorted route short names (natural numeric order)
  return Array.from(set).sort((a,b)=> String(a).localeCompare(String(b), undefined, { numeric: true }));
}

