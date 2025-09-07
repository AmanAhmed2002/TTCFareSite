// backend/src/lib/adapters/ttc.js
import { fetchRT, defaultStopMatcher } from './base.js';
import { openTtcSqlite, mapTripIdsToRouteShort } from '../sqlite.js';

// Public Bustime GTFS-RT endpoints
const urls = {
  vehicles: process.env.TTC_RT_VEHICLES || 'https://bustime.ttc.ca/gtfsrt/vehicles',
  trips:    process.env.TTC_RT_TRIPS    || 'https://bustime.ttc.ca/gtfsrt/trips',
  alerts:   process.env.TTC_RT_ALERTS   || 'https://bustime.ttc.ca/gtfsrt/alerts',
};

// Prefix-match helper: "83" should match "83A"
function routeShortMatches(shortName, routeRef) {
  if (!routeRef) return true;
  const a = String(shortName||'').toLowerCase();
  const b = String(routeRef||'').toLowerCase();
  return a === b || a.startsWith(b);
}

export const ttc = {
  /**
   * Realtime arrivals at a single stopId (parent expansion happens upstream).
   * We parse TripUpdates, then enrich route short-names from SQLite by trip_id.
   */
  async nextArrivalsByStop(stopId, { limit = 10, routeRef = null, fromEpochSec = undefined } = {}) {
    const feed = await fetchRT(urls.trips);
    const now = Math.floor(Date.now() / 1000);
    const minTs = Number.isFinite(fromEpochSec) ? fromEpochSec : now;

    const rows = [];
    const tripIds = new Set();

    for (const ent of (feed.entity || [])) {
      const tu = ent.tripUpdate;
      if (!tu) continue;
      const trip = tu.trip || {};
      const tid = String(trip.tripId || '').trim();
      for (const stu of (tu.stopTimeUpdate || [])) {
        if (!defaultStopMatcher(stu.stopId, stopId)) continue;
        const t = Number(stu.arrival?.time || stu.departure?.time);
        if (!Number.isFinite(t) || t < minTs) continue;
        rows.push({ tid, t, headsign: trip.scheduleRelationship ? '' : (trip.tripHeadsign || '') });
        if (tid) tripIds.add(tid);
      }
    }
    if (!rows.length) return [];

    // Enrich trip_id -> route short name via static DB (file-backed, cheap)
    let shortByTrip = new Map();
    try {
      const db = await openTtcSqlite();
      shortByTrip = mapTripIdsToRouteShort(db, Array.from(tripIds));
    } catch { /* if sqlite missing, we still return times but may not filter by routeRef */ }

    const filtered = rows
      .map(r => ({ ...r, short: shortByTrip.get(r.tid) || '' }))
      .filter(r => routeShortMatches(r.short, routeRef))
      .sort((a,b)=> a.t - b.t)
      .slice(0, Math.max(1, limit))
      .map(r => ({
        when: new Date(r.t * 1000).toISOString(),
        realtime: true,
        routeShortName: r.short,
        headsign: r.headsign || ''
      }));

    return filtered;
  },

  async alerts(routeRef) {
    // We keep your existing alerts logic elsewhere (unchanged) – if you had
    // an alertsFromFeed helper use it here. Kept minimal as your earlier code handled this.
    const feed = await fetchRT(urls.alerts);
    const out = [];
    for (const ent of (feed.entity || [])) {
      const a = ent.alert; if (!a) continue;
      const informed = (a.informedEntity || []).map(e => e.routeId).filter(Boolean);
      if (routeRef && !informed.some(rid => String(rid).toLowerCase().startsWith(String(routeRef).toLowerCase()))) continue;
      out.push({
        id: ent.id,
        routes: informed,
        start: a?.activePeriod?.[0]?.start ? new Date(Number(a.activePeriod[0].start) * 1000).toISOString() : null,
        end:   a?.activePeriod?.[0]?.end   ? new Date(Number(a.activePeriod[0].end)   * 1000).toISOString() : null,
        headerText: a.headerText?.translation?.[0]?.text || '',
        descriptionText: a.descriptionText?.translation?.[0]?.text || '',
        cause: a.cause || '',
        effect: a.effect || ''
      });
    }
    return out;
  },
};

