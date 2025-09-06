// backend/src/lib/adapters/base.js

import GtfsRT from 'gtfs-realtime-bindings';
const TransitRealtime = GtfsRT.transit_realtime;

export async function fetchRT(url, { timeoutMs = 8000 } = {}) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ac.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    const buf = new Uint8Array(await res.arrayBuffer());
    return TransitRealtime.FeedMessage.decode(buf);
  } finally {
    clearTimeout(t);
  }
}

// ---------- normalization ----------
function stripNonAlnum(s) {
  return String(s ?? '').toLowerCase().replace(/[^a-z0-9]/g, '');
}
function normalizeRouteKey(s) {
  const x = stripNonAlnum(s);
  return x.replace(/^0+/, '');
}
function routeMatches(feedRouteId, requested) {
  if (!requested) return true;
  const a = normalizeRouteKey(feedRouteId);
  const b = normalizeRouteKey(requested);
  if (!b) return true;
  if (a === b) return true;
  return a.startsWith(b) || b.startsWith(a);
}

// Default: strict equality (case-insensitive) to avoid cross-stop leakage
function defaultStopMatcher(rtStopId, wantedStopId) {
  return String(rtStopId).toLowerCase() === String(wantedStopId).toLowerCase();
}

// Brampton: tolerate prefixes/suffixes (e.g., agency prefixes)
export function looseStopMatcher(rtStopId, wantedStopId) {
  const a = stripNonAlnum(rtStopId);
  const b = stripNonAlnum(wantedStopId);
  return a === b || a.endsWith(b) || b.endsWith(a);
}

/**
 * Extract arrivals at a given stopId from TripUpdates.
 * You can pass a custom stopIdMatcher; otherwise strict match is used.
 */
export function arrivalsFromTripUpdates(
  feed,
  stopId,
  { limit = 10, routeRef, fromEpochSec, stopIdMatcher } = {}
) {
  const now = Math.floor(Date.now() / 1000);
  const minTs = Number.isFinite(fromEpochSec) ? fromEpochSec : now;
  const matchStop = stopIdMatcher || defaultStopMatcher;

  const out = [];
  for (const ent of feed.entity || []) {
    const tu = ent.tripUpdate;
    if (!tu) continue;
    const trip = tu.trip || {};
    const rtRoute = (trip.routeId ?? '').toString();

    if (routeRef && !routeMatches(rtRoute, routeRef)) continue;

    for (const stu of tu.stopTimeUpdate || []) {
      if (!matchStop(stu.stopId, stopId)) continue;
      const t = Number(stu.arrival?.time || stu.departure?.time);
      if (!Number.isFinite(t) || t < minTs) continue;

      out.push({
        when: new Date(t * 1000).toISOString(),
        realtime: true,
        routeShortName: rtRoute || '',
        headsign: stu.stopHeadsign || trip.tripId || '',
        vehicleId: tu.vehicle?.id || undefined,
      });
    }
  }
  out.sort((a, b) => new Date(a.when) - new Date(b.when));
  return out.slice(0, limit);
}

// Alerts helper
export function alertsFromFeed(feed, { routeRef } = {}) {
  const routeKey = normalizeRouteKey(routeRef);
  const items = [];
  for (const ent of feed.entity || []) {
    const a = ent.alert;
    if (!a) continue;
    const informed = (a.informedEntity || []).map(e => e.routeId).filter(Boolean);
    if (routeKey) {
      const has = informed.some(r => routeMatches(r, routeKey));
      if (!has) continue;
    }
    items.push({
      id: ent.id,
      headerText: a.headerText?.translation?.[0]?.text || '',
      descriptionText: a.descriptionText?.translation?.[0]?.text || '',
      cause: a.cause || '',
      effect: a.effect || '',
      routes: informed,
      start: a?.activePeriod?.[0]?.start ? new Date(Number(a.activePeriod[0].start) * 1000).toISOString() : null,
      end: a?.activePeriod?.[0]?.end ? new Date(Number(a.activePeriod[0].end) * 1000).toISOString() : null,
    });
  }
  return items;
}

