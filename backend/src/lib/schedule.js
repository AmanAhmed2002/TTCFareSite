// backend/lib/schedule.js
// Compatibility wrapper. Your routes import nextArrivalsFromSchedule & linesAtStopWindow.
// In production we stream the GTFS zip to keep RAM low; in dev you can keep any existing logic.

import { nextArrivalsFromZipStreaming, linesAtStopFromZipStreaming } from './gtfsZipSchedule.js';

export async function nextArrivalsFromSchedule(agencyKey, stopId, { limit = 10, routeRef = null, fromTime = undefined } = {}) {
  if (agencyKey !== 'ttc') return [];
  const now = fromTime || new Date();
  return await nextArrivalsFromZipStreaming({
    stopIds: [String(stopId)],
    routeRef,
    limit,
    now
  });
}

export async function linesAtStopWindow(agencyKey, stopId, { windowMin = 60 } = {}) {
  if (agencyKey !== 'ttc') return [];
  return await linesAtStopFromZipStreaming({
    stopIds: [String(stopId)],
    windowMin,
    now: new Date()
  });
}

// (Optional) station expansion helper – keep your existing expandStopIdsIfStation in its own module.
export async function expandStopIdsIfStation(agencyKey, stopId) {
  // If you already have a station→platform resolver, keep using it.
  // Otherwise, return the single stopId; the high-level route logic already handles this.
  return [String(stopId)];
}

