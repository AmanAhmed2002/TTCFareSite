// backend/src/lib/schedule.js
// Wrapper used by your routes. Delegates to the streaming GTFS tools above.

import {
  nextArrivalsFromZipStreaming,
  linesAtStopFromZipStreaming,
  expandStopIdsIfStationFromZip
} from './gtfsZipSchedule.js';

export async function nextArrivalsFromSchedule(agencyKey, stopId, { limit = 10, routeRef = null, fromTime = undefined } = {}) {
  if (agencyKey !== 'ttc') return [];
  return await nextArrivalsFromZipStreaming({
    stopIds: [String(stopId)],
    routeRef,
    limit,
    now: fromTime || new Date()
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

export async function expandStopIdsIfStation(agencyKey, stopId) {
  if (agencyKey !== 'ttc') return [String(stopId)];
  return await expandStopIdsIfStationFromZip(String(stopId));
}

