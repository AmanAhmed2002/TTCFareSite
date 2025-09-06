// backend/src/lib/schedule.js
// Wraps the TTC zero-DB schedule loader & adds helper to get lines-at-stop.
import {
  loadTtcGtfsFromUrl,
  ttcNextArrivalsFromSchedule,
  expandStopIdsStationAware,
  ttcLinesAtStopInWindow,
} from './gtfsZipSchedule.js';

let loadOncePromise = null;

const DEFAULT_TTC_GTFS =
  process.env.TTC_GTFS_ZIP_URL ||
  // Current TTC static GTFS (Open Data Toronto CKAN resource). You can pin via env.
  'https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/7795b45e-e65a-4465-81fc-c36b9dfff169/resource/cfb6b2b8-6191-41e3-bda1-b175c51148cb/download/TTC Routes and Schedules Data.zip';

async function ensureLoaded() {
  if (!loadOncePromise) {
    loadOncePromise = (async () => {
      try {
        await loadTtcGtfsFromUrl(DEFAULT_TTC_GTFS);
        console.log('[GTFS] TTC zip loaded for schedule fallback');
      } catch (e) {
        console.error('[GTFS] TTC zip load failed:', e?.message || e);
      }
    })();
  }
  return loadOncePromise;
}

export async function nextArrivalsFromSchedule(agencyKey, stopId, opts = {}) {
  const ag = String(agencyKey || '').toLowerCase();
  if (ag !== 'ttc') return [];
  await ensureLoaded();
  return ttcNextArrivalsFromSchedule(stopId, opts);
}

// Return station-aware expansion of a base stop id (TTC only).
export async function expandStopIdsIfStation(agencyKey, stopId) {
  const ag = String(agencyKey || '').toLowerCase();
  if (ag !== 'ttc') return [String(stopId)];
  await ensureLoaded();
  return expandStopIdsStationAware(stopId);
}

// Compute distinct line short names serving stop within window (TTC only).
export async function linesAtStopWindow(agencyKey, stopId, { windowMin = 60 } = {}) {
  const ag = String(agencyKey || '').toLowerCase();
  if (ag !== 'ttc') return [];
  await ensureLoaded();
  return ttcLinesAtStopInWindow(stopId, { windowMin });
}

