// backend/src/lib/adapters/ttc.js
import { fetchRT, arrivalsFromTripUpdates, alertsFromFeed } from './base.js';

// Public Bustime GTFS-RT endpoints
const urls = {
  vehicles: process.env.TTC_RT_VEHICLES || 'https://bustime.ttc.ca/gtfsrt/vehicles',
  trips:    process.env.TTC_RT_TRIPS    || 'https://bustime.ttc.ca/gtfsrt/trips',
  alerts:   process.env.TTC_RT_ALERTS   || 'https://bustime.ttc.ca/gtfsrt/alerts',
};

export const ttc = {
  async nextArrivalsByStop(stopId, opts = {}) {
    const feed = await fetchRT(urls.trips);
    // TTC stop_id in TripUpdates == GTFS stop_id → strict match (default)
    return arrivalsFromTripUpdates(feed, stopId, { ...opts });
  },
  async alerts(routeRef) {
    const feed = await fetchRT(urls.alerts);
    return alertsFromFeed(feed, { routeRef });
  },
};

