// backend/src/lib/adapters/miway.js
import { fetchRT, arrivalsFromTripUpdates, alertsFromFeed } from './base.js';

// MiWay public endpoints (documented by Transitland + MiWay dev page)
const urls = {
  vehicles: process.env.MIWAY_RT_VEHICLES || 'https://www.miapp.ca/GTFS_RT/Vehicle/VehiclePositions.pb',
  trips:    process.env.MIWAY_RT_TRIPS    || 'https://www.miapp.ca/GTFS_RT/TripUpdate/TripUpdates.pb',
  // Alerts may be present; if not, we’ll just return an empty list gracefully
  alerts:   process.env.MIWAY_RT_ALERTS   || 'https://www.miapp.ca/GTFS_RT/ServiceAlert/ServiceAlerts.pb',
};

export const miway = {
  async nextArrivalsByStop(stopId, opts = {}) {
    const feed = await fetchRT(urls.trips);
    // MiWay stop_id generally equals GTFS stop_id → strict match
    return arrivalsFromTripUpdates(feed, stopId, { ...opts });
  },
  async alerts(routeRef) {
    try {
      const feed = await fetchRT(urls.alerts);
      return alertsFromFeed(feed, { routeRef });
    } catch {
      return [];
    }
  },
};

