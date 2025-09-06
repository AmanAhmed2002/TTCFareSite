// backend/src/lib/adapters/brampton.js
import { fetchRT, arrivalsFromTripUpdates, alertsFromFeed, looseStopMatcher } from './base.js';

// NextRide usually on port 81 (public)
const urls = {
  vehicles: process.env.BRAMPTON_RT_VEHICLES || 'http://nextride.brampton.ca:81/API/VehiclePositions?format=gtfs.proto',
  trips:    process.env.BRAMPTON_RT_TRIPS    || 'http://nextride.brampton.ca:81/API/TripUpdates?format=gtfs.proto',
  alerts:   process.env.BRAMPTON_RT_ALERTS   || 'http://nextride.brampton.ca:81/API/ServiceAlerts?format=gtfs.proto',
};

export const brampton = {
  async nextArrivalsByStop(stopId, opts = {}) {
    const feed = await fetchRT(urls.trips);
    // Brampton sometimes prefixes stop IDs → relaxed matcher
    return arrivalsFromTripUpdates(feed, stopId, { ...opts, stopIdMatcher: looseStopMatcher });
  },
  async alerts(routeRef) {
    const feed = await fetchRT(urls.alerts);
    return alertsFromFeed(feed, { routeRef });
  },
};

