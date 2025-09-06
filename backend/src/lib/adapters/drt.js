import { fetchRT, arrivalsFromTripUpdates, alertsFromFeed } from './base.js';

const urls = {
  vehicles: process.env.DRT_RT_VEHICLES || 'https://drtonline.durhamregiontransit.com/gtfsrealtime/VehiclePositions',
  trips:    process.env.DRT_RT_TRIPS    || 'https://drtonline.durhamregiontransit.com/gtfsrealtime/TripUpdates',
  alerts:   process.env.DRT_RT_ALERTS   || 'https://maps.durham.ca/OpenDataGTFS/alerts.pb',
};

export const drt = {
  async nextArrivalsByStop(stopId, opts = {}) {
    const feed = await fetchRT(urls.trips);
    return arrivalsFromTripUpdates(feed, stopId, opts);
  },
  async alerts(routeRef) {
    const feed = await fetchRT(urls.alerts);
    return alertsFromFeed(feed, { routeRef });
  },
};

