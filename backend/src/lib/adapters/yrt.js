import { fetchRT, arrivalsFromTripUpdates } from './base.js';

const urls = {
  trips:    process.env.YRT_RT_TRIPS    || 'http://rtu.york.ca/gtfsrealtime/TripUpdates',
  vehicles: process.env.YRT_RT_VEHICLES || 'http://rtu.york.ca/gtfsrealtime/VehiclePositions',
};

export const yrt = {
  async nextArrivalsByStop(stopId, opts = {}) {
    const feed = await fetchRT(urls.trips);
    return arrivalsFromTripUpdates(feed, stopId, opts);
  },
  async alerts() { return []; }
};

