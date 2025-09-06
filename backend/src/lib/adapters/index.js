// backend/src/lib/adapters/index.js
import { ttc } from './ttc.js';

export function normalizeAgency(s) {
  const x = String(s || '').toLowerCase();
  if (['ttc','toronto'].includes(x)) return 'ttc';
  // everything else disabled for now
  return '';
}

export const adapters = {
  ttc,
};

