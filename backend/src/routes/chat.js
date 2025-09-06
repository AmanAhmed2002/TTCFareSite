// backend/src/routes/chat.js
import { Router } from 'express';
import { normalizeAgency } from '../lib/adapters/index.js';

const router = Router();

function hasWord(t, w) { return new RegExp(`\\b${w}\\b`, 'i').test(t); }

// Normalize frequent typos and compact forms like "wardenstation" or "arrivalsa"
function normalizeFreeText(raw) {
  let text = String(raw || '').trim();

  // common misspellings / variants
  text = text.replace(/\bstaion\b/gi, 'station');
  text = text.replace(/\bsttion\b/gi, 'station');
  text = text.replace(/\bstn\b/gi, 'station');

  // "arrivalsat" / "arrivalat" / "arrivalsa" → "arrivals at"
  text = text.replace(/\b(arrival|arrivals)\s*at\b/gi, 'arrivals at');
  text = text.replace(/\barrivalsa\b/gi, 'arrivals at');
  text = text.replace(/\barrivalsat\b/gi, 'arrivals at');

  // Insert space before the word "station" if stuck to the end of a name (e.g., "wardenstation")
  text = text.replace(/([a-z])station\b/gi, '$1 station');

  // Normalize multiple spaces
  text = text.replace(/\s{2,}/g, ' ');

  return text;
}

router.post('/parse', (req, res) => {
  const raw = String((req.body?.text || '')).trim();
  const sessionId = String(req.body?.sessionId || '');
  const text = normalizeFreeText(raw).toLowerCase();

  // intent
  let intent = null;
  if (/(arrival|arrivals|next|coming)/.test(text)) intent = 'arrivals';
  else if (/(alert|delay|advisory|disruption)/.test(text)) intent = 'alerts';
  else if (/(route|termini|stops)/.test(text)) intent = 'route';

  // agency
  let agency = null;
  for (const a of ['ttc','toronto','miway','mississauga','brampton','drt','durham','yrt','york']) {
    if (text.includes(a)) { agency = normalizeAgency(a); break; }
  }

  // route_ref (e.g., "bus 83", "83", "line 2")
  let route_ref = null;
  const mBus = text.match(/\bbus\s+([a-z0-9]+)\b/i);
  const mLine = text.match(/\bline\s+([a-z0-9]+)\b/i);
  const mRoute = text.match(/\broute\s+([a-z0-9]+)\b/i);
  const mBare = text.match(/\b([0-9]{1,3}[a-z]?)\b/i); // simple bare route number like 83, 504, 2
  if (mBus) route_ref = mBus[1];
  else if (mLine) route_ref = mLine[1];
  else if (mRoute) route_ref = mRoute[1];
  else if (mBare && !/\b(at|in|to)\b/.test(text)) route_ref = mBare[1]; // avoid capturing times/addresses

  // stop_ref: after "at" or "in", or tail of string if it looks like "<name> station"
  let stop_ref = null;
  const mAt = text.match(/\b(?:at|in)\s+(.+?)$/i);
  if (mAt) stop_ref = mAt[1]
    .replace(/\b(ttc|toronto)\b/gi,'')
    .replace(/\b(station|stn)\b/gi,' station') // normalize "stn" to "station"
    .replace(/\s{2,}/g,' ')
    .trim();

  // fallback: "<name> station" anywhere
  if (!stop_ref) {
    const mStation = text.match(/([a-z0-9 .'-]+)\s+station\b/i);
    if (mStation) stop_ref = mStation[0];
  }

  // fallback: if we have agency + "at ..." then assume arrivals
  if (!intent && (stop_ref || text.includes(' at '))) intent = 'arrivals';

  const slots = {};
  if (agency) slots.agency = agency;
  if (route_ref) slots.route_ref = route_ref;
  if (stop_ref) slots.stop_ref = stop_ref;

  const missing = [];
  if (!intent) missing.push('intent');
  if (!slots.agency) missing.push('agency');
  if (intent === 'arrivals' && !slots.stop_ref) missing.push('stop_ref');

  res.json({
    ok: true,
    text: raw,
    normalizedText: text,
    sessionId,
    intent,
    slots,
    missingSlots: missing,
  });
});

export default router;

