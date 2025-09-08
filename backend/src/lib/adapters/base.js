// src/lib/adapters/base.js
import GtfsRT from 'gtfs-realtime-bindings';
const TransitRealtime = GtfsRT.transit_realtime;

const RT_CACHE = new Map();

export async function fetchRT(url, { timeoutMs = 8000, ttlMs = Number(process.env.RT_CACHE_TTL_MS || 5000) } = {}) {
  const now = Date.now();
  const hit = RT_CACHE.get(url);
  if (hit && (now - hit.t) < ttlMs) return hit.msg;

  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ac.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    const buf = new Uint8Array(await res.arrayBuffer());
    const msg = TransitRealtime.FeedMessage.decode(buf);
    RT_CACHE.set(url, { t: now, msg });
    if (RT_CACHE.size > 4) RT_CACHE.delete(RT_CACHE.keys().next().value);
    return msg;
  } finally {
    clearTimeout(t);
  }
}

export function defaultStopMatcher(rtStopId, targetStopId) {
  return String(rtStopId) === String(targetStopId);
}

