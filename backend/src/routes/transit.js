// backend/src/routes/transit.js
import { Router } from 'express';
import { adapters, normalizeAgency } from '../lib/adapters/index.js';
import { getStopId, findCandidateStopIds } from '../lib/stopResolver.js';
import { nextArrivalsFromSchedule, expandStopIdsIfStation, linesAtStopWindow } from '../lib/schedule.js';

const DISABLE_RT = String(process.env.DISABLE_RT || '') === '1';


const router = Router();

// ---------- helpers ----------

async function expandStopIds(agencyKey, stopId) {
  try { return await expandStopIdsIfStation(agencyKey, stopId); }
  catch { return [String(stopId)]; }
}

function mergeAndSortArrivals(lists, limit) {
  const seen = new Set();
  const out = [];
  for (const arr of lists) {
    for (const a of arr) {
      const key = [a.routeShortName || '', a.headsign || '', a.when || ''].join('|');
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(a);
    }
  }
  out.sort((a,b)=> new Date(a.when) - new Date(b.when));
  return typeof limit === 'number' ? out.slice(0, limit) : out;
}

function stripTags(html) {
  return String(html || '').replace(/<script[\s\S]*?<\/script>/gi,'')
    .replace(/<style[\s\S]*?<\/style>/gi,'')
    .replace(/<[^>]+>/g,' ')
    .replace(/\s{2,}/g,' ')
    .trim();
}

async function fetchText(url) {
  const r = await fetch(url, { headers: { 'user-agent': 'Mozilla/5.0' }});
  if (!r.ok) throw new Error(`Fetch failed ${r.status} for ${url}`);
  return r.text();
}

// Very lightweight parser: split by <h3> sections on TTC advisories pages and pull adjacent text.
// Returns [{ headerText, descriptionText, start, url }]
function parseTtcSections(html, baseUrl) {
  const items = [];
  const h3Regex = /<h3[^>]*>([\s\S]*?)<\/h3>/gi;
  let m;
  const indices = [];
  while ((m = h3Regex.exec(html)) !== null) {
    indices.push({ start: m.index, end: h3Regex.lastIndex, titleHtml: m[1] });
  }
  // add sentinel to capture tail text
  indices.push({ start: html.length, end: html.length, titleHtml: '' });

  for (let i = 0; i < indices.length - 1; i++) {
    const curr = indices[i];
    const next = indices[i+1];
    const title = stripTags(curr.titleHtml);
    const block = html.slice(curr.end, next.start);
    const text = stripTags(block);
    // crude "Effective:" timestamp extraction if present
    const effMatch = text.match(/\bEffective\b:\s*([^]+?)(?:\n|$)/i);
    const start = effMatch ? new Date(effMatch[1]).toISOString() : null;

    const urlMatch = block.match(/href="([^"]+)"/i);
    const url = urlMatch ? (new URL(urlMatch[1], baseUrl)).toString() : baseUrl;

    if (title) {
      items.push({
        headerText: title,
        descriptionText: text,
        start,
        end: null,
        url
      });
    }
  }
  return items;
}

function overlapsWindow(startIso, endIso, winStartMs, winEndMs) {
  const s = startIso ? new Date(startIso).getTime() : null;
  const e = endIso ? new Date(endIso).getTime() : null;
  if (s == null && e == null) return true;
  const s2 = (s == null) ? -Infinity : s;
  const e2 = (e == null) ? Infinity : e;
  return !(e2 < winStartMs || s2 > winEndMs);
}

function blobIncludesTokens(header, desc, tokens) {
  if (!tokens?.length) return true;
  const blob = `${header || ''} ${desc || ''}`.toLowerCase();
  return tokens.every(t => blob.includes(t));
}

function normalizeTokensStopName(name) {
  if (!name) return [];
  return name.toLowerCase()
    .replace(/\bstn\b/g,'station')
    .replace(/\bstation\b/g,'')
    .split(/\s+/).filter(Boolean);
}

// ---------- ARRIVALS ----------

router.get('/arrivals', async (req, res) => {
  const agencyKey = normalizeAgency(req.query.agency || '');
  const stopRefRaw = String(req.query.stop_ref || '').trim();
  const routeRef = String(req.query.route_ref || '').trim() || null;
  const limit = Math.min(Math.max(Number(req.query.limit || 10), 1), 50);
  const fromEpochSec = Number(req.query.from || 0) || undefined;

  // NEW: make availableRoutes optional (default off)
  const includeRoutes = String(req.query.include_routes || '0') === '1';

  if (!agencyKey) return res.status(400).json({ error: 'agency required (ttc)' });
  if (!stopRefRaw) return res.status(400).json({ error: 'stop_ref required' });

  const adapter = adapters[agencyKey];
  if (!adapter) return res.status(400).json({ error: `Unsupported agency: ${agencyKey}` });

  // Tunables (keep these conservative to avoid Azure timeouts)
  const OVERALL_DEADLINE_MS = Number(process.env.ARRIVALS_DEADLINE_MS || 12000);
  const PER_STOP_RT_TIMEOUT_MS = Number(process.env.RT_STOP_TIMEOUT_MS || 3500);
  const PER_STOP_SCH_TIMEOUT_MS = Number(process.env.SCH_STOP_TIMEOUT_MS || 3500);
  const MAX_STOP_IDS = Number(process.env.ARRIVALS_MAX_STOPIDS || 8);

  const start = Date.now();
  const timeLeft = () => Math.max(0, OVERALL_DEADLINE_MS - (Date.now() - start));

  try {
    const exactId = /^[A-Za-z0-9_-]+$/.test(stopRefRaw) ? stopRefRaw : null;
    const candidates = [];
    if (exactId) candidates.push({ id: exactId, name: exactId });

    if (!exactId || /[A-Za-z]/.test(stopRefRaw)) {
      const more = await withTimeout(
        findCandidateStopIds(agencyKey, stopRefRaw, 10),
        Math.min(2500, timeLeft()),
        'findCandidateStopIds'
      );
      for (const c of more) {
        if (!candidates.find(x => String(x.id) === String(c.id))) candidates.push(c);
      }
    }

    if (!candidates.length) return res.status(404).json({ error: 'Stop not found' });

    // Expand candidates -> stop IDs (cap to avoid huge fan-out)
    const expandedSets = await withTimeout(
      Promise.all(candidates.map(c => expandStopIds(agencyKey, c.id))),
      Math.min(3000, timeLeft()),
      'expandStopIds'
    );

    const allStopIds = [...new Set(expandedSets.flat().map(String))];
    const stopIdsToQuery = cap(allStopIds, MAX_STOP_IDS);

    const chosen = candidates[0] || null;

    let arrivals = [];
    let source = 'rt';

    // ---- realtime (parallel-ish, with per-stop timeouts) ----
    if (!DISABLE_RT && timeLeft() > 0) {
      const tasks = stopIdsToQuery.map(sid =>
        withTimeout(
          adapter.nextArrivalsByStop(sid, {
            limit,
            routeRef: routeRef || undefined,
            fromEpochSec
          }),
          Math.min(PER_STOP_RT_TIMEOUT_MS, timeLeft()),
          `rt:${sid}`
        ).catch(() => [])
      );

      const rtLists = await Promise.all(tasks);
      arrivals = mergeAndSortArrivals(rtLists.filter(Boolean), limit);
      if (arrivals.length) source = 'rt';
    }

    // ---- schedule fallback ----
    if (!arrivals.length && timeLeft() > 0) {
      const tasks = stopIdsToQuery.map(sid =>
        withTimeout(
          nextArrivalsFromSchedule(agencyKey, sid, {
            limit,
            routeRef: routeRef || undefined,
            fromTime: fromEpochSec ? new Date(fromEpochSec * 1000) : undefined,
          }),
          Math.min(PER_STOP_SCH_TIMEOUT_MS, timeLeft()),
          `schedule:${sid}`
        ).catch(() => [])
      );

      const schLists = await Promise.all(tasks);
      arrivals = mergeAndSortArrivals(schLists.filter(Boolean), limit);
      if (arrivals.length) source = 'schedule';
    }

    // IMPORTANT: availableRoutes is expensive — default OFF.
    // Use /api/transit/lines to populate the dropdown instead.
    let availableRoutes = undefined;
    if (includeRoutes && !routeRef && timeLeft() > 0) {
      const idsForLines = stopIdsToQuery.length ? stopIdsToQuery : await expandStopIds(agencyKey, chosen.id);
      const set = new Set();

      // reduce window to avoid heavy work (full-day 1440 can be slow)
      const WINDOW_MIN = Number(process.env.LINES_WINDOW_MIN || 360);

      const tasks = idsForLines.map(sid =>
        withTimeout(
          linesAtStopWindow(agencyKey, sid, { windowMin: WINDOW_MIN }),
          Math.min(2500, timeLeft()),
          `lines:${sid}`
        ).catch(() => [])
      );

      const lists = await Promise.all(tasks);
      for (const lines of lists) for (const l of (lines || [])) set.add(String(l));

      availableRoutes = Array.from(set).sort((a,b)=>
        String(a).localeCompare(String(b), undefined, { numeric: true })
      );
    }

    return res.json({
      arrivals,
      source,
      stopId: chosen?.id ?? null,
      stopName: chosen?.name ?? null,
      availableRoutes,
      appliedRouteFilter: !!routeRef,
      generatedAt: new Date().toISOString(),
    });
  } catch (e) {
    // If we exceeded our own deadline, return a clean timeout instead of Azure 504
    const msg = String(e?.message || e);
    if (msg.includes('timed out')) {
      return res.status(504).json({ error: msg });
    }
    return res.status(500).json({ error: msg });
  }
});


// ---------- ALERTS (real-time + TTC planned categories) ----------

router.get('/alerts', async (req, res) => {
  const agencyKey = normalizeAgency(req.query.agency || '');
  const stopRefRaw = String(req.query.stop_ref || '').trim();
  const routeRef = String(req.query.route_ref || '').trim() || null;
  const windowMin = Math.max(1, Number(req.query.window || 60));
  const limit = Math.min(Math.max(Number(req.query.limit || 20), 1), 200);
  const mode = String(req.query.mode || 'rt').toLowerCase(); // 'rt' or planned categories

  if (!agencyKey) return res.status(400).json({ error: 'agency required (ttc)' });

  const adapter = adapters[agencyKey];
  if (!adapter?.alerts) return res.status(400).json({ error: `Unsupported agency: ${agencyKey}` });

  const now = Date.now();
  const winStart = now - windowMin * 60 * 1000;
  const winEnd = now + windowMin * 60 * 1000;

  try {
    // Build stop context (tokens + lines serving that station) for scoping
    let stopTokens = [];
    let routeTokensAtStop = [];
    if (stopRefRaw) {
      const stopId = await getStopId(agencyKey, stopRefRaw);
      if (stopId) {
        const expanded = await expandStopIds(agencyKey, stopId);
        const set = new Set();
        for (const sid of expanded) {
          try {
            const lines = await linesAtStopWindow(agencyKey, sid, { windowMin: 1440 });
            for (const l of lines) set.add(String(l));
          } catch {}
        }
        routeTokensAtStop = Array.from(set).map(s => s.toLowerCase());
        stopTokens = normalizeTokensStopName(stopRefRaw);
      }
    }

    let items = [];

    if (mode === 'rt') {
      // Real-time: pull GTFS-RT alerts and filter to time + stop/route context
      const all = await adapter.alerts();
      items = (all || []).filter(it => {
        const tMatch = overlapsWindow(it.start, it.end, winStart, winEnd);
        if (!tMatch) return false;
        // If route_ref provided, require mention
        if (routeRef) {
          const r = String(routeRef).toLowerCase();
          const blob = `${it.headerText || ''} ${it.descriptionText || ''}`.toLowerCase();
          if (!(blob.includes(` ${r} `) || blob.includes(`${r}-`) || blob.includes(`line ${r}`))) return false;
        }
        // If stop provided, require tokens or a line serving the stop
        if (stopTokens.length) {
          const blob = `${it.headerText || ''} ${it.descriptionText || ''}`.toLowerCase();
          const tokensOk = stopTokens.every(t => blob.includes(t));
          const lineOk = routeTokensAtStop.some(rt => blob.includes(` ${rt} `) || blob.includes(`${rt}-`) || blob.includes(`line ${rt}`));
          if (!tokensOk && !lineOk) return false;
        }
        return true;
      });
    } else {
      // Planned categories: fetch & parse TTC pages
      const map = {
        'service_changes': 'https://www.ttc.ca/service-advisories/Service-Changes',
        'subway': 'https://www.ttc.ca/service-advisories/subway-service',
        'streetcar': 'https://www.ttc.ca/service-advisories/Streetcar-Service-Changes',
        'accessibility': 'https://www.ttc.ca/service-advisories/accessibility',
        'construction': 'https://www.ttc.ca/service-advisories/construction-notices'
      };
      const url = map[mode] || map['service_changes'];
      const html = await fetchText(url);
      const parsed = parseTtcSections(html, url);

      items = parsed.filter(it => {
        // Window applies to "Effective:" if present; otherwise include
        const timeOk = overlapsWindow(it.start, it.end, winStart, winEnd);
        if (!timeOk) return false;

        // Route and stop token scoping
        const blob = `${it.headerText || ''} ${it.descriptionText || ''}`.toLowerCase();
        if (routeRef) {
          const r = String(routeRef).toLowerCase();
          if (!(blob.includes(` ${r} `) || blob.includes(`${r}-`) || blob.includes(`line ${r}`))) return false;
        }
        if (stopTokens.length) {
          const tokensOk = stopTokens.every(t => blob.includes(t));
          const lineOk = routeTokensAtStop.some(rt => blob.includes(` ${rt} `) || blob.includes(`${rt}-`) || blob.includes(`line ${rt}`));
          if (!tokensOk && !lineOk) return false;
        }
        return true;
      });
    }

    const out = limit ? items.slice(0, limit) : items;
    return res.json({
      items: out,
      generatedAt: new Date().toISOString(),
      windowMin,
      mode,
      stop: stopRefRaw || null,
      routeRef: routeRef || null,
      total: items.length
    });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

// ---------- LINES helper (unchanged) ----------

router.get('/lines', async (req, res) => {
  const agencyKey = normalizeAgency(req.query.agency || '');
  const stopRefRaw = String(req.query.stop_ref || '').trim();
  const windowMin = Number(req.query.window || 60);

  if (!agencyKey) return res.status(400).json({ error: 'agency required (ttc)' });
  if (!stopRefRaw) return res.status(400).json({ error: 'stop_ref required' });

  try {
    const exactId = /^[A-Za-z0-9_-]+$/.test(stopRefRaw) ? stopRefRaw : null;
    const candidates = [];
    if (exactId) candidates.push({ id: exactId, name: exactId });
    if (!exactId || /[A-Za-z]/.test(stopRefRaw)) {
      const more = await findCandidateStopIds(agencyKey, stopRefRaw, 10);
      for (const c of more) if (!candidates.find(x => String(x.id) === String(c.id))) candidates.push(c);
    }
    if (!candidates.length) return res.status(404).json({ error: 'Stop not found' });

    const first = candidates[0];
    const stopIds = await expandStopIds(agencyKey, first.id);

    const set = new Set();
    for (const sid of stopIds) {
      const lines = await linesAtStopWindow(agencyKey, sid, { windowMin });
      for (const l of lines) set.add(String(l));
    }
    const routes = Array.from(set).sort((a,b)=> String(a).localeCompare(String(b), undefined, { numeric: true }));

    return res.json({
      stopId: first.id,
      stopName: first.name,
      windowMin,
      generatedAt: new Date().toISOString(),
      routes,
      method: 'schedule+station-expansion',
    });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

export default router;

