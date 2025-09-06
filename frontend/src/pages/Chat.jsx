import { useState } from 'react';
import { API_BASE } from '../apiBase.js';

const SYSTEMS = [
  { label:'TTC', value:'TTC', enabled:true },
  { label:'MiWay (coming soon)', value:'MiWay', enabled:false },
  { label:'Brampton (coming soon)', value:'Brampton', enabled:false },
  { label:'DRT (coming soon)', value:'DRT', enabled:false },
  { label:'YRT (coming soon)', value:'YRT', enabled:false },
];
const INTENTS = ['Arrivals','Alerts'];

// NEW: TTC-style alert categories
const ALERT_TYPES = [
  { label: 'Service alerts (real-time)', value: 'rt' },
  { label: 'Service changes (planned)', value: 'service_changes' },
  { label: 'Subway closures (planned)', value: 'subway' },
  { label: 'Streetcar changes (planned)', value: 'streetcar' },
  { label: 'Accessibility advisories (planned)', value: 'accessibility' },
  { label: 'Construction notices (planned)', value: 'construction' },
];

function Chip({ active, children, onClick, disabled=false, title='' }) {
  const base = 'px-3 py-1.5 rounded-full border transition';
  const activeCls = 'bg-slate-900 text-white border-slate-900';
  const normal = 'border-slate-300 text-slate-700 hover:bg-slate-100';
  const disabledCls = 'border-slate-200 text-slate-400 cursor-not-allowed';
  return (
    <button
      className={`${base} ${disabled?disabledCls:(active?activeCls:normal)}`}
      onClick={disabled ? undefined : onClick}
      disabled={disabled}
      title={title}
    >{children}</button>
  );
}

function ErrorNotice({ text }) {
  if (!text) return null;
  return (
    <div className="rounded-xl border border-red-300 bg-red-50 text-red-800 px-3 py-2 text-sm">
      {text}
    </div>
  );
}

function ResultCard({ data, onChooseRoute }) {
  if (!data) return null;

  if (data.error && !(data.arrivals || data.items)) {
    return <ErrorNotice text={data.error} />;
  }

  const showLineSuggestions = Array.isArray(data.availableRoutes) && data.availableRoutes.length > 1;

  return (
    <div className="space-y-3">
      {showLineSuggestions && (
        <div className="rounded-xl border p-3 text-left">
          <div className="text-sm mb-2 text-slate-600">Pick a line?</div>
          <div className="flex flex-wrap gap-2">
            {data.availableRoutes.map(r => (
              <Chip key={r} active={false} onClick={() => onChooseRoute?.(r)}>{r}</Chip>
            ))}
          </div>
        </div>
      )}

      {data.arrivals && (
        <div className="rounded-xl border p-4 text-left">
          <div className="mb-2 text-sm text-slate-500">
            Source: {data.source === 'rt' ? 'Real-time' : 'Schedule'} • {new Date(data.generatedAt).toLocaleTimeString()}
          </div>
          {data.arrivals.length ? (
            <ul className="space-y-1">
              {data.arrivals.map((a,i)=>(
                <li key={i}>
                  <strong>{a.routeShortName || '—'}</strong>
                  {a.headsign ? ` → ${a.headsign}` : ''}
                  {' — '}
                  <span className="font-mono">{new Date(a.when).toLocaleTimeString()}</span>
                  {a.realtime && <span className="ml-2 text-xs px-1 rounded bg-emerald-100 text-emerald-700">RT</span>}
                </li>
              ))}
            </ul>
          ) : (
            <div className="text-slate-600">No upcoming vehicles found here right now.</div>
          )}
        </div>
      )}

      {data.items && (
        <div className="rounded-xl border p-4 text-left space-y-2">
          {data.items.length ? data.items.map(x=>(
            <div key={x.url || x.id || Math.random()} className="border-b pb-2 last:border-0">
              <div className="font-semibold">{x.headerText || 'Alert'}</div>
              {x.descriptionText && <div className="text-sm text-slate-700 whitespace-pre-line">{x.descriptionText}</div>}
              <div className="text-xs text-slate-500">
                {x.start ? new Date(x.start).toLocaleString() : ''}
                {x.end ? ` – ${new Date(x.end).toLocaleString()}` : ''}
                {x.url && <a className="ml-2 underline" href={x.url} target="_blank" rel="noreferrer">Details</a>}
              </div>
            </div>
          )) : <div className="text-slate-600">No matching advisories.</div>}
        </div>
      )}
    </div>
  );
}

export default function Chat() {
  const [text, setText] = useState('');
  const [system, setSystem] = useState('TTC'); // default TTC
  const [intent, setIntent] = useState('');
  const [stopRef, setStopRef] = useState('');
  const [routeRef, setRouteRef] = useState('');
  const [limit, setLimit] = useState(10);

  // NEW: alerts controls
  const [alertWindow, setAlertWindow] = useState(60);
  const [alertMode, setAlertMode] = useState('rt');

  const [result, setResult] = useState(null);
  const [missing, setMissing] = useState([]);
  const [loading, setLoading] = useState(false);
  const [inlineError, setInlineError] = useState('');

  async function parseAndMaybeAsk(q) {
    setInlineError('');
    try {
      const r = await fetch(`${API_BASE}/api/chat/parse`, {
        method: 'POST', headers:{'content-type':'application/json'},
        body: JSON.stringify({ text: q, sessionId: 'web' })
      }).then(r=>r.json());
      setMissing(r.missingSlots || []);
      if (r.slots?.agency && r.slots.agency.toUpperCase()==='TTC') setSystem('TTC');
      if (r.intent) setIntent(r.intent);
      if (r.slots?.stop_ref) setStopRef(r.slots.stop_ref);
      if (r.slots?.route_ref) setRouteRef(r.slots.route_ref);
      return r;
    } catch (e) {
      setInlineError(String(e?.message || e));
    }
  }

  // Lines helper (kept from earlier)
  async function fetchLines({ agency, stop_ref, scope='all', windowMin=60 }) {
    const params = new URLSearchParams({ agency, stop_ref, scope, window: String(windowMin) });
    const res = await fetch(`${API_BASE}/api/transit/lines?${params.toString()}`);
    if (!res.ok) return null;
    return res.json().catch(()=>null);
  }

  async function run(overrides = {}) {
    setInlineError('');
    setResult(null);
    const sys = overrides.system ?? system;
    const inx = overrides.intent ?? intent;
    const stop = overrides.stopRef ?? stopRef;
    const route = overrides.routeRef ?? routeRef;
    const lim = overrides.limit ?? limit;
    const win = overrides.alertWindow ?? alertWindow;
    const mode = overrides.alertMode ?? alertMode;

    if (sys !== 'TTC') {
      setInlineError('Only TTC is enabled right now. Others are coming soon.');
      return;
    }
    if (!inx || (inx==='arrivals' && !stop)) {
      setInlineError('Please fill the required fields.');
      return;
    }
    setLoading(true);
    try {
      const ag = 'ttc';
      let url = '';
      if (inx === 'arrivals') {
        const params = new URLSearchParams({ agency: ag, stop_ref: String(stop).trim(), limit: String(lim) });
        if (route) params.set('route_ref', String(route).trim());
        url = `${API_BASE}/api/transit/arrivals?${params.toString()}`;
      } else if (inx === 'alerts') {
        const params = new URLSearchParams({ agency: ag, limit: String(lim), window: String(win), mode });
        if (stop) params.set('stop_ref', String(stop).trim());
        if (route) params.set('route_ref', String(route).trim());
        url = `${API_BASE}/api/transit/alerts?${params.toString()}`;
      }
      const res = await fetch(url);
      const data = await res.json().catch(()=> ({}));

      if (!res.ok) {
        setResult({ error: data?.error || `Request failed (${res.status})` });
      } else {
        // keep arrival chips reliable
        if (inx === 'arrivals' && !route) {
          try {
            const lines = await fetchLines({ agency: 'ttc', stop_ref: String(stop).trim(), scope: 'all', windowMin: 60 });
            const base = Array.isArray(data.availableRoutes) ? data.availableRoutes : [];
            const merged = Array.from(new Set([...(base || []), ...(lines?.routes || [])]))
              .sort((a,b)=> String(a).localeCompare(String(b), undefined, { numeric:true }));
            data.availableRoutes = merged;
          } catch {}
        }
        setResult(data);
      }
    } catch (e) {
      setResult({ error: String(e?.message || e) });
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="page">
      <div className="container-narrow py-10 text-left">
        <h1 className="text-2xl font-bold mb-2">Transit Chat — TTC</h1>
        <p className="text-slate-600 mb-2">
          TTC realtime with schedule fallback. Other systems are coming soon.
        </p>
        <div className="rounded-xl border p-3 mb-4 text-slate-700 text-sm">
          <div className="font-semibold mb-1">Tips for best accuracy</div>
          <ul className="list-disc pl-5 space-y-1">
            <li>Include the <strong>line</strong> for arrivals. Example: <span className="font-mono">TTC 83 arrivals at Donlands station</span>.</li>
            <li>For alerts near a location, try: <span className="font-mono">TTC alerts at Warden station</span>, then adjust the <strong>time window</strong> and <strong>alert type</strong> below.</li>
          </ul>
        </div>

        <div className="mb-3 flex gap-2 flex-wrap">
          {SYSTEMS.map(s => (
            <Chip
              key={s.value}
              active={system===s.value}
              onClick={()=> s.enabled && setSystem(s.value)}
              disabled={!s.enabled}
              title={s.enabled ? '' : 'Coming soon'}
            >
              {s.label}
            </Chip>
          ))}
        </div>

        <div className="mb-5 flex gap-2 flex-wrap">
          {INTENTS.map(i => <Chip key={i} active={intent===i.toLowerCase()} onClick={()=>setIntent(i.toLowerCase())}>{i}</Chip>)}
        </div>

        <div className="flex gap-2 mb-3">
          <input className="flex-1 rounded-xl border px-3 py-2" placeholder="Ask… e.g., 'TTC 83 arrivals at Donlands Station' or 'TTC alerts at Warden Station'" value={text} onChange={e=>setText(e.target.value)} />
          <button className="btn btn-ghost" onClick={async ()=>{ await parseAndMaybeAsk(text); }}>Parse</button>
        </div>

        <div className="grid sm:grid-cols-4 gap-3 mb-3">
          <div>
            <label className="block text-xs mb-1">System</label>
            <select className="w-full rounded-xl border px-3 py-2" value={system} onChange={e=>setSystem(e.target.value)}>
              {SYSTEMS.map(s => <option key={s.value} value={s.value} disabled={!s.enabled}>{s.label}</option>)}
            </select>
          </div>

          <div>
            <label className="block text-xs mb-1">Intent</label>
            <select className="w-full rounded-xl border px-3 py-2" value={intent} onChange={e=>setIntent(e.target.value)}>
              <option value="">Select…</option>
              <option value="arrivals">Arrivals</option>
              <option value="alerts">Alerts</option>
            </select>
          </div>

          <div>
            <label className="block text-xs mb-1">{intent === 'arrivals' ? 'Stop (name or ID)' : 'Stop (optional)'}</label>
            <input className="w-full rounded-xl border px-3 py-2" placeholder={intent==='arrivals' ? 'e.g., Warden Station' : 'e.g., Warden Station'} value={stopRef} onChange={e=>setStopRef(e.target.value)} />
          </div>

          <div>
            <label className="block text-xs mb-1">{intent === 'arrivals' ? 'Filter by route (optional)' : 'Route (optional)'}</label>
            <input className="w-full rounded-xl border px-3 py-2" placeholder="e.g., 83" value={routeRef} onChange={e=>setRouteRef(e.target.value)} />
          </div>
        </div>

        {intent === 'arrivals' && (
          <div className="mb-3">
            <label className="block text-xs mb-1">How many results?</label>
            <select className="w-full rounded-xl border px-3 py-2 max-w-[10rem]" value={limit} onChange={e=>setLimit(Number(e.target.value))}>
              {[5,10,15,20,25].map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
        )}

        {intent === 'alerts' && (
          <>
            <div className="mb-3 grid sm:grid-cols-2 gap-3">
              <div>
                <label className="block text-xs mb-1">Alert type</label>
                <select className="w-full rounded-xl border px-3 py-2" value={alertMode} onChange={e=>setAlertMode(e.target.value)}>
                  {ALERT_TYPES.map(a => <option key={a.value} value={a.value}>{a.label}</option>)}
                </select>
              </div>
              <div>
                <label className="block text-xs mb-1">Time window (minutes)</label>
                <select className="w-full rounded-xl border px-3 py-2" value={alertWindow} onChange={e=>setAlertWindow(Number(e.target.value))}>
                  {[30,60,90,120,180,240].map(n => <option key={n} value={n}>{n}</option>)}
                </select>
              </div>
            </div>
            <div className="mb-3">
              <label className="block text-xs mb-1">Limit</label>
              <select className="w-full rounded-xl border px-3 py-2 max-w-[10rem]" value={limit} onChange={e=>setLimit(Number(e.target.value))}>
                {[5,10,20,30].map(n => <option key={n} value={n}>{n}</option>)}
              </select>
            </div>
          </>
        )}

        {inlineError && <div className="mb-3"><ErrorNotice text={inlineError} /></div>}

        <div className="flex gap-2">
          <button className="btn btn-primary" onClick={()=>run({})} disabled={loading || !system || !intent || (intent==='arrivals' && !stopRef)}>
            {loading ? 'Loading…' : 'Ask'}
          </button>
          {missing.length > 0 && <div className="text-sm text-slate-600 self-center">Missing: {missing.join(', ')}</div>}
          {result?.arrivals?.length >= limit && (
            <button className="btn btn-ghost" onClick={()=> run({ limit: limit + 5 })}>
              Load 5 more
            </button>
          )}
        </div>

        <div className="mt-6">
          <ResultCard
            data={result}
            onChooseRoute={(r)=>{
              setRouteRef(r);
              run({ routeRef: r });
            }}
          />
        </div>
      </div>
    </div>
  );
}

