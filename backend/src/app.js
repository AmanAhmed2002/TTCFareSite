// backend/src/app.js
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import morgan from 'morgan';
import rateLimit from 'express-rate-limit';

import { CORS_ALLOWLIST } from './config.js';

import checkRoute from './routes/check.js';
import stopsRoute from './routes/stops.js';
// import pushRoute from './routes/push.js';
import remindersRoute from './routes/reminders.js';
import jobsRoute from './routes/jobs.js';
import smsRoute from './routes/sms.js';
import transitRoute from './routes/transit.js';
import chatRoute from './routes/chat.js';

export const app = express();

app.set('trust proxy', 1);

// Security headers
app.use(helmet({ crossOriginResourcePolicy: { policy: 'cross-origin' } }));
app.use(morgan('combined'));

console.log('CORS_ALLOWLIST:', CORS_ALLOWLIST);

// ---- CORS (must be BEFORE routes and BEFORE rate limiting) ----
const corsOptions = {
  origin(origin, cb) {
    // Allow non-browser requests (curl/postman/azure health checks)
    if (!origin) return cb(null, true);

    // If allowlist empty, allow all (useful for local dev)
    if (!CORS_ALLOWLIST || CORS_ALLOWLIST.length === 0) return cb(null, true);

    // Exact-match allowlist
    if (CORS_ALLOWLIST.includes(origin)) return cb(null, true);

    return cb(new Error(`Origin ${origin} not allowed by CORS`));
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  credentials: false,
  optionsSuccessStatus: 204,
};

app.use(cors(corsOptions));

// Express 5: use regex instead of '*'
app.options(/.*/, cors(corsOptions));

// Parse JSON after CORS is fine (preflight has no body)
app.use(express.json());

// ---- Rate-limit the API (skip OPTIONS preflight) ----
const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
  skip: (req) => req.method === 'OPTIONS',
});
app.use('/api/', limiter);

app.use((req, res, next) => {
  res.setTimeout(45000,() => {
    if (!res.headersSent) {
      res.status(504).json({ ok: false, error: "Server timed out (15s)" });
    }
  });
  next();
});


// ---- Routes ----
app.get('/api/health', (_req, res) => res.json({ ok: true }));

app.use('/api/check', checkRoute);
app.use('/api/stops', stopsRoute);
// app.use('/api/push', pushRoute);
app.use('/api/reminders', remindersRoute);
app.use('/api/jobs', jobsRoute);
app.use('/api/sms', smsRoute);
app.use('/api/transit', transitRoute);
app.use('/api/chat', chatRoute);

// ---- CORS error handler (so it doesn't fail silently) ----
app.use((err, req, res, next) => {
  if (err?.message?.includes('not allowed by CORS')) {
    return res.status(403).json({ ok: false, error: err.message });
  }
  next(err);
});

export default app;
