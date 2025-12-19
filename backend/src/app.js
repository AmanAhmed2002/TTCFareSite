// backend/src/app.js
import express from 'express';
import helmet from 'helmet';
import morgan from 'morgan';
import rateLimit from 'express-rate-limit';
import { CORS_ALLOWLIST } from './config.js';
import checkRoute from './routes/check.js';
import stopsRoute from './routes/stops.js';
import remindersRoute from './routes/reminders.js';
import jobsRoute from './routes/jobs.js';
import smsRoute from './routes/sms.js';
import transitRoute from './routes/transit.js';
import chatRoute from './routes/chat.js';

export const app = express();

app.set('trust proxy', 1);

// Security / parsing
app.use(helmet({ crossOriginResourcePolicy: { policy: 'cross-origin' } }));
app.use(express.json());
app.use(morgan('combined'));

console.log('CORS_ALLOWLIST:', CORS_ALLOWLIST);

// --- SIMPLE CORS + PREFLIGHT HANDLER ---
app.use((req, res, next) => {
  const origin = req.headers.origin;

  // If no allowlist configured, allow all origins
  if (!CORS_ALLOWLIST || CORS_ALLOWLIST.length === 0) {
    if (origin) res.header('Access-Control-Allow-Origin', origin);
  } else if (origin && CORS_ALLOWLIST.includes(origin)) {
    res.header('Access-Control-Allow-Origin', origin);
  }

  // Basic CORS headers for all requests
  res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  // Handle preflight directly
  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }

  next();
});
// --- END CORS ---

// Rate-limit the API
const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
});
app.use('/api/', limiter);

// Routes
app.get('/api/health', (_req, res) => res.json({ ok: true }));
app.use('/api/check', checkRoute);
app.use('/api/stops', stopsRoute);
app.use('/api/reminders', remindersRoute);
app.use('/api/jobs', jobsRoute);
app.use('/api/sms', smsRoute);
app.use('/api/transit', transitRoute);
app.use('/api/chat', chatRoute);

export default app;
