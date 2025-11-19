// backend/src/app.js
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
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

// Trust reverse proxies (Azure, Vercel, NGINX)
app.set('trust proxy', 1);

// Security + Parsing
app.use(helmet({ crossOriginResourcePolicy: { policy: 'cross-origin' } }));
app.use(express.json());
app.use(morgan('combined'));

// CORS handling
const corsOptions = {
  origin(origin, cb) {
    if (!origin) return cb(null, true); // allow mobile apps / curl
    if (CORS_ALLOWLIST.includes(origin)) return cb(null, true);
    cb(new Error(`CORS blocked for origin: ${origin}`));
  }
};
app.use(cors(corsOptions));

// Rate limiting
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
