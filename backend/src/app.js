// backend/src/app.js
import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import morgan from 'morgan';
import rateLimit from 'express-rate-limit';
import { CORS_ALLOWLIST } from './config.js';
import checkRoute from './routes/check.js';
import stopsRoute from './routes/stops.js';
//import pushRoute from './routes/push.js';
import remindersRoute from './routes/reminders.js';
import jobsRoute from './routes/jobs.js';
import smsRoute from './routes/sms.js';
import transitRoute from './routes/transit.js';
import chatRoute from './routes/chat.js';


export const app = express();

app.set('trust proxy', 1);

// Trust reverse proxies (e.g., ngrok) when opted in
/*if (process.env.TRUST_PROXY) {
  // true = trust all proxies; or set a number (e.g., 1) if you prefer
  const val = process.env.TRUST_PROXY === 'true' ? true :
              /^\d+$/.test(process.env.TRUST_PROXY) ? Number(process.env.TRUST_PROXY) :
              process.env.TRUST_PROXY;
  app.set('trust proxy', val);
}
*/
// Security / parsing
app.use(helmet({ crossOriginResourcePolicy: { policy: 'cross-origin' } }));
app.use(express.json());
app.use(morgan('combined'));

console.log('CORS_ALLOWLIST:', CORS_ALLOWLIST);

// CORS: allow local dev and your Vercel domain(s)
/*const corsOptions = {
  origin(origin, cb) {
    if (!origin) return cb(null, true);
    if (CORS_ALLOWLIST.includes(origin)) return cb(null, true);
    cb(new Error('CORS not allowed for this origin'));
  },
  credentials: false
};
app.use(cors(corsOptions));
*/


app.use(
  cors({
    origin(origin, cb) {
      // Non-browser requests (curl, Postman, Azure health checks)
      if (!origin) return cb(null, true);

      // If no allowlist configured, allow everything (handy for local dev)
      if (CORS_ALLOWLIST.length === 0) {
        return cb(null, true);
      }

      // Only allow known frontends
      if (CORS_ALLOWLIST.includes(origin)) {
        return cb(null, true);
      }

      return cb(new Error(`Origin ${origin} not allowed by CORS`));
    },
  })
);


// Rate-limit the API
// limiter happy while preserving your existing behavior.
const limiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
});
app.use('/api/', limiter);

// Routes
app.get('/api/health', (_req, res) => res.json({ ok: true }));
app.use('/api/check', checkRoute);
app.use('/api/stops', stopsRoute);
//app.use('/api/push', pushRoute);
app.use('/api/reminders', remindersRoute);
app.use('/api/jobs', jobsRoute);
app.use('/api/sms', smsRoute);
app.use('/api/transit', transitRoute);
app.use('/api/chat', chatRoute);


export default app;

