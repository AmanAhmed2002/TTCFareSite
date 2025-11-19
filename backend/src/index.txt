// backend/src/index.js
import 'dotenv/config';
import { PORT } from './config.js';
import { app } from './app.js';
// Import the jobs module as a namespace to avoid bare-identifier issues
import * as Jobs from './routes/jobs.js';

app.listen(PORT, () => {
  console.log(`API listening on http://localhost:${PORT}`);
});

/**
 * Optional in-process job loop (for Render web services).
 */
if (process.env.JOBS_LOOP === 'true') {
  let running = false;

  async function tick() {
    if (running) return;
    running = true;
    try {
      const fn = Jobs?.runDueSmsJobs;
      if (typeof fn !== 'function') {
        // Defensive guard: avoids ReferenceError and logs helpful context
        console.error('[jobs] runDueSmsJobs not available (did routes/jobs.js export it?)');
        return;
      }
      const r = await fn();
      if (r?.processed > 0) {
        console.log(`[jobs] sent ${r.processed} SMS job(s)`);
      }
    } catch (e) {
      console.error('[jobs] loop error:', e);
    } finally {
      running = false;
    }
  }

  // First tick shortly after boot, then every 60s
  setTimeout(tick, 5000);
  setInterval(tick, 60_000);
}

