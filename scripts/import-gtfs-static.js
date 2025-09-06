// scripts/import-gtfs-static.js
// Usage: node scripts/import-gtfs-static.js --zip ./gtfs.zip --agency TTC
import fs from 'fs';
import unzipper from 'unzipper';
import csv from 'fast-csv';
import 'dotenv/config';
import pg from 'pg';
const { Pool } = pg;

function arg(name, def=null){ const i = process.argv.indexOf(`--${name}`); return i>=0 ? process.argv[i+1] : def; }
const zipPath = arg('zip');
const agency = (arg('agency','TTC')||'').toUpperCase();
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL missing');
if (!zipPath || !fs.existsSync(zipPath)) throw new Error('--zip missing or not found');

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 3 });

// DDL (separate from your existing stops)
const ddl = `
CREATE TABLE IF NOT EXISTS calendar (
  service_id TEXT PRIMARY KEY,
  monday SMALLINT, tuesday SMALLINT, wednesday SMALLINT, thursday SMALLINT, friday SMALLINT, saturday SMALLINT, sunday SMALLINT,
  start_date DATE, end_date DATE
);
CREATE TABLE IF NOT EXISTS calendar_dates (
  service_id TEXT, date DATE, exception_type SMALLINT
);
CREATE TABLE IF NOT EXISTS routes (
  route_id TEXT PRIMARY KEY, agency TEXT NOT NULL, route_short_name TEXT, route_long_name TEXT
);
CREATE TABLE IF NOT EXISTS trips (
  trip_id TEXT PRIMARY KEY, route_id TEXT, service_id TEXT, trip_headsign TEXT, direction_id SMALLINT, agency TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stop_times (
  trip_id TEXT, arrival_time INT, departure_time INT, stop_id TEXT, stop_sequence INT
);
CREATE INDEX IF NOT EXISTS stop_times_stop_idx ON stop_times (stop_id);
CREATE INDEX IF NOT EXISTS stop_times_time_idx ON stop_times (arrival_time, departure_time);
`;

// helpers
const files = {};
async function parseZip() {
  const stream = fs.createReadStream(zipPath).pipe(unzipper.Parse({ forceStream: true }));
  for await (const entry of stream) {
    const base = entry.path.split('/').pop().toLowerCase();
    if (!['calendar.txt','calendar_dates.txt','routes.txt','trips.txt','stop_times.txt'].includes(base)) {
      entry.autodrain(); continue;
    }
    files[base] = await entry.buffer();
  }
}

async function loadCSV(buf, onRow) {
  return new Promise((resolve, reject) => {
    const rows = [];
    csv.parseString(buf.toString('utf8'), { headers: true, ignoreEmpty: true })
      .on('error', reject)
      .on('data', r => rows.push(r))
      .on('end', async () => { try { await onRow(rows); resolve(); } catch(e){ reject(e);} });
  });
}

function hhmmssToSec(s) {
  const [h,m,sec] = String(s||'0:0:0').split(':').map(Number);
  return (h*3600 + m*60 + (sec||0))|0;
}

async function main() {
  const client = await pool.connect();
  try {
    await client.query(ddl);

    if (files['routes.txt']) {
      const rows = []; await loadCSV(files['routes.txt'], async data => {
        for (const r of data) rows.push([
          r.route_id, agency,
          r.route_short_name || null, r.route_long_name || null
        ]);
      });
      await client.query('BEGIN');
      await client.query('DELETE FROM routes WHERE agency=$1', [agency]);
      for (const r of rows) {
        await client.query(
          `INSERT INTO routes (route_id, agency, route_short_name, route_long_name) VALUES ($1,$2,$3,$4)
           ON CONFLICT (route_id) DO UPDATE SET agency=EXCLUDED.agency, route_short_name=EXCLUDED.route_short_name, route_long_name=EXCLUDED.route_long_name`,
          r
        );
      }
      await client.query('COMMIT');
    }

    if (files['trips.txt']) {
      const rows = []; await loadCSV(files['trips.txt'], async data => {
        for (const r of data) rows.push([
          r.trip_id, r.route_id, r.service_id, r.trip_headsign || null, r.direction_id?.length ? Number(r.direction_id) : null, agency
        ]);
      });
      await client.query('BEGIN');
      await client.query('DELETE FROM trips WHERE agency=$1', [agency]);
      for (const r of rows) {
        await client.query(
          `INSERT INTO trips (trip_id, route_id, service_id, trip_headsign, direction_id, agency)
           VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT (trip_id) DO UPDATE SET route_id=EXCLUDED.route_id, service_id=EXCLUDED.service_id, trip_headsign=EXCLUDED.trip_headsign, direction_id=EXCLUDED.direction_id, agency=EXCLUDED.agency`,
          r
        );
      }
      await client.query('COMMIT');
    }

    if (files['calendar.txt']) {
      const rows = []; await loadCSV(files['calendar.txt'], async data => {
        for (const r of data) rows.push([
          r.service_id, r.monday, r.tuesday, r.wednesday, r.thursday, r.friday, r.saturday, r.sunday,
          r.start_date, r.end_date
        ]);
      });
      await client.query('BEGIN');
      await client.query('DELETE FROM calendar');
      for (const r of rows) {
        await client.query(
          `INSERT INTO calendar (service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::date,$10::date)
           ON CONFLICT (service_id) DO UPDATE SET monday=EXCLUDED.monday,tuesday=EXCLUDED.tuesday,wednesday=EXCLUDED.wednesday,thursday=EXCLUDED.thursday,friday=EXCLUDED.friday,saturday=EXCLUDED.saturday,sunday=EXCLUDED.sunday,start_date=EXCLUDED.start_date,end_date=EXCLUDED.end_date`,
          r
        );
      }
      await client.query('COMMIT');
    }

    if (files['calendar_dates.txt']) {
      const rows = []; await loadCSV(files['calendar_dates.txt'], async data => {
        for (const r of data) rows.push([r.service_id, r.date, r.exception_type]);
      });
      await client.query('BEGIN');
      await client.query('DELETE FROM calendar_dates');
      for (const r of rows) {
        await client.query(
          `INSERT INTO calendar_dates (service_id, date, exception_type)
           VALUES ($1, $2::date, $3::smallint)`,
          r
        );
      }
      await client.query('COMMIT');
    }

    if (files['stop_times.txt']) {
      await client.query('BEGIN');
      await client.query('DELETE FROM stop_times');
      let batch = [];
      await loadCSV(files['stop_times.txt'], async data => {
        for (const r of data) {
          batch.push([
            r.trip_id, hhmmssToSec(r.arrival_time), hhmmssToSec(r.departure_time),
            r.stop_id, Number(r.stop_sequence)
          ]);
          if (batch.length >= 5000) {
            const values = batch.map((_,i)=>`($${i*5+1},$${i*5+2},$${i*5+3},$${i*5+4},$${i*5+5})`).join(',');
            await client.query(
              `INSERT INTO stop_times (trip_id, arrival_time, departure_time, stop_id, stop_sequence) VALUES ${values}`,
              batch.flat()
            );
            batch = [];
          }
        }
      });
      if (batch.length) {
        const values = batch.map((_,i)=>`($${i*5+1},$${i*5+2},$${i*5+3},$${i*5+4},$${i*5+5})`).join(',');
        await client.query(
          `INSERT INTO stop_times (trip_id, arrival_time, departure_time, stop_id, stop_sequence) VALUES ${values}`,
          batch.flat()
        );
      }
      await client.query('COMMIT');
    }

    console.log(`Static GTFS imported for ${agency}.`);
  } finally {
    pool.end();
  }
}

await parseZip();
await main();

