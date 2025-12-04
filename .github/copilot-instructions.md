<!-- .github/copilot-instructions.md -->
# Copilot instructions for TTCFareSite

Purpose
- Help AI coding assistants be immediately productive in this repository: show the architecture, key files, run/test commands, and project-specific patterns.

Big picture
- Backend: an Express API in `backend/src/` (ES modules). Entry: `backend/src/index.js` → builds `app` in `backend/src/app.js`. Key responsibilities: fare eligibility (see `fareEngine.js`), route handlers in `routes/`, and DB pooling in `db.js`.
- Frontend: a Vite + React SPA in `frontend/` (`frontend/src/`, `vite` scripts). Static assets and PWA logic live under `frontend/` and `public/`.
- Scripts: GTFS and data import utilities live in the repo `scripts/` folder and are invoked from `backend/package.json` via relative paths (e.g., `npm run static-data-ttc`).
- Data: prebuilt protobuf blobs and GTFS zips are under `data/` (used by adapters in `backend/src/lib/`).

How to run (developer commands)
- Backend (dev):
  - Open PowerShell, `cd backend`
  - `npm install`
  - `npm run dev` — runs `nodemon` and restarts on JS changes. (Script in `backend/package.json`.)
- Backend (start):
  - `cd backend; npm start` — runs `node src/index.js`.
- Frontend (dev):
  - `cd frontend; npm install`
  - `npm run dev` — starts Vite on default port (5173).
- Tests:
  - Backend unit tests: `cd backend; npm test` (uses Node's `--test`) — see `backend/tests/`.
  - Frontend tests: `cd frontend; npm run test` (Vitest)
- Data import and DB init (note relative paths):
  - From `backend/`: `npm run db:init` and `npm run import-gtfs` — these call scripts in `../scripts/` and expect `DATABASE_URL` and related env vars.

Important conventions & patterns
- ES modules everywhere: `package.json` contains `"type": "module"` in `backend` and `frontend`. Use `import`/`export` not CommonJS `require`.
- Environment via `dotenv`: `backend/src/config.js` reads `process.env`. Typical vars: `PORT`, `DATABASE_URL`, `DATABASE_CA_CERT_PATH`, `DATABASE_CA_CERT`, `TRUST_PROXY`, `JOBS_LOOP`.
- DB pooling & TLS: `backend/src/db.js` reads `DATABASE_CA_CERT*` and sets `ssl` accordingly. Avoid changing that logic without testing against the target DB provider.
- Validation: `zod` schemas are used (see `backend/src/validators.js`) and controllers expect validated payload shapes.
- Fare logic centralized: `backend/src/fareEngine.js` contains domain constants and eligibility logic — prefer changing behavior there rather than scattering business rules across routes.
- Agency adapters: transit schedule adapters live under `backend/src/lib/adapters/` (e.g., `ttc.js`, `miway.js`) — they normalize external feeds.

Integration points / external dependencies
- PostgreSQL / managed DB: `DATABASE_URL` used by `pg` pool in `db.js`.
- GTFS and GTFS-RT: import scripts in `scripts/` and `backend/src/lib/gtfs*` and `backend/src/lib/adapters/` consume GTFS files in `data/`.
- Web push / email / SMS: `nodemailer`, `web-push`, and SMS logic exist in `backend/src/email.js` and `routes/sms.js` — check those before modifying messaging behavior.

Editing guidance for AI agents
- Small focused edits: keep changes minimal, run unit tests (`backend` + `frontend`) and run `npm run dev` after edits.
- When changing API shapes, update `backend/src/validators.js` and tests under `backend/tests/` to reflect the new contract.
- Avoid changing database connection semantics or SSL handling unless reproducing the target environment locally.
- Use explicit file references when suggesting edits (e.g., "Change `fareEngine.js:checkEligibility` to include X").

Files to inspect first
- `backend/src/index.js`, `backend/src/app.js`, `backend/src/fareEngine.js`, `backend/src/db.js`, `backend/src/validators.js`, `backend/src/routes/*`
- `frontend/src/` (React pages/components), `frontend/package.json`, and `frontend/vite.config.js`
- `scripts/` (import scripts): `import-gtfs-stops.js`, `db-init.js`

When to ask the human
- Any change that requires credentials, an external DB, or access to private GTFS blobs (ask for `DATABASE_URL`, CA certs, or test data).
- Large refactors touching many routes or cross-cutting concerns (CORS, DB pooling, SSL).
- Clarification on intended business rules if a change touches `fareEngine.js` logic beyond trivial bugfixes.

If something seems ambiguous, propose a small, testable change with a clear roll-back path.

-- End of file
