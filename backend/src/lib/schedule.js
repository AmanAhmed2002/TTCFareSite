// backend/src/lib/schedule.js
// Delegates TTC schedule work to SQLite (file-backed, low RAM) without changing public API.
import { nextArrivalsFromSchedule, expandStopIdsIfStation, linesAtStopWindow } from './schedule-sqlite.js';
export { nextArrivalsFromSchedule, expandStopIdsIfStation, linesAtStopWindow };

