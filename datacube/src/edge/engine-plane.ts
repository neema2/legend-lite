// DATACUBE'S LEGEND-ENGINE PLANE, its compat entry's one door (H11, 2026-11-17, the orchestrator's alignment): a page
// configured to plan on legend-engine (config.json's legendEngine, the demo's `engine` plane) loads this module by its
// URL (demo/legend-engine.js, its own bundle) when that plane is chosen -- never by an import of the product's, which
// reaches no edge module (//tools/deps:app_runs_test) -- and is handed legend-engine's planner over its pure/v1 API.

import { UpstreamPlanner } from './upstream-planner.ts';

/** legend-engine's planner at `baseUrl`, over `model` on `runtime`. */
export function legendEnginePlanner(baseUrl: string, model: string, runtime: string): UpstreamPlanner {
  return new UpstreamPlanner({ baseUrl, model, runtime });
}
