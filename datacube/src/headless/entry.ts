// The headless bundle's entry: what Python's host loads into V8 (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md
// §3), built as one script whose exports are the global `legendLiteHeadless`. Its first two imports put the web layer
// in place before any module that needs it as it loads.

import './install-web.ts';
import './install-url.ts';

import { REQUEST_KINDS, type PlannerWorker } from '../../../engine-client/src/legend/planner-protocol.ts';
import { hostCalls, hostDatabase, hostWorker, type HostDatabase } from './host.ts';
import {
  gridCalculate, gridChart, gridClass, gridCube, gridExport, gridFilter, gridRelation, gridView,
} from './grid.ts';
import { forget, ingest, mount, remoteAccess } from './data-in.ts';
import { classDuplicate, pageModelKept, pageModelNames } from './model.ts';
import { layoutAdd, layoutArrange, layoutRemove, layoutStack } from './layout.ts';
import { nameGiven, pageRead } from './page.ts';

/** The request kinds this bundle's planner sends: the host checks, before anything runs, that it answers each (§7). */
export { REQUEST_KINDS };

/** A grid, headless (§4): its view, its rows as a relation, its rows as a class's (B4), its export (D), a chart of it
 *  as DataCube makes and judges one (chart-spec.ts chartFrom and chartProblems: written once, which Python asks), and
 *  its cube over a frame as DataCube makes one, new or rebuilt over the frame as it is now (cube-document.ts
 *  rebuilt), and a calculated column added to it and its filter set as the screen's editors do. */
export { gridCalculate, gridChart, gridClass, gridCube, gridExport, gridFilter, gridRelation, gridView };

/** Data in (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §4.3): a file read into the host's database by upload.ts, its
 *  table dropped again, and a remote file mounted as a view (its extensions and keys put in place first, where the host
 *  checks what it names). */
export { forget, ingest, mount, remoteAccess };

/** The page's model as DataCube names it, a class's key's facts (B4), and the model a page keeps for the cubes it still
 *  holds: what Python asks beside the generic library. */
export { classDuplicate, pageModelKept, pageModelNames };

/** A sheet's layout edited by DataCube's own rules (layout/bands.ts): a tile placed, taken off, the places arranged,
 *  tiles stacked -- what Python's pages ask, so they place nothing by rules of their own. */
export { layoutAdd, layoutArrange, layoutRemove, layoutStack };

/** A page document read as DataCube reads it (any version, migrated, every rule held) and written as DataCube writes
 *  it: what Python's pages ask when one is loaded, read back, laid out or saved. */
export { pageRead };

/** A page's, a tile's or a sheet's name as a person gives it, by DataCube's one rule: what Python's names ask. */
export { nameGiven };

/** What DataCube's code runs on in this host: its planner's worker, and the database. */
export interface Headless {
  readonly worker: () => PlannerWorker;
  readonly database: HostDatabase;
}

/** The planner's worker and the database, over the host's calls (`legendLiteHost`, defined before the bundle loads). */
export function start(): Headless {
  const calls = hostCalls();
  return { worker: () => hostWorker(calls), database: hostDatabase(calls) };
}
