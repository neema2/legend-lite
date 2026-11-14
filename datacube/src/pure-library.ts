// DATACUBE'S PURE LIBRARY (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.6; the user, 2026-10-26: "try as pure
// function -- doesn't have to be built in -- can be datacube pure scripts"): gs-quant's timeseries functions written in
// Pure over ANY table, which a timeseries tile's queries call by name --
// `->timeseries::mean(over(~symbol, ~day->ascending(), rows(-21, 0)), ~close, 22, ~mean)` -- and which a person may
// call in a query of their own. legend-lite expands each call into its
// body over the call's own columns (docs/SEMANTICS_REGISTER.md S112: legend-engine refuses a user function with type
// parameters, so a planner of legend-engine's refuses these in its own words). The database computes every figure.
//
// The library is DataCube's, never the platform's: a model takes it only where a query calls it (withLibrary), so a
// query that calls none is planned over its model as it is, byte for byte. Its text is a chunk of its own
// (pure-library-text.ts), fetched the first time a query calls it, so a page that calls none never downloads it.

import { functionsCalled, walk, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import { lazily } from './lazily.ts';

/** The package the library's functions are in. */
export const LIBRARY_PACKAGE = 'timeseries';

/** Whether a query calls a function of the library -- or holds a #TS{...}# island, whose every figure is the
 *  library's (docs/TS_LANGUAGE_BUILD_2026_10_30.md ruling 4). */
export function callsLibrary(query: ValueSpecification): boolean {
  for (const f of functionsCalled(query)) if (f.startsWith(`${LIBRARY_PACKAGE}::`)) return true;
  let island = false;
  walk(query, (n) => {
    if (n._type === 'classInstance' && n.type === 'TS') island = true;
  });
  return island;
}

/** The library's text (pure-library-text.ts), fetched where a query first calls it: a chunk of its own. */
export async function loadLibrary(): Promise<string> {
  return (await lazily(() => import('./pure-library-text.ts'))).DATACUBE_LIBRARY;
}

/** A model with the library after it: what a query that calls the library is planned over. */
export function withLibrary(model: string, library: string): string {
  return `${model}\n${library}`;
}
