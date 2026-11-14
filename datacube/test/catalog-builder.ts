// Planners for tests: legend-lite's own planner, through the test's host (#test-host) -- in Node the module the tab
// loads, in the headless lane the native library behind the host's worker.

import { plannerOptions } from '#test-host';
import { LitePlanner } from '../src/lite-planner.ts';
import type { PageDocument } from '../src/page-document.ts';
import { readSeriesTexts, type TsReader } from '../src/timeseries-text.ts';

/** A planner over a model, from the test's host. */
export function plannerFor(model: string, runtime: string, how: { readonly mapping?: string } = {}): LitePlanner {
  return new LitePlanner({ model, runtime, ...how, ...plannerOptions(), cache: false });
}

/** Core's reader of a timeseries series' text (timeseries-text.ts TsReader): one planner of no model, made when first
 *  asked. */
let grammar: LitePlanner | undefined;
export const readText: TsReader = (text) => (grammar ??= plannerFor('', 'local::RT')).tsItem(text);

/** Every timeseries tile's series' texts of `page` read by core (A3 step 3): what a walk of their trees needs first. */
export async function readPageTexts(page: PageDocument): Promise<PageDocument> {
  await Promise.all(page.views.flatMap((v) => (v.kind === 'timeseries' ? [readSeriesTexts(v.spec.series, readText)] : [])));
  return page;
}
