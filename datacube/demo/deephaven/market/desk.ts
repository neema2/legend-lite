// THE TRADING DESK, AN EXAMPLE OF DATACUBE'S (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading desk;
// docs/ONE_CONNECTION_DESK_2026_10_28.md): what the desk adds to DataCube's app, the same on every site -- where its
// grids run is DataCube's, by its runtime's connections (step 5: the tab's DuckDB, or a Deephaven server):
//
//   * THE PROJECT the desk's saved queries belong to (screens.ts `marketSource`): the one trading model, the desk's own
//     and its environments (environments.pure: the data space market::TradingDesk, a context for the tab's DuckDB and
//     one for a Deephaven server) -- a version the site releases, found in the model home as every project is
//     (docs/ONE_CONNECTION_DESK_2026_10_28.md D5: the site's releases folder), so a saved query of it opens as any does;
//   * THE MARKET IN THIS TAB, until the Market data source (step 6): the stores of the data space's tab context made live
//     in the tab's DuckDB, a world simulated in it writing them (market-sim/local-host.ts) -- started when a grid on
//     that context first reads its rows;
//   * THE DESK, first under Open…'s Examples: one page, a sheet per screen (screens.ts), written from the sources'
//     columns and the values they answer now, on the context the deployment names (config.json `contexts`), else the
//     data space's default -- its saved queries name it, so the page saved keeps it (src/run-on.ts).

import type { Services } from '../../../src/app/services.ts';
import type { ExamplePage } from '../../../src/examples/catalog.ts';
import type { LiveSource } from '../../../src/live-in-tab.ts';
import { runOn } from '../../../src/run-on.ts';
import { DESK_SPACE, IN_TAB, ON_DEEPHAVEN } from './model.ts';
import { DESK, MARKET_PROJECT, type MarketValues, type SourceTypes } from './screens.ts';

/** The desk opened: the tab's services with its market added, and its example. */
export interface Desk {
  readonly services: Services;
  readonly examples: readonly ExamplePage[];
}

export interface DeskOptions {
  /** The execution context the deployment runs each data space on, by its path (config.json `contexts`): the desk's
   *  absent, its data space's default. */
  readonly contexts: Readonly<Record<string, string>>;
  /** The market in this tab (market-sim/local-host.ts), the stores of the data space's tab context made live; absent
   *  where the page has no tab DuckDB. */
  readonly inTab?: (model: () => Promise<string>) => LiveSource;
}

/** Open…'s words for the desk, by the context it is written on. */
const WORDS: Readonly<Record<string, Pick<ExamplePage, 'label' | 'about'>>> = {
  [IN_TAB.name]: {
    label: 'Trading desk: ten screens, live in this tab',
    about: 'A trading floor on one page, a tab a screen, every figure computed by DuckDB in this tab from a market world '
      + 'simulated in it, ticking: the market map and movers; a stock from the open, its candles, quote, prints and news; a '
      + 'news terminal; the order flow; a crypto desk; the execution blotter; TCA and markouts; P&L against limits; the options '
      + 'chain and its risk; FX rates, crosses and RFQs. Nothing runs on a server.',
  },
  [ON_DEEPHAVEN.name]: { label: DESK.label, about: DESK.about },
};

/** The desk over the tab's `services`. */
export function openDesk(services: Services, o: DeskOptions): Desk {
  const { queries, modelHome } = services;
  if (queries === undefined) throw new Error('the trading desk\'s screens are saved queries: this page reads none');
  if (modelHome === undefined) throw new Error('the trading desk\'s project is found in the model home: this page reaches none (config.json `depot`)');
  /** The project's model: its version's text, read once from the model home's Depot, as its saved queries read it. */
  let reading: Promise<string> | undefined;
  const model = (): Promise<string> => (reading ??= Promise.all([import('../../../../depot-client/src/model-text.ts'), modelHome.connect()])
    .then(([{ modelText }, home]) => modelText(home.depot, MARKET_PROJECT.groupId, MARKET_PROJECT.artifactId, MARKET_PROJECT.versionId)));
  if (o.inTab !== undefined) services.engine.local?.live.serve(o.inTab(model));
  const context = o.contexts[DESK_SPACE];
  const words = WORDS[context ?? IN_TAB.name];
  if (words === undefined) throw new Error(`the trading desk has no context '${context}' (${DESK_SPACE}): its contexts are ${Object.keys(WORDS).join(', ')}`);
  return {
    services,
    examples: [{ ...DESK, ...words, build: () => written(services, model, context) }],
  };
}

/** The desk, written on `context` (absent: the data space's default): its sources typed, and what it starts on read
 *  now, by the rows of the runtime the context runs on -- wherever DataCube runs them. */
async function written(services: Services, model: () => Promise<string>, context: string | undefined) {
  const text = await model();
  // the project's elements first: what the context's runtime runs on is read with them
  const on = contextOf(await services.engine.reading.elements(text), context);
  const rows = services.engine.rows(text, on.runtime);
  try {
    const typer = rows.runner ?? rows.planner;
    const parsed = (query: string) => typer.parse(`|${query}`);
    const types: SourceTypes = async (query) => (await typer.relationType(await parsed(query)))
      .map((c) => ({ name: c.name, type: c.type }));
    const values: MarketValues = async (query) => {
      const lambda = await parsed(query);
      const table = rows.runner !== undefined ? (await rows.runner.host.planAndRun(lambda, 1)).rows
        : await rows.engine.execute(await rows.planner.plan(lambda), 1);
      const column = table.columns[0];
      return column === undefined ? [] : [...column.values].filter((v) => v !== null && v !== undefined).map(String);
    };
    return runOn(await DESK.build(types, values), DESK_SPACE, on.name);
  } finally {
    rows.planner?.dispose?.();
  }
}

/** The desk's data space's context named `context` (absent: its default), and its runtime, as the elements declare it. */
function contextOf(elements: readonly unknown[], context: string | undefined): { readonly name: string; readonly runtime: string } {
  const space = elements.find((e) => {
    const o = e as { _type?: unknown; package?: unknown; name?: unknown };
    return o._type === 'dataSpace' && `${String(o.package)}::${String(o.name)}` === DESK_SPACE;
  }) as { defaultExecutionContext?: string; executionContexts?: readonly { name: string; defaultRuntime?: { path: string } }[] } | undefined;
  if (space === undefined) throw new Error(`the desk's model declares no data space ${DESK_SPACE}`);
  const name = context ?? space.defaultExecutionContext;
  const runtime = space.executionContexts?.find((c) => c.name === name)?.defaultRuntime?.path;
  if (name === undefined || runtime === undefined) throw new Error(`${DESK_SPACE} has no execution context '${name ?? ''}' with a runtime`);
  return { name, runtime };
}
