// THE TRADING DESK, ONE HOST OVER EITHER ENGINE (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading desk;
// docs/DUCKDB_LIVE_2026_10_25.md L3): what the desk adds to DataCube's app -- the same whichever engine its market runs
// on, a Deephaven server (host.ts) or the tab's own DuckDB (market-sim/local-host.ts):
//
//   * THE PROJECT the desk's saved queries belong to (screens.ts `marketSource`): the one trading model, the desk's own
//     and its environments (environments.pure: the data space market::TradingDesk, a context for the tab's DuckDB and
//     one for a Deephaven server), so a saved query of it opens as any does;
//   * THE ONE BRANCH: a grid on the engine's context (its runtime) is the engine's, its rows the market's; a grid on the
//     desk's other context is refused, by name (docs/ONE_CONNECTION_DESK_2026_10_28.md D2: never run elsewhere); every
//     other grid's rows stay the tab's;
//   * THE DESK, first under Open…'s Examples: one page, a sheet per screen (screens.ts), written from the sources'
//     columns as the market's module types them and the values its engine answers now, on the engine's context.
//
// What differs is the engine's alone (`DeskEngine`): its context, when it starts, how a grid's rows are read and
// followed. (Step 5 of the plan moves the engine's choice into DataCube, by the connection's type.)

import type { Plan } from '../../../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../../../engine-client/src/result.ts';
import type { GridRows, Services } from '../../../src/app/services.ts';
import type { ExamplePage } from '../../../src/examples/catalog.ts';
import type { LitePlanner } from '../../../src/lite-planner.ts';
import { runOn } from '../../../src/run-on.ts';
import { DESK_SPACE, IN_TAB, ON_DEEPHAVEN, type DeskContext } from './model.ts';
import { DESK, MARKET_PROJECT, type MarketValues, type SourceTypes } from './screens.ts';

/** The market, opened: what it adds to the tab's services, and its example pages. */
export interface Market {
  /** The tab's services with the market's added. */
  over(services: Services): Services;
  /** Open…'s Examples of its own: the trading desk. */
  readonly examples: readonly ExamplePage[];
}

/** The market's engine started: the whole model's text (the corpus, the desk's own, the environments), and the module
 *  that holds it. */
export interface Started {
  readonly model: string;
  readonly planner: LitePlanner;
}

/** What the desk needs of the engine its market runs on. */
export interface DeskEngine {
  /** The project's files: where the page serves the trading model, the desk's own and its environments. */
  readonly modelUrl: string;
  readonly deskModelUrl: string;
  readonly environmentsUrl: string;
  /** The execution context of the desk's data space this engine runs (model.ts): its runtime's grids are its own. */
  readonly context: DeskContext;
  /** Where the market is, in the project's title: "the market world", "the market in this tab". */
  readonly where: string;
  /** Open…'s label and words for the desk, where they are not the desk's own (screens.ts DESK). */
  readonly example?: Pick<ExamplePage, 'label' | 'about'>;
  /** The engine started, once; `ready` where it has started already (its knowledge then read as the tab starts). */
  start(): Promise<Started>;
  readonly ready: boolean;
  /** A source's rows on the engine, its runtime the context's: its runner, and each grid's own live rows. Its model's
   *  text is `text`. */
  rows(text: string): GridRows;
  /** A plan's rows now, read once: what a page starts on. */
  once(plan: Plan): Promise<ResultTable>;
}

/** The desk over `engine`. */
export function openDesk(engine: DeskEngine): Market {
  // the desk's other contexts: a page run on one this site does not run is told so, never run on another engine
  const elsewhere = [IN_TAB, ON_DEEPHAVEN].filter((c) => c.runtime !== engine.context.runtime);
  /** A source's columns as the module types them, by its text. */
  const types = (s: Started): SourceTypes => async (text) => (await s.planner.relationTypeText(text))
    .map((c) => ({ name: c.name, type: c.type }));
  /** The values of a query's first column now. */
  const values = (s: Started): MarketValues => async (text) => {
    const column = (await engine.once(await s.planner.planText(text))).columns[0];
    return column === undefined ? [] : [...column.values].filter((v) => v !== null && v !== undefined).map(String);
  };
  return {
    over: (services) => {
      const { engine: tab, queries } = services;
      if (queries === undefined) throw new Error('the market\'s screens are saved queries: this page reads none');
      /** A model's elements, read once for the model read last (the desk's 2.3 MB of Pure: every saved query of its
       *  project asks for them), at lite's dialect -- its connections lite's own words (docs/ONE_CONNECTION_DESK_2026_10_28.md
       *  D1). */
      let elements: { readonly model: string; readonly read: Promise<unknown[]> } | undefined;
      const read = (model: string): Promise<unknown[]> => {
        if (elements?.model !== model) {
          const reading = tab.reading.elements(model);
          elements = { model, read: reading };
          reading.catch(() => { if (elements?.read === reading) elements = undefined; });
        }
        return elements.read;
      };
      // read now where the engine has started (eager about knowledge: docs/TENETS.md) -- the first saved query opened
      // waited for it
      if (engine.ready) void engine.start().then((s) => read(s.model)).catch(() => undefined);
      return {
        ...services,
        engine: {
          ...tab,
          reading: { parse: (text) => tab.reading.parse(text), elements: read },
          // THE ONE BRANCH: the context's runtime runs on the market's engine; another of the desk's is refused, by name
          rows: (text, runtime, how) => {
            if (runtime === engine.context.runtime) return engine.rows(text);
            const other = elsewhere.find((c) => c.runtime === runtime);
            if (other !== undefined) {
              throw new Error(`this page runs the trading desk on '${other.name}', which this site does not run: it runs `
                + `'${engine.context.name}' (${DESK_SPACE})`);
            }
            return tab.rows(text, runtime, how);
          },
        },
        queries: {
          ...queries,
          // its files in the order the model joins them: the corpus, the desk's own, the environments
          projects: [...queries.projects, { ...MARKET_PROJECT, title: `The trader screens (${engine.where})`,
            models: [engine.modelUrl, engine.deskModelUrl, engine.environmentsUrl] }],
        },
      } satisfies Services;
    },
    // the desk on this engine's context: its saved queries name it, so the page saved keeps it (Run on: src/run-on.ts)
    examples: [{ ...DESK, ...engine.example, build: async () => {
      const s = await engine.start();
      return runOn(await DESK.build(types(s), values(s)), DESK_SPACE, engine.context.name);
    } }],
  };
}

/** A model file's text, fetched: refused, in words, when the page does not serve it. */
export async function fetched(what: string, url: string): Promise<string> {
  const answered = await fetch(url);
  if (!answered.ok) throw new Error(`${what} (${url}) answered ${answered.status}`);
  return answered.text();
}
