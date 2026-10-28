// THE TRADING DESK, ONE HOST OVER EITHER ENGINE (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, the trading desk;
// docs/DUCKDB_LIVE_2026_10_25.md L3): what the desk adds to DataCube's app -- the same whichever engine its market runs
// on, a Deephaven server (host.ts) or the tab's own DuckDB (market-sim/local-host.ts):
//
//   * THE PROJECT the desk's saved queries belong to (screens.ts `marketSource`): the one trading model, the desk's own
//     and the connections -- the engine's, a file of the page's own -- so a saved query of it opens as any does;
//   * THE ONE BRANCH: a grid over the market's runtime is the engine's, its rows the market's; every other grid's rows
//     stay the tab's;
//   * THE DESK, first under Open…'s Examples: one page, a sheet per screen (screens.ts), written from the sources'
//     columns as the market's module types them and the values its engine answers now.
//
// What differs is the engine's alone (`DeskEngine`): when it starts, how a grid's rows are read and followed.

import type { Plan } from '../../../../engine-client/src/relation-type.ts';
import type { ResultTable } from '../../../../engine-client/src/result.ts';
import type { GridRows, Services } from '../../../src/app/services.ts';
import type { ExamplePage } from '../../../src/examples/catalog.ts';
import type { LitePlanner } from '../../../src/lite-planner.ts';
import { MARKET_RUNTIME } from './model.ts';
import { DESK, MARKET_PROJECT, type MarketValues, type SourceTypes } from './screens.ts';

/** The market, opened: what it adds to the tab's services, and its example pages. */
export interface Market {
  /** The tab's services with the market's added. */
  over(services: Services): Services;
  /** Open…'s Examples of its own: the trading desk. */
  readonly examples: readonly ExamplePage[];
}

/** The market's engine started: the whole model's text (the corpus, the desk's own, the connections), and the module
 *  that holds it. */
export interface Started {
  readonly model: string;
  readonly planner: LitePlanner;
}

/** What the desk needs of the engine its market runs on. */
export interface DeskEngine {
  /** The project's files: where the page serves the trading model and the desk's own, and the connections' text. */
  readonly modelUrl: string;
  readonly deskModelUrl: string;
  readonly connections: string;
  /** Where the market is, in the project's title: "the market world", "the market in this tab". */
  readonly where: string;
  /** Open…'s label and words for the desk, where they are not the desk's own (screens.ts DESK). */
  readonly example?: Pick<ExamplePage, 'label' | 'about'>;
  /** The engine started, once; `ready` where it has started already (its knowledge then read as the tab starts). */
  start(): Promise<Started>;
  readonly ready: boolean;
  /** A source's rows on the engine: its runner, and each grid's own live rows. Its model's text is `text`. */
  rows(text: string): GridRows;
  /** A plan's rows now, read once: what a page starts on. */
  once(plan: Plan): Promise<ResultTable>;
}

/** The desk over `engine`. */
export function openDesk(engine: DeskEngine): Market {
  // the project's connections, a file of its own: the page's text, served by the browser from memory
  const connections = URL.createObjectURL(new Blob([engine.connections], { type: 'text/plain' }));
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
      /** The model's elements, read once (2.3 MB of Pure: every saved query of the project asks for them), at lite's
       *  dialect, the whole model's -- its connections lite's own words (docs/ONE_CONNECTION_DESK_2026_10_28.md D1). */
      let elements: Promise<unknown[]> | undefined;
      const read = (model: string): Promise<unknown[]> => {
        elements ??= tab.reading.elements(model);
        elements.catch(() => { elements = undefined; });
        return elements;
      };
      // read now where the engine has started (eager about knowledge: docs/TENETS.md) -- the first saved query opened
      // waited for it
      if (engine.ready) void engine.start().then((s) => read(s.model)).catch(() => undefined);
      return {
        ...services,
        engine: {
          ...tab,
          reading: {
            parse: (text) => tab.reading.parse(text),
            // the market's project's model ends in its connections, the engine's own file: any other is the tab's
            elements: (text) => (text.endsWith(engine.connections) ? read(text) : tab.reading.elements(text)),
          },
          // THE ONE BRANCH: the market's runtime runs on the market's engine
          rows: (text, runtime, how) => (runtime === MARKET_RUNTIME ? engine.rows(text) : tab.rows(text, runtime, how)),
        },
        queries: {
          ...queries,
          // its files in the order the model joins them: the corpus, the desk's own, the connections
          projects: [...queries.projects, { ...MARKET_PROJECT, title: `The trader screens (${engine.where})`,
            models: [engine.modelUrl, engine.deskModelUrl, connections] }],
        },
      } satisfies Services;
    },
    examples: [{ ...DESK, ...engine.example, build: async () => {
      const s = await engine.start();
      return DESK.build(types(s), values(s));
    } }],
  };
}

/** A model file's text, fetched: refused, in words, when the page does not serve it. */
export async function fetched(what: string, url: string): Promise<string> {
  const answered = await fetch(url);
  if (!answered.ok) throw new Error(`${what} (${url}) answered ${answered.status}`);
  return answered.text();
}
