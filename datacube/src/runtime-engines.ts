// WHERE A GRID'S ROWS RUN, by its runtime's connections (docs/ONE_CONNECTION_DESK_2026_10_28.md step 5): from a
// model's elements, what each of its runtimes runs on (runtime-connections.ts `runtimesOf`) -- a Deephaven server's
// runtime there, each grid its own reader (deephaven-grids.ts); one a source has made live in this tab on the live
// engine (live-in-tab.ts); any other on the tab's DuckDB, as the page plans and runs every grid (services/browser.ts).
// This code is a chunk of its own, fetched with the first model the page reads; the Deephaven grids' code is fetched
// when a model read names a server -- code, not work: a server is signed in to when a grid first reads its rows.

import type { GridRows } from './app/services.ts';
import type { DeephavenGrids, DeephavenGridsOptions } from './deephaven-grids.ts';
import { liveRows, type LiveSource } from './live-in-tab.ts';
import { runtimesOf } from './runtime-connections.ts';

export interface RuntimeEnginesOptions extends DeephavenGridsOptions {
  /** The runtimes sources have made live in this tab (services/browser.ts `live.serve`), by their path. */
  readonly served: ReadonlyMap<string, LiveSource>;
  /** The Deephaven grids' code, fetched (the page's own fetch of a part: lazily.ts). */
  readonly deephaven: () => Promise<typeof import('./deephaven-grids.ts')>;
}

/** A model's runtimes, read: a grid's rows where its runtime runs elsewhere than on the tab's DuckDB; undefined where it
 *  runs there. A runtime the model does not declare, or cannot run (runtime-connections.ts), is refused by name. */
export type ModelEngines = (model: string, runtime: string) => GridRows | undefined;

export class RuntimeEngines {
  readonly #o: RuntimeEnginesOptions;
  #code: Promise<DeephavenGrids> | undefined;
  #grids: DeephavenGrids | undefined;

  constructor(o: RuntimeEnginesOptions) {
    this.#o = o;
  }

  /** What each runtime of a model with these `elements` runs on; the Deephaven grids' code fetched where one runs there. */
  async read(elements: readonly unknown[]): Promise<ModelEngines> {
    const runtimes = runtimesOf(elements);
    for (const on of runtimes.values()) {
      if (!(on instanceof Error) && on.engine.kind === 'deephaven') await this.#deephaven();
    }
    return (model, runtime) => {
      const on = runtimes.get(runtime);
      if (on === undefined) throw new Error(`the model declares no runtime ${runtime}`);
      if (on instanceof Error) throw on;
      if (on.engine.kind === 'deephaven') {
        if (this.#grids === undefined) throw new Error('unreachable: a model naming a Deephaven server fetches its grids\' code as it is read');
        return this.#grids.rows(on.engine.server, model, runtime, on.connections.map((c) => c.store));
      }
      const source = this.#o.served.get(runtime);
      return source === undefined ? undefined : liveRows(source, model);
    };
  }

  #deephaven(): Promise<DeephavenGrids> {
    return (this.#code ??= this.#o.deephaven().then((m) => {
      this.#grids = new m.DeephavenGrids(this.#o);
      return this.#grids;
    }));
  }
}
