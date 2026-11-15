// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// THE DESK'S DEEPHAVEN, FOR A TEST (docs/DEPLOYMENT_DESIGN_2026_11_14.md step 6): the market world behind the door and
// the desk's deployment installed (demo/deephaven/market/on-deephaven.ts), then the pinned Deephaven started on the
// install -- lite's loader making every table from the plan -- with the fixture's own flags. A test's lanes start the
// desk's server this one way: no world runs inside the server.

import { join } from 'node:path';
import { readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';

import { runfileDirUrl, runfileFromEnv } from '../../../tools/js/runfiles.mts';
import { startDeskOnDeephaven, type DeskOnDeephaven } from '../../demo/deephaven/market/on-deephaven.ts';
import { DESK_MODEL, ON_DEEPHAVEN } from '../../demo/deephaven/market/model.ts';
import type { MarketSettings } from '../../demo/market-sim/market.ts';
import { LitePlanner } from '../../src/lite-planner.ts';

/** The desk's model, from the runfiles the lane names (TRADING_MODEL, MARKET_PURE, ENVIRONMENTS_PURE). */
function deskModel(): string {
  return DESK_MODEL(readFileSync(runfileFromEnv('TRADING_MODEL'), 'utf8'), readFileSync(runfileFromEnv('MARKET_PURE'), 'utf8'),
    readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8'));
}

/** The planner over `model`: legend-lite's WebAssembly module, in this thread. */
function plannerOver(model: string): LitePlanner {
  return new LitePlanner({ model, runtime: ON_DEEPHAVEN.runtime, assetBaseUrl: runfileDirUrl('WASM_PLANNER') });
}

/** How the pinned Deephaven is started (demo/deephaven/site.mjs startDeephaven, which the driver hands in). */
export type StartDeephaven = (o: { readonly quiet: boolean; readonly fixture: string; readonly env: Readonly<Record<string, string>>;
  readonly args: readonly string[] }) => { next(what: string): Promise<{ url: string; psk: string }>; restart(): void;
  stop(): Promise<void> };

export interface DeskServer {
  readonly desk: DeskOnDeephaven;
  /** The fixture's next JSON line (its address and key, at start and after a restart). */
  next(what: string): Promise<{ url: string; psk: string }>;
  restart(): void;
  stop(): Promise<void>;
}

/** The world (`settings`) behind the door, the deployment installed, the pinned Deephaven (MARKET_FIXTURE) started on it
 *  by `startDeephaven` with `args`; `after`, files the server's application runs once the tables are made. */
export async function startDeskServer(startDeephaven: StartDeephaven, settings: MarketSettings,
  o: { readonly args?: readonly string[]; readonly after?: readonly string[] } = {}): Promise<DeskServer> {
  const model = deskModel();
  const dir = join(process.env['TEST_TMPDIR'] ?? tmpdir(), 'desk');
  const desk = await startDeskOnDeephaven({ model, planner: plannerOver(model), dir, loader: runfileFromEnv('DESK_LOADER'),
    settings, ...(o.after ? { after: o.after } : {}) });
  const server = startDeephaven({ quiet: true, fixture: 'MARKET_FIXTURE', env: desk.installed.env,
    args: [`--jvm_flag=-Dembedded.deephaven.application=${desk.installed.app}`, ...(o.args ?? [])] });
  return {
    desk,
    next: (what) => server.next(what),
    restart: () => server.restart(),
    stop: async () => {
      await server.stop();
      await desk.feed.close();
    },
  };
}
