// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// THE DESK ON DEEPHAVEN, ONE PATH (docs/DEPLOYMENT_DESIGN_2026_11_14.md §10, step 6): the market world behind lite's
// Kafka door -- the producers' stand-in, publishing every topic the desk's ingests read and dropping position keeping's
// files -- and the desk's deployment installed for a Deephaven server: its plan (the planner's, data) beside lite's one
// fixed loader, which makes each table from its topic or its file when the server starts. No world runs inside the
// server: what it holds is the plan's, landed from the log. The server itself is the caller's to start, on the
// application this writes (`installed.app`, with `installed.env`). Node alone.

import { join } from 'node:path';

import { writeDeliveries } from '../../market-sim/door/deliveries.ts';
import { startFeed, type Feed } from '../../market-sim/door/feed.ts';
import type { MarketSettings } from '../../market-sim/market.ts';
import { installDeephaven, planOf, type DeephavenPlan, type Installed } from '../install.ts';
import { DESK_ON_DEEPHAVEN } from './model.ts';

/** The desk's environments name the brokers and the drop location these: a deployment run elsewhere names its own. */
export const DESK_BROKERS = "bootstrapServers: '127.0.0.1:19092';";
export const DESK_DROP = "location: 'file:///tmp/legend-desk/positions/';";

export interface DeskOnDeephavenOptions {
  /** The desk's whole model (model.ts DESK_MODEL): its environments name the brokers and drop location above. */
  readonly model: string;
  /** The planner over it: its `deephaven` request answers the topics and the deployment's plan. */
  readonly planner: { deephaven(op: string, args: Readonly<Record<string, unknown>>): Promise<string> };
  /** Where the install and the delivered files go. */
  readonly dir: string;
  /** Lite's loader (core/src/main/deephaven/legend-loader.groovy), its path. */
  readonly loader: string;
  /** The world's size and pace. */
  readonly settings?: MarketSettings;
  /** The door's port (0: any free one). */
  readonly port?: number;
  /** Files the server's application runs after the loader (a test's check). */
  readonly after?: readonly string[];
  /** Where a refusal of the door's is said. */
  readonly say?: (message: string) => void;
}

export interface DeskOnDeephaven {
  readonly feed: Feed;
  readonly plan: DeephavenPlan;
  readonly installed: Installed;
  /** The model as the server's plan was rendered from: its brokers the door's, its drop location `dir`'s. */
  readonly model: string;
}

/** The world started behind the door, its files dropped, and the desk's deployment installed in `dir`. */
export async function startDeskOnDeephaven(o: DeskOnDeephavenOptions): Promise<DeskOnDeephaven> {
  if (!o.model.includes(DESK_BROKERS) || !o.model.includes(DESK_DROP)) {
    throw new Error(`the desk's environments name no brokers (${DESK_BROKERS}) or drop location (${DESK_DROP})`);
  }
  const feed = await startFeed({ model: o.model, planner: o.planner, port: o.port ?? 0, settings: o.settings ?? {},
    ...(o.say ? { say: o.say } : {}) });
  try {
    const model = o.model.replaceAll(DESK_BROKERS, `bootstrapServers: '127.0.0.1:${feed.port}';`)
      .replaceAll(DESK_DROP, `location: 'file://${join(o.dir, 'drop')}/';`);
    const plan = planOf(await o.planner.deephaven('plan', { model, deployment: DESK_ON_DEEPHAVEN }));
    // position keeping drops its files before the server starts: the start of the day, whole
    writeDeliveries(plan.tables.filter((t) => t.source.kind === 'file'), feed.kept);
    const installed = installDeephaven(join(o.dir, 'app'), plan, o.loader, o.after ?? []);
    return { feed, plan, installed, model };
  } catch (e) {
    await feed.close();
    throw e;
  }
}
