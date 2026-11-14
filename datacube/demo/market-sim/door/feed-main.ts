// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// THE FEED, AS A PROCESS (docs/DESK_DATA_ROADMAP_2026_10_29.md B1): `bazel run //datacube:market_feed` starts the market
// world behind the door on the loopback, for a Deephaven beside it to read with its own Kafka reader. The model is the
// desk's (the corpus, the desk's own, its environments: the runfiles TRADING_MODEL, MARKET_PURE, ENVIRONMENTS_PURE), the
// planner legend-lite's WebAssembly module in this thread (WASM_PLANNER). Settings, each optional: DOOR_PORT (19092),
// FEED_NAMES (the world's equities, 100), FEED_CRYPTO ('off' for no Coinbase feed: its coins are then simulated, as
// they are when the feed is refused). Every 10 s it says what it has written; it stops on SIGINT or SIGTERM.

import { readFileSync } from 'node:fs';

import { runfileDirUrl, runfileFromEnv } from '../../../../tools/js/runfiles.mts';
import { DESK_MODEL, ON_DEEPHAVEN } from '../../deephaven/market/model.ts';
import { LitePlanner } from '../../../src/lite-planner.ts';
import { startFeed } from './feed.ts';

const model = DESK_MODEL(readFileSync(runfileFromEnv('TRADING_MODEL'), 'utf8'),
  readFileSync(runfileFromEnv('MARKET_PURE'), 'utf8'), readFileSync(runfileFromEnv('ENVIRONMENTS_PURE'), 'utf8'));
const planner = new LitePlanner({ model, runtime: ON_DEEPHAVEN.runtime, assetBaseUrl: runfileDirUrl('WASM_PLANNER') });
const env = process.env;
const feed = await startFeed({
  model, planner,
  port: Number(env['DOOR_PORT'] ?? 19092),
  settings: {
    ...(env['FEED_NAMES'] !== undefined ? { names: Number(env['FEED_NAMES']) } : {}),
    ...(env['FEED_CRYPTO'] === 'off' ? { feed: 'off' as const } : {}),
  },
});
console.log(`the door listens on 127.0.0.1:${feed.port}: ${feed.topics.map((t) => t.topic).join(', ')}`);
const report = setInterval(() => {
  console.log(Object.entries(feed.counts.records).map(([t, n]) => `${t} ${n}`).join('; '));
}, 10_000);
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => {
    clearInterval(report);
    void feed.close().then(() => process.exit(0));
  });
}
