// THE MARKET WORLD'S FEED, READ (//datacube:deephaven_feed_test; docs/DEEPHAVEN_START_HERE.md, the desk under load, 5):
// the page holds the trading model with the WebAssembly planner, as the market lane's page does (market-page.ts), and
// answers the driver (feed.mjs), which runs a fake feed beside the server: what the world says of its crypto, and the
// coins' prints by venue. The driver makes the feed misbehave and judges.

import { DeephavenSession, type DeephavenServer } from '../../../engine-client/src/deephaven.ts';
import { LiveClient, type Followed, type FollowedRows } from '../../../engine-client/src/deephaven-live.ts';
import { followedTable } from '../../../engine-client/src/deephaven-rows.ts';
import { relationColumns, type Plan } from '../../../engine-client/src/relation-type.ts';
import { DESK_MODEL, ON_DEEPHAVEN } from '../../demo/deephaven/market/model.ts';
// @ts-ignore the module's runtime, served beside the page from //wasm:planner
import { load } from './wasm-gc-module-runtime.js';

const TRADE = 'marketdata::MarketTrade.all()';
const W = '#>{market::World.world}#';
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

const feed: {
  ready: boolean;
  setupError?: string;
  /** The world's crypto row: its state and what it says. */
  crypto?: () => Promise<[string, string]>;
  /** The coins' prints by venue, and the equities' prints, now. */
  prints?: () => Promise<{ venues: Record<string, number>; equities: number }>;
} = { ready: false };
(window as unknown as { __feed: typeof feed }).__feed = feed;

try {
  const config = await (await fetch('config.json')).json() as { url: string; psk: string };
  const server: DeephavenServer = {
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  };
  const [corpus, desk, environments] = await Promise.all(['trading-model.pure', 'market.pure', 'environments.pure']
    .map(async (f) => (await fetch(f)).text()));
  const teavm = await load('classes.wasm', { stackDeobfuscator: { enabled: false } });
  const exports = teavm.exports as {
    holdModelOrError(model: string): string;
    planHeldOrError(handle: number, query: string, runtime: string): string;
    deephavenOrError(op: string, args: string): string;
  };
  const held = exports.holdModelOrError(DESK_MODEL(corpus!, desk!, environments!));
  if (!held.startsWith('OK\n')) throw new Error(`the trading model did not compile: ${held.replace(/\n/g, ' | ')}`);
  const handle = Number(held.slice(3));
  const session = await DeephavenSession.open(server, async (op, args) => exports.deephavenOrError(op, JSON.stringify(args)));
  const live = await LiveClient.open(server, (why) => { feed.setupError ??= `the live reader ended: ${why}`; });
  const planned = (query: string): Plan => {
    const plan = exports.planHeldOrError(handle, query, ON_DEEPHAVEN.runtime);
    if (!plan.startsWith('OK\n')) throw new Error(`the planner refused ${query}: ${plan.replace(/\n/g, ' | ')}`);
    const body = JSON.parse(plan.slice(3)) as { sql: string; type: unknown };
    return { sql: body.sql, columns: relationColumns(body.type) };
  };
  /** A query's rows as DataCube's engine reads them: its first snapshot. */
  const rows = async (query: string): Promise<unknown[][]> => {
    const plan = planned(query);
    const kept = await session.keepAlive(plan.sql);
    try {
      const name = await session.publishKept(kept.exportId);
      let first: unknown[][] | null = null;
      let failed: unknown = null;
      const followed: Followed = await live.follow(name, (r: FollowedRows) => {
        try {
          const table = followedTable(r, plan, 0, 0);
          first ??= Array.from({ length: table.rowCount }, (_, i) => table.columns.map((c) => c.values[i]));
        } catch (e) {
          failed ??= e;
        }
      }, (e) => { failed ??= e; });
      for (let i = 0; i < 200 && first === null && failed === null; i++) await sleep(50);
      followed.close();
      if (failed !== null) throw failed;
      if (first === null) throw new Error(`no rows in 10 s for ${query}`);
      return first;
    } finally {
      kept.release();
    }
  };
  feed.crypto = async () => {
    const [row] = await rows(`${W}->filter(x|$x.item == 'crypto')`);
    return [String(row?.[1]), String(row?.[2])];
  };
  feed.prints = async () => {
    const byVenue = await rows(`${TRADE}->project(~[venue:x|$x.venue, crypto:x|$x.instrument.assetClass == 'CRYPTO'])`
      + '->groupBy(~[venue, crypto], ~[n:x|1:y|$y->count()])');
    const venues: Record<string, number> = {};
    let equities = 0;
    for (const [venue, crypto, n] of byVenue) {
      if (crypto === true) venues[String(venue)] = Number(n);
      else equities += Number(n);
    }
    return { venues, equities };
  };
  feed.ready = true;
} catch (e) {
  feed.setupError = String((e as Error)?.stack ?? e);
}
