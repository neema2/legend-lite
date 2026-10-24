// THE MARKET WORLD, CHECKED (//datacube:deephaven_market_test; docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K2, DH28):
// the page plans each question in Pure on the market's runtime with the WebAssembly planner, sends it straight to the
// pinned Deephaven running the market world (demo/deephaven/market/market.groovy), and reads the rows as DataCube's
// engine reads them (followedTable). Its feed points at an address that refuses it, so the world must say so and
// simulate crypto in its place. Every check runs here; the driver (market.mjs) reports what the page found.

import { DeephavenSession, type DeephavenServer } from '../../../engine-client/src/deephaven.ts';
import { LiveClient, type Followed, type FollowedRows } from '../../../engine-client/src/deephaven-live.ts';
import { followedTable } from '../../../engine-client/src/deephaven-rows.ts';
import { relationColumns, type Plan } from '../../../engine-client/src/relation-type.ts';
import { MARKET_MODEL, MARKET_RUNTIME } from '../../demo/deephaven/market/model.ts';
// @ts-ignore the module's runtime, served beside the page from //wasm:planner
import { load } from './wasm-gc-module-runtime.js';

interface Outcome {
  readonly name: string;
  readonly ok: boolean;
  readonly message: string;
}

const verdict: { outcomes: Outcome[]; setupError?: string; done: boolean } = { outcomes: [], done: false };
(window as unknown as { __market: typeof verdict }).__market = verdict;

const I = '#>{market::Store.instrument}#';
const Q = '#>{market::Store.quote}#';
const T = '#>{market::Store.trade}#';
const W = '#>{market::Store.world}#';
const RM = 'meta::pure::functions::math::mathUtility::rowMapper';
const LEFT = 'meta::pure::functions::relation::JoinKind.LEFT';
const INNER = 'meta::pure::functions::relation::JoinKind.INNER';

async function check(name: string, run: () => Promise<string | null>): Promise<void> {
  try {
    const wrong = await run();
    verdict.outcomes.push({ name, ok: wrong === null, message: wrong ?? '' });
  } catch (e) {
    verdict.outcomes.push({ name, ok: false, message: String((e as Error)?.stack ?? e) });
  }
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

try {
  const config = await (await fetch('config.json')).json() as { url: string; psk: string; names: number; trades: number };
  const server: DeephavenServer = {
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  };
  const at = new URL(config.url);
  const model = MARKET_MODEL(at.hostname, Number(at.port));
  const teavm = await load('classes.wasm', { stackDeobfuscator: { enabled: false } });
  const exports = teavm.exports as {
    planOrError(model: string, query: string, runtime: string): string;
    deephavenOrError(op: string, args: string): string;
  };
  const session = await DeephavenSession.open(server, async (op, args) => exports.deephavenOrError(op, JSON.stringify(args)));
  const live = await LiveClient.open(server, (why) => { verdict.setupError ??= `the live reader ended: ${why}`; });
  const planned = (query: string): Plan => {
    const plan = exports.planOrError(model, query, MARKET_RUNTIME);
    if (!plan.startsWith('OK\n')) throw new Error(`the planner refused ${query}: ${plan.replace(/\n/g, ' | ')}`);
    const body = JSON.parse(plan.slice(3)) as { sql: string; type: unknown };
    return { sql: body.sql, columns: relationColumns(body.type) };
  };
  /** A query's rows as DataCube's engine reads them, its first snapshot; `each` sees every snapshot for `ms` more. */
  const rows = async (query: string, ms = 0, each?: (rows: unknown[][]) => void): Promise<unknown[][]> => {
    const plan = planned(query);
    const kept = await session.keepAlive(plan.sql);
    try {
      const name = await session.publishKept(kept.exportId);
      const read = (r: FollowedRows): unknown[][] => {
        const table = followedTable(r, plan, 0, 0);
        return Array.from({ length: table.rowCount }, (_, i) => table.columns.map((c) => c.values[i]));
      };
      let first: unknown[][] | null = null;
      let failed: unknown = null;
      const followed: Followed = await live.follow(name, (r) => {
        try {
          const now = read(r);
          first ??= now;
          each?.(now);
        } catch (e) {
          failed ??= e;
        }
      });
      for (let i = 0; i < 200 && first === null && failed === null; i++) await sleep(50);
      if (ms > 0) await sleep(ms);
      followed.close();
      if (failed !== null) throw failed;
      if (first === null) throw new Error(`no rows in 10 s for ${query}`);
      return first;
    } finally {
      kept.release();
    }
  };
  /** The one number a query answers in its one row. */
  const number = async (query: string): Promise<number> => Number((await rows(query))[0]?.[0]);

  const equities = config.names;
  // the world's first beat quotes every name; a second for its trades to start
  await sleep(1500);

  await check('the world says what it is: equities simulated, crypto simulated because its feed was refused', async () => {
    const got = await rows(`${W}->sort(~item->ascending())`);
    const [crypto, eq] = got;
    if (got.length !== 2 || crypto?.[0] !== 'crypto' || eq?.[0] !== 'equities') return `the world: ${JSON.stringify(got)}`;
    if (eq[1] !== 'simulated' || !String(eq[2]).startsWith(`${equities} names`)) return `equities: ${JSON.stringify(eq)}`;
    return crypto[1] === 'simulated' && String(crypto[2]).startsWith('the feed was refused') ? null
      : `crypto: ${JSON.stringify(crypto)}`;
  });

  await check(`every name is an instrument: ${equities} equities and 8 coins, eleven sectors and crypto`, async () => {
    const kinds = await rows(`${I}->groupBy(~[kind], ~[n:x|$x.sym:y|$y->count()])->sort(~kind->ascending())`);
    const sectors = await number(`${I}->groupBy(~[], ~[n:x|$x.sector:y|$y->distinct()->count()])`);
    return JSON.stringify(kinds) === JSON.stringify([['CRYPTO', 8], ['EQUITY', equities]]) && sectors === 12 ? null
      : `kinds ${JSON.stringify(kinds)}, sectors ${sectors}`;
  });

  await check('every name is quoted, its bid under its ask by at least its tick', async () => {
    const quoted = await number(`${Q}->groupBy(~[], ~[n:x|$x.qsym:y|$y->count()])`);
    const crossed = await number(`${Q}->join(${I}, ${LEFT}, {q, i | $q.qsym == $i.sym})`
      + `->filter(x|($x.bid <= 0.0) || (($x.ask - $x.bid) < ($x.tick->toOne() - 0.0000001)))`
      + `->groupBy(~[], ~[n:x|$x.qsym:y|$y->count()])`);
    return quoted === equities + 8 && crossed === 0 ? null : `${quoted} quoted, ${crossed} crossed or under a tick`;
  });

  await check('every trade is a known name\'s, at a positive price, an aggressor\'s side, a size', async () => {
    // one query, so the trades counted are the trades checked: all of them, and those of no instrument or ill formed
    const [counted] = await rows(`${T}->join(${I}, ${LEFT}, {t, i | $t.tsym == $i.sym})`
      + `->extend(~[bad:x|$x.sym->isEmpty() || ($x.px <= 0.0) || ($x.sz <= 0.0) || !$x.side->in(['B', 'S'])])`
      + `->groupBy(~[], ~[all:x|$x.seq:y|$y->count(), bad:x|if($x.bad, |1, |[]):y|$y->count()])`);
    const [all, bad] = [Number(counted?.[0]), Number(counted?.[1])];
    return all > 0 && bad === 0 ? null : `${bad} of ${all} trades are ill formed`;
  });

  await check(`trades print at the world's pace: about ${config.trades} a second`, async () => {
    const count = `${T}->groupBy(~[], ~[n:x|$x.seq:y|$y->count()])`;
    const t0 = Date.now();
    const a = await number(count);
    await sleep(5000);
    const b = await number(count);
    // the equities' rate, and the simulated coins' tenth of it
    const rate = (b - a) / ((Date.now() - t0) / 1000);
    const expected = config.trades * 1.1;
    return rate > expected * 0.6 && rate < expected * 1.4 ? null : `${rate.toFixed(0)} a second, not about ${expected}`;
  });

  await check('a minute\'s candles hold their open and close between their low and high', async () => {
    const candles = `${T}->extend(~[bar:x|$x.ts->timeBucket(1, DurationUnit.MINUTES)])`
      + `->groupBy(~[tsym, bar], ~[o:x|${RM}($x.px, $x.seq):y|$y->minBy(), hi:x|$x.px:y|$y->max(),`
      + ` lo:x|$x.px:y|$y->min(), c:x|${RM}($x.px, $x.seq):y|$y->maxBy(), v:x|$x.sz:y|$y->sum()])`;
    const n = await number(`${candles}->groupBy(~[], ~[n:x|$x.tsym:y|$y->count()])`);
    const outside = await number(`${candles}->filter(x|($x.o < $x.lo) || ($x.o > $x.hi) || ($x.c < $x.lo)`
      + ` || ($x.c > $x.hi) || ($x.v <= 0.0))->groupBy(~[], ~[n:x|$x.tsym:y|$y->count()])`);
    return n > 0 && outside === 0 ? null : `${n} candles, ${outside} with an open or close outside them`;
  });

  await check('a name\'s day so far: its VWAP between its low and its high', async () => {
    const day = `${T}->groupBy(~[tsym], ~[pv:x|$x.px * $x.sz:y|$y->sum(), v:x|$x.sz:y|$y->sum(),`
      + ` hi:x|$x.px:y|$y->max(), lo:x|$x.px:y|$y->min()])->extend(~[vwap:x|$x.pv / $x.v])`;
    const names = await number(`${day}->groupBy(~[], ~[n:x|$x.tsym:y|$y->count()])`);
    // a group's least and greatest are Pure's [0..1]: their arithmetic takes their one
    const off = await number(`${day}->filter(x|($x.vwap < ($x.lo->toOne() - 0.000001))`
      + ` || ($x.vwap > ($x.hi->toOne() + 0.000001)))`
      + `->groupBy(~[], ~[n:x|$x.tsym:y|$y->count()])`);
    return names > 0 && off === 0 ? null : `${names} names traded, ${off} with a VWAP outside their range`;
  });

  await check('crypto ticks, simulated, with its feed refused: every coin traded, each print the simulator\'s', async () => {
    const coins = await rows(`${T}->join(${I}, ${INNER}, {t, i | $t.tsym == $i.sym})->filter(x|$x.kind == 'CRYPTO')`
      + `->groupBy(~[venue], ~[n:x|$x.tsym:y|$y->distinct()->count()])`);
    return JSON.stringify(coins) === JSON.stringify([['SIM', 8]]) ? null : `the coins' prints by venue: ${JSON.stringify(coins)}`;
  });

  await check('a quote ticks: the busiest name\'s moves several times in two seconds', async () => {
    const busiest = String((await rows(`${I}->filter(x|$x.kind == 'EQUITY')->sort(~shares->descending())->limit(1)`
      + `->select(~[sym])`))[0]?.[0]);
    const seen = new Set<string>();
    await rows(`${Q}->filter(x|$x.qsym == '${busiest}')->select(~[bid, ask, bidSize, askSize])`, 2000,
      (now) => seen.add(JSON.stringify(now)));
    return seen.size >= 3 ? null : `${busiest}'s quote seen ${seen.size} ways in two seconds`;
  });

  live.close();
  await session.close();
} catch (e) {
  verdict.setupError = String((e as Error)?.stack ?? e);
}
verdict.done = true;
