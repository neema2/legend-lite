// THE MARKET WORLD, CHECKED (//datacube:deephaven_market_test; docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K2, DH28,
// DH34): the page holds the one trading model (the stress corpus's, served beside it) with the WebAssembly planner, plans
// each question as a query over the corpus's own classes through stress::AllMapping on the desk's Deephaven runtime, sends it
// straight to the pinned Deephaven running the market world (demo/deephaven/market/market.groovy, which publishes the
// corpus's tables), and reads the rows as DataCube's engine reads them (followedTable). Its feed points at an address
// that refuses it, so the world must say so and simulate crypto in its place. Every check runs here; the driver
// (market.mjs) reports what the page found.

import { DeephavenSession, type DeephavenServer } from '../../../engine-client/src/deephaven.ts';
import { LiveClient, type Followed, type FollowedRows } from '../../../engine-client/src/deephaven-live.ts';
import { followedTable } from '../../../engine-client/src/deephaven-rows.ts';
import { relationColumns, type Plan } from '../../../engine-client/src/relation-type.ts';
import { DESK_MODEL, ON_DEEPHAVEN } from '../../demo/deephaven/market/model.ts';
// @ts-ignore the module's runtime, served beside the page from //wasm:planner
import { load } from './wasm-gc-module-runtime.js';

interface Outcome {
  readonly name: string;
  readonly ok: boolean;
  readonly message: string;
}

const verdict: { outcomes: Outcome[]; setupError?: string; done: boolean } = { outcomes: [], done: false };
(window as unknown as { __market: typeof verdict }).__market = verdict;

// the corpus's classes, each read through stress::AllMapping; the world's own status, a table of its own
const INSTRUMENT = 'products::Instrument.all()';
const EQUITY = 'products::Equity.all()';
const QUOTE = 'marketdata::MarketQuote.all()';
const TRADE = 'marketdata::MarketTrade.all()';
const W = '#>{market::World.world}#';
const RM = 'meta::pure::functions::math::mathUtility::rowMapper';
/** A print's columns: its sequence, its name's ticker and id, its time, price and size. */
const PRINTS = `${TRADE}->project(~[seq:x|$x.sequenceNumber, sym:x|$x.instrument.ticker, inst:x|$x.instrument.instrumentId,`
  + ' at:x|$x.printTime, px:x|$x.price, sz:x|$x.quantity])';

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
  const config = await (await fetch('config.json')).json() as { url: string; psk: string; names: number; trades: number; morning: number; news: number };
  const server: DeephavenServer = {
    url: config.url,
    signIn: { type: 'io.deephaven.authentication.psk.PskAuthenticationHandler', token: config.psk },
  };
  // the desk's model, its environments this test's copy: the Deephaven context at this server
  const [corpus, desk, environments] = await Promise.all(['trading-model.pure', 'market.pure', 'environments.pure']
    .map(async (f) => (await fetch(f)).text()));
  const model = DESK_MODEL(corpus!, desk!, environments!);
  const teavm = await load('classes.wasm', { stackDeobfuscator: { enabled: false } });
  const exports = teavm.exports as {
    holdModelOrError(model: string): string;
    planHeldOrError(handle: number, query: string, runtime: string): string;
    deephavenOrError(op: string, args: string): string;
  };
  // the model compiled once (the whole corpus: measured 4.4 s in WebAssembly), every query planned against it
  const held = exports.holdModelOrError(model);
  if (!held.startsWith('OK\n')) throw new Error(`the trading model did not compile: ${held.replace(/\n/g, ' | ')}`);
  const handle = Number(held.slice(3));
  const session = await DeephavenSession.open(server, async (op, args) => exports.deephavenOrError(op, JSON.stringify(args)));
  const live = await LiveClient.open(server, (why) => { verdict.setupError ??= `the live reader ended: ${why}`; });
  const planned = (query: string): Plan => {
    const plan = exports.planHeldOrError(handle, query, ON_DEEPHAVEN.runtime);
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
      }, (e) => { failed ??= e; });
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

  await check('the world says what it is: equities simulated, crypto simulated because its feed was refused, the news'
    + ' simulated, filings simulated for want of a contact EDGAR asks for', async () => {
    const got = await rows(`${W}->sort(~item->ascending())`);
    const [crypto, eq, filings, news] = got;
    if (got.length !== 4 || crypto?.[0] !== 'crypto' || eq?.[0] !== 'equities' || filings?.[0] !== 'filings'
      || news?.[0] !== 'news') return `the world: ${JSON.stringify(got)}`;
    if (eq[1] !== 'simulated' || !String(eq[2]).startsWith(`${equities} names`)) return `equities: ${JSON.stringify(eq)}`;
    if (news[1] !== 'simulated' || !String(news[2]).startsWith(`${config.news} stories a minute`)) return `news: ${JSON.stringify(news)}`;
    if (filings[1] !== 'simulated' || !String(filings[2]).startsWith('EDGAR asks for a contact')) return `filings: ${JSON.stringify(filings)}`;
    return crypto[1] === 'simulated' && String(crypto[2]).startsWith('the feed was refused') ? null
      : `crypto: ${JSON.stringify(crypto)}`;
  });

  await check(`every name is an instrument: ${equities} equities and 8 coins, in eleven sectors and crypto`, async () => {
    // the listed options are instruments too (the derivatives desk's, K11): counted by the options' own check
    const kinds = await rows(`${INSTRUMENT}->filter(x|$x.assetClass != 'OPTION')->project(~[kind:x|$x.assetClass, id:x|$x.instrumentId,`
      + ` sector:x|$x.sector.name])->groupBy(~[kind], ~[n:x|$x.id:y|$y->count(), s:x|$x.sector:y|$y->distinct()->count()])`
      + `->sort(~kind->ascending())`);
    return JSON.stringify(kinds) === JSON.stringify([['CRYPTO', 8, 1], ['EQUITY', equities, 11]]) ? null
      : `kinds and their sectors: ${JSON.stringify(kinds)}`;
  });

  await check('every equity has its company, its sector and its listing, through the corpus\'s associations', async () => {
    const [counted] = await rows(`${EQUITY}->project(~[t:x|$x.ticker, cap:x|$x.marketCap, sector:x|$x.instrument.sector.name,`
      + ` venue:x|$x.instrument.exchange.name])->extend(~[bad:x|$x.sector->isEmpty() || $x.venue->isEmpty()`
      + ` || ($x.cap <= 0.0)])->groupBy(~[], ~[all:x|$x.t:y|$y->count(), bad:x|if($x.bad, |1, |[]):y|$y->count()])`);
    const [all, bad] = [Number(counted?.[0]), Number(counted?.[1])];
    return all === equities && bad === 0 ? null : `${bad} of ${all} equities miss their sector, listing or capitalisation`;
  });

  await check('every name is quoted, its bid under its ask', async () => {
    const quoted = await number(`${QUOTE}->project(~[id:x|$x.quoteId])->groupBy(~[], ~[n:x|$x.id:y|$y->count()])`);
    const crossed = await number(`${QUOTE}->filter(x|($x.bid <= 0.0) || ($x.ask <= $x.bid))->project(~[id:x|$x.quoteId])`
      + `->groupBy(~[], ~[n:x|$x.id:y|$y->count()])`);
    return quoted === equities + 8 && crossed === 0 ? null : `${quoted} quoted, ${crossed} crossed`;
  });

  await check('a quote carries the day so far: its last inside its low and high, its open too', async () => {
    const [counted] = await rows(`${QUOTE}->filter(x|$x.last->isNotEmpty())->project(~[id:x|$x.quoteId,`
      + ` o:x|$x.openPrice->toOne(), h:x|$x.highPrice->toOne(), l:x|$x.lowPrice->toOne(), c:x|$x.last->toOne(),`
      + ` v:x|$x.volume->toOne()])->extend(~[bad:x|($x.c < $x.l) || ($x.c > $x.h) || ($x.o < $x.l) || ($x.o > $x.h)`
      + ` || ($x.v <= 0.0)])->groupBy(~[], ~[all:x|$x.id:y|$y->count(), bad:x|if($x.bad, |1, |[]):y|$y->count()])`);
    const [all, bad] = [Number(counted?.[0]), Number(counted?.[1])];
    return all > 0 && bad === 0 ? null : `${bad} of ${all} quotes with a day outside its range`;
  });

  await check('every print is a known name\'s, at a positive price and size, an aggressor\'s side but off-exchange',
    async () => {
      // one query, so the prints counted are the prints checked: all of them, and those of no instrument or ill formed
      const [counted] = await rows(`${TRADE}->project(~[seq:x|$x.sequenceNumber, inst:x|$x.instrument.instrumentId,`
        + ` px:x|$x.price, sz:x|$x.quantity, side:x|$x.aggressorSide, off:x|$x.isOffExchange])`
        + `->extend(~[bad:x|$x.inst->isEmpty() || ($x.px <= 0.0) || ($x.sz <= 0.0)`
        + ` || if($x.off, |$x.side->isNotEmpty(), |!$x.side->in(['B', 'S']))])`
        + `->groupBy(~[], ~[all:x|$x.seq:y|$y->count(), bad:x|if($x.bad, |1, |[]):y|$y->count()])`);
      const [all, bad] = [Number(counted?.[0]), Number(counted?.[1])];
      return all > 0 && bad === 0 ? null : `${bad} of ${all} prints are ill formed`;
    });

  await check(`the day from the open (K14): the tape starts at the session's open, ${config.morning / 60} minutes before the world, and each quote's volume is its prints'`, async () => {
    const said = await rows(`${W}->filter(x|$x.item == 'equities')`);
    const open = /the session open since (\S+)$/.exec(String(said[0]?.[2] ?? ''))?.[1];
    if (open === undefined) return `the world says no open: ${JSON.stringify(said)}`;
    const first = await rows(`${TRADE}->filter(x|$x.instrument.assetClass == 'EQUITY')->project(~[t:x|$x.printTime])`
      + '->groupBy(~[], ~[first:x|$x.t:y|$y->min()])');
    const late = (new Date(String(first[0]?.[0])).getTime() - new Date(open).getTime()) / 1000;
    // each equity's prints summed against its quote's volume: a quote lags its name's prints by at most a few seconds'
    const [counted] = await rows(`${TRADE}->filter(x|$x.instrument.assetClass == 'EQUITY')`
      + '->project(~[i:x|$x.instrument.instrumentId, q:x|$x.quantity])->groupBy(~[i], ~[tape:x|$x.q:y|$y->sum()])'
      + `->join(${QUOTE}->project(~[i2:x|$x.instrument.instrumentId, v:x|$x.volume]), meta::pure::functions::relation::JoinKind.INNER, {a, b|$a.i == $b.i2})`
      + '->extend(~[bad:x|abs($x.tape->toOne() - $x.v->toOne()) > (0.02 * $x.v->toOne())])'
      + '->groupBy(~[], ~[all:x|$x.i:y|$y->count(), bad:x|if($x.bad, |1, |[]):y|$y->count()])');
    const [all, bad] = [Number(counted?.[0]), Number(counted?.[1])];
    return late >= 0 && late < 60 && all === equities && bad === 0 ? null
      : `the open ${open}, the first print ${String(first[0]?.[0])} (${late.toFixed(0)} s after it); ${bad} of ${all} quotes' volumes off their prints'`;
  });

  await check(`prints reach the tape at the world's pace: about ${config.trades} a second`, async () => {
    const count = `${TRADE}->project(~[seq:x|$x.sequenceNumber])->groupBy(~[], ~[n:x|$x.seq:y|$y->count()])`;
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
    const candles = `${PRINTS}->extend(~[bar:x|$x.at->timeBucket(1, DurationUnit.MINUTES)])`
      + `->groupBy(~[sym, bar], ~[o:x|${RM}($x.px, $x.seq):y|$y->minBy(), hi:x|$x.px:y|$y->max(),`
      + ` lo:x|$x.px:y|$y->min(), c:x|${RM}($x.px, $x.seq):y|$y->maxBy(), v:x|$x.sz:y|$y->sum()])`;
    const n = await number(`${candles}->groupBy(~[], ~[n:x|$x.sym:y|$y->count()])`);
    const outside = await number(`${candles}->filter(x|($x.o < $x.lo) || ($x.o > $x.hi) || ($x.c < $x.lo)`
      + ` || ($x.c > $x.hi) || ($x.v <= 0.0))->groupBy(~[], ~[n:x|$x.sym:y|$y->count()])`);
    return n > 0 && outside === 0 ? null : `${n} candles, ${outside} with an open or close outside them`;
  });

  await check('a name\'s day so far: its VWAP between its low and its high', async () => {
    const day = `${PRINTS}->groupBy(~[sym], ~[pv:x|$x.px * $x.sz:y|$y->sum(), v:x|$x.sz:y|$y->sum(),`
      + ` hi:x|$x.px:y|$y->max(), lo:x|$x.px:y|$y->min()])->extend(~[vwap:x|$x.pv / $x.v])`;
    const names = await number(`${day}->groupBy(~[], ~[n:x|$x.sym:y|$y->count()])`);
    // a group's least and greatest are Pure's [0..1]: their arithmetic takes their one
    const off = await number(`${day}->filter(x|($x.vwap < ($x.lo->toOne() - 0.000001))`
      + ` || ($x.vwap > ($x.hi->toOne() + 0.000001)))`
      + `->groupBy(~[], ~[n:x|$x.sym:y|$y->count()])`);
    return names > 0 && off === 0 ? null : `${names} names traded, ${off} with a VWAP outside their range`;
  });

  await check('windows over the tape (K6): a running VWAP and a running EMA inside their running range, a previous print'
    + ' for all but the first of each name', async () => {
    // one query, so the prints windowed are the prints counted; a running low and high are Pure's [0..1], and so is an
    // EMA (lite's ema: a weighted mean of the prices so far, never outside their range)
    const [counted] = await rows(`${PRINTS}->extend(over(~sym, ~seq->ascending(), rows(unbounded(), 0)),`
      + ` ~[pv:{p,w,r|$r.px * $r.sz}:y|$y->plus(), v:{p,w,r|$r.sz}:y|$y->plus(), hi:{p,w,r|$r.px}:y|$y->max(),`
      + ` lo:{p,w,r|$r.px}:y|$y->min(), e:{p,w,r|$r.px}:y|$y->ema(0.2)])`
      + `->extend(over(~sym, ~seq->ascending()), ~prev:{p,w,r|$p->lag($r).px})`
      + `->extend(~[off:x|(($x.pv / $x.v) < ($x.lo->toOne() - 0.000001)) || (($x.pv / $x.v) > ($x.hi->toOne() + 0.000001)),`
      + ` emaOff:x|($x.e->toOne() < ($x.lo->toOne() - 0.000001)) || ($x.e->toOne() > ($x.hi->toOne() + 0.000001))])`
      + `->groupBy(~[], ~[all:x|$x.seq:y|$y->count(), off:x|if($x.off, |1, |[]):y|$y->count(),`
      + ` emaOff:x|if($x.emaOff, |1, |[]):y|$y->count(),`
      + ` firsts:x|if($x.prev->isEmpty(), |1, |[]):y|$y->count(), names:x|$x.sym:y|$y->distinct()->count()])`);
    const [all = 0, off = 0, emaOff = 0, firsts = 0, names = 0] = (counted ?? []).map(Number);
    return all > 0 && off === 0 && emaOff === 0 && firsts === names ? null
      : `${all} prints: ${off} with a VWAP and ${emaOff} with an EMA outside their range, ${firsts} with no previous`
        + ` print for ${names} names`;
  });

  await check('crypto ticks, simulated, with its feed refused: every coin traded, each print the simulator\'s', async () => {
    const coins = await rows(`${TRADE}->filter(x|$x.instrument.assetClass == 'CRYPTO')->project(~[venue:x|$x.venue,`
      + ` inst:x|$x.instrument.instrumentId])->groupBy(~[venue], ~[n:x|$x.inst:y|$y->distinct()->count()])`);
    return JSON.stringify(coins) === JSON.stringify([['SIM', 8]]) ? null : `the coins' prints by venue: ${JSON.stringify(coins)}`;
  });

  await check('a quote ticks: the largest name\'s moves several times in two seconds', async () => {
    const largest = String((await rows(`${EQUITY}->project(~[t:x|$x.ticker, cap:x|$x.marketCap])`
      + `->sort(~cap->descending())->limit(1)->select(~[t])`))[0]?.[0]);
    const seen = new Set<string>();
    await rows(`${QUOTE}->filter(x|$x.instrument.ticker == '${largest}')->project(~[bid:x|$x.bid, ask:x|$x.ask,`
      + ` bsz:x|$x.bidSize, asz:x|$x.askSize])`, 2000, (now) => seen.add(JSON.stringify(now)));
    return seen.size >= 3 ? null : `${largest}'s quote seen ${seen.size} ways in two seconds`;
  });

  // THE BANK'S OWN DAY (K9-K13): its desks, books, positions and limits; its orders worked into fills; its FX prices
  // and the clients' requests; the listed options -- each read through the corpus's classes
  await check('the bank: six desks, ten books, eighteen traders, each book its desk\'s', async () => {
    const desks = await number('org::Desk.all()->project(~[d:x|$x.deskId])->groupBy(~[], ~[n:x|$x.d:y|$y->count()])');
    const traders = await number('org::Trader.all()->project(~[t:x|$x.traderId, d:x|$x.desk.deskId])'
      + '->filter(x|$x.d->isNotEmpty())->groupBy(~[], ~[n:x|$x.t:y|$y->count()])');
    const books = await number('positions::Book.all()->project(~[b:x|$x.bookId, d:x|$x.desk.name])'
      + '->filter(x|$x.d->isNotEmpty())->groupBy(~[], ~[n:x|$x.b:y|$y->count()])');
    return desks === 6 && traders === 18 && books === 10 ? null : `${desks} desks, ${traders} traders, ${books} books`;
  });

  await check('every position is its book\'s, on an instrument the world lists, its value its quantity at its price', async () => {
    const [counted] = await rows('positions::Position.all()->project(~[p:x|$x.positionId, b:x|$x.book.bookId,'
      + ' i:x|$x.instrument.instrumentId, q:x|$x.quantity, mv:x|$x.marketValue])'
      + '->extend(~[bad:x|$x.b->isEmpty() || $x.i->isEmpty() || ($x.q == 0.0) || (($x.q > 0.0) != ($x.mv > 0.0))])'
      + '->groupBy(~[], ~[all:x|$x.p:y|$y->count(), bad:x|if($x.bad, |1, |[]):y|$y->count()])');
    const [all, bad] = [Number(counted?.[0]), Number(counted?.[1])];
    return all > 100 && bad === 0 ? null : `${bad} of ${all} positions ill formed`;
  });

  await check('each desk that holds positions has a day\'s loss limit and a gross limit', async () => {
    const limits = await rows('risk::RiskLimit.all()->project(~[d:x|$x.desk.deskId, t:x|$x.limitType, a:x|$x.limitAmount])'
      + '->groupBy(~[t], ~[n:x|$x.d:y|$y->distinct()->count(), least:x|$x.a:y|$y->min()])->sort(~t->ascending())');
    return JSON.stringify(limits.map((r) => [r[0], r[1]])) === JSON.stringify([['DAY_LOSS', 5], ['GROSS_NOTIONAL', 5]])
      && limits.every((r) => Number(r[2]) > 0) ? null : `the limits: ${JSON.stringify(limits)}`;
  });

  await check('orders are worked into fills: each fill its order\'s, an order\'s fills its filled quantity', async () => {
    await sleep(3000);
    const [counted] = await rows('trading::Order.all()->project(~[o:x|$x.orderId, q:x|$x.quantity, f:x|$x.filledQuantity,'
      + ' s:x|$x.status, sym:x|$x.instrument.ticker, b:x|$x.book.bookId, t:x|$x.trader.traderId])'
      + '->join(trading::Execution.all()->project(~[eo:x|$x.order.orderId, eq:x|$x.filledQuantity])'
      + '->groupBy(~[eo], ~[done:x|$x.eq:y|$y->sum()]), meta::pure::functions::relation::JoinKind.LEFT, {a, b|$a.o == $b.eo})'
      + '->extend(~[bad:x|$x.sym->isEmpty() || $x.b->isEmpty() || $x.t->isEmpty()'
      + ' || (abs($x.f->toOne() - if($x.done->isEmpty(), |0.0, |$x.done->toOne())) > 0.000001)'
      + ' || ($x.f->toOne() > ($x.q + 0.000001))])'
      + '->groupBy(~[], ~[all:x|$x.o:y|$y->count(), bad:x|if($x.bad, |1, |[]):y|$y->count(),'
      + ' filled:x|if($x.s == \'FILLED\', |1, |[]):y|$y->count()])');
    const [all, bad, filled] = [Number(counted?.[0]), Number(counted?.[1]), Number(counted?.[2])];
    return all > 0 && bad === 0 ? null : `${bad} of ${all} orders disagree with their fills (${filled} filled)`;
  });

  await check('FX: ten pairs priced, each bid under its ask', async () => {
    const [counted] = await rows('marketdata::FxRate.all()->project(~[p:x|$x.currencyPair, b:x|$x.bidRate, a:x|$x.askRate])'
      + '->groupBy(~[], ~[n:x|$x.p:y|$y->distinct()->count(), crossed:x|if($x.b->toOne() >= $x.a->toOne(), |1, |[]):y|$y->count()])');
    return Number(counted?.[0]) === 10 && Number(counted?.[1]) === 0 ? null : `the rates: ${JSON.stringify(counted)}`;
  });

  await check('FX: each cross within 3 pips of the rate its legs imply', async () => {
    // a cross priced on its own drifted 17 pips off its legs in a minute (the trader screens, 2026-10-25): a free lunch
    const mid = new Map((await rows('marketdata::FxRate.all()->project(~[p:x|$x.currencyPair, m:x|$x.midRate])'))
      .map((r) => [String(r[0]), Number(r[1])]));
    const at = (pair: string): number => mid.get(pair) ?? Number.NaN;
    const off = ([['EUR/GBP', at('EUR/USD') / at('GBP/USD'), 0.0001], ['EUR/JPY', at('EUR/USD') * at('USD/JPY'), 0.01],
      ['GBP/JPY', at('GBP/USD') * at('USD/JPY'), 0.01]] as const).map(([pair, implied, pip]) => [pair, (at(pair) - implied) / pip] as const);
    return off.every(([, pips]) => Math.abs(pips) <= 3) ? null : `quoted less implied, in pips: ${off.map(([p, x]) => `${p} ${x.toFixed(1)}`).join(', ')}`;
  });

  await check('RFQs: quoted unless declined, a hit only when done, some done', async () => {
    const [counted] = await rows('trading::Rfq.all()->project(~[r:x|$x.rfqId, s:x|$x.status, h:x|$x.isHit, q:x|$x.quotedRate,'
      + ' c:x|$x.client.shortName, d:x|$x.dealer.traderId])'
      + '->extend(~[bad:x|($x.h && ($x.s != \'DONE\')) || (($x.s == \'DECLINED\') && $x.q->isNotEmpty())'
      + ' || ($x.s->in([\'QUOTED\', \'DONE\', \'MISSED\', \'EXPIRED\']) && $x.q->isEmpty()) || $x.c->isEmpty() || $x.d->isEmpty()])'
      + '->groupBy(~[], ~[all:x|$x.r:y|$y->count(), bad:x|if($x.bad, |1, |[]):y|$y->count(),'
      + ' done:x|if($x.s == \'DONE\', |1, |[]):y|$y->count()])');
    const [all, bad, done] = [Number(counted?.[0]), Number(counted?.[1]), Number(counted?.[2])];
    return all > 0 && bad === 0 && done > 0 ? null : `${all} requests: ${bad} ill formed, ${done} done`;
  });

  await check('the option chain: four underlyings, each option its own instrument, calls and puts at each strike', async () => {
    const chain = await rows('products::Option.all()->project(~[u:x|$x.underlying.ticker, k:x|$x.instrument.assetClass,'
      + ' t:x|$x.optionType, id:x|$x.optionId])->groupBy(~[u], ~[n:x|$x.id:y|$y->count(),'
      + ' calls:x|if($x.t == \'CALL\', |1, |[]):y|$y->count(), kinds:x|$x.k:y|$y->distinct()->count()])');
    return chain.length === 4 && chain.every((r) => Number(r[1]) > 0 && Number(r[2]) * 2 === Number(r[1]) && Number(r[3]) === 1)
      ? null : `the chain by underlying: ${JSON.stringify(chain)}`;
  });

  await check('the chain priced live by the model\'s Black and Scholes (K11): every price and greek the textbook\'s, computed'
    + ' here on the same inputs', async () => {
    const started = performance.now();
    // the desk's own chain (market.pure), as the options screen reads it: what the server computed for each option
    const chain = await rows('market::screens::optionChain(0.05)->select(~[call, spot, strike, t, vol, price, delta, gamma,'
      + ' vega, theta])');
    // the textbook's, independently: the normal distribution integrated (Simpson's rule, error far below the model's
    // Abramowitz and Stegun's 7.5e-8), a put by its own formula, not by parity
    const pdf = (x: number): number => Math.exp(-x * x / 2) / Math.sqrt(2 * Math.PI);
    const cdf = (x: number): number => {
      const steps = 2000;
      const h = Math.abs(x) / steps;
      let sum = pdf(0) + pdf(Math.abs(x));
      for (let i = 1; i < steps; i++) sum += (i % 2 === 1 ? 4 : 2) * pdf(i * h);
      const half = (sum * h) / 3;
      return x >= 0 ? 0.5 + half : 0.5 - half;
    };
    const r = 0.05;
    const wrong: string[] = [];
    for (const row of chain) {
      const call = row[0] === true || row[0] === 'true';
      const [S, K, T, V, price, delta, gamma, vega, theta] = row.slice(1).map(Number) as
        [number, number, number, number, number, number, number, number, number];
      const d1 = (Math.log(S / K) + (r + V * V / 2) * T) / (V * Math.sqrt(T));
      const d2 = d1 - V * Math.sqrt(T);
      const disc = K * Math.exp(-r * T);
      const want = call
        ? { price: S * cdf(d1) - disc * cdf(d2), delta: cdf(d1), theta: (-(S * pdf(d1) * V) / (2 * Math.sqrt(T)) - r * disc * cdf(d2)) / 365 }
        : { price: disc * cdf(-d2) - S * cdf(-d1), delta: cdf(d1) - 1, theta: (-(S * pdf(d1) * V) / (2 * Math.sqrt(T)) + r * disc * cdf(-d2)) / 365 };
      const greeks = { gamma: pdf(d1) / (S * V * Math.sqrt(T)), vega: (S * pdf(d1) * Math.sqrt(T)) / 100 };
      // to a millionth of the spot for a price, a millionth for a delta: the model's distribution's error, scaled
      const off = (got: unknown, expected: number, scale: number): boolean => !(Math.abs((got as number) - expected) <= 1e-6 * scale);
      if (off(price, want.price, S) || off(delta, want.delta, 1) || off(gamma, greeks.gamma, Math.abs(greeks.gamma) + 1e-9)
        || off(vega, greeks.vega, Math.abs(greeks.vega) + 1e-9) || off(theta, want.theta, Math.abs(want.theta) + S * 1e-4)) {
        wrong.push(`${call ? 'call' : 'put'} ${K} at ${S}, ${T.toFixed(3)} y, vol ${V}: price ${String(price)} (${want.price}),`
          + ` delta ${String(delta)} (${want.delta}), theta ${String(theta)} (${want.theta})`);
      }
    }
    return chain.length > 0 && wrong.length === 0 ? null
      : `${chain.length} options, ${wrong.length} not the textbook's: ${wrong.slice(0, 3).join('; ')} (${(performance.now() - started).toFixed(0)} ms)`;
  });

  const NEWS = 'marketdata::NewsStory.all()';
  // a share offering's is Capital Markets (2026-10-25, the trading desk: a brief moves its name as often down as up)
  const CATEGORIES = new Set(['Earnings', 'Guidance', 'M&A', 'Analyst', 'Macro', 'Industry', 'Regulatory', 'Management',
    'Product', 'Filing', 'Dividend', 'Crypto', 'Capital Markets']);
  const stories = await rows(`${NEWS}->project(~[id:x|$x.storyId, at:x|$x.publishTime, head:x|$x.headline, body:x|$x.body,`
    + ' cat:x|$x.category, urg:x|$x.urgency, tickers:x|$x.tickers, sym:x|$x.instrument.ticker, sector:x|$x.sector.name])');
  await check(`the newswire (K15): the morning's news at the world's pace, every ticker a name's, a story about a name`
    + ' naming it, a sector\'s naming its leaders, a macro story neither', async () => {
    const syms = new Set((await rows(`${INSTRUMENT}->project(~[t:x|$x.ticker])`)).map((r) => String(r[0])));
    const wrong: string[] = [];
    for (const [id, , head, , cat, urg, tickers, sym, sector] of stories) {
      const named = tickers === null ? [] : String(tickers).split(' ');
      if (!head || !CATEGORIES.has(String(cat)) || !['FLASH', 'STORY', 'BRIEF'].includes(String(urg))) wrong.push(`${id}: ill formed`);
      if (named.some((t) => !syms.has(t))) wrong.push(`${id}: names ${String(tickers)}, not all the world's`);
      if (sym !== null && !named.includes(String(sym))) wrong.push(`${id}: about ${String(sym)}, naming ${String(tickers)}`);
      if (cat === 'Industry' && (sector === null || sym !== null || named.length === 0)) wrong.push(`${id}: a sector's story, ${String(sector)}`);
      if (cat === 'Macro' && (sector !== null || sym !== null || tickers !== null)) wrong.push(`${id}: a macro story about ${String(sym ?? sector)}`);
    }
    // the morning's alone, at the macro, sector and brief stories' share of the pace (crypto's waits for the world)
    const expected = (config.morning / 60) * config.news * (11.5 / 12);
    if (stories.length < expected * 0.7) wrong.push(`${stories.length} stories, not about ${expected.toFixed(0)}`);
    return wrong.length === 0 ? null : wrong.slice(0, 8).join('; ');
  });

  await check('a name\'s jump is its story, at its instant: the price before the story and after it moved its way, by'
    + ' about the headline\'s size', async () => {
    const jumps = stories
      .map(([id, at, , body, , urg, , sym]) => ({ id, at: new Date(String(at)).getTime(), body: String(body), urg, sym,
        said: /shares (rose|fell) ([\d.]+)%/.exec(String(body)) }))
      .filter((j) => j.urg !== 'BRIEF' && j.sym !== null && j.said !== null && Number(j.said[2]) >= 2)
      .slice(0, 8);
    if (jumps.length < 3) return `${jumps.length} jumps of 2% or more to check`;
    const wrong: string[] = [];
    for (const j of jumps) {
      const prints = (await rows(`${TRADE}->filter(x|$x.instrument.ticker == '${String(j.sym)}')`
        + '->project(~[seq:x|$x.sequenceNumber, at:x|$x.printTime, px:x|$x.price])'))
        .map(([seq, at, px]) => ({ seq: Number(seq), at: new Date(String(at)).getTime(), px: Number(px) }))
        .sort((a, b) => a.seq - b.seq);
      // the last print two seconds before the story, and the first at or after it
      const before = prints.filter((p) => p.at < j.at - 2000).at(-1);
      const after = prints.find((p) => p.at >= j.at);
      if (before === undefined || after === undefined) continue;
      const moved = after.px / before.px - 1;
      const size = Number(j.said![2]) / 100 * (j.said![1] === 'rose' ? 1 : -1);
      if (Math.sign(moved) !== Math.sign(size) || Math.abs(moved) < Math.abs(size) / 2) {
        wrong.push(`${String(j.id)} (${String(j.sym)}): "${j.body}", the price ${before.px} then ${after.px}`);
      }
    }
    return wrong.length === 0 ? null : wrong.join('; ');
  });

  live.close();
  await session.close();
} catch (e) {
  verdict.setupError = String((e as Error)?.stack ?? e);
}
verdict.done = true;
