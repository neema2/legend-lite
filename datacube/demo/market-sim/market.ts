// THE MARKET WORLD IN THE TAB (docs/DUCKDB_LIVE_2026_10_25.md L2): market.groovy's world -- an equity market simulated
// always, crypto from Coinbase's public feed where the tab reaches it and simulated where it does not, the newswire, the
// bank's own day -- run by the page itself, no server, its rows the ONE TRADING MODEL's tables (tables.ts) written into
// whatever the page gives it (a MarketSink: the tab's live DuckDB, engine-client/src/duckdb-live.ts). The same world as
// the server's: the same draws from the same seed, its names, prices and stories (random.ts); its clock the page's.
//
// Its start: the coins' prior closes the feed's (asked for a few seconds), the reference written, then THE MORNING (the
// session opened `morning` seconds ago, each equity's morning on the tape and the wire's, so a chart has a day to draw),
// then the clock: a beat every `beatMs` (the market's step, the feed's messages, the bank's day every 100 ms), its rows
// written every `flushMs` -- one write a table, so the database sees a few batches a second, never a row at a time.

import type { MadeColumn } from '../../../engine-client/src/arrow-ipc.ts';
import { dayOf } from './format.ts';
import { dayRows, makeBank, type Bank, type DayRows } from './bank.ts';
import { CoinbaseFeed } from './coinbase.ts';
import { coins as makeCoins, equities as makeEquities, GICS, SECTORS, type Name } from './names.ts';
import { Newsroom } from './newsroom.ts';
import { JavaRandom } from './random.ts';
import { equityRows, exchanges, instruments, sectors } from './reference.ts';
import { marketRows, quoteRow, Simulator, tradeRow, type MarketRows } from './simulator.ts';
import { TableRows, WORLD, type MarketTable } from './tables.ts';

/** The world's size and pace (the server's MARKET_* settings, the tab's own defaults: a tab's database is one thread). */
export interface MarketSettings {
  /** equities (100) */
  readonly names?: number;
  /** quote updates a second over all equities (2000) */
  readonly quotes?: number;
  /** trades a second over all equities (400) */
  readonly trades?: number;
  /** 'coinbase': Coinbase Exchange's public feed for crypto, simulated if refused (the default); 'off' */
  readonly feed?: 'coinbase' | 'off';
  /** the feed's address (wss://ws-feed.exchange.coinbase.com) */
  readonly feedUrl?: string;
  /** how long the coins' prior closes are waited for from the feed, ms (3000) */
  readonly feedWaitMs?: number;
  /** seconds of the session before the world starts (1800): the open that long ago, its morning on the tape */
  readonly morning?: number;
  /** the most prints the morning writes (100000): a thinner tape of larger prints past it */
  readonly morningPrints?: number;
  /** stories a minute besides each name's jump (12) */
  readonly news?: number;
  /** a name's jumps a trading day (0.5) */
  readonly jumps?: number;
  /** parent orders a second over the equity and crypto books (0.8) */
  readonly orders?: number;
  /** clients' requests for a quote a second (2) */
  readonly rfqs?: number;
  /** the simulation's random seed (20261024, the server's) */
  readonly seed?: number;
  /** a beat of the clock, ms (50) */
  readonly beatMs?: number;
  /** how often the beats' rows are written, ms (250) */
  readonly flushMs?: number;
  /** the time now, ms since the epoch (Date.now): a test's own */
  readonly clock?: () => number;
  /** false: no timers, the caller beats and flushes -- a test */
  readonly timer?: boolean;
  /** false: the world beats on its timer, but its caller writes its rows (`flush`) -- a live engine's cycle, which
   *  writes and then refreshes (live-market.ts) */
  readonly flushes?: boolean;
}

/** Where the world's rows go: a table's rows (one write: a stream's appended, a keyed table's upserted, the reference
 *  put in once), the number of them, each column as made. */
export type MarketSink = (table: MarketTable, count: number, rows: Record<string, MadeColumn>) => Promise<unknown>;

/** What the world has done, for a page that shows it and a test that checks it. */
export interface MarketCounts {
  beats: number;
  flushes: number;
  /** rows written, by table */
  readonly rows: Record<string, number>;
  /** the last flush's time, ms */
  lastFlushMs: number;
  /** why the last flush failed, if it did; null since one that did not */
  lastFailure: string | null;
  /** the morning: its prints and how long it took, ms */
  morningPrints: number;
  morningMs: number;
}

const MORNING_CHUNK = 25_000;

export class MarketWorld {
  readonly equities: readonly Name[];
  readonly coins: readonly Name[];
  readonly simulator: Simulator;
  readonly newsroom: Newsroom;
  readonly bank: Bank;
  readonly feed: CoinbaseFeed | null;
  readonly sessionOpen: number;
  readonly counts: MarketCounts = { beats: 0, flushes: 0, rows: {}, lastFlushMs: 0, lastFailure: null, morningPrints: 0, morningMs: 0 };
  readonly #sink: MarketSink;
  readonly #clock: () => number;
  readonly #bySym: ReadonlyMap<string, Name>;
  readonly #market: MarketRows = marketRows();
  readonly #day: DayRows = dayRows();
  readonly #world = new TableRows(WORLD);
  readonly #equitiesDetail: string;
  readonly #newsDetail: string;
  #lastStep: number;
  #lastBankBeat: number;
  #retryAt = 0;
  #said = '';
  #flushing: Promise<void> = Promise.resolve();
  #timers: ReturnType<typeof setInterval>[] = [];

  private constructor(fields: {
    equities: Name[]; coins: Name[]; simulator: Simulator; newsroom: Newsroom; bank: Bank; feed: CoinbaseFeed | null;
    sessionOpen: number; sink: MarketSink; clock: () => number; now: number; equitiesDetail: string; newsDetail: string;
  }) {
    this.equities = fields.equities;
    this.coins = fields.coins;
    this.simulator = fields.simulator;
    this.newsroom = fields.newsroom;
    this.bank = fields.bank;
    this.feed = fields.feed;
    this.sessionOpen = fields.sessionOpen;
    this.#sink = fields.sink;
    this.#clock = fields.clock;
    this.#lastStep = fields.now;
    this.#lastBankBeat = fields.now;
    this.#equitiesDetail = fields.equitiesDetail;
    this.#newsDetail = fields.newsDetail;
    this.#bySym = new Map([...fields.equities, ...fields.coins].map((n) => [n.sym, n]));
  }

  /** The world made and started: its reference and its morning written into `sink`, its clock running. */
  static async open(settings: MarketSettings, sink: MarketSink): Promise<MarketWorld> {
    const clock = settings.clock ?? Date.now;
    const seed = settings.seed ?? 20261024;
    const names = settings.names ?? 100;
    const quotes = settings.quotes ?? 2000;
    const trades = settings.trades ?? 400;
    const morning = settings.morning ?? 1800;
    const news = settings.news ?? 12;
    const random = new JavaRandom(seed);
    const equities = makeEquities(random, names);
    const coins = makeCoins(random);

    // the feed tried first: its 24 hours' open is each coin's prior close, when it names every coin in time
    let feed: CoinbaseFeed | null = null;
    if ((settings.feed ?? 'coinbase') === 'coinbase') {
      feed = new CoinbaseFeed(settings.feedUrl ?? 'wss://ws-feed.exchange.coinbase.com', coins.map((c) => c.sym));
      feed.connect();
      const opens = new Map<string, number>();
      const until = Date.now() + (settings.feedWaitMs ?? 3000);
      while (Date.now() < until && opens.size < coins.length && feed.state !== 'refused') {
        for (const m of feed.messages) {
          if (m.type === 'ticker' && m.open_24h !== undefined) opens.set(m.product_id, Number.parseFloat(m.open_24h));
        }
        await new Promise((r) => setTimeout(r, 100));
      }
      if (opens.size === coins.length) {
        for (const c of coins) c.mid = c.prevClose = opens.get(c.sym)!;
      } else if (feed.state !== 'refused') {
        feed.refused(`no ticker for every coin in ${(settings.feedWaitMs ?? 3000) / 1000} s`);
      }
    }

    const now = clock();
    const today = dayOf(now);
    const bank = makeBank(new JavaRandom(seed + 2), equities, coins, today, now,
      { orders: settings.orders ?? 0.8, rfqs: settings.rfqs ?? 2 });
    const simulator = new Simulator(random, equities, SECTORS.length, quotes, trades);
    // the newswire on its own random, so the prices' draws are the simulation's as they were
    const newsroom = new Newsroom(new JavaRandom(seed + 2), equities, coins, GICS.map((g) => `SEC-${g}`), news);
    simulator.news = newsroom;
    simulator.jumpsADay = settings.jumps ?? 0.5;
    const sessionOpen = now - morning * 1000;
    const world = new MarketWorld({
      equities, coins, simulator, newsroom, bank, feed, sessionOpen, sink, clock, now,
      equitiesDetail: `${names} names, ${quotes.toLocaleString('en-US')} quotes and ${trades.toLocaleString('en-US')} trades a second, `
        + `the session open since ${new Date(sessionOpen).toISOString().replace(/\.\d+Z$/, 'Z')}`,
      newsDetail: `${news} stories a minute besides each name's jump, each where its move is`,
    });

    // the reference, written once
    for (const rows of [exchanges(), sectors(equities, coins, today), instruments([...equities, ...coins], bank.options),
      equityRows(equities, new JavaRandom(seed + 1), today), ...bank.tables]) {
      await world.#write(rows);
    }
    // THE MORNING: the session opened `morning` seconds ago, each equity's morning on the tape; crypto's day is the
    // feed's (or the simulator's from now), never made up before the world began
    if (morning >= 1) {
      const started = performance.now();
      const thin = Math.min(1, (settings.morningPrints ?? 100_000) / (trades * simulator.morningPace(morning)));
      const rows = marketRows();
      simulator.morning(morning, sessionOpen, thin, rows);
      world.counts.morningPrints = rows.trades.count;
      await world.#write(rows.trades, MORNING_CHUNK);
      await world.#write(newsroom.rows);
      world.counts.morningMs = performance.now() - started;
      // each name's quote written at the clock's first beat, its day so far the morning's
      for (const n of equities) n.quoted = false;
    }
    // a coin's activity on its own scale: the simulator's rates are the equities'; coins quote and trade a tenth as often
    for (const c of coins) c.weight /= 10;
    bank.day.opening(now, world.#day);
    world.#say(false);
    await world.flush();
    if (settings.timer !== false) {
      world.#timers.push(setInterval(() => world.beat(), settings.beatMs ?? 50));
      if (settings.flushes !== false) world.#timers.push(setInterval(() => { void world.flush(); }, settings.flushMs ?? 250));
    }
    return world;
  }

  /** Whether crypto is live: the feed has spoken in the last 15 seconds. */
  cryptoLive(): boolean {
    return this.feed !== null && this.feed.state === 'live' && Date.now() - this.feed.lastMessageMs < 15_000;
  }

  /** One beat of the clock at `now`: the simulation's step, the feed's messages, the bank's day, the world said. */
  beat(now = this.#clock()): void {
    const dt = Math.min(1, Math.max(0, (now - this.#lastStep) / 1000));
    this.#lastStep = now;
    this.counts.beats++;
    const live = this.cryptoLive();
    this.simulator.step(dt, now, live ? this.equities : [...this.equities, ...this.coins], this.#market);
    if (this.feed !== null) {
      this.#drain(now, live);
      if (!live && this.feed.state !== 'connecting' && now >= this.#retryAt) {
        // asked again a minute after it was refused, or went quiet
        this.#retryAt = now + 60_000;
        this.feed.connect();
      }
    }
    if (now - this.#lastBankBeat >= 100) {
      this.#lastBankBeat = now;
      this.bank.day.beat(now, this.#day);
    }
    this.#say(live);
  }

  /** Every table's rows since the last flush written, one write a table; one flush at a time. */
  flush(): Promise<void> {
    const next = this.#flushing.then(async () => {
      const started = performance.now();
      try {
        for (const rows of [this.#market.quotes, this.#market.trades, this.newsroom.rows, this.#day.orders, this.#day.executions,
          this.#day.routes, this.#day.rates, this.#day.rfqs, this.#world]) {
          await this.#write(rows);
        }
        this.counts.flushes++;
        this.counts.lastFailure = null;
      } catch (e) {
        this.counts.lastFailure = e instanceof Error ? e.message : String(e);
        throw e;
      } finally {
        this.counts.lastFlushMs = performance.now() - started;
      }
    });
    this.#flushing = next.catch(() => { /* said in counts.lastFailure */ });
    return next;
  }

  close(): void {
    for (const t of this.#timers) clearInterval(t);
    this.#timers = [];
    this.feed?.close();
  }

  /** `rows` written, in writes of at most `chunk` rows (the morning's tape: a few appends, not one huge). */
  async #write(rows: TableRows, chunk = Number.POSITIVE_INFINITY): Promise<void> {
    const taken = rows.take();
    if (taken === null) return;
    for (let from = 0; from < taken.count; from += chunk) {
      const n = Math.min(chunk, taken.count - from);
      await this.#sink(rows.table, n, from === 0 ? taken.rows : offset(taken.rows, from));
      this.counts.rows[rows.table.name] = (this.counts.rows[rows.table.name] ?? 0) + n;
    }
  }

  /** The feed's messages as rows; dropped while crypto is simulated, so one source makes its rows at a time. */
  #drain(now: number, live: boolean): void {
    const messages = this.feed!.messages.splice(0);
    if (!live) return;
    for (const m of messages) {
      const n = this.#bySym.get(m.product_id);
      if (n === undefined) continue;
      const at = m.time !== undefined ? Math.floor(Date.parse(m.time)) : now;
      if (m.type === 'ticker') {
        if (m.best_bid === undefined || m.best_ask === undefined) continue;
        n.bid = Number.parseFloat(m.best_bid);
        n.ask = Number.parseFloat(m.best_ask);
        n.bidSize = Number.parseFloat(m.best_bid_size ?? '0');
        n.askSize = Number.parseFloat(m.best_ask_size ?? '0');
        n.mid = (n.bid + n.ask) / 2;
        n.quoted = true;
        quoteRow(this.#market, n, at, 'CBX');
      } else if (m.price !== undefined && m.size !== undefined) {
        const px = Number.parseFloat(m.price);
        // the side the feed names is the maker's: the aggressor took the other
        tradeRow(this.#market, ++this.simulator.seq, at, n, px, Number.parseFloat(m.size), m.side === 'sell' ? 'B' : 'S', 'CBX', false);
        n.mid = px;
      }
    }
  }

  /** What each part of the world is (the world table: market/model.ts market::World), written when it changes. */
  #say(live: boolean): void {
    const feed = this.feed;
    const crypto = feed === null ? 'simulated|the feed is off'
      : live ? `live|${feed.detail}`
        : feed.state === 'connecting' ? `simulated|connecting to ${feed.detail}`
          : `simulated|the feed was refused: ${feed.detail}`;
    const filed = 'simulated|EDGAR answers no page (no CORS; the User-Agent it asks for a page cannot set): the newsroom files its names\' own';
    const text = `${crypto}\n${filed}`;
    if (text === this.#said) return;
    this.#said = text;
    const [cState, cDetail] = split(crypto);
    const [fState, fDetail] = split(filed);
    this.#world.add({ item: 'equities', state: 'simulated', detail: this.#equitiesDetail });
    this.#world.add({ item: 'crypto', state: cState, detail: cDetail });
    this.#world.add({ item: 'news', state: 'simulated', detail: this.#newsDetail });
    this.#world.add({ item: 'filings', state: fState, detail: fDetail });
  }
}

function split(said: string): [string, string] {
  const at = said.indexOf('|');
  return [said.substring(0, at), said.substring(at + 1)];
}

/** Columns read from row `from` on: each column's row i its row from + i. */
function offset(rows: Record<string, MadeColumn>, from: number): Record<string, MadeColumn> {
  const out: Record<string, MadeColumn> = {};
  for (const [name, c] of Object.entries(rows)) {
    out[name] = 'text' in c ? { text: c.text, pick: (i) => c.pick(from + i) }
      : 'doubles' in c ? { doubles: (i) => c.doubles(from + i) }
        : 'integers' in c ? { integers: (i) => c.integers(from + i) }
          : 'instants' in c ? { instants: (i) => c.instants(from + i) }
            : 'days' in c ? { days: (i) => c.days(from + i) } : { truths: (i) => c.truths(from + i) };
  }
  return out;
}
