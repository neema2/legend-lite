// THE SIMULATION (market.groovy's Simulator): a market factor, one per sector, each name's own, and jumps; quotes and
// prints at each name's pace, into the tables' rows (MARKET_QUOTE's latest a name, MARKET_TRADE's tape). Realism,
// simulated: prices move together; a cent tick; spreads by liquidity, a name's activity by its size (the largest names
// quote and trade most); quotes that move when the price leaves them; trades at the bid or the ask (the aggressor's side
// leaning with the move), some at the midpoint off-exchange; round lots mostly, odd lots some; each print moves the
// price a little. The same draws as the server's, in the same order.

import type { Name } from './names.ts';
import type { JavaRandom } from './random.ts';
import { MARKET_QUOTE, MARKET_TRADE, TableRows } from './tables.ts';

/** seconds in a trading day: a daily volatility's horizon */
export const DAY = 23_400;
/** the lit venues by MIC, and off-exchange (a TRF's print: the FINRA facility) */
const VENUES = ['XNAS', 'XNYS', 'ARCX', 'BATS', 'EDGX', 'IEXG', 'FINRA'];
const VENUE_SHARE = [0.30, 0.20, 0.10, 0.15, 0.08, 0.04, 0.13];

/** What the newswire does for the simulator (newsroom.ts): its stories where the prices move, its shocks to them. */
export interface Wire {
  /** One step's macro and sector stories at `at`: the market's shock (0 when none), a sector's added to `factors`. */
  beat(dt: number, at: number, factors: number[], morning: boolean): number;
  /** A name's jump, reported at its instant. */
  jump(n: Name, r: number, at: number): void;
}

/** The rows a step makes: the quotes (each a name's latest at its time) and the prints. */
export interface MarketRows {
  readonly quotes: TableRows;
  readonly trades: TableRows;
}

export function marketRows(): MarketRows {
  return { quotes: new TableRows(MARKET_QUOTE), trades: new TableRows(MARKET_TRADE) };
}

const orNull = (v: number): number | null => (Number.isNaN(v) ? null : v);

/** `n`'s quote now: its top of book, and the day so far as it stands. */
export function quoteRow(rows: MarketRows, n: Name, at: number, source: string): void {
  rows.quotes.add({
    QUOTE_ID: `MQ-${n.sym}`, QUOTE_TIME: at, BID: n.bid, ASK: n.ask, MID: (n.bid + n.ask) / 2, LAST: orNull(n.last),
    VOLUME: n.volume, OPEN_PRICE: orNull(n.open), HIGH_PRICE: orNull(n.high), LOW_PRICE: orNull(n.low),
    CLOSE_PRICE: n.prevClose, SOURCE: source, IS_DELAYED: false, INSTRUMENT_ID: n.instrumentId, BID_SIZE: n.bidSize,
    ASK_SIZE: n.askSize,
  });
}

/**
 * A print of `n` on the tape: no aggressor (`side` null) off-exchange; an equity's lot under 100 is odd (I), a thinned
 * morning's print a bunched trade (B: the aggregate of regular trades the tape reports under it).
 */
export function tradeRow(rows: MarketRows, seq: number, at: number, n: Name, px: number, sz: number, side: string | null,
  venue: string, offExchange: boolean, bunched = false): void {
  rows.trades.add({
    PRINT_ID: `MT-${seq}`, SEQUENCE_NUMBER: seq, PRINT_TIME: at, PRICE: px, QUANTITY: sz, AGGRESSOR_SIDE: side, VENUE: venue,
    TRADE_CONDITION: n.kind === 'EQUITY' && sz < 100 ? 'I' : bunched ? 'B' : '@', IS_OFF_EXCHANGE: offExchange,
    INSTRUMENT_ID: n.instrumentId,
  });
  n.printed(px, sz);
}

export class Simulator {
  readonly random: JavaRandom;
  readonly names: readonly Name[];
  readonly sectors: number;
  readonly quoteRate: number;
  readonly tradeRate: number;
  /** the tape's sequence: the last print's number */
  seq = 0;
  /** the newswire: its stories where the prices move, its shocks to them; none, no news */
  news: Wire | null = null;
  /** a name's jumps a trading day (its news, a 2% move about): one every two days */
  jumpsADay = 0.5;

  constructor(random: JavaRandom, names: readonly Name[], sectors: number, quoteRate: number, tradeRate: number) {
    this.random = random;
    this.names = names;
    this.sectors = sectors;
    this.quoteRate = quoteRate;
    this.tradeRate = tradeRate;
  }

  gauss(): number {
    return this.random.nextGaussian();
  }

  /** How many of an event with `mean` expected this step: its floor, and one more by the remainder's chance. */
  events(mean: number): number {
    const n = Math.floor(mean);
    return n + (this.random.nextDouble() < mean - n ? 1 : 0);
  }

  /** `dt` seconds of the market, `stepped` the names simulated now, their rows into `rows` stamped `now` (ms). */
  step(dt: number, now: number, stepped: readonly Name[], rows: MarketRows): void {
    const root = Math.sqrt(dt / DAY);
    let market = this.gauss() * 0.010 * root;
    const sector: number[] = [];
    for (let k = 0; k < this.sectors; k++) sector.push(this.gauss() * 0.008 * root);
    // the newswire's macro and sector stories, their shocks the market's and their sectors'
    if (this.news !== null) market += this.news.beat(dt, now, sector, false);
    for (const n of stepped) {
      let r = n.beta * market + 0.6 * sector[n.sectorIx]! + this.gauss() * n.vol * root;
      // news: about one a name every two trading days, a 2% move, reported as it happens
      const jump = this.random.nextDouble() < dt * this.jumpsADay / DAY ? this.gauss() * 0.02 : 0;
      r += jump;
      n.mid = Math.max(n.tick, n.mid * Math.exp(r));
      n.drift = r;
      if (jump !== 0 && this.news !== null) this.news.jump(n, jump, now);
      let quotes = this.events(this.quoteRate * n.weight * dt);
      // a quote the price has left is moved, at once
      if (!n.quoted || n.mid <= n.bid || n.mid >= n.ask) quotes = Math.max(quotes, 1);
      for (let q = 0; q < quotes; q++) this.quote(n, now, rows);
      const trades = this.events(this.tradeRate * n.weight * dt);
      for (let t = 0; t < trades; t++) this.trade(n, now, rows);
    }
  }

  quote(n: Name, now: number, rows: MarketRows): void {
    this.requote(n);
    quoteRow(rows, n, now, n.kind === 'EQUITY' ? 'SIP' : 'SIM');
  }

  /** `n`'s top of book about its price now: its spread by its liquidity, a size each side. */
  requote(n: Name): void {
    const ticks = Math.max(1, Math.round(n.mid * n.spreadBps / 1e4 / n.tick));
    const bid = Math.floor(n.mid / n.tick - ticks / 2) * n.tick;
    n.bid = roundTo(bid, n.tick);
    n.ask = roundTo(bid + ticks * n.tick, n.tick);
    n.bidSize = this.size(n);
    n.askSize = this.size(n);
    n.quoted = true;
  }

  /**
   * THE MORNING (K14): the session from its `open` (ms), `seconds` before the world starts, so a name's chart has a day
   * to draw -- each second its price on the same factors as a step's, its quote moved where the price leaves it (never
   * written: a quote is the latest), its prints at the day's pace, busiest at the open (the first half of an intraday
   * U). The tape keeps a `thin` share of them, each that much larger, so a minute's volume is a minute's at any thinning,
   * each such print a bunched trade (B). Each second's prints in time order before they are numbered, so a print's
   * sequence is its place on the tape. The newswire's morning is written as it goes, each story at its second's start.
   */
  morning(seconds: number, open: number, thin: number, rows: MarketRows): void {
    const bunched = thin < 1;
    const second: [number, number, number, number, number][] = [];
    const whose: Name[] = [];
    for (let t = 0; t < Math.floor(seconds); t++) {
      const root = Math.sqrt(1 / DAY);
      let market = this.gauss() * 0.010 * root;
      const sector: number[] = [];
      for (let k = 0; k < this.sectors; k++) sector.push(this.gauss() * 0.008 * root);
      // the second's stories at its start, so a jump's story precedes the prints it moved
      const start = open + t * 1000;
      if (this.news !== null) market += this.news.beat(1, start, sector, true);
      const pace = 1 + 2 * Math.exp(-t / 900);
      second.length = 0;
      whose.length = 0;
      for (const n of this.names) {
        let r = n.beta * market + 0.6 * sector[n.sectorIx]! + this.gauss() * n.vol * root;
        const jump = this.random.nextDouble() < this.jumpsADay / DAY ? this.gauss() * 0.02 : 0;
        r += jump;
        n.mid = Math.max(n.tick, n.mid * Math.exp(r));
        n.drift = r;
        if (jump !== 0 && this.news !== null) this.news.jump(n, jump, start);
        if (!n.quoted || n.mid <= n.bid || n.mid >= n.ask) this.requote(n);
        const trades = this.events(this.tradeRate * n.weight * pace * thin);
        // the name's prints in this second, in time order
        const at: number[] = [];
        for (let k = 0; k < trades; k++) at.push(this.random.nextDouble());
        at.sort((a, b) => a - b);
        for (let k = 0; k < trades; k++) {
          const [px, sz, side, venue] = this.morningPrint(n, thin);
          second.push([at[k]!, px, sz, side, venue]);
          whose.push(n);
        }
      }
      // the second's prints across the names in time order, numbered so (a stable sort, as Java's object sort)
      const order = second.map((_, i) => i).sort((a, b) => second[a]![0] - second[b]![0]);
      for (const i of order) {
        const [frac, px, sz, side, venue] = second[i]!;
        const n = whose[i]!;
        tradeRow(rows, ++this.seq, Math.floor(open + (t + frac) * 1000), n, px, sz, side < 0 ? null : side === 1 ? 'B' : 'S',
          VENUES[venue]!, side < 0, bunched);
      }
    }
  }

  /** The prints a morning of `seconds` would have, each name at its weight, before any thinning. */
  morningPace(seconds: number): number {
    // the integral of the day's pace (1 + 2 e^(-t/900)) over the morning, at the world's trade rate
    return seconds + 1800 * (1 - Math.exp(-seconds / 900));
  }

  /** One of `n`'s morning prints, `1 / thin` times a print's size: its price, size, side (1 buy, 0 sell, -1 none) and
   *  venue's index; the price moved its way. */
  morningPrint(n: Name, thin: number): [number, number, number, number] {
    const buy = this.random.nextDouble() < 0.5 + 0.25 * Math.tanh(n.drift * 2000);
    let u = this.random.nextDouble();
    let venue = VENUES.length - 1;
    for (let v = 0; v < VENUES.length; v++) {
      u -= VENUE_SHARE[v]!;
      if (u < 0) {
        venue = v;
        break;
      }
    }
    const off = VENUES[venue] === 'FINRA';
    const px = off ? roundTo((n.bid + n.ask) / 2, n.tick / 2) : buy ? n.ask : n.bid;
    let sz = this.random.nextDouble() < 0.10 ? 1 + this.random.nextInt(99)
      : Math.max(100, Math.round(n.sizeMedian * 0.5 * Math.exp(this.gauss() * 0.9) / 100) * 100);
    sz = sz < 100 ? sz : Math.max(100, Math.round(sz / thin / 100) * 100);
    n.mid *= Math.exp((buy ? 1 : -1) * 0.00002 * Math.sqrt(Math.max(1, sz / Math.max(1, n.sizeMedian))));
    return [px, sz, off ? -1 : buy ? 1 : 0, venue];
  }

  /** A size at the quote: an equity's in round lots, a coin's its median's notional about. */
  size(n: Name): number {
    const s = n.sizeMedian * Math.exp(this.gauss() * 0.7);
    return n.kind === 'EQUITY' ? Math.max(100, Math.round(s / 100) * 100) : Math.round(s * 1e6) / 1e6;
  }

  trade(n: Name, now: number, rows: MarketRows): void {
    const buy = this.random.nextDouble() < 0.5 + 0.25 * Math.tanh(n.drift * 2000);
    let u = this.random.nextDouble();
    let venue = VENUES[VENUES.length - 1]!;
    for (let v = 0; v < VENUES.length; v++) {
      u -= VENUE_SHARE[v]!;
      if (u < 0) {
        venue = VENUES[v]!;
        break;
      }
    }
    // off-exchange at the midpoint (to half a tick), with no aggressor; else at the side the aggressor takes
    const off = n.kind === 'EQUITY' && venue === 'FINRA';
    const px = off ? roundTo((n.bid + n.ask) / 2, n.tick / 2) : buy ? n.ask : n.bid;
    const sz = n.kind === 'EQUITY'
      ? this.random.nextDouble() < 0.10 ? 1 + this.random.nextInt(99)
        : Math.max(100, Math.round(n.sizeMedian * 0.5 * Math.exp(this.gauss() * 0.9) / 100) * 100)
      : Math.round(n.sizeMedian * 0.3 * Math.exp(this.gauss() * 1.1) * 1e6) / 1e6;
    tradeRow(rows, ++this.seq, now, n, px, sz, off ? null : buy ? 'B' : 'S', n.kind === 'EQUITY' ? venue : 'SIM', off);
    // a print moves the price a little its way
    n.mid *= Math.exp((buy ? 1 : -1) * 0.00002 * Math.sqrt(Math.max(1, sz / Math.max(1, n.sizeMedian))));
  }
}

export function roundTo(v: number, tick: number): number {
  return Math.round(v / tick) * tick;
}
