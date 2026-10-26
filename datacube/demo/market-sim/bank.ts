// THE BANK'S OWN DAY (market.groovy's Day and its reference, K9-K13): a business unit, six desks (four over the market's
// sectors and crypto, equity derivatives, FX), three traders a desk, its books, each book's positions at the prior
// close, each desk's limits, the FX desk's clients; the listed options on the largest names, each an instrument of its
// own; and, ticking, its orders worked by algorithms into fills on the market's own prices (TRADE_ORDER, EXECUTION,
// ORDER_ROUTE), its FX prices (FX_RATE) and the clients' requests for quotes on them (RFQ). Every table the corpus's,
// every column it declares. Drawn from the bank's own random in the server's order.

import { dayAt, isoDay, padded } from './format.ts';
import { Name } from './names.ts';
import type { JavaRandom } from './random.ts';
import { DAY } from './simulator.ts';
import {
  BOOK, BUSINESS_UNIT, COUNTERPARTY, DESK, EXECUTION, FX_RATE, OPTION, ORDER_ROUTE, POSITION, RFQ, RISK_LIMIT, TableRows,
  TRADE_ORDER, TRADER,
} from './tables.ts';

interface DeskDef {
  readonly id: string;
  readonly name: string;
  readonly asset: 'EQUITY' | 'CRYPTO' | 'EQUITY_DERIVATIVES' | 'FX';
  readonly sectors: readonly string[];
}

const DESKS: readonly DeskDef[] = [
  { id: 'DESK-TMT', name: 'TMT', asset: 'EQUITY', sectors: ['Technology', 'Communication Services'] },
  { id: 'DESK-HCF', name: 'Healthcare & Financials', asset: 'EQUITY', sectors: ['Health Care', 'Financials', 'Real Estate'] },
  {
    id: 'DESK-CYC', name: 'Cyclicals & Defensives', asset: 'EQUITY',
    sectors: ['Consumer Discretionary', 'Industrials', 'Materials', 'Energy', 'Consumer Staples', 'Utilities'],
  },
  { id: 'DESK-DIG', name: 'Digital Assets', asset: 'CRYPTO', sectors: ['Crypto'] },
  { id: 'DESK-EQD', name: 'Equity Derivatives', asset: 'EQUITY_DERIVATIVES', sectors: [] },
  { id: 'DESK-FX', name: 'FX e-Trading', asset: 'FX', sectors: [] },
];
const FIRST = ['Ava', 'Ben', 'Chloe', 'Dev', 'Elena', 'Farid', 'Grace', 'Hiro', 'Isla', 'Jonah', 'Kira', 'Leo', 'Maya', 'Nikhil',
  'Olivia', 'Pablo', 'Quinn', 'Rosa'];
const LAST = ['Adler', 'Brooks', 'Chen', 'Dubois', 'Evans', 'Fischer', 'Gupta', 'Hayes', 'Ito', 'Jensen', 'Kowalski', 'Lopez',
  'Mensah', 'Novak', 'Okafor', 'Petrov', 'Quint', 'Rossi'];
const SENIORITY = ['MD', 'VP', 'Associate'];
const CLIENTS: readonly (readonly [string, string, string])[] = [['Aurora Capital', 'HEDGE_FUND', 'T1'], ['Bluewater Partners', 'HEDGE_FUND', 'T1'],
  ['Cinder Asset Mgmt', 'ASSET_MANAGER', 'T1'], ['Driftwood Pension', 'PENSION', 'T2'], ['Eastgate Insurance', 'INSURER', 'T2'],
  ['Fjord Shipping', 'CORPORATE', 'T3'], ['Galena Metals', 'CORPORATE', 'T3'], ['Halcyon Fund', 'HEDGE_FUND', 'T2'],
  ['Ironbark Bank', 'BANK', 'T2'], ['Jasper Retail', 'CORPORATE', 'T3'], ['Kestrel Macro', 'HEDGE_FUND', 'T1'],
  ['Larch Endowment', 'ASSET_MANAGER', 'T2'], ['Monarch Airlines', 'CORPORATE', 'T3'], ['Nimbus Tech', 'CORPORATE', 'T3'],
  ['Opal Sovereign', 'SOVEREIGN', 'T1'], ['Pelican Bank', 'BANK', 'T2'], ['Quartz Quant', 'HEDGE_FUND', 'T1'],
  ['Rowan Family Office', 'ASSET_MANAGER', 'T3'], ['Sable Commodities', 'CORPORATE', 'T2'], ['Tern Global', 'ASSET_MANAGER', 'T2']];
/** the pairs the FX desk prices: [pair, base, mid, pip, spread in pips, daily vol, weight] */
const PAIRS: readonly (readonly [string, string, number, number, number, number, number])[] = [
  ['EUR/USD', 'EUR', 1.0850, 0.0001, 0.3, 0.0055, 0.30], ['GBP/USD', 'GBP', 1.2700, 0.0001, 0.6, 0.0065, 0.12],
  ['USD/JPY', 'USD', 151.20, 0.01, 0.4, 0.0070, 0.15], ['USD/CHF', 'USD', 0.8800, 0.0001, 0.8, 0.0060, 0.05],
  ['AUD/USD', 'AUD', 0.6550, 0.0001, 0.6, 0.0080, 0.08], ['USD/CAD', 'USD', 1.3750, 0.0001, 0.8, 0.0055, 0.07],
  ['NZD/USD', 'NZD', 0.6000, 0.0001, 1.0, 0.0085, 0.04], ['EUR/GBP', 'EUR', 0.8543, 0.0001, 0.8, 0.0045, 0.07],
  ['EUR/JPY', 'EUR', 164.05, 0.01, 1.0, 0.0080, 0.07], ['GBP/JPY', 'GBP', 192.02, 0.01, 1.5, 0.0095, 0.05]];
const RATE = 0.05;

/** A listed option: its id, ticker, kind, strike, expiry (a day), settlement at the prior close, volatility, money-ness,
 *  underlying, instrument, open interest. */
export interface OptionRow {
  readonly id: string;
  readonly ticker: string;
  readonly type: 'CALL' | 'PUT';
  readonly strike: number;
  readonly expiry: number;
  readonly premium: number;
  readonly iv: number;
  readonly itm: boolean;
  readonly underlying: Name;
  readonly instrument: string;
  readonly oi: number;
}

/** A currency pair the desk makes prices in. A cross (`leg1` set) is quoted about its legs' product, or their ratio
 *  (`divide`), `basis` pips of its own about it. */
export class Pair {
  readonly pair: string;
  readonly base: string;
  mid: number;
  prevClose: number;
  readonly pip: number;
  readonly spreadPips: number;
  readonly vol: number;
  readonly weight: number;
  basis = 0;
  leg1: Pair | null = null;
  leg2: Pair | null = null;
  divide = false;

  constructor(pair: string, base: string, mid: number, prevClose: number, pip: number, spreadPips: number, vol: number,
    weight: number) {
    this.pair = pair;
    this.base = base;
    this.mid = mid;
    this.prevClose = prevClose;
    this.pip = pip;
    this.spreadPips = spreadPips;
    this.vol = vol;
    this.weight = weight;
  }

  /** A cross's mid as its legs imply it. */
  implied(): number {
    return this.divide ? this.leg1!.mid / this.leg2!.mid : this.leg1!.mid * this.leg2!.mid;
  }
}

/** The bank, made: its reference tables' rows, and what its day trades. */
export interface Bank {
  readonly tables: readonly TableRows[];
  readonly options: readonly OptionRow[];
  readonly day: Day;
}

/** Black and Scholes, for an option's settlement at the prior close: its normal distribution by Abramowitz and Stegun
 *  26.2.17. */
function cdf(x: number): number {
  const t = 1 / (1 + 0.2316419 * Math.abs(x));
  const d = 0.3989422804014327 * Math.exp(-x * x / 2);
  const p = d * t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
  return x >= 0 ? 1 - p : p;
}

function black(call: boolean, s: number, k: number, t: number, v: number, r: number): number {
  const d1 = (Math.log(s / k) + (r + v * v / 2) * t) / (v * Math.sqrt(t));
  const d2 = d1 - v * Math.sqrt(t);
  return call ? s * cdf(d1) - k * Math.exp(-r * t) * cdf(d2) : k * Math.exp(-r * t) * cdf(-d2) - s * cdf(-d1);
}

const round = (v: number, to: number): number => Math.round(v / to) * to;

/** The bank at `now` (ms; `today` its day): every reference row, drawn from `bank` as the server's are. */
export function makeBank(bank: JavaRandom, equities: readonly Name[], coins: readonly Name[], today: number, now: number,
  rates: { readonly orders: number; readonly rfqs: number }): Bank {
  const yesterday = today - 1;
  const businessUnit = new TableRows(BUSINESS_UNIT);
  businessUnit.add({
    BU_ID: 'BU-MARKETS', NAME: 'Global Markets', HEAD: 'TR-01', DIVISION: 'Markets', IS_ACTIVE: true, HEADCOUNT: 18, BUDGET: 4.2e7,
    COST_CENTER: 'CC-100', REGION: 'AMER', ESTABLISHED_DATE: dayAt(2009, 1, 5), DESCRIPTION: 'Sales and trading', LEGAL_ENTITY_ID: 'LE-US',
  });

  const deskTraders = new Map<string, string[]>();
  const traderIds: string[] = [];
  DESKS.forEach((d, i) => {
    const ids = [0, 1, 2].map((j) => `TR-${padded(i * 3 + j + 1, 2)}`);
    deskTraders.set(d.id, ids);
    traderIds.push(...ids);
  });
  const traders = new TableRows(TRADER);
  traderIds.forEach((id, i) => {
    traders.add({
      TRADER_ID: id, FIRST_NAME: FIRST[i]!, LAST_NAME: LAST[i]!, BADGE: `B${padded(41000 + i * 37, 5)}`,
      EMAIL: `${FIRST[i]!}.${LAST[i]!}`.toLowerCase() + '@bank.example', HIRE_DATE: dayAt(2012 + i % 12, 1 + i % 12, 1 + i % 27),
      SENIORITY: SENIORITY[i % 3]!, IS_ACTIVE: true, SPECIALIZATION: DESKS[Math.floor(i / 3)]!.name, LICENSE: 'Series 7',
      MAX_NOTIONAL: [5e7, 2e7, 1e7][i % 3]!, PNL_YTD: Math.round(bank.nextGaussian() * 2e6),
      DESK_ID: DESKS[Math.floor(i / 3)]!.id, MANAGER_ID: i % 3 === 0 ? null : traderIds[i - i % 3]!,
    });
  });

  // the books: two an equity desk (making markets and taking a view), one for crypto, two for derivatives, one for FX
  const books: { id: string; name: string; strategy: string; desk: DeskDef; scale: number }[] = [];
  for (const d of DESKS) {
    const kinds: [string, string, number][] = d.asset === 'EQUITY' ? [['MM', 'market making', 0.8], ['DIR', 'directional', 1.6]]
      : d.asset === 'CRYPTO' ? [['MM', 'market making', 1.0]]
        : d.asset === 'FX' ? [['FLOW', 'client flow', 1.0]]
          : [['VOL', 'volatility', 1.0], ['FLOW', 'client flow', 1.0]];
    for (const [code, strategy, scale] of kinds) {
      books.push({ id: `BK-${d.id.substring(5)}-${code}`, name: `${d.name} ${strategy}`, strategy, desk: d, scale });
    }
  }

  // the options: a listed chain on the four largest names -- three expiries, nine strikes about the prior close, calls
  // and puts; each settled at the prior close by Black and Scholes on its implied volatility (a smile)
  const options: OptionRow[] = [];
  for (const u of equities.slice(0, 4)) {
    const step = u.prevClose >= 200 ? 5 : u.prevClose >= 50 ? 2.5 : 1;
    const atm = Math.round(u.prevClose / step) * step;
    for (const days of [7, 30, 91]) {
      const expiry = today + days;
      for (let j = -4; j <= 4; j++) {
        const strike = atm + j * step * Math.max(1, Math.round(u.prevClose * 0.05 / step));
        if (strike <= 0) continue;
        for (const type of ['CALL', 'PUT'] as const) {
          const m = Math.log(strike / u.prevClose);
          const iv = Math.round(u.vol * Math.sqrt(252) * (1 + 1.2 * m * m - 0.6 * m) * (1 + 0.05 * Math.sqrt(days / 30)) * 10000) / 10000;
          const t = days / 365;
          // a listed option trades at a tick at least
          const premium = Math.max(0.01, Math.round(black(type === 'CALL', u.prevClose, strike, t, iv, RATE) * 100) / 100);
          // a whole strike without its point (Java's long), any other as Java writes a double (152.5)
          const id = `${u.sym}-${isoDay(expiry).replaceAll('-', '')}-${type.substring(0, 1)}-${String(strike)}`;
          options.push({
            id: `OPT-${id}`, ticker: id, type, strike, expiry, premium, iv,
            itm: type === 'CALL' ? u.prevClose > strike : u.prevClose < strike, underlying: u, instrument: `INST-OPT-${id}`,
            oi: 100 + bank.nextInt(20000),
          });
        }
      }
    }
  }
  const optionRows = new TableRows(OPTION);
  for (const o of options) {
    optionRows.add({
      OPTION_ID: o.id, OPTION_TYPE: o.type, STRIKE_PRICE: o.strike, EXPIRATION_DATE: o.expiry, PREMIUM: o.premium,
      IMPLIED_VOLATILITY: o.iv, EXERCISE_STYLE: 'AMERICAN', IS_IN_THE_MONEY: o.itm, UNDERLYING_PRICE: o.underlying.prevClose,
      CONTRACT_SIZE: 100, OPEN_INTEREST: o.oi, UNDERLYING_ID: o.underlying.instrumentId, INSTRUMENT_ID: o.instrument,
    });
  }

  // each book's positions at the prior close: an equity book holds names of its desk's sectors (a market maker small and
  // two-way, a directional book larger and mostly long); crypto's its coins; derivatives' the options
  interface Held { id: string; qty: number; cost: number; px: number; unit: number; book: string; instrument: string; desk: string }
  const held: Held[] = [];
  for (const b of books) {
    const candidates: (Name | OptionRow)[] = b.desk.asset === 'EQUITY' ? equities.filter((n) => b.desk.sectors.includes(n.sector)).slice(0, 60)
      : b.desk.asset === 'CRYPTO' ? [...coins] : b.desk.asset === 'EQUITY_DERIVATIVES' ? [...options] : [];
    bank.shuffle(candidates);
    const take = b.desk.asset === 'EQUITY' ? 30 : candidates.length < 40 ? candidates.length : 40;
    for (const h of candidates.slice(0, take)) {
      const option = !(h instanceof Name);
      const px = option ? (h as OptionRow).premium : (h as Name).prevClose;
      const unit = option ? 100 : 1;
      const gross = (b.desk.asset === 'EQUITY' ? 2.5e6 : b.desk.asset === 'CRYPTO' ? 1.5e6 : 2.0e5) * b.scale * Math.exp(bank.nextGaussian() * 0.6);
      const longSide = bank.nextDouble() < (b.strategy === 'directional' ? 0.75 : 0.5);
      let qty = Math.max(1, Math.round(gross / Math.max(0.01, px * unit)));
      if (!option && (h as Name).kind === 'EQUITY') qty = Math.max(100, Math.round(qty / 100) * 100);
      if (option || (h as Name).kind !== 'EQUITY') qty = Math.max(1, Math.round(qty));
      qty = longSide ? qty : -qty;
      const cost = Math.round(px * (1 + bank.nextGaussian() * 0.04) * 100) / 100;
      held.push({
        id: `POS-${padded(held.length + 1, 5)}`, qty, cost, px, unit, book: b.id, desk: b.desk.id,
        instrument: option ? (h as OptionRow).instrument : (h as Name).instrumentId,
      });
    }
  }
  const positions = new TableRows(POSITION);
  // the server draws each position's realised P&L as it makes the table: after every position
  const realised = held.map(() => Math.round(bank.nextGaussian() * 2e4));
  held.forEach((p, i) => {
    positions.add({
      POSITION_ID: p.id, QUANTITY: p.qty, AVERAGE_COST: p.cost, MARKET_VALUE: p.qty * p.px * p.unit,
      UNREALIZED_PN_L: p.qty * (p.px - p.cost) * p.unit, REALIZED_PN_L: realised[i]!, CURRENCY: 'USD', POSITION_DATE: yesterday,
      DIRECTION: p.qty >= 0 ? 'LONG' : 'SHORT', COST_BASIS: p.qty * p.cost * p.unit, LAST_UPDATED: now, IS_OPEN: true,
      BOOK_ID: p.book, INSTRUMENT_ID: p.instrument,
    });
  });

  // each desk's gross at the prior close, by which its limits are set
  const deskGross = new Map(DESKS.map((d) => [d.id, held.filter((p) => p.desk === d.id).reduce((s, p) => s + Math.abs(p.qty * p.px * p.unit), 0)]));
  const bookRows = new TableRows(BOOK);
  const bookPnl = books.map(() => Math.round(bank.nextGaussian() * 3e6));
  books.forEach((b, i) => {
    bookRows.add({
      BOOK_ID: b.id, NAME: b.name, BOOK_TYPE: 'TRADING', CURRENCY: 'USD', IS_ACTIVE: true, OPEN_DATE: dayAt(2020, 1, 2),
      LEGAL_ENTITY: 'LE-US', STRATEGY: b.strategy, PNL_YTD: bookPnl[i]!, POSITION_COUNT: held.filter((p) => p.book === b.id).length,
      // a book's own day loss limit: a share of its desk's
      RISK_LIMIT: Math.max(5e4, Math.round(deskGross.get(b.desk.id)! * 0.002 / 1e4) * 1e4), DESK_ID: b.desk.id,
    });
  });
  const deskRows = new TableRows(DESK);
  DESKS.forEach((d, i) => {
    deskRows.add({
      DESK_ID: d.id, NAME: d.name, DESK_TYPE: 'TRADING', ASSET_CLASS: d.asset, REGION: 'AMER', IS_ACTIVE: true,
      HEAD_TRADER: deskTraders.get(d.id)![0]!, COST_CENTER: `CC-${110 + i}`, FLOOR: '7', BUILDING: '200 Market Street',
      RISK_LIMIT: Math.round(deskGross.get(d.id)! * 1.5 / 1e6) * 1e6, PNL_TARGET: Math.round(deskGross.get(d.id)! * 0.0005 / 1e3) * 1e3,
      BUSINESS_UNIT_ID: 'BU-MARKETS',
    });
  });

  // each desk's limits: its day's loss, set by how far its book moves in a day (a quarter of a percent of an equity
  // desk's gross; crypto's and options' moves are wider), so a bad day breaches it; and its gross notional
  const limits = new TableRows(RISK_LIMIT);
  for (const d of DESKS) {
    const gross = deskGross.get(d.id)!;
    if (gross <= 0) continue;
    const loss = d.asset === 'CRYPTO' ? 0.012 : d.asset === 'EQUITY_DERIVATIVES' ? 0.04 : 0.0025;
    const code = d.id.substring(5);
    const lossAmount = Math.max(2.5e4, Math.round(gross * loss / 1e4) * 1e4);
    const grossAmount = Math.round(gross * (1.1 + 0.5 * bank.nextDouble()) / 1e6) * 1e6;
    for (const [id, type, amount] of [[`LIM-${code}-LOSS`, 'DAY_LOSS', lossAmount], [`LIM-${code}-GROSS`, 'GROSS_NOTIONAL', grossAmount]] as const) {
      limits.add({
        LIMIT_ID: id, LIMIT_TYPE: type, LIMIT_AMOUNT: amount, CURRENCY: 'USD', EFFECTIVE_DATE: dayAt(2026, 1, 2), STATUS: 'ACTIVE',
        APPROVED_BY: 'CRO', APPROVAL_DATE: dayAt(2025, 12, 15), WARNING_THRESHOLD: 0.8, HARD_LIMIT: amount * 1.2, IS_ACTIVE: true,
        DESK_ID: d.id,
      });
    }
  }

  // the FX desk's clients: hedge funds, real money, corporates and banks, each an edge the desk prices it with
  const clientIds = CLIENTS.map((_, i) => `CP-${padded(i + 1, 3)}`);
  const countries = ['US', 'GB', 'DE', 'SG', 'JP'];
  const counterparties = new TableRows(COUNTERPARTY);
  CLIENTS.forEach(([name, type, tier], i) => {
    counterparties.add({
      COUNTERPARTY_ID: clientIds[i]!, LEGAL_NAME: `${name} Ltd`, SHORT_NAME: name, LEI: `5493${padded(7_000_000_000 + i * 7919, 16)}`,
      COUNTERPARTY_TYPE: type, DOMICILE: countries[i % 5]!, INCORPORATION_COUNTRY: countries[i % 5]!, IS_ACTIVE: true,
      ONBOARD_DATE: dayAt(2015 + i % 10, 1 + i % 12, 1), CREDIT_RATING: ['AA', 'A', 'BBB'][i % 3]!, RATING_AGENCY: 'S&P', TIER: tier,
      INDUSTRY: type, COUNTRY_CODE: countries[i % 5]!,
    });
  });
  // a first-tier client is priced tightest
  const clientEdges = CLIENTS.map(([, , tier]) => (tier === 'T1' ? 0.5 : tier === 'T2' ? 0.2 : 0));

  // the pairs: the majors, and three crosses quoted about their legs' product (or ratio)
  const pairs = PAIRS.map(([pair, base, mid, pip, spread, vol, weight]) =>
    new Pair(pair, base, mid, mid * Math.exp(bank.nextGaussian() * 0.003), pip, spread, vol, weight));
  const pairOf = new Map(pairs.map((p) => [p.pair, p]));
  for (const [cross, l1, l2, divide] of [['EUR/GBP', 'EUR/USD', 'GBP/USD', true], ['EUR/JPY', 'EUR/USD', 'USD/JPY', false],
    ['GBP/JPY', 'GBP/USD', 'USD/JPY', false]] as const) {
    const p = pairOf.get(cross)!;
    p.leg1 = pairOf.get(l1)!;
    p.leg2 = pairOf.get(l2)!;
    p.divide = divide;
    p.mid = p.implied();
    p.prevClose = divide ? p.leg1.prevClose / p.leg2.prevClose : p.leg1.prevClose * p.leg2.prevClose;
  }

  // the books that send orders: the equity and crypto desks', each its desk's names and traders
  const orderBooks = books.filter((b) => b.desk.asset === 'EQUITY' || b.desk.asset === 'CRYPTO').map((b) => ({
    id: b.id, names: b.desk.asset === 'CRYPTO' ? coins : equities.filter((n) => b.desk.sectors.includes(n.sector)),
    traders: deskTraders.get(b.desk.id)!,
  }));
  const day = new Day(bank, orderBooks, pairs, clientIds, clientEdges, deskTraders.get('DESK-FX')!, rates.orders, rates.rfqs, now);
  return {
    tables: [businessUnit, traders, bookRows, deskRows, optionRows, positions, limits, counterparties],
    options,
    day,
  };
}

/** A parent order an algorithm works into fills on the market's prices. */
interface WorkingOrder {
  id: string;
  bookId: string;
  traderId: string;
  name: Name;
  side: 'BUY' | 'SELL';
  quantity: number;
  filled: number;
  arrival: number;
  strategy: string;
  timeInForce: 'DAY' | 'IOC';
  limit: number;
  priority: number;
  created: number;
  status: 'NEW' | 'WORKING' | 'PARTIALLY_FILLED' | 'FILLED' | 'CANCELLED';
  /** fills it is worked in, about: a slice's size is its quantity over them */
  slices: number;
}

/** A client's request for a quote: asked, quoted (or declined) after the desk's response, then done, missed or let
 *  expire by the client. */
interface Request {
  id: string;
  asked: number;
  pair: Pair;
  side: 'BUY' | 'SELL';
  notional: number;
  tenor: string;
  clientId: string;
  dealerId: string;
  clientEdge: number;
  quotedRate: number;
  spreadBps: number;
  status: 'NEW' | 'QUOTED' | 'DECLINED' | 'DONE' | 'MISSED' | 'EXPIRED';
  responseMs: number;
  hit: boolean;
  quoteAtMs: number;
  decideAtMs: number;
}

/** The rows a beat of the bank's day makes. */
export interface DayRows {
  readonly orders: TableRows;
  readonly executions: TableRows;
  readonly routes: TableRows;
  readonly rates: TableRows;
  readonly rfqs: TableRows;
}

export function dayRows(): DayRows {
  return {
    orders: new TableRows(TRADE_ORDER), executions: new TableRows(EXECUTION), routes: new TableRows(ORDER_ROUTE),
    rates: new TableRows(FX_RATE), rfqs: new TableRows(RFQ),
  };
}

const STRATEGIES = ['VWAP', 'TWAP', 'POV', 'IS', 'DARK'];
const BROKERS = ['GSCO', 'MSCO', 'JPMS', 'CITI', 'BARC'];
const LIT = ['XNAS', 'XNYS', 'ARCX', 'BATS', 'EDGX', 'IEXG'];
const DARK = ['SIGMA-X', 'CROSSFINDER', 'MS-POOL', 'LIQUIDNET'];
const TENORS = ['SPOT', 'SPOT', 'SPOT', 'SPOT', 'SPOT', 'SPOT', '1W', '1M', '1M', '3M'];

/** A book that sends orders: its id, its desk's names and traders. */
interface OrderBook {
  readonly id: string;
  readonly names: readonly Name[];
  readonly traders: readonly string[];
}

export class Day {
  readonly #random: JavaRandom;
  readonly #books: readonly OrderBook[];
  readonly pairs: readonly Pair[];
  readonly #clients: readonly string[];
  readonly #clientEdge: readonly number[];
  readonly #dealers: readonly string[];
  readonly #orderRate: number;
  readonly #rfqRate: number;
  readonly #working: WorkingOrder[] = [];
  readonly #pending: Request[] = [];
  #orderSeq = 0;
  #execSeq = 0;
  #routeSeq = 0;
  #rfqSeq = 0;
  #last: number;

  constructor(random: JavaRandom, books: readonly OrderBook[], pairs: readonly Pair[], clients: readonly string[], clientEdge: readonly number[], dealers: readonly string[],
    orderRate: number, rfqRate: number, now: number) {
    this.#random = random;
    this.#books = books;
    this.pairs = pairs;
    this.#clients = clients;
    this.#clientEdge = clientEdge;
    this.#dealers = dealers;
    this.#orderRate = orderRate;
    this.#rfqRate = rfqRate;
    this.#last = now;
  }

  #events(mean: number): number {
    const n = Math.floor(mean);
    return n + (this.#random.nextDouble() < mean - n ? 1 : 0);
  }

  /** Every pair priced once, so the screens have a rate for each before the first beat moves it. */
  opening(now: number, rows: DayRows): void {
    for (const p of this.pairs) rateRow(rows, p, now);
  }

  /** One beat at `now` (ms): new orders and their fills, the FX prices moved, the clients' requests asked and answered. */
  beat(now: number, rows: DayRows): void {
    const dt = Math.min(1, (now - this.#last) / 1000);
    this.#last = now;
    this.#work(dt, now, rows);
    this.#price(dt, now, rows);
    this.#request(dt, now, rows);
  }

  // ------------------------------------------------ orders, worked into fills

  #work(dt: number, now: number, rows: DayRows): void {
    const arriving = this.#working.length < 80 ? this.#events(this.#orderRate * dt) : 0;
    for (let i = 0; i < arriving; i++) orderRow(rows, this.#arrive(now), now);
    for (const w of [...this.#working]) {
      if (w.status === 'NEW') {
        w.status = 'WORKING';
        orderRow(rows, w, now);
        continue;
      }
      // a slice about every two seconds, the dark strategy's less often (it waits for a match)
      const every = w.strategy === 'DARK' ? 4 : 2;
      if (this.#random.nextDouble() >= dt / every) continue;
      this.#slice(w, now, rows);
      orderRow(rows, w, now);
      if (w.status === 'FILLED' || w.status === 'CANCELLED') this.#working.splice(this.#working.indexOf(w), 1);
    }
  }

  #arrive(now: number): WorkingOrder {
    const r = this.#random;
    const book = this.#books[r.nextInt(this.#books.length)]!;
    // a name by its activity: the largest are traded most
    let u = r.nextDouble() * book.names.reduce((s, n) => s + n.weight, 0);
    let name = book.names[book.names.length - 1]!;
    for (const n of book.names) {
      if ((u -= n.weight) < 0) {
        name = n;
        break;
      }
    }
    const notional = 250_000 * Math.exp(r.nextGaussian() * 0.9);
    const qty = name.kind === 'EQUITY' ? Math.max(100, round(notional / name.mid, 100)) : round(notional / name.mid, name.mid > 1000 ? 0.01 : 1);
    const id = `ORD-${padded(++this.#orderSeq, 6)}`;
    const traderId = book.traders[r.nextInt(book.traders.length)]!;
    const side = r.nextBoolean() ? 'BUY' : 'SELL';
    const arrival = (name.bid + name.ask) / 2;
    const strategy = STRATEGIES[r.nextInt(STRATEGIES.length)]!;
    const timeInForce = r.nextDouble() < 0.9 ? 'DAY' : 'IOC';
    const priority = 1 + r.nextInt(3);
    const slices = 6 + r.nextInt(20);
    const w: WorkingOrder = {
      id, bookId: book.id, traderId, name, side, quantity: qty, filled: 0, arrival, strategy, timeInForce, limit: Number.NaN,
      priority, created: now, status: 'NEW', slices,
    };
    // a limit on a third of them, a little through the arrival price
    if (r.nextDouble() < 0.33) w.limit = round(w.arrival * (w.side === 'BUY' ? 1.003 : 0.997), name.tick);
    this.#working.push(w);
    return w;
  }

  /** One child slice of `w`: routed, and filled -- or, now and then, rejected by its venue -- at the market's price. */
  #slice(w: WorkingOrder, now: number, rows: DayRows): void {
    const r = this.#random;
    const n = w.name;
    const remaining = w.quantity - w.filled;
    const lot = n.kind === 'EQUITY' ? 100 : n.mid > 1000 ? 0.01 : 1;
    const clip = Math.min(remaining, Math.max(lot, round(w.quantity / w.slices * Math.exp(r.nextGaussian() * 0.4), lot)));
    const dark = w.strategy === 'DARK' ? r.nextDouble() < 0.8 : r.nextDouble() < 0.15;
    const venue = n.kind !== 'EQUITY' ? 'CBX' : dark ? DARK[r.nextInt(DARK.length)]! : LIT[r.nextInt(LIT.length)]!;
    const broker = BROKERS[r.nextInt(BROKERS.length)]!;
    const buy = w.side === 'BUY';
    // the price: across the spread (aggressive), at the near side (passive), or the midpoint (dark)
    const u = r.nextDouble();
    const liquidity = dark ? 'MIDPOINT' : u < 0.45 ? 'REMOVE' : 'ADD';
    const px = liquidity === 'MIDPOINT' ? (n.bid + n.ask) / 2 : liquidity === 'REMOVE' ? (buy ? n.ask : n.bid) : (buy ? n.bid : n.ask);
    const outside = !Number.isNaN(w.limit) && (buy ? px > w.limit : px < w.limit);
    const rejected = r.nextDouble() < 0.03 || outside;
    const route = ++this.#routeSeq;
    rows.routes.add({
      ROUTE_ID: `RT-${padded(route, 7)}`, ROUTE_DATE: now, DESTINATION: venue, ROUTE_QUANTITY: clip, ROUTED_PRICE: px,
      STATUS: rejected ? 'REJECTED' : 'FILLED', ROUTE_TYPE: dark ? 'MIDPEG' : liquidity === 'REMOVE' ? 'IOC' : 'LIMIT',
      PRIORITY: w.priority, LATENCY_MS: 1 + r.nextInt(4), IS_SMART_ROUTED: n.kind === 'EQUITY',
      REJECT_REASON: rejected ? (outside ? 'PRICE_OUTSIDE_LIMIT' : 'VENUE_REJECT') : null, BROKER: broker, ORDER_ID: w.id,
    });
    if (rejected) return;
    w.filled += clip;
    const done = w.quantity - w.filled < lot / 2;
    w.status = done ? 'FILLED' : 'PARTIALLY_FILLED';
    const exec = ++this.#execSeq;
    rows.executions.add({
      EXECUTION_ID: `EX-${padded(exec, 7)}`, EXECUTION_TIME: now, FILLED_QUANTITY: clip, FILL_PRICE: px, VENUE: venue,
      BROKER: broker, COMMISSION: round(clip * (n.kind === 'EQUITY' ? 0.003 : px * 0.0005), 0.01), FILL_TYPE: done ? 'FULL' : 'PARTIAL',
      // the source keeps no slippage or impact of its own: the screens compute them, the database does
      EXECUTION_ALGO: w.strategy, ARRIVAL_PRICE: w.arrival, IS_PARTIAL_FILL: !done, SEQUENCE_NUMBER: exec, LAST_MARKET: venue,
      ORDER_ID: w.id,
    });
    // an order of a day's time in force is cancelled now and then with some left; an IOC's rest at once
    if (!done && (w.timeInForce === 'IOC' || r.nextDouble() < 0.01)) w.status = 'CANCELLED';
  }

  // ------------------------------------------------ FX prices

  #price(dt: number, now: number, rows: DayRows): void {
    const r = this.#random;
    const moved = new Set<Pair>();
    for (const p of this.pairs) {
      if (p.leg1 !== null) continue;
      p.mid *= Math.exp(r.nextGaussian() * p.vol * Math.sqrt(dt / DAY));
      // about ten prices a second for the most traded, fewer for the rest
      if (r.nextDouble() < Math.min(1, 10 * p.weight * dt / 0.15)) {
        rateRow(rows, p, now);
        moved.add(p);
      }
    }
    // each cross about its legs' product (or ratio), a few tenths of a pip of its own about it (a basis that reverts),
    // priced again whenever a leg is -- never a free lunch against its legs
    for (const p of this.pairs) {
      if (p.leg1 === null) continue;
      p.basis = 0.7 * p.basis + 0.21 * r.nextGaussian();
      p.mid = p.implied() + p.basis * p.pip;
      if (moved.has(p.leg1) || moved.has(p.leg2!) || r.nextDouble() < Math.min(1, 10 * p.weight * dt / 0.15)) rateRow(rows, p, now);
    }
  }

  // ------------------------------------------------ the clients' requests

  #request(dt: number, now: number, rows: DayRows): void {
    const r = this.#random;
    const asking = this.#events(this.#rfqRate * dt);
    for (let i = 0; i < asking; i++) {
      const c = r.nextInt(this.#clients.length);
      let u = r.nextDouble() * this.pairs.reduce((s, p) => s + p.weight, 0);
      let pair = this.pairs[this.pairs.length - 1]!;
      for (const p of this.pairs) {
        if ((u -= p.weight) < 0) {
          pair = p;
          break;
        }
      }
      const id = `RFQ-${padded(++this.#rfqSeq, 6)}`;
      const side = r.nextBoolean() ? 'BUY' : 'SELL';
      const notional = Math.max(1, Math.round(5 * Math.exp(r.nextGaussian() * 0.8))) * 1e6;
      const tenor = TENORS[r.nextInt(TENORS.length)]!;
      const dealerId = this.#dealers[r.nextInt(this.#dealers.length)]!;
      const q: Request = {
        id, asked: now, pair, side, notional, tenor, clientId: this.#clients[c]!, dealerId, clientEdge: this.#clientEdge[c]!,
        quotedRate: Number.NaN, spreadBps: Number.NaN, status: 'NEW', responseMs: -1, hit: false, quoteAtMs: 0, decideAtMs: 0,
      };
      q.responseMs = 40 + Math.round(Math.abs(r.nextGaussian()) * 250);
      q.quoteAtMs = now + q.responseMs;
      this.#pending.push(q);
      rfqRow(rows, q);
    }
    for (const q of [...this.#pending]) {
      if (q.status === 'NEW' && now >= q.quoteAtMs) {
        if (r.nextDouble() < 0.04) {
          // the desk declines: no price
          q.status = 'DECLINED';
          rfqRow(rows, q);
          this.#pending.splice(this.#pending.indexOf(q), 1);
          continue;
        }
        // a spread wider for a larger ticket and a longer tenor, narrower for a client the desk wants
        const bps = Math.max(0.2, 0.6 + 0.08 * q.notional / 1e6 + (q.tenor === 'SPOT' ? 0 : 1.5) - q.clientEdge + r.nextGaussian() * 0.3);
        q.spreadBps = Math.round(bps * 100) / 100;
        q.quotedRate = round(q.pair.mid * (1 + (q.side === 'BUY' ? 1 : -1) * bps / 1e4), q.pair.pip / 10);
        q.status = 'QUOTED';
        q.decideAtMs = now + 300 + Math.trunc(r.nextDouble() * 2500);
        rfqRow(rows, q);
        continue;
      }
      if (q.status === 'QUOTED' && now >= q.decideAtMs) {
        // the tighter the price, the likelier it is hit; a few let it lapse
        const hitChance = 1 / (1 + Math.exp(2.2 * (q.spreadBps - 1.2)));
        const u = r.nextDouble();
        q.status = u < 0.06 ? 'EXPIRED' : u < 0.06 + 0.94 * hitChance ? 'DONE' : 'MISSED';
        q.hit = q.status === 'DONE';
        rfqRow(rows, q);
        this.#pending.splice(this.#pending.indexOf(q), 1);
      }
    }
  }
}

function orderRow(rows: DayRows, w: WorkingOrder, now: number): void {
  rows.orders.add({
    ORDER_ID: w.id, ORDER_DATE: Math.floor(w.created / 86_400_000), SIDE: w.side, QUANTITY: w.quantity, FILLED_QUANTITY: w.filled,
    REMAINING_QUANTITY: w.status === 'CANCELLED' ? 0 : w.quantity - w.filled, LIMIT_PRICE: Number.isNaN(w.limit) ? null : w.limit,
    TIME_IN_FORCE: w.timeInForce, STATUS: w.status, PRIORITY: w.priority, CREATED_TIME: w.created, LAST_MODIFIED_TIME: now,
    IS_ALGO_ORDER: true, ALGO_STRATEGY: w.strategy, INSTRUMENT_ID: w.name.instrumentId, TRADER_ID: w.traderId, BOOK_ID: w.bookId,
  });
}

/** An FX price's row: its change on the day against the pair's prior close. */
function rateRow(rows: DayRows, p: Pair, now: number): void {
  const half = p.spreadPips * p.pip / 2;
  const mid = round(p.mid, p.pip / 10);
  rows.rates.add({
    RATE_ID: `FX-${p.pair.replace('/', '')}`, CURRENCY_PAIR: p.pair, SPOT_RATE: mid, BID_RATE: round(mid - half, p.pip / 10),
    ASK_RATE: round(mid + half, p.pip / 10), RATE_DATE: now, SOURCE: 'SIM', MID_RATE: mid, DAILY_CHANGE: mid - p.prevClose,
    DAILY_CHANGE_PCT: mid / p.prevClose - 1, IS_OFFICIAL: false, BASE_CURRENCY_CODE: p.base,
  });
}

/** A request's row, at each of its states. */
function rfqRow(rows: DayRows, q: Request): void {
  rows.rfqs.add({
    RFQ_ID: q.id, REQUEST_TIME: q.asked, CURRENCY_PAIR: q.pair.pair, SIDE: q.side, NOTIONAL: q.notional, TENOR: q.tenor,
    QUOTED_RATE: Number.isNaN(q.quotedRate) ? null : q.quotedRate, SPREAD_BPS: Number.isNaN(q.spreadBps) ? null : q.spreadBps,
    STATUS: q.status, RESPONSE_TIME_MS: q.status === 'NEW' ? null : q.responseMs, IS_HIT: q.hit, COUNTERPARTY_ID: q.clientId,
    TRADER_ID: q.dealerId,
  });
}
