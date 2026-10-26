// THE NEWSWIRE (market.groovy's Newsroom, K15): the stories a terminal's feed shows, each the world's own, written
// where the simulator moves a price so that a headline and its move agree. A name's jump is reported at its instant (its
// category by chance, its wording by its sign, its size in the headline, a FLASH from 3%); a macro story moves the whole
// market and a sector's story its sector, each by the shock its headline names; a brief (an analyst's note, a filing,
// an appointment, a dividend) nudges its name its way or not at all; crypto's briefs say where a coin stands against its
// prior close, live or simulated. The names are the world's (made up), the brokers, funds and wires too; no story is
// about a real company. Its own random, so the prices' draws are the simulation's. The tab reads no filings of EDGAR's
// (it answers no browser: no CORS, a User-Agent a page cannot set), so the newsroom files its names' own.

import { fixed, grouped, padded } from './format.ts';
import type { Name } from './names.ts';
import type { JavaRandom } from './random.ts';
import { DAY, type Wire } from './simulator.ts';
import { NEWS_STORY, TableRows } from './tables.ts';

const BROKERS = ['Halberd Securities', 'Northgate Capital', 'Ridgeway & Co', 'Pinecrest Research', 'Calder Partners', 'Ashby Markets'];
const FUNDS = ['Larkspur Capital', 'Greyfield Partners', 'Ostrander Value Fund', 'Kestrel Holdings'];
const FILING_ITEMS = ['results of operations', 'change in directors or officers', 'entry into a material agreement',
  'Regulation FD disclosure', 'other events'];
/** each sector's name on the wire, and why it moves: up, then down (the simulator's sectors, in the world's order) */
const GROUPS = ['Tech stocks', 'Drugmakers', 'Banks', 'Retailers', 'Media and telecom shares', 'Industrials',
  'Consumer staples', 'Energy shares', 'Utilities', 'Real estate shares', 'Miners'];
const RALLY = [
  ['AI spending forecasts are raised', 'the chip demand outlook improves'],
  ['a drug pricing plan is softened', 'the regulator speeds up approvals'],
  ['yields climb and the curve steepens', 'stress tests clear the way for buybacks'],
  ['consumer spending beats forecasts', 'the holiday sales outlook is raised'],
  ['advertising spending rebounds', 'spectrum auction rules are eased'],
  ['factory orders rise more than expected', 'an infrastructure bill clears its vote'],
  ['investors seek defensive shares', 'input costs fall'],
  ['oil rises on supply cuts', 'gas climbs on a cold forecast'],
  ['bond yields fall', 'power demand forecasts rise on data centres'],
  ['mortgage rates fall', 'office occupancy rises'],
  ['metal prices rally on China demand', 'copper hits a six-month high']];
const SLIDE = [
  ['export curbs on chips are widened', 'a bellwether\'s outlook disappoints'],
  ['a drug pricing proposal is tightened', 'insurers flag higher costs'],
  ['commercial property loan worries return', 'a regional lender cuts its outlook'],
  ['consumer confidence falls', 'tariffs on imports are raised'],
  ['the advertising outlook is cut', 'subscriber growth slows'],
  ['factory activity contracts', 'freight volumes fall'],
  ['input costs rise', 'a large grocer warns on margins'],
  ['oil falls on demand worries', 'inventories rise more than forecast'],
  ['bond yields rise', 'a regulator rejects a rate increase'],
  ['mortgage rates climb', 'office vacancies rise'],
  ['metal prices fall on weak China data', 'steel tariffs are lifted']];
const PRODUCTS = [
  ['an AI accelerator', 'a cloud platform', 'a new chip line'], ['a cancer drug', 'a diabetes treatment', 'a heart device'],
  ['a digital bank', 'a payments app', 'a wealth platform'], ['an electric SUV', 'a holiday collection', 'a store format'],
  ['a streaming tier', 'a 5G network', 'an ad platform'], ['a jet engine', 'a freight network', 'a robotics line'],
  ['a snack range', 'a beverage line', 'a store brand'], ['an offshore field', 'an LNG project', 'a refinery expansion'],
  ['a grid upgrade', 'a solar farm', 'a new rate plan'], ['an office tower', 'a logistics park', 'a data centre campus'],
  ['a copper mine', 'a lithium plant', 'a packaging line']];

/**
 * THE MARKET'S MOVES COME AT A DAY'S PACE, whatever the wire's (a busy terminal's rate writes more stories, not a more
 * violent market): a day has about this many macro shocks to the market, sector shocks to each sector, and nudges to
 * each name from its briefs; a story beyond them is one the market shrugs off.
 */
const MACRO_MOVES_A_DAY = 6;
const SECTOR_MOVES_A_DAY = 2;
const BRIEF_MOVES_A_NAME_A_DAY = 2;

/** a move's size as a headline writes it */
const pct = (r: number): string => `${fixed(Math.abs(Math.expm1(r) * 100), 1)}%`;
const price = (n: Name): string => `$${grouped(n.mid, n.mid >= 10 ? 2 : 4)}`;
/** the wire's score of a move: its share of `scale`, to a twentieth, within -1 and 1 */
const score = (r: number, scale: number): number => Math.round(Math.max(-1, Math.min(1, r / scale)) * 20) / 20;

export class Newsroom implements Wire {
  readonly random: JavaRandom;
  readonly equities: readonly Name[];
  readonly coins: readonly Name[];
  readonly sectorIds: readonly string[];
  /** each sector's largest names, the tickers a sector's story names */
  readonly leaders: readonly (readonly Name[])[];
  /** the equities' running weights, for a brief's name drawn by its share of the market's activity */
  readonly cumulative: readonly number[];
  /** stories a minute over the market, besides the jumps: macro, sector, briefs and crypto */
  readonly rate: number;
  count = 0;
  lastAt = 0;
  /** the stories since the last write: NEWS_STORY's rows */
  readonly rows = new TableRows(NEWS_STORY);

  constructor(random: JavaRandom, equities: readonly Name[], coins: readonly Name[], sectorIds: readonly string[], rate: number) {
    this.random = random;
    this.equities = equities;
    this.coins = coins;
    this.sectorIds = sectorIds;
    this.rate = rate;
    this.leaders = sectorIds.map((_, k) => equities.filter((e) => e.sectorIx === k).sort((a, b) => b.weight - a.weight).slice(0, 3));
    let sum = 0;
    this.cumulative = equities.map((e) => (sum += e.weight));
  }

  pick(xs: readonly string[]): string {
    return xs[this.random.nextInt(xs.length)]!;
  }

  target(n: Name, up: boolean): string {
    return `$${fixed(n.mid * (up ? 1.12 + 0.2 * this.random.nextDouble() : 0.88 - 0.15 * this.random.nextDouble()), 0)}`;
  }

  /** A story at `at` (ms), kept in time order (two in one instant a millisecond apart, so its id is its place). */
  story(at: number, head: string, text: string | null, src: string, cat: string, urg: string, sent: number,
    syms: string | null, reg: string | null, n: Name | null, sectorId: string | null): void {
    const when = at > this.lastAt ? at : this.lastAt + 1;
    this.lastAt = when;
    this.rows.add({
      STORY_ID: `NS-${padded(++this.count, 7)}`, PUBLISH_TIME: when, HEADLINE: head, BODY: text, SOURCE: src, CATEGORY: cat,
      URGENCY: urg, SENTIMENT: Number.isNaN(sent) ? null : sent, TICKERS: syms, REGION: reg,
      INSTRUMENT_ID: n?.instrumentId ?? null, SECTOR_ID: n !== null ? n.sectorId : sectorId,
    });
  }

  /** One step of `dt` seconds at `at`: the market's shock a macro story names (0 when none), a sector's added to its
   *  factor in `factors`, the briefs and (not in the morning, crypto's day being the feed's) a coin's brief. */
  beat(dt: number, at: number, factors: number[], morning: boolean): number {
    const perStory = dt * this.rate / 60 / 12;
    let market = 0;
    if (this.random.nextDouble() < perStory) market = this.macro(at, this.moves(MACRO_MOVES_A_DAY, this.rate / 12));
    if (this.random.nextDouble() < 1.5 * perStory) {
      const k = this.random.nextInt(GROUPS.length);
      factors[k] = factors[k]! + this.industry(k, at, this.moves(SECTOR_MOVES_A_DAY * GROUPS.length, 1.5 * this.rate / 12));
    }
    const briefs = 9 * perStory;
    const b = Math.floor(briefs) + (this.random.nextDouble() < briefs - Math.floor(briefs) ? 1 : 0);
    for (let i = 0; i < b; i++) this.brief(at, this.moves(BRIEF_MOVES_A_NAME_A_DAY * this.equities.length, 9 * this.rate / 12));
    if (!morning && this.coins.length > 0 && this.random.nextDouble() < 0.5 * perStory) this.crypto(at);
    return market;
  }

  /** Whether a story of a kind written `perMinute` a minute moves its price: so that a day has `perDay` of them. */
  moves(perDay: number, perMinute: number): boolean {
    return this.random.nextDouble() < perDay / Math.max(perDay, perMinute * DAY / 60);
  }

  /** A macro headline and its shock to the market (a log move), its sign the headline's -- or, `moves` false, one the
   *  market shrugs off. */
  macro(at: number, moves: boolean): number {
    const size = moves ? 0.0015 + 0.004 * this.random.nextDouble() * this.random.nextDouble() : 0;
    const up = this.random.nextBoolean();
    let head: string;
    let reg = 'Americas';
    switch (this.random.nextInt(7)) {
      case 0:
        head = up ? `US jobless claims fall to ${200 + this.random.nextInt(20)}K, fewer than forecast`
          : `US jobless claims rise to ${230 + this.random.nextInt(20)}K, the most in two months`;
        break;
      case 1: {
        const v = fixed(2.4 + this.random.nextDouble(), 1);
        head = up ? `US inflation cools to ${v}% annual pace, below estimates` : `US inflation runs hotter than forecast at ${v}% annual pace`;
        break;
      }
      case 2:
        head = up ? 'Fed official says rate cuts could come sooner than markets expect'
          : 'Fed minutes show officials wary of cutting rates too soon';
        break;
      case 3: {
        const v = fixed(3.9 + this.random.nextDouble() * 0.8, 2);
        head = up ? `Treasury yields slide after soft retail sales; 10-year at ${v}%` : `US 10-year yield climbs to ${v}%, highest in a month`;
        break;
      }
      case 4: {
        const v = fixed(1.5 + 2.5 * this.random.nextDouble(), 1);
        head = up ? `Oil falls ${v}% as supply concerns ease` : `Oil jumps ${v}% after a pipeline outage`;
        break;
      }
      case 5:
        head = up ? 'ECB signals a rate cut at its next meeting' : 'ECB holds rates and pushes back on cut bets';
        reg = 'EMEA';
        break;
      default:
        head = up ? 'China unveils stimulus for property and consumer spending' : 'China factory activity contracts for a third month';
        reg = 'APAC';
    }
    const shock = up ? size : -size;
    const text = moves ? `Stocks ${up ? 'rise' : 'fall'} broadly on the headline, the market about ${pct(shock)} ${up ? 'higher' : 'lower'}.`
      : 'Stocks are little changed on the headline.';
    this.story(at, head, text, 'Macro Desk', 'Macro', size >= 0.004 ? 'FLASH' : 'STORY', moves ? score(shock, 0.005) : 0,
      null, reg, null, null);
    return shock;
  }

  /** A sector's story and its shock to the sector's factor (a name takes 0.6 of it), its sign the headline's. */
  industry(k: number, at: number, moves: boolean): number {
    const size = moves ? 0.003 + 0.012 * this.random.nextDouble() * this.random.nextDouble() : 0;
    const up = this.random.nextBoolean();
    const move = 0.6 * size * (up ? 1 : -1);
    const head = GROUPS[k]! + (!moves ? ' little changed as ' : up ? ' rally as ' : ' slide as ') + this.pick(up ? RALLY[k]! : SLIDE[k]!);
    const lead = this.leaders[k]!;
    const most = lead.map((e) => `${e.name} (${e.sym})`).join(', ');
    const text = moves ? `The sector moves about ${pct(move)} ${up ? 'higher' : 'lower'}; ${most} among the most active.`
      : `The sector is little changed; ${most} among the most active.`;
    const syms = lead.map((e) => e.sym).join(' ');
    this.story(at, head, text, 'Market Wire', 'Industry', Math.abs(move) >= 0.006 ? 'FLASH' : 'STORY', score(move, 0.008),
      syms === '' ? null : syms, 'Americas', null, this.sectorIds[k]!);
    return size * (up ? 1 : -1);
  }

  /** A name's jump, reported at its instant, `n.mid` already moved: a story its size, a brief when it is small. */
  jump(n: Name, r: number, at: number): void {
    const up = r > 0;
    if (n.kind === 'CRYPTO') {
      this.story(at, `${n.name} ${up ? 'jumps' : 'slides'} ${pct(r)} as ${up ? 'fund inflows accelerate' : 'exchange outflows rise'}`,
        `${n.sym} trades at ${price(n)}.`, 'Crypto Desk', 'Crypto', Math.abs(r) >= 0.03 ? 'FLASH' : 'STORY', score(r, 0.05),
        n.sym, null, n, null);
      return;
    }
    if (Math.abs(r) < 0.005) {
      const b = this.pick(BROKERS);
      this.story(at, `${b} ${up ? 'raises' : 'cuts'} ${n.name} price target to ${this.target(n, up)}`,
        `${n.name} (${n.sym}) at ${price(n)}.`, 'Street Desk', 'Analyst', 'BRIEF', score(r, 0.04), n.sym, 'Americas', n, null);
      return;
    }
    const p = pct(r);
    const products = PRODUCTS[n.sectorIx]!;
    let cat: string;
    let head: string;
    switch (this.random.nextInt(8)) {
      case 0:
        cat = 'Earnings';
        head = up ? `${n.name} quarterly profit beats estimates; shares up ${p}`
          : `${n.name} misses quarterly estimates as margins narrow; shares down ${p}`;
        break;
      case 1:
        cat = 'Guidance';
        head = up ? `${n.name} raises its full-year outlook on strong demand` : `${n.name} cuts its full-year guidance, citing weaker orders`;
        break;
      case 2:
        cat = 'M&A';
        head = up ? `${n.name} jumps ${p} on a report of a takeover approach` : `${n.name} falls ${p} after agreeing a costly acquisition`;
        break;
      case 3: {
        cat = 'Analyst';
        const b = this.pick(BROKERS);
        head = up ? `${b} upgrades ${n.name} to buy, target ${this.target(n, up)}` : `${b} downgrades ${n.name} to sell, target ${this.target(n, up)}`;
        break;
      }
      case 4:
        cat = 'Regulatory';
        head = up ? `${n.name} wins regulatory approval for ${this.pick(products)}` : `Regulator opens a probe into ${n.name} -- filing`;
        break;
      case 5:
        cat = 'Management';
        head = up ? `${n.name} names a new chief executive; shares rise ${p}` : `${n.name}'s chief executive to step down`;
        break;
      default:
        cat = 'Product';
        head = up ? `${n.name} unveils ${this.pick(products)}; shares climb ${p}` : `${n.name} recalls ${this.pick(products)}; shares drop ${p}`;
    }
    this.story(at, head, `${n.name} (${n.sym}) shares ${up ? 'rose' : 'fell'} ${p} to ${price(n)}.`, 'Market Wire', cat,
      Math.abs(r) >= 0.03 ? 'FLASH' : 'STORY', score(r, 0.04), n.sym, 'Americas', n, null);
  }

  /**
   * A brief about a name drawn by its share of the market's activity: its nudge, a little its way, if it `moves` -- as
   * often down as up (a stake cut as a stake taken, a share offering as a buyback), so a busy name's briefs move it
   * about, never only up.
   */
  brief(at: number, moves: boolean): void {
    const i = binarySearch(this.cumulative, this.random.nextDouble() * this.cumulative[this.cumulative.length - 1]!);
    const n = this.equities[i < 0 ? Math.min(-i - 1, this.equities.length - 1) : i]!;
    let nudge = 0;
    const size = 0.001 + 0.002 * this.random.nextDouble();
    const up = this.random.nextBoolean();
    let head: string;
    let cat: string;
    let src = 'Market Wire';
    switch (this.random.nextInt(7)) {
      case 0:
        nudge = (up ? 1 : -1) * (0.002 + 0.003 * this.random.nextDouble());
        head = `${this.pick(BROKERS)} ${up ? 'raises' : 'cuts'} ${n.name} price target to ${this.target(n, up)}`;
        cat = 'Analyst';
        src = 'Street Desk';
        break;
      case 1: {
        const rating = this.random.nextInt(3);
        nudge = (rating - 1) * 0.002;
        head = `${this.pick(BROKERS)} starts ${n.name} at ${['sell', 'hold', 'buy'][rating]!}`;
        cat = 'Analyst';
        src = 'Street Desk';
        break;
      }
      case 2: {
        nudge = up ? size : -size;
        const fund = this.pick(FUNDS);
        head = up ? `${fund} discloses ${fixed(5 + 4 * this.random.nextDouble(), 1)}% stake in ${n.name} -- filing`
          : `${fund} cuts its stake in ${n.name} to ${fixed(1 + 3 * this.random.nextDouble(), 1)}% -- filing`;
        cat = 'Filing';
        src = 'Filings';
        break;
      }
      case 3:
        head = n.name + (this.random.nextBoolean() ? ' appoints a new chief financial officer' : ' adds two directors to its board');
        cat = 'Management';
        break;
      case 4:
        head = `${n.name} declares a quarterly dividend of $${fixed(n.mid * (0.002 + 0.006 * this.random.nextDouble()), 2)} a share`;
        cat = 'Dividend';
        break;
      case 5: {
        nudge = up ? size : -size;
        const bn = fixed(Math.max(0.1, n.shares * n.mid * (up ? 0.02 : 0.01) / 1e9), 1);
        head = up ? `${n.name} authorises a $${bn}bn share buyback` : `${n.name} prices a $${bn}bn share offering`;
        cat = up ? 'Management' : 'Capital Markets';
        break;
      }
      default:
        head = `${n.name} files 8-K: ${this.pick(FILING_ITEMS)}`;
        cat = 'Filing';
        src = 'Filings';
    }
    if (!moves) nudge = 0;
    if (nudge !== 0) n.mid = Math.max(n.tick, n.mid * Math.exp(nudge));
    this.story(at, head, `${n.name} (${n.sym}) at ${price(n)}.`, src, cat, 'BRIEF', nudge === 0 ? Number.NaN : score(nudge, 0.01),
      n.sym, 'Americas', n, null);
  }

  /** Where a coin stands against its prior close, from its price now: the feed's when live, else the simulator's. */
  crypto(at: number): void {
    const n = this.coins[Math.min(this.coins.length - 1, Math.floor(Math.pow(this.random.nextDouble(), 2) * this.coins.length))]!;
    const r = Math.log(n.mid / n.prevClose);
    this.story(at, `${n.name} at ${price(n)}, ${r >= 0 ? 'up' : 'down'} ${pct(r)} from its prior close`, null, 'Crypto Desk',
      'Crypto', 'BRIEF', score(r, 0.05), n.sym, null, n, null);
  }
}

/** Java's Arrays.binarySearch over ascending `xs`: the index of `key`, else -(its insertion point) - 1. */
function binarySearch(xs: readonly number[], key: number): number {
  let lo = 0;
  let hi = xs.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >>> 1;
    const v = xs[mid]!;
    if (v < key) lo = mid + 1;
    else if (v > key) hi = mid - 1;
    else return mid;
  }
  return -(lo + 1);
}
