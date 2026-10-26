// THE NAMES the world quotes and trades (market.groovy's Name, and its making): equities made up, ranked by size, each
// a sector's by its share of the market, and the coins Coinbase lists. Drawn as the server's world draws them, in the
// same order from the same random, so a seed's names are the server's names.

import type { JavaRandom } from './random.ts';

/** One name: what it is, and where its price, quote and day stand. */
export class Name {
  sym = '';
  name = '';
  sector = '';
  kind: 'EQUITY' | 'CRYPTO' = 'EQUITY';
  sectorIx = 0;
  prevClose = 0;
  shares = 0;
  tick = 0.01;
  /** the corpus's ids: the instrument's, the equity's (none for a coin), the sector's, the listing exchange's */
  instrumentId = '';
  equityId: string | null = null;
  sectorId = '';
  exchange: string | null = null;
  /** daily volatility, beta to the market, and the share of the market's activity it takes */
  vol = 0;
  beta = 1;
  weight = 0;
  spreadBps = 0;
  sizeMedian = 0;
  /** the price now, and the quote last published */
  mid = 0;
  bid = 0;
  ask = 0;
  bidSize = 0;
  askSize = 0;
  quoted = false;
  /** the price's move in the last step, which the aggressor's side leans with */
  drift = 0;
  /** the day so far: its first, highest, lowest and last print (none yet: NaN), and its volume */
  open = Number.NaN;
  high = Number.NaN;
  low = Number.NaN;
  last = Number.NaN;
  volume = 0;

  constructor(fields: Partial<Name>) {
    Object.assign(this, fields);
  }

  /** A print of this name: the day's figures moved by it. */
  printed(px: number, sz: number): void {
    if (Number.isNaN(this.open)) this.open = px;
    this.high = Number.isNaN(this.high) ? px : Math.max(this.high, px);
    this.low = Number.isNaN(this.low) ? px : Math.min(this.low, px);
    this.last = px;
    this.volume += sz;
  }
}

export const SECTORS = ['Technology', 'Health Care', 'Financials', 'Consumer Discretionary', 'Communication Services',
  'Industrials', 'Consumer Staples', 'Energy', 'Utilities', 'Real Estate', 'Materials'];
const SECTOR_SHARE = [0.22, 0.12, 0.13, 0.10, 0.08, 0.09, 0.06, 0.05, 0.05, 0.05, 0.05];
/** each sector's GICS code (Information Technology 45, Health Care 35, ...): its corpus id is SEC-<code> */
export const GICS = ['45', '35', '40', '25', '50', '20', '30', '10', '55', '60', '15'];
const SECTOR_WORDS = [['Systems', 'Software', 'Semiconductor', 'Networks', 'Cloud'], ['Therapeutics', 'Biosciences', 'Health', 'Medical', 'Pharma'],
  ['Bancorp', 'Financial', 'Capital', 'Insurance', 'Trust'], ['Retail', 'Motors', 'Brands', 'Apparel', 'Leisure'],
  ['Media', 'Telecom', 'Interactive', 'Broadcasting', 'Studios'], ['Industries', 'Aerospace', 'Machinery', 'Logistics', 'Rail'],
  ['Foods', 'Beverages', 'Household', 'Grocers', 'Farms'], ['Energy', 'Petroleum', 'Drilling', 'Pipelines', 'Refining'],
  ['Power', 'Electric', 'Water', 'Gas', 'Utilities'], ['Realty', 'Properties', 'Towers', 'Storage', 'Residential'],
  ['Materials', 'Chemicals', 'Mining', 'Steel', 'Packaging']];
const PREFIXES = ['Apex', 'Blue', 'Cedar', 'Delta', 'Ember', 'Frontier', 'Granite', 'Harbor', 'Iron', 'Juniper', 'Keystone',
  'Lumen', 'Meridian', 'North', 'Orion', 'Pioneer', 'Quarry', 'Ridge', 'Summit', 'Tidal', 'Union', 'Vantage',
  'Willow', 'Xeno', 'Yarrow', 'Zenith', 'Atlas', 'Beacon', 'Crest', 'Dawn', 'Echo', 'Falcon', 'Glacier', 'Horizon'];

/** `count` equities: each a sector by its share, a ticker of its own, ranked by size (the first the largest: a
 *  capitalisation from about 2 trillion down to 1 billion), its weights the market's activity's shares. */
export function equities(random: JavaRandom, count: number): Name[] {
  const used = new Set<string>();
  const out: Name[] = [];
  for (let i = 0; i < count; i++) {
    let u = random.nextDouble();
    let k = 0;
    while (k < SECTORS.length - 1 && (u -= SECTOR_SHARE[k]!) >= 0) k++;
    let sym: string;
    do {
      const len = random.nextDouble() < 0.6 ? 4 : 3;
      sym = '';
      for (let c = 0; c < len; c++) sym += String.fromCharCode(65 + random.nextInt(26));
    } while (used.has(sym) || !used.add(sym));
    const cap = 2.0e12 * Math.pow(i + 1, -1.15) + 1.0e9;
    const px = Math.max(3, Math.min(900, 60 * Math.exp(random.nextGaussian() * 0.9)));
    const name = `${PREFIXES[random.nextInt(PREFIXES.length)]!} ${SECTOR_WORDS[k]![random.nextInt(5)]!}`;
    const vol = 0.012 + 0.025 * Math.min(1, i / count) * Math.exp(random.nextGaussian() * 0.3);
    const beta = Math.max(0.3, 1.0 + random.nextGaussian() * 0.3);
    const n = new Name({
      sym, name, sector: SECTORS[k]!, kind: 'EQUITY', sectorIx: k, prevClose: Math.round(px * 100) / 100,
      shares: Math.round(cap / px), tick: 0.01, vol, beta, weight: Math.pow(i + 1, -0.9),
      spreadBps: 1.0 + 25 * Math.pow(i / count, 1.5), sizeMedian: 300 + 1500 * Math.pow(i + 1, -0.5),
    });
    // the day opens a little away from its prior close
    n.mid = Math.max(n.tick, n.prevClose * Math.exp(random.nextGaussian() * 0.008));
    n.instrumentId = `INST-${sym}`;
    n.equityId = `EQ-${sym}`;
    n.sectorId = `SEC-${GICS[k]!}`;
    n.exchange = i % 5 < 3 ? 'XNAS' : 'XNYS';
    out.push(n);
  }
  const total = out.reduce((s, n) => s + n.weight, 0);
  for (const n of out) n.weight /= total;
  return out;
}

/** Crypto: Coinbase Exchange's product ids, and a prior close each (the feed's 24 hours' open, where it answers). */
export const COINS: readonly (readonly [string, string, number])[] = [['BTC-USD', 'Bitcoin', 67000], ['ETH-USD', 'Ether', 2600],
  ['SOL-USD', 'Solana', 150], ['XRP-USD', 'XRP', 0.55], ['DOGE-USD', 'Dogecoin', 0.12], ['LTC-USD', 'Litecoin', 70],
  ['ADA-USD', 'Cardano', 0.35], ['AVAX-USD', 'Avalanche', 27]];

export function coins(random: JavaRandom): Name[] {
  return COINS.map(([sym, name, px], i) => {
    const n = new Name({
      sym, name, sector: 'Crypto', kind: 'CRYPTO', sectorIx: 0, prevClose: px, shares: 0, tick: px >= 10 ? 0.01 : 0.0001,
      vol: 0.035, beta: 1.5, weight: Math.pow(i + 1, -1.0) / 2.72, spreadBps: 1.5, sizeMedian: 3000 / px,
    });
    n.mid = px * Math.exp(random.nextGaussian() * 0.02);
    n.instrumentId = `INST-${sym}`;
    n.sectorId = 'SEC-CRYPTO';
    return n;
  });
}
