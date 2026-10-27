// Sample files to open, each one awkward in a different way.
//
// ONE definition, two consumers: the picker in the page offers these,
// and `bazel run //datacube:run_stress` runs every cube operation against every one of
// them. That coupling is the point -- an option that stopped working
// would fail the stress run rather than disappoint whoever picked it.
//
// The set here is deliberately the WORKING set. Shapes that expose
// open bugs (a quote inside a header, non-ASCII header names, a
// boolean compared to a string literal) live in the stress corpus
// only, so the suite keeps watching them without the page offering
// something that half-works.

import { DESK_TRADES, sampleTradesCsv } from './sample-trades.ts';

/** Deterministic, so the same choice gives the same file. */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** RFC 4180: quote when the value holds a comma, quote or newline. */
export function csvCell(value: string): string {
  return /[",\n\r]/.test(value)
    ? `"${value.replace(/"/g, '""')}"`
    : value;
}

function lines(n: number, f: (i: number) => string): string {
  const out: string[] = new Array(n);
  for (let i = 0; i < n; i++) out[i] = f(i);
  return out.join('\n');
}

export interface Sample {
  readonly id: string;
  /** Shown in the picker. */
  readonly label: string;
  /** What makes this one worth opening. */
  readonly about: string;
  /** Rows to generate when the caller has no preference. */
  readonly defaultRows: number;
  /** The file's format, and so its extension. Absent: CSV. */
  readonly format?: 'csv' | 'jsonl';
  build(rows: number, seed?: number): string;
}

/** The file name a sample is saved and opened under. */
export function sampleFileName(s: Sample): string {
  return `sample-${s.id}.${s.format ?? 'csv'}`;
}

const SKUS = [
  { sku: 'KB-01', category: 'Keyboards', price: 89.0 },
  { sku: 'KB-02', category: 'Keyboards', price: 149.0 },
  { sku: 'MS-01', category: 'Mice', price: 39.5 },
  { sku: 'MS-02', category: 'Mice', price: 74.0 },
  { sku: 'MN-27', category: 'Monitors', price: 329.0 },
  { sku: 'MN-32', category: 'Monitors', price: 499.0 },
  { sku: 'CB-USB', category: 'Cables', price: 12.0 },
  { sku: 'HS-01', category: 'Headsets', price: 119.0 },
];
const TAGS = ['gift', 'express', 'b2b', 'promo', 'returning'];
const TIERS = ['gold', 'silver', 'bronze'];
const DISCOUNTS = [{ code: 'SPRING', pct: 10 }, { code: 'LOYALTY', pct: 5 }, { code: 'BULK', pct: 15 }];
const COLORS = ['red', 'blue', 'black', 'white'];
const SIZES = ['S', 'M', 'L', 'XL'];
const CITIES = ['London', 'Paris', 'New York', 'Tokyo', 'Berlin'];
const CARRIERS = ['DHL', 'UPS', 'FedEx'];
const STATUSES = ['label_created', 'picked_up', 'in_transit', 'delivered'];
const CUSTOMERS = ['Acme', 'Globex', 'Initech', 'Umbrella', 'Hooli',
  'Stark', 'Wayne', 'Wonka'];

/**
 * Orders as newline-delimited JSON: flat fields beside a nested
 * object, an array of objects and an array of strings -- the three
 * shapes semi-structured data comes in -- nested inside one another
 * too: a customer's addresses (an array in an object), each line's
 * discounts and its attributes' sizes (arrays in array elements), and
 * shipments whose tracking events are an array of objects in an array
 * of objects. Each nested field arrives as a Variant column.
 */
export function sampleOrdersJsonl(
  options: { rows?: number; seed?: number } = {},
): string {
  const rows = Math.max(1, options.rows ?? 2000);
  const rand = rng(options.seed ?? 20260926);
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)]!;
  const out: string[] = new Array(rows);
  for (let i = 0; i < rows; i++) {
    const lines = Array.from({ length: 1 + Math.floor(rand() * 4) }, () => {
      const p = pick(SKUS);
      return { sku: p.sku, category: p.category,
        qty: 1 + Math.floor(rand() * 5), price: p.price,
        // an array of objects inside each element, sometimes empty
        discounts: DISCOUNTS.filter(() => rand() < 0.2),
        // an object inside each element, holding an array
        attributes: { color: pick(COLORS), sizes: SIZES.filter(() => rand() < 0.5) } };
    });
    const tags = TAGS.filter(() => rand() < 0.25);
    const month = String(1 + Math.floor(rand() * 12)).padStart(2, '0');
    const day = String(1 + Math.floor(rand() * 28)).padStart(2, '0');
    const name = pick(CUSTOMERS);
    const shipments = Array.from({ length: 1 + Math.floor(rand() * 2) }, (_, n) => ({
      carrier: pick(CARRIERS),
      // an array of objects inside an array of objects
      events: STATUSES.slice(0, 1 + Math.floor(rand() * STATUSES.length)).map((status, k) => ({
        status, at: `2025-${month}-${day}T${String(8 + n * 4 + k).padStart(2, '0')}:00:00`,
      })),
    }));
    out[i] = JSON.stringify({
      order_id: 5000 + i,
      region: REGIONS[i % REGIONS.length],
      placed_on: `2025-${month}-${day}`,
      customer: { name, tier: TIERS[name.length % TIERS.length],
        // Only some customers have a contact: a key that is sometimes
        // absent, which is what `get` returning nothing is for.
        ...(i % 3 === 0 ? { contact: { email: `${name.toLowerCase()}@example.com` } } : {}),
        // an array inside the object: billing always, shipping sometimes elsewhere
        addresses: [{ kind: 'billing', city: pick(CITIES) },
          ...(rand() < 0.5 ? [{ kind: 'shipping', city: pick(CITIES) }] : [])] },
      items: lines,
      tags,
      shipments,
      total: Number(lines.reduce((t, l) => t + l.qty * l.price, 0).toFixed(2)),
    });
  }
  return `${out.join('\n')}\n`;
}

const REGIONS = ['EMEA', 'APAC', 'AMER'];
const DESKS = ['Rates', 'Credit', 'FX', 'Equities', 'Commodities'];
const BOOKS = [
  'Alpha Fund', 'Smith, "Big" Fund', "O'Brien Holdings", 'Beta Fund',
];

export const SAMPLE_COLUMNS = [
  'trade_id', 'trade_date', 'booked_at', 'region', 'desk', 'book',
  'year', 'quarter', 'notional', 'pnl', 'quantity', 'settled',
] as const;

/** The default: a realistic trade blotter across every column type. */
export function sampleCsv(
  options: { rows?: number; seed?: number } = {},
): string {
  const rows = Math.max(1, options.rows ?? 5000);
  const rand = rng(options.seed ?? 20260920);
  const out: string[] = [SAMPLE_COLUMNS.join(',')];
  for (let i = 0; i < rows; i++) {
    const year = 2021 + (i % 5);
    const month = 1 + Math.floor(rand() * 12);
    const day = 1 + Math.floor(rand() * 28);
    const mm = String(month).padStart(2, '0');
    const dd = String(day).padStart(2, '0');
    out.push([
      String(100000 + i),
      `${year}-${mm}-${dd}`,
      `${year}-${mm}-${dd} ${String(Math.floor(rand() * 24)).padStart(2, '0')}`
        + `:${String(Math.floor(rand() * 60)).padStart(2, '0')}:00`,
      REGIONS[i % REGIONS.length]!,
      DESKS[Math.floor(rand() * DESKS.length)]!,
      BOOKS[Math.floor(rand() * BOOKS.length)]!,
      String(year),
      `Q${1 + Math.floor((month - 1) / 3)}`,
      (rand() * 1_000_000).toFixed(2),
      (rand() * 200_000 - 100_000).toFixed(2),
      String(1 + Math.floor(rand() * 500)),
      rand() < 0.8 ? 'true' : 'false',
    ].map(csvCell).join(','));
  }
  return `${out.join('\n')}\n`;
}

/** The sales sample's places: a region, a country (its ISO code and name), a city where it sells and where that is. */
const SALES_PLACES = [
  { region: 'EMEA', code: 'GBR', country: 'United Kingdom', city: 'London', lat: 51.51, lon: -0.13, weight: 5 },
  { region: 'EMEA', code: 'DEU', country: 'Germany', city: 'Berlin', lat: 52.52, lon: 13.4, weight: 4 },
  { region: 'EMEA', code: 'FRA', country: 'France', city: 'Paris', lat: 48.86, lon: 2.35, weight: 3 },
  { region: 'AMER', code: 'USA', country: 'United States', city: 'New York', lat: 40.71, lon: -74.01, weight: 8 },
  { region: 'AMER', code: 'CAN', country: 'Canada', city: 'Toronto', lat: 43.65, lon: -79.38, weight: 2 },
  { region: 'AMER', code: 'BRA', country: 'Brazil', city: 'Sao Paulo', lat: -23.55, lon: -46.63, weight: 2 },
  { region: 'APAC', code: 'JPN', country: 'Japan', city: 'Tokyo', lat: 35.68, lon: 139.69, weight: 4 },
  { region: 'APAC', code: 'AUS', country: 'Australia', city: 'Sydney', lat: -33.87, lon: 151.21, weight: 2 },
  { region: 'APAC', code: 'IND', country: 'India', city: 'Mumbai', lat: 19.08, lon: 72.88, weight: 3 },
  { region: 'APAC', code: 'SGP', country: 'Singapore', city: 'Singapore', lat: 1.35, lon: 103.82, weight: 1 },
] as const;

/** What it sells: a category, a product, its price and the share of its price that is cost. */
const SALES_PRODUCTS = [
  { category: 'Hardware', product: 'Laptop', price: 1400, cost: 0.78 },
  { category: 'Hardware', product: 'Monitor', price: 380, cost: 0.72 },
  { category: 'Hardware', product: 'Dock', price: 220, cost: 0.65 },
  { category: 'Software', product: 'Suite', price: 600, cost: 0.2 },
  { category: 'Software', product: 'Cloud', price: 90, cost: 0.35 },
  { category: 'Software', product: 'Support', price: 300, cost: 0.45 },
  { category: 'Services', product: 'Consulting', price: 2400, cost: 0.62 },
  { category: 'Services', product: 'Training', price: 800, cost: 0.5 },
] as const;

const SALES_CHANNELS = ['Online', 'Retail', 'Partner'] as const;

/** The sales sample's columns, in order. */
export const SALES_COLUMNS = [
  'order_id', 'sold', 'region', 'country_code', 'country', 'city', 'lat', 'lon', 'channel', 'category', 'product',
  'units', 'revenue', 'cost', 'profit', 'discount', 'target',
] as const;

/**
 * A SALES LEDGER for dashboards (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §3.3): two years of orders, day by day, in
 * ten countries of three regions (each with its ISO code, a city and where that is, for maps), through three channels,
 * of eight products in three categories -- with a trend (growth through the two years), a season (a strong fourth
 * quarter) and a target per order, so a chart has something to say. Every amount has two places (a DOUBLE to DuckDB:
 * a Float), units and ids are whole (an Integer), the day a date (a StrictDate).
 */
export function salesCsv(rows = 6000, seed = 20261014): string {
  const rand = rng(seed);
  const places = SALES_PLACES.flatMap((p) => Array.from({ length: p.weight }, () => p));
  const days = 731;
  const out: string[] = [SALES_COLUMNS.join(',')];
  for (let i = 0; i < rows; i++) {
    const day = Math.floor((i / rows) * days);
    const date = new Date(Date.UTC(2024, 0, 1 + day));
    const month = date.getUTCMonth();
    const place = places[Math.floor(rand() * places.length)]!;
    const product = SALES_PRODUCTS[Math.floor(rand() * SALES_PRODUCTS.length)]!;
    const channel = SALES_CHANNELS[Math.floor(rand() * SALES_CHANNELS.length)]!;
    const growth = 1 + day / days * 0.35;
    const season = month >= 9 ? 1.4 : month <= 1 ? 0.85 : 1;
    const units = 1 + Math.floor(rand() * 12 * season);
    const discount = [0, 0, 0.05, 0.1, 0.15, 0.2][Math.floor(rand() * 6)]!;
    const revenue = units * product.price * growth * (1 - discount);
    const cost = revenue * (product.cost + (rand() - 0.5) * 0.1 + discount * 0.5);
    const target = units * product.price * 1.05;
    out.push([
      String(1000000 + i),
      date.toISOString().slice(0, 10),
      place.region, place.code, place.country, place.city,
      place.lat.toFixed(2), place.lon.toFixed(2),
      channel, product.category, product.product,
      String(units),
      revenue.toFixed(2), cost.toFixed(2), (revenue - cost).toFixed(2),
      discount.toFixed(2), target.toFixed(2),
    ].map(csvCell).join(','));
  }
  return `${out.join('\n')}\n`;
}

/**
 * The countries the sales sample sells in, by ISO code -- less Singapore, which it sells in, and with Mexico and South
 * Africa, where it does not -- so each way of joining the orders to them has rows to show (examples/prep-joins.ts): the
 * orders with no country, the countries with no orders.
 */
const COUNTRIES = [
  { code: 'GBR', country: 'United Kingdom', continent: 'Europe', capital: 'London', population: 68.3 },
  { code: 'DEU', country: 'Germany', continent: 'Europe', capital: 'Berlin', population: 84.5 },
  { code: 'FRA', country: 'France', continent: 'Europe', capital: 'Paris', population: 68.2 },
  { code: 'USA', country: 'United States', continent: 'North America', capital: 'Washington', population: 335.0 },
  { code: 'CAN', country: 'Canada', continent: 'North America', capital: 'Ottawa', population: 40.1 },
  { code: 'MEX', country: 'Mexico', continent: 'North America', capital: 'Mexico City', population: 129.7 },
  { code: 'BRA', country: 'Brazil', continent: 'South America', capital: 'Brasilia', population: 211.1 },
  { code: 'JPN', country: 'Japan', continent: 'Asia', capital: 'Tokyo', population: 124.0 },
  { code: 'IND', country: 'India', continent: 'Asia', capital: 'New Delhi', population: 1438.1 },
  { code: 'AUS', country: 'Australia', continent: 'Oceania', capital: 'Canberra', population: 26.9 },
  { code: 'ZAF', country: 'South Africa', continent: 'Africa', capital: 'Pretoria', population: 63.0 },
] as const;

/** The countries sample's columns, in order. */
export const COUNTRY_COLUMNS = ['code', 'country', 'continent', 'capital', 'population_m'] as const;

/** The countries sample: a row per country, its population in millions (a DOUBLE to DuckDB: a Float). */
export function countriesCsv(): string {
  const out = [COUNTRY_COLUMNS.join(','),
    ...COUNTRIES.map((c) => [c.code, c.country, c.continent, c.capital, c.population.toFixed(1)].map(csvCell).join(','))];
  return `${out.join('\n')}\n`;
}

/** The sample a table of countries is: the join example's second table. */
export const COUNTRIES_SAMPLE = 'countries';

/** The business days (Monday to Friday) from `from` to `to`, ISO days. */
function businessDays(from: string, to: string): string[] {
  const out: string[] = [];
  for (let d = new Date(`${from}T00:00:00Z`); d <= new Date(`${to}T00:00:00Z`); d = new Date(d.getTime() + 86_400_000)) {
    if (d.getUTCDay() !== 0 && d.getUTCDay() !== 6) out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

/** A standard normal draw (Box-Muller) from a uniform source. */
function normal(rand: () => number): number {
  return Math.sqrt(-2 * Math.log(1 - rand())) * Math.cos(2 * Math.PI * rand());
}

/** The markets sample's instruments: each its asset class, its first close, its drift and volatility a year, its beta
 *  to the market and its usual daily volume. */
const INSTRUMENTS = [
  { symbol: 'SPX', asset: 'Index', start: 3250, drift: 0.15, vol: 0.15, beta: 1, volume: 2.5e9 },
  { symbol: 'NDX', asset: 'Index', start: 8800, drift: 0.2, vol: 0.2, beta: 1.15, volume: 1.2e9 },
  { symbol: 'AAPL', asset: 'Equity', start: 75, drift: 0.28, vol: 0.27, beta: 1.15, volume: 9e7 },
  { symbol: 'MSFT', asset: 'Equity', start: 160, drift: 0.26, vol: 0.25, beta: 1.05, volume: 3e7 },
  { symbol: 'NVDA', asset: 'Equity', start: 6, drift: 0.85, vol: 0.5, beta: 1.6, volume: 4e8 },
  { symbol: 'JPM', asset: 'Equity', start: 140, drift: 0.14, vol: 0.25, beta: 1.1, volume: 1e7 },
  { symbol: 'XOM', asset: 'Equity', start: 70, drift: 0.14, vol: 0.3, beta: 0.8, volume: 2e7 },
  { symbol: 'GLD', asset: 'Commodity', start: 145, drift: 0.1, vol: 0.14, beta: 0.05, volume: 8e6 },
  { symbol: 'TLT', asset: 'Bond', start: 136, drift: -0.05, vol: 0.16, beta: -0.25, volume: 1.5e7 },
  { symbol: 'EURUSD', asset: 'FX', start: 1.12, drift: -0.005, vol: 0.07, beta: 0.15, volume: 0 },
] as const;

/** The markets sample's columns, in order. */
export const MARKET_COLUMNS = ['symbol', 'asset_class', 'date', 'open', 'high', 'low', 'close', 'volume'] as const;

/**
 * DAILY PRICES for a timeseries tile (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md): five years of business days of
 * ten instruments -- indices, equities, gold, a bond fund, a currency -- each an open, a high, a low, a close and a
 * volume, moved by one market and its own noise (so they correlate as a market does), through a crash in the spring of
 * 2020 and a bear market in 2022. Prices have two places (four for a currency; a DOUBLE to DuckDB: a Float), the volume
 * is whole (an Integer), the date a date (a StrictDate). `rows`: the rows asked for, divided among the instruments, its
 * days counted back from the last.
 */
export function marketsCsv(rows = 13_050, seed = 20261058): string {
  const rand = rng(seed);
  const all = businessDays('2020-01-02', '2024-12-31');
  const days = all.slice(Math.max(0, all.length - Math.max(1, Math.ceil(rows / INSTRUMENTS.length))));
  const close: number[] = INSTRUMENTS.map((x) => x.start);
  const out: string[] = [MARKET_COLUMNS.join(',')];
  for (const day of days) {
    // the market's day: a crash (Feb-Mar 2020), its rebound, a bear market (2022), and calm otherwise
    const regime = day >= '2020-02-20' && day <= '2020-03-23' ? { mu: -0.012, sigma: 2.8 }
      : day > '2020-03-23' && day <= '2020-06-30' ? { mu: 0.004, sigma: 1.8 }
        : day >= '2022-01-03' && day <= '2022-10-12' ? { mu: -0.0011, sigma: 1.5 } : { mu: 0, sigma: 1 };
    const market = normal(rand) * regime.sigma;
    INSTRUMENTS.forEach((x, i) => {
      const daily = x.vol / Math.sqrt(252);
      const shock = x.beta * market * 0.7 + normal(rand) * Math.sqrt(Math.max(0.2, 1 - 0.49 * x.beta * x.beta)) * Math.min(regime.sigma, 1.6);
      const open = close[i]! * Math.exp(normal(rand) * daily * 0.3);
      const next = close[i]! * Math.exp(x.drift / 252 - daily * daily / 2 + daily * shock + regime.mu * x.beta);
      const high = Math.max(open, next) * (1 + Math.abs(normal(rand)) * daily * 0.5);
      const low = Math.min(open, next) * (1 - Math.abs(normal(rand)) * daily * 0.5);
      close[i] = next;
      const places = x.asset === 'FX' ? 4 : 2;
      const volume = x.volume === 0 ? 0 : Math.round(x.volume * (0.7 + 0.3 * Math.abs(shock)) * (0.85 + rand() * 0.3));
      out.push([x.symbol, x.asset, day, open.toFixed(places), high.toFixed(places), low.toFixed(places), next.toFixed(places), String(volume)]
        .map(csvCell).join(','));
    });
  }
  return `${out.join('\n')}\n`;
}

/** The rates sample's tenors, each its years and how far its yield moves with the front of the curve. */
const TENORS = [
  { tenor: '3M', years: 0.25, front: 1 }, { tenor: '2Y', years: 2, front: 0.85 }, { tenor: '5Y', years: 5, front: 0.65 },
  { tenor: '10Y', years: 10, front: 0.5 }, { tenor: '30Y', years: 30, front: 0.38 },
] as const;

/** The rates sample's columns, in order. */
export const RATE_COLUMNS = ['tenor', 'years', 'date', 'yield'] as const;

/**
 * A TREASURY CURVE, day by day, beside the markets sample: five tenors' yields in percent over the same business days
 * -- near zero through 2020 and 2021, rising through 2022 and 2023 with the front faster than the back (the curve
 * inverted), easing in 2024 -- so a timeseries reads two grids: a spread (10Y less 2Y), a bond fund against its yield.
 */
export function ratesCsv(rows = 6525, seed = 20261026): string {
  const rand = rng(seed);
  const all = businessDays('2020-01-02', '2024-12-31');
  const days = all.slice(Math.max(0, all.length - Math.max(1, Math.ceil(rows / TENORS.length))));
  // the policy rate's path: its level on each date, by straight lines between turns
  const turns: readonly (readonly [string, number])[] = [
    ['2020-01-02', 1.55], ['2020-03-16', 0.08], ['2022-03-01', 0.1], ['2022-12-15', 4.3], ['2023-07-27', 5.33], ['2024-09-18', 5.33], ['2024-12-31', 4.35],
  ];
  const policy = (day: string): number => {
    const t = Date.parse(day);
    for (let i = 1; i < turns.length; i++) {
      const [a, va] = turns[i - 1]!;
      const [b, vb] = turns[i]!;
      if (day <= b) return va + (vb - va) * (t - Date.parse(a)) / Math.max(1, Date.parse(b) - Date.parse(a));
    }
    return turns.at(-1)![1];
  };
  const noise: number[] = TENORS.map(() => 0);
  const out: string[] = [RATE_COLUMNS.join(',')];
  for (const day of days) {
    const level = policy(day);
    const common = normal(rand) * 0.04;
    TENORS.forEach((x, i) => {
      // a term premium that grows with the tenor, the front's level carried less far out, and a slow wander of its own
      noise[i] = noise[i]! * 0.98 + common + normal(rand) * 0.02;
      const y = Math.max(0.01, level * x.front + (1 - x.front) * 2.6 + 0.12 * Math.log1p(x.years) + noise[i]!);
      out.push([x.tenor, String(x.years), day, y.toFixed(3)].map(csvCell).join(','));
    });
  }
  return `${out.join('\n')}\n`;
}

export const SAMPLES: readonly Sample[] = [
  {
    id: 'trades',
    label: 'Trades — a bit of everything',
    about: 'Dates, a timestamp, a boolean, key-like integers, measures, '
      + 'and a book name containing a comma and a quoted phrase.',
    defaultRows: 5000,
    build: (rows, seed) =>
      sampleCsv(seed === undefined ? { rows } : { rows, seed }),
  },
  {
    // the rows the demo starts on (sample-trades.ts), as a file: what a page saved over its start's grid rebuilds
    id: DESK_TRADES,
    label: 'Desk trades -- the rows DataCube starts on',
    about: 'Regions, desks, books, years and quarters, with a notional, a P&L and a quantity: '
      + 'the trades the start page groups and pivots, as a file.',
    defaultRows: 5000,
    build: (rows) => sampleTradesCsv(rows),
  },
  {
    id: 'sales',
    label: 'Sales -- for dashboards',
    about: 'Two years of orders in ten countries: dates, regions, ISO codes and coordinates, channels, products, '
      + 'units, revenue, cost, profit, discount and a target. What the chart examples are drawn from.',
    defaultRows: 6000,
    build: (rows, seed) => salesCsv(rows, seed),
  },
  {
    id: 'markets',
    label: 'Markets -- daily prices, for timeseries',
    about: 'Five years of business days of ten instruments (indices, equities, gold, a bond fund, a currency): an open, '
      + 'high, low, close and volume each, through a crash and a bear market. What a timeseries tile reads.',
    defaultRows: 13_050,
    build: (rows, seed) => marketsCsv(rows, seed),
  },
  {
    id: 'rates',
    label: 'Rates -- a Treasury curve, day by day',
    about: 'Five tenors\' yields over the markets sample\'s days, rising and inverting through 2022: a second grid for a '
      + 'timeseries to read beside the prices (a spread, a bond fund against its yield).',
    defaultRows: 6525,
    build: (rows, seed) => ratesCsv(rows, seed),
  },
  {
    // the sales sample's countries: a table to join its orders to (examples/prep-joins.ts)
    id: COUNTRIES_SAMPLE,
    label: 'Countries -- to join the sales to',
    about: 'Eleven countries by ISO code, with their continent, capital and population: nine the sales sample sells in, '
      + 'two it does not, and one it sells in left out -- so joining the two leaves rows unmatched on both sides.',
    defaultRows: COUNTRIES.length,
    build: () => countriesCsv(),
  },
  {
    id: 'types',
    label: 'Every column type',
    about: 'One column per type DuckDB sniffs: integer, bigint, double, '
      + 'decimal, boolean, date and timestamp.',
    defaultRows: 2000,
    build: (rows) => 'name,n_int,n_big,n_dbl,n_dec,flag,d,ts\n'
      + lines(rows, (i) =>
        `row${i % 7},${i},${9007199254740000 + i},${(i / 7).toFixed(4)},`
        + `${(i / 100).toFixed(2)},${i % 2 === 0},`
        + `2021-${String(1 + (i % 12)).padStart(2, '0')}-`
        + `${String(1 + (i % 28)).padStart(2, '0')},`
        + `2021-${String(1 + (i % 12)).padStart(2, '0')}-`
        + `${String(1 + (i % 28)).padStart(2, '0')} `
        + `${String(i % 24).padStart(2, '0')}:30:00`) + '\n',
  },
  {
    id: 'nulls',
    label: 'Missing values everywhere',
    about: 'Empty fields in every column, plus one column that is '
      + 'entirely null. Aggregates have to skip them.',
    defaultRows: 3000,
    build: (rows) => 'k,v,always_null,sometimes\n'
      + lines(rows, (i) => `${i % 5 === 0 ? '' : `k${i % 5}`},`
        + `${i % 3 === 0 ? '' : i},,${i % 2 ? '' : 'x'}`) + '\n',
  },
  {
    id: 'dates',
    label: 'Dates and timestamps',
    about: 'ISO dates, ISO timestamps, and a slash-formatted column '
      + 'DuckDB reads as text rather than a date.',
    defaultRows: 3000,
    build: (rows) => 'iso,stamp,slashy,n\n'
      + lines(rows, (i) => {
        const mm = String(1 + (i % 12)).padStart(2, '0');
        return `2021-${mm}-15,2021-${mm}-15T${String(i % 24)
          .padStart(2, '0')}:00:00,${mm}/15/2021,${i}`;
      }) + '\n',
  },
  {
    id: 'wide',
    label: 'Wide — 60 columns',
    about: 'A wide select and a wide group-by, for the column panel '
      + 'and the horizontal scroll.',
    defaultRows: 2000,
    build: (rows) => Array.from({ length: 60 }, (_, c) => `c${c}`).join(',')
      + '\n' + lines(rows, (i) => Array.from({ length: 60 },
        (_, c) => (c % 3 === 0 ? `s${i % 4}` : String(i * c))).join(',')) + '\n',
  },
  {
    id: 'pivot',
    label: 'Wide pivot — 250 distinct values',
    about: 'A column with 250 distinct values, to pivot on and watch '
      + 'the header build.',
    defaultRows: 20000,
    build: (rows) => 'grp,piv,val\n'
      + lines(rows, (i) => `g${i % 3},p${i % 250},${i}`) + '\n',
  },
  {
    id: 'tall',
    label: 'Tall — lots of rows',
    about: 'Few columns, many rows: the data cost rather than the '
      + 'planning cost.',
    defaultRows: 200000,
    build: (rows) => 'region,desk,amount\n'
      + lines(rows, (i) => `r${i % 5},d${i % 11},${(i % 997) / 7}`) + '\n',
  },
  {
    id: 'awkward-headers',
    label: 'Awkward column names',
    about: 'Headers with spaces, commas, SQL keywords and a leading '
      + 'digit. All of these quote correctly.',
    defaultRows: 2000,
    build: (rows) => '"total pnl",select,from,2024,"x,y",  padded  \n'
      + lines(rows, (i) => `${i}.5,s${i % 3},f${i % 2},${2020 + (i % 4)},`
        + `"c,${i}",p${i % 2}`) + '\n',
  },
  {
    id: 'hostile-values',
    label: 'Hostile values',
    about: "Values that look like SQL and like Pure: a DROP TABLE, an "
      + "OR '1'='1', backslashes, quotes and LIKE wildcards.",
    defaultRows: 2000,
    build: (rows) => 'label,amount\n'
      + `"Robert'); DROP TABLE x;--",1\n`
      + `"a' OR '1'='1",2\n`
      + `"back\\slash",3\n`
      + `"quote""inside",4\n`
      + `"%wildcard_",5\n`
      + lines(Math.max(0, rows - 5), (i) => `ok${i},${i}`) + '\n',
  },
  {
    id: 'extremes',
    label: 'Numbers at the edges',
    about: 'Int64 limits, 1e300, 1e-300, negative zero. Some arithmetic '
      + 'on these legitimately overflows.',
    defaultRows: 2000,
    build: (rows) => 'k,big,small,neg,sci\n'
      + 'a,9223372036854775807,0.000000001,-0.0,1e300\n'
      + 'b,-9223372036854775808,1e-300,-99999999.99,-1e300\n'
      + 'c,0,0,0,0\n'
      + lines(Math.max(0, rows - 3),
        (i) => `d${i % 4},${i},${i / 1000},-${i},${i}e2`) + '\n',
  },
  {
    id: 'quoting',
    label: 'Quoting and whitespace',
    about: 'Embedded newlines inside quoted fields, padded values, and '
      + 'mixed case for the case-insensitive filters.',
    defaultRows: 2000,
    build: (rows) => 'note,label,n\n'
      + '"line one\nline two",MiXeD,1\n'
      + '"another\n\nwith a blank",  padded  ,2\n'
      + lines(Math.max(0, rows - 2), (i) =>
        `plain${i},${['  padded  ', 'MiXeD', 'lower', 'UPPER'][i % 4]},`
        + `${i + 3}`) + '\n',
  },
  {
    id: 'semicolons',
    label: 'Semicolon-delimited',
    about: 'Not a comma in sight. DuckDB has to sniff the delimiter.',
    defaultRows: 2000,
    build: (rows) => 'k;v;w\n'
      + lines(rows, (i) => `k${i % 4};${i};${i * 2}`) + '\n',
  },
  {
    id: 'crlf-bom',
    label: 'Windows line endings and a BOM',
    about: 'CRLF throughout and a UTF-8 byte-order mark, which can end '
      + 'up inside the first column name.',
    defaultRows: 2000,
    build: (rows) => '﻿k,v\r\n'
      + lines(rows, (i) => `k${i % 4},${i}`).replace(/\n/g, '\r\n') + '\r\n',
  },
  {
    id: 'mixed-column',
    label: 'A column that changes its mind',
    about: 'Numeric for most rows and then not, so DuckDB widens the '
      + 'whole column to text.',
    defaultRows: 2000,
    build: (rows) => 'k,maybe_num\n'
      + lines(Math.max(0, rows - 1), (i) => `k${i % 3},${i}`)
      + '\nk0,not-a-number\n',
  },
  {
    id: 'orders-json',
    label: 'Orders — nested JSON',
    about: 'Newline-delimited JSON: a customer object (with an array of '
      + 'addresses), line items (each with discounts and attributes), tags '
      + 'and shipments (each with tracking events) beside flat fields. The '
      + 'nested ones arrive as Variant columns.',
    defaultRows: 2000,
    format: 'jsonl',
    build: (rows, seed) =>
      sampleOrdersJsonl(seed === undefined ? { rows } : { rows, seed }),
  },
  {
    id: 'one-row',
    label: 'A single row',
    about: 'Every aggregate over exactly one value, and a grid with '
      + 'nothing to scroll.',
    defaultRows: 1,
    build: () => 'a,b,c\nx,1,2.5\n',
  },
];

export function sampleById(id: string): Sample | undefined {
  return SAMPLES.find((s) => s.id === id);
}
