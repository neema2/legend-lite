// THE MARKETS SAMPLE IN THE TRADING MODEL'S OWN TABLES (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11, Phase 6):
// the markets sample's days (samples.ts marketBars) written into the tables of the stress corpus's store (store::DB),
// under their own names and in their own columns -- EXCHANGE, SECTOR, CURRENCY and INSTRUMENT, a MARKET_QUOTE a day of
// each instrument (its open, high, low and close, its volume, its time the close), and TRADE, the bank's buys and sells
// on them -- so a page reads them through the corpus's classes (marketdata::MarketQuote, products::Instrument,
// trading::Trade) and their associations, by stress::AllMapping. The same prices as the markets sample, value for value:
// a series over the class is the series over the table. Each table a CSV text, every column its store declares in its
// order, a value of its type wherever the store's class has one (DuckDB's own reader types them), none where the class
// has none. Deterministic: the same call, the same text.

import { macroCsv, marketBars, ratesCsv, type MarketBar } from './samples.ts';

/** Each table's columns, in the order the store declares them (core/src/test/resources/stress/30-store.pure). */
export const STRESS_MARKET_COLUMNS = {
  EXCHANGE: ['MIC', 'NAME', 'CITY', 'TIMEZONE', 'OPEN_TIME', 'CLOSE_TIME', 'IS_ELECTRONIC', 'OPERATING_MIC', 'WEBSITE',
    'REGULATORY_BODY', 'COUNTRY_ID'],
  SECTOR: ['SECTOR_ID', 'NAME', 'GICS_CODE', 'LEVEL', 'DESCRIPTION', 'IS_ACTIVE', 'PARENT_SECTOR_ID', 'MARKET_CAP_WEIGHT',
    'NUMBER_OF_COMPANIES', 'LAST_UPDATED'],
  CURRENCY: ['CODE', 'NAME', 'NUMERIC_CODE', 'MINOR_UNIT', 'SYMBOL', 'IS_ACTIVE', 'REGION', 'IS_MAJOR', 'CENTRAL_BANK',
    'FIXING_SOURCE', 'COUNTRY_ID'],
  INSTRUMENT: ['INSTRUMENT_ID', 'TICKER', 'NAME', 'ISIN', 'CUSIP', 'SEDOL', 'ASSET_CLASS', 'PRODUCT_TYPE', 'IS_ACTIVE',
    'LISTING_DATE', 'MATURITY_DATE', 'NOTIONAL_AMOUNT', 'CURRENCY', 'DESCRIPTION', 'ISSUE_DATE', 'EQUITY_ID', 'BOND_ID',
    'FUTURE_ID', 'FX_FORWARD_ID', 'SWAP_ID', 'CDS_ID', 'STRUCTURED_PRODUCT_ID', 'ETF_ID', 'CURRENCY_CODE', 'EXCHANGE_MIC',
    'SECTOR_ID'],
  MARKET_QUOTE: ['QUOTE_ID', 'QUOTE_TIME', 'BID', 'ASK', 'MID', 'LAST', 'VOLUME', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE',
    'CLOSE_PRICE', 'SOURCE', 'IS_DELAYED', 'SNAPSHOT_ID', 'INSTRUMENT_ID'],
  TRADE: ['TRADE_ID', 'TRADE_DATE', 'SETTLEMENT_DATE', 'QUANTITY', 'PRICE', 'NOTIONAL', 'SIDE', 'STATUS', 'TRADE_TYPE',
    'CURRENCY', 'COMMISSION', 'FEES', 'EXECUTION_VENUE', 'IS_BLOCK', 'CREATED_TIME', 'LAST_MODIFIED_TIME', 'ORDER_ID',
    'BLOCK_ID', 'AVG_PRICE_TRADE_ID', 'BOOK_ID', 'COUNTERPARTY_ID', 'INSTRUMENT_ID', 'TRADER_ID'],
  // the rates sample's Treasury curve, a point a tenor a day (curves::CurvePoint, the store's RATE_CURVE_POINT), and the
  // macro sample's series (markets::MacroObservation, the project's own markets-model.pure): the copies over classes
  // (src/examples/on-classes.ts; docs/TS_DECISIONS_2026_11_07.md §4, 2026-11-07)
  RATE_CURVE_POINT: ['CURVE_ID', 'COB_DATE', 'TENOR_LABEL', 'TENOR_DAYS', 'ZERO_RATE', 'DISCOUNT_FACTOR', 'IS_INTERPOLATED',
    'SOURCE_SERIES_ID', 'CURVE_NAME', 'CURVE_CURRENCY', 'CURVE_TYPE', 'CURVE_BENCHMARK_ID'],
  MACRO_SERIES: ['SERIES', 'OBS_DATE', 'VALUE'],
} as const;

/** The tables, in the order a page reads them in (each one's references before it). */
export type StressMarketTable = keyof typeof STRESS_MARKET_COLUMNS;
export const STRESS_MARKET_TABLES: readonly StressMarketTable[] = ['EXCHANGE', 'SECTOR', 'CURRENCY', 'INSTRUMENT', 'MARKET_QUOTE', 'TRADE',
  'RATE_CURVE_POINT', 'MACRO_SERIES'];

/** A row: each column's value as the CSV writes it, none for a column not named. */
type Row = Readonly<Record<string, string | number | boolean>>;

const EXCHANGES: readonly Row[] = [
  { MIC: 'XNAS', NAME: 'Nasdaq', CITY: 'New York', TIMEZONE: 'America/New_York', OPEN_TIME: '09:30', CLOSE_TIME: '16:00', IS_ELECTRONIC: true, OPERATING_MIC: 'XNAS', REGULATORY_BODY: 'SEC' },
  { MIC: 'XNYS', NAME: 'New York Stock Exchange', CITY: 'New York', TIMEZONE: 'America/New_York', OPEN_TIME: '09:30', CLOSE_TIME: '16:00', IS_ELECTRONIC: false, OPERATING_MIC: 'XNYS', REGULATORY_BODY: 'SEC' },
  { MIC: 'ARCX', NAME: 'NYSE Arca', CITY: 'New York', TIMEZONE: 'America/New_York', OPEN_TIME: '09:30', CLOSE_TIME: '16:00', IS_ELECTRONIC: true, OPERATING_MIC: 'XNYS', REGULATORY_BODY: 'SEC' },
  { MIC: 'XCBO', NAME: 'Cboe Options Exchange', CITY: 'Chicago', TIMEZONE: 'America/Chicago', OPEN_TIME: '08:30', CLOSE_TIME: '15:15', IS_ELECTRONIC: true, OPERATING_MIC: 'XCBO', REGULATORY_BODY: 'SEC' },
  { MIC: 'XOFF', NAME: 'Off exchange', CITY: 'London', TIMEZONE: 'Europe/London', OPEN_TIME: '00:00', CLOSE_TIME: '23:59', IS_ELECTRONIC: true },
];

const SECTORS: readonly Row[] = [
  { SECTOR_ID: 'S-IT', NAME: 'Information Technology', GICS_CODE: '45', LEVEL: 1, IS_ACTIVE: true, NUMBER_OF_COMPANIES: 3, LAST_UPDATED: '2024-12-31' },
  { SECTOR_ID: 'S-FIN', NAME: 'Financials', GICS_CODE: '40', LEVEL: 1, IS_ACTIVE: true, NUMBER_OF_COMPANIES: 1, LAST_UPDATED: '2024-12-31' },
  { SECTOR_ID: 'S-EN', NAME: 'Energy', GICS_CODE: '10', LEVEL: 1, IS_ACTIVE: true, NUMBER_OF_COMPANIES: 1, LAST_UPDATED: '2024-12-31' },
  { SECTOR_ID: 'S-IDX', NAME: 'Broad Market', LEVEL: 1, IS_ACTIVE: true, NUMBER_OF_COMPANIES: 2, LAST_UPDATED: '2024-12-31' },
  { SECTOR_ID: 'S-CMD', NAME: 'Commodities', LEVEL: 1, IS_ACTIVE: true, NUMBER_OF_COMPANIES: 1, LAST_UPDATED: '2024-12-31' },
  { SECTOR_ID: 'S-RAT', NAME: 'Rates', LEVEL: 1, IS_ACTIVE: true, NUMBER_OF_COMPANIES: 1, LAST_UPDATED: '2024-12-31' },
  { SECTOR_ID: 'S-FX', NAME: 'Currencies', LEVEL: 1, IS_ACTIVE: true, NUMBER_OF_COMPANIES: 1, LAST_UPDATED: '2024-12-31' },
];

const CURRENCIES: readonly Row[] = [
  { CODE: 'USD', NAME: 'US Dollar', NUMERIC_CODE: 840, MINOR_UNIT: 2, SYMBOL: '$', IS_ACTIVE: true, REGION: 'Americas', IS_MAJOR: true, CENTRAL_BANK: 'Federal Reserve' },
  { CODE: 'EUR', NAME: 'Euro', NUMERIC_CODE: 978, MINOR_UNIT: 2, SYMBOL: '€', IS_ACTIVE: true, REGION: 'Europe', IS_MAJOR: true, CENTRAL_BANK: 'European Central Bank' },
];

/** Each instrument of the markets sample as the trading model holds it: its name, product type, listing, sector. */
const LISTINGS: Readonly<Record<string, { name: string; product: string; mic: string; sector: string; listed: string }>> = {
  SPX: { name: 'S&P 500 Index', product: 'Index', mic: 'XCBO', sector: 'S-IDX', listed: '1957-03-04' },
  NDX: { name: 'Nasdaq-100 Index', product: 'Index', mic: 'XNAS', sector: 'S-IDX', listed: '1985-01-31' },
  AAPL: { name: 'Apple Inc.', product: 'Equity', mic: 'XNAS', sector: 'S-IT', listed: '1980-12-12' },
  MSFT: { name: 'Microsoft Corporation', product: 'Equity', mic: 'XNAS', sector: 'S-IT', listed: '1986-03-13' },
  NVDA: { name: 'NVIDIA Corporation', product: 'Equity', mic: 'XNAS', sector: 'S-IT', listed: '1999-01-22' },
  JPM: { name: 'JPMorgan Chase & Co.', product: 'Equity', mic: 'XNYS', sector: 'S-FIN', listed: '1969-03-05' },
  XOM: { name: 'Exxon Mobil Corporation', product: 'Equity', mic: 'XNYS', sector: 'S-EN', listed: '1920-03-01' },
  GLD: { name: 'SPDR Gold Shares', product: 'ETF', mic: 'ARCX', sector: 'S-CMD', listed: '2004-11-18' },
  TLT: { name: 'iShares 20+ Year Treasury Bond ETF', product: 'ETF', mic: 'XNAS', sector: 'S-RAT', listed: '2002-07-22' },
  EURUSD: { name: 'Euro / US Dollar', product: 'Spot', mic: 'XOFF', sector: 'S-FX', listed: '1999-01-04' },
};

/** The instruments the bank trades: the equities and the funds (an index is no instrument one buys; a currency's
 *  trades are another desk's). */
const TRADED: ReadonlySet<string> = new Set(['AAPL', 'MSFT', 'NVDA', 'JPM', 'XOM', 'GLD', 'TLT']);

const instrumentId = (symbol: string): string => `INS-${symbol}`;

/** Deterministic, so the same call gives the same trades (samples.ts's own generator, a stream of its own). */
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

/** RFC 4180: quoted when the value holds a comma, a quote or a line break. */
const cell = (v: string | number | boolean | undefined): string => {
  if (v === undefined) return '';
  const s = String(v);
  return /[",\n\r]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
};

function csv(table: StressMarketTable, rows: readonly Row[]): string {
  const columns = STRESS_MARKET_COLUMNS[table];
  return `${[columns.join(','), ...rows.map((r) => columns.map((c) => cell(r[c])).join(','))].join('\n')}\n`;
}

/** A quote a day of an instrument: its close its last and its mid, a bid and an ask either side of it (two basis
 *  points), its time the close in New York as the corpus's wall clock holds it. */
function quote(b: MarketBar): Row {
  const places = b.asset === 'FX' ? 4 : 2;
  const close = Number(b.close);
  return {
    QUOTE_ID: `Q-${b.symbol}-${b.day}`, QUOTE_TIME: `${b.day} 16:00:00`,
    BID: (close * (1 - 0.0002)).toFixed(places), ASK: (close * (1 + 0.0002)).toFixed(places), MID: b.close, LAST: b.close,
    VOLUME: b.volume, OPEN_PRICE: b.open, HIGH_PRICE: b.high, LOW_PRICE: b.low, CLOSE_PRICE: b.close,
    SOURCE: 'EOD', IS_DELAYED: false, INSTRUMENT_ID: instrumentId(b.symbol),
  };
}

/**
 * THE BANK'S TRADES on the instruments it trades: on about one day in twelve of each, a buy or a sell of a round lot at a
 * price within that day's low and high, booked and settled two business days on (its own stream of chance, so the
 * prices are the markets sample's whatever the trades are).
 */
function trades(bars: readonly MarketBar[], seed: number): Row[] {
  const rand = rng(seed);
  const days = [...new Set(bars.map((b) => b.day))];
  const settles = new Map(days.map((d, i) => [d, days[i + 2]] as const));
  const out: Row[] = [];
  for (const b of bars) {
    if (!TRADED.has(b.symbol) || rand() >= 1 / 12) continue;
    const [low, high] = [Number(b.low), Number(b.high)];
    const price = Number((low + rand() * (high - low)).toFixed(2));
    const quantity = 100 * (1 + Math.floor(rand() * 50));
    const settled = settles.get(b.day);
    out.push({
      TRADE_ID: `T-${String(out.length + 1).padStart(5, '0')}`, TRADE_DATE: b.day,
      ...(settled ? { SETTLEMENT_DATE: settled } : {}),
      QUANTITY: quantity, PRICE: price.toFixed(2), NOTIONAL: (quantity * price).toFixed(2), SIDE: rand() < 0.5 ? 'B' : 'S',
      STATUS: settled ? 'SETTLED' : 'BOOKED', TRADE_TYPE: 'CASH', CURRENCY: 'USD', COMMISSION: (quantity * 0.005).toFixed(2),
      FEES: '0.00', EXECUTION_VENUE: LISTINGS[b.symbol]!.mic, IS_BLOCK: quantity >= 4000, CREATED_TIME: `${b.day} 14:30:00`,
      LAST_MODIFIED_TIME: `${b.day} 14:30:00`, INSTRUMENT_ID: instrumentId(b.symbol),
    });
  }
  return out;
}

/** A sample's CSV text as its rows: each line's cells by its header's names. */
function sampleRows(text: string): Readonly<Record<string, string>>[] {
  const [header, ...lines] = text.trim().split('\n');
  const names = header!.split(',');
  return lines.map((l) => Object.fromEntries(l.split(',').map((v, i) => [names[i]!, v])));
}

/** The rates sample's curve (its defaults: the gallery's) as the curve's points: a US Treasury curve, each tenor its days
 *  (a year 365 of them) and its zero rate the sample's yield, value for value; its discount factor from it. */
function curvePoints(): Row[] {
  return sampleRows(ratesCsv()).map((r) => {
    const years = Number(r['years']);
    return {
      CURVE_ID: 'UST', COB_DATE: r['date']!, TENOR_LABEL: r['tenor']!, TENOR_DAYS: Math.round(years * 365),
      ZERO_RATE: r['yield']!, DISCOUNT_FACTOR: Math.exp(-Number(r['yield']) / 100 * years).toFixed(8),
      IS_INTERPOLATED: false, CURVE_NAME: 'US Treasury', CURVE_CURRENCY: 'USD', CURVE_TYPE: 'GOVT',
    };
  });
}

/** The macro sample's series (its defaults: the gallery's), value for value. */
function macroObservations(): Row[] {
  return sampleRows(macroCsv()).map((r) => ({ SERIES: r['series']!, OBS_DATE: r['date']!, VALUE: r['value']! }));
}

/** The markets sample's days (`rows`, `seed`: marketBars's) as the trading model's tables: each table's CSV text. */
export function stressMarketCsvs(rows = 13_050, seed = 20261058): Readonly<Record<StressMarketTable, string>> {
  const bars = marketBars(rows, seed);
  const symbols = [...new Set(bars.map((b) => b.symbol))];
  const instruments: Row[] = symbols.map((symbol) => {
    const l = LISTINGS[symbol];
    if (!l) throw new Error(`the markets sample's ${symbol} has no listing in the trading model's tables`);
    const asset = bars.find((b) => b.symbol === symbol)!.asset;
    return {
      INSTRUMENT_ID: instrumentId(symbol), TICKER: symbol, NAME: l.name, ASSET_CLASS: asset, PRODUCT_TYPE: l.product,
      IS_ACTIVE: true, LISTING_DATE: l.listed, CURRENCY: 'USD', CURRENCY_CODE: symbol === 'EURUSD' ? 'EUR' : 'USD',
      EXCHANGE_MIC: l.mic, SECTOR_ID: l.sector,
    };
  });
  return {
    EXCHANGE: csv('EXCHANGE', EXCHANGES),
    SECTOR: csv('SECTOR', SECTORS),
    CURRENCY: csv('CURRENCY', CURRENCIES),
    INSTRUMENT: csv('INSTRUMENT', instruments),
    MARKET_QUOTE: csv('MARKET_QUOTE', bars.map(quote)),
    TRADE: csv('TRADE', trades(bars, seed + 1)),
    RATE_CURVE_POINT: csv('RATE_CURVE_POINT', curvePoints()),
    MACRO_SERIES: csv('MACRO_SERIES', macroObservations()),
  };
}

/** The Data element the markets sample's tables are, as a project carries its rows (query/demo/models/trading-data.pure's
 *  way): its path. */
export const STRESS_MARKETS_DATA = 'stress::markets::MarketsData';

/**
 * THE MARKETS SAMPLE'S TABLES AS A PROJECT'S DATA (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5): one relational Data
 * element of the store's tables, each its CSV -- what a page fills a database of the project's own from, every table
 * made by the store's own declaration (the planner's projectData), so each column has the store's type.
 */
export function stressMarketsData(rows = 13_050, seed = 20261058): string {
  const csvs = stressMarketCsvs(rows, seed);
  // each line a Pure string, its quote escaped, the text's own line break written as the escape it is
  const quoted = (line: string): string => `'${line.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}\\n'`;
  const tables = STRESS_MARKET_TABLES.map((t) => {
    const lines = csvs[t].trimEnd().split('\n');
    return `    default.${t}:\n${lines.map((l) => `      ${quoted(l)}`).join('+\n')};`;
  });
  return `###Data\n// The markets sample's days as the trading model's tables (datacube/src/stress-markets.ts): written, never edited.\n`
    + `Data ${STRESS_MARKETS_DATA}\n{\n  Relational\n  #{\n${tables.join('\n')}\n  }#\n}\n`;
}
