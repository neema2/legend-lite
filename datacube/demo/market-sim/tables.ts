// THE MARKET'S TABLES, as the one trading model declares them (core/src/test/resources/stress/30-store.pure: every
// column, in its order, by its type -- a test holds each shape to the store's declaration), and the rows a beat makes of
// one, a column at a time (TableRows), as the live engine takes them (engine-client/src/duckdb-live.ts LiveRows). How
// each is written (docs/DUCKDB_LIVE_2026_10_25.md): a STREAM is appended to (the tape, the wire, the fills and routes),
// a KEYED table upserted by its key (the latest quote a name, an order's latest state, a pair's latest rate, a
// request's), and the REFERENCE written once, as the world starts.

import type { MadeColumn } from '../../../engine-client/src/arrow-ipc.ts';

/** A column's kind, by the store's type: VARCHAR text, DOUBLE, INTEGER, TIMESTAMP an instant (ms), BIT a truth, DATE a
 *  day (days since the epoch). */
export type ColumnKind = 'text' | 'double' | 'integer' | 'instant' | 'truth' | 'day';

/** A table's columns, in the store's order. */
export type Shape = Readonly<Record<string, ColumnKind>>;

/** How a table is written: a stream (appended), keyed (upserted by its key), or reference (written once). */
export type TableWrite = { readonly kind: 'stream' } | { readonly kind: 'keyed'; readonly key: string } | { readonly kind: 'reference' };

export interface MarketTable {
  readonly name: string;
  readonly shape: Shape;
  readonly write: TableWrite;
}

const t = (name: string, write: TableWrite, columns: string): MarketTable => ({
  name,
  write,
  shape: Object.fromEntries(columns.split(/,\s*/).map((c) => {
    const [column, kind] = c.split(':') as [string, ColumnKind];
    return [column, kind];
  })),
});

const STREAM: TableWrite = { kind: 'stream' };
const REFERENCE: TableWrite = { kind: 'reference' };
const keyed = (key: string): TableWrite => ({ kind: 'keyed', key });

export const EXCHANGE = t('EXCHANGE', REFERENCE, 'MIC:text, NAME:text, CITY:text, TIMEZONE:text, OPEN_TIME:text, CLOSE_TIME:text, IS_ELECTRONIC:truth, OPERATING_MIC:text, WEBSITE:text, REGULATORY_BODY:text, COUNTRY_ID:text');
export const SECTOR = t('SECTOR', REFERENCE, 'SECTOR_ID:text, NAME:text, GICS_CODE:text, LEVEL:integer, DESCRIPTION:text, IS_ACTIVE:truth, PARENT_SECTOR_ID:text, MARKET_CAP_WEIGHT:double, NUMBER_OF_COMPANIES:integer, LAST_UPDATED:day');
export const INSTRUMENT = t('INSTRUMENT', REFERENCE, 'INSTRUMENT_ID:text, TICKER:text, NAME:text, ISIN:text, CUSIP:text, SEDOL:text, ASSET_CLASS:text, PRODUCT_TYPE:text, IS_ACTIVE:truth, LISTING_DATE:day, MATURITY_DATE:day, NOTIONAL_AMOUNT:double, CURRENCY:text, DESCRIPTION:text, ISSUE_DATE:day, EQUITY_ID:text, BOND_ID:text, FUTURE_ID:text, FX_FORWARD_ID:text, SWAP_ID:text, CDS_ID:text, STRUCTURED_PRODUCT_ID:text, ETF_ID:text, CURRENCY_CODE:text, EXCHANGE_MIC:text, SECTOR_ID:text');
export const EQUITY = t('EQUITY', REFERENCE, 'EQUITY_ID:text, TICKER:text, COMPANY_NAME:text, SHARE_CLASS:text, MARKET_CAP:double, SHARES_OUTSTANDING:double, DIVIDEND_YIELD:double, PE_RATIO:double, BETA:double, PRIMARY_EXCHANGE:text, IS_ADR:truth, LOT_SIZE:integer, LAST_PRICE:double, PRICE_DATE:day, INSTRUMENT_ID:text');
export const MARKET_QUOTE = t('MARKET_QUOTE', keyed('QUOTE_ID'), 'QUOTE_ID:text, QUOTE_TIME:instant, BID:double, ASK:double, MID:double, LAST:double, VOLUME:double, OPEN_PRICE:double, HIGH_PRICE:double, LOW_PRICE:double, CLOSE_PRICE:double, SOURCE:text, IS_DELAYED:truth, SNAPSHOT_ID:text, INSTRUMENT_ID:text, BID_SIZE:double, ASK_SIZE:double');
export const MARKET_TRADE = t('MARKET_TRADE', STREAM, 'PRINT_ID:text, SEQUENCE_NUMBER:integer, PRINT_TIME:instant, PRICE:double, QUANTITY:double, AGGRESSOR_SIDE:text, VENUE:text, TRADE_CONDITION:text, IS_OFF_EXCHANGE:truth, INSTRUMENT_ID:text');
export const NEWS_STORY = t('NEWS_STORY', STREAM, 'STORY_ID:text, PUBLISH_TIME:instant, HEADLINE:text, BODY:text, SOURCE:text, CATEGORY:text, URGENCY:text, SENTIMENT:double, TICKERS:text, REGION:text, INSTRUMENT_ID:text, SECTOR_ID:text');
export const BUSINESS_UNIT = t('BUSINESS_UNIT', REFERENCE, 'BU_ID:text, NAME:text, HEAD:text, DIVISION:text, IS_ACTIVE:truth, HEADCOUNT:integer, BUDGET:double, COST_CENTER:text, REGION:text, ESTABLISHED_DATE:day, DESCRIPTION:text, LEGAL_ENTITY_ID:text');
export const TRADER = t('TRADER', REFERENCE, 'TRADER_ID:text, FIRST_NAME:text, LAST_NAME:text, BADGE:text, EMAIL:text, HIRE_DATE:day, SENIORITY:text, IS_ACTIVE:truth, SPECIALIZATION:text, LICENSE:text, MAX_NOTIONAL:double, PNL_YTD:double, PHONE:text, DESK_ID:text, MANAGER_ID:text');
export const BOOK = t('BOOK', REFERENCE, 'BOOK_ID:text, NAME:text, BOOK_TYPE:text, CURRENCY:text, IS_ACTIVE:truth, OPEN_DATE:day, CLOSE_DATE:day, LEGAL_ENTITY:text, STRATEGY:text, PNL_YTD:double, POSITION_COUNT:integer, RISK_LIMIT:double, DESCRIPTION:text, DESK_ID:text');
export const DESK = t('DESK', REFERENCE, 'DESK_ID:text, NAME:text, DESK_TYPE:text, ASSET_CLASS:text, REGION:text, IS_ACTIVE:truth, HEAD_TRADER:text, COST_CENTER:text, FLOOR:text, BUILDING:text, PHONE:text, RISK_LIMIT:double, PNL_TARGET:double, BUSINESS_UNIT_ID:text');
export const OPTION = t('OPTION', REFERENCE, 'OPTION_ID:text, OPTION_TYPE:text, STRIKE_PRICE:double, EXPIRATION_DATE:day, PREMIUM:double, IMPLIED_VOLATILITY:double, DELTA:double, GAMMA:double, THETA:double, VEGA:double, EXERCISE_STYLE:text, IS_IN_THE_MONEY:truth, UNDERLYING_PRICE:double, CONTRACT_SIZE:integer, OPEN_INTEREST:integer, UNDERLYING_ID:text, INSTRUMENT_ID:text');
export const POSITION = t('POSITION', REFERENCE, 'POSITION_ID:text, QUANTITY:double, AVERAGE_COST:double, MARKET_VALUE:double, UNREALIZED_PN_L:double, REALIZED_PN_L:double, CURRENCY:text, POSITION_DATE:day, DIRECTION:text, COST_BASIS:double, ACCRUED_INTEREST:double, LAST_UPDATED:instant, IS_OPEN:truth, SETTLEMENT_DATE:day, BOOK_ID:text, PORTFOLIO_ID:text, INSTRUMENT_ID:text');
export const RISK_LIMIT = t('RISK_LIMIT', REFERENCE, 'LIMIT_ID:text, LIMIT_TYPE:text, LIMIT_AMOUNT:double, CURRENT_UTILIZATION:double, UTILIZATION_PCT:double, CURRENCY:text, EFFECTIVE_DATE:day, EXPIRY_DATE:day, STATUS:text, APPROVED_BY:text, APPROVAL_DATE:day, WARNING_THRESHOLD:double, HARD_LIMIT:double, IS_ACTIVE:truth, DESK_ID:text');
export const COUNTERPARTY = t('COUNTERPARTY', REFERENCE, 'COUNTERPARTY_ID:text, LEGAL_NAME:text, SHORT_NAME:text, LEI:text, COUNTERPARTY_TYPE:text, DOMICILE:text, INCORPORATION_COUNTRY:text, IS_ACTIVE:truth, ONBOARD_DATE:day, CREDIT_RATING:text, RATING_AGENCY:text, TIER:text, PARENT_ENTITY_ID:text, INDUSTRY:text, WEBSITE:text, CLASSIFICATION_ID:text, COUNTRY_CODE:text');
export const TRADE_ORDER = t('TRADE_ORDER', keyed('ORDER_ID'), 'ORDER_ID:text, ORDER_DATE:day, SIDE:text, QUANTITY:double, FILLED_QUANTITY:double, REMAINING_QUANTITY:double, LIMIT_PRICE:double, STOP_PRICE:double, TIME_IN_FORCE:text, STATUS:text, PRIORITY:integer, CREATED_TIME:instant, LAST_MODIFIED_TIME:instant, EXPIRY_DATE:day, IS_ALGO_ORDER:truth, ALGO_STRATEGY:text, INSTRUMENT_ID:text, TRADER_ID:text, BOOK_ID:text');
export const EXECUTION = t('EXECUTION', STREAM, 'EXECUTION_ID:text, EXECUTION_TIME:instant, FILLED_QUANTITY:double, FILL_PRICE:double, VENUE:text, BROKER:text, COMMISSION:double, FILL_TYPE:text, EXECUTION_ALGO:text, ARRIVAL_PRICE:double, SLIPPAGE:double, MARKET_IMPACT:double, IS_PARTIAL_FILL:truth, SEQUENCE_NUMBER:integer, LAST_MARKET:text, ORDER_ID:text');
export const ORDER_ROUTE = t('ORDER_ROUTE', STREAM, 'ROUTE_ID:text, ROUTE_DATE:instant, DESTINATION:text, ROUTE_QUANTITY:double, ROUTED_PRICE:double, STATUS:text, ROUTE_TYPE:text, PRIORITY:integer, LATENCY_MS:integer, IS_SMART_ROUTED:truth, REJECT_REASON:text, BROKER:text, ORDER_ID:text');
export const FX_RATE = t('FX_RATE', keyed('RATE_ID'), 'RATE_ID:text, CURRENCY_PAIR:text, SPOT_RATE:double, BID_RATE:double, ASK_RATE:double, RATE_DATE:instant, SOURCE:text, MID_RATE:double, DAILY_CHANGE:double, DAILY_CHANGE_PCT:double, IS_OFFICIAL:truth, FIXING_TIME:text, SNAPSHOT_ID:text, BASE_CURRENCY_CODE:text');
export const RFQ = t('RFQ', keyed('RFQ_ID'), 'RFQ_ID:text, REQUEST_TIME:instant, CURRENCY_PAIR:text, SIDE:text, NOTIONAL:double, TENOR:text, QUOTED_RATE:double, SPREAD_BPS:double, STATUS:text, RESPONSE_TIME_MS:integer, IS_HIT:truth, COUNTERPARTY_ID:text, TRADER_ID:text');
/** The world's own status table, outside the corpus (market/model.ts market::World): what each part of the world is. */
export const WORLD = t('world', keyed('item'), 'item:text, state:text, detail:text');

/** Every table the market writes, the store's (store::DB) and the world's own. */
export const MARKET_TABLES: readonly MarketTable[] = [
  EXCHANGE, SECTOR, INSTRUMENT, EQUITY, MARKET_QUOTE, MARKET_TRADE, NEWS_STORY, BUSINESS_UNIT, TRADER, BOOK, DESK, OPTION,
  POSITION, RISK_LIMIT, COUNTERPARTY, TRADE_ORDER, EXECUTION, ORDER_ROUTE, FX_RATE, RFQ, WORLD,
];

/** A value of a column's kind: a whole number of ms for an instant, of days for a day; null where the row has none. */
export type Value = string | number | boolean | null;

/** A row of a table: its columns by name, each of its kind; one left out is null. */
export type Row = Readonly<Record<string, Value>>;

/**
 * THE ROWS A BEAT MAKES OF ONE TABLE, a column at a time: each `add` a row (a column it leaves out is null), and `rows`
 * them as the live engine takes them, each text column's values once each. Its columns are the table's, every one.
 */
export class TableRows {
  readonly table: MarketTable;
  readonly #columns: readonly (readonly [string, ColumnKind])[];
  #values: Value[][];
  count = 0;

  constructor(table: MarketTable) {
    this.table = table;
    this.#columns = Object.entries(table.shape);
    this.#values = this.#columns.map(() => []);
  }

  add(row: Row): void {
    for (const name of Object.keys(row)) {
      if (!(name in this.table.shape)) throw new Error(`${this.table.name} has no column ${name}`);
    }
    this.#columns.forEach(([name, kind], i) => {
      const v = row[name] ?? null;
      if (v !== null && !fits(kind, v)) throw new Error(`${this.table.name}.${name} is ${kind}: not ${String(v)}`);
      this.#values[i]!.push(v);
    });
    this.count++;
  }

  /** The rows as columns, the live engine's (arrow-ipc.ts MadeColumn each). */
  rows(): Record<string, MadeColumn> {
    const out: Record<string, MadeColumn> = {};
    this.#columns.forEach(([name, kind], i) => {
      out[name] = made(kind, this.#values[i]!);
    });
    return out;
  }

  /** The rows taken (to write), and none kept. */
  take(): { readonly count: number; readonly rows: Record<string, MadeColumn> } | null {
    if (this.count === 0) return null;
    const taken = { count: this.count, rows: this.rows() };
    this.#values = this.#columns.map(() => []);
    this.count = 0;
    return taken;
  }
}

function fits(kind: ColumnKind, v: Value): boolean {
  switch (kind) {
    case 'text': return typeof v === 'string';
    case 'double': return typeof v === 'number';
    case 'truth': return typeof v === 'boolean';
    case 'integer':
    case 'instant':
    case 'day': return typeof v === 'number' && Number.isInteger(v);
  }
}

function made(kind: ColumnKind, values: readonly Value[]): MadeColumn {
  const at = (i: number): number | null => values[i] as number | null;
  switch (kind) {
    case 'text': {
      const distinct = new Map<string, number>();
      const picks = values.map((v) => {
        if (v === null) return null;
        let k = distinct.get(v as string);
        if (k === undefined) distinct.set(v as string, k = distinct.size);
        return k;
      });
      return { text: [...distinct.keys()], pick: (i) => picks[i] ?? null };
    }
    case 'double': return { doubles: at };
    case 'integer': return { integers: at };
    case 'instant': return { instants: at };
    case 'day': return { days: at };
    case 'truth': return { truths: (i) => values[i] as boolean | null };
  }
}
