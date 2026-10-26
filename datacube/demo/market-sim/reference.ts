// THE MARKET'S REFERENCE (market.groovy's tables written once): the listing venues, the sectors and their shares of the
// market, every name an instrument (and each listed option one, the bank's: bank.ts), each equity's company, shares out,
// capitalisation at the prior close. Every column the store declares; a null where the world has no value. The facts the
// simulation does not move (a yield, an earnings multiple) from a draw of their own (`facts`), as the server's.

import { GICS, SECTORS, type Name } from './names.ts';
import type { JavaRandom } from './random.ts';
import { EQUITY, EXCHANGE, INSTRUMENT, SECTOR, TableRows } from './tables.ts';
import type { OptionRow } from './bank.ts';
import { javaDouble, isoDay } from './format.ts';

export function exchanges(): TableRows {
  const rows = new TableRows(EXCHANGE);
  rows.add({
    MIC: 'XNAS', NAME: 'Nasdaq Stock Market', CITY: 'New York', TIMEZONE: 'America/New_York', OPEN_TIME: '09:30',
    CLOSE_TIME: '16:00', IS_ELECTRONIC: true, OPERATING_MIC: 'XNAS', WEBSITE: 'https://www.nasdaq.com', REGULATORY_BODY: 'SEC',
    COUNTRY_ID: 'US',
  });
  rows.add({
    MIC: 'XNYS', NAME: 'New York Stock Exchange', CITY: 'New York', TIMEZONE: 'America/New_York', OPEN_TIME: '09:30',
    CLOSE_TIME: '16:00', IS_ELECTRONIC: false, OPERATING_MIC: 'XNYS', WEBSITE: 'https://www.nyse.com', REGULATORY_BODY: 'SEC',
    COUNTRY_ID: 'US',
  });
  return rows;
}

/** The eleven GICS sectors and crypto: each one's share of the market's capitalisation at the prior close, its names. */
export function sectors(equities: readonly Name[], coins: readonly Name[], today: number): TableRows {
  const rows = new TableRows(SECTOR);
  const capOf = (n: Name): number => n.shares * n.prevClose;
  const total = equities.reduce((s, n) => s + capOf(n), 0);
  SECTORS.forEach((name, k) => {
    const of = equities.filter((n) => n.sectorIx === k);
    rows.add({
      SECTOR_ID: `SEC-${GICS[k]!}`, NAME: name, GICS_CODE: GICS[k]!, LEVEL: 1, IS_ACTIVE: true,
      MARKET_CAP_WEIGHT: of.reduce((s, n) => s + capOf(n), 0) / total, NUMBER_OF_COMPANIES: of.length, LAST_UPDATED: today,
    });
  });
  rows.add({ SECTOR_ID: 'SEC-CRYPTO', NAME: 'Crypto', LEVEL: 1, IS_ACTIVE: true, NUMBER_OF_COMPANIES: coins.length, LAST_UPDATED: today });
  return rows;
}

/** Every name an instrument (INST-<ticker>), and each listed option one of its own beside them. */
export function instruments(names: readonly Name[], options: readonly OptionRow[]): TableRows {
  const rows = new TableRows(INSTRUMENT);
  for (const n of names) {
    rows.add({
      INSTRUMENT_ID: n.instrumentId, TICKER: n.sym, NAME: n.name, ASSET_CLASS: n.kind,
      PRODUCT_TYPE: n.kind === 'EQUITY' ? 'COMMON_STOCK' : 'SPOT', IS_ACTIVE: true, CURRENCY: 'USD', DESCRIPTION: n.sector,
      EQUITY_ID: n.equityId, CURRENCY_CODE: 'USD', EXCHANGE_MIC: n.exchange, SECTOR_ID: n.sectorId,
    });
  }
  for (const o of options) {
    rows.add({
      INSTRUMENT_ID: o.instrument, TICKER: o.ticker,
      NAME: `${o.underlying.sym} ${isoDay(o.expiry)} ${javaDouble(o.strike)} ${o.type}`, ASSET_CLASS: 'OPTION',
      PRODUCT_TYPE: 'LISTED_OPTION', IS_ACTIVE: true, MATURITY_DATE: o.expiry, CURRENCY: 'USD', DESCRIPTION: o.underlying.sector,
      CURRENCY_CODE: 'USD', EXCHANGE_MIC: 'XCBO', SECTOR_ID: o.underlying.sectorId,
    });
  }
  return rows;
}

/** Each equity's company, shares out, capitalisation at the prior close, beta, lot; its yield and multiple drawn once. */
export function equityRows(equities: readonly Name[], facts: JavaRandom, today: number): TableRows {
  const rows = new TableRows(EQUITY);
  // the server draws every yield, then every multiple: a column at a time
  const dividends = equities.map(() => Math.round(facts.nextDouble() * 400) / 10000);
  const multiples = equities.map(() => Math.round((8 + facts.nextDouble() * 32) * 10) / 10);
  equities.forEach((n, i) => {
    rows.add({
      EQUITY_ID: n.equityId, TICKER: n.sym, COMPANY_NAME: `${n.name} Inc.`, SHARE_CLASS: 'COMMON',
      MARKET_CAP: n.shares * n.prevClose, SHARES_OUTSTANDING: n.shares, DIVIDEND_YIELD: dividends[i]!, PE_RATIO: multiples[i]!,
      BETA: Math.round(n.beta * 100) / 100, PRIMARY_EXCHANGE: n.exchange, IS_ADR: false, LOT_SIZE: 100, LAST_PRICE: n.prevClose,
      PRICE_DATE: today - 1, INSTRUMENT_ID: n.instrumentId,
    });
  });
  return rows;
}
