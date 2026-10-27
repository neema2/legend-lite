// THE MARKET WORLD'S MODEL (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K2, DH34): three files, in order --
//   1. the ONE TRADING MODEL: the stress corpus's classes, store and mappings (//scripts/corpus:trading_model, `corpus`
//      here), which the market world publishes on its Deephaven server (market.groovy: every column the store declares);
//   2. the DESK'S OWN MODEL (market.pure, `market` here): the world's status table, the options' and the screens'
//      functions -- what every screen of the desk shows -- and the runtime;
//   3. the CONNECTIONS, the environment's: a Deephaven server's address and key, or the tab's own DuckDB (written here).
// A page plans the corpus's classes (marketdata::MarketTrade, products::Instrument, ...) through stress::AllMapping on
// the runtime, and the desk's saved queries name the screens' functions (screens.ts).

/** The market's connection at `host`:`port` over TLS, its store `store`: the model's own (store::DB), and a page's
 *  server bases' (src/server-base.ts `connectionOf`: their Database in a model of their own). */
export const MARKET_CONNECTION = (host: string, port: number) => (store: string): string => `RelationalDatabaseConnection market::Live
{
  store: ${store};
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}`;

/** The model whose connections are a Deephaven server's at `host`:`port`, over TLS: the corpus, the desk's own, then
 *  the connections. */
export const MARKET_MODEL = (corpus: string, market: string, host: string, port: number): string =>
  `${corpus}\n${market}\n${MARKET_LIVE(host, port)}`;

/** The connections to a Deephaven server at `host`:`port`, over TLS: a project's third model file, after the corpus's
 *  and the desk's own (the trading desk's project: host.ts). */
export const MARKET_LIVE = (host: string, port: number): string => `###Connection
${MARKET_CONNECTION(host, port)('store::DB')}
RelationalDatabaseConnection market::Status
{
  store: market::World;
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}`;

/**
 * THE MARKET IN THE TAB (docs/DUCKDB_LIVE_2026_10_25.md): the connections the tab's own DuckDB -- the same connections'
 * and runtime's names, so a saved page of the desk runs on either: what it reads is the host's to say.
 */
export const MARKET_TAB = (): string => `###Connection
RelationalDatabaseConnection market::Live
{
  store: store::DB;
  type: DuckDB;
  specification: DuckDB { };
  auth: Test;
}
RelationalDatabaseConnection market::Status
{
  store: market::World;
  type: DuckDB;
  specification: DuckDB { };
  auth: Test;
}`;

/** The model in the tab: the corpus, the desk's own, then the connections to the tab's DuckDB. */
export const MARKET_TAB_MODEL = (corpus: string, market: string): string => `${corpus}\n${market}\n${MARKET_TAB()}`;

/** The runtime the desk's model declares (market.pure). */
export const MARKET_RUNTIME = 'market::LiveRuntime';
