// THE MARKET WORLD'S MODEL (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K2, DH34): the ONE TRADING MODEL -- the stress
// corpus's classes, store and mappings (//scripts/corpus:trading_model, `corpus` here) -- with a connection from its
// store to the Deephaven server running the market world (market.groovy publishes the corpus's tables, every column the
// store declares), and the world's own status table beside it, which no class of the corpus models. A page plans the
// corpus's classes (marketdata::MarketTrade, products::Instrument, ...) through stress::AllMapping on the runtime.

/** The model, its connections at `host`:`port` over TLS, over the corpus's text. */
export const MARKET_MODEL = (corpus: string, host: string, port: number): string => `${corpus}
###Relational
Database market::World
(
  Table world (item VARCHAR(16) PRIMARY KEY, state VARCHAR(16) NOT NULL, detail VARCHAR(256) NOT NULL)
)
###Connection
RelationalDatabaseConnection market::Live
{
  store: store::DB;
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
RelationalDatabaseConnection market::Status
{
  store: market::World;
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
###Runtime
Runtime market::LiveRuntime
{
  mappings: [ stress::AllMapping ];
  connections: [ store::DB: [ live: market::Live ], market::World: [ status: market::Status ] ];
}`;

export const MARKET_RUNTIME = 'market::LiveRuntime';
