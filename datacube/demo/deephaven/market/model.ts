// THE MARKET WORLD'S MODEL (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K2): the tables the market's Deephaven server
// holds (market.groovy, as it makes them: a declaration the browser route trusts, DH16), a connection to that server,
// and the runtime a page plans on. A quote's and a trade's name are `qsym` and `tsym`, so a join of either with the
// instrument names each column once.

/** The model, its connection at `host`:`port` over TLS. */
export const MARKET_MODEL = (host: string, port: number): string => `###Relational
Database market::Store
(
  Table instrument (sym VARCHAR(16) PRIMARY KEY, name VARCHAR(64) NOT NULL, kind VARCHAR(8) NOT NULL,
                    sector VARCHAR(32) NOT NULL, prevClose DOUBLE NOT NULL, shares BIGINT NOT NULL,
                    tick DOUBLE NOT NULL)
  Table quote (qsym VARCHAR(16) PRIMARY KEY, qts TIMESTAMP NOT NULL, bid DOUBLE NOT NULL, ask DOUBLE NOT NULL,
               bidSize DOUBLE NOT NULL, askSize DOUBLE NOT NULL)
  Table trade (seq BIGINT PRIMARY KEY, ts TIMESTAMP NOT NULL, tsym VARCHAR(16) NOT NULL, px DOUBLE NOT NULL,
               sz DOUBLE NOT NULL, side VARCHAR(1) NOT NULL, venue VARCHAR(8) NOT NULL)
  Table world (item VARCHAR(16) PRIMARY KEY, state VARCHAR(16) NOT NULL, detail VARCHAR(256) NOT NULL)
)
###Connection
RelationalDatabaseConnection market::Live
{
  store: market::Store;
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
###Runtime
Runtime market::LiveRuntime
{
  mappings: [];
  connections: [ market::Store: [ live: market::Live ] ];
}`;

export const MARKET_RUNTIME = 'market::LiveRuntime';
