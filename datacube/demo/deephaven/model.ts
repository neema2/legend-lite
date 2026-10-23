// THE RISK DEMO'S MODEL (docs/DEEPHAVEN_PLAN_2026_10_22.md D0, DH23): the tables the demo's Deephaven server holds
// (risk.groovy), a connection to that server, and the runtime the cube plans on.

/** The model, its connection at `host`:`port` over TLS. */
export const MODEL = (host: string, port: number): string => `###Relational
Database risk::Store
(
  Table instrument (iid VARCHAR(8) PRIMARY KEY, sector VARCHAR(16) NOT NULL, ccy VARCHAR(3) NOT NULL)
  Table book (bid VARCHAR(8) PRIMARY KEY, desk VARCHAR(16) NOT NULL, region VARCHAR(8) NOT NULL)
  Table position (pid INTEGER PRIMARY KEY, book_id VARCHAR(8) NOT NULL, inst_id VARCHAR(8) NOT NULL, qty BIGINT NOT NULL)
  Table price (px_iid VARCHAR(8) PRIMARY KEY, px DOUBLE NOT NULL)
  Table fx (fx_ccy VARCHAR(3) PRIMARY KEY, rate DOUBLE NOT NULL)
)
###Connection
RelationalDatabaseConnection risk::Live
{
  store: risk::Store;
  type: Deephaven;
  specification: Deephaven { host: '${host}'; port: ${port}; useTls: true; };
  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };
}
###Runtime
Runtime risk::LiveRuntime
{
  mappings: [];
  connections: [ risk::Store: [ live: risk::Live ] ];
}`;
