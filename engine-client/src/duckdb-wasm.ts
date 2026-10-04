// DuckDB-WASM, for an app starting DuckDB in its tab: the package itself, re-exported from here so every
// app's bundle takes it -- and the Apache Arrow it brings -- from this package's one copy, the same Arrow
// the warehouse's result chunks are read with (warehouse.ts). An app imports `* as duckdb` from this file,
// never from '@duckdb/duckdb-wasm' itself; it still copies the package's .wasm and worker files to serve.

export * from '@duckdb/duckdb-wasm';
