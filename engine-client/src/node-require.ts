// A `require` rooted in this package, for code run in NODE (tests, benchmarks, harnesses) that loads DuckDB-WASM's
// Node build: `engineClientRequire('@duckdb/duckdb-wasm/blocking')`, and `.resolve(...)` for its dist/ files. It
// resolves this package's one copy, so no app keeps DuckDB-WASM in its own lock. A browser bundle never imports this
// file (the page takes DuckDB from duckdb-wasm.ts).

import { createRequire } from 'node:module';

export const engineClientRequire = createRequire(import.meta.url);
