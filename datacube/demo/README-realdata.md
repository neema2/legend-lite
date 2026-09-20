# Running DataCube on your own data

No legend-lite process, no API: the planner and DuckDB both run in
the tab.

## Once

From the repo root:

```bash
mvn -pl core -am -DskipTests install      # -am installs the parent pom too
cd datacube
npm install
npm run demo:vendor                       # duckdb-wasm binaries
npm run planner:build                     # compiles the planner to wasm
npm run planner:vendor                    # copies it into demo/vendor
npm run build:demo
```

## Point it at a file

Serve the repo (the page links `../src/*.css`, so serve `datacube/`,
not `datacube/demo/`):

```bash
cd datacube && python3 -m http.server 8000
```

then open

```
http://localhost:8000/demo/index.html?remote=http://localhost:8000/trades.parquet&format=parquet
```

`format` is `parquet`, `csv` or `iceberg`, and is inferred from the
URL when the extension says so. `s3://bucket/key` works too — httpfs,
parquet and iceberg are compiled into the duckdb-wasm binary.

Credentials are **not** accepted in the query string, deliberately: a
URL lands in browser history, proxy logs and over your shoulder. A
bucket that needs them is configured by the host page through
`mountRemote`'s `s3` option.

**Byte-range support is worth having, but is not required.** DuckDB
reads the Parquet footer and then only the row groups a query needs.
Both were measured against the same 50,000-row / 533 KB file:

| server | requests | bytes pulled |
|---|---|---|
| Range-capable (206) | 9 | partial reads |
| `python3 -m http.server` (ignores Range, 200) | 7 | 1.07 MB |

Python's `http.server` does not implement `Range`, so every
uncached fetch drags the whole file — it renders correctly, it just
re-downloads. Fine for a look; use a Range-capable server (nginx,
`npx serve`, S3) for anything large.

## Your file needs the model's columns

`demo/trades.pure` declares exactly:

```
region VARCHAR, desk VARCHAR, book VARCHAR, year INTEGER,
qtr VARCHAR, notional DOUBLE, pnl DOUBLE, qty INTEGER
```

The remote file is mounted as a VIEW under the table's name, so
`#>{trades::DB.TRADES}#` still compiles to `SELECT ... FROM TRADES`
and the planner never learns the data is remote. That is also why the
columns have to match. For a different shape, edit `trades.pure` AND
the `columns` list in `demo/boot.ts` — the latter is what tells the
cube each field's type and which ones are dimensions.

## Check it end to end

`npm run verify:realdata` drives the real page in a headless browser
and compares the grid's totals against DuckDB's own answer for the
same file, so it fails if any layer is wrong rather than only if the
page is blank:

```bash
duckdb -c "SELECT region, round(sum(notional)) \
           FROM 'trades.parquet' GROUP BY region"

DATA=/abs/path/trades.parquet \
EXPECT='{"Iberia":74988250,"Levant":74985610,"Nordics":74991390}' \
npm run verify:realdata
```

Add `FORMAT=csv` for a CSV. Both were verified against a 50,000-row
file: per-region totals matched DuckDB to the dollar (±1, from
rounding cents for display).
