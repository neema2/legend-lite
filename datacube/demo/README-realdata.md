# Running DataCube on your own data

No legend-lite process, no API, no backend at all: the planner and
DuckDB both run in the tab.

## Just run it

```bash
cd datacube && npm install
npm start
```

That is the whole thing. It builds whatever is missing — the
duckdb-wasm binaries, the planner module, the bundles — prints a URL,
and serves it. The first run compiles the planner to WebAssembly and
takes about a minute; after that it is instant because nothing needs
rebuilding.

```
  http://localhost:8000/demo/index.html
```

Add `--open` to launch a browser, `--port N` to move it.

## No file handy?

Click **Need a file? Get a sample CSV** next to the upload control,
or from the command line:

```bash
npm run sample                                  # sample-trades.csv, 5000 rows
npm run sample -- --rows 100000 --out /tmp/big.csv
```

The sample is deliberately not the demo's own columns. It carries a
DATE, a TIMESTAMP, a BOOLEAN, a key-like integer (`trade_id`,
`year`), a measure integer, a negative-going float, and a book name
containing a comma and a quoted phrase — so opening it exercises the
schema inference and the CSV reader rather than just proving a file
can be read. It is seeded, so regenerating gives the same file.

## Just open a file in the page

Easiest of all: start it, then use the **Open a file** control at the
bottom. Pick a CSV or Parquet and the cube rebuilds around it.

There is no model to write. DuckDB sniffs the schema, a Pure
`Database` / `Connection` / `Runtime` is generated from what it
found, and the planner compiles against that like any other model —
so the SQL panel, the tree and the snap plane keep working with no
second code path. The file never leaves the tab.

Numeric columns become measures, except the key-like ones — `year`,
`*_id`, `zip`, `*_code` — which stay dimensions and keep their digits
unseparated, because `2,019` reads as a bug in the data.

Only the in-browser entry offers this. `index-server.html` plans
against a fixed model on a running legend-lite, where an uploaded
file would have nowhere to live, so the control is absent there
rather than present and broken.

## Your own file, from the command line

```bash
npm start -- --data ~/trades.parquet
```

It serves the file alongside the page and prints a URL that already
points at it. `.parquet` and `.csv` are recognised by extension.

Or point at a URL yourself, if the data is already hosted:

```
http://localhost:8000/demo/index.html?remote=https://host/trades.parquet&format=parquet
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

## Putting it on a real website

There is no server to deploy, so a deployment is a directory:

```bash
npm run dist          # -> datacube/dist/
npx serve dist        # or copy dist/ anywhere that serves files
```

`dist/` is self-contained — the page with its CSS inlined, the
bundles, the model, and the two WebAssembly runtimes. It was checked
under a plain `python3 -m http.server` with no configuration. About
81 MB, of which 36 MB is duckdb-wasm and 4.2 MB the planner; both
compress heavily, so enable gzip or brotli on whatever serves it.

S3, GitHub Pages, nginx, a CDN — anything that serves files will do,
and the URL is the product.

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
