# @legend-lite/datacube

DataCube in your own application's page. Your application fetches its data and hands it over; DataCube takes it from
there: its page of grids and charts, pivots, filters, calculated columns, joins, exports and saved pages.

Nothing runs on a server. Every query is planned by legend-lite's compiler (WebAssembly) and run by DuckDB
(WebAssembly), both in the page, over the data you handed in.

```js
import { mountDataCube } from '@legend-lite/datacube';
import '@legend-lite/datacube/styles.css';

const rows = await (await fetch('/api/orders')).json();      // your data, fetched your way

const cube = await mountDataCube(document.getElementById('cube'), {
  assets: '/datacube-assets/',                               // where you serve this package's assets/ folder
  data: [{ name: 'orders', rows }],
});

// later
await cube.add({ name: 'customers', format: 'parquet', content: await (await fetch('/api/customers.parquet')).arrayBuffer() });
cube.dispose();
```

## Setting it up

1. **Install it.** The package has no dependencies: what DataCube runs on is inside it.

2. **Serve its `assets/` folder from your application's own origin**, at any path, and say where in `assets`. It is
   static files: copy `node_modules/@legend-lite/datacube/assets` into the folder your site serves as it is (often
   `public/`), or have your build copy it. DataCube fetches from there, each file the first time it is needed, and
   from nowhere else:

   | What | Size | Fetched |
   |---|---|---|
   | DuckDB (`duckdb-eh.wasm` and its worker) | 36 MB | as it starts |
   | DuckDB's extensions (`duckdb-extensions/`) | 10 MB in all | ICU as it starts; JSON, Parquet and httpfs when a file of that kind is read |
   | legend-lite's compiler (`classes.wasm`, its runtime and `planner-worker.js`) | 9 MB | as it starts |
   | The saved pages' rules (`queries/`) | 0.3 MB | at the first Save or Open |
   | Roboto (`fonts/`) | 0.1 MB | when `fonts.css` is used |

   Serve them compressed and cached as you would any static file: they are named by what they are, not by a hash, so
   let their cache lifetime follow your deployment's. The origin must be the page's: a browser starts a worker from no
   other, and `mountDataCube` refuses an `assets` URL on another origin, saying so.

3. **Give the element a height.** DataCube fills the element it is mounted in and scrolls inside it.

4. **Import the styles** (`@legend-lite/datacube/styles.css`). Every rule is scoped to DataCube's own classes, so
   nothing else on your page changes. DataCube's type is Roboto, then the system's sans-serif:
   `@legend-lite/datacube/fonts.css` brings Roboto from the assets folder, for the same look as DataCube's own site.

## What you hand it

Each table is a grid of its own on DataCube's page. A table is one of:

- **Records**: `{ name, rows }`, an array of plain objects. They are written as JSON and read by DuckDB, which decides
  each column's type from its values (a `Date` is a timestamp; a `bigint` stays exact). The simplest for what an API
  call returned; a large table is better handed over as Parquet.
- **A file's content**: `{ name, format, content }`, with `format` one of `'csv'`, `'json'` or `'parquet'` and `content`
  its text or bytes (a string, an `ArrayBuffer`, a typed array or a `Blob`). Parquet is the one to use for large data:
  it is read as it is, with its own types.
- **A `File`**, as a file input or a drop gives one, told apart by its name (`.csv`, `.json`, `.jsonl`, `.ndjson`,
  `.parquet`, each also gzipped as `.gz`).

DataCube reads a table exactly as it reads a file a person opens in it, so everything it does with such a file it does
with yours. The data stays in the page: it is copied into DuckDB there, and goes when DataCube is disposed or the page
is left.

## The API

All of it is `mountDataCube(element, options)`, which resolves to a `DataCube` once the app is open and every table of
`options.data` is on its page. The types ship with the package (`dist/index.d.ts`).

| Option | |
|---|---|
| `assets` | Where the `assets/` folder is served: a URL, absolute or relative to the page, on the page's origin. Required. |
| `data` | The tables to open on. None: DataCube's blank page, which offers to open a file. |
| `windows` | The element DataCube's windows float in and are kept inside. It must be positioned. Default: the mounted element. |
| `askBeforeLeaving` | Have the browser confirm before the page is left while DataCube holds unsaved changes or opened data, as DataCube does in a tab of its own. Default: `false`. |

| `DataCube` | |
|---|---|
| `add(data)` | Another table, a grid of its own beside the page's. Rejected, with the reason, when it cannot be read. |
| `dispose()` | Takes DataCube out of the element and ends its database and its compiler. The element is left empty. |

`mountDataCube` is rejected, with the reason, when DataCube cannot start or a table cannot be read; nothing of it is
then left in the element. One DataCube at a time in a page: dispose one before mounting another.

Nothing else is promised. DataCube's modules, its markup and its class names change from one version to the next.

## What to know

- **Browsers.** The compiler needs WebAssembly garbage collection and exception references: Chrome and Edge 137,
  Firefox 131, Safari 18.4, or later. In an older browser DataCube cannot start, and `mountDataCube` is rejected.
- **Its own things in your page.** DataCube keeps its saved pages in the browser's IndexedDB and its settings in
  `localStorage`, under your origin. It sets no title, reads nothing of your page's address and makes no request but
  for its assets -- and for a remote file a person opens in it themselves.
- **Its keys.** Once a person has clicked in DataCube, some shortcuts pressed anywhere on the page are DataCube's
  until it is disposed: Ctrl/Cmd+E (its query editor), and, outside a text field, Ctrl/Cmd+Z, Ctrl/Cmd+Shift+Z and
  Ctrl/Cmd+Y (its undo and redo).
- **What it leaves out here.** A warehouse to sign in to, saved queries, and the model actions (Create a Class…) each
  need a server or a site your application is not; the app does not offer them.
- **Versions.** Pin an exact version. Until 1.0 a minor version may change what is promised above.

## Licence

Apache-2.0 (`LICENSE`). What legend-lite takes from FINOS Legend, and the notices it carries, are in `NOTICE`; the
third-party software this package redistributes, with its licences, is in `THIRD_PARTY_NOTICES.md`.
