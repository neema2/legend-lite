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

In a component that may be taken down while DataCube is still loading (a React effect, a route change), give the mount
a signal, so it can be stopped before it has resolved:

```js
useEffect(() => {
  const stop = new AbortController();
  let cube;
  mountDataCube(ref.current, { assets: '/datacube-assets/', data, signal: stop.signal })
    .then((mounted) => { cube = mounted; }, (e) => { if (e.name !== 'AbortError') report(e); });
  return () => { stop.abort(); cube?.dispose(); };
}, [data]);
```

## Setting it up

1. **Install it.** The package has no dependencies: what DataCube runs on is inside it.

2. **Serve its `assets/` folder from your application's own origin**, at any path, and say where in `assets`. It is
   static files: copy `node_modules/@legend-lite/datacube/assets` into the folder your site serves as it is (often
   `public/`), or have your build copy it. DataCube fetches these from there, each the first time it is needed:

   | What | Size | Fetched |
   |---|---|---|
   | DuckDB (`duckdb-eh.wasm` and its worker) | 36 MB | as it starts |
   | DuckDB's extensions (`duckdb-extensions/`) | 10 MB in all | ICU as it starts; JSON, Parquet and httpfs when a file of that kind is read |
   | legend-lite's compiler (`classes.wasm`, its runtime and `planner-worker.js`) | 9 MB | as it starts |
   | The saved pages' rules (`queries/`) | 0.3 MB | at the first Save or Open |
   | Roboto (`fonts/`) | 0.1 MB | when `fonts.css` is used without a bundler |

   Serve them compressed and cached as you would any static file: they are named by what they are, not by a hash, so
   let their cache lifetime follow your deployment's. The origin must be the page's: a browser starts a worker from no
   other. `mountDataCube` checks the folder before it starts anything, and refuses one on another origin or one that
   is not there, naming the file it missed -- also when your site answers a missing file with its own page, as an
   application's router does.

3. **Give the element a height.** DataCube fills the element it is mounted in and scrolls inside it.

4. **Import the styles** (`@legend-lite/datacube/styles.css`). Every rule is scoped to DataCube's own classes, so
   nothing else on your page changes. DataCube's type is Roboto, then the system's sans-serif:
   `@legend-lite/datacube/fonts.css` brings Roboto, for the same look as DataCube's own site (through a bundler, the
   font files are your build's to place; linked as a file, they come from the assets folder beside it).

## What you hand it

Each table is a grid of its own on DataCube's page. A table is one of:

- **Records**: `{ name, rows }`, an array of objects, one a row. They are written as JSON and read by DuckDB, which
  decides each column's type from its values: a `Date` is a timestamp, `NaN` and the infinities are null, a nested
  object or array is a column of that shape. A `bigint` stays exact, in a browser that has `JSON.rawJSON`; in one
  that has not, the table is refused. The simplest for what an API call returned; a large table is better handed
  over as Parquet.
- **A file's content**: `{ name, format, content }`, with `format` one of `'csv'`, `'json'` or `'parquet'` and `content`
  its text or bytes (a string, an `ArrayBuffer`, a typed array or a `Blob`). Parquet is the one to use for large data:
  it is read as it is, with its own types.
- **A `File`**, as a file input or a drop gives one, read by its name's ending (`.csv`, `.json`, `.jsonl`, `.ndjson`,
  `.parquet`, each also gzipped as `.gz`). A name with another ending is refused.

DataCube reads a table exactly as it reads a file a person opens in it, so everything it does with such a file it does
with yours -- and it shows it as one: a table named `orders` is the file `orders.json` in its grid's title and in a
saved page. The data stays in the page: it is copied into DuckDB there, and goes when DataCube is disposed or the page
is left.

A page's only grid shows all its rows. A grid added beside others shows up to its row limit, 1,000 until changed, as
on DataCube's own pages.

## The API

All of it is `mountDataCube(element, options)`, which resolves to a `DataCube` once the app is open and every table of
`options.data` is on its page. The types ship with the package (`dist/index.d.ts`).

| Option | |
|---|---|
| `assets` | Where the `assets/` folder is served: the folder's URL, absolute or relative to the page, on the page's origin. Required. |
| `data` | The tables to open on. None: DataCube's blank page, which offers to open a file. |
| `signal` | An `AbortSignal` that stops a mount which has not resolved: the mount is rejected with the signal's reason, nothing is left in the element, and the page may mount again at once. |
| `windows` | The element DataCube's own windows float in and are kept inside (Open…, the Generated Pure & SQL). It must be positioned. Default: the mounted element. |
| `askBeforeLeaving` | Have the browser confirm before the page is left while DataCube holds unsaved changes or opened data, as DataCube does in a tab of its own. Default: `false`. |

| `DataCube` | |
|---|---|
| `add(data)` | Another table, a grid of its own beside the page's. Tables added together are read one after another. Resolves once it is on the page; rejected, with the reason, when it cannot be read, when it did not land (the person put another page in DataCube meanwhile, or kept a page it would have replaced), or when DataCube was disposed meanwhile. |
| `dispose()` | Takes DataCube off the page -- its element's content, its windows, menus and dialogs -- and ends its database and its compiler. The element is left empty. |

`mountDataCube` is rejected, with the reason, when DataCube cannot start, a table cannot be read, or its signal is
aborted; nothing of it is then left in the element. One DataCube at a time in a page: a second mount is refused until
the first is disposed or aborted.

Nothing else is promised. DataCube's modules, its markup and its class names change from one version to the next.

## What to know

DataCube was written to have a browser tab to itself. Mounted in your page it keeps to its element where it can, and
this is where it does not:

- **Its menus and dialogs** are laid over the whole document, not kept in its element: a dialog (Open a data
  source, Save, Join) dims the page behind it until it is closed.
- **Its keys.** While a grid is on DataCube's page, these keys pressed anywhere on your page are DataCube's: Ctrl/Cmd+E
  (its properties window; in your text fields too), and, outside a text field, Ctrl/Cmd+Z, Ctrl/Cmd+Shift+Z and
  Ctrl/Cmd+Y (its undo and redo) and `?` (its list of shortcuts).
- **Touch.** From its first mount on, and after it is disposed, DataCube listens for long presses on the whole
  document, for its right-click menus: a finger held still for half a second raises a `contextmenu` event on what it
  pressed, and the click of its release is not delivered.
- **Storage.** It keeps its saved pages in the browser's IndexedDB (a database named `datacube`) and its settings in
  `localStorage` (`dataCube.settings`), under your origin; at the first Save it asks the browser to keep the origin's
  storage. A saved page keeps each grid's query and layout, not your data: opened again, DataCube asks the person for
  the file of a grid whose table it does not hold.
- **Names on the page.** Its windows have ids (`#pure`, `#sql`, `#sharelink` among them), which is why a page holds
  one DataCube at a time; while mounted it also sets a few names on `window` that begin `__data`, which its own
  tests read.
- **What it leaves out here.** A warehouse to sign in to, saved queries, and the model actions (Create a Class…) each
  need a server or a site your application is not, and are not offered. Its blank page still names a warehouse
  among the sources it can open; the picker it opens offers none.

It sets no title, reads nothing of your page's address, and makes no request but for its own module and assets -- and
for a remote file a person opens in it themselves.

And:

- **Browsers.** The compiler needs WebAssembly garbage collection and exception references: Chrome and Edge 137,
  Firefox 131, Safari 18.4, or later. In an older browser DataCube cannot start, and `mountDataCube` is rejected.
- **Bundlers.** The module is ES modules with its lazy parts as chunks beside it (ECharts is fetched the first time a
  chart draws), and a bundler takes it as it is. What it loads from the assets folder it loads by URL as it runs, and
  says so in the comments webpack and Vite read, so neither tries to bundle it. Only its names are minified: your
  bundler does the rest.
- **Versions.** Pin an exact version. Until 1.0 a minor version may change what is promised above.

## Licence

Apache-2.0 (`LICENSE`). What legend-lite takes from FINOS Legend, and the notices it carries, are in `NOTICE`; the
third-party software this package redistributes, with its licences, is in `THIRD_PARTY_NOTICES.md`.
