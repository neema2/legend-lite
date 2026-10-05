# DataCube: save, load and share (#21) — plan, 2026-09-28

A saved cube is upstream's `DataCubeSpecification`, and one spec moves unchanged between every
place it can live: the browser's own store, a share link, a single file, and legend-lite's
server. The spec writer and reader are the core; everything else is a place to put a spec.

Standing rulings that bind: upstream APIs only, exact shapes; types come from the compiler;
Relation API only in what we build; users over upstream parity, never lose functionality; user
identity always, no service accounts; everything through Bazel; no Java dependencies; no
trademarks in names.

## Where we were (before milestone 1)

- Save View / Load View: one `localStorage` slot, our own `SavedView` format (version 3).
- Only the snapshot, the open paths, the column order and formats are saved, and order and
  formats are not restored on load (audit P2-70). The rest of the configuration (titles,
  colours, fonts, widths, hidden columns, display names, heatmaps, grid lines, pins) is never
  saved. Tree sort, pivot-total aggregates, leaf counts and kept grouped columns revert (P2-71).
- Export › "DataCube Specification" writes that `SavedView` JSON under upstream's name, and
  nothing can open it again (P2-90).
- No reader from Pure query text to a cube; no store endpoints; no Load / Save / Delete
  dialogs; no `/:id` route; lite does not serve `grammarToJson/valueSpecification` (E3).

## Step 0 — upstream, as read (DONE 2026-09-28)

Read at legend-studio `c5b2f2c78` (the census pin; `/Users/neemsandv/legend/legend-studio`) and
legend-engine 4.145.0 (`~/legend/legend-engine`). Paths below are under
`packages/legend-data-cube/src/stores/core/` unless named.

### The spec (`model/DataCubeSpecification.ts`)

```
DataCubeSpecification {
  query: string                       // the PARTIAL query: see below
  configuration?: DataCubeConfiguration
  source: PlainObject                 // raw source JSON, tagged by `_type`
  options?: { autoEnableCache?: boolean }
  dimensionalTree?: DataCubeDimensionalTree
}
```

- `query` is the cube's composition WITHOUT its source: upstream builds the full query over a
  dummy `''` source, prints it, and strips the leading `''->` (`DataCubeEngine.getPartialQueryCode`).
  So a saved query reads `select(~[a, b])->groupBy(...)->sort(...)->limit(500)`.
- The spec a view saves = source (unchanged from open) + configuration (from the snapshot) +
  partial query code + dimensional tree (`view/DataCubeViewState.generateSpecification`).
- Serialization is serializr: on READ, fields it does not know are DROPPED silently; on write,
  only schema fields go out. Our extras survive our own round trip, not upstream's.

### The configuration (`model/DataCubeConfiguration.ts`)

- Cube level: `name`, `description`, `columns[]`, grid lines and colour, `gridMode`, fonts
  (family, size, bold, italic, underline, strikethrough, case), `textAlign`, the four
  foreground and four background colours, alternate rows (on, standard mode, colour, count),
  `showSelectionStats`, `showWarningForTruncatedResult`, `initialExpandLevel`,
  `showRootAggregation`, `showLeafCount`, `treeColumnSortDirection`, `pivotStatisticColumnName`,
  `pivotStatisticColumnPlacement`, `pivotLayout {expandedPaths[]}`, `dimensions {dimensions[{name,
  columns[]}]}`.
- Per column: `name`, `type` (precise primitives converted to plain), `kind`, `displayName`,
  number format (`decimals`, `displayCommas`, `negativeNumberInParens`, `numberScale`,
  `missingValueDisplayText`, `unit`), fonts, alignment, the eight colours, `isSelected`,
  `hideFromView`, `blur`, widths (`fixedWidth`, `minWidth`, `maxWidth`), `pinned`,
  `displayAsLink`, `linkLabelParameter`, `aggregateOperator`, `aggregationParameters`,
  `excludedFromPivot`, `pivotSortDirection`, `pivotStatisticColumnFunction`.
- The open tree already has a home: `pivotLayout.expandedPaths`.

### The reader (`DataCubeSnapshotBuilder.ts`)

- Accepts exactly one composition, in this order (anything else is refused, naming the
  supported composition):
  `extend()* → filter() → select() → [sort()→pivot()→cast()] → [groupBy()→sort()] → extend()* →
  sort() → limit()`. `extend(over(...))` (window columns) is accepted.
- No `select()` means no column is selected.
- A pivot is `sort → pivot(~[keys], ~[aggregates]) → cast(@Relation<(the pivoted columns)>)`:
  the cast lists the pivot's result columns, i.e. the values found when it was saved.
- Aggregates and filters are matched against upstream's own operation lists:
  - aggregates `sum avg count min max uniq first last var var_sample std std_sample strjoin`
    (median and wavg are commented out upstream);
  - filters: `= != < <= > >= in, not in, is null, is not null, contains / starts / ends (and
    not), case-insensitive = != contains starts ends`, and the column-to-column forms.
- Then the configuration is VALIDATED against the query, strictly: the same column set, in the
  same order (select, group-extended, then the rest), the same `type` and `isSelected` per
  column, the same `kind` for pivot and group columns, the same `aggregateOperator` (and
  compatible parameters) when aggregated, the same `excludedFromPivot` and
  `pivotSortDirection` when pivoted, the tree sort direction when grouped, and no dimension
  left un-excluded from a pivot. No configuration: one is generated from the query.
- Conformance suite, ready made: `__tests__/DataCubeQueryRoundtrip.data-cube-test.ts`, 290 cases
  (163 refusals, each with upstream's exact message).

### Sources

- `localFile` (`legend-application-data-cube/src/stores/model/LocalFileDataCubeSource.ts`):
  `{_type:'localFile', fileName, fileFormat:'csv', _ref, columnNames}`. Reopening a saved one
  ASKS THE USER FOR THE FILE again, checks its columns match `columnNames`, and re-ingests it
  (`LocalFileDataCubeSourceLoaderState`). Only CSV upstream (Parquet etc. are a TODO there).
- `freeformTDSExpression`: `{query (Pure text), runtime, mapping?, model}`.
- `userDefinedFunction`: `{functionPath, runtime?, model}`.
- Also `legendQuery`, two lakehouse kinds, the REPL's, and a cached source (`db, schema, table,
  count, model, runtime`) behind `options.autoEnableCache`.
- Upstream's app reopens only `localFile` through a loader; others are processed directly.

### The store (legend-engine `legend-engine-application-query`)

- Record `DataCubeQuery {id, name, description, content (the spec), owner, createdAt,
  lastUpdatedAt, lastOpenAt}`; `PersistentDataCube` is the client's twin.
- `POST query/dataCube/search` with `QuerySearchSpecification {searchTermSpecification
  {searchTerm, exactMatchName, includeOwner}, limit, showCurrentUserQueriesOnly,
  sortByOption: SORT_BY_CREATE | SORT_BY_VIEW | SORT_BY_UPDATE, …}`: returns a LIGHT projection
  (`id name owner createdAt lastUpdatedAt lastOpenAt`, no content); the current user's first,
  then the sort, then the limit (capped). Search term matches id, name, or owner.
- `GET dataCube/batch?queryIds=` (at most 50), `GET dataCube/{id}` (stamps `lastOpenAt`),
  `POST dataCube` (the CLIENT supplies the id; a taken id is refused; owner forced to the
  current user), `PUT dataCube/{id}` (id change refused; owner-only, an unowned one is claimed),
  `DELETE dataCube/{id}` (owner-only). Also `dataCube/events` and `dataCube/stats`.
- Upstream stores it in MongoDB.

### Routes and dialogs (`legend-application-data-cube`)

- Route `/:dataCubeId?`: with an id, the cube is fetched from the store and its source loaded.
- `?sourceData=<url-safe base64 of the raw SOURCE JSON>`: builds a NEW cube on that source (the
  launch point other apps use). It does not carry a whole cube.
- Delete confirmation: type `<user>_<yyyyMMdd>` (anonymous: `_<yyyyMMdd>`).

### Ours, checked

- lite parses and types `pivot(...)->cast(@Relation<(...)>)` (`TypeInferenceIntegrationTest`).
- lite does not yet serve E3 (`grammarToJson/valueSpecification`), which reading a partial
  query needs; E4 prints lambdas byte for byte with upstream.

## Decisions (the user, 2026-09-28) — these REPLACE the upstream-format plan that followed step 0

1. **Our own clean format.** A saved cube is OUR versioned document, designed the way we think is
   right; a legacy `DataCubeSpecification` is read by a one-way TRANSLATOR (milestone 2). Opening
   our cubes in upstream DataCube does not matter. Step 0 becomes the translator's specification
   and the superset checklist (gap found: first/last aggregates).
2. **What matters:** opening EXISTING saved cubes in ours, and running ours against legend-engine
   proper — both milestone 2, designed with T9 (sources).
3. **Milestone 1** works in legend-lite's closed ecosystem so people save NOW, built on the step-0
   homework so milestone 2 needs no rework. Then 1b (sharing), then 2.
4. **A pivot is saved as intent** (`pivot(~[keys], ~[aggs])` where query text is written; never the
   values found); a legacy `sort → pivot → cast` is rewritten to ours, never run as is.
5. **The store is upstream's API and record** (`DataCubeQuery`); `content` is our document.
6. **Read access**: the database enforces data access under the reader's identity; the store does
   not gatekeep reads.
7. **Model versions are pinned**, with an easy upgrade path. **Schema drift** reconciles and reports,
   never refuses: a new column is available but hidden; a part that uses a column that is gone is
   left out and named (no auto-save); a changed type is the compiler's, reported.
8. **A saved cube never stores data.** A file is named by identity (name, format, size, SHA-256,
   columns). Reopening: a sample is rebuilt from its seed; a kept file handle (File System Access,
   Chrome/Edge) is read from where it was picked, after one click if the browser asks; otherwise the
   user is asked for the file. The handle stays in this browser, never in a document or a share.
9. **Local files first** (no model home needed: the model is derived from the file); model-backed
   cubes follow the model home (`docs/MODEL_HOME_2026_09_28.md`).

## The cube document, version 1 (`datacube/src/cube-document.ts`)

```
{
  kind: "datacube.cube", version: 1, name,
  source: { _type: "file", name, format: "csv"|"parquet"|"json", size, sha256, columns[{name,type}],
            sample?: { id, rows } },
  query:  the cube's definition: columns (with the settings the cube set on each), derived and
          groupDerived (lambdas as exact protocol JSON), filter, rows, pivotOn, pivotValues,
          pivotSort, pivotTotal, keepGroupedColumns, leafCount, childCount, measures, sorts,
          treeColumnSort, maxRows — read WHOLE (no allow-list; unknown fields carried);
          never the relation (derived from the source on open), the epoch or the row window
  configuration: differences from the product defaults only (null = a default unset)
  tree: { open: typed row paths, showTotals }
  ...fields a newer writer added, kept verbatim
}
```

Refused with a message: not JSON, not a saved cube, a legacy specification ("not built yet"), a newer
version, an unknown source kind or file format.

## Milestone 1, step 1 — local-file cubes end to end: DONE 2026-09-28

- `src/cube-document.ts` (write, read, reconcile on open), `src/cube-store.ts` (upstream's store rules
  once, over memory or IndexedDB records; content kept as exact JSON text), `src/file-handles.ts`,
  `src/ui/cube-library.ts` (the Cubes window: save, save as new, search, sort, open, delete,
  open a cube file), the page's flow in `demo/boot.ts` (host menu "Cubes…").
- The one-slot Save View / Load View and `src/persist.ts` are deleted; Export › "Cube File (JSON)"
  writes the document (offered only when the cube knows its source).
- Proof: `test/cube-document.test.ts` (every snapshot field round-trips — a `Required<CubeSnapshot>`
  fixture, so a new field cannot be forgotten), `test/cube-store.test.ts` (upstream's rules),
  `test/cube-open/cube-open.ts` (the real app over DuckDB-WASM and the planner: save, re-read the
  file, a FRESH app shows the same typed values; a lost column and a new one reconcile as ruled),
  `bazel run //datacube:verify_cubes` (a real browser: save, reload, open asking for the file, the
  same values; a sample cube reopens with no question; delete) 5/5; `verify_features` 170/170.
- Not yet: the file handle path cannot be driven by the harness (Playwright hands files to an
  `<input>`, which keeps no handle) — covered by reading only; Ad Hoc Analysis state and named
  dimensions are not saved yet.
- "Changed since saved" (2026-09-28): the cube's definition and settings against the baseline it was
  saved or first opened with (not its name, not its open rows): a `•` in the page title and "unsaved
  changes" in the Cubes window, refreshed as each view lands; the browser's leave-page guard; opening
  another cube or file over unsaved changes asks; a cube opened with parts left out is changed from
  the start, and Save over it first says what the saved copy would lose (save anyway / save as new).
  Found on the way: the app told its host about a view BEFORE taking it in, so a host read it one
  view behind -- the host is now told last. Proof: `test/cube-library.test.ts`, `verify_cubes` 7/7.
  A single "cube changed" event is Leg B's (one state owner); until then the check runs per view.

## Milestone 1b — sharing: step 0 and the plan (2026-09-28)

**The unit is the page** (user ruling, 2026-09-28): what is shared is exactly what Save writes,
`app.pageDocument` (`page-document.ts`) -- the cube's own document inside it, its charts and layout.

**Upstream, as recorded in step 0:** upstream shares a LINK TO A RECORD in its shared server store
(route `/:dataCubeId`), and `?sourceData=` launches a NEW cube on a source. Our store is each
browser's own IndexedDB, so a record id means nothing to anyone else -- until legend-lite serves the
store (milestone 2, with T9), when an id link becomes the upstream-shaped share.

**Measured (probes, 2026-09-28):**
- DuckDB 1.5.4 (the app's duckdb-wasm) writes a page into a Parquet file's own key-value metadata
  (`COPY ... (FORMAT parquet, KV_METADATA {'datacube.page': '<json>'})`) and reads it back exactly
  (`parquet_kv_metadata`), quotes and all. The file stays a plain Parquet table any reader opens.
- A realistic page (12 columns, grouped + pivoted, 3 measures, a calculated column, a filter,
  per-column formats, two charts and a layout) is 2,969 bytes of JSON; deflated and base64url'd it
  is a **935-character** link fragment.

**The three ways, one share sheet:**

1. **A link** -- the page, deflated, in the URL's FRAGMENT (`#p1.<data>`, as built: step 1 below): never sent to any server,
   nothing stored anywhere. Opening it opens the page; its source is found as a saved cube's is (a
   sample rebuilt from its seed; a file asked for, and matched by its fingerprint). So a link is
   complete for samples (and model-backed sources, later) and "bring the same file" for a file.
2. **One file, data included** -- a Parquet file of the cube's source table with the page in its
   metadata: open it and you have the page AND its rows, nothing else needed. Opening one reads the
   rows as a new file source (the page's cube is reconciled with them, as any reopen is).
3. **The page alone** -- the JSON file Export writes today (no data), for keeping or version control.

**The share sheet** (one window, from the title bar menu and the Cubes window): the three, each
saying what it carries -- "a link: the layout and settings, not the data; they need trades.csv
(18 MB)" / "a file with all 120,000 rows of trades.csv" -- plus the system share where the browser
has one.

**Decisions for the user (recommendations):**
- A. *Big pages in a link:* the fragment carries the page; above a size that chat tools mangle
  (decided at step 1: 2,000 characters, where Outlook and Teams start cutting), the person is told
  and offered the file instead. (Server-stored id links come
  with the server store, milestone 2.)
- B. *What the shared file carries:* the whole source table the cube reads -- so the recipient has
  the same cube, every drill and filter still works -- never only the rows on screen. The sheet
  says how many rows and what file they came from, before the file is made.

## Milestone 1b, step 1 — the link: DONE 2026-09-28

**Decided (the user, 2026-09-28, after measuring the alternatives):** the link is the saved page's
JSON exactly as Save writes it (`pageToJson`), raw-deflated at level 9 against a PRESET DICTIONARY,
base64url, after the `#`: `#p1.<data>`. The column lists that could be derived again stay in (the
user: not worth dropping for a tiny saving, and a link read by eye explains itself).

- **The dictionary** (`src/share/link-p1.ts`) is text the compressor may refer back to; the app
  ships it and never sends it. It is GENERATED once, from the product's own tables through its own
  writers (`tools/link-dictionary/make.ts`; the next version is cut by `bazel run //datacube:cut_link_dictionary`): the
  document's keys and kinds (a template page using every kind of thing a page holds, empty), the
  protocol's node shapes and every calculated-column, window and core function by name, the filter
  operators, the compiler's types and aggregates, the chart marks. It holds no user text (pinned: the
  only names in it are the placeholders `''`, `a`, `x`). Every column and format setting is listed
  there as `Required<...>`, so a setting added later fails the build there, to be weighed for the
  next version.
- **Frozen per version.** `p1` names the dictionary; its bytes are pinned by their sha256 in
  `test/share-link.test.ts` and never change, so every p1 link opens forever. A new vocabulary is
  `p2`, alongside. A link of a version this build does not know is refused by name ("made by a newer
  DataCube"); a damaged or cut-short one says so; nothing half-read ever opens.
- **Deflate from fflate 0.8.3** (the one dependency, approved): the browser's `CompressionStream`
  cannot take a preset dictionary.
- **Budget 2,000 characters.** Over it, the link still works and the person is told, with the file
  offered instead.
- **Opening one:** the host reads `#p1.` at load, takes it out of the address (a reload never
  reopens it over later work), and opens the page the way a saved one opens: a sample is rebuilt with
  no question, a file is asked for and matched by its fingerprint.
- **Encoded, not encrypted:** anyone holding the link can read the page, filter values included --
  said in the message when the link is copied.

**Measured (full URLs, `test/share-link.test.ts` diagnostics and the browser check):**

| page | characters |
|---|---|
| the harness's sample page, grouped (real browser) | 484 |
| realistic 12 columns, 4 calculated columns, filter, formats, open rows, a chart, layout | 868 |
| the same with 60 columns | 1,371 |
| 12 columns + a filter of 2,000 hand-typed ids | 9,713 (flagged long) |

Weighed and not taken: binary encodings of the page (bigger than JSON + dictionary once deflated,
and a second format to keep), the query as Pure text (bigger than protocol JSON against a protocol
dictionary). Deferred to a v2 if a real page needs it: a structure-aware coder, a trained model,
values by rank, a diff from a known base (estimated floor ~170-250 characters for the realistic page).

**Proven:** `share_link_test` (exact round trip incl. Unicode, quotes, newlines and exact decimals;
loud failures; the frozen hash; the budget) and three browser checks in `verify-cubes.mjs`: a
sample page's link opens the same typed values in a fresh tab with no question; a file page's link
asks for the file, then shows the same values; a damaged link says so and opens nothing. The
browser check found a real bug before it landed: the link opened before the setup had declared the
sample rebuild, so opening now runs last in the setup.

For now the entry point is **Copy Share Link** in the host menu; the share sheet (step 3) replaces it.

## Next

- Milestone 1 step 2: model-backed cubes on the model home's first slice (pointer, `demo:trades:1.0.0`).
- Milestone 1b: step 2, one Parquet file with the page in its metadata; step 3, the share sheet.
- Milestone 2 (with T9): the legacy translator, upstream sources, legend-engine proper.

## What this closes

Census §1, §F "open a cube from Pure query text", §H (routes, dialogs, what a save holds, delete,
owner); audit P2-70, P2-71, P2-90; contract C3, E3, Q2, S1, P6.
