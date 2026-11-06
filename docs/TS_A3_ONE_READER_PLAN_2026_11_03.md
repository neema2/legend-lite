# A3, one reader: the plan (2026-11-03, for the user to rule)

**The goal** (`docs/TS_V1_PROGRAM_2026_11_01.md` A3; the one-tree plan's D1): core's `#TS` parser is the only reader
of a formula. The tile's own grammar (`datacube/src/timeseries-expr.ts`), its signatures (`TS_SIGNATURES` in
`timeseries-spec.ts`) and its translator (`timeseries-ts.ts`) are deleted, and a guard keeps a grammar from coming back.

## 1. What is there today (measured 2026-11-03)

- **A series is stored as the tile's syntax tree.** `TsSeries.expr: TsExpr` (`timeseries-spec.ts:81`) is in every
  page document (version 6). The reader checks the tree's shape against `TS_SIGNATURES` (`exprProblem`, `:624`).
  There is no upgrade code: a page of another version is refused (`page-document.ts:674-678`).
- **Python shows the trees.** `Timeseries.series` returns them (`python/legend_lite/page.py:1998`), and three Python
  tests assert them. Python's input is already text.
- **Every example is written as text** (`s(id, text)` in `examples/markets.ts:73`: about 195 series), so none holds a
  tree literally.
- **The tree is walked in about 40 places.** 15 analyses in `timeseries-expr.ts` (unit, level, bounds, band, candles,
  condition, candles from a column, problems, the `$A` references, the columns and sources read, inlining, two
  printers), and about 25 more walkers in the query writer, the option builder, the editor, the spec reader and the
  view. `inlined` (a series reading another, `$A`) alone has about 25 call sites.
- **The formula box** is a plain input. Each keystroke parses it, and the box shows one error at one character. It
  has no completions: a function picker and a static list stand in for them. Sliders rewrite the outermost call's
  numbers.
- **Core has the parts, not the entry points.** It parses a whole query (`TsParser.parse`), not one item. It stops
  at the first error. Its catalog of words (`TsCatalog`) is exported at build time into `datacube/src/generated/
  ts-words.ts`, which nothing reads yet. Nothing in core knows a series' unit, level, band or candles.

## 2. The steps (each leaves the tests green and the frozen answers unchanged)

1. **The facts in the library.** A word's unit (same, percent, ratio, number, index), whether it keeps or makes a
   level, its bounds (RSI's 0 to 100), and whether it draws a band or candles become tags on the word in
   `projects/timeseries/model.pure`. Core's catalog reads them. Today they are in `WORDS` and `TS_SIGNATURES`, in
   TypeScript. *Pure decides; nothing here is Java's to know.*
2. **Core reads one item.** A new entry `TsParser.item(text)` reads one `#TS` item. It is offered through the planner
   the tab already holds, as a route like Show #TS's (`/api/lite/v1/grammar/tsItem`). It answers:
   - the item's canonical text;
   - the sources, columns and `$` references it reads;
   - its facts from step 1 (unit, level, band, candles, bounds, condition);
   - or its error, with a range and the words that could come next (completions).
   The tile keeps its grammar meanwhile. A test runs every example series both ways and requires the same answers
   (the frozen answers, and the same facts).

   *As built (2026-11-03):* the route answers the item's canonical text and core's own tree as data (`TsTree`: each
   node its kind `k`, its place `at`, its parts), or the error placed at a line and column. The facts are not in the
   answer: step 1 put them in the catalog the tile already reads (`ts-words.ts`), keyed by the word, so a node's
   `call` name finds them. The sources, columns and `$` references are the tree's `column` nodes, read by the tile.
   Completions wait for step 3, with the formula box that shows them. The proof is a round trip: every example series'
   item (`tsItemText`) read by core prints back as itself (`timeseries-ts-parity.test.ts`); core's own test reads a
   span, a window of everything, a subscript and an option, and refuses two items.

   *The keystroke's cost, measured (2026-11-03, the risk §4 names):* a read takes 0.51 ms at the median, 0.70 ms at the
   90th percentile and 3.68 ms at the slowest of the 210 items, the first 1.69 ms -- in-thread WebAssembly in Node, as a
   tab's module answers (the parity test prints it). The box can ask core on every keystroke.
3. **The tile stores text.** `TsSeries.text` replaces `TsSeries.expr`. The page document becomes version 7 (§3,
   decision 1). Python's `Timeseries.series` returns text. Every walker moves to the answer of step 2, one module per
   commit:
   - the query writer: text in, the `#TS` statement out (no translator);
   - the option builder;
   - the editor;
   - the spec reader;
   - the view.

   The formula box asks core on each keystroke, debounced, and shows the error's range and the completions.

   *The way in (2026-11-03):* the tile maps core's tree to its own `TsExpr` first, so the about 40 walkers move
   unchanged and the tile's parser is the first thing deleted; each walker then moves to the facts in the catalog.
4. **The problems from the compiler.** `timeseriesProblems` (a column missing, not a number, a date that is no date)
   becomes the compiler's own messages: the series' query typed through `relationType`, which the tile already calls.
   Only what is the tile's own stays in TypeScript: a series with no plot, and a split that differs.
5. **Delete and guard.** `timeseries-expr.ts`'s grammar, `timeseries-ts.ts` and `TS_SIGNATURES` are deleted. A guard
   fails if `datacube/src` declares a `#TS` tokenizer or a word table again.

**Done when** those files are gone, and these all hold: the frozen answers, the tile's suites, the Python suites, and
the bundle and cold-start budgets. Each step re-measures the formula box's keystroke cost and the tile's first plan.

## 3. Decisions for the user

1. **Pages saved before the change.** Version 6 pages hold trees.
   - (a) Refuse them, as every older version is refused today. You said on 2026-10-31 that nobody uses the tile
     yet, and every example is built from text.
   - (b) Upgrade them once, from tree to text, while the tree printer (`exprSource`) still exists, then delete the
     upgrade with the grammar.
   - *Recommended: (a).* It is today's rule, and it has no code to keep.
2. **Where a word's facts live** (step 1).
   - (a) Tags on the word in the Pure library.
   - (b) A table in Java beside the catalog.
   - *Recommended: (a).* The tenet is "Java parses and binds; Pure decides", and the facts are the words' own.
3. **Completions in v1.** The box today has none.
   - (a) Word names and their parameters, from the catalog.
   - (b) Also the grid's columns and instruments, which the box reads from the grid already.
   - *Recommended: (b).* It costs little more than (a), because the columns are already on the screen.

4. **What `/` gives where the divisor is 0** (step 3: the box's text is `#TS`, R10). The tile writes `a / nullif(b, 0)`:
   nothing there. An item's per-point expression means what Postgres means (the spec, principle 2): an error, and the
   whole chart fails. gs-quant (pandas) gives infinity.
   - (a) The tile's: `#TS`'s `/` gives nothing at 0, on every database; a row of `docs/SEMANTICS_REGISTER.md`.
   - (b) Postgres's: an error; a formula that can divide by 0 writes `nullif` itself.
   - *Recommended: (a).* Under (b) a chart of `close / volume` fails whole wherever one instrument has no volume (the
     sample's EURUSD), and nothing in a chart can show infinity.
5. **What a comparison gives** (`ma50 > ma200`). The tile draws a condition as the periods it holds, and its value is
   1 or 0, so `and`, `or` and arithmetic work on it. Postgres and gs-quant both give true or false.
   - (a) True or false, as both upstreams: the tile draws true as held; arithmetic on a condition writes `CASE`.
   - (b) 1 or 0, the tile's.
   - *Recommended: (a).* Both upstreams agree, and the spec's examples already write `ma50 > ma200 AS golden_cross`.

Two more questions about the text are already answered by the spec's §7 and §8, so they are not asked here: a series
is named by its item's name (`AS ma50`, which another series reads as `ma50`; the tile's `$A`), and a column taken by
its period is written as its aggregate (`sum(volume['AAPL'])`, the tile's `.sum`), a bare column taking its grid's.

## 4. Cost and risk

- **Size: large.** About 40 walkers, the document version, Python, and about 40 test targets that import the two
  files.
- **Riskiest: the keystroke cost.** Each keystroke becomes a call into the planner. It is measured in step 2 before
  anything depends on it. If it is slow, the box parses on a pause, not on every key.
- **The order is safe.** Steps 1 and 2 change no behaviour and can land alone. Step 3 is where the document changes.

## 5. Status

| Step | State |
|---|---|
| 1. The facts in the library | Built 2026-11-03: the `timeseries::Words` profile's tags and stereotypes on each word; core's catalog reads them; a test requires the tile's table to say the same |
| 2. Core reads one item | Built 2026-11-03: `TsParser.item`, `TsTree`, the route `/api/lite/v1/grammar/tsItem`, `LitePlanner.tsItem`; the round trip over every example series |
| 3. The tile stores text | Decisions 4 and 5 answered 2026-11-06 (4(a) built in core, `497292554`, register S151, pending the user's ruling; 5(a) is SQL's own meaning, no change). The step itself is not started: two facts it needs, measured 2026-11-06, are below (§6) |
| 4. The problems from the compiler | After 3 |
| 5. Delete and guard | After 4 |

## 6. Step 3's two open facts (measured 2026-11-06)

1. **The read is asynchronous.** A page is read synchronously at all five of its call sites (`readSaved`: the app's
   open, open-text and example open, the headless `pageRead` and `pageOpened`), but core's reader is the planner's,
   and in a browser the planner runs on a worker. So a page of version 7 holding text cannot give the tile its trees at
   read time: the opening must hydrate them, `await`ing core for each series' text, before the tile's walkers run.
   Every call site is already in an async function (`openSaved` is async; Python's headless bridge awaits a
   JavaScript promise, `_headless.py` `call`), so the hydrate step fits each; the edit commands that rewrite a tree
   (`renameSource`) print the text again from it.
2. **A series that reads another.** The tile writes a series' #TS item with every `$A` it reads inlined
   (`tsItemText`: `inlined(series.expr, ts.series)`), so that text is no series' own: stored, a series reading
   another would lose the reading (edit the one, the other keeps the old formula). The stored text has to be the
   language's own naming -- a series named by its item (`AS ma50`), read by that name (`ma50 - ma200`; the spec's
   §7) -- which the tile's writer does not write today, and core's tree reads as a column the hydrate maps to a
   `ref` where the name is another series'.

So step 3 starts with a writer of a series' own item text (its references by name, its source's columns as the item
reads them) proved by a round trip -- every example series' own text, read by core and mapped, the tile's tree again --
before any document changes version.
