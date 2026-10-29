# Timeseries right and expert on the platform (2026-10-28)

The user, 2026-10-28, after PR #77: "the 3 values, query writer, moving more into lib functions, and inconsistent
options all as next PR as 'do timeseries RIGHT and EXPERT on platform'". Built on `claude/timeseries-platform`, stacked
on PR #77. This note says what each item is, what is decided, and what waits on the user (§5).

## 1. Where things are

- **The maths is the database's**, every figure through DataCube's Pure library `datacube::ts`
  (`datacube/src/pure-library-text.ts`, ~1,700 lines, ~110 functions, generic over relations: S112; since step B the
  Pure file `datacube/src/pure/datacube_ts.pure`, the TypeScript module written from it).
- **The assembly is TypeScript's**: `timeseries-expr.ts` parses the formula language (gs-quant's shorthand,
  `returns(px:close[SPX], 1, log)`), and `timeseries-query.ts` (~2,700 lines) writes the Pure each formula is -- the
  library calls, the periods of a frequency, and the joins and windows that put two series on one set of dates
  (`align`: intersect, step, time, zero). Python and the screen both reach it through DataCube's code (headless).
- **So the platform cannot do what the tile does** without TypeScript: a person's Pure can call every library function,
  but combining two series under an alignment, or writing a formula, means writing what the TypeScript writes.
- **The library reached the planner only through TypeScript** (until step B): its text was a TypeScript string, sent
  with each query that calls it.

## 2. The four items

1. **One source for the library's option values** (done here, §4): `datacube/src/generated/library-enums.ts`, generated
   from the library's `Enum datacube::ts::...` declarations by `datacube/tools/library-enums.mts`, diff-tested by
   `//datacube:update_generated`; the catalog's words (`timeseries-spec.ts`) and the writer's check
   (`timeseries-query.ts libraryOption`) read it. A changed enumeration changes the screen's words; a stale copy fails
   the build.
2. **The query writer onto the platform** (§3, decisions in §5).
3. **More of the assembly as library functions** (§3, step A): alignment first.
4. **Consistent options** (done here, §4): `excessReturns`, `totalReturn`, `basket` and `periodsPerYear` take
   `timeUnit: DurationUnit[1]` -- the unit their numeric time column counts in, as `projection`'s does -- where they
   took `perDay: Integer[1]` (1 or 86400); the database works out a day's units (`dateDiff(%1970-01-01, %1970-01-02,
   $timeUnit)`).

## 3. The plan, in steps, each held to today's numbers

- **A. Alignment as library functions (done, §4).** The fills, each over one relation by a line, an order and a marker
  (the column that says a date is the part's): `fillZero`, `fillStep` (each date its last value at or before, else its
  first: gs-quant's ffill then bfill), `fillTime` (linear in time between the values before and after, `linearAt`), and
  the pieces they are made of, which the writer calls so a part's columns share one set of windows (`lastValued`,
  `firstValued`, `nextValued`, `valueAt`, `orElse`: a window more each is a level more DuckDB's optimizer plans through,
  §5.6 of the design note). The merge itself is a join: a person's Pure writes it with Pure's own `join` and fills the
  rest by name, and answers as the tile's `align` does (`timeseries-queries.test.ts`, "two calendars joined"). The
  writer's merge stays composition (which rows meet which), C's to write.
- **B. The library as a Pure file (done, §4).** `datacube/src/pure/datacube_ts.pure` the source; the TypeScript text
  module (`pure-library-text.ts`) and the enumerations table generated from it, each diff-tested. The platform takes it
  as a `###Pure` section after a model's own, as the page sends it; nothing of the platform's own changes. A person's
  Pure on the platform then names `datacube::ts` with no TypeScript in the path: `//core:timeseries_library_test`
  compiles the file whole and runs a person's Pure over it on every in-process database (DuckDB, H2, SQLite).
- **C. The timeseries query language (2026-10-29: docs/TIMESERIES_LANGUAGE_2026_10_29.md).** A SQL dialect for
  timeseries -- `SELECT … FROM (prices WHERE …) ON date BY symbol SAMPLE BY 1w FILL(PREV) FILTER …` -- whose meaning is
  its expansion into plain SQL, then Pure through the existing translator, the library's functions found by name as
  user-defined functions. The tile's formula box holds one select item; Python's API maps onto the clauses unchanged.
  It replaces the `#Quant` sketch and §5's D1-D3. Tested on paper before it is built (that document's §13); then ported
  by parity: every gallery formula and every query-suite case planned both ways, the same answers required, before the
  TypeScript writer is deleted.
- **D. The writer thin.** The tile sends `#Quant{...}#` and reads the answer; Python the same; `timeseries-query.ts`
  keeps what is the screen's alone (the points cut to a limit, the columns a drawing reads).
- **E. The rest of the list.** The library on a second database (H2 or Postgres, the query suite's figures). B's test
  found the first gap: a calendar period back (`lagPeriod`, and every `*Period` function on it) is an `asOfJoin`, and
  H2's and SQLite's dialects write none -- refused by name, never answered wrong; the portable rewrite (`AsOfAsPick`, a
  left join keeping each row's latest match) is Trino's alone, so giving it to them is a dialect change of its own. The
  SQL's size; a semantics register row for an enumeration's value equalling its name's string (S6); "B" (a series written in
  Pure, which C makes the same thing); brushing a period to filter.

## 4. Done on the branch

Items 1 and 4 (§2), held by `timeseries_queries_test` (every number unchanged), the typecheck and
`//datacube:update_generated`'s diff tests. `FilterOperator`'s values reordered (`empty` first) so the screen's order is
the library's. Step A (§3): the fills and their pieces, the writer on them with every alignment number unchanged
(163 tests), and a person's own Pure answering as the tile's step and time. Step B (§3): the library a Pure file, the page's module and the
enumerations written from it (the text byte for byte as before); `//core:timeseries_library_test` (the core lane)
compiles it whole and runs a person's Pure on DuckDB, H2 and SQLite -- a mean, returns simple and log, volatility, a
month's number and the three fills answer as arithmetic on all three; a week's returns on DuckDB, refused by name on the
other two (step E).

## 5. What waits on the user

*Superseded 2026-10-29.* D1-D3 below were the `#Quant` sketch's questions. Working through them with the user led to
the query language in docs/TIMESERIES_LANGUAGE_2026_10_29.md: the core never names DataCube's package (the library's
functions are found by name, as SQL's user-defined functions are), a formula's context is the query's own clauses, and
the order is: test the language on paper, then build it behind a parity test. Kept as the record of how it started.

- **D1. Does the platform's `#Quant` emit calls to `datacube::ts`?** The world-map tenet (docs/WORLD_MAP.md) has the
  platform ship natives and declarations, no other Pure bodies. `datacube::ts` stays DataCube's (a program: input, never
  ported to Java), so `#Quant` would refuse by name a model without it ("#Quant reads datacube::ts: add the library").
  The island writes the composition -- the joins, periods and order of the calls, as `#SQL{...}#` writes relation
  algebra -- and every figure's maths is a library call. The alternative, writing the figures' maths in Java, ports the
  library's bodies, which the tenet forbids. **Recommended: emit library calls; the library a source the platform loads
  (B).**
- **D2. How an island names its sources.** Recommended: let-bound relations with their date and key said once
  (`px = $prices(date: day, key: symbol)`), as the tile's spec says them, so a formula reads as the tile's does.
- **D3. Order.** Recommended: A, B, C, D, E -- each a commit held to today's numbers, C landing behind a parity test so
  the TypeScript writer is deleted only when nothing differs.
