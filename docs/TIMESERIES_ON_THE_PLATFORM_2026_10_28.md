# Timeseries right and expert on the platform (2026-10-28)

The user, 2026-10-28, after PR #77: "the 3 values, query writer, moving more into lib functions, and inconsistent
options all as next PR as 'do timeseries RIGHT and EXPERT on platform'". Built on `claude/timeseries-platform`, stacked
on PR #77. This note says what each item is, what is decided, and what waits on the user (§5).

## 1. Where things are

- **The maths is the database's**, every figure through DataCube's Pure library `datacube::ts`
  (`datacube/src/pure-library-text.ts`, ~1,700 lines, ~110 functions, generic over relations: S112).
- **The assembly is TypeScript's**: `timeseries-expr.ts` parses the formula language (gs-quant's shorthand,
  `returns(px:close[SPX], 1, log)`), and `timeseries-query.ts` (~2,700 lines) writes the Pure each formula is -- the
  library calls, the periods of a frequency, and the joins and windows that put two series on one set of dates
  (`align`: intersect, step, time, zero). Python and the screen both reach it through DataCube's code (headless).
- **So the platform cannot do what the tile does** without TypeScript: a person's Pure can call every library function,
  but combining two series under an alignment, or writing a formula, means writing what the TypeScript writes.
- **The library reaches the planner only through TypeScript**: its text is a TypeScript string, sent with each query
  that calls it.

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

- **A. Alignment as library functions.** The fills first, each over one relation: `fillZero`, `fillStep` (each date its
  last value at or before, else its first: gs-quant's ffill then bfill), `fillTime` (linear in time between the values
  before and after, `linearAt`), by a line and an order. Then the merge of two series on their line and period (a FULL
  join, each period dated at its latest part's last observation) and one `align(a, b, how: Align)`. The writer calls
  them; the numbers are the gallery's and the query suite's.
- **B. The library as a Pure file.** `datacube/src/pure/datacube_ts.pure` the source; the TypeScript text module and the
  enumerations table generated from it; Java and Python load the same file as a library source
  (`Runner.registerLibrarySource` takes any package but `meta::pure::functions::`). A person's Pure on the platform then
  names `datacube::ts` with no TypeScript in the path.
- **C. `#Quant{...}#`.** An island, as `#SQL{...}#` is (`SpecParser`'s island switch, `protocol/sql/SqlToPure`): the
  formula language parsed in Java and written as a Pure relation expression of library calls -- what
  `timeseries-query.ts` writes today. Its sources named in the island (`#Quant{ px = $prices(date: day, key: symbol);
  returns(px:close[SPX], 1, log) }#`, the shape to settle: §5 D2). Ported by parity: every gallery formula and every
  query-suite case planned both ways, the same SQL's answers required, before the TypeScript writer is deleted.
- **D. The writer thin.** The tile sends `#Quant{...}#` and reads the answer; Python the same; `timeseries-query.ts`
  keeps what is the screen's alone (the points cut to a limit, the columns a drawing reads).
- **E. The rest of the list.** The library on a second database (H2 or Postgres, the query suite's figures); the SQL's
  size; a semantics register row for an enumeration's value equalling its name's string (S6); "B" (a series written in
  Pure, which C makes the same thing); brushing a period to filter.

## 4. Done on the branch

Items 1 and 4 (§2), held by `timeseries_queries_test` (every number unchanged), the typecheck and
`//datacube:update_generated`'s diff tests. `FilterOperator`'s values reordered (`empty` first) so the screen's order is
the library's.

## 5. What waits on the user

- **D1. Does the platform's `#Quant` emit calls to `datacube::ts`?** The world-map tenet (docs/WORLD_MAP.md) has the
  platform ship natives and declarations, no other Pure bodies. `datacube::ts` stays DataCube's (a program: input, never
  ported to Java), so `#Quant` would refuse by name a model without it ("#Quant reads datacube::ts: add the library").
  The alternative -- `#Quant` writing the relation algebra itself -- ports the library's bodies into Java, which the
  tenet forbids. **Recommended: emit library calls; the library a source the platform loads (B).**
- **D2. How an island names its sources.** Recommended: let-bound relations with their date and key said once
  (`px = $prices(date: day, key: symbol)`), as the tile's spec says them, so a formula reads as the tile's does.
- **D3. Order.** Recommended: A, B, C, D, E -- each a commit held to today's numbers, C landing behind a parity test so
  the TypeScript writer is deleted only when nothing differs.
