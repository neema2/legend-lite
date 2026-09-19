# Blocker: `pivot` over a class-sourced relation is not resolvable

Found while wiring the client to the real planner. This is on the
critical path for DataCube and is not a dialect problem.

## What happens

`POST /engine/plan` with the shape DataCube actually produces -- a
`project` over a mapped class, then a `pivot`:

    demo::Trade.all()
      ->project(~[region:x|$x.region, year:x|$x.year,
                  notional:x|$x.notional])
      ->pivot(~[year], ~[total:x|$x.notional:y|$y->sum()])

returns

    500 {"error":"class query under TypedPivot is not resolvable yet
         (H2 vocabulary)"}

## It is not H2-specific

The message reads as though it were, and that was the first guess. It
is not: the same query against a **DuckDB** runtime
(`type: DuckDB; specification: DuckDB { }`) fails identically.
`"(H2 vocabulary)"` is fixed text on the error, not a condition.

## Where it comes from

`resolver/StoreResolver.java:617-623`, the `default ->` arm of
`resolveNode` -- a deliberate "NAMED wall" that throws for any typed
node with no explicit case rather than silently passing it through:

    // The NAMED wall: an ANCHORED variant with no arm — loud, never
    // a silent pass-through; a USER CALL wrapper names its CALLEE
    default -> throw new NotImplementedException("class query under "
            + n.getClass().getSimpleName()
            + ...
            + " is not resolvable yet (H2 vocabulary)");

`TypedPivot` has no arm there. The wall is working exactly as designed;
the arm simply has not been written.

## Why this matters here

Pivot works over TDS and relation literals -- `PivotCheckerTest` passes
23 tests over `#TDS` fixtures -- because those never go through store
resolution. A DataCube query does: it projects over a mapped model and
then pivots, so it lands in `StoreResolver` and hits the wall.

So the earlier conclusion that "the hardest engine piece already
exists" holds for the lowering, the IR node and the two-phase dynamic
staticization, but **not** for the path the product actually uses. The
missing piece is a `TypedPivot` arm in `StoreResolver`.

## Second, smaller finding

The static-value form is documented on `TypedPivot` as
`pivot(~col, [v…], ~agg…)`, but

    ->pivot(~[year], [2023, 2024], ~[total:x|$x.notional:y|$y->sum()])

is rejected with

    no overload of 'pivot' matches 4 argument(s) of these shapes —
    candidates: [meta::pure::functions::relation::pivot/3,
                 meta::pure::functions::relation::pivot/4]

so `pivot/4` exists but this argument shape is not it. Since v1 uses
the dynamic (full-width) form, this is not blocking, but the pinned
form will need its real spelling established before the column
windowing escape hatch can be built.

## Verified separator, while here

legend-lite's own passing tests assert generated pivot column names of
the form `2011__|__total`, `2012__|__count`, and `USA__|__NYC` for a
multi-dimension key -- i.e. it uses `__|__` between value and measure,
matching DataCube's `PIVOT_COLUMN_NAME_VALUE_SEPARATOR`.

DuckDB's native `PIVOT` instead names columns `2021_notional`. The
client's header builder therefore splits on the **known measure
names** rather than on a separator, which is correct for either
producer. See `datacube/src/grid/columns.ts`.
