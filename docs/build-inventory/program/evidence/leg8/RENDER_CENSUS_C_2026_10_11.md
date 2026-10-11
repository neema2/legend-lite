# Leg 8 step 2 (c): the render census and the census, main against the change (2026-10-11)

The change: a relation column written without a multiplicity is `[0..1]`, as legend-engine and legend-pure read it
(`docs/PROTOCOL_PROGRAM_2026_10_05.md` leg 8; the Compiler Rewrite line's (c)). Before: main at `741f24f9a`. After:
`leg8-records` at `89759ccf5` (that commit on top of `741f24f9a`). Both trees measured the same hour, one after the
other, on one machine.

## The render census

`docs/execution-plan-boundary-2026-10-05/render-census/run.sh` on each side (every JVM lane: `//gates:core`,
`//gates:stress`, the four PCT lanes, `//spec:corpus_duckdb`, `//spec:corpus_h2`), then its `compare.py`:

    52567 (lane, dialect, kind, text) entries; 48 differ

`render_pairs.py` (this folder) masks a lambda's scope id (`fn:<hex>:<length>`; `resolver/FunctionBodyRows.scopeId`
hashes the typed lambda's print, and the print now shows the `[0..1]` on the lambda's written row type) and compares
again:

    with scope ids masked: 12 entries differ

So 36 entries (both corpus lanes, DuckDB and H2) differ in a scope id alone. The 12 left are six statements, each
once before and once after, all in `//gates:core` on DuckDB: a filter comparing a column whose written type is now
`[0..1]` picks the `[0..1]` overload of the comparison, and the lowering writes its guard.

| before | after |
|---|---|
| `WHERE t0."2011__\|__total" > 3000` (a pivot, then a cast to a written relation type) | `WHERE (t0."2011__\|__total" IS NOT NULL AND t0."2011__\|__total" > 3000)` |
| `WHERE t0."2011__\|__total" > 1000` (the same, another pivot) | `WHERE (t0."2011__\|__total" IS NOT NULL AND t0."2011__\|__total" > 1000)` |
| `WHERE _tds0.age > 30` (a TDS, its lambda's row type written) | `WHERE (_tds0.age IS NOT NULL AND _tds0.age > 30)` |
| `SELECT t0.AGE_VAL ... WHERE t0.AGE_VAL > 30` | `... WHERE (t0.AGE_VAL IS NOT NULL AND t0.AGE_VAL > 30)` |
| `SELECT t0.FIRST_NAME, t0.AGE_VAL ... WHERE t0.AGE_VAL > 30` (rendered 3 times) | `... WHERE (t0.AGE_VAL IS NOT NULL AND t0.AGE_VAL > 30)` (3 times) |
| `... WHERE t0.AGE_VAL > 30 AND t0.FIRST_NAME = 'Bob'` | `... WHERE t0.AGE_VAL IS NOT NULL AND t0.AGE_VAL > 30 AND t0.FIRST_NAME = 'Bob'` |

Each comparison stands directly in the WHERE, or in a conjunction there; none is negated or on one side of an OR. So
each selects the same rows: a missing value is excluded by the bare comparison's NULL and by the guard's `false`
alike. Under a NOT or in a disjunction the two would differ (the guard's `false` negated includes the row); no such
statement is in the census. legend-engine renders the bare comparison (`docs/SEMANTICS_REGISTER.md` S40, read from
its code): the guard is a difference of SQL text, recorded in S40. No cast moved and no projection gained a guard.

## The census

`//spec:manifest_world_census` on each side: identical, every line but the date.

    # modules=27 files=1772 loadWalls=27 duplicates=0 bodies OK=16133 FAILED=1120 WALLED=32
    # by reason: {kernel=218, normalize=50, other=595, overload=108, unknown-function=3, unknown-property=15, unknown-type=131}

No body newly fails and none newly types.
