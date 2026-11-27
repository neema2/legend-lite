# The landing of step 6.2: every recorded file it moves, and why (2026-11-27)

*The edition program's step 6, slice 6.2: values whose type is known only at run time. This page is the dated reason
for each pin, golden and ledger the slice re-records. A guard file moves only with a dated reason (AGENTS.md).*

## The rulings it stands on

The user, 2026-11-26 about 23:10 UTC, on the decisions D1 to D5 put with upstream's measured answers
(`upstream-step6/answers.md`): **"I agree with the framing"**: lite's own rule at lite's doors, upstream's behaviour at
legend-pure's and legend-engine's doors, each as a row of the door's table. On legend-pure's door following
interpreted legend-pure where compiled differs: "I'm a bit nervous on the pure one bc I think most things run in
compiled but it's fine to start this way".

## What the slice does

| | Before | After |
|---|---|---|
| Numbers of different types in a mixed list (`[3, 'a']->contains(3D)`) | refused by name | false: the type is part of the value (D1; register S127) |
| A value typed only at run time (a `Number`, an `Any`) against a mixed value | refused | compared by the type it holds, read from its SQL type where that is a plain column or a widening cast; any other shape still refused by name |
| Equal numbers of different types in a sort | no rule (the database's) | lite: by type, Integer, Float, Decimal; the other two doors: as written, as upstream (D3b; S428) |
| `max`, `min`, `greatest`, `least` over such a tie | the first written | lite: the higher type for the greatest, the lower for the least; the other two doors: upstream's element (the last written for a number list's `max` and `min`, the first for `least` and the generic `min`, the left of two arguments) (D2; S428) |
| The host judge's sort | ties in any order | ties by type, so both judges agree (D4) |
| A mixed list's sort on BigQuery | refused by BigQuery (an ORDER BY over a STRUCT) | the key written field by field |

Not in this slice, each carried to where it is built: `<`, `<=`, `>`, `>=` between values typed at run time (D3) and a
literal against a `Number` inside a function (D5) need the typer to mark the body: slice 6.3. A value carried as JSON
compared by its JSON kind, and an `Any` list read as a tagged list: with the one owner of `==` (slice 6.4), where the
assert's planning shows which shapes are met. An abstract Date's text and the tolerance assert: slice 6.6. A table's
cells counted by an assert: decision D6, open.

## Product SQL size

`core/src/test/resources/lean-sql/baseline.tsv`, the sort family (four queries a database):

| Database | Characters before | after |
|---|---|---|
| MySQL | 16,826 | 7,941 |
| MariaDB | 11,257 | 5,459 |
| SQLite | 9,798 | 4,038 |
| BigQuery | 8,365 | 3,800 |
| Snowflake | 6,072 | 2,367 |
| ClickHouse | 3,295 | 3,043 |
| Postgres | 2,783 | 2,659 |
| Databricks | 1,728 | 1,352 |
| Trino | 1,706 | 1,678 |
| DuckDB | 1,107 | 1,072 |
| **H2** | 1 statement, 39 characters, 3 refused | **2 statements, 1,818 characters, 2 refused** |

Core's own sort query: MySQL 169,581 to 40,331; MariaDB 116,624 to 27,484; SQLite 69,361 to 21,961; Snowflake 52,039
to 16,357; every other database down.

**The one row that goes up is H2's:** a mixed list's sort that H2 refused is now planned and answers (PCT's
`testMixedSortNoComparator` passes on H2's legend-engine lane where it was refused). It is a query that had no SQL,
not a query whose SQL grew. **Put to the user with the slice.**

`max`, `min`, `greatest`, `least`: the same character counts on every database (the tie is chosen by permuting the
written candidates when the query is planned, no SQL added).

The doors' rows move no size row (measured on the merged head against its base: 3,745 rows of the core baseline and
626 of the corpus's, none differing).

`pct/.../channelb/ratchets.tsv`: essential judge characters 902,304 to 901,459.

## Tests whose answer moves

- PCT on H2, legend-engine's lane: `meta::pure::functions::collection::tests::sort::testMixedSortNoComparator` from
  refused to pass (`pct/engine_lane/h2_essential.tsv`, `refusals_h2.tsv`, `summary_h2.tsv`).
- PCT on H2, lite's own lane: the same test from an expected failure ("UNNEST reached a dialect without an unnest
  placement") to pass; its entry leaves `Test_LegendLite_H2_EssentialFunctions_PCT` (84 pinned to 83). Found by CI
  on the pull request (`linux / pct_h2`, `macos / pct_h2`): the local focused run did not include H2's lite lane.
- No other recorded suite: PCT's expected failures, the corpus rosters, the edge goldens and the edge corpus are
  regenerated unchanged. PCT on DuckDB and channel B: no verdict line differs with the doors' rows.

New tests: `exec/TaggedListTest` (numbers of different types never equal in a mixed list; a value typed at run time
compared by the type it holds; equal numbers sort in one order), `exec/TaggedSelectionTest` (lite's tie rule),
`edge/UpstreamTiesTest` (each door's answers, case by case against `answers.md` groups B and E),
`testexec/PureAssertsTest` (the host judge's tie), `sql/dialect/BigQueryDialectTest` (the struct key).

## Guards moved, each dated where it stands

| Guard | What moves | Why |
|---|---|---|
| `JavaEvalLedgerTest` `testexec/Equality.java` | 319 to 320 | the host judge's sort breaks a tie of value by type (the user's word on D4, 2026-11-26) |
| `JavaEvalLedgerTest`, the edge's class register | + `PureTable.java`, `UpstreamTies.java` | data: a map from a function to the name of a rule the lowering holds |
| `ArchitectureTest.PLANNER_ENTRIES` | + `NamedRule#SORT_TIES_AS_WRITTEN`, `#EXTREME_KEEPS_THE_FOLDS_TIE`, caller `edge.UpstreamTies` | the two rules the compatibility doors' tables name |

## Recorded differences (register)

- S127 amended: inside a mixed list too, numbers of different types are never equal, at every door (compiled
  legend-pure answers `[3, 'a']->contains(3D)` true; no PCT or corpus test holds the case).
- S428, new: where equal numbers of different types land. **One difference at legend-engine's door is posted to the
  user to rule:** inside a query sent to a database, legend-engine lets the database choose the result's type, so
  `max` of an Integer column and a Float column answers a Float even where the Integer was kept; lite answers the
  kept element in its own type.

## What judged it

The local focused set on the merged head and CI on the pull request (every operating system, both corpora, PCT on
H2). The database lanes and the outside audit: see the plan record's log for this landing.
