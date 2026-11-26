# Step 8, slice 1: the shortest-Float-text node leaves the product (2026-11-26)

*Written by the step 8 session (`claude/edition-step8`), started from `claude/modest-galileo-h6swr4` at `9c751659`.
The step's specification is `../r3/research-step8-presentation.md`; this is its section F, slice 8.1.*

## What it was

`SqlFn.FLOAT_SHORTEST_TEXT` was a node of the product's SQL tree: a Float's shortest round-trip text made plain
inside two windows (a whole value below 1e38, a fractional one from 1e-17 below 2^53), NaN and the infinities by
Java's names. The product carried for it:

- `sql/dialect/FloatShortestText.java` (184 lines): the composition over the database's text of a double, and the
  pass that expanded the node at render;
- an arm in each of the 12 dialects' `call` switch;
- two hooks on the base renderer, `spellsFloatShortestText` and `readsLongTextOnce`;
- Snowflake's own spelling of it from the double's digits (`floatShortestText`, `javaNonFinite`) and BigQuery's
  override that bound the text once.

Since B2b's S5a to S5c (2026-11-25) no product code wrote the node. Its one writer was the edge:
`edge/EdgePureOnlyTexts.representationOf`, legend-pure's `%r` and `toRepresentation` of a Float. The product
refuses both calls by name.

## What changed

- The composition moved, unchanged, into `edge/EdgePureOnlyTexts` (`floatPrint`): the edge composes the text at
  lowering from the product's own nodes. This is what B8 did for `FORMAT_PADDED`, `INSTANCE_TEXT` and
  `REPRESENTATION_TEXT` (2026-11-17). No new edge file.
- Deleted from the product: the SqlFn constant, `FloatShortestText.java`, the 12 arms, the two hooks, the pass's
  place in `AnsiSqlRenderer.renderPasses`, `floatShortestTextByCast`, Snowflake's spelling and BigQuery's override,
  and the constant's entries in `SqlTyping`, `SqlFn`'s may-skip set, `MySql`'s pass-through list and SQLite's
  operand set.
- Lines, main sources (`git diff --stat`): see "The numbers" below.

## What was measured

A temporary probe (never committed) ran on the base and on the change:

1. **The edge's SQL, planned for 12 databases** (11 planned; MongoDB is not a database a relational connection can
   name, so its seven were not planned). Seven queries each over a table with a DOUBLE column: `format('%r', [$r.F])`,
   the same over `$r.F * 2.0`, over the literal `3.14`, `$r.F->toRepresentation()`, two `%r` of one computed column,
   a filter on a computed column's `toRepresentation`, and `%r` of `sqrt($r.F)`.
   - **Byte-identical on DuckDB, H2, Postgres, SQLite, MariaDB, MySQL, Trino, ClickHouse and Databricks**, all seven
     queries (63 statements), and on Snowflake and BigQuery for the literal (2). The worry that the lowering's copy
     budget would now see the composition's copies and isolate a computed column did not happen on any of these.
   - **Longer on Snowflake and BigQuery where the operand is a double** (12 statements). Their product dialects read
     the double's long text once; the composition reads it about nine times:

     | Query | Snowflake before | after | BigQuery before | after |
     |---|---|---|---|---|
     | `format('%r', [$r.F])` | 6,384 | 47,727 | 2,738 | 10,468 |
     | `format('%r', [($r.F * 2.0)])` | 6,411 | 48,456 | 2,757 | 10,981 |
     | `$r.F->toRepresentation()` | 6,382 | 47,725 | 2,708 | 10,438 |
     | two `%r` of one computed column | 12,800 | 97,038 | 5,474 | 11,082 |
     | a filter on the representation | 6,459 | 48,504 | 2,769 | 10,993 |
     | `format('%r', [sqrt($r.F)])` | 6,491 | 50,616 | 2,857 | 13,681 |

     (characters of the plan's statements). This is the edge's SQL, for legend-pure's `%r` of a Float that is not a
     literal, on two databases. The product's SQL is not touched: it refuses `%r`. No pin covers these sizes.
2. **The edge's answers on DuckDB, H2 and SQLite** for 14 expressions (`%r` and `toRepresentation` of literal and
   computed Floats, inside and outside the windows): the same on the base and on the change, every one. They are
   pinned now in `PureOnlyTextTest.theEdgeWritesPuresRepresentationOfAFloat`.

## Found on the way, not this slice's

- **SQLite answers `toRepresentation(1.0e20)` as `9223372036854775807.0`** at the edge, before this slice and after
  it: the composition's whole-value branch casts through a 64-bit integer there. legend-pure prints
  `100000000000000000000.0`. The new test does not pin SQLite's answer for it.
- Outside the two windows the text is each database's own exponent form (`1e-20` on DuckDB, `1.0E-20` on H2,
  `1.0e-20` on SQLite), where legend-pure prints plain digits. Known since the node was written; the new test pins
  each lane's as measured.
- The plan record at `9c751659` has no section 7a.

## The guards that moved, each with its dated note

See the commit; the list is in the session's report.
