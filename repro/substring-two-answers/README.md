# `substring` returns a different string in SQL than in memory

**F37's property-mapping half withdrawn (2026-10-29).** A relational mapping's `substring` is the relational
dynafunction, which every dialect extension writes as SQL's `SUBSTRING`; legend-pure's `substring` is a different
function of the same name, as `splitPart` is (the user's reading, `docs/STRESS_CORPUS_PLAN_2026_10_19.md`, D9). So
`stress::F37_SubstringSql` now asserts `lpha` and passes, beside `F37_SubstringPure`'s `ph`. F37 stands for a query's
`substring` answering SQL's, as a TDS `extend`'s does (`scripts/corpus/repro/substring-tds-extend/`). The original
report follows.

    substring('alpha', 2, 4)

    in memory   "ph"      0-based start, exclusive end   (Java String.substring)
    in SQL      "lpha"    1-based start, third arg = LENGTH

Both are this engine, on one string, with one pair of arguments.

## The two services

`core/src/test/resources/stress/77-substring-paths.pure` runs each path separately:

| service | source | evaluator | result |
| --- | --- | --- | --- |
| `stress::F37_SubstringPure` | JSON payload | in-memory M2M | `ph` — **passes** |
| `stress::F37_SubstringSql` | DuckDB table | pushed into SQL | `lpha` — fails |

The in-memory service passing is the load-bearing part. It means the corpus's expectation is
the engine's own semantics, so the relational answer is not a convention this corpus failed
to anticipate — it is the same engine contradicting itself.

## Two false starts, both worth recording

**Chaining M2M onto a relational source proves nothing.** The first version of this repro put
both columns in one row, an M2M mapping reading a relational source, expecting `ph` and
`lpha` side by side. Both came back `lpha`: a model chain over a relational store pushes the
expression *down into SQL*, so both halves of the comparison were the same half. Isolating
the in-memory evaluator requires a source with no store behind it.

**Reaching it costs a change of query shape too.** A JSON-fed model connection rejects
`project(...)` with *"Found unexpected connection type for TabularDataSet Query"*, so the
in-memory service has to use graph fetch. Between the source change and the query change,
nobody compares these two paths by accident.

## Cause

`duckdbExtension.pure` passes the arguments straight through:

    dynaFnToSql('substring', $allStates, ^ToSql(format='substring%s', ...)),
    // TODO - pure uses 0-based indexing, duck db returns location with 1-based index,
    // keeping this as H2 also returns 1-based currently, many user tests need to be fixed

SQL's `substring(string, start, length)` then reads Pure's `(start, end)` as `(start, length)`.
The TODO names the index base; the third argument is a second divergence on top of it.
