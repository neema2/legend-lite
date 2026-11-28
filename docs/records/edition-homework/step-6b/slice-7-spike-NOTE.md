# Slice 7's spike: what this branch is (2026-11-28)

A scratch branch, not for merge. Its code is the commit `58326d46` ("SCRATCH, slice 7's spike"), whose message says
"NOT BUILT, NOT RUN": that was true when it was pushed. **Since then it was built and run** (2026-11-28, about 10:00
to 10:40 UTC), unchanged, and this note corrects the record.

The results are in `docs/records/edition-homework/step-6b/slice-7-PLAN-2026-11-28.md`, section 4, on the branch
`claude/slice-7-doors-research-bufdvi`. In short, against the baseline `4be6626b`:

- it builds; the relational corpus (DuckDB, H2), PCT channels A and B with lite's lanes, the edge net and the engine
  lane pass as before; `//core:core_edge_tests` passes 1,067 of 1,068 (the one test asserts the table rows the spike
  deletes); lite's `//core:equality_positions_pin_test` passes;
- of about 119,000 SQL statements compared by digest, one moved: `iqrClassifyTest` on DuckDB, the same query with
  its table aliases renumbered;
- it trips five guard tests, by design or by two shortcuts a real slice must not take: the lowering asking
  `compiler.spec.EqualityClasses` which functions are comparisons (it needs no such list: it reads the call's
  reading), and `Scalars.doorsRule` naming a `NamedRule` constant.

It is one cut through every layer to measure the type-checker design. It is not slice B1 to B4 of the plan: it keeps
the five-argument constructor, does not carry the reading at the resolver's three derived comparisons, and leaves
`NumberFamilies` and the named rule in place.
