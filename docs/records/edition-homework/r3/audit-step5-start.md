# The independent audit of step 5's start (2026-11-26)

*An independent reviewer (Opus 5.5, high effort) read three commits of the edit branch before they were built:
`0b67a27f` (a row names a rule of the lowering's, `lowering/NamedRule`; `substringAsGiven` and `substrAsGiven` leave
the language), `8f78fe49` (`round` at legend-engine's door) and `0feb5d42` (PCT's legend-engine run's own edition).
It was given the commits and the rules, not the author's conclusions. Its findings as it sent them, set out, with
what was done about each.*

| # | Severity | Finding | Done |
|---|---|---|---|
| 1 | Blocker (a test fails) | `EngineTable` now names `Pure.AT_MATH_ROUND`, so the generated claims ledger's `also` column for `round`'s three overloads becomes `EngineTable;Scalars`; the committed file said `Scalars` | regenerated (`bazel run //:update_generated`), committed with the build |
| 2 | Should fix | Under PCT's own edition every `substring`, `substr`, two-argument `indexOf`, `toOne` of empty and every unchecked kind takes the database's meaning, so the legend-engine lane moves well beyond `round`; its pins were not regenerated and the moved checks were not listed | re-recorded; each of the 24 moved rows read and listed in the plan record's 6a and in the commit |
| 3 | Should fix | `round` in a store query turns from an answer into a refusal at legend-engine's door on every database but DuckDB and H2; amendment 22 rules only "both databases" | stated in register S422 and brought to the user (the plan record's 6b, item 10) |
| 4 | Note | `SqlTyping` types `ROUND` but not `ROUND_HALF_UP`; a Float `round(x, n)` at the door loses its DOUBLE fact | fixed: the corpus's SQL size check found the cost the same day (an extra `CAST(... AS DOUBLE)` around a Float `round(x, 2)`, 14 characters a plan on DuckDB); `ROUND_HALF_UP` of a DOUBLE is typed DOUBLE, which every dialect's form answers (ClickHouse's, which rounds in a decimal, casts a double's back; the product writes it over decimals alone, so none of today's SQL moves) |
| 5 | Note | The new architecture rule matches accesses only (a class literal evades it) and read the product's jars only | the rule reads core, the hosts' doors and the edge, the edge's classes let through; the class-literal limit is named in its text |
| 6 | Note | The two claim pins removed were the user's; the dated comment cites no approval to retire them | brought to the user (6b, item 9); the comment says so |
| 7 | Note | With the signature test deleted, nothing checked that a row's rule fits its call's arity | `EngineTablePlacementTest.eachRowsRuleLowersEachOverloadItMaps`: every overload of every row planned at the door |
| 8 | Note | Stale sentences ("PCT keeps UPSTREAM", the plan's "`substring` to `substringAsGiven`"), an unused import | fixed |

**Checked and found sound by the reviewer:** no reference to the removed symbols outside comments anywhere in the
repository; the first commit changes no behaviour (no feature rule or styled rule keyed on the substitutes, the same
decision label, `trustOne`'s rule is its first argument); the rows' overload order matches the declared groups;
`ROUND_HALF_UP` is the plain `ROUND` on DuckDB and H2 and `round` reaches the lowering by one path; `PureText`'s
statics initialise in order; PCT's storeless runtime declares its lane's database, so the "no single database"
refusal is not reached; the resolver's stamping of the model's code is gated by the edition.

**Not verifiable without a build, by its own account:** compilation, the databases' actual answers, the JSON's
spacing, javac's synthetic class names. The build and the lane answered these the same day: nine target groups pass,
and lite's answers on the moved rows equal legend-engine's manifests'.
