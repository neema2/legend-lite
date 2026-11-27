# Step 6, "one equality": what changes, in plain words (2026-11-27)

For the user's review before step 6 lands. The detail, the evidence and every ruling's words are in
`step-6-one-equality-2026-11-27.md` (same folder) and in `docs/SEMANTICS_REGISTER.md` rows S127, S428 and S429.

**The idea of the step.** Before it, a query's `==` and a test's `assertEquals` were two pieces of code that could
disagree. Now one rule answers both.

## 1. What changes on lite's own route

| # | What you write | Before | Now | Whose decision |
|---|---|---|---|---|
| 1 | `1 == 1D`, `2 < 2.5D`, `[3, 3D]->removeDuplicates()` | a compile error (three number types) | compared by value: whole numbers and Decimals are one family | yours, 2026-11-27 ("Yes I like the rule, keep ordering strict") |
| 2 | `1 == 1.0`, `$i < $f` (a whole number or Decimal against a Float) | a compile error | still a compile error, ordering included; a written literal still adapts when nothing is lost (`$x.qty > 0` over a Float) | same ruling |
| 3 | a sort or `max` of a list mixing `3` and `3.0` | a tie placed Integer, Float, Decimal | a tie placed exact number first, then Float; equal exact numbers keep their written order | same ruling |
| 4 | a function generic over its caller's columns that compares two of them, called with columns of two types | the database's comparison, unchecked | a compile error at that call, naming the function | yours ("the split with compile error when we know") |
| 5 | a function whose parameters are declared `Number`, `Any` or `Date`, called with two types | the database's comparison | compared when the query runs: two types are never equal | same ruling |
| 6 | `assertEquals(1, 'a')`, any assert across two known types | judged, and failed | a compile error, like the `==` it means | yours, 2026-11-26 ("yes compile error") |
| 7 | `$p.n == $p.m` where both are declared `Number` but one column is whole numbers and the other floating point | the database's own conversion | refused when the query is planned, naming the fix; a written literal still adapts | yours ("great do it") |
| 8 | `$t.at == %2020-01-01`, `$t.at >= %2020-01-01` where `at` is declared `Date` and its column holds date-times | the database's comparison (the day read as midnight) | refused when the query is planned, for "the same" and for "the later" alike | yours ("error way if it's only for lite mode"; "we don't really want greater-than to work and equal not") |
| 9 | `$amount * 0.1`, `$amount + 0.2` where `amount` is a Decimal | floating-point maths under a type that says Decimal (`0.010000000000000002`) | exact: the written `0.1` is read as the Decimal 0.1, which is Pure's answer | yours ("good let's do it") |
| 10 | a computed list compared as a query's answer: `[1, 2]->filter(...) == [1, 2]` | different answers or an error depending on the database | the same answer on every database | the step's design |
| 11 | `[] == [1, 2]->filter(x \| $x > 5)` | false, or an error on H2 | true: an empty list is an empty list | the step's design |
| 12 | two maps written in different orders, as a query's answer | not equal | equal, as Pure and the assert say | found by probing, 2026-11-27 |
| 13 | two tables with `==`, as a query's answer | column names and row order ignored | strict: the column names must match and the rows must be the same in order. A table with no defined order (no sort; or after a grouping, a join or a union) is refused when the query is compiled, naming the two fixes | yours ("Yeah or ~= if want to be cute, or setCompare()") |
| 13b | `sameElements($a, $b)`, as a query's answer | did not exist | new: two lists or two tables compared in any order. Duplicates count (`[1, 1, 2]` and `[1, 2, 2]` are not the same). A function, not an operator; named after `assertSameElements` | same decision |
| 14 | two instances with `==` | compared as one piece of text of their keys (`1.0D` differed from `1.00D`; the declared class, not the actual one) | the actual class, then each key by its value | the step's design |
| 15 | `date(2015, 4, 16, 14) == %2015-04-16T14` inside an expression | a database error | compared by the instant | a defect fixed |
| 16 | a list mixing days and date-times, passed through `map`, then `contains` | a wrong "true" on DuckDB | refused by name, as the assert already did | a defect fixed |

A note on row 8. It is about a type that only a COLUMN gives. Where the query's author said "decide when the query
runs" (a parameter declared `Date`, or a written `->cast(@Date)`), nothing can be refused in advance: a day and a
date-time are then never equal, and are ordered by value, the one ordering ruled on 2026-11-26 for values typed
only at run time.

Two fixes that are not about equality but were found by it:
- "First day of the week / month / quarter / year" is a day. The database always returned a day; lite had it
  recorded as a date-time.
- A day held behind a `Date` type, moved by days, weeks, months or years, stays a day.

## 2. What does not change

- **legend-engine's door and legend-pure's door keep the answers they had** for numbers (`==`, `in`, `contains`,
  `removeDuplicates`, the orderings, sort ties, `max`/`min`), for comparisons of whole lists, maps and tables, and for
  the Java checker's handling of ties. Two outside reviewers looked for any change there; the two they found are
  fixed.
- Three things do change at a door:
  - legend-pure's door now answers a comparison written inside a function as legend-pure itself does (your "yes we
    should measure and hopefully fix").
  - The exact-decimal arithmetic of row 9 applies at every door ("2 okay A"). At legend-engine's door that differs
    from legend-engine on H2, SQLite, MariaDB and MySQL: recorded as difference 24.
  - At legend-engine's door, `max` and `min` of an Integer column and a Float column now answer a Float, as
    legend-engine does inside a query sent to a database ("great to match engine but only if not too complex and
    costly"). Only that pair: it is the one that was run on legend-engine.

## 3. SQL size

- The whole size table is 490,000 characters smaller than before the step (13.96 million to 13.47 million).
- 45 rows go up because a query that was refused now runs (an Integer against a Decimal, on each database).
- 4 rows go up with longer SQL: PCT `let` on H2 (126,502 to 136,762), core `equal` on SQLite (24,280 to 27,271),
  core `let` on DuckDB (136,469 to 137,877) and on SQLite (29,965 to 31,150). Behind them are 15 queries: 14 compare
  instances field by field (row 14 above), one compares a mixed list on SQLite.
- The test checker's own SQL on PCT goes up by 9,773 characters in all: six tests that sort or take the extreme of a
  list mixing number types (row 3), and one test that now runs.

## 4. What you decided on 2026-11-27, and what came of each

1. **The size rows that go up:** accepted ("1 okay on size").
2. **Exact-decimal arithmetic at legend-engine's door:** Pure's answer there too ("2 okay A").
3. **Two tables with `==`:** strict, with a separate function for "in any order". Built: rows 13 and 13b.
4. **A comparison that is always false but whose side would raise an error:** fixed ("4 agreed"). One correction
   to what I told you: my example, `[1, 2]->at(5)->cast(@Number) == 1.0`, was wrong. A written `1.0` against a
   whole number is read as `1`, so that query already raised. The case that answered a quiet "false" is a literal
   that cannot be read that way (`== 1.5`) or two computed sides.
5. **One side empty, shown as a column:** you said "fix for lite if small". **It was not small, so it is backed
   out.** I built it as a guard the comparison writes for itself. The same comparison is also what a filter, a
   join, an `exists`, a `CASE` and an enumeration mapping use as their condition, and the guard showed up in all of
   them (five test groups failed). The right fix adds the guard only where a comparison is shown as a column, by
   the code that writes the column. That is a slice of its own; the answer today is what it was before step 6.
6. **A Float column combined with a Decimal:** kept as it is ("Okay keep the float thing then as is"); recorded.
7. **`max`/`min` at legend-engine's door:** built for an Integer and a Float (section 2).
8. **The date rule:** strict for "the same" and for "the later" alike (row 8).

## 5. Known gaps, written down and not fixed in this step

- **One side empty, shown as a column** (item 5 above): blank where Pure says false.
- Lists, maps and tables compared inside an expression (not as the query's answer) on databases without a list type
  (H2; partly SQLite, MySQL, MariaDB): deferred by you until everything else is done. `sameElements` inside an
  expression is refused by name for the same reason.
- A function generic over a table whose other parameters are declared `Number`, called with `1` and `1.0`: a
  compile error where the rule says "false". Looked at again; the fix would change how every such function's
  arithmetic is typed, far wider than this rare call. The error names the fix.
- `in`, `contains` and `indexOf` across two types with a side that would raise: still a quiet answer (only `==`
  was fixed).
- A mixed list holding an Integer and Decimals more than 38 digits apart; `date(2015, 4)` against a date-time (an
  error, as before); which of two equal exact numbers `max` keeps.
- What legend-pure answers for `==` on two tables was read in its source (it has no table equality), not run.

Fixed since the first version of this page:
- An instance built in the query and compared with itself (`$xs->at(0) == $xs->at(0)`) now answers true.
- A comparison the query's author writes inside a lambda handed to a generic function is now checked as the
  query's own (it was checked as that function's code).

## 6. How it was checked

- The full focused run (88 targets: core, the guards, PCT on DuckDB and H2 at all three doors, the time-series
  library, the parser checks): 87 pass, none fail, one skipped, at the commit with the reviewers' fixes. Run again
  after the date rule was put back to strict: 86 pass and one planner test fails (a day upcast to `Date`, carried by
  a variable, ordered against a date-time); fixed, and the planner, exec and time-series targets pass at the last
  commit (`6cf58b522`).
- Two outside reviews of the whole diff (read-only): 2 serious and 14 medium findings; both serious ones and 8 of
  the medium ones fixed, the other 6 in sections 4 and 5.
- Database lanes (Postgres, Trino, MariaDB, MySQL, ClickHouse): running. Cloud lanes (Snowflake, Databricks,
  BigQuery) and CI on every operating system: not yet run.
