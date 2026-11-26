# The landing of step 6.1's batch: every recorded file it moves, and why (2026-11-26)

*The edition program's step 6, slice 6.1: one value within one type (a Decimal by its number at any scale, a date-time
by its instant at any written precision), a list written in the query with no list in the SQL, and the fold rule.
This page is the dated reason for each pin, golden and ledger the batch re-records, so the regenerated commits (which
say only which target wrote them) can be read against it. A guard file moves only with a dated reason (AGENTS.md,
"Guard files are load-bearing").*

## The rulings it stands on

- **The fold rule** (the user, 2026-11-26 ~18:10 UTC, "Yes apply fold rule", after "Why are we adding any logic to
  constant folding? I thought we were taking it all out except string and boolean const"), made precise the same
  evening: **"Whole numbers fine"**. The compiler itself compares Strings, Booleans, enumeration values and whole
  numbers; a Float, a Decimal or a date is compared by the database (`LiteralUnroll.comparedHere`).
- **One value within one type** (the equality rule; register S139 and S399): `1.5D == 1.50D` and
  `%2014-01-01T00:00:00 == %2014-01-01T00:00:00.000` are true in a mixed list as everywhere else.

## What judged it

- The fixed pre-push set in the build tree on the batch's last code commit and its regenerated files.
- Six database lanes (Postgres and Trino; MariaDB, MySQL and ClickHouse; SQLite and MongoDB; Databricks; Snowflake;
  BigQuery) on the batch's stricter first version: no PCT test moved on any of them. Probes on ClickHouse, MySQL
  and MariaDB of a Decimal's `IN` and `=` at different scales: correct.
- An independent audit (`docs/records/edition-homework/audits/`), its three blockers fixed (below).

## Product SQL size (may only go down; up only with the user's word)

`core/src/test/resources/lean-sql/baseline.tsv`, the same 2,300 queries on each database:

| Database | Statements | Characters before | after | change |
|---|---|---|---|---|
| H2 | 1,722 to 1,736 | 492,904 | 456,664 | 36,240 down |
| BigQuery | 2,181 to 2,195 | 973,275 | 933,587 | 39,688 down |
| MySQL | 2,191 to 2,205 | 2,106,971 | 2,073,572 | 33,399 down |
| MariaDB | 2,190 to 2,204 | 1,636,505 | 1,611,938 | 24,567 down |
| SQLite | 2,247 to 2,261 | 1,562,673 | 1,542,296 | 20,377 down |
| Postgres | 2,190 to 2,204 | 957,199 | 939,815 | 17,384 down |
| Trino | 2,266 to 2,280 | 919,969 | 916,347 | 3,622 down |
| Databricks | 2,193 to 2,207 | 655,898 | 653,611 | 2,287 down |
| Snowflake | 2,266 to 2,280 | 846,314 | 844,152 | 2,162 down |
| DuckDB | 2,266 to 2,280 | 529,909 | 528,503 | 1,406 down |
| **ClickHouse** | 2,258 to 2,272 | 1,774,191 | 1,775,423 | **1,232 up** |

By family, summed over the eleven databases:

| Family | Statements | Characters | Why |
|---|---|---|---|
| `contains` | 121, unchanged | 189,361 to 10,707 | a written mixed list's membership is `value IN (the elements of the value's type)`: no list is built |
| `in` | 22 to 44 | 17,551 to 4,690 | the same; two more queries on each database, which the compiler answered itself before (the fold rule) |
| `eq` | 44 to 110 | 3,076 to 8,770 | **up.** Six queries per database the compiler answered itself are sent: a Float, a Decimal or a date compared with another constant (`1.0 == 1.0`, `%2014-01-01 == %2014-01-01`). The fold rule |
| `equal` | 187 to 220 | 20,196 to 23,043 | **up.** Three more per database, the same |
| `not` | 55 to 88 | 3,128 to 6,202 | **up.** Three more per database: `!(a == b)` over such constants |

So every row that goes up (37 of them: `eq`, `equal` and `not` on each database, and the four below) is a comparison
of constants that the compiler used to answer and the database answers now: 14 more statements a database, about 30
characters each on DuckDB. ClickHouse alone ends higher overall because each of its statements carries 303
characters of settings (the database follow-up's item D3, deferred by the user to after Stage B). **Ruled by the fold
rule** ("Yes apply fold rule", "Whole numbers fine").

Core's own set: `let` 194 characters up on DuckDB and on H2 (a constant comparison inside a `let`, now the
database's); DuckDB `equal` 8 to 10 statements and 84 characters up; DuckDB `contains` 353 down.

`core/src/test/resources/lean-sql/cast-case-census.tsv` (a census, explained, not a shrink-only pin): casts for a
mixed list's carrier down on every database (DuckDB 324 to 312, Postgres 455 to 359, BigQuery 486 to 390); the
dialect's `CASE`s down (H2 499 to 211; BigQuery, MariaDB, MySQL, Postgres and SQLite 159 to 63); the casts a program's
own literals write up by 8 on every database (the 14 new statements' Decimal and date literals).

`pct/.../channelb/ratchets.tsv` (test bodies under both judges): essential 1,991 characters down; **grammar 1,178 up
and standard 198 up**, the same constant comparisons inside test bodies. The same ruling.

The relational corpus's size baselines and its two rosters: unchanged.

## Tests whose answer moves

None of the recorded suites: PCT's expected failures on DuckDB and H2, the engine lane's tables, the edge contract's
goldens, the corpus rosters and the edge corpus are regenerated unchanged. The six database lanes moved no test.

New tests hold the slice: `exec/OneValueWithinOneTypeTest`, `exec/TaggedListTest` (membership in a written list is a
plain `IN`; a list holding a parameter is planned as before), `compiler/spec/NumberLiteralsTest` (a literal past a
Decimal's 38 digits stays a comparison across types).

## Guards moved, each dated where it stands

| Guard | What moves | Why |
|---|---|---|
| `SqlFnSortedTest.BOTH_WRITERS` | `IN` gains the product writer `lowering.TaggedLists` | a written list's membership is the database's `IN` now. Found by the batch's full run; a product writer added, nothing moved to the edge |

No other pin, ceiling or allowlist moves.

## What the independent audit found, and what was done

| Finding | Done |
|---|---|
| **Blocker: a wrong answer from the membership fold.** A String needle could match a list's date whose folding key was the same text | the fold compares within one class of literal only |
| **Blocker: removing the whole-number fold broke the recursion unroll** (a function that stops on `size() == 0` no longer stopped) and five corpus tests | whole numbers stay in the fold; put to the user, who ruled "Whole numbers fine" |
| **Blocker: an `IN` over one value was read by a dialect as a list parameter** | one candidate is written `=` |
| a bare `=` chain where a side may be NULL | null-safe (`IS NOT DISTINCT FROM`) where either side can be NULL, plain `=` where neither can |
| ClickHouse might truncate a Decimal in `IN` | not reproduced: probed on the lane at three scales, correct |
| a build break the quick type check missed (a nullable local passed to a lambda; the full compiler's null check refused it) | rewritten without nullable locals |

## What the re-check of those fixes found (`audits/batch-6-1-recheck-2026-11-26.md`), and what was done

No blocker; the three blockers above confirmed fixed. Three new items, each fixed before the batch went to CI:

| Finding | Done |
|---|---|
| a position of two written lists compared with a bare `=` where a value could still be NULL: the check trusted the SQL type's "never NULL" label, which only repeats the Pure multiplicity (a `[1]` property on a nullable column) | plain `=` only where both values are literals, read off the node; otherwise the null-safe equality itself |
| with whole numbers back in the compiler's comparison, a whole number inlined against Float literals (`1->in([0.0, 1.0])` through a `Number` parameter) folded to false, where the previous batch left it to the database (true) and its mirror still answers true | left to the database again, as the previous batch left it; `LiteralRuleAnswersTest` holds both directions. What such a comparison SHOULD answer under the run-time rule is slice 6.2's question |
| an answer given by the types alone (two lengths, two kinds at a position, an element of another type left out of `IN`) wrote no element, so an element that raises lost its error | such an answer is taken only where no element can raise; otherwise the lists compare as before, every element evaluated |

Also from it: the fold's switch on a literal's kind throws on a kind it does not compare (it returned false).
Recorded, not changed here: `[2D, 'a']->contains(2)` is refused while `2->in([2D, 'a'])` answers false (the refusal
goes in slice 6.2, both false); a map's `get` and `groupBy` still compare Decimals and dates in the compiler (step 10's
cut-back of the folds).

## Carried forward (the plan record's section 5b)

A list holding a parameter or a column still goes through the list's carrier; a mixed list's sort on BigQuery orders
by a STRUCT, which BigQuery refuses (slice 6.2); a table's cells counted as its rows (slice 6.2).
