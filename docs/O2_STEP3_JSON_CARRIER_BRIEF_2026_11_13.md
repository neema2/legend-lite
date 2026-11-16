# O2 step 3: one JSON-array carrier for a mixed list — the brief (2026-11-13)

**Status: a brief, not a build. No carrier code is written until the user rules.** The orchestrator put the choice to
the user on 2026-11-13: (a) build it now through a fresh worker; (b) defer it as a planned track and fix the one known
defect now; (c) scope it to the truly mixed family alone (recommended against). This brief serves whichever is ruled.
Its author is E (claude/total-wall-e); the ruling it builds on is O2 (start-here §6.1 item 3, the user, 2026-11-13: a
mixed list keeps kinds and answers as upstream), and its design write-up is
docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md §5.7.

## 1. Where things stand

- **The answers are already upstream's.** Today's carrier, a tag plus typed slots (`SqlExpr.TaggedList`, track L
  2026-10-29), keeps kinds. E-R1, E-R2, E-R3 and E-R5 answer as legend-pure. The cross-kind sort uses upstream's rank:
  number, then date, then boolean, then string, then the value.
- **Two defects were fixed on today's carrier** (3a711976af):
  - D1: at one instant, the coarser date sorts first.
  - D2: min, max, least and greatest across kinds are the ends of the sort.
- **One known defect remains** (the orchestrator's ruling (c)), §6 below.
- **Step 3 is about SIZE and ONE LOWERING, not answers.** The tag-and-slots shape reads a slot array per kind and per
  position. Measured on the product door at 7efcff55c3:

| case | DuckDB | SQLite |
|---|---|---|
| `['1', 2, '1', '3', 1, 3, '3', 2]->removeDuplicates()->sort()` | 3,606 | 65,375 |
| `[1, 1.0]->removeDuplicates()` | 1,062 | 16,262 |
| `['b', 1, 'a', 2.5, %2020-01-01, true]->sort()` | 2,673 | 11,169 |

  §4.1 of the design doc puts the mixed family at 74 channel-A queries: 3.6% (DuckDB) to 10.9% (Snowflake) of the
  product's characters. It is up to 98% of a server database's characters in core's 4 such queries.
- **The orchestrator's ruling on scope** (2026-11-13): switch BOTH roads at once, one carrier. No edge split may
  absorb a difference. A moved answer or engine-text spelling on any lane is a finding brought back, not ledgered.

## 2. The design

**One value: a JSON array whose elements keep their kinds.** One MIR record replaces `SqlExpr.TaggedList` and its
channels. Working name: `SqlExpr.KindedList(SqlExpr json)`. A mixed element is one JSON value:

| Pure kind | JSON element | why |
|---|---|---|
| Integer | a JSON integer, `1` | told apart from a Float by its text: no fraction, no exponent |
| Float | a JSON number with a fraction or exponent, `1.0`, `2.5e-7` | `1` and `1.0` stay two values (E-R1) |
| Decimal | `{"decimal": "2.50"}` | the scale is part of the value (Pure's BigDecimal equality reads it) |
| String | a JSON string | |
| Boolean | `true` / `false` | |
| StrictDate | `{"date": "2025-02-10"}` | never a bare string (E-R5); the text is ISO, ordered as upstream within its precision |
| a partial Date | `{"date": "2025-02"}` | its precision is its text's length |
| DateTime | `{"datetime": "2025-02-10T00:00:00", "p": <precision>}` | the subsecond text kept, as Pure's date equality reads it |
| an enumeration's value | `{"enum": "<path>", "name": "<NAME>"}` | after every primitive, by name (CompiledSupport.compareInt) |

**The sort key the database computes**, for every ordering operation: `(rank, number, instant, precision, text)`.
- rank: number 0, date 1, boolean 2, string 3, enumeration 4.
- It is read from each element's JSON type and tag.
- Ties keep position, as a stable sort, the way legend-pure's sort keeps them.
- These are the same five fields D1 gave today's rank, so the order is the one MixedListConformanceTest already pins.

**Equality** (dedup, contains, list equality, `==`) compares kind and value: the JSON element's canonical text, so `1`
is not `1.0` and `{"date": "2025-02-10"}` is not the midnight DateTime. That holds only where the dialect's JSON keeps
that text (§4).

**The root and the decode.**
- A mixed list's root is its elements as rows, one JSON value each, in one column typed `TAGGED<...>` as today, or a
  new `KINDED` slot. The plan format moves only if the slot does.
- The decoder (`JdbcDecode`, `RowDecode`, `HostRows`, `ArrowWriter`) reads each JSON value's kind from its form and
  its tag, then gives the cell as today's tagged cell does: a Long, a Double, a BigDecimal at its scale, a String, a
  Boolean, a PureDate at its precision, an enum value.
- The writers write what they write today, so no answer moves.

**What deletes when every dialect has its form:**
- the tag-and-slots MIR (`SqlExpr.TaggedList`, `Tagged`'s slot channels);
- `TaggedLists`, most of `MixedEncoding` and `ValueCollections`' channel cells;
- `CarrierStrategies`' list-less strategies for the tagged list.

That is about 3,700 lines and some 30 files that touch it, among them Scalars (73 sites), Lowerer, LambdaBinding,
LetBinding, Windows, Render, SqlExpr, SqlType, SqlTyping, MirJson, ColumnFacts, JdbcDecode, RowDecode, HostRows, Batch,
ArrowWriter, EngineStyleH2 and MongoDb. Until every dialect has its form, the two carriers coexist, and the cut-over is
per dialect. That is a G1 tension for the duration, and it ends at the deletion.

## 3. The operations to re-lower (today's `TaggedLists` surface, and the root)

Each is one form per dialect, measured against today's size on the lean-SQL sets (condition (d)):

| operation | today (`TaggedLists`) | uses | the JSON form's shape |
|---|---|---|---|
| size | `size` | 4 | the array's length |
| element read | `at`, `first`, `last` | 1+3+3 | the element at an index, out-of-range raised as today (`RaiseText INDEX_OUT_OF_BOUNDS`) |
| reverse | `reverse` | 1 | the array reversed |
| sort | `sort` (+ rows form) | 5 | unnest with position, order by the key (§2), aggregate back in order |
| min / max / least / greatest | D2: an end of `sort` | 4 | an end of the sorted array (closes §6's defect by construction) |
| dedup | `dedup`, `dedupBy` | 2+1 | first position per canonical text (or per key's value) |
| equality | `sameElement` (3 forms), `sameList`, `sameListByKinds`, `sameListOfKind` | 4+2+2+1 | the canonical texts compared (element, list) |
| membership | `contains`, `containsElement`, `containsBy` | 2+2+1 | any element whose canonical text equals the needle's |
| filter / exists / forAll / map | `filter`, `exists`, `forAll`, `map` | 1 each | unnest, the lambda over each element decoded to its kind, aggregate |
| hash | `hash` | 1 | over the canonical text |
| print | `print` | 1 | each element's print form (the print forms' seam with D is unchanged) |
| present positions | `presentPositions` | 1 | the array's non-null elements |
| as a Variant | `asVariantList` | 2 | the identity: it already is one |
| the root | `root` | 1 | the elements as rows (§2) |

## 4. Per-dialect risks (to settle on each lane before its form lands)

1. **A Float's JSON text.** A dialect that writes 17 digits (`0.30000000000000004`) where Pure writes the shortest
   breaks equality with a literal, and the print. The JSON element must carry the shortest round-tripping digits, as
   today's Float canon does.
2. **A Decimal's scale.** No JSON number keeps `2.50` against `2.5` on every dialect, hence the tag. Its text is the
   database's cast of the decimal to text at its declared scale.
3. **Subsecond precision.** A DateTime's written digits (`DateTime(n)` today) must survive. The tag's text holds them.
   Dialects that normalize a timestamp's text (trailing zeros) need the written precision `p` beside it.
4. **1 against 1.0.** A dialect whose JSON equality compares numbers by value (Postgres `jsonb`: `'1'::jsonb =
   '1.0'::jsonb` is true) cannot use JSON equality. It compares the elements' TEXT, or the key's kind and value.
5. **SQLite's json1** orders `json_each` by its own key. Position must be carried explicitly (`json_each.key`), and the
   ordering key built from `json_type` plus the tag.
6. **H2** has no list placement today (a capability gap outside O2, a found item). Its refusal by name must stay a
   refusal by name.
7. **The engine-text printers** (`EngineStyleH2` and family) print a mixed selection as the plain selection over its
   comparables (corpus `sqlstring::testGreatestLeast`). Their text must not move. If it would, stop and report
   (condition (c)).

## 5. The tools and the pins (what a builder runs)

- **Answers, one by one (condition (b)).** On the base, `bazel test //core:answer_snapshot --nocache_test_results`,
  then copy `bazel-testlogs/core/answer_snapshot/test.outputs/answers-*.tsv` aside. Snap again after each change and
  compare:
  `python3 -I docs/lean-sql-homework/compare_answers.py.txt before/answers-<db>.tsv after/answers-<db>.tsv`.
  It reports same / order only / MOVED. On one tree it reports only the two GUID cases (generateGuid, and an extend by
  it) as moved.
- **PCT on every local lane:** DuckDB, H2, SQLite, and Postgres under
  `--run_under="<copy of tools/postgres/as_unprivileged.sh> ubuntu" --strategy=TestRunner=standalone`. Channel A and
  channel B. The external lanes go to the orchestrator as a lane round on the commit's hash.
- **Size (condition (d)):** `//core:lean_sql_baseline_test` and `//spec:lean_sql_corpus_{duckdb,h2}`. Re-pin with
  `bazel run //core:update_lean_sql_baseline` and `//spec:update_lean_sql_corpus_<lane>`, per form and per database,
  dated. The cast census, `//core:census` and `//core:update_cast_case_census`: the carrier's casts carry reason
  CARRIER.
- **Before every push:** `bazel test //:generated` (the ladder and every update_* diff) and `//gates:local`.
- **The pins that must not move (condition (a)):** `MixedListConformanceTest` (core planner suite). It holds E-R1,
  E-R2 (and `type()`), E-R3, E-R5, the cross-kind sorts, D1 and D2, on every in-process database, and a dialect with
  no placement refuses by name. Step 3 ADDS §6's four cases.

## 6. The known defect step 3 closes

`[%2025-02-10T00:00:00, %2025-02-10]->min()` answers the DateTime; legend-pure answers the StrictDate. `max`, `least`
and `greatest` have the same tie. A date-only mix takes the older `LIST_MIN`/`LIST_POSITION` path
(`MixedEncoding.MixedElems.tagged`), which keeps the first of tied instants. Under step 3 every extreme is an end of
the sort, whose key orders by precision after the instant, so it closes by construction. Step 3's acceptance includes
the four cases pinned in MixedListConformanceTest with upstream's answers:
- min and least: the StrictDate;
- max and greatest: the DateTime.

The orchestrator's ruling (c), 2026-11-13: if step 3 slips or is cut, the defect returns for a fix now. E's choice is
in §8.

## 7. Estimate

| piece | estimate |
|---|---|
| the MIR record, DuckDB's form for every operation in §3, the root and the decode, conditions (a)-(d) on the local lanes | 2-3 days |
| each further dialect's form, measured on its lane: Postgres, SQLite, MariaDB/MySQL, Trino, ClickHouse, Snowflake, Databricks, BigQuery | 0.5-1 day each |
| the deletion of the tag-and-slots carrier, once every dialect has its form | 0.5 day |
| **total** | **about 1.5-2 weeks** |

## 8. If the user rules (b), deferral: the known defect's fix now

E would build the **sort path**: route a date-only mix's min, max, least and greatest through D2's `TaggedLists.sort`
ends. It is correct by construction. It reuses the order D1 and D2 already pin and the shape every lane already runs.
It leaves the tagged value's comparable channel, which downstream comparisons read, untouched. The cost is size on
those cases only: about 451 to about 1,400 characters on DuckDB, case 2075 among them. The precision-aware winner
would be smaller, but it changes what `MixedElems.tagged` compares on every dialect, including the struct-less ones
(SQLite, MariaDB) where a struct key orders as its text.
