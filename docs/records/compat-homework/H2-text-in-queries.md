# H2: text built inside a query and then used (compat homework, 2026-11-19)

The question (the homework brief `docs/records/compat-homework-2026-11-19.md` on
`origin/claude/refactor-code-quality-review-9fqhug`, row H2): how common is text built inside a query and then used,
which is rule 6's case for an input rewrite (and for a named core capability, R1)? The count is per source: expressions
whose text output FEEDS further computation, against text that is only RETURNED. For the feeding cases, the record
also gives the text function and the value type.

## 0. The answer

Upstream programs almost never spell a value as text and then compute with it.

- **PCT** (1,249 tests, 2,719 queries) has 3 such sites, in 2 tests: `toString` of an **Integer** column fed to `==` or
  `contains`. Another 9 sites, in 3 tests, feed `removeDuplicates` comparators and keys: there `toString` spells
  **literal** mixed Integer/String lists, so their text is known before the query runs.
- **The relational corpus** (2,761 tests, 2,598 queries) has 2 such sites at the query level. One is an **Integer**
  `toString` in a filter. The other is an **enum**'s `toString` in a derived property, filtered by `startsWith`. Its
  mappings and stores also build text in the database: `toString` or `convertVarchar128` of an **integer** ID. Queries
  use that text in 1 filter and in 3 kinds of join or association key, one of them repeated across 4 mappings (§6).
- **No upstream program spells a Float, a Decimal, a Date, a DateTime or a Boolean value and then uses the text.**
  Those types are spelled only in the toString/format unit tests themselves (literals, returned as is) and on the judge
  side (the asserts printing results).

Rule 6's own examples, counted:

- `filter($x.price->toString() == ...)` over a Float: 0 sites in every source.
- A group key built by `toString`: 1 site upstream, and it is a toString of a String (identity).
- `joinStrings` over spelled Floats: 0 sites.

The R1-shaped cases are lite's own: three DataCube emitters and the Cypher translator.

- The DataCube can turn a Date, a number, a Boolean or an enum into a text column that the cube then groups or filters
  on (§9).
- The Cypher translator spells Integers and Booleans into JSON or date text that it parses back (§9).

Neither is an upstream program.

## 1. What was counted

- **A site** is one call, in a program's AST, of a function that **spells a value as text**. The set comes from
  upstream's own declarations: every function in the pinned legend-pure platform and the three engine function modules
  whose return type is `String` and which takes a non-String parameter. Sigs.java read 87 String-returning signatures
  with lite's parser.
  - The spelling set is `toString`, `toRepresentation`, `format`, `formatDate`, `char`, `enumName`, `id`,
    `elementToPath`, `encrypt` and relation printing (`toString(Relation)`, `s`).
  - Text -> text functions (`plus` over strings, `joinStrings`, `substring`, `toUpper`, `replace` and the rest) are not
    sites. They carry a site's text on and mark it COMPOSED.
  - They are also counted separately as COMBINER sites (§7), because the brief names `joinStrings` and `plus`.
  - `plus` on a string over a non-string operand matches none of `plus`'s overloads. Read from the pinned sources,
    they are `String[*]`, `Integer[*]`, `Float[*]`, `Decimal[*]` and `Number[*]`. So it appears only as
    `'x' + $v->toString()`, where the `toString` is the site.
- **Where each site's text goes** is decided by a forward taint interpreter over the program:
  - **FEEDS**: the text reaches content-dependent computation before the query's result. That is a filter, an `if` or
    `exists` predicate, a comparison, a sort, group, join, window or pivot key, `distinct`, a `removeDuplicates` key or
    comparator, an aggregate, `in`, or a text -> non-text function (`length`, `contains`, `startsWith`, `parseInteger`,
    `fromJson` and the like). It reaches it directly, through a `let`, or through a relation or TDS column by name
    (`extend(~c: ...)` and later `filter(r|$r.c ...)`).
  - **RETURNED-BARE**: the text is the query's result, a projected or extended column, or an element of one, as it is.
  - **RETURNED-COMPOSED**: the text is returned inside a bigger or cut string (`'a' + $x->toString()`, `substring`,
    `joinStrings`). Reported apart, because no presenter can re-spell the inner value from the outer string after
    results.
  - **HARNESS (the judge side)**: the site is outside the query. In PCT that means outside the lambda given to
    `$f->eval(...)`. In the corpus it means outside the lambda given to `execute`, `executionPlan`, `toSQLString` and
    the like. These are the asserts printing or joining results to compare them. Rule 9 puts them on the host judge.
  - **Other**: DROPPED (the text reaches only `size`, `isEmpty` or the like); UNREACHED (in a lambda nothing applies);
    QUERY-ASSERT (an assert inside a lite snippet whose whole body was taken as the query).
- **The value type** is the spelled operand's type: from literals, declared parameter and property types, `#TDS`
  headers or first rows, column types carried through `extend`, and function return types taken from upstream's
  declarations. The **origin** is LITERAL (a constant), DATA (a column, property or parameter) or COMPUTED (an
  expression over either). Unresolved types on FEEDS sites were typed by hand from the snippet (§4).

## 2. Method, per source, and its error bars

Every count was made by lite's own parser.

- The lexer/parser/protocol/model closure of `core/src/main/java` (with `base` and `json`, 1,350 classes) was
  compiled standalone with `javac`. No Bazel was needed, and no product file changed.
- `.pure` files were parsed with `ModelFromText.parse(text, GrammarLevel.LEGEND_PLATFORM)`, the call `ChannelB` and
  `MinimalCorpus` use. Java-embedded snippets were parsed with `SpecParser`.
- The probes are in `H2-probes/` (as `.txt`, so no build or guard picks them up). Every site found is a row of
  `H2-data/*.tsv`, with its file, line, events and snippet. HARNESS-side combiner rows are left out of those files.

| source | how | parse walls | error bar |
|---|---|---|---|
| PCT essential, grammar, standard, relation, unclassified (the pinned trees at legend-pure 5.99.0 / legend-engine 4.145.0, cloned at their release tags; the five `ReportScope` directories lite's `pct/` runs) | every `<<PCT.test>>` function in a scope (ChannelB's discovery rule); the query is the lambda given to `$f->eval`, inline or `let`-bound | 1, `platform/pure/grammar/m3.pure`, outside every scope | 0 sites unreached, 0 unknown sinks. The operand of 3 sites is an empty list `[]` (nothing is ever spelled). Inlining the tests' own helper functions changes nothing (557 rows either way) |
| relational corpus (`core_relational/relational`, the tree `MinimalCorpus` walks) | every `<<test.Test>>`; the query is the zero-parameter lambda given to `execute`, `executionPlan`, `toSQLString` or `toSQL`; user functions and derived properties inlined into the query (as G½ and the resolver do) | 0 in the corpus tree (5 in the engine-core library files read only for class types) | 1 site unreached (test machinery, not a query). 4 sinks unmodelled, all on the judge side. Mapping and store text counted separately, by a reflective walk of the parsed mappings and databases (StoreScan) |
| `core/src/test` Java (836 files) | every maximal string-literal run (parser-equivalence's `InlineSnippets.literalRuns`, copied). A run is parsed as a model if it looks like one (each function and derived property is then a program), else as an expression (a leading `->` fragment gets a placeholder receiver; a statement block is parsed as a code block). The whole snippet is the query | 2,005 of 9,313 candidate runs do not parse (SQL, JSON, CSV, messages, fragments). 1,341 duplicates are counted once | 23 unparsed runs contain a text-producer name: the upper bound of missed sites. No query/judge split inside a snippet (13 QUERY-ASSERT) |
| `core/src/test` `.pure` (242 files: stress models, upstream-api, smoke) | as projects; a file that is one query is parsed as an expression | 1 (a multi-lambda query file) | none found |
| `projects/` (61 projects, 168 files, timeseries included) | every function body and derived property is a program, its result returned; user calls inlined | 0 | none |
| DataCube (`datacube/src`, 278 TS files) | it builds Pure through an AST builder (`fn('name', ...)`), so its emitters were counted statically in the TS source (`fn('toString'`, `fn('format'`, the retype table), with the position each emitted column takes | n/a | it emits per user action, so instances are not countable; shapes only (§9) |

The **interpreter's limits**:

- Columns are followed by name through `extend`, `project`, `select`, `rename`, `without`, `groupBy`, `aggregate`,
  `join`, `pivot` and `over`, through lite's `read`, and through TDS `getString('c')` and its siblings. A dynamic
  column (`$r->read($col)`) is over-approximated as every column.
- A function the tables do not model carries its arguments' taint and records UNKNOWN. After the tables were filled,
  no query-side site ends UNKNOWN.
- Typing is local, not lite's Typer. It agrees with the snippets for every FEEDS site, which were read by hand.
- **Not covered**:
  - the variant PCT suite (`legend-engine-pure-code-functions-variant`, 7 files), which is not one of lite's five;
  - Pure expressions inside the corpus's M2M mappings, which the reflective walk counts (6 `plus`, 1 `joinStrings`)
    but does not follow.

## 3. Sources and sizes

| source | files | lines | programs | queries |
|---|---|---|---|---|
| PCT essential | 183 | 14,455 | 345 PCT.test | 855 eval |
| PCT grammar | 48 | 5,058 | 137 | 555 |
| PCT standard | 55 | 5,413 | 205 | 708 |
| PCT relation | 65 | 18,915 | 469 | 479 |
| PCT unclassified | 65 | 2,810 | 93 | 122 |
| relational corpus | 552 | 170,812 | 2,761 test.Test | 2,598 (2,333 execute, 146 executionPlan, 118 toSQL*) |
| core/src/test Java | 836 | 221,260 | 5,028 parsed snippets and functions | (each snippet a query) |
| core/src/test .pure | 242 | | 309 functions and derived properties | |
| projects/ | 168 | 73,925 | functions and derived properties (timeseries: 640 functions, 5,248 lines) | |
| datacube/src | 278 TS | 111,749 | 5 emitter shapes | |

## 4. The counts

Spelling sites: a non-text value spelled as text. "of which over DATA" counts the FEEDS sites whose operand is a column,
property or computed value rather than a literal. The last column counts the combiner sites the brief names
(`joinStrings`, string `plus`) on the query side, with the FEEDS among them in brackets.

| source | spelling sites | judge side (harness) | query side | FEEDS | of which over DATA | RETURNED-COMPOSED | RETURNED-BARE | other | text-only toString/format (String operand) | combiner sites, query side (FEEDS) |
|---|---|---|---|---|---|---|---|---|---|---|
| PCT essential | 96 | 1 | 95 | 9 | 0 | 2 | 84 | 0 | 5 | 22 (0) |
| PCT grammar | 3 | 0 | 3 | 0 | 0 | 3 | 0 | 0 | 0 | 14 (3) |
| PCT standard | 61 | 58 | 3 | 0 | 0 | 0 | 3 | 0 | 0 | 1 (1) |
| PCT relation | 379 | 373 | 6 | 3 | 3 | 3 | 0 | 0 | 3 | 49 (7) |
| PCT unclassified | 9 | 0 | 9 | 0 | 0 | 0 | 9 | 0 | 0 | 0 (0) |
| relational corpus | 464 | 459 | 5 | 2 | 2 | 0 | 2 | 1 | 0 | 98 (17) |
| core/src/test Java | 216 | n/a | 216 | 71 | 51 | 20 | 79 | 46 | 3 | 361 (206) |
| core/src/test .pure | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 6 (0) |
| projects/ | 3 | 0 | 3 | 0 | 0 | 3 | 0 | 0 | 0 | 42 (0) |
| **all** | **1,231** | **891** | **340** | **85** | **56** | **31** | **177** | **47** | **11** | **593 (234)** |

Upstream alone (PCT and the corpus): 1,012 spelling sites. 891 are on the judge side and 121 in queries: FEEDS 14,
COMPOSED 8, BARE 98, other 1. Programs with a feeding spelling site: PCT 5 of 1,249, the corpus 2 of 2,761.

**The FEEDS spelling sites, by function, value type, origin and sink** (unresolved types typed by hand: an `[]` operand
is shown as "empty list"; the core Java ones marked † are Integer year/day parts or an `INTEGER` column, read off the
snippet):

| source | function | value type | origin | sink | sites | tests |
|---|---|---|---|---|---|---|
| PCT essential | toString | Integer and String (a mixed literal list) | LITERAL | `removeDuplicates` comparator (`==`) | 4 | 1 |
| PCT essential | toString | Integer and String | LITERAL | `removeDuplicatesBy` key | 2 | 1 |
| PCT essential | toString | empty list | LITERAL | `removeDuplicates` comparator and key | 3 | 1 |
| PCT relation | toString | Integer | DATA (a column) | `filter ==`, after `+` with a String column | 1 | 1 |
| PCT relation | toString | Integer | DATA | `filter contains`, after `+` | 2 | 1 |
| relational corpus | toString | Integer | DATA | `filter ==` | 1 | 1 |
| relational corpus | toString | enum (mapped to a column) | DATA, in a derived property | `filter startsWith`, after `+` | 1 | 1 |
| core Java (Cypher translator goldens) | toString | Integer (†) | COMPUTED or DATA | `fromJson` (JSON text built, then parsed) | 37 | 1 file |
| core Java (Cypher) | toString | Boolean | COMPUTED | `fromJson`; `contains` | 15 | 1 file |
| core Java (Cypher) | toString | Integer (date parts) | COMPUTED | `dateTimeOfText` (date text built, then parsed) | 8 | 1 file |
| core Java (others) | toString | Integer | DATA | `filter ==` (lite's copies of testExtendFilter) | 3 | 3 |
| core Java (others) | toString | Integer | LITERAL | `contains` in an assert | 2 | 1 |
| core Java (others) | toString | Integer and String | LITERAL | `removeDuplicates`/`By` (lite's copies of the PCT tests) | 5 | 4 |
| core Java (others) | toString | Integer (†, an `INTEGER` column) | DATA | `parseInteger` | 1 | 1 |

There are also 3 toString-over-String sites (identity: no value is spelled). They are in PCT relation and feed a
`contains` and a group key (`project(~[name2: x|$x.name->toOne()->toString()])->groupBy(~name2, ...)`).

**The RETURNED spelling sites by value type** (query side, all sources: 208):

- **BARE**, 177. 130 have LITERAL operands, the toString/format unit tests:
  - Float: `toString` 28, `format` 23;
  - DateTime: 16 and 15;
  - StrictDate or Date: 7 and 7;
  - Integer: 3 and 8;
  - Boolean: 2 and 2;
  - Decimal: 1 and 0;
  - `char` of an Integer: 11;
  - enum, class, List, Pair and element paths: 7.
- The other 47 BARE sites are over DATA or COMPUTED operands:
  - Float 7, Integer 6, `char` 2, StrictDate 1 (corpus `$t.date->toString()`, projected), Boolean 1, Number 1;
  - Relation 3;
  - enum, class or Pair 10 (among them PCT's `$x.date->dayOfWeek()->toString()`);
  - unresolved 16, of which 4 are `format` of a column.
  - 37 are in lite's own tests. 10 are upstream's:
    - 7 spell instances PCT builds from literals (List, Pair, a class);
    - 3 spell data: the corpus's Date, its `char($p.age)`, and PCT's day-of-week enum.
- **COMPOSED**, 31. Integer 22 (8 over DATA), Boolean 2, Float 1 (`projects/static-core`'s `label()`:
  `$this.standardName + ' = ' + $this.magnitude->toString() + ...`), Pair 1, unresolved 5.

## 5. The judge side

891 spelling sites sit outside the queries, in the asserts.

- PCT has 432: `toString(Relation)` in `assertEquals` 431 (relation 373, standard 58), and 1 class name.
- The corpus has 459:
  - `$result.values->at(0)->sort(...)->toString()` and other relation prints: 127;
  - the calendar tests' `min()/max()->toOne()->toString()` over cast StrictDates: 92;
  - row maps for `assertSameElements` and the like, and other prints: the rest. Their operands are unresolved 189,
    Integer 28, element paths 10, enum or class 7, Float 4, `char` 1 and StrictDate 1.

The same holds for the combiners: 1,464 of the 1,724 upstream combiner sites are on the judge side. These are channel
A's harness and channel B's asserts. Rule 9 makes them the host judge's.

## 6. Upstream's store level (the relational mapping DSL)

A reflective walk over the corpus's 550 parsed mappings and 138 databases found the following dynafunctions:

- `toString` 16 times and `convertVarchar128` 3 times, every one over an **integer** ID column;
- `concat` 82 times: 70 a literal and a column, the rest over columns or nested calls.

The integer-to-text ones that queries then use:

- `int1String: toString(int1)` is filtered by `$s.int1String == '1'` (`tests/mapping/sqlFunction/testSqlFunctionsInMapping.pure:778`, `:188`).
- `Join Trade_Account(convertVarchar128(tradeTable.accountID) = accountTable.ID)` (`tests/mapping/join/advancedRelationalSetUp.pure:54`).
- `+synonymProductIdString : String[1] : toString([DB1]synonymTable.productId)` is the cross-store association key
  `$this.productIdString() == $that.synonymProductIdString` (`graphFetch/tests/testOrderedCrossStoreGraphFetch.pure:163`, `:174`; 4 mappings).
- `toString(Trades.Trade.ENTITY_ID_FK) = Entity.LegalEntity_View.ENTITY_ID` sits in a join and in model joins
  (`modelJoins/testModelJoinsToRelationalJoins.pure:239`, `:326`).

The rest are projected (`id: toString(BookTable.book_id)`, `positionType: toString(...id)`). The demo projects' stores
and mappings contain no text dynafunction (`count`, `sum`, `max` and `min` only).

## 7. The combiners (`joinStrings`, string `plus`)

On the query side there are 593 combiner sites (PCT 86, corpus 98, core Java 361, core .pure 6, projects 42). 234
feed:

- PCT: concatenated names into group and sort keys (`test_Extend_GroupBy_Project`), a `joinStrings` aggregate
  filtered by `== 'A,B,C'` (`testJoinStrings_GroupBy_ForcingSubSelect`), and the equality grammar tests.
- The corpus: `firstName + ' ' + lastName` style keys in filters, `sortBy`, `in` and group keys.
- Core Java: mostly the Cypher translator's JSON text.

Of the query-side combiner sites, 148 carry a spelled value inside them:

- PCT 9 (the grammar and essential `toString(1) + ' ' + ...` tests, `'(' + $pairs->map(p|$p.toString())->joinStrings(',') + ')'`,
  and the relation `str + val->toString()` ones);
- the corpus 1 (the enum above);
- core Java 135, of which 128 are `plus` and 7 `joinStrings`. 94 of the `plus` sites FEED, nearly all the Cypher
  translator's JSON text; the `joinStrings` ones include `->map(c|$c->char())->joinStrings('')` and an assert
  message's `toRepresentation`s;
- projects 3 (the `label()` derived properties).

Upstream programs have 10 of these, and only 3 of the 10 feed: the 2 relation ones and the corpus enum.

**No `joinStrings` anywhere joins spelled Floats.** The 8 `joinStrings` that carry a spelled value (PCT 1, core Java
7) join a Pair's, an Integer's, a `char`'s or a `toRepresentation`'s text.

A further 157 `plus(?)` sites on the query side have unresolved operand types (PCT 70, corpus 5, core Java 67, projects
15). They are numeric: `reduce(..., {y|$y->plus()})` and Float arithmetic in timeseries. They are left out of the text
counts.

## 8. Value types, for R1

- **Integer.** This is the only type spelled from DATA and then used in upstream programs: PCT 3 sites, the corpus 1,
  the store level about 5. Lite's own feeding uses are Integer too, apart from the Cypher translator's Booleans.
  - Whether lite's Integer text equals legend-pure's and legend-engine's (H2's `cast`) is **not** measured here; H1 or
    H3 should read it from the sources or ask the runner.
  - If it is equal (class S), the measured upstream feeding cases need no capability at all.
- **enum.** 1 corpus site. Upstream itself disagrees with itself here. legend-pure spells the enum's name. The corpus
  test's own expected SQL spells the mapped column's raw value: `concat("root".ISSUER, cast("root".ISSUER_TYPE as varchar)) like 'issuer1%'`
  (`tests/mapping/embedded/testInlineEmbeddedTargetIds.pure:130`). That is a meaning question (rule 5) before it is a
  spelling one.
- **Integer and String literal lists.** 9 PCT sites in 3 tests. Every operand is a literal, so the text is known
  before the query runs.
- **Float, Decimal, Date, DateTime, Boolean.** 0 feeding sites in any upstream program. They are spelled only as
  literals in the toString/format tests (returned bare) and on the judge side.
  - The returned DATA cases are few: a Date projected in the corpus, a Float in a project `label()` (composed), and
    lite's own tests.
  - Those are presenter questions (H1, P), unless the presenter cannot see the value behind the text.
- **Lite's own generators** (§9) are where Float, Decimal, Date, DateTime, Boolean and enum text can feed a group key or
  filter: the DataCube's retype, date-format and enum-name columns. They speak LITERAL's meaning unless the DataCube
  must also speak an upstream audience's.

## 9. Lite's own generators

**DataCube** (`datacube/src`) emits five text shapes, counted in its source:

1. **Retype to String** (`select-columns.ts:27-29`): Integer, Float, Decimal, Boolean, StrictDate or DateTime column
   -> `toString`. The retyped column replaces the column for the next box, which may filter, group, sort or join on
   it: it can FEED.
2. **Date written as text** (`date-tools.ts:73`): `format('%t{<format>}', [$x.<date column>])`, a Date or DateTime
   with a user's format. It is made a **dimension** (`kind: 'dimension'`), which the cube groups by: it FEEDS.
3. **Enumeration read as its name** (`saved-queries.ts:108`, `enumsAsStrings`): `toString` on an enum column in a
   projection. The cube then groups or filters on it: it can FEED.
4. and 5. **`joinStrings` aggregates** (`query.ts:222`, `json-shape.ts:481,505`) over String columns: RETURNED.

A user's calculated column is free Pure text and may hold anything (`test/offer-facts.test.ts:80`:
`$x.qty->toOne()->toString()`). The **timeseries** library (`projects/timeseries/model.pure`, 640 functions) contains
no text producer at all. Its TypeScript `toString()`s are TS methods (`Term.toString`), not Pure.

**The Cypher translator** (`core/src/main/java/com/legend/protocol/cypher/CypherToPure.java:385-391`): a node or date
value is "built as text by the database and read as a Variant". Integer and Boolean parts are spelled, concatenated
into JSON or date text, and parsed back by `fromJson` or `meta::legend::lite::dateTimeOfText`. That accounts for 60 of
core Java's 71 feeding sites (`CypherToPureTest.java`, e.g. lines 688, 690, 756). This is lite's meaning for Cypher,
with no upstream audience.

## 10. Twenty representative feeding examples

Paths are relative to the pinned trees:

- PCT: `core_functions_relation/relation/...`, or `platform/pure/essential/...` in legend-pure;
- corpus: `core_relational/relational/...`;
- lite: the repository root.

| # | source (file:line) | program | expression (abridged) | text function | value type | sink |
|---|---|---|---|---|---|---|
| 1 | PCT essential, `collection/transformation/removeDuplicates.pure:118` | testRemoveDuplicatesPrimitiveNonStandardFunction | `[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates({x, y \| $x->toString() == $y->toString()})` | toString | Integer and String, literal | dedup comparator `==` |
| 2 | PCT essential, `collection/transformation/removeDuplicatesBy.pure:49` | testRemoveDuplicatesByPrimitive | `[1, 2, '1', ...]->removeDuplicatesBy({x \| $x->toString()})` | toString | Integer and String, literal | dedup key |
| 3 | PCT relation, `tests/composition.pure:62-63` | testExtendFilter | `#TDS val, str ...#->extend(~newCol:x\|$x.str->toOne() + $x.val->toOne()->toString())->filter(x\|$x.newCol == 'qw4')` | toString, then `+` | Integer column | filter `==` |
| 4 | PCT relation, `tests/composition.pure:1097-1099` | testMixColumnNamesRenameExtend | `->extend(~newCol:c\|$c.col_one_num->toOne()->toString() + $c.colTwoLetter->toOne()->toString() + ...)->rename(~newCol, ~_new_col)->filter(x\|$x._new_col->toOne()->toString()->contains('A'))` | toString, then `+` | Integer columns (and a String) | filter `contains`, through a rename |
| 5 | PCT relation, `tests/composition.pure:1629-1630` | testGroupBy_Conflicting_Alias_With_Table_Columns | `->project(~[name2: x\|$x.name->toOne()->toString()])->groupBy(~name2, ...)` | toString | String (identity) | group key |
| 6 | PCT relation, `functions/aggregation/joinStrings.pure:141-142` | testJoinStrings_GroupBy_ForcingSubSelect | `#->groupBy(~grp, ~names : g \| $g->joinStrings(~name, ',', ~id->ascending()))->filter(r \| $r.names == 'A,B,C')` | joinStrings | String column | filter `==` |
| 7 | PCT relation, `tests/composition.pure:1785` | test_Extend_GroupBy_Project | `#->extend(~fullName:x\|$x.firstName->toOne() + ' ' + $x.lastName->toOne())->groupBy(~[id, fullName], ...)` | plus | String columns | group key |
| 8 | corpus, `tests/query/testWithFunction.pure:228` | testFilterUsingToStringFunction | `execute(\|Trade.all()->filter(t \| $t.id->toString() == '1'), ...)` | toString | Integer property | filter `==` |
| 9 | corpus, `tests/mapping/embedded/testInlineEmbeddedMapping.pure:210`, used by `testInlineEmbeddedTargetIds.pure:125` | testQualifierProperty | `description(){ $this.name+$this.type->toString() }` and `BondDetail.all()->filter(p \| $p.issuer.description->startsWith('issuer1'))` | toString, then `+` | enum (mapped column) | filter `startsWith` |
| 10 | corpus, `tests/mapping/sqlFunction/testSqlFunctionsInMapping.pure:778` and `:188` | (mapping) and its filter test | `int1String: toString(int1)`, then `SqlFunctionDemo.all()->filter(s \| $s.int1String == '1')` | dynafunction toString | Integer column | filter `==` (store level) |
| 11 | corpus, `tests/mapping/join/advancedRelationalSetUp.pure:54` | (database) | `Join Trade_Account(convertVarchar128(tradeTable.accountID) = accountTable.ID)` | convertVarchar128 | Integer column | join key (store level) |
| 12 | corpus, `graphFetch/tests/testOrderedCrossStoreGraphFetch.pure:163`, `:174` | (mapping) | `+synonymProductIdString : String[1] : toString([DB1]synonymTable.productId)` and `$this.productIdString() == $that.synonymProductIdString` | dynafunction toString | Integer column | cross-store association key |
| 13 | corpus, `tds/tests/testTDSFilter.pure:36` | testFilterWithStringConcat | `Person.all()->project([#/Person/firstName!name#])->filter({r \| ($r.getString('name') + 'A') == 'JohnA'})` | plus | String column | filter `==` |
| 14 | corpus, `functions/tests/testSort.pure:49` | testSortByLambdaColumnAddition | `Person.all()->sortBy(p \| $p.lastName + '\|' + $p.firstName)` | plus | String properties | sort key |
| 15 | lite, `core/src/test/java/com/legend/integration/TypeInferenceIntegrationTest.java:3299-3304` | (lite's copy of #3) | `#->extend(~newCol:x\|$x.str->toOne() + $x.val->toOne()->toString())->filter(x\|$x.newCol == 'qw4')` | toString, then `+` | Integer column | filter `==` |
| 16 | lite, `core/src/test/java/com/legend/exec/InterimEdgeSitesTest.java:33` | | `#>{s::DB.T}#->extend(~r: r\|$r.ID->toString()->parseInteger())` | toString | Integer column (`ID INTEGER`) | `parseInteger` |
| 17 | lite, `core/src/test/java/com/legend/AssertVerdictsTest.java:152` | | `[1,2,3]->map(i\|assert(['1','2','3']->contains($i->toString()), 'element missing'))` | toString | Integer, literal | `contains` (in an assert) |
| 18 | lite, `core/src/test/java/com/legend/protocol/cypher/CypherToPureTest.java:688-690` (from `CypherToPure.java:385-391`) | Cypher date and node values | `... ->year()->toString() + '-' + ...` into `meta::legend::lite::dateTimeOfText()`; `fromJson('[' + toString(10) + ... + $r.x->toOne()->toString() + ']')` | toString, then `+` | Integer (date parts, columns), Boolean | `dateTimeOfText`, `fromJson` |
| 19 | lite, `datacube/src/date-tools.ts:73` | the DataCube's Date tool | `format('%t{dd.MM.yyyy}', [$x.trade_date])` as a `dimension` column | format | Date or DateTime column | group key (a cube dimension) |
| 20 | lite, `datacube/src/select-columns.ts:27-29` (with `saved-queries.ts:108`) | the DataCube's Select retype, and enums read as names | `$x.price->toOne()->toString()` as `price__String`, handed on as `price`; `toString` on an enum column | toString | Integer, Float, Decimal, Boolean, Date, DateTime; enum | whatever the next box does: filter, group, sort, join |

## 11. Reproducing

1. Clone the pinned upstream tags: `git clone --depth 1 --branch legend-pure-5.99.0 https://github.com/finos/legend-pure`,
   and `legend-engine-4.145.0` likewise. These are the `release.MODULE.bazel` pins.
2. Compile lite's parser closure with `javac`:
   `find core/src/main/java/com/legend/{parser,lexer,protocol,model,values,spi,error} base/src/main/java json/src/main/java -name '*.java'`
   plus `core/src/main/java/com/legend/door/ModelFromText.java`.
3. Compile the probes against it.
4. Run `run_pct.sh`, `run_corpus.sh` and `run_lite.sh` (each with `-Dcensus.combiners=true`). For the Java snippets,
   run `TextCensus java <out> core-java=core/src/test/java`. For the store level, run `StoreScan <corpus root>`.
5. Run `final.py` to print §4's tables from the `.tsv` files.

The whole run takes about 10 seconds.
