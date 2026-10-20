# Cypher in Pure: the `#Cypher{...}#` island, Neo4j as the spec, and graph algorithms the database runs

**Status: research and brainstorm (2026-10-15). Nothing is scheduled or announced in `docs/IN_FLIGHT.md`; the
decisions of §4 are proposals for the user.** This is the third study of its kind, after
`docs/LEGEND_SQL_STUDY_2026_10_11.md` (Postgres SQL, the `#SQL{...}#` island) and `docs/IQ_SQL_STUDY_2026_10_13.md`
(SAP IQ, the `#IQ{...}#` island). It answers the user's ask of 2026-10-15: do for Neo4j's Cypher what those two lines
did for Postgres and IQ, and go further, so that the graph algorithms Neo4j runs can run here, translated to SQL
where that is feasible. Read the two studies' method sections first (the Postgres study's §3.4, §8 and §9; the IQ
study's §2, §4 and §6): this line inherits their rules and reuses their harness.

§0 is the answer; §1 is what the two lines did, as the method; §2 is what Cypher is; §3 the spec sources, the corpus
and the oracles, in numbers; §4 the decisions; §5 the architecture; §6 recursion; §7 the algorithms; §8 the harness;
§9 the slices; §10 the risks; §11 the sources.

No reference checkout is used: legend-engine has no graph store and no Cypher reader (its non-relational stores are
MongoDB, Elasticsearch, the service store and flat data), and legend-pure has nothing of the kind. Like the IQ line,
this one is built from a spec and an oracle; there is nothing to port. The measurements below were taken on
2026-10-15 from a clone of `opencypher/openCypher` at `677cbafabb8c` (2026-03-20) and from Neo4j's published manuals
for the 2026.06 and 2026.07 releases.

---

## 0. Start here

1. **What the island is.** `#Cypher{...}#` makes a query written for Neo4j an ordinary Pure Relation expression:

   ```
   #Cypher{
     MATCH (p:Person)-[:firm]->(f:Firm)
     WHERE f.legalName STARTS WITH 'A'
     RETURN f.legalName AS firm, count(p) AS headcount
     ORDER BY headcount DESC LIMIT 10
   }#->filter(r | $r.headcount > 1)
   ```

   The translator reads Cypher and builds the Pure a person would write (`Person.all()->project(...)->groupBy(...)
   ->sort(...)->limit(10)`); the compiler types it once, like any query, and it runs on any backend legend-lite
   renders. It returns **Neo4j's answer**: three-valued `null`, Neo4j's ordering of mixed values, `1 = 1.0`, integer
   `/` that truncates, `count(*)` that counts rows.
2. **Why it fits better than SQL did.** Legend's class model is a property graph: labels are classes (a superclass is a
   label every subclass's node also carries), relationship types are association ends and to-one or to-many
   properties, node properties are properties. The Postgres line added "classes as tables" as a later phase; Cypher
   starts there, and the compiler already lowers `Person.all()->project(~[firm: p | $p.firm.legalName])` to joins
   through the mapping. A relationship with properties is an edge class, declared as one by a stereotype the
   compiler reads as it reads `<<equality.Key>>`; the graph a query sees is a data space, selected with Neo4j's own
   `USE` (§5.6). The pitch: graph queries over any warehouse lite renders, with no graph database.
3. **What is new, in two words: recursion and determinism.** Bounded paths (`*1..3`) unroll into a union of one-,
   two- and three-hop joins, which is "the unroll rearranges shapes" (`docs/TENET_CHARTER.md` C6.2) and needs nothing
   new. Unbounded paths, `shortestPath` and every iterative algorithm need the database to iterate. Every backend lite
   renders has `WITH RECURSIVE` in some form, and the dialect layer already emits one (`FoldsAsRecursiveCtes`, for
   BigQuery); what Pure lacks is a spelling of a fixpoint. That is one new native with one lowering rule per dialect
   (§6), and it touches C6.3 and C6.4 of the tenet charter, which name recursion as excluded or residue. **The user
   ruled on 2026-10-15 that recursion is required** for the graph queries and the algorithms, so the native is built
   (PR5, §6.3); what remains to agree is its shape. And "all of Neo4j's graph algorithms" has to mean "every
   deterministic one": about a third of the catalog is randomized or heuristic, and there is no answer to be right
   about (§7).
4. **The spec and the corpus are better than IQ's.** openCypher is Apache 2.0, with a published grammar (377
   productions) and a conformance kit, the TCK: 220 feature files, 1,615 scenarios, 3,880 with their examples
   expanded, each carrying its own expected rows or error, under Apache 2.0 (§3.1). It is on Maven Central as
   `org.opencypher:tck`, so it is test input exactly as the engine's parity YAML is. Its weakness is the function
   table: 17 of its mathematical feature files hold 6 scenarios between them; lite's own Neo4j-judged cases must fill
   that, as the Postgres line's `lite/*.yaml` did.
5. **The oracle is free and runs anywhere, and it is GPL.** Neo4j Community is GPLv3, so it is never a dependency. The
   IQ pattern fits: a local snap tool over the Apache 2.0 Python driver, answers committed, CI judges from the file
   with the STALE, UNSNAPPED and GONE guards. Unlike IQ, any machine with Docker or a JDK can add a case, and the Graph
   Data Science plugin loads in Community, so the same tool snaps the algorithms' answers (§3.3). LDBC Graphalytics,
   whose six algorithms are defined to be deterministic and come with reference outputs, is a second oracle that owes
   nothing to Neo4j (§3.4).
6. **The version switch (the user's nit of 2026-10-15).** Neo4j selects the language version with its own prefix
   inside the query, `CYPHER 5 MATCH ...` or `CYPHER 25 MATCH ...`. The island should honour exactly that and invent
   no tag syntax: one island type, `#Cypher{...}#`, the text inside it being what Neo4j accepts, the prefix
   included. For a read-only translator the two versions are one parser with a version flag (§2.1). **Decided by the
   user, 2026-10-15:** one island, the version in the query as Neo4j spells it, Cypher 25 the default (§4, C1, C9).
7. **The one-line rule of the line,** inherited: zero FAIL, zero BUG, zero ERROR, always. Where Neo4j's answer is not
   a function of the input (a tie `SHORTEST 1` breaks arbitrarily, a community a heuristic found), or where lite would
   have to compute a value in Java, the translator refuses by name and position.
8. **Size.** The Postgres and IQ readers are about 10k and 12k lines of main code. Cypher is a bigger grammar, plus
   lists, maps, nodes and paths as values, plus the native, plus the algorithm library: 20k to 25k lines over six to
   eight landings, each with its own cases, ledger, independent review and GATES entry (§9).

---

## 1. The method this line inherits

What the two lines did, as the rules a third one follows. The evidence is in their studies' §9.

- **One translator per language into one target.** `#SQL{}#` and `#IQ{}#` read text into a Pure Relation
  `ValueSpecification` that `compiler/spec/SqlIslands` hands to the Typer, which types it once; nothing is re-spelled
  as text and nothing is typed twice (invariant 1). The IQ line put Postgres's rules behind `InputDialect` and judged
  the move by the generated Pure of 2,225 cases, byte for byte.
- **The vendor is the spec; the oracle is real; CI judges from a file.** Postgres 16 runs embedded in CI and its
  answers are snapped (`sql-parity/postgres-answers.jsonl`), the Postgres lane checking the file against Postgres on
  every run. IQ cannot run here, so SQL Anywhere 17 is snapped locally by `tools/iq-oracle/snap.py` (1,142 lines of
  standard-library Python), and the lanes judge the committed answers with the STALE (an edited case is never judged
  by its old answer), UNSNAPPED and GONE guards.
- **Statuses that can only move with a dated entry.** PASS, FAIL (a wrong answer), BUG (answers what the oracle
  rejects), ERROR (errs where the oracle answers), REFUSED (by name), BOTH_ERROR, SKIP, per case per lane, in a ledger
  the test ratchets (REGRESSION, FIX DETECTED, STATUS MISMATCH).
- **A refusal by name beats a possibly wrong answer.** Each study's §9 lists what is refused and why, each reason
  measured on the oracle and on both lanes. The IQ line refused its riskiest provisional decisions outright until a real
  IQ can verify them.
- **Java never computes a value.** A missing semantic is a Pure spelling or a refusal; the IQ line's three natives
  turned out to be compositions of existing natives (its §9.8).
- **Randomized differentials and independent reviews.** The Postgres line's generator ran about 9,000 type-correct
  queries through both lanes against Postgres; every landing was reviewed by an agent that wrote none of it, each
  finding fixed and pinned by a case.
- **The ledgers at the last landings:** `sql-parity` 2,180 cases, PASS 1,527, 0 FAIL, 0 BUG, 0 ERROR; `iq-parity`
  about 1,600 cases, 0 FAIL, 0 BUG, 0 ERROR.

---

## 2. What Cypher is

### 2.1 Two versions, one prefix

Since Neo4j 2025.06 the server carries two language versions. **Cypher 5** is frozen as it was at 2025.06: bug fixes
and security updates only. **Cypher 25** is a clone of Cypher 5 that evolves with every monthly server release. A query
picks its version with Neo4j's own prefix, `CYPHER 5` or `CYPHER 25` before the first clause (other options may follow
it: `CYPHER 25 runtime=parallel`); otherwise the database's default applies: `db.query.default_language` in
`neo4j.conf`, `CREATE DATABASE ... SET DEFAULT LANGUAGE CYPHER 25`, or `ALTER DATABASE`. The defaults when nothing is
set: self-managed 2025.06 to 2026.01, Cypher 5; self-managed 2026.02 on, new installs write `CYPHER_25` into
`neo4j.conf` and a missing setting still means Cypher 5; Aura, Cypher 25 for new organisations.

What separates the two, as far as a **read-only translator** sees it (the 2025.06 to 2026.07 changelog):

| Cypher 25 adds | Cypher 25 removes | Changed |
|---|---|---|
| clauses `FILTER`, `LET`, `NEXT`, `WHEN`/`ELSE`; `UNION` and `UNION ALL` mixed inside braces; the `REPEATABLE ELEMENTS` and `DIFFERENT RELATIONSHIPS` match modes; `ACYCLIC` paths (2026.03); dynamic labels and types in `MATCH`; the `VECTOR` type and its functions; `allReduce()`, `format()`, `string.indexOf`, `string.join`, `string.regexReplace` (2026.05); GQL naming aliases (`collect_list`, `percentile_cont`, `zoned_datetime`, ...) | some identifier characters (a backticked composite part, unescaped `\u0085` and `$`); `SET n = node` (a write); `MERGE` reading another entity's property (a write); a few procedures | `WITH` no longer required between a read and a write clause (writes); `replace()` takes a limit; parameters may start with a digit; an imported variable in a `COLLECT`, `COUNT` or `EXISTS` subquery is a constant |

So for reads, Cypher 25 is Cypher 5 plus clauses and functions, minus two identifier spellings, with one scoping rule
changed. One reader with a version flag covers both; the flag decides which identifiers parse and whether the added
clauses are known. The island's spec is therefore **one pinned Neo4j release's Cypher 25** (§4, C9), with `CYPHER 5`
as the frozen profile.

**GQL.** ISO/IEC 39075:2024 is the standard Cypher 25 grows toward. Neo4j's conformance appendix (2026.06) claims
support for "the majority of mandatory GQL features", declares the open graph type (GG01), the mandatory property
types BOOLEAN, FLOAT (64-bit), INTEGER (64-bit) and STRING, and names its Unicode version by the JVM's. The standard's
text is not free; the openCypher grammar is published "in ISO WG3 BNF format" and is the free spelling of the same
language family. The island follows Neo4j's manual, not the ISO text, and says so.

### 2.2 The language a read-only translator meets

- **Clauses.** `MATCH`, `OPTIONAL MATCH`, `WHERE`, `WITH` (a pipeline boundary, with `DISTINCT`, `ORDER BY`, `SKIP`,
  `LIMIT` and `WHERE` of its own), `UNWIND`, `RETURN` (`*`, `DISTINCT`, aliases, implicit grouping), `ORDER BY`,
  `SKIP`, `LIMIT`, `UNION [ALL]`, `CALL { }` subqueries (with imported variables), `CALL proc() YIELD`, the Cypher 25
  `FILTER`, `LET`, `NEXT`, `WHEN`. Writes: `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, `FOREACH`, `LOAD CSV`. `USE`,
  `SHOW`, administration.
- **Patterns.** Node patterns with label expressions (`:A`, `:A:B`, `:A&B`, `:A|B`, `:!A`, `:%`); relationship
  patterns with a type expression, a direction or none, a variable, properties; fixed-length paths; the legacy
  variable-length `-[:T*1..3]->` (lower bound default 1, unbounded `*` allowed); GQL quantified path patterns
  `((a)-[:T]->(b) WHERE ...){1,3}` and quantified relationships `-[:T]->{1,3}`, `+`, `{,10}`, with group variables
  that are lists outside the pattern; named paths `p = (...)`; the legacy `shortestPath()` and `allShortestPaths()`;
  the path selectors `ANY`, `ANY SHORTEST`, `SHORTEST k`, `SHORTEST k GROUPS`, `ALL SHORTEST`, `ALL`; the match modes
  `DIFFERENT RELATIONSHIPS` (the default: a relationship is traversed once per `MATCH`, a node may repeat) and
  `REPEATABLE ELEMENTS`; `ACYCLIC`. Pattern comprehensions `[(a)-->(b) | b.name]`; `EXISTS { }`, `COUNT { }`,
  `COLLECT { }` subqueries.
- **Values.** `null`, booleans, 64-bit integers, doubles, strings, lists (any mix), maps, nodes, relationships, paths,
  temporal values (`DATE`, `LOCAL TIME`, `ZONED TIME`, `LOCAL DATETIME`, `ZONED DATETIME`, `DURATION`), spatial
  points, vectors, UUIDs (2026). Lists are 0-based, a negative index counts from the end, an index out of range is
  `null`, a slice out of range is truncated, `range(a, b)` is inclusive at both ends.
- **Functions.** The 2026.07 manual lists 176 entries in 19 categories: aggregating 15, list 20, string 20, scalar 25,
  numeric 8, logarithmic 6, trigonometric 16, predicate 7, temporal instant 31, duration 6, format 1, spatial 3,
  vector 6, UUID 3, graph 4, database 1, security 2, LOAD CSV 2, user-defined (types only). Function names are
  case-insensitive (the TCK calls `cOuNt(` and `nOdEs(`); labels, types, property keys and variables are
  case-sensitive.
- **Null** is three-valued throughout: `AND`, `OR`, `XOR`, `NOT` by the SQL tables; `x = null` and `x <> null` are
  `null`; `IN` is `true` on a match, `null` on no match with a `null` element, `false` on no match otherwise, and a
  `null` left side is `null` for a non-empty list and `false` for an empty one; `[][0]`, `head([])`, a missing property
  and `1 + null` are `null`; a `WHERE` keeps only `true`.
- **Ordering and equality.** `ORDER BY` uses one global order of types, maps first, then nodes, relationships, lists,
  paths, vectors, points, the temporal types, strings, booleans, UUIDs and numbers last, `null` after everything in
  ascending order; strings compare "lexicographically" (the manual does not say by code point or UTF-16 unit:
  to measure); `1 = 1.0` is `true` (the TCK's `Comparison1` pins it); lists compare element by element and maps by
  key; every comparison with `NaN` is `false`; `<` on nodes, relationships or maps is an error; a duration or a UUID
  never orders.
- **Arithmetic** is Java's: `/` of two integers is an integer, `10 ^ 5` is the double `100000.0`, an integer result
  out of 64 bits is an error (the TCK's `IntegerOverflow`), and so is a double result out of range (the TCK's
  `FloatingPointOverflow`). Truncation direction and the sign of `%` are not stated in the manual: Java's, to
  measure.
- **Strings** are Unicode characters, never UTF-16 units (`size()` of a supplementary character is 1); `substring` is
  0-based, a negative or a null length is an error; `left` and `right` refuse a negative count; `btrim`, `ltrim`,
  `rtrim` take a character set; `split` takes a delimiter or a list of them; `toString` writes a double as `11.5` and a
  boolean as `true`; `=~` is a Java regular expression over the whole string.
- **Aggregation.** Non-aggregated `RETURN` or `WITH` items are the implicit group keys; `count(*)` counts rows,
  `count(x)` non-nulls; `avg` of integers is a double, `sum` of integers an integer, `sum` of nothing `0`, `avg` of
  nothing `null`; `collect` skips `null` and is `[]` over nothing; `min` and `max` rank mixed values by the global
  order; `percentileDisc` is the nearest value "by rounding", `percentileCont` interpolates; `stDev` of nothing is
  `null` since 2026.05 and was `0` before, which is the kind of change a pinned release freezes (C9).
- **Output names.** A returned expression without an alias is named by its text, as IQ names an unaliased item.

### 2.3 Where Cypher differs from SQL and from Pure

The rows the translator spells out from the first slice. Each is in our words from the manual or the TCK, and marked
where it is still to measure on the oracle.

| Behaviour | Cypher (Neo4j) | SQL (the Postgres island) | Pure | Id |
|---|---|---|---|---|
| A property no entity of the label has | `null` | a compile error (no such column) | a compile error | C4: a model has a schema; refused, not `null` |
| Numeric equality across kinds | `1 = 1.0` is `true` | `true` | `true` | agree |
| `=` across kinds | `1 = 'a'` is `false`, never `null` (to measure); `<` is an error for incomparable kinds | an error | an error | spelled per kind |
| `null = null` | `null` | `null` | `[] == []` is `true` | three-valued pairs, as `SqlToPure.Tx` |
| `IN` with `null` | the rule of §2.2 | the same | `in` is two-valued | the same spelling the Postgres island uses |
| Ordering of mixed kinds | one global order, `null` last ascending | an error | an error | a column of one kind orders; a mixed column is refused |
| Integer `/` | integer, Java's truncation | integer, toward zero | `Number`, a float on DuckDB | the Postgres island's exact quotient |
| Integer overflow | an error | an error | the database's | the Postgres island's checks only where a result can leave 64 bits |
| `^` | always a double | `^` of numerics is numeric | `pow` | `pow` then a double |
| String positions | 0-based `substring`, Unicode characters | 1-based, characters | `substring` 1-based in the relational spelling | the offset shifted by one |
| String order | lexicographic, basis unstated | the collation's | the database's | DuckDB and the embedded Postgres compare bytes, which is code-point order; equal to UTF-16 order inside the BMP; to measure beyond it |
| Case folding | Java's, all of Unicode | the collation's | the database's | DuckDB's `upper` folds all of Unicode; Postgres's in the C locale folds ASCII only: `toUpper` on the Postgres lane is refused for a value that may hold a non-ASCII letter until measured |
| Lists and maps as values | first-class | arrays and `jsonb` | collections, `Variant` | `Variant` (jsonb on Postgres, JSON on DuckDB), as the Postgres island reads `jsonb` |
| A node as a value | labels, properties, an id | a row | an instance | a `Variant` of labels and properties (C8); `id()` and `elementId()` refused |
| A path as a value | nodes and relationships in order | none | none | a `Variant` list (C8) |
| Output name of an unaliased item | the expression's text | `?column?` | the lambda's | the text, as IQ |
| `count(*)` of no rows | 0 | 0 | 0 | agree |
| Relationship uniqueness | once per `MATCH` | none | none | spelled as `r1 <> r2` per unrolled hop |
| Undirected pattern | each relationship matched in both directions | none | an association's two ends | a union of the two directions |
| Procedures (`CALL db.*`, `apoc.*`) | the server's | none | none | refused by name |
| Writes | `CREATE`, `MERGE`, `SET`, `DELETE`, `REMOVE` | none in lite yet | `write` is F1 of `docs/BI_AND_ETL_PLAN_2026_09_29.md`, not built | refused by name until F1 |

### 2.4 What each side cannot say

- **Cypher cannot say** a class's keys, a milestoned navigation, a mapping, a union of stores, an M2M transform, a
  typed function signature, a constraint. A model written for the island is written in Pure (or Python, or DataCube's
  "Make a class"); the island queries it.
- **Legend cannot say,** today, a property on a relationship (associations carry none: an edge class says it,
  §5.6), a label-less node, a dynamic label, a node of two unrelated classes at once. Each is a decision (§4, C3)
  or a refusal.

---

## 3. The spec sources, the corpus and the oracles, in numbers

### 3.1 openCypher: the grammar and the TCK

**License.** The openCypher project is Apache 2.0 (its `LICENSE`, its README's "The openCypher project is licensed
under the Apache license 2.0", the TCK README's "licensed with Apache license 2.0, which is inherited"). Each feature
file carries Neo4j Sweden AB's Apache header with an attribution notice: an implementation extended beyond the
consensus process "should not be described as Cypher or as openCypher" but as an implementation extension. The
island is named `#Cypher{...}#` after the language it reads; the documentation says "legend-lite's Cypher island",
never "a Cypher implementation".

**The grammar.** `grammar/openCypher.bnf`: 1,533 lines, 377 productions, in the ISO WG3 notation, the free spelling
of the language family GQL and Cypher 25 belong to. The IQ research had 244 productions. Neo4j's own Cypher 25 ANTLR
grammar lives in the GPL server tree and is not read.

**The TCK, measured** (`tck/features`, clone `677cbafabb8c`, 2026-03-20; the census scripts are in the session's
scratch, 60 lines of Python over the feature files):

| | feature files | scenarios | with examples expanded | expect an error | the query writes | read-only and expecting rows |
|---|---|---|---|---|---|---|
| `clauses/` | 93 | 827 | 1,251 | 120 | 288 | 457 |
| `expressions/` | 125 | 758 | 2,599 | 72 | 24 | 668 |
| `useCases/` | 2 | 30 | 30 | 0 | 0 | 30 |
| **all** | **220** | **1,615** | **3,880** | **188** | **312** | **1,155** |

Per family (scenarios; errors; writes; read-only expecting rows):

| family | scenarios | errors | writes | read-only | what it holds |
|---|---|---|---|---|---|
| clauses/match | 161 | 21 | 1 | 139 | nodes, relationships, fixed and variable-length patterns, named paths, optional match, the named graphs |
| clauses/match-where | 34 | 2 | 0 | 32 | filters, equi- and non-equi-joins, null predicates |
| clauses/return, return-orderby, return-skip-limit | 129 | 31 | 4 | 97 | projection, renaming, implicit grouping, ordering by type, skip and limit |
| clauses/with, with-orderBy, with-skip-limit, with-where | 156 | 11 | 1 | 144 | the pipeline; `with-orderBy` alone expands to 292 examples |
| clauses/union | 12 | 4 | 0 | 8 | |
| clauses/unwind | 14 | 0 | 2 | 12 | |
| clauses/call | 41 | 16 | 0 | 25 | procedures: refused by name (no procedures here) |
| clauses/create, merge, set, delete, remove | 280 | 35 | 280 | 0 | writes: refused by name until F1 |
| expressions/literals | 131 | 25 | 0 | 106 | every literal spelling; 25 syntax errors |
| expressions/quantifier | 100 | 4 | 0 | 96 | `all`, `any`, `none`, `single`: 604 expanded examples |
| expressions/list | 94 | 6 | 4 | 84 | indexing, slicing, comprehension, `IN`, `range` |
| expressions/temporal | 89 | 0 | 12 | 77 | 1,004 expanded examples: dates, times, zones, durations, truncation |
| expressions/graph | 48 | 8 | 6 | 34 | `labels`, `type`, `properties`, `keys`, `nodes`, `relationships` |
| expressions/precedence | 43 | 0 | 0 | 43 | |
| expressions/boolean, comparison, null | 85 | 5 | 0 | 80 | three-valued logic, comparison across types |
| expressions/pattern, path, existentialSubqueries | 53 | 8 | 2 | 45 | pattern comprehensions, path functions, `EXISTS { }` |
| expressions/aggregation | 27 | 3 | 0 | 24 | |
| expressions/map, conditional, typeConversion | 50 | 8 | 0 | 42 | |
| expressions/string | 32 | 0 | 0 | 32 | 14 files; thin |
| expressions/mathematical | 6 | 1 | 0 | 5 | **17 files, 6 scenarios**: 13 of the files are empty stubs (division, modulo, power, rounding, logarithms, trigonometry) |
| useCases | 30 | 0 | 0 | 30 | counting subgraph matches, triadic selection |

What the census says:
- **1,155 scenarios are the island's target** from the first slice: read-only, expecting rows. 188 expect an error
  (156 of them `SyntaxError` at compile time: the syntax ratchet, free). 312 write and are refused by name until F1
  gives lite writes; 41 call procedures and are refused by name.
- **723 scenarios set their graph up with `having executed: CREATE ...`**, and 19 use one of the two named graphs
  (binary trees of depth 4 with three relationship types). The harness must turn those `CREATE`s into tables (§8.2);
  the oracle does that for it.
- **The TCK is written for a schema.** Its property-naming convention assigns a type per name (`num` integer, `name`
  string, `price` float, `ok` boolean, `var` for the few mixed-type cases): only 7 feature files use `var`. A model
  inferred from each scenario's `CREATE`s therefore types almost every property, and the handful of mixed-type
  scenarios are refused by name.
- **Values in the expected rows** are written in a small grammar the judge must read: integers, decimals, `NaN`,
  `Inf`, single-quoted strings, `true`, `null`, `[...]`, `{k: v}`, `(:L1:L2 {p: 0})`, `[:T {p: 0}]`, and paths
  `<(:A)-[:T]->(:B)>`. Nodes and relationships compare by labels, type and properties, never by id. Rows are a set
  unless the `RETURN` carries `ORDER BY`; "in order" then means ordered by the first sort key only.
- **Functions used**, 65 distinct: `count` 143, `collect` 58, `reverse` 58, `all` 48, `any` 46, `size` 42, `sum` 32,
  `none` 31, `single` 27, `toString` 24, `labels` 23, `keys` 21, `type` 16, `toInteger` 15, `nodes` 12, `coalesce`
  10, `range` 10, `tail` 10, `max`, `min`, `properties` 9 each; the temporal constructors and `truncate`s about 150
  between them; `test.my.proc` 28 (procedures). The whole string and mathematical table is barely touched: **lite's
  own cases supply it** (§8.1), judged by Neo4j, as the Postgres line's `lite/functions.yaml` (139 cases) did.
- **`rand()`** appears in 116 expanded examples, all in the quantifier outlines, where it feeds a predicate's operand;
  those are volatile and are skipped, as `sql-parity`'s seven volatile cases are.

### 3.2 Neo4j's manuals: read, never copied

The Cypher manual, the GDS manual and the operations manual are the spec of what Neo4j answers. Their terms are
Neo4j's; the Bolt protocol documentation is CC BY-NC-SA 4.0 (its repository's `LICENSE.txt`). As with SAP's manuals
(the IQ study's I7): nothing from them is copied into this repository. Behaviour is written in our words with the
manual's page named; a case's query is ours; the review of each landing checks the diff for quoted text.

### 3.3 The oracle: Neo4j Community, local only

- **Neo4j Community Edition is GPLv3** (the server; Enterprise is closed and commercial). It cannot be a dependency of
  anything here, test-only included: no jar on any classpath, no `neo4j-harness`, no Testcontainers in CI (the hosted
  macOS and Windows runners have no Docker anyway: `tools/postgres/postgres.bzl`).
- **The Python driver is Apache 2.0** (`neo4j-python-driver`, `LICENSE.APACHE2.txt`), over Bolt. The snap tool is
  Python's standard library plus that driver, run isolated (`python3 -I`), as `tools/iq-oracle/snap.py` is.
- **The server runs locally**: the official image `neo4j:2026.07.1` (Community tags carry no suffix; `-enterprise`
  does) with `NEO4J_PLUGINS='["graph-data-science"]'`, which Neo4j documents for development, or the Community
  tarball on a JDK 21 for a machine without Docker. GDS is available in Community, capped at 4 cores.
- **CI judges from the committed answers.** `cypher-parity/neo4j-answers.jsonl`, one line per case, with the server
  version, the Cypher version the case ran under, the `sqlSha` over query, version and schema (STALE), and the
  dumped graph for scenarios that set one up (§8.2). `snap.sh --check` is the drift check a landing records in GATES.
- **A pinned release** (C9). Cypher 25 changes monthly (`stDev` of nothing, 2026.05). The answers carry the server
  version; a case re-snapped on a newer server that answers otherwise is a FIX DETECTED or a REGRESSION to adjudicate,
  as a drifted Postgres answer was on 2026-10-13.

### 3.4 A second oracle for the algorithms: LDBC Graphalytics

The LDBC Graphalytics benchmark (specification v1.0.5, CC-BY) defines six algorithms **to be deterministic** and ships
reference outputs with its datasets, validated by exact match (BFS), equivalence of partitions (WCC, CDLP: a two-way
mapping between the labels) or an epsilon match (PageRank, LCC, SSSP). Its definitions, in our words:

| Algorithm | Definition | Validation |
|---|---|---|
| BFS | each vertex's depth from the root; unreachable is `9223372036854775807` | exact |
| PageRank | `PR0(v) = 1/|V|`; `PRi(v) = (1-d)/|V| + d * Σ PR(u)/outdeg(u) over in-neighbours + (d/|V|) * Σ PR(w) over sink vertices`; a fixed number of iterations; doubles | epsilon |
| WCC | a component label per vertex, edges read as undirected | partitions equivalent |
| CDLP | `L0(v) = v`; each iteration every vertex takes the most frequent label among its in- and out-neighbours (a neighbour on both sides counts twice), the **smallest** label on a tie, its own label with no neighbours; synchronous; a fixed number of iterations | partitions equivalent |
| LCC | 0 for fewer than two neighbours, else the edges among the undirected neighbour set divided by `|N|(|N|-1)`, directions enforced inside | epsilon |
| SSSP | the weighted shortest distance from the root, non-negative double weights, unreachable is infinity | epsilon |

Why it matters: GDS's PageRank is the 1998 formula unnormalized (scores are not scaled to sum to 1 unless
`scaler: "L1Norm"` is asked), with `dampingFactor 0.85`, `maxIterations 20`, `tolerance 1e-7` and an early stop, and
its manual does not say how a dangling node's mass is handled; GDS's label propagation breaks ties "arbitrarily but
deterministically" by an order the manual does not state. Graphalytics states every such rule. So an algorithm here
is **one computation with the spec's constants as parameters** (initial value, teleport term, sink handling, tie rule,
iteration count or tolerance), judged against the oracle whose rule it was given: Neo4j for GDS's profile, the
reference outputs for Graphalytics's. Where GDS's rule is not stated and cannot be measured to a fixed rule, GDS's
profile of that algorithm is refused by name and Graphalytics's is offered.

### 3.5 Other tools, and why none is a dependency

- **Cypher for Apache Spark / Morpheus** (openCypher, Apache 2.0, archived): Cypher over DataFrames, graphs stored as
  one scan table per label and per relationship type, a fixed schema inferred or declared. The closest prior art to a
  relational, typed translation; its storage model is the model's own here (a class per label).
- **Graphiti** (2025) translates `MATCH` to inner joins and `OPTIONAL MATCH` to outer joins, syntax-directed; **Raqlet**
  (CIDR 2026) compiles Cypher and SQL/PGQ through a Datalog-like form to recursive SQL. Both confirm the shape of §5
  and §6; neither is a runtime for a JVM, TeaVM and GraalVM product.
- **Apache AGE** (Apache 2.0): openCypher inside Postgres, with its own `agtype`. A C extension built per platform;
  not loadable into the embedded Postgres the lanes run, and not Neo4j's answers.
- **DuckPGQ** (SQL/PGQ in DuckDB, a community extension): `MATCH` with `ANY SHORTEST` and bounded hops, a multi-source
  BFS operator. It targets DuckDB 1.4.4 and is documented as unavailable for 1.5.x, which `MODULE.bazel` pins (1.5.5),
  and as incomplete. Not a base; a possible render target for DuckDB later, as `QUALIFY` is for Snowflake.
- **Kùzu** (MIT, embeddable, Cypher): archived on 2025-10-10 after its acquisition; forks exist. Not an oracle.
- **DuckDB's `USING KEY`** recursion (since 1.3): a recursive CTE whose rows are keyed, so an iteration **replaces** a
  node's row instead of appending it. Built for exactly the iterative algorithms of §7; the DuckDB render of the
  recursion native uses it where the native's shape is keyed.

---

## 4. Decisions (proposed, for the user)

`C` for Cypher, distinct from the Postgres study's `D` and the IQ study's `I`.

- **C1. The island is `#Cypher{...}#`; the version is Neo4j's own prefix inside the text.** The user's nit of
  2026-10-15 asked whether the version should be a profile, a tagged value or a stereotype, or two island types
  `#Cypher5{...}#` and `#Cypher25{...}#`, rather than a change to the parser that upstream's parser would not read.
  The facts:
  - lite's lexer reads an island opening as `#`, then everything up to the first `{` or `#` (`lexer/Lexer.java`, the
    `ISLAND_OPEN` scan), so `#IQ:iq16-default{`, `#Cypher25{` and `#Cypher{` all lex alike today; the parser's
    `parseDsl` arm decides what a type means;
  - legend-engine's grammar refuses every island type it does not know, so `#IQ{`, `#IQ:x{`, `#Cypher{` and
    `#Cypher25{` are all equally foreign to upstream: the LEGEND_ENGINE surface refuses them, the own-corpus census
    declares them (`docs/OWN_CORPUS_DECISIONS.md` §14 for IQ). No choice among them is more upstream-compatible than
    another;
  - Neo4j itself puts the version **in the query**: `CYPHER 25 MATCH ...`.

  The proposal: one island type, no tag syntax, the text inside it exactly what Neo4j accepts, the `CYPHER 5` or
  `CYPHER 25` prefix honoured as Neo4j honours it. A query copied from Neo4j Browser runs unchanged. The default
  without a prefix is **Cypher 25** (what Aura's new organisations and 2026.02's new installs get, and where GQL's
  clauses live); `CYPHER 5` is the frozen profile. A stereotype or tagged value on the enclosing function is weaker:
  a bare query in Query, DataCube or Python has no function to carry it, and the island would not describe itself.
  `#Cypher5{...}#` and `#Cypher25{...}#` as sugar for the prefix are harmless and cost one more `parseDsl` arm each;
  they can be added if the user prefers the version visible in the tag. The wire shape is a `classInstance` of type
  `Cypher` with `{"cypher": <text>}`, as `IQ` carries `{"sql", "profile"}`.
  **Decided by the user, 2026-10-15: one island type, `#Cypher{...}#`, the version in the query as Neo4j spells it;
  no sugar types; no stereotype or tagged value for the version (the query carries it, so nothing in the model
  needs to).**
- **C2. The graph is the class model.** A label is a class the data space (or the query's mapping and runtime) can
  query; a node of class `Employee extends Person` carries both labels; a relationship type is an association end or
  a to-one or to-many property, by name; a node property is a property. The island therefore runs over any model
  that `Person.all()` runs over, on every backend, with no new store kind. Cypher over bare tables is "Make a class"
  first (DataCube's model line), never a parallel declaration.
- **C3. Relationship types, directions and edge properties.**
  - `(p:Person)-[:firm]->(f:Firm)` reads property `firm` of `Person`; `(f)-[:employees]->(p)` the other end;
    `(p)-[:PersonFirm]-(f)` the association by name, either direction. A type that names nothing on the left label is a
    compile error naming the label's properties.
  - **A relationship with properties is an edge class, declared as one** (§5.6, the full design; the user on
    2026-10-15: "let's not shortcut"). Pure's `Association` has exactly two properties and nothing else
    (`model/AssociationDefinition`), so the only Legend element that can hold a relationship's own properties, keys
    and mapping is a class: `Acting` with `actor: Actor[1]`, `movie: Movie[1]` and `role`. The class says it is an
    edge, and which ends are which, by an ordinary Pure `Profile` in a library project (`graph::graph`, with the
    four stereotypes `Edge`, `from`, `to` and `type`, and no tags), which the engine and Studio compile as any
    profile and which lite reads by name exactly as it reads `<<equality.Key>>` (`compiler/element/EqualityKeys`,
    `PlatformTypes.EQUALITY_PROFILE`): `Class <<graph.Edge>> demo::Acting` whose ends are the stereotyped
    properties `<<graph.from>> actor: Actor[1]` and `<<graph.to>> movie: Movie[1]`, declared on the class or
    injected by two associations (the decomposed association class, the idiomatic form), and whose type is a
    stereotyped derived property spelling one value of the model's `RelationshipType` enumeration: element
    references the engine checks, never strings.
    Then `(a:Actor)-[r:ACTED_IN]->(m:Movie) RETURN r.role` is Neo4j's own spelling, `r`'s identity is Pure's
    instance equality, its keys if it declares them and otherwise the row itself as its mapping's primary key
    realizes it, never a model property (C5), and the class maps to its bridge table like any class, which is what SQL/PGQ's "edge table with a source
    key and a destination key" is. Nothing is inferred from a class's shape (invariant 4); an undeclared class is a
    label. The graph's exposure and execution context are the data space's, selected with Neo4j's own `USE`
    (§5.6): no new element kind, and no lite-only model construct.
  - Multiple labels on one node pattern (`:Person:Employee`, `:Person&Employee`) is the most specific class of the
    set when the set is a chain of the model's inheritance, else no rows (`&`) or a union (`|`). `:!A` and `:%` and a
    label-less `(n)` over the whole model are a union of every queryable class with a common schema (id, labels,
    properties as a Variant): offered for small models, refused by name over a bound the slice measures.
  - A dynamic label (`:$(expr)`, Cypher 25) is refused: the model is the schema.
- **C4. The static rule.** A property no class of the pattern's label declares is a compile error with Neo4j's
  wording and the model's candidates, not `null` (a register row). A property typed differently on two classes a
  union reads is refused by name. This is where lite is stricter than Neo4j on purpose, and the TCK's naming
  convention (§3.1) makes it cheap.
- **C5. Identity is Pure's instance equality: the model's `<<equality.Key>>` where a class declares it, instance
  identity otherwise, realized by the mapping and never by a model property** (the user, 2026-10-15, twice: no
  physical key leaks into the model, Legend's developers "care about that a lot", with local mapping properties for
  what the class must not show; and `<<equality.Key>>` is the object world's identity, `~primaryKey` the relational
  mapping's). The two are layers, not alternatives:
  - **`<<equality.Key>>` is the model's identity**: when two instances are the same thing, by business key (a
    `Movie` is its `title` and `year`). It is what Pure's `==`, `equal()` and `removeDuplicates()` use on instances
    (D91, `lowering/InstanceEquality`'s keyed half, on every lane), and it holds across stores and across a union
    mapping that reads one entity from two tables.
  - **`~primaryKey` is the relational mapping's**: which columns make one row one object in that mapping,
    `~primaryKey(...)` where declared, else the main table's `PRIMARY KEY` columns (`ClassMapping.primaryKey`,
    `lineage/PkInference`), which the engine itself uses to merge rows into objects. It is physical, so it never
    enters the model; a physical attribute a mapping needs and the class must not show is a local mapping property
    (`+x: Type[1]`, `PropertyMapping.LocalProperty`).
  - **Pure's rule composes them**, and it is the graph's rule: a keyed class's node is its key, so two rows with one
    key are one node; a keyless class's node is the row itself, and the relational realization of "the row itself"
    is the mapping's primary key. So `DISTINCT`, `count(DISTINCT n)`, `collect(n)`, `n = m`, relationship
    uniqueness and path equality are spelled in the Pure the island builds as instance equality, `==` and
    `removeDuplicates()`, the Pure a person would write, and lowered per the class's keyedness.
  - **The dependency, stated:** the identity half of `InstanceEquality` (the minted `__id` from the mapping's
    primary key, for keyless classes) "stays verdict-lane-gated" today. C5 needs it on the product lane: the
    database owner's leg, announced in `IN_FLIGHT` before PR3 (the model slice), and until it lands a pattern that
    needs identity over a keyless class is refused by name, never approximated by comparing properties. A keyless
    class whose mapping has no primary key either is refused likewise.
  - **One difference from Neo4j, a register row:** Neo4j's identity is always physical, so where a table holds two
    rows with one business key, Neo4j shows two nodes and lite one, because the model said so.
  - `id()` and `elementId()` are refused: Neo4j's are numbers no model has, and the physical key is exactly what
    must not leak.
- **C6. The recursion native and the bounded-unroll-first rule.** A bounded path or quantifier (`*1..3`, `{1,3}`,
  `{,10}`) unrolls into a union of fixed-length joins: no new native, C6.2's "rearranges shapes". Unbounded `*`, `+`,
  `shortestPath`, the `SHORTEST` selectors over unbounded patterns, and every iterative algorithm need the native of
  §6. **Decided by the user, 2026-10-15: recursion is required** ("for these graph queries and algos for sure"), so
  the native is built, in PR5, as the world map's kind 1 (one lowering rule per dialect), and the charter's C6.3 and
  C6.4 gain a dated note saying so. Still to agree, in PR5's design: its name and shape (§6.3), the termination guard,
  and its owner (it lands in the database owner's files). Until PR5 the unbounded forms are refused by name; the
  first four slices need none of them.
- **C7. Algorithms: the three buckets of §7 and the refusal rule.** An algorithm whose answer is a function of the
  input is translated and judged; one that is deterministic but costly is bounded by a size the slice measures, as
  the IQ island bounds its Pure node count; one whose answer is not a function of the input (randomized, heuristic,
  order-dependent) is refused by name. A tie Neo4j breaks arbitrarily (`SHORTEST 1`, `ANY`) is refused where the tie
  can occur and answered where it cannot (`ALL SHORTEST`, `SHORTEST k GROUPS`, distances, counts).
- **C8. Result values.** A scalar column is a Relation column. A list, a map, a node, a relationship or a path is a
  `Variant` column (jsonb on Postgres, JSON on DuckDB; the Postgres island's S49 rule): a node as
  `{"labels": [...], "properties": {...}}` built in SQL (`SqlExpr.JsonObject`, which the lowering already emits for
  Variant egress), a path as a list of nodes and relationships. The TCK's judge reads them structurally (§8.3). A
  temporal value is a Pure date where Pure has one (`DATE`, `LOCAL DATETIME` as `DateTime`); zoned types, times and
  durations are refused by name until a spelling exists.
- **C9. The spec is one pinned Neo4j release**, its Cypher 25 the default and its Cypher 5 the profile, named in the
  answers file. The first candidate is the newest Community release at the first landing (2026.07.x today).
  Re-pinning is a landing of its own, as a Postgres bump is. **Decided by the user, 2026-10-15: the newest, most used
  feature set, Cypher 25, is the default**; a query says `CYPHER 5` to get the frozen one.
- **C10. Placement and contention.** A new line (the eleventh, if the numbering holds), announced in `IN_FLIGHT` with
  its files. New: `core/src/main/java/com/legend/protocol/cypher/` (lexer, parser, AST, `CypherToPure`, the function
  table), `compiler/spec/CypherIslands.java`, `core/src/test/resources/cypher-parity/`, `tools/neo4j-oracle/`, the
  tests. Edited: the ten island plumbing sites of §5.3. Not touched until the recursion ruling: `lowering/`,
  `sql/dialect/`, `builtin/Pure.java`. The translator is a **sibling** of `SqlToPure`, not an `InputDialect` of it:
  Cypher's AST is not `SqlAst`. It shares the Pure-building helpers `SqlToPure` made public and copies the
  three-valued `Tx` discipline as a pattern; extracting the shared half into one builder is a later pure move judged
  by byte-identical Pure, as the IQ line's PR1 was, and only if the duplication hurts.

Decided by the user on 2026-10-15: C1 (one island, the version in the query, no sugar), C3 (the edge class design of
§5.6 as it stands after the day's questions: four stereotypes from a library profile, the ends on stored, inherited,
association-injected or derived properties, the type an enumeration value, the data space as the graph, `USE` to
select it; "awesome, really like this plan"; the class form primary, §5.7; identity layered as C5 says), C6
(recursion is required: the native is built), C9 (Cypher 25 is the default; the spec is the newest Neo4j Community
release at each landing: "neo4j latest release"), the names (the profile `graph::graph` with `Edge`, `from`, `to`,
`type`, in the style of `meta::pure::profiles::equality`: "profile name you choose best"; the library project
`projects/graph`; the model's own `RelationshipType` enumeration), and **the start: the line begins on 2026-10-15
("let's start implementing"), announced in `IN_FLIGHT` as the eleventh line; the plan is
`docs/CYPHER_PLAN_2026_10_15.md`.** Open: the native's name, shape, guard and owner (C6, in PR5's design).

---

## 5. Architecture

```
#Cypher{...}#  ─▶ CypherLexer, CypherParser (protocol/cypher/) ─▶ CypherAst (spans) ─▶ CypherToPure ─┐
                                                                                                    ▼
Typer's island arm ─▶ compiler/spec/CypherIslands: types each label's class through the model,      a Relation
                     answers the translator's questions (a label's properties and their types,      ValueSpecification
                     an association's ends, a class's keys, the variables in scope)                  │
                                                                                                    ▼
                                              NameResolver ─▶ Typer types it once ─▶ G½ … K   (unchanged)
```

### 5.1 The reader: `protocol/cypher/`

- **Hand-written**, lexer plus Pratt parser, like `SqlParser` and `IqParser`: core compiles to TeaVM WebAssembly and
  GraalVM native image, and the protocol layer depends on nothing but the JDK, `values` and JSON
  (`ArchitectureTest.protocolIsTheBottomLayer`).
- **Grammar:** Cypher 25 as the pinned release's manual spells it, with the version flag; the openCypher BNF is the
  cross-check of the productions. Every clause parses, writes included, so that a write is refused by name after
  parsing with Neo4j's wording, not as a syntax error.
- **AST:** records with spans and the written case of every name; errors positioned in the enclosing Pure source as
  `SqlIslands.position` places them.
- **Printer:** a fixpoint, `parse(print(ast)) == ast`, over the TCK's queries.
- **The function table as data:** Neo4j name (case-insensitive) → the Pure spelling and the argument rule; an unknown
  name is a positioned error naming the nearest known one.

### 5.2 The translator: Cypher AST → Relation `ValueSpecification`

The target is the Relation API, entering at `Compiler.query` through the island arm, as the SQL islands do. The
clause mapping:

| Cypher | Relation API |
|---|---|
| `MATCH (n:Label)` | `Label.all()` as a relation of the class's columns (the rows of a `project` over every property the query reads; the scope keeps the class so later hops navigate) |
| `MATCH (a)-[:prop]->(b)` | a navigation `$a.prop`, which the mapping lowers to the join; for a to-many end a row per target, as `project` flattens |
| `MATCH (a)-[:T]-(b)` (undirected) | `concatenate` of the two directions |
| a second `MATCH` | `join` on the shared variables, INNER |
| `OPTIONAL MATCH` | `join` LEFT, its `WHERE` inside the join condition (Neo4j's rule: a `WHERE` on an `OPTIONAL MATCH` is part of the match) |
| `WHERE` | `filter`, three-valued, as `SqlToPure.Tx` spells it |
| `WITH` | a pipeline cut: `extend` + `select`; `WITH DISTINCT` adds `distinct`; its `ORDER BY`, `SKIP`, `LIMIT` sort and slice at that point |
| `UNWIND list AS x` | `lateral` + `flatten` over the list value, as the Postgres island reads `unnest` |
| `RETURN items` with aggregates | `groupBy` on the non-aggregated items, `extend` for expressions over the keys |
| `RETURN DISTINCT` | `distinct` |
| `ORDER BY`, `SKIP`, `LIMIT` | `sort`, `drop`, `limit` |
| `UNION`, `UNION ALL` | `concatenate->distinct`, `concatenate` |
| `EXISTS { }`, `COUNT { }`, `COLLECT { }` | `exists`, `size`, a list aggregate, over the subquery's relation correlated to the enclosing row (the Postgres island's subquery machinery) |
| `CALL { WITH x ... }` subquery | a correlated relation joined back, or refused by name where it reads nothing of the outer row and returns many rows (a cross product: feasible, measured) |
| `collect(x)` | the list aggregate (`SqlAgg.Fn.LIST`, which `Lowerer` already emits for lists of scalars); `collect(n)` a list of Variants (C8) |
| `[(a)-->(b) \| b.name]` | the list aggregate over a correlated navigation |
| `*1..3`, `{1,3}` | the union of the fixed-length unrolls, each with the uniqueness conjuncts |
| `*`, `shortestPath`, `SHORTEST k` | the recursion native (§6), after the ruling; refused before |
| `CASE`, `coalesce`, arithmetic, strings, lists | the Postgres island's spellings, Cypher's rules (§2.3) |
| `n` (a node) as a value | a Variant built from `labels` and the properties the class declares (C8) |
| `properties(n)`, `keys(n)`, `labels(n)`, `type(r)` | the Variant's parts, or constants the model knows at compile time (`labels(n)` of a node of one class is a spelled list: C6.2's "spelled tokens", no computation) |

The generated Pure should read like hand-written Pure, as the IQ showcase's does; the harness dumps it per case so that
a review reads it.

### 5.3 Plumbing (the cross-area edits a landing announces)

The island type sites, from the IQ island's own diff and a grep of `SqlIsland` today:

| File | Edit |
|---|---|
| `protocol/spec/CypherIsland.java` (new) | the record: text, span, version as written (null when the text carries none) |
| `protocol/spec/ValueSpecification.java` | the sealed permits list, `children()`, `withPos` (three sites) |
| `parser/SpecParser.java` | the `parseDsl` arm for `Cypher` (and the sugar types, if C1 wants them); the LEGEND_ENGINE surface keeps its refusal |
| `protocol/SpecIslandReader.java` | the `classInstance` type `Cypher` |
| `protocol/ProtocolEmitter.java` | two sites (the island with and without a span) |
| `protocol/PureComposer.java` | `#Cypher{` + text + `}#` |
| `compiler/NameResolver.java` | the island passes through, as `SqlIsland` does |
| `compiler/spec/Typer.java` | the island arm, one line, to `CypherIslands.desugar` |
| `compiler/spec/CypherIslands.java` (new) | the seam: the model's classes, properties, associations, keys and store types, the variables in scope; positions; the error kinds |
| guards | `CodeShapeGuardrailTest` (the reader's cursor fields), `PlatformNamesGuardrailTest` (keyword arms), `OwnDialectCensusTest`, `OwnCorpusConformanceTest`, `FixtureAdjudicationTest`, `corpus-manifest.tsv` (the island as a declared lite extension, LITE-DESIGN-cypher-island, `docs/OWN_CORPUS_DECISIONS.md` §15) |
| docs | `docs/SEMANTICS_REGISTER.md` rows (C4's property rule, string order, case folding, Variant egress, each refusal that is a difference from Neo4j), `docs/GATES.md`, `docs/IN_FLIGHT.md` |

Not touched by the first slices: `lowering/`, `sql/dialect/`, `builtin/`, the lexer (the island opening lexes already),
`SqlToPure.java` and `InputDialect.java` (the seventh and tenth lines' seam).

### 5.4 Constraints

- Invariant 7b: `protocol/**` depends only on protocol, values, JSON and the JDK.
- TeaVM: no JDK method it lacks; each landing builds `//sdlc-server:page` and `//wasm:planner`.
- `//parser-equivalence:diagnostics` on every landing that touches a parser, lexer or protocol file.
- Java never computes a value: `labels(n)` of a known class is a spelled constant, `size([1,2,3])` is the database's.

### 5.5 Bolt, for later: the wire

Bolt is Neo4j's binary client protocol, the counterpart of Postgres's frontend/backend protocol. Every Neo4j driver
(Java, Python, JavaScript, .NET, Go) and every tool built on them (Neo4j Browser, Bloom, the BI connectors, the
LangChain and LlamaIndex graph stores) speaks it over one TCP connection, port 7687 by default: a version handshake,
then messages (`HELLO`, `LOGON`, `RUN` with the query text and its parameters, `PULL`, `DISCARD`, `BEGIN`, `COMMIT`,
`ROLLBACK`, `RESET`, `GOODBYE`) and records streamed back, every value encoded in PackStream (integers, floats,
strings, lists, maps, and structures for nodes, relationships, paths and the temporal types). Other graph databases
implement it so that Neo4j's drivers work against them (Memgraph, Amazon Neptune).

A **Bolt server in lite** is the graph counterpart of the pgwire server the server program plans for SQL
(`docs/SERVER_PROGRAM_2026_09_26.md` W3; the Postgres study's D3: hand-written, JDK only): it would let any Neo4j
driver, and Neo4j Browser, connect to a legend-lite server as if it were a Neo4j, run the island's translation of each
`RUN` over the Legend model, and stream the rows back as Bolt records, nodes as PackStream structures. The protocol's
documentation is CC BY-NC-SA 4.0 (§3.2): read for behaviour, never copied, as pgwire is written from Postgres's
manual. The cheaper first wire is Neo4j's **HTTP Query API** (`POST /db/{name}/query/v2`, JSON in and out), which lite's
existing HTTP server can serve with no new transport. Both are after the island itself (§9, "Later").

### 5.6 Relationship properties: the edge class, the data space as the graph, `USE`

**The question.** Neo4j's relationships carry properties (`[:ACTED_IN {role: 'Neo'}]`), have their own identity,
and may run in parallel (two `ACTED_IN` between one actor and one movie). Legend's `Association` is two properties
and nothing else: no properties of its own, no keys, no mapping beyond the join. What is the right Legend element
for a relationship with properties, and where does the model say so?

**The candidates, and why four of them are wrong here:**

| Candidate | Verdict |
|---|---|
| Extend `Association` with properties (UML's association class in the metamodel) | a fork of Pure's metamodel: the engine's parser, protocol and compiler would refuse every such model; `parser-equivalence` would carry a permanent exception. Rejected |
| Read the relationship's properties off the mapping's join table (the bridge table's extra columns) | the wrong layer: the physical mapping would define the logical graph, the properties would be store-typed and per-store, and a model-to-model mapping would have none. Legend's tenet is that the model is the semantic layer. Rejected |
| Infer an edge from a class's shape (two to-one properties to keyed classes) | guessing: invariant 4. A class with two to-one ends is often not an edge (an `Address` with a `country` and a `region`). Rejected |
| A new element kind, `###Graph`, a GQL "graph type" or SQL/PGQ "property graph" over the model, naming node types, edge types, ends and directions | standards-shaped, but it duplicates the data space (which already is the consumer's view: the exposed elements, the default mapping and runtime), and it costs a section grammar (`DataSpaceSectionGrammar` is 727 lines) plus a model record, the protocol reader and emitter, the composer, `FromProtocol` and the census. Kept as a later option only if one data space must expose several graphs |
| **An edge class, declared by a stereotype from an ordinary `Profile` in a library project; the data space as the graph; `USE` to pick it** | **the proposal** |

**The design.**

1. **An edge is a class.** It is the only Legend element that can hold a relationship's properties (ordinary
   properties), its identity (its mapping's primary key, C5), its mapping (a class mapping over the bridge table, its two ends
   as to-one property mappings through joins: exactly SQL/PGQ's edge table with a source key and a destination key),
   its constraints, its documentation, its milestoning, and later its writes (`CREATE (a)-[:Acting {role}]->(m)` is
   a new instance of it). Nothing new is invented below the class.
2. **The class says it is an edge, with an ordinary Pure `Profile` that compiles everywhere** (the user,
   2026-10-15: "if we build it as a real profile that compiles in pure/engine then we just have to inject that
   profile"). The profile is a plain element in a library project, as `core_types::CtGovernance` already is in
   `projects/core-types/model.pure` for every project that depends on it:

   ```
   ###Pure
   Profile graph::graph
   {
     stereotypes: [Edge, from, to, type];
   }
   ```

   Four stereotypes (five since 2026-10-17: `Open`, a class of an open graph, §12.45) and no tags: nothing in the
   declaration is a string. A model that wants edge classes depends on
   that project (through the depot, as on any library) and writes the edge in either of Legend's two forms of a
   link. **The idiomatic form, an association class decomposed as Legend can say it** (the user, 2026-10-15: "should
   that be moved to association and only the property exist on the class?"): the class holds the relationship's
   own data and identity, and two associations attach it to its ends, which also gives the ends their inverse
   navigation (`$actor.actings.movie`) and draws the edge in a diagram as a box between two lines:

   ```
   import graph::*;
   Class <<graph.Edge>> demo::Acting
   {
     role: String[1];
     since: StrictDate[0..1];
   }
   Association demo::ActorActing { <<graph.from>> actor: demo::Actor[1]; actings: demo::Acting[*]; }
   Association demo::ActingMovie { <<graph.to>>   movie: demo::Movie[1]; actings: demo::Acting[*]; }
   ```

   **The compact form**, the ends as the class's own properties, with no inverse navigation:

   ```
   Class <<graph.Edge>> demo::Acting
   {
     <<graph.from>> actor: demo::Actor[1];
     <<graph.to>>   movie: demo::Movie[1];
     role: String[1];
   }
   ```

   **Why `from` and `to` remain in the association form** (the user asked, 2026-10-15): direction. A Pure
   association has none; it is two properties, traversed both ways by construction. A Neo4j relationship always
   has one, and `(a)-[r]->(m)`, `startNode(r)` and a self-typed `(p:Person)-[:KNOWS]->(q:Person)` depend on it. The
   marker and the two injected ends tell the compiler which two classes an edge joins, never which way. Labels in a
   pattern would disambiguate only sometimes; declaration order would make a reorder flip every relationship
   silently; one stereotype with the other by elimination is one explicit and one inferred; reading edges as
   undirected would deviate from Neo4j. So the two stereotypes carry the one bit an association cannot. Seen the
   other way: without them the edge class is a node with two relationship types, and the query writes two hops,
   `(a)-[:actings]->(x:Acting)-[:movie]->(m)`, each directed by its property name; `from` and `to` are exactly the
   declaration that collapses that shape into one directed relationship with properties. A plain association used as
   a relationship type needs no stereotype because the pattern names an end property (`[:firm]`, `[:employees]`),
   whose owner-to-target is the direction; an edge class's type names the class, which names no end.

   The rule is one for both: the edge class has exactly one to-one class-typed property carrying `<<graph.from>>`
   and one carrying `<<graph.to>>`, **wherever that property is declared**: on the class, inherited, or injected by an
   association (`ModelContext` already sees association-injected navigation uniformly as the class's properties).
   An association that touches an edge class is one of its ends, never a relationship type of its own (nor is its
   inverse `actings`); one whose edge-class-side property carries neither stereotype is an error, since an edge has
   two ends and no third link. The modeler chooses the form, as Legend always lets a modeler choose a property or an
   association; the island cannot tell the difference, and neither can the engine.

   **The ends are stereotyped properties, not strings naming properties** (the user, 2026-10-15: "any way we can do
   it more type-safe?"). That is how `<<equality.Key>>` is applied too: on the property, and read by
   `ClassCompiler` by the profile's full name and the stereotype's name (`ClassCompiler.java:58`). Every reference in
   the declaration is then an element reference the engine's own compiler checks: the profile exists, the
   stereotypes exist in it, the property exists and has the type and multiplicity written beside it. Nothing names a
   property by its spelling, so a renamed property cannot silently detach its end.

   **The relationship type is typed: an enumeration value, spelled in a stereotyped derived property** (the user,
   2026-10-15: "should type also be a typed type?", then "any way to use enums, inheritance, any combination?").
   A tagged value is a string in Pure; a stereotype is a typed marker but cannot carry a value; the one place a class
   can state a *typed constant* that the engine checks is a derived property whose body is a spelled value. So the
   model declares its catalog of relationship types as an enumeration, and each edge class names its type by one:

   ```
   Enum demo::RelationshipType { ACTED_IN, DIRECTED, KNOWS }

   Class <<graph.Edge>> demo::Acting
   {
     <<graph.type>> type() { demo::RelationshipType.ACTED_IN }: demo::RelationshipType[1];
     role: String[1];
   }
   ```

   The engine checks all of it: the enumeration and its value exist, the body's type is the declared return type,
   the stereotype exists in the profile. Lite reads it in phase F without typing the body (invariant 6): the derived
   property carrying `<<graph.type>>` must have a body that is exactly one spelled enumeration value (one
   `EnumValue` node of the protocol), which is what the charter's C6.2 lets the compiler read ("a spelled enum's
   name"); anything else is a compile error naming the property. The relationship type's name is the value's name:
   `(a:Actor)-[r:ACTED_IN]->(m:Movie)`, and `type(r)` is the value, written as its name on the wire, which is
   Neo4j's string. Without the derived property, the type is the class's simple name. Two edge classes may name
   the same type with different end pairs, as one Neo4j type may join several label pairs; `[:ACTED_IN]` is then
   their union and the endpoint labels choose.

   This is the typed alias the user asked for, and more: the enumeration is the graph's catalog of relationship
   types, an element a data space can expose, a DataCube filter can offer, and a query can compare against
   (`type(r) = RelationshipType.ACTED_IN` in Pure; `type(r) = 'ACTED_IN'` in Cypher). The same mechanism names an
   end the model does not own: an end property declared by another project's association, or inherited, cannot be
   stereotyped there, so the edge class declares `<<graph.from>> from() { $this.actor }: demo::Actor[1]`, a derived
   property whose body is exactly one navigation of `$this` to a to-one class-typed property; the engine types it,
   lite reads the spelled navigation. The rule stays one: the end is the property carrying the stereotype, stored or
   derived.

   **Other typed channels, considered and set aside.** A marker superclass (the user asked to see it, 2026-10-15):

   ```
   Class <<typemodifiers.abstract>> graph::Edge {}                   // the library project

   Class demo::Acting extends graph::Edge                            // the model
   {
     <<graph.from>> actor: demo::Actor[1];
     <<graph.to>>   movie: demo::Movie[1];
     <<graph.type>> type() { demo::RelationshipType.ACTED_IN }: demo::RelationshipType[1];
     role: String[1];
   }
   ```

   It replaces only the `Edge` marker: a superclass cannot type the ends (that needs a generic `Edge<From, To>`, which
   the engine refuses, and `from: Any[1]` cannot be narrowed by a subclass, since Pure has no covariant
   redeclaration) and cannot declare the type (Pure has no abstract member to override). It is as typed as the
   stereotype for that one thing, and would make "every relationship" a class (`graph::Edge.all()`) only once each
   model writes the superclass a union mapping over its edge classes, which the translator does for free as it does
   for a label-less node; and it enters the inheritance that mapping extension, equality keys, milestoning, `match`
   and graph fetch read. The stereotype marks without inheriting. A
   stored property with a default value (`type: RelationshipType[1] = RelationshipType.ACTED_IN;`) is typed but is a
   column to map, not a constant. A class constraint calling a library function with the two ends would need that
   function to exist in the engine as a Pure function and in lite as a known name, and the engine's refusal of
   generics means its signature could not check the ends' types. Each is recorded so that it is not re-proposed.

   The engine, Studio and the SDLC compile and show this as they show any stereotype and tagged value: the model is
   not lite-only. Lite alone gives it meaning: the compiler knows the profile by its full name
   (`PlatformTypes.GRAPH_PROFILE`, beside `EQUALITY_PROFILE`, which names the engine's `meta::pure::profiles::equality`
   the same way), checks in phase F that the element at that name is a `Profile` declaring exactly the stereotypes
   and the tag above (any other shape is a compile error naming the path), and reads the applications as
   `EqualityKeys` reads `<<equality.Key>>`.

   **The other type-safe route, and why not.** A generic superclass, `Class demo::Acting extends
   graph::Edge<demo::Actor, demo::Movie>`, would make the ends typed properties `from` and `to` inherited from the
   library and the end types generic arguments, with no convention at all beyond the superclass. Pure allows it,
   and lite compiles generics end to end (`docs/OWN_CORPUS_DECISIONS.md` §11). **The engine does not**: "Type and/or
   multiplicity parameters are not authorized in Legend Engine" (its §2), so such a model would be lite-only, the
   very thing the library profile avoids, and the ends would lose their domain names (`actor`, `movie`) for the
   library's (`from`, `to`). The stereotyped properties keep the names, the types and the engine.

   Three consequences, each deliberate:
   - **The prelude does not ship it.** A prelude copy beside the library's would be a duplicated element
     (`ModelIntegrity` refuses one), and it would make the model lite-only again. The library project is the one
     home; lite's `projects/graph` holds it and publishes it.
   - **The path stays out of `meta::`.** `meta::` is Pure's platform root; lite itself refuses library elements under
     `meta::pure::functions::` (`LibraryPlatformNamespaceGuardTest`), and whether the engine refuses a user element
     under `meta::` was not verified here and need not be: a plain path avoids the question.
   - **What the engine checks, and what only lite checks.** The engine checks every reference: the profile, the
     stereotypes, the properties they sit on, their types and multiplicities. Only lite checks the *rule*: exactly
     one `<<graph.from>>` and one `<<graph.to>>` property per edge class (own, inherited or association-injected),
     each `[1]` and typed by a class, on a class that carries `<<graph.Edge>>` (an end stereotype without the class
     marker, or the marker without both ends, is an error either way); no third link on an edge; an edge class never
     used as a label. A misdeclared edge compiles in the engine and fails here, in phase F, with a positioned error
     naming the class and its properties; since only lite runs the island, that is where the rule belongs, and
     Studio on lite shows it at compile time.
   - **One small model change for the association form.** The parser already reads an association end "as an
     ordinary wire property (annotations, span and all)" (`ElementParser.parseAssociationBody`), but the model record
     drops them: `AssociationDefinition.AssociationEndDefinition` holds the name, the target and the multiplicity
     only. It gains the end's stereotypes, which `ModelContext` then carries on the injected property as it carries
     a class's own. The protocol JSON already carries them (the engine's wire does), so the wire and the composer
     change nothing.

   The checks, each a compile error with Neo4j's and the model's words: exactly one property of the class (own,
   inherited or association-injected) carries `<<graph.from>>` and exactly one `<<graph.to>>`, each of multiplicity
   `[1]` (a Neo4j relationship always has both ends) and each typed by a class, which are the edge type's end node
   types; the relationship type is the `<<graph.type>>` derived property's spelled enumeration value, else the
   class's simple name; an edge class is never a label (a node pattern
   `(x:Acting)` is refused: "Acting is an edge type"); a subclass of an edge class is an edge type of its own,
   inheriting the ends; every other property, inherited ones included, is the relationship's property (`r.role`,
   `properties(r)`, `keys(r)`); a to-many property to a class is refused on an edge (a relationship has two ends),
   the associations' inverse ends excepted, which are the ends seen from the node.
3. **Identity.** The edge's identity is Pure's instance equality (C5): its `<<equality.Key>>` where the class
   declares one (an edge keyed by its two ends says one relationship per pair), else the row itself, realized by the
   mapping's primary key, the bridge table's `PRIMARY KEY` or the mapping's `~primaryKey(...)`, which the model
   never shows. `DISTINCT r`, `collect(r)`, `count(DISTINCT r)`, relationship uniqueness (`DIFFERENT RELATIONSHIPS`)
   and path equality use it. Parallel relationships are rows with distinct identities, as in Neo4j. A keyless edge
   whose mapping has no primary key is refused wherever identity is needed, as a node class is (C5).
4. **A relationship without properties stays an association or a property.** `(p:Person)-[:firm]->(f:Firm)` reads
   `Person.firm`; `(f)-[:employees]->(p)` the other end; `(p)-[:PersonFirm]-(f)` the association by name. Its
   identity is the pair of its ends' keys, `properties(r)` is `{}`, `type(r)` the name. Where Neo4j would hold two
   parallel relationships of one type between the same two nodes, an association holds one link: a
   `SEMANTICS_REGISTER` row, and the edge class is the complete representation whenever that matters.
5. **Direction.** For an edge class, from the `from` end to the `to` end. For a property, from its owner to its
   target. An undirected pattern `-[:T]-` is the union of the two orientations, each relationship once, as Neo4j
   matches it. Labels on the endpoints narrow each end to the label's class or a subclass.
6. **The graph is the data space.** Which classes and edges a query sees, and the mapping and runtime it runs on, are
   what a `DataSpace` already declares (its `elements`, its execution contexts). The island selects one with
   Neo4j's own clause, `USE demo::MovieSpace MATCH ...`; without `USE`, the island runs as any Pure query does, on
   the `from(mapping, runtime)` that follows it, over every class the mapping maps. This is the SQL study's own
   choice for the Postgres wire ("database = a project or data space", its §3.3), and it keeps every projection fact
   in a Legend element that exists.
7. **Names are the model's, and typed.** A relationship type is the edge class's `<<graph.type>>` enumeration value
   when it declares one, else the edge class's name, the association's name or the property's name; there is no
   renaming layer of strings. Pure has no type alias (the user asked, 2026-10-15): its element kinds are classes,
   associations, enumerations, profiles, functions, mappings and the runtime and tooling elements, and the one
   alias-shaped construct, `Primitive P extends Integer`, is for primitives; a subclass is not an alias (it
   narrows). The enumeration value above is the typed second name. A zero-argument function returning the class's
   instances, `function demo::ACTED_IN(): demo::Acting[*] { demo::Acting.all() }`, would be more than an alias, a
   typed view relationship type resolved as Legend SQL resolves `func('...')` sources and inlined by G½; noted as a
   later option of its own.
8. **Patterns over edge classes lower as joins.** `(a:Actor)-[r:Acting]->(m:Movie)` is `Acting.all()` joined to
   its `actor` and `movie` ends through the mapping, with the endpoint labels as filters; `r.role` is the row's
   column; `type(r)` a spelled constant; `startNode(r)` and `endNode(r)` the ends. A variable-length pattern over an
   edge class unrolls with two joins per hop. Nothing in the lowering is new: it is navigation.

**What it costs, and what it does not.** New: the library project `projects/graph` with the one profile, the
constant `PlatformTypes.GRAPH_PROFILE`, a phase-F reader beside `EqualityKeys` (`GraphEdges`: the validation and the
resolved ends per class), the stereotypes kept on association ends (above), the island's resolution of a
relationship type to an edge class, an association or a property, and the `USE` clause. Untouched: the parser
(stereotypes parse today, on class and association properties alike), the protocol, the mapping layer, the data
space, the prelude. **No lite-only model construct**: a model with edge classes compiles in the engine, in Studio
and here, byte for byte the same text; the own-corpus census needs no new family for it. The one convention is the
profile's name, which is the same kind of convention as `meta::pure::profiles::equality`, and it is checked, never
assumed. **No string anywhere in the declaration**: four stereotypes, an enumeration and its values, each an element
reference the engine checks.

**What this is not.** It is not a shortcut around the association: the association remains the right element for a
relationship with no properties, and the edge class is the right element for one with properties, which is how a
UML association class and a GQL edge type relate to a plain association. The two spellings coexist in one model and
one query.

### 5.7 The worked example: every feature in one model (the user's ask, 2026-10-15)

One model with the library profile, the enumeration, keyed and keyless nodes, a subclass label, both regular-edge
forms, the three property-edge shapes (the class form, a self-typed edge, ends through associations), the store,
the mapping, the runtime and the data space; then the queries, each with what it reads and the Pure the island
builds. The mapping has nothing new in it: edge classes on their bridge tables, every end a join, identity from the
tables' keys, the subclass by a filter, as `AssociationIntegrationTest` writes its own.

**The library project `projects/graph`, one element:**

```
###Pure
Profile graph::graph
{
  stereotypes: [Edge, from, to, type];
}
```

**The model:**

```
###Pure
import graph::*;
import demo::*;

Enum demo::RelationshipType { ACTED_IN, DIRECTED, KNOWS }

// ---- nodes ----
Class demo::Person { name: String[1]; born: Integer[0..1]; }          // keyless: a node is the row, realized by T_PERSON.ID (C5)
Class demo::Actor extends demo::Person { stageName: String[0..1]; }   // its nodes carry the labels Actor and Person
Class demo::Firm { legalName: String[1]; }
Class demo::Movie
{
  <<equality.Key>> title: String[1];                                  // keyed: a node is its business key (C5)
  <<equality.Key>> year: Integer[1];
  studio: Firm[0..1];                                                 // REGULAR EDGE, one way: a property, owner -> target
}

// ---- regular edge, both ways: ONE association, no properties ----
Association demo::Employment { employees: Person[*]; firm: Firm[0..1]; }

// ---- property edge, class form (the primary form): one element ----
Class <<graph.Edge>> demo::Acting
{
  <<graph.from>> actor: Actor[1];
  <<graph.to>>   movie: Movie[1];
  <<graph.type>> type() { RelationshipType.ACTED_IN }: RelationshipType[1];
  role: String[1];
}

// ---- property edge, self-typed: direction is knowable only from the model ----
Class <<graph.Edge>> demo::Knows
{
  <<graph.from>> knower: Person[1];
  <<graph.to>>   known: Person[1];
  <<graph.type>> type() { RelationshipType.KNOWS }: RelationshipType[1];
  since: StrictDate[1];
}

// ---- property edge, ends through associations: when Person and Movie should navigate to it in Pure ----
Class <<graph.Edge>> demo::Directing
{
  <<graph.type>> type() { RelationshipType.DIRECTED }: RelationshipType[1];
}
Association demo::DirectorDirecting { <<graph.from>> director: Person[1]; directings: Directing[*]; }
Association demo::DirectingMovie    { <<graph.to>>   movie: Movie[1];     directings: Directing[*]; }
```

**The store, mapping, runtime and data space:**

```
###Relational
Database demo::DB
(
  Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), BORN INTEGER, FIRM_ID INTEGER, IS_ACTOR INTEGER, STAGE_NAME VARCHAR(100))
  Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
  Table T_MOVIE (ID INTEGER PRIMARY KEY, TITLE VARCHAR(200), YEAR INTEGER, STUDIO_ID INTEGER)
  Table T_ACTING (ID INTEGER PRIMARY KEY, ACTOR_ID INTEGER, MOVIE_ID INTEGER, ROLE VARCHAR(100))
  Table T_KNOWS (KNOWER_ID INTEGER PRIMARY KEY, KNOWN_ID INTEGER PRIMARY KEY, SINCE DATE)
  Table T_DIRECTING (ID INTEGER PRIMARY KEY, DIRECTOR_ID INTEGER, MOVIE_ID INTEGER)
  Filter ActorFilter(T_PERSON.IS_ACTOR = 1)
  Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
  Join MovieStudio(T_MOVIE.STUDIO_ID = T_FIRM.ID)
  Join ActingActor(T_ACTING.ACTOR_ID = T_PERSON.ID)
  Join ActingMovie(T_ACTING.MOVIE_ID = T_MOVIE.ID)
  Join KnowsKnower(T_KNOWS.KNOWER_ID = T_PERSON.ID)
  Join KnowsKnown(T_KNOWS.KNOWN_ID = T_PERSON.ID)
  Join DirectingDirector(T_DIRECTING.DIRECTOR_ID = T_PERSON.ID)
  Join DirectingMovie(T_DIRECTING.MOVIE_ID = T_MOVIE.ID)
)

###Mapping
import demo::*;
Mapping demo::Map
(
  Person: Relational { ~mainTable [demo::DB] T_PERSON  name: [demo::DB] T_PERSON.NAME, born: [demo::DB] T_PERSON.BORN }
  Actor: Relational  { ~filter [demo::DB] ActorFilter ~mainTable [demo::DB] T_PERSON
                       name: [demo::DB] T_PERSON.NAME, born: [demo::DB] T_PERSON.BORN, stageName: [demo::DB] T_PERSON.STAGE_NAME }
  Firm: Relational   { ~mainTable [demo::DB] T_FIRM  legalName: [demo::DB] T_FIRM.LEGAL_NAME }
  Movie: Relational  { ~mainTable [demo::DB] T_MOVIE  title: [demo::DB] T_MOVIE.TITLE, year: [demo::DB] T_MOVIE.YEAR,
                       studio: [demo::DB]@MovieStudio }
  Acting: Relational { ~primaryKey([demo::DB] T_ACTING.ID) ~mainTable [demo::DB] T_ACTING
                       role: [demo::DB] T_ACTING.ROLE, actor: [demo::DB]@ActingActor, movie: [demo::DB]@ActingMovie }
  Knows: Relational  { ~mainTable [demo::DB] T_KNOWS  since: [demo::DB] T_KNOWS.SINCE,
                       knower: [demo::DB]@KnowsKnower, known: [demo::DB]@KnowsKnown }
  Directing: Relational { ~mainTable [demo::DB] T_DIRECTING }
  demo::Employment: Relational { AssociationMapping ( employees: [demo::DB]@PersonFirm, firm: [demo::DB]@PersonFirm ) }
  demo::DirectorDirecting: Relational { AssociationMapping ( director: [demo::DB]@DirectingDirector, directings: [demo::DB]@DirectingDirector ) }
  demo::DirectingMovie: Relational { AssociationMapping ( movie: [demo::DB]@DirectingMovie, directings: [demo::DB]@DirectingMovie ) }
)

###Connection
RelationalDatabaseConnection demo::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

###Runtime
Runtime demo::RT { mappings: [ demo::Map ]; connections: [ demo::DB: [ environment: demo::Conn ] ]; }

###DataSpace
DataSpace demo::MovieSpace
{
  executionContexts: [ { name: 'default'; mapping: demo::Map; defaultRuntime: demo::RT; } ];
  defaultExecutionContext: 'default';
  elements: [ demo ];
}
```

An `Actor` row is a `Person` row: both sets read `T_PERSON`, and a keyless node's identity is its table's key in
whichever set reads it, so `(n:Actor)` and `(n:Person)` meet the same node (the identity half of C5, which PR3
needs on the product lane).

**Regular edges.**

| Cypher | What it reads | The Pure the island builds |
|---|---|---|
| `USE demo::MovieSpace MATCH (p:Person)-[:firm]->(f:Firm) RETURN p.name, f.legalName` | the association's end `Person.firm`; `USE` picks the data space | `Person.all()->filter(p\|$p.firm->isNotEmpty())->project(~[name: p\|$p.name, legalName: p\|$p.firm.legalName])` |
| `MATCH (f:Firm)-[:employees]->(p:Person) RETURN f.legalName, count(p) AS headcount` | the other end, opposite direction, grouped: one row per firm with at least one employee | `Firm.all()->filter(f\|$f.employees->isNotEmpty())->project(~[legalName: f\|$f.legalName, headcount: f\|$f.employees->size()])` |
| `MATCH (p:Person)-[:Employment]-(f:Firm) RETURN p.name, f.legalName` | the association by name, either direction | the union of the two |
| `MATCH (m:Movie)-[:studio]->(f:Firm) RETURN m.title, f.legalName` | the property; owner to target | `Movie.all()->filter(...)->project(...)` |
| `MATCH (f:Firm)<-[:studio]-(m:Movie) RETURN f.legalName, m.title` | the same rows from the firm's side | the same Pure |
| `MATCH (f:Firm)-[:studio]->(m:Movie)` | nothing: `Firm` has no `studio` | no rows, and a note at compile time |
| `OPTIONAL MATCH (p:Person)-[:firm]->(f:Firm) RETURN p.name, f.legalName` | a left join; `null` where unemployed | `Person.all()->project(~[name: p\|$p.name, legalName: p\|$p.firm.legalName])` with the empty navigation kept |
| `MATCH (p)-[r:firm]->(f) RETURN type(r), properties(r)` | | `'firm'`, `{}` |

**Property edges.**

| Cypher | What it reads | The Pure the island builds |
|---|---|---|
| `MATCH (a:Actor)-[r:ACTED_IN]->(m:Movie) RETURN a.name, a.stageName, r.role, m.title` | the edge class, from `actor` to `movie`; the `Actor` label is the filtered set | `Acting.all()->project(~[name: x\|$x.actor.name, stageName: x\|$x.actor.stageName, role: x\|$x.role, title: x\|$x.movie.title])` |
| `MATCH (m:Movie)<-[r:ACTED_IN]-(a) WHERE r.role = 'Neo' RETURN type(r), properties(r), startNode(r).name` | the same rows from the movie's side | `type(r)` is `RelationshipType.ACTED_IN`, written `'ACTED_IN'`; `properties(r)` the Variant `{role: 'Neo'}`; `startNode(r)` the actor |
| `MATCH (d:Person)-[:DIRECTED]->(m:Movie) RETURN d.name, collect(m.title) AS titles` | the edge whose ends come through the associations; a list-valued aggregate | `Directing.all()->groupBy(~[name: x\|$x.director.name], ~[titles: x\|$x.movie.title: y\|<the list aggregate>])`: the registry has no `list` native today, so the aggregate's Pure spelling is PR2's to settle (it lowers to the `LIST` reducer the MIR already has) |
| `MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.name, b.name, k.since` | self-typed: `knower` to `known`, from the model, since the labels cannot tell | `Knows.all()->project(~[a: x\|$x.knower.name, b: x\|$x.known.name, since: x\|$x.since])` |
| `MATCH (a:Person)<-[k:KNOWS]-(b:Person) RETURN a.name, b.name` | reversed: `a` is the known | the same rows with the columns swapped |
| `MATCH (a:Person)-[k:KNOWS]-(b:Person) RETURN a.name, b.name` | undirected: both orientations, each relationship once | the union of the two orientations |
| `MATCH (a:Person {name: 'Ann'})-[:KNOWS*1..3]->(c) RETURN DISTINCT c.name` | one, two and three hops unrolled, with the relationship-uniqueness conjuncts | three navigations of `Knows` concatenated, `distinct` |
| `MATCH (n:Actor:Person) RETURN n.name` | both labels: the `Actor` set, whose rows are also `Person` rows | `Actor.all()->project(~[name: n\|$n.name])` |
| `MATCH (n:Person) RETURN count(DISTINCT n)` | keyless: each row one node, actors included, identity `T_PERSON.ID` | `Person.all()->removeDuplicates()->size()` |
| `MATCH (m:Movie) RETURN count(DISTINCT m)` | keyed: two rows with one `title` and `year` are one node | `Movie.all()->removeDuplicates()->size()`, lowered by the keys |
| `MATCH ()-[r:ACTED_IN]->() RETURN count(DISTINCT r)` | keyless edge: each row one relationship, identity `T_ACTING.ID` | `Acting.all()->removeDuplicates()->size()` |
| `MATCH (x:Acting) RETURN x` | | refused: "Acting is an edge type, not a label" |

**Versions, composition and refusals.**

| Cypher | What happens |
|---|---|
| `MATCH (p:Person) FILTER p.born > 1980 RETURN p.name` | Cypher 25, the default: `FILTER` is read |
| `CYPHER 5 MATCH (p:Person) WHERE p.born > 1980 RETURN p.name` | the frozen version, as Neo4j selects it |
| `#Cypher{ MATCH (a:Actor)-[:ACTED_IN]->(m:Movie) RETURN a.name AS actor, m.title AS title }#->filter(x \| $x.title->startsWith('The'))->from(demo::Map, demo::RT)` | the island composes with Pure; no `USE`, so the `from` supplies the context |
| `MATCH (p:Person) RETURN p.salary` | refused: no class of label `Person` declares `salary`; `Person` declares `name`, `born` (C4; a register row, Neo4j answers `null`) |
| `CALL db.labels()` | refused by name: no procedures |
| `MATCH (p:Person)-[:$($t)]->() RETURN p` | refused: a dynamic type; the model is the schema |
| `CREATE (:Person {name: 'Bob'})` | refused by name until F1 gives lite writes |
| `MATCH p = ANY SHORTEST (a:Person)-[:KNOWS]-+(b:Person) RETURN p` | refused where ties can occur; `ALL SHORTEST` and `SHORTEST 2 GROUPS` answer |
| `MATCH (a:Person)-[:KNOWS*]->(c) RETURN c.name` | refused by name until PR5's native lands |

**The class form is the primary form** of a property edge (the user's review of 2026-10-15: one element for one
relationship type, the shape a Neo4j user and a Legend link entity share); an end moves into an association only
when a model wants to navigate from a node to its edges in Pure or DataCube, as `Directing` shows. §5.6's earlier
wording that called the association form idiomatic is superseded by this section.

**Verified (2026-10-15, the user: "feel good about the Pure?").** The three code blocks above, concatenated as
one text, were compiled by legend-lite at `74d873d` through its every-error route
(`POST /api/lite/v1/compilation/compile`, `Compiler.compileErrors`: A to F, then every body), in this cloud
container after `tools/cloud-distdir.sh`: **`{"errors":[]}`** (re-run after the profile was renamed `graph::graph`:
the same). So the profile, the stereotypes on the class, on
stored properties, on derived properties and on association ends, the derived property's enumeration body, the
subclass with a filter over its superclass's table, the edge classes over their bridge tables, the association
mappings, the DuckDB connection, the runtime and the data space all parse and compile, bodies included.
Three negative controls, each a one-token edit of the same text:
- `RelationshipType.NOPE` in the derived property: refused, "in function 'demo::Acting$prop$type': enumeration
  demo::RelationshipType has no value 'NOPE'". The body is typed.
- `actor: Actorr[1]`: refused, "Unknown type: 'Actorr'". The ends are resolved.
- `<<graph.Edgee>>`, a stereotype the profile does not declare: **accepted, `{"errors":[]}`.** legend-lite does not
  check a stereotype or a profile reference against the profile today (no such check exists in `core/`, and
  `docs/LENIENCY_CATALOG.md` does not list the leniency); legend-engine refuses it ("Can't find stereotype"). Two
  consequences: the `GraphEdges` reader of PR3 validates the graph profile's references itself, so a misspelled
  `<<graph.from>>` is an error here as it is in the engine; and the general leniency is its own finding for the
  compiler line, recorded with this verification, since a model that compiles here with a misspelled stereotype
  fails in the engine.
**Verified against the real legend-engine too (2026-10-15).** The same text, through the repository's engine
runner (`bazel run //tools/engine-runner:testable -- example-model.pure`, legend-engine at the pinned 4.145.0 from
`@maven_runner`, parse and compile with no testables): **parse 432 ms, compile 4,686 ms, no error, exit 0** (re-run
after the profile was renamed `graph::graph`: parse 482 ms, compile 6,267 ms, no error). The
model with its edge classes, the profile, the stereotyped properties, derived properties and association ends, the
enumeration body, the mapping, the DuckDB connection, the runtime and the data space is engine-compatible, byte for
byte the same text, which is the claim of §5.6. The engine's answers to the controls, each an `EngineException`
with exit 1: `<<graph.Edgee>>`, "Can't find stereotype 'Edgee' in profile 'graph'"; `RelationshipType.NOPE`, "Can't
find enum value 'NOPE' in enumeration 'RelationshipType'". So the engine checks the profile's stereotypes and lite
does not: the asymmetry above, measured on both sides.

---

## 6. Recursion: what Pure lacks, what the databases have, what the native is

### 6.1 The need

| Construct | Needs iteration? |
|---|---|
| `*1..3`, `{1,3}`, `{,10}`, `shortestPath` with an upper bound | no: unroll to the bound (a `shortestPath` bounded at k is the first non-empty of the k unrolls, each a union) |
| `*`, `+`, `{1,}`, unbounded `shortestPath`, `SHORTEST k`, `ALL SHORTEST` | yes: breadth-first to a fixpoint |
| reachability (`EXISTS { (a)-[*]->(b) }`) | yes |
| WCC, SCC, BFS depths, SSSP, k-core, topological sort | yes: to a fixpoint |
| PageRank, ArticleRank, eigenvector, HITS, CDLP | yes: a fixed count of iterations, or to a tolerance |
| degree, triangles, LCC, node similarity, link prediction, modularity, conductance | no: joins and aggregates |

### 6.2 What the databases have

Every backend lite renders documents a recursive CTE: Postgres, DuckDB, H2, SQLite, MariaDB and MySQL, Snowflake,
BigQuery (top-level only, which `FoldsAsRecursiveCtes` already handles for the fold), Databricks, Trino (with a
depth limit) and ClickHouse. Each dialect's exact rules are measured in PR5, not taken from memory. The MIR already
carries the construct: `SqlWith(ctes, body, recursive)` and `SqlSource.Cte`; `AnsiSqlRenderer` writes
`WITH RECURSIVE`.

The rules differ, and they decide the native's lowering shape:
- **Postgres** iterates a working table: the non-recursive term, then the recursive term over the previous
  iteration's rows only, until an iteration adds no row; `UNION` drops rows seen before, `UNION ALL` keeps every
  row. The recursive self-reference may appear once, not inside a subquery, not on the nullable side of an outer
  join, not under `INTERSECT` or `EXCEPT`; no `ORDER BY`, `LIMIT` or `OFFSET` in the recursive term. The SQL standard
  forbids an aggregate over the recursive reference in the recursive term and Postgres follows it (to be measured
  on the embedded Postgres 16 in the first recursion slice, with the exact message). `SEARCH BREADTH FIRST` and
  `CYCLE ... SET ... USING path` are built in (Postgres 14+).
- **DuckDB** iterates likewise, and in the recursive term the CTE's name is the last iteration's rows while
  `recurring.<name>` is the union so far; its examples put joins, `NOT EXISTS`, a scalar subquery over `recurring`
  and a window function in the recursive term. **`USING KEY`** (since 1.3) keys the rows: an iteration's row with an
  existing key replaces that row rather than appending it, which is the shape of a label that converges or a score
  that is updated per node. No depth limit; termination is the query's.
- **BigQuery:** top-level only. **Trino:** `max_recursion_depth`. **SQLite, MariaDb:** standard, measured in the
  slice.

Two lowering shapes follow, chosen per dialect by the renderer, never by the translator:
1. **A row per node per iteration** (DuckDB with `USING KEY`, and every database for frontier algorithms that need no
   aggregate over the previous iteration: BFS, reachability, WCC by minimum label with `UNION`).
2. **One row per iteration carrying the whole state as an array** (Postgres and any database that forbids an
   aggregate over the recursive reference): the recursive term reads the previous row once, unnests its arrays in a
   lateral subquery that references no CTE, aggregates there, and re-packs. PageRank over `n` nodes is a row of `n`
   doubles per iteration. The database still computes; the SQL is larger and the size bound (C7) measures it.

### 6.3 The native

What Pure has: no fixpoint. Pure's `fold` is over a spelled list; C6.3 keeps recursion out of prelude bodies; C6.4
names "recursion over row-backed trees" as residue. What is proposed is not Java recursion and not a program: it is a
**native** in the world map's first kind, one Java lowering rule per dialect, like `lateral` or `asOfJoin`, under
`meta::legend::lite::` beside the 37 lite natives `docs/LITE_REVIEW_2026_09_11.md` reviews (`joinWithPrefix`,
`asOfJoinWithPrefix` are its LITE_SURFACE precedents). A first shape, for the user to shape:

```
native function meta::legend::lite::recurse<T>(
    base: Relation<T>[1],
    step: Function<{Relation<T>[1] -> Relation<T>[1]}>[1],   // the next rows from the last iteration's rows
    mode: RecursionMode[1],                                   // UNION (new rows only, a set) or UNION_ALL
    limit: Integer[0..1]                                      // iterations; none for "to a fixpoint"
): Relation<T>[1]
```

and a keyed variant `recurseByKey(base, step, ~key, limit)` whose rows replace by key (DuckDB's `USING KEY`
natively; the array shape elsewhere). The lowering: the base relation as the non-recursive term, the step's body with
its parameter bound to the CTE's name as the recursive term, the mode as `UNION` or `UNION ALL`, the limit as an
iteration counter column the step cannot see. The Typer types `step` once, with `T` the base's row type (the step must
return the same columns: a compile error otherwise).

**The user's ruling, 2026-10-15: recursion is required**, for the graph queries and the algorithms alike. So the
native is built, as kind 1 and not C6.4's residue (the charter gains a dated note at C6.3 and C6.4 when PR5 lands).
What PR5's design still settles: the name and the shape above; that a termination guard (`limit`) is required where
the database has no depth limit, so that no query can loop (a refusal without one over an unbounded pattern, or a
default the register names); which line owns it (the database owner's `sql/dialect/` and `lowering/` files, announced
in `IN_FLIGHT`). Until PR5: refused by name, and the first four slices (§9) need nothing of it.

---

## 7. The graph algorithms

### 7.1 The catalog

Neo4j GDS 2026.07's production tier lists about 60 algorithms (and GraphSAGE in beta): the full names are in §11's
reference. Cypher itself has `shortestPath`, `allShortestPaths` and the `SHORTEST` selectors. The question for each
is the same: **is the answer a function of the input?** If it is, lite can answer it exactly and a judge can say so.

### 7.2 Three buckets

| Bucket | Algorithms | How it runs | Judge |
|---|---|---|---|
| **1. Deterministic, set-based or fixed-iteration: translate** | degree centrality; triangle count and listing; local clustering coefficient; weakly connected components; strongly connected components; breadth-first search (depths); the shortest-path **distances** of Dijkstra, Bellman-Ford, delta-stepping and A* (one answer, however computed); all-pairs shortest paths; PageRank, ArticleRank, eigenvector centrality, HITS (power iteration, a fixed count or a tolerance); k-core decomposition; topological sort (as levels); longest path in a DAG; node similarity (Jaccard, overlap, cosine) and its filtered form; the six topological link-prediction functions (Adamic-Adar, common neighbours, preferential attachment, resource allocation, same community, total neighbours); the modularity metric; conductance; CDLP as Graphalytics defines it | joins and aggregates; the recursion native for the iterative ones | Neo4j GDS, snapped; floats within the parity harness's tolerance; partitions by equivalence; Graphalytics' reference outputs where its definition is the one asked |
| **2. Deterministic but costly or tie-dependent: translate with a bound, refuse the tie** | betweenness, closeness and harmonic centrality (all-pairs); articulation points and bridges; spanning tree and k-spanning tree (the weight sum answers; the edge set only when weights are distinct); Yen's k shortest paths (ordered enumeration; refused beyond a small k); clique counting; the **path** chosen among equal-cost shortest paths; `SHORTEST k` and `ANY` where ties can occur | the same, under a size bound the slice measures, as the IQ island bounds its Pure (20,000 nodes) | the same; a tie-dependent output is refused by name where the input can tie, answered where it cannot (`ALL SHORTEST`, `SHORTEST k GROUPS`) |
| **3. Randomized or heuristic: refuse by name** | Louvain, Leiden, label propagation as GDS runs it (ties by an unstated order), speaker-listener LPA, K1 coloring, modularity optimization, approximate max k-cut, random walk, k-means, HDBSCAN, FastRP, Node2Vec, HashGNN, GraphSAGE, CELF influence maximization, the Steiner trees (heuristic), max flow and min-cost max-flow (sequential augmenting; a later study if the value alone is wanted), k-nearest neighbours (approximate), scale properties and split relationships (preprocessing, not queries) | nothing | nothing: Neo4j's own answer is not reproducible across runs, so there is no parity to claim; the refusal names the algorithm and the reason |

The count: about 30 of the 60 are bucket 1, about 10 bucket 2, about 20 bucket 3. "All of Neo4j's graph algorithms"
means buckets 1 and 2, measured, and bucket 3 refused with its reason.

### 7.3 The spellings, by example

Each is a Pure program over the model (never a Java computation), with the native where marked:

- **Degree:** `groupBy(~[node], ~[degree: r | $r.other : y | $y->count()])` over the relationship's relation; in and
  out and undirected as Neo4j's `orientation`.
- **Triangles:** three navigations `a-b`, `b-c`, `c-a` with `a < b < c` on the key, counted; LCC as the triangle
  count over `deg * (deg - 1) / 2`, Graphalytics' directed rule enforced inside.
- **WCC:** the native, `UNION` mode: each node starts as its own label (its key), each step takes the minimum label of
  its neighbours; the fixpoint is the components. Judged by partition equivalence.
- **BFS depths and reachability:** the native, `UNION` mode, the frontier as the step; depth as the iteration.
- **SSSP:** the native, keyed: each step relaxes every edge from the last iteration's improved nodes and keeps the
  smaller distance per node (Bellman-Ford in rounds; Dijkstra's answer, not its order); judged with epsilon.
- **PageRank:** the native, keyed, `limit` the iteration count: a step reads every node's score, sums the
  contributions over in-edges, adds the teleport and (in the Graphalytics profile) the sinks' redistributed mass; a
  tolerance stop as a step that returns no changed row beyond the tolerance. The two profiles (GDS's unnormalized
  formula, Graphalytics' normalized one with sinks) are two parameter sets of one program.
- **CDLP (Graphalytics):** the native, keyed and synchronous: a step counts the previous labels over in- and
  out-neighbours and takes the smallest of the most frequent; a fixed count of iterations.
- **Node similarity:** the neighbour sets as list aggregates, Jaccard as the intersection's size over the union's,
  a self-join on candidate pairs; `topK` ties refused where they can occur.

### 7.4 What the judge compares

- A score column: within the harness's float tolerance (`SqlParity.outcome`'s rule for doubles), or Graphalytics'
  epsilon where that is the spec.
- A component or community id: never by value (GDS's ids are the in-memory graph's and "not guaranteed stable", its
  WCC page says); by **equivalence of partitions**, Graphalytics' rule, implemented in the harness as a two-way mapping
  check (test-side Java, a verdict: `docs/TENET_CHARTER.md` C2c's World 1).
- A path: by its nodes and relationships in order, as the TCK writes it.
- A distance: exact for unweighted, epsilon for weighted.

---

## 8. The harness

### 8.1 Files: `core/src/test/resources/cypher-parity/`

| File | Contents |
|---|---|
| `tck/` (or the Maven artifact `org.opencypher:tck`, as `@legend_engine_src//:sql_parity_tests` reads the engine's corpus) | the feature files, test input, Apache 2.0 |
| `lite/*.yaml` | lite's own cases, Neo4j-judged: the functions (strings, numbers, lists, maps, temporal), the model-shaped cases (associations, inheritance, keys, edge classes), the algorithms |
| `graphs/*.yaml` | the models and rows the cases run over: the TCK's two named graphs and a Legend-shaped one (persons, firms, addresses, an edge class) |
| `neo4j-answers.jsonl` | Neo4j's answer per case: columns, rows in the TCK value grammar, the server version, the Cypher version, the `sqlSha`; for a scenario with a `CREATE` setup, the dumped graph it produced |
| `ledger.tsv` | per case: the DuckDB status, the Postgres status, the judge (`tck`, `neo4j`, `graphalytics`) |
| `syntax-pins.tsv` | the 156 TCK compile-time syntax errors and lite's own, ratcheted: a case that errs stays an error |

### 8.2 The two things the harness must build

1. **A Gherkin reader for the TCK's subset.** `Scenario` and `Scenario Outline` with `Examples`, the steps `Given an
   empty graph`, `Given the <name> graph`, `Given any graph`, `And having executed:`, `And parameter values are:`,
   `When executing query:`, `Then the result should be, in any order / in order / (ignoring element order for lists)`,
   `Then the result should be empty`, `Then a <Type> should be raised at <phase>: <detail>`, `And the side effects
   should be:`, `And no side effects`. Test-side Java, 300 lines; `tck-api` is Scala and is not used.
2. **The graph a scenario sets up, as tables, and its model.** The scenario's `CREATE`s are run on the oracle, and the
   oracle's graph is dumped (`MATCH (n) RETURN labels(n), properties(n)`, the same for relationships) into the answers
   file; CI seeds the lanes' tables from the dump and never interprets a `CREATE`. The **model is inferred** from the
   dump: a class per label set (a node's label set is its class; a set that extends another is a subclass), a
   property per key typed by its values (Integer, Float, String, Boolean; a list as Variant), a relationship type per
   type and label pair as an association, with the keys the harness mints. A key typed two ways across a class's nodes
   makes the scenario REFUSED by name (the TCK's `var` cases). Nodes of no label are a class `_Node`. The inferred
   model is printed as Pure beside the case, so that a review reads it.

### 8.3 The judge

`CypherParity` reuses `SqlParity`'s grid, statuses, ratchet, ledger I/O and YAML reader, with its own `outcome()`: it
reads the TCK value grammar on both sides, compares rows as a set or in the first sort key's order, nodes and
relationships by labels, type and properties, lists in order (or ignoring order where the step says), maps by key,
doubles within the tolerance, `NaN` and the infinities by name. A Variant cell from lite is read as its JSON. Nodes
never compare by id (C5).

### 8.4 Lanes and guards

- `//core:cypher_parity_test` (DuckDB) and `//core:cypher_parity_postgres_test` (embedded Postgres 16), both in
  `//gates:core`; `//core:cypher_syntax_test` for the syntax ratchet.
- UNSNAPPED, STALE, GONE, as `iq-parity`; a case pinned to `CYPHER 5` or `CYPHER 25` is snapped under that version;
  a case with no prefix under the default (C1), and its answer records which.
- The version drift guard: an answer snapped on another server version than the pinned one is STALE.
- The partition judge for component ids (§7.4).
- The randomized check, as the Postgres line's: a generator of type-correct Cypher over the Legend-shaped graph
  (patterns of one to three hops, optional matches, `WITH` pipelines, aggregates, list expressions), each query run on
  Neo4j and on both lanes.

---

## 9. Milestones and slices

Each landing: `//gates:local`, the TeaVM builds, one full CI run, an independent review by an agent that wrote none
of it, a GATES entry, this document's §12 (the record of landings, to be added), `IN_FLIGHT`.

| Slice | Content | Cases | Needs |
|---|---|---|---|
| **PR1** the line | this study with the user's decisions; `tools/neo4j-oracle/` (`snap.py`, `snap.sh`, the Docker and tarball recipes, `--write`, `--check`); the island plumbing (§5.3); the reader (every clause parses; the version flag); `CypherToPure` for `MATCH` over labels with fixed-length patterns in both directions, `WHERE`, `WITH`, `RETURN`, `ORDER BY`, `SKIP`, `LIMIT`, `DISTINCT`, literals, comparisons, boolean logic, `CASE`, `coalesce`, the implicit grouping with `count`, `sum`, `avg`, `min`, `max`; the TCK reader; the inferred models; the two named graphs; the own-corpus declaration | TCK `clauses/match` (fixed-length), `match-where`, `return*`, `with*`, `expressions/literals`, `boolean`, `comparison`, `null`, `precedence`: about 650 read-only scenarios reachable; `lite/basics.yaml` | nothing outside the protocol layer, the Typer arm and the seam |
| **PR2** values | lists and maps as Variants, `UNWIND`, `collect`, `size`, `head`, `tail`, `last`, `range`, `reverse`, comprehension, `IN`, `all`/`any`/`none`/`single`; strings and numbers from Neo4j's table (lite's own cases, the TCK being thin there); `toInteger`, `toFloat`, `toString`, `toBoolean` | `expressions/list`, `quantifier`, `string`, `typeConversion`, `map`, `conditional`, `aggregation`; `lite/functions.yaml` | |
| **PR3** the model | `OPTIONAL MATCH`, `UNION`, `EXISTS`/`COUNT`/`COLLECT` subqueries, pattern comprehensions, nodes and relationships and paths as values (C8), `labels`, `type`, `properties`, `keys`, inheritance and multi-label patterns (C3), edge classes, keys and identity (C5), dates as Pure dates | `clauses/union`, `unwind`, `expressions/graph`, `pattern`, `path`, `existentialSubqueries`, `useCases`; `lite/model.yaml` | the edge stereotype (C3) |
| **PR4** bounded paths | `*m..n`, `{m,n}`, quantified path patterns with inline `WHERE` and group variables, named paths, bounded `shortestPath`, relationship uniqueness, `ALL SHORTEST` and `SHORTEST k GROUPS` within a bound | `clauses/match` (variable-length), `lite/paths.yaml`; the randomized check | nothing new: unrolling |
| **PR5** recursion | the native (§6.3), its lowering and dialect arms; unbounded `*` and `+`, unbounded `shortestPath` and selectors, reachability; termination guards | `lite/recursion.yaml`; measured on every dialect lite renders (rendered; run on DuckDB, Postgres, H2, SQLite) | the user's ruling (given 2026-10-15); the native's design agreed; the database owner's files |
| **PR6** algorithms 1 | bucket 1: degree, triangles, LCC, WCC, BFS, SSSP distances, PageRank (both profiles), CDLP, k-core, node similarity, link prediction, modularity; the partition judge; the GDS snap through `CALL gds.*.stream` on the oracle; the Graphalytics example graphs and reference outputs | `lite/algorithms.yaml`, `graphalytics/` | PR5 |
| **PR7** algorithms 2 | bucket 2 under size bounds; the refusals of bucket 3 by name with reasons | | |
| **Later** | a Bolt server, hand-written from the protocol's behaviour as pgwire is planned (its documentation is CC BY-NC-SA: read, never copied), so Neo4j Browser and every Bolt driver can query a Legend model; writes after F1; Cypher over bare tables through "Make a class"; SQL/PGQ `GRAPH_TABLE` as a second input spelling, and as a render target where a warehouse has it | | the server program's resumption; F1 |

**First slice: PR1 alone.** It ships Cypher over the class model inside any Pure query and proves the translator
against about 650 TCK scenarios before any value, path or algorithm work starts.

---

## 10. Risks

- **The recursion native's design.** The ruling is given (C6); the risk is now the native's shape: a step lambda the
  Typer types once against the base's row type, two lowering shapes per dialect (§6.2), a termination guard, and the
  size of the SQL it writes on the array-state shape. PR5 measures each on every dialect before anything unbounded
  is accepted; the first four slices ship without it.
- **Cypher is schema-optional; lite is not.** C4 refuses what Neo4j answers with `null`. Real-world queries over
  ad-hoc properties will meet that refusal first. The register row and the error's wording ("no class of label `A`
  declares `x`; `A` declares `y`, `z`") are the mitigation.
- **The oracle is GPL.** It never enters the repository; a license review of each landing's diff checks for Neo4j
  code and manual text, as the IQ reviews check for SAP's.
- **Cypher 25 moves monthly.** A pinned release (C9) and the version drift guard; a bump is a landing.
- **Value kinds Pure lacks.** Zoned times, durations, points, vectors: refused by name; the register names each.
  The TCK's 89 temporal scenarios (1,004 expanded) are mostly out of reach until Pure spells durations.
- **The size of the SQL.** Three-valued spellings per hop, unrolled unions, array-state recursion on Postgres: the
  IQ line met a fourfold growth per nested level; the harness measures the generated SQL's size per case from the
  first landing, and the size bound refuses beyond it.
- **Ownership and contention.** The island plumbing touches ten files other lines own (§5.3); the native touches
  the database owner's. Announced in `IN_FLIGHT` before each landing, as the tenth line's were.
- **Trademark.** "Cypher" is Neo4j's registered mark; the island is "legend-lite's Cypher island", its extensions
  "implementation extensions", per the openCypher attribution notice.

---

## 11. Sources

- **openCypher** (Apache 2.0): the repository `opencypher/openCypher` at `677cbafabb8c` (2026-03-20): `LICENSE`,
  `README.adoc`, `grammar/openCypher.bnf`, `tck/README.adoc`, `tck/index.adoc`, `tck/graphs/named-graphs.adoc`,
  `tck/features/**`; the Maven artifact `org.opencypher:tck` 2024.2 (2025-07-03); opencypher.org/resources (the
  Cypher 9 reference, the M23 ANTLR and EBNF grammars, the TCK API).
- **Neo4j Cypher manual** (2026.06 and 2026.07): "Select Cypher version"; "Deprecations, additions, removals and
  compatibility"; "Equality, ordering and comparison of value types"; "Working with null"; "Lists"; "Aggregating
  functions"; "String functions"; "Mathematical operators"; "Functions" (the catalog of 176); "Patterns" and
  "Variable-length patterns"; "Shortest paths"; "GQL conformance".
- **Neo4j Graph Data Science manual** (2026.07): "Algorithm references" (the tiers); "PageRank" (the defaults, the
  unnormalized scores); "Label Propagation" (ties "arbitrarily but deterministically"); "Weakly Connected
  Components" (ids "not guaranteed stable"); the licensing of GDS: the open sources GPLv3, the distributed library
  partly closed.
- **Neo4j operations manual** (2026.06): Docker images and tags (Community without a suffix), `NEO4J_PLUGINS`,
  GDS in Community.
- **Neo4j licensing:** the `neo4j/neo4j` README (Community under `community/` is GPLv3; Enterprise closed and
  commercial); the Python driver's `LICENSE.APACHE2.txt`; `neo4j/docs-bolt` `LICENSE.txt` (CC BY-NC-SA 4.0).
- **LDBC Graphalytics:** the specification v1.0.5 (arXiv 2011.15028, CC-BY): §2.3 the six algorithms, §2.4 the
  validation rules.
- **Databases:** PostgreSQL 16 "WITH queries" (the working table, `UNION` vs `UNION ALL`, `SEARCH` and `CYCLE`);
  DuckDB "WITH clause" (`recurring`, `USING KEY`, the 1.5.0 deprecation of `UNION` under `USING KEY`) and the
  2025-05-23 `USING KEY` post; the DuckDB community extension page for DuckPGQ (1.4.4; unavailable for 1.5.x).
- **Prior art:** openCypher's CAPS announcement (2017) and the Morpheus talks; Graphiti (arXiv 2504.03182); Raqlet
  (CIDR 2026, arXiv 2508.03978); Apache AGE (age.apache.org); the Kùzu archiving reports (2025-10).
- **This repository:** `docs/LEGEND_SQL_STUDY_2026_10_11.md`, `docs/IQ_SQL_STUDY_2026_10_13.md`,
  `docs/IQ_SQL_SHOWCASE.md`, `docs/TENET_CHARTER.md` (C2, C2b, C6), `docs/WORLD_MAP.md`,
  `docs/LITE_REVIEW_2026_09_11.md`, `docs/NATIVE_PROVENANCE_2026_09_11.md`, `docs/OWN_CORPUS_DECISIONS.md` §14,
  `docs/BI_AND_ETL_PLAN_2026_09_29.md` (F1), `core/.../protocol/sql/InputDialect.java`,
  `core/.../compiler/spec/SqlIslands.java`, `core/.../sql/dialect/FoldsAsRecursiveCtes.java`,
  `core/.../sql/SqlWith.java`, `core/.../lowering/InstanceEquality.java`, `tools/iq-oracle/`,
  `core/src/test/java/com/legend/testcases/SqlParity.java`.

## 12. The record of landings

### 12.1 PR1a, 2026-10-15: the island, the reader, the translator over fixed-length patterns, the seam

The first code of the line, on `claude/keen-noether-v5mis0`, after the study's and the plan's commits. It is the
plan's PR1 less its harness: the island plumbing (§5.3), the reader (§5.1, every clause parses), `CypherToPure` for
the constructs the plan names for PR1, and the seam `compiler/spec/CypherIslands`. The oracle tool
(`tools/neo4j-oracle/`), the TCK reader, the inferred models and the parity lanes are PR1b, next; until they land,
the integration test's rows are derived by hand from Cypher's semantics, and the study's method (§1: the vendor is
the spec, a real oracle judges every case) applies to them only when PR1b snaps them.

- **The reader** (`protocol/cypher/`, JDK only; `CypherLexer` 366 lines, `CypherParser` 775, `CypherAst` 165):
  Neo4j's lexical rules (backticks with the doubled escape, hex and octal integers, the string escapes with
  `\uXXXX` and `\UXXXXXXXX`, `$name`, both comment forms), the `CYPHER 5` and `CYPHER 25` prefix (another version is
  Neo4j's error; `CYPHER runtime=...` is refused), `USE`, `MATCH` and `OPTIONAL MATCH` with named paths, property
  maps, label lists, relationship types with `|`, both arrows and the undirected form, length ranges; `WITH` and
  `RETURN` with `DISTINCT`, `ORDER BY`, `SKIP` or `OFFSET`, `LIMIT`, `WHERE`; every other clause keyword (`UNWIND`,
  `CREATE`, `MERGE`, `SET`, `DELETE`, `DETACH DELETE`, `REMOVE`, `CALL`, `UNION`, `FOREACH`, `LOAD CSV`, `FILTER`,
  `LET`, `NEXT`, `FINISH`, `SHOW`, `INSERT`) kept as an `OtherClause` with its text, so the translator refuses it by
  name. Expressions by Pratt precedence as Neo4j's manual orders it (§5.1), chained comparisons as `AND`s, `^`
  left-associative and looser than unary minus (`-3 ^ 2` is 9 and `4 ^ 1 ^ 3` is 64: the TCK's Precedence2, which
  the first harness run caught against the manual's reading; verified on Neo4j 2026.09.0). Refused in the reader, each by name: label
  expressions, a `WHERE` inside a pattern, path selectors, quantified patterns, dynamic labels, list indexing, label
  predicates in expressions, pattern predicates and comprehensions, `EXISTS`/`COUNT`/`COLLECT` subqueries, type
  predicates.
- **The translator** (`CypherToPure`, 1,583 lines): one `Model` interface (labels to classes; a class's property by
  name and an association end's inverse, each reported through a sink as `SqlToPure.Sources` reports a column, so
  the seam constructs no protocol record; subclassing), answered by the seam over the compiled model. A pattern is read
  from one ROOT node, the node from which every hop navigates to at most one node (a to-one property, or the to-one
  inverse end of a to-many one); every row of the result is one instance of the root's class and every other node a
  navigation from it (`$p.firm.hq`); a hop whose property may hold nothing adds `->isNotEmpty()` to the filter
  (Cypher's `MATCH` finds the node or does not match); a pattern with no such root is refused as one that fans out,
  until the join form (PR2's lists and `UNWIND` give the row-multiplying shape). A second pattern extends the graph
  from a node already bound; a cartesian product and a cycle (both ends bound) are refused by name. `WITH` whose
  items are all pattern variables keeps the graph (a filter); any other `WITH` projects, and the clauses after it read
  columns (`extend`, `select`, `rename` where an alias takes a taken name). `RETURN` without a pattern is one row
  (a `#TDS` literal, the Postgres island's own). The implicit grouping: the non-aggregating items are the keys, the
  aggregates' arguments are projected first; `count(*)` and `count(n)` of a matched variable count rows; `sum` of
  nothing is 0 (a finish after the grouping, as the Postgres island's own aggregates do it); `avg` is a Float. Null
  rules are `SqlToPure.Tx`'s (§5.2): `WHERE` keeps the rows where the condition is TRUE, `NOT null` is null, `AND`
  and `OR` are three-valued, `XOR` spelled from both, `IS NULL` two-valued, a nullable Boolean property a condition
  only where it is known, `coalesce` and `CASE` nullable where their results are, `ORDER BY` with null last ascending
  and first descending (Neo4j's rule, Postgres's spelling). Integer division truncates toward zero
  (`SqlToPure.integerQuotient`), `%` is `rem`, `^` a Float, string `+` a concatenation.
- **The seam** (`CypherIslands`, 140 lines): a label is a class's simple name, or its full path in backticks; several
  classes of one simple name are an error naming them (write the path); a property is `ModelContext.findProperty`'s
  (own, inherited, association-injected; a qualified property is none); an association end's inverse is the other end
  of `findAssociationOf`; the three refusal kinds become the island errors the SQL island's do (PARSE, TYPE, a
  refusal), positioned in the enclosing source.
- **The plumbing**, as announced in IN_FLIGHT: `SqlIsland.CYPHER`; `SpecParser.cypherIsland` (the LEGEND_ENGINE
  surface keeps "unknown DSL island type"); the wire `classInstance` of type `Cypher` with value `{"cypher": text}`
  in `SpecIslandReader` and `ProtocolEmitter`; `SqlIslands.desugar` dispatches the language. `Typer`, `NameResolver`,
  `ValueSpecification`, `SqlToPure`, `InputDialect` untouched.
- **Tests, 67:** `CypherParserTest` (22: shapes, precedence, literals, the other clauses, syntax errors with
  positions), `CypherToPureTest` (29: the Pure of each shape printed as a person writes it, the null rules, the model
  as the schema, Neo4j's errors, 27 refusals by name), `CypherIslandRoundTripTest` (3: text to wire to text byte for
  byte; the engine surface's refusal), `CypherIslandIntegrationTest` (13, DuckDB: a model with an association and a
  class-typed property, the hops in both directions, the grouping, `WITH`, `CASE`, `coalesce`, null ordering,
  arithmetic, the island composing with Pure, the three error kinds placed in the source).
- **Guards, each with a dated reason:** `CodeShapeGuardrailTest` (the reader's cursors and the translator's
  fresh-name counter); `IdentityGuardrailTest` (NAME_CUTTING 107 to 108: the seam's index of the model's classes by
  simple name, built once per model, because a Cypher label is a simple name by the language's nature); the
  dialect-leak ledger (`dialect-leaks/ledger.tsv`: the DuckDB end-to-end test names its database, as the SQL
  island's does). The guards that objected on the way were met in the code: `DropInSurfaceTextRuleTest` (a regex in
  the translator, replaced by a character loop), `ErrorShapeGuardrailTest` (a `default ->` arm spelling a name, now
  an arm per case), `ArchitectureTest.protocolNodesAreConstructedOnlyByTheParserAndTheNormalizer` (the seam built
  the translator's property records: now sinks). No own-corpus row: no test embeds the island in a `###Pure` model
  (the IQ line's row came from one that did); the declaration LITE-DESIGN-cypher-island
  (`docs/OWN_CORPUS_DECISIONS.md` §15) is written when the first such test lands.
- **Register:** `docs/SEMANTICS_REGISTER.md` S57 to S59.
- **Decided in code, to confirm against the oracle in PR1b:** `sum` of no values is 0 and `avg`, `min`, `max` null
  (Neo4j's aggregating functions); null last ascending and first descending; the chained comparison; `-2 ^ 2`;
  `7 / 2` is 3 and `-7 / 2` is -3; `'a' + 1` refused (Neo4j concatenates `a1`; PR2 reads it with `toString`); a
  comparison between values of different types refused (Neo4j: `=` false, `<` null; PR2 spells them).

### 12.2 PR1b, 2026-10-15: the oracle, the TCK as test input, the parity lanes, the showcase

The plan's PR1 completed: the harness of §8, with the first ledger and the showcase (`docs/CYPHER_SHOWCASE.md`).

- **The oracle,** `tools/neo4j-oracle/` (`snap.py`, `snap.sh`, `README.md`, a build test in the checks lane): Neo4j
  Community 2026.09.0 (the newest Community release at this landing, C9: `CypherParity.PINNED_SERVER`; the tarball
  on the container's JDK 21, authentication off, loopback only), the Apache 2.0 Python driver 6.4.1. For each case:
  the graph emptied, the setup run, the graph dumped (nodes with labels and properties, relationships with ends,
  type and properties, every value in the TCK's value grammar), then the query under its Cypher version (lite's
  default 25 when the case names none), its rows in the value grammar or Neo4j's error by code and first line. The
  answers file: `cypher-parity/neo4j-answers.jsonl`, 3,823 answers (3,147 with rows, 673 errors, 3 setup errors; lite's
  26 among them), 4 minutes to snap. The guards: UNSNAPPED, STALE (a sha over the query, the version, the setup and
  the parameters as written, computed alike by the oracle and `CypherAnswers.sha`), GONE, VERSION (a file snapped on
  another server than the pinned one).
- **The TCK as test input:** `cypher-parity/tck/` (the feature files and the two named graphs, Apache 2.0, at
  openCypher commit 677cbaf; its `README.md`), read by `CypherTck` (the Gherkin subset, 300 lines) and by the oracle's
  reader step for step: 3,880 scenarios (1,339 plain, 276 outlines over their examples), 3,116 expecting rows, 667 an
  error, 83 skipped by both readers (a control query, a procedure). The first run caught the one difference
  between the readers (the files' CRLF line endings, which Python's text mode folds) as 66 STALE answers.
- **The inferred models,** `CypherGraph`: a class per label (a node with several labels waits for PR3), a property
  per key typed by its values (a list, map or temporal value waits for PR2; a key typed two ways is refused), each
  relationship type an association between the one class pair it joins (several pairs wait for PR3), its ends named
  `T` and `T_in`, realized as a foreign key on the to-one side (`{target}` for a self-join) or a junction table with a
  join chain; every node a row of its class's table, `n_<label>`, columns `p_<key>` and `r_<type>`; two cases with
  one graph seed once. The model is pinned for the showcase (`showcase/model.pure`). What the inference cannot model
  is SKIP in the ledger with its reason (330: procedures 50, control queries 33, temporal properties 84, a type over
  several class pairs 70, several labels 18, ...).
- **The judge,** `CypherParity` over `SqlParity`'s statuses and ratchet: columns by name in order; rows as a bag or in
  order as the scenario says (a lite case: in order when it has an `ORDER BY`); values by the grammar (`CypherValues`:
  nodes by label set and properties, maps by key, lists in order or as bags where the step says, doubles within 1e-9);
  REFUSED for a refusal by name and for C4's compile error (S58); the decisions file (`decisions.tsv`, a register row
  per decided case, per lane: rows, or the database's error) for the six NaN cases (S60).
- **The lanes,** `//core:cypher_parity_test` (DuckDB) and `//core:cypher_parity_postgres_test` (Postgres 16, as the
  container's unprivileged user), both in `//gates:core`; the ledger `cypher-parity/ledger.tsv`:

  | | both lanes |
  |---|---|
  | cases | 3,906 (3,880 TCK, 26 lite) |
  | PASS | 184 (162 TCK: literals 65, precedence 45, comparison 16, boolean 11, conditional 10, null 5, with-orderBy 4, mathematical 2, return 2, list 1, return-skip-limit 1; lite 22 of 26, the other 4 the refusals and the error the cases are about) |
  | FAIL, BUG, ERROR | 0 |
  | REFUSED by name | 2,723 (a map as a value 577, a pattern that fans out 305, a query starting with WITH 269, the list predicates 563, UNWIND 128, list comprehensions 90, ...) |
  | BOTH_ERROR | 669 |
  | SKIP | 330 |

  The two columns are identical.
- **What the first run found and fixed in the translator and the reader** (each with its test): `^` is
  left-associative and looser than unary minus (`-3 ^ 2` is 9; the manual's reading in §12.1 was wrong, the TCK's
  Precedence2 and the oracle right); `ORDER BY` after a projection reads the projected aliases first (an alias shadows
  the variable it came from); `<-[]->` is undirected; a map key may be a keyword; `-9223372036854775808` is the
  smallest integer; `MERGE`'s `ON MATCH SET` belongs to the `MERGE`; a `UNION` after a `RETURN` is refused as a
  `UNION`; list predicates and comprehensions are refused before their arguments are read; an `ORDER BY` aggregate
  outside the items is refused by name. **Decided:** the NaN comparisons (S60). **Reported to the database owner**
  (IN_FLIGHT): lite's Postgres dialect spells a Float literal in plain digits, which Postgres types numeric (`1e308`
  answers an exact numeric where Neo4j and DuckDB answer a float); the translator refuses a float literal beyond 1e15
  or below 1e-15 until the dialect spells a float.
- **The showcase,** `docs/CYPHER_SHOWCASE.md`: the query over the Legend-shaped graph (`graphs/legend.cypher`), Neo4j's
  answer, lite's identical on both lanes, the inferred model, the Pure and the DuckDB and Snowflake SQL pinned byte for
  byte (`showcase/`).
- **Not yet, carried to PR2:** the randomized differential check (§8.4); the syntax ratchet (`syntax-pins.tsv`); the
  independent review of PR1 (next, before the merge).

### 12.3 The refusal census: the burn-down list (2026-10-15, after PR1b)

Each lane writes `cypher-parity-<lane>-census.tsv` to its test outputs (`CypherParity.census`): the REFUSED and SKIP
cases by reason, specifics folded away. A case counts once, under the FIRST construct the translator refused, so
attacking a category uncovers the next refusal in its queries (a TCK expression scenario typically starts with
`WITH [..] AS list` or `UNWIND`, and most of the 577 "a map as a value" cases are `WITH {..} AS map` scenarios). The
2,723 REFUSED by the slice that reads them:

| slice | reason (the translator's, folded) | cases |
|---|---|---|
| PR2, values | a map as a value | 577 |
| PR2 | the list predicates `all`, `any`, `none`, `single` (and `reduce`) | 564 |
| PR2 | a query starting with `WITH` (a value pipeline over literals) | 269 |
| PR2 | a function not read yet: the temporal constructors `date`, `datetime`, `localdatetime`, `time`, `localtime`, `duration` and their `transaction`/`statement`/`realtime` forms (about 180), `range` (40), `toString` (4), `abs`, `sqrt`, `substring`, `reverse` | 243 |
| PR2 | `UNWIND` | 128 |
| PR2 | a list comprehension | 90 |
| PR2 | a list as a value | 78 |
| PR2 | list indexing and slicing | 52 |
| PR2 | `IN` with a right side other than a literal list, over mixed types, or over a list holding null | 15 |
| PR2 | a comparison across types (String with Integer or Float, Boolean with Integer), ordering booleans, `%` of floats, `STARTS WITH` on a non-string, `RETURN *`, a parameter | 14 |
| PR2 or PR3, the join form | a pattern that fans out (a to-many hop with no to-one inverse), a relationship pattern without a type or with several types, a second pattern sharing no variable (a cartesian product), a `MATCH` after a projecting `WITH` | 328 |
| PR3, the model | `OPTIONAL MATCH` | 25 |
| PR3 | a node or relationship as a value | 17 |
| PR3 | a pattern inside an expression (a pattern predicate or comprehension), a label predicate in an expression | 18 |
| PR3 | `UNION` | 6 |
| PR4, paths | a named path | 27 |
| PR4 | a variable-length relationship | 5 |
| decided, stays | a label naming no class of the model, a property no class declares (C4, S58: the TCK queries labels its graph does not have; Neo4j answers no rows) | 40 |
| the database owner's | a float literal beyond 1e15 or below 1e-15 (the Postgres dialect's float spelling, IN_FLIGHT) | 6 |
| writes, after F1 | `CREATE` 57, `SET` 50, `MERGE` 43, `REMOVE` 31, `DELETE` 29, `DETACH DELETE` 8 | 218 |
| | an `ORDER BY` aggregate outside the items | 1 |

The 330 SKIP are the harness's, not lite's: procedures (50) and control queries (33) in the scenarios; temporal,
duration and list properties the inferred model cannot hold yet (about 100, PR2); a relationship type joining several
class pairs (about 90) and a node with several labels (18), both PR3's inheritance; a few setups Neo4j refused (3).

**The order of attack PR2 takes from this:** values first (maps, lists, `UNWIND`, the list predicates and
comprehensions: 1,700 cases open behind the first refusal), then the function table with the temporal constructors
(243), then the join form (328), which PR3's `OPTIONAL MATCH` and nodes as values build on.

### 12.4 The independent review of PR1, 2026-10-15, and what it changed

An agent that wrote none of PR1, read-only, each claim run on Neo4j 2026.09.0 and on lite over DuckDB files seeded
in the harness's inferred-model style (its report: 17 findings, 4 BLOCKER, 5 MAJOR, 8 MINOR). What it verified
correct on both sides: three-valued `AND`, `OR`, `XOR`, `NOT` over nullable booleans, `IS NULL`, `CASE` in every
form, `coalesce`, the five aggregates over rows, nothing, nulls and keyed groups, the `WITH ... WHERE` HAVING,
null ordering for every type, string ordering by code point on both lanes, `DISTINCT` with nulls, `SKIP` and `LIMIT`
at 0 and past the end, `WITH` renames onto taken names, `WITH` carrying a node, the root choice from either end, a
second `MATCH`, backticked names, unaliased column names verbatim, string escapes, integer division and modulo with
negatives, `^`, chained comparisons, `IN` with an empty list; and that the oracle and the Java reader agree on every
scenario. The findings, and what was done (each with its test; the lite cases `review_*` in `lite/basics.yaml`,
judged by Neo4j):

| # | finding | done |
|---|---|---|
| 1 BLOCKER | relationship uniqueness: `(p)-[:WORKS_AT]->(f), (p)-[:WORKS_AT]->(g)` and a self-loop read twice in one path answered rows where Neo4j binds the two slots to different relationships and answers none | two hops of one type in one `MATCH` refused by name until identity (PR3); two `MATCH` clauses may share one, as in Neo4j |
| 2 BLOCKER | `RETURN 017` answered 17; Neo4j's syntax error | a leading zero is the reader's syntax error |
| 3 BLOCKER | float literals computed as decimals: `0.1 + 0.2 = 0.3` true, `0.1 + 0.2` 0.3 (a bare Pure Float literal lowers to a decimal, S9) | a float literal is `toFloat(...)`, so the database computes in double: `0.30000000000000004`, `1.0 / 3` Neo4j's |
| 4 BLOCKER | 64-bit overflow: `9223372036854775807 - -1` is Neo4j's error; lite answers DuckDB's 128-bit promotion or Postgres's error | recorded as S61 (provisional); PR2 decides whether to spell overflow checks as the IQ island did |
| 5 MAJOR | `2147483647 + 1` is DuckDB's "Overflow in addition of INT32": lite's DuckDB dialect leaves an integer literal untyped and DuckDB types it 32-bit | reported to the database owner (IN_FLIGHT); no case of the corpus reaches it |
| 6 MAJOR | `ORDER BY p.age` with `p.age` not returned failed in the lowering's execute path ("render over an ordered relation whose sort key is not an output column"), a refusal naming no construct; the SQL island has the same | refused by name without a `SKIP` or `LIMIT` (with one the sort is a subquery's and renders); reported to the database owner |
| 7 MAJOR | `ORDER BY a + 'x'` over the items after an aggregation or `DISTINCT`: Neo4j answers, lite refused | computed after the grouping over the projected columns |
| 8 MAJOR | the judge's tolerance (1e-9 relative) hid finding 3 and `-0.0` (Literals5 [9], [10]) | two units in the last place (S15's rule) and a signed zero its own value; `-0.0` now answered (a float's negation is IEEE's, `x * -1.0`: Pure's `minus` lowers to `0 - x`, which is `+0.0`) |
| 9 MAJOR | a label spelled like a relationship type made the inferred model declare `g::T` twice | the association is `g::R_<type>` |
| 10 MINOR | `1<-1` read as an arrow | `<-` in an expression is `<` then a negation |
| 11 MINOR | `end` refused as a name | `END` is a name outside a `CASE` |
| 12 MINOR | `(x) + 1` refused as a pattern | a pattern in an expression has a label, a map or a relationship after it |
| 13 MINOR | `WITH p.age AS a WHERE p.age > 1` refused with the wrong reason | Neo4j's "Variable `p` not defined" |
| 14 MINOR | a superclass label on a hop's target refused as narrowing | a superclass widens: accepted |
| 15 MINOR | `RETURN 1.0 / 0.0` (Infinity) fails the server's JSON writer | the server line's (IN_FLIGHT); the lanes read the JDBC double |
| 16 MINOR | unlabelled nodes and a `:NoLabel` label would merge | the class is `Unlabelled`, a label of that name refused |

The ledger after the review, both lanes: 3,914 cases, PASS 190, FAIL 0, BUG 0, ERROR 0, REFUSED 2,724, BOTH_ERROR
670, SKIP 330; the oracle's check clean.

### 12.5 PR2, 2026-10-15: the values slice — lists, maps, the quantifiers, `UNWIND`, `WITH` first, the function table

The plan's PR2, less what §12.5.4 names. The user's direction was "keep going until all the tests pass or as many
that make sense to pass"; the refusal census (§12.3) set the order: the list predicates, the lists and maps, `WITH`
as a first clause, `UNWIND`, the functions. Everything below is judged by Neo4j 2026.09.0 through the lanes (the
TCK's scenarios and 29 lite cases in `lite/values.yaml`, snapped by the oracle); nothing was derived by hand.

#### 12.5.1 The design: a value is a scalar, a known shape, or a collection the database computes

`CypherToPure` reads every expression into a `Val`:

| `Val` | what it is | who computes |
|---|---|---|
| `Scalar` | a `Tx` (the SQL island's three-valued value), with its literal when it is one of the query's | the database |
| `ListVal`, `MapVal` | a list or map of KNOWN SHAPE: its elements each a `Val`; the shape read from the spelling | the shape here (the world map's compare-not-compute: rearranged, never computed), the elements the database |
| `Coll` | a collection the database computes: a Pure `T[*]` of one kind holding no null (`range`, `split`, a comprehension with a `WHERE`, `tail`, `reverse`) | the database |

Over a known shape, the translator unrolls: `size` is the count of the spelled elements, `keys` the spelled keys in
order, `list[i]` and `list[a..b]` the spelled element or sub-list (negative from the end, clamped, null beyond
either end), `map.key` and `map['key']` the entry (null when none), `+` the elements in a row, `=` the elements
compared pairwise (lists of different sizes, maps of different keys, a list and a scalar are never equal), `IN` a
disjunction of equalities (so nulls, nested lists and maps among the members follow Cypher's rules; a scalar among
literal scalars of its kind stays Pure's `in`), the four quantifiers the predicate read once per element with the
variable bound to it (`BoundScope`) and the results combined by Cypher's three-valued rules (`all` a conjunction,
`any` a disjunction, `none` its negation, `single` the database's count of the elements that hold and of those
unknown: true when exactly one holds and none is unknown, false when two or more hold or none does and none is
unknown, else null — the TCK's Quantifier2 [10]), a comprehension without a `WHERE` the elements each mapped
(the list stays known). Over a `Coll`, Pure's `filter`, `map`, `forAll`, `exists`, `in`, `size`, `first`, `last`,
`drop`, `reverse`; `single` over one is refused by name.

A list or map RETURNED is the Variant the database writes (C8): a shape of literals is one JSON constant
(`fromJson`); a list holding values of the row is built by the database (`toVariant` of a Pure collection) within
what a Pure collection carries — one kind, no null (lite's `toOne` asserts on an empty value), two or more elements
(Pure reads `[x]` as `x`, and lite's `toVariant` of a one-element or mixed literal collection writes the scalar or the
elements' spellings: reported, §12.5.5) — else refused by name; a map holding values of the row is refused by name
(`newMap`'s Variant is a list of pairs on Postgres, §12.5.5). The judge reads the cell as the
database returns it (DuckDB's `JsonNode`, Postgres's `PGobject`) into the TCK's values, so `[3.4, 3.0]` keeps its
float.

`WITH` as a first clause starts from one row, as `RETURN` without a pattern does. A list or map a `WITH` names is
CARRIED as its shape when it reads nothing of the row (literals only: `Col.carried`), for the clauses after it to
read; one holding a value of the row is refused by name (the leaves would need hidden columns). A `null` a `WITH`
names stays null wherever it is read (`Col` of kind NULL).

`UNWIND list AS x`: a `Coll` or a known list of scalars of one kind holding no null is Pure's `flatten`; one holding
nulls, or of one element (no database unnests Pure's `[x]`), is read BY PLACE (a range the database flattens, the
element picked by place); `null` and `[]` give no row; after a projection the rows in hand are each paired with the
elements (Pure's `lateral`). The typer types a flattened column `[0..1]` whatever the elements, so the column is read
as one that may be null. `UNWIND` after a `MATCH` is refused by name: the graph state has no row to pair until the
join form (PR3) projects its bindings.

The function table (`FUNCTIONS`; any other name is refused BY NAME before its arguments are read, so `labels(n)`
names the function, not the node): `coalesce`, `size`/`char_length`/`character_length`, `keys`, `head`, `last`,
`tail`, `reverse`, `isEmpty`, `range`, `split`, `toString`, `toInteger`, `toFloat`, `toBoolean`, `abs`, `sign`,
`sqrt`, `exp`, `log`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `ceil`, `floor`, `round`, `toLower`, `toUpper`,
`trim`, `ltrim`, `rtrim`, `left`, `right`, `replace`, `substring`; `rand`, `randomUUID`, `timestamp` refused as values
of the call, not of the data. Three facts of the reference, each verified on Neo4j 2026.09.0 by a lite case and
spelled so the database computes them: `range(start, end, step)` takes `(end - start) / step` steps, the quotient
truncated toward zero, so `range(0, 1, -123)` is `[0]` (the TCK's text says empty; the oracle's answer is the judge:
Pure's `range` stops before its end, so the last value is spelled and the end moved one past it); `round` is Java's,
half up toward positive infinity (`round(-2.5)` is `-2.0`; lite's `round` of a Float on DuckDB is half to even:
spelled `floor(x + 0.5)`); `substring(s, start)` is 0-based where lite's `substring` is SQL's (legend-engine's own
without its CORRECT_SQL_SUBSTRING_INDEXING flag: a 1-based start and a length, passed through), so the start is
moved by one. Values of different types are never equal in Cypher (`1 = 'a'` is false, `1 < 'a'` null): a type is a
fact of the spelling, so the comparison is known false here (null when either side may be null); PR1 refused it.
Booleans order false before true, as the databases order them (`true > false`, verified on Neo4j by the lite case
`ordering_booleans`; a boolean against another type is null): 2026-10-16, the 21 "ordering booleans" refusals.

#### 12.5.2 The ledger, before and after

Both lanes, identical:

| | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| after the review of PR1 (§12.4) | 3,914 | 190 | 0 | 0 | 0 | 2,724 | 670 | 330 |
| PR2 | 3,942 | 1,037 | 0 | 0 | 0 | 1,904 | 671 | 330 |
| PR2 + boolean ordering (2026-10-16) | 3,943 | 1,044 | 0 | 0 | 0 | 1,898 | 671 | 330 |

What the runs found on the way, each fixed with its test, each judged by Neo4j: a float's negation folded into
the literal lost `-0.0` (kept as IEEE's `x * -1.0`, with the literal's spelling for JSON); the typer's `[0..1]` for a
flattened column (`and`, `or`, `not`, a collection literal of the column: 25 ERROR); a null a `WITH` named read as
a string column (Map1 [2], Map2 [3], Map3 [3], Null1 [5], Null2 [5]); a function's arguments read before its name
(`labels(p)` refused as a node as a value); `toUpper` spelled in lower case to Pure; `range` with a step and
`round` and `substring` as above.

#### 12.5.3 The refusal census after PR2 (DuckDB; Postgres identical)

| reason (folded) | cases | slice |
|---|---|---|
| the function X is not supported yet — of which the temporal constructors and truncations (`date`, `time`, `localtime`, `datetime`, `localdatetime`, their `.truncate`, `duration`, `duration.between/inSeconds/inMonths/inDays`) about 960, `rand` 31, `properties` 5 | 999 | temporal: a slice of its own (Cypher's Time, LocalTime, Duration and zoned DateTime have no Pure type; §12.5.4) |
| a pattern that fans out | 316 | the join form |
| the writes (`CREATE` 69, `SET` 53, `MERGE` 48, `REMOVE` 31, `DELETE` 29, `DETACH DELETE` 8) | 238 | later (the plan: after F1) |
| UNWIND over a list of lists or maps (43), over values of different types (16) | 59 | a Variant column with a known shape (PR2b) |
| a named path | 40 | PR4 |
| a label naming no class (C4), a property no class declares | 40 | decided |
| `collect` | 30 | PR2b (a `Coll` column the aggregate builds) |
| OPTIONAL MATCH (29), UNION (8), a parameter (16), a node or relationship as a value (16), a pattern in an expression (14), a relationship pattern without a type (13), sharing no variable (7), several types (2), a label predicate (2), two hops of one type (1) | 108 | PR3 |
| `%` and integer `/` inside a comprehension or predicate the database computes (9; §12.5.5), a float literal beyond 1e15 or below 1e-15 (6), `RETURN *` (5), a variable-length relationship (5), comparing a Map with a Map after a WITH (5), `toInteger`/`toFloat` of a String (6), a comprehension whose condition may be null (3), lists of row values that may be null or of different types (4), a map of row values (1), the rest (9) | 53 | by name, each a small leg |

SKIP, unchanged: 330 (the harness's own, §12.3).

#### 12.5.4 What PR2 leaves to a next slice, by name

`collect` (an aggregate building a `Coll` column, then `all(x IN eq WHERE x)` over it: the TCK's Precedence1 [14] to
[28]) — lite's relation layer has no collection-valued aggregate today (the typer wants a reduce of `[0..1]`:
"multiplicity [*] is not compatible with result [0..1]"; `toVariant` has no aggregate lowering), so `collect` waits
on a lowering leg of the database owner's (announced, IN_FLIGHT), not on the translator; `UNWIND` after a `MATCH` and over lists of lists or maps (a Variant column with a known shape: the elements'
kinds agree in every case of the corpus); lists and maps holding values of the row that may be null (the inferred
models type every property `[0..1]`, so `[p.age, p.age * 2]` is refused today: a hidden column per leaf, or
`toVariant` of a `[0..1]`); `toInteger`/`toFloat` of a string (null for text that is not a number, where Pure's
`parseInteger` raises); the temporal values (about 960 TCK cases: `date`, `time`, `datetime`, `duration` and their
arithmetic — Pure has StrictDate and DateTime only; a design question of its own: which Cypher temporal types map to
which Pure types, and what the lanes can judge).

#### 12.5.5 Found in other lines' code (IN_FLIGHT)

- lite's `toVariant` of a Pure collection LITERAL of one element writes the scalar (`toVariant([$x])` is `30`, not
  `[30]`: Pure's `[x]` is `x`), of mixed kinds the elements' spellings (`toVariant([1, 'a', true])` is
  `["1","'a'","true"]`), and of a one-element nested literal the element (`[[1,2],[3]]` as `[[1,2],3]`); of a
  computed `[*]` collection (`range`, `filter`) an array always. The translator writes a JSON constant for literal
  shapes and refuses the other two by name (the database owner's).
- lite's `round` of a Float lowers to DuckDB's `round`, half to even (`round(2.5)` is 2, `round(-2.5)` -2); Pure's
  documented rounding is the lowering's to decide (the database owner's); the translator spells Neo4j's own.
- the typer types a `flatten`ed column `[0..1]` whatever the collection's element multiplicity (the typer's; harmless:
  the translator reads the column as one that may be null).
- lite's Postgres lowering of `rem` inside an array lambda (a `filter` or `map` over a collection) types the
  divide-by-zero guard as text (`CASE WHEN 2 = 0 THEN CAST(... AS VARCHAR) ELSE MOD("x", 2) END`: "CASE types bigint
  and character varying cannot be matched"), where the same `rem` in a projection is fine; DuckDB runs both. The
  translator refuses `%` and integer `/` inside a comprehension or predicate the database computes, by name, on
  both lanes (the database owner's; the TCK's Quantifier5 to Quantifier8 `x % 2 = 0` rows).
- a `CASE` whose two last branches are bare `NULL`s is text to Postgres ("CASE types text and boolean cannot be
  matched"): lite's `cast([], @Boolean)` renders a bare `NULL` there. The translator's `UNWIND` by place lets a null
  element fall through to the one `ELSE`.
- lite's Postgres lowering writes `toVariant(newMap([pair('name', ...), ...]))` as a list of `{key, value}` pairs
  (`[{key: 'name', value: 'Alice'}, ...]`) where DuckDB's writes the object (`{name: 'Alice', ...}`): a map built
  from values of the row is refused by name on both lanes (a literal map is a JSON constant and fine); the lite case
  `returned_map_over_rows_is_refused` marks it (the database owner's).
- on Postgres a returned list or map is jsonb, which spells a whole float as an integer (`[3.4, 3.0]` reads back as
  `[3.4, 3]`): S62, one TCK case decided per lane (`decisions.tsv`); DuckDB's JSON keeps `3.0`.

PR2 kept S60 (the NaN comparisons: no `isNaN` native) and S61 (64-bit overflow) as they are, the register says why.

### 12.6 The temporal slice (PR2c), 2026-10-16: dates and local date-times

The user's direction after PR2: "let's do temporal — Pure supports every combination of date and time and date
time". What Pure (as lite declares and lowers it) carries: StrictDate (the database's DATE), DateTime (TIMESTAMP, to
the microsecond: `date(y, m, d[, h, mi, s])`, `adjust` by any `DurationUnit` down to MICROSECONDS, `dateDiff`, the
field natives `year`, `monthNumber`, `dayOfMonth`, `dayOfWeekNumber` (Monday 1), `weekOfYear` (ISO), `quarterNumber`,
`dayOfYear`, `hour`, `minute`, `second`, the first-of natives, `datePart`, `parseDate`, `toString`; every one
verified on DuckDB before the design). What it does not: a time of day (`StrictTime` is declared, no SQL type
carries it), a zone (a DateTime is one instant, GMT-normalized; Cypher's `datetime` keeps its offset or zone and
prints it), a duration (no type). So the slice reads Cypher's `date` as StrictDate and `localdatetime` as DateTime,
and refuses `datetime`, `time`, `localtime` and `duration` by name, each with its reason.

#### 12.6.1 What the translator reads

- **Constructors** `date(x)`, `localdatetime(x)`: from a map of components — the calendar form `date(y, m, d)`
  (month and day default to 1), the week form (the Monday of ISO week 1 is the Monday of the week of January 4th:
  `adjust(date(y, 1, 4), 1 - dayOfWeekNumber(...), DAYS)`, then `(week - 1) * 7 + (dayOfWeek - 1)` days on), the
  quarter and ordinal forms (`adjust` from the quarter's or year's first day), the time part `hour`, `minute`,
  `second` as whole numbers and `millisecond`, `microsecond`, `nanosecond` (a multiple of 1000) as `adjust` steps —
  lite's `date(...)` with a fractional second lowers to a TEXT carrier (§12.6.4), so the fraction is added, not
  written; from an ISO text, a literal in the calendar form as Pure's date literal (`%1984-10-11`,
  `%1984-10-11T12:31:14.645`; other forms — compact, week, ordinal, a zone — refused by name; a text of the row is
  the database's `parseDate`); from another temporal value or one under `date:` / `datetime:` with fields written
  over it (`localdatetime({date: d, day: 28, hour: 10})`; a written-over date-time keeps its fraction through
  `dateDiff(firstMillisecondOfSecond(dt), dt, MICROSECONDS)`). A `time:` or `timezone:` component is refused by
  name. The database validates the ranges (month 13 is DuckDB's "Invalid month" and Neo4j's error: BOTH_ERROR).
- **Fields** `d.year`, `quarter`, `month`, `week`, `weekYear` (the year of the ISO week's Thursday), `day`,
  `ordinalDay`, `weekDay`, `dayOfQuarter` (`dateDiff` from the quarter's first day), and of a date-time `hour`,
  `minute`, `second`; the sub-second fields refused by name (microseconds are the database's, Neo4j's are nanoseconds).
- **Comparison** of dates and of date-times, the database's; `toString` of a date (`'1984-10-11'`, the database's
  spelling agrees with Neo4j's); `toString` of a date-time refused by name (Neo4j spells it as Java does, with the
  seconds and fraction left out when zero; the database spells its own).
- **Truncation** `date.truncate(unit, d[, fields])`, `localdatetime.truncate(...)`: millennium, century and decade
  by the year's remainder (`date(y - y % 1000, 1, 1)`), year, quarter, month, week and day by the first-of natives,
  weekYear by the Monday of week 1 of the week-year, hour, minute and second by the first-of natives; the fields
  written over the start (`{day: 2}`, `{month: 3, hour: 5}`; `dayOfWeek`, `dayOfQuarter`, `ordinalDay` as days on
  from the unit's start); a `timezone` field or a sub-second unit refused by name; a written `null` in place of the
  value is Neo4j's own error (the TCK's "Invalid call signature for Truncate").
- **Stored dates:** the inferred models (`CypherGraph`) type a property holding `Date('...')` as StrictDate (DATE)
  and `DateTime('...')` without an offset as DateTime (TIMESTAMP), the seed binding their texts; a time, a zoned
  date-time or a duration stays "a kind the model does not hold".
- **The judge** reads a date cell (lite's own date literal values, or the driver's) as Neo4j's Python driver spells
  one in the answers: `Date('yyyy-MM-dd')`, `DateTime('yyyy-MM-ddTHH:mm:ss.nnnnnnnnn')` with nine fraction digits
  always.
- **The clock** (`date()`, `date.realtime()`, `localdatetime.statement()` ...): refused as a value of the call, not
  of the data.

#### 12.6.2 The ledger

Both lanes, identical:

| | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| before (PR2 + boolean ordering) | 3,943 | 1,044 | 0 | 0 | 0 | 1,898 | 671 | 330 |
| the temporal slice | 3,959 | 1,168 | 0 | 0 | 0 | 1,800 | 673 | 318 |

Of the TCK's temporal features (1,004 cases): 111 PASS, 843 REFUSED by name (the zoned, time-of-day
and duration values, the sub-microsecond precisions, the non-ISO texts), 0 BOTH_ERROR, 50 SKIP
(stored times, zoned date-times and durations). SKIP fell from 330 to 318 and BOTH_ERROR rose by two: the graphs
holding dates and local date-times are inferred now (`CypherGraph`). 16 lite cases in `lite/temporal.yaml`, each
judged by Neo4j.

#### 12.6.3 What the first runs found, each fixed with its case

A date built by `adjust` (the week, quarter and ordinal forms, the week-year's Monday) came back as a timestamp
(DuckDB's date plus an interval is a timestamp): `datePart` takes the day back. The arithmetic of integers is a
Number to Pure, which `date` and `adjust` refuse: a cast to Integer. A `dayOfWeek` field over a week truncation
(the TCK's Temporal9 [1] #43). Neo4j 2026.09.0 raises an internal error (50N42, "Unexpected error") on
`date({year: null})`; the island answers null, and the case is not pinned.

#### 12.6.4 Found in other lines' code (IN_FLIGHT, the database owner's)

lite's `date(y, m, d, h, mi, s)` with a fractional second (a Number that is not a literal) lowers to the
kind-faithful TEXT carrier (`strftime(make_timestamp(...))` concatenated), which a comparison or a field read then
sees as a VARCHAR ("Cannot compare values of type TIMESTAMP and type VARCHAR"; `date_part('hour', VARCHAR)`), where
the same date with whole seconds, and a date-time literal with a fraction, are TIMESTAMPs. The translator writes
whole seconds and adds the fraction with `adjust(..., MILLISECONDS | MICROSECONDS)`.

#### 12.6.5 Left to a next slice, by name

Zoned date-times (`datetime`: an offset or a named zone kept with the value and printed; the database's
TIMESTAMPTZ could carry the instant, the zone is the question); times of day (`time`, `localtime`: `StrictTime` is
declared, a SQL TIME type is the lowering's leg); durations (`duration`, `duration.between/inMonths/inDays/inSeconds`,
temporal arithmetic: a months-days-seconds-nanoseconds value with its own normalization and `P...` spelling, no Pure
type); nanosecond precision (the databases keep microseconds); `toString` of a date-time; the sub-second fields;
compact and week-form texts.

### 12.7 The time-of-day leg, 2026-10-16: `localtime` over Pure's StrictTime, a SQL TIME

The user's direction after the temporal slice: "Yes do it all here" — times of day, zoned date-times, durations —
with the time-of-day leg first, since every database the lanes run names a TIME type. This leg is the first that
touches the database owner's files: Pure declares `StrictTime` and only prints it, and legend-engine's relational
lowering refuses it (`pureToSQLQuery.pure`, "not supported yet"), so a time of day in SQL is lite's own
(`docs/SEMANTICS_REGISTER.md` S63).

#### 12.7.1 What lands

**The platform.** `SqlType.Scalar.TIME` (DuckDB, Postgres, H2 `TIME`; MariaDB and Trino `TIME(6)`; Snowflake and
BigQuery `TIME`; SQLite its text; ClickHouse, whose `Time` type is experimental, and Databricks, which has none,
refuse by name; the engine-text renderers refuse, as legend-engine does) and `SqlExpr.TimeLit`, rendered
`TIME 'HH:mm:ss[.ffffff]'`. `StrictTime` lowers to it (`PureSql.primitiveCarrier`; a `%12:31:14.645` literal is a
`TimeLit`, its minute form padded with its seconds). Two lite natives, each one lowering rule, both emitted by the
Cypher translator only and by FQN (`Pure.Lite.TIME_OF`, `Pure.Lite.AT_TIME`; `INTERNAL_DESUGAR` 17 → 19 with the
dated note in `NativeCatalogGovernanceTest`): `meta::legend::lite::timeOf(Date[1]):StrictTime[1]`, a CAST to
TIME, and `meta::legend::lite::atTime(Date[1], StrictTime[1]):DateTime[1]`, the date-time at that time of that
day (`SqlFn.DATE_AT_TIME`: `date + time` on DuckDB, Postgres and H2, `TIMESTAMP(d, t)` on MariaDB,
`TIMESTAMP_NTZ_FROM_PARTS` on Snowflake, `DATETIME(d, t)` on BigQuery, the texts joined on SQLite and Trino). The
executor reads a TIME cell as `PureTimeLiteral` (the date's one-carrier rule, for the time): DuckDB's driver delivers
a `LocalTime`, pgjdbc a `java.sql.Time` holding milliseconds that the executor re-fetches as `LocalTime` for the
microseconds; `WireTypes` maps `Types.TIME`. No TIME store column: the `###Relational` store grammar (legend-engine's,
and lite's) has none, so a time of day is never stored (S64) — the island computes one from texts, components and
date-times, and the harness refuses a graph holding one by name.

**The island.** `localtime(...)` from an ISO text (`hh:mm[:ss[.f{1,9}]]`; the compact forms refused by name), from
components (`hour` to `nanosecond`, a `time` base — a time or a date-time — with fields written over it: the
fraction kept through a written-over second, as the TCK pins for date-times), from a date-time (`timeOf`), the
fields (`hour`, `minute`, `second`, and the sub-second fields `millisecond`, `microsecond`, `nanosecond` — new for
date-times too: the whole fraction of the second in that unit, the database's `dateDiff` from the second's start),
comparison, ordering, truncation (`day` to `second`, fields written over), null propagation,
`localdatetime({date: d, time: t})` (`atTime`, with fields written over), and times of stored date-times. The one
design rule: **Pure's date-time natives read a time of day through the date-time at it on 1970-01-01** — `atTime(%1970-01-01, t)`
for a field, a truncation, a comparison (Pure orders no `StrictTime`) — and `timeOf` takes the time back. The date is
a token the translator spells; the database computes every value. Neo4j's errors as Neo4j spells them: "Cannot get the
time of: 1984-10-11" (a written date; one of the row is refused by name, the message names the value), "Unsupported
field: Year", "hour must be specified", "Unit is too large to be used for truncation", "Not supported: date" /
"Not supported: time" (a date component of a time, a time component of a date), "Cannot construct local time from:
NO_VALUE" (a null `time` component). Refused by name: a zoned `time`, `toString` of a time (Neo4j trims the
fraction's zero groups, the database spells its own way), a list or map holding a time (the Variant spells it as
text — and now a date or date-time too, which the JSON carrier had spelled as text), a text of the row in a form the
database does not parse, the clock functions.

**The judge.** `CypherValues.ofCell` renders a `PureTimeLiteral` (or a driver's `LocalTime`) as the Python driver
spells a local time, `Time('HH:mm:ss.nnnnnnnnn')`, nine digits.

#### 12.7.2 The ledger

Both lanes, identical:

| | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| before (the temporal slice) | 3,959 | 1,168 | 0 | 0 | 0 | 1,800 | 673 | 318 |
| the time-of-day leg | 3,979 | 1,195 | 0 | 0 | 0 | 1,785 | 681 | 318 |

Of the TCK's temporal features (1,004 cases): 129 PASS, 825 REFUSED by name, 50 SKIP. 35 lite
cases in `lite/temporal.yaml`, each judged by Neo4j.

#### 12.7.3 What the first runs found

DuckDB has no `date_trunc` over a TIME (measured, 1.4.4) — which is why nothing truncates a TIME: the epoch-day rule
above. The composer refuses to print a synthesized temporal literal without its written form, so the island's date
and time literals now carry one (`CDate(lit, written, pos)`), and the translator's unit test can read the Pure it
writes. Neo4j 2026.09.0 answers `localtime({time: null})` with "Cannot construct local time from: NO_VALUE" where
`date({year: null})` is its internal error: the island mirrors the first and answers null for the second.

#### 12.7.4 Left by name

Zoned date-times and times (`datetime`, `time`: the next leg, as instants); durations and temporal arithmetic;
`toString` of a time or date-time; nanosecond precision; truncation to the millisecond or microsecond; the compact
and week-form texts; a stored time of day (a TIME store column, if ever wanted, is a grammar decision).

### 12.8 The zoned date-time leg, 2026-10-16: `datetime` as an instant, its zone carried beside

The second of the user's three ("do it all here"): zoned date-times. The spec is upstream's model, as the user asked
("Does upstream legend-pure or legend-engine support timezones? We should try to use that as spec"): legend-pure's
DateTime is an instant — a literal's offset is normalised to UTC at parse, the offset discarded — and zones exist
only at formatting (`%t{[zone]pattern}`) and on the connection (`timeZone`). So the island carries Cypher's
`datetime` as Pure's DateTime, the instant, and the zone as a fact beside the value, which the translator knows while
it can see it (`docs/SEMANTICS_REGISTER.md` S65).

#### 12.8.1 What lands

**The zone beside the value.** `Kind.ZONED` is a date-time instant whose zone, when known, rides the translation's
detail (`SqlToPure.Tx.detail`, the hook the SQL islands' dialects use for what they know of a type beyond its
kind) as a `Zone`: a spelled offset's sign, hours, minutes and seconds as written, or a region's name. A projected
column carries it (`Col.detail`), so `WITH datetime(...) AS d RETURN d.hour` reads the field in the zone; a value of
a list, an aggregate's or one otherwise computed has no zone, and reading it in its zone is refused by name ("the
island keeps the instant"). **An offset is adjust steps:** the wall time of `datetime('...T12:31+01:00')` is the
literal, the instant `adjust(-1, HOURS)` of it, the field `hour(adjust(+1, HOURS))` — the translator spells the
offset's digits, the database computes every value (the second-precision offsets of the TCK's Temporal1 [13],
`+02:05:59`, are three steps). **A named zone is the database's:** two lite natives, each two of the database's zone
conversions (the `convertTimeZone` lowering's pair), `meta::legend::lite::instantIn(local, zone)` —
`timezone('UTC', timezone(zone, local))` — and `localIn(instant, zone)` — `timezone(zone, timezone('UTC',
instant))`; DuckDB reads region names through the ICU it bundles (measured 1.4.4: Europe/Stockholm in and out of
summer time), Postgres's `timezone(zone, t)` is `AT TIME ZONE`, the same pair; `INTERNAL_DESUGAR` 19 → 21.

**The island.** `datetime(...)` from an ISO text with `Z` or an offset (no zone is the default zone, UTC; a named
zone in brackets is refused by name), from components (the wall time the components spell, in the written zone,
else the base's, else UTC; `{datetime: d, timezone: z}` alone converts, the instant kept, as the TCK's Temporal3
[11] pins), from a local date-time (UTC), from epoch seconds and milliseconds; the fields read on the wall time
(`year` to `nanosecond`), the zone fields (`timezone`, `offset` as Neo4j prints them, `offsetMinutes` and
`offsetSeconds` as the database's distance between the instant and the wall time, `epochSeconds` and `epochMillis`
as its distance from the epoch); comparison and ordering of instants; truncation by the wall time, the result in
the zone written over it (`{timezone: 'Europe/Stockholm'}`), else the source's; `localdatetime`, `date` and
`localtime` of a zoned value by its wall time; null propagation. Neo4j's errors as Neo4j spells them ("Cannot
select datetime from: 1984-10-11", "Cannot get the offset of: 1984-10-11T12:00:00", "Invalid value for TimeZone:
Text '+0100' could not be parsed at index 0" — a component's offset needs its colon where a text's does not —
"Cannot assign time zone if also assigning other fields."). Refused by name: a zoned `time`, a named zone in a text,
a zoned text of the row, `toString`, lists holding zoned values, the clock functions.

**The judge.** Neo4j's driver spells a zoned date-time with its offset, `DateTime('...T12:31:14.000000000+01:00')`;
lite's cell is the instant, `DateTime('...T11:31:14.000000000')`. A pair of one zoned and one local text is equal
when they name one instant (`CypherValues.sameInstant`, S65); two of one kind compare as text. A local date-time
Neo4j prints without an offset still has to match lite's text exactly.

#### 12.8.2 The ledger

Both lanes, identical:

| | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| before (the time-of-day leg) | 3,979 | 1,195 | 0 | 0 | 0 | 1,785 | 681 | 318 |
| the zoned date-time leg | 3,998 | 1,401 | 0 | 0 | 0 | 1,592 | 687 | 318 |

Of the TCK's temporal features (1,004 cases): 323 PASS, 631 REFUSED by name, 50 SKIP. 55 lite
cases in `lite/temporal.yaml`, each judged by Neo4j.

#### 12.8.3 Left by name

Zoned times of day (`time`: the same carriage, a time of day and its zone; not built here); durations and temporal
arithmetic (the third leg); `toString` of a zoned value; nanosecond precision; a zoned value stored (no zoned store
column: the harness refuses the graph); the fields of a zoned value whose zone the translator does not carry.

### 12.9 The duration leg, 2026-10-16: a class of the model, not a platform type

The third of the user's three, and the user's question with it: "can this duration concept be a user class that has
a slot for each duration unit, does it need built into platform?"

#### 12.9.1 The answer: a user class, nothing in the platform

**Yes, and nothing new in the platform.** A Cypher duration is Neo4j's four-slot value — months, days, seconds,
nanoseconds (years are twelve months, weeks seven days, hours and minutes seconds; the nanoseconds kept in
{@code [0, 1e9)} with the seconds floored) — and the island carries one as an instance of a class **the model
declares**:

```
Class graph::Duration
{
    months: Integer[1];
    days: Integer[1];
    seconds: Integer[1];
    nanoseconds: Integer[1];
}
```

The translator finds it as the model's one class named `Duration` with those four `Integer[1]` slots (any package;
refused by name with the declaration to add when the model has none). This is the world-map tenet's PROGRAM
case: the class is model text, typed constants at build time; the platform ships no `Duration` primitive and no
native for it. What the platform already gives is all a duration needs: the instance literal
`^Duration(months=…, days=…, seconds=…, nanoseconds=…)` lowers to the database's struct (DuckDB's STRUCT,
Postgres's `jsonb_build_object`), a slot reads as a struct field, `adjust` adds a slot to a temporal value and
`dateDiff` measures one, and every value is the database's arithmetic over the slots: the translator spells `12 *
years + months`, never computes it. (Pure's own `meta::pure::functions::date::Duration` — one number and one
`DurationUnit`, the argument of `add(date, duration)` — is a single-unit shape and stays what it is.)

What a platform type would have bought: a `toString` in Neo4j's `P1Y2M3DT4H5M6.007S` spelling (refused by name
here: the database would have to build the text), ordering (Neo4j answers null, refused by name) and a stored
duration (a struct column: no store grammar for one, as for a time of day). None of those needs the type to be
the platform's; each is a lowering or a grammar decision for later.

#### 12.9.2 What lands

`duration({years, months, weeks, days, hours, minutes, seconds, milliseconds, microseconds, nanoseconds})`
(each a whole number; a fractional component — Neo4j carries the fraction down the units — is refused by name),
`duration('P1Y2M3DT4H5M6.007S')` in the unit form with signs and a fraction on the seconds (the date-time form
`P2012-02-02T14:37:21` refused by name; a text of the row refused by name), `duration(duration)`; the twenty
fields (`years` to `nanoseconds`, the whole units of a slot, `nanoseconds` the seconds and nanoseconds together;
`quartersOfYear` to `nanosecondsOfSecond`, the remainders within the next unit), each the database's integer
quotient and remainder; `temporal + duration` and `temporal - duration` (a date takes the months and days, a time
of day the seconds and wraps, a local date-time all four in Neo4j's order, a zoned date-time the months and days
on its wall time and the seconds on its instant), `duration ± duration`, `duration * integer` (a division is
refused by name: fractions again); `duration.between(a, b)`, `inMonths`, `inDays`, `inSeconds` — Neo4j's split as
the database's: the whole months (the calendar count, one back when stepping it overshoots, one on when it
undershoots), the whole days from there, the seconds and nanoseconds of the rest, over two dates or date-times
(a date at midnight, a zoned date-time its instant) or two times of day; equality by the four slots; null
propagation. Neo4j's errors as it spells them ("Unknown field: quarters", "Text cannot be parsed to a Duration",
"Invalid call signature for DurationFunction: Provided input was [1984-10-11, String(\"DEFAULT_TEMPORAL_ARGUMENT\")]").

**The judge.** Neo4j's driver spells a duration `Duration('P30Y8M13D')`; lite's cell is the instance, a map of
its four slots (DuckDB's struct as the executor's field map, Postgres's jsonb object). The judge sums the text's
units into the slots and compares (`CypherValues.sameDuration`, `docs/SEMANTICS_REGISTER.md` S67); the parity
harness declares `g::Duration` in every inferred model.

#### 12.9.3 The ledger

Both lanes, identical:

| | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| before (the zoned date-time leg) | 3,998 | 1,401 | 0 | 0 | 0 | 1,592 | 687 | 318 |
| the duration leg | 4,014 | 1,488 | 0 | 0 | 0 | 1,517 | 690 | 318 |

Of the TCK's temporal features (1,004 cases): 402 PASS, 552 REFUSED by name, 50 SKIP. 70 lite
cases in `lite/temporal.yaml`, each judged by Neo4j.

#### 12.9.4 Left by name

`toString` of a duration; fractional components and division; the date-time text form; `duration.between` of a
date and a time of day (Neo4j's reading of the mix; landed in §12.10); zoned times of day; a stored duration;
nanosecond precision.

### 12.10 The temporal remainder, 2026-10-16: the text forms, fields over a base, fine truncation, the mixed `between`, a named zone in a text

The translator only: what §12.6 to §12.9 left by name that the database can answer, taken from the ledger's refusal
census (the user: "keep going until we pass all the tests or as many as makes sense").

#### 12.10.1 What lands

**The text forms.** One reader (`TemporalText`) for the forms Neo4j reads beyond the ISO calendar one: a date's
compact form (`20150721`), a month (`2015-07`, `201507`), a year (`2015`), the week forms (`2015-W30-2`,
`2015W302`, `2015-W30`, `2015W30`), the ordinal forms (`2015-202`, `2015202`), the quarter forms (`2015-Q2-60`,
`2015Q260`, `2015-Q2`, `2015Q2`); a time of day as `214032.142`, `2140`, `21`, with `.` or `,` before the
fraction; a date-time as any date form, `T`, any time form; after the time a zone: `Z`, an offset with or without
its colon (`+01:30`, `+0130`, `+01`), a region's name in brackets, alone or after an offset. The calendar forms
keep Pure's literal (the Pure the island writes prints `%2015-07-21T21:40:32.142`); the week, ordinal and quarter
forms are the components' spelling, the database computing the date as it does for `date({year, week,
dayOfWeek})`. A named zone alone is the database's reading of the wall time in that zone (`instantIn`, §12.8);
with an offset too, the instant is the offset's and the zone is kept beside, under Neo4j's check that the two
agree — the database judges it: the instant read back in the zone (`localIn`) is the wall time written, else
`fail('Timezone and offset do not match: ...')` raises in the database (Pure's `fail` in an `if` arm, the typer's
raise-typed-at-the-value rule). An overlap's second reading passes (`2017-10-29T02:30+01:00[Europe/Stockholm]`),
a gap's or a wrong offset's does not. The errors are Neo4j's: `Text cannot be parsed to a Date` (`LocalDateTime`,
`LocalTime`, `DateTime`), `Invalid value for TimeZone: Text '+2' could not be parsed at index 0`. A signed year
(ISO's expanded form, `+2015-07-21`, Neo4j's nine-digit years) is refused by name.

**Week, quarter and ordinal fields over a date base.** `date({date: d, week: 1})` keeps `d`'s week-year and day
of the week (`2015-07-21` to `2014-12-30`, as Neo4j), `{date: d, quarter: 1}` its year and day of the quarter,
`{date: d, ordinalDay: 5}` its year, `{date: date('1816-12-30'), week: 2, dayOfWeek: 3}` the base's week-year
1817; each field not written is the database's read of the base (`weekOfYear`, `dayOfWeekNumber`, the year of the
week's Thursday, `quarterNumber`, the days since the quarter's first day), then the §12.6 construction. Over a
date-time base too (`localdatetime({datetime: dt, week: 1})`).

**Truncation to the millisecond and the microsecond.** The millisecond: the second's start plus the whole
milliseconds since it (`adjust` by `dateDiff`); the microsecond: the value itself, the database keeping
microseconds. For local and zoned date-times and times of day; a field written over either is refused by name
(Neo4j sets it on the truncated value). A date's: Neo4j's `Unit too small for truncation: Millis`.

**`duration.between` of a time of day and a date or date-time.** Neo4j's reading: the time on the other's date — a
date at its midnight, a local date-time's date, a zoned date-time's wall date with the time then read in its zone
(`instantOf`) — so only the times tell (`between(date, localtime('12:00'))` is `PT12H`; the reverse `PT-12H`;
`inMonths` and `inDays` of the mix `PT0S`). The day summer time ends in Stockholm measured as instants, as Neo4j's.

#### 12.10.2 The ledger

Both lanes, identical:

| | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| before (the duration leg) | 4,014 | 1,488 | 0 | 0 | 0 | 1,517 | 690 | 318 |
| the temporal remainder | 4,036 | 1,577 | 0 | 0 | 0 | 1,443 | 698 | 318 |

Of the TCK's temporal features (1,004 cases): 477 PASS, 477 REFUSED by name, 50 SKIP. 93 lite
cases in `lite/temporal.yaml`, each judged by Neo4j.

#### 12.10.3 Left by name, and the census after this leg

The refusal census over the whole corpus (the per-case file beside the ledger): a pattern that fans out, 326 (the
join form, PR3); a nanosecond component, 282 (the database keeps microseconds: Temporal9's 202 are the
nanosecond feature itself — stays refused); `time(...)` and `time.truncate`, 165 (a zoned time of day: the next
leg, a time with a zone beside as §12.8's date-time); the write clauses `CREATE`, `SET`, `MERGE`, `REMOVE`,
`DELETE`, 230 (a read-only island); `UNWIND` over lists of lists or maps, 43; `collect`, 40 (the database
owner's aggregate); named paths, 40; a label naming no class (C4), 39; `rand()`, 30 and the clock functions
(`date()`, `datetime.transaction()`, ...: a clock is the database's, 16); `OPTIONAL MATCH`, 29; parameters, 16;
`toString` of a duration, 12; fractional durations, 9; a field over a fine truncation; the `offset` field of a
named zone (the database would format the offset it reads); signed years.

### 12.11 The zoned time-of-day leg, 2026-10-16: `time` as its instant on the epoch day, the offset beside

The last of the temporal values: Cypher's `time` (Neo4j's OffsetTime, a wall time and an offset), the ledger's
largest temporal refusal after the nanosecond (165 cases: `time(...)` 117, `time.truncate` 48). The translator
and the judge only; no file of the database owner's, no native.

#### 12.11.1 The model

§12.8's, carried down: a zoned time of day is **its instant on the epoch day** — Pure's DateTime
`1970-01-01T11:31:14.645876` for `12:31:14.645876+01:00`, the day before or after when the offset crosses
midnight — with the offset beside it as a fact of the translation (`Kind.ZONED_TIME`, the `Zone` on the
translation's detail). Why the instant on a day and not the time of day in UTC: Neo4j orders and compares
`time` values as Java's OffsetTime does, by the instant of the wall time less the offset on one day, so
`00:30+01:00` (23:30 the day before) is before `23:30Z`; a time of day in UTC would fold the two together. The
wall time is the instant shifted by the offset (`adjust` steps the database computes, as §12.8's), and
`timeOf` takes the time of day back when one is read as a local time. The fields read the wall time; the zone
fields are the offset's spelling (`timezone`, `offset`) or the database's distance between the instant and its
wall time (`offsetMinutes`, `offsetSeconds`); `epochSeconds` is Neo4j's "Epoch not supported.".

**The judge.** Neo4j's driver spells one `Time('12:31:14.645876000+01:00')`; lite's cell is the instant,
`DateTime('1970-01-01T11:31:14.645876000')`. They are the same value when they name one instant on the epoch
day (`CypherValues.sameEpochTime`, `docs/SEMANTICS_REGISTER.md` S68). Two differences the register keeps: Neo4j's
`=` between two zoned times is true only when they name one instant and one offset (`time('12:00+01:00') =
time('11:00Z')` is false; lite's instant says true: the lite case `time_comparison` is decided, S66 extended);
and the oracle's Python driver prints a zoned time's second-precision offset to the minute without moving the
wall time (`+02:05:59` as `+02:05`: an instant 59 seconds off — a zoned date-time's it prints with the wall time
moved, the instant kept), so the TCK's own expected text judges those three cases (S69, `decisions.tsv`).

#### 12.11.2 What lands

`time('21:40:32.142+0100')` in every time form of §12.10 with `Z` or an offset after it (none: UTC; a region's
name in brackets is Neo4j's error "Using a named time zone e.g. [UTC] is not valid for a time without a date");
`time({hour, minute, second, millisecond, microsecond, nanosecond, timezone})` (an offset with seconds too; a
region's name is refused by name: Neo4j takes its offset at the current date, a value of the call); `time(x)` of a
local time or date-time (UTC), of a zoned date-time (its wall time, its offset; a named zone's refused by name),
of another `time`; the selections of the TCK's Temporal3 [3] — `{time: t}`, `{time: t, timezone: z}` (a zoned
base converts: the instant kept, the offset changed; a local base keeps its wall time), fields written over a base
(applied to the wall time in the base's zone, then converted to the written zone); a zoned time into the other
constructors — `localtime(t)` and `{time: t}` the wall time, `localdatetime({date, time: t})` the wall time on
the date, `datetime({year, month, day, time: t})` the wall time on the date in the time's offset, converted when
a `timezone` is written; the fields; `=`, `<>` and the orderings by the instant; `ORDER BY`; `time.truncate` to
the day, hour, minute, second, millisecond and microsecond, of a time, a local time (UTC), a local or zoned
date-time, fields written over it, a `timezone` written over it replacing the source's (Neo4j's: no conversion);
`localtime.truncate` of a zoned time; `time ± duration` (the wall time moved by the seconds, wrapping within the
day, the offset kept); `duration.between` and its three siblings over a zoned time and a zoned time (the instants),
a local time (read in the zoned one's offset), a date or local date-time (read in the time's offset, the time on
its date), a zoned date-time (the time on its wall date in the time's own offset, then the instants) — the
TCK's Temporal10 rows, each; null propagation. Neo4j's errors as it spells them ("Cannot get the time of:
1984-10-11", "Cannot construct time from: NO_VALUE", "hour must be specified", "Unsupported field: Year",
"Epoch not supported.", "Unit is too large to be used for truncation").

Refused by name: a named zone in a component or of a date-time read as a time (the clock's offset), the current
time (`time()`, `time({timezone: z})`), `toString`, a time of a list, a text of the row, a stored time (S64).

#### 12.11.3 The ledger

Both lanes, identical:

| | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| before (the temporal remainder) | 4,036 | 1,577 | 0 | 0 | 0 | 1,443 | 698 | 318 |
| the zoned time-of-day leg | 4,062 | 1,678 | 0 | 0 | 0 | 1,357 | 709 | 318 |

Of the TCK's temporal features (1,004 cases): 566 PASS, 388 REFUSED by name, 50 SKIP. 119 lite
cases in `lite/temporal.yaml`, each judged by Neo4j. What stays refused in temporal: the nanosecond (282, the
database keeps microseconds; Temporal9's truncation rows carry one), the clock functions, `toString` of the new
values, fractional durations, a stored time.

### 12.12 The nanosecond leg, 2026-10-16: DuckDB's nanoseconds, Postgres refusing by name

The user's question ("DuckDB has nanos now, doesn't it?") and the answer measured on DuckDB 1.4.4 (the build's
driver) and 1.5.6 (the newest release): `TIMESTAMP_NS` keeps nanoseconds at rest — literals, casts, comparisons,
ordering, `epoch_ns`, `make_timestamp_ns`, the JDBC cell — and every temporal function drops them: `+ INTERVAL`
returns a microsecond `TIMESTAMP` and `INTERVAL` has no nanosecond unit, `date_trunc` and `time_bucket` return
`TIMESTAMP`, `date_diff` and `date_part` know no `nanosecond`, `TIME_NS` (new in 1.5) has no arithmetic, and the
tracker holds no request to change any of it. Postgres is microseconds throughout. So the leg is DuckDB's, over the
nanosecond epoch, and the first deliberate lane divergence of the line.

#### 12.12.1 The model

A temporal value of nanosecond precision is the database's `TIMESTAMP_NS`: a date-time, a zoned date-time (its
instant), a zoned time of day (its instant on the epoch day, §12.11) and — new — a local time of day, carried as
the date-time at it on the epoch day too (the database's `TIME` is microseconds). The translation carries the
precision beside the value as it carries a zone (`Carried(zone, nanos)` on the translation's detail; through a
`WITH` as a column's detail), and spells every operation on such a value over the nanosecond epoch, which the
database computes:

- the nanoseconds since the epoch are Pure's `dateDiff(%1970-01-01T00:00:00, x, NANOSECONDS)`, a nanosecond
  step Pure's `adjust(x, n, NANOSECONDS)`; the lowering (`DateShifts`, `Scalars.dateDiffExpr`) gives the
  `NANOSECONDS` unit, which no engine's interval has, the two MIR functions `EPOCH_NS` and `FROM_EPOCH_NS`; DuckDB
  renders `epoch_ns` and `make_timestamp_ns`, every other dialect throws `DialectCapability` by name;
- a literal finer than a microsecond is the literal to the microsecond moved by its sub-microsecond digits (the
  translator splits the written digits, computing nothing), so no literal of nine digits reaches the lowering's
  text path for them;
- a sub-day shift (an offset's hours, a duration's seconds and nanoseconds) is one nanosecond step; a calendar
  step (`adjust` by months or days) and a named zone's conversion (`instantIn`, `localIn`) keep microseconds, so
  the sub-microsecond remainder (`rem(epochNanos(x), 1000)`) is put back after each;
- the fields read the nanoseconds since the second's start (the millisecond and microsecond their quotients);
  truncation to the microsecond is the second's start plus the whole microseconds; the `millisecond`,
  `microsecond` and `nanosecond` fields written over a truncation are added after it (the nanosecond one as a
  nanosecond step when not a multiple of a thousand);
- `duration.between` of such values measures the rest in nanoseconds (the whole months and days stepped with the
  remainder kept), and a duration with a nanosecond component that is not a multiple of a thousand, or a text's
  fraction finer than a microsecond, is of nanosecond precision itself: added to any temporal value it takes
  the nanosecond path;
- comparison and ordering need nothing: the database compares `TIMESTAMP_NS` exactly, and a nanosecond time of
  day against a microsecond one compares as the date-times at them on the epoch day.

A nanosecond component that is not a spelled number (one of the row) is refused by name: whether it is a multiple
of a thousand decides the path, and that is a fact of the spelling.

**The judge.** Neo4j's driver spells `Time('12:31:14.645876123')`; lite's nanosecond local time is
`DateTime('1970-01-01T12:31:14.645876123')`, judged equal when the clocks agree (`CypherValues.sameEpochLocalTime`,
`docs/SEMANTICS_REGISTER.md` S70); the other carriers judge as before (S65, S68). The executor already read a
`TIMESTAMP_NS` cell to its nine digits (the driver's `java.sql.Timestamp`); the harness classifies a dialect's
`DialectCapability` at render as a refusal by name, so the Postgres lane refuses what DuckDB answers (S71).

#### 12.12.2 The ledger

The lanes, for the first time, differ by design:

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| before (both) | 4,062 | 1,678 | 0 | 0 | 0 | 1,357 | 709 | 318 |
| DuckDB | 4,076 | 2,030 | 0 | 0 | 0 | 1,019 | 709 | 318 |
| Postgres | 4,076 | 1,682 | 0 | 0 | 0 | 1,367 | 709 | 318 |

Of the TCK's temporal features (1,004 cases): 902 PASS on DuckDB, 569 on Postgres. 133 lite cases in
`lite/temporal.yaml`, each judged by Neo4j; the `nanos_*` cases pass on DuckDB and are refused by name on Postgres
("a nanosecond epoch: Postgres keeps microseconds").

### 12.13 The label-less node leg, 2026-10-16: `MATCH (n)` over every class of the model

The ledger's "fans out" bucket (326 TCK cases) was two things on inspection: 238 patterns of one label-less node
(`MATCH (n)`, `MATCH (n {p: v})`) and 88 label-less patterns with relationships — not to-many hops at all, but
nodes with no label, which the rooted reading (§5.3) could not root. A label-less node is a node of any class of
the model, and the model's classes are known: the translator reads one **arm** per class (`Arms`, a state of
graphs that share the pattern's bindings, each with its own conditions: the property map and the `WHERE`
read against that class), and at the first projection (`WITH`, `RETURN`) projects every arm to the same columns
and concatenates them (Pure's `concatenate`); the grouping, `DISTINCT`, the sort, `SKIP` and `LIMIT` then read the
whole. A `WITH n` that carries the graph carries every arm. A property a class lacks reads as null where another
class declares it (Neo4j's heterogeneous nodes: `n.num` of a node without one is null), typed as the property is
there; a property declared on no class stays C4's refusal (the graph is the model). The arms' columns must agree
in type; a property typed differently across the classes is refused by name. Each arm's fresh names are the same
(the fresh counter rewinds per arm), so the arms' columns and the aggregates' argument columns coincide.

Refused by name: a node as a value (`RETURN n`: C8, the next wall), a `MATCH` after a label-less pattern, a
label-less pattern with relationships (the join form proper), `UNWIND` after a `MATCH`.

Beside it, one rule of the WITH's: after a grouping or a `DISTINCT`, the `WHERE` and the `ORDER BY` read the
projected columns and, Neo4j's rule, an expression that re-spells a key's expression reads the key (`WITH DISTINCT
a.name2 AS name WHERE a.name2 = 'B'`; `WITH a.name AS name, count(*) AS cnt ORDER BY a.name + 'C'`): the
`KeyedScope`, matching on the spelled text as Neo4j matches on the expression's shape (the TCK's WithWhere1 [2]
and WithOrderBy2 [23], which the arms reached first).

#### 12.13.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4094 | 2098 (from 2,030) | 0 | 0 | 0 | 969 | 709 | 318 |
| Postgres | 4094 | 1750 (from 1,682) | 0 | 0 | 0 | 1317 | 709 | 318 |

18 lite cases in `lite/labelless.yaml`, each judged by Neo4j.

### 12.14 The nodes-as-values leg, 2026-10-16: a node returned, a node carried through WITH

C8 (§5) — a node as a value — was the ledger's largest refusal after the arms (168 TCK cases, most of them
`MATCH (a) WITH a ORDER BY a.num LIMIT 4 RETURN a`). Two forms, neither of which makes Java compute a value:

- **A node returned** (or used as a value) is the JSON the judge reads as a node:
  `{"@labels":["A"],"num":9,"bool":true}` — the class's label (`Model.labelOf`) and each stored property of the
  class (`Model.properties`: its own and inherited; an association end is navigation, not a property) that holds a
  value, Neo4j storing no null. The database builds it as text (`'{"@labels":' + labels + if(isEmpty(num), '',
  ',"num":' + toString(num)) + ... + '}'`) and `fromJson` reads it as a Variant: an integer or boolean by its
  `toString`, a float with its `.0` when the database's text drops it (Postgres), a string in quotes with its
  quotes and backslashes escaped. The judge (`CypherValues.ofJson`) reads an object with `@labels` as a `Node`;
  the harness maps its class of the unlabelled nodes (`Unlabelled`, a convention of its own) to no label.
- **A node through a projecting WITH** (`WITH a ORDER BY a.num LIMIT 4`, `WITH a, a.x AS x`) is carried as its
  columns — the labels' JSON text in one, each property of its class in its own (`_cypher_k`, synthetic), the node
  itself a *virtual* column (`Col.virtual`: no column of the relation bears its name; `NodeShape` holds the
  mapping) — so the clauses after read `a.num` as the typed column it is, an `ORDER BY a.num` sorts that column
  (no hidden copy), a second `WITH a` carries the same columns on, and `RETURN a` builds the JSON from them. Over
  a label-less root the carried properties are the union over the candidate classes (null where a class lacks one,
  the same fresh names in every arm, so the arms' columns agree). `RETURN *` over a pattern expands to its
  variables.

Refused by name: `DISTINCT` over a node, `ORDER BY` a node, a node among the grouping keys (Neo4j tells nodes apart
and orders them by identity, which the JSON does not carry — the harness's `id` column is no property of the
model); a relationship as a value; a node holding a temporal property, as a value (the Variant spells it as
text); a node that may be absent.

#### 12.14.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4,108 | 2,244 (from 2,098) | 0 | 0 | 0 | 837 | 709 | 318 |
| Postgres | 4,108 | 1,896 (from 1,750) | 0 | 0 | 0 | 1,185 | 709 | 318 |

14 lite cases in `lite/nodes.yaml`, each judged by Neo4j.

### 12.15 The parameters leg, 2026-10-16: the harness binds a scenario's parameters; `labels(n)`, `properties(n)`

A TCK scenario's parameters (`And parameters are:`, 16 cases) are values in the TCK's grammar, whose spelling of a
list, map, string, number, boolean or null is Cypher's own. The island has no parameter channel (a `#Cypher{...}#`
island is text inside a Pure query), so the **harness** binds them: `CypherParity.Case.island` replaces each
`$name` in the query's text by its value's literal text before the island runs (`bindParameters`: a TCK
scenario's `name=value` lines, or a lite case's `params:` flow map in the value grammar, printed back by the
judge's printer); the oracle passes the same values to Neo4j as parameters. The translator still refuses a `$name`
by name: binding is the harness's, not the island's. What a bound parameter reaches is whatever the translator
reads of the literal (`expr[idx]` over a projected index stays refused: "indexing a list by a value that is not a
literal").

Beside it, two functions of a node now that a node is a value (§12.14): `labels(n)` is the node's labels, a list
(`fromJson` of the labels' text — the constant of the class, or the column carrying it); `properties(n)` is the
node's properties that hold a value, a map (the same pieces as the node's JSON, without the labels). The harness's
class of the unlabelled nodes reads as no label here too (`CypherGraph.ownLabels`: a `labels()` list of that one
name is empty — the harness refuses a graph that labels a node so, so the list is its own).

#### 12.15.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4,116 | 2,266 (from 2,244) | 0 | 0 | 0 | 823 | 709 | 318 |
| Postgres | 4,116 | 1,918 (from 1,896) | 0 | 0 | 0 | 1,171 | 709 | 318 |

8 lite cases in `lite/params.yaml`, each judged by Neo4j.

### 12.16 The join-form leg, 2026-10-16: a pattern that fans out reads one row per pair

The rooted reading (§5.3) needs a node from which every hop navigates to-one; a many-to-many relationship (the
harness's junction table) or a chain through a to-many end has none, and was refused as "a pattern that fans
out". The leg reads it: when no to-one root exists, **the first node is the root and each hop goes forward**, a
to-many hop binding a *fanning* node (one row per node reached). Three things make that honest:

- **Materialization at the end of the MATCH.** A fanning graph cannot stay a graph (a condition over
  `$a.T.x`, a `[*]`, is collection equality, not a per-row test), so the MATCH materializes it to rows at once:
  every node carried as its columns (§12.14's carry: the labels' text, each property navigated from the root),
  the rows multiplying through the navigated columns -- lite's `project` of a to-many navigation is one row per
  leaf (`UnionTargetLeanJoinTest`), and every column along one path shares its join. A root with no leaf is no
  match: the hop adds `isNotEmpty` to the root's filter. The clauses after see rows: `RETURN a, b` builds the
  nodes from the columns, `count(*)` counts pairs, `WITH a, b ORDER BY b.y LIMIT 2` carries the columns on.
- **Deferred conditions.** A far node's property map and the MATCH's WHERE are not root filters here: they are
  read over the materialized rows, where a far node's property is a typed column (`NodeConstraint`,
  `Graph.deferredWhere`; an unnamed far node gets a synthetic name so it is carried). A node reached by an
  optional hop is never absent (the hop's `isNotEmpty` is the graph's), so the carry no longer refuses it.
- **Label-less ends read from the type.** A label-less far node is the property's class (as before); a label-less
  start node is the one class of the model declaring the type as a property holding a class (the harness's
  model declares a type on one class pair); several or none, refused by name.

Refused by name: a relationship variable in a fanning pattern (a relationship as a value), a fanning hop to a
class without a property (the rows multiply only through a navigated column: lite's `project` of a navigation
mapped to a constant stays one row per root, measured), an untyped relationship (`-->`, `-[r]-`: the next stage,
one arm per association), an incoming untyped hop from a label-less node, a label-less single node among several
patterns (a cartesian product over every class), cycles and relationship uniqueness as before.

#### 12.16.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4,127 | 2,276 (from 2,266) | 0 | 0 | 0 | 824 | 709 | 318 |
| Postgres | 4,127 | 1,928 (from 1,918) | 0 | 0 | 0 | 1,172 | 709 | 318 |

11 lite cases in `lite/joins.yaml`, each judged by Neo4j.

### 12.17 The join form's second stage, 2026-10-16: an untyped relationship is one arm per relationship the model declares

`MATCH (a)-->(b)`, `(a)-[r]-(b)`, `(x)-->(y)-->(z)` (74 TCK cases refused as "a relationship pattern without a
type"): a relationship pattern with no type names every relationship the model declares between the classes it
could stand between. The translator **enumerates the arms** (`untypedArms`, `assign`): from the node before the
hop, read forward, each relationship of its class (`Model.ends`: the class's stored properties holding a class,
and the associations whose *first* end is its — the end a relationship type names, which the harness declares
first; the inverse end is the other class's); read backward, each relationship of another class reaching it; both
for an undirected hop; a typed hop on the way its own type's ways. Each arm is the pattern rewritten with the
chosen type and direction and the classes it decides as labels, read as a typed pattern is (a to-one root or
§12.16's fanning root), its WHERE read against its own classes, materialized to rows — every node carried as the
union of its classes' properties across the arms (`Graph.variableClasses`), the nodes in the pattern's order and
every unnamed node named synthetically before the arms, so the arms' columns agree — and the arms concatenated.
The seam indexes the associations by the class their first end is injected onto (`CypherIslands.Ends`; the
context gained `findAssociation`).

Refused by name: a relationship variable in a pattern read as rows (a relationship as a value), an undirected hop
over a self-association (a loop matches once in Neo4j, which needs identity), an untyped pattern after a bound
pattern or among several patterns, one the model declares no relationship for (no rows).

**A harness fix found by the leg (TriadicSelection1 [1], the binary tree).** The harness's model of a
self-association (a relationship type between nodes of one class, realized as a foreign key) mapped both ends to
one join written from the inverse end's side — and lite reads `{target}` as the node navigated to, so the forward
end navigated backward (`(c:X)<-[:FRIEND]-(b:X)` read b1's own friends). A self-association now gets one join
per end, each end mapped to its own (`CypherGraph`); the four cases that read a self-association's forward end
changed outcome, nothing else (measured by the per-case diff). The TCK's `binary-tree-1` graph is copied beside
lite's `graphs/` as a fixture for the diagnostic cases.

#### 12.17.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4,138 | 2,293 (from 2,276) | 0 | 0 | 0 | 818 | 709 | 318 |
| Postgres | 4,138 | 1,945 (from 1,928) | 0 | 0 | 0 | 1,166 | 709 | 318 |

22 lite cases in `lite/joins.yaml`, each judged by Neo4j.

### 12.18 The OPTIONAL MATCH leg, 2026-10-16: optional hops from a bound node, absent nodes

`OPTIONAL MATCH` (45 TCK cases refused by name) extends the graph in hand from a bound node with **optional
hops**: a hop adds no `isNotEmpty` to the root's filter, so no row is lost; a node the hop finds none of is null,
its properties null (`Binding.optionalNode`: the node *may be absent*, which a MATCH's optional to-one hop never
is, its `isNotEmpty` being the graph's). Three readings follow:

- **Presence.** A node's value, `labels(n)` and `properties(n)` are read where the node is present — the
  navigation finds one (`NodeParts.presence`: `if($a.T->isEmpty(), |null, |fromJson(...))`), or, once carried
  through a projection, its labels column holds a value (the carry writes the labels' text where the hop finds a
  node, null where none; `n IS NULL` reads that). A to-many optional hop fans out as §12.16's and the arms are
  read as rows at once: lite's outer join keeps one row for a root with no leaf, which is Neo4j's row of nulls
  (`count(b)` counts the leaves, 0 for none).
- **Absent nodes.** A type the model declares nowhere, or a label the hop does not reach, binds an *absent*
  node (`Binding.absentNode`): the optional pattern never matches, so the node is null everywhere — its value,
  properties, `labels()`, `properties()` — typed by the classes the other arms give the variable, so the arms'
  columns unite (`MATCH (n) OPTIONAL MATCH (n)-[:T]->(x)` over every class: `x` bound through `T` on the class
  declaring it, absent on the others).
- **The first clause.** `OPTIONAL MATCH (n) ...` as the first clause is a MATCH (the model has classes, so there
  are rows; an empty model stays "the model declares no class").

Refused by name: a WHERE on the OPTIONAL MATCH (the WHERE is the optional pattern's, not the rows'), a property
map on an optional node or on the bound node, an untyped relationship or a relationship variable in it, an
OPTIONAL MATCH after a WITH that projected its variables or after a pattern read as rows, of several patterns or
of a single node (a cartesian product), from a relationship variable.

#### 12.18.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4,150 | 2,305 (from 2,293) | 0 | 0 | 0 | 818 | 709 | 318 |
| Postgres | 4,150 | 1,957 (from 1,945) | 0 | 0 | 0 | 1,166 | 709 | 318 |

12 lite cases in `lite/optional.yaml`, each judged by Neo4j.

### 12.19 The relationships-as-values leg, 2026-10-16: `r` as its type, `type(r)`, optional relationships

A relationship binding now carries its **type** (the type the pattern names, or the arm's chosen one, §12.17). As
a value it is the JSON the judge reads as a relationship, `{"@type":"T"}` (`CypherValues.ofJson`: an object with
`@type` is a `Rel`) — the harness's model holds no relationship property (an association end is navigation), so
the value is its type alone; `type(r)` is the type's text. An OPTIONAL MATCH's relationship is present where its
hop finds a node (`r IS NULL`, `type(r)` null where none), an absent one null. Carried through a projection, a
relationship is its **type's column** (`RelShape`; the relationship's own column virtual, its value rebuilt from
the type): arms concatenated then carry each their own type in the one column, which is what `type(r)` after an
untyped pattern needs. `DISTINCT`, `ORDER BY` and a grouping over a relationship are refused by name as a node's
are (identity); `r.prop` stays refused ("a property of a relationship", an edge class).

**The harness's edge-property skip.** A graph whose relationships carry properties cannot answer a query that
names a relationship as a value (Neo4j shows the properties; the model has none), so the harness skips such a
case — a relationship holding a property, named by the query (`CypherParity.RELATIONSHIP_VARIABLE`: `-[r:T]->`,
`-[r]-`, `-[r {...}]->`): 50 TCK cases, none of them passing before (measured by the per-case diff). An edge
class would lift it.

#### 12.19.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4,160 | 2,328 (from 2,305) | 0 | 0 | 0 | 758 | 707 | 368 |
| Postgres | 4,160 | 1,980 (from 1,957) | 0 | 0 | 0 | 1,106 | 707 | 368 |

11 lite cases in `lite/rels.yaml`, each judged by Neo4j (one skipped by the harness, by design).

### 12.20 The UNION and empty-model leg, 2026-10-16

**UNION.** A statement with `UNION` clauses is split at them: each part is a query of its own (translated by a
translator of its own, so its fresh names are its own), the parts' rows concatenated by position (Cypher unites
by name and requires the same names in the same order: "All sub queries in an UNION must have the same return
column names"), `distinct` unless `UNION ALL`. The parser now keeps `UNION ALL` as its own keyword (it read the
`ALL` and dropped it, so every UNION was distinct and a mixed `UNION`/`UNION ALL` passed where Neo4j refuses
it). Refused by name: a UNION whose parts type a column differently, and `UNION` (distinct) over a node or
relationship value (identity; `UNION ALL` reads).

**The empty model.** Over a model with no class (the TCK's "empty graph" scenarios that still MATCH), a
label-less `MATCH (n)` is no row — `noRows` — with `n` an absent node (§12.18's: null everywhere), so
`count(*)` is 0 and `n.x` null; an `OPTIONAL MATCH (n)` as the first clause is one row with `n` absent (Neo4j's
null row). The MATCH's WHERE and property map are still read over that scope: Neo4j's static errors (an
aggregate in WHERE, an undefined variable) are the query's, not the data's, and the harness classes a lite
answer where Neo4j errs as a BUG. `RETURN *` over no variable is Neo4j's error too.

#### 12.20.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4,172 | 2,351 (from 2,328) | 0 | 0 | 0 | 746 | 708 | 368 |
| Postgres | 4,172 | 2,003 (from 1,980) | 0 | 0 | 0 | 1,094 | 708 | 368 |

6 lite cases in `lite/union.yaml` and 6 in `lite/empty.yaml`, each judged by Neo4j.

### 12.21 The temporal remainder's second leg, 2026-10-16: fractional durations, `toString(duration)`, a date inside a node

- **A fraction cascades down the units** (`duration({months: 0.75})`, `duration('P5M1.5D')`; 9 TCK cases): as
  Neo4j's does — a month's into days at 30.436875 days a month (Neo4j's average: `P22DT19H51M49.5S` for three
  quarters of a month, the TCK's example), a day's into seconds, a second's into nanoseconds — the whole parts
  summed into the slots as Float sums, the floors and the remainders the database's (`durationOfParts`; the
  all-whole case keeps its exact Integer path). The text form reads its numbers as written (a fraction on the
  seconds exactly, to the nanosecond; on another unit as a Float literal carried down the same way) and
  both forms end in the one builder.
- **`toString(duration)`** (12 TCK cases) is Neo4j's spelling, `P[nY][nM][nD][T[nH][nM][n[.f]S]]`, each unit of a
  non-zero value and `PT0S` for none: the years and months from the months slot (a quotient toward zero, Java's,
  so `-14` months is `P-1Y-2M`), the hours, minutes and seconds from the seconds and nanoseconds as one signed
  count of nanoseconds (so `-60.001` s is `PT-1M-0.001S`), the seconds' fraction trimmed of trailing zeros
  (`regexpReplace`). The database computes every quotient and spells every number. Two measurements shaped it:
  a nested quotient (`integerQuotient` of a quotient of a quotient) spelled the duration struct once per copy of
  its argument — tens of thousands of copies in one SQL, and the container killed the lane (exit 137) — so each
  quantity reads straight from the slots with a truncating division and a remainder keeping the dividend's
  sign; and the product of the seconds and a billion folds to 32 bits over a literal-built struct on DuckDB (the
  slot cast does not widen it), so the total goes through a Float constant and is cast back, exact to 2^53.
- **A date inside a node** (10 TCK cases, WithOrderBy1/2's node-with-date rows): the node's JSON spells it as the
  oracle spells one, `Date('1910-05-06')`, and the judge reads a JSON string of that grammar back as the opaque
  value a cell of that type is (`CypherValues.TEMPORAL_TEXT`), so a node holding a date compares as Neo4j's does.

#### 12.21.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4,179 | 2,377 (from 2,351) | 0 | 0 | 0 | 726 | 708 | 368 |
| Postgres | 4,179 | 2,029 (from 2,003) | 0 | 0 | 0 | 1,074 | 708 | 368 |

6 new lite cases in `lite/temporal.yaml` (and two refusal cases turned into passing ones), each judged by Neo4j.

### 12.22 `toString` of date-times and times, 2026-10-16

`toString` of a local date-time, a zoned date-time, a local time and a zoned time (Temporal6, 6 TCK cases with the
lite ones) is Neo4j's spelling, which is Java's: `yyyy-MM-ddTHH:mm:ss[.f]` with the offset (`+01:00`, `Z`) after
it for a zoned one, `HH:mm:ss[.f]` for a time; the fraction of the second trimmed of its trailing zeros (Neo4j's, as
a duration's: ten microseconds is `.00001`, measured against the oracle), none when zero. Every field is the
database's, read off the wall time as the fields are (§12.8, §12.11: a time's off the date-time at it on the epoch
day, a zoned value's off its wall time), zero-padded by text arithmetic (the digits of 100 plus the number, after
the 1; the year's of 10000 plus it); a named zone (its offset at the instant and its name) and a year outside
0..9999 are refused by name.

The TCK's round trips (`localtime(toString(d)) = d` over a nanosecond value) showed a text of the row parsed at
the microsecond: the database's parse. The text `toString` spells for a nanosecond value is marked as one
(`withNanos` on the String), and a parse of a text so marked reads the digits past the microsecond — the first
three of them, zero-padded, `parseInteger`'d and stepped on as nanoseconds (`parsedWithNanos`) — so the round
trip holds on DuckDB, and no other text's parse changes (a stored or built text keeps the database's parse).

#### 12.22.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4,183 | 2,385 (from 2,377) | 0 | 0 | 0 | 722 | 708 | 368 |
| Postgres | 4,183 | 2,035 (from 2,029) | 0 | 0 | 0 | 1,072 | 708 | 368 |

4 new lite cases in `lite/temporal.yaml` (and three refusal cases turned into passing ones), each judged by Neo4j.

### 12.23 Odds and ends, 2026-10-16: the duration round trip, numbers from texts, UNWIND after a MATCH, a list indexed by the row

Four small refusals with a clear reading, 25 TCK cases between them:

- **`duration(toString(d))` reads `d`.** The text `toString` spells for a duration carries the duration it spells
  (`DurationText`), and `duration(...)` of such a text is that duration: print then parse is the identity, and
  the translator can see it. A duration text of the row from anywhere else stays refused by name (its parse would
  be the database's reading of seven units, a later leg).
- **`toInteger` and `toFloat` of a text** are the database's parse behind a shape guard (`matches` against the
  integer's or the float's spelling): an integer's digits `parseInteger`'d, a float's spelling `parseFloat`'d
  (and truncated toward zero for `toInteger`), any other text null, which is Neo4j's. One dialect quirk surfaced
  on Postgres: it reads a bare text literal under a cast as a constant of the cast's type when it PARSES the
  statement, before any `CASE` around it is weighed, so `toInteger('foo')` was the parse's error there, not null
  (DuckDB evaluates the guard first). The Postgres dialect now spells a text literal under a cast as a typed text
  first (`CAST(CAST('foo' AS VARCHAR) AS BIGINT)`; `parseInteger`, `parseFloat`, `parseDate` and every other
  cast of a literal to a non-text type): a typed text's cast is the planner's, which drops a `CASE` arm whose
  test is false before it folds the arm (measured on Postgres 16). Pinned by
  `PostgresDialectTest.aTextLiteralUnderACastIsSpelledAsATextFirst`; PCT on Postgres and the Postgres parity
  lanes unchanged.
- **UNWIND after a MATCH** reads the graph as rows first (§12.16's materialization, every node carried), then
  the lateral over the rows as after a WITH.
- **A list indexed by a value of the row** (`l[i]`, `i` a column) is the element at that place, the database
  choosing among the elements' scalars (one kind) through an `if` chain, a negative place counted from the end,
  none beyond null; strict over the place (a null place is null).

#### 12.23.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4189 | 2407 (from 2,385) | 0 | 0 | 0 | 706 | 708 | 368 |
| Postgres | 4189 | 2057 (from 2,035) | 0 | 0 | 0 | 1056 | 708 | 368 |

6 lite cases in `lite/odds.yaml`, each judged by Neo4j.

### 12.24 Aggregates in expressions, 2026-10-16: an expression around aggregates, an ORDER BY aggregate, a null column aggregated, and three small readings

- **An item that is an expression around aggregates** (`count(*) * 10`, `count(n) / 60 / 60`,
  `$age + avg(person.age) - 1000`, `count(a) > 0`) is Cypher's implicit grouping with the aggregates computed
  first: each aggregate inside is a hidden output of the grouping (by its spelling, so one spelled twice is
  computed once), finished as an item's aggregate is (a sum of nothing 0), and the expression is `extend`ed over
  the finished aggregates and the keys after the grouping, under the item's name; the items alone are selected.
  Its other parts are constants and parameters: a value of the row among them (`p.age + count(*)`) is Neo4j's
  implicit grouping key, an error.
- **An ORDER BY over aggregates after a grouping** re-spells the items' aggregates (`ORDER BY $age + avg(x) - 1000`
  with `avg(x) AS avgAge` an item) through the keyed scope, as it re-spells a key's expression; an aggregate it
  asks for beyond the items (`WITH a.num2 % 3 AS mod, min(sum) AS min ORDER BY sum(sum)`) is a hidden aggregate
  of the grouping over the scope before the projection, kept beside the items until the sort and dropped after
  it. Neo4j 2026.09 answers these (measured by the oracle: the sort's aggregate of a variable the WITH dropped is
  computed, not refused); the TCK's expected `UndefinedVariable` (WithOrderBy4 [13], [14]) is older, and the
  oracle's answer is the judge.
- **A sum or an average of a null column** (a property no node of an empty graph holds, which the empty model
  reads as null, Cypher's `avg(person.age)` over `MATCH (person)` of nothing): no value to add, so each row
  contributes 0 to the sum (0 over no row too, the finish) and the average is null, the finish a typed null and
  its column a null the query named (read as null wherever it is read: a read through `toOne` of a provably
  empty value is the lowering's raise, which Postgres types as text); the grouping still collapses the rows as
  Cypher's does. Not an aggregate over a typed null: the lowering folds
  `cast([], @Integer)` to a bare `NULL`, and Postgres cannot type `SUM(NULL)` ("function sum(unknown) is not
  unique"), measured on the lane. The aggregate's finish is now one operator over the raw result's read (a sum's
  0 of nothing, an average's null), where it was a flag.
- **`labels(null)`, `type(null)` and `properties(null)`** are null; `properties(map)` is the map.
- **A text and a number do not order**: `'1' < 1` is null (Neo4j's), as a boolean and another value already were;
  their equality stays false.
- **The harness, one line:** Neo4j names an unaliased item by its text, a parameter as written
  (`$age + avg(person.age) - 1000`); the lane binds the parameters into the query's text (§12.15), so the island
  names it by the bound text. The judge reads Neo4j's column names through the same binding.

#### 12.24.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4202 | 2435 (from 2,407) | 0 | 0 | 0 | 691 | 708 | 368 |
| Postgres | 4202 | 2085 (from 2,057) | 0 | 0 | 0 | 1041 | 708 | 368 |

13 lite cases in `lite/aggregates.yaml`, each judged by Neo4j.

### 12.25 Lists and maps of values of the row, 2026-10-16: the shape's JSON text built by the database

§12.5.1 built a list holding values of the row by {@code toVariant} of a Pure collection, within what a collection
carries: one kind, no null, two or more elements (Pure reads `[x]` as `x`); a map holding values of the row was
refused by name (`newMap`'s Variant is a list of pairs on Postgres). Now a shape holding values of the row is its
**JSON text built by the database**, as a node is (§12.14): each scalar spelled by its kind (`jsonText`: a number's
or a boolean's spelling, a whole float with its `.0`, a string in quotes escaped, a date as the oracle spells one),
an empty one `null`, a nested list's or map's text within its brackets, a Variant (a node, a relationship, a list
or map the database holds) by its canonical text (`toString` of a Variant is its compact JSON), a collection the
database computes by its Variant's text; the whole read as a Variant (`fromJson`). A shape of literals stays a JSON
constant, and a collection the database computes stays `toVariant` of it. So `[p.age]`, `[p.age, p.name]` with a
null, `[p.age, p.name, p.age > 1, 2.5]`, `{age: p.age, name: p.name}`, `[{age: p.age}, [p.name, 1]]`,
`[n IN things | toInteger(n)]` with a null, `[p, p.age]`, `[range(1, p.age), p.age]` and `[1] + (2 IN [3]) + 4`
are values. A time, a date-time or a duration inside a shape is still refused by name (the Variant would spell
it as text the judge does not read yet); a shape carried through a WITH (not returned) still is.

One register row retired, S62: on Postgres a list of floats of the row was `toVariant` of a Pure collection, a jsonb
value that spells a whole float as an integer (`[3.4, 3]` where Neo4j has `[3.4, 3.0]`, TypeConversion3 [1], decided
on that lane). The text route spells the float with its `.0` on both lanes (`jsonText`), so the difference is gone:
the row and its decision (`cypher-parity/decisions.tsv`) are removed, the case judged by Neo4j's answer on both lanes.

#### 12.25.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4213 | 2456 (from 2,435) | 0 | 0 | 0 | 681 | 708 | 368 |
| Postgres | 4213 | 2106 (from 2,085) | 0 | 0 | 0 | 1031 | 708 | 368 |

11 lite cases in `lite/shapes.yaml`, each judged by Neo4j; `lite/values.yaml`'s map-over-rows refusal case is a
passing case now (renamed `returned_map_over_rows`), and its two list cases with a null and with strings of the row
pass.

### 12.26 `keys()` of a node or relationship, and a returned node's property as a sort key, 2026-10-16

- **`keys(n)` of a pattern node** is the names of its properties that hold a value (Neo4j stores no null), a
  collection the database builds: the names' JSON array text (each as `,"name"` where its value is present, the
  first comma dropped, as `properties(n)` builds its text), read back as the strings it holds (`toMany(@String)`),
  so it is UNWIND-able (a lateral), IN-able and sized as any collection the database computes. Not a Pure collection
  literal of optional names (the Typer takes no `[0..1]` element) and not a concatenation of them (its lowering
  spells an untyped empty list on Postgres: "a list literal of Unknown[]"). `keys(r)` of a relationship holds none
  (the model holds no relationship property); of a node an OPTIONAL MATCH never finds, null; of a node or
  relationship it may not find, refused by name (a collection is never null).
- **`RETURN n ORDER BY n.name SKIP 2`**: the projected scope reads a property of an item that is the pattern's own
  node (its value a node, its name the pattern variable's) from the pattern, where it read the returned Variant and
  refused ("a property of a value the database holds as a Variant"); with a SKIP or LIMIT the sort key is a hidden
  column (without one the lowering's limit of §12.5 holds).

#### 12.26.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4221 | 2476 (from 2,456) | 0 | 0 | 0 | 669 | 708 | 368 |
| Postgres | 4221 | 2126 (from 2,106) | 0 | 0 | 0 | 1019 | 708 | 368 |

8 lite cases in `lite/keys.yaml`, each judged by Neo4j.

### 12.27 Named paths, 2026-10-16: `p = (a)-[:T]->(b)` as a value, `length`, `nodes`, `relationships`

A named path over a fixed-length pattern is a value: its nodes and its relationships in the pattern's written order,
each as the value its variable is (§12.14, §12.19), and each relationship's **stored direction** relative to that
order (forward when the node before it holds the node after: the hop read the property's own end rather than its
inverse; a pattern read backward from its root flips it). The JSON the judge reads, built as text by the database:
`{"@path":[node, rel, node, ...], "@dirs":[true, ...]}`; the judge (`CypherValues.Path`, structured now: nodes,
relationships, directions) reads Neo4j's `<(:A)-[:T]->(:B)>` into the same shape, an arrow's direction from its
spelling, and compares them structurally (`text()` rebuilds Neo4j's spelling for a report). So a path written
against the stored direction (`p = (b:B)<-[:T]-(a:A)`) and an undirected one (`(b:B)-[:T]-(a:A)`) print as Neo4j
prints them, `<(:B)<-[:T]-(:A)>`.

- Every node and relationship of a named path is bound (an unnamed one under a synthetic name, as a label-less
  pattern's are), the path recorded on the graph (`Graph.paths`) under its name; a relationship's direction on
  the graph (`Graph.forward`) as the hop resolves it. Over a typed pattern, an untyped one (each arm records the
  path over its own typed rewrite), and a label-less node alone (`p = (n)`: a zero-length path per arm).
- `length(p)` is spelled from the pattern (its hops); `nodes(p)` and `relationships(p)` are lists of the values
  (§12.25's text route; `[]` for a zero-length path's relationships).
- Through a WITH the path passes under its name with the nodes and relationships it reads (a graph carried) or, when
  the graph is read as rows (a fanning pattern, the arms), as its JSON column; `RETURN *` lists it (and no
  synthetic name). After that it is a Variant: `length`, `nodes` and `relationships` of it are refused by name.
- Refused by name: a path over an OPTIONAL MATCH or a node it may not find, a variable-length pattern, a path over a
  model with no class, path equality and ordering (`p1 = p2`: Neo4j compares by identity, direction aside).

#### 12.27.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4233 | 2499 (from 2,476) | 0 | 0 | 0 | 658 | 708 | 368 |
| Postgres | 4233 | 2149 (from 2,126) | 0 | 0 | 0 | 1008 | 708 | 368 |

12 lite cases in `lite/paths.yaml`, each judged by Neo4j.

### 12.28 The cartesian product, 2026-10-16: `MATCH (a:A), (b:B)` as every pair of the two patterns' rows

A pattern sharing no node or relationship variable with the graph so far (the second pattern of a MATCH, or a MATCH
after a MATCH) was refused as a cartesian product. Now it is one: the graph so far is read as rows (§12.16's
materialization: every node carried as its columns, every relationship and path as its value), the pattern is read
into its own graph (its own root, as a first pattern is) and as rows too, and every pair of rows is joined --
Pure's relation `join`, `JoinKind.INNER`, a condition that always holds (`{l, r | true}`; the lowering writes
`JOIN ... ON TRUE`). The MATCH's WHERE is a filter over the pairs; the clauses after read the rows, so a
pattern after them is still refused by name (§12.16's "a MATCH after a pattern read as rows"), as is a third
pattern in the same MATCH after the product. The two sides' columns are distinct by construction (the variables
by the condition, the synthetic names by one counter). An item that is a list or map around aggregates
(`{name: count(b)}`, §12.24) is the Variant it is as a value.

#### 12.28.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4240 | 2509 (from 2,499) | 0 | 0 | 0 | 655 | 708 | 368 |
| Postgres | 4240 | 2159 (from 2,149) | 0 | 0 | 0 | 1005 | 708 | 368 |

7 lite cases in `lite/cartesian.yaml`, each judged by Neo4j.

### 12.29 `OPTIONAL MATCH ... WHERE`, 2026-10-16: the WHERE as the optional pattern's guard

An OPTIONAL MATCH's WHERE is the optional pattern's, not the rows': `OPTIONAL MATCH (x)-[:E1]->(y:Y) WHERE x.val
< y.val` finds `y` only where the condition holds, and `x` keeps its row with `y` null otherwise (§12.18's
semantics with one more reason to be absent). The reading: the WHERE is a **guard** on what the pattern bound.
After the optional hops are bound onto each arm's graph, the condition is read over the graph (the pattern's nodes
null where their hops find none, so a comparison with one is null, so false) and every binding the OPTIONAL MATCH
made (its nodes and relationships, not the bound start) carries it (`Binding.guard`). A binding is **absent** where
its navigation finds nothing or its guard does not hold (`GraphScope.absent`), and every read of an OPTIONAL
MATCH's binding tests that one absence: a node's value and its parts (`NodeParts.absent`, a boolean now, where
it was the navigation to test), a relationship's value and its type, a property (null where absent), the carried
columns (the labels column and each property), `keys`. The guard is read over the graph's own row, so it travels
with the bindings through a WITH that carries the graph.

Over to-one hops; a hop that fans out under a WHERE is refused by name (the pairs would need a left join to keep
the start node's row). The TCK's MatchWhere6 [6], [7] (two hops) and [8] (the WHERE on a second OPTIONAL MATCH
from an optional node) pass; [4] (untyped, label-less) and [5] (a relationship bound twice) stay refused.

#### 12.29.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4246 | 2518 (from 2,509) | 0 | 0 | 0 | 652 | 708 | 368 |
| Postgres | 4246 | 2168 (from 2,159) | 0 | 0 | 0 | 1002 | 708 | 368 |

6 lite cases in `lite/optional_where.yaml`, each judged by Neo4j; `lite/optional.yaml`'s refusal case
`optional_with_where_refused_by_name` is `optional_with_where`, a passing case.

### 12.30 Two small buckets, 2026-10-16: the percentile aggregates, a map indexed by a value of the row

- **`percentileDisc(x, p)` and `percentileCont(x, p)`** are Pure's `percentile` aggregate over the group's values
  (ascending; discrete, a value of the group typed as it is, or continuous, a Float the database interpolates:
  the lowering's `QUANTILE_DISC`/`QUANTILE_CONT`), `p` a spelled number (another is refused by name); null over no
  value, as Neo4j's. `percentileCont` over values that are not floats is refused by name: Neo4j 2026.09 answers
  the element itself, an integer, where the percentile lands on one (`[1, 2, 4]` at 0.5 is `2`) and a float
  where it interpolates (at 0.75, `3.0`), a type the data decides, which no column has (measured by the oracle).
- **A known map indexed by a text of the row** (`{a: 1, b: 2}[k]`, `$expr[$idx]` through a WITH) is the
  database's choice among the entries' scalars (one kind) through an `if` chain, null for a key the map lacks,
  strict over the key; as a list indexed by a value of the row is (§12.23).

#### 12.30.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4253 | 2532 (from 2,518) | 0 | 0 | 0 | 645 | 708 | 368 |
| Postgres | 4253 | 2182 (from 2,168) | 0 | 0 | 0 | 995 | 708 | 368 |

7 lite cases in `lite/small.yaml`, each judged by Neo4j (one a refusal by name).

### 12.31 Pattern comprehensions, 2026-10-16: one hop from a bound node as a navigation

- **`[(a)-[:T]->(b) WHERE p | e]`** is a navigation from the bound node `a` of the graph in hand: each way the
  model reads the hop (the typed association end; every end of `a`'s class for an untyped `-->`; the inverses for
  `<--`; both for `--`; the ways a label on the end node admits) is one navigation of `a`'s path, the expression
  read in a scope where the hop's node is the navigated instance (its properties, a node value, `labels`,
  `keys`), the relationship its type (`type(r)`, `r` as a value), and a named path (`[p = (a)-->() | p]`) the
  path the hop makes (§12.27's JSON, the direction stored; `length`, `nodes`, `relationships` of it). A way over
  an end holding many is a `filter` (the WHERE) and a `map` (the expression) over the navigation; a way over an
  end holding one (the harness types a relationship used once as `[0..1]`) reads its instance through the path,
  as a MATCH reads a to-one hop, under a presence guard (the navigation not empty, the WHERE holding). The list
  is a collection the database computes when the expression is a scalar that is never null and the hop has one
  way holding many (sized, UNWIND-able, `IN`-able); otherwise its JSON text (§12.25's shape: each value's text
  led by a comma, joined, the first comma cut): the ways' texts concatenated. No way (a class with no such end)
  is the empty list.
- **`size([(a)-[:T]->() | e])`** is the hop's matches counted by the database: each way's navigation `size`d (the
  WHERE a filter; a to-one way one or none), the sizes summed; the expression is not read. `count([...])` counts
  rows, as Neo4j does.
- **The refusals by name:** a comprehension over rows (after a WITH that carried the node, or a pattern read as
  rows); a pattern of more than one hop; two bound ends (identity); a start node an OPTIONAL MATCH may not find;
  a property map on a node or relationship of the pattern; a variable-length relationship; an undirected hop over
  a self-association (§12.17's loop); over an end holding many, an expression that reads a node outside the
  pattern (`a.name + b.name`, or the path, whose start is `a`): the lowering maps the navigated instances alone
  (`CorrelatedSubselects`: an outer-correlated mapper body is a separate demand feed, not built), so the text
  would read the outer row inside the map.

#### 12.31.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4274 | 2555 (from 2,532) | 0 | 0 | 0 | 643 | 708 | 368 |
| Postgres | 4274 | 2205 (from 2,182) | 0 | 0 | 0 | 993 | 708 | 368 |

21 lite cases in `lite/patterns.yaml`, each judged by Neo4j (five refusals by name).

### 12.32 An untyped relationship from a bound node, 2026-10-16: the continuation of a graph

- **`MATCH (a:A) WITH a MATCH (a)-->(b)`, `MATCH (a:A) MATCH (x)-[r]-(a)`:** an untyped relationship pattern from a
  node the graph in hand binds (a WITH of bare variables keeps the graph, §12.5; a MATCH before it) is one arm per
  way the model reads the hop from that node's class, as §12.17 decides a first pattern's arms (every end leaving
  the class forward, every end of another class reaching it backward, both undirected, a label on the far node
  picking among them, the next hops on from the class each way reaches): each arm the graph extended by the typed
  pattern (from the bound end: the pattern reversed when its last node is the bound one), the WHERE read against
  the arm, the arms materialized to rows (every node as the union of its classes' columns, a relationship as its
  type) and concatenated. Over a label-less pattern's arms (§12.13), each arm's ways. A class no way leaves reads
  no row; no way from any arm is refused by name (no rows).
- **Refused by name:** two ends bound (identity), a pattern through a bound node, a pattern sharing no variable
  with the graph (a cartesian product over the ways), a start an OPTIONAL MATCH may not find, a named path over
  the hop, a MATCH of several patterns with an untyped one, after rows (a WITH that projected).

#### 12.32.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4287 | 2566 (from 2,555) | 0 | 0 | 0 | 645 | 708 | 368 |
| Postgres | 4287 | 2216 (from 2,205) | 0 | 0 | 0 | 995 | 708 | 368 |

13 lite cases in `lite/continue.yaml`, each judged by Neo4j (three refusals by name).

### 12.33 Products with a label-less pattern, `WITH *`, a comprehension's null condition, 2026-10-16

- **`MATCH (n), (m)`, `MATCH (a:A), (n)`, `MATCH (a) WITH a MATCH (b)`, `WITH * MATCH (n)`:** a pattern sharing no
  variable with the graph in hand is a product of the two patterns' rows (§12.28's join), either side a label-less
  pattern: its arms' rows (§12.13's union, every node as the union of its classes' columns). `MATCH (n), ...` reads
  the label-less node's arms first, the next pattern the product over them; a third pattern, or a pattern sharing a
  variable with a label-less one through a typed hop, stays refused by name (an untyped hop continues, §12.32).
  The WHERE of the MATCH is read over the joined rows.
- **`WITH *`** over a graph keeps it (every variable and path under its name), as a WITH of bare variables does
  (§12.5), so the MATCH after it continues the graph.
- **`[x IN list WHERE condition]`** keeps an element when the condition is true: null, like false, drops it (Neo4j's
  rule), the same filter a WHERE clause is (§12.3's truth); the refusal of a condition that may be null is lifted.
  The quantifiers (`all`, `any`, `none`, `single`) keep theirs over a list the database computes: a null condition
  takes part in their three-valued answer.

#### 12.33.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4301 | 2585 (from 2,566) | 0 | 0 | 0 | 640 | 708 | 368 |
| Postgres | 4301 | 2235 (from 2,216) | 0 | 0 | 0 | 990 | 708 | 368 |

10 lite cases in `lite/products.yaml` (one a refusal by name) and 4 in `lite/comprehensions.yaml`, each judged by
Neo4j.

### 12.34 Lists ordered element by element, 2026-10-16

- **`[1, 2] < [3, 4]`, `[1, null] >= [1]`, `[a.x, 1] < [2, 1]`:** two lists of known shape order as Cypher orders
  them: element by element, the first pair that is not equal deciding (the pair's own ordering, §12.3's; null when
  either of the pair is null, or the two do not order: a text and a number), the lengths deciding when every pair is
  equal (the longer list the greater). The decision is the database's at each pair (the pair's equality and its
  ordering, three-valued, nested from the last pair back: `if equal then the pairs after else this pair's order`),
  the lengths a shape compared here. A list and a value of another type (a number, a boolean, a map) do not order:
  null, as Neo4j answers. Lists holding lists or maps, maps against maps, and a list the database computes stay
  refused by name.

#### 12.34.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4307 | 2599 (from 2,585) | 0 | 0 | 0 | 632 | 708 | 368 |
| Postgres | 4307 | 2249 (from 2,235) | 0 | 0 | 0 | 982 | 708 | 368 |

6 lite cases in `lite/ordering.yaml`, each judged by Neo4j (one a refusal by name).

### 12.35 Label predicates, relationships of several types, node comparisons named, 2026-10-16

- **`n:Label`, `n:A:B` as an expression** (a WHERE, a returned item): true when the node carries every label, read
  from its labels' JSON text (a spelled constant for a node of a known class, which the database folds; the column
  carrying it for a node read as rows: the database tests the text for each quoted label); null for a node an
  OPTIONAL MATCH may not find, and for a null value; Neo4j's type error for a value that is no node.
- **`(a)-[:KNOWS|HATES]->(b)`, `(a)-[:T|:T]->(b)`:** a relationship pattern of several types is one arm per distinct
  type (a type spelled twice is one), as an untyped pattern's arms are decided (§12.17, from a label-less or a
  labelled node, after a bound node too, §12.32), each arm read as a typed pattern, materialized and concatenated.
- **`a = b` over nodes**, relationships or paths is refused by name as what it is: Neo4j compares them by identity,
  which their values do not carry (the refusal read "comparing a Map with a Map" before).

#### 12.35.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4318 | 2610 (from 2,599) | 0 | 0 | 0 | 631 | 709 | 368 |
| Postgres | 4318 | 2260 (from 2,249) | 0 | 0 | 0 | 981 | 709 | 368 |

11 lite cases in `lite/labels.yaml`, each judged by Neo4j (one a refusal by name, one Neo4j's own error).

### 12.36 A hop from an absent node, a pattern no way of the model reads, 2026-10-16

- **`OPTIONAL MATCH (a) WITH a MATCH (a)-->(b)`, `WITH null AS a OPTIONAL MATCH p = (a)-[r]->()`:** a hop from a node
  the rows in hand carry as null everywhere (a node an OPTIONAL MATCH never found, carried through a WITH; a null the
  WITH named) finds nothing. A MATCH reads no row: the rows in hand limited to none (`->limit(0)`); an OPTIONAL MATCH
  keeps every row, the pattern's other variables absent -- each node an absent node (one null labels column serves
  them all), each relationship an absent relationship (a null type column: `type(r)` null, `properties(r)` null),
  the path, when named, null (`nodes(p)`, `relationships(p)`, `length(p)` null, as of a null). The pattern is read
  from whichever end is the absent node. The property maps and the WHERE are still read, for their static errors
  (a variable not defined); the rows they would filter are none. The graph a WITH kept (`WITH a` alone) answers the
  same, materialized first.
- **`MATCH ()-[r]->()` over a model declaring no relationship, `MATCH (b:B) WITH b MATCH (b)-->(x)` from a class no
  way leaves:** no row, as Neo4j answers, every variable of the pattern absent (the bound node's rows limited to
  none; a first pattern over no row at all). `count(r)`, `count(*)` over it are 0; `nodes(p)` of its path null.
- **`properties(r)` of a relationship:** the empty map (the harness's model holds no relationship property); of one
  an OPTIONAL MATCH never found, null.
- A variable-length hop stays refused by name before any of this: a zero-length hop (`[*0..]`) is a row in Neo4j,
  not "no way". A pattern whose far end is bound already stays the identity refusal.

#### 12.36.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4334 | 2636 (from 2,612) | 0 | 0 | 0 | 621 | 709 | 368 |
| Postgres | 4334 | 2286 (from 2,262) | 0 | 0 | 0 | 971 | 709 | 368 |

14 lite cases in `lite/absent.yaml`, each judged by Neo4j (one a refusal by name: a cycle from an absent node).

### 12.37 A stored list, a parenthesized label predicate, a first-clause OPTIONAL MATCH over no way, 2026-10-16

- **`CREATE ({numbers: [1, 2, 3]})`, `MATCH (n) RETURN n.numbers, n`:** a list property of the graph (Neo4j's: a list
  of primitives of one kind, no null) is a Variant over a SEMISTRUCTURED column -- the harness declares the class's
  property `meta::pure::metamodel::variant::Variant[0..1]`, the store column `SEMISTRUCTURED` (seeded as JSON: DuckDB's
  JSON, Postgres's JSONB), the island's model adapter reports it as a value (not an end to a class), and the
  translator reads it as a JSON kind: returned as the list; in a node's value and in a map, its canonical text in
  place; under DISTINCT and a grouping, the database's JSON equality. `ORDER BY` over it is refused by name (Neo4j
  orders a list by its elements, which the Variant's text does not); `+` of two stored lists, `size`, `IN` and the
  list functions over it stay what they were for a Variant (refused by name).
  One exemption in the compiler: the resolver's rewrite refused any class-typed property read whole as "graph
  output" (`resolver/Substitution.java`, Phase H4); a Variant-typed one is the column's value, not an instance, and
  reads whole as its JSON.
- **`RETURN (n:Foo)`:** a label predicate in parentheses is the predicate, not a pattern: the parser reads
  `(n:A:B ...)` as a pattern only when a property map or a relationship follows the labels.
- **`OPTIONAL MATCH (a:A)-[r]->(x)` as the first clause, no way leaving `A`; `OPTIONAL MATCH ()-[r]->()` over a model
  declaring no relationship:** the root's rows (one per node of its class, or of every class), the rest of the
  pattern absent; over no class at all, one null row (Neo4j's). A relationship's property reads null (the model holds
  none; of a relationship never found, null too).

#### 12.37.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4343 | 2648 (from 2,636) | 0 | 0 | 0 | 638 | 709 | 348 |
| Postgres | 4343 | 2298 (from 2,286) | 0 | 0 | 0 | 988 | 709 | 348 |

6 lite cases in `lite/stored.yaml`, 2 more in `lite/absent.yaml`, 1 in `lite/labels.yaml`, each judged by Neo4j.

#### 12.37.2 The review of the post-merge diff, 2026-10-16

A fresh-eyes review of the absent-hop and stored-list legs and the resolver's condition (five findings, each
verified in the code): a stored list reached the sort through a carried node's property under SKIP or LIMIT and
through a key computed after a grouping (both refuse by name now); the no-row paths read a node's property map but
not a relationship's (both read now, for their static errors); the collision refusal from an absent node named every
collision a cycle (it names the variable); `[(n:Foo)]` parsed as a pattern comprehension (the parenthesized
predicate's lookahead, in a list too). The review found the indexing, the fresh-column naming, the flag threading,
the resolver's condition (a pure relaxation; `isVariant` admits the `meta::json` family too, consistently) and the
harness's JSON spelling correct.

### 12.38 The edge class: a relationship type with properties, 2026-10-16 (PR3, step 1; the design is §5.6)

- **The model.** The library project `projects/graph` declares the one profile, `graph::graph`
  (`stereotypes: [Edge, from, to, type]`, no tag), as `core_types::CtGovernance` is declared for every project that
  depends on it; `PlatformTypes.GRAPH_PROFILE` names it, as `EQUALITY_PROFILE` names the equality profile. A class
  carrying `<<graph.Edge>>` (itself or a superclass) is a relationship type with properties; its two ends are the
  properties carrying `<<graph.from>>` and `<<graph.to>>`, each `[1]` and typed by a class, declared on the class, inherited,
  or injected by an association (whose other end is the node class's navigation to the edge); its type is the
  enumeration value the derived property carrying `<<graph.type>>` spells, else the class's simple name; every other
  stored property is the relationship's. `compiler/element/GraphEdges` reads it in phase F (`ModelIntegrity`), as
  `EqualityKeys` reads `<<equality.Key>>`, with an error under the class's name for a misdeclared edge: an end
  stereotype without the marker, the marker without both ends, an end that is not `[1]` or not a class, two ends of one
  kind, a type whose body is not one spelled enumeration value, a to-many property holding a class, the profile not
  declared or not of that shape. Two small changes outside the island (announced in `IN_FLIGHT`): the association
  end's stereotypes, which the parser read and the model record dropped, now ride on
  `AssociationDefinition.AssociationEndDefinition`; and `NameResolver.resolveDerivedProperty` kept a rebuilt derived
  property's annotations (it rebuilt with the five-argument constructor, so a `<<graph.type>>` whose body named an
  enumeration lost its marker on resolution).
- **The island.** The model adapter reports the edge classes (`CypherToPure.Model.edges()`), leaves them out of the
  classes a label-less pattern reads, and reports no end holding an edge class (an edge is reached by its type, never
  by an end's name). A relationship type naming an edge class resolves, from a node of its from class, to a hop of two
  steps: the node's inverse of the from end (to-many: the graph fans out, one row per relationship) to the edge
  instance, then the edge's to end to the node after; incoming, the inverse of the to end then the from end; undirected,
  whichever the node's class allows. The far node's label picks among the edge classes of one type (a type joining
  several class pairs, §5.6's union) and must otherwise be the far end's class or a superclass. The relationship is
  bound to the edge instance with the edge class as its class: `r.prop` navigates there as a node's property does,
  `r` as a value is `{"@type": T, props}` built as a node's JSON is, `properties(r)` and `keys(r)` read the edge's
  properties, a property map on the relationship pattern filters the edge instance, a relationship carried through
  `WITH` carries its type's column and its properties' columns, and a path's relationship prints its properties. An
  untyped hop reads the edge ways beside the association ends; a pattern comprehension's hop over an edge is rooted
  at the edge instance. Refused by name: an edge class as a label ("a relationship type, not a node label"); the
  compact form read from the node side (an end that is the edge's own property has no inverse for the node to
  navigate; the association form gives it one); an undirected pattern over a self-typed edge (a loop); a type naming
  both an edge class and a property; an end's name used as a type; a hop from a node reached through an edge class
  (the resolver's nested navigation reads no association end past the edge's end, `$a.ACTED_IN_out.to.MADE_BY`:
  a finding for the resolver line, `IN_FLIGHT`, with a plain three-hop to-one chain that fails the same way).
- **The harness.** A relationship type whose relationships carry properties is declared as an edge class in the
  association form: `Class <<graph::graph.Edge>> g::E_T` with the properties and
  `<<graph::graph.type>> type() { g::RelationshipType.T }: g::RelationshipType[1]`, the ends
  `Association g::E_T_from { <<graph::graph.from>> from: g::Src[1]; T_out: g::E_T[*]; }` and
  `Association g::E_T_to { <<graph::graph.to>> to: g::Dst[1]; T_in: g::E_T[*]; }`, over a bridge table `r_t (id PRIMARY
  KEY, src_id, dst_id, p_...)` with one row per relationship and a join per end; the profile inlined. The SKIP for "a
  relationship holding a property, named by the query" is gone (50 TCK cases, most of them writes). A type without
  properties stays an association, byte for byte as before (the showcase's pins hold).

#### 12.38.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4363 | 2671 (from 2,649) | 0 | 0 | 0 | 683 | 711 | 298 |
| Postgres | 4363 | 2321 (from 2,299) | 0 | 0 | 0 | 1033 | 711 | 298 |

18 lite cases in `lite/edges.yaml`, each judged by Neo4j (one of them the hop past an edge, refused by name). The SKIP
count fell from 348: the 50 cases the harness skipped for a relationship property run now (most of them writes,
refused by name; the read-only ones pass).

### 12.39 A relationship type joining several class pairs, as several edge classes of one type, 2026-10-16 (PR3, step 2)

- **The model.** One Neo4j relationship type may join several label pairs (`(:A)-[:T]->(:B)` and `(:D)-[:T]->(:B)`);
  §5.6 says two edge classes may name the same type with different end pairs, `[:T]` is then their union and the
  endpoint labels choose. The harness now declares one edge class per class pair a type joins, numbered in the order
  the pairs appear (`g::E_T_1`, `g::E_T_2`, each `<<graph::graph.type>> type() { g::RelationshipType.T }`, each over
  its own bridge table, its ends `T_1_out`/`T_1_in` on the node classes), with or without properties; a type joining
  one pair stays what it was (an association, or an edge class when its relationships carry properties). Its SKIP
  "joins several class pairs, which waits for PR3's inheritance" is gone: 117 TCK cases run.
- **The island.** A typed relationship pattern whose type names several edge classes reads as arms, through the
  machinery an untyped relationship reads by (§12.17, §12.32): one arm per edge class the start node's class allows
  (`edgeWays`), the far node labelled with the class that edge reaches, each arm a typed pattern over one edge, the
  arms materialized to rows and concatenated. A far label written in the pattern keeps the arms it agrees with (an
  edge reaching another class is left out, as a label narrowing an end would be). A relationship carried through
  the arms is the union of its edge classes' properties (null where an edge class lacks one), as a node is of its
  classes'. Where the ways stay several outside the arms (an OPTIONAL MATCH's hop, a pattern comprehension's), the
  hop is refused by name: "the relationship type T names 2 edge classes read from A".

#### 12.39.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4369 | 2690 (from 2,671) | 0 | 0 | 0 | 785 | 713 | 181 |
| Postgres | 4369 | 2340 (from 2,321) | 0 | 0 | 0 | 1135 | 713 | 181 |

6 more lite cases in `lite/edges.yaml`, each judged by Neo4j (one of them the OPTIONAL MATCH form, refused by name).
The SKIP count fell from 298 to 181: the 117 cases the harness skipped for a type joining several pairs run, 14 of
them passing, 2 erroring on both sides as the TCK expects, the other 101 refused by name (variable-length
relationships, patterns inside expressions, an OPTIONAL MATCH after rows, identity); 5 more passes come from cases
whose graphs mix a multi-pair type with what they read.

### 12.40 A pattern as a predicate, and `EXISTS { ... }`, 2026-10-16

- **`WHERE (n)-[:T]->()`, `WHERE NOT (n)<-[]-(:B)`, `WHERE (n)-[:T|U]->() AND n.x = 1`:** a pattern inside an
  expression is a predicate, true where the hop from its bound node finds a match. The parser reads it as a
  `PatternPredicate` (a pattern starting after the parenthesis, rather than the refusal it was); the translator
  spells it over the pattern comprehension's hop resolution (§12.31): each way the model reads the hop, the
  navigation's non-emptiness (a to-one way `isNotEmpty`, a to-many way the collection's), the ways or-ed, no way
  false; a condition with its false case, so `NOT`, `AND` and `OR` around it read both. Neo4j allows a pattern
  expression only where a boolean is tested, and so does the island: outside a condition (a RETURN item) it is
  refused with Neo4j's words, and both error.
- **`WHERE EXISTS { MATCH (n)-[:T]->(b) WHERE b.x = 1 RETURN b }`, `WHERE exists { (n)-->() }`:** an existential
  subquery over one pattern, an optional WHERE (a filter over the hop's node, as the comprehension's is) and an
  optional RETURN (its items unread: a row's existence is what counts), spelled the same way. A subquery with
  several patterns or further clauses, and `COUNT { }` and `COLLECT { }`, stay refused by name.
- **Refused by name**, as the comprehension machinery refuses: a pattern over rows (a WHERE read over a fanning
  graph's materialized rows, or after a WITH), both ends bound (`MATCH (a), (b) WHERE (a)-[:T]->(b)`: identity),
  a property map in the pattern, more than one hop, a variable-length hop.

#### 12.40.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4381 | 2712 (from 2,690) | 0 | 0 | 0 | 774 | 714 | 181 |
| Postgres | 4381 | 2362 (from 2,340) | 0 | 0 | 0 | 1124 | 714 | 181 |

12 lite cases in `lite/predicates.yaml`, each judged by Neo4j (one erroring on both sides, two refused by name).

#### 12.40.2 The review of the three landings, 2026-10-16

Two independent reviews of the diff since PR #48 merged, one over the model and phase-F half, one over the
translator, parser and harness, each finding verified by a probe against the built jars; every finding fixed.

**The reader (`GraphEdges`, nine findings).** The island's view of the model read platform primitives as classes
(the context's lookup reaches the native catalog), so a `String[*]` property on an edge passed phase F and then broke
every query: both sources now read the model's classes alike. An end typed by a primitive, an enumeration or an edge
class passed: refused. A subclass of an association-form edge inherited an inverse reaching the superclass's
relationships (`[:Sub]` read every superclass instance): an inherited association end has no inverse for the
subclass, and the translator refuses the hop by name. The association scan ran only once a marked class had been
read, so an end stereotype on an unmarked class was caught or missed by class order: the injected ends are indexed
once. An association linking an edge by an end carrying neither marker (a third link), a type marker on an unmarked
class, both markers on one property, two inherited types that disagree, and a derived end (`<<graph.from>> from()
{...}`, which the reader does not read) went unchecked or were refused with the wrong words: each refused by name. A
fault in an inherited declaration was blamed on the subclass: it names the declaring class. A tolerant build walled
only the first bad edge, and the derived view then threw on every query: phase F checks class by class, one wall
each. A generic superclass dropped out of the lineage: read by its head. Open, recorded: a `<<graph.type>>` marker
on a derived property is read by its bare profile name (the resolver does not resolve a derived property's
stereotypes), so a profile named `graph` in another package would be read as the graph profile's.

**The translator, the parser and the harness (twelve findings).** Wrong answers: a hop over a to-one inverse
(`T_out: E[0..1]`) never filtered, every start node returned (the hop is optional now, so a row without an edge is no
match); an edge end with no inverse was silently skipped outside the typed MATCH (an untyped hop, a predicate, an
OPTIONAL MATCH, a comprehension answered as if the edge were absent): refused by name wherever the edge is a way; a
pattern predicate ignored its start node's label (`WHERE (a:B)-->()` over an `a:A`): the bound node's class or a
superclass is a no-op, a class apart matches nothing, a subclass is refused; a far label that is a subclass of the
class a way reaches made a predicate false (and an arm vanish) instead of refusing: refused as narrowing; an
undirected typed pattern from a label-less start read one direction only: refused. An execution error: a
relationship carried across arms as an edge in one arm and an association or an absent one in another, or an
unnamed relationship with a property map over a multi-pair type, gave the arms different columns (a union arity
error): every arm now carries the union's columns, null where its relationship lacks them, and such relationships
are named before the arms are read. Accepting what Neo4j rejects: a pattern predicate could introduce new variables
(`WHERE (a)-[r:K]->(m)`): refused with Neo4j's words, as EXISTS may introduce them; the boolean-context rule was a
dynamic flag, so a pattern inside a function's argument under a WHERE passed and one in a quantifier's predicate
was refused as Neo4j's rejection: the rule is structural now (a WHERE, a CASE test, a quantifier's or
comprehension's predicate, and the operands of NOT, AND, OR, XOR under one; a quantifier's pattern is then refused
by name, its scope not being the graph's). Missing or misplaced refusals: the hop-past-edge refusal was bypassed by
an unnamed far node (named now) and by a comprehension's end (marked now); the parser took `(a) - (b)` for a
relationship (arithmetic now) and refused `(:A)-[:K]->(a)`, `()-->(a)` and `({k: 1})-->(a)` as syntax errors (predicates
now); EXISTS gave syntax errors on `RETURN *`, `RETURN DISTINCT x` and `OPTIONAL MATCH` (read, read, refused by
name) and never read its RETURN items (read now for their static errors: an undefined variable errors, as in
Neo4j); a carried edge relationship's undeclared property read null where a bound one errored (C4: both error now),
and three edge refusals lacked the C4 phrase the harness classifies by. The harness's made-up names (`E_T_from`,
`RelationshipType`, a bridge table `r_t_1` beside a type named `T_1`) could collide with the graph's own: refused by
name.

**The Postgres lane's catch after the fixes (one finding, the lanes' own).** The arms' union gives a relationship
(and a carried node) the union of its classes' properties across the arms, null where an arm's class lacks one; the
arm's own property map or WHERE then read that column (`isNotEmpty && toOne() == 1`), which the lowering spells, over
a null constant, as the runtime error a `toOne()` over nothing raises: text on Postgres, refused against an integer
when the statement is typed (DuckDB's `error(...)` takes any type, so the DuckDB lane passed). Within the arm the
property is the known null: the carried column is marked absent, a read of it is the typed null constant the
comparison folds to null and the property map to false (as a label-less node's own property does, §12.13), `IS NULL`
folds too, and the marks are cleared on the union, where the column holds the other arms' values. The node form
showed a second gap beside it: the arms route tries each labelled node of an arm as the pattern's root, and a tried
root's property map was read against no candidate class, so a far node's map over a property its arm's class lacks
was refused as C4's error where the same test in a WHERE was lenient; an arm's every class is its pattern's
candidates from the start. Three more lite cases (the node form, a WHERE over the property, the property read after
the union) and one unit test pin both. The
reader's review also moved a guard: the lineage-order compares it made (`fqn.equals(cd.qualifiedName())`) counted
against `IdentityGuardrailTest`'s reversed-compare pin; the reader indexes the lineage instead (the declaring class
is position 0), the pin unmoved.

The ledger after the review, DuckDB: PASS 2718 (from 2,712), REFUSED 779, BOTH_ERROR 717, SKIP 181, over 4395 cases;
Postgres: PASS 2368 (from 2,362); 0 FAIL, 0 BUG, 0 ERROR on both. 14 more lite cases pin the findings Neo4j judges
(`lite/predicates.yaml`, `lite/edges.yaml`), 28 unit tests the rest.

### 12.41 `collect`, and lists the database holds, 2026-10-16

- **`collect(x)`:** the group's values as a list the database builds. Each value's JSON text (a number, a boolean, a
  string, a date as the oracle spells one, a node's or a list's Variant text; the spelling of §12.14 and §12.25), a
  null contributing nothing (Neo4j's `collect` skips nulls), joined with commas by Pure's `joinStrings` (the
  database's string aggregate) and read as a Variant between brackets; `[]` over no value. The elements' order is the
  database's, which the TCK's "ignoring element order for lists" allows and lite's cases avoid depending on. No new
  aggregate was needed: the ask to the database owner for a collection-valued aggregate (IN_FLIGHT, 2026-10-16) is
  withdrawn. `collect(DISTINCT x)` stays refused by name.
- **The element kind travels with the list.** A collected column whose elements are of one scalar kind (an integer,
  a float, a string, a boolean, a date), and a spelled list of such lists, carry the kind as a mark on the Variant
  (`ListOf`), so what reads the list reads its elements by type: `UNWIND xs AS y` flattens `toMany(@Integer)` and
  `y` is an integer column (ordered, compared, added); a list predicate, `IN`, an index and a list comprehension over
  it read the collection the database computes (`toMany` by the kind), as they read any computed collection. A
  list without a known element kind (a stored list, a collected list of nodes, a list of maps) unwinds as Variants
  (`toMany(@Variant)`), each read back by its own value; `size` of any Variant list counts its elements through the
  database.
- **UNWIND over a spelled list of lists, of maps, or of values of several kinds:** each element a Variant (its JSON
  text, read as a Variant), null elements null, the column marked with the lists' common element kind when they
  have one.
- **A list or map holding a value of the row, carried through WITH** (`WITH [p.age, p.age * 10] AS l`, `WITH {name:
  p.name} AS m`): the Variant the database builds (§12.25's text), marked with the element kind where the list's
  elements share one, so the clauses after unwind, size, test and index it as above; among the grouping keys it
  stays refused by name.
- **Refused by name:** `min` and `max` over Variants (Neo4j ranks lists, maps and mixed values by type: the three
  TCK cases that reached them through a mixed UNWIND answered wrongly before the refusal); `UNWIND` of a node, a
  relationship or a path (one row in Neo4j); `ORDER BY` a Variant; `single` over a collection the database computes
  (as before); `collect(DISTINCT x)`; `collect` of a time, a date-time or a duration (the Variant spells none of
  them yet).

#### 12.41.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4422 | 2784 (from 2,718) | 0 | 0 | 0 | 740 | 717 | 181 |
| Postgres | 4422 | 2434 (from 2,368) | 0 | 0 | 0 | 1090 | 717 | 181 |

22 lite cases in `lite/collect.yaml` and 5 in `lite/withlists.yaml`, each judged by Neo4j (one refused by name:
`collect(DISTINCT x)`). The TCK:
38 more scenarios pass (the `collect` bucket, Precedence1's quantifiers over collected booleans, Unwind1's double
unwinding and collected lists, Aggregation's and Return's `collect`s).

### 12.42 Variable-length relationships with an upper bound, 2026-10-16

- **`(a)-[:T*1..2]->(b)`, `(a)-[r:T*2]->(b)`, `(a)-[*0..1]->(b)`, `p = (a)-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c)`:**
  a variable-length relationship with an upper bound is the union of the patterns of each length within its bounds
  (Neo4j's missing lower bound is 1), each a chain of that many hops of the relationship's types and direction
  through nodes the query does not name, read as arms by §12.17's machinery (every chain's ways from every class
  the start node may be), the arms materialized and concatenated. The unroll's own nodes and relationships are
  bound in the arm's graph under names the materialization never carries as columns, so the arms' columns agree
  across lengths; a zero-length hop binds the far node to the near one (its labels and property map then the
  near node's; a far label the near node cannot carry leaves no chain of length zero). The relationship's
  variable is the list of the chain's relationships (the Variant the database builds, `[]` for the empty chain);
  a named path over the unroll is the chain's path, and its length, nodes and relationships are carried beside it
  when the arms are materialized, so `length(p)` reads over the rows. The WHERE is read per arm.
- **Relationship uniqueness is not spelled.** Neo4j never walks one relationship twice within a chain; telling
  the same relationship from another needs identity (C5), so a graph whose data loops a chain back over the same
  relationship (a self-loop walked twice, a two-node cycle walked round and back) answers rows Neo4j does not. A
  deliberate difference, recorded (`docs/SEMANTICS_REGISTER.md` S75); the TCK's variable-length graphs are
  acyclic or walk a cycle once, and lite's `a_variable_length_round_a_cycle` pins the walk Neo4j allows.
- **Refused by name:** no upper bound (`*`, `*2..`: a recursion, §6.3's native); a bound beyond 4; a pattern
  unrolling to more than 16 chains; a property map on the relationship; a pattern unrolling to more than two hops
  in all (the resolver's nested navigation reads no association end past a second step: the resolver line's
  finding in `docs/IN_FLIGHT.md`); a variable-length relationship after another clause, beside another pattern,
  or in an OPTIONAL MATCH; a chain past an edge class (§12.38's refusal, the same resolver limit).

#### 12.42.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4438 | 2802 (from 2,784) | 0 | 0 | 0 | 738 | 717 | 181 |
| Postgres | 4438 | 2452 (from 2,434) | 0 | 0 | 0 | 1088 | 717 | 181 |

16 lite cases in `lite/varlen.yaml`, each judged by Neo4j (three refused by name). The TCK: 5 more scenarios pass
(Match4, Match6, Match9, Path2, Path3).

### 12.43 A property or an index of a value the database holds as a Variant, 2026-10-17

- **`m.a`, `m['b']`, `m[k]`, `l[0]`, `l[-1]`, `l[i]`, `(l[1]).name`, `n['na' + 'me']`, `type(l[0])`, `labels(n)`
  over a value the database holds as a Variant** (a map or list carried through WITH, an UNWIND's row over a
  spelled list of maps or of lists, a collected node, a stored list): the platform's `get` by the key or the place
  (`get(Variant[0..1], String[1])`, `get(Variant[0..1], Integer[1])`: a negative place counted from the end, null
  beyond either end, null for a key the value lacks; the JSON arrow on DuckDB, jsonb's on Postgres, which agree). The
  result is read **by type where the Variant's shape says it**, through the platform's `to(@T)`: a spelled map's
  entries (`MapOf`: each entry's kind, nullability and shape), a list of one kind's elements (`ListOf`, §12.41, now
  carrying the elements' own shape, so a collected node unwinds as a node), a spelled list of several kinds' places
  (`TupleOf`: `[r, 1]` carried through WITH reads `l[0]` as the relationship and `l[1]` as the Integer). The shape
  travels as the Variant's mark: `variant(...)` attaches it to a list or map carried through WITH or returned,
  `collect` to the list it builds, an UNWIND over a spelled list to its rows when every element's shape agrees (a
  list spelling no element of a kind, `[]` or `[null]`, agrees with any list's; so `[[], [true], [null], [true,
  false]]` unwinds to lists of Booleans, and the TCK's Precedence1 `b IN c` cases pass). Where the shape says
  nothing (a map's member of a node the collect carried, a list's element by a place of the row, a key of the row),
  the result stays a Variant: a RETURN writes it and the judge reads the value it holds; a comparison, an ORDER BY,
  arithmetic over it stay refused by name. An entry that may be null is read only where its text is not the JSON
  null (Postgres's cast of a jsonb null raises; DuckDB's reads none).
- **`type`, `labels`, `properties` and `length` over values:** of a Variant whose mark says the function applies
  (`type` of a relationship's `"@type"`, `labels` of a node's `"@labels"`, `properties` of a known map), the entry;
  of a Variant of no known mark, a refusal by name (Neo4j raises for a value of the wrong kind, which the island
  could not tell from a right one); of a value the function does not apply to (`type(n)` of a node, `length(r)`,
  `properties(1)`, `labels('a')`, `type(1)` inside a comprehension), Neo4j's type error in Neo4j's words, so the
  TCK's negative scenarios agree (Graph4, Graph9, Path3).
- **`size` of a Variant list that may be null** tests the list and reads it only where it holds a value, without
  `toOne`: Postgres plans a FROM function's argument (`jsonb_array_elements(CASE WHEN x IS NULL THEN <raise> ...)`)
  before any row is read, so `toOne`'s raise inside it fired for a row whose list held a value (the Postgres lane's
  catch on `an_unwound_map_s_list_entry_sized`; the ninth line's finding in `docs/IN_FLIGHT.md`).
- **Refused by name, still:** a list comprehension or predicate over a list the database holds as a Variant of no
  known element kind (the quantifier scenarios, 26: `[y IN input.list ...]` where the map's entry is a list of
  Integers is read; `reverse(list) + x` over it is not), `ORDER BY` a Variant (18: a stored list's element by place,
  `a.list[1]`, is a Variant of no kind), two Variants compared (Comparison2, 4), `properties` of a node or
  relationship the database holds as a Variant (its keys would need stripping).

#### 12.43.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4473 | 2840 (from 2,802) | 0 | 0 | 0 | 724 | 728 | 181 |
| Postgres | 4473 | 2490 (from 2,452) | 0 | 0 | 0 | 1074 | 728 | 181 |

35 lite cases in `lite/variants.yaml`, each judged by Neo4j (eleven Neo4j's errors, matched). The TCK: 14 more
scenarios agree (Unwind1, Graph4, Graph6, Graph7, Precedence1; the error scenarios of Graph4, Graph9 and Path3 now
agree by the type error, not by a refusal).

### 12.44 A MATCH after rows whose pattern is new, 2026-10-17

- **`WITH a.num AS property MATCH (b:End) WHERE property = b.num`, `UNWIND xs AS x MATCH (n)`, `WITH 1 AS x MATCH
  ()-[r]->()`:** a MATCH after rows (a WITH that projected its variables, an UNWIND, a pattern read as rows) whose
  one pattern names no variable of the rows is the product of the rows and the pattern's rows, §12.28's cartesian
  machinery (the pattern read on its own as a first pattern, a graph, a label-less node's arms or rows, then its
  rows; every pair joined by a condition that always holds), the WHERE read over the pairs, the clauses after
  reading the rows (the pattern's nodes carried as their columns, so `RETURN b` and `b.name` read them). A pattern
  naming a variable of the rows stays refused by name: a hop from a carried node after rows is the continuation
  §12.32 reads where it can, two ends bound is identity (C5), and a variable of another kind (`WITH p.name AS n
  MATCH (n)`) is Neo4j's static error, which the island does not spell. A MATCH of several patterns after rows is
  refused by name too.
- **`collect(DISTINCT x)` stays refused by name, tried and withdrawn:** the distinct texts joined
  (`distinct()->joinStrings(',')`) answer in the database's order of the distinct values, which is not Neo4j's
  encounter order and not the same twice (lite's `collect_distinct_is_refused` passed one run as `[1, 2]` and
  failed the next as `[2, 1]`); a list is an ordered value, so a list in an order the database chooses is a wrong
  answer, not a difference to register.

#### 12.44.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4484 | 2854 (from 2,840) | 0 | 0 | 0 | 720 | 729 | 181 |
| Postgres | 4484 | 2504 (from 2,490) | 0 | 0 | 0 | 1070 | 729 | 181 |

11 lite cases in `lite/rows.yaml`, each judged by Neo4j (one refused by name, as Neo4j errors). The TCK: 4 more
scenarios pass (With4, Comparison1).

### 12.45 Open and closed graphs, 2026-10-17

- **The frame (the user, 2026-10-17: "look at the GQL spec, seems like they support open and closed graphs, so
  maybe we do the same"):** GQL (ISO/IEC 39075) calls a graph CLOSED when a graph type constrains it (it holds only
  the node and edge types the type declares) and OPEN when none does (any label, any property). Neo4j's graphs
  are open. A lite model IS a graph type, so a lite graph was closed by construction and C4 (S58) was the closed
  reading: a label, a property or a relationship type the model lacks is a compile error. The leg adds the open
  reading as a declaration of the graph, not a switch on the engine: a class carrying `<<graph::graph.Open>>`
  (the profile's fifth stereotype, `projects/graph`) is a type of an open graph, and when every class of the model
  carries it the graph is open (`CypherIslands.Openness`, read once per model; `CypherToPure.Model.open()`). The
  parity harness marks every class it generates open, because a Neo4j fixture is an open graph; a model in a
  user's source is closed unless it says otherwise, so the typo guard (`p.nmae`) stays the default and the TCK
  certifies the open reading of an open graph, which is what it measures on Neo4j.
- **Where the graph is declared (the user, 2026-10-17, three turns): not on a class ("a static decision on a class
  means it cannot be configured two ways"), not an island prefix ("a random string or boolean", "weird new syntax"),
  but Cypher's own `USE` naming a DataSpace ("DataSpace can play the role of Graph").** The leg's first step had put
  `<<graph.Open>>` on every class; the landing reads it as GQL does: `USE g::Graph MATCH ...` opens the island with
  the graph it reads, a DataSpace of the model by its path (the island's parser already read `USE` as a Pure path
  or a backticked name), whose `elements` are the labels (`MATCH (n)` reads them, not every class of the model;
  a package path includes its classes, a `-` prefix excludes), whose execution context is the store (what a write
  will write into), and whose `<<graph::graph.Open>>` is the open reading. Without a `USE` the island reads the
  whole model, closed, as before. The seam (`CypherIslands.graphOf`) parses first, resolves the DataSpace
  (`ModelContext.findDataSpace`: the typed model now indexes DataSpaces, and `DataSpaceDefinition` carries its
  stereotypes), builds the translator's model scoped to it, and the translator reads the leading `USE` as done
  (`CypherToPure.leadingUse`; a `USE` anywhere else is Neo4j's error, "USE clause must be either the first clause
  in a (sub-)query or preceded by an importing WITH clause"; a `USE` naming no DataSpace is an error at its
  place). The parity harness emits `DataSpace <<graph::graph.Open>> g::Graph` listing every class the dump implies
  and opens each island with `USE g::Graph` after the version prefix; the profile is declared on every model.
- **A name the model has nothing of (`MATCH (n:Nope)`, `MATCH (a:A)-[:NOPE]->(b)`):** read before the MATCH's
  pattern is (`unknownName`): a label naming no class, or a relationship type that is no edge class's type and no
  class's property holding a class, makes the MATCH match nothing (`noneOf`): as the first clause, §12.36's no-row
  form (an OPTIONAL MATCH: one null row); after a graph or rows, the rows in hand limited to none (an OPTIONAL
  MATCH: kept), the pattern's own variables absent and a bound one kept as the pattern's anchor; the property maps
  and the WHERE read for their static errors (`Variable q not defined` is Neo4j's error too). A pattern
  comprehension, a pattern predicate or `EXISTS` over such a name matches nothing (`Hopped` with no way); `n:Nope`
  was already false (the labels list never contains it).
- **A typed hop no way of the class reads (`MATCH (a:A)-[:U]->(x)` where only `B` has `U`):** `hop` answers no way
  (null) where the closed graph threw C4, and the labelled pattern's `extend` throws `NoWay`, which `match` reads
  as the same nothing. The arms machinery (§12.17, §12.35) already dropped such ways; an OPTIONAL MATCH already
  bound an absent node.
- **A property no class declares (`n.nope`):** Neo4j's null, the untyped null the island already carried for an
  absent node's property, at each of the four reads (a bound node's property navigated from the root, a carried
  node's column, a carried relationship's, an ORDER BY key with SKIP or LIMIT, which orders nothing); a comparison
  folds it as the known null (`WHERE n.nope = 'a'` keeps no row, `n.nope IS NULL` every row, `{nope: 'a'}` none),
  `coalesce` passes it, arithmetic over it is null.
- **What stays as it was:** the closed graph's three errors, pinned by name; a model inconsistency (a label naming
  an edge class, a type naming both an edge class and a property) is an error in both readings.
- **Found on the way, fixed (the open reading reaches eight TCK scenarios C4 refused before):** a UNION whose part
  answers an untyped null (`RETURN n.nope`, `RETURN null`) re-projects that column as the other part's typed null,
  so the parts concatenate; `WITH me.age + you.age, count(*) AS cnt` is Neo4j's static error (`Expression in WITH
  must be aliased (use AS)`), which the island now raises (WithOrderBy4); an incoming (or undirected) typed hop
  to a label-less node (`(advertiser)-->(out)-->(red)<-[:AA_HAS_VALUE]-(a)`, MatchWhere2, WithWhere2) reads the
  one class of the model declaring the type as a property holding the node before's class, as §12.17's arms
  decide a label-less root's ways (several such classes refused by name), where the closed graph raised C4 for the
  wrong reason; and an expression around aggregates may read a grouping key, Neo4j's rule that any other item of
  the projection is one (`RETURN me.age, me.age + count(you.age)`, `WITH age, age + count(you.age) AS agg`,
  `{foo: a.name = 'Andres', kids: collect(child.name)}` beside `a.name`: Return6, With6), the key's spelling read
  as its column after the grouping (`KeyedScope`); a key is a variable or a property of one (Neo4j's leaf keys:
  `me.age + you.age` projected beside `me.age + you.age + count(*)` is still its
  `AmbiguousAggregationExpression`, Return6/[21], With6/[9]), a property of a key is no key, and a value of the
  row that is no key stays the implicit-grouping error in Neo4j's words. The incoming-hop reading reaches the
  resolver's limit on MatchWhere2 and WithWhere2 (`ADV_HAS_PRODUCT.AP_HAS_VALUE.AA_HAS_VALUE_in`: "property
  'AA_HAS_VALUE_in' of nested navigation ... is not mapped"), so a pattern navigating more than two association
  steps from its root is refused by name, as the variable-length unroll already was (W9 of the TCK-to-zero plan).
  The no-way sentinel is a null graph from `pattern()`, not an exception: the catch-that-returns-a-value guardrail
  (`ErrorShapeGuardrailTest`) holds at its pin.

#### 12.45.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4513 | 2945 (from 2,854) | 0 | 0 | 0 | 657 | 730 | 181 |
| Postgres | 4513 | 2584 (from 2,504) | 0 | 0 | 0 | 1018 | 730 | 181 |

29 lite cases in `lite/open.yaml`, each judged by Neo4j (one BOTH_ERROR, as Neo4j errors: a variable not
defined). The TCK: agreement 3093 of 3880 (from 3,031): 62 more scenarios, Match5 (29: the label-less and
schema-optional MATCH feature), Match7 (7), Return6 (4), ReturnOrderBy6 (3), Null1 and Null2 (4), Graph5 and
Graph6 (3), With6 (2), WithOrderBy4 (2), Match3 (2), ExistentialSubquery1, Match2, Return2, Return3, Return4,
ReturnSkipLimit2 (one each); no case lost.

### 12.46 Identity: a node is its row's key, 2026-10-17

- **The frame (the TCK-to-zero plan's W2, the user's design in five turns, its §3.2 Q2):** Cypher's `a = b` on
  nodes, DISTINCT, grouping, `IN` over nodes and relationship uniqueness mean "the same node", and a node is a
  row: two nodes named Ann are two nodes. C5 (§4) had composed the model's `<<equality.Key>>` with the mapping's
  key; the user drew the line between them: **value equality** (`==`, `equal`, `in`) is Pure's rule by the
  `<<equality.Key>>` properties, unchanged, and **identity** is the store's, the mapped instance's key, in the
  engine's order with no fallback past the last step: a constructed instance's minted `__id` (the PCT lane's,
  `lowering/InstanceEquality`), a mapped instance's `~primaryKey(...)` when its class mapping declares one, else
  its main table's `PRIMARY KEY` columns, else a compile error naming the class ("the class g::A has no identity
  under the mapping g::Map: its mapping declares no ~primaryKey and its main table no PRIMARY KEY"). Never all
  the mapped columns, never `<<equality.Key>>`: a key states meaning, and would merge distinct nodes on exactly
  the models that declare one. The user's doubt ("a class made up of five tables joined with filters and
  exploding rows?") is answered by the mapping's own rule: a relational class mapping has one main table and its
  instances ARE that table's rows, every join inside a property mapping fetches columns for the same instance or
  reaches other classes' instances, and an exploded projection still carries the main row's key, which is how
  the engine's graph fetch rebuilds objects. **Verification pending:** the claim that the engine defaults to the
  main table's primary key when `~primaryKey` is absent is to be checked against the engine's compiler source
  when a reference checkout is on the machine (none is; the plan's §5).
- **The product: one native, `meta::legend::lite::elementId`** (`instance: Any[0..1]): String[0..1]` and
  `(instances: Any[*]): String[*]`, `Pure.Lite.ELEMENT_ID`, under the catalog's governance ceiling, with the
  internal-desugar natives). Resolved, never lowered: `ClassSources.pseudoBindings` registers each class source's
  key columns as pseudo-bindings (`$pk:<column>`, `ClassMapping.primaryKeyBinding`, the engine's precedence as
  above: `binding.primaryKeyColumns()` else `RelationalRootForm.primaryKeyColumns`), and `StoreResolver`
  rewrites `elementId(X)` anywhere in an object-space chain (`identityReads`, a pre-pass of `collectOpChain`
  over the chain and its lambdas, so a read inside a project's or filter's lambda rides the one property funnel
  beside the class's properties) to `'<class fqn>:' + toString(X.$pk:col)`: the text is unique across the
  graph, as Neo4j's `elementId` carries its type and database ('4:db-uuid:0'); the class names the table, the key
  the row. An optional instance (an optional navigation, a left join's empty side) has NO identity: the
  concatenation would skip the empty operand (Pure's `plus` over `String[*]`, the database's `concat`), so the
  key's presence gates it (`if($x.$pk->isNotEmpty(), |'g::B:' + ..., |[])`, the database's `CASE`). A composite
  key is refused by name until it is spelled; a chain-level `elementId(Person.all()->first())` reads the same
  way through `anchoredNode`. A Pure user writes `$p->meta::legend::lite::elementId()` and gets `test::Person:1`;
  with `~primaryKey ([store::DB] T_PERSON.NAME)` declared, `test::Person:Alice` (the integration test
  `theIdentityOfAMappedInstanceIsItsKey` pins both, the null of an absent navigation, and the keyless error).
- **The island:** a node's parts carry its identity beside its labels and properties (`NodeParts.identity`,
  `NodeValue`, `RelValue` for an edge class's relationship, `NodeShape` through WITH), a carried node is one more
  column (`carry()`: `_cypher_n:a|$a->meta::legend::lite::elementId()`, null in an absent branch), and `a = b`
  or `a <> b` on two nodes compares the identities (`equal` reads `identityOf` on each side: a bound node's from
  the graph, a carried node's from its column), with Cypher's null rules (an absent node compares to null). Nodes
  of different classes never compare equal (the prefix differs); a label-less pattern's arms carry each class's
  identity; a relationship over an edge class (§5.6) carries the edge row's identity the same way (`RelShape`,
  `RelValue`), so `MATCH (a:A)-[r:T]->(), (b:A)-[s:T]->() WHERE r = s` and the same after a WITH compare
  relationships; a node a to-many hop reaches (`$a.T->elementId()`, the `[*]` overload) reads each instance's
  identity element-wise (the resolver's `map`, never the concatenation over the collection, which would join
  them). Relationship uniqueness across the patterns of one MATCH (`MATCH (a:A)-[r:T]->(), (b:A)-[s:T]->()`:
  Neo4j binds `r` and `s` to different relationships, so `WHERE r = s` finds nothing) is spelled over the
  product's rows for relationships carrying an identity (`cartesian`: each pair of edge-class relationships
  distinct by it); over an association it stays unspelled (S75), as within one pattern two relationships of one
  type stay refused by name. Refused by name, recorded: a relationship over an association compared (`MATCH ()-[a]->() WITH a
  MATCH ()-[b]->() WHERE a = b`: no row of its own, so no identity; S74's ordered pair of node identities is the
  next step), a path compared (Comparison1/[14]), DISTINCT, grouping and ORDER BY over a node (the next leg's).
- **Found on the way:** the resolver's chain-level arm never sees a call inside a lambda (the substitution
  rewrites lambda bodies with no model context), hence the pre-pass; a node carried twice (`WITH a ... WITH a`),
  the arms' union and a carried relationship's value dropped the identity column on their way (40 cases errored
  on the first run, WithOrderBy1 and WithWhere3-5 among them), each re-carry now keeps it; the protocol layer spells the native's name
  (it reads no catalog, AGENTS invariant 7b), so `PlatformNamesGuardrailTest`'s function-FQN ceiling moves 324 →
  325 with its justification, and the resolver's `toString` callee is the catalog's constant; the island's
  labelled-pattern pins gain the identity column, and the `comparing_nodes_refused` case of `lite/labels.yaml`
  moved to `lite/identity.yaml` as a positive case.

#### 12.46.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4528 | 2965 (from 2,945) | 0 | 0 | 0 | 652 | 730 | 181 |
| Postgres | 4528 | 2604 (from 2,584) | 0 | 0 | 0 | 1013 | 730 | 181 |

16 lite cases in `lite/identity.yaml`, each judged by Neo4j (15 PASS; the relationship over an association
compared stays REFUSED by name); `lite/labels.yaml`'s `comparing_nodes_refused` moved in as `two_nodes_equal`.
The TCK: agreement 3098 of 3880 (from 3,093): 5 more scenarios, MatchWhere3/[1] and MatchWhere4/[1] (a join
between node identities, on inequality), WithWhere3/[1] and WithWhere4/[1] (the same after WITH), Comparison1/[4]
(nodes compared after WITH); no case lost. The showcase's §20 regenerated; its pinned examples return values,
not nodes, so their pinned Pure and SQL are unchanged.

### 12.47 Nodes told apart by identity: DISTINCT, grouping, count(DISTINCT), 2026-10-17

- **The frame (the plan's W2, leg 2):** Neo4j's `RETURN DISTINCT n`, `RETURN n, count(*)`, `WITH n, count(*) AS c`
  and `count(DISTINCT n)` tell nodes apart by identity: two nodes alike are two rows, two groups, two counted. The
  island had refused each by name since the nodes-as-values leg (§12.14), because a node's JSON holds its labels
  and properties only, and distinct JSON texts would merge nodes alike. §12.46's identity column is what was
  missing.
- **The reading:** a returned node or relationship under DISTINCT or a grouping is CARRIED as its columns, as a
  node through WITH already was (`outputs`' `carryReturned`), its identity among them; the DISTINCT is over those
  columns, a grouping takes each of them as a key (the identity makes nodes alike two groups; the other columns
  ride along, determined by it), and the item's JSON is rebuilt from the columns LAST (`project`: an `extend` over
  a `RowScope` of the rows after the sort, SKIP and LIMIT, then the items' columns alone, in order), so an ORDER
  BY over the node's property after the DISTINCT reads the carried column, as the sort after a grouping already
  did. `count(DISTINCT x)` is Pure's `y|$y->distinct()->count()` (the lowering's `COUNT(DISTINCT ...)`);
  `count(DISTINCT n)` over a pattern variable counts its identities (an absent node's null, which count skips).
  A relationship over an edge class is told apart by the edge row's identity the same way; one over a property
  or an association has no identity yet, so `DISTINCT r` and a grouping by it stay refused by name until S74's
  ordered pair is its identity (W2 leg 3), as `collect(DISTINCT ...)` stays (§12.44: the database's order).
  ORDER BY a node stays refused (Neo4j orders nodes by its own ids).
- **Found on the way:** the grouped projection's select after the `groupBy` named the virtual column of a carried
  node (a column no relation bears) and now skips it; the identity read of an optional to-one navigation lowers
  through the string-aggregate path of `plus` rather than `concat` (the `if` around it), correct and gated by the
  key's presence, left as the lowering spells it.

#### 12.47.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4546 | 2997 (from 2,965) | 0 | 0 | 0 | 638 | 730 | 181 |
| Postgres | 4546 | 2636 (from 2,604) | 0 | 0 | 0 | 999 | 730 | 181 |

18 lite cases in `lite/grouping.yaml`, each judged by Neo4j (15 PASS; a relationship over an association under
DISTINCT or a grouping, and `collect(DISTINCT ...)`, REFUSED by name); `lite/nodes.yaml`'s two refusal cases
renamed as positive ones. The TCK: agreement 3112 of 3880 (from 3,098): 14 more scenarios, Return6 (3: a node
among the keys, `count(a) + 3`, counting per group), Return5 (3: `count(DISTINCT ...)` over lists in maps),
ReturnOrderBy2 (4: sort and distinct, ordering by a property after distinct, count star in scope, the columns
under ORDER BY), Aggregation5 and Aggregation8 (3: `collect()` filtering nulls beside a node, `count(DISTINCT a)`
on an unbound node, on null), WithWhere6 (1: filtering a single aggregate); no case lost.

### 12.48 A relationship over an association is the ordered pair of its ends, and no relationship is walked twice, 2026-10-17

- **The frame (the plan's W2, leg 3; S74 and S75 revisited):** a relationship over a plain `Association` or a
  class-typed property has no row of its own, so §12.46 gave it no identity, and §12.47 refused DISTINCT and a
  grouping over it; S75 recorded that relationship uniqueness (Neo4j binds the relationship variables of one
  MATCH to different relationships, and walks none twice along a chain) was not spelled. The link IS
  identifiable: its type and the ordered pair of its ends' identities in its stored direction,
  `'<type>:<source>><target>'`, which is one text per link (an association holds one link per pair, S74), and
  what Neo4j's own id of such a relationship distinguishes.
- **The reading:** a relationship binding carries the hop's stored direction (`Binding.forward`: true when the
  node before holds the node after, the direction `Hop` already knew for a path's `@dirs`). Over to-one hops the
  identity is spelled in the graph scope (`GraphScope.identityOf`: `pairIdentity` over the near and the far
  node's `elementId`, read as one inside the gate that makes the whole null where a hop finds no node, since the
  concatenation would skip an empty operand); over a to-many hop the pair's two columns ride the materialized
  rows (`carryRel`: the source's and the target's identities, `RelShape.source`/`target`, exploded one row per
  link with the far node's columns) and the text is rebuilt over them (`RowScope.relIdentity`), as a node's
  identity rides its column. Everything of §12.46 and §12.47 then holds for such relationships: `r = s`, `r <>
  s`, `r = r` (null for an absent one), `RETURN DISTINCT r`, `WITH r, count(*)`, `count(DISTINCT r)`, the
  comparison after a WITH (Comparison1/[5]).
- **Uniqueness (S75 overturned for one MATCH):** the MATCH's patterns are pre-scanned for the types their
  relationships repeat (`MatchFrame.scan`; an untyped relationship beside another counts as every type); an
  unnamed relationship of a repeated type is named (a fresh name, as one with a property map already was), and
  each relationship read registers against the earlier ones of its type (`relationshipRead`): the pairwise
  `r1 <> r2` conditions are conjoined to the MATCH's WHERE (`withUniqueness`, at each of the four readers: the
  single graph, the label-less and the untyped arms, the variable-length arms' base), so they land in the
  graph's conditions over to-one hops and in the deferred WHERE over materialized rows otherwise, and across
  the patterns of one MATCH the product's filter (§12.46) now reads the pair identities too, of the
  relationships this MATCH read (a second MATCH clause joins an earlier MATCH's graph freely: the same
  relationship may bind in both, as in Neo4j; the chain's first run caught the product applying it across
  clauses), the second pattern read under the whole MATCH's pre-scan (an unnamed relationship of a type repeated
  across the patterns is named). A self-loop walked
  twice (`(a)-[:T]->(b)-[:T]->(c)` over `(n)-[:T]->(n)`), a chain back and forth over one relationship, a
  two-node cycle walked round and back: none, as in Neo4j; two relationships of different types may share their
  ends. A relationship alone in its MATCH is named by nothing (nothing to tell it from).
- **Still unspelled, recorded:** uniqueness along a variable-length chain (the unroll's hidden relationships,
  §12.42: `(x)-[:T*3]->(z)` over a two-node cycle still walks a relationship twice, the lite case says so), and
  within an OPTIONAL MATCH's chain (two of one type stay refused there by name).
- **Found on the way:** the guardrail against mutable instance fields moved the per-MATCH bookkeeping into one
  frame object (`MatchFrame`); a graph-scope relationship value over an association had carried no identity at
  all (`relValue`), only the edge class's did.

#### 12.48.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4562 | 3020 (from 2,997) | 0 | 0 | 0 | 631 | 730 | 181 |
| Postgres | 4562 | 2659 (from 2,636) | 0 | 0 | 0 | 992 | 730 | 181 |

16 lite cases in `lite/uniqueness.yaml`, each judged by Neo4j (15 PASS; the variable-length chain over a cycle
stays REFUSED by name, the unroll past two hops); the three association refusal cases of `lite/identity.yaml`
and `lite/grouping.yaml` renamed as positive ones. The TCK: agreement 3115 of 3880 (from 3,112): 3 more
scenarios, Comparison1/[5] (relationships compared after WITH), Match6/[9] (a longer path's results in written
order, two relationships of one type in one pattern), Graph4/[2] (`type()` on two relationships); no case
lost. Most uniqueness cases of the TCK run over association
graphs the harness already read correctly where the data walked no relationship twice, which is why the gain
is small and the correctness gain large (the chain's first run flagged the product applying uniqueness across
clauses).

### 12.49 Re-anchoring: a pattern after rows joined back by identity, 2026-10-17

- **The frame (the plan's W2, leg 4):** a MATCH or OPTIONAL MATCH after rows (a WITH that projected values or
  grouped, an UNWIND, a pattern read as rows) whose pattern names a variable the rows hold was refused by name
  since §12.44 read only the pattern sharing nothing (the product): a continuation from a carried node had
  nothing to navigate from, since a carried node is columns, not an instance. Identity (§12.46) is what joins
  them: the carried node's identity column names the row of its class.
- **The reading (`reanchor`):** the pattern is read as a graph of its own, from the carried node's class when
  the pattern starts (or, reversed with its directions flipped, ends) at one (`NodeShape.classPath`, the class
  a carry of one class records; a node carried as one of several classes is refused by name), else from its
  first node's label or every class (a relationship the rows hold is enough to share); every variable the rows
  hold reads under a fresh name in that graph; the graph is materialized (every node and relationship carried
  with its identity, §12.16) and joined to the rows on the identity of each shared variable: a node by its
  identity column, a relationship by its identity column or by the columns of its type and the pair of its
  ends' (§12.48). The joined rows carry the rows' own columns and the pattern's new ones; the copies of the
  shared variables ride unnamed. A MATCH's WHERE reads over the joined rows; an OPTIONAL MATCH is a left join
  (`JoinKind.LEFT`), the pattern's columns nullable after it (a node's labels column null where the pattern
  found nothing: the absent node §12.18 already reads), its WHERE read over the pattern's own rows before the
  join, under aliases of the shared variables to the pattern's copies (the same nodes by the join); an OPTIONAL
  MATCH's WHERE reading a value of the rows beside its pattern is refused by name.
- **What it reaches:** `WITH a, a.name AS n MATCH (a)-[:T]->(b)`, `WITH a, count(*) AS c MATCH (a)-[:T]->(b)`,
  `OPTIONAL MATCH (a)-[:T]->(b)` after rows with or without a WHERE, both ends bound (`WITH a, b, 1 AS x MATCH
  (a)-[:U]->(b)`), a pattern ending at the carried node, a bound relationship matched again (`WITH r, x.name AS
  xn MATCH (y)-[r]->(z)`; With6/[2]'s `WITH r1 AS r2, count(*) AS c MATCH ()-[r2]->()`), re-anchoring twice.
  With it, the fan-out to a class without a property (§12.16's refusal: no navigated column to multiply the
  rows) is lifted: the node's identity column navigates its path.
- **Still refused by name, recorded:** a MATCH after rows naming a scalar the rows hold (Neo4j's error), from a
  node carried as one of several classes, a named path after rows, a variable-length relationship after rows,
  an OPTIONAL MATCH's WHERE reading the rows' values.

#### 12.49.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4576 | 3068 (from 3,020) | 0 | 0 | 0 | 597 | 730 | 181 |
| Postgres | 4576 | 2707 (from 2,659) | 0 | 0 | 0 | 958 | 730 | 181 |

14 lite cases in `lite/anchor.yaml`, each judged by Neo4j (12 PASS; a re-anchoring from a node of several
classes and an OPTIONAL MATCH's WHERE over the rows' values REFUSED by name). The TCK: agreement 3149 of 3880
(from 3,115): 34 more scenarios, TriadicSelection1 (9: friend-of-a-friend with an OPTIONAL MATCH after the
rows), Match (8), With (4), Graph (4), MatchWhere (3), WithSkipLimit (3), Return (2), WithWhere (1); no case
lost. The chain's first run had 9 TriadicSelection cases erroring in the resolver (an edge hop after an
association hop is three navigation steps, which the translator's depth refusal had exempted): the refusal now
counts every step, so they are REFUSED by name, as W9 records.

### 12.50 Cycles, the optional fallbacks, and the undirected self-association, 2026-10-17

- **The frame (the plan's W2, leg 5, the last of its identity legs):** three readings waited on identity since the
  first PR: a node named again within one pattern (`(n)-[r]->(n)`, `(a)-[:A]->()-[:B]->(a)`: "a pattern whose two
  ends are both bound already, a cycle compared by identity"), the OPTIONAL MATCH shapes the optional hop cannot
  read (an untyped relationship, a relationship with a property map, a far node already bound), and the
  undirected relationship over a self-association (`(a)-[r]-(b)` where `T` holds `A` on both ends: a loop
  `(x)-[:T]->(x)` matches once in Neo4j, which the two directions read twice).
- **Cycles:** a node named again reads under a fresh name, the same node as the first by identity, a condition
  the MATCH's WHERE takes with the uniqueness conditions (§12.48's `withUniqueness`): spelled in the graph over
  to-one hops, deferred over the materialized rows otherwise. The renaming happens once per MATCH before its
  arms are read (`nameRepeats`, beside the naming of relationships with property maps), so the arms' columns
  agree, and the arms' column order is the pattern's after the renaming (`patternOrder`: the nodes, then the
  relationships, each once; an order taken before the renaming misaligned the arms, JSON garbage and wrong
  counts in the chain's first run). The cycle conditions live on the MATCH's frame (`MatchFrame.cycles`, cleared
  per MATCH), not the per-arm lists the arms reader wipes. The pre-checks that refused a pattern with both ends
  bound are gone; the hop reader keeps the same rule for a pattern read directly.
- **A second-step hop of a fanning graph is present on the rows:** `MATCH (a {name: 'A'}), (b {name: 'B'}) MATCH
  (a)-->(x)<-->(b)` (Match3/[19]) re-anchors as `(a)-[:KNOWS]->(x)-[:KNOWS|KNOWS_in]-(b)` over a many-to-many
  `KNOWS`, and the graph's filter `$a.KNOWS->isNotEmpty() && $a.KNOWS.KNOWS_in->isNotEmpty()` fails to resolve
  (`UnfoldableRef: KNOWS_KNOWS_b_src_id`): the resolver checks a two-step existence in a filter per fanned row
  over a foreign-key step but not past a bridge-table step (the plan's §5). The translator cannot see the
  mapping, so it stops spelling such a hop's existence in the filter at all: when the graph already fans before
  the hop, the hop's node is "present" (`Graph.present`), its identity and properties nullable, and `materialize`
  filters the rows where its identity is (`$r._cypher_25->isNotEmpty()`, beside the deferred property maps):
  the same rows, said over the materialized rows where the resolver's LEFT JOIN is, as the cycle's identity guard
  already was. A first hop's existence, or a second step after a to-one first hop, stays in the filter.
- **`head` and `last` over a list the database holds:** `MATCH ()-[r*0..1]-() RETURN last(r)` (Match9/[1]) reads
  `last` over a variable-length relationship's list, a Variant the arms build; before, the function read only
  spelled lists and collections ("expected List<T> but was Map"). It is the list's first or last place as `r[0]`
  and `r[-1]` read it (§12.43's `get`, null beyond either end: null for the empty list). The union's shape: the
  zero-length arm spells `[]` (a tuple of no places), the next `[rel]`, and the first arm's shape stood for the
  column, so the index read a place beyond the empty tuple and answered null everywhere. `materializeArms` now
  merges the arms' list shapes (`mergedShapes`: a list of the kind and mark the arms' places share, a relationship
  of no known type where types differ, no shape where kinds differ; a node's or relationship's columns keep the
  first arm's, as before).
- **The optional fallbacks:** an OPTIONAL MATCH over a graph with an untyped relationship, a relationship with a
  property map or a bound far node materializes the graph and re-anchors (§12.49: the pattern read as its own
  graph, joined back by identity with a left join), so `MATCH (a:A)-[:T]->(b:B) OPTIONAL MATCH (b)-[u:U]->(a)`
  and `OPTIONAL MATCH (a)-->(x)` read; one from no node the graph binds stays refused by name.
- **The undirected self-association:** an undirected relationship whose type holds the node's own class
  (`selfAssociation`, the declared properties and association ends scanned) reads through the arms reader (both
  directions as arms, as an untyped relationship does), and the arms' materialized rows are deduped by identity
  (`armsRows`: Pure's `distinct` over every carried column, the identities among them): a loop is the same row
  in both arms and reads once; two nodes linked once read twice, once each way, as in Neo4j.
- **Still refused by name, recorded:** an undirected typed relationship from a label-less node; a cycle within an
  OPTIONAL MATCH read as a chain is routed through the fallback instead.

#### 12.50.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4586 | 3110 (from 3,068) | 0 | 0 | 0 | 565 | 730 | 181 |
| Postgres | 4586 | 2749 (from 2,707) | 0 | 0 | 0 | 926 | 730 | 181 |

10 lite cases in `lite/cycles.yaml`, each judged by Neo4j (10 PASS). The TCK: agreement 3181 of 3880 (from 3,149): 32
more scenarios, Match3 (8), CountingSubgraphMatches1 (8), Match6 (3), Match7 (3), MatchWhere6 (2), Graph8 (2),
MatchWhere1 (1), Match2 (1), Match9 (1), WithWhere1 (1), With7 (1), Aggregation1 (1); no case lost. The chain's first
run had 4 FAIL and 5 ERROR: the per-arm reset wiping the cycle conditions and the arms' columns ordered before the
renaming (both fixed, the frame's `cycles` and `patternOrder`), Match3/[19] erroring in the resolver (the two-step
existence past a bridge-table step, now said over the rows), and Match9/[1] refusing `last(r)` over the arms' list
(now read as `r[-1]`, the arms' list shapes merged).

### 12.51 rand() and the statement clocks, 2026-10-17

- **The frame (the plan's W3):** the functions whose value is the call's, not the data's: `rand()` and
  `randomUUID()`, and the clocks `date()`, `localtime()`, `time()`, `localdatetime()`, `datetime()` with no
  argument, with their `.transaction`, `.statement` and `.realtime` forms (the TCK's Temporal4/[13] nulls and
  Temporal10/[12] zero durations, Return6/[15]'s error; `rand()` itself is the gate of the Quantifier9-12 cases,
  which then need a list comprehension over a Variant list, W12).
- **`rand()` is Pure's `meta::pure::functions::math::random(): Float[1]`**, the database's random float in
  `[0, 1)` (corrected 2026-10-17, the spec guards' repair, §12.81: the leg landed it as a lite native,
  `meta::legend::lite::random`, on the claim that legend-pure has no random native; legend-pure declares one, in
  `essential/math/random.pure`, which the regenerated prelude showed, so the platform implements upstream's and
  lite's twin left the governance ceiling and the census), lowered to `SqlFn.RANDOM` with
  one render arm per dialect (`random()` on DuckDB, Postgres and Trino, `RAND()` on H2, MariaDB and BigQuery,
  `rand()` on Databricks and the engine's H2 text, `randCanonical()` on ClickHouse, `UNIFORM(0::FLOAT, 1::FLOAT,
  RANDOM())` on Snowflake, SQLite's signed 64-bit `random()` shifted and scaled). `randomUUID()` is Pure's
  `generateGuid()`, the database's uuid as text (`SqlFn.GUID`, which every dialect already spelled).
- **Once per row, never per place:** `WITH rand() AS r RETURN r = r` answered false: the lowering substituted the
  projection's expression into the consumer (`random() = random()`). A non-deterministic call draws afresh at
  every place it is written, so `Fold.nondeterministic` (RANDOM, GUID) now isolates the projection as a window
  or a reducer does (`scalarInlineable`, `whereSafe`): the value is computed once, in its own select, and the
  consumer reads the column. The clocks need none of it: `now()` and `today()` are one value per statement on
  every database.
- **The clocks are the statement's:** `date()` is Pure's `today()`, `localdatetime()` its `now()`, `localtime()`
  the time of day of `now()` (`timeOf`), `datetime()` the instant `now()` in UTC (the session's zone, pinned;
  Neo4j's default), `time()` that time of day in UTC; the three named clocks are the same reads (Neo4j's
  `realtime` differs by its microseconds within a statement, a register row, S77); a null argument is null
  (Temporal4/[13]); a time zone argument is refused by name (no TCK scenario spells one). `count(rand())` is
  Neo4j's error ("Can't use non-deterministic (random) functions inside of aggregate functions", Return6/[15]).
- **Still refused by name, recorded:** `timestamp()` (the epoch milliseconds of now: no TCK scenario), a clock
  with a time zone.

#### 12.51.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4598 | 3141 (from 3,110) | 0 | 0 | 0 | 545 | 731 | 181 |
| Postgres | 4598 | 2780 (from 2,749) | 0 | 0 | 0 | 906 | 731 | 181 |

12 lite cases in `lite/clock.yaml`, each judged by Neo4j (11 PASS, `count(rand())` BOTH_ERROR as Neo4j's error). The
TCK: agreement 3201 of 3880 (from 3,181): 20 more scenarios, Temporal4 (15: the clocks' `.transaction`, `.statement`
and `.realtime` forms of null), Temporal10 (5: `duration.inSeconds` of a clock with itself); no case lost. The lite
cases found two things the TCK does not reach: `WITH rand() AS r RETURN r = r` false until the lowering stopped
substituting a non-deterministic projection, and Neo4j's `realtime` clock differing from its `statement` clock within
one statement (S77; the case pins the near-equality).

### 12.52 Pattern predicates over rows, and comprehensions over lists the database holds, 2026-10-17

- **The frame (the plan's W12):** two refusals with the same root, "the value is not where the reader looks": a
  pattern predicate in a WHERE over rows (a cartesian product's `MATCH (n), (m) WHERE (n)-[]->(m)`, a WITH's, a
  fanning graph's materialized rows: Pattern1, the lite predicates refused since §12.40), and a list comprehension
  or quantifier over a list the database holds as a Variant (a collected list, a list through CASE or coalesce,
  a list plus a value: Quantifier9-12 behind `rand()`, List12).
- **A pattern predicate over rows is a flag the rows carry (`withPatternFlags`, `patternFlags`):** before the
  WHERE is read, each pattern predicate or EXISTS subquery of it whose named nodes and relationships the rows all
  carry with their identity is read as its own graph from the carried node's class (§12.49's re-anchoring, split
  into `anchored` and the join), its rows narrowed to the identity columns of the shared variables and
  deduplicated (a pattern matching several times is one match of the predicate), a constant beside them, and
  left-joined back on those identities; the predicate's value is the constant's presence on the row
  (`patternFlag`), which NOT, AND and OR compose as any boolean, and the flag columns are dropped after the filter
  (`withoutFlags`). The WHERE sites: the product's (`cartesian`), a re-anchored MATCH's, a fanning graph's deferred
  WHERE (`materialize`), and a WITH's (the flags on the rows before the projection, which drops every synthetic
  column). A predicate reading a lambda's variable keeps the graph reading (`hopped`, which now reads through a
  quantifier's or comprehension's binding to the graph beneath it: the lite case `any(x IN [1, 2] WHERE
  (a)-[:T]->())`). A node the rows carry as one of several classes re-anchors from every class (the pattern
  label-less at its first node, the join on identity keeping the row's own class), which lifts §12.49's refusal
  for a MATCH after rows too; a bounded variable-length relationship after rows unrolls in the pattern's own
  graph (§12.42); a relationship of several types matches through the types the model has (one whose every type
  is unknown matches nothing, as before).
- **A list the database holds reads as the collection it is (`asCollection`, `Coll.elementDetail`):** a Variant of
  known shape (`ListOf`) was already read back by `toMany(@Kind)` for a comprehension of scalars; now elements the
  database holds as Variants too (collected relationships, nodes, lists: `toMany(@Variant)`, the elements' mark on
  the bound variable, so `type(x)` reads inside), `single` over such a collection (the holds counted), `reverse`
  and `tail` over it, `list + value` and `list + list` (`concatenate`; a value that may be null makes the result
  null, Cypher's, held as a Variant of the list's shape), a CASE's results' shapes and coalesce's arguments'
  merged (`mergedShape`) so the list keeps its kind through them, `labels(n)` a list of strings. `%` inside a
  lambda the database computes is read when its divisor is a literal that is not zero: the lowering spells no
  divide-by-zero guard for such a divisor (the guard's raise envelope typed as text where Postgres wants the
  remainder's integer, the reason the refusal was there).
- **Two findings of the Postgres lane, fixed below the translator:** (1) a comprehension over a CASE of a Variant's
  member (`WITH CASE WHEN m.f THEN m.l ELSE [0] END AS c RETURN [x IN c WHERE x > 0 | x]`) raised "Cannot cast a
  collection of size 0" before any row was read (`EXPLAIN` alone fails): Postgres estimates an `unnest`
  argument's array length while planning, evaluating its constant sub-expressions, the `toOne` guard's raise (a
  stable cast of a constant text) among them, even in an arm no row reaches, and it pulls simple subqueries up
  first, so the argument holds by then the expression of every column it reads: the raise was in the WITH's
  projection (the guard on `m.f`), not in the `unnest` argument as written, which is why a guard that looked for
  a raise in the argument caught nothing. The dialect now writes every `unnest` through one writer that holds the
  argument behind a scalar subquery (`unnest((SELECT ...))`, `Postgres.unnest`, eleven sites: UNWIND's lateral,
  comprehensions, reductions, map entries), which the estimate reads as a parameter it does not open (measured on
  the embedded Postgres 16.15 by bisection: the same lateral plans with the raises replaced by NULL, with the
  subquery kept from pulling up by `OFFSET 0`, or with the argument held; fails bare). (2) `list + value` lowered the
  value's side as `CASE WHEN v IS NULL THEN [] ELSE [v] END`, and the empty literal had no type, which Postgres
  refuses ("a list literal of Unknown[]"): an empty `ArrayLit` now keeps the type it is given, the one-element
  side's (`ListEncodings.concatSide`).
- **Still refused by name, recorded:** a pattern comprehension over rows (`[p = (a)-->(b) | p]` with both ends
  carried: a collect per row, Pattern2/[3], [8], one lite case); a comprehension producing values that may be null
  (Pure's `map` drops an empty result); a hop from a node read out of a list (`[x IN nodes(p) | size([(x)-->() |
  1])]`, Pattern2/[7]); `nodes(x)` of a path held in a list (List12/[4], [5]); a comprehension over a literal list
  beside a node value (`MATCH (n:A) RETURN n, [x IN [1, 2, 3] WHERE x > 1 | x]`: the resolver's object-space
  lambda reads no `filter` over a literal collection, "TypedFilter is not substitutable", logged in the plan's §5).

#### 12.52.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4614 | 3232 (from 3,141) | 0 | 0 | 0 | 470 (from 545) | 731 | 181 |
| Postgres | 4614 | 2871 (from 2,780) | 0 | 0 | 0 | 831 (from 906) | 731 | 181 |

The 16 lite cases of `lite/overrows.yaml` PASS on both lanes, judged by Neo4j (`snap.sh --check`: 0 problems over
4531 cases). The TCK: agreement 3270 of 3880 (from 3,201): 69 more scenarios on each lane, none lost: Quantifier11
(20), Quantifier12 (15), Quantifier9 (15), Quantifier10 (5), Pattern1 (3), List12 (2), Quantifier5, 6, 7 and 8 (2
each), Match7 (1); Postgres 2920 of 3880 (from 2,851). The Quantifier9-12 scenarios had been REFUSED on Postgres by
the untyped empty literal and then, with that fixed, raised at planning: both findings above.

### 12.53 The tail's small shapes: a hidden sort key, string predicates by kind, collect(DISTINCT), a duration's date-time text, 2026-10-17

- **The frame (the plan's W12, leg 2):** the small refusals the DuckDB table showed after leg 1, each a shape of
  its own: an ORDER BY on an expression the query does not return (ReturnOrderBy2, ReturnOrderBy4), `STARTS
  WITH`, `ENDS WITH` and `CONTAINS` on a value that is not a string (String8-10/[8], Precedence4/[4]),
  `collect(DISTINCT x)` (Aggregation5/[2], Aggregation8/[3], [4]), a duration text in the date-time form
  (Temporal2/[7] #7).
- **An ORDER BY on an expression the query does not return renders (`Render.withSortOutputs`):** the translator
  had sorted by a hidden column and selected it away, which is legal SQL (`SELECT a FROM ... ORDER BY h`), but
  the lowering's result render hoists an ordered relation's order out of the row subselect into the aggregate
  that builds the wire (`hoistOrder`), where only an output column can be read, so it walled ("render over an
  ordered relation whose sort key is not an output column"), and the translator refused the shape unless a SKIP
  or LIMIT made the sort a subquery's (§12.4, the review's finding 6). The render now gives the ordered relation a
  hidden output per sort key the projection dropped (`_sortN`, the key's expression, typed by its fact), before
  it reads the hoist; the rows' rendering is built from the outputs read before, so the key never shows. The
  translator's refusal is gone; the same hidden key spells with and without a SKIP or LIMIT.
- **A string predicate on a value that is not a string is null (Neo4j's reading):** `12 STARTS WITH '1'`,
  `'abc' ENDS WITH 3`, `true CONTAINS 'r'` are null, spelled as the null boolean with no string test; a value the
  rows hold as a Variant (an UNWIND of a mixed list) is its string when it holds one, else null: lite's `isKind`
  native, the database's reading of the Variant's JSON kind (`SqlFn.JSON_TYPE` against the dialect's own kind
  name, `SqlExpr.JsonKindLit`; the kind named as Cypher names them, `string`, `number`, `boolean`, `list`,
  `map`, `null`), which legend-pure's Variant has no function for (the governance ceiling 23 → 24, the census's
  row). `x STARTS WITH y` over two Variants reads both by kind: `['ab', 1, 'b']` against itself gives two true,
  two false and five null, as Neo4j does.
- **`collect(DISTINCT x)` is the distinct texts joined (register row S78):** `STRING_AGG(DISTINCT ...)` over the
  elements' JSON texts, the list rebuilt from them as `collect(x)` is (§12.44); the order is the database's,
  where Neo4j's is the encounter order of the first occurrences, which the TCK reads ignoring the elements'
  order (Aggregation5/[2]) or over one element (Aggregation8/[3], [4]): tried and withdrawn in §12.44 for that
  order, adopted with the row. `size(collect(DISTINCT p.name))` counts the distinct values; §12.44's refusal pin
  (`lite/collect`'s `collect_distinct_is_refused`, `[1, 1, 2]` collected distinct) ran as `[2, 1]` against
  Neo4j's `[1, 2]` the moment the refusal went, the row's case in one line, and is now the order-free
  `collect_distinct_counts_the_distinct_values`.
- **A duration text in the date-time form reads its fields:** `duration('P2012-02-02T14:37:21.545')` is Neo4j's
  `P2012Y2M2DT14H37M21.545S`; the text is read character by character (the translator grows no grammar of its
  own: the guardrail refuses a regex in it), years, months, days, hours, minutes, seconds and the seconds'
  fraction to the nanosecond, each a field of the map form, negated as a whole by a leading sign.
- **Probed, recorded, still refused by name:** `toString` of a list, a map or a node is Neo4j's own text
  (`[1, a, [2, true]]`: the elements' texts unquoted; `{a: 1, b: x}`; a node its labels, `(:A)`), not a JSON
  text (lite/tail's `to_string_of_a_list_a_map_and_a_node_refused`, judged); `SKIP` with a variable-free
  expression (`SKIP toInteger(rand()*9)`, ReturnSkipLimit1/[3]) needs a row count the MIR holds as an expression
  (its `limit` and `offset` are constants, `ConstBounds`); the time of a date-time in a named zone
  (Temporal3/[3] #16-20) needs the zoned time's offset read by the database at that date, where the zoned time
  carries its zone as a written detail; an untyped relationship in a MATCH of several patterns (Match3/[10],
  With1/[2], MatchWhere2/[1], WithWhere2/[1]) is the untyped arms' machinery over a second pattern; the ORDER BY
  of a list the database holds as a Variant (18 cases) and min/max over mixed values are W11's, Cypher's type
  rank. The §5 of the plan records the first three.

#### 12.53.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4626 | 3259 (from 3,232) | 0 | 0 | 0 | 455 (from 470) | 731 | 181 |
| Postgres | 4626 | 2898 (from 2,871) | 0 | 0 | 0 | 816 (from 831) | 731 | 181 |

The 12 lite cases of `lite/tail.yaml`, 11 PASS on both lanes and one REFUSED by name (the `toString` probe), and
`lite/collect`'s renamed `collect_distinct_counts_the_distinct_values`, judged by Neo4j (`snap.sh --check`: 0
problems over 4543 cases). The TCK: agreement 3282 of 3880 (from 3,270): 12 more scenarios on each lane, none
lost: ReturnOrderBy2/[8], ReturnOrderBy4/[1], [2], ReturnOrderBy5/[1] (the hidden sort key), String8/[8],
String9/[8], String10/[8], Precedence4/[4] (the string predicates by kind), Aggregation5/[2], Aggregation8/[3], [4]
(`collect(DISTINCT)`), Temporal2/[7] #7 (the duration's date-time text); Postgres 2932 of 3880 (from 2,920).

### 12.54 Deep patterns cut at two steps, and an absent node's identity, 2026-10-17

- **The frame (the plan's W12 leg 3, W9's depth-two limit):** a pattern navigating more than two association
  steps from its first node was refused by name ("a pattern navigating more than two association steps from its
  root", 13 cases), as was a hop from a node reached through an edge class (18: an edge hop is two steps, the
  edge instance and its far end). The limit is the resolver's: a navigation chain past a second association
  step reads in a projection and in a filter on a leaf property (`$a.T.U.V.name`), but not in an existence
  filter (`$a.T.U.V->isNotEmpty()`: "property 'V' of nested navigation 'T.U' is not mapped"; past a
  bridge-table step at two steps already, `UnfoldableRef`, §12.50), and a hop that may find nothing is spelled
  as that existence. The island does not wait for the resolver: it cuts the pattern.
- **Segments of at most two steps, met by identity (`segments`, `deep`):** the hops are walked from the first
  node (its label's class; a label-less first node the class its first relationship's type leaves, as `rooted`
  reads it), each the steps its navigation spells (`Hop.steps`); where the next hop would pass two, the node
  before it ends a segment and starts the next, named (freshly where the query did not). The first segment is
  read as a graph (over the MATCH's graph in hand, where there is one) and materialized; each next segment is a
  MATCH over the rows so far, re-anchored on the cut node by identity (§12.49's `reanchor`: the segment read as
  its own graph from the carried node's class, joined on the identity columns), so a cycle closing across a cut
  (`(x)-[:KNOWS]->(y)-[:KNOWS]->(z)-[:KNOWS]->(x)`) is the join on both ends. The MATCH's WHERE and its
  property maps on far nodes hold over the final rows; relationship uniqueness holds within a segment by each
  segment's own reading and across segments by the relationships' identities (an edge's own, or the ordered pair
  of its ends, S74): a relationship whose type the whole pattern repeats is named before the cut, so the pairs
  can be spelled. Four hops cut twice; a deep pattern after a WITH or over a first MATCH's graph re-anchors from
  the carried node first. The routes the cut does not reach yet: a pattern with an untyped hop (the arms' route
  reads each arm as one graph), a named path (its value spans the segments), a variable-length chain, an OPTIONAL
  MATCH; recorded in the plan.
- **An absent node's identity was its class's prefix (the resolver's `identityText`, fixed):** the identity of
  an instance is `'<class>:' + toString($x.key)`, a run of Pure's `plus` over `String[*]`, which the database
  spells as `concat`, and `concat` skips a NULL argument on DuckDB and Postgres, so the far node of a fanning hop
  that found nothing read as present (`'g::P:'`, never null): the two-hop `MATCH (d)-[:KNOWS]->(n)-[:KNOWS]->(w)`
  returned a row with `w` null, which the cut exposed on four hops (`lite/depth`'s `four_hops_two_cuts`). The
  resolver gated an optional instance's identity on its key's presence already; it now gates every read, since
  an element of a to-many navigation is `[1]` in Pure yet reads as a null row where the navigation finds none.
  The island's presence test (`present`, §12.50) is right again on both lanes.
- **`count(DISTINCT r)` over rows counts identities (Return6/[16], caught by the cut):** over a graph the count
  of a pattern variable was its distinct identities (§12.47); over rows (after a WITH, or after a cut) it fell
  to the generic reading, the relationship's value, which is one for two relationships alike (`{"@type": "ATE",
  "times": 7}` twice: Peter's bread and veggies), so `count(DISTINCT r2)` undercounted. The row scope now answers
  a relationship's identity (an edge's own, or the ordered pair of its ends, S74) as it answers a node's, and
  `count(DISTINCT x)` counts identities wherever the scope has one (lite/depth's
  `distinct_relationships_alike_counted_over_rows`).

#### 12.54.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4636 | 3273 (from 3,259) | 0 | 0 | 0 | 451 (from 455) | 731 | 181 |
| Postgres | 4636 | 2912 (from 2,898) | 0 | 0 | 0 | 812 (from 816) | 731 | 181 |

The 10 lite cases of `lite/depth.yaml` PASS on both lanes, judged by Neo4j (`snap.sh --check`: 0 problems over
4553 cases). The TCK: agreement 3285 of 3880 (from 3,282): 3 more scenarios on each lane, none lost,
MatchWhere2/[2] and WithWhere2/[2] (`(advertiser)-[:ADV_HAS_PRODUCT]->(out)-[:AP_HAS_VALUE]->(red)<-[:AA_HAS_VALUE]-(a)`,
three typed hops from a label-less node) and Return6/[16] (two edge hops from a label-less node, `count(DISTINCT r)`
over the rows); Postgres 2935 of 3880 (from 2,932). The typed cut alone reaches few scenarios: the deep patterns
the TCK spells mostly carry an untyped hop (the arms' route), a named path or a variable-length relationship, leg
4's.

### 12.55 The cut under the arms' route: untyped hops, and one identity for a relationship across arms, 2026-10-17

- **The frame (the plan's W12 leg 4, step 1):** the deep patterns the TCK spells mostly carry an untyped hop
  (`MATCH (a:A)-[:KNOWS]->(b:X)-->(c)`, the triadic selections; `(a:A)-->(n)-->(m)`; `(person:Person)<--(message)
  <-[like]-(:Person)`; `()-[r1]->()<--()`), read by the arms' route (§12.17: one typed arm per way the model has),
  where an arm with an edge among its ways runs past two steps and was refused ("a hop from X, a node reached
  through an edge class"). §12.54's cut reached only the plain typed route.
- **An untyped hop is a segment of its own (`cuts`, `segments`, `untypedArms`):** the walk reads a hop without
  one type, or one from a node whose class it does not know, as two steps, so it opens a segment before and
  after it; the arms' route, once its arms are enumerated, asks whether any arm needs a cut (an edge among its
  ways) and, if so, cuts the written pattern instead: the first segment is a MATCH of its own (an untyped one
  the arms' route reads, a typed one the graph's), materialized, each next segment a MATCH over the rows
  re-anchored on the cut node (`untypedArmsFrom`'s and `reanchor`'s readings, §12.32, §12.49). A shallow untyped
  pattern keeps the arms' route as it was. A hop with no inverse end, cut off into a segment of its own, reads
  from its other end (`(f:Firm)-[:hq]->(c:City)<-[:hq]-(g:Firm)`, refused "no inverse" since §12.4, reads).
- **One identity column for a relationship across arms (`carryRel`):** a relationship the arms read as several
  types or edge classes carried no identity (its edge classes differ per arm, and the pair form was the
  association's alone), so the uniqueness across a cut could not be spelled, and `MATCH ()-[r1]->()<--() RETURN
  sum(r1.times)` (Match8/[3]) summed the rows where the two were one relationship. The arms now carry one
  identity column, each arm's own text (an edge's row identity, an association's ordered pair of ends, S74), the
  same column in every arm, so the union compares relationships across rows: the cut's uniqueness, over every
  pair of relationships that may be one (two typed differently never are), and `count(DISTINCT r)`. Where a pair
  still carries no identity, the cut refuses by name rather than answer without the uniqueness.
- **Still refused by name, recorded:** an untyped relationship in a MATCH of several patterns (MatchWhere2/[1],
  Match3/[10], With1/[2], WithWhere2/[1]); a named path over more than two steps (Match6/[3], [10], [11], [16]);
  a variable-length chain past two hops, bounded (Path2, Match9/[2]-[4], Pattern1/[18]) or not (Match4/[8],
  Match9/[6], [7]: W10's recursion).

#### 12.55.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4640 | 3291 (from 3,273) | 0 | 0 | 0 | 437 (from 451) | 731 | 181 |
| Postgres | 4640 | 2930 (from 2,912) | 0 | 0 | 0 | 798 (from 812) | 731 | 181 |

The 14 lite cases of `lite/depth.yaml` (the 10 before and four new: two untyped hops with an edge among the ways,
a triadic selection with an OPTIONAL MATCH after the cut, an untyped chain from a label-less node with uniqueness,
an incoming untyped hop then a named incoming relationship) PASS on both lanes, judged by Neo4j (`snap.sh --check`:
0 problems over 4557 cases). The TCK: agreement 3299 of 3880 (from 3,285): 14 more scenarios on each lane, none
lost: the nine triadic selections TriadicSelection1/[7]-[11] and [16]-[19] (`(a:A)-[:KNOWS]->(b)-->(c)` with an
OPTIONAL MATCH after), Match7/[9] (a longer pattern with bound nodes), Match8/[3] (`()-[r1]->()<--()` summed over
one relationship's identity), ReturnSkipLimit2/[8], Return4/[11] and With4/[6] (`(a)-->(b)-->(c)` reusing names);
Postgres 2949 of 3880 (from 2,935). What the cut still refuses by name is the paragraph above's: an untyped
relationship in a MATCH of several patterns, a named path over the cut, a variable-length chain past two hops.

### 12.56 A named path over the cut: partial paths, directions as columns, the path assembled over the rows, 2026-10-17

- **The frame (the plan's W12 leg 4, step 2):** a named path deeper than the cut (`MATCH p = (a {name: 'A'})-[rel1]->(b)
  -[rel2]->(c) RETURN p`, Match6/[3]; `p = (n)-->(m)--(o)`, Match6/[10]; `path = (n)-->(m)--(o)--(p)`, Match6/[11])
  was refused: the cut of §12.54 and §12.55 reads unnamed patterns only, since a path's value (§12.27) is built in
  one graph from its bindings and the graph's `forward` map (each relationship's stored direction in the written
  order), and a cut's segments are materialized to rows before the whole path exists.
- **A segment of a named path is a partial path (`SEGMENT`, `PathSpec.partial`):** the cut names every node and
  relationship first (`namedPath`), then records each segment under a synthetic name (`_cypher_seg_<p>_<k>`) as a
  partial path: a path a graph records but never materializes (no JSON, no parts), whose relationships carry their
  stored direction as a column (`RelShape.forward`, `carryRel`: a constant of the arm, since an undirected hop
  reads either way one arm at a time, null for an absent relationship). Re-anchoring over rows (`anchored`) keeps a
  segment's name, so the pattern's own graph records it too; a segment starts at the node the rows carry (never
  reversed there).
- **The whole path assembled over the rows (`withPath`):** after the segments are read and the uniqueness across
  them filtered, the rows are extended with the path's JSON (§12.27's shape: each node's text from its columns,
  each relationship's from its type and properties, the `@dirs` from the direction columns, negated when the cut
  read the pattern from its last node), its length (a constant), its nodes and its relationships (the Variants the
  database builds), under the names a materialized path's parts carry (`_cypher_path_<fn>_<p>`), so `length(p)`,
  `nodes(p)` and `relationships(p)` read them as they read a variable-length pattern's. The MATCH's WHERE runs
  after, so it may read the path.
- **Reaches:** Match6/[3], [10], [11] (two and three untyped hops, undirected among them) through the arms' route's
  cut; `lite/depth.yaml` pins a named path over two untyped hops, one with an undirected hop after the cut, one cut
  twice with its parts, one with a WHERE on its end.

### 12.57 Variable-length chains past two steps as rows per chain; lists as keys; a Variant beside a scalar, 2026-10-17

- **The frame (the plan's W12 leg 4, step 3):** a variable-length relationship whose unroll (§12.42) runs past two
  steps (an edge hop counting two: `(a)-[r:REL*2..2]->(b:End)` over an edge class, Match9/[2]-[4], Path2/[1], [2],
  Match6/[16]; three hops, Match6/[14]; `*0..3` from a label-less start, the quantifiers' Quantifier1-4/[8]) was
  refused ("unrolls to more than two hops in all", or "a hop from a node reached through an edge class"): the
  arms' route reads every chain as one graph, and the unroll's nodes and relationships are hidden (never columns),
  so the cut could not re-anchor on them.
- **Each chain read as rows of its own (`chainRows`):** when any arm of any chain needs a cut, every chain is read
  as a MATCH of its own (the plain or the arms' route, cut around its hops where it runs past two steps, §12.54-
  §12.56), its hidden names renamed afresh so the rows carry every node and relationship; the relationship's list
  is the Variant of the chain's relationships' values (empty for a zero-length hop, whose far node reads the near
  node's columns); the MATCH's WHERE filters each chain's rows. A relationship of a type the chain repeats is named
  by the cut, so relationship uniqueness along a long chain is spelled by identity where it was not along a short
  one (S75's remaining gap narrows to the chains the arms' route still reads).
- **The chains' rows under the same columns (`canonical`):** the union of chains of different lengths needs the
  same columns in every chain, which fresh names from different readings do not give. Each chain's rows are
  projected to the pattern's own variables with the fresh-name counter reset before each (as `materializeArms`
  resets it per arm): a node as its labels, the union of its properties across the chains (null where this chain's
  shape lacks one, marked absent) and its identity; a relationship as its type, its properties, its identity, its
  pair and its direction where any chain carries them; the lists and the path's columns under their names (a
  constant the chain carried, an empty path's relationships, made a column). The chains concatenated, the list
  shapes merged (`mergedShapes`, as the arms' are).
- **The cut's walk never asks a hop of several ways (`cuts`, `edgeWayCount`; `segments`, `startClasses`):** a type
  naming several edge classes read from a class (`-[:REL]-` from a node that is the source of one and the target
  of another) is two steps, the arms' route's; a label-less first node whose outgoing type several classes declare
  has no class (two steps likewise), where `startClassOf` refused by name.
- **A list or map among the grouping keys, or under DISTINCT (`scalarShaped`):** the Variant the database builds
  is the key, equal as text, when its elements are scalars, lists or maps of them (`WITH [p.a, p.b] AS l, count(*)
  AS n`); one holding a node or a relationship (equal by identity in Neo4j, which the text lacks: two alike nodes
  would be one key), or an element of a shape the spelling does not say, is refused by name (S79). Quantifier1-4/
  [9] (`WITH tail(relationships(p)) AS relationships, COUNT(*) AS c`) stay refused by it.
- **A Variant of no known shape beside a scalar (`variantEqual`):** `x.name = 'a'` where `x` is a node the
  database holds as a Variant (an element of `nodes(p)`, a collected node) read `x.name` as a Variant of no known
  shape and refused the comparison ("comparing a Map with a String"). Now `v = x` for a scalar `x` of a kind the
  Variant reads back by type (string, boolean, number) is equal where `isKind(v, kind)` holds and `v->to(@T)`
  agrees with `x` (a number as a float, S80), false where the Variant holds another kind (Cypher: values of
  different types are never equal), null where either is null or the Variant holds the JSON null; `v <> x` is its
  negation (`not` of the condition). A collection of Variants keeps its elements' mark through `tail`, `reverse`
  and `toVariant` (`shapeOf(Coll)`: a list of Variants with their mark), so `tail(nodes(p))` through a WITH reads
  back as nodes.
- **A list predicate whose condition may be null, over a list the database computes:** `none(x IN nodes WHERE
  x.name = 'a')` was refused (Pure's `forAll` and `exists` read a two-valued condition). Now the elements the
  condition holds for, fails for and is unknown for are counted (`filter` and `size`, three lambdas over the
  collection) and combined by Cypher's rules, as `single` over a spelled list was: `any` true when one holds, false
  when none holds and none is unknown, else null; `all` false when one fails, true when none fails and none is
  unknown; `none` the negation of `any`; `single` true when exactly one holds and none is unknown, false when two
  or more hold or none holds and none is unknown.
- **A list comprehension producing values that may be null:** `[y IN ns | y.k = 1]` over a computed list was
  refused (a collection of values drops a null). Now each element's JSON text (the null spelled) is joined as the
  list's JSON the database builds (as `collect` spells one), a Variant of no known shape (a null among the
  elements is not what a reader by kind reads back; a reader refuses by name).
- **Still refused by name, recorded:** a variable-length relationship after another clause (Match4/[6]), in an
  OPTIONAL MATCH (Match7/[12]-[14], Match9/[8]) or beside another pattern; one without an upper bound (W10's
  recursion); a list holding nodes or relationships as a grouping key (Quantifier1-4/[9]); an undirected
  relationship over an edge class whose two ends are one class (a loop, identity's); a property typed differently
  across the classes a label-less node reads (W11's Variant columns).

#### 12.57.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4655 | 3322 (from 3,291) | 0 | 0 | 0 | 421 (from 437) | 731 | 181 |
| Postgres | 4655 | 2961 (from 2,930) | 0 | 0 | 0 | 782 (from 798) | 731 | 181 |

The 29 lite cases of `lite/depth.yaml` (the 14 before and fifteen new: a named path over two untyped hops, one with
an undirected hop after the cut, one cut twice with its parts, one with a WHERE on its end; a variable-length hop
over edges past two steps, a named one of lengths one and two, an undirected one from both ends, one from zero with
its list and path, a pattern predicate with a variable-length of two over edges, one past two steps with a WHERE on
the far end; the tail of a deep path's nodes under a quantifier; a list of scalars among the grouping keys, WITH
DISTINCT over one, a list of nodes refused; a Variant property beside a number, a string and a boolean) PASS on
both lanes but the designed refusal, judged by Neo4j (`snap.sh --check`: 0 problems over 4572 cases); two refusal
pins turned positive (`lite/uniqueness`: a variable-length chain past two steps walks no relationship twice;
`lite/varlen`: a variable-length unrolling past two hops reads). The TCK: agreement 3314 of 3880 (from 3,299): 15
more scenarios on each lane, none lost: Match6/[3], [10], [11] (named paths over two and three untyped hops,
undirected among them), Match6/[14] (a named path with an undirected variable-length of three), Match6/[16] (a
variable-length named path over edges), Match9/[2]-[4] and Path2/[1], [2] (`-[r:REL*2..2]-` over edges, directed and
undirected, `r` and `relationships(p)`), Pattern1/[18] (a pattern predicate with a variable-length of two) and
Quantifier1-4/[8] (`none`, `single`, `any`, `all` over `tail(nodes(p))` of a `*0..3` path, `x.name = 'a'` on a
node the database holds); Postgres 2964 of 3880 (from 2,949).

### 12.58 Cypher's orderability over Variants: the type rank as a text key, 2026-10-17

- **The frame (the plan's W11):** Cypher orders values across types by a type rank -- a map before a node, a
  node before a relationship, then a list, a path, a string, a boolean, a number, null last ascending -- and
  within a type by the value, a list element by element with a prefix first; `ORDER BY` a Variant (a stored
  list, a mixed `UNWIND`, a path), `min` and `max` over Variants, `<` and `=` between Variants were refused by
  name (36 TCK scenarios: ReturnOrderBy1/[9]-[12], WithOrderBy1/[9], [10], [21], [22], [31], [32], [45],
  WithOrderBy2/[9], [10], WithOrderBy4/[15], With3/[1], Aggregation2/[5], [6], [9]-[12], Comparison2/[3],
  Return2/[7]).
- **The orderability key (`variantKey`, `keyOf`):** one text the database builds per Variant, ordering as Cypher
  does: the rank digit first (`0` map, `1` node, `2` relationship, `3` list, `4` path, `5` string, `6` boolean,
  `7` number, `8` beyond the largest double -- NaN and the infinities, which the databases order beyond every
  number -- `9` null), then the value: a number as the text of its millionths offset by 10^18, left-padded to
  twenty digits (S81: exact within ±9e12 and to the millionth); a string as its text with the key's marks
  escaped; a boolean as `0` or `1`; a list as its elements' keys joined by a separator and closed by an end mark
  lower than the separator (so a prefix orders first, as Cypher's element-wise order does), the elements' keys
  spelled recursively to depth three (deeper, a list is its JSON text); a map, node, relationship or path as its
  JSON text (S81: Neo4j orders nodes and relationships by identity, which the text is not). The kinds are lite's
  `isKind` (§12.53) and the island's `@labels`, `@type` and `@path` marks; the marks are `U+0001`, `U+0002` and
  `U+0003` (no NUL, which Postgres's text refuses).
- **ORDER BY (`SortKey`):** a Variant sort key is a hidden column holding the key: after a grouping or a
  DISTINCT in the `extend` that computes the keys over the grouped rows; in a plain projection the Variant itself
  is the hidden column of the projection and the key is an `extend` over the rows after it (`keyed`): the key's
  lambdas (`toMany`, `map`, `joinStrings`) lower over rows, not inside a projection over a class extent, which
  WithOrderBy2/[9], [10] (`WITH a ORDER BY a.list`) found. A carried list's and a stored list property's sort
  keys take the same route (the refusals "ORDER BY a list or map", "ORDER BY a list, a map or a path the database
  holds as a Variant" are gone; a node among the keys stays refused: Neo4j orders nodes by identity).
- **min and max (`Aggregate.keyColumn`, lite's `minBy` and `maxBy`):** over Variants, the value at the smallest
  or the largest key: two lite natives (`meta::legend::lite::minBy`, `maxBy`) over `rowMapper(value, key)` rows
  with a TEXT key, the lowering's ARG_MIN and ARG_MAX (every dialect renders them: DuckDB's `arg_min`, Postgres's
  ordered `array_agg`); Pure's own `minBy` and `maxBy` take a number key, which a rank is not (the governance
  ceiling 24 → 26, `NativeCatalogGovernanceTest`, with the justification). A null Variant's key is null, so the
  reducer skips it as Neo4j's `min` and `max` skip nulls. `max([1, 2.0, 5, null, 3.2, 0.1])` keeps the value's
  own type (the Variant `5`, not a float).
- **Comparisons (`variantOrdered`, `variantsEqual`, `variantEqual`):** `<` and the others where a side is a
  Variant: by the keys where both are of one rank the key orders by value (a string, a boolean, a number, a
  list), null where the ranks differ (Cypher: values of different types do not order) or either is null; `=`
  between two Variants of no node shape by the keys (a number as a float, `[1] = [1.0]` true as in Cypher; a
  list element-wise; a map, node or relationship as its text, S81); a scalar beside a Variant by its own key
  (`scalarKey`). A list the database computes beside a spelled list, in `=` or `<`, takes the same route
  (`equalVal`, `orderedVal`: `collect(n) = range(1, 3)`, `[x IN values WHERE x < value]`).
- **Spelled lists read as Variants (`collection`):** a spelled list of lists or maps, of values of several
  kinds, or holding a null, read by the database (a comprehension, a quantifier, `+`), is the Variant the
  database builds of it with its elements read back as Variants (`toMany`): the refusals "a list of lists or
  maps, read by the database", "a list holding values of different types", "a list holding a value that may be
  null" are gone (a null element is the JSON null, an element, as Cypher keeps it); such a list indexed by a value
  of the row is the Variant indexed (`variantIndex`). A mixed list's `UNWIND` makes every element a Variant
  (`asVariant`), a scalar the row computes among literals too (`0.0 / 0.0` beside `1.5` typed the `if` chain
  `Any`).
- **A list beside a Variant in `+` (`concatVariant`):** `list + x` where `x` is a Variant of no known kind
  (Quantifier9-12: `CASE ... END + x` over `UNWIND inputList AS x`) is the list's JSON built as text: the
  elements' texts, then `x`'s elements (its text within the brackets) where `x` holds a list, else `x`'s text;
  null where `x` is null or holds the JSON null (Cypher: a list beside null is null). Built as text because a
  conditional over collections (`if(isList, |toMany(x), |x)`) has no SQL carrier (the Lowerer spells `if` as a
  scalar CASE), where one over texts has. Two stored lists (`a.list2 + a.list1`, Return2/[7]), Variants of no
  known shape, concatenate as lists (`variantElements`, S81).
- **Refused by name, recorded:** a float divided by a zero literal inside a list or map (`NotANumber`: NaN or an
  infinity in Neo4j, which Postgres's division raises on and whose JSON holds neither; ReturnOrderBy1/[11], [12],
  WithOrderBy1/[21], [22]); a list of dates the database holds as a Variant read back as values (the Variant
  spells a date as the oracle's text, which no database reads back: WithOrderBy1/[45]'s dates); lists of times
  and of nanosecond values of the year 1 (the temporal refusals of before); ORDER BY a node.

- **What the Postgres lane taught (the second chain):** three families of ERROR, each a lesson about the key.
  (1) The key's places read a Variant as a float, a boolean or a string (`to(@Float)` and the others) under the
  kind test that proves the value present; spelled `->toOne()`, each place is a `CASE` whose raise arm is a
  constant (the lowering's size-0 raise, `Scalars.guarded`), and Postgres's planner, estimating the selectivity
  of a comprehension's `WHERE` (`[x IN values WHERE x < value]`: the key of `value`, an outer row's column,
  compared with the key of the element), evaluates every stable constant in every `CASE` arm of the operand that
  names no column of the subquery -- the raise fires at PLAN time, "Cannot cast a collection of size 0 to
  multiplicity [1]" with no row read (`estimate_expression_value`; the dialect's own note on `raise` records the
  same for `unnest`'s argument, and keeps scalar raises non-volatile for the planner's sake). The key now spells
  lite's `trustOne`, the platform's unguarded to-one conformance, at those places (its own test is the guard;
  `PlatformNamesGuardrailTest` 330 → 331 with the justification). (2) A spelled list beside a Variant
  (`ns = [1, 2, 4]`) keys the literal `fromJson('[1,2,4]')->toMany(@Variant)`, and the dialects' strategy pass
  folds the elements of a literal Variant array at compile time (`CarrierStrategies`' literal-variant fold, which
  Postgres runs and DuckDB does not): it re-emitted each scalar element as a bare SQL literal, so `isKind` reached
  `jsonb_typeof(integer)`; the fold now re-emits every element as the JSON cast literal it is (a Variant array's
  elements are Variants) -- the same fold had made `lite/rank/a_mixed_spelled_list_read_by_the_database` a
  Postgres refusal (a list filter over `Unknown[]`), which it no longer is. (3) The Postgres `variantGet` of a
  computed index cast it to an integer, which broke a computed KEY (`n[k]`, Graph7/[1]); the cast now follows the
  index's MIR type (numeric: cast; text: as is). And one translator gap both lanes shared: `value < [1, 3]` with
  `value` a Variant of the row answered null (`orderedVal` saw no two spelled lists); a Variant beside a spelled
  list or map now takes the keys, as `=` did (`a_list_of_the_row_ordered_against_a_spelled_list`,
  `a_comprehension_comparing_the_lists_of_the_row`, 23 lite cases). The number key clamps beyond ±9e12 (one key
  each side, S81) where the millionths offset would overflow the database's integer.

#### 12.58.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4678 | 3386 (from 3,322) | 0 | 0 | 0 | 380 (from 421) | 731 | 181 |
| Postgres | 4678 | 3025 (from 2,961) | 0 | 0 | 0 | 741 (from 782) | 731 | 181 |

The 23 lite cases of `lite/rank.yaml` (lists element-wise with a prefix first, descending with a limit; values of
several kinds by rank, both ways; nodes and relationships before lists; a stored list ordering its rows, carried
then ordered; min and max over mixed values, over lists and numbers, over mixed numbers keeping the value's type;
comparisons across types null but numbers; ordering agreeing with comparisons over strings and over lists; two
stored lists concatenated; a computed list equal to a range; a Variant of no known shape among numbers; a list
beside a Variant by its kind; a mixed spelled list read by the database, and indexed by a value of the row; a list
of the row ordered against a spelled list; a comprehension comparing the lists of the row) PASS on both lanes but
the two designed refusals (NaN inside a list, a list of dates read back), judged by Neo4j (`snap.sh --check`: 0
problems); the quantifier and comprehension scenarios of W3's `rand()` tail PASS where they hold no NaN. TCK
agreement (PASS or BOTH_ERROR over the 3,880 TCK rows) DuckDB 3314 → 3354, Postgres 2964 → 3004.

### 12.59 Stored temporal values, leg 1: a time of day in a TIME column, a duration as an embedded instance (2026-10-17)

The TCK-to-zero plan's W5. The harness skipped every graph holding a time of day, a duration or a zoned date-time as
a property ("a kind the model does not hold yet": 61 cases, and 11 more of sub-microsecond date-times), because the
relational store grammar had no column for them (S64). This leg gives the model two of the three.

- **A TIME column (lite's extension of the relational grammar, S64 revised).** `RelationalDataType.Time` (the
  keyword `TIME`), its Pure type `StrictTime` (`StoreCompiler`), its DDL kind `TIME` spelled by every dialect's
  DDL arm as its TIME scalar is (DuckDB, Postgres, H2, BigQuery, Snowflake `TIME`; Trino and MariaDB `TIME(6)`;
  SQLite `TEXT`; ClickHouse and Databricks unspelled, as their TIME scalar is walled), the protocol's `Time`
  (parse, compose, read), the plan text and the precise types, the system metamodel's datatype class
  `meta::relational::metamodel::datatype::Time` (the prelude, the data-type kinds and their sets), the seed's
  literal. legend-engine's grammar has none: a model naming a TIME column is lite's own, which S64 now records as
  the extension it is. The harness declares a TIME column for a `localtime` property, binds it as the JDK's
  `LocalTime` (DuckDB's and pgjdbc's TIME), and the island reads it as the StrictTime it already carries: ordering,
  fields, comparison, arithmetic with a duration, `toString`, a node's value (the oracle's `Time('HH:mm:ss.f')`
  spelling, its text Neo4j's; the judge reads two local-time texts as one time whatever the fraction's digits,
  `sameLocalTime`) and the null of an absent one all come from the TIME leg's machinery (§12.6). A time of
  sub-microsecond precision is refused by the harness by name (leg 2: a `TIMESTAMP_NS` column where the database
  has one), a zoned time or date-time too (leg 2: its offset a column beside the value).
- **A stored duration: an embedded instance.** The harness declares the duration value class at
  `g::values::Duration` (so the label `Duration` is free to name a node class: the island's label lookup skips the
  model's Duration value class, the four-slot one it carries durations as) and a duration property as
  `dur: g::values::Duration[0..1]` with an EMBEDDED property mapping over four BIGINT columns (`p_dur_months`,
  `_days`, `_seconds`, `_nanoseconds`: the relational mapping's own `prop ( slot: [db] t.col, ... )` block), the
  seed filling the four from Neo4j's text (`durationSlots`: years to months, weeks to days, the time part to
  seconds and a nanosecond rest in [0, 1e9)). The island reads the property as a DURATION value
  (`storedDuration`): the resolver refuses an embedded instance projected whole ("class-typed property used as a
  whole value is graph output"), so the read is the island's own instance built of the four slot navigations
  (`^Duration(months=$d.dur.months->toOne()->cast(@Integer), ...)`, the database's struct), null where the row
  holds none (the months slot empty: an embedded instance is never null itself). Every operation over durations
  then applies unchanged (arithmetic with dates, date-times and times, fields, `toString`, `=`, `IS NULL`), on
  a labelled or a label-less node.
- **A date moved by a duration (a rule the stored durations exposed).** Neo4j adds a duration's months, then its
  days AND the whole days of its seconds (the fraction of a day cut toward zero: 29 days and 33 hours move a date
  by 30; 23 hours by none; Temporal8/[1] #3's `P12Y11M29DT33H58M13.5S`); the island added the months and days
  only. The DATE arm of `durationArithmetic` now adds `integerQuotient(seconds, 86400)` days
  (`lite/temporal/a_date_moved_by_a_duration_of_more_than_a_day`).

#### 12.59.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4692 | 3425 (from 3,386) | 0 | 0 | 0 | 383 (from 380) | 731 | 153 (from 181) |
| Postgres | 4692 | 3052 (from 3,025) | 0 | 0 | 0 | 756 (from 741) | 731 | 153 (from 181) |

The 12 stored-temporal cases of `lite/stored.yaml` (a stored time of day read and ordered, its fields and text,
compared with a spelled one, absent on a node, inside a node's value, plus a duration; a stored duration added to a
date, to a date-time and a time, its fields and text, compared and absent, on a node labelled Duration, read
label-less) and `lite/temporal/a_date_moved_by_a_duration_of_more_than_a_day` PASS on both lanes, judged by Neo4j
(`snap.sh --check`: 0 problems); the TCK's Temporal8 and Temporal5/[7] duration scenarios pass but the three
multiply-and-divide refusals of before. TCK agreement (PASS or BOTH_ERROR over the 3,880 TCK rows) DuckDB
3354 → 3379, Postgres 3004 → 3017.

### 12.60 Procedures: a Pure function over its rows, CALL joined to the rows in hand (2026-10-17)

The TCK-to-zero plan's W8. The TCK declares a procedure per scenario ("there exists a procedure test.my.proc(in ::
INTEGER?) :: (out :: STRING?):" and a table of its rows, the inputs' columns then the outputs'), and the harness
skipped the 50 scenarios of Call1-6. Neo4j runs no procedure of ours (a TCK procedure is a plugin of its runner),
so the leg has three parts: the representation, the clause, and the judging.

- **A procedure is a function of the model.** The harness declares each as a Pure function at the path its dotted
  name spells ({@code test.my.proc} is {@code test::my::proc}), its inputs the parameters (each {@code [0..1]}),
  its outputs the relation it returns, the body a typed TDS literal of the declared rows (the brace-less
  {@code #TDS ... #}, the island's own spelling) filtered on the inputs ({@code $r.in == $in}: Pure's equality
  joins a null argument to a null cell, Call4) and cut to the outputs; a procedure of no output is a function
  returning a Boolean (VOID). The island's `Model.procedure(name)` reports a procedure as the function at that path
  (`CypherIslands.Graph`: `findFunction`, the parameters, the relation's columns through the generic `Relation`),
  null when the model declares none; a model with no procedures reports none (the interface's base).
- **The CALL clause.** The parser reads {@code CALL name[(args)] [YIELD * | item [AS alias], ...]} (a dotted
  name; no parentheses: the implicit form, its arguments the query's parameters; a `CALL {` subquery refused by
  name). The translator (`procedureCall`): standalone, the function's relation is the result, every output
  without a YIELD, the yielded ones selected and renamed (each through a fresh name first, so `YIELD a AS b, b AS
  a` swaps two); after rows, the rows laterally joined to the relation of each row's arguments
  (`lateral(rows, r | f($r.x))`), the yields the new columns (nullable, of the outputs' kinds); a VOID procedure
  leaves the state as it is (a graph stays a graph; a query it starts is one row), standalone it answers no row
  (under a hidden column: a relation has one at least, where Neo4j's result has none). Neo4j's static errors are
  raised as its: an unknown procedure (ProcedureNotFound), too many or few arguments (InvalidNumberOfArguments), an
  argument of the wrong kind (InvalidArgumentType: an integer passes for a NUMBER or a FLOAT input, as Neo4j
  widens it, a null for any), an aggregate among the arguments (InvalidAggregation), implicit arguments or an
  unnamed yield inside a query (InvalidArgumentPassingMode, the TCK's UndefinedVariable and UnexpectedSyntax), a
  yield of an unknown output, of a bound name or of two outputs to one name (VariableAlreadyBound).
- **Implicit arguments.** The harness binds a scenario's parameters into the island's text (§12.16); a standalone
  `CALL test.my.proc` with no parentheses calls the procedure with its inputs' namesakes among the parameters
  (Cypher's rule), which the harness spells as the explicit call of those parameters before binding them
  (`Case.implicitArguments`), so Call2/[3] reads as `CALL test.my.proc('Stefan', 1)`. A parameter the scenario
  does not provide (Call1/[11], MissingParameter) reaches the island as `$in`, which it refuses by name: REFUSED,
  not an agreement claimed.
- **The judge.** The oracle's snapper runs a procedure case's setup and dumps its graph (Neo4j is the oracle of the
  graph, as for every case) and runs no query (`run_case`: the answer holds the graph alone); the answer's hash
  takes the declarations' text (`Procedure.text`, spelled the same by `snap.py`), so a changed table is a stale
  answer. The lane judges the case by the TCK's own expectation (`procedureOutcome`): its rows (the columns and
  the rows, in order where it says so), an empty result (no row), or an error (any error of lite's but a refusal
  by name, which stays REFUSED). A lite case declares procedures the same way (`procedures:` a declaration line in
  the TCK's form, then its table) and its own `expect:` block (`rows in order` / `rows in any order` and the
  table, `empty`, `error <type>`): `lite/procedures.yaml`, ten cases.
- **A platform fix on the way.** The TDS literal's brace form (`#TDS{...}#`, the engine's own grammar, what the
  composer prints) parsed from text kept its braces: the checker read `{` as the first header cell (every row a
  cell of one column named `{`). `TdsChecker` strips the braces now; the brace-less form, the island's own, was
  never affected (`RelationApiIntegrationTest.testTdsLiteralBraceForm`).

#### 12.60.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4702 | 3484 (from 3,425) | 0 | 0 | 0 | 384 (from 383) | 731 | 103 (from 153) |
| Postgres | 4702 | 3111 (from 3,052) | 0 | 0 | 0 | 757 (from 756) | 731 | 103 (from 153) |

The 10 procedure cases of `lite/procedures.yaml` (a standalone call yielding every output, a call over the rows
of a MATCH with an aliased yield, a VOID procedure keeping the rows, a null argument matching a null cell, a NUMBER
input taking an integer or a float, outputs swapped by their aliases, a call twice over a count, and three errors: an
unknown procedure, a wrong argument kind, a yield of a bound name) PASS on both lanes, judged by their own expect
blocks (Neo4j snaps their graphs: `snap.sh --check` 0 problems); of the TCK's 52 Call scenarios 49 PASS, 2 are
BOTH_ERROR (an unknown procedure, where Neo4j errors too) and 1 REFUSED (the implicit call whose parameter the
scenario omits). The harness's SKIP count falls by the 50 procedure scenarios. TCK agreement (PASS or BOTH_ERROR over the 3,880 TCK rows) DuckDB
3379 → 3428, Postgres 3017 → 3066.

### 12.61 Nodes with several labels: an anonymous class per label set, a label reading its class and the subclasses (2026-10-17)

The plan's W6 (decision 5, option (a), 2026-10-17). Neo4j's `(:A:B)` carries two labels at once; the model has no
class of two names. The reading, Pure's own: a node of several labels is an instance of a class that is both, a
class extending each label's class (multiple inheritance), and `MATCH (n:A)` reads A's instances and its subclasses'
(what every label does from now on: a label names its class and the classes under it). What such a class lacks is
a name that is no label: the graph profile's sixth stereotype gives it one.

- **`<<graph.Anonymous>>` on a class** (`projects/graph/model.pure`; `GraphEdges.STEREOTYPES` checks the profile's
  six, no tag): the class's name is no label; its instances carry its superclasses' labels alone. A node of several
  labels is an instance of an anonymous class extending each label's class; a node of none, of an anonymous class
  extending nothing (the harness's `Unlabelled`, honest now: the judge's strip of its name, `ownLabels`, is gone).
  Without the marker a class's simple name is a label its instances and its subclasses' instances carry.
- **A class's label set** (`Model.labels(classPath)`, `CypherIslands.Labels.sets`): its own simple name unless
  anonymous, then each superclass's set in declaration order, a label once (a diamond contributes once; a cycle is
  phase F's error). `labels(n)` lists it (`labelsJson`), `n:B` tests it (`contains` over the JSON text, as §12.35),
  a node value carries it. An anonymous class is named by no label (`Labels.bySimpleName` leaves it out: `MATCH
  (n:N_A_B)` over an open graph reads no rows, over a closed one is C4's error), but is a class of the model
  (`classes()` lists it: a label-less pattern reads its instances).
- **A label's candidates** (`labelledClass`, `labelClasses`, `candidates`, `carries`): a label names one class (a
  simple name: the one class of that name; a backticked path: the class at it); the classes a node pattern's
  labels READ are every node class of the model carrying each label (a subclass of each label's class, the class
  itself included), in the model's order; none when no class carries them all. A backticked path reads that class
  ALONE (the register's S84: a path names a class, a simple name a label; the island's own arm patterns and
  re-anchorings name classes by their paths, meaning exactly them). A label-less pattern reads every class as
  before (§12.13).
- **Reading several classes: arms.** Where a pattern's node reads one class (today's routes: a to-one graph, a
  hop's far end checked against the class a way reaches), nothing changes; where its labels read several classes,
  or none, the first pattern takes the arms route (`matchPattern` routes it as a label-less node's: `arms` for a
  node alone, `untypedArms` with hops, `rootClassesOf`), one arm per class, each read as the class's own pattern and
  unioned at the first projection. A far node's labels are a check on the class a way reaches (`carries`; a label
  under the reached class is the narrowing refusal as before); a bound node named again with a label its class
  does not carry matches nothing (`nodeConstraints` adds `false`: an arm of a class carrying it reads its own rows),
  over a graph or over arms (`MATCH (n:A) WITH n MATCH (n:B)`: the second MATCH constrains each arm). Labels
  reading several classes at a place that reads one (a hop's far end on the to-one route, a later pattern's) are
  refused by name ("reading several classes ... in this position").
- **The harness** (`CypherGraph.infer`): a node's class is its label set's. A label no node carries among others
  keeps its own class (the model as before: nothing of the corpus changes shape); a label carried among others is
  a MARKER class (empty: no property, no node, an empty table, so `MATCH (n:A)` reads it as an arm of no rows) every
  class of a set holding it extends; each label set with a node is an anonymous class `N_<labels sorted>` (table
  `n_n_<labels>`) extending each label's class, holding its nodes' properties (typed per set: `(:A {p: 1})` and
  `(:A:B {p: 'x'})` type p on different classes, and `MATCH (n:A) RETURN n.p` is then §12.13's "typed differently"
  refusal, W7's). A relationship is keyed by its ends' classes as before (§5.6: a type over several pairs, one edge
  class per pair), so every association is a LEAF class's: an inherited association end would resolve against the
  subclass's table (the resolver applies the superclass mapping's join to the subclass's main table: `FnProbe3`,
  a hole of the resolver's to close under its own guard, logged in the plan's §5), and the harness declares none.
  A label spelled as a made-up name (`N_A_B`, `Unlabelled`) or two labels differing only in case are refused by
  name. `labels(n)` lists the model's order (the harness declares a set's labels sorted); Neo4j lists creation
  order (S83): a lite case creates its labels alphabetically, the TCK's `labels()` cases read lists ignoring order
  (Graph3/[6]) or over one label.
- **Verified first, as the plan asked:** `A.all()` over a relational mapping reads A's own mapping alone (no union
  of a subclass mapped on its own table: `FnProbe3`), so the island unions the subclasses itself; a subclass's
  mapping maps an inherited property on its own table.
- **Pinned.** `CypherToPureTest.aLabelReadsItsClassAndTheSubclassesAndANodeOfSeveralLabelsIsAnAnonymousClass`
  (over a model with `demo::Manager extends demo::Person` and the anonymous `demo::N_Firm_Person`): a label's arms,
  a path's class alone, two labels' one class, `labels(m)` as `["Manager","Person"]`, an anonymous class's set,
  its name naming no class, a label set no class carries reading no rows. `lite/labelsets.yaml`: 17
  cases judged by Neo4j (a label's subclasses, two labels in either order, labels and label predicates of
  two-labelled nodes, node values of two and four labels, typed and untyped hops to a label with subclasses, an
  OPTIONAL MATCH to one, a bound node re-matched with a label of its set and with one it lacks, the labels of an
  unlabelled node, a property only the two-labelled nodes hold, the made-up name as a label, a label set no node
  carries, a count over a label with subclasses).

#### 12.61.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4719 | 3513 (from 3,484) | 0 | 0 | 0 | 388 (from 384) | 733 (from 731) | 85 (from 103) |
| Postgres | 4719 | 3140 (from 3,111) | 0 | 0 | 0 | 761 (from 757) | 733 (from 731) | 85 (from 103) |

The 17 cases of `lite/labelsets.yaml`: 17 PASS on DuckDB, 17 on Postgres, judged by Neo4j. Of the TCK's 18
several-label cases 12 PASS (Match1/[3], Match3/[7], [26], Match7/[23], Graph3/[6], Graph5/[1], [3], [4] #1-#5), 2
BOTH_ERROR (Graph3/[8], [9]: Neo4j's errors, raised as its), 4 REFUSED by name (Merge1/[10], Remove2/[2], [3]: the
writes, W4; Match4/[5]: the unbounded variable-length pattern, W10). The harness's SKIP count falls by the 18. TCK
agreement (PASS or BOTH_ERROR over the 3,880 TCK rows) DuckDB 3428 → 3442, Postgres 3066 → 3080.

### 12.62 A property typed two ways: a Variant of each value, compared by kind (2026-10-17)

The plan's W7. Neo4j types nothing: one label's nodes may hold `var: 0` and `var: 'xx'`, and a label-less pattern
reads classes that type one name two ways. The model types every property once, so a property typed two ways is a
VARIANT, as a list property already is (§12.37), and each value is the database's own spelling of it.

- **The harness** (`CypherGraph.merged`): a property whose values differ in kind across one class's nodes (or one
  edge class's relationships) is a Variant column (`JSONB` / `SEMISTRUCTURED`) holding each value's JSON, where
  both kinds are a Variant's (an integer, a float, a string, a boolean, a list); a date beside a string stays
  refused by name (the Variant would hold its text). The two harness SKIPs of Comparison2 fall.
- **The island, across the classes a label-less node reads** (`unionKinds`, `candidateKind`, `asKind`): where two
  candidate classes type a name two ways (both a Variant's kind), the union's kind is JSON: the root's own read
  (`propertyOf`, when the candidates are an arm's every class) is the class's value as a Variant (`asVariant`:
  `toVariant`, null kept), a class lacking it reads the JSON null, a carried node's column is the Variant of each
  arm's value (`carry`, `carryRel`), a variable-length pattern's chains agree the same way (`chainRows`,
  `canonical`); so the arms agree (the projection's check, §12.13, and MatchWhere5/[4], WithWhere5/[4]). A
  temporal or duration beside another kind stays the named refusal.
- **A comparison across kinds** (`variantOrderedScalar`, from `variantOrdered`): a Variant of no known shape
  against a string, a boolean or a number orders where the Variant holds a value of that kind (a number read as
  a float, the other converted; `isKind` guards the read, as `variantEqual` does in §12.57), and is NULL where
  it holds another kind, where either is null, or where it holds the JSON null: Cypher's "values of different
  types do not order" (so `0 > 'x'` is null and `NOT (0 > 'x')` null, where §12.58's keys answered false and
  true). The Variant on either side (`mirrored`). Two Variants, or a list beside one, still order by their keys
  (§12.58: a key holds a list's element-wise order); equality is §12.57's.
- **A platform fix on the way** (`CastChecker`, pinned by `TypeCheckerTest.anEmptyCastToAClassIsTheTypedEmptyNotNil`):
  `[]->cast(@Variant)`, the island's typed null Variant, was typed `Nil[0..0]` where `[]->cast(@String)` is
  `String[0..0]`: the cast's identity rule (a source already conforming to the target class is the source) took
  the []-born Nil, the bottom of every class, so a null Variant column refused to unite under a `concatenate`
  (the arms' union). The empty is what the cast types: a Nil source is never the identity.
- **And a second, under it** (`Lowerer`'s empty-collection arm, `CastPolicy`, `Postgres.variantAwareCast`; pinned
  by `LowerRelationTest`, `CastNoReWrapTest.anEmptyCastToAVariantIsTheTypedNull` and `NullVariantCastTest`): the
  typed tree folds `[]->cast(@Variant)` into the literal's own type (an empty collection typed Variant[0..0])
  before lowering, and the lowering's empty collection was the bare, untyped NULL, which Postgres's jsonb
  operators refuse (`operator is not unique: unknown #>> unknown`, the orderability key over the arm's column).
  An empty typed as the Variant now lowers to the typed null (`CAST(NULL AS JSON)` on DuckDB), a NULL cast to the
  Variant that does reach the cast policy keeps its cast, and Postgres spells a NULL cast to its JSON carrier as
  `CAST(NULL AS JSONB)` (`to_jsonb(NULL)` has no type to go on); the showcase's hard SQL re-pinned (an absent
  node's NULL typed: `CAST(NULL AS JSON)`, Snowflake's `TO_VARIANT(NULL)`).
- **And a third, DuckDB's binder** (`DuckDb.variantElements`, pinned by `LowerRelationTest`'s `ELEMENTS` spelling):
  with the typed null in the arm, the label-less read's list concatenation (`variantElements` over each arm's
  column, S81) failed to bind on DuckDB (`Invalid LIST argument during lambda function binding`): the binder types
  `json_extract`'s wildcard `'$[*]'` over a constant NULL input as JSON, not a list, so `list_transform` over the
  CASE refuses it. The ARRAY branch now spells `CAST(json_extract(.., '$[*]') AS JSON[])`, the identity on a list
  and the list type on the constant; the stored-column reads are unchanged in value.
- **And W11's lesson again** (`keyOf`, `variantEqual`, `variantOrderedScalar`): the one read of a Variant before its
  typed conversion is `trustOne`, never `toOne`: an arm whose class lacks the property reads the typed null,
  stamped `[0..0]`, and `toOne` of a provably empty value is the raise itself in the lowering, which Postgres's
  planner folds in every CASE arm (the kind test guards the place; the conformance never raises).
- **Pinned.** `lite/twoways.yaml`: 15 cases judged by Neo4j (a property typed two ways on one
  label, compared with a string and a number, equal to each, null-or-compared in a WHERE, NOT of a comparison
  across kinds, the Variant on the right, a boolean beside a float, a list beside a scalar, the node value, the
  classes of a label-less node, the node carried through WITH, returned from a hop, across a variable-length
  pattern's chains, grouped by the Variant); `lite/labelless/property_typed_differently_reads_a_variant` (was
  the refusal `property_typed_differently_refused_by_name`).

#### 12.62.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4734 | 3533 (from 3,513) | 0 | 0 | 0 | 387 (from 388) | 733 (from 733) | 81 (from 85) |
| Postgres | 4734 | 3160 (from 3,140) | 0 | 0 | 0 | 760 (from 761) | 733 (from 733) | 81 (from 85) |

The 15 cases of `lite/twoways.yaml`: 15 PASS on DuckDB, 15 on Postgres, judged by Neo4j. Of the TCK's
typed-two-ways cases: Comparison2/[1], [2] (the harness's), MatchWhere5/[4], WithWhere5/[4] (the island's) PASS; Match4/[4]
waits for W10 (an unbounded variable-length pattern) and Merge1/[8] for W4. TCK agreement (PASS or BOTH_ERROR over the
3,880 TCK rows) DuckDB 3442 → 3446, Postgres 3080 → 3084.

### 12.63 The static variable errors, raised before any clause is read (W10 leg 1, 2026-10-17)

Neo4j's semantic analysis raises `VariableAlreadyBound` and `VariableTypeConflict` over the whole statement before
it plans a clause: `MATCH ()-[r*]->() MATCH (r)` is the conflict, not a question of whether the unbounded pattern
runs. The island raised the same errors inside the clauses that met the name again (the graph's `bind`, a named
path's declaration, the continuation's start, UNWIND, YIELD), so an error scenario whose first clause the island
refuses (an unbounded variable-length pattern, W10's) agreed with the TCK by the refusal, not by the error:
BOTH_ERROR (the TCK's error met with a refusal) where PASS (the error) is owed. W10's second leg lifts the
unbounded refusal, and every such scenario would then run to rows and FAIL. The scan comes first.

- **The scan** (`CypherToPure.checkVariableKinds`, before any clause of a statement is read): each clause's
  bindings and their kind, in order. A MATCH binds its nodes (NODE), its single relationships (REL), its
  variable-length relationships (RELS: a list), then its path variable (PATH); a WITH or RETURN opens the scope of
  its items (a bare variable keeps its kind, a literal written out is LITERAL, any other expression a VALUE the
  scan does not type, `*` keeps every binding); an UNWIND binds a VALUE (a list of literals: LITERAL); a YIELD
  binds VALUEs. The errors, Neo4j's texts: a name bound as a path, or a pattern's own path variable, named as a
  node or a relationship, and a node, relationship or value named again as a path, is `Variable `p` already
  declared` (VariableAlreadyBound); a name bound as one of NODE, REL, RELS or LITERAL and named as another is
  `Type mismatch: r defined with conflicting type` (VariableTypeConflict); an UNWIND or a YIELD over a bound name
  is `already declared`. A node or relationship named again as the same kind is a bound one, read again (the
  clauses' own reads). A VALUE never conflicts here: an expression may well be a node (`WITH head(collect(n)) AS n
  MATCH (n)`), and the clause's read keeps its check.
- **Compare, never compute** (`docs/WORLD_MAP.md`): the scan reads names and shapes of the statement; it types no
  expression (a literal is its own shape).
- **Pinned.** `lite/varkinds.yaml`: 16 cases judged by Neo4j (a relationship, a path and a literal named
  as a node, a node and a literal list named as a relationship, a variable-length named as a single relationship, a
  relationship, a literal and a value named as a path, a path bound again in its own pattern, an unwound literal
  named as a node, an UNWIND over a bound name, a node bound again read again, a name dropped by a WITH bound
  afresh, a node carried by `WITH *` named as a variable-length, a list of relationships named as a variable-length
  reading on: a list of variables is a value, Match4/[8]'s, where a list of literals is a literal); `CypherToPureTest`
  pins the scan's errors over an unbounded pattern (the error, not the refusal). The TCK's Match1/[7]-[11], Match2/[9]-[13], Match6/[21]-[25]
  (250 rows) stay in agreement; the ones an unbounded or deep pattern refused first now raise the error.

#### 12.63.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4750 | 3535 (from 3,533) | 0 | 0 | 0 | 388 (from 387) | 746 (from 733) | 81 (from 81) |
| Postgres | 4750 | 3162 (from 3,160) | 0 | 0 | 0 | 761 (from 760) | 746 (from 733) | 81 (from 81) |

The 16 cases of `lite/varkinds.yaml`: 15 on DuckDB, 15 on Postgres in agreement with Neo4j (the error
scenarios BOTH_ERROR, the reading ones PASS). No TCK row moves: the 250 error scenarios of Match1/[7]-[11], Match2/[9]-[13]
and Match6/[21]-[25] agreed by the refusal and agree by the error. TCK agreement (PASS or BOTH_ERROR over the 3,880 TCK
rows) DuckDB 3446 → 3446, Postgres 3084 → 3084.

### 12.64 Stored nanosecond times and date-times: a TIMESTAMP_NS column under the profile's marks (W5 leg 2a, 2026-10-17)

Neo4j's temporals are nanosecond values, and the TCK stores them: `localtime({..., nanosecond: 645876123})` and
`localdatetime({..., nanosecond: 645876123})` as node properties (WithOrderBy1/[35], [36], WithOrderBy2/[13], [14],
Temporal5/[3], [5]: ordering, fields, arithmetic over them). The island already carries such values when it computes
them (§12.12: the database's TIMESTAMP_NS, a time of day as the date-time at it on the epoch day, every operation
spelled over the nanosecond epoch); the STORE had no column for them (a TIME column keeps microseconds, as a
TIMESTAMP does), so the harness skipped the scenarios by name.

- **The column: `TIMESTAMP_NS`**, lite's extension of the `###Relational` grammar as `TIME` was (§12.59, S64): a
  date-time of nanosecond precision (`RelationalDataType.TimestampNs`; the parser, the protocol and the composer,
  the store compiler typing it DateTime, the DDL kind spelled by DuckDB as `TIMESTAMP_NS`, by H2 and Trino as
  `TIMESTAMP(9)`, by Snowflake as `TIMESTAMP_NTZ(9)`, by ClickHouse as `DateTime64(9)`, by SQLite as text; refused
  by name on Postgres, MariaDB, BigQuery and Databricks, whose timestamps keep microseconds; the metamodel's datatype
  class, the plan's and the precise types' spellings, the data generator's literal).
- **The marks: `timeOfDay` and `nanoseconds`** (the graph profile's seventh and eighth stereotypes, on a DateTime
  property; `GraphEdges.STEREOTYPES` moved with this leg). Pure's DateTime names no precision and no shape: the
  island cannot tell from the type that a column holds nanoseconds, nor that it holds a time of day (its own
  carriage of one finer than a TIME column is the date-time at it on the epoch day), and the translator reads the
  model, never the mapping (the column type is the store's). The marks say it: `nanoseconds` reads the property's
  values at nanosecond precision (the §12.12 operations), `timeOfDay` reads a DateTime as a time of day of that
  precision (its fields and comparisons the nanosecond ones, its node-value text `Time('...')`). The mark is the
  declaration the §3.1 decision 4 of the plan foresaw (`graph.timeOfDay`), the column type the one the W5 plan named.
- **The island** (`Model.marks`, the `Property` record's marks, `kindOf(Property)`, `nullOfStored`,
  `nanosDeclared`): a marked property's value is marked nanosecond as it is read (`propertyOf`), carried through a
  projection marked (`NodeProp.nanos`, the carried column's detail), its typed null the DateTime null marked (a
  class the arms read that lacks it; a StrictTime null would not unite with the DateTime column); a property of
  nanosecond precision on some of the classes a label-less node reads and not on others is refused by name (the
  harness never makes one: below). A node's value spells a stored date-time too (`DateTime('...')`, the oracle's
  spelling, which no node value spelled before: a stored date-time property in a returned node was refused by name).
- **The harness** (`CypherGraph`, `SqlParity.seed`): a property NAME's precision is the finest the graph holds under
  it (a time or date-time with digits past the microsecond on any node or relationship makes every class's property
  of that name a nanosecond one), so a label-less read unites columns of one kind; such a property is declared
  `<<graph::graph.timeOfDay, graph::graph.nanoseconds>> time: DateTime[0..1]` (a time of day) or
  `<<graph::graph.nanoseconds>> datetime: DateTime[0..1]` over a `TIMESTAMP_NS` column, its cell the value's full
  text (a time on the epoch day), which the seed casts to the column (`CAST(? AS TIMESTAMP_NS)`: the driver's
  timestamp keeps microseconds, measured). On the Postgres lane the seed refuses the column by name (the dialect's
  own refusal, as its DDL's), which the lane records as REFUSED. A nanosecond date-time whose year is outside 1678 to
  2261 (the database's nanosecond range, §12.12; the TCK's year-1 and year-9999 values) stays the harness's skip,
  named: WithOrderBy1/[39]-[42], WithOrderBy2/[17]-[20] (20 rows) hold one each.
- **Compare, never compute**: the harness types by the values' digits; the island reads marks and spells
  operations; the database holds and computes the nanoseconds.
- **Pinned.** `lite/stored.yaml` grows 9 cases judged by Neo4j (a nanosecond time ordered and read,
  its fields and text, plus a duration, as a node's value, compared, absent on a class of a label-less read; a
  nanosecond date-time's fields, ordered, truncated and moved, as a node's value and text);
  `CypherToPureTest.aMarkedDateTimeIsANanosecondTimeOfDayOrDateTime` pins the island's read of the marks;
  `GraphEdgesTest` and `CypherIslandIntegrationTest` the profile's eight stereotypes; `TypeSpellingParityTest` the
  column type's two spellings.
- **Still refused or skipped, by name.** A stored zoned time or date-time (its offset a column beside the value:
  leg 2b, the island's Zone carrying a column); the year-1 and year-9999 nanosecond date-times (above); Temporal5/[6]'s
  named zone.

#### 12.64.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4759 | 3556 (from 3,535) | 0 | 0 | 0 | 388 (from 388) | 746 (from 746) | 69 (from 81) |
| Postgres | 4759 | 3162 (from 3,162) | 0 | 0 | 0 | 782 (from 761) | 746 (from 746) | 69 (from 81) |

The 9 new cases of `lite/stored.yaml`: 9 PASS on DuckDB, 9 REFUSED on Postgres (the TIMESTAMP_NS column, the dialect's
refusal). The TCK's stored nanosecond local times and date-times: WithOrderBy1/[35], [36], WithOrderBy2/[13], [14], Temporal5/[3], [5]
SKIP → PASS on DuckDB (REFUSED on Postgres); WithOrderBy1/[39]-[42], WithOrderBy2/[17]-[20] SKIP, named (a year outside 1678 to 2261). TCK agreement (PASS or BOTH_ERROR over the 3,880 TCK
rows) DuckDB 3446 → 3458, Postgres 3084 → 3084.

### 12.65 Stored zoned times and date-times: a value class of the instant and its offset (W5 leg 2b, 2026-10-17)

The TCK stores zoned values too: `time({..., timezone: '+01:00'})` and `datetime({..., timezone: '+00:15'})`
(WithOrderBy1/[37], [38], WithOrderBy2/[15], [16]: ordered, projected, moved by a duration, returned in a node;
Temporal5/[4]: its fields and zone; WithOrderBy1/[41], [42], WithOrderBy2/[19], [20]: zoned date-times of the
years 1 and 9999 beside nanosecond ones; Temporal5/[6]: a named zone). The island carries a zoned value as its
instant with the zone as a fact beside it while the translator sees it (§12.8, §12.11, S65): a stored one has
its zone in the store.

- **The representation: a value class**, as a stored duration is (§12.59): `g::values::ZonedTime` and
  `g::values::ZonedDateTime` with two slots, `instant: DateTime[1]` (a zoned time's the date-time at it on the
  epoch day, the island's own carriage; marked `nanoseconds` where any zoned value of the kind is sub-microsecond,
  §12.64) and `offsetSeconds: Integer[1]`; a property holding one is an embedded instance over two columns. The
  island recognises the classes as it recognises the Duration class, by simple name and slots (`zonedClasses`),
  never as nodes (`valueClasses`); a node's keys list the property, its value spells the instant's text.
- **What the island reads: the instant.** A stored zoned value is its instant slot (`storedZoned`), a `ZONED_TIME`
  or `ZONED` value whose zone is NOT carried (`Carried(null, nanos)`): ordering, comparison and arithmetic with a
  duration go by the instant (S65, S66: `=` between two zoned values is the instants'; the lite case
  `a_stored_zoned_time_compared_with_a_zoned_literal` is judged against S66, `cypher-parity/decisions.tsv`), a node's
  value and a returned value spell the instant (`DateTime('1970-01-01T11:31:14.645876123')` for a time, which the
  judge reads against Neo4j's `Time('12:31:14.645876123+01:00')`, S68). An operation reading the zone (`.hour` of
  the wall time, `.timezone`, `.offset`, `toString`) refuses by name through `zoneKnown`, as a zoned value of a
  list or an aggregate does: the offset slot is leg 2c's (the island's `Zone` carrying a column, which every
  projection must carry beside the value).
- **The harness** (`CypherGraph`, `CypherValues.zonedParts`): a `Time('..±oo:oo')` or `DateTime('..±oo:oo')` text
  is split into its instant's seed text (the UTC wall time, a time's on 1970-01-01, the day before or after when
  the offset crosses midnight) and its offset in seconds; the class's instant is a `TIMESTAMP_NS` column where its
  kind holds a sub-microsecond value anywhere in the graph (the seed casts the text), a `TIMESTAMP` otherwise; a
  nanosecond zoned date-time of a year outside 1678 to 2261 is the harness's skip, named (WithOrderBy1/[41], [42],
  WithOrderBy2/[19], [20] hold one each); a duration or zoned value on a relationship is refused by name (an
  embedded value is a node's).
- **Compare, never compute**: the harness splits texts; the island reads a slot; the database orders instants.
- **Pinned.** `lite/stored.yaml` grows 8 cases judged by Neo4j (a zoned time ordered and read in a
  node, plus a duration, compared with a zoned literal (S66), absent on a class of a label-less read, its zone and
  its text refused by name; a zoned date-time ordered and read in a node, moved and compared);
  `CypherToPureTest.aStoredZonedValueIsItsInstantWithNoZoneCarried` pins the read and the refusals.
- **Still refused or skipped, by name.** The zone of a stored zoned value (Temporal5/[4]; leg 2c); the year-1 and
  year-9999 nanosecond zoned date-times (the harness's range); Temporal5/[6]'s named zone.

#### 12.65.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4767 | 3565 (from 3,556) | 0 | 0 | 0 | 399 (from 388) | 746 (from 746) | 57 (from 69) |
| Postgres | 4767 | 3163 (from 3,162) | 0 | 0 | 0 | 801 (from 782) | 746 (from 746) | 57 (from 69) |

The 8 new cases of `lite/stored.yaml`: 4 in agreement on DuckDB (two refused by name: the zone's fields and text),
8 on Postgres (the nanosecond instant's TIMESTAMP_NS column refused by the dialect, the microsecond ones read). The TCK's stored
zoned times: WithOrderBy1/[37], [38], WithOrderBy2/[15], [16] SKIP → PASS on DuckDB (REFUSED on Postgres); Temporal5/[4] SKIP → REFUSED
(the zone), WithOrderBy1/[41], [42], WithOrderBy2/[19], [20] SKIP, named (a year outside 1678 to 2261); Temporal5/[6] SKIP → REFUSED (a named zone). TCK agreement (PASS or BOTH_ERROR over the 3,880 TCK
rows) DuckDB 3458 → 3463, Postgres 3084 → 3084.

### 12.66 The tail, leg 5: extreme float literals and an unbound parameter (2026-10-17)

- **Extreme float literals** (Literals5/[5], [6], [11], [12], [25], [26]: `1e308`, `-1e-305`, `123456789e300`, a
  three-hundred-digit mantissa). The island refused a float literal beyond 1e15 or below 1e-15 by name (2026-10-15)
  because lite's Postgres dialect spelled a Float literal in plain digits, which Postgres typed `numeric` (an exact
  value where Neo4j answers a float). Every dialect spells `toFloat` of such a literal as a DOUBLE cast of its
  exponent form now (`CAST(1.0E308 AS DOUBLE PRECISION)`, measured on both lanes), so the guard was stale: lifted.
  The judge compares the doubles (Neo4j prints `1e+308`, `1.2635418652381264e+305`).
- **An unbound parameter** (Call1/[11]: `CALL test.my.proc` with the parameter `in` missing). The island has no
  parameter channel (§12.15: a lane binds a scenario's parameters into the text, implicit arguments spelled as the
  explicit call of the parameters), so a `$x` the text did not bind is Neo4j's error, `ParameterMissing: Expected
  parameter(s): x`, not a refusal.
- **Still refused, by name.** `range` over a bound that may be null or a step of the row (List11/[3]: the UNWIND of
  a list carried through a WITH types its values nullable, and a step of the row needs the end's sign spelled at
  run time, `if($step > 0, |$last + 1, |$last - 1)`); `toString` of a list, a map, a node, a relationship or a path
  (TypeConversion4/[10]: Neo4j 2026.09 answers its own texts, `'[]'`, `'{}'`, `'()'`, `'[:T]'`, `'()-[:T]->()'`,
  where the TCK expects an error); a signed year beyond the database's dates (Temporal10/[9], [10]:
  `date('-999999999-01-01')`); a duration divided by a number (Temporal8/[7]: Neo4j's own normalisation of the
  fractional months and days).
- **Pinned.** `lite/tail.yaml` grows 2 cases judged by Neo4j (the extreme literals returned, an
  unbound parameter's error); `CypherToPureTest` pins the literals' spelling and the parameter's error (its
  former refusal pin turned).

#### 12.66.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4769 | 3573 (from 3,565) | 0 | 0 | 0 | 392 (from 399) | 747 (from 746) | 57 (from 57) |
| Postgres | 4769 | 3171 (from 3,163) | 0 | 0 | 0 | 794 (from 801) | 747 (from 746) | 57 (from 57) |

The 2 new cases of `lite/tail.yaml`: 1 in agreement on DuckDB, 1 on Postgres. Literals5/[5], [6], [11], [12],
[25], [26] REFUSED → PASS on both lanes; Call1/[11] REFUSED → PASS (the TCK's error met). TCK agreement (PASS or BOTH_ERROR over the 3,880 TCK
rows) DuckDB 3463 → 3470, Postgres 3084 → 3091.

### 12.67 A recursion over a relation: the platform's native for the unbounded walk (W10 leg 2a, 2026-10-17)

An unbounded variable-length pattern (`(a)-[:T*]->(b)`, `*2..`) is a walk the data bounds, not the query: Cypher's
relationship uniqueness (no path walks one relationship twice) ends it. The island unrolls a bounded pattern into
its chains (§12.42, §12.56); an unbounded one has no chains to unroll, and the SQL for a walk the data bounds is
the database's recursive common table expression. legend-pure has no function for one.

- **The native** (`meta::legend::lite::recurse<T>(base: Relation<T>[1], step: Function<{Relation<T>[1]->
  Relation<T>[1]}>[1]): Relation<T>[1]`, lite's own, docs/LITE_INVENTION_CENSUS.md): the base's rows and,
  repeatedly, the rows the step yields from the previous round's rows, until a round yields none. The step is a
  lambda over a relation of the base's type (the first relation-typed lambda parameter outside the window
  functions' partition), yielding one of the same type: the signature's `Relation<T>` unifies the three, so a step
  projecting other columns is a type error at the checker (`RecurseChecker`, the generic check as `filter`'s),
  never the database's.
- **The lowering** (`TypedRecurse`, a relation operator; `Lowerer.recurse`): `WITH RECURSIVE __walkN AS (base UNION
  ALL step) SELECT * FROM __walkN`, self-contained as a derived table where the relation stands (DuckDB and
  Postgres read a WITH inside a subquery), the step's parameter lowering to the expression's own name (a CTE
  source, `recursions`): the step reads the previous round's rows, and the database stops when a round yields
  none. No depth guard of the platform's: termination is the query's, by what the step filters (the island's
  uniqueness filter, leg 2b).
- **Compare, never compute**: the translator spells the walk's shape; the database walks.
- **Pinned.** `LowerRelationTest.aRecursionWalksUntilItsStepYieldsNoRow` (a count from 1 to 5 over a TDS literal,
  executed on DuckDB; the SQL's `WITH RECURSIVE` and `UNION ALL`). The island does not emit the native yet (leg 2b
  spells the unbounded pattern over it); no TCK row moves.
- **The guards moved, each with its dated justification.** `NativeCatalogGovernanceTest`'s `INTERNAL_DESUGAR` ceiling
  26 → 27 (the native). `Lowerer.relation`'s two new arms put it over the method-length guardrail (256 lines, limit
  250): the `lateral` arm's body moved out along its seam to `Lowerer.lateral`, and the recursion's step parameter
  reads through `Lowerer.recursionSelf`; the guardrail stays at 250. The parked-work ledger's anchor on `new SqlWith(`
  (PARK-2) holds: the lowering builds the expression through `SqlWith.prepend`.

- **The contract, for another island's use** (the SAP IQ / SQL Anywhere island plans to spell SQL's `WITH RECURSIVE`
  through it; noted 2026-10-17): the signature above, unchanged since its landing; `UNION ALL` only (no distinct
  form); no depth guard of the platform's (the step's filter ends the walk); the step's parameter lowers to a
  reference to the expression by name, a fresh alias at each reference, so a step reading its parameter twice (a
  self-join, or a union of two branches each reading it) renders, though no lite test runs either shape and Postgres
  refuses more than one recursive reference in a recursive term (the database's error, not the native's); since the
  W10 leg 2b landing the store resolver resolves a recursion's base and its step's body as relation expressions. It
  lives on this branch until the Cypher island's work reaches `main`; a change to the signature or the lowering is
  recorded here and in a GATES landing.

#### 12.67.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4769 | 3573 (from 3,573) | 0 | 0 | 0 | 392 (from 392) | 747 (from 747) | 57 (from 57) |
| Postgres | 4769 | 3171 (from 3,171) | 0 | 0 | 0 | 794 (from 794) | 747 (from 747) | 57 (from 57) |

No lite case and no TCK row moves: the native alone, pinned by the lowering's test. TCK agreement (PASS or BOTH_ERROR over the 3,880 TCK
rows) DuckDB 3470 → 3470, Postgres 3091 → 3091.

### 12.68 The walk the data bounds: an unbounded variable-length relationship as lite's recursion (W10 leg 2b, 2026-10-17)

- **`MATCH (a {name: 'A'})-[*]->(x)`, `(n)-[:T*]->(m)` over twenty-one hops, `(a)-[:T*2..]->(x)`, `p = ()-[*0..]->()`,
  `(a:Blue)-[r*]->(b:Green) RETURN count(r)`, `(a:Artist)-[:WORKED_WITH* {year: 1988}]->(b)`, `(a)-[:T*..7]->(x)`:** a
  variable-length relationship without an upper bound, or with one past the unroll's (§12.42: beyond 4), is the walk
  the data bounds, spelled as lite's recursion (§12.67's `recurse`) over the relationship's hop read as rows
  (`CypherToPure.walk`). Three readings feed it, each the island's own machinery: the start node alone as a MATCH of
  its own (a label-less one every class's arms, §12.13), the far node alone likewise, and the hop as a segment-named
  pattern between two label-less nodes (`(s)-[:T]->(d)`, §12.56's segment: its relationship carries its identity,
  an edge's own or the ordered pair of its ends', S74, and its stored direction), each carried with its identity.
  The hop's rows narrow to six columns: the source's identity, the target's, the relationship's, and the JSON texts
  of the relationship, the target node and the stored direction. The walk's rows carry the start node's identity,
  the node reached, the depth, the identities walked (each between bars) and the JSON texts of the relationships,
  the nodes, the path's elements and the directions walked (comma-separated bodies): its base is every start node
  at depth zero (distinct), its step the previous round's rows joined to the hop on the node reached, those whose
  relationship the row has walked dropped (**Neo4j's relationship uniqueness, spelled at every step** — S75 revised:
  a two-node cycle is walked round once, a self-loop once), the depth under the upper bound where there is one, the
  texts extended. The start node's rows join the walk on its identity, the far node's on the node reached (a zero
  lower bound binds the far node to the start at depth zero: its labels and property map hold as the join's), the
  depth at least the lower bound (Neo4j's missing lower bound is 1); the relationship's variable is the list of the
  relationships walked (`fromJson('[' + body + ']')`, a Variant of relationship values); a named path is assembled
  from the texts as §12.27's shape (`{"@path": [start, rel, node, ...], "@dirs": [...]}`, the directions the hop's
  own for a directed pattern, the segment's column for an undirected one), its length the depth, its nodes and
  relationships the lists, under §12.42's `PATH_PART` names. The WHERE is read over the rows. The walk's own columns
  are dropped: the rows are the pattern's variables.
- **After rows, in an OPTIONAL MATCH, as a pattern predicate:** the walk is the re-anchored pattern's own (§12.49):
  `MATCH (a:Single), (x:C) OPTIONAL MATCH (a)-[*]->(x)`, `MATCH (a:A) WITH a MATCH (a)-[:T*]->(x)`, `MATCH (n), (m)
  WHERE (n)-[:REL1*]-(m)` read the pattern from the rows' node as a MATCH of its own (the walk above, from every
  node of that class) and join back by identity, a left join for the OPTIONAL MATCH (its WHERE over the pattern's
  rows before it, §12.49's rule). An OPTIONAL MATCH over a graph with a variable-length relationship (bounded or
  not) takes this route too (`viaRows`), where it was refused by name.
- **A path through a WITH carries its parts** (`WITH count(*) AS c, p AS p RETURN nodes(p)`, With6/[4]): a WITH
  projecting a path projects beside it its length, nodes and relationships under the `PATH_PART` names a
  materialized path carries them under (§12.42), so the clauses after read columns and a grouping by the path keys on
  them too: Postgres reads no grouped expression inside a subquery ("subquery uses ungrouped column"), which reading
  into the grouped path's JSON would ask of it. `materialize` relies on the projection for them (it listed them
  itself before). Where no parts ride beside a path carried as its value, `nodes(p)`, `relationships(p)` and
  `length(p)` read its JSON by the database: its `@path` list's elements, the nodes those carrying labels, the
  relationships those carrying a type, the length the relationships' count (`toMany`, `get`, `filter`, `size`); the
  path functions of a path an OPTIONAL MATCH may not find stay refused by name.
- **A hop no way of the model reads** (`p = ()-[*0..]->()` over a model with no association, Match6/[15]): the walk
  is its base alone, the far node the start at depth zero (nothing under a lower bound).
- **The resolver** (`StoreResolver`, 2026-10-17): a `TypedRecurse` resolves its base as the relation chain it is and
  its step's body too — the step's parameter is a relation, no chain of a store, so what the body joins resolves as
  any relation expression above a class chain does, never as a lambda's data (which left the hop's class chain
  unresolved: "store resolution left getAll(...) unresolved").
- **Compare, never compute**: the translator spells the walk's shape and the texts' concatenation; the database
  walks, concatenates, compares the identities and builds the lists.
- **Refused by name:** other relationships in the pattern beside the walk (`(a)-[:T*]->(x)-[:U]->(y)`; a later
  slice cuts the pattern around it, §12.57's cut), the pattern's two ends one node, an OPTIONAL MATCH as the first
  clause, a MATCH after rows carrying neither end, a variable-length relationship bound to a list of relationships
  (`WITH [r1, r2] AS rs MATCH (first)-[rs*]->(second)`: Match4/[8], Match9/[6], [7]; the plan's §5), the path
  functions of a nullable carried path.
- **Pinned.** `CypherToPureTest` (the walk's recursion, the uniqueness filter, the depth bounds, the list and the
  path beside, after rows, the left join of an OPTIONAL MATCH, the refusals); `lite/varlen.yaml`'s 21 new cases
  (the walk with a lower bound, from zero, past the unroll's bound, round a cycle, undirected, incoming, untyped,
  to a far label, its relationship list, a named path and one through a WITH, a WHERE, a property map, after rows,
  in an OPTIONAL MATCH, between bound nodes, as a pattern predicate, over no way, and the refusal), each judged by
  Neo4j; the TCK rows that move: Match4/[2], Match4/[4], Match4/[5], Match6/[15], Match6/[20], Match7/[12], Match7/[13], Match9/[5], Match9/[8], ReturnOrderBy2/[12], Return6/[13], Return6/[8], With6/[4], List12/[4], List12/[5], Pattern1/[16], Pattern1/[17].

#### 12.68.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4789 | 3610 (from 3,573) | 0 | 0 | 0 | 375 (from 392) | 747 (from 747) | 57 (from 57) |
| Postgres | 4789 | 3208 (from 3,171) | 0 | 0 | 0 | 777 (from 794) | 747 (from 747) | 57 (from 57) |

The 17 TCK rows that move (DuckDB REFUSED → PASS; Postgres the same): Match4/[2], Match4/[4], Match4/[5], Match6/[15], Match6/[20], Match7/[12], Match7/[13], Match9/[5], Match9/[8], ReturnOrderBy2/[12], Return6/[13], Return6/[8], With6/[4], List12/[4], List12/[5], Pattern1/[16], Pattern1/[17]; the 21 new lite
cases PASS on both lanes but the refusal, REFUSED on both. TCK agreement (PASS or BOTH_ERROR over the 3,880 TCK rows) DuckDB
3470 → 3487, Postgres 3091 → 3108.

### 12.69 The variable-length remainder: after a MATCH, as a WHERE's predicate, and arms read one at a time (2026-10-17)

- **A bounded variable-length relationship after another clause** (`MATCH (a:A) MATCH (a)-[r*2]->()`, Match4/[6]): the
  pattern re-anchored (§12.49), read from the rows' node as a MATCH of its own, unrolled there (§12.42), and joined
  back by identity, as the walk does after rows (§12.68). Refused by name only where no end of the pattern is a node
  the rows carry.
- **A WHERE holding a variable-length pattern predicate over a graph** (`MATCH (n) WHERE (n)-[:REL1*]->() RETURN n`,
  Pattern1/[7]-[10]): the pattern is read as rows and the WHERE over them, the predicate a flag (§12.52) whose
  pattern, a MATCH of its own, is the unroll or the walk; never a navigation, which reads no variable length.
  `CypherToPure.variableLengthPredicate` names the case (a pattern predicate or an EXISTS subquery holding a
  relationship with a length range).
- **The nodes reached through an edge class, scoped to their arm.** The translator refuses a hop from a node reached
  through an edge class (§12.38: the resolver reads no association end past the edge's end), keeping those nodes in
  one set for the statement. A relationship of several types, or a type joining several class pairs, is read as
  arms (§12.35, §12.17), each its own graph: the set kept the earlier arms' nodes, so an arm over an association
  read from a node an edge arm had reached was refused (`(n)-[:REL1|REL2]-(m)` with `REL1` over edge classes,
  Pattern1/[13]; the undirected `OPTIONAL MATCH (a)-[*3..]-(b)`, Match7/[14]). Each arm now starts from the set as
  it was before the arms (`resetReachedThroughEdge`), and the set holds every arm's nodes again once the arms are
  read, as it did.
- **Pinned.** `CypherToPureTest` (the re-anchored bounded pattern after a MATCH, the predicate's recursion and its
  left join); `lite/varlen.yaml`'s 6 new cases (a bounded pattern after a MATCH, a variable-length predicate, an
  undirected fixed-length predicate, a negated predicate, a relationship of an edge type and an association type,
  an optional walk whose lower bound is past its reach), each judged by Neo4j; the TCK rows that move: Match4/[6], Match7/[14], Pattern1/[10], Pattern1/[13], Pattern1/[7], Pattern1/[8], Pattern1/[9].

#### 12.69.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4795 | 3623 (from 3,610) | 0 | 0 | 0 | 368 (from 375) | 747 | 57 |
| Postgres | 4795 | 3221 (from 3,208) | 0 | 0 | 0 | 770 (from 777) | 747 | 57 |

The 7 TCK rows move REFUSED → PASS on both lanes; the 6 new lite cases PASS on both. TCK agreement
(PASS or BOTH_ERROR over the 3,880 TCK rows) DuckDB 3487 → 3494, Postgres 3108 → 3115.

### 12.70 A MATCH of several patterns as rows joined on identity, patterns that find nothing, named paths re-anchored (2026-10-17)

- **A MATCH of several patterns the graph route does not read as one graph** (`MATCH (a)--(b)--(c)--(d)--(a), (b)--(d)`,
  MatchWhere2/[1]; `MATCH (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'})` then `MATCH (a)-->(x), (b)-->(x),
  (c)-->(x)`, Match3/[20], [21]; `WITH n LIMIT 1 MATCH (m:B), (n)-->(x:X)`, WithSkipLimit2/[3], With1/[2]):
  `CypherToPure.joinedPatterns` reads each pattern as rows of its own and joins it to the rows before it, on the
  identities of the variables it shares with them (the re-anchoring of §12.49), else as their product (§12.28); the
  MATCH's relationships are pairwise distinct by identity across its patterns (Neo4j's uniqueness within one MATCH,
  each pattern's own read spelling it within the pattern; an unnamed relationship named for it), then the WHERE
  over the joined rows. It is the route where the graph route refused: an untyped relationship in a MATCH of
  several patterns, a pattern sharing no variable with the first followed by another, a MATCH of several patterns
  after rows.
- **A re-anchored pattern that finds nothing** (`MATCH (a:A), (c:C) OPTIONAL MATCH (a)-->(b)-->(c)` where no way of
  the model continues, Match7/[8]; `OPTIONAL MATCH (a1)<-[r]-(b2)` against the stored way, Match7/[4]; a predicate
  `(a)-[:T*]->(b:MissingLabel)` naming a label the open graph lacks, MatchWhere4/[2]): the pattern, read from the rows'
  node as a MATCH of its own, has no way (its nodes absent, §12.36, §12.45), so its copies of the rows' nodes carry
  no identity to join on. Such a pattern matches nothing (`absentCopy`): a MATCH reads no row, an OPTIONAL MATCH keeps
  every row with the pattern's own variables absent (`noneOf`), a pattern predicate's flag is null on every row.
- **A named path re-anchored after rows** (`OPTIONAL MATCH p = (a)-->(x)` after a cartesian product, Match7/[17];
  `OPTIONAL MATCH p = (a)-[*]->(b)`, Match7/[20], Match9/[9]): the anchored pattern may name its path; it is read as
  written (never reversed, so the path keeps its order) and the join on identity binds whichever of its nodes the
  rows carry; the path's value and its parts (§12.68's columns) ride the right side, null where the OPTIONAL MATCH
  found nothing.
- **A label-less start against the stored way** (`MATCH ()-[r:KNOWS]-()`, Graph8/[5]): the graph route reads a
  label-less start's class from its outgoing relationship's declaring class; an undirected or incoming one is the
  arms' (one arm per way of the type, each end of its class: `againstTheStoredWay`); an OPTIONAL MATCH over a graph
  whose relationship runs against the stored way to a label-less node goes through the rows likewise.
- **A type an OPTIONAL MATCH's pattern repeats** (`OPTIONAL MATCH (a)-[:KNOWS]->()-[:KNOWS]->(foo)`, Match7/[7]): read
  through the rows, where the first-clause reading names the repeated relationships and spells their uniqueness.
- **A variable an absent pattern names twice** (`MATCH (a)-[r]->()-[r]->(a)` over a model with no relationship,
  Match3/[29]): one absent value, no refusal.
- **A list is statically a list** (`WITH [n] AS users MATCH (users)-->(m)`, Match3/[30]): the static scan of §12.63
  gains `VarKind.LIST` (a list written out whatever its elements, or a `collect`): named as a node, a relationship or
  a path it is Neo4j's VariableTypeConflict; as a variable-length relationship's list it reads on. (The scenario had
  agreed by a refusal; once the MATCH after rows read it, the scan had to say what Neo4j says.)
- **Also:** an optional column keeps a property's nanosecond mark (`nullableProps`).
- **Refused by name, left:** an OPTIONAL MATCH whose WHERE reads a value of the rows beside its pattern (MatchWhere6/[5],
  Match7/[11]: the WHERE belongs in the left join's condition; the plan's §5), a named path over a variable-length
  relationship among other relationships in an OPTIONAL MATCH over a graph (Match7/[19]).
- **Pinned.** `CypherToPureTest` (the three-pattern product, the components after a WITH, the list conflicts);
  `lite/multipattern.yaml`'s 12 cases, each judged by Neo4j; the TCK rows that move: MatchWhere2/[1], MatchWhere4/[2], Match3/[10], Match3/[20], Match3/[21], Match3/[29], Match4/[7], Match7/[17], Match7/[20], Match7/[26], Match7/[4], Match7/[7], Match7/[8], Match9/[9], WithSkipLimit2/[3], WithWhere2/[1], WithWhere4/[2], With1/[2], Graph8/[5].

#### 12.70.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4807 | 3655 (from 3,623) | 0 | 0 | 0 | 348 (from 368) | 747 | 57 |
| Postgres | 4807 | 3253 (from 3,221) | 0 | 0 | 0 | 750 (from 770) | 747 | 57 |

The 19 TCK rows move to PASS on both lanes (Match3/[30] stays BOTH_ERROR); the 12 lite cases PASS on
both. TCK agreement DuckDB 3494 → 3513, Postgres 3115 → 3134.

### 12.71 Nodes and relationships held as values carry their identities (2026-10-17)

- **The identity in the value.** A node's or relationship's JSON value (§12.14, §12.19) now carries its identity under
  `"@id"` beside its labels or type: lite's elementId of the instance (`nodeJsonText` spells it wherever the parts
  know the identity: a graph's node, a carried node's identity column, an edge's row, an association relationship's
  ordered pair of its ends, S74); an association relationship carried in rows without properties is rebuilt from its
  parts too, so its pair rides along. The judge (`CypherValues`) reads past `"@id"`, Neo4j's values carrying none
  (S85).
- **A node read back from a Variant re-anchors on it** (`WITH a, collect(b1) AS bees UNWIND bees AS b2 MATCH
  (a)-[:Y]->(b2)`, Unwind1/[12]; `WITH coalesce(b, c) AS x MATCH (x)-->(d)`, Match7/[22]):
  `withValueIdentities` gives a node the rows hold as a value (its mark a node's) an identity column read from its
  JSON, marked `HeldNode` for `joinKeys`, its own mark restored after the join (`unheld`); an absent node read from
  rows is a node's null, so a coalesce of nodes stays a node.
- **A variable-length relationship bound to a list** (`WITH [r1, r2] AS rs MATCH (first)-[rs*]->(second)`, Match4/[8],
  Match9/[6], [7]): the list the rows hold and the walk's relationship list (§12.68) are each read as their identities
  in order (`identitiesText`: each element's `"@id"`, bar-separated) and joined on them: the paths whose relationships
  are the list's, in its order.
- **Lists of nodes or relationships as grouping keys** (`WITH tail(relationships(p)) AS relationships, count(*) AS c`,
  Quantifier1-4/[9]): their text is identity-exact now, so `scalarShaped` admits them.
- **A Variant grouping key read into a later computation** (the platform, `Lowerer.scopedResolver`): a computed GROUP BY
  key typed as a Variant is unfoldable into an expression over its grouped select, so the read goes through the
  isolated select by name. A dialect may spell a read into a Variant as a subquery (Postgres's
  `jsonb_array_elements`), and a subquery over a grouped expression's columns is no grouped value ("subquery uses
  ungrouped column"); the isolation keeps the semantics.
- **Also:** `lite/optional.yaml`'s `optional_to_many_node_as_value` sorts by the node's name after the row's (its tie
  had passed by the order of the rows); the hard showcase's Pure and SQL spell the identities (pinned again,
  docs/CYPHER_SHOWCASE.md refreshed).
- **Pinned.** `CypherToPureTest` (the pins spelling node and relationship JSON regenerated with the identity piece,
  the held node's re-anchoring, the list's identities, a node list as a key); `lite/heldvalues.yaml`'s 7 cases, each
  judged by Neo4j; the TCK rows that move: Match4/[8], Match7/[22], Match9/[6], Match9/[7], Unwind1/[12], Quantifier1/[9], Quantifier2/[9], Quantifier3/[9], Quantifier4/[9].

#### 12.71.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4814 | 3671 (from 3,655) | 0 | 0 | 0 | 339 (from 348) | 747 | 57 |
| Postgres | 4814 | 3269 (from 3,253) | 0 | 0 | 0 | 741 (from 750) | 747 | 57 |

The 9 TCK rows move to PASS on both lanes; the 7 lite cases PASS on both. TCK agreement DuckDB
3513 → 3520, Postgres 3134 → 3141.

### 12.72 Pattern comprehensions over rows, and a first OPTIONAL MATCH's null row (2026-10-17)

- **A comprehension read as a list column of the rows** (`MATCH (n) RETURN [p = (n)-->() | p]`, Pattern2/[1], [2];
  `MATCH (a:A), (b:B) RETURN [p = (a)-->(b) | p]`, [3]; `WITH [p = (n)-->() | p] AS ps, count(b) AS c`, [8];
  `WITH [p = (a)-[*]->(b) | p] AS paths, count(a) AS c`, [9]; `RETURN [p = (liker)--() | p] ORDER BY liker.time`,
  [11]): before the projection, `withPatternLists` reads every comprehension whose pattern names a node or
  relationship the rows carry with its identity, and whose expression and WHERE read the pattern's variables alone
  (`listable`), as a column of the rows. The pattern is read as its own rows (the re-anchoring of §12.49,
  `anchored`; its WHERE over them), each match's value of the expression is its JSON text (the rows' variables read
  from the pattern's copies, the same nodes by the join), the texts are grouped by the copies' identities and joined
  with commas, left-joined back on those identities, and read as a Variant between brackets: `[]` where the pattern
  matches nothing, a row whose node an OPTIONAL MATCH did not find among them (Neo4j's `[]` for a comprehension
  from a null). `size(...)` of one
  is the list's size. The list's elements come in the database's order (S86).
- **Over a graph, where the navigations do not read it** (`throughRows`): more than one hop, a variable-length
  relationship, a bound end or relationship, a start not bound while another node is, a named path (its start is the
  graph's node, which the navigation's map does not read beside the navigated instance), or an expression reading a
  node outside the pattern; and any comprehension in an ORDER BY (its key reads it beside the projected items). The
  graph is read as its rows first. The navigations of §12.31 keep every other comprehension. Since §12.71 a node's
  value carries its identity, so a path's text reads its start node's: Pattern2/[1] and [2], which §12.31 read as
  navigations, read this way now.
- **A first OPTIONAL MATCH that finds nothing is one row of nulls** (`OPTIONAL MATCH (a:A {name: 'zz'}) RETURN a`):
  it read as a MATCH over a model with classes, so it answered no row where Neo4j answers one (`orNullRow`: a
  one-row relation left-joined to the pattern's rows on nothing, their variables optional). The TCK's first
  OPTIONAL MATCHes read labels the model lacks, which §12.20 and §12.45 already answered; `lite/optional.yaml`'s
  new cases pin it. `keys(n)` of a node or relationship an OPTIONAL MATCH may not find is null where it is absent
  (`absentOr`, Graph8/[4], [7], which read through the graph before).
- **Pinned.** `CypherToPureTest` (the comprehensions the navigations do not read: grouped, joined with commas,
  left-joined back); `lite/patternlists.yaml`'s 15 cases and `lite/optional.yaml`'s 7 new ones, each judged by
  Neo4j; `lite/patterns.yaml`'s five cases named for their refusal renamed for what they read now.

#### 12.72.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4836 | 3704 (from 3,671) | 0 | 0 | 0 | 328 (from 339) | 747 | 57 |
| Postgres | 4836 | 3302 (from 3,269) | 0 | 0 | 0 | 730 (from 741) | 747 | 57 |

Pattern2/[1], [2], [3], [8], [9], [11] move to PASS on both lanes (Pattern2/[1] and [2] had left PASS with
§12.71's identities, for the refusal of an outer read inside a navigation's map; the rows read them now); the 15
cases of `lite/patternlists.yaml` and the 7 new ones of `lite/optional.yaml` PASS on both. TCK agreement DuckDB
3520 → 3526, Postgres 3141 → 3147. Left: Pattern2/[7] (a comprehension inside a list comprehension, reading its
variable), the nested EXISTS subqueries (ExistentialSubquery3/[1]-[3], ExistentialSubquery2/[2]).

### 12.73 The expression tail: UNION over nodes, float remainders, computed SKIP and LIMIT, paths compared, range, toString of values (2026-10-17)

- **UNION (distinct) over nodes and relationships** (Union1/[4]): their values carry their identities (S85), so the
  database's DISTINCT over the parts' rows tells two nodes apart as Neo4j does. A part whose node or relationship value
  carries no identity, a relationship over a property, is refused by name; parallel relationships of one type over an
  association share their pair's identity (S74) and fall together, as S74 records.
- **`%` of floats** (Precedence2/[2]): Pure's `rem` of the two as floats, a Float, the remainder with the dividend's
  sign as Neo4j's; a spelled zero divisor is marked NaN as a division is. Postgres's dialect refuses a double
  remainder by name, so the case passes on DuckDB and is refused on Postgres (a lane divergence, as the nanosecond
  values are).
- **A SKIP or LIMIT the database computes** (`SKIP toInteger(rand()*9)`, ReturnSkipLimit1/[3]; `LIMIT
  toInteger(ceil(1.7))`, ReturnSkipLimit2/[6]): `slicedByRowNumber` numbers the rows in the sort's order (Pure's
  `rowNumber` over the sort), computes the counts once over one row beside them (Neo4j's counts read no variable),
  keeps the rows beyond the SKIP and within the LIMIT, and sorts them by their number. A negative count raises in the
  database (`fail`), as Neo4j's does; a count that is not an integer is Neo4j's error, one that may be null refused.
- **Two paths compared** (Comparison1/[14]): equal when their elements' identities are, in order (`pathIdentities`, the
  stored directions apart). A node, a relationship or a path against a value of a known other kind (a scalar, a list
  or map of known shape, a path against a node) is never equal, as values of different types.
- **range(...)** (List11/[3]): a step the row computes (`computedStepRange`: the last value and the end spelled by the
  step's sign, which the database computes), a bound or step that may be null raising where it is (Neo4j's TypeError),
  a zero step raising. The Postgres dialect writes a raise inside generate_series's arguments with a volatile piece
  (`SqlWriter.planned`): Postgres's planner estimates the function's rows from its arguments and evaluated the raise
  before any row asked for it, as an unnest's argument already was.
- **toString of a list, a map, a node, a relationship or a path** (TypeConversion4/[10]): Neo4j 2026's texts, built
  by the database: a list's elements' texts within brackets and a map's `key: text` entries within braces, joined with
  `", "` (a string unquoted, a null `null`); a node `(:A:B)`, its labels; a relationship `[:T]`; a path its nodes and
  relationships with their stored directions (`(:A)-[:T]->(:B)`), each read from the value the database holds. Over a
  graph the projection reads the graph's rows first (a class extent's projection folds no map over a Variant;
  `textsGraphValue`); in a WHERE over a graph it is refused by name. Postgres's `->>` by a computed index casts it to
  an integer, as its `->` did.
- **Pinned.** `PostgresDialectTest` (the raise inside a range's arguments); `CodeShapeGuardrailTest`'s allowlist (the
  writer's depth, dated); `lite/exprtail.yaml`'s 16 cases, each judged by Neo4j. Left: NaN inside a list or map (the
  JSON holds none; §5), the duration multiplied or divided by a number (W5's temporal tail).

#### 12.73.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4852 | 3731 (from 3,704) | 0 | 0 | 0 | 315 (from 328) | 749 (from 747) | 57 |
| Postgres | 4852 | 3327 (from 3,302) | 0 | 0 | 0 | 719 (from 730) | 749 (from 747) | 57 |

ReturnSkipLimit1/[3], ReturnSkipLimit2/[6], Union1/[4], Comparison1/[14], List11/[3] and TypeConversion4/[10]'s five
examples move to PASS on both lanes, Precedence2/[2]'s three on DuckDB; the 16 lite cases PASS (two BOTH_ERROR, Neo4j's
errors) on both, `float_remainders` refused on Postgres. TCK agreement DuckDB 3526 → 3537, Postgres 3147 → 3157.

### 12.74 The temporal tail, leg 1: durations by numbers, zoned texts read back (2026-10-17)

- **A duration multiplied by a float, or divided by a number** (Temporal8/[7]): Neo4j's `DurationValue.approximate`,
  spelled for the database (`approximated`): each slot scaled as a double; the months' whole kept (truncated toward
  zero, Java's long cast) and their fraction carried into the days at the average month (2,629,746 seconds), the
  days' fraction into the seconds at 86,400, the seconds' into the nanoseconds at a billion, each step in doubles in
  Neo4j's order; the nanoseconds normalized into a second as any duration's. An integer factor stays exact (each
  slot multiplied); a division by zero raises. The result is of nanosecond precision.
- **A zoned value's text parsed back** (`time(toString(d)) = d`, Temporal6/[3]; `datetime(toString(d))`, [5]): the
  text `temporalText` prints for a zoned time or date-time carries the value it prints (`Carried.printedFrom`), and
  the parse of a text of the row that is such a printing reads the value itself, print then parse being the identity
  (as a duration's text, §12.21). A column never holds the source (`Col`'s canonical constructor drops it), so a
  printed text carried through a WITH and parsed after is refused by name, as any zoned text of the row. Both cases
  are nanosecond values: DuckDB passes, Postgres refuses by name (§12.12).
- **Physically out of reach, logged (§5).** Nanosecond date-times of the year 1 and of the year 9999 (26 TCK rows: the
  databases' nanosecond timestamps hold 1678 to 2261); years of ±999,999,999 (2 rows: past the databases' dates).
- **Pinned.** `lite/temporaltail.yaml`'s 8 cases, each judged by Neo4j (one BOTH_ERROR: a division by zero, one
  refused by name: a printed text parsed after a WITH).

#### 12.74.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4860 | 3743 (from 3,731) | 0 | 0 | 0 | 310 (from 315) | 750 (from 749) | 57 |
| Postgres | 4860 | 3337 (from 3,327) | 0 | 0 | 0 | 716 (from 719) | 750 (from 749) | 57 |

Temporal8/[7]'s three examples move to PASS on both lanes, Temporal6/[3] and [5] on DuckDB. TCK agreement DuckDB
3537 → 3542, Postgres 3157 → 3160.

### 12.75 The temporal tail, leg 2: zones the database reads (2026-10-17)

- **A zone whose offset the database reads** (`Zone.read`): beside the spelled offsets and named zones of §12.8 and
  §12.11, a zone may be an offset the database computes, in seconds. `shifted` steps by it (one `adjust` of the
  seconds, or of their nanoseconds for a nanosecond value); its text is the database's (`offsetTextOf`: `Z` for none,
  else the sign and the hours and minutes of its size read as a time on the epoch day, the seconds when there are
  any); `offsetSeconds` is it and `offsetMinutes` its whole minutes toward zero (Java's).
- **A stored zoned value's zone** (W5 leg 2c): the value class's offset slot (`offsetSeconds`), read beside the
  instant slot (§12.65), so `d.timezone`, `d.offset`, `d.offsetMinutes`, `d.offsetSeconds`, the wall-time fields,
  `toString`, a duration added to a zoned time of day (its wall time wraps within the day), and every constructor
  over the value read its own offset (Temporal5/[4], WithOrderBy2/[15] and [16]).
- **Through a WITH** (`ZONE_PART`, `REGION_PART`): a column holds no expression of the scope it was computed in
  (`Col` drops a read zone, as it drops a printed text's source, §12.74); the projection writes the offset (and the
  region, below) as hidden columns named for the value's column, and a read of the column attaches them again. A
  node carried through a WITH carries its zoned properties' zones the same way, the columns named for the property's
  column (`zoneColumns`; null in an arm whose class lacks the property, so the arms' columns agree). A step that
  renames the columns (a re-anchoring, a variable-length chain) does not carry them: a zoned property read after it
  refuses by name, as before.
- **A named zone's offset at an instant** (`namedOffset`): the database's wall time of the instant in the zone
  against the instant, in seconds. The time of a date-time in a named zone keeps that offset alone (Neo4j's Time is
  of an offset: Temporal3/[3] #16 to #20); `d.offset` of a named zone is its text; `toString` of a value in a named
  zone is the offset at its instant and the zone's name in brackets, Java's spelling (Temporal6/[7]).
- **A stored region** (Temporal5/[6]): the oracle's dump spells a date-time of a named zone with the zone's name in
  brackets after its offset (`snap.py`'s `stored`; the driver's text is the offset alone: tools/neo4j-oracle/
  README.md), the harness declares a third slot, `zone: String[0..1]`, on the zoned date-time class when a stored
  value names one, and the island reads it beside the offset (`Zone.region`, String[0..1], empty for a value of an
  offset alone). `timezone` is the region where the row names one, else the offset's text; `toString` appends it in
  brackets. A value moved in it (a duration added, a constructor over it, a truncation, `duration.between` against a
  local value) takes the region's rules where the row names one: the instant is the database's reading of the wall
  time in the region (else the wall time shifted back by the offset), and the moved value's offset the region's at
  the new instant (`toInstant`, `after`); across a change to summer time the offset follows (`+01:00` in January,
  `+02:00` six months on).
- **A resolver fix** (core, StoreResolver.registerAssociationJoins): a path that crosses an association and then
  reads an embedded property of the hop's target (`$a.T.d.instant`, `d` an embedded value of the target's class) took
  the embedded property for a second association hop and named it unmapped. The hop walk now stops at a segment the
  target's class source binds (embedded or slot): the rest is a read on the target's row, as at the root.
  `RelationalMappingIntegrationTest`'s embedded-on-an-association's-target case pins it.
- **Pinned.** `lite/zones.yaml`'s 18 cases, each judged by Neo4j: stored offsets read directly, through a WITH and
  as a carried node's property; stored regions read, carried, moved across a change to summer time, rebuilt,
  truncated, converted and measured; the time of a named-zone date-time; a named zone's texts; a zoned value grouped
  and one an OPTIONAL MATCH does not find.

#### 12.75.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4878 | 3782 (from 3,743) | 0 | 0 | 0 | 289 (from 310) | 750 | 57 |
| Postgres | 4878 | 3365 (from 3,337) | 0 | 0 | 0 | 706 (from 716) | 750 | 57 |

On DuckDB WithOrderBy2/[15] (three examples) and [16] (two), Temporal3/[3] #16 to #20, Temporal5/[4] and [6] and
Temporal6/[7] move to PASS; on Postgres Temporal3/[3] #16 to #20 (the others are nanosecond values, which Postgres
refuses by name, §12.12). TCK agreement DuckDB 3542 → 3555, Postgres 3160 → 3165. Of lite's own cases, the 18 of
`lite/zones.yaml` and eight earlier ones pass: `lite/stored.yaml`'s zoned time moved and ordered, zoned date-time
moved and compared, and two cases named for the zone's refusal (`a_stored_zoned_time_s_zone_refused_by_name`,
`a_stored_zoned_time_s_text_refused_by_name`); `lite/temporal.yaml`'s four named for a named zone's refusal
(`datetime_named_zone_offset_refused_by_name`, `time_of_a_named_zone_datetime_refused_by_name`,
`nanos_time_of_a_named_zone_datetime_refused_by_name`, `to_string_in_a_named_zone_refused_by_name`). The six so
named are renamed with the next leg.

### 12.76 Temporal values inside lists and maps, read back (2026-10-17)

- **The spelling** (`jsonText`, unchanged): a Variant holds a temporal as the oracle spells one, `Date('...')`,
  `Time('...')`, `DateTime('...')` (a zoned value as its instant's, its zone not carried, S65, S68). A list or map
  of times, local date-times and zoned values is now spelled so too (`scalarJsonText`; a duration inside one is
  still refused by name), and `collect` of them builds the same text (`collect(DISTINCT ...)` of a zoned value is
  refused by name: two values of one instant in two zones would be one).
- **Read back** (`readsBack`, `elementsOf`, `temporalOfText`): where the list or map is read as values -- an UNWIND,
  an index, a comprehension or a quantifier, IN, a map's entry -- the database parses the text inside the oracle's
  spelling: a date's (`parseDate`), a time of day's on the epoch day, a date-time's, each to the nanosecond where
  the elements are (the list's mark, `NANOS_ELEMENT`). A date had been read by `toMany(@StrictDate)` of its text,
  which the database refuses (`Date('1910-05-06')` is no date: a latent error no case reached); it reads by the
  parse now.
- **A collection of temporals held as a Variant** (`collVariant`): the JSON text of its elements' spellings, never
  the database's own JSON of a date (its ISO text, which neither the judge nor a read back takes for the date).
- **Precision across a list** (`asNanos`): IN of a microsecond time among nanosecond ones (or the reverse) reads the
  microsecond side at nanosecond precision, as the comparisons read both; an index read keeps the elements' mark;
  an absent entry is the nanosecond null.
- **A minute-precision local date-time literal** (`isoLiteral`): `localdatetime('2017-01-01T12:00')` is Pure's
  date literal to the second, its seconds zero; Pure's minute literal has none, and its `second()` is Pure's error,
  which `toString` read (it printed `2017-01-01T12:00:`, a latent error no case reached).
- **The judge** (`CypherValues.sameLocalDateTime`): two local date-times of the value grammar are equal when they
  name one local date-time (Neo4j spells nine digits of the second, the island trims them), as two local times
  already were.
- **Renamed.** Twelve lite cases named for a refusal that now pass drop the `_refused_by_name` suffix (their answers
  carried to the new keys: the answer's sha is over the query, not the name).
- **Pinned.** `lite/temporallists.yaml`'s 14 cases, each judged by Neo4j; WithOrderBy1/[45] #6 to #8 (dates, local
  times, zoned times sorted as compared) on DuckDB, #6 on Postgres (the others are nanosecond values).

### 12.77 Nested and multi-hop EXISTS subqueries (2026-10-17)

- **The normal form** (`CypherParser.nested`): `EXISTS { MATCH (m) WHERE EXISTS { pattern } }` and `EXISTS { MATCH
  (m) WHERE pattern }`, the outer match a lone node whose variable the inner pattern binds (its labels and
  properties moved onto the inner pattern's node when only one level constrains it), are the inner pattern's own
  existence: a lone node has no relationship, so the two matches' uniqueness rules agree, and the rows the outer
  match adds are the inner pattern's node. A shape rearranged, nothing computed; an inner pattern that does not
  bind the node (the outer match then counts every node) stays as written.
- **More than one hop** (`variableLengthPredicate`, `withPatternFlags`): a pattern predicate or EXISTS subquery of
  more than one hop in a MATCH's WHERE over a graph reads the pattern as rows and the WHERE over them, the predicate
  the flag of §12.52, its pattern the re-anchored chain (relationship uniqueness within it, so `(l)<-[:R]-(n)-
  [:R]->(m)` needs two of n's relationships); a variable of the subquery's own, which the rows do not carry, is the
  pattern's (as an unnamed node is).
- **A count in the subquery** (`CypherAst.Counted`, `countedExists`): `EXISTS { MATCH pattern [WHERE w] WITH keys,
  count(*) AS c [WHERE having] RETURN ... }` with keys the scope's own variables (one group, or none where the
  pattern matches nothing) holds where the matches, counted as their comprehension's size, are some and `having`
  holds of the count; a key of the subquery's own, any other WITH, is refused by name.
- **Pinned.** `lite/existsnested.yaml`'s 11 cases, each judged by Neo4j; ExistentialSubquery3/[1], [2], [3] and
  ExistentialSubquery2/[2] on both lanes.

#### 12.77.1 The ledger (§12.76 and §12.77 together)

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4903 | 3817 (from 3,782) | 0 | 0 | 0 | 279 (from 289) | 750 | 57 |
| Postgres | 4903 | 3394 (from 3,365) | 0 | 0 | 0 | 702 (from 706) | 750 | 57 |

On DuckDB WithOrderBy1/[45] #6 to #8, ExistentialSubquery3/[1], [2], [3] and ExistentialSubquery2/[2] move to PASS;
on Postgres WithOrderBy1/[45] #6 and the four EXISTS rows. TCK agreement DuckDB 3555 → 3562, Postgres 3165 → 3170.
The 25 lite cases of the two new files pass on both lanes (the nanosecond ones refused by name on Postgres).

### 12.78 An OPTIONAL MATCH whose WHERE reads the rows (2026-10-17)

- **The join's condition** (`CypherToPure.anchored`, `reanchor`, `JoinScope`): an OPTIONAL MATCH re-anchored over
  the rows (§12.49) whose WHERE reads a value of the rows beside its pattern (`OPTIONAL MATCH (b)-[r2]-(c) WHERE r
  <> r2`, `... WHERE a1 = a2`, `... WHERE y.w > x.v`) is the left join's condition, ANDed to the identities the join
  meets on: a row keeps its nulls where no match holds of it, as Cypher's OPTIONAL MATCH does, which a filter
  before the join (the pattern's rows alone) or after it (dropping the row) would not. The condition reads the
  pattern's own variables from the match's side and every other from the row's (`JoinScope`, two `RowScope`s
  naming their side `l` and `r`); the comparison is the island's own (identity for nodes and relationships, §12.46;
  Cypher's null rules for values), nothing computed. A WHERE holding a pattern or an aggregate of its own, and such
  a WHERE in a pattern predicate or a comprehension over rows, is refused by name.
- **A WHERE over a fanning hop** (`optionalFans`): an OPTIONAL MATCH's WHERE over a hop that fans out (a to-many
  step) reads as rows first, the re-anchored pattern's WHERE over its rows or the join's, so `MATCH (x:X) OPTIONAL
  MATCH (x)-[:T]->(y:Y) WHERE y.w > x.v` keeps the matches that hold and the row where none does; a to-one hop keeps
  the guard of §12.29.
- **A named optional path** (`p = (a)-[:T]->(b)` in an OPTIONAL MATCH) reads through the re-anchoring as a named
  path after rows (§12.70), its value null where the pattern finds nothing.
- **Left** (plan §5): a variable-length relationship beside other relationships in one pattern (Match7/[19]); a
  pattern comprehension per element of a list comprehension (Pattern2/[7]).
- **Pinned.** `lite/optrows.yaml`'s 6 cases, each judged by Neo4j; Match7/[11] and MatchWhere6/[5] on both lanes.

#### 12.78.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4909 | 3827 (from 3,817) | 0 | 0 | 0 | 275 (from 279) | 750 | 57 |
| Postgres | 4909 | 3404 (from 3,394) | 0 | 0 | 0 | 698 (from 702) | 750 | 57 |

Match7/[11] and MatchWhere6/[5] move to PASS on both lanes; TCK agreement DuckDB 3562 → 3564, Postgres 3170 → 3172.
The 6 cases of `lite/optrows.yaml` pass on both lanes, and two lite cases named for the refusals this lifts pass,
renamed (`lite/anchor/an_optional_match_where_reading_the_rows`, `lite/optional_where/where_over_a_fanning_hop`; their
answers carried to the new keys).

### 12.79 Floats that are not finite inside lists, a relationship's label, types differing only in case (2026-10-17)

- **NaN and the infinities in a list or map** (`CypherToPure.elementText`, `asVariant`): a float divided by a zero
  literal (`NotANumber`, NaN or an infinity in Neo4j) inside a list, a map or a `collect` is the Variant the
  database builds of it (`toVariant`), where every other float is spelled as its text: DuckDB's JSON writes `NaN`,
  `Infinity` and `-Infinity`, which no JSON number spells, and reads them back; the value is the database's, as the
  scalar's already was (S60). Postgres's float division raises on a zero divisor first, where Neo4j answers: the
  decided outcome S60 already records for the scalar, now for these (`decisions.tsv`).
- **Their order** (`numberKey`): the orderability key ranks a number beyond the largest double `80` for the infinity
  and `81` for NaN (both databases order NaN beyond the infinity, which the database parses from its text), so
  `-Infinity`, the numbers, `Infinity`, NaN, then null, as Neo4j orders them; `-Infinity` takes the lowest number
  key (S81).
- **The judge** (`DuckJson`, the parity grid's and the Cypher judge's): a DuckDB JSON cell read by the JSON grammar
  and DuckDB's three tokens for a double that is not finite; the product's JSON library, which refuses them as JSON must, is not loosened.
- **A relationship's label** (`labelPredicate`): `r:T` is true when the relationship's type is T (Neo4j 5's label
  expression on a relationship), each label written compared with the type (a relationship has one type, so
  `r:T:U` never holds), null where an OPTIONAL MATCH did not find it.
- **Types differing only in case** (the harness, `CypherGraph.RelType.sql`): two relationship types spelled alike
  but for case (`T2` and `t2`, two Pure names) were refused because their tables and columns, named in lower case
  (the databases fold an unquoted name's case), would be one; the second takes a suffix no type's lower-case name
  has.
- **Pinned.** `lite/nonfinite.yaml`'s 8 cases and `lite/rellabels.yaml`'s 5, each judged by Neo4j;
  ReturnOrderBy1/[11], [12], WithOrderBy1/[21], [22] and Graph5/[2].

#### 12.79.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4922 | 3846 (from 3,827) | 0 | 0 | 0 | 270 (from 275) | 750 | 56 |
| Postgres | 4922 | 3423 (from 3,404) | 0 | 0 | 0 | 693 (from 698) | 750 | 56 |

ReturnOrderBy1/[11], [12], WithOrderBy1/[21], [22] and Graph5/[2] move to PASS on DuckDB, and on Postgres the four
NaN rows by the decision S60 (the division's error) and Graph5/[2]; TCK agreement DuckDB 3564 → 3569, Postgres
3172 → 3177. The 13 cases of `lite/nonfinite.yaml` and `lite/rellabels.yaml` pass on both lanes (the NaN ones
on Postgres by S60), and `lite/rank/nan_inside_a_list`, named for its refusal before, is renamed (its answer carried
to the new key).

### 12.80 A comprehension over a path's parts, EXISTS among a projection's items (2026-10-17)

- **A comprehension over a named path's parts** (`CypherToPure.unrolledOverPaths`, `Renaming`): after `MATCH p =
  (n:X)-->()`, `RETURN [x IN nodes(p) | size([(x)-->(:Y) | 1])]` has no node to start the inner pattern from (the
  comprehension's variable is an element of a list); a fixed-length path's parts are the MATCH's own variables, so
  the comprehension, with no WHERE and a projection that holds a pattern, reads as the list of its projection per
  part, `[size([(n)-->(:Y) | 1]), size([(m)-->(:Y) | 1])]`, each pattern then from a node the MATCH binds. Read in
  the RETURN or WITH right after the MATCH, where the parts are in scope; the path's unnamed parts named for it with
  synthetic names, which no `*` lists. The substitution renames the comprehension's variable where it is free (a
  lambda binding the name shadows it) and keeps the expression as written where a binder inside would capture the
  part's name. A shape rearranged, nothing computed.
- **EXISTS or a pattern predicate among a projection's items over rows** (the projection's `withPatternFlags`):
  read as a flag column of the rows, as a WHERE's is (§12.52), the projection keeping its items alone; so
  `RETURN EXISTS { (b)-->(:C) } AS e, count(*)` over rows groups by the flag.
- **Tried and withdrawn: a null grouping key on Postgres.** Postgres refuses a bare NULL GROUP BY key
  (`ConstantKeysAsExpressions`: a constant key is an expression only where it is typed), eleven TCK rows. Casting the
  key to the plan's declared type of its column in the lowering lets them run and keeps the IQ corpus green; one of
  them, ReturnOrderBy6/[5], then reaches the planner with `toOne` of a column stamped `[0..0]` (the raise, typed
  VARCHAR by Postgres's spelling) in a CASE arm never taken, added to a count. Casting the raise to its value's
  carrier fixed it and broke 125 IQ cases on Postgres and 29 on DuckDB (a typed raise changes a CASE's type
  unification, the quotients' scale). Both withdrawn; the rows stay refused by name; the fix belongs to the island
  (an item that is a null constant read back as one, so no `toOne` is spelled), the plan's §5.
- **Pinned.** `lite/pathunroll.yaml`'s 8 cases, each judged by Neo4j; Pattern2/[7] on both lanes.

#### 12.80.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4930 | 3855 (from 3,846) | 0 | 0 | 0 | 269 (from 270) | 750 | 56 |
| Postgres | 4930 | 3432 (from 3,423) | 0 | 0 | 0 | 692 (from 693) | 750 | 56 |

Pattern2/[7] moves to PASS on both lanes; TCK agreement DuckDB 3569 → 3570, Postgres 3177 → 3178. The 8 cases of
`lite/pathunroll.yaml` pass on both lanes.

### 12.81 The spec guards' repair (2026-10-17)

- **What was red.** `//spec:spec_tests` and four of the generated files' diff tests, on the branch since the W2, W3,
  W5, W10 and W11 legs; the per-leg chain ran the Cypher lanes and three core suites, never these. Four causes:
  lite natives landed without moving the catalog's NOT_UPSTREAM pin (`CatalogUpstreamDiffTest`), elementId with no
  claim (`native-claims.tsv` UNCLAIMED, a ceiling of zero), the implementation table's measured kinds not
  re-measured (`ratchets.tsv`), and the generated prelude edited by hand (the two store data types of W5 written
  into it, which also dropped the file's carriage returns, kept byte for byte by `.gitattributes`).
- **The store data types are the platform's own elements.** `meta::relational::metamodel::datatype::Time` and
  `TimestampNs` (lite's extension of the store grammar, S64) are declared in the system metamodel
  (`SystemMetamodel`'s Pure section), where `PlatformNamesSpellingTest` reads the platform's own names; the prelude
  is the generator's again, byte for byte.
- **elementId is claimed as a resolver form** (`NativeFn.ResolverForm.ELEMENT_ID`): the store resolver rewrites it to
  the read of the instance's primary key (`StoreResolver.elementIdRead`), so it is a form of that layer, as
  `tdsContains` and Variant's `get` are; the resolver dispatches on the form, not on the name.
- **rand() is upstream's native.** The regenerated prelude dropped legend-pure's `essential/math/random.pure`: the
  claimed bare name `random` (lite's twin) made upstream's a platform-owned name. §12.51's premise was wrong:
  legend-pure declares `meta::pure::functions::math::random(): Float[1]`. The platform now implements upstream's
  declaration (`native-membership.tsv`, its signature generated verbatim; `Scalars`' RANDOM rule moved to it), the
  translator emits it, and lite's `meta::legend::lite::random` left: the INTERNAL_DESUGAR ceiling 27 → 26
  (`NativeCatalogGovernanceTest`), the census's row, the implementation table's Unimplemented 71 → 70.
- **The pins moved, dated.** NOT_UPSTREAM 48 → 54 (elementId's two overloads, recurse, isKind, minBy, maxBy);
  `ratchets.tsv` re-measured (Form 218 → 219, Intrinsic 702 → 708, Unimplemented 71 → 70).
- **The chain from now on** runs `//core:core_tests`, `//spec:spec_tests`, the generated files' and ratchets' diff
  tests, and the IQ and SQL parity lanes on both databases, besides the Cypher lanes.

### 12.82 Keys null on every row (2026-10-17)

- **The case.** `MATCH (me:Person)--(you:Person) WITH me.age AS age, you RETURN age, age + count(you.age)` over a
  graph with no such property (the open graph's typed null, §12.45) groups by a key that is null on every row: one
  group over the rows, none over no rows. Postgres refuses a bare constant key (`ConstantKeysAsExpressions`: a
  constant is an expression only where it is typed), so eleven TCK rows were refused by name there (§12.80).
- **The key typed by its plan (the lowering).** A grouping key the lowering finds to be `NULL` (a typed empty)
  groups as `CAST(NULL AS t)`, `t` the plan's declared type of its column (`Lowerer`, the group-by keys; the type
  read off the contract, never inferred). DuckDB answers the same; the IQ and SQL corpora unchanged.
- **The key read back as the null it is (the island).** A projected column whose value is a typed null constant
  carries the mark `NullConstant` (`CypherToPure.columnDetail`): the clauses after read it as that null
  (`RowScope.variable`), never as the column, so `age + count(...)` is null by the island's own rule and no
  `toOne` of the column is spelled. That `toOne` of a column the typer stamps `[0..0]` lowers to the raise, which
  Postgres types as text inside a CASE when it plans (`varchar + bigint`, ReturnOrderBy6/[5]); typing the raise in
  the lowering instead broke the IQ corpus (§12.80), so the island spells nothing to raise. The column itself
  stays: it is the grouping's key, which keeps "no groups over no rows".
- **A sort key null on every row orders nothing.** An ORDER BY item, a hidden key or a key after a grouping whose
  value is a null constant is dropped (as an open graph's absent property under a SKIP or LIMIT was, §12.45):
  every row ties on it, so the sort is the rest of the keys, or none. Nothing is computed: the null is the
  expression's own static value.
- **One arm only.** A label-less read projects one arm per class, and the rows keep the first arm's columns: a value
  null in some arms only is not marked, and a sort key null in some arms only is kept in every arm, so the arms'
  columns agree (a key dropped in one arm only was the one regression the first run found,
  `lite/twoways/across_the_classes_of_a_labelless_node`).
- **Pinned.** `lite/nullkeys.yaml`'s 10 cases, each judged by Neo4j; CypherToPureTest's
  `aTypedNullItemReadsBackAsTheNullItIs`; Return6/[3], [6], [18], [19], ReturnOrderBy6/[2], [3], [5],
  WithOrderBy4/[17], [18], With6/[6], [7] on Postgres.

#### 12.82.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4940 | 3865 (from 3,855) | 0 | 0 | 0 | 269 | 750 | 56 |
| Postgres | 4940 | 3453 (from 3,432) | 0 | 0 | 0 | 681 (from 692) | 750 | 56 |

The eleven rows move to PASS on Postgres (they passed on DuckDB); TCK agreement DuckDB 3570, Postgres 3178 → 3189.
The 10 cases of `lite/nullkeys.yaml` pass on both lanes.

### 12.83 A walk beside single hops (2026-10-18)

- **The case.** `MATCH (a {name: 'A'}) OPTIONAL MATCH p = (a)-->(b)-[*]->(c) RETURN p` (Match7/[19]): the walk
  (§12.68) read a pattern of one variable-length relationship between two nodes, so a pattern with single hops
  beside it was refused by name (§12.78, the plan's §5).
- **The pattern cut at the walk** (`CypherToPure.walk`): the part before the walked relationship (its first node to
  the walk's start node) is the start node's rows and the part after it (the walk's far node to the last) the far
  node's, each read as a MATCH of its own, as the node alone was; the walk joins the first on the start node's
  identity and the second on the node it reaches. Every node and every single hop is named first (synthetic names,
  which no `*` lists), so the rows carry each with its identity.
- **Relationship uniqueness across the cut.** Each side's own reading spells its own; across, a single hop's
  relationship is never one the walk walked (its identity, an edge's own or the ordered pair of its ends', S74,
  between bars in the text of the identities walked, as the walk's own step reads it), and a hop before the walk
  never one after it (their identities differ). Two hops each of one type, and not the same, are never one
  relationship and are not compared. A hop whose rows carry no identity of its own (over several edge classes, the
  arms' columns) is refused by name.
- **The named path across the three parts.** Its JSON (§12.27's shape) is the part before the walk (its nodes and
  relationships in order), the walk's text (each relationship and the node it reaches), then the part after the far
  node; the stored directions and the relationships the same three, joined by commas where the walk's is not empty
  (depth zero); its length the walk's depth plus the single hops; its nodes the three parts' nodes. The single hops
  are read as segments of the path (§12.56) where the path is named, so each carries its stored direction.
- **The walk's length.** `CypherToPure.walk` grew past `//core:guardrails`' 250 lines; the uniqueness across the
  cut is its own method (`uniqueAcrossTheWalk`).
- **Refused by name, still:** another variable-length relationship in the pattern, a node named on both sides of
  the walk (a cycle through it), the walk's two ends one node.
- **Pinned.** `lite/walkbeside.yaml`'s 11 cases, each judged by Neo4j; CypherToPureTest's pinned refusal of a walk in
  a pattern of other relationships turned into a translation check (the hop's uniqueness against the walk) and a
  refusal of a second variable-length relationship; `lite/varlen/an_unbounded_walk_in_a_pattern_of_other_relationships_is_refused`
  renamed `an_unbounded_walk_then_a_hop` (its answer carried); Match7/[19] on both lanes.

#### 12.83.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4951 | 3878 (from 3,865) | 0 | 0 | 0 | 267 (from 269) | 750 | 56 |
| Postgres | 4951 | 3466 (from 3,453) | 0 | 0 | 0 | 679 (from 681) | 750 | 56 |

Match7/[19] moves to PASS on both lanes; TCK agreement DuckDB 3570 → 3571, Postgres 3189 → 3190. The 11 cases of
`lite/walkbeside.yaml` and `lite/varlen/an_unbounded_walk_then_a_hop` pass on both lanes.

### 12.84 Neo4j's own errors where the island refused (2026-10-18)

- **What hid.** The harness counts a case as BOTH_ERROR when lite and Neo4j both err, a refusal by name of lite's
  included: 23 TCK rows were "agreeing" only because the island refused what Neo4j rejects (CypherParity's
  `outcome`: a `NotImplementedException` beside Neo4j's error). The rows' status does not move; what the island
  raises does: Neo4j's error, where it has one.
- **A conversion of a value it does not take** (TypeConversion1/[5], TypeConversion2/[8], TypeConversion3/[6]: 12
  rows). Neo4j's `toBoolean`, `toInteger` and `toFloat` raise a TypeError (InvalidArgumentValue) at run time for a
  node, a relationship, a path, a list or a map (`toBoolean` a float too, `toFloat` a boolean): the island's
  `invalidArgument` spells the database's raise (`fail`, behind the value's presence: null stays null) with Neo4j's
  message, so a query over no rows raises nothing, as Neo4j's does not (measured on DuckDB and Postgres). Which
  values are rejected is what the translation knows them to be (`rejectedByConversions`: a Variant of no known
  shape is not one of them, and stays refused by name). A literal argument is Neo4j's type mismatch at compile time
  (`toFloat(true)`), the accepted types listed as Neo4j's compiler lists them.
- **Two conversions Neo4j takes, read.** `toBoolean` of an integer (false for 0, true for any other) and `toInteger`
  of a boolean (1 or 0), refused by name before.
- **Neo4j's static errors**, raised with the variable errors before any clause is read (`checkExpression`, §12.63's
  scan): `size()` of a pattern (List6/[6], 8 rows: a pattern expression only tests existence) and a property of a
  path (MatchWhere1/[14]); a list comprehension's or a quantifier's variable shadows a path of its name. An update
  clause inside EXISTS (ExistentialSubquery2/[3]) is the parser's InvalidClauseComposition.
- **A function Neo4j does not define** (Return2/[18]): `Unknown function 'foo'`, Neo4j's error, where the island
  refused every function it does not read by name. The island tells the two apart by Neo4j's own list of its
  functions (`CypherFunctions`, 173 names lower-cased): spec by verification, the oracle's `snap.sh --functions`
  snapping `SHOW FUNCTIONS` into `cypher-parity/cypher-functions.txt`, every `--check` comparing it with the server,
  and `CypherParityTest` holding the constant to the file. A name Neo4j defines that the island does not read
  stays a refusal by name.
- **Two crashes behind BOTH_ERROR, fixed** (found scanning lite's errors beside Neo4j's): `size()` of a path
  (List6/[5]) threw a NullPointerException and is Neo4j's type mismatch in the statement scan; a float literal
  beyond the largest double (Literals5/[27]) threw a NumberFormatException and is the parser's FloatingPointOverflow.
  Every BOTH_ERROR row now stands behind a syntax error, a compile error, a raise of the database's or (one) a
  refusal by name.
- **Left (the plan's §5):** `percentileDisc(0.90, deg)` with the percentile read from the rows (Aggregation6/[5]):
  Neo4j raises at run time where the percentile falls outside 0 to 1; the island spells a percentile the database
  takes as a constant only.
- **Pinned.** `lite/errors.yaml`'s 19 cases, each judged by Neo4j; CypherToPureTest's
  `theErrorsRaisedWhereTheTranslatorRefused`.

#### 12.84.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4970 | 3885 (from 3,878) | 0 | 0 | 0 | 267 | 762 (from 750) | 56 |
| Postgres | 4970 | 3473 (from 3,466) | 0 | 0 | 0 | 679 | 762 (from 750) | 56 |

The TCK's rows keep their statuses (agreement DuckDB 3571, Postgres 3190); 22 of the 23 BOTH_ERROR rows a refusal
stood behind now stand behind Neo4j's error, on both lanes. The 19 cases of `lite/errors.yaml`: 7 PASS and 12
BOTH_ERROR on each lane, as Neo4j answers them.

### 12.85 Nanosecond values as a date and a time of day, leg 1: the spike (W13, 2026-10-18)

- **The decision.** The user's ("Let's do it", 2026-10-18), on the plan's W13: replace §12.12's `TIMESTAMP_NS`
  carriage with one every dialect computes, the spike first; it overturns S71 (Postgres refusing nanoseconds by name)
  and adds a register row for how such a value leaves the database (S87).
- **The representation.** A local time of nanosecond precision is its nanoseconds since midnight, an Integer (the
  database's BIGINT); a local date-time is its date (a StrictDate, the database's DATE) and those: java.time's shape,
  `LocalDateTime` a `LocalDate` and a `LocalTime`. The translation's own value is the nanoseconds; the date rides on
  its detail (`Parts`), present exactly when the value is. Every database computes both exactly, over every year its
  DATE holds: the year 1 and the year 9999 (WithOrderBy1's) are dates like any other. A microsecond value is
  unchanged (the database's TIME and TIMESTAMP).
- **What leg 1 spells over the parts.**
  - Literals and texts: the date as written, the clock a sum the database adds (a float product over a literal:
    both databases type two small integer literals 32 bits wide, and their product overflowed, measured).
  - Construction from components, over a base time's nanoseconds where one is given (a field written over it, the
    base's fraction kept unless a sub-second field is written, as §12.7 reads them); selection (`localtime(d)`,
    `localdatetime({date, time})`, `date(d)`).
  - The fields: a date's of its date, the clock's the quotients of the nanoseconds (the floor of the database's
    float quotient, exact below 2^53; a day's nanoseconds are below 2^47; the exact integer quotient the island
    used before repeats its argument six times, and the texts it fed grew without bound).
  - `toString`, Neo4j's spelling from the parts.
  - Comparison and ordering: a time of day by its integer, a date-time by its date then its integer, a microsecond
    value read into the same parts.
  - CASE and coalesce, part by part (a choice of the whole value would keep one part only: measured, a CASE
    returned the bare integer).
  - Carriage through WITH: the date a hidden column beside the value (`DATE_PART`, as a zone's offset rides,
    §12.75), read back by the row scope; WITH DISTINCT and a WITH's grouping keys over both columns; sort keys over
    both parts, hidden keys beside a returned value.
- **The egress (S87).** A returned split value is the ISO text the database writes of its parts
  (`yyyy-MM-ddTHH:mm:ss.nnnnnnnnn`, the clock alone for a time of day), carried by lite's `dateTimeOfText` and
  `timeOfText` (two natives, INTERNAL_DESUGAR 26 → 28, each one lowering rule): the text itself is the cell, under
  the `TEMPORAL_TEXT` label (a cast that never renders), and the executor reads it as the date-time or time of day
  it spells, a time of day's by the column's Pure type (StrictTime). The result is the plan's date-time with its nine
  digits on both lanes; the Java side parses the database's text, as it parses every temporal cell; the database
  computes.
- **The bridge.** What leg 1 does not re-spell takes a split value in §12.12's epoch-nanosecond carriage (`legacy`:
  DuckDB's, 1678 to 2261, a written year beyond it refused by name as before): duration arithmetic, truncation,
  `duration.between`, zoned values, lists, a map's Variant, `collect`, `min` and `max`, a returned grouping key. A
  value already in that carriage (a stored one, a zoned value's wall time) keeps its paths. The bridge goes as the
  legs land.
- **Found by the leg's own cases: Neo4j's checks of a temporal value's components.** A date's fields in order (`day
  cannot be specified without month`), the date whole when a clock is written with no time to take it from (`day
  must be specified`), the hour, then the clock's fields in order (`subsecond cannot be specified without minute`):
  the island built a value Neo4j rejects (Neo4j's builders' `assertDefinedInOrder`, measured on the server). Not
  spelled: a field of another form (`Cannot assign month to week date.`), whose message follows Neo4j's map order
  (plan §5).
- **The checkpoint.** W13 asked the spike two questions. The egress: clean (one label and the decode seam the
  executor already had; the judge reads nine digits on both lanes). The two-part shape: contained (one record on the
  detail, one hidden column, one read-back, the sort keys, CASE and coalesce, about 600 lines with the bridge). So
  the legs go on: leg 2 (durations, truncation, `duration.between`), then the zoned values.
- **Pinned.** `lite/nanoparts.yaml`'s 16 cases, each judged by Neo4j; CypherToPureTest's
  `aNanosecondValueIsItsDateAndItsNanosecondsSinceMidnight`.

#### 12.85.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4989 | 3900 (from 3,886) | 0 | 0 | 0 | 266 (from 267) | 767 (from 764) | 56 |
| Postgres | 4989 | 3518 (from 3,474) | 0 | 0 | 0 | 648 (from 679) | 767 (from 764) | 56 |

TCK agreement: DuckDB 3571 (unchanged: the TCK's local nanosecond rows passed there already, through the old
carriage), Postgres 3190 → 3220, the 30 rows of local construction, selection and comparison that reach no zoned
value, duration or truncation (Temporal1/[5], [7]; Temporal3/[1], [2], [5], [6]; Temporal7/[2], [4]). Nothing
moved back on either lane. The 16 cases of `lite/nanoparts.yaml`: 13 PASS and 3 BOTH_ERROR on each lane, as Neo4j
answers them; `lite/temporal/nanos_year_outside_the_range_refused_by_name` (the year 1) passes on both lanes now,
its name a leftover of §12.74 (renamed with leg 2's run). The rest of Postgres's nanosecond refusals (358 at render,
38 at seeding) wait on the legs after: truncation (Temporal9, 262 rows) above all, most of it of zoned values.

### 12.86 Nanosecond values as a date and a time of day, leg 2: durations, truncation, between (W13, 2026-10-18)

- **What leg 2 spells over the parts** (§12.85's representation: a time of day its nanoseconds since midnight, a
  date-time its date and those), each the database's integer and date arithmetic, over every year its DATE holds:
  - **A duration added or subtracted** (`splitMoved`): the months, then the days on the date (a time of day has no
    date to move); the seconds and nanoseconds on the clock, the seconds' whole days apart so no product overflows
    (the seconds past whole days times a billion, a float product cast back: exact below 2^53, and DuckDB types a
    literal-built slot 32 bits wide, whose integer product with a billion overflowed, measured); the clock's whole
    days carried into the date (the floor of the database's float quotient, exact below 2^53, as §12.85's fields);
    a time of day wrapping within its day, as Neo4j's does. A microsecond value moved by a nanosecond duration is
    read into the parts first.
  - **Truncation** (`splitTruncated`): a unit of a day or more on the date (the unit's start, as a microsecond
    value's: §12.10), a day of the week, quarter or year written over it as that many days on, the year, month and
    day written over it, the clock from midnight; a smaller unit on the clock, floored to the unit (the millisecond's
    and microsecond's starts as the nanoseconds less their remainder), its hour, minute and second written over; the
    sub-second fields written added. A zoned value's wall time is read into the parts, truncated, and its instant
    converted after through the old carriage (the zoned values are leg 3's).
  - **`duration.between`, `inMonths`, `inDays`, `inSeconds` of two local values, one of them split**
    (`splitBetween`): the whole months from a to b (the calendar count, one back where stepping it from a passes b,
    by date then clock), then the whole days from there, then the rest's seconds and nanoseconds -- the days' and the
    clocks' apart (86,400 times the dates' distance in days, plus the clocks' difference's whole seconds), so no
    nanosecond count of a long span overflows: from the year 1 to the year 9999 is 3.2e20 nanoseconds, past a long,
    and 315,537,897,599 seconds, well inside one. Two times of day, the clocks' difference alone. A date is its
    midnight.
- **Found by the leg's own cases.**
  - **Postgres read the year 1 as 2001.** A StrictDate literal reached the SQL as Pure spells it (`DATE
    '1-01-01'`), and Postgres reads a leading field of fewer than three digits as no four-digit year (`'1-01-01'` as
    2001-01-01, `'10-01-01'` as 2001-10-01, measured on the scratch server); DuckDB reads it as the year 1. The
    lowering's date and timestamp literals (`MatchFold.dateLit`) write the year in ISO's four digits now (0 to 9999;
    a year beyond keeps Pure's spelling). No other row needed it: the only cases writing a year below 1000 into SQL on
    Postgres are nanosecond values (§12.74's), refused there until now.
  - **The judge overflowed a long.** `CypherValues.durationSlots` summed a duration text's units as nanoseconds in a
    long; Neo4j's `P9998Y11M30DT23H59M59.999999998S` is fine, but `PT87649415H59M59.999999998S` is 3.2e20
    nanoseconds. The seconds and nanoseconds are split from the exact sum now (BigDecimal's division), each a long.
- **What the bridge still carries** (§12.85's `legacy`): the zoned values and their arithmetic, truncation and
  `between` (leg 3); lists, a map's Variant, `collect`, `min` and `max`, a returned grouping key, `IN` (leg 4).
- **Pinned.** `lite/nanoparts.yaml`'s six new cases (truncation to each unit, with fields written over; durations,
  at the years' ends; between, across the millennia), each judged by Neo4j; `lite/temporal`'s year-1 case renamed
  `nanos_in_the_year_one` (it passes on both lanes since §12.85).

#### 12.86.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 4995 | 3906 (from 3,900) | 0 | 0 | 0 | 266 | 767 | 56 |
| Postgres | 4995 | 3602 (from 3,518) | 0 | 0 | 0 | 570 (from 648) | 767 | 56 |

TCK agreement: DuckDB 3571 (unchanged: the TCK's local nanosecond rows passed there already, through the old
carriage), Postgres 3220 → 3296, the 76 rows of local truncation and arithmetic: Temporal9/[1], [3], [4] (70: the
truncation of local date-times and times to each unit, with fields written over), Temporal8/[2], [4] (6: local
date-times and times moved by durations). Nothing moved back on either lane. The six new cases of
`lite/nanoparts.yaml` PASS on each lane; on Postgres `lite/temporal/nanos_ordering` and `nanos_duration_fields`
pass now too (a duration step and a `between` of local values). The rest of Postgres's nanosecond refusals of the TCK (250
at render, 24 at seeding, 6 by year) are of zoned values above all (Temporal9's 192 zoned truncations, and zoned
construction, selection, arithmetic and comparison in Temporal1, 3, 5, 6, 7 and 8), then lists and stored values
(WithOrderBy1 and 2's 32): legs 3, 4 and 5. DuckDB's TCK refusals are the writes and the 8 by year.

### 12.87 Nanosecond values as their parts, leg 3: the zoned values (W13, 2026-10-18)

- **The representation.** A zoned date-time of nanosecond precision is its instant's parts, the UTC date and the UTC
  nanoseconds since midnight (`Parts` on the detail, as §12.85's local date-time), the zone beside as §12.8 carries
  it (a spelled offset, a region's name, an offset and region the database reads, §12.75). A zoned time of day is
  its UTC nanoseconds from the epoch day's midnight, below 0 or past a day where the offset carries the wall time
  across midnight: §12.11's instant on the epoch day as an integer, so two order as Neo4j's do (its
  `getNanosOfDayUTC`).
- **The conversions, each the database's arithmetic over the parts.**
  - The wall time of an instant (`splitWall`): the UTC parts moved by the offset at the instant, the clock's whole
    days carried into the date; a spelled offset a sum the database adds, a read one the database's, a region's the
    database's reading of the instant's microseconds in it (an offset changes on a whole second, so the digits past
    the microsecond never matter to it).
  - The instant of a wall time (`splitInstant`): the wall time moved back by the offset there, a region's the
    database's instant for the wall time's microseconds against them (§12.8's `instantIn`), a read region's offset
    after the one at the instant (`splitAfter`).
  - A zoned time: its wall time the nanoseconds plus the offset's, within the day; from a wall time, the nanoseconds
    less the offset's.
- **What leg 3 spells over the parts.** Construction from texts, components (the wall time's parts in the zone) and
  `datetime.fromepoch` (the epoch day moved by the seconds' whole days and the rest's, the seconds past whole days a
  float product cast back); a text in a non-calendar form (week, ordinal, quarter) of nanosecond precision too,
  which leg 1 had left in the old carriage. The fields (a zoned value's of its wall time's parts), the offset fields
  (`offset`, `offsetMinutes`, `offsetSeconds` at the instant) and the epoch fields (86,400 seconds a day of the UTC
  date's distance from the epoch day, the whole seconds or milliseconds of the UTC nanoseconds). `toString` (a
  region's offset at the instant, its name in brackets). Comparison and ordering. Carriage through WITH (the UTC date
  the hidden `DATE_PART` column, the zone the column's when spelled, else `ZONE_PART`'s and `REGION_PART`'s, as
  §12.75 carries a read zone). The egress: the instant's ISO text (a zoned time's instant on the epoch day), read as
  the date-time it spells, as §12.8 and §12.11 return a zoned value. Duration arithmetic (a date-time's wall time
  moved by the months and days, then its instant by the seconds and nanoseconds; a zoned time's wall time moved,
  wrapping), truncation (of the wall time, the instant converted after), `duration.between` (every reading §12.10
  and §12.11 give a zoned operand, over the parts). A print then a parse of a local nanosecond value reads the value
  back too (§12.74's identity, which only zoned texts had: Temporal6/[2], [4] on Postgres).
- **Found by the leg's own cases, at either precision.**
  - **One instant in two zones, measured and left to S66.** Neo4j's `=` on zoned values is java.time's `equals`,
    the instant and the zone (`datetime('2017-01-01T12:00+01:00') = datetime('2017-01-01T11:00Z')` is false, so is
    `time('12:00+01:00') = time('11:00Z')`, and `+02:00` against `[Europe/Stockholm]` at one instant), and its order
    breaks a tie of instants by the offsets (the larger after) and then the zones (an offset before a region, two
    regions by name: `+02:00` before `Europe/Stockholm`, `Europe/Berlin` before `Europe/Stockholm`), as its
    `unsafeCompareTo` does. The island answers both by the instant, the register's S66, a decision of the user's
    whose reason (a zone kept with the value would need a Pure type) the carried zone (S65) now answers where the
    island sees the zone: both were spelled over the carried zones and measured green on both lanes, then withdrawn
    for the decision, which stays the user's (the plan's §5).
  - **A wall time a region's clocks show twice.** When summer time ends, the database reads an ambiguous wall time
    as the later instant (DuckDB's ICU and Postgres's AT TIME ZONE, measured: Stockholm's 02:30 on 2017-10-29 is
    +01:00), java.time as the earlier, the offset before the transition (Neo4j's +02:00). The island spells java.time's
    reading over the database's (`regionInstant`): the offset a day before the database's instant read, and the
    instant it names kept where it is earlier and names the same wall time; a gap's reading is the database's, as
    java.time's (forward by the gap).
  - **A zoned time converted to another offset.** `time({time: t, timezone: z})` is java.time's
    `withOffsetSameInstant`: the wall time in the new offset, wrapped within the day (measured: 23:00 at +01:00 to
    +14:00 is 12:00 at +14:00, which orders before 00:00Z); the island had kept the instant on the epoch day, which
    orders it after. Now the wall time wrapped and placed in the offset, at either precision.
  - **`datetime.fromepoch`'s nanoseconds past the second.** Neo4j's `Invalid nanosecond: N` outside 0 to 999,999,999
    (measured), raised for a written argument.
- **What the bridge still carries** (§12.85's `legacy`): lists, a map's Variant, `collect`, `min` and `max`, a
  returned grouping key, `IN`, a CASE or coalesce choosing among zoned values (the choice keeps no zone): leg 4;
  stored values (TIMESTAMP_NS columns): leg 5. The written year rides through a zone's shift, so a zoned value in a
  list beyond the old carriage's years is refused by name as a local one is (and one of the range's edge years, its
  instant a day from it at most).
- **Pinned.** `lite/nanozones.yaml`'s 14 cases, each judged by Neo4j; CypherToPureTest's
  `aZonedNanosecondValueIsItsInstantsParts`.

#### 12.87.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 5009 | 3918 (from 3,906) | 0 | 0 | 0 | 266 | 769 (from 767) | 56 |
| Postgres | 5009 | 3868 (from 3,602) | 0 | 0 | 0 | 316 (from 570) | 769 (from 767) | 56 |

TCK agreement: DuckDB 3571 (unchanged: the TCK's zoned nanosecond rows passed there through the old carriage),
Postgres 3296 → 3540, the 244 rows of zoned and remaining local nanosecond values: Temporal9's 192 truncations
(zoned, and local ones to a zoned result), Temporal3's 21 selections, Temporal1's 18 constructions, Temporal8's 6
zoned durations, Temporal6's 5 texts (local ones parsed back among them), Temporal7's 2 comparisons. Nothing moved
back on either lane. The 14 cases of `lite/nanozones.yaml`: 12 PASS and 2 BOTH_ERROR on each lane. Postgres's
nanosecond refusals left: the lists (WithOrderBy1's [13] to [16], [19], [20], [45] and the `temporallists` cases:
leg 4), the stored values (24 at seeding: leg 5), the 6 by year on both lanes (lists: leg 4).

### 12.88 Nanosecond values as their parts, leg 4: lists, maps and aggregates (W13, 2026-10-18)

- **A value of nanosecond precision in a list or a map is its text.** A list or map the database builds as a Variant
  holds each temporal as the oracle's spelling (§12.76: `Time('...')`, `DateTime('...')`, a zoned value's instant's),
  which the database now writes of the parts (`splitSpelled`); a spelled list the database computes as a collection
  (`collection`) holds the same texts, a microsecond value among them read into the parts first. Read back where it
  is read, the text is parsed into the parts (`splitOfIso`: the date's ten characters, the clock's fields, the
  fraction's digits padded to nine; the database's parse of each): an element by its index, `head`, `last`, a
  comprehension's or a quantifier's variable, a map's entry, an UNWIND's rows (each text unwound, then its parts the
  column and the hidden date column beside, as a WITH carries them, §12.85). A zoned value's zone is not carried
  through a list, as before (S65).
- **`IN`.** A split value against a spelled list is unrolled into equalities (Pure's `in` over the members' values
  would compare the nanoseconds alone); against a collection the database computes, a member equal to it, each
  read as its parts.
- **`collect`.** Each value's text, as a list holds it.
- **`min` and `max`.** The least or greatest of the values' sortable texts (`splitSortable`: the ISO text with
  every digit of the second, of fixed width over the years 0 to 9999, so its order is the value's; a zoned value's
  instant's, a zoned time's instant on the epoch day), the aggregate's column marked `SortableText`: read back as the
  parts by the clauses after, returned as the value its text spells (the egress, §12.85).
- **A returned grouping key** of nanosecond precision groups by its egress text, which the result reads as the
  value.
- **CASE and coalesce among zoned values**, part by part, the instants' (a CASE keeps no zone, as a microsecond
  one's does not).
- **A nanosecond component the row computes** (`localtime({..., nanosecond: i})`) is the parts' too: the refusal
  by name it had since §12.12 goes (`lite/temporal/nanos_component_of_the_row`, renamed). The epoch fields of a
  zoned value whose zone the island does not carry (an aggregate's) read the instant alone, which they need.
- **The bridge is gone.** §12.85's `legacy` (a split value in §12.12's epoch-nanosecond carriage) has no caller
  left and is removed, with the refusal by name of a year outside 1678 to 2261: a local or zoned value of nanosecond
  precision is its parts wherever the island carries it, on every lane. What still takes the `TIMESTAMP_NS` carriage
  is a stored value (a `TIMESTAMP_NS` column, §12.64): leg 5.
- **Found by the leg's cases: Postgres's hundred arguments.** A spelled list of six zoned times' texts is one
  `concat` of more than 100 arguments, which Postgres refuses ("cannot pass more than 100 arguments to a function",
  its `FUNC_MAX_ARGS`). The dialect writes a longer concatenation as a `concat` of the `concat`s of runs of at most
  100 (`maxFunctionArguments`, the base's unbounded; `concat` never yields NULL, so the nesting keeps its value).
- **Pinned.** `lite/nanolists.yaml`'s 11 cases, each judged by Neo4j.

#### 12.88.1 The ledger

| lane | cases | PASS | FAIL | BUG | ERROR | REFUSED | BOTH_ERROR | SKIP |
|---|---|---|---|---|---|---|---|---|
| DuckDB | 5020 | 3936 (from 3,918) | 0 | 0 | 0 | 259 (from 266) | 769 | 56 |
| Postgres | 5020 | 3896 (from 3,868) | 0 | 0 | 0 | 299 (from 316) | 769 | 56 |

TCK agreement: DuckDB 3571 → 3577 (WithOrderBy1's six rows of the years 1 and 9999 in a list: [17], [18], [45]
#9 and #10, refused by name since §12.74), Postgres 3540 → 3552 (WithOrderBy1's twelve list rows: [13] to [20],
[45]'s nanosecond examples). Nothing moved back on either lane. The 11 cases of `lite/nanolists.yaml` PASS on each
lane, and `lite/temporallists`' four on Postgres. The TCK's nanosecond refusals left: none at render on either lane;
Postgres's 24 at seeding (a stored value's `TIMESTAMP_NS` column, leg 5) and DuckDB's 20 harness skips of the same
values beyond 1678 to 2261 (leg 5); the 2 signed years on both (beyond every database's DATE).

### 12.89 Nanosecond values as their parts, leg 5: stored values (W13, 2026-10-18)

- **A stored value of nanosecond precision is an embedded instance of a value class of its parts.** A model holds a
  node's local date-time of nanosecond precision as a class `LocalDateTime { date: StrictDate[1]; nanosOfDay:
  Integer[1]; }`, a time of day as `LocalTime { nanosOfDay: Integer[1]; }`, a zoned date-time as `ZonedDateTime
  { date; nanosOfDay; offsetSeconds; zone? }` (the instant's UTC date and nanoseconds since midnight, §12.87) and a
  zoned time as `ZonedTime { nanosOfDay; offsetSeconds; }` (its UTC nanoseconds from the epoch day's midnight),
  each slot a column of the node's table through the property mapping's embedded block, as §12.59's Duration and
  §12.65's zoned classes are. The island knows the classes by their simple names and slots (`partsKind`; a zoned
  class with an `instant` slot stays §12.65's), and reads the property as the split value its slots spell
  (`storedParts`: the date slot the parts' date, the nanoseconds the clock, the offset and region slots the zone,
  as §12.75 reads a stored zone; null where an OPTIONAL MATCH's binding found none). The database's `DATE` and
  `BIGINT` hold every year 1 to 9999 on every lane, so the `TIMESTAMP_NS` column (§12.64) and its 1678 to 2261 are
  no longer a node property's.
- **Carried with its node.** A node carried through WITH writes a split property as its parts' columns (the
  nanoseconds under the property's column with the parts' mark, a date-time's date under `DATE_PART` beside, the
  zone's columns as §12.75's), null in an arm whose class lacks it (a label-less node's arms, §12.13); read back as
  the parts wherever the node is read after (its property, its JSON value, a second WITH that carries it again, a
  variable-length pattern's chains, which carry the date column too); the node's value spells the property as the
  oracle's text of its parts (§12.88).
- **The harness's seeding.** `CypherGraph` declares the value classes and seeds a node's nanosecond value as its
  parts (the scenario's text read by the harness: its date as written, its clock's nanoseconds; a zoned value's
  instant's), so a node of any year is seeded on both lanes: Postgres's 24 seeding refusals (no `TIMESTAMP_NS`) and
  DuckDB's 20 harness skips (years outside 1678 to 2261) pass. A relationship's property (no embedded value on an
  edge class) keeps §12.64's `TIMESTAMP_NS` column: leg 6. Found by the seeding: `java.sql.Date` binds a date by
  the Julian calendar before 1582, so the year 1's dates would move; a `DATE` is bound as the JDK's `LocalDate`,
  the proleptic Gregorian calendar the databases and Neo4j count.
- **Found by the leg's cases: a date-time text without its time.** Neo4j reads `localdatetime('1984-10-12')` and
  `datetime('1984-10-12')` as midnight of the date (its pattern makes the time optional and puts a zone only after
  it; measured, the compact, week and ordinal forms too); the island refused the text. Now read at midnight; a zone
  without a time stays the parse error Neo4j raises.
- **Pinned.** `lite/nanostored.yaml`'s 12 cases, each judged by Neo4j.

### 12.90 The remainder of floats, exact on every lane (2026-10-18)

- **What Neo4j answers.** Cypher's `%` of floats is Java's `%`: the remainder with the dividend's sign, exact (C's
  `fmod`: `0.3 % 0.1` is `0.09999999999999998`, `-4.0 % 2.0` is `-0.0`, `1.7976931348623157e308 %
  2.2250738585072014e-308` is `0.0`, measured). By zero it is Java's too: NaN for a Float divisor (`2.5 % 0.0`, `WITH
  0.0 AS z RETURN 5 % z`) and for a spelled zero (`2.5 % 0`, which Neo4j folds as a double); an Integer zero of the row
  is its error (`WITH 0 AS i RETURN 2.5 % i` is `/ by zero`).
- **Postgres.** It has no remainder of doubles (`mod` takes integers and numerics), and its cast of a double to
  NUMERIC is the shortest decimal that reads back as the double, not the double's value (`0.1::float8::numeric(70,60)`
  is `0.1000…0`, measured); `x - trunc(x / y) * y` is not exact and overflows ("value out of range: underflow",
  measured over extreme values). §12.73 refused it by name, the lanes' divergence. The dialect now spells it exactly:
  each operand an integer times a power of two (the power 60 less the floor of its logarithm, so an estimate off by
  one still leaves an integer; the scaling by powers of two and the cast to BIGINT are exact), both scaled to the one
  power, the NUMERIC remainder of the two integers (exact), scaled back to a double (the quotient to a thousand
  places, far below the least double's unit; a larger power a product, exact). The special values as Java's: NaN for
  a NaN, an infinite dividend or a zero divisor, the dividend for an infinite divisor, a zero dividend or one smaller
  than the divisor; the sign the dividend's, a zero's too. Each operand is written once, in a scalar subquery (a
  nested remainder does not double its text); an operand holding a window function is refused by name, since the
  subquery would compute the window over its own one row (measured). Measured against `fmod` over 4,000 pairs
  (random bit patterns, the extremes, subnormals, signed zeros, infinities, NaN): every result the same double.
  The floored `mod` of floats (no Cypher spelling) stays refused by name on Postgres.
- **The island, by zero.** The float `%` was Pure's `rem`, whose lowering raises "Cannot divide x by zero" (Pure's
  rule); it is now NaN for a Float divisor equal to zero (`if`, the database's parse of `'NaN'`, as §12.79 reads an
  infinity) and for a spelled zero, Pure's `rem` and its error for an Integer zero of the row.
- **Pinned.** `lite/exprtail.yaml`'s `float_remainders_exact` (eight pairs of the extremes, a remainder of a
  remainder), `float_remainders_by_zero` and `a_float_remainder_by_an_integer_zero_of_the_row`, each judged by Neo4j;
  Precedence2/[2] #3 passes on Postgres.

### 12.91 Nanosecond values as their parts, leg 6: relationships, and the epoch carriage retired (W13, 2026-10-18)

- **A relationship's temporal values are a node's.** An edge class's property holding a duration, a zoned value or a
  value of nanosecond precision is an embedded instance of its value class (§12.59, §12.65, §12.89), as a node's is:
  the harness declares the embedded block on the edge class (one shared reading of an embedded value's columns, its
  mapping and its cells for node and edge tables), and finds a zoned value's precision and a named zone over the
  relationships too, so one class per kind serves the graph. Its refusal of "a duration or a zoned value on a
  relationship" goes, with the `TIMESTAMP_NS` column it gave a relationship's nanosecond value and that column's 1678
  to 2261.
- **Carried as a node's.** A relationship carried through WITH writes a nanosecond property as its parts' columns and
  a zoned one's zone beside it (`carryRel`, as `carry`); carried again it keeps the parts' mark and its date and zone
  columns; read back, a carried property (of a node or a relationship, alone or spelled in the value) is its parts or
  its zone's columns (`carriedProp`, now the one reading).
- **A model's marked column read into the parts.** A `TIMESTAMP_NS` column under the profile's marks (§12.64, lite's
  grammar extension, S64) is read at the property into the parts: the database's date of it and its nanoseconds since
  midnight (`dateDiff` in nanoseconds from the date, DuckDB's), a zoned value class's marked instant its UTC parts;
  a class lacking it the parts' nulls. So nothing past the read holds §12.12's epoch carriage (a temporal value marked
  of nanosecond precision without its parts): every place that minted it is now a check that fails loudly, and the
  chain ran with them green on both lanes. A text printed from a nanosecond value and a duration of nanosecond
  precision keep their marks (neither is a temporal held whole). The unit tests over a marked model
  (CypherToPureTest) pin the parts' spelling.
- **The carriage deleted.** With nothing minting it, the operations §12.12 spelled over the nanosecond epoch (the
  sub-microsecond digits put back after a calendar step or a zone conversion, a time of day as the date-time at it
  on the epoch day, the epoch's nanoseconds) are gone from comparison, `IN`, the zone conversions, duration
  arithmetic, `between`, construction, fields, `toString` and truncation: each reads a value held whole at the
  microsecond, a value of nanosecond precision being its parts, handled before. A text a nanosecond value was
  printed as is read back as the parts it spells. No row moved.
- **Pinned.** `lite/edgevalues.yaml`'s 6 cases, each judged by Neo4j: a relationship's nanosecond local values over
  the years 1 and 2015, zoned values of an offset and a region, a duration, through two WITHs, an OPTIONAL MATCH and a
  variable-length pattern.

### 12.92 The three setups "Neo4j refused": the oracle's version, not Neo4j's (2026-10-18)

- **What it was.** Three TCK scenarios were SKIP as "Neo4j refused the setup": WithSkipLimit1/[1], WithSkipLimit2/[2]
  and With2/[1], whose setups read a node created earlier in the same `CREATE` in a later node's property map
  (`CREATE (a:End {num: 42, id: 0}), (:End {num: 3}), (:Begin {num: a.id})`). The TCK's setups are openCypher 9's
  text, and the oracle ran every statement under lite's default Cypher 25 (C1); Cypher 25 narrows `CREATE` there
  ("Creating an entity (a) and referencing that entity in a property definition in the same CREATE is not allowed.
  Only reference variables created in earlier clauses."), Cypher 5 reads it as written (measured). So Neo4j passes
  the three scenarios: under `CYPHER 5` its answers are the TCK's expected rows, `({name: 'A', num: 0, id: 0})` and
  `(:End {num: 42, id: 0})`. The refusal was the oracle's choice of version for a statement it does not judge.
- **The oracle.** A setup statement runs under `CYPHER 5` (`SETUP_CYPHER` in `tools/neo4j-oracle/snap.py`) unless it
  names its own version; the query keeps the version the case names, or lite's 25. A setup only builds the graph the
  case is judged over, and its dump is what the lanes seed from. Measured: the drift check over all 5041 cases moved
  these three answers and no other (every other setup builds the same graph under either version).
- **The lanes.** All three pass on both lanes as the island stands (a WITH's projected value joined to a fresh
  `MATCH` by its `WHERE`, §12.44). No setup of the TCK is refused now; outside the write clauses
  (and the control queries of their scenarios) the TCK holds, on either lane, only the 2 signed years past every
  database's dates.

### 12.93 The independent review of the branch, before its PR (2026-10-18)

- **What it was.** Before the branch's PR, two reviewers read it apart from the lanes: one the platform outside the
  island, one W13's nanosecond parts and the Postgres remainder. Each finding was measured (a probe, Neo4j's answer, a
  unit test) before it was fixed; the user: "Fix them before PR". The TCK did not move (it held none of them); the
  island's findings are `lite/review.yaml`'s 21 cases, each a reviewer's query judged by Neo4j.
- **The island (W13).** A split value (a nanosecond date-time, time or zoned value as its parts, §12.85) left the
  parts' carriage in seven places where a whole value was expected: an aggregate inside an expression
  (`max(t) + duration('PT1S')`, returned or through a WITH), `count(DISTINCT t)` (counted by its parts together), an
  ORDER BY after a grouping or a DISTINCT that reads a field of the value, a list comprehension mapping to one, an
  UNWIND of a list holding a null, and a UNION whose arms carry it as parts beside a whole microsecond value. Each now
  carries the parts through (the returned text built at the final projection, `egressed`). A year past 9999 is
  spelled `+10000` in a text, as Neo4j and Java spell it; a year before 1 raises by name, the databases counting those
  years apart (S88, decided ERROR on both lanes). Truncation to the millisecond or microsecond with written fields
  keeps Neo4j's rule (a written sub-second field replaces the truncated digits below the unit, the others kept).
  `duration.between` follows Java's `until`: the months by the packed date, a day stepped back where the clocks
  cross; zoned values by their wall times in the first value's zone, the remainder by instants across a named zone's
  change of offset. A clock's field the row computes, and an epoch nanosecond count, are checked against their range
  (Neo4j's error where out of it). A CASE choosing a split value by a `rand()` test is refused by name: each part
  is a CASE of its own, so the test would draw once per part and the parts could come from different branches (the
  database shares no expression); `a_case_choosing_by_a_random_test` pins the refusal.
- **The platform.** Each a finding measured, then fixed and pinned:
  - **The inliner (G½).** A bound `random()` or `generateGuid()` read twice, or read once per row under a lambda,
    is refused by name (`UserCallInliner.readOnce`): the substitution writes the bound term at each read and the
    database draws at each place, so `let r = random(); $r == $r` answered false. A user call whose parameter binds one
    does not reduce; it stands, as a recursive call does, and SQL walls naming the function. `UserCallInlinerTest`.
  - **Postgres's exact float remainder (§12.90)** is written inline (no scalar subquery, which Postgres refuses over a
    grouped computed key: "subquery uses ungrouped column"), with its NULL arm; 4,000 pairs of doubles again matched
    `fmod`, and the SQL of the largest case fell from 2.1 MB to 190 KB.
  - **H2's `TIME`** keeps no fraction of a second (its default precision is 0, measured 2.1.214): casts and DDL write
    `TIME(9)`.
  - **`random()` in `[0, 1)`.** SQLite's formula (its signed 64-bit `random()` shifted by 2^63 as a REAL and scaled)
    answered exactly 1.0 for a draw within 1024 of the top (measured): the low 53 bits over 2^53. Snowflake's
    `UNIFORM` is inclusive: an integer of 53 bits over 2^53.
  - **The streamed JSON rows** (`Render.jsonWireRows`) read a sort key among the outputs it shows, and refused the
    hidden one an ORDER BY of an unreturned expression adds (§12.53); they read the inner relation's outputs, as the
    snapshot's hoist does. `CypherIslandIntegrationTest`.
  - **A partial date in date arithmetic** (`%5->dateDiff(...)`) was written `DATE '5-01-01'`, which Postgres reads as
    2001-05-01 (measured): four digits, as `MatchFold` writes a date literal's. `PostgresDialectTest`.
  - **`elementId` over several instances** (§12.46) typed its lambda by the identity's type, not by its function
    type `{C[1]->String[1]}`.
  - **Small ones.** A procedure name with several overloads is refused by name, not answered by the first; a
    procedure's parameter of a non-primitive type is named by its type's name for the refusal, not by the record's
    text; `RelationalDataType.fromName` reads `TIMESTAMP_NS` as it reads `TIME`.
- **Kept, with the reason written.** The graph profile's check stays exact (`GraphEdges.checkProfile`): the path
  names the graph project's one profile, so fewer stereotypes is an older declaration, and one beyond them would be read
  as nothing. The engine text's `TIMESTAMP_NS` (`DdlSpelling.engineText`) stays the metamodel's type text, as `OTHER`
  is: H2 refuses the column at execution, loudly.
- **The lanes.** Both lanes in update mode, FAIL/BUG/ERROR at zero, the same census on both (PASS 3997 / REFUSED 260 /
  BOTH_ERROR 772 / SKIP 33, of 5062 cases: the 21 new cases are 18 PASS, one of them decided by S88, 2 BOTH_ERROR, the
  clock field and the epoch count out of range, and 1 REFUSED, the CASE over a random test); no case moved against the
  branch's head, and the TCK holds 3600 of 3880 on both lanes; the oracle check 0 problems; the guards green (four
  tests' pins of the translator's text moved with the year-range texts and the delayed egress, `CypherToPureTest`; `projectArm`
  back under the 250-line ceiling by its ungrouped projection, `ungroupedProjection`; the native claims regenerated,
  `UserCallInliner` reading `random` and `generateGuid`).
