# The Cypher TCK to zero — a holistic plan, 2026-10-17

**Status: a proposal for the user's decisions (§3), written at the forty-third landing of the `#Cypher{...}#`
line (`docs/GATES.md`; the ledger: `docs/CYPHER_SHOWCASE.md` §20).** The user's ask: "can we come up with a holistic
plan to pass *all* TCK tests?", after three corrections to the line's standing claims (§0). Nothing here is decided
until the user says so; the register rows it would overturn are named where they fall.

## 0. Three corrections, and what "zero" means

The openCypher TCK is a conformance kit: every scenario is written to be passable by a conforming implementation,
with a deterministic outcome. Three claims this line made about "unwinnable" or "decided" scenarios do not survive
the user's questions of 2026-10-17:

1. **`rand()` and the clock functions are passable.** The 31 `rand()` scenarios shuffle a list with `rand() > 0.5`
   and assert an invariant that holds whatever the draw (`single(x IN list WHERE p) = (size([...]) = 1)` is
   `true`); the 19 clock scenarios assert `duration.inSeconds(datetime(), datetime())` is `PT0S` or `f(null)` is
   null. The island refused them "as a value of the call, not of the data"; the database's own `random()` and
   clock answer them as Neo4j does. No oracle problem: the judge compares the deterministic final rows.
2. **A property no class declares can read as null.** The island made it a compile error (C4, register S58: "the
   graph is the model"). Neo4j's graph is schema-optional: a missing property is null, a label no node carries
   reads no rows, a relationship type nobody has reads no rows. The island can spell all three statically (the
   model tells it the property is declared by no candidate class), so the choice is a rule, not a limit.
3. **Identity is available.** C5 was read as "no physical key may leak into the model"; the user's rule is that
   keys can be hidden where needed, not that they must be absent. The parity harness already gives every node
   table an `id BIGINT PRIMARY KEY` and every relationship table `src_id`/`dst_id`; the platform resolves a
   class's `<<equality.Key>>` properties at compile time (`compiler/element/EqualityKeys`). Node identity is a
   hidden key property; nothing is approximated by comparing user properties.

**"Zero" means:** every one of the 3880 TCK scenarios is `PASS` (rows match Neo4j's cell for cell) or
`BOTH_ERROR` (Neo4j errors and lite errors too), on both parity lanes; `SKIP` is 0 (the harness models every
scenario); the island refuses nothing the TCK asks. Refusals by name may remain for what the TCK never asks.

## 1. The inventory: the 849 non-passing cases, by what they need

The census of `cases-rows44b` (the forty-third landing): PASS 2361, BOTH_ERROR 670, REFUSED 668, SKIP 181. The 849
non-passing cases fall into twelve workstreams. "Where" says who changes: the island (`protocol/cypher`), the
harness (`testcases/CypherGraph`, `CypherTck`, `CypherValues`), or the platform (lowering, resolver, dialects,
store grammar, executor).

| # | workstream | cases | today's reason | where | rule to change |
|---|---|---|---|---|---|
| W1 | Schema-optional reads | 56 | a label naming no class (42), a property no class declares (10+2), a type naming no association (2): C4 | island | S58 |
| W2 | Identity | ~100 | OPTIONAL MATCH after rows (36), both ends bound (11), undirected self-association (15), a node among grouping keys (8), nodes compared (7), a MATCH binding a projected variable again (8), untyped relationship in OPTIONAL MATCH (7), RETURN DISTINCT over a node (3), two relationships of one type (3), the rest ones and twos; plus S75's uniqueness and S74's parallel links | island, harness | C5 as read, S74, S75 |
| W3 | Non-deterministic functions | 50 | `rand()` (31), `datetime()`/`date()`/`time()`/`localtime()`/`localdatetime()` and their `.transaction`/`.statement`/`.realtime` forms (19) | island | the "value of the call" rule |
| W4 | Writes | 274 | CREATE 69, SET 53, MERGE 49, REMOVE 31, DELETE 30, DETACH DELETE 9; 33 write scenarios the harness skips for their control query | platform (F1), island, harness | — |
| W5 | Stored temporal values | 72 | properties holding a time of day (10 scenarios), a duration (8), a zoned date-time (5): 61 skipped; a date-time of sub-microsecond precision: 11 skipped | harness, store grammar, island | S64 |
| W6 | Nodes with several labels | 18 | the harness skips `(:A:B)` | harness, island | — |
| W7 | A property typed two ways | 5 | `var: 1` on one node, `var: 'a'` on another of the same label (4), two types differing only in case (1) | harness, island | — |
| W8 | Procedures | 50 | `CALL test.my.proc(...)` declared by the scenario with its signature and result rows | harness, island | — |
| W9 | Lowering and resolver limits | 49 | a fan-out to a class without a property (21: `project` of a navigation mapped to a constant stays one row per root), a hop past an edge class (15: the resolver's nested navigation), `%` inside a database lambda on Postgres (9), ORDER BY on a non-returned expression without SKIP or LIMIT (4) | platform | — |
| W10 | Variable-length without a bound or past two hops | 25 | the unroll stops at a bound of 4 and two hops in all; no bound needs a recursion | platform (a recursion native), island | — |
| W11 | Variants of no known shape | 53 | a list comprehension over a Variant (26, all also `rand()`), ORDER BY a Variant (18), two Variants or two computed lists compared (9) | island, judge | — |
| W12 | The tail | ~45 | pattern comprehensions over rows (11) and over two hops (3), `count(DISTINCT)` (6), `toString` of lists and nodes (5), min/max over mixed values (6), untyped relationships beside other patterns (4), SKIP/LIMIT expressions (2), text predicates on non-strings (4), temporal texts of the row (4), and singletons | island | — |
| — | Setup oddities | 3 | Neo4j refused the setup — RESOLVED 2026-10-18 (the study's §12.92): the oracle ran the TCK's openCypher 9 setups under Cypher 25, which narrows `CREATE`; under `CYPHER 5` Neo4j answers the TCK's rows and both lanes pass | harness (the oracle) | — |

The sum exceeds 849 because W11's comprehension cases are also W3's, and W2 unlocks cases counted under W12.

## 2. The workstreams

### W1. Schema-optional reads (S58 overturned) — LANDED as open graphs, 2026-10-17 (the study's §12.45: TCK agreement 3031 → 3093, 62 scenarios; the declaration site reopened by the user, §3.2 Q1 addendum)

A property declared by no candidate class of a pattern variable reads the known null (the island already reads a
property an arm's class lacks as the known null, §12.40.2: the same mechanism, one step wider). `MATCH (n:Nope)`
over a model with no such class reads no rows (the first-clause form already exists for an OPTIONAL MATCH over no
class, §12.20); a relationship type naming no association end or edge class reads no rows (the pattern's "no way"
form, §12.36). `n.missing` inside a comparison folds as the known null does today. Neo4j's static errors stay
static errors (a property on a non-map). One leg; the compile-error form kept only behind a query option if the
user wants the typo guard back (not proposed). **Reframed 2026-10-17 (§3.2 Q1): the open reading is the OPEN
graph's, GQL's own distinction; the closed graph keeps C4, so the typo guard stays the default of a user's model
and no query option is needed.**

### W2. Identity (C5 as the user meant it) — the design settled 2026-10-17 (§3.2 Q2 and its three addenda)

**Two questions, two rules, as Legend already keeps them.** Value equality (`==`, `equal`, `in`) is Pure's rule,
unchanged: by the `<<equality.Key>>` properties when the class declares them, else by identity. Identity (`eq`: "the
same instance") is what Cypher means by `a = b` on nodes and relationships, by DISTINCT, grouping, `IN` over nodes
and relationship uniqueness; two nodes named Ann are two nodes. `<<equality.Key>>` is never in the identity chain:
it states meaning ("same name, same person for comparison"), identity states the store ("rows are distinguishable
by this key"); using the first for the second would merge distinct nodes exactly on the models that declare keys.

**The identity of an instance, in order, with no fallback past the last step (the engine's own precedence, to be
verified against its compiler source in the leg, per the verification tenet):**

1. a constructed instance (`^Person(...)`, the PCT lane): the `__id` the lowering mints per construction site
   (exists);
2. a mapped instance: the class mapping's `~primaryKey(...)` when declared;
3. else the main table's `PRIMARY KEY` columns from the store's DDL (a relational class mapping has exactly one
   main table and its instances ARE that table's rows: joins inside property mappings fetch columns for the same
   instance or reach other classes' instances, and a projection's exploded rows still carry the main row's key,
   which is how the engine's graph fetch rebuilds objects; a relation-function mapping's key is the engine's
   inference over its expression, `PkInference`, already lite's);
4. else a compile error naming the class ("no identity"): never all mapped columns, never `<<equality.Key>>`.

**Who does what.** The PRODUCT: one native, `meta::legend::lite::elementId(x: Any[1]): String[1]` (Cypher's own
`elementId(n)` and `id(n)`), lowered to the instance's identity as a value (the key columns, a composite spelled as
text), the only expression Pure lacks for "the identity of this mapped instance"; the same `__id` carrier on the
value lane later unifies `eq` for mapped and constructed instances. The HARNESS: nothing (its tables declare
`id BIGINT PRIMARY KEY`; step 3). A USER: nothing beyond a `~primaryKey` or a table key. The hidden-key property
and the `graph.hidden` marker of the first draft are withdrawn: there is no key property to hide.

**The island.** Node comparisons are `elementId($a) == elementId($b)`; a carried node keeps its identity as a
column; DISTINCT, grouping and `IN` over nodes use it; a MATCH or OPTIONAL MATCH after rows from a carried node
re-anchors by a JOIN on identity (`Class.all()` joined on `elementId`), which also retires the resolver's depth
limit from the translator's side (chains past two steps, variable-length unrolls past two hops, hops from a node
reached through an edge class: every hop beyond what the resolver navigates becomes a join on the id over
materialized rows); both ends bound is the far identity compared with the bound node's; an undirected hop over a
self-association dedupes by identity; relationship uniqueness along a chain is the relationships' identities
pairwise distinct (an edge class's own; a plain association's the ordered pair of node identities, S74; S75
overturned); `collect(DISTINCT n)` and `count(DISTINCT n)` count identities. A node value carries `"@id"` in its
JSON, which the judge ignores (Neo4j's ids are its own).

Legs: the native and its lowering (platform, under its guards); node identity in the island (comparisons,
DISTINCT, grouping, IN, values); re-anchoring joins (continuations, cycles, deep chains); uniqueness and the
undirected self-association.

**Progress.** Leg 1 LANDED 2026-10-17 (the study's §12.46, GATES' forty-fifth landing; TCK agreement 3093 →
3098, DuckDB PASS 2945 → 2965): the native
(`meta::legend::lite::elementId`, resolved by `StoreResolver` through the class source's key pseudo-bindings as
`'<class fqn>:<key>'`, null for an absent instance, the keyless class a compile error), node comparisons in the
island (`a = b`, `a <> b`, after WITH, in products, over arms, over an edge class's relationships), the identity
carried as a column of every node, and of every relationship over an edge class (compared after a WITH, and
distinct across the patterns of one MATCH: relationship uniqueness, spelled over the product for relationships
carrying an identity). The text is class-qualified so nodes of different classes never compare equal, as Neo4j's
`elementId` carries its type and database. Still refused by name: a relationship over an association compared
(next: S74's ordered pair of the ends' identities, which also spells uniqueness over associations), paths
compared, a composite key. Leg 2 LANDED 2026-10-17 (the study's §12.47, GATES' forty-sixth landing; TCK agreement 3098 → 3112, DuckDB
PASS 2965 → 2997): DISTINCT
over and grouping by nodes (a returned node carried as its columns, the identity among them, its JSON rebuilt
last), `count(DISTINCT x)` and `count(DISTINCT n)`, the same over edge-class relationships; a relationship
without identity stays refused under both until leg 3; ORDER BY a node stays refused (Neo4j's own ids). Leg 3
LANDED 2026-10-17 (the study's §12.48, GATES' forty-seventh landing; TCK agreement 3112 → 3115, DuckDB PASS
2997 → 3020): an association relationship's identity as
the ordered pair of its ends' (S74), comparisons, DISTINCT, grouping and `count(DISTINCT r)` over it, and
relationship uniqueness within one MATCH (S75 overturned there: the relationships of a repeated type pairwise
distinct, an unnamed one named for it); still unspelled along a variable-length chain and within an OPTIONAL
MATCH's chain. Leg 4 LANDED 2026-10-17 (the study's §12.49, GATES' forty-eighth landing; TCK agreement 3115 →
3149, DuckDB PASS 3020 → 3068): re-anchoring, a MATCH
or OPTIONAL MATCH after rows joined back to the rows on the identity of every shared node or relationship (the
pattern read from the carried node's class), both ends bound, a bound relationship matched again, the fan-out
to a property-less class lifted. Still refused: re-anchoring from a node of several classes (the arms would
each need the join), a named path or a variable-length relationship after rows, an OPTIONAL MATCH's WHERE over
the rows' values. Leg 5 LANDED 2026-10-17 (the study's §12.50, GATES' forty-ninth landing; TCK agreement
3149 → 3181, DuckDB PASS 3068 → 3110): cycles within one pattern by identity, the OPTIONAL MATCH fallbacks through
re-anchoring, the undirected self-association deduped by identity; with them a second-step hop's presence said
over the rows (the resolver's bridge-table limit, §5) and `head`/`last` over a Variant list. W2 is closed but for
uniqueness along variable-length chains (the unroll's hidden relationships) and hops from an edge-reached node
(W9's resolver depth).

### W3. Non-deterministic functions

`rand()` is the database's random float in `[0, 1)` (every dialect has one); `datetime()`, `date()`, `time()`,
`localtime()`, `localdatetime()` with no argument are the database's clock at the kind the island already carries,
their `.transaction`/`.statement`/`.realtime` forms the same (one clock per statement is what the TCK asserts:
`duration.inSeconds(datetime(), datetime())` is `PT0S`; the island reads the clock once per statement and reuses
it, as Neo4j's `statement` clock does). The lite cases pin only predicates (`rand() >= 0`, `date() = date()`).
One leg. LANDED 2026-10-17 (the study's §12.51, GATES' fiftieth landing; TCK agreement 3181 → 3201, DuckDB PASS
3110 → 3141): the lite native `random()`, `SqlFn.RANDOM` in every dialect, the lowering's once-per-row rule for a
non-deterministic projection (`Fold.nondeterministic`), the clocks as `today()`/`now()` (S77), `randomUUID()`;
the Quantifier9-12 cases now stop at the Variant list comprehension (W12).

### W4. Writes (the largest)

- **The platform (F1 in `docs/BI_AND_ETL_PLAN_2026_09_29.md`).** `write` lowers to DML; the MIR's `SqlDml` grows
  `Update(table, set, where)`, `DeleteWhere(table, where)` and `InsertSelect`; each dialect renders them; the
  executor runs a statement's DML in one transaction and returns the counts. The `write` native itself is the
  relation form (`rel->write(#>{db.t}#)`); the island needs the row forms beside it.
- **The island.** `CREATE (n:A {props})` inserts a row with a minted key (W2's hidden id: `max(id)+1` in the
  statement, or the database's sequence); `CREATE (a)-[:T]->(b)` inserts the junction or edge row from the bound
  nodes' keys; `SET n.p = v`, `SET n += {...}`, `SET n:Label` update the row (a label added is a row in the
  label's table sharing the id — W6's representation); `REMOVE n.p` sets null, `REMOVE n:Label` deletes that
  table's row; `DELETE n` deletes the row (an error while relationships remain, as Neo4j's); `DETACH DELETE`
  deletes the relationship rows first; `MERGE` is a read, then a `CREATE` of what the read lacks, in one
  statement (`ON CREATE SET`, `ON MATCH SET`). A write clause after reads runs per row of the rows in hand; a
  query mixing writes and a RETURN returns the rows after the writes.
- **The harness.** The TCK's write scenarios assert side effects (`+nodes 1, +relationships 2, +properties 3,
  +labels 1`, and the minuses) and sometimes a control query's rows. The harness counts nodes, relationships,
  labels and property values before and after (a query per table), and runs the control query through the
  island. Neo4j's side effects are snapped as today's answers are.
- Six to ten legs after F1, in their own PR.

### W5. Stored temporal values (S64 revisited)

- A time of day in the store: lite's relational grammar adds a `TIME` column type (a lite extension: legend-
  engine's grammar has none, which S64 records), rendered by every dialect; the harness declares one for a
  `localtime`/`time` property, the island reads it as the `StrictTime` it already carries (a zoned time keeps its
  offset in a second column, as a zoned date-time keeps its instant and offset). Or, without touching the grammar,
  a text column read as a time by the property's declared type: the plan prefers the column type, the honest
  representation.
- A duration property: the `g::Duration` class the harness already declares for computed durations, as a Variant
  column holding the four slots.
- A zoned date-time property: the instant in a `TIMESTAMP` column and the offset beside it, as the island carries
  zoned values.
- A date-time of sub-microsecond precision: a `TIMESTAMP_NS` column type where the database has it (DuckDB), the
  Postgres lane refusing by name as the nanosecond leg does.
- Two to three legs.
- Leg 1 LANDED 2026-10-17 (the study's §12.59, GATES' fifty-seventh landing; TCK agreement DuckDB 3354 → 3379, Postgres 3004 → 3017; DuckDB PASS 3,386 → 3425, Postgres 3,025 → 3052): the TIME column type
  (the relational grammar's extension, through the store compiler, the DDL of every dialect, the protocol, the
  metamodel's datatype class; S64 revised), a stored duration as an embedded instance over four columns read as the
  island's own instance, the whole days of a duration's seconds when it moves a date. Leg 2: a `TIMESTAMP_NS`
  column (DuckDB; the TCK's stored times and date-times are nanosecond ones: 22 + 11 cases) and the offset column
  beside a stored zoned value (the island's Zone carrying a column, not a literal).
- Leg 2a LANDED 2026-10-17 (the study's §12.64, GATES' sixty-second landing; TCK agreement DuckDB 3446 → 3458, Postgres 3084 → 3084; DuckDB PASS 3,535 → 3556, Postgres 3,162 → 3162): the `TIMESTAMP_NS`
  column type (DuckDB spells it; a microsecond engine refuses by name, the Postgres lane REFUSED), the graph
  profile's `timeOfDay` and `nanoseconds` marks on a DateTime property (the declaration decision 4 foresaw), the
  harness declaring a property name's finest precision under them and seeding the text through a cast, the island
  reading a marked property as its nanosecond time of day or date-time; the stored nanosecond local times and
  date-times of WithOrderBy1/[35], [36], WithOrderBy2/[13], [14], Temporal5/[3], [5]. Still open: leg 2b, the
  offset column beside a stored zoned value; the year-1 and year-9999 nanosecond date-times (WithOrderBy1/[39]-[42],
  WithOrderBy2/[17]-[20]) are outside the database's nanosecond range (the harness skips them by name).
- Leg 2b LANDED 2026-10-17 (the study's §12.65, GATES' sixty-third landing; TCK agreement DuckDB 3458 → 3463, Postgres 3084 → 3084; DuckDB PASS 3,556 → 3565, Postgres 3,162 → 3163): a stored zoned value as
  an embedded value class of the instant and its offset, the island reading the instant and carrying no zone
  (ordering, comparison, arithmetic and node values by the instant: S65, S66, S68), an operation reading the zone
  refused by name; WithOrderBy1/[37], [38], WithOrderBy2/[15], [16] on DuckDB. Leg 2c, open: the island's Zone
  carrying a column (the offset slot read: Temporal5/[4]'s fields and zone texts), which every projection must carry
  beside the value.
- Leg 2c LANDED 2026-10-17 (the study's §12.75, GATES' seventy-third landing; TCK agreement DuckDB 3542 → 3555,
  Postgres 3160 → 3165): a zone whose offset the database reads (`Zone.read`), a stored value's offset slot and, for
  a date-time of a named zone, a region slot the harness declares from the oracle's dump (the zone's name in
  brackets); carried through a WITH and with a carried node's properties as hidden columns; a value moved in a
  stored region taking the region's offset at the new instant; a resolver fix for an embedded value on an
  association's target. Temporal5/[4], [6], WithOrderBy2/[15], [16] (DuckDB).

### W6. Nodes with several labels

`(:A:B)` is an instance of a class `A_B` extending `A` and `B` (Pure's multiple inheritance), stored in its own
table with the shared id; `MATCH (n:A)` reads `A` and its subclasses (the island unions the subclass tables as it
unions arms today; whether the relational mapping reads subclasses through `A.all()` is to be verified first);
`labels(n)` is the class's label set; `n:B` is a subclass test. Two legs.
- LANDED 2026-10-17 in one leg (the study's §12.61, GATES' fifty-ninth landing; TCK agreement DuckDB 3428 → 3442, Postgres 3066 → 3080; DuckDB PASS 3,484 → 3513, Postgres 3,111 → 3140): verified first
  that `A.all()` reads A's own mapping alone (the island unions the subclasses itself); `<<graph.Anonymous>>`, the
  profile's sixth stereotype, marks a class whose name is no label; the harness declares an anonymous class per
  label set extending each label's class (a label carried among others a marker class with no node of its own, so
  every association is a leaf class's); a label reads its class and the subclasses as arms; `labels(n)` is the
  class's label set, in the model's order (S83); a backticked path reads its class alone (S84). Of the 18: 12 PASS,
  2 BOTH_ERROR (Graph3/[8], [9]: Neo4j's errors), 3 writes (W4), 1 unbounded variable-length pattern (W10).

### W7. A property typed two ways

A property whose values differ in kind across the nodes of one label is a Variant column (as a list property is,
§12.37), read by kind through W11's shapes; a comparison across kinds is Neo4j's (false or null). One leg, with W11.
- LANDED 2026-10-17 in one leg (the study's §12.62, GATES' sixtieth landing; TCK agreement DuckDB 3442 → 3446, Postgres 3080 → 3084; DuckDB PASS 3,513 → 3533, Postgres 3,140 → 3160): the harness's Variant
  column where one class's values differ in kind; the island's JSON union where candidate classes type a name two
  ways (the root's read as a Variant, a carried node's and a chain's columns converted); a Variant against a string,
  a boolean or a number ordered by kind, null across kinds (Cypher's; two Variants keep §12.58's keys, S81).

### W8. Procedures

The scenario declares each procedure's signature and its result rows (`there exists a procedure test.my.proc(in
:: INTEGER?) :: (out :: INTEGER?): | in | out | ...`). The harness models a procedure as a table of its rows (a
relation the model declares) and its signature; the island reads `CALL proc(args) YIELD cols` as the rows whose
inputs equal the arguments, joined to the rows in hand, and raises Neo4j's static errors for the wrong arity, a
missing parameter or a yield of nothing. Two legs.
- LANDED 2026-10-17 in one leg (the study's §12.60, GATES' fifty-eighth landing; TCK agreement DuckDB 3379 → 3428, Postgres 3017 → 3066; DuckDB PASS 3,386 → 3484, Postgres 3,025 → 3111): a procedure is the
  model's function at its dotted name's path, over a typed TDS literal of its rows; the CALL clause (standalone,
  or `lateral` over the rows in hand; YIELD with swap-safe renames; Neo4j's static errors); the harness declares
  the TCK's procedures and judges a procedure case by the TCK's own table, the snapper keeping Neo4j's graph alone.
  REFUSED by name: the implicit call whose parameter the scenario omits (MissingParameter: the island reads no
  parameter).

### W9. Lowering and resolver limits (outside the island)

- `project` of a to-many navigation mapped to a constant keeps one row per root: fix in the lowering so the join
  fans out whenever the navigation is to-many (21 cases).
- The resolver's nested navigation past a second association step (15 cases): extend the resolver's
  property-chain rewrite to any depth. **Measured 2026-10-17 (W12 leg 3):** a chain of three foreign-key
  association ends projects and filters on a leaf property; what fails is the EXISTENCE of the chain
  (`$a.T.U.V->isNotEmpty()`: "property 'V' of nested navigation 'T.U' is not mapped"), and past a bridge-table
  step at two steps already (`$u.KNOWS.KNOWS->isNotEmpty()`: `UnfoldableRef`; its identity read: "multi-hop
  navigation ... through an embedded/slot head is not supported yet"). The island cuts its patterns at two steps
  (the study's §12.54), so the resolver fix is no longer on the TCK's path for typed patterns; the probes are
  `$S/pprobe/abcd_fk.pure` and `knows.pure`.
- `rem` inside a list lambda on Postgres types its divide-by-zero guard as text (9): the dialect's arm.
- A sort key that is not an output column without a SKIP or LIMIT (4): the render's subquery form.
- Four legs, each in the platform's own guards, with the database owner.

### W10. Variable-length without a bound

A recursion native in the MIR (`WITH RECURSIVE` on every dialect that has it; a bound of the recursion's depth as
the dialect's guard), the island spelling an unbounded or deep pattern as the recursion over the association's
join, uniqueness by W2's keys. Two to three legs, after W2 and W9's resolver work.
- Leg 1 LANDED 2026-10-17 (the study's §12.63, GATES' sixty-first landing; TCK agreement DuckDB 3446 → 3446, Postgres 3084 → 3084; DuckDB PASS 3,533 → 3535, Postgres 3,160 → 3162): Neo4j's static variable
  errors (VariableAlreadyBound, VariableTypeConflict) raised over the whole statement before any clause is read,
  so the 250 error scenarios of Match1, Match2 and Match6 stay in agreement when leg 2 lifts the unbounded
  refusal; `lite/varkinds.yaml`. No TCK row moves (the error scenarios agreed by the refusal; now by the error).
- Leg 2a LANDED 2026-10-17 (the study's §12.67, GATES' sixty-fifth landing; TCK agreement DuckDB 3470 → 3470, Postgres 3091 → 3091; DuckDB PASS 3,573 → 3573, Postgres 3,171 → 3171): the recursion native
  (`meta::legend::lite::recurse`, typed generically, lowered to `WITH RECURSIVE` as a derived table, executed on
  DuckDB).
- Leg 2b LANDED 2026-10-17 (the study's §12.68, GATES' sixty-sixth landing; TCK agreement DuckDB 3470 → 3487, Postgres 3091 → 3108; DuckDB PASS 3,573 → 3610, Postgres 3,171 → 3208): the island spelling an
  unbounded pattern (or one bounded past the unroll's 4) as the recursion over the hop's rows (identities, JSON
  texts, stored directions), relationship uniqueness at every step (S75 revised), the far node joined by identity,
  the relationship's list and the named path (its length, nodes and relationships) from the walk's carried texts;
  after rows, in an OPTIONAL MATCH and as a pattern predicate through the re-anchoring; the path functions of a
  path carried as its value; the resolver resolving a recursion's step. Left (the §5 log): a variable-length
  relationship bound to a list of relationships (Match4/[8], Match9/[6], [7]); other relationships beside the walk
  in one pattern (no TCK row).
- The temporal tail, leg 1 LANDED 2026-10-17 (the study's §12.74, GATES' seventy-second landing; TCK agreement DuckDB 3537 → 3542, Postgres 3157 → 3160):
  a duration multiplied by a float or divided by a number as Neo4j's approximation (Temporal8/[7]); a zoned
  value's text parsed back in its scope as the value itself (Temporal6/[3], [5], DuckDB). Logged (§5): nanosecond
  values outside the databases' range, years beyond their dates. Next: a zone whose offset the database reads (a
  stored zone's, a named zone's at an instant).
- The temporal tail, leg 2 LANDED 2026-10-17 (the study's §12.75, GATES' seventy-third landing; TCK agreement DuckDB
  3542 → 3555, Postgres 3160 → 3165): zones the database reads (W5 leg 2c above), a named zone's offset at an
  instant (the time of a Stockholm date-time, Temporal3/[3] #16-20 on both lanes; its `offset`; its `toString` with
  the zone's name, Temporal6/[7]). Next: temporals inside a Variant read back as values (WithOrderBy1/[45] #6-#8).
- Temporal values inside lists and nested EXISTS LANDED 2026-10-17 (the study's §12.76, §12.77, GATES' seventy-fourth
  landing; TCK agreement DuckDB 3555 → 3562, Postgres 3165 → 3170): a Variant's temporal read back by the
  database's parse of the oracle's spelling (WithOrderBy1/[45] #6-#8); the EXISTS normal form (a lone node the inner
  pattern binds), a multi-hop EXISTS in a MATCH's WHERE over the pattern's rows, a counted EXISTS as its
  comprehension's size (ExistentialSubquery3/[1]-[3], ExistentialSubquery2/[2]). Next: an OPTIONAL MATCH's WHERE
  reading a value of the rows (Match7/[11], MatchWhere6/[5]).
- An OPTIONAL MATCH whose WHERE reads the rows LANDED 2026-10-17 (the study's §12.78, GATES' seventy-fifth landing;
  TCK agreement DuckDB 3562 → 3564, Postgres 3170 → 3172): the WHERE as the left join's condition over the row and the
  match (Match7/[11], MatchWhere6/[5]), a WHERE over a fanning optional hop through the rows, a named optional path.
  Left (§5): a variable-length relationship beside another in one OPTIONAL MATCH's named path (Match7/[19]).
- Floats that are not finite inside lists LANDED 2026-10-17 (the study's §12.79, GATES' seventy-sixth landing; TCK
  agreement DuckDB 3564 → 3569, Postgres 3172 → 3177): NaN and the infinities in a list, a map or a
  `collect` as the database's own Variant, ordered after the numbers (ReturnOrderBy1/[11], [12], WithOrderBy1/[21],
  [22]; Postgres by S60); a relationship's label as its type, types differing only in case (Graph5/[2]).
- A comprehension over a path's parts LANDED 2026-10-17 (the study's §12.80, GATES' seventy-seventh landing; TCK
  agreement DuckDB 3569 → 3570, Postgres 3177 → 3178): a list comprehension over a fixed-length named
  path's nodes or relationships read per part (Pattern2/[7]); EXISTS among a projection's items over rows.
- The spec guards' repair LANDED 2026-10-17 (the study's §12.81, GATES' seventy-eighth landing; the lanes unchanged):
  `//spec:spec_tests` and the generated files' diff tests, red since earlier legs, green again -- the store's Time
  and TimestampNs declared in the system metamodel, the prelude regenerated, elementId claimed as a resolver form,
  rand() as legend-pure's own `math::random` (lite's twin left), the pins moved with dated reasons; the per-leg
  chain runs those suites and the IQ and SQL lanes from now on.
- Keys null on every row LANDED 2026-10-18 (the study's §12.82, GATES' seventy-ninth landing; TCK agreement DuckDB
  3570, Postgres 3178 → 3189): a NULL grouping key typed by its plan (the lowering), a projected typed null read back
  as that null (the island), a sort key null on every row ordering nothing (Return6/[3], [6], [18], [19],
  ReturnOrderBy6/[2], [3], [5], WithOrderBy4/[17], [18], With6/[6], [7] on Postgres).
- A walk beside single hops LANDED 2026-10-18 (the study's §12.83, GATES' eightieth landing; TCK agreement DuckDB
  3570 → 3571, Postgres 3189 → 3190): a variable-length relationship without an upper bound in a pattern of single
  hops, the pattern cut at the walk, the hops' relationships never one the walk walked, the named path assembled
  across (Match7/[19]). The DuckDB lane's rows outside the writes are now §5's alone.
- Neo4j's own errors where the island refused LANDED 2026-10-18 (the study's §12.84, GATES' eighty-first landing;
  agreement unchanged): 23 TCK rows were BOTH_ERROR behind a refusal of lite's; 22 now raise Neo4j's error (the
  conversions' TypeError as the database's raise, `size()` of a pattern, a property of a path, an update inside
  EXISTS, an unknown function by Neo4j's snapped list of its functions), and `toBoolean` of an integer and
  `toInteger` of a boolean read.
- The expression tail LANDED 2026-10-17 (the study's §12.73, GATES' seventy-first landing; TCK agreement DuckDB 3526 → 3537, Postgres 3147 → 3157):
  UNION over nodes by identity, `%` of floats (DuckDB; Postgres's dialect refuses), computed SKIP and LIMIT, paths
  compared by identity, range of a computed step or a nullable bound, toString of lists, maps, nodes, relationships
  and paths (ReturnSkipLimit1/[3], ReturnSkipLimit2/[6], Union1/[4], Comparison1/[14], List11/[3], Precedence2/[2],
  TypeConversion4/[10]). Left: NaN inside lists (§5).
- The remainder of floats on Postgres LANDED 2026-10-18 (the study's §12.90, GATES' eighty-eighth landing; TCK
  agreement Postgres 3596 → 3597): exact as Java's `%` on every lane (the dialect's spelling over integers times powers
  of two), and Java's NaN by a Float zero (the island raised Pure's error there). The two lanes' censuses are the same
  since.
- Pattern comprehensions over rows LANDED 2026-10-17 (the study's §12.72, GATES' seventieth landing; TCK agreement DuckDB 3520 → 3526, Postgres 3141 → 3147):
  a comprehension the navigations do not read is a list column of the rows (re-anchored, grouped by the shared
  identities, left-joined back; S86); a first OPTIONAL MATCH that finds nothing is one null row; `keys()` of an
  optional node is null where absent (Pattern2/[1], Pattern2/[2], Pattern2/[3], Pattern2/[8], Pattern2/[9],
  Pattern2/[11]). Left: Pattern2/[7] (a comprehension reading a list comprehension's variable).
- Identities in values LANDED 2026-10-17 (the study's §12.71, GATES' sixty-ninth landing; TCK agreement DuckDB 3513 → 3520, Postgres 3134 → 3141):
  node and relationship JSON carry `"@id"` (S85); held nodes re-anchor, list-bound variable-length relationships
  match by identities, node and relationship lists group by identity; the lowering isolates a grouped Variant key
  read into a later computation (Match4/[8], Match7/[22], Match9/[6], Match9/[7], Unwind1/[12], Quantifier1/[9], Quantifier2/[9], Quantifier3/[9], Quantifier4/[9]).
- Several patterns joined on identity LANDED 2026-10-17 (the study's §12.70, GATES' sixty-eighth landing; TCK agreement DuckDB 3494 → 3513, Postgres 3115 → 3134):
  a MATCH of several patterns as rows joined on shared identities, re-anchored patterns that find nothing,
  named paths re-anchored, a label-less start against its stored way, the static list kind (MatchWhere2/[1], MatchWhere4/[2], Match3/[10], Match3/[20], Match3/[21], Match3/[29], Match4/[7], Match7/[17], Match7/[20], Match7/[26], Match7/[4], Match7/[7], Match7/[8], Match9/[9], WithSkipLimit2/[3], WithWhere2/[1], WithWhere4/[2], With1/[2], Graph8/[5]).
- The remainder LANDED 2026-10-17 (the study's §12.69, GATES' sixty-seventh landing; TCK agreement DuckDB 3487 → 3494, Postgres 3108 → 3115): a bounded
  variable-length relationship after another clause re-anchored, a WHERE's variable-length pattern predicate over
  a graph read over rows, the nodes reached through an edge class scoped to their arm (Match4/[6], Match7/[14], Pattern1/[10], Pattern1/[13], Pattern1/[7], Pattern1/[8], Pattern1/[9]).

### W11. Variants of no known shape

- ORDER BY a Variant list: Cypher orders values across types by a type rank, then within the type; the island
  spells a sort key of (rank, value) over a Variant's elements, pairwise, as §12.34 orders known lists. Two legs.
- Two computed lists or two Variants compared: element-wise with Cypher's null rules over the Variant's elements
  (the JSON null distinguished from a missing element). One leg.
- List comprehensions over a Variant of no known element kind: the elements read as Variants, each operation
  over a Variant element spelled by W11's typed reads. One leg (with W3, these are the quantifier scenarios).
- LANDED 2026-10-17 in one leg (the study's §12.58, GATES' fifty-sixth landing; TCK agreement DuckDB 3314 → 3354, Postgres 2964 → 3004; DuckDB PASS 3,322 → 3386, Postgres 2,961 → 3025): one text key per
  Variant (the rank digit, then the value; lists element-wise, a prefix first) for ORDER BY, `min`/`max` (lite's
  `minBy`/`maxBy` over a text key), `<` and `=`; spelled lists of lists, of several kinds or with nulls read as
  Variants; `list + x` by `x`'s kind. Refused by name: NaN inside a list (Postgres's JSON holds none), a list of
  dates read back, ORDER BY a node; the key's number precision and text-ordered maps and nodes are S81.

### W12. The tail

Pattern comprehensions over rows and over two hops (W2's continuation, then the hop machinery), `count(DISTINCT)`
over scalars (the distinct count: order-free, unlike `collect(DISTINCT)`), `toString` of lists and nodes (the
JSON text), min/max over mixed values (Cypher's type rank, with W11), untyped relationships beside other patterns,
SKIP/LIMIT expressions, `STARTS WITH` on non-strings (null), temporal texts of the row. Three legs. Leg 1 LANDED
2026-10-17 (the study's §12.52, GATES' fifty-first landing; TCK agreement 3201 → 3270, DuckDB PASS 3141 → 3232,
Postgres 2780 → 2871):
pattern predicates and EXISTS subqueries over rows as flag columns joined back by identity (Pattern1, the lite
predicates), comprehensions, quantifiers (`single` too), `reverse`, `tail`, `+`, CASE and coalesce over lists the
database holds as Variants (Quantifier9-12 behind `rand()`, List12), a node carried as one of several classes
re-anchoring from every class, `%` inside a database lambda with a literal divisor. Still refused: a pattern
comprehension over rows (a collect per row: Pattern2/[3], [8]), `nodes(x)` of a path held in a list (List12/[4],
[5]), a hop from a node read out of a list (Pattern2/[7]). Leg 2 LANDED 2026-10-17 (the study's §12.53, GATES'
fifty-second landing; TCK agreement 3270 → 3282, DuckDB PASS 3232 → 3259, Postgres 2871 → 2898): an ORDER BY on an expression the query does not return (the result render's
hidden sort output), string predicates on non-strings (null; a Variant by its kind, lite's `isKind`),
`collect(DISTINCT x)` (S78), a duration text in the date-time form. Leg 3, the rest of the tail: a pattern
comprehension over rows, `toString` of lists, maps and nodes (Neo4j's own texts: `[1, a]`, `{a: 1}`, `(:A)`), an
untyped relationship in a MATCH of several patterns; ORDER BY and min/max over Variants are W11's (Cypher's type
rank over JSON kinds, which `isKind` now reads). Leg 3 LANDED 2026-10-17 (the study's §12.54, GATES'
fifty-third landing; TCK agreement 3282 → 3285, DuckDB PASS 3259 → 3273, Postgres 2898 → 2912): a pattern deeper than two association steps, or hopping on from an edge-reached
node, cut into segments of at most two steps met by identity (W9's depth-two limit routed around for typed
patterns); an absent node's identity null (the resolver's `identityText` gated on the key). Leg 4: the cut under
the arms' route (untyped hops), named paths over the segments, variable-length chains past two hops; then the
pattern comprehension over rows and `toString` of lists, maps and nodes. Leg 4 step 1 LANDED 2026-10-17 (the
study's §12.55, GATES' fifty-fourth landing; TCK agreement 3285 → 3299, DuckDB PASS 3273 → 3291, Postgres 2912 → 2930): the cut under the arms' route (an untyped hop a segment
of its own), one identity column for a relationship across arms (uniqueness and `count(DISTINCT r)` over the
union). Steps 2 and 3 LANDED 2026-10-17 (the study's §12.56 and §12.57, GATES' fifty-fifth landing; TCK agreement 3299 → 3314, DuckDB PASS 3291 → 3322, Postgres 2930 → 2961):
a named path over the cut (partial paths, directions as columns, the path assembled over the rows), variable-length
chains past two steps as rows per chain under the same columns; with them a list of scalars as a grouping key
(S79), a Variant of no known shape beside a scalar (S80), three-valued list predicates and nullable comprehensions
over computed lists. Next: an untyped relationship in a MATCH of several patterns, a variable-length relationship
after another clause or in an OPTIONAL MATCH; then W11 (ORDER BY and min/max over Variants), `toString` of lists,
maps and nodes, the pattern comprehension over rows.
- Leg 5 LANDED 2026-10-17 (the study's §12.66, GATES' sixty-fourth landing; TCK agreement DuckDB 3463 → 3470, Postgres 3084 → 3091; DuckDB PASS 3,565 → 3573, Postgres 3,163 → 3171): the extreme float
  literals (the island's stale guard lifted: the dialects cast `toFloat` of one to a double now) and a parameter the
  text did not bind as Neo4j's ParameterMissing (Call1/[11]).

### W13. Nanosecond values as a date and a time of day (the user's decision, 2026-10-18)

The decision: "Let's do it" (the user, 2026-10-18), on the recommendation to replace the nanosecond leg's
`TIMESTAMP_NS` path (§12.12) with a representation every dialect computes, starting with a spike. It overturns S71
(the Postgres lane refusing nanoseconds by name, the user's 2026-10-16 decision) and adds a register row for how a
nanosecond value leaves the database. Why: the same temporal semantics on every database (DuckDB's `TIMESTAMP_NS`
holds only 1678 to 2261, Postgres has no nanosecond type, `timestamp9` is a C extension with the same range and no
arithmetic); the redesign REPLACES the `TIMESTAMP_NS` path, never runs beside it (two nanosecond paths in one
translator is the cost the decision weighed against).

**The representation.** A value of microsecond precision is unchanged: the database's `TIME`, `TIMESTAMP` (S68,
S70's microsecond half). A value of nanosecond precision is java.time's shape, `LocalDateTime` = `LocalDate` +
`LocalTime`:

- a local time: its nanoseconds since midnight, an Integer (the database's `BIGINT`);
- a local date-time: its date (StrictDate, the database's `DATE`) and its nanoseconds since midnight;
- a zoned date-time: the UTC date and the UTC nanoseconds of day, the zone beside as today (§12.8);
- a zoned time: its UTC nanoseconds of day, the offset beside as today (§12.11).

**The operations**, each the database's, the translator spelling them (compare, never compute):

- a calendar step (months, days) on the DATE, as a microsecond value's is;
- time arithmetic in integers with a day carry: `t + s * 10^9 + n`, the whole days of it (floor division by
  86,400 × 10^9) added to the DATE, the rest the new time of day; a time of day's arithmetic wraps (mod);
- fields as integer quotients (hour, minute, second, millisecond, microsecond, nanosecond); truncation as the
  integer floored to the unit, a calendar truncation on the DATE;
- comparison and ordering lexicographic on (date, nanoseconds), a microsecond value read into the same parts;
- a fixed offset as integer arithmetic with the day carry; a named zone through a microsecond timestamp for the
  region's offset at that instant, the sub-microsecond digits kept (as §12.12 keeps them after a calendar step);
- the epoch functions in integers; `duration.between` by whole months and days on the dates and the rest in
  nanoseconds.

**Carriage.** A projection carries both parts (the date a hidden column beside the value, as a zone's offset rides,
§12.75); grouping, DISTINCT and the sort keys read both; a list or `collect` holds the value's ISO text inside the
Variant, read back by its kind; a stored value is a value class of the two parts (W5's embedded classes).

**The egress (the register row).** A returned nanosecond value is one cell: the database writes its ISO text and a
lite native carries it to the caller as the temporal it spells (the `TEMPORAL_TEXT` label, the executor's one
decode seam, which already parses a temporal in text carriage), so the result is the plan's date-time or time with
its nine digits; the Java side carries and parses, the database computes (C1).

**What it lifts:** the 26 DuckDB rows outside 1678 to 2261 (6 refusals, 20 harness skips) and about 417 Postgres
rows (S71's 372 at render, 38 at seeding, 7 by year); not the 2 signed years (beyond every database's DATE).

**The legs:**

1. The spike: the representation, literals, construction, fields, comparison, ordering, text output and the
   egress, for local date-times and times, on both lanes. **Checkpoint:** if the egress is unclean or the two-part
   shape unmanageable in the translator, stop and report to the user before leg 2.
2. Duration arithmetic, truncation, `duration.between`.
3. Zoned values: offsets, named zones, the epoch functions.
4. Carrying through WITH, grouping and DISTINCT, lists and `collect`.
5. Storage and the harness's seeding (a value class), which lifts the 20 skips.
6. The Postgres lane in full, S71 revised, the register row, and the island's `TIMESTAMP_NS` path retired.

- Leg 1 LANDED 2026-10-18 (the study's §12.85, GATES' eighty-third landing; TCK agreement DuckDB 3571 → 3571,
  Postgres 3190 → 3220; DuckDB PASS 3886 → 3900, Postgres 3474 → 3518): the parts (`Parts` on the detail, a WITH's
  hidden date column), literals, construction and selection, fields, `toString`, comparison and ordering, CASE and
  coalesce, grouping and DISTINCT through WITH, sort keys; the egress (`dateTimeOfText`, `timeOfText`: the
  `TEMPORAL_TEXT` label, register row S87); a bridge to the old carriage for what is not re-spelled yet; Neo4j's
  checks of a temporal value's components (found by the leg's cases). **Checkpoint met:** the egress is clean and the
  two-part shape contained, so the legs go on. Next, leg 2: truncation (Temporal9's 262 Postgres rows, most of zoned
  values, so the zoned leg follows close), duration arithmetic, `duration.between`; with it the rename of
  `lite/temporal/nanos_year_outside_the_range_refused_by_name`, which passes now.
- Leg 2 LANDED 2026-10-18 (the study's §12.86, GATES' eighty-fourth landing; TCK agreement DuckDB 3571 → 3571,
  Postgres 3220 → 3296; DuckDB PASS 3900 → 3906, Postgres 3518 → 3602): over the parts, a duration added or
  subtracted (the months and days on the date, the seconds and nanoseconds on the clock, its whole days carried),
  truncation to every unit with fields written over, `duration.between` and its `in` forms of two local values (the
  days' and the clocks' distances apart: 9998 years overflow nothing). Found by its cases: Postgres read a year below
  1000 written in fewer digits as another (`DATE '1-01-01'` as 2001; the lowering writes ISO's four digits now), and
  the judge's duration slots overflowed a long. The year-1 case renamed `nanos_in_the_year_one`. Next, leg 3: the
  zoned values (Temporal9's 192 zoned truncations on Postgres above all), their offsets, named zones and the epoch
  functions, over UTC parts with the zone beside.
- Leg 3 LANDED 2026-10-18 (the study's §12.87, GATES' eighty-fifth landing; TCK agreement DuckDB 3571 → 3571,
  Postgres 3296 → 3540; DuckDB PASS 3906 → 3918, Postgres 3602 → 3868): the zoned values over the parts (a
  zoned date-time its UTC date and UTC nanoseconds since midnight, a zoned time its UTC nanoseconds from the epoch
  day's midnight, the zone beside): the conversions (the offset a spelled sum, a read one, a region's the database's
  reading of the instant's microseconds), construction (texts, components, `fromepoch`), fields, offset and epoch
  fields, `toString`, comparison and order, carriage through WITH, the egress, arithmetic, truncation, `between`;
  a printed local nanosecond value parsed back (Temporal6). Found by its cases and fixed at either precision: the
  database's reading of a wall time a region shows twice (the later instant; java.time's the earlier), a zoned time
  converted to another offset (java.time's wall time wrapped), `fromepoch`'s nanoseconds past a second (Neo4j's
  error). Measured and withdrawn for the user's S66: Neo4j's equality and order of one instant in two zones (§5).
  Next, leg 4: lists, `collect`, `min` and `max`, grouping keys and `IN` over the parts (WithOrderBy1 and the
  `temporallists` cases on Postgres, the 6 year refusals on both lanes).
- Leg 4 LANDED 2026-10-18 (the study's §12.88, GATES' eighty-sixth landing; TCK agreement DuckDB 3571 → 3577,
  Postgres 3540 → 3552; DuckDB PASS 3918 → 3936, Postgres 3868 → 3896): lists and maps of nanosecond values as the
  oracle's texts of their parts, read back as the parts; `IN`, `collect`, `min` and `max` (by the sortable text),
  returned grouping keys (by the egress text), CASE and coalesce of zoned values; a nanosecond component of the row.
  The bridge to §12.12's carriage is removed: no TCK row refuses a nanosecond value at render on either lane. Found by
  its cases: Postgres's limit of 100 arguments to a function (the dialect nests `concat` past it). Next, leg 5: stored
  values (a value class of the parts in place of the `TIMESTAMP_NS` column: Postgres's 24 seeding refusals, DuckDB's
  20 harness skips), then leg 6, the `TIMESTAMP_NS` path retired.
- Leg 5 LANDED 2026-10-18 (the study's §12.89, GATES' eighty-seventh landing; TCK agreement DuckDB 3577 → 3597,
  Postgres 3552 → 3596; DuckDB PASS 3936 → 3968, Postgres 3896 → 3966): a node's stored nanosecond value is an
  embedded instance of a value class of its parts (`LocalDateTime`, `LocalTime`, and the zoned `ZonedDateTime` and
  `ZonedTime` of the instant's UTC parts beside the offset), read as the split value, carried with its node through
  WITH, a label-less node's arms, an OPTIONAL MATCH and a variable-length pattern's chains; the harness seeds the
  parts on both lanes. Found by its cases: a date-time text without its time (Neo4j's midnight). No TCK row outside
  the write clauses is refused for a nanosecond value on either lane. Next, leg 6: a relationship's stored nanosecond
  value (an edge class's `TIMESTAMP_NS` column, which no TCK row needs) onto the parts, the island's
  `TIMESTAMP_NS` carriage and the profile's marks retired, S71 revised in full.
- Leg 6 LANDED 2026-10-18 (the study's §12.91, GATES' eighty-ninth landing; TCK agreement 3597 on both lanes,
  unchanged; PASS 3976 on both): a relationship's temporal values as a node's (embedded value classes, carried as
  parts and zones); a model's marked `TIMESTAMP_NS` column read into the parts at the property, so the epoch carriage
  is minted nowhere (each former mint a loud check, green on both lanes). The profile's marks stay: a model's own
  `TIMESTAMP_NS` column is read through them (S64). The carriage's now-unreachable branches (the operations spelled
  over the nanosecond epoch) deleted after it, 2026-10-18: no row moved, the chain green on both lanes. W13 is done.
- The three setups "Neo4j refused" RESOLVED 2026-10-18 (the study's §12.92, GATES' ninetieth landing; TCK agreement
  3597 → 3600 on both lanes): the oracle runs a setup statement under `CYPHER 5` (the TCK's setups are openCypher 9's
  text, which Cypher 25 narrows); Neo4j passes WithSkipLimit1/[1], WithSkipLimit2/[2] and With2/[1], and so do both
  lanes. Outside the write clauses the TCK holds only the 2 signed years.
- The independent review of the branch, before its PR, 2026-10-18 (the study's §12.93, GATES' ninety-first landing):
  the island's findings (W13's parts in seven places, years outside 1 to 9999 in texts, truncation with written
  fields, `duration.between`, row clocks' ranges) and the platform's (the inliner's draws, Postgres's `REM` under a
  grouping, H2's `TIME`, `random()`'s range on SQLite and Snowflake, the streamed rows' hidden key, a partial
  date's year) fixed and pinned; `lite/review.yaml`'s 21 cases; the TCK unchanged at 3600 on both lanes.

## 3. Decisions asked of the user

1. **W1:** adopt Neo4j's schema-optional reads (null, no rows) for the island, overturning S58? (Proposed: yes.)
   **Reframed by the user, 2026-10-17 (§3.2 Q1): GQL's OPEN and CLOSED graphs.** An open graph reads an unknown
   label or relationship type as no rows and an undeclared property as null; a closed graph keeps C4's compile
   errors. The harness's graphs are open (a `CREATE` with no schema); an island embedded in Pure source is
   closed; S58 becomes the two readings. Agreed.
2. **W2:** node identity through a hidden `<<equality.Key>>` property the harness declares, relationships as
   edge classes wherever a setup holds parallel links; S74 and S75 rewritten? (Proposed: yes.)
3. **W3:** `rand()` and the clock functions as the database's? (Proposed: yes.)
4. **W5:** a `TIME` column type added to lite's relational grammar (a lite extension of legend-engine's), or
   times of day stored as texts? (Proposed: the column type.)
5. **W6:** a node with several labels as a subclass of each label's class? (Proposed: yes, after verifying
   subclass reads.)
6. **W4:** writes in their own PR after PR #50, F1 first, judged by side effects? (Proposed: yes.)
7. **W9, W10:** this line may change the lowering, the resolver and the dialects under their own guards, or
   hands the four findings to the database owner? (Proposed: this line, one leg each, each with its guard.)

### 3.1 The decisions weighed (2026-10-17, on the user's ask)

**1. Schema-optional reads (W1, 56 cases).** For: Neo4j's rule, which a Cypher user expects; the island already reads
a property an arm's class lacks as the known null, so the change is one step wider and one leg. Against: a typo in a
property name (`p.nmae`) answers nulls where today it fails at compile time; that guard was the reason for C4 and is
in the spirit of "no fallbacks". Cost: one leg; S58 rewritten. Alternatives: (a) keep the compile error by default
and take Neo4j's semantics under a per-query option the harness sets, which keeps the guard for users at the price of
two behaviours to test and a TCK that certifies the option, not the default; (b) a middle rule, no rows for an
unknown label (schema-optional) but an error for a property no class of the whole model declares (the true typo
signal), which keeps 10 of the 56 cases refused. Recommended: Neo4j's rule, one behaviour; (a) if the guard matters
more than one behaviour.

**2. Identity through hidden keys (W2, ~100 cases, S74 and S75).** For: identity is what Neo4j's node is, and the
pieces exist (the harness's `id` columns, the platform's `<<equality.Key>>` resolution); nothing is approximated by
comparing properties; writes need a minted identity anyway. Against: a user model must declare a key per class to
get identity-dependent queries (a keyless class keeps today's refusals); a hidden-key stereotype is one more lite-only
meaning on the graph profile; parallel relationships as edge classes make every hop over them two steps and meet the
resolver's depth limit (W9) on chains. Cost: three to four legs plus harness and register. Alternatives: (a) the
lowering's own row identity (a class instance's mapped primary key, exposed to the island as an expression): no model
change, works for any class with a mapped key, but it is the database owner's line and a class mapped without a key
has none; (b) identity by row address (`rowid`, `ctid`): unstable across joins and unions, no; (c) identity by all
properties: an approximation, two equal nodes merge, no. Recommended: the hidden key property now, with (a) as the
later migration if the owner provides it.

**3. `rand()` and the clock as the database's (W3, 50 cases).** For: the TCK's invariants hold for any draw or
instant; every dialect has a random and a clock; one leg. Against: a query's rows can differ run to run, so lite's
own cases may pin only invariants (`rand() >= 0`, `date() = date()`); the clock forms differ (Neo4j's
`.statement`/`.transaction` are one instant per statement, `.realtime` per call): Postgres's `now()` is per
transaction and `clock_timestamp()` per call, DuckDB's `now()` per transaction. Cost: one leg. Alternative: seed the
database's random per lite case (`setseed`, both databases have it) so a lite case can pin a drawn value; a
nicety, not needed for the TCK. Recommended: adopt.

**4. Stored times of day: a `TIME` column type, or an existing type under a marker (W5, 72 cases).** (a) Add `TIME`
(and a nanosecond timestamp) to lite's relational store grammar: honest typing, each dialect's DDL; but a model using
it no longer compiles in legend-engine or Studio, which the graph profile was careful to keep. (b) Keep the grammar:
a time of day as a `TIMESTAMP` on the epoch day under a `graph.timeOfDay` marker (the convention the nanosecond leg
and the judge already use for computed times), a duration as the `g::Duration` Variant the harness already declares,
a zoned date-time as its instant plus an offset column, a sub-microsecond date-time as `BIGINT` epoch nanoseconds
under a marker; the island reads each by its marker. (c) Texts in `VARCHAR` with a transform: orders only when every
value has one precision, and writes must format. Cost: (a) about two legs, (b) one to two, (c) two. Recommended: (b),
reversing the proposal above: the same markers-give-meaning approach as the graph profile, and no grammar divergence.

**5. Nodes with several labels (W6, 18 cases).** (a) A class per label set, `A_B extends A, B`, `MATCH (n:A)` reading
`A` and its subclasses: Pure-native, `labels(n)` the class's set, `n:B` a subtype test; but every label set must be a
class before the query runs (a `SET n:B` on a node of `A` alone moves its row to `A_B`, which the model must already
hold), the subclass tables multiply, and whether the relational mapping reads subclasses through `A.all()` is to be
verified first. (b) A class per label and a node's row in each of its labels' tables under one hidden id (decision
2): `MATCH (n:A)` reads `A`, `labels(n)` is the id's presence across the label tables, `SET n:B` inserts a row and
`REMOVE n:B` deletes one; but a property of a node with two labels must live in one table and be found from the other,
so the harness declares it on every class the node carries (duplicated columns) or the island reads across tables by
id. Cost: two legs either way. Recommended: (a) for reads now; (b) is the better shape once writes land, so decide
with decision 6 in view.

**6. Writes in their own PR after F1, judged by side effects (W4, 274 cases).** (a) F1 first: Pure's `write` and the
DML the MIR lacks (`Update`, `DeleteWhere`, `InsertSelect`), each dialect, one transaction per statement; the island's
clauses on top: one compiler, and the ETL sinks of the BI plan get it too. (b) The island emitting DML plans of its
own beside its Pure: faster to start, but a second output path from one island, against the one-compiler tenet.
Judging: the TCK asserts side-effect counts (`+nodes 1, +properties 2`) and sometimes a control query; a stronger
lite-only judge compares the whole graph after the write with Neo4j's (nodes by labels and properties, relationships
by endpoints, type and properties). Cost: F1 is the BI plan's "L"; the island six to ten legs. Recommended: (a),
side-effect counts first, the whole-graph judge for lite's cases.

**7. Who changes the lowering, resolver and dialects (W9, W10, 74 cases).** The four findings are platform work:
`project` of a to-many navigation mapped to a constant (lowering, medium), the resolver's nested navigation past a
second association step (resolver, medium), `rem`'s guard typed as text inside a Postgres lambda (dialect, small), a
sort key that is not an output column without SKIP or LIMIT (render, small); the recursion native for unbounded
paths is a feature (a MIR node, every dialect, a depth guard), large. (a) This line takes the four under the
platform's guards (ArchitectureTest, the PCT suites, the corpus lanes): no waiting, but it touches the one compiler
every island and PCT runs on, and the heavy lanes run only in CI. (b) The database owner's line takes them: ownership
kept, at the queue's pace. Recommended: this line takes the two small ones and the fan-out with the owner's review,
the resolver depth too; the recursion native stays a planned platform item with the owner.

**A cross-cutting alternative, not recommended:** a "Neo4j compatibility mode" switch that turns decisions 1 and 3
on for the harness and leaves lite's stricter defaults for users. It would keep every present guard, but it doubles
the behaviours to test and the TCK would then certify the mode rather than the island; the island is Cypher, and
Cypher's semantics are its contract.

### 3.2 The user's six questions, answered (2026-10-17)

**Q1. GQL's open and closed graphs, for decision 1.** GQL (ISO/IEC 39075:2024) calls a graph *closed* when a
graph type constrains it (it holds only the node and edge types the type declares, with their labels and property
types) and *open* when none does (any label, any property, created on the fly). Neo4j's graphs are open: a label or
a property exists the moment a node carries it. A lite graph is a model, and a model IS a graph type (classes with
typed properties, associations as edge types), so every lite graph is closed by construction and C4 is the closed
reading: a label no class has, a property no class of the label declares, a relationship type naming no association
end is a compile error. Adopting GQL's pair: a graph declared OPEN reads an unknown label or relationship type as no
rows and an undeclared property as null (Neo4j's answers; the island already reads a property an arm's class lacks
as null, so the rule is one step wider); a CLOSED graph keeps C4. The verdict asked for: yes, and it is a better
frame than the "compatibility mode" §3.1 rejected, because openness is a property of the GRAPH, in the standard's
own words, not a switch on the engine; the TCK's fixtures are open graphs (a `CREATE` with no schema), so the TCK
then certifies the open reading of an open graph, which is what it measures on Neo4j, and a user's model stays
closed unless it says otherwise, so the typo guard (`p.nmae`) is lost nowhere by default. Where it is declared: GQL
declares it on the graph; lite has no graph element yet (the model is the graph), so the translator's `Graph` takes
its openness from its caller: the parity harness builds its model from a scenario's `CREATE` and passes OPEN; an
island embedded in Pure source (`#Cypher{...}#`) is CLOSED. The one open point: a model-side spelling of openness
(a graph element, which the writes of W4 may want anyway as the thing `CREATE` writes into, or a tag of the graph
profile) when a user wants an open graph; not needed for the TCK. Cost: one leg, the closed pins kept and the open
reading pinned beside them; S58 rewritten as the two readings. **Addendum (the user, later the same day): the graph
is declared with Cypher's own `USE`, naming a DataSpace (GQL's graph: its elements the labels, its execution context
the store), open by `<<graph.Open>>` on the DataSpace; neither a class stereotype (static, one way per model) nor
an island prefix (invented syntax). Landed so (the study's §12.45).**

**Q2. The identity problem, and the right thing on the Legend side.** What Neo4j does: every node and relationship
has an identity of its own (an element id the store mints), independent of its labels and properties. Two nodes
created alike are two nodes: after `CREATE (:A {x: 1}), (:A {x: 1})`, `MATCH (a:A), (b:A) WHERE a <> b RETURN
count(*)` is 2, `MATCH (a:A) RETURN count(DISTINCT a)` is 2, and `collect(a)` holds two. Identity is what `a = b`
and `a <> b` compare on nodes and relationships, what `DISTINCT`, the grouping keys, `count(DISTINCT n)`, `n IN
list` and `MERGE`'s matching use, and what relationship uniqueness is (one match never uses the same relationship
twice: `(a)-[r1]->(b)-[r2]->(c)` has `r1 <> r2` by identity). What lite does today: a node value is its label and
its properties, so two nodes alike are one value; the ~100 cases of W2 are the queries where that is visible (S74,
S75: counts, distinctness, comparisons, uniqueness, the triadic selection use case), and the harness's node tables
already hold an `id BIGINT PRIMARY KEY` the model does not see. What Legend's own notion is: a Pure instance has
identity; `==` on two instances compares their `<<equality.Key>>` properties when the class declares them, else the
instances themselves (the platform's `InstanceEquality`: keyed classes by their key tree, keyless by identity, which
the verdict lane mints as `__id` for constructed instances); under a relational mapping an instance's identity is
the class mapping's `~primaryKey` (legend-engine builds its graph-fetch objects by it; a mapping without one keys
on every mapped column, which is exactly the "two alike nodes are one" collapse). So the right thing on the Legend
side is: node identity = the class mapping's primary key, Legend's own identity of a mapped instance, never a
comparison of properties. Two ways to reach it from the island's Pure: (a) the harness declares the `id` column as
a property of the class marked `<<equality.Key>>` and hidden (a `graph.hidden` tag: `properties(n)`, `keys(n)` and a
node value omit it), so `$a == $b`, `distinct`, grouping and `in` resolve to the key through the platform's existing
EqualityKeys and the identity travels through `WITH` as a column; works today, no platform change, three to four
legs; (b) the platform resolves `==` of a keyless mapped class to its mapping's primary key (reference identity
under the mapping, the faithful semantics, not a fallback) and the island spells identity through a native
(`identity($n)`, lowered to the key columns), so a user's model needs no extra property; the database owner's line,
and the island still needs the native to carry identity through projections. Recommended: (a) now (the user's
"a way to hide keys if needed"), (b) as the platform item that lets any mapped class, keyed or not, answer
identity queries without a declared property. Relationships: an edge class gets the same hidden key; a relationship
mapped as a bare association end (no edge class) has no row of its own and no identity, so uniqueness over it is
the pair of endpoint identities, which is Neo4j's answer wherever the graph holds no parallel relationships (S74).
**Addendum (the user, three turns later): superseded by W2's settled design: identity is the mapped instance's row
key by the engine's precedence (`~primaryKey`, else the main table's `PRIMARY KEY`, else refuse), carried as the
PCT lane's `__id` and exposed by one native (`elementId`); `<<equality.Key>>` stays value equality and is never
identity; no key property is declared or hidden by anyone.**

**Q3. How the TCK's `rand()` and clock scenarios pass.** The 50 cases of W3 assert nothing a draw or an instant
decides. The quantifier features (Quantifier9, 11, 12: 30 cases) build a list by random filtering and random
reversal three times (`[y IN inputList WHERE rand() > 0.5 | y]`, `CASE WHEN rand() < 0.5 THEN reverse(list) ELSE
list END + x`) and then assert an identity that holds for every list: `none(x IN list WHERE p) = (NOT any(x IN
list WHERE p))` is `true`, `none(x IN list WHERE false)` on a non-empty list is `true`, `single(x IN list WHERE p) =
(size([x IN list WHERE p | x]) = 1)` is `true`; the randomness is there to vary the list, the answer is the same
row every run. The clock features (Temporal4/[13]: 15 cases, Temporal10/[12] and [13]: 5 cases) assert
`date(null)`, `date.statement(null)`, `localtime.realtime(null)` ... = `null` (the argument decides, the clock is
never read) and `duration.inSeconds(datetime(), datetime())` = `'PT0S'`, which holds because Neo4j's clock is one
instant per statement (`.statement`, the default; `.transaction` per transaction; `.realtime` per call): both calls
read the same instant. In SQL the statement clock exists on every dialect (`now()` and `current_timestamp` are the
transaction's start on Postgres and DuckDB; `clock_timestamp()` is the per-call one), so `datetime()` lowered as
the statement clock gives `PT0S` too, and `.realtime` lowered as the per-call clock gives Neo4j's semantics. So the
judge needs nothing new: it compares final rows, and the final rows are deterministic. The two rand cases outside
W3 already pass: `RETURN count(rand())` is a compile error on both sides (Return6/[15]) and `SKIP
toInteger(rand()*9)` asserts only `count > 0` (ReturnSkipLimit1/[3]). Lite's own cases pin invariants only
(`rand() >= 0 AND rand() < 1`, `date() = date()`); the database's `setseed` is a nicety for pinning a draw, not
needed.

**Q4. Times of day and Pure's `StrictTime`.** The same thing. Pure's `StrictTime` is a time of day with no date and
no zone (`%12:34:56.789`); Cypher's `localtime` is exactly that, and the island already carries it as StrictTime (the
MIR's `TIME`, every dialect, the nanosecond leg's `TIMESTAMP_NS` on the epoch day for sub-microsecond ones).
Cypher's `time` is a time of day with a UTC offset (Neo4j's `OffsetTime`), which the island carries as the UTC
StrictTime with the offset beside (§12.11). The W5 gap is not the value but the STORE: lite's `###Relational`
grammar (legend-engine's, kept verbatim) has `TIMESTAMP` and `DATE` and no `TIME` column type (S64), so a StrictTime
property has no column to map to, and a scenario whose `CREATE` stores a time of day is skipped by the harness.
§3.1's decision 4 stands: a `TIMESTAMP` on the epoch day under a `graph.timeOfDay` marker, read back as StrictTime,
keeps the grammar legend-engine's.

**Q5. Labels, and what "several labels" asks.** A label is a tag a node carries: `(:Person)` carries one, `(n:Person:
Employee)` carries two, `()` none; a node's labels are a SET, not a type, so any node may carry any labels, nodes of
the same label may hold different properties, and `SET n:Employee` and `REMOVE n:Employee` change the set at run
time. A pattern `(n:A)` matches every node whose set contains `A`; `(n:A:B)` every node whose set contains both;
`labels(n)` returns the set; `n:B` tests membership. Lite today: a label is a class (the harness makes one class and
one table per label), so a node's label set is one class, and a node carrying two labels has no class: the harness
skips the scenario (18 cases, "harness: a node with several labels"). Match1/[3] shows the whole problem: the
setup creates `(:A:B:C), (:A:B), (:A:C), (:B:C), (:A), (:B), (:C)` and three unlabelled nodes, and `MATCH (a:A:B)
RETURN a` must answer `(:A:B)` and `(:A:B:C)`: seven distinct label sets, the answer two of them, and the returned
node value shows ALL its labels, including the `C` the pattern never named. Match3/[7] creates one node of
thirteen labels and one of six and matches both by their full sets in another order. Two shapes: (a) a class per
LABEL SET, tagged with its labels (`g::A_B_C` with `{graph.labels = 'A,B,C'}`), `MATCH (n:A:B)` reading the arms of
every class whose set contains both, exactly the label-less machinery (§12.13 unions every class's arm at the first
projection) restricted by label membership; `labels(n)` is the class's set, `n:B` a constant per arm, a node value
prints its class's set: reads need no new mechanism and one harness change, and seven classes for Match1/[3] is
what the setup holds; writes are the cost: `SET n:C` on a node of `A_B` MOVES its row to `A_B_C`, which the model
must already declare (the harness can, from every label set the scenario mentions; a user cannot foresee every
set). (b) A class per LABEL and a node's row in each of its labels' tables under one identity (decision 2):
`MATCH (n:A:B)` is a join of `A` and `B` on the id, `labels(n)` is the id's presence across the label tables,
`SET n:C` inserts one row and `REMOVE n:C` deletes one; but a node's properties must live in one place (a node
table keyed by the id, the label tables holding only ids, or every label table repeating the columns), so each
read becomes id-joins and the harness's model changes shape. Recommended: (a) for the reads of Phase C (18
cases, two legs, no identity needed), reconsidered when W4 reaches `SET n:Label` (Set3, Merge3, Merge4: 7 cases),
where (b) is the honest relational shape and (a) a row move. Inheritance (`A_B extends A, B`) is not needed for
either: the arms do what subclass reads would, and a relational mapping's `A.all()` reads A's table only.

**Q6. DML: the "poor man's" MIR, and the right way.** What exists: `SqlDml` (`InsertValues`, `InsertFromTable`,
`DeleteAll`, `CreateTempTableAs`) is a real MIR half, rendered by every dialect through the same writer as a query;
what is poor about it is everything around it: it has no Pure front (Java builds the records: the CSV seed, the
DDL seeding, the row loader, the test-data generator's temp table), no `UPDATE`, no `DELETE ... WHERE`, no `INSERT
... SELECT` of a query, no `MERGE`, and three ad-hoc execution sites. Pure's own write native already exists in
lite's registry: legend-engine's `write<T>(rel: Relation<T>[1], accessor: RelationElementAccessor<T>[1]): Integer[1]`
(`#TDS ...#->write(#/db/T#)`, rows in, a count out), declared and not lowered. The right way, by the tenets (Java
orchestrates, the database executes; one compiler): a write is a Pure relation expression like a read, planned by
the one compiler to one DML statement the dialect renders and the database executes in a transaction, the count
its result: `write` for insert (its relation any query: `INSERT ... SELECT`, or a `Values` source: `INSERT ...
VALUES`), and lite natives in `meta::legend::lite::` for what legend-engine never declared: `update(accessor,
filter-lambda, ~[col: x | expr])` → `UPDATE ... SET ... WHERE`, `delete(accessor, filter-lambda)` → `DELETE ...
WHERE` (`deleteAll` the filter-less form, today's `DeleteAll`), `merge(rel, accessor, ~[keys])` → `MERGE` /
`INSERT ... ON CONFLICT` per dialect; each one MIR variant (`SqlDml.InsertSelect`, `Update`, `DeleteWhere`,
`Merge`), one render arm per dialect, one execution seam (`Execution.executeWrite`, one transaction per
statement or per `[...]` of statements). Who uses it: the Cypher island's `CREATE`, `SET`, `REMOVE`, `DELETE`,
`MERGE` (W4, 274 cases), the BI plan's ETL sinks ("L"), the test-data generator, and any user writing Pure. The
Java seeding sites keep constructing the records where no Pure exists (a fixture's rows are data, and building the
MIR directly is orchestration, not evaluation) or move to `write` of a `Values` relation; the temp table stays
planning-internal. So: yes, real relation functions, and `SqlDml` grows into their MIR rather than being replaced;
F1 of W4 is exactly this, before the island's clauses.

## 4. The order, and the count it would reach (rewritten 2026-10-17 on the user's "attack expertly" ask)

The remaining 787 (DuckDB: REFUSED 606, SKIP 181; agreement 3093 of 3880) are three kinds of work, ordered by what
gates what:

| phase | legs | what, and why here |
|---|---|---|
| 1 identity, then joins on it | ~5 | W2: the keystone most buckets lean on (cycles, DISTINCT and grouping over nodes, uniqueness, MERGE and DELETE later, the triadic use cases); then re-anchoring by join on identity, which retires the resolver's depth limit from the translator's side: ~150 cases hang off the pair |
| 2 cheap translator fillers | ~6 | W3 (50: a lite `random()` native and the statement clock, one MIR function each, a render arm per dialect) and W12 (62: COUNT/COLLECT subqueries, untyped relationships in OPTIONAL and multi-pattern MATCH, string predicates on non-strings, SKIP/LIMIT expressions, named-zone leftovers), interleaved while bigger legs wait on lanes |
| 3 the harness's models | ~5 | W5 stored temporals (72: the marker convention, no grammar change), W6 label sets (18: a class per set, multiple inheritance, the arms union; reads only), W7 two-typed properties (6), W8 procedures (50: the TCK declares each procedure's signature and rows, so the harness declares it as a Pure function returning that relation and `CALL ... YIELD` calls it) |
| 4 the platform's limits | ~9 | W9's remainder (fan-out to a property-less class 21, `%` in a database lambda 9, literal edge cases 14, ORDER BY a non-returned item 4), W11 the Variant family (68: comprehension over a JSON array, element-wise list ordering and comparison by a canonical key the database computes, min/max across kinds), W10 (15: the one real feature, a recursive MIR node for unbounded paths, WITH RECURSIVE on DuckDB and Postgres with a depth guard) |
| 5 writes, on a real foundation | ~7 | F1 first as its own landing (Pure's `write`, lite's `update`/`delete`/`merge` natives, four MIR variants, a render arm per dialect, one execution seam in a transaction); then the stronger judge BEFORE any write clause (the whole graph dumped after the write and compared structurally with Neo4j's); then CREATE, SET and REMOVE, DELETE and DETACH DELETE, MERGE; the 33 control-query scenarios come free |

About 32 legs. Each lands as the forty-four before it: Neo4j-judged lite cases, both lanes in update mode with FAIL,
BUG and ERROR at zero, the oracle check, the guards, the study's section, GATES, a commit and a push on the branch.
Three risks held in view: the writes judge (built before the first write clause), nondeterminism (`rand`, the order
of collected and distinct values: pin invariants only), the two lanes diverging (every leg runs both).

**Standing order (the user, 2026-10-17):** keep going until the TCK is at zero or as close as makes sense; no
questions, no PRs; a blocker is logged in §5 and the line moves to the next item; no hacks or shortcuts.

## 5. Blockers log

What got stuck, where, and why, for later; each entry names the cases it holds back.

- **2026-10-18, W13 leg 3 (§12.87) — one instant in two zones (S66, a decision of the user's).** Neo4j's `=` on
  zoned values compares the instant and the zone (java.time's `equals`: `datetime('2017-01-01T12:00+01:00') =
  datetime('2017-01-01T11:00Z')` is false; `time('12:00+01:00') = time('11:00Z')` too; `+02:00` against
  `[Europe/Stockholm]` at one instant), and its order breaks a tie of instants by the offsets, then the zones (an
  offset before a region, two regions by name). The island answers both by the instant, as S66 decided ("a zone kept
  with the value would need a Pure type"). Since S65 the zone rides beside the value wherever the translator sees it,
  so both can be spelled over the carried zones (the offsets and regions the database reads compared by it; a zone
  not carried, a CASE's or an aggregate's, refused by name for `=`): implemented and measured green on both lanes,
  withdrawn because S66 is the user's to revisit. It holds back no TCK row (the TCK compares no one instant in two
  zones); the lite cases `datetime_comparison`, `time_comparison` and
  `stored/a_stored_zoned_time_compared_with_a_zoned_literal` pin S66.

- **2026-10-18, W13 leg 1 (§12.85) — a temporal value's field of another form.** `date({year: 2000, week: 2, month:
  1})` is Neo4j's `Cannot assign month to week date.`, `date({year: 2000, quarter: 2, day: 3})` its `Cannot assign
  quarter to calendar date.`: the field named is the one Neo4j's builder meets second, in its map's order (measured:
  `{quarter, dayOfQuarter, month}` names dayOfQuarter, `{quarter, day}` quarter), which the island does not know; it
  builds a value where Neo4j rejects one. No TCK case is held back (the lanes found none); Neo4j's other checks of the
  components (order, completeness, the hour) are raised since §12.85.

- **2026-10-18, W12 (§12.84) — a percentile read from the rows.** `RETURN percentileDisc(0.90, deg), deg`
  (Aggregation6/[5]): the percentile is the grouping key `deg`, so Neo4j raises at run time where it falls outside 0
  to 1 ("Invalid input '3.0' is not a valid argument"). The island spells a percentile the database takes as a
  constant only (`PERCENTILE_DISC(p) WITHIN GROUP`); a percentile of each group needs either the database to take a
  per-group fraction or the range check spelled as the database's raise beside a constant one. One TCK row, a
  BOTH_ERROR behind a refusal (the last such).

- **2026-10-17, W5 (§12.74) — nanosecond values outside the databases' nanosecond range.** `localdatetime({year: 1,
  ..., nanosecond: 1})` and year 9999 beside nanosecond values (WithOrderBy1/[17]-[20], [39]-[42], [45] #9 and #10,
  WithOrderBy2/[17]-[20]: 6 refusals and 20 harness skips): the island carries a nanosecond date-time as the
  database's TIMESTAMP_NS (§12.12), which DuckDB holds from 1678 to 2261 and Postgres not at all. Physically out of
  the native types' reach; an encoding of lite's own would carry them (a microsecond timestamp and the nanoseconds
  past it, epoch arithmetic in 128-bit integers), every nanosecond operation re-spelled over it: a design decision
  for the user, not taken here. TAKEN 2026-10-18: W13 (a date and the nanoseconds of the day, on every dialect);
  since its leg 1 a local value of any year its DATE holds is carried and returned on both lanes, the zoned values
  since leg 3, lists since leg 4 (WithOrderBy1's six rows pass on DuckDB, §12.88), a node's stored values since
  leg 5 (the 20 harness skips and Postgres's 24 seeding refusals pass, §12.89).
- **2026-10-17, W5 (§12.74) — years beyond the databases' dates.** `duration.between(date('-999999999-01-01'),
  date('+999999999-12-31'))` (Temporal10/[9], [10]): DuckDB's dates end near year 5,881,580, Postgres's near
  5,874,897; Neo4j's years reach 999,999,999. Physically out of reach of the databases' date types.

- **2026-10-17, W12 (§12.70) — an OPTIONAL MATCH whose WHERE reads the rows beside its pattern.** RESOLVED 2026-10-17 (§12.78: the WHERE is the left join's condition, each variable read off its side). `MATCH (a1)-[r]->()
  WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2` (MatchWhere6/[5]); `MATCH (a)-[r {name: 'r1'}]-(b)
  OPTIONAL MATCH (b)-[r2]-(c) WHERE r <> r2` (Match7/[11]). The re-anchored pattern's WHERE is read over the
  pattern's own rows before the left join; one reading a value of the rows beside the pattern belongs in the left
  join's condition, over both sides. The translator reads a condition over one row (`RowScope`, its columns read off
  one variable); spelling it over the join's two needs either a scope reading each column off its side, or the
  rows' values the WHERE reads joined into the pattern's rows first (on the shared identities) and the left join
  matched on them too. Two TCK rows.
- **2026-10-17, W12 (§12.80) — a null grouping key on Postgres.** RESOLVED 2026-10-18 (§12.82: the key typed by its plan, the column read back as its null, a sort key null on every row dropped). `MATCH (a) WITH a.num AS a, count(*) AS count
  RETURN count` and ten more (Return6/[3], [6], [18], [19], ReturnOrderBy6/[2], [3], [5], WithOrderBy4/[17], [18],
  With6/[6], [7]) group by a property no class declares, a typed null; Postgres refuses a bare NULL key, and its
  dialect refuses it by name (`ConstantKeysAsExpressions`). Casting the key to the plan's declared type lets ten of
  them run (IQ-safe); ReturnOrderBy6/[5] then needs `toOne` of the null column not spelled at all (a raise in a CASE
  arm Postgres types when it plans), which the island can do by reading an item that is a null constant back as
  one; typing the raise in the lowering broke the IQ corpus (§12.80). Eleven Postgres rows; the next island leg.
- **2026-10-17, W10 (§12.78) — a variable-length relationship beside another relationship in one pattern.** RESOLVED 2026-10-18 (§12.83: the pattern cut at the walk, each side a MATCH of its own, uniqueness across the cut by identity, the path assembled across). `MATCH
  (a {name: 'A'}) OPTIONAL MATCH p = (a)-->(b)-[*]->(c) RETURN p` (Match7/[19]): the walk (§12.68) reads a pattern
  of one variable-length relationship between two nodes; beside a single relationship, the pattern is cut at the
  walk (each part its rows, met on the shared node's identity), and two things are unspelled there. Relationship
  uniqueness between the single relationship and the walk's list: the single one is carried as its JSON, with no
  identity column of its own past the cut, where the walk compares identities (S75), so the cut's uniqueness check
  has nothing to compare (the experiment reached "relationship uniqueness across a deep pattern's cut ... over a
  relationship carried without an identity of its own"). And the named path across a walk segment: the walk's path
  parts (its carried texts) joined to the single hop's (§12.56's assembly over the rows reads fixed-length segments
  only). Both are the walk carrying, past the cut, the identities and the path parts it already builds inside its
  recursion; one TCK row (no lite case reaches it).
- **2026-10-17, W10 leg 2b — a variable-length relationship bound to a list of relationships.** RESOLVED 2026-10-17 (the study's §12.71: the values carry their identities). `MATCH ()-[r1]->()
  -[r2]->() WITH [r1, r2] AS rs LIMIT 1 MATCH (first)-[rs*]->(second)` (Match4/[8], Match9/[6], [7]): Neo4j walks
  only the paths whose relationships are the list's, in order. The rows carry `rs` as the Variant list of the
  relationships' JSON values (`{"@type": "Y"}`), which holds no identity; the walk compares identities (the ordered
  pair of an association relationship's ends, S74). A faithful spelling carries the identities' text beside such a
  list when a WITH projects it (as a path's parts ride beside it) and joins the walk's identities-walked text to it
  (the re-anchoring's `joinKeys` pairing the two); comparing the JSON texts instead would equate two parallel
  relationships of one type, which C5 forbids. Three TCK rows; the next W10 slice.
- **2026-10-17, W2 leg 1 — the engine's key precedence, unverified.** The claim that legend-engine's relational
  mapping defaults an instance's identity to the main table's `PRIMARY KEY` when `~primaryKey` is absent (the
  design's step 3) is implemented from memory of the engine's `PrimaryKey` inference; no reference checkout is on
  this machine (`-Dlegend.engine.root` unset), so it is not yet verified verbatim against the engine's compiler
  source (the verification tenet). Holds back nothing; to be checked at the next session with a checkout.
- **2026-10-17, W12 leg 1 — the resolver's object-space lambda reads no collection operation over a literal
  list.** `g::A.all()->project(~[n:n|..., odd:n|[1, 2, 3]->filter(x|$x > 1)->map(x|$x)->toVariant()])` fails to
  resolve ("object-space expression node TypedFilter is not substitutable yet (H2 vocabulary)"): a comprehension
  or quantifier over a literal list inside a projection over a class extent (`MATCH (n:A) RETURN n, [x IN [1, 2,
  3] WHERE x > 1 | x]`); the same lambda over rows (`extend` after a `project`) lowers. The island reaches it
  only when a literal-list comprehension stands beside a node value in a RETURN over a graph (no TCK case
  today: the TCK's literal-list comprehensions are over rows); the fix belongs to the resolver (W9), where the
  object-space substitution would carry `filter`, `map`, `exists` and `forAll` over a `TypedCollection` as it
  carries them over a navigation.
- **2026-10-17, W2 leg 5 — the resolver's two-step existence past a bridge-table step.** `A.all()->filter(a|
  $a.KNOWS.LIKES->isNotEmpty())` fails to lower (`Resolvers$UnfoldableRef: KNOWS_LIKES_a_dst_id`) whenever the
  second step is an association mapped over a bridge table (`@LIKES_a > @LIKES_b`); the same filter over a
  foreign-key step lowers (a `SELECT DISTINCT` subselect correlated per fanned row), and a one-step existence
  over a bridge lowers too (`tools`: `$S/pprobe/ab_bridge.pure`, `knows.pure` reproduce it). The seam is
  `NavExistsMaterial.register` (`filterTwoHop`) and `CorrelatedSubselects.compositeChainTarget`: the second
  hop's exists material correlates on a sibling joinslot column the fanned row does not expose, where it should
  pull the bridge into its own subselect and correlate on the fanned row's key, as the one-step form does. The
  translator routes around it (a second-step hop of a fanning graph is present on the rows, the study's §12.50),
  so no TCK case is held back; the resolver fix belongs to W9 (platform limits), with the depth-two limit.
- **2026-10-17, W11 — NaN and the infinities inside a Variant.** RESOLVED 2026-10-17 (§12.79: the database's own Variant on DuckDB, the judge reading its tokens; Postgres's division error by S60). `UNWIND [..., 0.0 / 0.0, ...]` (ReturnOrderBy1/[11],
  [12], WithOrderBy1/[21], [22]): DuckDB computes NaN and its JSON writes the bare token `NaN`, which the
  harness's JSON reader refuses; Postgres's float division raises and its jsonb holds no NaN. The island refuses
  a float divided by a zero literal inside a list or map by name (`NotANumber`). A faithful spelling would carry
  NaN as a marked Variant (the string `"NaN"` with a kind the key ranks after every number) on both lanes and
  teach the judge the mark; four TCK cases.
- **2026-10-17, W11 — a list of dates the database holds as a Variant, read back.** `WITH [date(...), ...] AS
  values ... [x IN values WHERE x < value]` (WithOrderBy1/[45]'s dates): the Variant spells a date as the
  oracle's text (`Date('1910-05-06')`, what the judge reads), so `toMany(@StrictDate)` over it fails at runtime;
  refused by name in `asCollection`. The spelling of temporals inside Variants (ISO texts the database reads back,
  the judge mapping them) is W5's; one TCK case, with the lists of times and of year-1 nanosecond values beside it.
- **2026-10-17, W12 leg 4 — a list of nodes or relationships as a grouping key.** RESOLVED 2026-10-17 (§12.71). `WITH tail(relationships(p)) AS
  relationships, COUNT(*) AS c` (Quantifier1-4/[9]) groups by a list of relationships, equal by identity in Neo4j;
  the island's Variant carries the relationships' JSON (type and properties), so two alike relationships would be
  one key, and the island refuses by name (S79). A faithful spelling would carry each list element's identity
  beside the Variant (`nodes(p)`'s and `relationships(p)`'s identities are known when the path is assembled) and
  group by those; four TCK cases, W11's Variant program.
- **2026-10-17, W12 leg 4 — an untyped variable-length pattern from a typed root over the unit-test model.**
  `MATCH (f:Firm)-[*3..3]->(x) RETURN x` over `CypherToPureTest`'s demo model refuses "no way of the model
  reads", where the typed `-[:employees|firm|hq*3..3]->` reads: the untyped unroll's `assign` finds no way from
  the demo model's associations (the TCK's untyped unrolls read over inferred models, Quantifier1-4/[8] pass). To
  be traced; holds back no TCK case today.
- **2026-10-17, W12 leg 2 — SKIP and LIMIT with a variable-free expression.** RESOLVED 2026-10-17 (§12.73: the rows numbered in the sort's order, the counts computed once over one row). `MATCH (n) WITH n SKIP
  toInteger(rand()*9)` (ReturnSkipLimit1/[3]): the MIR's `limit` and `offset` are constants (`SqlSelect.limit`,
  `ConstBounds.intOf`), so a row count the database computes has no carrier; one TCK case. The MIR would take an
  expression bound (every dialect renders `LIMIT <expr>` for a constant-foldable one); W9.
- **2026-10-17, W12 leg 2 — the time of a date-time in a named zone.** RESOLVED 2026-10-17 (§12.75: the zone's offset at the instant, which the database reads, carried as an offset the database reads). `time(datetime({..., timezone:
  'Europe/Stockholm'}))` (Temporal3/[3] #16-20): the zoned time carries its zone as a written detail (§12.11), and
  a named zone's offset at that date is the database's to read (`localIn` minus the instant); a zoned time whose
  offset is a value of the row needs the representation to carry it as a column. Five TCK cases; W8's temporal
  model.
- **2026-10-17, W12 leg 2 — `toString` of a list, a map, a node.** RESOLVED 2026-10-17 (§12.73). Neo4j 2026.09 answers texts of its own
  (TypeConversion4/[10], probed by `lite/tail`'s refused case): a list's elements' texts unquoted and
  comma-separated (`[1, a, [2, true]]`), a map `{a: 1, b: x}`, a node its labels `(:A)`, a relationship `[:T]`,
  a path `()-[:T]->()`. The TCK's five cases expect the error older Neo4j raised; the texts are a spelling the
  database can build from the JSON (W11's kind reading); recorded, not built.
- **2026-10-17, W2 leg 1 — a composite key as an identity.** `elementId` refuses a class whose key is several
  columns ("a composite key (n columns) as the identity of X is not supported yet"); the harness's tables have
  one `id` column, so no TCK case is held back; a user's composite key waits for its spelling (the columns' texts
  joined with a separator the key's values cannot contain).
- **2026-10-17, W6 — the resolver navigates an inherited association end against the subclass's table.** With
  `Class A_B extends A, B` mapped on its own table `n_a_b` (its inherited properties mapped there) and an
  association `R_T` declared for A over a join on `n_a` (`Join T(n_a.r_t = n_c.id)`), `g::A_B.all()->project(~[t:
  x|$x.T.name])` lowers the superclass's association mapping against the subclass's main table (`LEFT OUTER JOIN
  n_c AS t1 ON t0.r_t = t1.id` with `t0` the subclass's table: a binder error where the column is absent, silently
  wrong rows where a column of the name exists) instead of refusing the unmapped end at resolve time (AGENTS.md
  invariant 4: legend-engine finds no property mapping for the end in the subclass's set and errors; the
  `extends` mapping form would inherit it). Reproduced by `$S/probe2/FnProbe3.java` (DuckDB). The harness routes
  around it: a label carried among others is a marker class with no association of its own, so no inherited end
  is ever navigated; no TCK case is held back. The fix belongs to the resolver (W9), under its own guard: refuse
  an inherited association end whose mapping is not the class's set's, and read the `extends` mapping form.
