# Audit 20b — WRONG-ROWS / VACUOUS PASSES (range 1f5bb41d^..HEAD)

Scope: B2a/B2b result frame, groupBy stage 2, null-consistency (task #62),
auto-map slices 1+2a. Method: code read of the resolver/executor/harness
seams + live probes against `core/target/classes` (DuckDB in-memory; probe
source `/Users/neema/.claude/jobs/9693939d/tmp/Audit20b.java`) + corpus
censuses over `core_relational` (python function-body sweeps).

## Verdict

**No corpus test in this range passes with wrong rows, and no newly-passing
test is vacuous.** The two engineered stand-ins hold their documented
loud-failure contract at every presentation seam I could reach: a
root-position `->toOne()` over an N-row chain surfaces **all N rows**
(probed: 4-row TABULAR, 4-element GRAPH array — the value compare then
fails), and the envelope `size()=1` literal is backed by an eager run that
provably exists on every frame-producing path. The null-consistency
emissions match the engine's own golden SQL on every counterexample I
constructed (both-null `!=`, not(in), double negation). What the audit DID
find: one test family whose pairwise assertions became **self-comparing**
(the engine's in-memory reference leg is spliced back into our own SQL —
its partition assertions still bite, so the pass is real but weaker), one
**order-dependent-vacuous class** (`first()`/`head()` = LIMIT 1 with no
ORDER BY) that currently has zero passing corpus witnesses, and one
**latent wrong-rows generator** (auto-map TABULAR presentation keeps NULL
"no-element" rows that pure semantics excludes, and the new `!=` null arm
actively admits them) that no passing test currently exercises. Counts:
0 wrong-rows passes, 0 fully vacuous passes, 2 medium latent hazards,
1 medium semi-vacuous family, 4 low/info findings.

---

## Findings

### F1 — toOne pass-through: loud as documented, but the typed lie is one consumer away (INFO / verified safe today)

`StoreResolver.isClassToOne` consumers:
- chain collector drops the node: `core/src/main/java/com/legend/resolver/StoreResolver.java:1820-1823`
- head-routing arm (slice 2a) widens what reaches it: `StoreResolver.java:210-211`
- doc contract: `StoreResolver.java:690-698`

Probe (4 persons match `age > 20`):

- `...->toOne().name` → TABULAR with **4 rows** and column multiplicity
  `[1..1]`;
- `...->toOne()` (root) → GRAPH single-row `json_group_array` with **4
  objects** (`arrayWrap` is unconditionally true at the graph root,
  `StoreResolver.java:2210`, `GraphEmission.java:270`).

Presentation seams verified loud:
- harness `evalScalar` returns the whole list when size≠1
  (`core/src/main/java/com/legend/harness/TestBody.java:1026-1035`) — a
  scalar expectation against 4 values fails;
- `assertJsonStringsEqual` bridges only **singleton** arrays to an object
  expectation (`TestBody.java:631-638`) — a 4-array fails.

Newly-passing tests through this arm (`tests/datatype`
`testSimpleTypeMapping*`, shape `let row = $r.values->filter(e|$e.tinyInt == 1)->toOne()`)
are **genuinely-safe**: the filter selects a unique key in the seeded data,
and if it ever matched 2 rows every subsequent `$row.<prop>` read would
present 2 values and fail loud. Classification: genuinely-safe; none
order-dependent (nothing truncates).

Residual risk worth naming: the resolved root **info() says multiplicity
[1] while the relation carries N rows**. Today no consumer trusts that
multiplicity (relation roots classify TABULAR), but
`Executor.execute` SCALAR arm reads exactly one row and **never checks for
a second** (`core/src/main/java/com/legend/exec/Executor.java:67-70`). Any
future reclassification that lets a store chain reach SCALAR shape turns
this stand-in from fails-loud into silently-picks-first. See F8.

### F2 — first()/head() = LIMIT 1 with no ORDER BY: the order-dependent-vacuous class (MEDIUM, latent — zero passing witnesses)

`StoreResolver.java:1812-1818` rewrites `first()/head()` to `TypedLimit 1`;
probe SQL confirms `LIMIT 1` over an unordered subselect. Engine emits the
same shape, but engine expectations encode **H2's** incidental row order and
we execute on **DuckDB** — a pass of a first()-headed multi-row chain is
order-luck, and the harness ORDER POLICY (multiset fallback,
`TestBody.java:1115-1137`) cannot help a single-row result.

Corpus census: root-position `->first()` inside an execute lambda appears
exactly once (`milestoning/tests/testBusinessDateMilestoning.pure:582`,
inside a project column), and that test is currently **ERROR**
(`docs/RELATIONAL_CORPUS.md:4884`) — so no test currently passes through
this emission. Flagged as the class to re-check whenever a first()-headed
test flips to PASS: it should be accompanied by a sort or a provably-≤1
filter.

### F3 — auto-map TABULAR presentation keeps NULL "no-element" rows; the new `!=` null arm actively admits them (MEDIUM, latent wrong-rows generator)

The auto-map folds (`foldScalarHop`/`foldHopMap`/`foldScalarHopFilter`,
`StoreResolver.java:576-643`) produce one-column relation roots →
`ResultShape` TABULAR → `Executor.tabular` keeps NULL cells, and the
harness flattens them into the compared values
(`TestBody.java:919-923`). The null-dropping COLLECTION arm whose comment
promises "`Person.all().middleName` … contributes nothing, not null"
(`Executor.java:71-83`) is **bypassed** for every auto-map chain.

Probe evidence (firm "Empty" has no employees; Bob's nick is NULL):

| query | ours | pure semantics |
|---|---|---|
| `Firm.all().employees.name` | `[Ann, Bob, Cat, null]` | `[Ann, Bob, Cat]` |
| `Person.all().nick` | `[annie, null, cat, null]` | `[annie, cat]` |
| `Firm.all().employees.nick->filter(n\|$n != 'annie')` | `[null, cat, null]` | `[cat]` |

The third row is the compounding case: `NullSemantics.notEqualNullArms`
(`core/src/main/java/com/legend/lowering/NullSemantics.java:27-48`)
emits `NICK <> 'annie' OR NICK IS NULL`, which admits both the left-join
miss row (no element exists) and the real-person-empty-nick row (element
does not exist either — `.nick` of Bob contributes nothing in pure).

Why nothing fails today: the newly-passing `testMap.pure` tests
(`testAssociationToManyAutoMap`, `testAssociationDeep`,
`testFilterOnSimpleTypeProperty{,Deep,Eq}`) run over seed data with **no
missing links** (all firms have employees, all persons have addresses) and
their filters use `=`/`in`, which drop NULL rows in SQL. Their
`assertSize`/`assertSameElements` assertions are real and would fail loudly
on any extra NULL element. So: no wrong-rows pass exists — but the first
corpus/user query combining an auto-map hop with a missing link (or a
nullable leaf plus `!=`) returns rows pure semantics forbids.

### F4 — foldScalarHopFilter's re-typed lambda carries the wrong function type (LOW)

`StoreResolver.java:626-643`: the emitted `TypedFilter` reuses
`f.predicate().info()` as the new lambda's info — a
`String[1] → Boolean` function type — while the rebuilt body reads a
relation-row variable (`r: RelationType`). The relation lowering resolves
predicate references by column name against the source select
(`Lowerer.java:970-1004`) and never consults the lambda's declared param
type, so rows are correct today (probe: `WHERE t1.NAME <> 'Ann' OR …`
folded into the projection select, correct rows). The `TypedFilter` info
reuses `rel.info()`, consistent with `FilterChecker`'s source-info
convention (`FilterChecker.java:19`). Latent typing lie only; no behavior
divergence found.

### F5 — testConsistencyWithNulls: the pure-reference leg is self-comparing (MEDIUM, semi-vacuous by construction)

Engine intent (`relational/functions/tests/testFilters.pure:241`,
`testIn.pure:176`): compare a **SQL-side** filter against the same filter
applied **in memory** to `.values` — SQL three-valued logic vs pure total
equality is the entire point. Our result-frame splice
(`StatementExecutor.spliceHook`, `StatementExecutor.java:351-420`) folds
`.values->filter(...)` back into the same typed chain, so both legs of
`assertEquals($notEqResult->size(), $notEqResult2->size())` execute **our
SQL** — those assertions are tautological in our harness.

Not fully vacuous: the partition assertions
(`eq->size() + notEq->size() == all->size()`) compare three *different*
queries and genuinely pin the null arms — deleting the `OR IS NULL` arm
flips them to FAIL (that is exactly the ERROR→FAIL progression that
motivated f67b12b8). Verdict: PASS is meaningful but strictly weaker than
the engine's test; the in-memory reference semantics are untested by
construction of the splice. Worth a scoreboard footnote, not a fix.

### F6 — null-arm counterexample sweep: emissions match the engine on all three constructed cases (INFO)

1. **col-vs-col `!=` with both NULL** — probe `nick != nick` → `[]`;
   `nick != name` keeps single-null rows. Matches the engine's
   processNotEqual arms, and the engine's own golden SQL for the `==` side
   (`testFilters.pure:377`: `… or (STREET is null and STREET is null)`)
   confirms both-null compares EQUAL — i.e. pure `equal([],[])` is TRUE.
   (The audit brief's premise `eq([],[]) = false` is contradicted by the
   engine's golden SQL; our emission follows the engine.)
2. **not(in)** — emitted as `NOT coalesce(X IN (...), FALSE)`, not the
   `OR X IS NULL` shape in the commit message: the dedicated
   `SqlFn.IN` branch of the not-rule (`Scalars.java`, f67b12b8 hunk) is
   **dead code** because `in` lowers pre-wrapped in COALESCE. Semantically
   equivalent (NULL operand → kept on the NOT side); probe confirms Bob/Dan
   (null nick) are admitted. Dead branch worth deleting, no row impact.
3. **not(not(equal))** — probe `!(!($p.nick == 'annie'))` →
   `NOT (NICK <> 'annie' OR NICK IS NULL)` → exactly the `annie` row;
   pure double negation agrees.

Honest known gap (not a silent pass): col-col `==` lacks the both-null-EQUAL
arm (engine processEqualFromFilter), so
`equal::testConsistencyWithNullsInColumnToColumnComparison` FAILs — correct
scoreboard state, documented follow-up in f67b12b8.

Theoretical divergence, no corpus witness: `x != <null literal>` would emit
`x <> NULL OR x IS NULL` (matches only null rows) while pure `x != []` is
true for **non-empty** x — inverted. Corpus spells this `isEmpty()`; flag
for `NullSemantics.isSqlLiteral`'s `NullLit` arm if a witness ever appears.

### F7 — envelope `size()=1`: correct and eager-backed, but only for the two-statement spelling (LOW inconsistency, no vacuous witness)

The literal-1 arm (`StatementExecutor.java:365-375`) fires only for a bare
frame **variable**. Verified structurally that every frame behind it ran
eagerly: let-position and result-position `execute()` both call
`buildFrame(…, eager=true)` (`StatementExecutor.java:99-101, 146-150`);
`aliasFrame` (`:306-342`) only aliases already-built frames; the sole
`eager=false` path is the inline `.values` splice (`:456`) whose chain is
by definition executed as part of the surrounding statement. A broken
pipeline therefore throws at (or before) the let on every path — the
size-1 literal can never mask a query that does not run.

Inconsistency: the same envelope read spelled inline —
`let tds = execute(...).values->at(0); … $tds->size()` (aliasFrame misses:
the walk bottoms at the execute call, not a variable → plain let) or
`$r.values->size()` — misses the arm and computes **row count** via
`relation::size`, where the engine answer is the envelope's 1
(`values: TabularDataSet[1]`). Divergent-but-loud when rowcount≠1;
a coincidental pass when rowcount=1 (e.g. the
`testFilterProject` idiom in `testAssociationMappingInheritance.pure:62-66`
— currently ERROR for unrelated mapping reasons, `RELATIONAL_CORPUS.md:5366`).
Corpus census for the mandated vacuity ("only assertion is the size-1
idiom"): a function-body sweep over all `test.Test` functions found **0**
tests with a relation-rooted alias + size assert and no `.rows` read — every
envelope-size assert travels with real row assertions.

### F8 — MapChecker's forced `[0..*]` is protective; the residual hazard is Executor's SCALAR arm (INFO)

`MapChecker.java:24-28` retypes any map over a `RelationType` source to
`[0..*]`. Without it, a row map over a spliced `Relation[1]` frame would
type `[1]` → `ResultShape.SCALAR` → `Executor.java:67-70` reads the first
row and **silently ignores the rest** — i.e. the change prevents a real
vacuous-pass channel. Cost: map-as-VALUE over a `Relation[1]` (engine: fn
applied once to the one TDS) is no longer typable as such — corpus census
of `values->map(` found only class-collection and `.rows` uses, zero
value-semantics witnesses. Recommend (follow-up, out of audit scope): make
the SCALAR arm loud on a second row — it converts every future
multiplicity lie from silent wrong-rows into an error.

### Cross-checks with no findings

- **groupBy stage 2** newly-passing tests
  (`tests/mapping/groupBy::testGroupByMappingProjectWithJoin{,AndTableFilter}`,
  `testGroupByMappingWithFilterOnAggregateWithJoin`) all carry exact-grid
  `toCSV`/`assertSameElements` row assertions that pin the aggregate values
  (300/8) — a pre-aggregation join (the classic double-count bug) cannot
  pass them; no vacuity.
- **classMappingFilterWithInnerJoin** +12 and the auto-map +9: all verified
  to assert concrete row values/sizes (spot-checked sources).
- Newly-FAIL entries added by the range (39→46) are honest fails, not
  masked passes (e.g. `testSimpleTypeMappingNulls` expected-[]-got-null
  wire-compare policy, per 4859a84f).
