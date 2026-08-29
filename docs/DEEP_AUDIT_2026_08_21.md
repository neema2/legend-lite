# Deep adversarial audit — full compiler + outside-compiler compensations

Audited at `origin/main` = `7f04dce1` (merged as `c3b7c99b`). Baseline:
`mvn -o -pl core test` → **4200 tests, 0 failures, 16 skipped**. Every
defect below is invisible to that suite.

**Evidence rule: code and execution only.** Docs, comments and commit
messages were read to extract *claims*, never as proof. Every behavioural
line was produced by building this tree and running the query. Twelve
parallel sweeps; ~4,800 fuzz executions; both reference implementations
on this machine used as oracles.

**Two axes, kept separate**, per the standing question:
- **IN** — shortcuts inside the compiler.
- **AROUND** — machinery outside it doing the compiler's job.

---

## 0. The previous round's fixes: verified, and they shipped two regressions

Blockers 1–3 targeted my last audit's top findings. Both code Blockers
**closed their target class** — that deserves saying first:

| | before | at HEAD |
|---|---|---|
| `P.all().nick->at(0)` (null-drop) | `null` | `Al` ✓ |
| `size()` vs `toOne()` on same query | 2 vs 3 | agree ✓ |
| `[1,2]->slice(0,2)->toOne()` | `SCALAR([1,2])` | raises "size 2" ✓ |
| `if(...)->toOne()`, `range(1,5)->toOne()`, `zip->toOne()` | silent lists | all raise ✓ |

The `toOne` fix was a genuine class fix: the lane now comes from the
typed node, so shapes I never reported (`range`, `zip`) closed too.

### But both fixes introduced hard regressions

**R1 — `take()`/`limit()` + `toOne()` went from correct to a compile abort.**

```
[1,2]->take(1)->toOne()    was: SCALAR(1) ✓   now: MULTIPLICITY-STAMP INVARIANT VIOLATED
[1,2]->take(2)->toOne()    was: "size 2" ✓    now: same crash
[1,2]->limit(2)->toOne()   was: "size 2" ✓    now: same crash
[1,2]->slice(0,1)->toOne() => SCALAR(1)        (control — slice has an arm)
```

`CollectionLanes.valueLane` is a closed whitelist of typed node types
with `default -> false` (`:105`). It has arms for `TypedSlice`,
`TypedDrop`, `TypedSort`, `TypedFilter`, `TypedMap`, `TypedCast`,
`TypedFrom`, `TypedDistinct`, `TypedConcatenate`, `TypedIf`,
`TypedNativeCall` — and **none for `TypedLimit`**, which is what
`take`/`limit` produce. `grep -c TypedLimit CollectionLanes.java` → **0**.
The old predicate covered it.

**R2 — `map(p|$p.x->toOne())` over a nullable column now throws.**

```
P.all()->map(p|$p.nick->toOne())  => IllegalStateException:
                                     "NULL cell reached COLLECTION egress"
P.all().nick                      => COLLECTION[Bo, Ci]   ✓ control
```

Blocker 1 replaced the Java null-drop with a **wall** at
`Executor.java:305`, but the compiled `IS NOT NULL` only fires when the
cell stamp is `[0..1]`. A user `toOne()` stamps the cell `[1]`, so no
filter is emitted and the NULL hits the wall. `$p.dept.name` is filtered
and works; `$p.dept->toOne().name` is not and crashes — same data, same
meaning.

Second-order: that wall fires wherever the cell stamp is `[1]` and the
data is not — precisely the "stamps the compiler cannot back" population.
**Fixing that population makes this wall fire wider.**

**Why nothing caught either:** `CompactList`, the node Blocker 1
introduced, appears in **zero test files**. No core test asserts on
`IS NOT NULL`, `u_map__`, or `list_filter`. A change that rewrote the
emitted SQL of every optional-cell read moved no assertion.

**Blocker 3 rec #12 was declined, and the commit message reads as done.**
`ChannelBRelationTest` still contains `assertTrue(out.size() == 287)`
against a reference scope of 348. What was added is a longer message and
by-name pins on the two walled files, so a parser gap still surfaces as a
*passing* test. Recs #9, #10, #11, #13 were genuinely done.

---

## 1. RETRACTIONS — findings of mine that are now refuted

An audit that never retracts is not measuring anything. Settled against
primary sources this round:

- **`1 == 1.0` being `true`, string `indexOf` base, `substring` base** —
  the two reference lanes genuinely diverge, and legend-lite is faithful
  to the **relational** lane, which is its conformance target. My §4 lines
  from the previous audit should be struck. legend-engine's own
  `duckdbExtension.pure:279` carries the admission: *"pure uses 0-based
  indexing, duck db returns location with 1-based index … many user tests
  need to be fixed."*
- **`ResolveNavigationTest` "audit R3"** — refuted by the reference's own
  golden. `testAssociationToOne.pure:23-26` has an explicit user
  `->toOne()` over `firm : Firm[0..1]` compiling to a bare
  `left outer join`, no guard. And `testClassMappingFilterWithInnerJoin.pure:203-209`
  projects a `[0..1]` property over a join matching 4 rows per person and
  **asserts 16 duplicated rows as correct**. The TDS lane does not guard
  `[1]`, by design.
- **`mod` on negatives** — correct (−5 mod 2 = 1, −12 mod 5 = 3).
- **NULLS ordering, `first`/`last` window frames, `sum` over an empty
  group, pivot `__|__` handling, to-many fan-out double counting** — all
  reference-faithful or better; a sub-sweep downgraded its own findings
  after checking.

**What survives from that dispute:** `GraphEmission.java:2781`'s
`LIMIT 1` on a to-one nav leaf is wrong on *every* lane. The engine's
graph-fetch file contains **0** occurrences of `toRow`/`fromRow` (no row
cap is expressible) against **3** of `distinct = true`; its discipline is
*dedup, then hard-fail* (`graphFetchCommon.pure:163`). `LIMIT 1` silently
picks a winner in the case the engine treats as fatal.

**Methodological consequence:** divergences against *Pure's spec* are
largely inherited from the engine and are not legend-lite's to fix
unilaterally. The findings that survive scrutiny are the ones provable
**without an external oracle** — internal self-contradiction. Those are
ranked first below.

---

# PART I — hacking IN the compiler

## 2. Self-contradiction #1: a filter that GROWS the set

No oracle needed.

```
Firm.all()->size()                                  => 3
Firm.all()->filter(f|$f.emps.name!='zzz')->size()   => 5
Firm.all()->filter(f|$f.emps.name!='zzz')->project([fname])
                                    => Acme, Acme, Acme, Beta, Ghost
Firm.all()->filter(f|$f.emps.name=='Ann')->size()   => 2   (want 1)
```

Data: Acme employs Ann, Bob, Ann; Beta employs Cid; **Ghost employs
nobody**. Two defects at once:

1. The to-many LEFT JOIN is never collapsed back to one row per root, so
   `filter` multiplies the root by its child count. A set operation
   cannot add elements.
2. The null-tolerant `ELSE TRUE` arm returns **Ghost** for
   `emps.name != 'zzz'` — an empty collection is treated as satisfying
   the predicate.

The correct machinery is one call away in the same compiler:
`filter(f|$f.emps->exists(e|$e.name=='Ann'))->size()` returns **1**.
`exists`, `isEmpty` and `size()>1` all emit proper
`LEFT JOIN (SELECT DISTINCT …)` semi-joins. Only the bare comparison over
a to-many navigation misses that route — and it is the most natural way
to write the query.

## 3. Self-contradiction #2: `['x']` is not a collection

A one-element collection literal is lowered as its element
(`ValueCollections.c1Singleton:122-124`), so list operations bind against
a scalar:

```
['ACTIVE']->contains('TIV')      => true    ✗   substring match
['ACTIVE','X']->contains('TIV')  => false   ✓   control
['abc']->indexOf('b')            => 2       ✗   want -1
['abc','x']->indexOf('b')        => -1      ✓   control
['a']->indexOf('a')              => 1       ✗   want 0
```

**Adding an element to a list changes what `contains` means.** Membership
on a one-element string list silently becomes a substring search — and
"is this value in this small list" is ordinary code.

Six more operations are hard `SQLException` leaks where the two-element
form works: `[7]->take(1)`, `->drop(0)`, `->slice(0,1)`, `->contains(7)`,
`->exists(...)`, `->zip([8])` — all `Binder Error: ARRAY_SLICE can only
operate on LISTs`.

**The mechanism was introduced by the stamp program itself**, in
`0386d20c` *"singleton collection literals lower as their element; census
1,021 → 191."* The census only ever compared the *stamp* against the *SQL
shape*; after this change they agree perfectly (`['abc']` is `[1]`, its
SQL is a scalar). Consistent, and wrong. The invariant cannot see it by
construction, because the change made the two sides match by altering the
carrier's meaning.

Bounded: literal collections only. `[7,8]->filter(...)->take(1)` is fine.

## 4. Self-contradiction #3: `relation->size()` contradicts its own row count

```
project([p|$p.dept.dname],['d'])          => 4 rows: Eng, Eng, null, null
project([p|$p.dept.dname],['d'])->size()  => 2
project([p|$p.name, p|$p.dept.dname])->size() => 4
```

`RelationPredicates.java:59-70` emits `COUNT(<col>)` for single-column
projections and `COUNT(*)` otherwise. The NULLs come from the mapping's
own LEFT OUTER join, so any "how many rows does this join produce" count
is wrong whenever the far side is optional.

## 5. Other confirmed wrong answers

| # | shape | result | correct |
|---|---|---|---|
| a | `<<temporal.businessTemporal>>` (camelCase) | milestoning **silently off**, `SELECT … FROM T_FIRM` with no WHERE | one row per entity |
| b | `(INNER)` on a property-mapping join pointer | discarded; 4 model variants → **byte-identical SQL**; 50% over-return | INNER JOIN |
| c | `#TDS` cell containing `'` | row truncated, NULL-padded, column mult weakened to `[0..1]` | parse the cell |
| d | `checked()` over dirty data | `"defects":[]` | multiplicity defects per property |
| e | `sort()->distinct()` | unsorted (`2,3,1,4,5`) | `5,4,3,2,1` |
| f | `sort()->concatenate(...)` | **unparseable SQL** (ORDER BY inside a UNION arm) | wrap the arm |
| g | `graphFetch->serialize->take(n)` | **unparseable SQL** | — |
| h | `serialize` tree wider than the `graphFetch` tree | silently widens the query | intersect or refuse |
| i | `MultiGrainFilter` | treated as a plain Filter; wrong value on 1 of 3 rows | skipped on simple PK equi-join |
| j | view ungrouped column | silent `ANY_VALUE` — **worse than the reference**, which emits the bare column and lets the DB raise | compile error |
| k | `isDistinct()` (any input) | `ArrayIndexOutOfBoundsException` | `true`/`false` |
| l | `[]->map({v|$v})` | `MULTIPLICITY-STAMP INVARIANT VIOLATED` | `[]` |
| m | `1 / 0`, `0 / 0` | `Infinity`, `NaN` — values Pure's grammar cannot express | error |
| n | `9223372036854775807 + 1` | promotes past Int64; but `-` **raises** | consistent policy |
| o | `1.0d / 3.0d` | `0.3333333333333333` (double) | scaled Decimal |
| p | `[2,3,4]->times()` | `24.0`, typed INTEGER | `24` |
| q | `[1,2]->filter(x\|false)->head()->sum()` | `null` | `0` |

(k) root cause: `Scalars.java:2231` registers `isDistinct` with **no
arity filter**, routing Pure's 1-arg collection form into the binary SQL
`IS DISTINCT FROM`. `Aggregates.java:92` — ten lines away in a sibling
file — uses `nativeKeysAt("isDistinct", 1)`. The guard exists; it just
wasn't applied.

(m)/(n): the *inconsistency* is the finding. `rem(0)` raises while
`/0` returns Infinity; `-` overflows loudly while `+`/`*` promote.

## 5b. The type checker: variance is inverted

The only type-system finding that produces a **silently wrong value**:

```
function m::callN(f: Function<{Number[1]->String[1]}>[1]): String[1] { $f->eval(1.5) }
function m::relay(g: Function<{Integer[1]->String[1]}>[1]): String[1] { m::callN($g) }

m::relay({i: Integer[1] | ($i + 1)->toString()})   => SCALAR(2.5)
```

An `Integer[1]`-declared lambda parameter receiving `1.5`.

`InferenceKernel.java:197-207` calls `b.enterContravariant()` and passes
`contravariantSlot=true` — so the code *knows* the slot is contravariant
— then:

```java
unify(formalParam, af.params().get(i).type(), b);          // covariant order, unchanged
unifyMult(..., /* contravariantSlot */ true);              // flag only SKIPS the check
```

`enterContravariant()` makes bindings rigid; it never swaps the subtype
direction. The reference swaps it at `TypeMatch.java:510` by flipping
`superType`/`subType`. Consequences, both confirmed:

- **Wrong-accept**: the unsound direction is accepted (above), reachable
  through the ordinary `map` native, not just user functions.
- **Wrong-reject**: the sound direction is refused —
  `Employee[*]->map(Person-fn)` fails, which is the *useful* case.
- **Multiplicity in a function slot is not checked in either direction**:
  a `[1]` lambda parameter bound to a 3-element collection type-checks,
  then dies as `Binder Error: upper(VARCHAR[])`.

Adjacent, confirmed:

| shape | legend-lite | reference |
|---|---|---|
| `^m::Employee(name='a').salary` (missing required `[1]`) | `SCALAR(null)` | compile error, `NewValidator.java:132` |
| `^m::Box<Integer>` matched against `[Box<String>, Box<Integer>]` | takes the **String** arm, then re-types the payload | correct arm |
| `Class m::A extends m::B` / `B extends A` | compiles; first `isSubtype` miss → **StackOverflowError** | `C3LinearizationConflictException` |
| `Class D extends B, C` diamond where `C` overrides `A.p` | resolves `A`'s type (DFS) | resolves `C`'s (C3 MRO) |
| `Primitive Meters extends Integer` | erased; `Feet` flows into a `Meters` slot | distinct nominal type |

`ModelContext.isSubtype` (`:223-244`) recurses over `superClassFqns()`
with no visited set. `InferenceKernel.ancestorsOf:1318` *does* keep a
`seen` set — the guard exists, just not in the primitive everything else
calls.

**Two corrections to my earlier audits.** Class constraints are **not**
dropped at parse: they are parsed, name-resolved, compiled into synthetic
functions, type-checked, and evaluated in the graph-fetch `checked()`
envelope. Only `^new` enforcement is missing. And the reference checks
`cast()` at **runtime**, not compile time — so "no compile-time check"
was never the divergence; the divergence is that legend-lite checks
nowhere.

**Held under attack, worth stating:** multiplicity conformance is fully
correct and single-owner (the previous round's fix survived everything I
threw at it); the LUB is right across primitives, classes, enums and
dates; recursion and mutual recursion type-check correctly; `eval` arity
and argument typing are right; lambda *result* variance is correct; type
variables in two parameter positions genuinely unify.

**One usability gap worth its own line:** no type error carries a source
position. `TypeInferenceException` holds a phase and a string; the only
locator is the enclosing FQN, added with the self-aware comment
*"Positions stopgap … the expression-level [line:col] is the deferred big
lift."* Parse errors do carry `[line:col]`.

## 6. The frontend, never audited before

Coverage is **good**: 3,422 of 3,455 reference `.pure` files parse
(**99.0%**), no systematic element loss, error positions accurate across
comments, section boundaries and islands, and the relational column-type
set matches the engine walker across all 21 types. `native function` and
`$x['0']` are refused with byte-identical engine messages.

The problem is **leniency**, all CONFIRMED:

```
Class m::A extends m::A            => ACCEPTED; any property miss => StackOverflowError
Class m::A { x: String[1]; x: Integer[1]; }  => ACCEPTED (first wins; reorder flips the type)
Class m::A {...} twice             => ACCEPTED (last wins; first's properties vanish)
Enum m::E { A, A }                 => ACCEPTED
x: String[2..1]                    => ACCEPTED; findClass then throws a raw,
                                      unpositioned IllegalArgumentException
Join bad(T.C = GHOST.C)            => ACCEPTED — and Compiler.plan() succeeds
Filter f(T.GHOST = 'x')            => ACCEPTED
%2020-99-99                        => emits WHERE t0.D = DATE '2020-99-99'
```

Worst because quietest: **a duplicate property mapping silently takes the
last**, so `code: T.CODE, code: T.NAME` emits `SELECT t0.NAME AS c` — a
valid-looking result from the wrong column. And store cross-references
(joins, filters, views → tables/columns) are **never validated**; the
engine checks these at `PureModel` build.

Date validation exists (`PureDateLiteral.java:509-530`) and is wired at
`SpecParser.java:936` to `LEGEND_PLATFORM` **only** — off for the
product dialect.

Twelve silent-repair sites, including an **unbounded** skip of stray
top-level `)` (`ElementParser.java:536-539`) and top-level `^Type(...)`
instances parsed then **dropped with no diagnostic** (7 across 5
parseable corpus files).

---

# PART II — hacking AROUND the compiler

## 7. Names the compiler reads out of user-controlled strings

Three sites decide compiler behaviour by pattern-matching a string the
user chose. One is now confirmed user-reachable end to end:

```
project([p|$p.name],['nm'])        => Tabular, 2 rows           ✓
project([p|$p.name],['u_map__nm']) => 1 row: SCALAR(ann)        ✗ shape and column name lost
                                   => 2 rows: IllegalStateException
```

`ResultShape.java:49-52` tests `columns().get(0).name().startsWith(SYNTH_MAP_COL)`.
Siblings: `CarrierStrategies.java:137` (`!oc.name().contains("__|__")`)
and `Fold.java:84` (`"u_ord".equals(p.outputName())`).

## 8. Unguarded synthetic names produce wrong data

With a physical column named `AGG_0` — an ordinary ETL name — a
`groupBy`+`sum` emits `SUM(...) AS agg_0` beside a projected group key
`AGG_0`. Unquoted identifiers fold, so `t2.agg_0` binds to the **group
key**: the query returns the department id where the sum belongs.

`StoreResolver.java:2015` claims `"agg_" + ord++` with no collision scan.
Its siblings `_pk` and `_cj` (`CorrelatedSubselects.java:379`, `:243-252`)
**do** scan and bump. The pattern exists; it wasn't applied.

## 9. Raw-SQL text rewriting corrupts data

`RawSqlBoundary.h2ToDuckDb0` (`:151-186`) is a chain of `replaceAll` over
the whole statement with **no string-literal awareness**, reachable from
user Pure via `executeInDb`:

```
'CURRENT_TIMESTAMP()'   ->  'CURRENT_TIMESTAMP'
'create schema Foo'     ->  'Create schema if not exists Foo'
'count(*), 1'           ->  'count(*) AS "COUNT(*)", 1'
```

Silent INSERT corruption. Same file splits an INSERT column list on `,`
inside quoted identifiers; `RawSql.splitStatements` is comment-blind, so
`-- ; comment` splits a statement and half-executes it.

**Credit, verified:** ordinary string-literal escaping is sound — every
adversarial value round-tripped through real execution (`'`, `''`,
backslash, newline, `--`, `/* */`, `;`, `"`, `${x}`, `%`, `_`, 5000
chars, `café 中文 😀`); only a NUL byte breaks it. `LIKE` is not a value
surface (`contains` lowers to `strpos`), so no wildcard leakage. The
**execution** renderer's `ident` both doubles quotes and validates
pre-quoted names — it is the engine-text renderer (`EngineStyleH2:955`)
that skips both and can emit `as "a"b"`.

---

# PART III — the pattern

## 10. Guards are enforced at spellings; meaning moves off-spelling

Verified mechanically with `javap`:

```
EngineStyleH2.java:171 -> PlatformTypes.TDS_NULL_CELL
   javap | grep -c com/legend/compiler  =>  0    (but: ldc "TDSNull")
SnapshotEnvelope.java:133,139 -> resolver.AsorRef.SEG_LEN_WIDTH / .MARKER
   javap | grep -c com/legend/resolver  =>  0
```

Both are real cross-layer source dependencies. Because the targets are
`static final` constants, javac inlines them and emits **no bytecode
edge**, so `sqlLayerIsFullyStandalone` and `packageDependenciesAreAcyclic`
are green on live violations of themselves.

Same shape elsewhere: 11 `ThreadLocal` ambient channels cross layer walls
with zero imports and pass `noStaticMutableState` (whose regex is
`static (?!final )`); `Lowerer.java:3143` branches on one and emits
different MIR for the same query.

## 11. The whitelist replaced the sniff, and inherited its blind spot

Blocker 2 replaced a SQL-shape sniff with a typed-node whitelist. That is
the right direction. But the whitelist has a `default -> false` and is
missing `TypedLimit`, and **what caught the omission was
`StampCensus.listShaped`** — i.e. the checker and the rule are still
coupled. The coupling now produces a crash where it used to produce
silence. Any list-shaped SQL that `listShaped` *also* misses stays silent.

The durable fix is an **exhaustive** switch over the sealed typed-node
hierarchy, so javac refuses to compile when a new node type is added
without classifying it.

---

## 11b. The compiler is deterministic and thread-safe; the connection layer is not

**Measured, not asserted — and this is a strong result:**

| check | scale | result |
|---|---|---|
| `plan()` SQL byte-stability, one JVM | 25 shapes × 200 reps | 0 unstable |
| cross-JVM byte-stability | 8 fresh JVMs, `-Xint`, serial GC, 64m | all identical |
| thread safety, distinct models | 48 threads × 40 iters × 12 models | **0 / 3840** mismatches |
| cold-start `<clinit>` race | 32 threads first-touching, 12 JVMs | no deadlock, 1 result |
| exception-path contamination | 10 poison × 6 good queries | **0 / 60** |

Structurally: **zero non-final static fields** in 524 main files (verified);
every rule registry is populated only from `static {}` and read-only
after; **no unordered-collection iteration reaches SQL text** (103
`HashMap`/`HashSet` sites censused; SQL paths use `List`/`LinkedHashMap`/
`TreeMap`); no `Statement`/`ResultSet` leaks.

The defects are all in one place:

- **`ConnectionResolver.java:76`** keys a cache of **live JDBC
  connections** on the connection **FQN**. Two unrelated models that both
  name a connection `store::Conn` receive the identical `Connection`
  object, so one model reads another's tables.
- **`:96`** — every non-file H2 connection is
  `jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1`, **no key at all**: one JVM-wide
  database shared across all models and all connection names.
- **`:125-130`** — `CACHE.get` then `CACHE.put` is check-then-put, not
  atomic. On a cold key, **8 of 8** concurrent threads each opened their
  own database; for in-memory DuckDB that is a wrong answer (7 callers get
  an empty database), not merely a leak. `computeIfAbsent` fixes both.
- **There is no `Connection.close()` anywhere in main** (grep: 0). Live
  H2 sessions grow linearly: 201 after 201 calls, 401 after 401.
- `StatementExecutor.java:441` re-renders **outside** the `TextGoldens`
  scope, so an *identity* table rename changes the emitted engine text
  (`select "root".NAME` → `select "root".NAME as "u_map__name"`).
- `PostProcessBoundary` is a `ThreadLocal` map never cleared at
  `StatementExecutor.execute` entry, so a table-replacement can outlive
  its query. Consumer side confirmed by probe; producer side not reachable
  from the LITE text surface today, so the live blast radius is the
  harness, where JUnit reuses threads and test *order* decides the answer.

**And the closing instance of §10's pattern.** The sanctioned
content-addressed cache — `com.legend.cache.ContentStore` — has **zero
production callers** (referenced only by itself and `Hash.java`). The rule
meant to protect it, `ArchitectureTest.cachesAreFunneledToContentAddressedStore`,
matches on class **name** (`haveSimpleNameEndingWith("Cache"/"Store")`),
so `ConnectionResolver`'s `private static final Map CACHE` passes cleanly.
Its stated purpose is to stop "a name-keyed cache (engine's `planCache`
scar) sneaking in unreviewed"; that is exactly what happened, thirty lines
from where it is forbidden. The rule measures a spelling.

## 11c. Gates that cannot fail, and the never-audited modules

CI runs `-pl core` only, so `nlq`, `pct` and `parser-equivalence` are
never compiled there. `tools/allgates.sh` is the entire standing gate. It
runs GREEN in 780s — and two of its gates cannot report failure:

- **Gate 5 sweeps 2,575 corpus tests and asserts nothing.**
  `RelationalCorpusRunner.java:325` opens `if (Runner.H2_BACKEND)` and
  `:378` is `return;`. The first assertion in the file is at `:497`. Gate 5
  sets `-Drcorpus.backend=h2`, so it always returns first. The run logged
  `1362/2575 pass, 944 unsupported, 6 failed seeds` and exited 0. **1362
  could become 1 without moving the verdict.**
- **`allgates.sh:225` — the tree-poisoning tripwire prints FAILED and
  `exit 0`.** There is exactly **one** `exit 1` in the entire script, so
  the single branch that detects a poisoned certification is the one that
  reports success to an automated caller. One character.
- **`parser-equivalence` is RED at HEAD** — `Tests run: 47, Errors: 2`
  (a `System.getProperty` with no default → NPE; a test reading a file
  another test writes later). Neither class is in GATE8's 20-class
  allowlist, so the gate is green while the module is red.
- **`-Dcorpus.manifest.regen=1` silently disarms the corpus-drift gate** —
  `CorpusManifestTest.java:64-71` returns having asserted nothing, and
  reports a genuine **PASS**, not a skip, so neither the `skipped()`
  detector nor the rename-goes-red loop can see it.
- **`nlq`'s tests are run by no gate at all.** `grep -n nlq
  tools/allgates.sh` → nothing; CI runs `-pl core` only. Measured:
  `mvn -o -pl nlq -am test` → **223 tests, 213 passing, 10 skipped, 0
  failures** in ~6s. This is the inverse of the other rows here — real,
  cheap, *passing* coverage that nothing claims, so a regression in the
  module would be found by no automated check. Adding `nlq` to
  `allgates.sh` is the cheapest coverage win in the repo.

**`nlq/` — never audited, and it trusts the model three times:**

- `GeminiClient.java:76-83` never inspects `finishReason` and caps output
  at 4096 tokens (`:129`). A `MAX_TOKENS` response is returned as a
  complete query — and a query whose trailing `->filter(...)` was
  truncated **still compiles**, so `NlqService` reports `isValid: true`.
  A dropped filter returns the whole table as an authoritative answer.
- A hallucinated `rootClass` passes as valid; `extractSchema` returns a
  31-character empty schema (vs 4,564 for a real class) and the planner
  works from it blind.
- The query *plan* is never validated — literal reasoning prose passed as
  a plan yields `isValid: true`.

Credit where due: the generated **query** is genuinely compile-checked
(`NlqService.java:297`), and `#SQL{...}#` islands are hard-rejected at
type-check (`Typer.java:142`), so prompt injection cannot reach raw SQL.

**Server hardening:** `LegendHttpServer.java:404` is `setExecutor(null)`
— single-threaded — while an LLM call can block for ~11 minutes
(124s backoff + 6×90s timeouts) and `NlqService` makes up to 7. Also
`Access-Control-Allow-Origin: *` (`:322`), a line-based unbounded
`readBody` (`:327-337`), and caller-supplied paths concatenated into JDBC
URLs (`ConnectionResolver.java:72-86`).

**Corrections to earlier audits, from this sweep:**
- **GATE8 is the best-defended gate in the repo** — `roots_present()`
  fails *closed*, a rename-goes-red loop verifies all 20 classes actually
  ran, parity assertions are exact-zero. With roots present: 8,891
  sources, 6,489 oracle-accepted, **0 diff**.
- **The "774 rows / 0 diff / SUCCEEDS" claim is stale** — with both roots
  absent, `CorpusSweepTest` now **FAILS** on an explicit corpus floor.
- **The 9 `assumeTrue` sites are effectively dead** — with roots absent,
  `Skipped: 0` everywhere; the committed fixture tier keeps the corpus
  non-empty.
- **`G7_MAX_ERR=22` is not slack** — measured actual is exactly
  `348/1/22`, zero headroom.
- **`allgates.sh` has zero `|| true`, zero `set +e`, zero unawaited
  background jobs.** No verdict is lost through a pipe.
- `Json.java` has **4** reflection sites, not the 6 a prior audit claimed.

## 12. Scale of the evidence

| sweep | volume | outcome |
|---|---|---|
| Differential fuzz vs the reference's own executable spec tests | **1,892** harvested (expected, expression) pairs + **2,925** combinatorial checks = **4,817** executions | 1,122 MATCH, 704 honest "not implemented", **19 MISMATCH + 39 CRASH** |
| Frontend corpus parse | **3,455** reference `.pure` files | **3,422 parse (99.0%)**; ~11 of 33 failures are genuine grammar divergences |
| Relation/TDS operators | 165 probes with hand-computed answers | 13 honest refusals, 5 invalid-SQL, **7 silent wrong answers** |
| Mapping/store features | 14 models, seeded data, SQL oracles | most features solid; **6 confirmed wrong answers** |
| Type checker | 20 deliberate errors + variance/generics/C3 attacks | 19/20 rejected; **1 silent wrong value** (§5b) |

The *rate* of wrong answers is low. The trouble is that it is
concentrated, and the concentrations are severe.

Four sub-sweeps **downgraded their own findings** after checking the
reference (NULLS ordering, window frames, `sum` over empty, pivot
separator handling, fan-out double-counting, `mod`). That is the
discipline this round was run for.

---

# Recommended order

1. **`CollectionLanes`: add `TypedLimit`, and make the switch exhaustive**
   over the sealed hierarchy instead of `default -> false`. (R1 — a
   working query now aborts the compile.)
2. **Reconcile the null-drop wall with `[1]` stamps** (R2) — either emit
   the filter for `[1]` cells too, or downgrade the wall until the
   `[1]`-stamps-the-compiler-cannot-back population is fixed.
3. **Route bare to-many comparisons through the existing `exists` form**
   (§2). A filter must not change cardinality.
4. **Stop lowering `[x]` as `x`** (§3), or make every collection rule
   handle the collapsed carrier. Membership must not become substring
   matching.
5. **Arity-guard `isDistinct`** (§5k) — one-line, copy `Aggregates.java:92`.
6. **Frontend validation** (§6): reject cyclic inheritance, duplicate
   declarations, `[2..1]` at parse with a position, and unresolved store
   cross-references at model build. Turn on date validation for
   `LEGEND_LITE`.
7. **Stop reading behaviour out of user-controlled names** (§7): carry an
   explicit flag on the typed node instead of `startsWith("u_map__")`.
8. **Collision-scan `agg_N`** (§8) — copy the `_pk`/`_cj` pattern.
9. **Make `h2ToDuckDb0` literal-aware** (§9), or restrict `executeInDb`
   to statements it has parsed.
10. **`GraphEmission`'s `LIMIT 1` → `distinct` + raise** (§1), matching
    the engine's actual discipline.
11. **Un-pin the ChannelB relation denominator** (Blocker 3 #12) so the
    parser gap fails rather than reporting a smaller 100%.
12. **Ban `static final` cross-layer constant reads** and register the
    `ThreadLocal` channels (§10) — today they are invisible to all 32
    ArchUnit rules.
13. **`InferenceKernel.java:204-207`** — swap the subtype direction and
    enable the multiplicity check for function-parameter slots (§5b).
    One site; fixes a wrong-accept, a wrong-reject, and an unchecked
    multiplicity together.
14. **`ModelContext.isSubtype`** — add a visited set (§5b: an inheritance
    cycle is currently a `StackOverflowError`).
15. **`ConnectionResolver`** (§11b): `computeIfAbsent` for atomicity; key
    on model *content* (`Hash.ofUtf8`) not FQN; give the `default` H2 arm
    a per-database name; and close connections. `ContentStore` already
    does content-addressing and has no users.
16. **Change `ArchitectureTest` Invariant 3 from a class-NAME rule to a
    field-TYPE rule** — any `static` `Map`/`Set` outside `com.legend.cache`
    mutated after `<clinit>`. Today the rule cannot see the cache it
    exists to prevent.

---

## Addendum — three commits landed after this audit's base (`7f04dce1`)

Re-measured at `7c1db9ac`. Suite **4200 → 4206, 0 failures**. Full probe
battery re-run: **no new regressions**; everything previously fixed is
still fixed.

**`7cf086e4` — `VerdictWorld2ConsistencyTest` is BUILT.** This is the
guard chartered in `HOST_LOGIC_AUDIT_2026_08_20` as "the guard that keeps
it fixed" and flagged as unbuilt in three consecutive audits. It runs each
host-side semantic arm's computation through the full SQL pipeline and
asserts the two worlds agree, with an **explicit shrink-only divergence
register** — rows expected to agree fail on divergence, rows registered as
known divergences fail when they *start* agreeing. Its javadoc states it
would have caught §5 (`size()`=2 vs `toOne()`="size 3") and §6 (`decodeAny`
precision loss) on day one. Correct — both were exactly two-worlds
disagreements.

This is the only fix in the series that closes the **pattern** rather than
an instance.

**`382795b2` — three findings burned, verified by probe:**

| finding | before | at `7c1db9ac` |
|---|---|---|
| §3a `ExistsJoinForm` collapsing correlation keys | compared `DEPT_NAME` to a person's name; wrong rows | **FIXED** — emits `SELECT DISTINCT t1.NAME, t2.NAME AS NAME_1, …` and the ON references `t3.NAME_1`; correct row |
| §3b `joinStrings` 3-arg dropping union order | `[aa,bb,zz,yy]` | **FIXED** — `[zz,yy,aa,bb]`, matching the 1-arg form |
| `cast()` unchecked | everything silent | **PARTIAL** |

The `cast()` boundary, mapped:

```
1->cast(@Boolean)          => Cast exception ✓      true->cast(@Integer)  => Cast exception ✓
'a'->cast(@Boolean)        => Cast exception ✓      true->cast(@String)   => Cast exception ✓
1->cast(@Date)             => Cast exception ✓      1->cast(@Number)      => 1  ✓ (a real upcast)
1->cast(@String)           => SCALAR(1) STRING  ✗    1->cast(@Float)      => 1.0  ✗
1.5->cast(@Integer)        => SCALAR(2)         ✗    %2024-01-01->cast(@String) => "2024-01-01" ✗
```

Unrelated-type pairs now raise; **convertible-primitive pairs still
silently convert**. That residue is the declared "pure's cast never
converts" divergence at `Lowerer.java:3156-3158`, so it is a known
boundary rather than an oversight — but it is still a silent wrong value
on the most common shape (`Integer → String`).

**`7c1db9ac` — §7b adjudicated with engine witnesses, no code change.**
The correct outcome: the reference goldens refute the finding (see §1).

**What these commits did NOT touch** — and what therefore sits at the top
of the queue:

1. **R1** — `[1,2]->take(1)->toOne()` still aborts the compile
   (`CollectionLanes` still has no `TypedLimit` arm; `grep -c` → 0).
2. **R2** — `map(p|$p.x->toOne())` over a nullable column still throws
   `NULL cell reached COLLECTION egress`.
3. §2 filter-grows-the-set (3 → 5), §3 `['ACTIVE']->contains('TIV')`,
   §5b inverted function-parameter variance, `isDistinct()`, `1/0` →
   `Infinity`, `[]->map({v|$v})`.

**Net: helped.** Two confirmed wrong answers burned, one partially, and
the structural guard finally built. The only cost is bookkeeping — three
rows of this report's open list were stale within hours of writing it.

---

## Appendix — the standing pattern, restated

Across four audits the failure mode has been consistent, and it is not
carelessness. This codebase has unusual discipline: `@SuppressWarnings`
appears 4 times in 522 files, `deadPrivateMethodsOnlyShrink` is pinned at
0, `GuardCoverage` makes guards assert their own scope, Invariant 7 has
zero exceptions, the lowering wall-to-silence ratio is ~7:1, and the
corpus scoreboard's published numbers are exact.

The pattern is narrower than "hacking":

1. **A guard is chartered in prose and not built.** `VerdictWorld2ConsistencyTest`
   still does not exist; the `conformToOne` leg was "RETIRED UNBUILT"; the
   union arm-factory leg was never built; the H2 `RAISE_ERROR` mechanism
   the docs describe has no source.
2. **A fix satisfies its reproduction, and the class stays open one level
   away.** `size()`/`toOne()` agree at the statement root and disagree one
   call in; the empty-identity fork closed for `and`/`or`/`joinStrings`
   and not for the arithmetic reductions.
3. **Meaning moves off-spelling and the guard silently stops covering it**
   (§10): `static final` constants, `ThreadLocal`s, `Object`/`String`
   erasure, and — new this round — a `default -> false` in a whitelist
   that replaced a predicate which had covered the missing case.
4. **An optimization is validated by the instrument it was built to
   satisfy.** §3 is the sharpest example: lowering `[x]` as `x` took the
   stamp census from 1,021 to 191 *and* made `['ACTIVE']->contains('TIV')`
   return true, because the census compares the stamp to the SQL shape and
   the change made them agree by altering the carrier's meaning.

The durable countermeasure for (4) is the same as for (3): make the
checker's evidence procedure **independent** of the rule it polices. An
exhaustive switch over a sealed hierarchy does this for free — javac
becomes the referee, and it cannot share a blind spot with the code it
checks.
