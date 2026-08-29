# Audit: the verdict-in-DB arc and the decoupled PCT lane

Audited at `1a4b0d12` (merge of `ccbd7df2`). Baseline `mvn -o -pl core test`
= **4255 tests, 0 failures, 16 skipped**. Evidence: code and execution
only; docs and commit messages treated as claims to verify.

---

# PART 0 — THE HEADLINE

**This is the first wave in this series that comes back clean.** Both
regressions the previous wave shipped are fixed, and several findings I
had logged as open are fixed too. The verdict-in-DB design is real and
working at 99.8%.

The defects below are concentrated in three places: a canon rule that
contradicts its own spec, a third equality lane that never received the
tightening, and two escape hatches in the PCT diff that excuse rows they
should not.

---

# PART 1 — REGRESSION BATTERY: CLEAN, AND BETTER

Re-ran the full standing probe battery. **Both prior regressions fixed:**

```
[1,2]->take(1)->toOne()     => SCALAR(1)           was: STAMP INVARIANT VIOLATED
map(p|$p.nick->toOne())     => COLLECTION[Bo, Ci]  was: "NULL cell reached COLLECTION egress"
```

**Also fixed since the last audit:**

| finding | before | now |
|---|---|---|
| **Inverted function-param variance** | `relay({i:Integer[1]\|…})` returned **2.5** into an `Integer[1]` slot | rejected: *"expected Integer, got Number"* |
| **Function-param multiplicity unchecked** | `[1]` param bound to 3 elements | rejected: *"multiplicity [*] is not compatible with [1]"* |
| `isDistinct()` | `ArrayIndexOutOfBoundsException` on every input | correct `true`/`false` |
| Six singleton-literal ops | `Binder Error: ARRAY_SLICE…` | `take`/`drop`/`slice`/`contains`/`exists`/`zip` all work |
| `%2023-02-29` | DuckDB conversion error at execution | positioned parse error `[1:3] invalid day for 2023-2: 29` |

The variance fix matters most — it was the type checker's only silent
wrong *value*.

**One asymmetry.** The singleton-literal class was fixed for the LOUD
failures and not the SILENT one:

```
[7]->take(1)                => [7]     ✓ fixed
['ACTIVE']->contains('TIV') => true    ✗ still wrong — substring match
['abc']->indexOf('b')       => 2       ✗ still wrong — want -1
```

Backwards from a risk standpoint: the six binder errors announced
themselves; `contains` returning true for a non-member ships bad data
quietly.

**Still open, unchanged:** `filter` grows a set (3→5); `[]->map({v|$v})`
trips the stamp invariant; `1/0` → `Infinity`; `times()` → Float; `^new`
omitting a required `[1]` → null; `Box<Integer>` takes the `Box<String>`
match arm; `cast()` partial.

---

# PART 2 — THE VERDICT-IN-DB ARC

## 2.1 What works — measured

Across all five PCT suites (1,118 tests), one process:

```
sql-verdict  agree=1645  disagree=0  declined=3  ulp-policy=0
host-byte    agree=1622  disagree=0  residue=26
```

**1,645 of 1,648 verdicts (99.8%) decided in the database, zero
dual-verdict disagreements.** The `13 → 3` decline burn is confirmed. The
design — host lattice demoted to a *parallel referee* that can never
rescue — is real, and `ArchitectureTest.hostVerdictIsReachableOnlyFromTheVerdictSeam`
is an actual ArchUnit rule. Adversarial values did not break it: 20k-char
strings, unicode, embedded quotes, `', '` inside strings vs list framing,
3-deep nesting, `1e300`/`4.9e-324`.

`ulp-policy=0` across the entire corpus: **the 2-ULP tolerance did zero
work.** That is hard evidence for retiring it (register V8/X6).

Their `VERDICT_RULE_AUDIT` derivation from `EqualityUtilities` was
verified arm-by-arm and is **correct**. X1/X2/X4 were the right fixes;
`PureAsserts.equalScalar` is now engine-exact for scalars; X5 keyed
instance equality is right.

## 2.2 The canon contradicts its own spec on temporals

`CanonicalRenderSql.temporalCanon` javadoc (`:627`) states *"trailing
subsecond zeros stripped"*, and `:642` does it:

```java
SqlExpr stripped = stripDot(stripTrailingZeros(t), "");
```

So `%…T00:00:00.100` and `%…T00:00:00.1` render identically, and `.000`
strips to a bare dot which `stripDot` removes — identical to no subsecond
at all. This contradicts **three of their own documents**:

- `CANONICAL_FORM_SPEC.md:46` — *"subsecond precision preserved as
  written (`.000` ≠ `.0` ≠ none)"*
- spec §3 — *"distinct subsecond precisions are DISTINCT pure values →
  precision-preserving render byte-differs. OK."*
- `VERDICT_RULE_AUDIT` **DERIVED** row — *"Temporal precision-sensitive
  record equality | AbstractPureDate.equals (components + exact subsecond
  string)"*

A sibling sweep reports four assertEquals pairs passing on this. **Scope
bound:** `assertEquals` does not resolve outside the PCT platform model
(even `assertEquals(1,1)` fails with "unknown function"), so the blast
radius is *tests that can pass when they should fail*, not user-facing
wrong answers.

## 2.3 The canon is not injective across kinds — the kind gate is what makes it sound

Measured directly through `CanonicalForm.render`:

```
Long 8       vs BigDecimal 8    canon: 8    vs 8     EQUAL   host=false
String '8'   vs Long 8          canon: 8    vs 8     EQUAL   host=false
BigDecimal 1.10 vs 1.1          canon: 1.10 vs 1.1   DIFFER  host=false
Long 8       vs Double 8.0      canon: 8    vs 8.0   DIFFER  host=false
NaN / +Inf                      canon: Residue[non-finite-float]
LocalDateTime (any)             canon: Residue[unmodeled-kind:LocalDateTime]
```

Three consequences:

1. The bare canon would return EQUAL where the lattice returns false. The
   byte channel is sound **only because the kind gate declines those pairs
   first** — the canon is not carrying the guarantee on its own.
2. **Decimal canon is scale-PRESERVING**, so X2 landed — and
   `CANONICAL_FORM_SPEC.md` §2/§3, which still specify "SCALE-NORMALIZED"
   and "integral Decimal renders WITHOUT `.0`", are **stale against
   shipped code**. Those rules were chosen specifically to make a grant
   byte-decidable that X1 has since deleted. The spec says changes need a
   witness; this one was not revised.
3. The **host** canon walls non-finites correctly but has **no
   `LocalDateTime` arm at all** — so "the canon is TOTAL" is true only of
   the DB half. The 26 residue rows are the host half's gap.

## 2.4 A third equality lane never received the tightening

X1–X4 tightened the host lattice and the byte canon's kind gate. The
**in-SQL `equal()` relation** still carries every deleted grant — confirmed
by direct execution:

```
assert(equal(1.10D, 1.1D))  => true
assert(equal(1, 1.0))       => true
assert(equal('1', 1))       => true
```

`assertEquals` is intercepted at the statement root; `assert(equal(...))`
is not. **A corpus test written the second way bypasses the entire verdict
program.**

Note this is partly *by design*: `EqualityWorldsConformanceTest` pins the
`1 == 1.0` host/SQL split with the reason *"SQL numeric coercion —
engine-relational parity"*, and the engine genuinely diverges between its
interpreted and relational lanes. The problem is not the split; it is that
the byte-verdict channel is a **third** position that agrees with neither
in places, and nothing pins it against the other two.

Deeper: **"engine-exact" is under-specified, because the engine has three
lanes** — interpreted, compiled, relational — which differ materially (the
compiled lane falls back to `toString().equals()` and gives keyless
instances identity rather than `false`). Legend-lite pinned itself to the
interpreted lane and nothing records that choice.

## 2.5 One expression, two answers, one program

`InstanceEquality.equality()` returns `null` at `:105-107` when the layout
lacks `__id` — and `__id` is added **only on the verdict lanes**. The
cross-class *"different classifiers → FALSE"* arm sits at `:115-117`,
after that early return, so it is unreachable from the product query lane.

Confirmed by execution, with `<<equality.Key>> id` on the class:

```
let x = ^K(id=1,other='x'); let y = ^K(id=1,other='y');
assert($x == $y, 'A');            => succeeds   (certifies them equal)
if($x == $y, |'EQ', |'NE');       => NE         (same expression, false)
```

Both statements run. The assert passes and the identical expression
evaluates false one statement later.

## 2.6 The eviction number moved the wrong way

| file | stripped | pin | movement |
|---|---|---|---|
| `AssertVerdicts.java` | **829** | 829 | **398 → 829 (+108%)**, at ceiling |
| `PureAsserts.java` | 269 | 311 | 42 lines of unclaimed slack |
| `CanonicalRenderSql.java` | 491 | **none** | in `lowering`, outside the ledger |

The arc moved the *render* into SQL and added 431 lines of host routing to
do it. Each bump is honestly justified in the ledger. But the largest new
file in the arc carries no size pin at all.

---

# PART 3 — THE DECOUPLED PCT LANE

## 3.1 Channel B is genuinely decoupled — the strongest positive finding

The only `org.finos` occurrence in `pct/.../channelb/` is the package
declaration. Every functional import is `com.legend.*`, JDK, or JUnit.

| concern | owner |
|---|---|
| model loading | **legend-lite** — `Compiler.parseSources(…, LEGEND_PLATFORM)` |
| discovery | **legend-lite** — its own `FunctionDefinition.stereotypes()` |
| adapter elimination | **legend-lite** — AST rewrite over `protocol.spec.*` |
| assertions | the reference's own Pure `assert*`, compiled by lite and lowered to SQL |
| `CoreInstance` | **never** — no `org.finos.legend.pure.m4` type appears |

Remaining reference dependency is **data, not code**. And the F15 shadow
parser — `ExecuteLegendLiteQuery`'s six source-extraction regexes plus
`reEscapeStringLiterals` — is **Channel A's alone**; Channel B's three
regex sites all scrape oracle files, never Pure source.

## 3.2 The relation wall burn is real — and it corrects my earlier finding

`over.pure` holds exactly **68** PCT tests. 287 + 68 = 355. All 68 appear
as individual PASS rows, verified by name.

**Mutation-tested, 68/68 live, zero vacuous.** Corrupting all 65 `#TDS`
expected literals killed exactly 65; a targeted operator killed the
remaining 3 (`assertError`/`assertTdsEquivalent` bodies the first operator
could not reach). The earlier "one test survives full mutation" lead was
an operator gap, not a dead test.

So Channel B had been running **68 fewer tests than Channel A**, silently.
The burn closed a hidden gap — the opposite of inflation. **My earlier
"self-selected denominator" finding is corrected: the growth is honest.**

## 3.3 But the denominator's stated explanation is wrong

The register says the ~7-test gap is "relation qualifier config filters".
It is **version skew**: `pct/pom.xml:24` pins engine `4.133.0`, whose jar
holds 348 relation tests; the working tree holds 355. All 7 extras are in
`composition.pure` and do not exist in 4.133.0. No qualifier filter exists
in the reference collector.

And **Channel B double-counts one test.** `collection::tests::get::testGet`
is discovered twice — once from the platform (`<<PCT.test>>`) and once
from the engine tree (a non-PCT `<<test.Test>>`). `ChannelB.java:161-166`
keys source attribution by FQN; the roots collide and last-writer-wins.

**Corrected: B's honest universe is 1,117, not 1,118** — still 8 more than
A's 1,109, but all 8 are tests A's pinned jars do not contain. The
advantage is **corpus freshness, not capability**; on the corpus A can
see, the channels are at parity. That is genuinely valuable — B tracks
reference HEAD while A is frozen at a release — but it is not what "MORE
than Channel A's 1,109" implies.

Discovery is pinned `==` in all five suites, so a parse gap **cannot**
silently shrink the universe any more.

## 3.4 ENGINE-FRONTIER excuses by name while the reason sits unread

`ChannelBDiff.java:35` scans the manifest with
`"test"\s*:\s*"(meta::[a-zA-Z:_0-9]+?)_Function_` — **name only**. The
same JSON objects carry an `expectedError` field giving the reference's
actual failure reason, and it is never read.

All 11 excused rows have mismatched reasons. Three are category
inversions:

| test | oracle `expectedError` | what legend-lite does |
|---|---|---|
| `testMixedSortNoComparator` | `Not supported: Number` | **executes and returns a wrong answer** |
| `testRemoveDuplicates…MixedTypes` | `Any is not managed yet!` | **wrong result ordering** |
| `testPersonToString` | `Assert failed` (engine ran it, got it wrong) | throws `NotImplementedException` |

So the bucket is laundering at least two rows where **legend-lite computes
a wrong answer** into "the reference fails this too." Exposure is
concentrated in Essential (148-name manifest); Relation's manifest has
**1** entry, so its 100% is not propped up by this.

## 3.5 A decline disables the dual-verdict alarm — and one passes silently

The tunnel is `catch (SQLException | RuntimeException)` at
`StatementExecutor.java:2718`, with `rider.decline(...)` at `:2734`,
`:2761`, `:2779`. On a caught canon failure it discards the DB verdict and
re-executes bare — **the host lattice decides alone**.

The three declines:

| test | reason | verdict |
|---|---|---|
| `sort::testMixedSortNoComparator` | `mixed-kind-collection` | FAIL |
| `map::testMapRelationshipFromManyToMany` | `struct_extract(STRUCT(…)[], …)` Binder Error | ERROR |
| **`lang::tests::letFn::testLetWithParam`** | `Malformed JSON at byte 0 … Input: "echo"` | **PASS** |

`testLetWithParam` is a clean PASS on a host-only verdict after a canon
JSON bug — the canon tried to JSON-parse the literal string `echo`.

The structural point is worse than the instance:
`CanonicalDivergence.probeSqlVerdict` is the only writer of
`SQL_DISAGREE` and requires **both** verdicts. So
`sqlDisagreeCount() == 0` is **structurally silent about exactly the rows
most likely to diverge**. And two of the three "declines" are SQL binder
errors, which their own `CANONICAL_FORM_SPEC.md` §4b calls *"a bug in the
wrap, not a new category"*.

The tunnel comment says *"counted, never a rescue"* — but a declined row
whose host verdict passes is, in effect, a rescue.

## 3.6 Nine rows assert agreement with a channel that never ran them

`ChannelBDiff.java:47-72`: for a B-only row, `aFails` is false by absence,
so a PASS increments `agreePass`. **9 rows affected** (7 relation
composition + `testEqualEmpty` + the `testGet` duplicate). AGREE-PASS is
inflated by 9 — 1,071 reported, 1,062 corroborated.

Fail-safe in the direction that matters: a B-only *failure* routes to
`wireBug` → `trueWireBug`, which is pinned `== 0`. The register's proposed
A-ABSENT bucket is the right fix.

---

# PART 4 — WHAT TO DO

**Correctness, in order:**

1. **Stop stripping subsecond zeros** in `CanonicalRenderSql.temporalCanon`
   (`:642`). Smallest change, largest correctness win, and it restores
   agreement with three of their own documents.
2. **Give the in-SQL `equal()` relation the X1–X4 treatment**, or pin the
   three lanes against each other so a divergence is a census row rather
   than a silent bypass (§2.4).
3. **Wall non-finite floats in the DB canon**, as the host half already
   does and the spec already promises.
4. **Fix the two binder-error declines** — they are wrap bugs, not
   boundary (§3.5).
5. **Move `InstanceEquality`'s classifier-mismatch arm above the
   `hasIdentityField` early return** so the product lane stops disagreeing
   with the assert lane (§2.5).

**Evidence integrity:**

6. **Compare `expectedError` in `ChannelBDiff`**, or at minimum split
   "lite errored" from "lite answered wrongly". Two wrong answers are
   currently excused as engine parity.
7. **Add the A-ABSENT bucket** so 9 rows stop claiming agreement with a
   channel that never ran them.
8. **Prefix-qualify the discovery key** in `ChannelB.java:161-166` to kill
   the FQN collision double-count.
9. **Make a decline fail the census rather than pass silently** — at
   minimum, a declined row must not report PASS without a second signal.

**Bookkeeping:**

10. **Revise `CANONICAL_FORM_SPEC.md` §2/§3** — stale against shipped
    code, on a doc that requires a witness for changes.
11. **Put `CanonicalRenderSql` (491 lines) on the eval ledger.**
12. **Correct the register**: the relation gap is version skew, not
    qualifier filtering; the honest universe is 1,117; frontier measures
    11, not 12.
13. **Retire the 2-ULP policy** — `ulp-policy=0` across the whole corpus
    is the evidence V8/X6 was waiting for.
