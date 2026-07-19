# Audit 22a — Typing + Inlining Correctness (ee28e683..f4510b93 + working-tree self-catch)

**Verdict.** The working-tree self-catch is REAL and NECESSARY — rebuilt with the committed
(f4510b93) `UserCallInliner`, a non-strict lambda over an empty `[0..1]` higher-order map
manufactures a value (probed: 4×`CONST` where pure yields empty); the fixed tree walls that
path loudly. But the fix closes only the literal `[0..1]` route: the SAME non-strict-over-empty
hazard survives through three adjacent β-reduce routes, two of them probed silent-wrong-value —
(1) a `[1]`-declared param fed an effectively-`[0..1]` actual (engine-convention acceptance)
still β-reduces, (2) a zero-arg DERIVED property read over a `[0..1]` receiver β-inlines its
body instead of auto-mapping (f4510b93's auto-map arm is `isMany()` only — real pure auto-maps
everything ≠ `[1]`), and (3) `match` statically dispatches a `[0..1]` input to a `[1]` branch
where real pure dispatches on runtime count. Independently, collection-valued `map` over a
VALUE collection does not flatten (pre-existing, but newly reachable through 822eb7ff's
higher-order inlining), and the legacy 2-arg `isDistinct(l,r)` IS reachable as an aggregate
reduce via 7f90adbe's name-family marker and silently renders the group-isDistinct SQL.
d1dd7ca6 (overload scoring) and b42a2019 (class-space agg map widening + first() collapse) are
clean under probe and inspection. Counts: **3 HIGH, 2 MED, 3 LOW** (two HIGH and one MED are
pre-existing behaviors surfaced by this range's new reachability, marked below).

Probe harness: `/Users/neema/.claude/jobs/9693939d/tmp/Audit22a.java`, `Audit22aE.java`
(runs the committed pre-fix `UserCallInliner` on a shadow classpath), `Audit22aF.java`.
Model: `Person(name, age)`, optional `employer: Firm[0..1]` (person "Dan" has none),
derived props `constD(){'K'}`, `nick(){'N'+$this.legal}`, `workers(){$this.employees}`,
`greet(){'Hi '+$this.name}`, `nameIf(pfx){...}`, plus `f::hof01/hof1/use/use1/useBad`.

---

## Item 1 — the self-catch (higher-order map β-reduce) and the non-strict census

### 1.1 Fix verified: pre-fix manufactured values; fixed tree is loud (fix CONFIRMED)
`f::hof01(v:String[0..1], func:...) { $v->map($func) }` over `$p.employer.legal`, lambda `x|'CONST'`:

- **committed f4510b93** (`upper <= 1` β-reduce): Dan → `CONST` — silent wrong value (pure: map
  over empty = empty).
- **working tree** (`lower==1 && upper==1` only): the `[0..1]` path rebuilds `TypedMap`, which
  dies LOUDLY downstream — `NotImplementedException: object-space expression node TypedMap is
  not substitutable yet (H2 vocabulary)` inside a project lambda; in pure-scalar space it hits
  DuckDB's `list_transform` binder error (`Invalid LIST argument`) — loud both ways, never a
  silent row. Guard-mult reading also safe for mult-VARs (non-`Bounded` falls to the rebuild).

### 1.2 HIGH — residual hole: `[1]`-declared param, effectively-`[0..1]` actual still β-reduces
`UserCallInliner.java:408` guards on `c.args().get(0).info().multiplicity()` — the
**pre-substitution** (callee-body, declared) multiplicity. `unifyMult` (InferenceKernel:546,
"engine convention") accepts a `[0..1]` actual into a `[1]` formal, so
`f::hof1(v:String[1], func)` called with `$p.employer.legal` β-reduces on the declared `[1]`
and the non-strict body manufactures: **probed — all 4 rows `CONST`, including Dan** (real pure:
runtime multiplicity error, never a value; map-over-empty auto-map semantics says empty).
Checking the POST-substitution `args.get(0).info()` instead would route this to the same loud
wall as 1.1. Caveat: the engine's own compile-time convention accepts this call shape too, and
its inlining may emit the same constant — engine parity unverified; against REAL pure this is a
manufactured value.

### 1.3 Census of the other β-reduce sites
- **`reduceEval` (eval over `[0..1]`) — CLEAN.** `eval` is APPLICATION (`eval<T,V|m,n>(func, param:T[n]):V[m]`,
  whole-collection binding, not per-element auto-map), so β-substitution is exactly pure's call
  semantics. The `[0..1]`-into-`[1]` erasure of 1.2 applies to ALL user calls (inlineCall has no
  mult guard) but that is the codebase's documented toOne-erasure trade, not a map-specific bug.
- **`TypedMatch` β-redex — MED (pre-existing).** `MatchChecker` dispatches STATICALLY by type,
  and "to-one input: any branch multiplicity accepts" (MatchChecker ~:98) — a `[0..1]` input
  binds the FIRST type-accepting branch even when empty at runtime. Probed:
  `$p.employer.legal->match([s:String[1]|'has '+$s, s:String[0..1]|'none'])` gives Dan `NULL`
  (picked the `[1]` branch, null-propagated) where real pure's runtime count dispatch gives
  `'none'`. Silent wrong value whenever branches discriminate on multiplicity ([1] vs [0..1]/[0])
  and the input can be empty. Not introduced by this range; in-scope as the census item.
- **`StoreResolver.substituteParam` — LOW (latent).** Guarded for >1 param reads of a non-row-
  rooted source (the decorrelation wall, audit 21b F6), but a mapper body with ZERO reads of its
  param substitutes unconditionally (StoreResolver:857) — a class-result mapper that ignores its
  param would manufacture over an empty source. No natural corpus spelling found; noted only.

### 1.4 Executor.java working-tree self-catch (outside the five items, inspected)
`variantRoot` gating of the JSON-node cell decode is correctly scoped: exact
`PlatformTypes.isVariant` on the root — a Variant result's contract is the JSON text; decode
stays available for non-variant casts (`cast(@Float)` recovery). No finding.

## Item 2 — d1dd7ca6 overload scoring (Function-carrier interior result mult): CLEAN

- **(a) mult-VAR formal stays permissive** — both new arms check
  `ff.result().multiplicity() instanceof Multiplicity.Bounded` before rejecting; a VAR formal
  result (fold's `{T[1],V[m]->V[m]}` etc.) skips the check. A VAR **actual** result is also
  accepted (`paramMultScore` tightness path; `Var.isMany()` is false by the documented decision).
- **(b) arity rejection** — matches real pure (FunctionType conformance requires equal param
  counts; no corpus spelling of arity-mismatched function coercion found). Probed:
  `f::useBad(func:Function<{Integer[1],Integer[1]->Integer[1]}>[1])` body `[1,2,3]->map($func)`
  is LOUD ("no overload structurally matches"). `Function<Any>`-style formals (interior not a
  FunctionType) skip both checks — permissive, correct.
- **(c) selection census** — the change is REJECTION-ONLY on candidates whose interior result
  mult cannot hold the actual's: it can only move a call from "wrong winner → unification death
  (loud)" to "right winner", never boost a wrong candidate. Probed the target family:
  `f::use(func:Function<{Integer[1]->Integer[*]}>[1])` selects map's `{T[1]->V[*]}` overload
  (was: tighter-VALUE `[0..1]` overload then loud death); `f::use1` (`->Integer[1]`) still
  resolves. Lambda literals score as `null` slots in `selectByPresentArgs` and reach
  `resolveOverload` fully typed — their inferred `[1]`/`[0..1]` results pass the `[0..1]`-interior
  formals (tightness ≥ 0), so no literal-lambda selection changes. FAIL-ledger byte-identity at
  the commit corroborates corpus-neutrality.

## Item 3 — f4510b93 derived auto-map

- **(a) capture — CLEAN.** The synthesized `map(receiver, _am0|_am0.prop)` puts the receiver
  OUTSIDE the lambda; the body reads only the param, which shadows any user `_am0` (Env binds
  param after copying scope; UserCallInliner's query-level lambda path α-renames under a
  non-empty env). Probed: `let _am0 = 'zzz'; ...` and `let _am0 = m::Firm.all(); $_am0.employees.greet`
  both produce correct rows.
- **(b) chained derived — COMPOSES.** `Firm.all().workers.greet` (both derived, receiver
  to-many) recurses into sibling `_am0` lambdas (never nested in one body) and returns correct
  rows.
- **(c) parameterized qualifier over to-many — LOUD.** `Firm.all().employees.nameIf('A')` dies
  with `in call to 'm::Person$prop$nameIf', argument 1: expected at most one value, got many ([*])`
  (Typer ~:348 arm routes to the body fn; `[*]` cannot unify into `this:[1]`). Never silent.
  (Real pure auto-maps parameterized qualifier calls too — a functionality gap, but an honest wall.)
- **HIGH — `[0..1]` receivers are NOT auto-mapped: non-strict derived bodies manufacture.**
  The arm fires only on `isMany()` (upper > 1 or unbounded); a `[0..1]` receiver falls to the
  β-inline route (`applyGeneric(bodyFn, receiver)` — `unifyMult` accepts `[0..1]` into
  `this:[1]`), and `UserCallInliner` splices the body over the possibly-empty receiver. Probed:
  `$p.employer.constD` (body `{'K'}` — non-strict in `this`) gives **Dan `'K'`** where real pure
  (map.pure grammarDoc: auto-map "when ... multiplicity different from [1]" — includes `[0..1]`)
  gives empty → `NULL`. Strict bodies (`nick(){'N'+$this.legal}`) ride null propagation
  correctly (Dan `NULL`). Same family as the item-1 self-catch, one layer up: fix is to auto-map
  `[0..1]` receivers too (or wall non-strict bodies). Pre-existing route, but this commit is the
  one that decided the auto-map boundary and quoted the grammarDoc; engine-parity caveat as in 1.2.
- 1-arg `joinStrings` + `STRING_AGG` empty-separator guard: signature matches
  stringExtension.pure:253; the explicit `''` prevents DuckDB's silent comma default. Clean.

## Item 4 — b42a2019 class-space agg map K[*] + first() collapse: CLEAN

- The widening lives in the two CLASS-space bridge signatures only; the RELATION-space
  `AggColSpec` stays `{T[1]->K[0..1]}` and `typeAggColSpec` (Typer:922) validates each map
  lambda against the FORMAL taken from the CHOSEN overload's registered signature, with
  `typeLambda` enforcing result mult via `unifyMult` (Typer:843) — no shared code path leaks the
  wider acceptance. Probed: relation-space `groupBy(~[leg], ~[m2 : x|[$x.age, $x.age] : y|$y->sum()])`
  is LOUD ("expected at most one value, got many ([2])"). Class-space acceptance is
  corpus-verified by the commit's +6 (AggToMany family).
- `values->first()` collapse: both `aliasFrame` and `spliceHook` (StatementExecutor:307/:380)
  collapse ONLY when the frame/spliced chain is relation-rooted (`relationRooted()` /
  `RelationType` root); class/scalar roots keep the `first()` native call over the spliced
  chain — a REAL selection (LIMIT-1 downstream). Correct by inspection; the envelope-holds-one-TDS
  guard on `at(k>0)` is preserved.

## Item 5 — 7f90adbe distinct aggregates

- **COUNT/AVG/SUM over `distinct($y)` — CORRECT.** Probed SQL:
  `COUNT(DISTINCT t0.AGE), AVG(DISTINCT t0.AGE), SUM(DISTINCT t0.AGE)`; rows correct against
  duplicated ages (ACME 25,25 → 1 / 25.0 / 25). `isDistinct()` renders
  `COUNT(DISTINCT x) = COUNT(x)` (engine testGroupByIsDistinct emission); rows correct
  (dup group → false). Bare-group-variable DISTINCT stays a loud wall (Lowerer:880).
- **MED — the legacy 2-arg `isDistinct(left,right)` IS reachable as an aggregate reduce.**
  `Aggregates.family("__IS_DISTINCT__", "isDistinct")` registers EVERY overload key at that name,
  including `IS_DISTINCT__ANY_1__ANY_1`; a reduce lambda `y|isDistinct(1, 2)` type-checks
  (body ignores `$y`), resolves the 2-arg overload, maps to `__IS_DISTINCT__`, and the literal
  args land in `extra` — which the `__IS_DISTINCT__` arm silently DROPS (Lowerer:906). Probed:
  renders `COUNT(DISTINCT t0.AGE) = COUNT(t0.AGE)` and returns `false` for the dup-age group
  where pure's `isDistinct(1,2)` is constantly `true`. Contrived spelling, but a silent wrong
  value; fix: register only the `T[*]` overload's key for the marker (exact-signature
  identification, per the registered-signature tenet) or reject non-empty `extra` in the arm.

## Cross-cutting (found via item-2 probes; pre-existing)

- **HIGH (pre-existing, newly reachable) — collection-valued `map` over a VALUE collection does
  not flatten.** `[1,2,3]->map(x|[$x, 10*$x])` returns `[[1,10],[2,20],[3,30]]` (3 nested lists)
  where pure's `map` (`{T[1]->V[*]} → V[*]`) yields the FLAT 6-element collection. The scalar
  arm (Lowerer:2256) emits bare `LIST_TRANSFORM` with no flatten, while the sibling arms already
  handle it (relation-root :216 UNNEST, scalar-subquery :2429 `LIST_FLATTEN` via
  `isCollectionMapper`). Identical on pre-fix and working trees (not introduced by this range),
  but 822eb7ff's higher-order inlining makes it reachable through `$coll->map($func)` in user
  fns (probed via `f::use`). Wrong VALUES at a collection root — the cardinal sin, silent.

## Findings index

| # | Sev | What | Where |
|---|-----|------|-------|
| 1 | HIGH | `[1]`-declared param + effectively-`[0..1]` actual still β-reduces non-strict map lambda (guard reads pre-substitution mult) | UserCallInliner.java:408 |
| 2 | HIGH | Derived read over `[0..1]` receiver β-inlines instead of auto-mapping; non-strict body manufactures (`constD` → 'K' for empty employer) | Typer.java:1229 (isMany-only arm) |
| 3 | HIGH* | Collection-valued map over VALUE collection not flattened (nested lists); newly reachable via higher-order inlining | Lowerer.java:2256 |
| 4 | MED* | `match` static dispatch sends possibly-empty `[0..1]` input to a `[1]` branch (pure dispatches on runtime count) | MatchChecker.java:~98 + UserCallInliner match β-redex |
| 5 | MED | Legacy 2-arg `isDistinct` reachable as agg reduce via name-family marker; extras silently dropped → wrong boolean | Aggregates.java family + Lowerer.java:906 |
| 6 | LOW | Self-catch turns previously-CORRECT strict-lambda `[0..1]` higher-order map (and scalar-space present value) into loud walls — honest regressions | UserCallInliner.java:417 → resolver/lowerer walls |
| 7 | LOW | `substituteParam` zero-param-read mapper bodies substitute over a possibly-empty source unconditionally | StoreResolver.java:857 |
| 8 | LOW | Item-2 notes: VAR-actual function results score permissively; `Function<Any>` formals skip interior checks (both correct-permissive; documented here for the record) | InferenceKernel.java:850-898 |

\* pre-existing behavior, surfaced/censused by this audit; not introduced in ee28e683..f4510b93.

Probes are reproducible: `cd /Users/neema/.claude/jobs/9693939d/tmp && CP="$(cat cp.txt):/Users/neema/legend/legend-lite/core/target/classes" && java -cp "$CP:." Audit22a` (also `Audit22aE` with `pre22/classes` prepended for the pre-fix comparison, `Audit22aF` for match).
