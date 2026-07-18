# Audit 20a — Overfit / Hardcoding, range 1f5bb41d^..HEAD

Auditor: 20a (overfit/hardcoding). Range: B2a/B2b Result surface, groupBy stage 2,
null-consistency, core-bridge, auto-map slices 1+2a, scoreboard commits.
Ground truth: /Users/neema/legend/legend-engine and /Users/neema/legend/legend-pure
(local checkouts), primarily `core_relational/relational/sqlQueryToString/dbExtension.pure`,
`pureToSQLQuery/pureToSQLQuery.pure`, `platform/pure/grammar/functions/boolean/inequality/greaterThan.pure`,
and `core/pure/corefunctions/stringExtension.pure`.

## Verdict

One confirmed overfit, and it is the range's headline feature: the not-rule's
per-function COALESCE set in `Scalars.java` is keyed at the wrong layer. The real
engine has **no** null-compensation under `not` for comparisons at all — its
`processNot` (dbExtension.pure:950) special-cases only `equal`/`in` and emits bare
`not` otherwise; the partition behavior the corpus pins comes instead from the
**pure `[0..1]` overloads themselves** (greaterThan.pure et al. in legend-pure;
contains/startsWith/endsWith in engine stringExtension.pure), whose
`$x->isNotEmpty() && ...` bodies get inlined and reach SQL as `X is not null and X > 30`
(pinned verbatim at engine testFilters.pure `testGreaterThanWithOptionalProperty`) —
compensation at the *comparison site*, keyed on *operand multiplicity*, active in
every context. Lite's set reproduces the corpus pins exactly but silently diverges
wherever no test looks: negation over a *composed* predicate (`!(a->startsWith(x) || b > y)`)
drops NULL rows that both pure and the engine keep, an un-negated `[0..1]` comparison
in value position yields SQL NULL where pure yields `false`, and `contains` is
covered only by the accident that it lowers to `STRPOS > 0` (`SqlFn.GREATER`).
The rest of the range holds up well: `notEqualNullArms` faithfully mirrors
`processNotEqual`'s literal-ness arms (two LOW node-classification nits); the
StoreResolver auto-map folds are exact-FQN, engine-shaped, and loud at every
narrow edge (one documented `toOne` divergence, one latent capture risk); the
ExecFrame envelope rules are grounded in Result.values-holds-one-TDS but the
recognizer set is the corpus read-idiom list verbatim, leaving unpinned envelope
spellings with inconsistent chain semantics; the groupBy stage-2 `k<i>__<base>`
naming is an invented but purely internal convention, not engine mimicry, and
every mismatch path is loud.

**Counts: HIGH 1, MED 2, LOW 4.**

---

## F1 — HIGH — CONFIRMED-OVERFIT: not-rule COALESCE set is pin-calibrated, wrong keying layer

`core/src/main/java/com/legend/lowering/Scalars.java:149-177` (set at 167-175);
comparisons lower bare at `Scalars.java:108-120`.

What lite does: under `not(...)`, if the direct child call's lowered `SqlFn` is in
`{LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, STARTS_WITH, ENDS_WITH}`, wrap it in
`COALESCE(x, false)`; otherwise emit bare `NOT`. `not(equal)`/`not(in)` get the
engine's arms (correct, see F2).

Engine ground truth:
- `processNot` (dbExtension.pure:950-964): `equal` → `processNotEqual`; `in` →
  `processNotIn`; `and`/`or`/`not` → `not (...)`; **everything else** → bare
  `'not ' + maybeWrapAsBooleanOperation(...)`, where the wrap only adds `= true`
  on DBs without boolean literals (dbExtension.pure:777-785). No COALESCE exists
  anywhere in this path.
- The null-partition behavior the corpus's `testConsistencyWithNulls` family pins
  comes from real pure: `greaterThan(String[0..1], String[1])` etc. are TOTAL
  concrete functions (`$left->isNotEmpty() && ...`,
  legend-pure `.../boolean/inequality/greaterThan.pure`; same for lessThan,
  greaterThanEqual, lessThanEqual over Number/Date/String/Boolean), and
  `contains`/`startsWith`/`endsWith` have `String[0..1]` total overloads in
  legend-engine `core/pure/corefunctions/stringExtension.pure:20-34`. The engine's
  relational map (pureToSQLQuery.pure:10735-10746) registers only the `[1],[1]`
  variants; the `[0..1]` overloads are routed by inlining their bodies, so the SQL
  carries `X is not null and X > 30` — pinned at
  `core_relational/relational/functions/tests/projection/testFilters.pure:41`.
  `matches` has NO `[0..1]` overload (native, `String[1]` only) — bare `not` for
  matches is therefore engine-grounded, not a pin accommodation.

Where lite is silently wrong (no corpus pin):
1. **Composed negations**: `!($a.street->startsWith('X') || $a.age > 30)` — the
   direct child of `not` is `OR`, not in the set → bare `NOT(... OR ...)` → NULL
   rows dropped. Engine/pure keep them (inner compensation survives composition).
2. **Value-position comparisons**: an un-negated `[0..1]` comparison projected as
   a boolean column (or compared with `== false`) yields SQL NULL in lite; pure
   and the engine yield `false`. Coincides only in WHERE context.
3. **Fragile accidental coverage**: `contains` (String, to-one) lowers to
   `STRPOS > 0` (`Scalars.java:1685-1707`) and is caught by the set only because
   that emits `SqlFn.GREATER`. The comment at Scalars.java:160-166 claims the
   family includes contains, but the set has no CONTAINS entry — any future
   change of the contains lowering (e.g. to LIKE) silently reopens the hole.
4. Conversely the set over-applies COALESCE to any GREATER/LESS emitted from
   non-comparison sources under `not` — semantically harmless (NULL≈false under
   negation is exactly the total semantics), noted for completeness.

Principled rule: emit the engine's compensation **at the comparison site** when an
operand is `[0..1]` — `X IS NOT NULL AND <cmp>` (emission, per the
conform-by-emission rule), for exactly the functions that have `[0..1]` total
overloads in real pure/engine-core (4 comparisons + contains/startsWith/endsWith);
delete the SqlFn set from the not-rule and keep `processNot`'s equal/in arms plus
bare default. Note `equal` composed under `or` inside `not` drops NULL rows in the
ENGINE too (no `[0..1]` overload for equal; `isEqualsFromFilter` is filter-top-level
only) — do not "fix" beyond the engine.

## F2 — LOW — CLEAN core, two node-classification nits: notEqualNullArms literal keying

`core/src/main/java/com/legend/lowering/NullSemantics.java:27-55`.

The three arms (both-literal bare; one-sided `OR col IS NULL`; both-non-literal
double-arm) mirror `processNotEqual` (dbExtension.pure:969-987) exactly, including
the corpus's computed-side case (`($a.street->toOne() + ' st') != 'x st'` → call
vs literal → `OR left is null`, same branch the engine takes). Two divergences:

1. `isSqlLiteral` (NullSemantics.java:50-55) omits `SqlExpr.TimestampLit`
   (declared at `core/src/main/java/com/legend/sql/SqlExpr.java:58`). A datetime
   literal therefore classifies non-literal and takes the double-arm branch.
   Row-equivalent (the extra arms degenerate when one side cannot be NULL), so
   SQL-text noise only — but it is an accidental omission, not a decision.
2. `NullLit` counts as a literal; in the engine `SQLNull` is its own
   RelationalOperationElement, NOT a `Literal`, so the engine's branch choice
   differs on the (likely unreachable) `x != <null literal>` shape. Both engine
   and lite diverge from pure there in different directions; flagged as
   theoretical only.

The engine's `FreeMarkerOperationHolder` (query-parameter) keying has no lite
counterpart because lite has no parameter placeholder node reaching this path —
parameters arrive pre-substituted as literals, which lands in the same branch
semantics. Not a divergence today; will become one if a Param node is ever added.

## F3 — MED — DEFENSIBLE-DIVERGENCE (documented): toOne over class chains is a pass-through

`core/src/main/java/com/legend/resolver/StoreResolver.java:690-698` (isClassToOne),
consumed via `isObjectSpace` (657-679) and the head arm at 210-211.

The engine raises on `toOne` of N≠1; lite passes all N rows through and relies on
the downstream value compare to fail loudly. The Javadoc says exactly this
("documented, weaker-but-never-silent stand-in"), and in the harness a wrong N
can only fail a test, never pass one. Outside a comparing consumer, however,
the divergence is silent (N rows flow where the engine would error). Kept MED
because it is a semantic contract (multiplicity enforcement) traded away, but
documented and fail-safe in the corpus context. Everything else in the auto-map
machinery audits clean: exact-FQN identification throughout (AT/FIRST/HEAD/SORT/
CONCAT/COMPARE constants, StoreResolver.java:700-707), `at` requires a literal
index (681-688, falls loud otherwise), `sort` requires the bare two-param
`compare` shape (722-752, falls loud otherwise — engine's processSort is likewise
shape-restricted, pureToSQLQuery.pure:3355-3419), `classExtentCount` (536-572)
projects a constant and counts — row-equivalent to the engine's `select count(*)`,
`foldScalarHopFilter` (626-643) refuses non-one-column folds loudly, and
`hopChainOf` (521-531) is a general structural walk, not a corpus-spelling match.

## F4 — LOW — latent capture risk in the hop-fold lambda composition

`core/src/main/java/com/legend/resolver/StoreResolver.java:576-621` (foldScalarHop,
foldHopMap) + `substituteParam` (648-655).

The folds always name the composed lambda's parameter `"p"` and β-substitute the
mapper's own parameter via the inliner's let reduction. If a mapper body ever
references an *outer* variable also named `p` (shadow-adjacent shape), the
composed read captures it. Not corpus-triggered, not a pin accommodation —
recorded as a latent correctness edge, not overfit.

## F5 — MED — CONFIRMED-OVERFIT (partial): the ExecFrame envelope-splice set is the corpus read-idiom list

`core/src/main/java/com/legend/StatementExecutor.java:212-223` (ExecFrame + FQN
constants), `aliasFrame` 306-342, `spliceHook` 351-420, `spliceValuesRead` 437-463;
hook is PRE-order (`UserCallInliner.java:303-309`).

The grounded parts: `relationRooted` — engine `Result.values` for a TDS query
holds ONE TabularDataSet, for class/scalar roots it IS the collection (corpus
reads `.values.rows`, `.values->at(0)` throughout engine testFilters.pure) — and
the `at(k>0)`-loud rule (335-337, 393-397): the engine would throw a runtime
bounds error on the same read; loud-for-loud is faithful. The `$tds->size()`/
`$r->size()` = 1 arm (366-375) matches pure (a `[1]` value has collection size 1).

The overfit: the recognizer set — `.values` on a frame var, `at(0)`/`toOne`
wrappers, `size` on a bare frame var, `.rows` marker, bare frame var → chain — is
exactly the idiom list the corpus spells (the code says "the splice rules moved
VERBATIM from the harness"; the bare-var arm is annotated "harness parity").
Envelope reads outside that list silently get **chain** (row-collection)
semantics on relation-rooted frames where pure gives **envelope** (one-TDS)
semantics, and the two spellings of the same read disagree:
- `let tds = $r.values; $tds->size()` → 1 (correct), but un-aliased
  `$r.values->size()` → the pre-order hook misses the size arm (arg is a
  PropertyAccess, not a Variable), `.values` splices bottom-up to the chain, and
  the result is the ROW COUNT. Pure/engine: 1.
- `first($r.values)`, `$r.values->map(...)`, or any wrapper other than
  at/toOne over a relation-rooted `.values` likewise land on the chain.
- A bare `$r` (the Result itself) reads as the chain — conflates Result with its
  values for any consumer other than the pinned ones.
These all fail in the visible direction when a test asserts on them (nothing can
silently pass), so the severity is MED not HIGH; but the rule set is shaped by
pins, not by a Result.values model, and each new read spelling will need another
arm. Principled shape: make envelope-ness a property of the frame's TYPE (one
value of RelationType) consulted by every collection native uniformly, rather
than a per-idiom recognizer.

## F6 — LOW — DEFENSIBLE (invented but internal): groupBy stage-2 key naming and claiming

`core/src/main/java/com/legend/normalizer/GroupBySynthesis.java:42-75`
(groupedKeyColumnNames), 82-129 (renameGroupedNavCond/renameSourceReads),
131-216 (applyGroupBy; `k<i>__<base>` at 46-49 and 140-144).

Engine ground truth: mapping-level `~groupBy` never renames anything — the
engine's `applyGroupBy` (pureToSQLQuery.pure:5768-5776) stamps the key columns
onto the select's `groupBy` under their original aliases, and join navigation
rides the ordinary join machinery. The `k<i>__<base>` names and the
claimed-key/PM alignment are lite inventions for its synthesized
relation-groupBy form. They are internal only (final TDS column names come from
the query's own projections, and the renamed nav condition references the
grouped relation privately), so nothing observable is being mimicked or faked;
and every mismatch is loud (non-navigate PM shape throws at 90-92, a non-key
source read throws at 111-116, orphan per-row formulas throw at 175-180).
Two structural notes: (a) the claiming loop exists twice (42-75 vs 147-184) with
first-match order as implicit shared contract — drift between them would produce
wrong-but-internal name maps; (b) first-match exclusivity (`claimed` sets)
falsely REJECTS two non-aggregate PMs mapped to the same key column (second PM
finds no unclaimed key → throw) — the engine accepts that mapping; loud, so a
scope gap rather than overfit.

## F7 — LOW — scoreboard/bridge commits: no overfit surface found

`2cd5e491`/`ce9cd0e1` (CoreBridge, PlanGenerator string entry) route the engine
module's entry points through core wholesale — no test-keyed branching found in
`engine/src/main/java/com/gs/legend/server/CoreBridge.java`. `27f22db2`/scoreboard
commits touch only docs. The B2a Typer/MapChecker/PlatformTypes surface changes
(`1f5bb41d`) add Result/execute typing by exact FQN via PlatformTypes, consistent
with the exact-FQN rule; the `.rows` marker erasure (StatementExecutor.java:359-364)
is typing-only plumbing for F5's arms and shares its assessment.
