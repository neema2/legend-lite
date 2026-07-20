# Audit 23 — Special-Case Conditional Census (user-ordered)

**Question audited:** how many if-statements special-case features, and which of
them are overfit to the corpus rather than grounded in engine semantics.

**Scope:** every name/FQN/magic-string/shape/literal-keyed conditional in
`core/src/main/java/com/legend/{resolver,normalizer,lowering,sql,compiler/spec,compiler/element,harness}`
(~37k LOC), classified:

- **(a) engine-grounded** — mirrors a cited legend-engine/legend-pure rule
- **(b) contract** — our own documented naming/carrier contract, both ends on a shared helper
- **(c) overfit-suspect** — plausibly exists only to pass specific corpus tests; concrete misfire named
- **(d) wall** — loud NotImplemented/IllegalState guard (healthy)
- **(e) harness-compensation** — the harness bending comparisons instead of the platform earning them

## Census totals

| layer | a | b | c | d | e |
|---|---|---|---|---|---|
| resolver (14 files, 12.1k LOC) | 99 | 59 | **84** | 149 | — |
| normalizer (11 files, 7.5k LOC) | ~60 | ~27 | **10** | 141 | — |
| lowering + sql | ~85 | ~58 | **14** | ~116 | 1 (sanctioned adaptRawSql) |
| checkers + element + harness | ~45 | ~40 | **14** | ~40 | **24** |

Positives that held: **zero corpus-test-data literals in code** anywhere;
wall density is high and both lowering frontier-defaults throw named errors;
PlatformTypes is a working exact-FQN home; the audit-19 harness-compensation
kill (B1–B7) held — no expected-value patching or actual-side bridges remain.

Systemic negatives:
1. **Four raw `endsWith` desugars in Typer** (renameColumn/renameColumns/pair/
   columnValues) — the largest direct violation of the exact-FQN/never-endsWith
   rule. A user function named `my::customRenameColumn` is silently hijacked.
2. **Golden-derived rules one step from overfit**: many resolver (a)-classified
   arms cite engine *goldens/row-counts* rather than engine *source*
   (Substitution's negation constant, SyntheticHeads' depth-1/deep split,
   TemporalFrame's inclusive-thru sweep date, AssociationJoins' self-assoc
   orientation). These are the (c) list's spine.
3. **NavMaterializer has zero throws in 574 lines** — every unsupported shape
   exits via silent return/continue, delegating loudness to backstops the file
   never invokes. Inverts the package's own loud-miss policy.
4. **One-end-hardcoded contracts**: `"_y"`, `alias+"_"`, `"rows"`,
   genBusinessDate/genProcessingDate, temporal strategy strings, the
   legacyAssocPredicate FQN, and the toOne-unwrap idiom (~15 copies) all lack
   shared constants/helpers.
5. **Uncollision-checked prefix family**: Pipelines slot prefixes (`alias+"_"`)
   and several minted row vars (`_cj`, `_y`, `_cl/_cr`, `v_stw`, `_canon`)
   have no freshness/collision guards where sibling code has exactly that
   machinery.

## Ranked global fix plan

### Slice A — exact-FQN violations + surgical checker fixes (small, high principle)
- A1 Typer:250/258/265/280 endsWith desugars → segment-aware bare-name or exact
  `meta::pure::tds::` FQN matching.
- A2 Typer:297/312 TDS_ROW_GETTERS + isNull/isNotNull arms → receiver
  relation-type gate (mirror the `get` arm at :345).
- A3 StoreResolver:3369-3372 `findFunction("equal"/"isNotEmpty")` short-name +
  `get(0)` → exact-FQN lookup, loud on absence.
- A4 ProjectChecker:153-162 implement the promised catalog-native name guard
  (doc-code drift; audit-13 F6 regression class).
- A5 CorrelatedSubselects:902 `catch (RuntimeException) → null` → check
  `binds()` first; never swallow resolution bugs (a partial subtype cast
  silently treated as total membership).
- A6 Shared constants: `"rows"` marker (Typer↔StoreResolver↔Lowerer),
  `"_y"`, genBusinessDate/genProcessingDate, legacyAssocPredicate FQN,
  one `unwrapToOne` helper.

### Slice B — silently-wrong-rows resolver arms (correctness)
- B1 Substitution:1242-1259 toManyCrossingRead first-crossing-only → loud on a
  second crossing (`!(kids… && pets…)` currently null-guards only kids).
- B2 Substitution:539-569 negation-isolation boolean constant — re-ground in
  engine source (not the two goldens) or wall the untested polarity.
- B3 StoreResolver:2057-2059 `"milestoning"` name truncation → gate on the
  class's temporal strategy, not the property name.
- B4 Substitution:1002-1012 witness guard string surgery → ClassMapping helper.
- B5 SyntheticHeads alphaCanonicalBody bails (217-227) → extend or make the
  bail loud (under-merge = row cross-multiplication).
- B6 NavMaterializer loudness pass: convert the silent skips that have no
  downstream wall (items 38-39, 78-83 in the resolver report) into walls.
- B7 Prefix/var collision guards: Pipelines:194/239 slot prefix, `_cj`,
  CorrelatedSubselects:124 wrong-row collision check.

### Slice C — lowering value-correctness
- C-a Lowerer:899 marker leak: bare-row-map early return before the
  `__UNIQUE_VALUE_ONLY__`/`__HASH_LIST__` arms → reorder (currently emits the
  marker as literal SQL).
- C-b Lowerer:3051 relationCast silent no-op on unknown columns → loud unless
  the source is pivot-dynamic.
- C-c Lowerer:1190 scopedResolver own-select last-resort fallback → make loud
  (self-correlation bug class; two prior regressions in its own comment).
- C-d 'TDSNull' string emissions/parses (Scalars:731/2755, Lowerer:2437) →
  scope to the harness boundary via a shared constant; document that pure
  drops empty elements where we print the sentinel.
- C-e Ledger the deliberate divergences (enum-vs-string equality, narrowing
  casts, type() typeof fallback, hasDay column form) in CONSTRUCT_COVERAGE.

### Slice D — harness leniency review (classify-first, tighten second)
- D1 endsInSort tail list (E4): add order-preserving tails (filter/select/
  rename) and sweep-classify what surfaces; each new FAIL is a real order bug
  or a mis-listed tail.
- D2 Stacked numeric tolerance (E1+E2): measure how many passes depend on the
  1e-11/half-ulp grant (instrument, count, then decide).
- D3 isExecuteCall (E19) and containsSqlText (E14) name-matching → gate by
  harnessVocabName like the wrapper inliner.

## Full per-layer suspect lists

The complete ranked (c)/(e) lists with file:line and misfire scenarios are in
the four auditor reports, reproduced condensed here:

### Resolver (84 suspects; top 8)
1. Substitution:539-569 negation constant generalizes two goldens — inverted
   booleans for no-children parents on other shapes.
2. Substitution:1242-1259 first-crossing-only null guard.
3. StoreResolver:2057-2059 `milestoning` property-name truncation.
4. StoreResolver:2417-2422 from()-rescope cache-key mix (root vs hops can bind
   different mappings).
5. GraphEmission:213-236 to-many leaf shape gate — wrapped reads explode rows.
6. SyntheticHeads:217-227 canonicalization bail defeats merge-by-identity.
7. NavMaterializer:407-419 from/thru sub-union join merge (two-dates-per-head).
8. NavMaterializer:527-530 hand-parsed `#` with literal `"x"` fallback —
   prefix collision.
(Items 9-85 + flags: see audit transcript; MED/LOW tail is dominated by
unchecked minted names, last-body-statement-only reads, and silent skips.)

### Normalizer (10 suspects; top 4)
1. JoinChainEmission:618-633 nullTolerant() spelling whitelist — INNER
   realization silently keeps null-extended parents on unlisted null-tolerant
   spellings.
2. UnionSynthesis:1099-1138 liftTargetMerged drops target suffixing — crossed
   routes with colliding keys match the wrong member.
3. AssociationSynthesis:221 first-PM-wins without the XStore agreement wall.
4. M2mRouteGuards:98-100 SHORT-class-name set matching (exact-FQN violation).

### Lowering (14 suspects; top 6)
C1/C2 'TDSNull' printed into user-visible strings where pure drops empties;
C3 scopedResolver silent fallback; C4 tdsCell harness-spelling parse;
C5 relationCast silent no-op; C7 enum-vs-string equality divergence;
C9 aggregate marker leak. (C13/C14 are golden-text-only.)

### Checkers + harness (14 c + 24 e; top)
C1-C3 endsWith cluster; C4/C5 ungated bare-name desugars; C6 rows/values/
columns shadow real columns; C7 FromChecker drops constructed runtimes;
C10 doc-code drift; C11 unsafe-incomplete strictness blacklist;
C12 beyond-engine fold reorder (float associativity).
E-list: stacked float tolerances (E1/E2), order-policy switch (E3/E4),
TDSNull sentinel ownership (E5/E6), advisory-bucket name matching (E14),
isExecuteCall bare name (E19).

## Verdict

The codebase's anti-overfit structure is real (walls, exact-FQN discipline,
zero test-data literals), but ~120 conditionals across the four layers are
either rule violations, golden-derived semantics without engine-source
grounding, or silent fallbacks in wrong-rows positions. Slices A-C are
mechanical-to-moderate; Slice D changes what the harness accepts and must be
sweep-classified like any behavior change.
