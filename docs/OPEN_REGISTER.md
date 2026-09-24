# THE OPEN REGISTER

ONE list of every open item, with its source and size. The PROGRAM-LEVEL map (buckets, arc, audit intake) is docs/PROGRAM_MAP.md — this file holds per-item state. Maintenance
rule (part of every slice's definition of done): a row moves to the
CLOSED section IN THE SAME COMMIT that closes it; new deferrals add a
row in the same commit that defers them. "What's unfinished?" must
always be a thirty-second read of this file.

Size classes: S (< half a gate cycle), M (one to a few slices),
L (a program leg).

## 1. Verdict work (CANONICAL_FORM_SPEC / STAMP_DISCIPLINE_PROGRAM)

| # | Item | Size | Notes |
|---|---|---|---|
| V7 | Corpus-lane cutover + harness arm DELETION | L | decoded-golden-text + grid problems are corpus-only; LAST, after PCT lane proves the system. PREREQUISITES V10a/V10b below. |

| V8 | R3 tolerance census: 2-ULP + the 21 cross-engine float rows | M | retire or declare; both counted today. Candidate retirement design (2026-08-22, user-briefed): same-arithmetic H2 referee — byte-differing Double pairs re-compare EXACTLY on H2 (the goldens' own libm); tolerance dies |
| V9 | Grid byte cutover closing slice (after V4/V8) | S | ledger says: ORDER BY + policy + GridCompare arm deletion, no emission |
| V12 | Single round trip per assert (user design 2026-08-22): side-tagged UNION ALL — per-side TYPED value columns NULL-padded (no promotion erasure), per-side canon columns, ORDER BY side+canon; literals INLINE in the same statement (never host-folded — testEmptyChar proved literal emission needs exercising); tunnel gains a rung (fused→split→bare→fold). GO/NO-GO: measure query.exec share via TimingLedger first | M |
| V13 | WHOLE-FUNCTION fusion (user insight 2026-08-22: assert = the verdict OVERLAY, the graphFetch→serialize species — 4th overlay after graph/PCT-wire/snapshot): let IS WITH (materialized CTE = evaluate-once let semantics, also dissolves within-test F13 identity), verdict table out, typed list() evidence columns for the referee. HAZARD: eager evaluation vs pure first-failure sequencing → fusion-gradient tunnel. SEQUENCED AFTER V7 (perturbs the golden-text lane, like prepared statements). This IS the legend-sql thesis in miniature | L |

## 1b. VERDICT RULE AUDIT (docs/VERDICT_RULE_AUDIT_2026_08_22.md — every rule vs engine source)

| # | Item | Size |
|---|---|---|
| X6 | 2-ULP reclassified: compensates IEEE-double carrier vs engine's exact-decimal floats — R3 decides declared-policy vs decimal carriage | R3 |

## 2. Audit findings still open

| # | Item | Source | Size |
|---|---|---|---|
| A1 | 1==1.0 / indexOf base / substring base — ADJUDICATED IRREDUCIBLE (user ruling 2026-08-23): 1-based indexing is REAL pure semantics in core_relational — a 0-based flip (even per-lane) conflicts with it, so the 5 indexOf/substring AGREE-FAIL rows are permanent ledger entries, not fixable divergence (both channels fail identically; reference-corroborated). Do NOT re-attempt the per-lane split; a semantic-node draft was built and REVERTED same day | CLOSED (ledger) |
| A2 | Parser invention census (53 skew + 42 crash rows) | DEEP_AUDIT_HANDOFF | M |
| A3 | Parser lenient→strict flip (LAST, after A2) | DEEP_AUDIT_HANDOFF:95 | M |
| A4 | Foundations Phase 3 de-duplication | FOUNDATIONS_PLAN §4 | M |
| A5 | u_map__ name sniff → explicit flag | deep-audit tier-2, AWAITS RATIFICATION | S |
| A6 | agg_N collision scan | deep-audit tier-2, AWAITS RATIFICATION | S |
| A7 | Raw-SQL literal-aware rewriting | deep-audit tier-2 (merges into prepared-statements leg) | M |
| A8 | static-final/ThreadLocal guard visibility | deep-audit tier-2, AWAITS RATIFICATION | S |
| A9 | missing-[1] on ^new | deep-audit tier-2, AWAITS RATIFICATION | S |
| A10 | nlq/server hardening (incl. uncached-connection closing). FLAKE WITNESS 2026-08-22: DiagramServiceTest.httpEndpointReturnsErrorForMissingCode 404-vs-400 once under the full G1 suite, 3/3 green standalone — port/leaked-server contention class | deep-audit tier-2, AWAITS RATIFICATION | M |

## 2b. CONTRACT PROGRAM (re-framed 2026-08-23 from the typed-IR program, user step-back: labels are NORMATIVE CONTRACTS, not descriptions to verify — stamp says what it means, label RECORDS how it travels [written at CONSTRUCTION by the emitter that knows], wire metadata PROVES delivery [rides with the data, no round trip]. Dialects CONFORM to contracts [the SqlFn.ROUND precedent extended to types]; contracts derive from PURE SEMANTICS [never blind wire-adoption: HUGEINT sums protect testLargePlus — adopt; value-changing casts forbidden]. The static judgment RETIRES as an inference engine; its rules relocate into typed builders incrementally. Each wire divergence adjudicates one of three ways: ADOPT (reality into the contract) / CONFORM (cast-or-normalize at the dialect boundary, value-preserving only) / FIX-EMITTER (record the representation choice). Nullability: construction-carried (builders know), guarded by VALUE-level decode tripwire (metadata is unknown-happy; DuckDB spells all-NULL columns INTEGER — recorded wire-census caveat))

| # | Item | Size |
|---|---|---|
| T1 | Slice 1 LANDED (static census, now the ARCHAEOLOGY record): SqlTyping bottom-up judgment (PARTIAL — null = no rule, never a guess) + SqlTypeCensus at the Executor choke point (declared OutputCol vs computed, classified). FIRST CENSUS (PCT lane): 13,204 agree / 364 mismatch / 808 untyped; mismatch tail = the KNOWN carrier conventions (TIMESTAMP<>VARCHAR precision convention 231, DOUBLE<>VARCHAR print-form carrier 20, Decimal width-widening ~13) + a handful needing eyes (DOUBLE<>Decimal 2). Untyped top: NullLit 123 (admissible-by-design), UNNEST 89, Case 66, LIST_MIN/MAX 82, ADD_INTERVAL 37, correlated Columns 36, numeric promotion | done |
| T2 | WIRE CENSUS LANDED (probeWire at executePrepared: label vs ResultSetMetaData, per dialect, classified+witnessed, failure-isolated). First reading (PCT std): 3,037 agree / 517 diverge / 0 unknown — NEW wire facts: DuckDB narrows int literals to INTEGER (199, value-safe), UBIGINT exists in our wire (5), all-NULL columns spell INTEGER in metadata (~82 — the padding class in wire form). Judgment-coverage growth RETIRED as a goal; rules relocate to builders | done
| T3 | ADMISSIBILITY relation: adjudicate each mismatch class as admissible-carrier (temporal VARCHAR, print-form, width-widening) or LIE; conform-by-emission at the lying seams (the 3 decline survivors' fix rides here — letFn to_json boxing, map row-shape, byte-carrier label). ADJUDICATION BURN LANDED 2026-08-23: `delivers(label, meta)` is the delivery relation (exact match after normalize / value-subset integer chain BIGINT←INT/SMALLINT/TINYINT / DECIMAL(p,s) same-scale narrower p / registered carrier conventions via admissibleWire). PCT diverge 1,778→79, corpus 7,492→181 — ceilings PINNED shrink-only (ChannelB suites ≤80 diverge/≤110 adopt-pending; corpus runner ≤181/≤130). Residue = THREE named families, witnesses attached: (1) hash() UBIGINT — CONFORMED 2026-08-23: SqlFn.HASH graduated from the Spellings data row to the `hashSigned` dialect arm (DuckDB: xor sign-bit + HUGEINT shift = exact two's-complement reinterpretation, bijective, hash evaluated once — plain CAST is range-checked and THROWS ≥2^63, caught by Channel B on first attempt); Lowerer's aggregate HASH_LIST arm had PRIVATELY shifted (+Long.MIN_VALUE through HUGEINT) while the scalar path lied — the second owner DELETED, dialect arm is the single owner; PCT diverge 79→74, pins tightened 80→75; (2) percentile/quantile returns input type BIGINT under DOUBLE ×3 (testPercentile — RE-ADJUDICATED against engine source: signature is `percentile(Number[*],Float[1]):Number[0..1]`, so discrete percentile over Integers IS an Integer and a DOUBLE cast would be value-changing/WRONG — belongs to family 3, the DOUBLE label is the erasure); (3) Number-erasure decimal delivery (zScore/max/median/DECIMAL(18,6) store reads — abstract-Number slots, rides the builder slice). ADOPT-PENDING bucket = integer aggregates delivering HUGEINT (108 PCT/130 corpus; witnesses: window/groupBy SUM-over-Integer + edge-arithmetic protection): contract WIDENS at construction per the testLargePlus rule. MEASURED STRUCTURAL FACT 2026-08-23: these labels derive from the RELATION SCHEMA (pure Integer → BIGINT via PureSql.type at the egress boundary), not from the aggregate emitter — recording HUGEINT travel needs the label-at-construction builder leg (T4 territory), NOT an expr-sniffing patch at the schema seam; the pinned bucket is the honest interim. int-or-null bucket (177/83) = all-NULL metadata ambiguity — RESOLVED 2026-08-26 by the D1 decode tripwire (value evidence: proven-empty 56/219 ceiling-pinned; valued rows land in the EQUALITY-0 diverge pin) | M |
| T4 | FLIP label authority: OutputCol computed from the judgment, stamp-vs-computed divergence alarm pinned 0, census counters become the permanent verifier. ADJUDICATED 2026-08-23 (user challenge killed a fixer pass): OutputCol.nullable today MEANS multiplicity-echo (PureSql.nullable is its only writer), NOT wire nullability — the engine has no such flag at all (nullability lives in pure multiplicity; union pads/subtype markers are unlabeled plumbing its Java assembly handles). The ~6.5k null-under-required-multiplicity rows are the flip's measured RE-LABEL backlog, not bugs; the meaning changes WHOLESALE at the flip (one meaning, one owner) — never per-site (mixed-meaning flags are worse than either meaning; the rejected NullPadLabels pass was the builder+fixer anti-pattern). CLOSED 2026-08-26 (charter §4bZ-V E + D — the one-owner requirement was met WITHOUT the wholesale flip): N0 machine-counted the backlog 100% literal NullLit; N1 = a projected literal NULL declares its slot nullable in reconcileLabels (the frame's own ctor — construction declares what construction knows; pure [1] contracts untouched); N2 = bottom-mult EQUALITY-0 on all four lanes, the bucket now the live tripwire for computed bottoms. The decode tripwire (D1) landed and settled int-or-null by VALUE evidence: 56 corpus / 219 pct PROVEN all-NULL (ceiling-pinned), and its first sweep caught 6 valued INTEGER wires under VARCHAR labels — SqlUnion's ctor was dropping the mapping-seam tag; union-label reconciliation (the SqlSelect compact-ctor idiom on SqlUnion) transports tag/type/nullability, diverge back to EQUALITY-0 | L |

## 3. Recorded engineering follow-ups (each noted in code/doc at its site)

| # | Item | Size |
|---|---|---|
| F1 | GraphEmission:2714 nested-nav TypedLimit (D6a family) | S |
| F2 | unwrapElemRefs Exists/ScalarSubquery pre-existing hole | S |
| F3 | CarrierStrategies CompactList strategy for H2 (145 loud h2-replay declines) | M |
| F4 | Scoped-run seeding artifact (-Drcorpus.only fails aggregationAware at HEAD) | M |
| F5 | Per-family corpus seeding (#112) | M |
| F6 | Derived-property identical-signature dup rejection | S |
| F7 | Dup-FQN coverage: services/connections/mappings namespaces | S |
| F8 | {target} + foreign-db join-ref validation (D6b skipped conservatively) | S |
| F9 | Invariant-3 register burn-down: wrap 21 write-once tables immutable | M |
| F17 | fold/copy `+=` DROPPED AT TYPING (diagnosed 2026-08-23, parked for the F10 finish per user direction): SpecParser records KeyExpression(isAdd), NewChecker.checkCopy never reads it — `^$p2(otherNames += $p1.lastName)` lowers as REPLACE (`[_i0.lastName]`); engine-true fix = protocol-level desugar in checkCopy: add=true ⇒ value' = concatenate(PropertyAccess(receiver, prop), value), reusing the whole pipeline. SECOND bug same witness: unset to-many properties emit UNTYPED NULL struct fields — DuckDB cannot unify the reduce lambda's VARCHAR[] with the seed's NULL type ("Unimplemented type for cast VARCHAR[] -> NULL"); fix = typed empty/CAST(NULL AS <elem>[]) at the instance-literal layout. Burns testFoldToMany + testFoldFiltering | S/M |
| F13b | Identity v2 residue: (a) the ARRAY-shaped keyless side (one witness — a [*] side whose plan projects an array-of-struct cell; the identity canon's struct_extract fails at bind and rides the canon-exec decline tunnel, counted); (b) lambda-minted ctors (site id cannot distinguish per-element evaluations — declined by the v1 exclusion scan, zero PCT witnesses today); (c) inlined-function-body ctors with SUBSTITUTED args rebuild per side (α-substitution) — a `let p = makeP('a')` shape would re-mint per side; zero witnesses (G9 TRUE-WIRE-BUG=0 pins it), fix = one inline pass per assert statement | S/M |
| F11 | Effectful-assert byte coverage: the containsEffect gate routes effectful assert statements to body inlining (host verdicts only) — the gate stands on statement-orchestration grounds (V11 adjudication at the gate site), so claiming these needs the side path to learn sequential effect execution; V7-territory sizing | S/M |
| F15 | Reference-adapter parser ingress: ExecuteLegendLiteQuery's SIX source-extraction regexes + reEscapeStringLiterals are a SHADOW PARSER (standing tenet violation, predates parser parity 6489/0) — parse PCT source with THE parser, splice from the AST, delete the patterns | S/M |
| F16 | Adapter kind-consolidation: **EXECUTED 2026-08-27 (F10 slice 4 cargo batch — docs/ADAPTER_NECESSITY_CENSUS.md)**: the declared-type consult arms measured ZERO on both PCT lanes and deleted (Double→Decimal consults, Float32, Number catch-all, StrictDate narrowing, date-text reparse, UUID, LocalTime, DuckDBStruct+classInstance — structToInstance is the ONE struct owner); the one live narrowing witness (parseDate bare-date literal) cured at the EMISSION (Scalars casts to DATE on the StrictDate stamp). The BigDecimal-under-Decimal-contract arms stay as the DECIMAL wire boundary (witnessed 23+21). STALE-NOTE CORRECTION: "remapErrorMessage dies with the error-composition leg" referred to the shift-error answer-key remap, which deep-audit H4 already deleted; the surviving prefix strip is a DIFFERENT arm, measured LOAD-BEARING (18 firings) — its burn is the Bucket 2 error-shape leg | CLOSED (residue: J6 prefix strip → Bucket 2) |
| F18 | UNTANGLE (UPSTREAM_BOUNDARY_PROGRAM.md §3 D, rewritten 2026-09-24): steps 0–3 landed (guardrail pins, catalog diff 0 divergent / 191 missing, declaration + implementation tables total over 3,157 rows, 0 dangling/0 conflicts; shadow census 2026-09-24: picks 2,672 agree / 11 differ in two classes, overload sets 0 today-more / 29 table-more / 357 bare-name, forms 0 qualified disagreements). 4a LANDED 2026-09-24 (the pick by table; checkpoint met). OPEN: 4b resolver qualifies platform natives (bare → 0; the catalog's bare index and the `CORE_FUNCTION_PACKAGES` courtesy die), 4c overload set from the table (PCT rule, `isPlatformOwnedFunction` die; the 29 hidden FQNs' overloads registered by id), 4d the fallbacks; the prelude-vs-upstream duplicate bodies (a harness double-load, `DeclarationTable.duplicates()`) become a model-builder refusal like upstream's (PCT rule, `isPlatformOwnedFunction`, `CORE_FUNCTION_PACKAGES`, bare-name `nativeKeysAt`, `CoreFn.of` fallback, `KNOWN_ABSENT`) — first switch is the CHECKPOINT; 2b stdlib-resource decision by measurement at step 4; step 5 identity pins → 0, `native-membership.tsv`/`native-claims.tsv` retire; the 89 `CoreFn.OWNS` entries reviewed (derived mechanically); #23 SQL sharing is on the critical path once Body inlining widens | L |
| F10 | Variant-aware byte canon — V1 LANDED 2026-08-23 (the LITERAL CHANNEL): Any-involving pairs byte-compare in pure's own literal spellings (six disjoint forms carry kind in the bytes), dispatched on the JSON carrier's runtime type IN THE DATABASE (anyJsonCanon: json_type CASE); typed sides append a literal candidate (guarded by column kind); Any-stamped plain columns render the COLUMN's literal (wire fact); trees mark U+0001 and the verdict DECLINES on sight; the canon-exec tunnel gained the MIDDLE RUNG (drop-literal re-wrap — a lying stamp never demotes bare byte verdicts); mixed-numeric gate exempts JSON-carried sides (no promotion). Declines 13 -> 3 (agree 1645); Pair-of-Pairs claimed by SUBSTITUTION-AWARE EqualityKeys (instantiation-keyed cycle guard). REMAINING RESIDUE = 2 (declines 3→2, 2026-08-23; ceilings 5→3): letFn BURNED by the Any-root FIX-EMITTER — scalarRoot boxes a judged-CONCRETE non-JSON expr under an Any/JSON label with TO_VARIANT (SqlTyping.judge at construction = the emitter consulting its own product; Bottom/Unknown never guess, stay censused). SLICE 2 LANDED 2026-08-23 (declines 1→0, ceilings →0, Channel B 1,084): mixed-NUMERIC collections ride the LITERAL carrier (SqlType.Scalar.LITERAL, physical VARCHAR everywhere; encoder LiteralSpelling.mixedNumericArray + sort arm's Array(LITERAL) construction-site cast marker read by scalarRoot; decoder sql/LiteralText; canon = the CELL ITSELF, literal-only) — testMixedSortNoComparator FLIPS TO PASS (Essential 293/9 — both sides keep Integer-vs-Float identity; the DOUBLE-promoted expected side was the eraser), the frontier row retires. mixedNumericKinds gate KEPT (narrowed charter: computed-mixed residue, zero witnesses, ceiling-0 backed — the doc's delete line was WRONG, the gate still guards shapes with no carrier claim). MixedEncoding numeric ids switched to the LITERAL table (floatCanon total fixed-point). map ManyToMany BURNED 2026-08-23 (declines 2→1, ceilings 3→2) — the decline was a SYMPTOM of a semantics bug, not a canon gap: to-many property nav over a collection (`[$p1,$p2].locations`) lowered to LIST_TRANSFORM without FLATTEN, so UNNEST peeled one level and rows carried STRUCT[] cells under a Struct label (the binder error was the honest witness). Fix at the emitter arm (Lowerer scalarStructural): the MODEL's declared property multiplicity decides — many property ⇒ LIST_FLATTEN over the mapped lists (DuckDB flatten drops NULL/empty inners = pure's empty-drop, probed). Test went ERROR → PASS as B-FIXES-A (channel A excludes it); grammar floor 132→133. The ARRAY-of-identity-struct canon idea was the WRONG fix (would have taught the referee to accept a malformed shape). Residue: mixedSort Number-stamped mixed only (engine-frontier test, fails both channels anyway). Also landed: -Dchb.only scoped-run filter (the rcorpus.only idiom) for single-test debugging. F10 PROPER RATIFIED 2026-08-23 — design doc docs/F10_CARRIER_DESIGN.md is AUTHORITATIVE (spelling-as-tag LITERAL carrier, 4 gated slices, deletions-as-proof); superseded summary: kind-tagged variant carrier (temporals/Decimals erase to JSON strings/doubles — engine equal('2014-01-01', %2014-01-01) FALSE is undecidable on this wire, host referee equally blind), carrier-owned decode, retires the +0000/D-suffix canon strips; fix the three stamp lies by conform-by-emission at the Any output seam | M |

## 4. Parked BY THE RATIFIED ARC ORDER (sequenced, not debt)

| # | Item | Size |
|---|---|---|
| P1 | Decoupled-PCT completion burn — RE-MEASURED 2026-08-23: the walls hide ZERO PCT tests (the '65 hidden' figure died with the relation wall burn, and the residual '3 essential hidden' was a grep counting COMMENT mentions in surveyor.pure — channel B discovers the on-disk truth in all five families, 1,118, MORE than channel A's configured 1,109: relation qualifier config filters ~7, grammar/unclassified ±1 enumeration edges unchased; B-only rows currently IMPUTE channel A's verdict in the diff — an A-ABSENT bucket would make the census exact). The burn's real payoff: (a) the m4-grammar differential (A5, 183 rows pinned ≤226 — six parse constructs: value-parameterized types, unit literals, @-multiplicity/@-relation-type annotations, raw ^instance graphs); (b) the plain <<test.Test>> universe those files carry (~62 unit tests in the six parse-walled files alone — the P2 test-corpus territory); (c) the reflection families (Multiplicity/ValueSpecification/PackageableElement model walls). Plus the still-live: instance-universe 13, date-error 5, big-number 4, A1's 3, prim-ext 2; frontier-12 stays pinned | L |
| P2 | ###Data execution → test-corpus branch unlock (DEFERRED_TEST_EXECUTION.md; census first) | L |
| P3 | Corpus burn-to-zero resume (2,347/2,575 — 228 left) | L |
| P4 | Prepared statements (LAST — perturbs the golden-SQL text lane; absorbs A7) | L |

## 5. Declared leniencies (LIVE POLICY, counted — revisited at V7/V8)

- Corpus temporal golden compares are INSTANT-based (goldenEqualScalar,
  H2Verify.norm) — the engine's two-subsecond-spellings adjudication.
- 2-ULP Double×Double dialect-arithmetic policy — USER-RATIFIED
  2026-08-22 as declared+counted (cross-libm last-ULP drift on
  transcendentals: H2/Java-minted goldens vs DuckDB acos/log/tan; no
  emission fix exists). Lives in TWO counted places: the host lattice
  arm (LL_TOL_COUNT instrument) and the byte-verdict policy arm
  (sqlUlpPolicy census); the golden seam falls through to the lattice
  rather than judging value-differing numeric pairs itself. GridCompare
  sig-digit cell tolerance (the 21 rows) unchanged. R3/V8 owns
  retirement (H2 same-arithmetic referee design).
- Float canon non-finite pass-through (witness-free edge,
  referee-guarded); the DECIMAL(38,18) unfold is DEAD (V10c textual
  exponent unfold).
- Latent Float×Decimal integral tension (host true / byte false) —
  zero witnesses, documented in R0 §3.
- STRING_AGG input-order contract (Render precedent, not a guarantee).
- Engine-frontier 12 (the engine's own relational executor fails them
  too) — pinned, burn if the engine moves.

## CLOSED

- RELATION WALL BURN CLOSED (2026-08-23, the 61-test discovery gap):
  over.pure (68 window PCT tests) and pctQualifiers.pure compiled —
  the '?' schema-algebra column wildcard classifies as the anonymous
  TypeVar (the InferenceKernel UNKNOWN_COLUMN_TYPE convention), and
  Profile self-stereotypes/tags parse in platform lanes and DROP
  faithfully (the engine protocol Profile has no applied-annotation
  field; the ENGINE GRAMMAR has no such slot — verified in
  DomainParserGrammar — so the LEGEND dialect refuses verbatim,
  refusesLiteExtensions-gated). Relation discovery 287 -> 355 (MORE
  than channel A's own 348 — its qualifier config filters ~7); 66 of
  68 new tests passed OUT OF THE BOX; the two failures were ONE
  renderer bug — the aggregate-ORDER-BY hoist dropped declared null
  placement (pure DESC NULLS FIRST sank to backend default;
  AggOrderNullPlacementTest pins it). Relation walls 23 -> 20, suite
  100% at the expanded universe (355/355, TRUE-WIRE-BUG 0). Channel B
  totals: 1,082 pass / 1,118 discovered.
- F13c CLOSED (2026-08-23, eq/equal identity IN CONDITIONS — user-driven:
  "do we pass the eq tests?"): the in-SQL eq/equal/contains/in arm
  family (InstanceEquality, identity lane only) compiles the ENGINE
  equality relation from the verdict layer's OWN canon — ONE owner. eq
  = __id compare (identity for keyed AND keyless; NO static classifier
  fold — a supertype-stamped alias of the same instance stays TRUE);
  equal/== = canonical-render compare (key tree / identity), static
  cross-class folds FALSE; contains/in = canonical membership
  (list_transform to canon texts — engine contains() is equal() per
  element). Identity layouts now cover ALL model classes (eq needs
  identity on keyed classes too; platform carriers excluded — their
  ctors short-circuit the layout). Assert-CONDITION sides join the
  identity lane (evalValue identity flag; boolean egress keeps every
  other lane blind). ENGINE-VERIFIED shadow rule: EqualityKeys now
  dedupes by ALL declared property names (_Class.collectEqualityKey-
  Properties = simple properties filtered by stereotype) — an un-keyed
  subclass redeclaration REMOVES the super's key (witness
  OtherBottomClass). Lowerer split at the file guardrail:
  InstanceEquality + InstanceProjection extracted (3684 -> 3378).
  RESULT: testEq/testEqualNonPrimitive PASS as B-FIXES-A (channel A
  EXCLUDES them — identity unobservable on its value wire; ours rides
  as data), grammar 130 -> 132; contains/in NonPrimitive regressions
  caught by the suites and claimed the same way. Declines 13 unchanged,
  disagree 0.
- F13 CLOSED (2026-08-22, synthetic instance identity): keyless-class
  equality is engine-true IDENTITY in BOTH channels, carried as DATA —
  the verdict lane's identity layout appends __id to keyless classes
  (ClassLayouts.layoutOf(.., withIdentity), RIDER LANE ONLY: golden-SQL
  text lanes and corpus value lanes keep the plain layout, zero
  perturbation), minted deterministically per construction-site NODE
  (InstanceIds in the ExecEnv — both sides share the minter; a copy
  site mints a NEW id). The byte canon renders {_type,_id}; the host
  lattice compares wire maps that now CARRY the id (content
  fabrication for keyless pairs is dead); eq()'s non-primitive wall
  opens for id-bearing wires (eq = id equality — ids unique per site).
  LOAD-BEARING FIX: UserCallInliner's TypedNativeCall/TypedEval/
  TypedLet/TypedLambda arms rebuilt unchanged subtrees, breaking the
  file's own "untouched subtrees keep identity" contract — sameRefs
  identity-preservation restored it (side-e/side-a now reach the same
  ctor NODE). Guards: keyless-ctor-under-lambda declines (counted, v1
  exclusion), identityless-instance-wire declines (a __id-less
  instance map never byte-judges; Map carriers exempt — mapEquals is
  F12's rule). PCT-lane declines 19 -> 13 (agree 1564 -> 1570,
  disagree 0). ATTRIBUTION CORRECTED same day (measure-before-claiming
  trip): F13 moved ZERO test-level outcomes — pre/post Essential FAIL
  sets are byte-identical at 292/21/10/4; the AGREE-PASS/WIRE-BUG pins
  (288/11 -> 292/10) were banking a PRE-EXISTING measured state left
  unratcheted by earlier slices, not an F13 effect. What F13 moved is
  the JUDGING: 6 keyless pairs from host-referee decline to DB byte
  verdict, engine-true. Ceilings BANKED (30/35/35/45 -> 15). Residue →
  F13b.
- F12 CLOSED (2026-08-22, the Map canon): mapEquals byte-decidable —
  entry texts [kLeaf, vLeaf] per key (map_extract pairs), SORTED (the
  engine's order-insensitive rule becomes byte comparison), JSON-framed
  with the carrier fqn; leaf kinds from the MAP layout's static types.
  Host referee already mapEquals-shaped (wireTree key-set + per-key).
  Declines 35→25; the 3 Pair unclaimable-leaf rows went with it.
- F14 OPENED AND CLOSED SAME DAY (2026-08-22, user chain of catches):
  the "unSQLable NUL string" was never a value-domain fact — DuckDB
  VARCHAR holds NUL fine (chr(0) concatenates/compares exactly,
  user-verified empirically); the failure was OUR StringLit renderer
  embedding the raw byte into statement text and killing the SQL
  LEXER. Fixed at the spelling: stringLit splices chr(0) between
  quoted segments. The BLOB-carrier design drafted in between is
  RETIRED unneeded; "Tier A" lost its best member to a renderer bug.
- X5 CLOSED (2026-08-22, equality.Key — DB-first per user directive):
  our Pair/List declarations now CARRY the engine's <<equality.Key>>
  stereotypes (root cause of the instance declines — the parser had
  preserved property stereotypes all along, compilation dropped them;
  Property.Stored gained the flag); EqualityKeys resolves the key tree
  from the model (hierarchy walk, keyless/cyclic poison → null);
  keyed-instance BYTE CANON in the DB — JSON framing with OUR canon
  strings as values (user ruling: JSON is the framing, never the
  spelling), '_type' carries the classifier in the bytes, kind-tagged
  leaves ('i:8' vs 'd:8'), Pair struct + List bare-array carriers,
  list_transform for to-many keys; host referee restricts both sides
  to the key tree (the engine's own relation) before judging. PLUS the
  Nil/empty claim: a Nil-stamped side is the EMPTY value, kind-gate
  vacuous, and EVERY empty form canons '[]' (unification kills the
  null-vs-'[]' latent hazard). PCT-lane declines 97→35 (agree
  1486→1548, disagree 0); ceilings BANKED (100 → 5/30/35/35/45).
  Residue named: Map (mapEquals — own rule, claimable later), Any wire
  trees, genuinely keyless classes (engine-FALSE territory), 3 Pair
  unclaimable-leaf shapes, mixed-identity (F10), NUL. The 2 PCT
  eq/equal NonPrimitive exclusions adjudicate in the decoupled-PCT
  burn (task #18) — eq is IDENTITY, not keyed equality.
- V11 CLOSED (2026-08-22, user-ratified twice — "collapse the renderer
  into the original query like m2m JSON"): the canon rides the side
  query itself (CanonicalRenderSql.wrapWithCanon → `SELECT value,
  canon(value) FROM (plan) side`, CanonRider carries the harvested
  texts out of the ONE execution) — prepCanon/runCanon and the
  double-execution soundness obligation DELETED (StatementExecutor
  eval-ledger BANKED DOWN 2728→2326; total verdict-system surface
  −355). Unrefined Number sides project one candidate column per fine
  kind; runtime value kinds SELECT (never evaluate). Collection framing
  ('[', ', ', ']') moved to the verdict layer over DB-computed element
  texts + DB-computed canonical order. LiteralFold yields to a
  canon-riding side (the DB must compute the canon), surviving only as
  the last-resort value source for unSQLable literals (NUL-bearing
  strings), counted decline. The canon-exec decline tunnel re-executes
  BARE on wrapped-query failure — a canon column can never poison the
  value fetch (witness: mixed-identity VARCHAR carrier, F10). All five
  ChannelB suites green: discovery 287/137, disagree 0, declines 97
  ≤ 100; corpus green, h2-exec 320+632 unchanged.
- V1 sql-verdict disagreement alarm pinned (all five ChannelB suites +
  corpus runner) — closed in the V1–V5 slice (32eb39ac) and this one
- V2 evalCanon broad catch adjudicated (error-shape register; split
  into prepCanon/runCanon tunnels this slice)
- V3 ArchUnit host-verdict reachability rule (32eb39ac)
- V4 assertSameElements byte cutover (32eb39ac)
- V5 assertEq onto the canon machinery (32eb39ac)
- V6 decline burn round 1: 207→97 (NUMBER plan-refinement,
  PrecisionDecimal=Decimal, enums claimed; PAIR RULES for pure's
  non-transitive numeric equality; mixed-kind-collection gate;
  zeros-unify amendment) — eead1066
- V10a CLOSED: goldenEqualScalar now compares by THE ENGINE CONVENTION
  (fromSQLTimestamp nine-digit normalization + exact record equality —
  STRICTER than the retired instant compare: date-only never equals a
  midnight datetime); H2Verify.norm re-justified as DERIVED (both
  sides of that seam are DB reads = nine-digit by convention)
- V10c CLOSED, all three derived: STRING_AGG order = DuckDB's
  documented preserve_insertion_order default; double-execution
  soundness = the upstream containsEffect gate; the DECIMAL(38,18)
  unfold REPLACED by a complete textual exponent shift (any finite
  double prints fixed-point exactly; dual-render conformance battery
  pins DB text == host text incl. 1e-30 and 1e300)
- V10b PROBED AND CLOSED (2026-08-22): the spec tree's temporal-
  computation inputs are StrictDate / YearMonth / second+subsecond
  datetimes — ZERO hour/minute-datetime witnesses; the witnessed
  partial-DATE precisions ride dedicated lowering rules (DateShifts),
  not the padded-timestamp path. The root-only scalarRoot swap covers
  the whole witnessed domain — 921c80c3+1
- X1–X4 CLOSED (2026-08-22, VERDICT_RULE_AUDIT execution): all
  cross-kind grants DELETED — the lattice is engine-exact
  (EqualityUtilities: same-primitive-kind only, Decimal scale-sensitive
  equals, BigInteger-widened integral equality); canonical Decimal
  render REVERSED to scale-preserving; kind-class value-mode replaced
  by runtime-kind refinement from fetched values (pure's own Number
  dispatch — the plan-refinement was circular). Deleting the grants
  exposed SIX real wire bugs, all fixed at emission: round/divide
  constant-scale DECIMAL casts + toDecimal input-kind scale
  (DecimalKindRules), scale≤0 DecimalLit root-only cast (RootLiterals —
  a blanket renderer cast measurably truncated VALUES columns),
  longValue() BigInteger overflow (the X1 grant had MASKED it),
  INTEGER-declared BigDecimal decode guard (Executor). Golden seam
  quarantine: goldenEqualScalar compares numerics BY VALUE (golden text
  carries no kind/scale) and falls through to the lattice on value
  difference so the declared 2-ULP policy still judges cross-libm
  drift. Dual-render conformance battery pins DB canon == host canon.
- V6b the 97 survivors DECLARED (class instances + wire-tree
  containers per spec §4, + unrefinable Numbers) and CEILING-pinned
  (sqlDeclined ≤ 100, shrink-only, all five ChannelB suites) — the
  PCT-lane verdict system is COMPLETE: four families byte-decided,
  disagreement pinned 0, declines pinned and declared — this commit
