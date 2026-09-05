// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TENET RATCHET (JAVA_EVICTION_PLAN E0; program CLOSED 2026-08-18;
 * claim recalibrated by docs/ADVERSARIAL_TENET_AUDIT_2026_08_18.md):
 * tenet #1 — "Java orchestrates, the DATABASE executes" — enforced as a
 * shrink-only ledger instead of prose. The honest, measurable claim: NO
 * host interpreter remains; the QUERY COMPILER executes no values; the
 * egress boundary is a small set of irreducible carriage sites plus the
 * NAMED, SHRINKING residue registered below (PCT wire, product
 * CSV/JSON, corpus rows, metadata grids, JSON-source frames, testdatagen
 * text, grid-read chains are all DATABASE-PRODUCED). Growth is a new
 * Java-evaluation site and needs a deliberate pin bump with a written
 * justification.
 *
 * <p>THE METAMODEL CHANNEL (ratified adjudication, JAVA_EVICTION_PLAN;
 * HostEval evicted whole with Phase 1 — row deleted 2026-08-19 audit):
 * MetamodelWalk/MetamodelSteps/PlanText/AggAwareActivities
 * evaluate MODEL CONSTANTS (instance construction from {@code ^Class}
 * literals), replicate engine metamodel TRANSFORMATIONS under test
 * (convertElement, wrapH2Boolean — node-to-node assertions, no text),
 * and compose engine-parity TEXT through single-owner spellings (the
 * Ddl ENGINE_TEXT flavor, dataTypeToSqlText, the plan-text envelope).
 * NO DATABASE VALUE can enter the channel: grid chains are typed
 * relations the ordinary pipeline compiles (GridReads DIED with Phase 1;
 * its ledger rows were deleted with it) and
 * {@code ArchitectureTest.theInterpreterPerformsNoJdbc} makes the
 * boundary mechanical (the channel cannot reach a connection).
 *
 * <p>PERMANENT-ALLOWED (the registered residue — justified, not
 * counted): the egress decode cluster ({@code Executor.fetch/unwrap/
 * latticeKind/decodeAny} — decoding carriers the DATABASE produced, by
 * declared contract), {@code LiteralFold} (the engine's own
 * ConstantExecutionNode, differential-pinned), the harness COMPARISON
 * layer (verification consumes two sides, never produces a result),
 * and {@code JsonAssertCanon.sortByKey} (re-creates the TEST'S OWN
 * canonicalization over a metamodel that never executes through SQL).
 */
class JavaEvalLedgerTest {

    /** SIZE rows — the METAMODEL-CHANNEL register: pinned MAX count of
     * COMMENT-STRIPPED NON-BLANK lines, shrink-only (growth needs a pin
     * bump with a written justification — the code-shape-guard
     * convention). Stripped counting is the Tier-2 audit's answer to
     * the ADVERSARIAL_TENET_AUDIT §3.1 probe: with raw line counts,
     * deleting comments funded new evaluation code under a green pin —
     * stripped, only CODE moves the number. The PCT extension row is
     * the E1 adapter-contract residue (ingress splicing, the scalar
     * bridge, the H4 message remap). */
    private static final Map<String, Integer> EVICT_SIZE = Map.ofEntries(
            // 844 -> 850 (documented-debts 2026-08-18): the emptyCell
            // single-owner helper (six scattered null-drops now route
            // through ONE argued rule) and the LocalDateTime arm's
            // declared-type consult (the one-carrier Executor change
            // exposed the missing StrictDate narrowing) — both shrink
            // ambiguity, not evaluation
            // 850 -> the three-file split (truthfulness burn, census
            // §5c): the burns took the pair well below the old pin and
            // the file split along the derived seams — entry (thin
            // orchestration), packer (transport-contingent inbound),
            // bridge (the permanent bijection). Pins re-seeded at the
            // split's measured stripped counts; shrink-only from here.
            Map.entry("pct/src/test/java/org/finos/legend/lite/pct/extension/PctExecuteNative.java", 131),
            // 250 -> 259 (B4): the no-shadowing WALL — a fixture function
            // colliding with a lite-native name refuses injection loudly;
            // guard growth, anti-compensation
            // 259 -> 266 (B3+B11): the decoration-strip regexes DELETED
            // (verbatim injection, probed) and the silent property-drop
            // channels became LOUD WALLS (generic classes, no-path
            // types) — wall growth, zero evaluation
            Map.entry("pct/src/test/java/org/finos/legend/lite/pct/extension/ModelPacker.java", 266),
            // +2 (B8): the BigDecimal-under-FLOAT arm — the reference's own
            // Float shape (FloatCoreInstance IS BigDecimal-backed)
            Map.entry("pct/src/test/java/org/finos/legend/lite/pct/extension/ValueBridge.java", 355),
            // MetamodelWalk (1307) + MetamodelSteps (196) DELETED (batch 55a,
            // 2026-09-04): the Java port of toPostgresModel and the host
            // metamodel walk are gone — the three tests they still scored
            // ride rows (plan connections) and the identity struct of a
            // property-less class
            // raw-line history: 888 -> 943 -> 957 (burn batches 1-2:
            // temp-table IN envelope emitters + PureExp let-allocation —
            // engine-parity plan TEXT, the register's own class);
            // re-seeded stripped 2026-08-18
            // 749 -> 750 (Phase 1 dialect-blind fix: spliceLeftVar takes
            // a render FUNCTION instead of the AnsiSqlRenderer type)
            // 750 -> 760 (batch 38, 2026-09-03): enumMappingOf chases the
            // mapping's INCLUDES (real pure visibility of an included
            // mapping's enumeration mappings) — lookup, not evaluation.
            // 760 -> 845 (batch 46, 2026-09-03): the RELATION-ROOTED
            // single node (table accessor / tableToTDS plans; engine
            // spelling facts — precisePrimitives + default relational
            // types via PreciseTypes) and the plan-root finders MOVED here
            // from StatementExecutor (its count fell 2724 -> ~2700 under
            // its unchanged pin). Printing engine plan text, no evaluation.
            Map.entry("core/src/main/java/com/legend/plan/PlanText.java", 845),
            // 225 -> 227 (lambda-classifier slice: the lambda spelling
            // reader unwraps the m3 carrier stamp — LambdaFunction<ft> —
            // via PlatformTypes.functionTypeOf; a TYPE read, no evaluation)
            Map.entry("core/src/main/java/com/legend/AggAwareActivities.java", 227),
            // ADVERSARIAL_TENET_AUDIT_2026_08_18 §5: the grid egress was
            // "the sixth class the JDBC guard doesn't name" — these four
            // rows pin it until the relation-typed fetchDb leg DELETES
            // GridReads + DbMetaData's carrier wholesale (delete the
            // rows with the files, never bump them)
            // Phase 1 (One-Platform Plan): GridReads is DELETED — its
            // recognition survives as ResultNav's navigation-to-relation
            // mapping; the string SQL, HostResultSet carrier, and hand
            // shaping DIED (execution rides MIR + the standard Executor)
            // +77 (Phase 1 batch 2): HostEval's fold-in — owns()/chainBottom
            // moved to their owner (the shim's 132 lines net -30)
            // Phase 1c ENDGAME: ResultNav is DELETED WHOLESALE — grid
            // chains are typed relations the pipeline serves; the probe
            // moved to RawGridSchema, chainBottom to StoreNav (its row
            // below absorbs the walker)
            // +25 (Phase 1 batch 2): owns() + the curated construction set
            // +~50 (Phase 1c endgame): chainBottom moved in from deleted
            // ResultNav (this predicate is the walker's last consumer)
            // 196→199 (audit slice 3): the nav walker recognizes BOTH
            // toOne spellings inline (invariant 6d keeps exec off the
            // frontend) — recognition lines, not evaluation.
            Map.entry("core/src/main/java/com/legend/exec/StoreNav.java", 199),
            Map.entry("core/src/main/java/com/legend/exec/DynamicPivot.java", 118),
            // Phase 1c endgame: the boundary resolver (stamp + marker
            // substitution over stamped schema — the DynamicPivot model;
            // audit 2026-08-18 Tier-3: size-pinned so the resolver never
            // silently grows back into a recognizer)
            // 248→237 (2026-08-19 P3-1): the static at-pick DELETED — a
            // resolver-side duplicate of the lowering's collection-at rule
            // (one owner: the compiler lowers at()); referee identical.
            // 237→261 (2026-08-19 P3-2 single-query): the DEMAND GATE —
            // the LIMIT-0 probe now runs ONLY when a columnNames/values
            // read statically demands the schema; an undemanded grid is
            // ONE query (egress adopts result headers). The growth is
            // demand ANALYSIS (a tree scan), not evaluation — it exists
            // to DELETE a whole query from the common path.
            // (RawGridSchema row RETIRED 2026-08-21: the rewrite pass
            // moved to resolver/ under Invariant 7's structural guard —
            // staged compilation; only the LIMIT-0 probe stays exec-side,
            // pinned below as GridProbe.)
            // 48 -> 52 (§4bZ-U follow-on, JUSTIFIED): probeTypedColumns
            // maps the LIMIT-0 metadata's SQL type names to Pure columns
            // for the boundary resolver's oracle — SCHEMA plumbing (the
            // probe's own metadata, one lookup per column), zero value
            // evaluation; it EXISTS to delete the Any-wildcard stamp so
            // late-bound cells type and the wire ledger burns.
            Map.entry("core/src/main/java/com/legend/exec/GridProbe.java", 52),
            // Phase 2: the comparison layer, size-pinned at its landing
            // 212 -> 221: assertEqWithinTolerance MIGRATED IN from the
            // harness arm (net move, not new evaluation)
            // 221→250 (2026-08-19 phase-2 deep audit): EXACT-arithmetic
            // tolerance (P2-1 — the double round-trip silently widened
            // it), temporal sort BY INSTANT (P2-2 — contract said
            // instant, code said text), assertEq with LOUD non-primitive
            // refusal (P2-5 — eq is identity, unobservable on a wire),
            // tree arms delegate to JsonCompare (P2-4/P2-6).
            // Adjudication-layer correctness, not new evaluation surface.
            // 250→264 (2026-08-19 Clause-2c redesign — the K-arm's honest
            // wire crossing exposed World-1 completeness gaps): integral×
            // Decimal equality is NUMERIC (spec witness testIntToDecimal),
            // the temporal string-carrier bridge is SYMMETRIC (the
            // designed partial-precision carrier sits on either side),
            // OffsetDateTime joins repr. Adjudication-layer correctness.
            // 264→298 (2026-08-19 standard-suite burn): assertInstanceOf's
            // World-1 adjudicator — the RUNTIME carrier kind against the
            // named type up the m3 value lattice (carrierTypeName is the
            // decode table). Spec witness testHashCode; the pure body is
            // unportable m3 reflection (elementToPath). Adjudication over
            // a DB-produced value, never evaluation.
            // 298→311 (2026-08-20 host-logic audit): temporalEquals
            // restructured to SPEC instant equality — pure DateTime
            // equality is instant-based and a naive DateTime means UTC
            // (parseDate.pure's own expectations), so an offset-bearing
            // EXPECTED literal string compares by instant against the
            // naive-UTC wire carrier. World-2 consistent BY CONSTRUCTION
            // now: parseDate normalizes to the same convention AT
            // EMISSION (the compensating utcLocal arm was deleted the
            // same day). On the audit's compile-through eviction path
            // (docs/HOST_LOGIC_AUDIT_2026_08_20.md fix queue 3).
            // 311 -> 313 (metamodel-as-relations batch 6, 2026-09-02):
            // toRepresentation's INSTANCE form (toRepresentation.pure:28,
            // `<id instanceOf T>`) for a FAILED assert's message over
            // instance rows — spec message text, no verdict is computed
            // here (the verdict rode the database; the message used to
            // wall "toRepresentation for LinkedHashMap is not modeled").
            Map.entry("core/src/main/java/com/legend/exec/PureAsserts.java", 313),
            // NEW ROW (2026-08-19 Clause-2c redesign): the K-arm —
            // assert-family VERDICT dispatch (World 1). Arguments execute
            // in the database (StatementExecutor.evalValue); this file
            // only routes members to PureAsserts and judges the quantified
            // boolean vector. Adjudication orchestration, never evaluation.
            // 221→246 (2026-08-19 standard-suite burn): the assertInstanceOf
            // arm — the RUNTIME carrier kind adjudicated against the named
            // type (spec witness testHashCode; the spec body needs
            // elementToPath, unportable m3 reflection). Verdict dispatch,
            // never evaluation — the instance still computes in the DB.
            // 246→300 (2026-08-19 relation-suite landing): the GRID VERDICT
            // arm — assertTdsEquivalent executes BOTH relations in the
            // database and cell-zips host-side via TdsCompare (Clause
            // 2c's chartered route; the 79-row relation witness). Verdict
            // orchestration over DB-produced frames, never evaluation.
            // 300→304 (2026-08-19 burn slice 1): the side-flatten's
            // ONE-CARRIER normalization — raw JDBC array elements arrive
            // as driver temporals; java.time is the platform convention
            // (the invisible-diff bug: a Timestamp reprs identically to
            // the LocalDateTime it never equals). Decode, not evaluation.
            // 304→398 (2026-08-20 slice 11): the assertIs IDENTITY verdict
            // (is.pure:23 "pointer equality", PCT.platformOnly). Identity
            // is NOT DB-computable by definition — a wire carries values,
            // never references (the eq/equalNonPrimitive irreducible
            // ruling) — so the K-arm adjudicates the STATICALLY-identified
            // cases in World 1: type refs (bare element / type() /
            // genericType().rawType, one canonical spelling) and same
            // let-bound instance provenance. Verdict adjudication of
            // compile-time facts, never value evaluation; every other
            // shape falls through to is()'s missing SQL rule and walls.
            // 398→455 (2026-08-22 R2a, CANONICAL_FORM_SPEC §0): the BYTE
            // VERDICT OF RECORD — sqlByteVerdict routes scalar-kind
            // assertEquals verdicts to DB-COMPUTED canonical renders
            // (CanonicalRenderSql; Java compares two byte strings) with
            // the host lattice demoted to the permanent parallel
            // referee. The growth is verdict ROUTING toward the
            // database — the ledger's own direction — plus the counted
            // decline/divergence census hooks; zero new value
            // evaluation.
            // 455→486 (2026-08-22 V4/V5): assertSameElements (canonical
            // ORDER BY multiset byte verdict) and assertEq join the DB
            // verdict of record — same routing shape, same census.
            // 486→573 (2026-08-22 V6): the decline burn — PAIR RULES for
            // pure's NON-TRANSITIVE numeric equality (stamp-certain
            // int×float static FALSE; Decimal/refined pairs by VALUE
            // spelling), the mixed-kind-collection gate (a ROUTING fact
            // read from host-fetched kinds — SQL column promotion erases
            // element kinds), decline REASONS + verdict detail into the
            // census. Verdict routing + diagnosis text, zero value
            // evaluation; declines fell 207→97.
            // 573→612 (2026-08-22 V6 round 2, user-caught): the CIRCULAR
            // plan-type refinement DELETED (OutputCol.type() is
            // stamp-derived — it carried no information), replaced by
            // RUNTIME-KIND refinement from the fetched values — pure's
            // own Number-equality dispatch. Routing + kind classification
            // over DB-produced values; zero evaluation.
            // 612→628 (2026-08-22 X-slice close): the USER-RATIFIED
            // 2-ULP dialect-arithmetic policy arm on the byte verdict —
            // runtime-kind refinement widened the byte channel's claim
            // into territory the unrefined-NUMBER decline used to route
            // to the host policy; the policy must ride the claim
            // EXPLICITLY (sqlUlpPolicy census) or it silently retires.
            // PureAsserts owns the tolerance; this only vectorizes the
            // pair check + counts. Zero value evaluation.
            // 628→675 (2026-08-22 V11 single-query canon): candidate-
            // column SELECTION (runtime kind picks which DB-computed
            // render judges) + renderSide FRAMING (spec separators over
            // DB-computed element texts) moved here when the canon
            // collapsed into the side query. In exchange the WHOLE
            // prepCanon/runCanon execution arm left StatementExecutor
            // (−402): the verdict system's total surface SHRANK 355.
            // The DB still computes every element text and the
            // canonical order — framing writes '[', ', ', ']' only.
            // 675→728 (2026-08-22 X5, equality.Key): the keyed-instance
            // rules at the verdict layer — instanceKeys (the pair's
            // shared key tree from the MODEL), restrictToKeys (the
            // engine's keyed-equality relation applied as evidence
            // projection before EITHER channel judges — EqualityUtilities
            // compares key properties ONLY), the Nil/empty kind-gate
            // bypass, and the '[]' empty-canon unification. Model-driven
            // routing + projection; zero value evaluation — the DB still
            // computes every render (instanceCanon, lowering-owned).
            // 728→787 (2026-08-22 F13, synthetic instance identity):
            // the IDENTITY-pair guards at the verdict seam — the v1
            // lambda-exclusion scan (a keyless ctor under a lambda
            // mints ONE site id for many evaluations — decline,
            // counted) and the identityless-wire decline (an instance
            // map with no __id must never byte-judge). Routing +
            // decline classification; the DB still computes every
            // identity compare (the {_type,_id} canon, lowering-owned).
            // 787->790 same slice: Map-carrier exemption (mapEquals is
            // F12's claimed rule, not an identity pair) + the shared
            // SYNTHETIC_ID spelling constant.
            // 790->796 (2026-08-23 F13c): the assert-CONDITION sides ride
            // the identity lane (identitySide — an evalValue flag, zero
            // evaluation; the in-SQL eq/equal arm needs instance identity
            // and the boolean egress keeps other lanes blind).
            // 796->829 (2026-08-23 F10 v1): the literal-channel SELECTION
            // (Any-involving pairs pick the literal candidates; the
            // mixed-numeric gate exempts JSON-carried sides — no
            // promotion), the tree-marker decline, and the anyAny gate
            // bypass. Routing + decline classification; the DATABASE
            // computes every literal render (anyJsonCanon,
            // lowering-owned).
            // +5 (B8): carrier-vs-kind resolution in selectedFineKind — a
            // runtime BigDecimal under a candidate set that rules
            // pure-Decimal OUT is a decimal-carried FLOAT (static truth
            // gates; the value never decides a kind alone)
            // +6 (2026-08-28 V7 batch 1): the side-size histogram hook in
            // decodeSide (CanonicalDivergence.v7SideRows) — measurement
            // only, the V12 VALUES-cost bracket; no evaluation added
            // 840->878 (V7 batch 2): SpliceHook threading through every
            // side evaluation (routing, not evaluation) + two byte-
            // channel adjudications the dual-verdict alarm caught: the
            // enum-under-Any NAMED decline and the DECLARED TDSNull-
            // sentinel policy arm (the 2-ULP shape — a counted policy
            // row, the LATTICE still owns the rule; the walker only
            // detects the golden's sentinel spelling)
            // 878->1145 (V7 batch 2 slice 2, D3): the ORDER-VIEW walker
            // (SORTED/INCIDENTAL/DEFINED — routing facts, the audited
            // harness list moved verbatim), the rendered-text and
            // grid-pair ARMS (the DATABASE still renders/computes every
            // side; the arms only ROUTE to the one comparison owner,
            // TdsCompare), and the forAll-contains subset fold (both
            // sides DB-computed, membership judged by the lattice — the
            // interpreter-free quantified shape). Routing + dispatch;
            // the burn target is the HARNESS lattice this replaces
            // wholesale at cutover.
            // 1145->1210 (slice 3a): assertSize's per-result-kind size
            // rule (envelopeCarriers is the MODEL's; the arm only keys
            // the read shape) + the assertContains membership arm (both
            // sides DB-computed, lattice judges). Dispatch, not eval.
            // 1210->1240 (slice 3b, D4): the assertJsonStringsEqual
            // verdict arm (sides DB-computed; JsonCompare — the V3
            // register's one tree walker — judges) and the Graph-side
            // decode convention (the DB-built JSON array's elements,
            // the harness Eval rule moved to the owner). Dispatch +
            // decode routing, not evaluation.
            // 1240->1480 (V7 §8 leg 1, user-ratified grid canon): the
            // FLAT-CELLS verdict arms — the DATABASE computes every
            // row's canonical text (wrapTdsCanon rides the side
            // query); Java's share is framing (chunking peer element
            // canons by width — separators only), the host-referee
            // dispatch, and counted declines. The comparison POLICIES
            // stay with TdsCompare/PureAsserts; nothing here renders
            // or evaluates. Burns the 353-decline wall class.
            // 1480->1522 (leg 1 alarm diagnostics + declared policy):
            // the dual-verdict disagreement detail carries the FIRST
            // DIFFERING canon pair (the reverted attempt's lesson — a
            // bare host/sql flag cannot be diagnosed), and the
            // declared 2-ULP dialect-arithmetic policy gets its grid
            // arm (positional cell gate; PureAsserts OWNS the
            // tolerance — this vectorizes it, counted in the policy's
            // own census row). Reporting + policy gate, zero
            // evaluation. (+3: the witness message folds its first
            // line so the census row carries the expected/actual
            // payload — the same diagnosability rule.)
            // 1525 -> 1401 (user ruling 2026-08-28, consolidation):
            // the grid comparison POLICIES moved to their owner
            // (TdsCompare — see its row); this file keeps dispatch
            // only. Shrink banked.
            // 1401 -> 1394 (D1, arch-audit): five hand-copied
            // verdict/message/probe tails become ONE finisher with the
            // structural coupling invariant. Shrink banked.
            // 1394 -> 1405 (leg 2/3): the JSON side reader — a GRAPH
            // result's DB-built envelope IS the String[1] document
            // (leg 2 typed it so); decode routing, zero evaluation.
            // 1405 -> 1418 (disagree-9 burn): valueRead, the host-twin
            // wire decode. 1418 -> 1408 (Java-eval retirement, SAME
            // DAY, user challenge honored): valueRead DELETED — the
            // decode rides the FETCH (wrapTdsCanon conforms the grid
            // plan on the GRID_FETCH lane; the executor's label-driven
            // unwrap hands BOTH channels the decoded value). Honest
            // net vs the 1405 day-start: +3 — the EVALUATION (+13
            // decode) is fully deleted; the +3 is renderedArm's
            // both-rendered dispatch (comparison-policy routing, the
            // header's permanent-allowed class), while disagree
            // burned 9 -> 0.
            // 1408 -> 1436 (harness-deletion item 1, slice 2): the
            // ASSERT-VERDICT LISTENER wrapper — tryAdjudicate reports
            // each owned verdict (pass, or the raised detail) to an
            // observer the runner supplies. OBSERVATION, zero
            // evaluation: judgment stays in adjudicate()/PureAsserts
            // untouched; the seam exists so the harness's THIRD assert
            // implementation (checkAssert's walk) can DELETE — the
            // whole-test flip scores from these events instead of
            // re-implementing assert semantics.
            // 1436 -> 1441 (SQLTEXT slice 3a, 2026-09-01): the
            // five-line dispatch hook handing the sql-text shape to
            // SqlTextVerdicts (its own ledger row below) — routing
            // only, no evaluation, no judgment change for other arms.
            // 1441 -> 1450 (charter §8.3d): the dual-golden root
            // dispatch hook (9 lines — exact-FQN match + hand-off to
            // the arm, the assertSameSQL hook's sibling).
            // 1450 -> 1459 (TDG scoring flip): the assertSqlEquals
            // root dispatch hook (exact-FQN match + hand-off).
            // 1459 -> 1511 (batch 28, 2026-09-03): the UNROLLED quantified
            // verdict — `[pairs]->map(p| lets...; assertEquals(e, a, fmt,
            // args))->distinct() == [true]` (the toSQLString dialect-table
            // idiom) unrolls per literal element through the inliner
            // (VerdictQueries.unrolledElement mints the nodes) and each
            // element's assert adjudicates through the EXISTING arms over
            // database results — a verdict SHAPE, no host evaluation
            // 1511 -> 1529 (batch 53, 2026-09-03, WORLD_MAP §4): the keyed
            // K-arm restricts a POLYMORPHIC key slot's values by their own
            // class (restrictNested reads the wire's __type) — the engine's
            // equal() rule for RelationalOperationElement[*] keys, model-
            // driven evidence projection, no value computed; the side-stamp
            // narrowing itself lives in the compiler layer
            // (ExecuteChainAssembly.narrowSideStamps)
            // 1529 -> 1568 (batch 54, 2026-09-04): a wider-DECLARED side
            // (a program's Node[1]) is narrowed by its wire __type before the
            // key tree judges; a class-kind side that rode a JSON carrier
            // decodes to its structure — both are evidence PROJECTION for
            // the key verdict (WORLD_MAP §4), no Java value computation.
            // 1568 -> 1576 (batch 55a): assertInstanceOf over a CLASS value
            // reads the wire's __type up the model's subtype relation (the
            // harness's NodeH string-match arm is deleted with the walk) —
            // a compare on the wire's classifier, no value computation.
            // 1576 -> 1595 (batch 58, the H2VERSION decision): an if whose
            // BRANCHES are asserts (assertEqualsH2Compatible's body) — the
            // condition is a value QUERY the database evaluates
            // (StatementExecutor.evalValue); Java only dispatches the taken
            // branch to the existing verdict arms. No value computation.
            // 1595 -> 1599 (batch 59, the lineage-tree row verdict): the
            // seam dispatches the assert to LineageTreeVerdicts (its rows
            // compute in the database) — dispatch only.
            // 1599 -> 1605 (batch 72a, 2026-09-05): the JSON verdict names
            // the GOLDEN side when the strict parse refuses it ("golden JSON
            // does not parse") — a message wrapper for the engine-golden-
            // defect register (the `]"` graphFetch goldens); no evaluation.
            Map.entry("core/src/main/java/com/legend/AssertVerdicts.java", 1605),
            // NEW ROW (2026-08-19 cross-phase audit E.2): the
            // K-ORCHESTRATOR itself. Not host evaluation — statement
            // routing, session plumbing, verdict dispatch — but it
            // absorbs by design, and absorption that should have been
            // COMPILATION is exactly what a silent-growth watch catches.
            // Shrink-only like every row; bump with written justification.
            // 2695→2724 (2026-08-19 deferred-TDS): the orchestrator's share
            // of the dynamic-pivot toString — supplies the LIMIT-0 probe
            // to the LOWERING-owned composition pass (invariant 6d kept
            // the layers honest: exec never calls the middle-end, so the
            // orchestrator bridges with a probe function). Plumbing, not
            // evaluation — the '#TDS' text still composes IN SQL.
            // 2724→2728 (2026-08-20 Row-vs-Relation model B): table
            // tests spell Type.isRelation/relationSchema on the wrapped
            // form — multiline type-spelling only, zero new evaluation.
            // 2728→2363 BANKED DOWN (2026-08-22 V11): prepCanon/runCanon
            // and their records DELETED — the canon rides the side query
            // itself (wrapWithCanon), one execution per side; the
            // residual +37 over the first cut is the decline tunnel
            // (wrapped→bare→fold, the designed sentinel chain).
            // 2363→2368 (X5): the driver resolves the key tree from the
            // model for the wrap (five lines of ctx plumbing).
            // 2368→2385 (F13): identity threading — the ExecEnv carries
            // the per-env site-id minter (one InstanceIds shared by both
            // verdict sides), and lowerAndPrepare selects the identity-
            // bearing layout on the rider lane only (golden-SQL text
            // lanes stay unperturbed). Plumbing, zero evaluation.
            // 2385->2402 (F13c): identity-flag threading (evalValue/
            // executeTyped overloads) + the keys-resolver handle to the
            // Lowerer — plumbing, zero evaluation.
            // 2402->2423 (F10 v1): the canon-exec tunnel's middle rung
            // (re-wrap without the literal candidate — bare byte
            // verdicts survive a lying stamp). Orchestration, zero
            // evaluation.
            // 2423->2456 (metamodel-store leg 2026-08-28): the seed
            // hook at the one execution-setup owner — a body reading
            // the system store gets its registry extent seeded (Ddl
            // renders, the database evaluates; the extent query itself
            // is ordinary lowered SQL). Setup orchestration, zero
            // evaluation.
            // 2456->2469 (V7 batch 2): the evalValue hook overload —
            // the statement loop's envelope-splice hook threads into
            // verdict side evaluation. Orchestration, zero evaluation.
            // 2469->2472 (V7 batch 2 slice 2): DriverPkAppend applies in
            // the verdict side lane exactly as in the generic statement
            // path (the option is EXECUTION ENV — the validation-family
            // probe caught the missing ID column). Parity, not eval.
            // 2472->2482 (V7 §8 leg 1): TABULAR-shaped rider routing —
            // the executor picks the GRID canon wrap from the declared
            // result shape (static, pre-execution); the database
            // renders the row canons. Routing, zero evaluation.
            // 2482 -> 2513 (sql-producer leg slice 1, task #13): the
            // Frames.relationalActivitySql door — the activity log's
            // RelationalActivity.sql answered from the frame's OWN query
            // via the SAME engineSql render toSQLString uses (a COMPILER
            // fact retained and served, no evaluation; ExecFrame keeps
            // its source execute call for it)
            // 2513 -> 2520 (slice-3 equality half): the renderSqlText
            // Frames door — toSQLString folds POSITION-INDEPENDENTLY at
            // the splice through the SAME toSqlString K-arm (a render
            // reuse, no evaluation added; burned 94 backlog rows whose
            // asserts were walled behind statement-root-only dispatch)
            // 2520 -> 2524 (§9a transcript-fidelity fix): the schema
            // K-arm records its executed statement on the MAIN replay
            // channel too — the metadata-only recording was editing a
            // corpus-authored statement out of the mirror's ledger
            // (root cause of the 9-test experiment residue). RECORDING,
            // zero evaluation.
            // 2524 -> 2541 (§10m ladder completion): the EFFECT-arm
            // REGISTRY (method-reference map + loud-miss + the two
            // governance accessors NativeRegistryGovernanceTest pins
            // against the catalog) replacing SIX scattered silhouette
            // arms + one raw-string-literal dispatch. Table + accessors,
            // zero evaluation logic added; the growth is the price of
            // registry-equals-catalog enforcement.
            // 2541 -> 2566 (harness-deletion item 1, slice 2): ExecEnv
            // carries the AssertListener + the execute() listener
            // overload. Plumbing for the runner's scoring seam — no
            // evaluation, no routing change; run-scoped fact rides the
            // env (never a static sink).
            // 2566 -> 2571 (SQLTEXT charter §2, slice 1 2026-09-01):
            // ExecEnv carries the SqlReplayOracle beside the listener
            // (same seam, same nullable carriage, same env-not-static
            // discipline). Registration plumbing only — no evaluation.
            // 2571 -> 2594 (metamodel-as-relations group F burn,
            // 2026-09-02): ExecEnv carries the resolver's side-output
            // seed rows (constructed metamodel instances as rows) and
            // the execution setup seeds them after the model's own —
            // the same env-not-static carriage; no evaluation.
            // 2594 -> 2680 (harness burn-down batches 14-15, GROUP D —
            // the router's string entry, 2026-09-03): ORCHESTRATION only —
            // the executeLegendQuery result frame (vars bound as lets, the
            // envelope over the chain, the activity text render, runtime
            // setups for inline frames), the statement-sequence route for
            // helper bodies with non-let intermediates, and the post-inline
            // adjudication of a string-entry read. Value evaluation still
            // lowers to SQL (TypedJsonResult / TypedJsonAccess); nothing
            // here computes a value host-side.
            // 2680 -> 2692 (batch 16, group D remainder): the from() node's
            // testDataSetupCsv FACTS become seed SQL at connection
            // establishment (CsvSeed against the store) — the executor's
            // half of the CSV route; the compiler only records the facts.
            // 2692 -> 2696 (batch 30, 2026-09-03): an effectful HELPER's
            // effect-free VALUE (let runtime = initDatabase(): DDL effects,
            // then ^Runtime(...)) binds as the let would have — a frame for
            // an execute() value, a plain let otherwise; orchestration
            // (binding), no host evaluation
            // 2696 -> 2699 (batch 40, 2026-09-03): planToString over a TDG
            // plan carrier routes to TestDataGenerationNatives.planTextResult
            // (the platform's own printer) — three routing lines, no
            // evaluation.
            Map.entry("core/src/main/java/com/legend/StatementExecutor.java", 2699),
            // NEW (SQLTEXT charter slice 3a, 2026-09-01): the sql-text
            // verdict arm — detection (typed-node + exact FQN),
            // four-artifact sequencing through evalValue and the
            // oracle SPI, outcome judgment + the emission census.
            // Zero evaluation: every text and every row computes in
            // the database; the §4 residue arms judge TEXT equality of
            // two DB-computed strings (Clause 2c judgment).
            // 145 -> 156 (same slice): the LET-AWARE producer chase
            // (the platform keeps lets as lets; measured — 44 flipped
            // tests all missed the arm until it landed), the arm-fired
            // probe marker, and the rows-null decline path; the
            // TypedFrom mint moved OUT to VerdictQueries (Invariant 7).
            // Navigation and outcome routing only — zero evaluation.
            // 156 -> 161: the OUR-ROWS leg is REFEREE-CLASS execution —
            // wire-census suspension with save/restore so the arm's
            // derivation plans never move the primary lane's shrink-only
            // ceilings (and an outer probe's suspension survives).
            // 161 -> 210 (charter §8.3b, the ~750-test assert-form
            // cohort): the assertSameSQL ROOT arm — the statement root
            // arrives pre-inline, OUR TEXT/ROWS are VerdictQueries
            // mints answered by the envelope splice, the rows leg +
            // verdict policy extracted to ONE shared tail both arms
            // use. Navigation, minted-read sequencing and judgment
            // only — zero evaluation (the judgment-vocabulary pin
            // covers this file mechanically).
            // 210 -> 293 (charter §8.3c): the exec-sql-read arm —
            // tryArmExecRead + the let-aware findSqlRead walk (exact
            // splice FQNs, first-statement forms only). Same tenet
            // argument as the file's header: navigation + sequencing +
            // judgment; both texts, our rows and the golden replay all
            // compute in the database. The judgment-only vocabulary
            // ban (verdictFilesJudgeOnly) still binds every line.
            // 293 -> 344 (charter §8.3d): the dual-golden arm
            // (tryArmH2Compat — replays the UPGRADED golden, the same
            // choice the engine's own version dispatch makes on our
            // oracle; both actual spellings owned via the existing
            // findSqlRead chase). Same class: detection + sequencing +
            // judgment; the vocabulary ban binds every line.
            // 344 -> 354 (§8.3e hardening): recognized-but-
            // underivable SQL shapes WALL counted instead of falling
            // through to a generic TEXT verdict (the charter's one
            // forbidden outcome) — guard growth, zero evaluation.
            // 354 -> 474 (charter §5 first cut): the plan-text arm
            // (tryArmPlanText + findPlanProducer) — referee bindings
            // are MINTED LETS (VerdictQueries.refereeBindings), the
            // rows leg is the lambda through the one router with those
            // lets, the golden replays hole-filled through the oracle.
            // Detection + sequencing + judgment; the vocabulary ban
            // binds every line; unbindable/operation-hole shapes WALL
            // counted.
            // 474 -> 585 (TDG scoring flip): tryArmTdgSql /
            // tryArmTdgRoot + hasTdgProducer — the fetch-text verdict
            // rides the SPI (verifyFetchTexts); both sides execute in
            // their databases, this file sequences and judges. Same
            // class, same vocabulary ban.
            // 585 -> 592 (TDG arm reach, 2026-09-01): hasTdgProducer
            // recognizes the generator carrier's FOLDED form (the
            // TestDataGenResult literal the orchestrator leaves in the
            // let) and the TDG door moves AHEAD of the exec-read claim
            // in tryArmExecRead (the old in-branch copy deleted — one
            // door). Routing + recognition only: both sides still
            // execute in their databases through verifyFetchTexts; no
            // judgment or evaluation added.
            // 592 -> 628 (batch 38, 2026-09-03): the exec-read rows leg
            // recovers the executed FRAME's mapping and root class for the
            // oracle's enum decode (frameMappingAndClass: the let-chase,
            // then the splice hook's activities read to the frame's own
            // execute() call) and the general arm let-chases a toSQLString
            // query lambda. Recognition + routing only: the oracle still
            // executes both sides in their databases; no judgment added.
            // 628 -> 657 (batch 39, 2026-09-03): a plan-text golden replays
            // its ONE `sql =` node (planReplaySql: recognition of the plan
            // text's sql line; a multi-node plan stays a counted text
            // contract) — routing only; the oracle still executes both
            // sides in their databases.
            // 657 -> 669 (batch 42, 2026-09-03): the exec-read frame facts
            // carry the STATIC extent-subset fact (VerdictQueries.extentSubset
            // over the typed chain — the compiler layer owns the walk) to
            // the oracle's pk-collapse — routing of a fact, no evaluation.
            // 669 -> 690 (batch 52, 2026-09-03): the golden-vs-render arm
            // accepts the RUNTIME overload of toSQLString (dialect read from
            // the connection, the runtime's post-processor hooks applied to
            // the rows leg through the env's tableReplace channel) — the
            // same recogniser the frame path uses; no evaluation moved here.
            // 690 -> 765 (batch 64, 2026-09-04): the chained generator-fetch
            // arm ADDRESSES a hop ($testData.sqls->at(i) → index + the
            // let-bound generator node) and hands the oracle SPI the
            // generator re-run as a supplier in the SPI's own transcript
            // terms — routing of a fact to the referee; the replay, temps
            // and compare live in the oracle.
            // 765 -> 830 (batch 65, 2026-09-04): the inline in-list temp
            // table — the arm reads the query's in([...]) literal (typed
            // literals, no evaluation) into a TempTable spec for the oracle;
            // the frame's query rides FrameFacts to the rows leg.
            // 830 -> 842 (batch 66, 2026-09-05): the plan-text arm hands a
            // multi-node plan and the referee bindings to the oracle's plan
            // replay (verifyPlan) — routing; the lambda's leading lets scope
            // the rows leg; the hop finder sees through sqlRemoveFormatting.
            // 842 -> 919 (batch 67, 2026-09-05): the exec-read arm ADDRESSES
            // the engine's two-statement in-list plan — the population
            // statement's rows leg is the query lambda's let expression, the
            // main statement's temp is a "population" TempTable spec for the
            // oracle; the n>0 read is owned for that shape. Routing of typed
            // facts; the replay and the compare stay in the oracle.
            // 919 -> 1011 (batch 69a, 2026-09-05): the engine's plan is one
            // statement per store-backed LET of the query lambda — golden(k)
            // routes to let k's own rows (statementRoute/statementLets),
            // shared by the exec-read arm and the H2Compatible arm (its
            // verdict tail extracted so the n-th read routes there too).
            // Routing of typed facts; the replay and compare stay in the
            // oracle.
            // 1011 -> 1035 (batch 69c): the engine's chained-plan warning
            // line is stripped off a golden before replay (a spec-text
            // shape), and the toSQLString arm takes MULTI-STATEMENT lambdas
            // — golden(0) routes to statement let 0's rows, the lets scope
            // the rows leg. Routing; the replay and compare stay in the
            // oracle.
            Map.entry("core/src/main/java/com/legend/SqlTextVerdicts.java", 1035),
            // NEW ROW (batch 59, 2026-09-04): the lineage-tree verdict arm —
            // the scanRelations sibling of SqlTextVerdicts: both prints
            // become rows through one DATABASE query (TREE_ROWS) and the
            // row lists compare; no value computation in Java.
            Map.entry("core/src/main/java/com/legend/LineageTreeVerdicts.java", 116),
            // NEW (same audit): the structural tree walker — replaces the
            // harness's private copy; verification CONSUMES two produced
            // sides, never produces a result
            Map.entry("core/src/main/java/com/legend/exec/JsonCompare.java", 70),
            // 295 -> 431 (V7 §8 leg 1 + user consolidation ruling
            // 2026-08-28): the GRID-CANON byte-channel policies land
            // with the OTHER grid comparison rules — row/cell canon
            // extraction, peer framing (separators only), the 2-ULP
            // grid gate (PureAsserts owns the tolerance), the alarm's
            // first-diff payload. Comparison layer (consumes two
            // sides, never produces a result — the header's
            // permanent-allowed class); the paired AssertVerdicts row
            // SHRANK 1525 -> 1401 in the same slice.
            // 431 -> 444 (charter §8.3b): TREE_MARKER decline-on-sight
            // at both TDS canon doors (markers are never comparable —
            // the F10 contract enforced where the cell is claimed) +
            // threading the chain's sortedness/form into the byte
            // channel's grid probe (the wobble burn — unordered grids
            // judge under the verdict's own declared row-multiset
            // policy). All comparison-layer judgment: consumes two
            // produced sides, never produces a result.
            Map.entry("core/src/main/java/com/legend/exec/TdsCompare.java", 444));
    // Phase 1c: DbMetaData MOVED OUT of the evaluator surface — its
    // content was always pure catalog-SQL composition (zero JDBC), now
    // compiler/spec/CatalogGrids (the Typer's fetchDb retype needs the
    // registered catalog SQL; Invariant 6e forbids compiler->exec)
    // E4.b LANDED (2026-08-17): DbMetaData's row is RETIRED — the
    // shadow-H2 replay is DELETED and every metadata VALUE is now
    // database-produced (catalog queries over the AMBIENT session's
    // information_schema, F6.6's rule; identifier columns upper()'d in
    // SQL for the H2 engine-parity spelling). The residual file is
    // catalog-query ORCHESTRATION + egress decode by contract — the
    // decision rule's permitted classes. (E4.e's fold/at interpreter
    // residue DIED with HostEval's Phase-1 eviction — grids are typed
    // relations now; its stale ledger row was deleted 2026-08-19.)
    // E5 wire rows LANDED (2026-08-17): the product wire is
    // PLAN-RENDERED (Compiler.executeWire → WireRender → Render
    // csvWire/jsonWire — the DB composes the bytes through the ONE
    // RFC-4180 owner and its own json_object policy). ResultJson is
    // DELETED (the Java JSON value policy died with it; streaming
    // writes plan-rendered row texts plus array punctuation only);
    // CsvSerializer/JsonSerializer shrank to format METADATA
    // (id/contentType/streaming capability — no serialize method
    // exists on the registry surface anymore), so their size rows
    // are retired rather than pinned.

    /** NAME rows (surgical surfaces inside shared files): explicit
     * name-family regex, EXACT pinned occurrence count (definitions and
     * call sites both — a stable proxy; any drift is a conscious
     * decision). */
    private static final Map<String, Object[]> EVICT_NAMES =
            new LinkedHashMap<>();

    static {
        // E4 — StatementExecutor's walk family. 42→40 (2026-08-21):
        // activityEnvelopeRead moved to compiler.spec.ResultEnvelopeSplice
        // (splice-ownership leg slice 1, Invariant 7) — there it is a
        // private REWRITE RULE; the Java-side derivation it asks for
        // (AggAwareActivities.rewrittenQuery) stays executor-side behind
        // the Frames SPI and stays on this ledger's radar.
        // (batch 18, 2026-09-03 — plan nodes as rows: the plan model's
        // ROWS registration lives in PlanAllocations; this row's count
        // stands at 40, the walk's own planModel call retires with planWalk.)
        // 40 -> 5 (batch 55a, 2026-09-04): the walk family is DELETED —
        // planWalk/walkProp/walkFilter/walkResult/constructNode/constructOp/
        // nodeValue/typeRefSimple are gone with MetamodelWalk/MetamodelSteps;
        // what stands is the plan MODEL (planModel, planConnOf,
        // connectionStoreElementOf) that PlanRows turns into rows.
        EVICT_NAMES.put("core/src/main/java/com/legend/StatementExecutor.java",
                new Object[]{"(planWalk|walkProp|walkFilter|walkResult|planModel|planConnOf|constructNode|constructOp|nodeValue|typeRefSimple|activityEnvelopeRead|connectionStoreElementOf)\\(",
                        5});
        // E4.d batch 1 LANDED (2026-08-17, user-ratified "engine-exact
        // text is a lower TARGET"): the second DDL speller is DEAD —
        // dropTableStatementText/createTableStatementText/engineSpell
        // merged into the ONE generator (Ddl.createTable + Flavor
        // {H2_EXEC, DUCK_EXEC, ENGINE_TEXT}; the flavored type spelling
        // is the only per-target delta). This row pins the dead names
        // at zero. setUpDataSqlsText* remain as the engine-text
        // setUpDataSQLs walkers, now composing THROUGH the one
        // generator — engine-golden text of the model's own seed data
        // (compilation-class; asserted against engine goldens).
        EVICT_NAMES.put("core/src/main/java/com/legend/exec/Ddl.java",
                new Object[]{"(dropTableStatementText|createTableStatementText|engineSpell)\\(",
                        0});
        // E2 LANDED (2026-08-17): the host-side row explosion is DEAD
        // — the scalar-stream projection explodes IN SQL (LEFT LATERAL
        // UNNEST at project lowering; probe: ZERO firings on the full
        // sweep). This row pins the deletion: a list cell in a scalar
        // slot is a loud lowering-defect wall now, never a repair.
        EVICT_NAMES.put("core/src/main/java/com/legend/exec/Executor.java",
                new Object[]{"two many-valued TDS cells", 0});
        // E3 LANDED (2026-08-17): the frame is a one-Variant-column
        // VALUES relation — each cell an object's RAW JSON TEXT, every
        // property a typed variant extraction IN SQL (get + to-cast +
        // toOne); the DATABASE does all value interpretation. This row
        // PINS the deletion of the Java realization (classSource /
        // cellText / Json.parseAll — the lossy string grid) at zero.
        // objectTexts residue is SCISSORS: a lexical string-aware brace
        // scan cutting the model-text payload into row spans at plan
        // build; no JSON value ever materializes in Java.
        EVICT_NAMES.put("core/src/main/java/com/legend/resolver/JsonSourceFrame.java",
                new Object[]{"(classSource|cellText)\\(", 0});
        // E5 LANDED (2026-08-17): the testdatagen ROW TEXT is SQL — the
        // cell display casts, the '---null---' token, and the comma
        // joins all ride the projection; Java (csvEnvelope) assembles
        // only the ENVELOPE from catalog metadata (schema/table/header
        // lines, table separators) and appends DB-produced lines. This
        // row pins the deleted value composition at zero. headerCase is
        // re-registered PERMANENT: identifier-DISPLAY casing over
        // catalog names (the engine's H2 uppercase parity rule) — no
        // value ever flows through it (decision rule: metadata text,
        // census-classified).
        EVICT_NAMES.put("core/src/main/java/com/legend/testdatagen/TestDataGenerator.java",
                new Object[]{"renderCsv\\(", 0});
        // E1 LANDED (2026-08-17): the composition family is DEAD — the
        // PLAN emits the PCT wire text (Lowerer/Render pctTds via
        // PctRender at the execution seam; PCT 1110/1110). This row now
        // PINS the deletion at zero. The adapter-contract RESIDUE moved
        // to the PERMANENT register: createTDSResult (wraps the DB text
        // into the TDSResult CoreInstance), multText (model-source
        // extraction, ingress), stripTrailingZeros (scalar-bridge date
        // instance precision decode), remapErrorMessage (error-text
        // adapter, H4 known weakness documented), reEscapeStringLiterals
        // (interpreter-artifact ingress).
        EVICT_NAMES.put("pct/src/test/java/org/finos/legend/lite/pct/extension/ValueBridge.java",
                new Object[]{"(formatAsTds|formatValue|formatDate|purePctName)\\(",
                        0});
        EVICT_NAMES.put("pct/src/test/java/org/finos/legend/lite/pct/extension/PctExecuteNative.java",
                new Object[]{"(formatAsTds|formatValue|formatDate|purePctName)\\(",
                        0});
    }

    /** The EXEC PACKAGE is a CLOSED REGISTER (Tier-2 audit 2026-08-18;
     * ADVERSARIAL_TENET_AUDIT §3 probe: "new class in com.legend.exec
     * hashing a live cell" landed GREEN — exec had no class-list pin).
     * The egress boundary lives here; a NEW class is a new egress
     * surface and registers consciously. Exact in both directions. */
    private static final java.util.Set<String> EXEC_CLASSES =
            java.util.Set.of(
                    "Column.java", "CsvSeed.java",
                    "Ddl.java", "DynamicPivot.java",
                    "ExecutionResult.java", "Executor.java",
                    // THE SYSTEM DATABASE (user ruling 2026-09-02): the
                    // graph's metamodel rows in a database of their own,
                    // separate from every user connection — opened once
                    // per graph per engine, written once; store-reading
                    // bodies ROUTE to it (StatementExecutor). Egress only:
                    // seed DDL + content-addressed constructed rows; no
                    // value a verdict reads is produced here
                    "SystemDatabase.java",
                    // Phase 2 (One-Platform Plan): THE COMPARISON LAYER —
                    // the platform assert family (spec = legend-pure's
                    // essential/tests/*.pure) + the TDS-grid compare
                    // policies. Verification CONSUMES two produced sides,
                    // never produces a result (the permanent-allowed
                    // decision rule); wireEquals' private copy DIED here.
                    "PureAsserts.java", "TdsCompare.java",
                    // B7 (truthfulness burn): the ONE owner of raised-
                    // message envelope unwrap — provenance-scoped
                    // (sentinel pair), so native errors keep their class;
                    // replaced the adapter's remapErrorMessage AND
                    // AssertErrorNative's broad prefix regex
                    "RaisedErrors.java",
                    // 2026-08-19 phase-2 deep audit: the ONE structural
                    // tree walker (wire-value trees + parsed JSON
                    // documents) — the harness's private jsonDeepEquals
                    // copy DIED here (its claimed ledger exception had
                    // never been registered)
                    "JsonCompare.java",
                    "H2Settings.java",
                    "PctProbe.java",
                    "PctRenderOption.java", "PostProcessBoundary.java",
                    "QueryPlan.java",
                    // Phase 1c: the LIMIT-0 schema probe — the
                    // DynamicPivot.staticize model (a FIRST query pins a
                    // late-bound raw grid's columns; schema read only,
                    // never values). The REWRITE pass moved to
                    // resolver.RawGridSchema (Invariant 7, staged
                    // compilation); only the probe stays in exec.
                    "GridProbe.java",
                    "ResultShape.java", "Row.java", "StoreNav.java",
                    "TimingLedger.java",
                    // R1 (CANONICAL_FORM_SPEC §0): the byte-channel
                    // REFERENCE render + its divergence census. Pure
                    // MEASUREMENT beside the comparison layer — probes
                    // consume two produced sides and count agreement;
                    // neither class can produce a result or affect a
                    // verdict (the probe returns void). R2 moves the
                    // render of record into SQL; these stay as the
                    // permanent parallel-referee half.
                    "CanonicalForm.java", "CanonicalDivergence.java",
                    // V11 (2026-08-22, user-ratified single-query
                    // canon): the rider that carries DB-computed canon
                    // texts OUT of the one side execution — pure
                    // carriage state (candidate kinds + harvested
                    // VARCHAR cells + decline reason); no JDBC, no
                    // evaluation, no verdict logic. It EXISTS so the
                    // second per-side execution (runCanon) could be
                    // deleted — tenet #1's number went DOWN with it.
                    "CanonRider.java",
                    // D4 (arch-audit 2026-08-28, user-ratified): the
                    // DECLINE-TAXONOMY REGISTER — a constants list of
                    // the byte channel's refusal prefixes, guarded by
                    // CanonDeclineTaxonomyTest so a respelled reason
                    // can never silently split a census class. Pure
                    // data: no JDBC, no evaluation, no verdict.
                    "CanonDeclines.java",
                    // F13 (2026-08-22, OPEN_REGISTER): the SITE-ID
                    // minter for synthetic instance identity — an
                    // IdentityHashMap from construction-site NODE to a
                    // deterministic id, scoped to one ExecEnv. Pure
                    // bookkeeping: no JDBC, no evaluation, no verdict —
                    // the id EMITS into SQL as a struct literal (the
                    // database still computes every compare over it).
                    "InstanceIds.java",
                    // TYPED-IR Slice 1 (2026-08-23, user-ratified deep
                    // fix): the LABEL-LIE CENSUS — declared OutputCol
                    // labels vs the bottom-up SqlTyping judgment, per
                    // executed plan. Pure measurement beside
                    // CanonicalDivergence (probes consume a finished
                    // plan and count; nothing here produces a result).
                    "SqlTypeCensus.java",
                    // HARNESS-DELETION item 1 slice 2 (2026-08-31): the
                    // assert-verdict OBSERVER interface — one callback
                    // the runner supplies to score (count/attribute)
                    // verdicts the platform judges. Pure notification:
                    // no JDBC, no evaluation, no verdict logic — it
                    // exists so the harness's own assert orchestration
                    // can delete at the whole-test cutover.
                    "AssertListener.java",
                    // SQLTEXT charter §2 (slice 1, 2026-09-01): the
                    // replay-oracle SPI — one interface the harness
                    // implements (its H2 mirror) and registers on
                    // ExecEnv beside the listener; production
                    // registers nothing and a SQL-text assert walls
                    // loudly. Pure seam: no JDBC of its own, no
                    // evaluation, no verdict logic — rows come from
                    // the ORACLE database, judgment stays in the
                    // verdict layer.
                    "SqlReplayOracle.java",
                    // SQLTEXT slice 3 (2026-09-01): the text-emission
                    // CENSUS — pure counters (LongAdders) the sql-text
                    // verdict arms feed and the runner prints. Under
                    // the row-verdict charter text match/diff is a
                    // census number, never a verdict: nothing here can
                    // affect one (the CanonicalDivergence pattern); no
                    // JDBC, no evaluation.
                    "SqlTextEmission.java",
                    "package-info.java");

    /** THE ORCHESTRATION ROOT, closed (user directive 2026-09-01,
     * SQLTEXT slice 3): the top-level package holds the executor and
     * the verdict arms — exactly where host evaluation likes to creep
     * back in — yet only its NAMED files carried line pins, so a NEW
     * file here grew the host surface with no gate ever seeing it (the
     * scan-boundary hole the new-assert-file question exposed). Every
     * file now registers at birth with its tenet argument, same as the
     * funnel packages; the line ledger above still ratchets the named
     * evaluators. */
    private static final java.util.Set<String> ROOT_CLASSES =
            java.util.Set.of(
                    "AggAwareActivities.java", "AssertErrorNative.java",
                    "AssertVerdicts.java", "Compiler.java",
                    "ConnectionFlags.java", "ConnectionLets.java",
                    "CrossStoreGuard.java", "LiteralFold.java",
                    "NonNull.java", "Nullable.java",
                    // metamodel-as-relations step 3 (2026-09-02): the
                    // system store's SEED derivations — compile-time
                    // facts (mappings, class mappings, include closure,
                    // tables) rendered as ROWS for the database to
                    // evaluate over; no value is computed here that a
                    // verdict reads (tenet #1: Java orchestrates, the
                    // database executes)
                    "MetamodelSeeds.java",
                    // group F burn (2026-09-02): the relational-operation
                    // TREE seeds (mapping / view expressions as node rows,
                    // types stamped) — the same seed-derivation class of
                    // fact as MetamodelSeeds; nothing a verdict reads is
                    // computed here
                    "OpSeeds.java",
                    "PlanAllocations.java", "PlanEnvelope.java",
                    "SeedSqlForms.java",
                    // SQLTEXT charter slice 3a (2026-09-01): the
                    // sql-text verdict arm — typed-tree navigation +
                    // outcome judgment ONLY (Clause 2c); both texts,
                    // our rows and the golden replay all compute in
                    // the database through evalValue and the
                    // SqlReplayOracle SPI. Line-pinned below like its
                    // sibling AssertVerdicts.
                    "SqlTextVerdicts.java", "StatementExecutor.java",
                    // batch 59: the lineage-tree sibling of SqlTextVerdicts —
                    // golden and ours become rows through ONE database query
                    // and compare; typed-tree navigation + judgment only
                    "LineageTreeVerdicts.java",
                    "package-info.java");

    /** The other two funnel packages (documented-debts 2026-08-18,
     * audit item 9's remainder): server and testdatagen may touch JDBC
     * per F1.3, so their class lists close the same way exec's does. */
    private static final java.util.Map<String, java.util.Set<String>>
            FUNNEL_PACKAGE_REGISTERS = java.util.Map.of(
                    "core/src/main/java/com/legend", ROOT_CLASSES,
                    "core/src/main/java/com/legend/exec", EXEC_CLASSES,
                    "core/src/main/java/com/legend/server",
                    java.util.Set.of("ConnectionResolver.java",
                            "DiagramService.java", "Json.java",
                            "LegendHttpServer.java", "OutputFormat.java",
                            "PureLspServer.java", "QueryService.java"),
                    "core/src/main/java/com/legend/testdatagen",
                    // TestDataGenerationNatives (TDG lane S1): the ORCHESTRATION-time fold
                    // of the checker's census CARRIER — computes the census
                    // (this layer owns it) and calls DOWN to the compiler's
                    // literal factory; no evaluation, no JDBC of its own
                    java.util.Set.of("TestDataGenerator.java",
                            "TestDataGenerationNatives.java"));

    /** THE JUDGMENT-ONLY VOCABULARY PIN (user directive 2026-09-01):
     * verdict-arm files may JUDGE database-produced results — equals,
     * boolean logic, dispatch, type classification (instanceof), calls
     * into the chartered judgment owners and the evaluation router —
     * but may not COMPUTE values. The classic eviction-era creep
     * ("the expected side is just 2+3, compute it here") arrives
     * through a small, enumerable API surface: value construction,
     * parsing, numeric/temporal arithmetic. That surface is BANNED AT
     * ZERO with no register to append to — the only fix for a hit is
     * routing the computation to the database. (Known hole, accepted:
     * bare primitive arithmetic is invisible to a call scan; real
     * evaluation reaches for these APIs within a line or two.
     * {@code .add(} is excluded from the arithmetic set — List.add —
     * and covered instead by the construction/parse bans: arithmetic
     * needs a number you cannot mint.) */
    private static final java.util.List<String> VERDICT_FILES =
            java.util.List.of(
                    "core/src/main/java/com/legend/AssertVerdicts.java",
                    "core/src/main/java/com/legend/SqlTextVerdicts.java",
                    "core/src/main/java/com/legend/LineageTreeVerdicts.java",
                    "core/src/main/java/com/legend/AssertErrorNative.java");

    private static final java.util.List<java.util.regex.Pattern>
            EVAL_VOCABULARY = java.util.stream.Stream.of(
                    "Math\\.",
                    "new\\s+(java\\.math\\.)?Big(Decimal|Integer)",
                    "Big(Decimal|Integer)\\.valueOf",
                    "\\.parse(Int|Long|Double|Float)\\(",
                    "(Integer|Long|Double|Float|Short|Byte)\\.valueOf\\(",
                    "(LocalDate|LocalDateTime|LocalTime|Instant|Duration"
                            + "|Period)\\.(now|of|parse|from)\\(",
                    "\\.plus(Days|Months|Years|Hours|Minutes|Seconds"
                            + "|Nanos)\\(",
                    "\\.minus(Days|Months|Years|Hours|Minutes|Seconds"
                            + "|Nanos)\\(",
                    "\\.(multiply|divide|subtract|pow|negate|remainder)\\(")
            .map(java.util.regex.Pattern::compile).toList();

    @Test
    void verdictFilesJudgeOnly() throws IOException {
        StringBuilder drift = new StringBuilder();
        for (String f : VERDICT_FILES) {
            Path p = Path.of("..", f);
            String src = Files.readString(p)
                    .replaceAll("(?s)/\\*.*?\\*/", "")
                    .replaceAll("//.*", "");
            String[] lines = src.split("\n", -1);
            for (int i = 0; i < lines.length; i++) {
                for (var pat : EVAL_VOCABULARY) {
                    if (pat.matcher(lines[i]).find()) {
                        drift.append("\n  ").append(f).append(":")
                                .append(i + 1).append(" matches banned"
                                        + " evaluation vocabulary '")
                                .append(pat.pattern()).append("': ")
                                .append(lines[i].strip());
                    }
                }
            }
        }
        assertTrue(drift.length() == 0,
                "verdict files JUDGE, never COMPUTE (banned at zero, no"
                        + " register — route the computation to the"
                        + " database):" + drift);
    }

    @Test
    void theFunnelPackagesAreClosedRegisters() throws IOException {
        StringBuilder drift = new StringBuilder();
        for (var e : FUNNEL_PACKAGE_REGISTERS.entrySet()) {
            Path dir = Path.of("..", e.getKey());
            java.util.Set<String> actual = new java.util.TreeSet<>();
            try (var s = Files.list(dir)) {
                s.map(p -> p.getFileName().toString())
                        .filter(n -> n.endsWith(".java"))
                        .forEach(actual::add);
            }
            for (String f : actual) {
                if (!e.getValue().contains(f)) {
                    drift.append("\n  NEW class in ").append(e.getKey())
                            .append(": ").append(f)
                            .append(" — a new funnel-package surface"
                                    + " registers consciously with its"
                                    + " tenet argument");
                }
            }
            for (String f : e.getValue()) {
                if (!actual.contains(f)) {
                    drift.append("\n  ").append(f).append(" in ")
                            .append(e.getKey())
                            .append(" is GONE — delete its register row");
                }
            }
        }
        assertTrue(drift.length() == 0,
                "funnel class-register drift (Tier-2 audit):" + drift);
    }

    @Test
    void javaEvaluationSurfaceOnlyShrinks() throws IOException {
        StringBuilder drift = new StringBuilder();
        for (var e : EVICT_SIZE.entrySet()) {
            Path p = Path.of("..", e.getKey());
            if (!Files.exists(p)) {
                drift.append("\n  ").append(e.getKey())
                        .append(": EVICTED WHOLE — delete this ledger row"
                                + " (a stale row is a register lying about"
                                + " what exists; found live once:"
                                + " HostEval.java, 2026-08-19 audit)");
                continue;
            }
            long lines = Files.readString(p)
                    .replaceAll("(?s)/\\*.*?\\*/", "")
                    .replaceAll("//.*", "")
                    .lines().filter(l -> !l.isBlank()).count();
            if (lines > e.getValue()) {
                drift.append("\n  ").append(e.getKey()).append(": ")
                        .append(lines).append(" > ").append(e.getValue())
                        .append(" stripped code lines — the evaluator"
                                + " GREW (tenet #1: the database"
                                + " executes; evict, or bump the pin"
                                + " with a written justification)");
            }
        }
        for (var e : EVICT_NAMES.entrySet()) {
            Path p = Path.of("..", e.getKey());
            int pinned = (Integer) e.getValue()[1];
            if (!Files.exists(p)) {
                if (pinned != 0) {
                    drift.append("\n  ").append(e.getKey())
                            .append(": file GONE — delete its ledger row");
                }
                continue;
            }
            String src = Files.readString(p)
                    .replaceAll("//.*", "")
                    .replaceAll("(?s)/\\*.*?\\*/", "");
            Matcher m = Pattern.compile((String) e.getValue()[0]).matcher(src);
            int n = 0;
            while (m.find()) {
                n++;
            }
            if (n != pinned) {
                drift.append("\n  ").append(e.getKey()).append(": ")
                        .append(n).append(n > pinned ? " > " : " < ")
                        .append(pinned)
                        .append(n > pinned
                                ? " — a NEW Java-evaluation site (tenet #1;"
                                        + " evict or register PERMANENT with"
                                        + " a justification)"
                                : " — an EVICTION landed: shrink this pin");
            }
        }
        assertTrue(drift.length() == 0,
                "Java-evaluation ledger drift (JAVA_EVICTION_PLAN E0):"
                + drift);
    }
}
