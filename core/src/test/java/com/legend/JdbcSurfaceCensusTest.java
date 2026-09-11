// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE JDBC SURFACE CENSUS (ADVERSARIAL_TENET_AUDIT_2026_08_18 fix plan
 * items 6+7): a WALK over every module and BOTH source roots, with an
 * explicit exempt register — the eval ledger's hard-coded paths could
 * not see code that moved or was born outside them (10 of the audit's
 * 15 probes landed green that way; the {@code src/main -> src/test}
 * harness move silently walked 7,564 lines out of seven guards).
 *
 * <p>Three assertions:
 * <ul>
 *   <li><b>Coverage floor</b> — the walk itself reports how many files
 *   it scanned, and that number must not drop: scope rot (a renamed
 *   root, a new module the walk misses) fails loudly instead of
 *   silently shrinking the guarded surface.</li>
 *   <li><b>Production register</b> — the EXACT set of {@code src/main}
 *   files that touch JDBC or a driver API ({@code java.sql},
 *   {@code javax.sql}, {@code org.duckdb}, {@code org.h2},
 *   {@code org.sqlite}, statement-method spellings, in
 *   comment-stripped source). A new file fails in BOTH directions:
 *   growth is a conscious registration, shrink deletes the row.</li>
 *   <li><b>Test register</b> — same, for test roots: the harness,
 *   checkers, and probes legitimately EXECUTE queries, so they
 *   register by file; a new test file reaching JDBC is a conscious
 *   decision, not ambient drift.</li>
 * </ul>
 *
 * <p>KNOWN LIMIT (the audit's own §3 lesson, recorded honestly): this
 * census is FILE-grained. New evaluation code added to an
 * already-registered file is invisible here — that residue is what the
 * eval ledger's size/name pins cover, and residue DELETION (the
 * relation-typed fetchDb leg) is the durable fix, not finer guards.
 */
class JdbcSurfaceCensusTest {

    /** PRECISION (2026-08-21): the bare word {@code ResultSet} is gone —
     * it over-matched the PURE-LANGUAGE class
     * {@code meta::relational::metamodel::execute::ResultSet} spelled in
     * native-signature STRINGS (builtin/Pure.java sat on the register
     * with zero JDBC). Genuine JDBC cannot hide from the tightened
     * pattern: the java.sql TYPE spelling appears in the import or FQN,
     * and connection-passed usage spells a statement-method call —
     * {@code createStatement} added for that same completeness. */
    private static final Pattern JDBC = Pattern.compile(
            "java\\.sql|javax\\.sql|org\\.duckdb|org\\.h2\\.|org\\.sqlite"
            + "|prepareStatement|createStatement|executeQuery");

    /** Every module source root the census walks (repo-relative). A
     * NEW MODULE must be added here — the coverage floor cannot see a
     * root it was never told about, so module creation reviews this
     * file. */
    private static final List<String> ROOTS = List.of(
            "core/src", "spec/src", "pct/src", "nlq/src", "parser-equivalence/src");

    /** Coverage floor: files scanned on 2026-08-18. Shrink needs a
     * written justification (files deleted); growth is free. */
    // 779 -> 778: HostEval DELETED (Phase 1 batch 2; GridReads was
    // a rename, net zero)
    private static final int FILE_FLOOR = 778;

    private static final Set<String> MAIN_REGISTER = new TreeSet<>(List.of(
            // Clause 2c: the assert-family verdict arm — argument values
            // compute in the database; the SQLException surface carries
            // the verdict to the runner
            "core/src/main/java/com/legend/AssertVerdicts.java",
            "core/src/main/java/com/legend/Compiler.java",
            "core/src/main/java/com/legend/StatementExecutor.java",
            "core/src/main/java/com/legend/exec/DynamicPivot.java",
            // Phase 1c: the LIMIT-0 schema probe (schema, never values;
            // moved from deleted ResultNav; split out of RawGridSchema at
            // the Invariant-7 staged-compilation move). (DbMetaData row
            // RETIRED: moved to compiler/spec/CatalogGrids — pure
            // SQL-text composition.)
            "core/src/main/java/com/legend/exec/GridProbe.java",
            // (PureAsserts + TdsCompare rows RETIRED 2026-08-21, the
            // D-arc dividend: PureDateLiteral is THE wire temporal
            // carrier, so the comparison layer's java.sql value arms
            // are GONE — sql types never escape the fetch seam.)
            "core/src/main/java/com/legend/exec/Executor.java",
            // THE SYSTEM DATABASE (user ruling 2026-09-02): the graph's
            // metamodel rows in a database of their own, separate from
            // every user connection — opened once per graph per engine,
            // written once; the executor routes store-reading bodies to
            // it. It opens the in-memory session and runs the seed DDL
            // through Executor.executeRaw; no value is read here.
            "core/src/main/java/com/legend/exec/SystemDatabase.java",
            // B7 (RaisedErrors): touches java.sql ONLY to rethrow a
            // SQLException whose raised-message envelope it removed —
            // the provenance seam at Executor's own funnel; it opens no
            // connection and executes nothing
            "core/src/main/java/com/legend/exec/RaisedErrors.java",
            // contract program: the wire census READS ResultSetMetaData
            // of results the Executor already fetched — measurement of
            // the wire's self-description, zero queries, zero decode;
            // the CanonicalDivergence pattern with a java.sql import
            "core/src/main/java/com/legend/exec/SqlTypeCensus.java",
            "core/src/main/java/com/legend/exec/PctProbe.java",
            // SQLTEXT charter §2: the replay-oracle SPI — a pure
            // interface (AssertListener precedent) whose SIGNATURE
            // names java.sql types (SQLException, the OracleRows cell
            // shape); it opens no connection and executes nothing —
            // the harness implementation does, on the testing side
            "core/src/main/java/com/legend/exec/SqlReplayOracle.java",
            // the product's test runner (batch 7a, 2026-09-11): opens one
            // session per package through the caller's connection factory and
            // HANDS the connection to the executor; the observer seam passes
            // it to the caller's referee; neither executes a statement of its
            // own (tenet #1 — the database executes what the platform compiles)
            "core/src/main/java/com/legend/test/PureTestRunner.java",
            "core/src/main/java/com/legend/test/TestObserver.java",
            "core/src/main/java/com/legend/server/ConnectionResolver.java",
            "core/src/main/java/com/legend/server/QueryService.java",
            "core/src/main/java/com/legend/testdatagen/TestDataGenerator.java",
            // TestDataGenerationNatives (TDG lane S2): pure ORCHESTRATION — threads the
            // ambient connection through to TestDataGenerator's fetches
            // (the database executes); no statements of its own
            "core/src/main/java/com/legend/testdatagen/TestDataGenerationNatives.java"
    ));

    private static final Set<String> TEST_REGISTER = new TreeSet<>(List.of(
            "core/src/test/java/com/legend/ArchitectureTest.java",
            // 2026-09-09, the connection lease: the CONNECTION LIFECYCLE is
            // the thing under test, so java.sql.Connection is the subject and
            // not an execution shortcut. It asserts isClosed() on a resolved
            // handle — that a caller-owned connection is released by its lease
            // and a store-owned one never is. No statements, no queries: the
            // database executes nothing here, which is exactly tenet #1.
            "core/src/test/java/com/legend/server/ConnectionLeaseTest.java",
            // batch 58: reads the referee H2 jar's VERSION constant (no
            // connection, no statement) to pin the raw-SQL boundary's H2
            // dialect level to the jar it translates for
            "core/src/test/java/com/legend/sql/dialect/H2VersionPinTest.java",
            // Charter Clause 2c fixture: World 2 IS a database execution
            // — the two-worlds agreement is the thing under test
            "core/src/test/java/com/legend/exec/EqualityWorldsConformanceTest.java",
            // Phase 4: assertError spec tests — a DuckDB session + the
            // SQLException surface IS the feature under test
            "core/src/test/java/com/legend/AssertErrorNativeTest.java",
            // Clause 2c: the verdict-arm spec tests — a DuckDB session
            // computes the argument sides; the verdict IS the test
            "core/src/test/java/com/legend/AssertVerdictsTest.java",
            // V7 batch 2: the SPLICE PIN — a DuckDB session executes
            // the frame (execute()) and the assert sides through the
            // envelope splice; the spliced verdict IS the test
            "core/src/test/java/com/legend/AssertVerdictSpliceTest.java",
            // F13: instance-identity spec pins — a DuckDB session
            // computes both verdict sides; the site-minted __id rides
            // the SQL and the verdict IS the test
            "core/src/test/java/com/legend/exec/InstanceIdentityTest.java",
            // D94 (slice-4 fold-in): the diamond-layout witness — the
            // executed half proves the [1] property reads back scalar
            // THROUGH the database (tenet #1: the value's shape is the
            // SQL layout's, so the assertion needs a real session)
            "core/src/test/java/com/legend/compiler/element/ClassLayoutsDiamondTest.java",
            // F10 v1: literal-channel spec pins — a DuckDB session
            // computes both sides; the byte verdict IS the test
            "core/src/test/java/com/legend/exec/LiteralChannelTest.java",
            // relation wall burn 2026-08-23: the aggregate-ORDER-BY
            // null-placement pin — a DuckDB session renders the sorted
            // toString; the produced text IS the assertion
            "core/src/test/java/com/legend/lowering/AggOrderNullPlacementTest.java",
            // Phase 4: map wire-shape + rigid-lattice spec pins execute
            // through a DuckDB session (the wire IS the assertion)
            "core/src/test/java/com/legend/lowering/MapOptionalSourceTest.java",
            // shortcut audit §5: the null-drop-in-the-lowerer pins run
            // e2e over a DuckDB session (size/at/toOne must agree ON THE
            // DATABASE — the bug was SQL-vs-egress disagreement)
            "core/src/test/java/com/legend/lowering/OptionalCollectionNullDropTest.java",
            // Part-1 silent-value witnesses (2026-08-26): the divide-by-
            // zero raise, times() integer kind, []->map empty, and the
            // missing-required rejection pin e2e VALUE semantics — the
            // executed result IS the assertion (the BurnLaneTest form)
            "core/src/test/java/com/legend/lowering/Part1SemanticsTest.java",
            // §4AD P1 placement witnesses (2026-08-29): value-position
            // ROW SEMANTICS execute IN the database — the R4
            // distinguishing witnesses (phantom-mint refutation,
            // multi-occurrence fork/share, double-NULL null-safe
            // parity) assert EXECUTED rows, tenet #1 conformant
            "core/src/test/java/com/legend/resolver/ValueMapPlacementTest.java",
            // D100 witnesses (2026-08-26): connection-cache isolation —
            // the resolver's per-(model, definition) key IS the feature
            // under test; the SQLException surface is the assertion
            "core/src/test/java/com/legend/server/ConnectionIsolationTest.java",
            // D102 witnesses (2026-08-26): the checked-envelope defect
            // CASE executes IN the database — the produced defects
            // JSON is the assertion (NULL-predicate unable-to-evaluate
            // arm vs violation arm vs null-safe equality)
            "core/src/test/java/com/legend/integration/GraphFetchCheckedIntegrationTest.java",
            // group F burn (2026-09-02): the mapping-metamodel query
            // functions as Pure bodies over store rows — the in-memory
            // DuckDB IS the database the rows are the verdict on
            "core/src/test/java/com/legend/integration/MetamodelQueryFunctionsTest.java",
            // slice-3 exit criterion: byte-decidable Any equality pins
            // execute ON THE DATABASE (dedup verdicts computed in SQL —
            // the carrier's disjoint spellings are the assertion)
            "core/src/test/java/com/legend/lowering/AnyLiteralByteDecidabilityTest.java",
            // M4 post-landing audit: contains-with-comparator over a
            // carried list computes its verdict IN SQL (the comparator
            // body executes in the database; the needle wrap's byte
            // outcome is the assertion) — the referee-silent corner
            // pinned e2e
            "core/src/test/java/com/legend/lowering/ComparatorConventionTest.java",
            // shortcut audit §1a: the typed-lane toOne pins raise pure's
            // size errors IN THE DATABASE — the assertion is the DB's
            // own error message, so the session is the test subject
            "core/src/test/java/com/legend/lowering/ToOneLaneTest.java",
            // R1: the World-2 paired-probe guard runs the SAME
            // computation through SQL — the DuckDB session IS World 1
            "core/src/test/java/com/legend/exec/VerdictWorld2ConsistencyTest.java",
            // burn lane: the cast cross-kind raise is the DATABASE's
            // error — the session is the assertion subject
            "core/src/test/java/com/legend/lowering/BurnLaneTest.java",
            // D4: variance pins run eval() e2e — the accepted direction
            // must EXECUTE, not merely type-check
            "core/src/test/java/com/legend/compiler/spec/VarianceD4Test.java",
            // V10c: the dual-render conformance battery — the DATABASE
            // computes the SQL canon text; agreement with the host
            // reference render IS the assertion
            "core/src/test/java/com/legend/lowering/SqlCanonConformanceTest.java",
            // D6b: the leniency pins run Compiler.execute e2e — the
            // valid-neighbor control must EXECUTE, and the bad-date pin
            // proves rejection moved from the DB to the parser
            "core/src/test/java/com/legend/compiler/LeniencyD6Test.java",
            "core/src/test/java/com/legend/JdbcSurfaceCensusTest.java",
            "core/src/test/java/com/legend/AuditRound3Test.java",
            // multiplicity audit slice 2: the strictness negative
            // fixtures — declared-return checks fire at INLINE time
            // (the execute path), and the positive controls run their
            // SQL in the database (tenet: no Java evaluation involved)
            "core/src/test/java/com/legend/compiler/spec/MultiplicityStrictnessTest.java",
            "core/src/test/java/com/legend/AuditRound5Test.java",
            "core/src/test/java/com/legend/ConstantPlanParityTest.java",
            "core/src/test/java/com/legend/TenetRatchetTest.java",
            "core/src/test/java/com/legend/compiler/spec/UserCallInlinerTest.java",
            // Tier-1 audit regression pins (2026-08-18): drive the fixed
            // findings through the real pipeline / real connections —
            // they verify egress spelling and wall behavior, computing
            // no values in Java
            "core/src/test/java/com/legend/exec/AuditTier1PipelineTest.java",
            "core/src/test/java/com/legend/exec/Phase1AuditTest.java",
            "core/src/test/java/com/legend/exec/RawGridSchemaTest.java",
            // Phase 4: channel B's runner — one fresh DuckDB session per
            // PCT test (the platform executes; the runner orchestrates)
            "pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java",
            "core/src/test/java/com/legend/exec/DynamicPivotKeyLiteralTest.java",
            "core/src/test/java/com/legend/testdatagen/PureReprTest.java",
            "core/src/test/java/com/legend/exec/ExecuteFrameTest.java",
            "core/src/test/java/com/legend/exec/ExecuteInDbTest.java",
            // P3-2 single-query pin: a JDBC PROXY that COUNTS wire
            // traffic — the probe-count discipline enforced by
            // observation, not narration (tenet argument: the test's
            // JDBC surface exists to PIN how little JDBC the platform
            // uses)
            "core/src/test/java/com/legend/exec/ExecuteInDbProbeCountTest.java",
            // the PCT.function suppression behavior pin (executes model
            // queries to prove which definition wins — session plumbing)
            "core/src/test/java/com/legend/compiler/PctFunctionSuppressionTest.java",
            "core/src/test/java/com/legend/exec/ExecutorTest.java",
            "core/src/test/java/com/legend/exec/StructValueTest.java",
            // FLIP PROBE (harness-deletion item 1 slice 2): the dual-run
            // agreement instrument — passes the family session's
            // Connection THROUGH to Compiler.executeResolved (the one
            // platform seam); no JDBC calls of its own; effect-gated so
            // it only re-runs bodies the compiler proves read-only.
            // Migration scaffolding: dies at the whole-test cutover.
            // SCORING FLIP (slice 3): the migration dispatch itself —
            // Connection passed THROUGH to the one platform seam; the
            // effectful cutover's atomic-attempt protocol (begin/
            // commit/rollbackAttempt — txn + ledger mark + mirror
            // repair as ONE owned invariant) lives in ReplayOracle and
            // this file only DRIVES it (SQLException handling at the
            // call site); at cutover the call site moves to the runner
            // and the walk's JDBC surfaces above delete.
            // (ExecCallFinder RETIRED from the register 2026-08-28 —
            // sideSqlText's evaluation path deleted with the slice-3
            // equality half; the finder is pure tree navigation now)
            "spec/src/test/java/com/legend/harness/H2Verify.java",
            // SQLTEXT charter §8 slice 1: THE ORACLE SERVICE — the one
            // owner of the family-mirror session, seed-ledger replay
            // and fresh-replay fallback (extracted from the four
            // duplicated H2Verify arms). Testing-side by design: the
            // reference database IS its job; the platform reaches it
            // only through the SqlReplayOracle SPI on ExecEnv.
            "spec/src/test/java/com/legend/harness/ReplayOracle.java",
            "core/src/test/java/com/legend/integration/AbstractDatabaseTest.java",
            "core/src/test/java/com/legend/integration/AsOfJoinCheckerTest.java",
            "core/src/test/java/com/legend/integration/AssociationIntegrationTest.java",
            "core/src/test/java/com/legend/integration/ComputedProjectIntegrationTest.java",
            "core/src/test/java/com/legend/integration/ConcatenateFlattenCheckerTest.java",
            "core/src/test/java/com/legend/integration/CorpusDifferentialTest.java",
            "core/src/test/java/com/legend/integration/DuckDBIntegrationTest.java",
            "core/src/test/java/com/legend/integration/DuckDBStructSyntaxTest.java",
            // batch 166 witness (UnionSynthesis finding A): the fixture's
            // tables and rows are loaded through the test's own JDBC
            // connection, exactly as ComputedProjectIntegrationTest does;
            // the query under test reaches the database only through the
            // platform (QueryService)
            "core/src/test/java/com/legend/integration/UnionJoinMappedPropertyTest.java",
            "core/src/test/java/com/legend/integration/DynaFunctionIntegrationTest.java",
            "core/src/test/java/com/legend/integration/EnumIntegrationTest.java",
            "core/src/test/java/com/legend/integration/ExecutionResultIntegrationTest.java",
            "core/src/test/java/com/legend/integration/ExtendCheckerTest.java",
            "core/src/test/java/com/legend/integration/ExtendWindowCheckerTest.java",
            "core/src/test/java/com/legend/integration/FilterCheckerTest.java",
            "core/src/test/java/com/legend/integration/FoldCheckerTest.java",
            "core/src/test/java/com/legend/integration/FromCheckerTest.java",
            "core/src/test/java/com/legend/integration/GetCheckerTest.java",
            "core/src/test/java/com/legend/integration/GroupByCheckerTest.java",
            "core/src/test/java/com/legend/integration/InheritanceIntegrationTest.java",
            "core/src/test/java/com/legend/integration/JoinCheckerTest.java",
            // metamodel-store leg (2026-08-28): an ordinary integration
            // suite — the caller's DuckDB connection through the one
            // execute door, like every Checker suite above
            "core/src/test/java/com/legend/integration/MetamodelStoreTest.java",
            "core/src/test/java/com/legend/integration/JsonM2MChainIntegrationTest.java",
            "core/src/test/java/com/legend/integration/JsonM2MIntegrationTest.java",
            "core/src/test/java/com/legend/integration/JsonMappingIntegrationTest.java",
            "core/src/test/java/com/legend/integration/LetCheckerTest.java",
            "core/src/test/java/com/legend/integration/M2M2RTabularTest.java",
            "core/src/test/java/com/legend/integration/M2MChainIntegrationTest.java",
            "core/src/test/java/com/legend/integration/M2MIntegrationTest.java",
            "core/src/test/java/com/legend/integration/PivotCheckerTest.java",
            "core/src/test/java/com/legend/integration/RelationApiIntegrationTest.java",
            // foundation probe 2026-09-01: the mapping-seam window rule
            // (a Relation ~func extent's window is an evaluation
            // boundary); test-side JDBC = fixture seeding on an in-memory
            // DuckDB, execution through QueryService like its siblings
            "core/src/test/java/com/legend/integration/RelationMappingWindowSeamTest.java",
            "core/src/test/java/com/legend/integration/RelationalMappingCompositionTest.java",
            "core/src/test/java/com/legend/integration/RelationalMappingIntegrationTest.java",
            "core/src/test/java/com/legend/integration/RenameCheckerTest.java",
            "core/src/test/java/com/legend/integration/RenderCsvIntegrationTest.java",
            "core/src/test/java/com/legend/integration/SQLiteIntegrationTest.java",
            "core/src/test/java/com/legend/integration/ScalarFunctionIntegrationTest.java",
            "core/src/test/java/com/legend/integration/SelectDistinctCheckerTest.java",
            "core/src/test/java/com/legend/integration/SlicingCheckerTest.java",
            "core/src/test/java/com/legend/integration/SortCheckerTest.java",
            "core/src/test/java/com/legend/integration/SourceUrlUserCallableTest.java",
            "core/src/test/java/com/legend/integration/StreamingIntegrationTest.java",
            "core/src/test/java/com/legend/integration/StressDomainTest.java",
            "core/src/test/java/com/legend/integration/StressTest.java",
            "core/src/test/java/com/legend/integration/StressTest100K.java",
            "core/src/test/java/com/legend/integration/StressTest10K.java",
            "core/src/test/java/com/legend/integration/StressTestChaotic.java",
            "core/src/test/java/com/legend/integration/StressTestComplexQueries.java",
            "core/src/test/java/com/legend/integration/StressTestDense.java",
            "core/src/test/java/com/legend/integration/StructFilterIntegrationTest.java",
            // §4bZ-V B2 subsumption receipts: the round-trip decode
            // witnesses MUST execute on the real backend (tenet #1 —
            // the database executes; a Java-side re-derivation would
            // prove nothing about the wire)
            "core/src/test/java/com/legend/integration/SubsumptionWitnessTest.java",
            "core/src/test/java/com/legend/integration/TypeConversionCheckerTest.java",
            "core/src/test/java/com/legend/integration/TypeInferenceIntegrationTest.java",
            "core/src/test/java/com/legend/integration/UserFunctionIntegrationTest.java",
            "core/src/test/java/com/legend/integration/VariantIntegrationTest.java",
            "core/src/test/java/com/legend/integration/WindowFunctionTest.java",
            "core/src/test/java/com/legend/integration/WriteCheckerTest.java",
            "core/src/test/java/com/legend/lowering/GroupByAverageMappingTest.java",
            "core/src/test/java/com/legend/lowering/JoinTortureTest.java",
            "core/src/test/java/com/legend/lowering/LowerRelationTest.java",
            "core/src/test/java/com/legend/lowering/NullSemanticsTest.java",
            "core/src/test/java/com/legend/lowering/ValueSortComparatorTest.java",
            "core/src/test/java/com/legend/normalizer/AssocSimpleNameProbeTest.java",
            "core/src/test/java/com/legend/normalizer/AssociationViewJoinTest.java",
            "spec/src/test/java/com/legend/rcorpus/DuckWorkspaces.java",
            // the product test runner's proof (batch 7a, 2026-09-11): opens an
            // in-memory DuckDB session and HANDS it to the runner, which hands
            // it to the platform; it executes no SQL of its own (tenet #1)
            "core/src/test/java/com/legend/test/PureTestRunnerTest.java",
            // the minimal harness (2026-09-06): opens the DuckDB session and
            // the referee's H2 mirror and HANDS them to the platform; it
            // executes no SQL of its own (tenet #1 — the database executes
            // what the platform compiles; the harness only finds and runs
            // tests)
            "spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java",
            "core/src/test/java/com/legend/resolver/ResolveDeepEmptinessProbeTest.java",
            "core/src/test/java/com/legend/resolver/ResolveDerivedLeafProbeTest.java",
            "core/src/test/java/com/legend/resolver/ResolveFilterDemandTest.java",
            "core/src/test/java/com/legend/resolver/ResolveGraphUnionProbeTest.java",
            "core/src/test/java/com/legend/resolver/ResolveM2mTest.java",
            "core/src/test/java/com/legend/resolver/ResolveNavigationTest.java",
            "core/src/test/java/com/legend/resolver/ResolveNestedNavTest.java",
            "core/src/test/java/com/legend/resolver/ResolveOtherwiseTest.java",
            "core/src/test/java/com/legend/resolver/ResolveOuterDatedNavTest.java",
            "core/src/test/java/com/legend/resolver/ResolveSerializeTest.java",
            "core/src/test/java/com/legend/resolver/ResolveSimpleClassTest.java",
            "core/src/test/java/com/legend/resolver/ResolveTemporalContextTest.java",
            "core/src/test/java/com/legend/resolver/ResolveUnionChainTest.java",
            "core/src/test/java/com/legend/resolver/ResolveUnionJtcProbeTest.java",
            "core/src/test/java/com/legend/resolver/ResolveUnionMultiHopProbeTest.java",
            "core/src/test/java/com/legend/resolver/ResolveUnionOuterDateProbeTest.java",
            // metamodel-as-relations step 3: the mapping metamodel as
            // rows — every verdict is a query the database answers
            "core/src/test/java/com/legend/integration/MetamodelMappingStoreTest.java",
            "core/src/test/java/com/legend/resolver/ResolveUnionTest.java",
            // run-time branch choice on the row's type column (metamodel
            // step 2): rows are the verdict for match/instanceOf/cast
            // over a user inheritance mapping — the SQL must execute
            "core/src/test/java/com/legend/resolver/RuntimeTypeDispatchTest.java",
            // navigation-depth leg (2026-09-02): 3-4 hop chains through
            // associations, slots and inheritance execute e2e — the ROWS
            // the database returns are the verdict (tenet #1)
            "core/src/test/java/com/legend/resolver/NavigationDepthTest.java",
            // harness burn-down leg 1 (2026-09-02): chain-position type
            // dispatch — the database RAISES on a non-conforming cast;
            // the raise and the rows are the verdict
            "core/src/test/java/com/legend/resolver/ChainTypeDispatchTest.java",
            "core/src/test/java/com/legend/sql/DuckDbValidityTest.java",
            "core/src/test/java/com/legend/sql/dialect/CarrierDifferentialTest.java",
            // GrammarFunctions_PCT REMOVED 2026-08-23 (F13b(a)): its only
            // JDBC mention was the map test's expected-error text naming
            // org.duckdb.DuckDBArray — the flatten fix moved that failure
            // past the decode, the new expected text is JDBC-free
            // renamed at the truthfulness-burn split (census §5c): the
            // connection-opening entry file of the former
            // ExecuteLegendLiteQuery triple
            "pct/src/test/java/org/finos/legend/lite/pct/extension/PctExecuteNative.java"
    ));

    @Test
    void jdbcSurfaceIsRegistered() throws IOException {
        List<Path> files = new ArrayList<>();
        for (String r : ROOTS) {
            Path root = Path.of("..", r);
            if (!Files.isDirectory(root)) {
                continue;
            }
            try (Stream<Path> s = Files.walk(root)) {
                s.filter(p -> p.toString().endsWith(".java"))
                        .forEach(files::add);
            }
        }
        assertTrue(files.size() >= FILE_FLOOR,
                "JDBC census coverage DROPPED: scanned " + files.size()
                + " files, floor " + FILE_FLOOR + " — a source root moved"
                + " or the walk rotted; re-point the census before"
                + " trusting any guard that scopes by path");

        Set<String> mainHits = new TreeSet<>();
        Set<String> testHits = new TreeSet<>();
        for (Path p : files) {
            String src = Files.readString(p)
                    .replaceAll("//.*", "")
                    .replaceAll("(?s)/\\*.*?\\*/", "");
            if (JDBC.matcher(src).find()) {
                String rel = Path.of("..").toAbsolutePath().normalize()
                        .relativize(p.toAbsolutePath().normalize())
                        .toString().replace(java.io.File.separatorChar, '/');
                (rel.contains("/main/") ? mainHits : testHits).add(rel);
            }
        }
        StringBuilder drift = new StringBuilder();
        diff(drift, "src/main", mainHits, MAIN_REGISTER);
        diff(drift, "test roots", testHits, TEST_REGISTER);
        assertTrue(drift.length() == 0,
                "JDBC surface census drift (tenet #1 — Java orchestrates,"
                + " the DATABASE executes; audit 2026-08-18 items 6+7):"
                + drift);
    }

    private static void diff(StringBuilder drift, String where,
            Set<String> actual, Set<String> register) {
        for (String f : actual) {
            if (!register.contains(f)) {
                drift.append("\n  NEW JDBC surface in ").append(where)
                        .append(": ").append(f)
                        .append(" — register it CONSCIOUSLY (with the")
                        .append(" tenet argument) or route through an")
                        .append(" existing seam");
            }
        }
        for (String f : register) {
            if (!actual.contains(f)) {
                drift.append("\n  ").append(f)
                        .append(" no longer touches JDBC — shrink the ")
                        .append(where).append(" register");
            }
        }
    }
}
