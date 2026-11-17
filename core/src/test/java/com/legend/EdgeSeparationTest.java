// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * THE EDGE IS SEPARATED, PROVABLY (the user, 2026-11-17: "Focus on the total and provable edge separation first";
 * the separation census, docs/records/f-separation-census-2026-11-17.md). Three umbrella guards, one per finish-line
 * item, each with a SHRINK-ONLY allowlist that is exactly the census's open lines, each entry naming its line:
 * <ol>
 *   <li>BUILD ({@link #theBuildReachesNoEdgeButItsOpenLines}): no product target reaches an edge target
 *   (the direct dependents of {@code //core:edge}, {@code :edge_server}, {@code :edge_service_tests} and
 *   {@code :edge_store_words} within the product's roots' closure, a genquery: a target reaching the edge transitively
 *   reaches it through one of them);</li>
 *   <li>CODE ({@link #theProductsCodeNamesNoCompatibilityButItsOpenLines}): no product class depends on an edge class,
 *   a compatibility class standing in a product package, a SqlFn edge-only node, the judges' compatibility mode or the
 *   referee and the appeals;</li>
 *   <li>TEST SIDE ({@link #theTestSideNamesNoCompatibilityButItsOpenLines}): the same over the test infrastructure
 *   outside the edge.</li>
 * </ol>
 * A census line is CLOSED when its entries are deleted and the guard stays green; reintroducing the dependency turns
 * it red ({@link #eachGuardBites}). "Lines left" is the number of allowlist entries ({@link #linesLeft}); separation is
 * DONE when all three are empty.
 *
 * <p>LIMIT, named: a text spelling ({@code "TDSNull"}, {@code "+0000"}, legend-engine's sentences) is a string
 * constant javac inlines, invisible to a dependency graph; its guard is {@link CompatSpellingGuardTest}'s source scan
 * (shrink-only, every row stepped), which this umbrella cites rather than repeats.
 */
@Tag("guardrail")
class EdgeSeparationTest {

    // ---- the vocabulary ---------------------------------------------------------------------------------------

    /** The compatibility classes that stand in product packages today (census B and C lines): a class here is itself an
     *  open line until it moves to the edge or is ruled; a product class depending on one is an entry too. */
    static final Map<String, String> COMPAT_CLASSES = Map.ofEntries(
            Map.entry("com.legend.planner.UpstreamPlans", "B2"),
            Map.entry("com.legend.sql.dialect.EngineTextDialect", "B3"),
            Map.entry("com.legend.sql.dialect.RawSqlBoundary", "B4"),
            Map.entry("com.legend.plan.PurePrint", "B6"),
            Map.entry("com.legend.plan.UpstreamRelationType", "B6"),
            Map.entry("com.legend.server.PureV1Api", "B7"),
            Map.entry("com.legend.sql.dialect.FloatPrintText", "B9"),
            Map.entry("com.legend.sql.dialect.DateTimePrintText", "B9"),
            Map.entry("com.legend.lowering.Render", "B17"),
            Map.entry("com.legend.lowering.Repr", "C5"),
            Map.entry("com.legend.testplan.EngineTestText", "C4"),
            Map.entry("com.legend.testplan.EngineTextPlanning", "C4"),
            Map.entry("com.legend.testplan.PlanStyleFacets", "C4"));

    /** The SqlFn members only the edge spells (census B8). */
    static final Set<String> EDGE_ONLY_SQLFN = Set.of("FLOAT_PRINT", "DATETIME_PRINT", "FORMAT_PADDED", "INSTANCE_TEXT",
            "RELATION_TEXT", "REPRESENTATION_TEXT", "TEXT_FORMS", "TRACE_TEXT");

    /** The referee and the appeals (census C2; G6: the edge's alone). */
    static final Set<String> REFEREE = Set.of("com.legend.testexec.SqlReplayReferee", "com.legend.testexec.Appeals",
            "com.legend.testexecutionplan.Appeal", "com.legend.testexecutionplan.PlannedAppeals",
            "com.legend.testplan.AppealsByVerdict", "com.legend.testplan.EngineTextAppeals");

    /** The test infrastructure's packages (the TEST SIDE guard's); every other product package is the CODE guard's. */
    static final List<String> TEST_SIDE = List.of("com.legend.testplan", "com.legend.testexec",
            "com.legend.testexecutionplan", "com.legend.test");

    // ---- the allowlists: exactly the census's open lines --------------------------------------------------------

    /** CODE: "source class -> target", each entry its census line and date. */
    static final Map<String, String> CODE_OPEN = Map.ofEntries(
            Map.entry("com.legend.database.PlanStyle -> com.legend.sql.dialect.EngineTextDialect", "B1 2026-11-17"),
            Map.entry("com.legend.lowering.KeyedPivot -> com.legend.lowering.Render", "B17 2026-11-17"),
            Map.entry("com.legend.lowering.LiteralSpelling -> SqlFn.DATETIME_PRINT", "B8 2026-11-17"),
            Map.entry("com.legend.lowering.Lowerer -> SqlFn.RELATION_TEXT", "B8 2026-11-17"),
            Map.entry("com.legend.lowering.Lowerer -> SqlFn.TRACE_TEXT", "B8 2026-11-17"),
            Map.entry("com.legend.lowering.Lowerer -> com.legend.lowering.Render", "B17 2026-11-17"),
            Map.entry("com.legend.lowering.Render -> (stands in a product package)", "B17 2026-11-17"),
            Map.entry("com.legend.lowering.Repr -> (stands in a product package)", "B12 2026-11-17"),
            Map.entry("com.legend.lowering.Repr -> SqlFn.FLOAT_PRINT", "C5 2026-11-17"),
            // (the orchestrator's ruling, 2026-11-17: lite's message Float reads FLOAT_PRINT's shortest digits until B9
            // splits it into a product node and the edge's spelling)
            Map.entry("com.legend.lowering.LiteText -> SqlFn.FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("com.legend.lowering.Repr -> SqlFn.REPRESENTATION_TEXT", "B12 2026-11-17"),
            Map.entry("com.legend.lowering.Scalars -> SqlFn.DATETIME_PRINT", "B8 2026-11-17"),
            Map.entry("com.legend.lowering.Scalars -> SqlFn.FORMAT_PADDED", "B8 2026-11-17"),
            Map.entry("com.legend.lowering.Scalars -> SqlFn.INSTANCE_TEXT", "B8 2026-11-17"),
            Map.entry("com.legend.lowering.Scalars -> SqlFn.TEXT_FORMS", "B8 2026-11-17"),
            Map.entry("com.legend.lowering.Scalars -> com.legend.lowering.Repr", "B12 2026-11-17"),
            Map.entry("com.legend.lowering.TaggedLists -> SqlFn.TEXT_FORMS", "B8 2026-11-17"),
            Map.entry("com.legend.lowering.TypedRows -> com.legend.lowering.Render", "B17 2026-11-17"),
            Map.entry("com.legend.lowering.VerdictSql -> com.legend.lowering.Repr", "C5 2026-11-17"),
            Map.entry("com.legend.plan.PurePrint -> (stands in a product package)", "B6 2026-11-17"),
            Map.entry("com.legend.plan.UpstreamRelationType -> (stands in a product package)", "B6 2026-11-17"),
            Map.entry("com.legend.planner.BodyPlanner -> com.legend.planner.UpstreamPlans", "B2 2026-11-17"),
            Map.entry("com.legend.planner.PlanAllocations -> com.legend.planner.UpstreamPlans", "B2 2026-11-17"),
            Map.entry("com.legend.planner.ProductStatements -> com.legend.planner.UpstreamPlans", "B2 2026-11-17"),
            Map.entry("com.legend.planner.QueryParameters -> com.legend.plan.PurePrint", "B6 2026-11-17"),
            Map.entry("com.legend.planner.UpstreamPlans -> (stands in a product package)", "B2 2026-11-17"),
            Map.entry("com.legend.server.ConnectionResolver -> com.legend.server.PureV1Api", "B7 2026-11-17"),
            Map.entry("com.legend.server.LegendHttpServer -> com.legend.server.PureV1Api", "B7 2026-11-17"),
            Map.entry("com.legend.server.ModelChanges -> com.legend.server.PureV1Api", "B7 2026-11-17"),
            Map.entry("com.legend.server.ModelInput -> com.legend.server.PureV1Api", "B7 2026-11-17"),
            Map.entry("com.legend.server.PureV1Api -> (stands in a product package)", "B7 2026-11-17"),
            Map.entry("com.legend.server.PureV1Api -> com.legend.plan.UpstreamRelationType", "B6 2026-11-17"),
            Map.entry("com.legend.sql.dialect.AnsiSqlRenderer -> com.legend.sql.dialect.DateTimePrintText", "B9 2026-11-17"),
            Map.entry("com.legend.sql.dialect.AnsiSqlRenderer -> com.legend.sql.dialect.FloatPrintText", "B9 2026-11-17"),
            Map.entry("com.legend.sql.dialect.DateTimePrintText -> (stands in a product package)", "B9 2026-11-17"),
            Map.entry("com.legend.sql.dialect.DuckDb -> com.legend.sql.dialect.RawSqlBoundary", "B4 2026-11-17"),
            Map.entry("com.legend.sql.dialect.EngineTextDialect -> (stands in a product package)", "B3 2026-11-17"),
            Map.entry("com.legend.sql.dialect.FloatPrintText -> (stands in a product package)", "B9 2026-11-17"),
            Map.entry("com.legend.sql.dialect.Postgres -> com.legend.sql.dialect.RawSqlBoundary", "B4 2026-11-17"),
            Map.entry("com.legend.sql.dialect.RawSqlBoundary -> (stands in a product package)", "B4 2026-11-17"),
            Map.entry("com.legend.testdatagen.TestDataGenerationNatives -> com.legend.sql.dialect.EngineTextDialect", "B13 2026-11-17"),
            Map.entry("com.legend.testdatagen.TestDataGenerator -> com.legend.sql.dialect.EngineTextDialect", "B13 2026-11-17"),
            Map.entry("planner.Boundary -> com.legend.edge.HostAnswers", "A1 2026-11-17"),
            Map.entry("planner.Boundary -> com.legend.edge.PureText", "A1 2026-11-17"),
            Map.entry("planner.Boundary -> com.legend.edge.PureV1Routes", "A1 2026-11-17"),
            Map.entry("planner.Boundary -> com.legend.plan.UpstreamRelationType", "A1 2026-11-17"),
            Map.entry("planner.Boundary -> com.legend.server.PureV1Api", "A1 2026-11-17"),
            Map.entry("planner.Folded -> com.legend.server.PureV1Api", "A1 2026-11-17"));

    /** TEST SIDE: the same. */
    static final Map<String, String> TEST_OPEN = Map.ofEntries(
            Map.entry("com.legend.TestExecution -> com.legend.testexec.SqlReplayReferee", "C2 2026-11-17"),
            Map.entry("com.legend.test.PureTestRunner -> com.legend.testexec.SqlReplayReferee", "C2 2026-11-17"),
            Map.entry("com.legend.test.TestObserver -> com.legend.testexec.SqlReplayReferee", "C2 2026-11-17"),
            Map.entry("com.legend.testexec.Appeals -> (stands in a product package)", "C2 2026-11-17"),
            Map.entry("com.legend.testexec.BodyRun -> com.legend.testexec.Appeals", "C2 2026-11-17"),
            Map.entry("com.legend.testexec.Equality -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testexec.HostJudgments -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testexec.SqlReplayReferee -> (stands in a product package)", "C2 2026-11-17"),
            Map.entry("com.legend.testexec.SqlReplayReferee -> com.legend.testexecutionplan.Appeal", "C2 2026-11-17"),
            Map.entry("com.legend.testexec.SqlTextRuns -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testexec.SqlTextRuns -> com.legend.testexec.SqlReplayReferee", "C2 2026-11-17"),
            Map.entry("com.legend.testexec.SqlTextRuns -> com.legend.testexecutionplan.Appeal", "C2 2026-11-17"),
            Map.entry("com.legend.testexec.TestRunner -> com.legend.testexec.Appeals", "C2 2026-11-17"),
            Map.entry("com.legend.testexec.TestRunner -> com.legend.testexec.SqlReplayReferee", "C2 2026-11-17"),
            Map.entry("com.legend.testexec.TestRunner -> com.legend.testexecutionplan.PlannedAppeals", "C2 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.Appeal -> (stands in a product package)", "C2 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.HostJson -> com.legend.testexecutionplan.Appeal", "C2 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.JudgeJson -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.PlannedAppeals -> (stands in a product package)", "C2 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.PlannedAppeals -> com.legend.testexecutionplan.Appeal", "C2 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.TestPlan -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> com.legend.testexecutionplan.Appeal", "C2 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> com.legend.testexecutionplan.PlannedAppeals", "C2 2026-11-17"),
            // (AgreementAsserts -> EngineTextAppeals closed: 2026-11-17, (b) of the orchestrator's order: the fused agreement path deleted)
            Map.entry("com.legend.testplan.AppealsByVerdict -> (stands in a product package)", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.AppealsByVerdict -> com.legend.testexecutionplan.PlannedAppeals", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.AppealsByVerdict -> com.legend.testplan.EngineTextAppeals", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.AssertVerdicts -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testplan.AssertVerdicts -> com.legend.testplan.EngineTestText", "C4 2026-11-17"),
            Map.entry("com.legend.testplan.BodyDriver -> com.legend.planner.UpstreamPlans", "B2 2026-11-17"),
            Map.entry("com.legend.testplan.BodyDriver -> com.legend.testplan.EngineTestText", "C4 2026-11-17"),
            Map.entry("com.legend.testplan.BodyDriver -> com.legend.testplan.EngineTextAppeals", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.DatabaseAsserts -> com.legend.testplan.EngineTestText", "C4 2026-11-17"),
            Map.entry("com.legend.testplan.DatabaseJudge -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTestText -> (stands in a product package)", "C4 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTestText -> com.legend.testexecutionplan.Appeal", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTextAppeals -> (stands in a product package)", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTextAppeals -> com.legend.testexecutionplan.Appeal", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTextPlanning -> (stands in a product package)", "C4 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTextPlanning -> com.legend.testexecutionplan.Appeal", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTextPlanning -> com.legend.testplan.EngineTestText", "C4 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTextPlanning -> com.legend.testplan.EngineTextAppeals", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.JudgePlans -> com.legend.testplan.EngineTestText", "C4 2026-11-17"),
            Map.entry("com.legend.testplan.JudgmentFacts -> com.legend.testexecutionplan.Appeal", "C2 2026-11-17"),
            Map.entry("com.legend.testplan.PlanStyleFacets -> (stands in a product package)", "C4 2026-11-17"),
            Map.entry("com.legend.testplan.PlanStyleFacets -> com.legend.planner.UpstreamPlans", "B2 2026-11-17"),
            Map.entry("com.legend.testplan.PlanStyleFacets -> com.legend.testplan.EngineTestText", "C4 2026-11-17"),
            Map.entry("com.legend.testplan.PlanningOptions -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testplan.Seeds -> com.legend.planner.UpstreamPlans", "B2 2026-11-17"),
            Map.entry("com.legend.testplan.TestPlanner -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testplan.TestPlanner -> com.legend.testplan.AppealsByVerdict", "C2 2026-11-17"));

    // ---- the guards ---------------------------------------------------------------------------------------------

    private static JavaClasses classes(String... properties) {
        List<java.util.jar.JarFile> jars = new java.util.ArrayList<>();
        try {
            for (String property : properties) {
                for (java.nio.file.Path jar : com.legend.testing.Runfile.listed(property)) {
                    jars.add(new java.util.jar.JarFile(jar.toFile()));
                }
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(jars);
    }

    private static String top(JavaClass c) {
        String n = c.getName();
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    private static boolean inEdge(String name) {
        return name.startsWith("com.legend.edge.");
    }

    private static boolean testSide(String name) {
        return TEST_SIDE.stream().anyMatch(p -> name.startsWith(p + ".")) || name.equals("com.legend.TestExecution");
    }

    /** Every "source -> target" naming the vocabulary, over {@code classes}, for the sources {@code side} admits. */
    static Set<String> found(JavaClasses classes, java.util.function.Predicate<String> side) {
        Set<String> out = new TreeSet<>();
        for (JavaClass c : classes) {
            String src = top(c);
            if (inEdge(src) || !side.test(src)) {
                continue;
            }
            if (COMPAT_CLASSES.containsKey(src) || REFEREE.contains(src)) {
                out.add(src + " -> (stands in a product package)");
            }
            c.getDirectDependenciesFromSelf().forEach(d -> {
                String target = top(d.getTargetClass());
                if (target.equals(src)) {
                    return;
                }
                if (inEdge(target)) {
                    out.add(src + " -> " + target);
                } else if (COMPAT_CLASSES.containsKey(target) || REFEREE.contains(target)) {
                    out.add(src + " -> " + target);
                }
            });
            if (!src.startsWith("com.legend.sql.")) {
                c.getFieldAccessesFromSelf().forEach(a -> {
                    String owner = a.getTarget().getOwner().getName();
                    String field = a.getTarget().getName();
                    if (owner.equals("com.legend.sql.SqlFn") && EDGE_ONLY_SQLFN.contains(field)) {
                        out.add(src + " -> SqlFn." + field);
                    } else if (owner.endsWith("TestPlan$JudgingRules") && field.equals("COMPILED_PURE_COMPAT")) {
                        out.add(src + " -> JudgingRules.COMPILED_PURE_COMPAT");
                    }
                });
            }
        }
        return out;
    }

    @Test
    void theProductsCodeNamesNoCompatibilityButItsOpenLines() {
        Set<String> f = found(classes("legend.product.jars", "legend.authored.jars"), n -> !testSide(n));
        assertEquals(new TreeSet<>(CODE_OPEN.keySet()), f, "CODE: a product class names the compatibility vocabulary"
                + " outside its census line (a new entry is a regression; a gone one closes, delete its entry)");
    }

    @Test
    void theTestSideNamesNoCompatibilityButItsOpenLines() {
        Set<String> f = found(classes("legend.product.jars"), EdgeSeparationTest::testSide);
        assertEquals(new TreeSet<>(TEST_OPEN.keySet()), f, "TEST SIDE: the test infrastructure names the compatibility"
                + " vocabulary outside its census line");
    }

    /** TEXT (limit (a)): CompatSpellingGuardTest's open rows -- every row not STAYS or RULED -- each with its census line,
     *  so the spellings a dependency graph cannot see count too ("file | spelling" as the register writes it). Exact both
     *  ways with the register ({@link #everyOpenCompatRowIsCounted}). */
    static final Map<String, String> CSG_OPEN = Map.ofEntries(
            Map.entry("compiler/element/type/PlatformTypes.java | TDS_NULL_CELL", "B10 2026-11-17"),
            Map.entry("database/PlanStyle.java | EngineTextDialect", "B1 2026-11-17"),
            Map.entry("lowering/JsonEmission.java | tdsBuilder", "B5 2026-11-17"),
            Map.entry("lowering/LiteText.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("lowering/LiteralSpelling.java | +0000", "B11 2026-11-17"),
            Map.entry("lowering/Lowerer.java | withEngineText", "B5 2026-11-17"),
            Map.entry("lowering/Repr.java | FLOAT_PRINT", "C5 2026-11-17"),
            Map.entry("lowering/Repr.java | Repr", "B12 2026-11-17"),
            Map.entry("lowering/Scalars.java | Repr", "B12 2026-11-17"),
            Map.entry("lowering/VerdictSql.java | Repr", "C5 2026-11-17"),
            Map.entry("lowering/VerdictSql.java | TDS_NULL_CELL", "C5 2026-11-17"),
            Map.entry("plan/PlanNode.java | executionTraceID", "B5 2026-11-17"),
            Map.entry("sql/SqlExpr.java | TDS_NULL_CELL", "B10 2026-11-17"),
            Map.entry("sql/SqlFn.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/SqlTyping.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/AnsiSqlRenderer.java | RawSqlBoundary", "B4 2026-11-17"),
            Map.entry("sql/dialect/BigQuery.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/ClickHouse.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/Databricks.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/DuckDb.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/DuckDb.java | RawSqlBoundary", "B4 2026-11-17"),
            Map.entry("sql/dialect/EngineTextDialect.java | EngineTextDialect", "B3 2026-11-17"),
            Map.entry("sql/dialect/DateTimePrintText.java | +0000", "B9 2026-11-17"),
            Map.entry("sql/dialect/FloatPrintText.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/H2.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/MariaDb.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/MongoDb.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/MySql.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/Postgres.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/Postgres.java | RawSqlBoundary", "B4 2026-11-17"),
            Map.entry("sql/dialect/RawSqlBoundary.java | RawSqlBoundary", "B4 2026-11-17"),
            Map.entry("sql/dialect/SQLite.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/Snowflake.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("sql/dialect/Trino.java | FLOAT_PRINT", "B9 2026-11-17"),
            Map.entry("testdatagen/TestDataGenerationNatives.java | EngineTextDialect", "B13 2026-11-17"),
            Map.entry("testdatagen/TestDataGenerator.java | EngineTextDialect", "B13 2026-11-17"),
            Map.entry("testdatagen/TestDataPlanner.java | +0000", "B11 2026-11-17"),
            Map.entry("testdatagen/TestDataPlanner.java | COMPAT", "B13 2026-11-17"),
            Map.entry("testexec/AssertRuns.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexec/CanonicalForm.java | +0000", "C5 2026-11-17"),
            Map.entry("testexec/HostJudgments.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexec/SideShapes.java | Cannot cast a collection of size", "C5 2026-11-17"),
            Map.entry("testexec/SideShapes.java | TDS_NULL_CELL", "C5 2026-11-17"),
            Map.entry("testexec/SqlTextRuns.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexecutionplan/HostJson.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexecutionplan/HostJudgment.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexecutionplan/PlannedAppeals.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexecutionplan/Verdicts.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexecutionplan/Verdicts.java | +0000", "B11 2026-11-17"),
            // (testplan/AgreementAsserts.java | EngineText closed: 2026-11-17, (b) of the orchestrator's order: the fused agreement path deleted)
            Map.entry("testplan/AssertVerdicts.java | EngineText", "C3 2026-11-17"),
            Map.entry("testplan/TestPlanner.java | COMPAT", "C1 2026-11-17"),
            Map.entry("compiler/spec/VerdictQueries.java | TDSNull", "B10 2026-11-17"),
            Map.entry("lowering/CanonicalRenderSql.java | TDSNull", "B17 2026-11-17"),
            Map.entry("lowering/VerdictSql.java | TDSNull", "B10 2026-11-17"),
            Map.entry("testexec/Equality.java | TDSNull", "B10 2026-11-17"),
            Map.entry("lowering/Render.java | TDSNull", "B17 2026-11-17"),
            Map.entry("testexec/Equality.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testexec/HostJudgments.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testexec/SqlTextRuns.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testexecutionplan/JudgeJson.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testexecutionplan/TestPlan.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testexecutionplan/TestPlanJson.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testplan/AssertVerdicts.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testplan/DatabaseJudge.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testplan/PlanningOptions.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testplan/TestPlanner.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"));

    /** The census's open lines (docs/records/f-separation-census-2026-11-17.md; closed: A4, B14 ruled; A2, A3 moved to
     *  the edge 2026-11-17 (A4 extended to the native image and the page module); records only:
     *  B15, B16, C6, C7). Each must hold at least one entry in the count ({@link #everyOpenLineIsCounted}), so an empty
     *  count cannot read DONE with a line left. A line closes by leaving this set with its last entry. */
    static final Set<String> OPEN_LINES = Set.of("A1", "A5", "A6", "B1", "B2", "B3", "B4", "B5", "B6",
            "B7", "B8", "B9", "B10", "B11", "B12", "B13", "B17", "C1", "C2", "C3", "C4", "C5");

    private static Set<String> linesOf(Map<String, String> m) {
        Set<String> out = new TreeSet<>();
        m.values().forEach(v -> out.add(v.split(" ")[0]));
        return out;
    }

    @Test
    void everyOpenCompatRowIsCounted() {
        Set<String> open = new TreeSet<>();
        for (String row : CompatSpellingGuardTest.REGISTER) {
            String[] p = row.split("\\|");
            String step = p[3].trim();
            if (!step.equals("STAYS") && !step.equals("RULED")) {
                open.add(p[0].trim() + " | " + p[1].trim());
            }
        }
        assertEquals(open, new TreeSet<>(CSG_OPEN.keySet()), "TEXT: CompatSpellingGuardTest's open rows and this"
                + " count's text entries differ (a row gone: delete its entry; a row added: give it its census line)");
    }

    @Test
    void everyOpenLineIsCounted() {
        Set<String> counted = new TreeSet<>();
        for (Map<String, String> m : List.of(BUILD_OPEN, CODE_OPEN, TEST_OPEN, CSG_OPEN)) {
            counted.addAll(linesOf(m));
        }
        assertEquals(new TreeSet<>(OPEN_LINES), counted, "the census's open lines and the lines this count holds"
                + " differ: an open line with no entry would read DONE with work left; an entry of a closed or"
                + " unknown line is a stale entry");
    }

    /** BUILD: the direct dependents of an edge target, within the product's roots' closure (core/BUILD.bazel's
     *  {@code edge_direct_dependents} genquery), each with its census line. */
    static final Map<String, String> BUILD_OPEN = Map.ofEntries(
            Map.entry("//wasm:boundary", "A1 2026-11-17"),
            Map.entry("//core:core_tests_lib", "A5 2026-11-17"),
            Map.entry("//core:test_cases", "A5 2026-11-17"),
            Map.entry("//core:seam_benchmark", "A6 2026-11-17"),
            Map.entry("//core:data_delivery_matrix_postgres_test", "A6 2026-11-17"));

    static Set<String> buildFound() {
        try {
            Set<String> out = new TreeSet<>();
            for (String l : java.nio.file.Files.readAllLines(com.legend.testing.Runfile.property("legend.edge_dependents"))) {
                if (!l.isBlank()) {
                    out.add(l.trim());
                }
            }
            return out;
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Test
    void theBuildReachesNoEdgeButItsOpenLines() {
        assertEquals(new TreeSet<>(BUILD_OPEN.keySet()), buildFound(), "BUILD: a product target depends on an edge"
                + " target outside its census line (a new one is a regression; a gone one closes: delete its entry)");
    }

    /** Each guard turns red when its dependency is reintroduced: an entry taken off the allowlist while its dependency
     *  stands is a violation, and a planted product class reaching the edge, a SqlFn edge-only node and the judges'
     *  compatibility mode is found, on each side. */
    @Test
    void eachGuardBites() {
        Set<String> build = buildFound();
        Set<String> fewer = new TreeSet<>(BUILD_OPEN.keySet());
        fewer.remove("//wasm:boundary");
        assertFalse(fewer.equals(build), "the BUILD guard bites: //wasm:boundary's dependency without its entry");
        JavaClasses planted = new ClassFileImporter().importClasses(
                com.legend.planner.PlantedCompatReach.class);
        Set<String> code = found(planted, n -> true);
        for (String want : List.of("com.legend.planner.PlantedCompatReach -> com.legend.edge.Presenter",
                "com.legend.planner.PlantedCompatReach -> SqlFn.FORMAT_PADDED",
                "com.legend.planner.PlantedCompatReach -> JudgingRules.COMPILED_PURE_COMPAT")) {
            assertFalse(!code.contains(want), "the CODE and TEST SIDE guards bite: " + want + " in " + code);
        }
    }

    /** "Lines left": the allowlists' entries, counted by the code, and by census line. */
    @Test
    void linesLeft() {
        Map<String, Integer> byLine = new TreeMap<>();
        for (Map<String, String> m : List.of(BUILD_OPEN, CODE_OPEN, TEST_OPEN, CSG_OPEN)) {
            m.values().forEach(v -> byLine.merge(v.split(" ")[0], 1, Integer::sum));
        }
        System.out.println("[edge separation] lines left: " + (BUILD_OPEN.size() + CODE_OPEN.size() + TEST_OPEN.size()
                + CSG_OPEN.size()) + " (BUILD " + BUILD_OPEN.size() + ", CODE " + CODE_OPEN.size() + ", TEST SIDE "
                + TEST_OPEN.size() + ", TEXT " + CSG_OPEN.size() + "); by census line " + byLine);
    }
}
