// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE COMPATIBILITY SPELLING GUARD: phase D's D2 (docs/data-delivery-homework/D_BRIEF_2026_11_10.md §4(a); the user,
 * 2026-11-12: "push ALL compatibility to the edge so never have to worry about leaking into product"). No product
 * source outside {@code com.legend.edge} carries one of legend-pure's or legend-engine's spellings: an upstream error
 * sentence raised in SQL or thrown at compile time (the D0 inventory's K1, K3, K8), Pure's print forms ({@code +0000},
 * {@code @_}, the TDS null cell, {@code %r}, Pure's Float text), the engine's trace comment, the engine-text printers,
 * the engine writers, the engine-corpus scan order and lexicon, or a compatibility mode a product class branches on.
 *
 * <p>The scan: every main source of core outside the edge package, comments stripped; each string literal is matched
 * against {@link #LITERALS} and the code (literals blanked) against {@link #SYMBOLS} as whole words. Every hit is
 * REGISTERED ({@link #REGISTER}) by file and spelling, with the count and the D step that removes it, exact both ways:
 * a new hit fails (it is compatibility leaking into the product: spell it at the edge), a count that falls fails until
 * re-pinned (the list is SHRINK-ONLY: a pin rises only with a dated justification, AGENTS.md "Guard files are
 * load-bearing"). D2 ends with no row naming a D step: what remains is {@code JUDGE} (the database and host judges'
 * own canon, the test road's lattice, D0 §10) or {@code STAYS} (a product semantic that happens to share the
 * spelling, its reason given).
 *
 * <p>Limits, stated (the orchestrator's caution, 2026-11-12): a fixed list catches only what is on it -- this is the
 * drift alarm. The weight is carried by the structural guards: {@code ArchitectureTest.noProductClassReachesTheEdge}
 * (G5, with its planted violator), the typed raise (a product raise is a {@code RaiseKind}, D1.1) and the hook
 * implemented only at the edge (D1.0).
 */
@Tag("guardrail")
class CompatSpellingGuardTest {

    private static final String MAIN = "core/src/main";
    private static final String JAVA_ROOT = "core/src/main/java/com/legend/";
    private static final String EDGE = "core/src/main/java/com/legend/edge/";

    /** Upstream's texts, matched inside a string literal (a pattern in {@code ^...$} matches the whole literal). */
    static final List<String> LITERALS = List.of(
            // K1: legend-pure's runtime sentences raised in SQL (D1.1); its compile-time twin in TdsDesugars (D1.6)
            "Cannot divide ", "Division by zero", "Unsupported number of bits to shift", "^Cannot get $",
            "trying to get an element at offset", "Unable to compute sqrt of", "Cannot cast a collection of size",
            "can't be higher than the high bound", "Invalid day: ", "Cast exception: ", "range step must not be 0",
            "Unable to evaluate constraint",
            // narrowed 2026-11-12 (D1.1): the whole of lite's old insert sentence, so the product's own wording of an
            // index's raise (exec.RaisedErrors, "index out of bounds") is no hit
            "add(set, index, value): index out of bounds",
            // K3: the reference database's (DuckDB's) words raised on another database (D1.1)
            "The map key list does not align", "Map keys can not be NULL", "Map keys must be unique",
            "Could not convert string '", "Could not parse '",
            // K8: legend-engine's compile and server sentences (D1.6; the query store's at pure/v1, D1.8)
            "Can't find ", "Invalid supertype", "arguments count (", "variables count (",
            "Missing external parameter(s)",
            // K9 and K7: Pure's print forms and the engine's trace comment
            "+0000", "@_", "executionTraceID",
            // K4: the engine corpus's scan order flag
            "legend.exec.engineScanOrder");

    /** The compatibility machinery, matched as whole words in code. */
    static final List<String> SYMBOLS = List.of(
            "FLOAT_PRINT", "FLOAT_CANON", "TDS_NULL_CELL", "dateLiteralPrint", "signedZeroPad", "Repr",
            "presentedName", "PureV1Writer", "TdsObjectsWriter", "traceComment",
            "StableScanOrder", "ENGINE_LEXICON",
            "withEngineText", "EngineText", "EnginePlans", "EngineTextDialect", "EngineStyleH2", "EngineStyleDB2",
            "EngineStyleComposite", "RawSqlBoundary",
            "COMPAT");

    /** The steps a row may name: a D slice that removes it, or a terminal class that keeps it, with its reason. */
    static final Set<String> STEPS = Set.of(
            "D1.1",   // Raise/RaiseKind: a product raise is a kind and a place; legend-pure's sentence is the edge's
            "D1.2",   // the scalar text nodes: Float, DateTime and the literal print forms; TestDataPlanner's COMPAT
            "D1.3",   // lists, maps, makeString (TDSNull) and format (%r, %0Nd), the instance and type forms refused
            "D1.4",   // the writers: CellText's engine half, PureV1Writer, TdsObjectsWriter, the pivot quoting, the trace
            "D1.5",   // the decorator: the engine corpus's scan order, the engine lexicon
            "D1.6",   // the compile messages in the product's own wording, keyed by kind
            "D1.7",   // engine text (EngineText, EnginePlans' text half, EngineStyle*), RawSqlBoundary, appeals, referee
            "D1.8",   // pure/v1's upstream routes (and the query store they answer) to the edge
            "JUDGE",  // the judges' own canon on the test road (D0 §10): not compatibility, never moved
            "STAYS"); // a product semantic sharing the spelling: lowering/DateFormats.java's pattern letter Z is +0000

    /**
     * Every hit standing today, "file | spelling | count | step"; the file is under {@code core/src/main/java/com/legend/}
     * (else under {@code core/src/main/}). Registered 2026-11-12 (phase D, step 1, at 3a20560f9's descendant): 178 rows.
     * SHRINK-ONLY: a row falls or goes as its step lands; a row rises or is added only with a dated justification.
     */
    static final List<String> REGISTER = List.of(
            "compiler/element/ModelIntegrity.java | Can't find  | 7 | D1.6",
            "compiler/element/ModelIntegrity.java | Invalid supertype | 1 | D1.6",
            "compiler/element/TypeClassifier.java | arguments count ( | 1 | D1.6",
            "compiler/element/TypeClassifier.java | variables count ( | 1 | D1.6",
            "compiler/element/type/PlatformTypes.java | TDS_NULL_CELL | 2 | D1.3",
            "compiler/spec/TableReferenceChecker.java | Can't find  | 1 | D1.6",
            "compiler/spec/TdsDesugars.java | TDS_NULL_CELL | 2 | D1.3",
            "compiler/spec/TdsDesugars.java | trying to get an element at offset | 1 | D1.6",
            "exec/PlanParameters.java | Missing external parameter(s) | 1 | D1.6",
            // moved from testplan/ActivityComments.java (2026-11-12, C's J10 merged at 4ef947a: the execution id one per
            // execute(), minted by the door; the activity's comment now the program runner's): a move, the count the same
            "exec/ProgramRunner.java | traceComment | 1 | D1.4",
            "exec/PlanRunner.java | traceComment | 1 | D1.4",
            "executionplan/ExecutionPlan.java | executionTraceID | 1 | D1.4",
            "executionplan/ExecutionPlan.java | traceComment | 1 | D1.4",
            "lowering/CastPolicy.java | Cast exception:  | 1 | D1.1",
            "lowering/CheckedEnvelope.java | Unable to evaluate constraint | 1 | D1.1",
            "lowering/Coercions.java | Cannot cast a collection of size | 2 | D1.1",
            "lowering/DateCtorRule.java | Invalid day:  | 1 | D1.1",
            "lowering/DateFormats.java | +0000 | 1 | STAYS",
            "lowering/DecimalKindRules.java | Division by zero | 1 | D1.1",
            "lowering/ListEncodings.java | can't be higher than the high bound | 1 | D1.1",
            "lowering/ListRules.java | range step must not be 0 | 1 | D1.1",
            "lowering/LiteralSpelling.java | +0000 | 8 | D1.2",
            "lowering/LiteralSpelling.java | FLOAT_CANON | 1 | JUDGE",
            "lowering/LiteralSpelling.java | FLOAT_PRINT | 1 | D1.2",
            "lowering/Lowerer.java | TDS_NULL_CELL | 1 | D1.3",
            "lowering/Lowerer.java | withEngineText | 1 | D1.7",
            "lowering/Repr.java | Repr | 2 | D1.3",
            "lowering/Scalars.java | +0000 | 2 | D1.2",
            "lowering/Scalars.java | @_ | 1 | D1.3",
            "lowering/Scalars.java | Cannot divide  | 2 | D1.1",
            "lowering/Scalars.java | Cannot get  | 1 | D1.1",
            "lowering/Scalars.java | Repr | 2 | D1.3",
            "lowering/Scalars.java | TDS_NULL_CELL | 2 | D1.3",
            "lowering/Scalars.java | Unable to compute sqrt of | 1 | D1.1",
            "lowering/Scalars.java | Unsupported number of bits to shift | 1 | D1.1",
            "lowering/Scalars.java | dateLiteralPrint | 3 | D1.2",
            "lowering/Scalars.java | add(set, index, value): index out of bounds | 1 | D1.1",
            "lowering/Scalars.java | signedZeroPad | 2 | D1.3",
            "lowering/Scalars.java | trying to get an element at offset | 1 | D1.1",
            "lowering/TaggedLists.java | trying to get an element at offset | 1 | D1.1",
            "lowering/ToOneGuard.java | Cannot cast a collection of size | 1 | D1.1",
            "lowering/VerdictSql.java | Repr | 3 | JUDGE",
            "lowering/VerdictSql.java | TDS_NULL_CELL | 1 | JUDGE",
            "normalizer/ImplicitInheritance.java | Can't find  | 1 | D1.6",
            "normalizer/MappingClosures.java | Can't find  | 2 | D1.6",
            "normalizer/MappingNormalizer.java | Can't find  | 1 | D1.6",
            "normalizer/MappingValidation.java | Can't find  | 1 | D1.6",
            "plan/PlanNode.java | executionTraceID | 1 | D1.4",
            "planner/BodyPlanner.java | EnginePlans | 3 | D1.7",
            "planner/EnginePlans.java | EnginePlans | 2 | D1.7",
            "planner/EnginePlans.java | EngineText | 20 | D1.7",
            "planner/EnginePlans.java | EngineTextDialect | 4 | D1.7",
            "planner/EnginePlans.java | withEngineText | 1 | D1.7",
            "planner/EngineText.java | EngineStyleComposite | 1 | D1.7",
            "planner/EngineText.java | EngineStyleDB2 | 2 | D1.7",
            "planner/EngineText.java | EngineStyleH2 | 3 | D1.7",
            "planner/EngineText.java | EngineText | 2 | D1.7",
            "planner/EngineText.java | EngineTextDialect | 4 | D1.7",
            "planner/FramePlanner.java | EnginePlans | 1 | D1.7",
            "planner/PlanAllocations.java | EnginePlans | 5 | D1.7",
            "planner/PlanAllocations.java | EngineText | 3 | D1.7",
            "planner/PlanEnvelope.java | EnginePlans | 1 | D1.7",
            "planner/PlanEnvelope.java | EngineText | 2 | D1.7",
            "planner/PlanEnvelope.java | EngineTextDialect | 1 | D1.7",
            "planner/ProductStatements.java | EnginePlans | 1 | D1.7",
            "queries/SavedCubes.java | Can't find  | 3 | D1.8",
            "queries/SavedQueries.java | Can't find  | 7 | D1.8",
            "resolver/ChainDispatch.java | Cast exception:  | 1 | D1.1",
            "resolver/ClassSources.java | TDS_NULL_CELL | 1 | D1.3",
            "resolver/GraphEmission.java | +0000 | 2 | D1.2",
            "resolver/Substitution.java | Cast exception:  | 1 | D1.1",
            "result/CellText.java | +0000 | 1 | D1.4",
            "result/CellText.java | @_ | 1 | D1.4",
            "result/CellText.java | presentedName | 1 | D1.4",
            "result/CsvWriter.java | presentedName | 1 | D1.4",
            "result/JsonRowsWriter.java | presentedName | 1 | D1.4",
            "result/Output.java | PureV1Writer | 12 | D1.4",
            "result/PureV1Writer.java | PureV1Writer | 6 | D1.4",
            "result/PureV1Writer.java | presentedName | 2 | D1.4",
            "result/PureV1Writer.java | traceComment | 1 | D1.4",
            "result/TdsObjectsWriter.java | TdsObjectsWriter | 2 | D1.4",
            "result/TdsObjectsWriter.java | presentedName | 1 | D1.4",
            "server/PureV1Api.java | Missing external parameter(s) | 1 | D1.6",
            "server/PureV1Api.java | PureV1Writer | 17 | D1.8",
            "sql/SqlExpr.java | TDS_NULL_CELL | 1 | D1.3",
            "sql/SqlFn.java | FLOAT_CANON | 1 | JUDGE",
            "sql/SqlFn.java | FLOAT_PRINT | 1 | D1.2",
            "sql/SqlTyping.java | FLOAT_CANON | 1 | JUDGE",
            "sql/SqlTyping.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/AnsiSqlRenderer.java | Cannot cast a collection of size | 1 | D1.1",
            "sql/dialect/AnsiSqlRenderer.java | RawSqlBoundary | 1 | D1.7",
            "sql/dialect/BigQuery.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/BigQuery.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/BigQuery.java | Map keys can not be NULL | 1 | D1.1",
            "sql/dialect/BigQuery.java | Map keys must be unique | 1 | D1.1",
            "sql/dialect/BigQuery.java | The map key list does not align | 1 | D1.1",
            "sql/dialect/ClickHouse.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/ClickHouse.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/Databricks.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/Databricks.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/DuckDb.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/DuckDb.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/DuckDb.java | RawSqlBoundary | 1 | D1.7",
            "sql/dialect/DuckDb.java | StableScanOrder | 1 | D1.5",
            "sql/dialect/DuckDb.java | legend.exec.engineScanOrder | 1 | D1.5",
            "sql/dialect/EngineStyleComposite.java | EngineStyleComposite | 1 | D1.7",
            "sql/dialect/EngineStyleComposite.java | EngineStyleDB2 | 1 | D1.7",
            "sql/dialect/EngineStyleComposite.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/EngineStyleComposite.java | FLOAT_PRINT | 1 | D1.7",
            "sql/dialect/EngineStyleDB2.java | EngineStyleDB2 | 4 | D1.7",
            "sql/dialect/EngineStyleDB2.java | EngineStyleH2 | 1 | D1.7",
            "sql/dialect/EngineStyleDB2.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/EngineStyleDB2.java | FLOAT_PRINT | 1 | D1.7",
            "sql/dialect/EngineStyleH2.java | ENGINE_LEXICON | 1 | D1.7",
            "sql/dialect/EngineStyleH2.java | EngineStyleH2 | 4 | D1.7",
            "sql/dialect/EngineStyleH2.java | EngineTextDialect | 1 | D1.7",
            "sql/dialect/EngineStyleH2.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/EngineStyleH2.java | FLOAT_PRINT | 1 | D1.7",
            "sql/dialect/EngineStyleH2.java | TDS_NULL_CELL | 1 | D1.7",
            "sql/dialect/EngineTextDialect.java | EngineTextDialect | 1 | D1.7",
            "sql/dialect/FloatCanonText.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/FloatPrintText.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/H2.java | ENGINE_LEXICON | 2 | D1.5",
            "sql/dialect/H2.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/H2.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/MariaDb.java | Could not convert string ' | 1 | D1.1",
            "sql/dialect/MariaDb.java | Could not parse ' | 1 | D1.1",
            "sql/dialect/MariaDb.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/MariaDb.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/MongoDb.java | +0000 | 4 | D1.2",
            "sql/dialect/MongoDb.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/MongoDb.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/MySql.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/MySql.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/Postgres.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/Postgres.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/Postgres.java | Map keys can not be NULL | 1 | D1.1",
            "sql/dialect/Postgres.java | Map keys must be unique | 1 | D1.1",
            "sql/dialect/Postgres.java | RawSqlBoundary | 1 | D1.7",
            "sql/dialect/Postgres.java | The map key list does not align | 1 | D1.1",
            "sql/dialect/RawSqlBoundary.java | RawSqlBoundary | 2 | D1.7",
            "sql/dialect/SQLite.java | Could not convert string ' | 1 | D1.1",
            "sql/dialect/SQLite.java | Could not parse ' | 1 | D1.1",
            "sql/dialect/SQLite.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/SQLite.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/Snowflake.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/Snowflake.java | FLOAT_PRINT | 1 | D1.2",
            "sql/dialect/StableScanOrder.java | StableScanOrder | 1 | D1.5",
            "sql/dialect/Trino.java | FLOAT_CANON | 1 | JUDGE",
            "sql/dialect/Trino.java | FLOAT_PRINT | 1 | D1.2",
            "test/ServiceTestRunner.java | PureV1Writer | 1 | D1.4",
            "test/ServiceTestRunner.java | TdsObjectsWriter | 1 | D1.4",
            "testdatagen/TestDataGenerationNatives.java | EngineTextDialect | 2 | D1.7",
            "testdatagen/TestDataGenerator.java | EngineTextDialect | 3 | D1.7",
            "testdatagen/TestDataPlanner.java | +0000 | 1 | D1.2",
            "testdatagen/TestDataPlanner.java | COMPAT | 3 | D1.2",
            "testexec/AssertRuns.java | EngineText | 1 | D1.7",
            "testexec/CanonicalForm.java | +0000 | 1 | JUDGE",
            "testexec/HostJudgments.java | EngineText | 1 | D1.7",
            "testexec/SideShapes.java | Cannot cast a collection of size | 3 | JUDGE",
            "testexec/SideShapes.java | TDS_NULL_CELL | 1 | JUDGE",
            "testexec/SqlTextRuns.java | EngineText | 1 | D1.7",
            "testexecutionplan/HostJson.java | EngineText | 4 | D1.7",
            "testexecutionplan/HostJudgment.java | EngineText | 2 | D1.7",
            "testexecutionplan/PlannedAppeals.java | EngineText | 1 | D1.7",
            "testexecutionplan/Verdicts.java | EngineText | 1 | D1.7",
            "testplan/AgreementAsserts.java | EngineText | 1 | D1.7",
            "testplan/AssertVerdicts.java | EngineText | 3 | D1.7",
            "testplan/BodyDriver.java | EnginePlans | 1 | D1.7",
            "testplan/BodyDriver.java | EngineText | 2 | D1.7",
            "testplan/BodyDriver.java | withEngineText | 1 | D1.7",
            "testplan/SeedSqlForms.java | EngineText | 2 | D1.7",
            "testplan/Seeds.java | EnginePlans | 1 | D1.7",
            "testplan/SqlTextVerdicts.java | EngineText | 1 | D1.7",
            "testplan/SqlTextVerdicts.java | withEngineText | 3 | D1.7",
            "testplan/TestPlanner.java | COMPAT | 1 | D1.2",
            "values/PureDateLiteral.java | +0000 | 4 | D1.2"
    );

    @Test
    void noCompatibilitySpellingLeaksIntoTheProduct() throws IOException {
        Map<String, Map<String, Integer>> found = scan();
        Map<String, Map<String, Integer>> pinned = new TreeMap<>();
        StringBuilder drift = new StringBuilder();
        for (String row : REGISTER) {
            String[] cells = row.split(" \\| ", -1);
            assertEquals(4, cells.length, "a register row is 'file | spelling | count | step': " + row);
            assertTrue(STEPS.contains(cells[3]), "a register row names a known step: " + row);
            assertTrue(LITERALS.contains(cells[1]) || LITERALS.contains("^" + cells[1] + "$")
                    || SYMBOLS.contains(cells[1]), "a register row names a listed spelling: " + row);
            Integer was = pinned.computeIfAbsent(cells[0], k -> new TreeMap<>()).put(cells[1],
                    Integer.parseInt(cells[2]));
            assertTrue(was == null, "a register row is named once: " + row);
        }
        for (var file : found.entrySet()) {
            for (var hit : file.getValue().entrySet()) {
                int pin = pinned.getOrDefault(file.getKey(), Map.of()).getOrDefault(hit.getKey(), 0);
                if (hit.getValue() > pin) {
                    drift.append("\n  ").append(file.getKey()).append(": '").append(hit.getKey()).append("' x")
                            .append(hit.getValue()).append(pin == 0
                                    ? " -- a compatibility spelling in the product: spell it at the edge (G5, E1)"
                                    : " > " + pin + " registered -- grew: the same");
                } else if (hit.getValue() < pin) {
                    drift.append("\n  ").append(file.getKey()).append(": '").append(hit.getKey()).append("' x")
                            .append(hit.getValue()).append(" < ").append(pin)
                            .append(" registered -- shrank: re-pin the row, with the date");
                }
            }
        }
        for (var file : pinned.entrySet()) {
            for (var pin : file.getValue().entrySet()) {
                if (!found.getOrDefault(file.getKey(), Map.of()).containsKey(pin.getKey())) {
                    drift.append("\n  ").append(file.getKey()).append(": '").append(pin.getKey())
                            .append("' registered x").append(pin.getValue()).append(" and GONE -- delete its row");
                }
            }
        }
        assertTrue(drift.length() == 0, "no compatibility spelling in the product (D2) -- drift:" + drift
                + "\n\ntoday's hits, as register rows (each row's step is yours to name):\n" + rows(found));
    }

    /** The scan bites: a product source holding an engine sentence, a Pure print form and the engine printer is
     *  found on each (a planted source, read as the guard reads a file). */
    @Test
    void theScanBitesAPlantedSource() {
        String planted = """
                package com.legend.lowering;
                // "+0000" in a comment is no hit
                final class Planted {
                    static final String S = "Cannot divide " + "x" + " by zero";
                    static String at(String s) { return s + "+0000"; }
                    static Object e() { return new com.legend.sql.dialect.EngineStyleH2(); }
                    static char q() { return '"'; }
                    /* "Can't find " in a block comment is no hit */
                }
                """;
        Map<String, Integer> hits = hits(planted);
        assertEquals(Map.of("Cannot divide ", 1, "+0000", 1, "EngineStyleH2", 1), hits);
    }

    /** Every product main source outside the edge, keyed by its path under core's java root (else under main). */
    private static Map<String, Map<String, Integer>> scan() throws IOException {
        Map<String, Map<String, Integer>> found = new TreeMap<>();
        int scanned = 0;
        for (Path f : SourceFiles.under(MAIN)) {
            String path = SourceFiles.path(f);
            if (!path.endsWith(".java") || path.startsWith(EDGE)) {
                continue;
            }
            scanned++;
            Map<String, Integer> h = hits(Files.readString(f));
            if (!h.isEmpty()) {
                found.put(path.startsWith(JAVA_ROOT) ? path.substring(JAVA_ROOT.length())
                        : path.substring(MAIN.length() + 1), h);
            }
        }
        GuardCoverage.assertFloor("CompatSpellingGuardTest", scanned, 1000);
        return found;
    }

    /** A source's hits: its string literals against the literal list, its code against the symbols. */
    static Map<String, Integer> hits(String source) {
        List<String> literals = new ArrayList<>();
        String code = lex(source, literals);
        Map<String, Integer> out = new TreeMap<>();
        for (String lit : literals) {
            for (String want : LITERALS) {
                if (want.startsWith("^")) {
                    if (lit.matches(Pattern.quote(want.substring(1, want.length() - 1)))) {
                        out.merge(want.substring(1, want.length() - 1), 1, Integer::sum);
                    }
                } else {
                    for (int at = lit.indexOf(want); at >= 0; at = lit.indexOf(want, at + want.length())) {
                        out.merge(want, 1, Integer::sum);
                    }
                }
            }
        }
        for (String sym : SYMBOLS) {
            Matcher m = Pattern.compile("\\b" + Pattern.quote(sym) + "\\b").matcher(code);
            while (m.find()) {
                out.merge(sym, 1, Integer::sum);
            }
        }
        return out;
    }

    /** The code with comments dropped and every string, text block and char literal blanked; each string's and text
     *  block's raw content (escapes unread) is collected. */
    static String lex(String s, List<String> literals) {
        StringBuilder code = new StringBuilder(s.length());
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (s.startsWith("/*", i)) {
                int end = s.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
                code.append(' ');
            } else if (s.startsWith("//", i)) {
                int end = s.indexOf('\n', i);
                i = end < 0 ? n : end;
            } else if (s.startsWith("\"\"\"", i)) {
                int end = s.indexOf("\"\"\"", i + 3);
                while (end > 0 && s.charAt(end - 1) == '\\') {
                    end = s.indexOf("\"\"\"", end + 1);
                }
                int stop = end < 0 ? n : end;
                literals.add(s.substring(i + 3, stop));
                i = end < 0 ? n : end + 3;
                code.append("\"\"");
            } else if (c == '"' || c == '\'') {
                int j = i + 1;
                while (j < n && s.charAt(j) != c && s.charAt(j) != '\n') {
                    j += s.charAt(j) == '\\' ? 2 : 1;
                }
                if (c == '"') {
                    literals.add(s.substring(i + 1, Math.min(j, n)));
                }
                i = j + 1;
                code.append(c).append(c);
            } else {
                code.append(c);
                i++;
            }
        }
        return code.toString();
    }

    /** Today's hits as register rows, the step left for the reader to name. */
    private static String rows(Map<String, Map<String, Integer>> found) {
        StringBuilder b = new StringBuilder();
        found.forEach((file, h) -> h.forEach((spelling, count) -> b.append("            \"").append(file)
                .append(" | ").append(spelling).append(" | ").append(count).append(" | ?\",\n")));
        return b.toString();
    }
}
