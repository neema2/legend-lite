// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE JAVA TEST FIXTURES' WRITES, BOUNDED (B2c, 2026-11-11, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6's B2c, the
 * user's yes: bounded by their guard, not converted; census M15's fixture half). A test that sets up its database by
 * JDBC sends the statements itself -- a CREATE, an INSERT, a DROP written as a literal into an {@code execute},
 * {@code executeUpdate}, {@code addBatch}, {@code executeQuery} or {@code prepareStatement} call. Each file that does is
 * pinned with its count; a new file, or a count that changes, fails here until its pin moves with a dated note naming
 * why. The total has a ceiling that only falls. Reads stay free (the read register was deleted 2026-09-29 for making
 * every test file ask), and so does a statement held in a variable (a shared seeder's: counted where it is written, as
 * a literal). Measured 2026-11-11: 906 literal write sends in 116 files.
 */
@Tag("census")
class FixtureWriteCensusTest {

    /** The test sources a fixture lives in (the census target's declared files). */
    private static final List<String> ROOTS = List.of("core/src/test/java", "spec/src", "testing/src/main/java",
            "warehouse/src/test");

    /** A send whose text is a literal, and the literal's first word: a JDBC send's, or a test's own text typed for a
     *  session's statements ({@code testbackend.Unplanned.sql}: since the C checkpoint's MUST-FIX 1, 2026-11-12, a
     *  session takes a PlannedSql, and a test's literal written through it is a send all the same). */
    private static final Pattern SEND = Pattern.compile(
            "(?:\\.(execute|executeUpdate|executeLargeUpdate|addBatch|executeQuery|prepareStatement)\\s*\\(|Unplanned"
                    + "\\.sql\\()\\s*(\"\"\"|\")\\s*(\\w+)");

    /** The words that write. */
    private static final Set<String> WRITES = Set.of("CREATE", "INSERT", "DROP", "UPDATE", "DELETE", "ALTER", "COPY",
            "MERGE", "GRANT", "TRUNCATE");

    /** The total's ceiling: it only falls. 906 -> 910 (2026-11-11, the B2a audit's S3 and S4): the session table's
     *  drop-safety tests make the USER'S table of its name -- the one row no plan may hold, since the plan's own must
     *  never reach it -- on every in-process database that spells one (DataTableSessionTest, 2) and on Postgres
     *  (DataDeliveryMatrixPostgresTest, 2). 910 -> 912 (2026-11-11, B2d, the user's ruling 3, register S191): the
     *  generator's faithful-output case seeds a cell holding a line break (TestDataPlannerTest, 2), which the test-data
     *  reader cannot read until B2d.2 makes it quote-aware -- then those two become its CSV and the ceiling falls back.
     *  912 -> 911 (2026-11-11, B2d.2's audit, H2 and M4): those two are TestDataPlannerTest's CSV now (the reader
     *  quote-aware), and GeneratedTestDataProgramTest seeds one text cell holding the empty text (1), which no CSV can
     *  seed (the reader loads an empty cell as NULL), for the reload's empty-text-as-NULL case. 911 -> 907 (2026-11-11,
     *  the JSON pass merged: no session table remains, so its drop-safety tests went -- DataTableSessionTest deleted,
     *  DataDeliveryMatrixPostgresTest's user's table of the name with it, 4). 907 -> 908 (2026-11-12, the B checkpoint's
     *  G4, the orchestrator's go): the reload keeps a keyless table's ('', 1) and (NULL, 1) as two rows, as upstream
     *  inserts each text row -- TestDataPlannerTest seeds the empty text (1), which no CSV can seed (the reader loads an
     *  empty cell as NULL); the table and its NULL row are its CSV. 908 -> 909 (2026-11-12, the B2d.3 audit's M4): the
     *  seed's COLLATE "C" on Postgres is shown on a column collated otherwise (PostgresArmTest, 1: one ALTER, the
     *  column's own collation, which no model or CSV can declare -- the embedded server's initdb --no-locale makes the
     *  database's own binary); the table and its rows are its CSV. 909 -> 912 (2026-11-12, the C checkpoint's MUST-FIX
     *  1): a session's statements take a PlannedSql, a test's own text typed by {@code Unplanned.sql}, which this census
     *  counts as a send (CatalogTurnFailureTest's 6 kept); ProgramRunnerTest's savepoint stand-ins, three INSERTs a
     *  {@code Savepoint} record held as strings this census did not see, are counted now (3). 912 -> 917 (2026-11-12,
     *  the third re-audit's MUST-FIX 2): a DECLARED H2 database is the user's, made by its user -- its table and rows
     *  written by the test as the user would, which no plan may do (DatabaseSettingsH2Test, 2: the point is that lite
     *  leaves it as it found it); and H2's own keywords as columns on a bare session, which no model can declare and no
     *  plan's load may write bare (H2KeywordsTest, 3). 917 -> 918 (2026-11-12, the same item's Cypher store, located by the
     *  D worker): its property tables' row, written by the dialect's own spelling on a bare session (H2KeywordsTest, 1).
     *  918 -> 922 (2026-11-12, the final audit's item 2, a DataCube table Output as one plan): the box's own table, made
     *  and filled as a page's file or a person's table is (DuckDbOutputPlanTest, 2: the Output reads a table its host
     *  holds, never one a model declares), a table of every DuckDB type a source column can hold, which no model can
     *  declare (1), and today's CREATE of the rows' statement LIMIT 0, the form the plan replaces, measured beside it
     *  (1). */
    private static final int CEILING = 922;

    /** Each file's literal write sends (2026-11-11, B2c: the baseline as measured). */
    private static final Map<String, Integer> PINNED = Map.ofEntries(
            Map.entry("core/src/test/java/com/legend/AssertVerdictSpliceTest.java", 2),
            Map.entry("core/src/test/java/com/legend/DecimalKindsTest.java", 2),
            Map.entry("core/src/test/java/com/legend/KeyedPivotKeyRoundTripTest.java", 2),
            Map.entry("core/src/test/java/com/legend/StoreTypesDuckDbTest.java", 2),
            Map.entry("core/src/test/java/com/legend/TabularFunctionTest.java", 3),
            Map.entry("core/src/test/java/com/legend/WireDecidedCellTest.java", 6),
            Map.entry("core/src/test/java/com/legend/compiler/spec/UserCallInlinerTest.java", 2),
            Map.entry("core/src/test/java/com/legend/exec/CatalogTurnFailureTest.java", 6),
            Map.entry("core/src/test/java/com/legend/exec/ContainsOverloadTest.java", 2),
            Map.entry("core/src/test/java/com/legend/exec/DataLoadsTest.java", 6),
            // 2026-11-12, the third re-audit's MUST-FIX 2: a declared database's own table, made by its user (the
            // ceiling's note)
            Map.entry("core/src/test/java/com/legend/exec/DatabaseSettingsH2Test.java", 2),
            // 2026-11-12, the C checkpoint's MUST-FIX 1: the savepoint's three stand-in texts, typed by Unplanned.sql
            Map.entry("core/src/test/java/com/legend/exec/ProgramRunnerTest.java", 3),
            // 2026-11-11, the B2a audit's S3: a user's table of a session table's name (the ceiling's note)
            // 2026-11-11, the B2a audit's S4: a user's table of a session table's name on Postgres (the ceiling's note)
            Map.entry("core/src/test/java/com/legend/exec/ExecuteFrameTest.java", 2),
            Map.entry("core/src/test/java/com/legend/exec/ExecuteInDbDuckDbTest.java", 4),
            Map.entry("core/src/test/java/com/legend/exec/ExecutorTest.java", 4),
            // 2026-11-12, the final audit's item 2: the box's own table, every DuckDB type's, and today's CREATE beside
            // the Output's plan (the ceiling's note)
            Map.entry("core/src/test/java/com/legend/planner/DuckDbOutputPlanTest.java", 4),
            Map.entry("core/src/test/java/com/legend/exec/MissingValueInComputedColumnTest.java", 3),
            Map.entry("core/src/test/java/com/legend/exec/Phase1AuditTest.java", 1),
            Map.entry("core/src/test/java/com/legend/exec/PlanRunnerTest.java", 1),
            Map.entry("core/src/test/java/com/legend/exec/RowLoadH2Test.java", 1),
            Map.entry("core/src/test/java/com/legend/exec/TaggedListTest.java", 2),
            Map.entry("core/src/test/java/com/legend/integration/AbstractDatabaseTest.java", 4),
            Map.entry("core/src/test/java/com/legend/integration/CypherIslandIntegrationTest.java", 9),
            Map.entry("core/src/test/java/com/legend/integration/DuckDBIntegrationTest.java", 247),
            Map.entry("core/src/test/java/com/legend/integration/EnumIntegrationTest.java", 16),
            Map.entry("core/src/test/java/com/legend/integration/GetCheckerTest.java", 4),
            Map.entry("core/src/test/java/com/legend/integration/GraphFetchCheckedIntegrationTest.java", 12),
            Map.entry("core/src/test/java/com/legend/integration/InheritanceIntegrationTest.java", 5),
            Map.entry("core/src/test/java/com/legend/integration/IsDistinctByTreeIntegrationTest.java", 4),
            Map.entry("core/src/test/java/com/legend/integration/JsonMappingIntegrationTest.java", 30),
            Map.entry("core/src/test/java/com/legend/integration/M2M2RTabularTest.java", 5),
            Map.entry("core/src/test/java/com/legend/integration/M2MChainIntegrationTest.java", 17),
            Map.entry("core/src/test/java/com/legend/integration/M2MIntegrationTest.java", 10),
            Map.entry("core/src/test/java/com/legend/integration/PivotJoinColumnTest.java", 2),
            Map.entry("core/src/test/java/com/legend/integration/PostgresRegressCorpusTest.java", 2),
            Map.entry("core/src/test/java/com/legend/integration/RelationApiIntegrationTest.java", 14),
            Map.entry("core/src/test/java/com/legend/integration/RelationCellsSortTest.java", 4),
            Map.entry("core/src/test/java/com/legend/integration/RelationMappingWindowSeamTest.java", 2),
            Map.entry("core/src/test/java/com/legend/integration/RenderCsvIntegrationTest.java", 2),
            Map.entry("core/src/test/java/com/legend/integration/SQLiteIntegrationTest.java", 4),
            Map.entry("core/src/test/java/com/legend/integration/SqlIslandIntegrationTest.java", 1),
            Map.entry("core/src/test/java/com/legend/integration/StreamingIntegrationTest.java", 2),
            Map.entry("core/src/test/java/com/legend/integration/UnionJoinMappedPropertyTest.java", 4),
            Map.entry("core/src/test/java/com/legend/integration/UnionTargetLeanJoinTest.java", 10),
            Map.entry("core/src/test/java/com/legend/integration/UserFunctionIntegrationTest.java", 7),
            Map.entry("core/src/test/java/com/legend/integration/VariantIntegrationTest.java", 4),
            Map.entry("core/src/test/java/com/legend/integration/WindowFunctionTest.java", 9),
            Map.entry("core/src/test/java/com/legend/integration/WriteCheckerTest.java", 2),
            Map.entry("core/src/test/java/com/legend/integration/WritePlansH2Test.java", 2),
            Map.entry("core/src/test/java/com/legend/ladder/FrameQuotedColumnTest.java", 2),
            Map.entry("core/src/test/java/com/legend/ladder/LadderRender.java", 2),
            Map.entry("core/src/test/java/com/legend/lowering/AnyLiteralByteDecidabilityTest.java", 1),
            Map.entry("core/src/test/java/com/legend/lowering/CorrelatedComputedColumnTest.java", 1),
            Map.entry("core/src/test/java/com/legend/lowering/GroupByAverageMappingTest.java", 2),
            Map.entry("core/src/test/java/com/legend/lowering/JoinTortureTest.java", 8),
            Map.entry("core/src/test/java/com/legend/lowering/LowerRelationDuckDbTest.java", 20),
            Map.entry("core/src/test/java/com/legend/lowering/NullSemanticsTest.java", 2),
            Map.entry("core/src/test/java/com/legend/lowering/OptionalCollectionNullDropTest.java", 2),
            Map.entry("core/src/test/java/com/legend/lowering/QuotedColumnNameTest.java", 2),
            Map.entry("core/src/test/java/com/legend/lowering/ScaleCostsDuckDbTest.java", 1),
            Map.entry("core/src/test/java/com/legend/lowering/SqlCanonConformanceH2Test.java", 2),
            Map.entry("core/src/test/java/com/legend/normalizer/AssocSimpleNameProbeTest.java", 5),
            Map.entry("core/src/test/java/com/legend/normalizer/AssociationSetIdTest.java", 6),
            Map.entry("core/src/test/java/com/legend/normalizer/AssociationViewJoinTest.java", 8),
            Map.entry("core/src/test/java/com/legend/resolver/ChainTypeDispatchTest.java", 6),
            Map.entry("core/src/test/java/com/legend/resolver/NavPrefixCollisionTemporalTest.java", 4),
            Map.entry("core/src/test/java/com/legend/resolver/NavigationDepthTest.java", 10),
            Map.entry("core/src/test/java/com/legend/resolver/NestedExistsCorrelationStampTest.java", 6),
            Map.entry("core/src/test/java/com/legend/resolver/PivotOverClassQueryTest.java", 2),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveDeepEmptinessProbeTest.java", 6),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveDerivedLeafProbeTest.java", 4),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveFilterDemandTest.java", 10),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveGraphUnionProbeTest.java", 42),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveM2mTest.java", 6),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveNavigationTest.java", 6),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveNestedNavTest.java", 8),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveOtherwiseTest.java", 4),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveOuterDatedNavTest.java", 6),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveSerializeTest.java", 4),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveSimpleClassTest.java", 10),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveUnionChainTest.java", 7),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveUnionJtcProbeTest.java", 8),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveUnionMultiHopProbeTest.java", 8),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveUnionOuterDateProbeTest.java", 6),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveUnionSelfJoinProbeTest.java", 4),
            Map.entry("core/src/test/java/com/legend/resolver/ResolveUnionTest.java", 24),
            Map.entry("core/src/test/java/com/legend/resolver/RoutedChainKeyTest.java", 8),
            Map.entry("core/src/test/java/com/legend/resolver/RoutedEquiJoinTest.java", 4),
            Map.entry("core/src/test/java/com/legend/resolver/RoutedNavigateTest.java", 6),
            Map.entry("core/src/test/java/com/legend/resolver/RuntimeIfClassQueryTest.java", 2),
            Map.entry("core/src/test/java/com/legend/resolver/RuntimeTypeDispatchTest.java", 4),
            Map.entry("core/src/test/java/com/legend/resolver/StackDesignWitnessTest.java", 24),
            Map.entry("core/src/test/java/com/legend/resolver/StackRatchetWitnessTest.java", 18),
            Map.entry("core/src/test/java/com/legend/resolver/StackShapeWitnessTest.java", 14),
            Map.entry("core/src/test/java/com/legend/resolver/ValueMapPlacementTest.java", 2),
            Map.entry("core/src/test/java/com/legend/server/CypherParityPostgresTest.java", 3),
            Map.entry("core/src/test/java/com/legend/server/IqParityPostgresTest.java", 2),
            // 2026-11-12, the B2d.3 audit's M4: a column's own collation on Postgres (the ceiling's note)
            Map.entry("core/src/test/java/com/legend/server/PostgresArmTest.java", 2),
            Map.entry("core/src/test/java/com/legend/server/SqlIslandPostgresTest.java", 4),
            Map.entry("core/src/test/java/com/legend/server/SqlParityPostgresTest.java", 2),
            Map.entry("core/src/test/java/com/legend/setup/RowLoadCellsTest.java", 2),
            Map.entry("core/src/test/java/com/legend/sql/DuckDbValidityTest.java", 6),
            Map.entry("core/src/test/java/com/legend/sql/dialect/CarrierDifferentialTest.java", 14),
            Map.entry("core/src/test/java/com/legend/sql/dialect/CatalogGridTest.java", 3),
            Map.entry("core/src/test/java/com/legend/sql/dialect/CatalogModelTest.java", 2),
            Map.entry("core/src/test/java/com/legend/sql/dialect/H2FirstInGroupTest.java", 2),
            // 2026-11-12, the third re-audit's MUST-FIX 2(c): H2's keywords as columns on a bare session (the ceiling's
            // note)
            Map.entry("core/src/test/java/com/legend/sql/dialect/H2KeywordsTest.java", 4),
            Map.entry("core/src/test/java/com/legend/sql/dialect/H2SplitPartTest.java", 2),
            Map.entry("core/src/test/java/com/legend/sql/dialect/HostOperationsTest.java", 3),
            Map.entry("core/src/test/java/com/legend/sql/dialect/SqlWriterTest.java", 2),
            Map.entry("core/src/test/java/com/legend/sql/dialect/TempTableAsTest.java", 2),
            Map.entry("core/src/test/java/com/legend/testbackend/PostgresBackend.java", 2),
            Map.entry("core/src/test/java/com/legend/testcases/CypherParity.java", 4),
            Map.entry("core/src/test/java/com/legend/testcases/IqParity.java", 1),
            Map.entry("core/src/test/java/com/legend/testcases/PostgresSeed.java", 4),
            Map.entry("core/src/test/java/com/legend/testcases/SqlParity.java", 1),
            // 2026-11-11, B2d.2's audit (M4): the reload's empty-text-as-NULL case seeds a text cell holding the empty
            // text, which no CSV can seed (the test-data reader loads an empty cell as NULL). TestDataPlannerTest's two
            // (B2d, ruling 3) are its CSV now, the reader quote-aware (H2): their pin went
            Map.entry("core/src/test/java/com/legend/testdatagen/GeneratedTestDataProgramTest.java", 1),
            // 2026-11-12, the B checkpoint's G4: the reload keeps a keyless table's ('', 1) and (NULL, 1) as two rows;
            // its empty text, which no CSV can seed, written as it is (the table and its NULL row are its CSV)
            Map.entry("core/src/test/java/com/legend/testdatagen/TestDataPlannerTest.java", 1),
            Map.entry("core/src/test/java/com/legend/testplan/RenderedTextTest.java", 2),
            Map.entry("spec/src/test/java/com/legend/harness/ReplayReferee.java", 7),
            Map.entry("testing/src/main/java/com/legend/testing/EmbeddedPostgres.java", 1),
            Map.entry("warehouse/src/test/java/com/legend/warehouse/WarehouseJdbcTest.java", 12));

    static int writes(String source) {
        int n = 0;
        Matcher m = SEND.matcher(source);
        while (m.find()) {
            if (WRITES.contains(m.group(3).toUpperCase(java.util.Locale.ROOT))) {
                n++;
            }
        }
        return n;
    }

    @Test
    void theFixturesWritesAreBounded() throws IOException {
        Map<String, Integer> found = new TreeMap<>();
        for (String root : ROOTS) {
            List<Path> files = SourceFiles.under(root);
            assertTrue(files.stream().anyMatch(f -> f.toString().endsWith(".java")), "the census read no Java under "
                    + root + ": declare it in the census target's files");
            for (Path f : files) {
                if (!f.toString().endsWith(".java") || f.getFileName().toString().equals("FixtureWriteCensusTest.java")) {
                    continue;
                }
                int n = writes(Files.readString(f, StandardCharsets.UTF_8));
                if (n > 0) {
                    found.put(SourceFiles.path(f), n);
                }
            }
        }
        assertEquals(new TreeMap<>(PINNED), found, "a test file's JDBC writes moved: a new file, or a count that"
                + " changed, moves its pin with a dated note naming why (fewer is the direction: the fixtures' data is"
                + " the plan's loads where a test can say it as a model)");
        int total = found.values().stream().mapToInt(Integer::intValue).sum();
        assertTrue(total <= CEILING, total + " literal write sends, above the ceiling " + CEILING);
    }

    @Test
    void theCountReadsALiteralWriteAndNoReadOrVariable() {
        assertEquals(3, writes("st.execute(\"CREATE TABLE T (ID INT)\"); st.executeUpdate(\n  \"insert into T values"
                + " (1)\"); c.prepareStatement(\"DROP TABLE T\");"));
        assertEquals(0, writes("st.executeQuery(\"SELECT 1\"); st.execute(sql); st.execute(\"SET x = 1\");"));
    }
}
