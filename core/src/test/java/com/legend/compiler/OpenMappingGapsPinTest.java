package com.legend.compiler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.error.LegendCompileException;
import com.legend.error.MappingResolutionException;
import com.legend.error.ModelException;
import com.legend.exec.PlanSessions;
import com.legend.exec.SessionSource;
import com.legend.result.Collector;
import com.legend.result.Output;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Three findings of the unstructured study (projects/unstructured/FINDINGS.md), each as legend-engine 4.145.0 answers
 * it on the same model (asked of the engine runner; the answers are quoted in FINDINGS.md). F-U1 and F-U2 were laxer
 * than upstream and are closed (2026-11-14): lite refuses where upstream refuses, with upstream's message. Two are still
 * laxer and are pinned at today's behaviour, to turn red the day each closes: F-U2's multi-hop form, and F-U7, a source
 * set id that names no set.
 * F-U5 matches upstream, by the owner's ruling, and is here so a change to it is seen.
 */
class OpenMappingGapsPinTest {

    private static String model(String columnOfA, String sourceSetOfBs, String targetSetOfBs) {
        return """
                ###Pure
                Class gap::A { id: String[1]; }
                Class gap::B { id: String[1]; }
                Association gap::AB { a: gap::A[0..1]; bs: gap::B[*]; }

                ###Relational
                Database gap::Db
                (
                   Table A (ID VARCHAR(10) PRIMARY KEY)
                   Table B (ID VARCHAR(10) PRIMARY KEY, A_ID VARCHAR(10))
                   Join AB(A.ID = B.A_ID)
                )

                ###Mapping
                Mapping gap::M
                (
                   gap::A[setA]: Relational { ~mainTable [gap::Db]A id: [gap::Db]A.%s }
                   gap::B[setB]: Relational { ~mainTable [gap::Db]B id: [gap::Db]B.ID }
                   gap::AB: Relational
                   {
                      AssociationMapping
                      (
                         bs[%s, %s]: [gap::Db]@AB,
                         a[setB, setA]: [gap::Db]@AB
                      )
                   }
                )

                ###Connection
                RelationalDatabaseConnection gap::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

                ###Runtime
                Runtime gap::R { mappings: [ gap::M ]; connections: [ gap::Db: [ c: gap::Conn ] ]; }
                """.formatted(columnOfA, sourceSetOfBs, targetSetOfBs);
    }

    private static final String A_TO_BS = "gap::A.all()->project(~[x:a|$a.bs.id])";
    private static final String B_TO_A = "gap::B.all()->project(~[x:b|$b.a.id])";

    @Test
    void theControlCompilesAndPlansBothDirections() {
        var context = Compiler.compileModel(model("ID", "setA", "setB"));
        assertDoesNotThrow(() -> Compiler.query(context, A_TO_BS).executionPlan("gap::R"));
        assertDoesNotThrow(() -> Compiler.query(context, B_TO_A).executionPlan("gap::R"));
    }

    /** Upstream: {@code Can't find column 'NO_SUCH_COLUMN'} at compile (HelperRelationalBuilder.java:163). */
    @Test
    void fU1_aMappingToAColumnThatDoesNotExistIsRefusedAtCompile() {
        String model = model("NO_SUCH_COLUMN", "setA", "setB");
        ModelException strict = assertThrows(ModelException.class, () -> Compiler.compileModel(model));
        assertEquals(LegendCompileException.Phase.MODEL, strict.phase());
        assertTrue(strict.getMessage().contains("Can't find column 'NO_SUCH_COLUMN'"), strict.getMessage());
        // a tolerant build withholds the set, and a query that reaches it says why
        var module = Compiler.buildModule(Compiler.parseModel(model));
        Exception atPlan = assertThrows(Exception.class, () -> Compiler.query(module.context(),
                "gap::A.all()->project(~[x:a|$a.id])").executionPlan("gap::R"));
        assertTrue(atPlan.getMessage().contains("Can't find column 'NO_SUCH_COLUMN'"), atPlan.getMessage());
    }

    /** Upstream compiles it, refuses the direction through the missing set at planning ({@code Expected 1 class mapping
     *  for 'B' in 'M', found 0}: pureToSQLQuery.pure:1934), and plans the other. */
    @Test
    void fU2_anAssociationMappingToATargetSetIdThatDoesNotExistIsRefusedAtPlanning() {
        var context = assertDoesNotThrow(() -> Compiler.compileModel(model("ID", "setA", "noSuchSet")));
        MappingResolutionException refused = assertThrows(MappingResolutionException.class,
                () -> Compiler.query(context, A_TO_BS).executionPlan("gap::R"));
        assertEquals(LegendCompileException.Phase.MAPPING, refused.phase());
        assertEquals("Expected 1 class mapping for 'B' in 'M', found 0", refused.getMessage());
        assertDoesNotThrow(() -> Compiler.query(context, B_TO_A).executionPlan("gap::R"),
                "the other direction's property mapping names real sets, and upstream plans it");
    }

    /** F-U2's multi-hop form, still open: upstream refuses it as it refuses the single hop (the same assert and
     *  message, asked of the engine runner), but lite navigates a multi-hop end through the property mapping the
     *  normalizer injects into the class's own function (AssociationSynthesis.injectMultiHopAssociationPMs), which reads
     *  no target set id, so the refusal above is never reached. */
    @Test
    void fU2_aMultiHopAssociationMappingToATargetSetIdThatDoesNotExistPlans() {
        String model = """
                ###Pure
                Class gap::A { id: String[1]; }
                Class gap::B { id: String[1]; }
                Association gap::AB { a: gap::A[0..1]; bs: gap::B[*]; }

                ###Relational
                Database gap::Db
                (
                   Table A (ID VARCHAR(10) PRIMARY KEY)
                   Table C (A_ID VARCHAR(10), B_ID VARCHAR(10))
                   Table B (ID VARCHAR(10) PRIMARY KEY)
                   Join AC(A.ID = C.A_ID)
                   Join CB(C.B_ID = B.ID)
                )

                ###Mapping
                Mapping gap::M
                (
                   gap::A[setA]: Relational { ~mainTable [gap::Db]A id: [gap::Db]A.ID }
                   gap::B[setB]: Relational { ~mainTable [gap::Db]B id: [gap::Db]B.ID }
                   gap::AB: Relational
                   {
                      AssociationMapping
                      (
                         bs[setA, noSuchSet]: [gap::Db]@AC > @CB,
                         a[setB, setA]: [gap::Db]@CB > @AC
                      )
                   }
                )

                ###Connection
                RelationalDatabaseConnection gap::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

                ###Runtime
                Runtime gap::R { mappings: [ gap::M ]; connections: [ gap::Db: [ c: gap::Conn ] ]; }
                """;
        var context = Compiler.compileModel(model);
        assertDoesNotThrow(() -> Compiler.query(context, A_TO_BS).executionPlan("gap::R"),
                "F-U2's multi-hop form is closed: the unknown target set id is now refused. Make this pin assert"
                        + " upstream's refusal, as the single hop's does, and change its FINDINGS.md row.");
    }

    /** Upstream refuses the direction whose SOURCE id names no set, at planning, by another assert ({@code The system
     *  can't find a mapping for the property 'gap::A.bs' in the mapping 'gap::M'}: helperFunctions.pure:395), and plans
     *  the other. Lite plans both. */
    @Test
    void fU7_anAssociationMappingFromASourceSetIdThatDoesNotExistPlans() {
        var context = Compiler.compileModel(model("ID", "noSuchSet", "setB"));
        assertDoesNotThrow(() -> Compiler.query(context, A_TO_BS).executionPlan("gap::R"),
                "F-U7 is closed: the unknown source set id is now refused. Change this pin and its FINDINGS.md row.");
        assertDoesNotThrow(() -> Compiler.query(context, B_TO_A).executionPlan("gap::R"));
    }

    /** The row outside ColorMap is the connection's test data, made by the plan's load: no statement of the test's own. */
    @Test
    void fU5_aValueOutsideTheEnumMappingReadsBackAsNullOnARequiredProperty() throws Exception {
        String model = """
                ###Pure
                Enum gap::Color { Red }
                Class gap::Item { id: String[1]; color: gap::Color[1]; }

                ###Relational
                Database gap::Db ( Table ITEM (ID VARCHAR(10) PRIMARY KEY, COLOR VARCHAR(10) NOT NULL) )

                ###Mapping
                Mapping gap::M
                (
                   gap::Color: EnumerationMapping ColorMap { Red: ['Red'] }
                   gap::Item[setItem]: Relational
                   {
                      ~mainTable [gap::Db]ITEM
                      id: [gap::Db]ITEM.ID,
                      color: EnumerationMapping ColorMap: [gap::Db]ITEM.COLOR
                   }
                )

                ###Connection
                RelationalDatabaseConnection gap::Conn
                {
                   store: gap::Db;
                   type: DuckDB;
                   specification: LocalH2 { testDataSetupCSV: 'default\\nITEM\\nID,COLOR\\na,Blue\\n'; };
                   auth: DefaultH2;
                }

                ###Runtime
                Runtime gap::R { mappings: [ gap::M ]; connections: [ gap::Db: [ c: gap::Conn ] ]; }
                """;
        try (Connection connection = TestBackends.platform().open()) {
            Collector collector = new Collector();
            var plan = Execution.run(model, "gap::Item.all()->project(~[id:i|$i.id, color:i|$i.color])", "gap::R",
                    SessionSource.given(TestSessions.over(connection)),
                    PlanSessions.of(TestSessions.over(connection)), new Output.Rows(collector));
            assertTrue(plan.toString().contains("ELSE NULL"), plan.toString());
            List<List<Object>> rows = new ArrayList<>(collector.collected().rows());
            assertEquals(List.of(Arrays.asList("a", null)), rows,
                    "F-U5 has moved: 'Blue' is outside ColorMap and color is [1]. legend-engine answers null here"
                            + " (FINDINGS.md), and the owner ruled lite is not stricter than upstream.");
        }
    }
}
