package com.legend.compiler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.exec.PlanSessions;
import com.legend.result.Collector;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins three findings of the unstructured study (projects/unstructured/FINDINGS.md) at today's behaviour. F-U1 and F-U2
 * are laxer than legend-engine, which refuses both: each is here to turn red the day the gap closes, and then it and its
 * FINDINGS.md row are deleted. F-U5 matches legend-engine, by the owner's ruling, and is here so a change to it is seen.
 */
class OpenMappingGapsPinTest {

    private static String model(String columnOfA, String targetSetOfBs) {
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
                         bs[setA, %s]: [gap::Db]@AB,
                         a[setB, setA]: [gap::Db]@AB
                      )
                   }
                )

                ###Connection
                RelationalDatabaseConnection gap::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

                ###Runtime
                Runtime gap::R { mappings: [ gap::M ]; connections: [ gap::Db: [ c: gap::Conn ] ]; }
                """.formatted(columnOfA, targetSetOfBs);
    }

    @Test
    void fU1_aMappingToAColumnThatDoesNotExistCompiles() {
        String model = model("NO_SUCH_COLUMN", "setB");
        var context = assertDoesNotThrow(() -> Compiler.compileModel(model),
                "F-U1 is closed: compileModel now refuses the column. Delete this pin and its FINDINGS.md row.");
        // Today only a query that reaches the property finds it.
        Exception atPlan = assertThrows(Exception.class,
                () -> Compiler.query(context, "gap::A.all()->project(~[x:a|$a.id])").executionPlan("gap::R"));
        assertTrue(atPlan.getMessage().contains("NO_SUCH_COLUMN"), atPlan.getMessage());
    }

    @Test
    void fU2_anAssociationMappingToASetIdThatDoesNotExistPlans() {
        String model = model("ID", "noSuchSet");
        assertDoesNotThrow(() -> Compiler.query(Compiler.compileModel(model),
                        "gap::A.all()->project(~[x:a|$a.bs.id])").executionPlan("gap::R"),
                "F-U2 is closed: the unknown set id is now refused. Delete this pin and its FINDINGS.md row.");
    }

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
                RelationalDatabaseConnection gap::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

                ###Runtime
                Runtime gap::R { mappings: [ gap::M ]; connections: [ gap::Db: [ c: gap::Conn ] ]; }
                """;
        try (Connection connection = TestBackends.platform().open();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE ITEM (ID VARCHAR(10) PRIMARY KEY, COLOR VARCHAR(10) NOT NULL)");
            statement.execute("INSERT INTO ITEM VALUES ('a', 'Blue')");
            Collector collector = new Collector();
            var plan = Execution.run(model, "gap::Item.all()->project(~[id:i|$i.id, color:i|$i.color])", "gap::R",
                    Execution.Source.given(TestSessions.over(connection)),
                    PlanSessions.of(TestSessions.over(connection)), ran -> collector);
            assertTrue(plan.toString().contains("ELSE NULL"), plan.toString());
            List<List<Object>> rows = new ArrayList<>(collector.collected().rows());
            assertEquals(List.of(Arrays.asList("a", null)), rows,
                    "F-U5 has moved: 'Blue' is outside ColorMap and color is [1]. legend-engine answers null here"
                            + " (FINDINGS.md), and the owner ruled lite is not stricter than upstream.");
        }
    }
}
