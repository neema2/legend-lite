package com.legend.compiler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.error.LegendCompileException;
import com.legend.error.MappingResolutionException;
import com.legend.error.ModelException;
import org.junit.jupiter.api.Test;

/**
 * F-U1 and F-U2 (projects/unstructured/FINDINGS.md) beyond OpenMappingGapsPinTest's model: the other places a column is
 * read, and the shapes an association's set ids take across an include. Each expected answer is legend-engine
 * 4.145.0's, asked of the engine runner on the same model with an H2 connection (2026-11-14; the recipe of
 * docs/result-boundary-homework/README.md section 7), and quoted beside its test.
 */
class MappingGapsAsUpstreamTest {

    private static final String STORE = """
            ###Pure
            Class gap::A { id: String[1]; bname: String[0..1]; }
            Class gap::B { id: String[1]; }
            Association gap::AB { a: gap::A[0..1]; bs: gap::B[*]; }

            ###Relational
            Database gap::Db
            (
               Table A (ID VARCHAR(10) PRIMARY KEY)
               Table B (ID VARCHAR(10) PRIMARY KEY, A_ID VARCHAR(10))
               Join AB(A.ID = B.A_ID)
            )

            ###Connection
            RelationalDatabaseConnection gap::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

            ###Runtime
            Runtime gap::R { mappings: [ gap::M ]; connections: [ gap::Db: [ c: gap::Conn ] ]; }

            ###Mapping
            """;

    private static final String SET_A = "   gap::A[setA]: Relational { ~mainTable [gap::Db]A id: [gap::Db]A.ID }\n";
    private static final String SET_B = "   gap::B[setB]: Relational { ~mainTable [gap::Db]B id: [gap::Db]B.ID }\n";
    private static final String A_TO_BS = "gap::A.all()->project(~[x:a|$a.bs.id])";
    private static final String B_TO_A = "gap::B.all()->project(~[x:b|$b.a.id])";

    private static String association(String lines) {
        return "   gap::AB: Relational\n   {\n      AssociationMapping\n      (\n" + lines + "      )\n   }\n";
    }

    private static void refusedAtCompile(String model, String upstream) {
        ModelException e = assertThrows(ModelException.class, () -> Compiler.compileModel(model));
        assertEquals(LegendCompileException.Phase.MODEL, e.phase());
        assertTrue(e.getMessage().endsWith(upstream), e.getMessage());
    }

    private static void refusedAtPlanning(String model, String query, String upstream) {
        var context = assertDoesNotThrow(() -> Compiler.compileModel(model));
        MappingResolutionException e = assertThrows(MappingResolutionException.class,
                () -> Compiler.query(context, query).executionPlan("gap::R"));
        assertEquals(LegendCompileException.Phase.MAPPING, e.phase());
        assertEquals(upstream, e.getMessage());
    }

    // ---- F-U1: each place a column is read

    /** Upstream: {@code Can't find column 'NOPE'}. */
    @Test
    void aPrimaryKeyColumnThatDoesNotExist() {
        refusedAtCompile(STORE + "Mapping gap::M\n(\n   gap::A[setA]: Relational { ~primaryKey ([gap::Db]A.NOPE)"
                + " ~mainTable [gap::Db]A id: [gap::Db]A.ID }\n)\n", "Can't find column 'NOPE'");
    }

    /** Upstream: {@code Can't find column 'NOPE'}. */
    @Test
    void aJoinsTerminalColumnThatDoesNotExist() {
        refusedAtCompile(STORE + "Mapping gap::M\n(\n   gap::A[setA]: Relational { ~mainTable [gap::Db]A"
                + " id: [gap::Db]A.ID, bname: [gap::Db]@AB | B.NOPE }\n)\n", "Can't find column 'NOPE'");
    }

    /** Upstream: {@code Can't find table 'NOTABLE' in schema 'default' and database 'Db'}. */
    @Test
    void aTableThatDoesNotExist() {
        refusedAtCompile(STORE + "Mapping gap::M\n(\n   gap::A[setA]: Relational { ~mainTable [gap::Db]A"
                + " id: [gap::Db]NOTABLE.ID }\n)\n", "Can't find table 'NOTABLE' in schema 'default' and database 'Db'");
    }

    /** Upstream: {@code Can't find column 'NO_STREET'}. */
    @Test
    void anEmbeddedColumnThatDoesNotExist() {
        refusedAtCompile("""
                ###Pure
                Class gap::P { id: String[1]; addr: gap::Addr[1]; }
                Class gap::Addr { street: String[1]; }

                ###Relational
                Database gap::Db ( Table P (ID VARCHAR(10) PRIMARY KEY, STREET VARCHAR(10)) )

                ###Mapping
                Mapping gap::M
                (
                   gap::P[setP]: Relational { ~mainTable [gap::Db]P id: [gap::Db]P.ID, addr ( street: [gap::Db]P.NO_STREET ) }
                )
                """, "Can't find column 'NO_STREET'");
    }

    // ---- F-U2: the set ids across an include, and every direction's id missing

    /** The included mapping holds A and the one property mapping; its target set is the including mapping's. Upstream
     *  plans it, resolving the id under the queried mapping. */
    @Test
    void aTargetSetTheIncludingMappingHoldsPlans() {
        var context = Compiler.compileModel(STORE + "Mapping gap::Inner\n(\n" + SET_A
                + association("         bs[setA, setB]: [gap::Db]@AB\n") + ")\n"
                + "Mapping gap::M\n(\n   include gap::Inner\n" + SET_B + ")\n");
        assertDoesNotThrow(() -> Compiler.query(context, A_TO_BS).executionPlan("gap::R"));
    }

    /** The same, its target set nowhere. Upstream: {@code Expected 1 class mapping for 'B' in 'M', found 0}. */
    @Test
    void aTargetSetNoMappingOfTheClosureHoldsIsRefused() {
        refusedAtPlanning(STORE + "Mapping gap::Inner\n(\n" + SET_A
                + association("         bs[setA, noSuchSet]: [gap::Db]@AB\n") + ")\n"
                + "Mapping gap::M\n(\n   include gap::Inner\n" + SET_B + ")\n",
                A_TO_BS, "Expected 1 class mapping for 'B' in 'M', found 0");
    }

    /** Both directions' target ids missing. Upstream refuses each: {@code Expected 1 class mapping for 'B' in 'M', found
     *  0}, and for {@code $b.a} the same with {@code 'A'}. */
    @Test
    void everyDirectionsTargetSetMissingRefusesEach() {
        String model = STORE + "Mapping gap::M\n(\n" + SET_A + SET_B + association(
                "         bs[setA, noSuchSet]: [gap::Db]@AB,\n         a[setB, noSuchSet2]: [gap::Db]@AB\n") + ")\n";
        refusedAtPlanning(model, A_TO_BS, "Expected 1 class mapping for 'B' in 'M', found 0");
        refusedAtPlanning(model, B_TO_A, "Expected 1 class mapping for 'A' in 'M', found 0");
    }

    /** A tolerant build compiles it as a strict one does, and planning refuses the same direction. */
    @Test
    void aTolerantBuildRefusesTheSameDirection() {
        var module = Compiler.buildModule(Compiler.parseModel(STORE + "Mapping gap::M\n(\n" + SET_A + SET_B
                + association("         bs[setA, noSuchSet]: [gap::Db]@AB,\n         a[setB, setA]: [gap::Db]@AB\n")
                + ")\n"));
        assertTrue(module.walls().isEmpty(), module.walls().toString());
        MappingResolutionException e = assertThrows(MappingResolutionException.class,
                () -> Compiler.query(module.context(), A_TO_BS).executionPlan("gap::R"));
        assertEquals("Expected 1 class mapping for 'B' in 'M', found 0", e.getMessage());
        assertDoesNotThrow(() -> Compiler.query(module.context(), B_TO_A).executionPlan("gap::R"));
    }

    /** Two property mappings of one property, each naming a missing target id: upstream refuses {@code $a.bs} by its
     *  union path's assert, {@code  Did not find setID x1 from properties bs in any in scope mapping}
     *  (pureToSQLQuery_union.pure:386), and plans {@code $b.a}. Lite refuses with F-U2's message: an open finding,
     *  pinned here to turn red when it changes. */
    @Test
    void twoMissingTargetIdsOfOnePropertyAreRefusedInF_U2sWords() {
        String model = STORE + "Mapping gap::M\n(\n" + SET_A + SET_B + association(
                "         bs[setA, x1]: [gap::Db]@AB,\n         bs[setA, x2]: [gap::Db]@AB,\n"
                        + "         a[setB, setA]: [gap::Db]@AB\n") + ")\n";
        refusedAtPlanning(model, A_TO_BS, "Expected 1 class mapping for 'B' in 'M', found 0");
        var context = Compiler.compileModel(model);
        assertDoesNotThrow(() -> Compiler.query(context, B_TO_A).executionPlan("gap::R"));
    }
}
