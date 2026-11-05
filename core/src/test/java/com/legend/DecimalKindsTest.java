// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.error.AssertFailed;
import com.legend.error.ModelException;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import com.legend.testing.Program;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * EXACT AND APPROXIMATE NUMBERS DECLARED ON THE MODEL (the user, 2026-11-05: "add some tests for decimal and decimal
 * with precision too in the model"). A class says how a number computes by the type it declares, as legend-pure
 * declares them (m3.pure; precisePrimitives.pure, read in the pinned sources):
 * <ul>
 *   <li>{@code Decimal} is exact: compiled Pure carries it as {@code java.math.BigDecimal}
 *       (TypeProcessor), and a DECIMAL column computes it exactly, so {@code 0.10 + 0.20} is {@code 0.30};</li>
 *   <li>{@code Numeric(precision, scale)} is a {@code Decimal} with its digits declared
 *       ({@code Primitive Numeric(precision:Integer[1], scale:Integer[1]) extends Decimal}): it computes as a Decimal
 *       and goes wherever a Decimal is declared;</li>
 *   <li>{@code Float} is an IEEE double: compiled Pure carries it as {@code double}, and a DOUBLE column computes
 *       {@code 0.1 + 0.2} as {@code 0.30000000000000004}.</li>
 * </ul>
 * Equality: lite's own rules compare two Decimals by value, as the database's {@code ==} does (S139, the user's
 * ruling of 2026-11-05); compatibility mode compares them by value AND scale, as both Pure runtimes do
 * ({@code BigDecimal.equals}); a Float never equals a Decimal. The model's declarations are refused as the
 * engine's compiler refuses them (CompileContext.newGenericType, its messages): a primitive takes exactly the type
 * variable values it declares, each of its variable's type. Each case runs on the platform's engine
 * (a lane's floor re-spells the connection for its database), under the lane's judge.
 */
class DecimalKindsTest {

    /** The model on the platform's engine; a lane's floor re-spells its connection for the lane's database. */
    private static String model() {
        return """
                ###Pure
                import meta::pure::precisePrimitives::*;
                Class m::Price
                {
                    id: Integer[1];
                    a: Decimal[1];
                    b: Decimal[1];
                    na: Numeric(10, 2)[1];
                    nb: Numeric(10, 2)[1];
                    fa: Float[1];
                    fb: Float[1];
                }
                function m::twice(d: Decimal[1]): Decimal[1] { $d * 2 }
                ###Relational
                Database s::DB ( Table T (ID INTEGER PRIMARY KEY, A DECIMAL(10,2), B DECIMAL(10,2),
                    NA DECIMAL(10,2), NB DECIMAL(10,2), FA DOUBLE, FB DOUBLE) )
                ###Mapping
                Mapping m::M (
                  m::Price: Relational { ~mainTable [s::DB] T
                    id: T.ID, a: T.A, b: T.B, na: T.NA, nb: T.NB, fa: T.FA, fb: T.FB }
                )
                ###Connection
                RelationalDatabaseConnection s::C { store: s::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime m::RT { mappings: [m::M]; connections: [ s::DB: [ c0: s::C ] ]; }
                """;
    }

    /** Ten rows: every A is 0.10, B is 0.20 on row 1 and 0.25 elsewhere; NA, NB 0.10, 0.20; FA, FB 0.1, 0.2. */
    private static Connection seeded() throws Exception {
        Connection c = TestBackends.platform().open();
        try (Statement st = c.createStatement()) {
            st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, A DECIMAL(10,2), B DECIMAL(10,2), NA DECIMAL(10,2),"
                    + " NB DECIMAL(10,2), FA DOUBLE, FB DOUBLE)");
            for (int i = 1; i <= 10; i++) {
                st.execute("INSERT INTO T VALUES (" + i + ", 0.10, " + (i == 1 ? "0.20" : "0.25")
                        + ", 0.10, 0.20, 0.1, 0.2)");
            }
        }
        return c;
    }

    /** Row 1's projection of {@code column}: one value, in a column of its declared type. */
    private static Lean.Result rowOne(Connection c, String column) {
        return Program.last(model(), "|m::Price.all()->filter(p|$p.id == 1)->project([p|" + column + "], ['v'])",
                "m::RT", c);
    }

    private static Program.Report judged(Connection c, String body) {
        return Program.run(model(), body, "m::RT", c);
    }

    private static final String ROW_ONE = "m::Price.all()->filter(p|$p.id == 1)->toOne()";

    @Test
    @DisplayName("a Decimal property computes exactly: 0.10 + 0.20 is the Decimal 0.30, carried as a BigDecimal")
    void decimalIsExact() throws Exception {
        try (Connection c = seeded()) {
            Lean.Result r = rowOne(c, "$p.a + $p.b");
            assertEquals("Decimal", r.columns().get(0).type());
            assertEquals(new BigDecimal("0.30"), assertInstanceOf(BigDecimal.class, r.one()));
        }
    }

    @Test
    @DisplayName("a Float property over a DOUBLE column computes as an IEEE double: 0.1 + 0.2 is 0.30000000000000004")
    void floatIsDouble() throws Exception {
        try (Connection c = seeded()) {
            Lean.Result r = rowOne(c, "$p.fa + $p.fb");
            assertEquals("Float", r.columns().get(0).type());
            assertEquals(0.30000000000000004, assertInstanceOf(Double.class, r.one()));
        }
    }

    @Test
    @DisplayName("a Numeric(10, 2) property is a Decimal: it computes exactly and goes where a Decimal is declared")
    void numericIsADecimal() throws Exception {
        try (Connection c = seeded()) {
            Lean.Result sum = rowOne(c, "$p.na + $p.nb");
            assertEquals("Decimal", sum.columns().get(0).type());
            assertEquals(new BigDecimal("0.30"), assertInstanceOf(BigDecimal.class, sum.one()));
            Lean.Result twice = rowOne(c, "m::twice($p.na)");
            assertEquals("Decimal", twice.columns().get(0).type());
            assertEquals(0, new BigDecimal("0.20").compareTo(assertInstanceOf(BigDecimal.class, twice.one())),
                    String.valueOf(twice.one()));
        }
    }

    @Test
    @DisplayName("assertEquals holds for a Decimal of the same value and scale; a column's sum and a filter by a Decimal literal are exact")
    void decimalAssertsHold() throws Exception {
        try (Connection c = seeded()) {
            assertTrue(judged(c, "|assertEquals(0.30D, " + ROW_ONE + ".a + " + ROW_ONE + ".b)").passed());
            assertTrue(judged(c, "|assertEquals(1.00D, m::Price.all()->map(p|$p.a)->sum())").passed());
            assertTrue(judged(c, "|assertEquals([1], m::Price.all()->filter(p|$p.b == 0.2D)->map(p|$p.id))")
                    .passed());
        }
    }

    @Test
    @DisplayName("lite compares a Decimal by value (0.3D is the column's 0.30, S139); compatibility mode by value and scale, as Pure; a Float never equals a Decimal")
    void decimalEqualityByRules() throws Exception {
        String scaled = "|assertEquals(0.3D, " + ROW_ONE + ".a + " + ROW_ONE + ".b)";
        try (Connection c = seeded()) {
            assertTrue(judged(c, scaled).passed());
            AssertFailed scale = assertThrows(AssertFailed.class, () -> Program.run(model(), scaled, "m::RT", c,
                    com.legend.ExecuteOptions.NONE.withJudgeMode(Program.laneJudge()).withEngineCompat(true)));
            assertEquals("\nexpected: 0.3D\nactual:   0.30D", scale.getMessage());
            AssertFailed kind = assertThrows(AssertFailed.class,
                    () -> judged(c, "|assertEquals(0.3, " + ROW_ONE + ".a + " + ROW_ONE + ".b)"));
            assertEquals("\nexpected: 0.3\nactual:   0.30D", kind.getMessage());
            assertThrows(AssertFailed.class, () -> judged(c, "|assertEquals(0.4D, " + ROW_ONE + ".a + " + ROW_ONE + ".b)"));
        }
    }

    @Test
    @DisplayName("clean mode compares a Float exactly (0.1 + 0.2 is not 0.3); compatibility mode holds it within 2 ULP")
    void floatEqualityIsExactInCleanMode() throws Exception {
        String body = "|assertEquals(0.3, " + ROW_ONE + ".fa + " + ROW_ONE + ".fb)";
        try (Connection c = seeded()) {
            AssertFailed exact = assertThrows(AssertFailed.class, () -> judged(c, body));
            assertEquals("\nexpected: 0.3\nactual:   0.30000000000000004", exact.getMessage());
            assertTrue(Program.run(model(), body, "m::RT", c, com.legend.ExecuteOptions.NONE
                    .withJudgeMode(Program.laneJudge()).withEngineCompat(true)).passed());
        }
    }

    @Test
    @DisplayName("a cast to Numeric(10, 2) is a type assertion: the value as it is, scale and all (S140)")
    void castKeepsTheValue() throws Exception {
        try (Connection c = seeded()) {
            // judged under the compatibility rules, which compare a Decimal's scale too: 1.25D stays 1.25D
            assertTrue(Program.run(model(), "|assertEquals(1.25D, 1.25D->cast(@Numeric(10, 2)))", "m::RT", c,
                    com.legend.ExecuteOptions.NONE.withJudgeMode(Program.laneJudge()).withEngineCompat(true)).passed());
        }
    }

    @Test
    @DisplayName("a list of Decimals compares by value in lite (the column's 0.10s are 0.1D, S139)")
    void decimalListByValue() throws Exception {
        try (Connection c = seeded()) {
            assertTrue(judged(c, "|assertEquals([0.1D, 0.1D], m::Price.all()->filter(p|$p.id <= 2)->map(p|$p.a))")
                    .passed());
        }
    }

    private static ModelException refused(String declaration) {
        return assertThrows(ModelException.class, () -> Compiler.compileModel(
                "###Pure\nClass x::A\n{\n  v: " + declaration + "[1];\n}\n"));
    }

    private static void compiles(String imports, String declaration) {
        Compiler.compileModel("###Pure\n" + imports + "Class x::A\n{\n  v: " + declaration + "[1];\n}\n");
    }

    @Test
    @DisplayName("the precise primitives resolve by their short names, through the core imports or an explicit import")
    void precisePrimitivesResolveByShortName() {
        compiles("", "Numeric(10, 2)");
        compiles("", "Varchar(200)");
        compiles("", "Int");
        compiles("import meta::pure::precisePrimitives::*;\n", "Numeric(10, 2)");
        compiles("", "meta::pure::precisePrimitives::Numeric(10, 2)");
        // a package the model imports itself comes first: there Varchar is the relational datatype CLASS
        compiles("import meta::relational::metamodel::datatype::*;\n", "Varchar");
    }

    @Test
    @DisplayName("a primitive takes exactly the type variable values it declares, each of its variable's type (the engine's messages)")
    void typeVariableValuesAreChecked() {
        assertTrue(refused("Decimal(1)").getMessage().endsWith("Wrong type variables count (1) for type: Decimal"));
        assertTrue(refused("Float(1)").getMessage().endsWith("Wrong type variables count (1) for type: Float"));
        assertTrue(refused("Numeric").getMessage().endsWith(
                "Wrong type variables count (0) for type: Numeric(precision:Integer,scale:Integer)"));
        assertTrue(refused("Numeric()").getMessage().endsWith(
                "Wrong type variables count (0) for type: Numeric(precision:Integer,scale:Integer)"));
        assertTrue(refused("Numeric(10)").getMessage().endsWith(
                "Wrong type variables count (1) for type: Numeric(precision:Integer,scale:Integer)"));
        assertTrue(refused("Varchar").getMessage().endsWith(
                "Wrong type variables count (0) for type: Varchar(x:Integer)"));
        assertTrue(refused("Numeric('a', 2)").getMessage().endsWith(
                "Error for type: Numeric(precision:Integer,scale:Integer), 'a' is not an instance of Integer"));
    }
}
