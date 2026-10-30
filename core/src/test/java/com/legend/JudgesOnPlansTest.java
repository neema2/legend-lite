// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend;

import com.legend.judge.AssertListener;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BOTH JUDGES ON PLANS (the plan path program's step 6, docs/PLAN_PATH_PROGRAM_2026_10_17.md §6): every assert body
 * below runs under the agreement gate ({@link ExecuteOptions.JudgeMode#BOTH}) on every in-process database -- the host judge
 * reading each side as a plan's result decoded by its declared types, the database judge's verdict a plan whose one
 * row is the verdict -- and holds with the two judges in agreement, every assert judged twice (a kind the database
 * judge left UNJUDGED would be named here, with the host deciding; none is, measured 2026-10-18).
 */
class JudgesOnPlansTest {

    /** A table of three rows, for the sides that read a store. */
    private static final String MODEL = """
            Class t::P { name: String[1]; age: Integer[1]; }
            Class t::K { <<equality.Key>> id: Integer[1]; other: String[1]; }
            Enum t::Kind { CITY, REGION }
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20), PRICE DECIMAL(10,2), D DATE,
                TS TIMESTAMP, B BIT ) )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: %s;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,NAME,PRICE,D,TS,B\\n1,a,1.50,2024-01-02,2024-01-02 10:30:00,true\\n2,b,2.25,2024-02-03,2024-02-03 11:00:00.123,false\\n3,c,3.00,2024-03-04,2024-03-04 00:00:00,true\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    private static final String T = "#>{s::DB.T}#";

    /** A body, whether the database judge is expected to judge it (false: a shape it names UNJUDGED today), and
     *  whether it runs on every in-process database or on the platform's engine alone (a list value and a class
     *  instance need a list encoding H2's dialect has not: a dialect capability, not a judge's matter). */
    private record Case(String body, boolean judgedTwice, boolean everyDatabase) {
        Case(String body, boolean judgedTwice) {
            this(body, judgedTwice, true);
        }
    }

    /** A case the platform's engine alone runs. */
    private static final boolean PLATFORM_ONLY = false;

    private static final List<Case> CASES = List.of(
            // primitives: a literal against a computed value of each kind
            new Case("assertEquals(3, 1 + 2)", true),
            new Case("assertEquals(3.0, 2 * 1.5)", true),
            new Case("assertEquals(0.00000025, 2.5e-7)", true),
            new Case("assertEquals(2.50D, 1.25D * 2)", true),
            new Case("assertEquals('ab', 'a' + 'b')", true),
            new Case("assertEquals(true, 1 < 2)", true),
            new Case("assertEquals(%2024-01-02, %2024-01-02)", true),
            new Case("assertEquals(%2024-01-02T10:30:00.123, %2024-01-02T10:30:00.123)", true),
            new Case("assertEquals(t::Kind.CITY, t::Kind.CITY)", true),
            new Case("assertEquals('a', ['a', 'b']->head())", true),
            new Case("assertEquals([1, 2, 3], [3, 1, 2]->sort())", true, PLATFORM_ONLY),
            new Case("assertSameElements([1, 2, 3], [3, 1, 2])", true),
            new Case("assertSize([1, 2], 2)", true),
            new Case("assertEmpty([])", true),
            new Case("assertNotEmpty([1])", true),
            new Case("assert(1 < 2)", true),
            new Case("assertFalse(2 < 1)", true),
            new Case("assertEq(2, 1 + 1)", true),
            new Case("assertEqWithinTolerance(1.0, 1.001, 0.01)", true),
            new Case("assertContains([1, 2], 2)", true),
            new Case("assertNotEquals(1, 2)", true),
            // the store: scalars read through a relation, and grids
            new Case("assertEquals(3, " + T + "->size())", true),
            new Case("assertEquals('b', " + T + "->filter(r|$r.ID == 2)->select(~NAME)->toOne().NAME)", true),
            new Case("assertEquals(2.25D, " + T + "->filter(r|$r.ID == 2)->select(~PRICE)->toOne().PRICE)", true),
            new Case("assertEquals(%2024-02-03, " + T + "->filter(r|$r.ID == 2)->select(~D)->toOne().D)", true),
            // a value root's timestamp in the judges' value lane: its digits trimmed, as the database judge's canon
            // spells it (a grid's cell keeps the engine's nine; step 7, 2026-10-18)
            new Case("assertEquals(%2024-02-03T11:00:00.123, " + T
                    + "->filter(r|$r.ID == 2)->select(~TS)->toOne().TS)", true),
            new Case("assertEquals(false, " + T + "->filter(r|$r.ID == 2)->select(~B)->toOne().B)", true),
            new Case("assertEquals(#TDS\n  ID, NAME\n  2, b\n#, " + T
                    + "->filter(r|$r.ID == 2)->select(~[ID, NAME]))", true),
            new Case("assertEquals(#TDS\n  ID\n  1\n  2\n  3\n#, " + T + "->select(~ID)->sort(~ID->ascending()))", true),
            // class instances: the host judge decodes each instance's structure and judges by Pure's rule -- two
            // keyless instances are distinct (identity), keyed instances equal on their keys -- and the database
            // judge judges the same pairs by their identity and key canons (measured 2026-10-18: both agree)
            new Case("assertNotEquals(^t::P(name='a', age=1), ^t::P(name='a', age=1))", true, PLATFORM_ONLY),
            new Case("assertEquals(^t::K(id=1, other='x'), ^t::K(id=1, other='y'))", true, PLATFORM_ONLY));

    @Test
    void everyBodyHoldsUnderBothJudges_onThePlatform() throws Exception {
        assertEveryBodyHolds(com.legend.testbackend.TestBackends.platform());    // and on H2: JudgesOnPlansH2Test
    }

    /** {@link #everyBodyHoldsUnderBothJudges_onThePlatform} on {@code backend} (main's merge, 2026-10-22: one test named
     *  DuckDB and H2; each database's run is its own test's). */
    static void assertEveryBodyHolds(com.legend.testbackend.TestBackend backend) throws Exception {
        List<String> failures = new ArrayList<>();
        List<String> table = new ArrayList<>();
        {
            DatabaseType type = backend.type();
            var ctx = Compiler.compileModel(MODEL.formatted(type.name()));
            for (Case c : CASES) {
                if (!c.everyDatabase() && backend != com.legend.testbackend.TestBackends.platform()) {
                    continue;
                }
                List<String> told = new ArrayList<>();
                String outcome;
                try (Connection conn = backend.open()) {
                    com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery("|" + c.body() + ";");
                    List<com.legend.protocol.spec.ValueSpecification> statements =
                            ((com.legend.protocol.spec.LambdaFunction) parsed).body();
                    Execution.runProgram(Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                            new com.legend.model.ImportScope(List.of()), ctx)), "s::RT",
                            com.legend.Execution.Source.given(com.legend.testbackend.TestSessions.over(conn)), new AssertListener() {
                                @Override
                                public void verdict(String assertName, boolean pass,
                                        @com.legend.base.Nullable String detail) {
                                    told.add((pass ? "pass " : "FAIL ") + assertName
                                            + (detail == null ? "" : ": " + detail));
                                }

                                @Override
                                public void declined(String assertName, String reason) {
                                    told.add("declined: " + reason);
                                }

                                @Override
                                public void unjudged(String assertName, String reason) {
                                    told.add("unjudged: " + reason);
                                }
                            }, null, ExecuteOptions.NONE.withJudgeMode(ExecuteOptions.JudgeMode.BOTH));
                    boolean passed = told.stream().anyMatch(l -> l.startsWith("pass "));
                    boolean onceOnly = told.stream().anyMatch(l -> l.startsWith("unjudged") || l.startsWith("declined"));
                    outcome = !passed ? "NO VERDICT " + told : onceOnly ? "host decided " + told : "both agree";
                    if (!passed) {
                        failures.add(type + " | " + c.body() + " -> " + outcome);
                    } else if (c.judgedTwice() && onceOnly) {
                        failures.add(type + " | " + c.body() + " -> expected both judges, " + outcome);
                    } else if (!c.judgedTwice() && !onceOnly) {
                        failures.add(type + " | " + c.body() + " -> the database judge now judges it: say so");
                    }
                } catch (RuntimeException e) {
                    outcome = "ERROR " + e.getClass().getSimpleName() + ": " + whole(e.getMessage());
                    failures.add(type + " | " + c.body() + " -> " + outcome);
                }
                table.add(type + "\t" + c.body().replace("\n", "\\n") + "\t" + outcome);
            }
        }
        System.out.println("[judges on plans] database\tbody\toutcome");
        table.forEach(System.out::println);
        assertTrue(failures.isEmpty(), String.join("\n", failures));
    }

    @Test
    void aFailingAssertFailsUnderBothJudges_withTheHostsMessage() throws Exception {
        var ctx = Compiler.compileModel(MODEL.formatted(com.legend.database.Databases.PLATFORM.name()));
        try (Connection conn = com.legend.testbackend.TestBackends.platform().open()) {
            com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery("|assertEquals(1, 1 + 1);");
            var failed = org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.AssertFailed.class,
                    () -> Execution.runProgram(Compiler.queryResolved(ctx, Compiler.resolveQuery(
                            ((com.legend.protocol.spec.LambdaFunction) parsed).body(),
                            new com.legend.model.ImportScope(List.of()), ctx)), "s::RT",
                            com.legend.Execution.Source.given(com.legend.testbackend.TestSessions.over(conn)), null, null,
                            ExecuteOptions.NONE.withJudgeMode(ExecuteOptions.JudgeMode.BOTH)));
            assertTrue(failed.getMessage().contains("expected: 1"), failed.getMessage());
            assertTrue(failed.getMessage().contains("actual:   2"), failed.getMessage());
            assertEquals(-1, failed.getMessage().indexOf("DISAGREE"), failed.getMessage());
        }
    }

    private static String whole(@com.legend.base.Nullable String s) {
        return s == null ? "" : s.strip().replaceAll("\\s*\\R\\s*", " | ");
    }
}
