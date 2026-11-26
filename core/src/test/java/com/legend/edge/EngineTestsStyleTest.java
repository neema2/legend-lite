// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TestExecution;
import com.legend.TypedQuery;
import com.legend.database.PlanStyle;
import com.legend.testexecutionplan.TestPlanJson;
import com.legend.testplan.PlanningOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * LEGEND-ENGINE'S OWN TESTS' STYLE ({@link PureText#ENGINE_TESTS}; the edition program's step 3, 2026-11-26): a test
 * of legend-engine's is Pure code around the queries it hands to {@code execute()}. The queries get legend-engine's
 * meaning (its table's rows); the test's own code is stamped and keeps Pure's, also where it works on a frame's rows.
 * Each case is read against the same body planned under {@link PureText#UPSTREAM}, which hands lite's edition: the
 * two plans are the same text exactly where no call of a query's is one the table answers.
 */
class EngineTestsStyleTest {

    private static final String MODEL = """
            Class x::Firm { name: String[1]; }
            ###Relational
            Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
            ###Mapping
            Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM name: [x::DB] FIRM.NAME } )
            ###Connection
            RelationalDatabaseConnection x::Conn
            {
                store: x::DB;
                type: DuckDB;
                specification: LocalH2 { testDataSetupCSV: 'default\\nFIRM\\nID,NAME\\n1,Acme\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ env: x::Conn ] ]; }
            """;

    private static String executed(String query) {
        return "meta::pure::router::execute(|" + query + ", x::M, x::RT.runtimeValue, [])";
    }

    private static final String NAMES = "x::Firm.all()->project([f|$f.name], ['name'])";

    /** The mark every raise lite writes carries in its SQL text. */
    private static final String RAISE = "LG#";

    /** {@code body}'s plan under {@code style}, as text: the body admitted as legend-engine's runners read it. */
    private static String planned(String body, PlanStyle style) {
        TypedQuery typed = UpstreamTestBodies.admitted(Compiler.query(Compiler.compileModel(MODEL), body));
        return TestPlanJson.write(TestExecution.planBody(typed, "x::RT",
                ExecuteOptions.NONE.withStyle(style).withJudgeMode(PlanningOptions.JudgeMode.HOST)));
    }

    @Test
    @DisplayName("a call inside the query an execute() runs is legend-engine's: the plan differs from lite's edition's")
    void theQueryIsLegendEngines() {
        String body = "{| let r = " + executed("x::Firm.all()->filter(f|$f.name->substring(1, 3) == 'cm')"
                + "->project([f|$f.name], ['name'])") + ";\n$r.values->filter(x|$x.name == 'Acme');}";
        assertNotEquals(planned(body, PureText.UPSTREAM), planned(body, PureText.ENGINE_TESTS));
        // lite's substring writes Pure's range check, a raise; legend-engine's is the database's own, with none
        assertTrue(planned(body, PureText.UPSTREAM).contains(RAISE), "lite's edition checks the range");
        assertFalse(planned(body, PureText.ENGINE_TESTS).contains(RAISE), "legend-engine's writes no check");
    }

    @Test
    @DisplayName("the same call in the test's own code, over a frame's rows, is Pure's: the plan is lite's edition's")
    void theTestsOwnCodeIsPures() {
        String body = "{| let r = " + executed(NAMES) + ";\n$r.values->filter(x|$x.name->substring(1, 3) == 'cm');}";
        assertEquals(planned(body, PureText.UPSTREAM), planned(body, PureText.ENGINE_TESTS));
    }

    @Test
    @DisplayName("a query the test binds to a let and then hands to execute() is still legend-engine's (the"
            + " independent audit of 2026-11-26, finding 1: the corpus writes most of its queries this way)")
    void aQueryBoundByALetIsLegendEngines() {
        String query = "x::Firm.all()->filter(f|$f.name->substring(1, 3) == 'cm')->project([f|$f.name], ['name'])";
        String bound = "{| let q = {|" + query + "};\nlet r = meta::pure::router::execute($q, x::M,"
                + " x::RT.runtimeValue, []);\n$r.values->filter(x|$x.name == 'Acme');}";
        assertTrue(planned(bound, PureText.UPSTREAM).contains(RAISE), "lite's edition checks the range");
        assertFalse(planned(bound, PureText.ENGINE_TESTS).contains(RAISE), "legend-engine's writes no check");
    }

    @Test
    @DisplayName("a let INSIDE the executed query that reads no store is legend-engine's own Java's, Pure's meaning,"
            + " as at its API")
    void aQuerysOwnStoreFreeLetIsPures() {
        String body = "{| let r = meta::pure::router::execute(|let cut = 'Acme'->substring(1, 3);"
                + " x::Firm.all()->filter(f|$f.name == $cut)->project([f|$f.name], ['name']);, x::M,"
                + " x::RT.runtimeValue, []);\n$r.values->filter(x|$x.name == 'Acme');}";
        assertEquals(planned(body, PureText.UPSTREAM), planned(body, PureText.ENGINE_TESTS));
    }

    @Test
    @DisplayName("a let of the test's own, read inside a later query, keeps Pure's meaning there")
    void aTestsLetReadInsideAQueryIsPures() {
        String body = "{| let cut = 'Acme'->substring(1, 3);\nlet r = " + executed("x::Firm.all()"
                + "->filter(f|$f.name == $cut)->project([f|$f.name], ['name'])") + ";\n"
                + "$r.values->filter(x|$x.name == 'Acme');}";
        assertEquals(planned(body, PureText.UPSTREAM), planned(body, PureText.ENGINE_TESTS));
    }
}
