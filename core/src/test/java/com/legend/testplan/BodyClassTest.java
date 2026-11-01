// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.database.Databases;
import com.legend.test.StorelessRuntime;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * THE CLASSES OF BODIES THAT RUN WHOLE ON THE PROGRAM ROAD (the test-is-the-product plan's R16, M5): a closed list
 * that only GROWS (a class moved is never moved back), each by a named rule over the body's shape.
 */
class BodyClassTest {

    /** The classes moved, in the order they moved: append only (M10 moves the last). */
    private static final List<String> MOVED = List.of("EFFECT_PROGRAM",
            // M5f (2026-11-01): the plain query programs
            "QUERY_PROGRAM");

    @Test
    void theMovedClassesOnlyGrow() {
        List<String> now = Arrays.stream(BodyClass.values()).map(Enum::name).toList();
        assertEquals(MOVED, now.subList(0, Math.min(now.size(), MOVED.size())), "a moved class was removed or renamed");
        assertEquals(MOVED.size(), now.size(), "a class was added: append it to MOVED with its step, dated");
    }

    private static final String CONN = "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
            + "type=meta::relational::runtime::DatabaseType.DuckDB);\n";
    private static final String EXEC = "meta::relational::metamodel::execute::executeInDb(";

    private static BodyClass classOf(String body) {
        TypedQuery q = Compiler.query(Compiler.compileModel(StorelessRuntime.with("", Databases.PLATFORM)), body);
        return BodyClass.of(q, StorelessRuntime.RUNTIME);
    }

    @Test
    void aSetupProgramIsAnEffectProgram() {
        assertEquals(BodyClass.EFFECT_PROGRAM, classOf(CONN + EXEC + "'Create Table m5_t(i INT);', $c, 0, 1000);\n"
                + EXEC + "'Insert into m5_t values (1);', $c, 0, 1000);\ntrue;}"));
    }

    @Test
    void aBodyOfProductStatementsIsAQueryProgram() {
        assertEquals(BodyClass.QUERY_PROGRAM, classOf("{| let x = 1; $x + 1;}"));
        assertEquals(BodyClass.QUERY_PROGRAM, classOf("{| print('x', 1); 'abc'->toUpper();}"));
    }

    @Test
    void aBodyWithAnAssertOrAComputedArgumentStaysOnTheWalk() {
        assertNull(classOf(CONN + EXEC + "'Create Table m5_a(i INT);', $c, 0, 1000);\nassert(1 == 1);}"));
        assertNull(classOf("{| assert(1 == 1);}"));
        // a String argument computed from a value only the run knows (the walk folds a let's literal, as the test door)
        assertNull(classOf(CONN + EXEC + "'Create Table t' + now()->toString() + '(i INT);', $c, 0, 1000);}"));
    }

    @Test
    void anEffectsResultReadAsAValueStaysOnTheWalk() {
        assertNull(classOf(CONN + "let r = " + EXEC + "'select 1', $c, 0, 1000);\n$r;}"));
    }
}
