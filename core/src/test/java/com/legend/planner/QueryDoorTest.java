// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.database.Databases;
import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.test.StorelessRuntime;
import org.junit.jupiter.api.Test;

/**
 * THE QUERY DOOR (the test-is-the-product plan's M3c, B12 item 1, B6): a product query holding a kind only a test's
 * program holds is refused by name before anything is planned; {@code fail} (a raise) and the plan's own runtime's
 * test data (its target's setup) are the product's, as M3a measured them in working queries.
 */
class QueryDoorTest {

    private static final String CONN_LET =
            "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                    + "type=meta::relational::runtime::DatabaseType.DuckDB);\n";

    /** Two runtimes over one store: {@code x::Seeded}'s connection declares test data, {@code x::Plain}'s none. */
    private static final String SEEDED = """
            Class x::Firm { name: String[1]; }
            ###Relational
            Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
            ###Mapping
            Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM name: [x::DB] FIRM.NAME } )
            ###Connection
            RelationalDatabaseConnection x::SeededConn
            {
                store: x::DB;
                type: H2;
                specification: LocalH2 { testDataSetupCSV: 'default\\nFIRM\\nID,NAME\\n1,Acme\\n'; };
                auth: DefaultH2;
            }
            RelationalDatabaseConnection x::PlainConn
            {
                store: x::DB;
                type: H2;
                specification: LocalH2 { };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime x::Seeded { mappings: [x::M]; connections: [ x::DB: [ env: x::SeededConn ] ]; }
            Runtime x::Plain { mappings: [x::M]; connections: [ x::DB: [ env: x::PlainConn ] ]; }
            """;

    private static TypedQuery storeless(String body) {
        return Compiler.query(Compiler.compileModel(StorelessRuntime.with("", Databases.PLATFORM)), body);
    }

    private static String refusal(TypedQuery query, String runtime) {
        return assertThrows(NotImplementedException.class, () -> query.executionPlan(runtime)).getMessage();
    }

    @Test
    void anAssertIsRefusedByName() {
        assertEquals("an assert in a query: asserts are a test's, and the product's query door plans none",
                refusal(storeless("|assert(1 == 1)"), StorelessRuntime.RUNTIME));
        assertEquals("an assert in a query: asserts are a test's, and the product's query door plans none",
                refusal(storeless("{|let x = 1; assertEquals(1, $x);}"), StorelessRuntime.RUNTIME));
    }

    @Test
    void anAssertErrorIsRefusedByItsName() {
        assertEquals("an assertError in a query: asserts are a test's, and the product's query door plans none",
                refusal(storeless("{|assertError(|[1,2]->at(3), 'x')}"), StorelessRuntime.RUNTIME));
    }

    @Test
    void anEffectIsRefusedByName() {
        assertEquals("an effect in a query: effects run in a test's program, and the product's query door plans none",
                refusal(storeless(CONN_LET + "meta::relational::metamodel::execute::executeInDb('Create Table q(i INT);',"
                        + " $c, 0, 1000); 1;}"), StorelessRuntime.RUNTIME));
    }

    @Test
    void aLetBoundExecuteIsRefusedAndAFailIsNoAssert() {
        TypedQuery let = Compiler.query(Compiler.compileModel(SEEDED), "{| let r = meta::pure::router::execute("
                + "|x::Firm.all()->project([f|$f.name], ['name']), x::M, x::Plain.runtimeValue, []);\n"
                + "$r.values;}");
        assertEquals("a let-bound execute(...) in a query: Pure runs it at the let, a test's eager run; a query reads"
                + " an execute(...)'s result inline, and the product's query door plans none", refusal(let, "x::Plain"));
        // fail is a raise, not a verdict: a Cypher write's guard calls it (M3a)
        TypedQuery raise = storeless("|fail('no')");
        assertNull(new QueryDoor.Scan(raise, StorelessRuntime.RUNTIME).of(raise.body().get(0)));
    }

    @Test
    void anotherRuntimesTestDataIsRefusedAndThePlansOwnIsItsSetup() {
        String read = "|x::Firm.all()->project([f|$f.name], ['name'])->from(x::M, x::Seeded)";
        assertEquals("the test data of a runtime that is not the plan's in a query: seeds are a test's; the plan's own"
                + " runtime's are its target's setup, and the product's query door plans none",
                refusal(Compiler.query(Compiler.compileModel(SEEDED), read), "x::Plain"));
        // the plan's own runtime: its test data is its target's setup step
        ExecutionPlan own = Compiler.query(Compiler.compileModel(SEEDED), read).executionPlan("x::Seeded");
        assertInstanceOf(ExecutionPlan.TdsResult.class, own.root());
        assertFalse(own.answerSql().target().setup().isEmpty(), "the declared rows, the target's setup");
    }
}
