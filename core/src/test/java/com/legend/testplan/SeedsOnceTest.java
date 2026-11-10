// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.database.Databases;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A runtime's declared test data seeds a test body's statement once, however many times the statement names it (the
 * correctness audit's C3, 2026-11-10): with no runtime given the test door runs a statement on its from()'s runtime,
 * and the seeds took that runtime's declared steps for the from() and again for the program -- an author's setup SQL
 * failing "already exists" the second time, plain inserts doubling the rows.
 */
class SeedsOnceTest {

    @Test
    void aRuntimeNamedByTheFromAndTheProgramIsSeededOnce() {
        String model = """
                ###Relational
                Database x::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )
                ###Connection
                RelationalDatabaseConnection x::Conn { store: x::DB; type: %s; specification: LocalH2 {
                    testDataSetupSqls: ['CREATE TABLE T (ID INTEGER, NAME VARCHAR(20))', 'INSERT INTO T VALUES (1, ''a'')'];
                }; auth: DefaultH2; }
                ###Runtime
                Runtime x::RT { mappings: [ ]; connections: [ x::DB: [ c: x::Conn ] ]; }
                """.formatted(Databases.PLATFORM.name());
        var ctx = Compiler.compileModel(model);
        var statement = Compiler.query(ctx, "|#>{x::DB.T}#->select(~[ID, NAME])->from(x::RT)").body().get(0);
        var dialect = Databases.dialect(Databases.PLATFORM);
        int fromAlone = Seeds.planned(statement, null, ctx, dialect).steps().steps().size();
        int withTheProgram = Seeds.planned(statement, "x::RT", ctx, dialect).steps().steps().size();
        assertEquals(fromAlone, withTheProgram, "the runtime's declared steps planned twice");
    }
}
