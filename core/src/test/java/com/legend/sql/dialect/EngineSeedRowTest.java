// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.model.DatabaseDefinition;
import com.legend.setup.Ddl;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The engine's seed rows (setUpDataSQLs), IR the engine-text dialect writes (the dialect program's Phase 3, the PR
 * audit of 2026-10-16): each cell by its column's type, as the engine's {@code convertValuesToCsvH2} reads it, and a
 * table of an included database found where the engine's {@code allSchemas()} finds it.
 */
class EngineSeedRowTest {

    private static final String MODEL = """
            ###Relational
            Database s::Base
            (
              Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20), D DATE, TS TIMESTAMP, B BIT, N DECIMAL(10,2) )
            )
            Database s::Top
            (
              include s::Base
            )
            """;

    private static List<String> seed(String csvRows) {
        ModelContext ctx = Compiler.compileModel(MODEL);
        DatabaseDefinition top = ctx.findDatabase("s::Top").orElseThrow();
        return Ddl.setUpDataSqlsText("default\nT\nID,NAME,D,TS,B,N\n" + csvRows, top, ctx::findDatabase,
                new EngineStyleH2());
    }

    private static String insert(String csvRow) {
        List<String> sqls = seed(csvRow + "\n");
        String last = sqls.get(sqls.size() - 1);
        assertTrue(last.startsWith("insert into "), sqls.toString());
        return last;
    }

    @Test
    void aTableOfAnIncludedDatabaseIsSeeded() {
        assertEquals("insert into T (ID,NAME,D,TS,B,N) values (1,'a','2014-12-01','2014-12-01 10:00:00',true,1.50);",
                insert("1,a,2014-12-01,2014-12-01 10:00:00,y,1.50"));
    }

    @Test
    void eachCellIsWrittenByItsColumnsType() {
        // an empty cell and the engine's ---null--- are null
        assertEquals("insert into T (ID,NAME,D,TS,B,N) values (2,null,null,null,null,null);",
                insert("2,,---null---,,,"));
        // a quote doubled in a string, a date and a timestamp alike
        assertEquals("insert into T (ID,NAME,D,TS,B,N) values (3,'O''Brien','2014''12','2014''12',false,7);",
                insert("3,O'Brien,2014'12,2014'12,n,007"));
        // every spelling of a bit's truth H2's bitValueFromString reads; a number by its value
        for (String t : List.of("y", "1", "true", "Y", "TRUE")) {
            assertTrue(insert("4,x,,," + t + ",1e3").endsWith(",true,1000);"), t);
        }
        for (String f : List.of("n", "0", "false", "N")) {
            assertTrue(insert("5,x,,," + f + ",-2.5").endsWith(",false,-2.5);"), f);
        }
    }

    @Test
    void aCellNoValueOfItsTypeIsRefused() {
        assertThrows(IllegalStateException.class, () -> insert("6,x,,,maybe,1"));
        assertThrows(IllegalStateException.class, () -> insert("7,x,,,y,lots"));
    }

    @Test
    void aColumnTheTableLacksIsRefused() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        DatabaseDefinition top = ctx.findDatabase("s::Top").orElseThrow();
        assertThrows(IllegalStateException.class, () -> Ddl.setUpDataSqlsText("default\nT\nID,COLOUR\n1,red\n", top,
                ctx::findDatabase, new EngineStyleH2()));
    }
}
