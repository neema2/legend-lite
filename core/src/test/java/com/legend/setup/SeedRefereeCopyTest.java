// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.executionplan.DataRef;
import com.legend.executionplan.ExecutionPlan;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A seed's load steps and the referee's copy of them (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §1.1, A2): a table's
 * rows are ONE content blob of inserts, at most {@link RowLoad#ROWS_PER_INSERT} rows each, and the referee's copy is ONE
 * text per load step -- the edge's ledger records a step's text by its place, so a table of more rows than one insert
 * holds must not shift the texts after it. Each cell is its declared column's typed literal; a cell its type cannot take
 * is refused at planning, naming the table, the column and the value.
 */
class SeedRefereeCopyTest {

    static final String MODEL = """
            ###Relational
            Database x::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), ON_DAY DATE, OK BIT) Table U (ID INTEGER) )
            ###Connection
            RelationalDatabaseConnection x::Conn { store: x::DB; type: DuckDB; specification: LocalH2 { %s; }; auth: DefaultH2; }
            ###Runtime
            Runtime x::RT { mappings: [ ]; connections: [ x::DB: [ c: x::Conn ] ]; }
            """;

    static CsvSeed.Planned planned(String csv) {
        ModelContext ctx = Compiler.compileModel(MODEL.formatted("testDataSetupCSV: '" + csv + "'"));
        return CsvSeed.planned(CsvSeed.declaredSteps("x::RT", ctx, Databases.dialect(Databases.PLATFORM)),
                Databases.dialect(Databases.PLATFORM), Databases.dialect(Databases.REPLAY_REFEREE));
    }

    @Test
    void aTableOfManyRowsIsOneBlobStepAndOneRefereeText_theTextsAfterItKeepTheirPlaces() {
        StringBuilder csv = new StringBuilder("default\\nT\\nID,NAME,ON_DAY,OK\\n");
        int rows = 2 * RowLoad.ROWS_PER_INSERT + 1;
        for (int i = 1; i <= rows; i++) {
            csv.append(i).append(",n").append(i).append(",2020-01-02,").append(i % 2 == 0).append("\\n");
        }
        csv.append("\\n-----\\ndefault\\nU\\nID\\n7\\n");
        CsvSeed.Planned planned = planned(csv.toString());
        List<ExecutionPlan.LoadStep> steps = planned.steps().steps();
        assertEquals(steps.size(), planned.refereeCopy().size(), "one referee text per step: " + planned.refereeCopy());
        ExecutionPlan.LoadStep.Statements tBlob = (ExecutionPlan.LoadStep.Statements) steps.stream()
                .filter(ExecutionPlan.LoadStep.Statements.class::isInstance).findFirst().orElseThrow();
        List<String> inserts = DataRef.Statements.decode(planned.steps().content().get(tBlob.ref().digest()));
        assertEquals(3, inserts.size(), "2,001 rows, three inserts of at most 1,000");
        assertTrue(inserts.get(0).contains("(1, 'n1', DATE '2020-01-02', FALSE)"), inserts.get(0));
        int at = steps.indexOf(tBlob);
        String copy = planned.refereeCopy().get(at);
        assertEquals(3, copy.split(";\\s*\n|;\\s*$").length, "the referee's copy of the blob: its three inserts");
        // the step after the blob is U's DDL, and its text is U's
        assertInstanceOf(ExecutionPlan.LoadStep.Statement.class, steps.get(at + 1));
        assertTrue(planned.refereeCopy().get(at + 1).contains("U"), planned.refereeCopy().get(at + 1));
    }

    @Test
    void aCellItsTypeCannotTakeIsRefusedNamingTheTableColumnAndValue() {
        var refused = assertThrows(com.legend.error.DataError.class,
                () -> planned("default\\nT\\nID,NAME,ON_DAY,OK\\nx1,a,2020-01-02,true\\n"));
        assertEquals("the table T's column ID is INTEGER, and its data holds 'x1', which is not one",
                refused.getMessage());
    }

    /** A date cell outside its ISO shape is text the DATABASE casts (the user's ruling, 2026-11-10): upstream quotes it
     *  and its database parses it; Java parses nothing. */
    @Test
    void aDateOutsideItsIsoShapeIsTextTheDatabaseCasts() {
        RowLoad load = new RowLoad(null, "U", com.legend.sql.NameQuoting.BARE, List.of("D"), java.util.Set.of(), 1,
                List.of(List.of("2020-01-02"), List.of("2020-01-02 10:00:00")),
                List.of(new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.DATE)));
        String insert = load.statements(Databases.dialect(Databases.PLATFORM)).get(0);
        assertTrue(insert.contains("DATE '2020-01-02'") && insert.contains("'2020-01-02 10:00:00'")
                && insert.toUpperCase(java.util.Locale.ROOT).contains("CAST("), insert);
    }

    @Test
    void aNumberThatIsNoIntegerInAnIntegerColumnIsItsDigits_whichTheDatabaseAssigns() {
        RowLoad load = new RowLoad(null, "U", com.legend.sql.NameQuoting.BARE, List.of("ID"), java.util.Set.of(), 1,
                List.of(List.of("1.0"), List.of("2.5")),
                List.of(new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.INTEGER)));
        assertEquals(List.of("INSERT INTO U (ID) VALUES (1), (2.5);"),
                load.statements(Databases.dialect(Databases.PLATFORM)));
    }
}
