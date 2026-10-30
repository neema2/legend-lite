// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.Program;
import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.setup.Ddl;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.EngineStyleH2;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A table or schema its store declared QUOTED ({@code Table "Items"}, {@code Schema "Sales"}) is one name to every
 * consumer of the plan. Decision D8 first carried the declaration as quote characters inside the name, and the PR #39
 * audit (2026-10-14) found each consumer that read the name broken: the engine text's alias ({@code ""items"_0"}),
 * the plan text's table lookup (a crash), the column lineage's (a refusal), and the engine's setup text and DDL
 * natives (half the statements quoted). The name is its text; the declaration is data ({@code sql.NameQuoting}).
 * Legend-engine's H2 text, run on H2 (DeclaredQuotedTableTest until the dialect program's Phase 6, 2026-10-16); the
 * DDL natives on the platform's engine.
 */
class DeclaredQuotedTableH2Test {

    private static final String MODEL = """
            ###Relational
            Database q::DB (
              Schema "Sales" ( Table "Orders" (id INTEGER PRIMARY KEY, "Amt" INTEGER, name VARCHAR(20)) )
              Table "Items" (id INTEGER PRIMARY KEY, label VARCHAR(20))
              Table lowerq (id INTEGER PRIMARY KEY)
            )
            ###Mapping
            Mapping q::M ( )
            ###Connection
            RelationalDatabaseConnection q::Conn { store: q::DB; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            ###Runtime
            Runtime q::RT { mappings: [q::M]; connections: [ q::DB: [ c1: q::Conn ] ]; }
            """;

    private static String engineText(EngineStyleH2 printer, String query) {
        ModelContext ctx = Compiler.compileModel(MODEL);
        return printer.render(Compiler.query(ctx, query).lower("q::RT", false).plan()).replace("\n", " ");
    }

    /** The engine text names a quoted table as the store wrote it, and aliases it by that name. */
    @Test
    void theEngineTextAliasesAQuotedTableByItsName() {
        assertEquals("select \"items_0\".id from (select top 2 * from Items as \"root\") as \"items_0\""
                        + " where \"items_0\".id > 0",
                engineText(new EngineStyleH2(), "#>{q::DB.Items}#->limit(2)->filter(r|$r.id > 0)->select(~[id])"));
        String quoted = engineText(new EngineStyleH2(true), "#>{q::DB.Sales.Orders}#->select(~[id, Amt])");
        assertTrue(quoted.contains("from \"Sales\".\"Orders\" as \"root\""), quoted);
        assertFalse(quoted.contains("\"\""), quoted);
    }

    /** The engine's own setup text spells every statement of a quoted schema and table one way, so it runs on H2,
     *  and the engine text's query reads the rows it loaded. (Its statements for the {@code default} schema, which H2
     *  refuses as a keyword, are older than D8 and not this test's: skipped.) */
    @Test
    void theEngineSetupTextRunsAndItsQueryReadsTheRows() throws Exception {
        ModelContext ctx = Compiler.compileModel(MODEL);
        var db = ctx.findDatabase("q::DB").orElseThrow();
        String csv = String.join("\n", "Sales", "Orders", "id,Amt,name", "1,10,a", "2,20,b");
        List<String> setup = Ddl.setUpDataSqlsText(csv, db, ctx::findDatabase, new EngineStyleH2());
        String query = engineText(new EngineStyleH2(), "#>{q::DB.Sales.Orders}#->select(~[id, Amt])");
        try (Connection c = TestBackends.of(DatabaseType.H2).open();
             Statement s = c.createStatement()) {
            for (String st : setup) {
                if (!st.endsWith(" default;") && !st.endsWith(" default cascade;")) {
                    s.execute(st);
                }
            }
            try (ResultSet rs = s.executeQuery(query)) {
                int n = 0;
                while (rs.next()) {
                    n++;
                }
                assertEquals(2, n, query);
            }
        }
    }

    private static String execute(String body) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            var r = Program.last(TestBackends.platform().withStorelessRuntime(MODEL), body,
                    com.legend.test.StorelessRuntime.RUNTIME, c);
            return String.valueOf(r.one());
        }
    }

    /** The DDL natives name a quoted table one way: the drop and the create name the same table. */
    @Test
    void theDdlNativesNameAQuotedTableOneWay() throws Exception {
        assertEquals("Drop table if exists Items;",
                execute("|meta::relational::functions::toDDL::dropTableStatement(q::DB, 'Items')"));
        assertEquals("Create Table Items(id INT NOT NULL,label VARCHAR(20) NULL, PRIMARY KEY(id));",
                execute("|meta::relational::functions::toDDL::createTableStatement(q::DB, 'Items')"));
    }

    /** The plan text finds a quoted table's definition by its name (it crashed: no table {@code "Items"}). */
    @Test
    void thePlanTextFindsAQuotedTable() throws Exception {
        String plan = execute("|meta::pure::executionPlan::executionPlan(|#>{q::DB.Items}#->select(~[id]), q::M,"
                + " ^meta::core::runtime::Runtime(connectionStores = ^meta::core::runtime::ConnectionStore("
                + "element = q::DB, connection = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                + "type = meta::relational::runtime::DatabaseType.H2))), [])"
                + "->meta::pure::executionPlan::toString::planToString([])");
        assertTrue(plan.contains("sql = select \"root\".id as \"id\" from Items as \"root\""), plan);
    }

    /** The column lineage finds the database that defines a quoted table (it refused: no database defines
     *  {@code "Items"}). */
    @Test
    void theLineageFindsTheDatabaseOfAQuotedTable() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        var plan = Compiler.query(ctx, "#>{q::DB.Sales.Orders}#->select(~[id, Amt])").lower("q::RT", false).plan();
        var entries = com.legend.lineage.ScanColumns.entries(plan);
        var rows = com.legend.lineage.ColumnLineageRows.rows("s", ctx, "q::M", entries).get("column_contexts");
        assertEquals(List.of("s", "0", "q::DB", "Sales", "Orders"), rows.get(0).subList(0, 5));
    }
}
