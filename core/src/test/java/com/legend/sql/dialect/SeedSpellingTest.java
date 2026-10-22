// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.setup.CsvSeed;
import com.legend.setup.RowLoad;
import com.legend.sql.NameQuoting;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A seed spells its table and column names as the SESSION's dialect references them
 * ({@code SqlDialect.storedName}, C3c): the seeded table and the query read one name. It used the union of
 * DuckDB's and H2's reserved words, whatever the session — Postgres, which folds a bare name to lowercase,
 * was not in it. The names stay their text in the load, with which the store declared quoted as data
 * ({@link NameQuoting}, the quoted columns), and the dialect spells each once, when it writes the insert. A dialect
 * test since the dialect program's Phase 6 (2026-10-16; until then {@code com.legend.setup}'s): each assertion is a
 * dialect's spelling.
 */
class SeedSpellingTest {

    private static final String[] COLS = {"ID", "order"};
    private static final List<String[]> ROWS = List.<String[]>of(new String[] {"1", "a"});

    /** The insert up to its rows: {@code INSERT INTO <table> (<columns>)}. */
    private static String into(SqlDialect dialect, RowLoad load) {
        String sql = dialect.render(load.values());
        return sql.substring(0, sql.indexOf(" VALUES"));
    }

    /** Postgres folds a store's bare names to lower case, bare where they may be (2026-10-12, Postgres.physicalName:
     *  the engine's unquoted convention; until then every name was quoted as declared). */
    @Test
    void postgresFoldsEveryStoredName() {
        RowLoad load = CsvSeed.rowLoad("S", "T", NameQuoting.BARE, COLS, Set.of(), ROWS);
        assertEquals("S", load.schema());
        assertEquals("T", load.table());
        assertEquals(List.of("ID", "order"), load.columns());
        assertEquals("INSERT INTO s.t (id, \"order\")", into(new com.legend.sql.dialect.Postgres(), load));
    }

    /** A name the store declared quoted ({@code Schema "Sales"}, {@code Table "Orders"}, a column {@code "Amt"}) keeps
     *  its case on Postgres, so the seed fills the table its DDL made and its queries read; the PR #39 audit
     *  (2026-10-14) found the seed folding a declared-quoted column ({@code amt}) its query read as {@code "Amt"}. */
    @Test
    void postgresKeepsTheCaseOfWhatTheStoreDeclaredQuoted() {
        RowLoad load = CsvSeed.rowLoad("Sales", "Orders", new NameQuoting(true, true), new String[] {"Amt", "ID"},
                Set.of("Amt"), ROWS);
        assertEquals("INSERT INTO \"Sales\".\"Orders\" (\"Amt\", id)", into(new com.legend.sql.dialect.Postgres(), load));
    }

    @Test
    void h2AndDuckDbQuoteOnlyWhatTheyReserve() {
        RowLoad load = CsvSeed.rowLoad(null, "T", NameQuoting.BARE, COLS, Set.of(), ROWS);
        for (SqlDialect d : List.of(new com.legend.sql.dialect.H2(), new com.legend.sql.dialect.DuckDb())) {
            assertEquals("INSERT INTO T (ID, \"order\")", into(d, load), d.getClass().getSimpleName());
        }
    }

    /** Every name is delimited once. The seed passed its names already spelled and the insert spelled them again: on
     *  BigQuery and Databricks, whose quote is a backtick, every name was delimited twice, which names another table
     *  and column (the PR #39 audit, 2026-10-14). */
    @Test
    void aBacktickDialectDelimitsEachNameOnce() {
        RowLoad load = CsvSeed.rowLoad("Sales", "order", new NameQuoting(true, false), new String[] {"ID", "Amt"},
                Set.of("Amt"), ROWS);
        assertEquals("INSERT INTO `Sales`.`order` (`ID`, `Amt`)", into(new com.legend.sql.dialect.Databricks(), load));
        assertEquals("INSERT INTO `Sales`.`order` (`ID`, `Amt`)", into(new com.legend.sql.dialect.BigQuery(), load));
    }

    private static final String MODEL = """
            ###Relational
            Database q::DB ( Table "Orders" (id INTEGER PRIMARY KEY, "Amt" INTEGER, name VARCHAR(20)) )
            ###Connection
            RelationalDatabaseConnection q::Conn { store: q::DB; type: TYPE; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime q::RT { mappings: []; connections: [ q::DB: [ c1: q::Conn ] ]; }
            """;

    /** On every database, a store's declared-quoted table and column are ONE name to the seed's DDL, its insert and
     *  the query that reads them: each spelled once, delimited by the dialect's own quote character, its case kept. */
    @Test
    void theSeedAndTheQueryNameOneTableAndOneColumn() {
        String csv = String.join("\n", "default", "Orders", "id,Amt,name", "1,10,a");
        for (DatabaseType type : Databases.relational()) {
            ModelContext ctx = Compiler.compileModel(MODEL.replace("TYPE", type.name()));
            SqlDialect d = Databases.dialect(type);
            String table = d.storedName("Orders", true);
            String amt = d.storedName("Amt", true);
            List<String> seed = CsvSeed.steps(csv, "q::DB", ctx, d).stream().map(s -> s.text(d)).toList();
            String create = seed.stream().filter(s -> s.toUpperCase(java.util.Locale.ROOT).startsWith("CREATE TABLE"))
                    .findFirst().orElseThrow();
            String insert = seed.stream().filter(s -> s.startsWith("INSERT")).findFirst().orElseThrow();
            String query = Compiler.query(ctx, "#>{q::DB.Orders}#->select(~[id, Amt])").plan("q::RT").sql();
            for (String sql : List.of(create, insert, query)) {
                assertTrue(sql.contains(table) && sql.contains(amt), type + ": " + table + " and " + amt + " in " + sql);
                assertFalse(sql.contains("``") || sql.contains("\\`") || sql.contains("\"\""),
                        type + ": a name delimited twice in " + sql);
            }
        }
    }
}
