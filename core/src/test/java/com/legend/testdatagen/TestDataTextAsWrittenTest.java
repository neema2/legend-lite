// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.setup.CsvSeed;
import com.legend.sql.dialect.SqlDialect;
import com.legend.database.Databases;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A TEST-DATA TEXT LOADED AS WRITTEN (the user's G7 ruling, 2026-11-12, "Def want windows files accepted"; register
 * S302): a connection's CSV test data loads each value exactly as written -- a padded cell keeps its spaces, the schema
 * and table lines are read verbatim, a schema or table the database does not declare refused with upstream's words
 * ({@code loadCsvDataToDbTable}) -- with one leniency: a CRLF line ending is a line break. On every database with an
 * in-process instance.
 */
class TestDataTextAsWrittenTest {

    private static final String MODEL = """
            ###Relational
            Database t::DB
            (
              Table K (ID INTEGER PRIMARY KEY, NAME VARCHAR(20))
              Table Q (ID INTEGER PRIMARY KEY, "Name" VARCHAR(20))
              Schema S ( Table T (ID INTEGER PRIMARY KEY) )
            )
            """;

    static List<DatabaseType> inMemory() throws java.sql.SQLException {
        return ReservedNamesGenerationTest.inMemory();
    }

    /** A text saved with CRLF line endings loads as its LF twin: each '\r' before a '\n' a line break, never part of
     *  a value or a name; a padded cell's spaces kept. */
    @ParameterizedTest
    @MethodSource("inMemory")
    void aCrlfTextLoadsAsWritten(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        ModelContext ctx = Compiler.compileModel(MODEL);
        // the first block's schema line is where upstream's reading of a CRLF text fails ("default\r")
        String crlf = "default\r\nK\r\nID,NAME\r\n1, ann \r\n2,bob\r\n-----\r\n";
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(crlf, "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            assertEquals(List.of("1| ann ", "2|bob"), rows(c, "SELECT ID, NAME FROM K ORDER BY ID"), type.name());
        }
    }

    /** A schema line read as written: a padded one is a schema the database does not declare, refused with upstream's
     *  words, as upstream's loader refuses it; a table the schema does not declare, likewise. */
    @ParameterizedTest
    @MethodSource("inMemory")
    void aSchemaOrTableTheDatabaseDoesNotDeclareIsRefusedAsUpstreamRefusesIt(DatabaseType type) {
        SqlDialect dialect = Databases.dialect(type);
        ModelContext ctx = Compiler.compileModel(MODEL);
        com.legend.error.AssertFailed schema = assertThrows(com.legend.error.AssertFailed.class,
                () -> CsvSeed.sqls(" default\nK\nID\n1\n", "t::DB", ctx, dialect));
        assertEquals("There should be exactly 1 schema with name  default found in the provided runtime database(s)"
                + " [t::DB] at the given connection.", schema.getMessage());
        com.legend.error.AssertFailed table = assertThrows(com.legend.error.AssertFailed.class,
                () -> CsvSeed.sqls("default\nK \nID\n1\n", "t::DB", ctx, dialect));
        assertEquals("Table \"K \" not found in Schema \"default\" in Database(s) t::DB", table.getMessage());
    }

    /** A header names its column as upstream's loader matches it (the orchestrator's go, 2026-11-12: exactness): trimmed,
     *  its matching quotes stripped, any case -- a quoted column's included (until then lite matched a quoted column by
     *  its exact name alone); a header no column matches refused with upstream's words. */
    @ParameterizedTest
    @MethodSource("inMemory")
    void aHeaderNamesItsColumnAsUpstreamMatchesIt(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        ModelContext ctx = Compiler.compileModel(MODEL);
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls("default\nQ\n id , \"NAME\"\n1,ann\n", "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            assertEquals(List.of("1|ann"), rows(c, "SELECT ID, \"Name\" FROM Q"), type.name());
        }
        com.legend.error.AssertFailed nope = assertThrows(com.legend.error.AssertFailed.class,
                () -> CsvSeed.sqls("default\nQ\nID,NOPE\n1,x\n", "t::DB", ctx, dialect));
        assertEquals("Column \"NOPE\" not found in Table \"Q\" in Database(s) t::DB", nope.getMessage());
    }

    /** A block of the default schema names a table of the default schema (the B2d.3 audit's L1, 2026-11-12): the
     *  database's only T is schema S's, so {@code default\nT} names none -- upstream's schema('default')->table('T')
     *  finds none -- refused with upstream's words; until then a bare lookup found S.T and created T in the default
     *  schema. */
    @ParameterizedTest
    @MethodSource("inMemory")
    void aDefaultSchemaBlockNamesADefaultSchemaTable(DatabaseType type) {
        SqlDialect dialect = Databases.dialect(type);
        ModelContext ctx = Compiler.compileModel(MODEL);
        com.legend.error.AssertFailed e = assertThrows(com.legend.error.AssertFailed.class,
                () -> CsvSeed.sqls("default\nT\nID\n1\n", "t::DB", ctx, dialect));
        assertEquals("Table \"T\" not found in Schema \"default\" in Database(s) t::DB", e.getMessage());
    }

    /** Two databases one connection serves (the B2d.3 audit's M3, 2026-11-12): the connection's test data is loaded
     *  ONCE, its tables looked up in every database it serves, as upstream's processRuntimeTestConnections loads each
     *  distinct connection (storeContract.pure ~119-141: setUpDataSQLs(csv, [DB1, DB2])) -- both databases' tables
     *  loaded, the connection's SQL planned once; a table neither declares refused with upstream's words, naming both. Until
     *  then lite loaded it per store binding and refused DB2's table against DB1 alone. */
    @ParameterizedTest
    @MethodSource("inMemory")
    void aConnectionServingTwoDatabasesLoadsItsDataOnceAgainstBoth(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        String model = """
                ###Relational
                Database s::DB1 ( Table A (ID INTEGER PRIMARY KEY, NAME VARCHAR(10)) )
                Database s::DB2 ( Table B (ID INTEGER PRIMARY KEY, N INTEGER) )
                ###Connection
                RelationalDatabaseConnection s::C
                {
                    store: s::DB1;
                    type: H2;
                    specification: LocalH2 { testDataSetupSqls: ['CREATE TABLE SEEDLOG (ID INTEGER)',
                        'INSERT INTO SEEDLOG VALUES (1)']; testDataSetupCSV: '%s'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB1: [ a: s::C ], s::DB2: [ b: s::C ] ]; }
                """;
        ModelContext ctx = Compiler.compileModel(model.formatted(
                "default\\nA\\nID,NAME\\n1,ann\\n-----\\ndefault\\nB\\nID,N\\n7,70\\n"));
        List<CsvSeed.Step> steps = CsvSeed.declaredSteps("s::RT", ctx, dialect);
        // the connection's two statements once, not once per store it serves (they are H2's text: not run here)
        assertEquals(2, steps.stream().filter(st -> st instanceof CsvSeed.Step.Sql).count(), type.name());
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (CsvSeed.Step step : steps) {
                    if (!(step instanceof CsvSeed.Step.Sql)) {
                        for (String sql : step.texts(dialect)) {
                            s.execute(sql);
                        }
                    }
                }
            }
            assertEquals(List.of("1|ann"), rows(c, "SELECT ID, NAME FROM A"), type.name());
            assertEquals(List.of("7|70"), rows(c, "SELECT ID, N FROM B"), type.name());
        }
        ModelContext undeclared = Compiler.compileModel(model.formatted(
                "default\\nA\\nID\\n1\\n-----\\ndefault\\nZ\\nID\\n1\\n"));
        com.legend.error.AssertFailed e = assertThrows(com.legend.error.AssertFailed.class,
                () -> CsvSeed.declaredSteps("s::RT", undeclared, dialect));
        assertEquals("Table \"Z\" not found in Schema \"default\" in Database(s) s::DB1, s::DB2", e.getMessage());
    }

    /** {@code sql}'s rows, each its cells joined by {@code |}. */
    private static List<String> rows(Connection c, String sql) throws Exception {
        List<String> out = new ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            int n = r.getMetaData().getColumnCount();
            while (r.next()) {
                List<String> cells = new ArrayList<>(n);
                for (int i = 1; i <= n; i++) {
                    cells.add(String.valueOf(r.getObject(i)));
                }
                out.add(String.join("|", cells));
            }
        }
        return out;
    }
}
