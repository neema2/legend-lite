// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testdatagen;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.setup.CsvSeed;
import com.legend.sql.dialect.SqlDialect;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE GENERATOR AS ONE PLANNED STATEMENT (B2d, the user's approved design of 2026-11-10): on every database with an
 * in-memory instance, the planner's one read-only statement answers the test-data text -- a two-table join, a self-join
 * (one clause per navigation step), a text cell hashed (upstream's hashString: the first 5 hex characters of its
 * SHA-256 tiled to its length) -- and one fetch text per fetch; it creates nothing on the database. Each text is pinned
 * (B2d.2, 2026-11-11: until then the interleaved run that fetched into temporary tables, the tests' own generator run,
 * was the reference, and is deleted). By the user's ruling 3 (2026-11-10, "faithful to the real data, line breaks
 * included"; register S191) a text cell holding a comma, a double quote or a line break is one quoted cell, RFC 4180, a
 * single quote kept, where upstream's Pure writer replaces {@code '} with a space, {@code ,} with {@code ;} and a line
 * break with a space.
 */
class TestDataPlannerTest {

    private static final String MODEL = """
            ###Relational
            Database t::DB
            (
              Table PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(40), MANAGERID INTEGER)
              Table BICYCLE (ID INTEGER PRIMARY KEY, PERSONID INTEGER)
              Join Person_Bicycle(PERSON.ID = BICYCLE.PERSONID)
              Join Person_Manager(PERSON.MANAGERID = {target}.ID)
            )
            ###Pure
            Class t::Person { name: String[0..1]; bicycles: t::Bicycle[*]; manager: t::Person[0..1]; }
            Class t::Bicycle { id: Integer[1]; }
            ###Mapping
            Mapping t::M
            (
              t::Person: Relational { ~mainTable [t::DB]PERSON name: [t::DB]PERSON.NAME,
                bicycles: [t::DB]@Person_Bicycle, manager: [t::DB]@Person_Manager }
              t::Bicycle: Relational { ~mainTable [t::DB]BICYCLE id: [t::DB]BICYCLE.ID }
            )
            """;

    /** The rows: person 5's name holds a comma, a double quote and a line break, one quoted cell the test-data reader
     *  reads whole (B2d.2's audit, H2: the two literal writes this case seeded before the reader was quote-aware are
     *  its rows now); person 6's name a NULL. */
    private static final String CSV = "default\nPERSON\nID,NAME,MANAGERID\n1,boss,\n2,ann,1\n3,\"o'neil, jr\",1\n"
            + "4,dan,2\n5,\"o'neil, \"\"jr\"\"\nthe 2nd\",1\n6,---null---,1\n-----\ndefault\nBICYCLE\nID,PERSONID\n10,2\n"
            + "11,3\n12,3\n13,4\n14,5\n";

    static List<DatabaseType> inMemory() throws java.sql.SQLException {
        return ReservedNamesGenerationTest.inMemory();
    }

    @ParameterizedTest
    @MethodSource("inMemory")
    void thePlannedStatementAnswersTheTestData(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        ModelContext ctx = Compiler.compileModel(MODEL);
        List<TestDataGenerator.TableRowIds> twoAndThree = List.of(new TestDataGenerator.TableRowIds("default",
                "PERSON", List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(2L)),
                        new TestDataGenerator.RowId(List.of("ID"), List.of(3L)))));
        List<TestDataGenerator.TableRowIds> four = List.of(new TestDataGenerator.TableRowIds("default", "PERSON",
                List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(4L)))));
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(CSV, "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            String join = "|t::Person.all()->project([p|$p.name, p|$p.bicycles.id], ['n', 'b'])";
            String bicycles = "default\nBICYCLE\nID,PERSONID\n10,2\n11,3\n12,3\n-----\n";
            // a two-table join, its text written faithfully, then hashed
            assertSame(c, ctx, dialect, join, twoAndThree, false, 2,
                    "default\nPERSON\nID,NAME\n2,ann\n3,\"o'neil, jr\"\n-----\n" + bicycles);
            assertSame(c, ctx, dialect, join, twoAndThree, true, 2,
                    "default\nPERSON\nID,NAME\n2,915\n3,d41eed41ee\n-----\n" + bicycles);
            // a self-join two steps deep: one clause per navigation step
            assertSame(c, ctx, dialect, "|t::Person.all()->project([p|$p.manager.manager.name], ['m'])", four,
                    false, 3, "default\nPERSON\nID,MANAGERID,NAME\n1,---null---,boss\n2,1,ann\n4,2,dan\n-----\n");
            // ruling 3 whole: a comma, a double quote and a line break in one cell, quoted, its quotes doubled; a
            // single quote kept; a NULL ---null---
            List<TestDataGenerator.TableRowIds> fiveAndSix = List.of(new TestDataGenerator.TableRowIds("default",
                    "PERSON", List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(5L)),
                            new TestDataGenerator.RowId(List.of("ID"), List.of(6L)))));
            assertSame(c, ctx, dialect, join, fiveAndSix, false, 2,
                    "default\nPERSON\nID,NAME\n5,\"o'neil, \"\"jr\"\"\nthe 2nd\"\n6,---null---\n-----\n"
                            + "default\nBICYCLE\nID,PERSONID\n14,5\n-----\n");
        }
    }

    /** The planned statement's text, run, equals {@code expected}; {@code fetches} fetch texts, each of which runs
     *  read-only; no table made. */
    private static void assertSame(Connection c, ModelContext ctx, SqlDialect dialect, String query,
            List<TestDataGenerator.TableRowIds> ids, boolean hash, int fetches, String expected) throws Exception {
        TestDataPlanner.Planned planned = TestDataPlanner.plan(ctx, query(query), "t::M", ids, null, hash);
        String sql = dialect.render(planned.csv());
        assertFalse(sql.toUpperCase(java.util.Locale.ROOT).contains("CREATE"), sql);
        String text;
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            assertTrue(r.next(), sql);
            text = r.getString(1);
        }
        assertEquals(expected, text, dialect.getClass().getSimpleName() + " " + query + (hash ? " hashed" : "")
                + ":\n" + sql);
        assertEquals(fetches, planned.fetches().size(), query + ": one text per fetch");
        // each fetch's own text runs, read-only
        for (com.legend.sql.SqlQuery fetch : planned.fetches()) {
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(dialect.render(fetch))) {
                while (r.next()) {
                    r.getObject(1);
                }
            }
        }
    }

    private static LambdaFunction query(String text) {
        return (LambdaFunction) com.legend.compiler.NameResolver.resolveQuery(Compiler.parseQuery(text));
    }
}
