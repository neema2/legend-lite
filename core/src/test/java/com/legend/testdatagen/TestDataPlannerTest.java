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

    /** A view in the query's tree (B2d.3, upstream's orderPnlView shape: a filter, distinct, a column through two
     *  joins), as upstream's interpreted generator fetches it: its main table's rows by their identifiers, its own join
     *  web, then its own fetch -- the view's relation over those clauses, one more fetch text -- whose rows the query's
     *  joins from the view start from; the text writes the tables only. */
    private static final String VIEW_MODEL = """
            ###Relational
            Database t::DB
            (
              Table ORDERS (ID INTEGER PRIMARY KEY, ACCOUNTID INTEGER)
              Table PNL (ORDER_ID INTEGER PRIMARY KEY, PNL DOUBLE)
              Table ACCOUNT (ID INTEGER PRIMARY KEY, NAME VARCHAR(40))
              Join Pnl_Order(PNL.ORDER_ID = ORDERS.ID)
              Join Order_Account(ORDERS.ACCOUNTID = ACCOUNT.ID)
              Join PnlView_Order(PNLVIEW.ORDER_ID = ORDERS.ID)
              Filter NonNegative(PNL.PNL > 0)
              View PNLVIEW
              (
                ~filter NonNegative
                ~distinct
                ORDER_ID: PNL.ORDER_ID PRIMARY KEY,
                pnl: PNL.PNL,
                accountName: @Pnl_Order > @Order_Account | ACCOUNT.NAME
              )
            )
            ###Pure
            Class t::OrderPnl { pnl: Float[1]; accountName: String[0..1]; order: t::Order[0..1]; }
            Class t::Order { id: Integer[1]; pnl: t::OrderPnl[0..1]; }
            ###Mapping
            Mapping t::M
            (
              t::OrderPnl: Relational { ~mainTable [t::DB]PNLVIEW pnl: [t::DB]PNLVIEW.pnl,
                accountName: [t::DB]PNLVIEW.accountName, order: [t::DB]@PnlView_Order }
              t::Order: Relational { ~mainTable [t::DB]ORDERS id: [t::DB]ORDERS.ID, pnl: [t::DB]@PnlView_Order }
            )
            """;

    private static final String VIEW_CSV = "default\nORDERS\nID,ACCOUNTID\n1,10\n2,20\n3,10\n4,20\n-----\n"
            + "default\nPNL\nORDER_ID,PNL\n1,100.0\n2,200.0\n3,-5.0\n4,150.0\n-----\n"
            + "default\nACCOUNT\nID,NAME\n10,acme\n20,zeta\n";

    @ParameterizedTest
    @MethodSource("inMemory")
    void aViewIsFetchedAsUpstreamFetchesIt(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        ModelContext ctx = Compiler.compileModel(viewModel(type));
        List<TestDataGenerator.TableRowIds> pnl123 = List.of(ids("PNL", "ORDER_ID", 1L, 2L, 3L));
        String tables = "default\nPNL\nORDER_ID,PNL\n1,100.0\n2,200.0\n3,-5.0\n-----\n"
                + "default\nORDERS\nACCOUNTID,ID\n10,1\n10,3\n20,2\n-----\n"
                + "default\nACCOUNT\nID,NAME\n10,acme\n20,zeta\n-----\n";
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(VIEW_CSV, "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            // the view as the root: its main table by its identifiers (a row its filter drops included: the main
            // table's fetch is by identifier), its join web, its own fetch -- 4 fetch texts, the tables only written
            assertSame(c, ctx, VIEW_MODEL, dialect, "|t::OrderPnl.all()->project([o|$o.pnl], ['p'])", pnl123, 4,
                    tables);
            // the query's join from the view starts from the view's own rows: order 3's pnl is negative, so the view
            // drops it and the query's ORDERS fetch reads orders 1 and 2 (their rows already fetched by the web)
            assertSame(c, ctx, VIEW_MODEL, dialect,
                    "|t::OrderPnl.all()->project([o|$o.pnl, o|$o.order.id], ['p', 'o'])", pnl123, 5, tables);
            // a child view: its main table's given identifiers start its tree, whatever the parent's rows (TDG:429)
            assertSame(c, ctx, VIEW_MODEL, dialect, "|t::Order.all()->project([o|$o.id, o|$o.pnl.pnl], ['i', 'p'])",
                    List.of(ids("ORDERS", "ID", 4L), ids("PNL", "ORDER_ID", 1L)), 5,
                    "default\nORDERS\nACCOUNTID,ID\n10,1\n20,4\n-----\n"
                            + "default\nPNL\nORDER_ID,PNL\n1,100.0\n-----\n"
                            + "default\nACCOUNT\nID,NAME\n10,acme\n-----\n");
        }
    }

    /** The census ({@code getRelationalCSVDataFromQuery}, B2d.3) is a model fact: per table reached, its primary key,
     *  then the columns read of it (a value, both sides of a join), each sorted by name; the tables by schema and
     *  name. Upstream's order, no database. */
    @org.junit.jupiter.api.Test
    void theCensusIsAModelFact() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        List<TestDataCensus.Table> census = TestDataCensus.of(ctx,
                query("|t::Person.all()->project([p|$p.name, p|$p.bicycles.id], ['n', 'b'])"), "t::M");
        assertEquals(List.of(new TestDataCensus.Table("default", "BICYCLE", List.of("ID", "PERSONID")),
                new TestDataCensus.Table("default", "PERSON", List.of("ID", "NAME"))), census);
    }

    /** The reload's tables by schema (the B checkpoint audit's G2, 2026-11-11): a database's flat table list holds a
     *  named schema's tables too, by their bare name, so the reload read them twice -- two schemas' tables of one name,
     *  or a default table beside a schema's of its name, refused as "declared twice"; a schema's lone table read as a
     *  phantom default one. Upstream reloads each table where its schema declares it. */
    @ParameterizedTest
    @MethodSource("inMemory")
    void theReloadReadsEachTableInItsSchema(DatabaseType type) {
        String store = """
                ###Relational
                Database t::DB
                (
                  Table T (ID INTEGER PRIMARY KEY, N INTEGER)
                  Schema S1 ( Table T (ID INTEGER PRIMARY KEY, N INTEGER) )
                  Schema S2 ( Table T (ID INTEGER PRIMARY KEY, N INTEGER) )
                )
                Database t::Other ( Table T (ID INTEGER PRIMARY KEY, N INTEGER) )
                Database t::One ( Schema S ( Table T (ID INTEGER PRIMARY KEY, N INTEGER) ) )
                ###Pure
                Class t::X { id: Integer[1]; n: Integer[0..1]; }
                ###Mapping
                Mapping t::M ( t::X: Relational { ~mainTable [t::DB]S1.T id: [t::DB]S1.T.ID, n: [t::DB]S1.T.N } )
                """;
        ModelContext ctx = Compiler.compileModel(store);
        TestDataPlanner.Planned planned = TestDataPlanner.plan(ctx, query("|t::X.all()->project([x|$x.n], ['n'])"),
                "t::M", List.of(new TestDataGenerator.TableRowIds("S1", "T", List.of(
                        new TestDataGenerator.RowId(List.of("ID"), List.of(1L))))), null, false, PlannerViews.of(ctx));
        // two schemas' T and the default T: each its own table, none refused as declared twice
        assertEquals(java.util.Set.of("T", "S1.T", "S2.T"), TestDataPlanner.slice(planned, ctx, "t::DB").keySet());
        assertEquals(java.util.Set.of("t::DB|T", "t::DB|S1.T", "t::DB|S2.T"), TestDataPlanner.reads(ctx, "t::DB"));
        // a schema's lone T is no default T: the reload reads S.T alone, so another database's default T is not
        // taken for one it replaces
        assertEquals(java.util.Set.of("t::One|S.T"), TestDataPlanner.reads(ctx, "t::One"));
    }

    /** The seed (B2d.3): the starting table's keys carried through the query, distinct, sorted by key, and -- the user's
     *  ruling R2, register S198 -- all of them where there are 10 or fewer, else the 10 at rows 1 + floor(i*(n-1)/9):
     *  the first, the last and 8 evenly between; the seed string writes them as upstream's Pure code, a text key's
     *  quote, backslash and line feed escaped as toRepresentation escapes them. */
    private static final String SEED_MODEL = """
            ###Relational
            Database t::DB
            (
              Table KEYS (ID INTEGER PRIMARY KEY, NAME VARCHAR(40))
              Table CODES (CODE VARCHAR(20) PRIMARY KEY, N INTEGER)
            )
            ###Pure
            Class t::K { id: Integer[1]; name: String[0..1]; }
            Class t::C { code: String[1]; n: Integer[0..1]; }
            ###Mapping
            Mapping t::M
            (
              t::K: Relational { ~mainTable [t::DB]KEYS id: [t::DB]KEYS.ID, name: [t::DB]KEYS.NAME }
              t::C: Relational { ~mainTable [t::DB]CODES code: [t::DB]CODES.CODE, n: [t::DB]CODES.N }
            )
            """;

    @ParameterizedTest
    @MethodSource("inMemory")
    void theSeedSpreadsItsTenKeys(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        String model = SEED_MODEL + "\n###Connection\nRelationalDatabaseConnection t::C { store: t::DB; "
                + com.legend.testbackend.TestBackends.of(type).connectionBody() + " }\n###Runtime\n"
                + "Runtime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }\n";
        ModelContext ctx = Compiler.compileModel(model);
        StringBuilder csv = new StringBuilder("default\nKEYS\nID,NAME\n");
        for (int i = 25; i >= 1; i--) {
            csv.append(i).append(",k").append(i).append('\n');
        }
        csv.append("-----\ndefault\nCODES\nCODE,N\n\"a'b\",1\n\"c\\d\",2\n\"e\nf\",3\n");
        TestDataPlanner.Queries queries = PlannerViews.queries(ctx, "t::M", "t::RT");
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(csv.toString(), "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            // 25 keys: the 10 at rows 1, 3, 6, 9, 11, 14, 17, 19, 22, 25 of the sorted keys
            assertEquals(List.of(1L, 3L, 6L, 9L, 11L, 14L, 17L, 19L, 22L, 25L), keys(c, dialect, TestDataPlanner.seed(
                    ctx, query("|t::K.all()->project([k|$k.name], ['n'])"), "t::M", false, queries)));
            // the keys the query reaches, not the table's: 13 of them, over 10, spread from 13 to 25
            assertEquals(List.of(13L, 14L, 15L, 17L, 18L, 19L, 21L, 22L, 23L, 25L), keys(c, dialect,
                    TestDataPlanner.seed(ctx, query("|t::K.all()->filter(k|$k.id > 12)->project([k|$k.name], ['n'])"),
                            "t::M", false, queries)));
            // 10 or fewer: all of them, as the seed string writes them
            TestDataPlanner.Seed three = TestDataPlanner.seed(ctx,
                    query("|t::K.all()->filter(k|$k.id <= 3)->project([k|$k.name], ['n'])"), "t::M", false, queries);
            assertEquals("\nmeta::relational::testDataGeneration::createTableRowIdentifiers(t::DB, 'default', 'KEYS', [\n"
                    + "       meta::relational::testDataGeneration::createRowIdentifier(['ID'], [1]),\n"
                    + "       meta::relational::testDataGeneration::createRowIdentifier(['ID'], [2]),\n"
                    + "       meta::relational::testDataGeneration::createRowIdentifier(['ID'], [3])\n  ])\n",
                    text(c, dialect, TestDataPlanner.seedString(ctx, three)));
            // a text key: quoted, its quote, backslash and line feed escaped (Pure's toRepresentation)
            TestDataPlanner.Seed codes = TestDataPlanner.seed(ctx, query("|t::C.all()->project([x|$x.n], ['n'])"),
                    "t::M", false, queries);
            assertEquals("\nmeta::relational::testDataGeneration::createTableRowIdentifiers(t::DB, 'default', 'CODES', [\n"
                    + "       meta::relational::testDataGeneration::createRowIdentifier(['CODE'], ['a\\'b']),\n"
                    + "       meta::relational::testDataGeneration::createRowIdentifier(['CODE'], ['c\\\\d']),\n"
                    + "       meta::relational::testDataGeneration::createRowIdentifier(['CODE'], ['e\\nf'])\n  ])\n",
                    text(c, dialect, TestDataPlanner.seedString(ctx, codes)));
        }
    }

    /** The seed's keys, as the database answers its rows statement. */
    private static List<Long> keys(Connection c, SqlDialect dialect, TestDataPlanner.Seed seed) throws Exception {
        List<Long> out = new java.util.ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(dialect.render(seed.rows()))) {
            while (r.next()) {
                out.add(r.getLong(1));
            }
        }
        return out;
    }

    /** A one-cell statement's text. */
    private static String text(Connection c, SqlDialect dialect, com.legend.sql.SqlQuery q) throws Exception {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(dialect.render(q))) {
            assertTrue(r.next());
            return r.getString(1);
        }
    }

    /** No identifiers for a starting table -- the root, a view's main table: upstream's error, its example's keys not
     *  read (the user's ruling R3, 2026-11-11; register S300). */
    @ParameterizedTest
    @MethodSource("inMemory")
    void noStartingRowsIsUpstreamsError(DatabaseType type) {
        ModelContext ctx = Compiler.compileModel(viewModel(type));
        com.legend.error.AssertFailed root = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.AssertFailed.class, () -> TestDataPlanner.plan(ctx,
                        query("|t::Order.all()->project([o|$o.id], ['i'])"), "t::M", List.of(), null, false,
                        PlannerViews.of(ctx, "t::RT", Databases.dialect(type))));
        assertEquals("Row Identifers should be provided for the root table: t::DB.ORDERS [ID:INT]\nExample:\n"
                + "  meta::relational::testDataGeneration::createTableRowIdentifiers(t::DB, 'default', 'ORDERS', [\n\n"
                + "  ])\n", root.getMessage());
        com.legend.error.AssertFailed view = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.AssertFailed.class, () -> TestDataPlanner.plan(ctx,
                        query("|t::Order.all()->project([o|$o.pnl.pnl], ['p'])"), "t::M",
                        List.of(ids("ORDERS", "ID", 1L)), null, false, PlannerViews.of(ctx, "t::RT", Databases.dialect(type))));
        assertEquals("Row Identifers should be provided for the root table of the view (PNLVIEW): t::DB.PNL"
                + " [ORDER_ID:INT]\nExample:\n"
                + "  meta::relational::testDataGeneration::createTableRowIdentifiers(t::DB, 'default', 'PNL', [\n\n"
                + "  ])\n", view.getMessage());
    }

    /** {@link #VIEW_MODEL} with a runtime on {@code type}'s in-memory database: a view's fetch plans on it. */
    private static String viewModel(DatabaseType type) {
        return VIEW_MODEL + "\n###Connection\nRelationalDatabaseConnection t::C { store: t::DB; "
                + com.legend.testbackend.TestBackends.of(type).connectionBody() + " }\n###Runtime\n"
                + "Runtime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }\n";
    }

    private static TestDataGenerator.TableRowIds ids(String table, String column, Long... values) {
        List<TestDataGenerator.RowId> rows = new java.util.ArrayList<>();
        for (Long v : values) {
            rows.add(new TestDataGenerator.RowId(List.of(column), List.of(v)));
        }
        return new TestDataGenerator.TableRowIds("default", table, rows);
    }

    /** {@link #assertSame(Connection, ModelContext, SqlDialect, String, List, boolean, int, String)} over a model
     *  of its own, unhashed. */
    private static void assertSame(Connection c, ModelContext ctx, String model, SqlDialect dialect, String query,
            List<TestDataGenerator.TableRowIds> ids, int fetches, String expected) throws Exception {
        assertSame(c, ctx, dialect, query, ids, false, fetches, expected, PlannerViews.of(ctx, "t::RT", dialect));
    }

    /** The planned statement's text, run, equals {@code expected}; {@code fetches} fetch texts, each of which runs
     *  read-only; no table made. */
    private static void assertSame(Connection c, ModelContext ctx, SqlDialect dialect, String query,
            List<TestDataGenerator.TableRowIds> ids, boolean hash, int fetches, String expected) throws Exception {
        assertSame(c, ctx, dialect, query, ids, hash, fetches, expected, PlannerViews.of(ctx));
    }

    private static void assertSame(Connection c, ModelContext ctx, SqlDialect dialect, String query,
            List<TestDataGenerator.TableRowIds> ids, boolean hash, int fetches, String expected,
            TestDataPlanner.Views views) throws Exception {
        TestDataPlanner.Planned planned = TestDataPlanner.plan(ctx, query(query), "t::M", ids, null, hash, views);
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
