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
            // an identifier list holding none is no filter: the table's rows by key, at most 20 (upstream's
            // orFilters([]); B2d.3e -- until then a planning error, "no row identifiers")
            assertSame(c, ctx, dialect, join, List.of(new TestDataGenerator.TableRowIds("default", "PERSON",
                    List.of())), false, 2, "default\nPERSON\nID,NAME\n1,boss\n2,ann\n3,\"o'neil, jr\"\n4,dan\n"
                            + "5,\"o'neil, \"\"jr\"\"\nthe 2nd\"\n6,---null---\n-----\n"
                            + "default\nBICYCLE\nID,PERSONID\n10,2\n11,3\n12,3\n13,4\n14,5\n-----\n");
            // a child joining no rows: its block's blank line, as upstream's joinStrings('', '\n', '\n') writes no row
            // (the B2d.3 audit's M2, 2026-11-12: until then the line was dropped)
            assertSame(c, ctx, dialect, join, List.of(new TestDataGenerator.TableRowIds("default", "PERSON",
                    List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(1L))))), false, 2,
                    "default\nPERSON\nID,NAME\n1,boss\n-----\ndefault\nBICYCLE\nID,PERSONID\n\n-----\n");
        }
    }

    /** Two identifier entries for one table: upstream's assert, by its words (the B2d.3 audit's L2, 2026-11-12:
     *  getRowIdentifiersForTable, TDG:487 -- until then the last entry was kept, silently). */
    @org.junit.jupiter.api.Test
    void twoEntriesForOneTableAreUpstreamsAssert() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        com.legend.error.AssertFailed e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.AssertFailed.class, () -> TestDataPlanner.plan(ctx,
                        query("|t::Person.all()->project([p|$p.name], ['n'])"), "t::M",
                        List.of(ids("PERSON", "ID", 2L), ids("PERSON", "ID", 3L)), null, false, PlannerViews.of(ctx)));
        assertEquals("Multiple table row identifiers passed for the table: PERSON", e.getMessage());
    }

    /** The server routes' text as their writer writes it (the B2d.3 audit's M1 and M2, 2026-11-12): the header each
     *  column's declared name -- its case kept, in double quotes where it holds a space -- the columns sorted by their
     *  model names case-sensitively, a quoted name with its quotes (upstream's plan form, TDG:885, and
     *  getQuotedLabelIfContainSpace, TestDataGeneration.java:306: "last name", AGE, ID, firstName), each line's cells
     *  in that order; the function's text keeps its H2-folded header; the default seed over an empty table a blank
     *  line (Collectors.joining("\n", "", "\n") of no row). */
    @ParameterizedTest
    @MethodSource("inMemory")
    void theRoutesTextIsItsWriters(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        String model = """
                ###Relational
                Database t::DB
                (
                  Table MIXED (ID INTEGER PRIMARY KEY, firstName VARCHAR(20), AGE INTEGER, "last name" VARCHAR(20))
                  Table NONE (ID INTEGER PRIMARY KEY, N INTEGER)
                )
                ###Pure
                Class t::Mixed { firstName: String[0..1]; age: Integer[0..1]; lastName: String[0..1]; }
                Class t::None { n: Integer[0..1]; }
                ###Mapping
                Mapping t::M
                (
                  t::Mixed: Relational { ~mainTable [t::DB]MIXED firstName: [t::DB]MIXED.firstName,
                    age: [t::DB]MIXED.AGE, lastName: [t::DB]MIXED."last name" }
                  t::None: Relational { ~mainTable [t::DB]NONE n: [t::DB]NONE.N }
                )
                ###Connection
                RelationalDatabaseConnection t::C { store: t::DB; %s }
                ###Runtime
                Runtime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }
                """.formatted(com.legend.testbackend.TestBackends.of(type).connectionBody());
        ModelContext ctx = Compiler.compileModel(model);
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls("default\nMIXED\nID,firstName,AGE,\"last name\"\n1,ann,30,lee\n"
                        + "2,bob,40,kim\n-----\ndefault\nNONE\nID,N\n", "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            LambdaFunction mixed = query("|t::Mixed.all()->project([m|$m.firstName, m|$m.age, m|$m.lastName],"
                    + " ['f', 'a', 'l'])");
            List<TestDataGenerator.TableRowIds> one = List.of(ids("MIXED", "ID", 1L, 2L));
            assertEquals("default\nMIXED\n\"last name\",AGE,ID,firstName\nkim,40,2,bob\nlee,30,1,ann\n-----\n",
                    text(c, dialect, TestDataPlanner.route(ctx, mixed, "t::M", one, null, false).csv()), type.name());
            // the function's text: its header as before (upper-cased plain names, sorted so)
            assertEquals("default\nMIXED\nAGE,FIRSTNAME,ID,last name\n30,ann,1,lee\n40,bob,2,kim\n-----\n",
                    text(c, dialect, TestDataPlanner.plan(ctx, mixed, "t::M", one, null, false, PlannerViews.of(ctx))
                            .csv()), type.name());
            // the default seed over an empty table: no key, so every row -- none -- and the block's blank line
            LambdaFunction none = query("|t::None.all()->project([x|$x.n], ['n'])");
            TestDataPlanner.Seed seed = TestDataPlanner.seed(ctx, none, "t::M", true,
                    PlannerViews.queries(ctx, "t::M", "t::RT"));
            assertEquals("default\nNONE\nID,N\n\n-----\n", text(c, dialect,
                    TestDataPlanner.route(ctx, none, "t::M", List.of(), seed, false).csv()), type.name());
        }
    }

    /** The seed's text keys in code-point order on the databases whose texts compare so by default (the B2d.3
     *  audit's M4, 2026-11-12, measured: DuckDB, H2, SQLite) -- the keys, the seed string's lines and the seed
     *  route's identifiers {@code B, Z, a, b}, never a case-blind {@code a, B, b, Z} (a database whose binary
     *  collation is not measured refuses it by name: {@code sql.dialect.BinaryCollateTest}; Postgres's
     *  {@code COLLATE "C"} is //core:postgres_arm_test's). */
    @ParameterizedTest
    @MethodSource("inMemory")
    void aTextKeyIsOrderedByItsCodePoints(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        String model = SEED_MODEL + "\n###Connection\nRelationalDatabaseConnection t::C { store: t::DB; "
                + com.legend.testbackend.TestBackends.of(type).connectionBody() + " }\n###Runtime\n"
                + "Runtime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }\n";
        ModelContext ctx = Compiler.compileModel(model);
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls("default\nCODES\nCODE,N\nb,1\nB,2\na,3\nZ,4\n", "t::DB", ctx,
                        dialect)) {
                    s.execute(sql);
                }
            }
            TestDataPlanner.Seed seed = TestDataPlanner.seed(ctx, query("|t::C.all()->project([x|$x.n], ['n'])"),
                    "t::M", false, PlannerViews.queries(ctx, "t::M", "t::RT"));
            List<String> keys = new java.util.ArrayList<>();
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(dialect.render(seed.rows()))) {
                while (r.next()) {
                    keys.add(r.getString(1));
                }
            }
            assertEquals(List.of("B", "Z", "a", "b"), keys, type.name());
            String line = "       meta::relational::testDataGeneration::createRowIdentifier(['CODE'], ['%s'])";
            assertEquals("\nmeta::relational::testDataGeneration::createTableRowIdentifiers(t::DB, 'default', 'CODES', [\n"
                    + String.join(",\n", line.formatted("B"), line.formatted("Z"), line.formatted("a"),
                            line.formatted("b")) + "\n  ])\n", text(c, dialect, TestDataPlanner.seedString(ctx, seed)),
                    type.name());
            assertTrue(text(c, dialect, TestDataPlanner.seedJson(ctx, seed)).endsWith("\"rowIdentifiers\":["
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"B\"}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"Z\"}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"a\"}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"b\"}]}]}"), type.name());
        }
    }

    /** The seed over the query's shapes (the design's §4.1; the B2d.3 audit's L6): a grouped query, its keys carried
     *  into its GROUP BY (so regrouped by them, as upstream's recursiveAddColumnsToRoot); a query whose root reads a
     *  derived table, its keys carried up through it; a table with no primary key, its non-null columns the keys and
     *  the seed string's names none (upstream's $mainTable.primaryKey, TDG:229; the audit's L4); a table with neither,
     *  upstream's words; a union, upstream's words; a view root, upstream's words for the seed string (the audit's
     *  L9: the root node's type, RelationTree). */
    @ParameterizedTest
    @MethodSource("inMemory")
    void theSeedOverTheQuerysShapes(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        String model = SEED_MODEL.replace("  Table CODES", """
                  Table NOKEY (A INTEGER NOT NULL, B VARCHAR(10) NOT NULL, C INTEGER)
                  Table NEITHER (A INTEGER, B VARCHAR(10))
                  Table CODES""").replace("###Mapping", """
                Class t::NoKey { a: Integer[1]; c: Integer[0..1]; }
                Class t::Neither { a: Integer[0..1]; }
                ###Mapping""").replace("  t::C: Relational", """
                  t::NoKey: Relational { ~mainTable [t::DB]NOKEY a: [t::DB]NOKEY.A, c: [t::DB]NOKEY.C }
                  t::Neither: Relational { ~mainTable [t::DB]NEITHER a: [t::DB]NEITHER.A }
                  t::C: Relational""")
                + "\n###Connection\nRelationalDatabaseConnection t::C { store: t::DB; "
                + com.legend.testbackend.TestBackends.of(type).connectionBody() + " }\n###Runtime\n"
                + "Runtime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }\n";
        ModelContext ctx = Compiler.compileModel(model);
        TestDataPlanner.Queries queries = PlannerViews.queries(ctx, "t::M", "t::RT");
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls("default\nKEYS\nID,NAME\n1,x\n2,x\n3,y\n4,z\n-----\n"
                        + "default\nNOKEY\nA,B,C\n1,p,10\n1,q,20\n2,p,30\n-----\ndefault\nNEITHER\nA,B\n1,p\n",
                        "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            // a grouped query: three groups of names, but the keys carried regroup it -- every key the query reaches
            LambdaFunction grouped = query(
                    "|t::K.all()->groupBy([k|$k.name], [agg(k|$k.id, y|$y->count())], ['n', 'c'])");
            assertFalse(((com.legend.sql.SqlSelect) queries.lowered(grouped)).groupBy().isEmpty(), "a grouped root");
            assertEquals(List.of(1L, 2L, 3L, 4L), keys(c, dialect, TestDataPlanner.seed(ctx, grouped, "t::M", false,
                    queries)), type.name());
            // a root reading a derived table (a column extended over a limited projection): the keys carried up
            // through it
            LambdaFunction derived = query("|t::K.all()->filter(k|$k.name == 'x')->project([k|$k.name], ['n'])"
                    + "->limit(10)->extend(~u: r|$r.n->toOne()->toUpper())");
            assertTrue(((com.legend.sql.SqlSelect) queries.lowered(derived)).from()
                    instanceof com.legend.sql.SqlSource.Subselect, "a root reading a derived table");
            assertEquals(List.of(1L, 2L), keys(c, dialect, TestDataPlanner.seed(ctx, derived, "t::M", false,
                    queries)), type.name());
            // no primary key: its non-null columns A and B are the keys; the seed string names none
            TestDataPlanner.Seed noKey = TestDataPlanner.seed(ctx, query("|t::NoKey.all()->project([x|$x.c], ['c'])"),
                    "t::M", false, queries);
            assertEquals(List.of("A", "B"), noKey.keys());
            String line = "       meta::relational::testDataGeneration::createRowIdentifier([], [%s])";
            assertEquals("\nmeta::relational::testDataGeneration::createTableRowIdentifiers(t::DB, 'default', 'NOKEY', [\n"
                    + String.join(",\n", line.formatted("1,'p'"), line.formatted("1,'q'"), line.formatted("2,'p'"))
                    + "\n  ])\n", text(c, dialect, TestDataPlanner.seedString(ctx, noKey)), type.name());
            // neither: upstream's words (TDG:272)
            assertEquals("Table : NEITHER must have atleast 1 primary key or non-null column to utilise this feature",
                    org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.AssertFailed.class,
                            () -> TestDataPlanner.seed(ctx, query("|t::Neither.all()->project([x|$x.a], ['a'])"),
                                    "t::M", false, queries)).getMessage());
            // a union: two roots, upstream's words -- the seed string's (TDG:221) and the seed route's (TDG:805)
            LambdaFunction union = query("|t::K.all()->project([k|$k.id], ['i'])->concatenate("
                    + "t::C.all()->project([x|$x.n], ['i']))");
            assertEquals("Seed data generation currently supports queries with a single root (got: 2)",
                    org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.AssertFailed.class,
                            () -> TestDataPlanner.seed(ctx, union, "t::M", false, queries)).getMessage());
            assertEquals("Seed data generation currently supports queries with a single root table/view (unions are"
                    + " not supported)", org.junit.jupiter.api.Assertions.assertThrows(
                            com.legend.error.AssertFailed.class,
                            () -> TestDataPlanner.seed(ctx, union, "t::M", true, queries)).getMessage());
        }
        // a view root: the seed string's words, upstream's (the audit's L9)
        ModelContext views = Compiler.compileModel(viewModel(type));
        assertEquals("Seed data generation currently supports queries with a root table (unions/views are not"
                + " supported, got: RelationTree)", org.junit.jupiter.api.Assertions.assertThrows(
                        com.legend.error.AssertFailed.class, () -> TestDataPlanner.seed(views,
                                query("|t::OrderPnl.all()->project([o|$o.pnl], ['p'])"), "t::M", false,
                                PlannerViews.queries(views, "t::M", "t::RT"))).getMessage());
    }

    /** An expected text's default-schema table looked up in the default schema (the B2d.3 audit's L1, 2026-11-12):
     *  {@code default\nT} names no table of a database whose only T is schema S's -- upstream's schema('default')
     *  ->table('T') finds none -- so the comparison fails with upstream's words; until then a bare lookup found S.T. */
    @org.junit.jupiter.api.Test
    void anExpectedDefaultTableIsLookedUpInTheDefaultSchema() {
        ModelContext ctx = Compiler.compileModel("""
                ###Relational
                Database t::DB ( Table T (ID INTEGER PRIMARY KEY) )
                Database t::One ( Schema S ( Table T (ID INTEGER PRIMARY KEY) ) )
                ###Pure
                Class t::X { id: Integer[1]; }
                ###Mapping
                Mapping t::M ( t::X: Relational { ~mainTable [t::DB]T id: [t::DB]T.ID } )
                """);
        TestDataPlanner.Planned planned = TestDataPlanner.plan(ctx, query("|t::X.all()->project([x|$x.id], ['i'])"),
                "t::M", List.of(ids("T", "ID", 1L)), null, false, PlannerViews.of(ctx));
        TestDataPlanner.Comparison c = TestDataPlanner.same(planned, List.of(new TestDataPlanner.Expected("default", "T",
                List.of("ID"), List.of(List.of("1")))), ctx, "t::One", "");
        assertEquals("Table \"T\" not found in Schema \"default\" in Database(s) t::One", c.fails());
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
              Table NOTE (ORDER_ID INTEGER PRIMARY KEY, TXT VARCHAR(10))
              Join Pnl_Order(PNL.ORDER_ID = ORDERS.ID)
              Join Order_Account(ORDERS.ACCOUNTID = ACCOUNT.ID)
              Join PnlView_Order(PNLVIEW.ORDER_ID = ORDERS.ID)
              Join PnlView_Note(PNLVIEW.ORDER_ID = NOTE.ORDER_ID)
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
            Class t::OrderPnl { pnl: Float[1]; accountName: String[0..1]; order: t::Order[0..1];
              note: t::Note[0..1]; }
            Class t::Order { id: Integer[1]; pnl: t::OrderPnl[0..1]; }
            Class t::Note { txt: String[0..1]; }
            ###Mapping
            Mapping t::M
            (
              t::OrderPnl: Relational { ~mainTable [t::DB]PNLVIEW pnl: [t::DB]PNLVIEW.pnl,
                accountName: [t::DB]PNLVIEW.accountName, order: [t::DB]@PnlView_Order, note: [t::DB]@PnlView_Note }
              t::Order: Relational { ~mainTable [t::DB]ORDERS id: [t::DB]ORDERS.ID, pnl: [t::DB]@PnlView_Order }
              t::Note: Relational { ~mainTable [t::DB]NOTE txt: [t::DB]NOTE.TXT }
            )
            """;

    private static final String VIEW_CSV = "default\nORDERS\nID,ACCOUNTID\n1,10\n2,20\n3,10\n4,20\n-----\n"
            + "default\nPNL\nORDER_ID,PNL\n1,100.0\n2,200.0\n3,-5.0\n4,150.0\n-----\n"
            + "default\nACCOUNT\nID,NAME\n10,acme\n20,zeta\n-----\n"
            + "default\nNOTE\nORDER_ID,TXT\n1,n1\n2,n2\n3,n3\n";

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
            // drops it and the query's NOTE fetch -- a table the view's web does not fetch -- reads the notes of
            // orders 1 and 2 alone; a join from the view's main table's rows would read order 3's too (the B2d.3
            // audit's L7, 2026-11-12: the ORDERS join this case read until then gave the root case's text either way)
            assertSame(c, ctx, VIEW_MODEL, dialect,
                    "|t::OrderPnl.all()->project([o|$o.pnl, o|$o.note.txt], ['p', 'n'])", pnl123, 5, tables
                            + "default\nNOTE\nORDER_ID,TXT\n1,n1\n2,n2\n-----\n");
            // a child view: its main table's given identifiers start its tree, whatever the parent's rows (TDG:429)
            assertSame(c, ctx, VIEW_MODEL, dialect, "|t::Order.all()->project([o|$o.id, o|$o.pnl.pnl], ['i', 'p'])",
                    List.of(ids("ORDERS", "ID", 4L), ids("PNL", "ORDER_ID", 1L)), 5,
                    "default\nORDERS\nACCOUNTID,ID\n10,1\n20,4\n-----\n"
                            + "default\nPNL\nORDER_ID,PNL\n1,100.0\n-----\n"
                            + "default\nACCOUNT\nID,NAME\n10,acme\n-----\n");
        }
    }

    /** A view over a table whose columns are declared in lower case (the B checkpoint's G4, 2026-11-12): the view's
     *  own fetch reads its table from the generator's clause under the declared names, as the reload's slice labels
     *  them, so its re-stamped reads meet their labels on every database -- under the fetch's upper-cased labels H2
     *  answers "Column not found". */
    @ParameterizedTest
    @MethodSource("inMemory")
    void aViewOverLowerCaseColumnsReadsItsClauseByTheirNames(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        String model = """
                ###Relational
                Database t::DB
                (
                  Table PNL (order_id INTEGER PRIMARY KEY, pnl DOUBLE)
                  Filter NonNegative(PNL.pnl > 0)
                  View PNLVIEW ( ~filter NonNegative order_id: PNL.order_id PRIMARY KEY, pnl: PNL.pnl )
                )
                ###Pure
                Class t::OrderPnl { pnl: Float[1]; }
                ###Mapping
                Mapping t::M ( t::OrderPnl: Relational { ~mainTable [t::DB]PNLVIEW pnl: [t::DB]PNLVIEW.pnl } )
                ###Connection
                RelationalDatabaseConnection t::C { store: t::DB; %s }
                ###Runtime
                Runtime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }
                """.formatted(com.legend.testbackend.TestBackends.of(type).connectionBody());
        ModelContext ctx = Compiler.compileModel(model);
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls("default\nPNL\norder_id,pnl\n1,100.0\n2,-5.0\n", "t::DB", ctx,
                        dialect)) {
                    s.execute(sql);
                }
            }
            assertSame(c, ctx, model, dialect, "|t::OrderPnl.all()->project([o|$o.pnl], ['p'])",
                    List.of(ids("PNL", "order_id", 1L, 2L)), 2, "default\nPNL\nORDER_ID,PNL\n1,100.0\n2,-5.0\n-----\n");
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
                        new TestDataGenerator.RowId(List.of("ID"), List.of(1L))))), null, false, PlannerViews.of(ctx),
                TestDataPlanner.Printing.CLEAN);
        // two schemas' T and the default T: each its own table, none refused as declared twice
        assertEquals(java.util.Set.of("T", "S1.T", "S2.T"), TestDataPlanner.slice(planned, ctx, "t::DB").keySet());
        assertEquals(java.util.Set.of("t::DB|T", "t::DB|S1.T", "t::DB|S2.T"), TestDataPlanner.reads(ctx, "t::DB"));
        // a schema's lone T is no default T: the reload reads S.T alone, so another database's default T is not
        // taken for one it replaces
        assertEquals(java.util.Set.of("t::One|S.T"), TestDataPlanner.reads(ctx, "t::One"));
    }

    /** The reload's rows as upstream's table holds them (the B checkpoint audit's G4, 2026-11-11; built 2026-11-12): the slice's rows are
     *  its fetches' distinct rows, each empty or ---null--- text cell then NULL -- upstream inserts each row, so a
     *  keyless table's ('', 1) and (NULL, 1) are two rows, not one. */
    @ParameterizedTest
    @MethodSource("inMemory")
    void theReloadKeepsRowsThatOnlyItsNullReadingMakesEqual(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        ModelContext ctx = Compiler.compileModel("""
                ###Relational
                Database t::DB ( Table K (NAME VARCHAR(10), N INTEGER) )
                ###Pure
                Class t::K { name: String[0..1]; n: Integer[0..1]; }
                ###Mapping
                Mapping t::M ( t::K: Relational { ~mainTable [t::DB]K name: [t::DB]K.NAME, n: [t::DB]K.N } )
                """);
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                // the table and its NULL row through the test-data reader; the empty text, which the reader reads as
                // NULL, written as it is
                for (String sql : CsvSeed.sqls("default\nK\nNAME,N\n---null---,1\n", "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
                s.execute("INSERT INTO K VALUES ('', 1)");
            }
            TestDataPlanner.Planned planned = TestDataPlanner.plan(ctx,
                    query("|t::K.all()->project([k|$k.name, k|$k.n], ['a', 'b'])"), "t::M",
                    List.of(new TestDataGenerator.TableRowIds("default", "K", List.of(
                            new TestDataGenerator.RowId(List.of("N"), List.of(1L))))), null, false, PlannerViews.of(ctx),
                    TestDataPlanner.Printing.CLEAN);
            com.legend.sql.SqlQuery k = TestDataPlanner.slice(planned, ctx, "t::DB").get("K");
            int rows = 0;
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(dialect.render(k))) {
                while (r.next()) {
                    rows++;
                }
            }
            assertEquals(2, rows, dialect.render(k));
        }
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
            // the seed route's answer (B2d.3e): upstream's JSON, the database's, by key
            String table = "{\"table\":{\"_type\":\"table\",\"table\":\"%s\",\"schema\":\"default\",\"database\":"
                    + "\"t::DB\"},\"rowIdentifiers\":[";
            assertEquals(table.formatted("KEYS") + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":1}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":2}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"ID\",\"value\":3}]}]}",
                    text(c, dialect, TestDataPlanner.seedJson(ctx, three)));
            assertEquals(table.formatted("CODES") + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"a'b\"}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"c\\\\d\"}]},"
                    + "{\"columnValuePairs\":[{\"name\":\"CODE\",\"value\":\"e\\nf\"}]}]}",
                    text(c, dialect, TestDataPlanner.seedJson(ctx, codes)));
            // no key: an empty list
            assertEquals(table.formatted("KEYS") + "]}", text(c, dialect, TestDataPlanner.seedJson(ctx,
                    TestDataPlanner.seed(ctx, query("|t::K.all()->filter(k|$k.id > 99)->project([k|$k.name], ['n'])"),
                            "t::M", false, queries))));
        }
    }

    /** L9's printing (the user's ruling of 2026-11-12: "Our clean sheet lite plan should always do the clean/nice/right
     *  thing. In backwards compat we can chop the timestamp and we can make float a known divergence"): a timestamp,
     *  with and without its fraction, NULL; a date; a float (register S306); a timestamp key of the seed's JSON. */
    private static final String PRINT_MODEL = """
            ###Relational
            Database t::DB
            (
              Table EVENT (ID INTEGER PRIMARY KEY, STAMP TIMESTAMP, ON_DAY DATE, AMOUNT DOUBLE)
              Table STAMPS (STAMP TIMESTAMP PRIMARY KEY, N INTEGER)
            )
            ###Pure
            Class t::E { id: Integer[1]; stamp: DateTime[0..1]; day: StrictDate[0..1]; amount: Float[0..1]; }
            Class t::S { stamp: DateTime[1]; n: Integer[0..1]; }
            ###Mapping
            Mapping t::M
            (
              t::E: Relational { ~mainTable [t::DB]EVENT id: [t::DB]EVENT.ID, stamp: [t::DB]EVENT.STAMP,
                day: [t::DB]EVENT.ON_DAY, amount: [t::DB]EVENT.AMOUNT }
              t::S: Relational { ~mainTable [t::DB]STAMPS stamp: [t::DB]STAMPS.STAMP, n: [t::DB]STAMPS.N }
            )
            """;

    @ParameterizedTest
    @MethodSource("inMemory")
    void aTimestampPrintsInFullOrAsUpstreamCutsIt(DatabaseType type) throws Exception {
        SqlDialect dialect = Databases.dialect(type);
        String model = PRINT_MODEL + "\n###Connection\nRelationalDatabaseConnection t::C { store: t::DB; "
                + com.legend.testbackend.TestBackends.of(type).connectionBody() + " }\n###Runtime\n"
                + "Runtime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }\n";
        ModelContext ctx = Compiler.compileModel(model);
        String csv = "default\nEVENT\nID,STAMP,ON_DAY,AMOUNT\n1,2020-01-02 03:04:05.123,2020-01-02,3.14\n"
                + "2,2020-01-02 03:04:05,2021-02-03,17.0\n3,---null---,---null---,---null---\n"
                + "4,2020-01-02 03:04:05.123456,2020-01-02,100000000000000000000.0\n"
                + "5,2020-01-02 03:04:05.5,2020-01-02,0.00001\n-----\n"
                + "default\nSTAMPS\nSTAMP,N\n2020-01-02 03:04:05.123,1\n2020-01-03 00:00:00,2\n";
        List<TestDataGenerator.TableRowIds> ids = List.of(new TestDataGenerator.TableRowIds("default", "EVENT",
                List.of(new TestDataGenerator.RowId(List.of("ID"), List.of(1L)),
                        new TestDataGenerator.RowId(List.of("ID"), List.of(2L)),
                        new TestDataGenerator.RowId(List.of("ID"), List.of(3L)),
                        new TestDataGenerator.RowId(List.of("ID"), List.of(4L)),
                        new TestDataGenerator.RowId(List.of("ID"), List.of(5L)))));
        LambdaFunction q = query("|t::E.all()->project([e|$e.stamp, e|$e.day, e|$e.amount], ['s', 'd', 'a'])");
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(type)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (String sql : CsvSeed.sqls(csv, "t::DB", ctx, dialect)) {
                    s.execute(sql);
                }
            }
            // a float is the database's own text in both printings (register S306, the known divergence): 3.14 and 17.0
            // as Pure prints them, 1e20 and 0.00001 as the database casts them, never Pure's 100000000000000000000.0
            String big = cast(c, 4);
            String small = cast(c, 5);
            assertFalse(big.equals("100000000000000000000.0") || small.equals("0.00001"), big + " " + small);
            // lite's own: the timestamp in full, lite's writers' canonical text, on every database
            assertEquals("default\nEVENT\nAMOUNT,ID,ON_DAY,STAMP\n"
                    + small + ",5,2020-01-02,2020-01-02T03:04:05.500000000+0000\n"
                    + "3.14,1,2020-01-02,2020-01-02T03:04:05.123000000+0000\n"
                    + "17.0,2,2021-02-03,2020-01-02T03:04:05.000000000+0000\n"
                    + big + ",4,2020-01-02,2020-01-02T03:04:05.123456000+0000\n"
                    + "---null---,3,---null---,---null---\n-----\n",
                    text(c, dialect, TestDataPlanner.plan(ctx, q, "t::M", ids, null, false, PlannerViews.of(ctx),
                            TestDataPlanner.Printing.CLEAN).csv()), type.name());
            // backwards compatibility: upstream's SimpleDateTimeFormat, the fraction cut
            assertEquals("default\nEVENT\nAMOUNT,ID,ON_DAY,STAMP\n" + small + ",5,2020-01-02,2020-01-02 03:04:05\n"
                    + "3.14,1,2020-01-02,2020-01-02 03:04:05\n"
                    + "17.0,2,2021-02-03,2020-01-02 03:04:05\n" + big + ",4,2020-01-02,2020-01-02 03:04:05\n"
                    + "---null---,3,---null---,---null---\n-----\n",
                    text(c, dialect, TestDataPlanner.plan(ctx, q, "t::M", ids, null, false, PlannerViews.of(ctx),
                            TestDataPlanner.Printing.COMPAT).csv()), type.name());
            // the seed's JSON: a timestamp key as upstream's ValueTransformer writes it, lite's canonical text
            TestDataPlanner.Seed stamps = TestDataPlanner.seed(ctx, query("|t::S.all()->project([x|$x.n], ['n'])"),
                    "t::M", false, PlannerViews.queries(ctx, "t::M", "t::RT"));
            assertEquals("{\"table\":{\"_type\":\"table\",\"table\":\"STAMPS\",\"schema\":\"default\",\"database\":"
                    + "\"t::DB\"},\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"STAMP\",\"value\":"
                    + "\"2020-01-02T03:04:05.123000000+0000\"}]},{\"columnValuePairs\":[{\"name\":\"STAMP\",\"value\":"
                    + "\"2020-01-03T00:00:00.000000000+0000\"}]}]}", text(c, dialect, TestDataPlanner.seedJson(ctx, stamps)),
                    type.name());
        }
    }

    /** EVENT {@code id}'s AMOUNT as the database casts it to a text. */
    private static String cast(Connection c, int id) throws Exception {
        try (Statement s = c.createStatement();
                ResultSet r = s.executeQuery("SELECT CAST(AMOUNT AS VARCHAR) FROM EVENT WHERE ID = " + id)) {
            assertTrue(r.next());
            return r.getString(1);
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
                        PlannerViews.of(ctx, "t::RT", Databases.dialect(type)), TestDataPlanner.Printing.CLEAN));
        assertEquals("Row Identifers should be provided for the root table: t::DB.ORDERS [ID:INT]\nExample:\n"
                + "  meta::relational::testDataGeneration::createTableRowIdentifiers(t::DB, 'default', 'ORDERS', [\n\n"
                + "  ])\n", root.getMessage());
        com.legend.error.AssertFailed view = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.AssertFailed.class, () -> TestDataPlanner.plan(ctx,
                        query("|t::Order.all()->project([o|$o.pnl.pnl], ['p'])"), "t::M",
                        List.of(ids("ORDERS", "ID", 1L)), null, false, PlannerViews.of(ctx, "t::RT", Databases.dialect(type)),
                        TestDataPlanner.Printing.CLEAN));
        assertEquals("Row Identifers should be provided for the root table of the view (PNLVIEW): t::DB.PNL"
                + " [ORDER_ID:INT]\nExample:\n"
                + "  meta::relational::testDataGeneration::createTableRowIdentifiers(t::DB, 'default', 'PNL', [\n\n"
                + "  ])\n", view.getMessage());
    }

    /** A column resolved by its exact name, as upstream resolves one (the orchestrator's go, 2026-11-12: no case
     *  leniency): a row identifier's column spelled in another case names no column (upstream's plan form filters the
     *  table's columns by {@code $x.name == $cv.first}), refused by name -- until then lite matched it in any case. */
    @org.junit.jupiter.api.Test
    void aColumnIsResolvedByItsExactName() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        com.legend.error.NotImplementedException e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.NotImplementedException.class, () -> TestDataPlanner.plan(ctx,
                        query("|t::Person.all()->project([p|$p.name], ['n'])"), "t::M",
                        List.of(ids("PERSON", "id", 2L)), null, false, PlannerViews.of(ctx), TestDataPlanner.Printing.CLEAN));
        assertEquals("testDataGen: column 'id' not on table 'PERSON'", e.getMessage());
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
        TestDataPlanner.Planned planned = TestDataPlanner.plan(ctx, query(query), "t::M", ids, null, hash, views,
                TestDataPlanner.Printing.CLEAN);
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
