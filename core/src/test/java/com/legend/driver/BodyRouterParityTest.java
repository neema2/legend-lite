// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.Execution;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.exec.Census;
import com.legend.exec.Sessions;
import com.legend.exec.StatementOrigin;
import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batch;
import com.legend.result.ResultSink;
import com.legend.sql.dialect.RawSqlBoundary;
import com.legend.test.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testplan.BodyClass;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE BODY ROUTER'S PARITY (the test-is-the-product plan's M5; the M5 audit's missing test): a body of a moved class
 * runs on the program road exactly as the walk runs it, observed at every boundary -- the outcome and its message, the
 * raw-SQL ledger the referee mirrors, the answer handed to the caller's sink (its shape, columns and rows), the
 * statement-origin census, the effects census and the database the body leaves -- each body run both ways on a fresh
 * database. A shape the class rule keeps on the walk is pinned as such (each the audit's counterexample). Until M10
 * deletes the walk.
 */
class BodyRouterParityTest {

    private static final String RT = StorelessRuntime.RUNTIME;
    private static final String EXEC = "meta::relational::metamodel::execute::executeInDb(";

    /** Every database the platform runs in-process that a test's connection can name (Pure's
     *  {@code DatabaseType}): each body runs both ways on each. */
    static List<TestBackend> backends() {
        List<String> named = Compiler.compileModel(TestBackends.platform().withStorelessRuntime(""))
                .findEnum("meta::relational::runtime::DatabaseType").orElseThrow().values();
        return TestBackends.inProcess().stream().filter(b -> named.contains(b.type().name())).toList();
    }

    private static String connection(TestBackend backend) {
        return "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                + "type=meta::relational::runtime::DatabaseType." + backend.type().name() + ");\n";
    }

    private static String effect(String sql) {
        return EXEC + "'" + sql + "', $c, 0, 1000);\n";
    }

    @ParameterizedTest
    @MethodSource("backends")
    void effectsThenALiteral(TestBackend backend) throws Exception {
        assertParity(backend, connection(backend) + effect("create table p1(i int)")
                + effect("insert into p1 values (1)") + "true;}", "select i from p1");
    }

    @ParameterizedTest
    @MethodSource("backends")
    void anEffectLastAnswersNoValue(TestBackend backend) throws Exception {
        assertParity(backend, connection(backend) + effect("create table p2(i int)") + "}",
                "select count(*) from p2");
    }

    @ParameterizedTest
    @MethodSource("backends")
    void aPrintLastAnswersNoValue(TestBackend backend) throws Exception {
        assertParity(backend, connection(backend) + effect("create table p3(i int)") + "print('x', 1);}",
                "select count(*) from p3");
    }

    @ParameterizedTest
    @MethodSource("backends")
    void theConnectionLastAnswersNoValue(TestBackend backend) throws Exception {
        assertParity(backend, connection(backend) + effect("create table p4(i int)") + "$c;}",
                "select count(*) from p4");
    }

    @ParameterizedTest
    @MethodSource("backends")
    void literalsBeforeAndLetsFoldedIntoAnEffect(TestBackend backend) throws Exception {
        assertParity(backend, connection(backend) + "let t = 'p5';\n'x';\n"
                + EXEC + "'create table ' + $t + '(i int)', $c, 0, 1000);\n'done';}", "select count(*) from p5");
    }

    /** M10c item 13: a RAW READ bound by a let no later statement reads -- the corpus's setup `let dataInTable =
     *  executeInDb('select ...', $connection)` -- is spliced into no reader on the walk and ridden by the door: nothing
     *  sent on either road, the body an effect program. The walk's, by name: a dead computed value (its reduction could
     *  raise at another turn), a raw read a later lambda's parameter of the same name shadows after an effect (the
     *  walk's LetPlanner stages it), a raw read that IS read (T8). */
    @ParameterizedTest
    @MethodSource("backends")
    void anUnreadLetInAnEffectProgram(TestBackend backend) throws Exception {
        String rawRead = connection(backend) + effect("create table p9(i int)") + "let dead = " + EXEC
                + "'select * from p9', $c, 0, 1000);\n" + effect("insert into p9 values (9)") + "true;}";
        ModelContext ctx = Compiler.compileModel(model(backend, false));
        // the let is unread, so it is the statement it binds (the user's ruling, 2026-11-02): the read runs at its turn
        assertEquals(BodyClass.EFFECT_PROGRAM, BodyClass.of(unreadAsStatements(typed(ctx, rawRead)), RT), rawRead);
        assertParity(backend, rawRead, "select i from p9");
        for (String stays : List.of(
                connection(backend) + effect("create table p8(i int)") + "let dead = 'abc'->toUpper();\n"
                        + effect("insert into p8 values (8)") + "true;}",
                connection(backend) + "let dead = " + EXEC + "'select 1', $c, 0, 1000);\n"
                        + effect("create table p10(i int)") + "let other = [1, 2]->map(dead | $dead + 1);\ntrue;}")) {
            assertNull(BodyClass.of(unreadAsStatements(typed(ctx, stays)), RT), "the walk's: " + stays);
        }
    }

    @ParameterizedTest
    @MethodSource("backends")
    void twoSegments(TestBackend backend) throws Exception {
        assertParity(backend, connection(backend) + effect("create table p6(i int)") + "'between';\n"
                + effect("insert into p6 values (6)") + "true;}", "select i from p6");
    }

    @ParameterizedTest
    @MethodSource("backends")
    void aScriptFailingAtItsSecondStatement(TestBackend backend) throws Exception {
        // a database that names the failing statement applied the first (the ledger records it); one that rolls the
        // script back as a unit applied nothing: the walk's rule, and its message, on both roads
        assertParity(backend, connection(backend) + effect("create table p7(i int)")
                + effect("insert into no_such_table values (1)") + "true;}", "select count(*) from p7");
    }

    @ParameterizedTest
    @MethodSource("backends")
    void queryPrograms(TestBackend backend) throws Exception {
        for (String body : List.of("{| true;}", "{| let x = 1; $x + 1;}", "{| 'abc'->toUpper();}",
                "{| [1, 2, 3]->map(x|$x * 2);}", "{| print('x', 1); 1;}",
                "{| #TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > 1);}",
                "{| let n = 1; #TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > $n)->select(~b);}",
                // the walk's answer contract, held by the router (§9, D30, D46): a required root of no row, a
                // to-one reader of two
                "{| #TDS\n  x:Integer\n  1\n#->filter(r|$r.x > 5)->map(r|$r.x)->toOneMany();}",
                "{| #TDS\na,b\n1,x\n2,y\n#->toOne();}")) {
            assertParity(backend, body, "select 1");
        }
    }

    @ParameterizedTest
    @MethodSource("backends")
    void assertPrograms(TestBackend backend) throws Exception {
        for (String body : List.of(
                "{| assertEquals(2, 1 + 1);}",
                "{| assertEquals(3, 1 + 1);}",
                "{| assertNotEquals(3, 1 + 1); 'after';}",
                "{| let n = 2; assertEquals(['a', 'b'], ['b', 'a']->sort()); assertEquals($n, 1 + 1);}",
                "{| assertSize([1, 2, 3], 3); assertEmpty([]); assertNotEmpty([1]); assert(1 < 2);"
                        + " assertFalse(1 > 2); true;}",
                "{| assertSameElements([1, 2], [2, 1]); assertContains([1, 2], 2);"
                        + " assertEqWithinTolerance(1.0, 1.05, 0.1);}",
                "{| assertJsonStringsEqual('{\"a\":1}', '{\"a\": 1}');}",
                // a class test by a type reference (M9c: a class reference is a value the class rule admits, as the
                // resolver reads it; the host checks plan the type's name)
                "{| assertInstanceOf(1, Integer); assertInstanceOf('a', String);}",
                "{| assertEquals(#TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > 1),"
                        + " #TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > 1));}",
                "{| assertEquals(1, #TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > 1)->size());}",
                "{| assertEq(1, 1); assertEq('a', 'a');}",
                // a side the database refuses: the assert reported failed, the data error the body's
                "{| assertEquals(1, 'abc'->parseInteger());}")) {
            assertParity(backend, body, "select 1");
        }
        // asserts, then a query answered: its planning after the asserts is the walk's at its turn
        ModelContext ctx = Compiler.compileModel(model(backend, false));
        assertNull(BodyClass.of(typed(ctx, "{| assertEquals(2, 1 + 1); #TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > 1);}"),
                RT, true));
    }

    /** The cases M6's audit asked for (S4): an assert after a failing one, a let between asserts, the class test,
     *  the TDS equivalence, the rendered text, and a planned raise from a side's planning. */
    @ParameterizedTest
    @MethodSource("backends")
    void assertProgramsTheAuditAsked(TestBackend backend) throws Exception {
        for (String body : List.of(
                // an assert after a failing one: planned, never reached
                "{| assertEquals(3, 1 + 1); assertEquals(2, 1 + 1);}",
                // a let between asserts; a statement that sends nothing last
                "{| assertEquals(1, 1); let x = 2; assertEquals($x, 2); 'done';}",
                "{| assertTdsEquivalent(#TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > 0),"
                        + " #TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > 0), 0.1);}",
                // the rendered text: one side a grid's text (the CSV form, toCSV, is the relational tests' own
                // function, absent from this model: the corpora measure it)
                "{| assertEquals('a,b\\n1,x\\n2,y\\n', #TDS\na,b\n1,x\n2,y\n#->toString());}",
                // a side whose planning raises (a computed bound the lowering refuses by name): raised at its turn,
                // after the assert before it ran and reported, counted under its family
                "{| assertEquals(1, 1); assertEquals(#TDS\na\n1\n2\n#->filter(r|$r.a > 1),"
                        + " #TDS\na\n1\n2\n#->limit('1'->parseInteger()));}",
                // the M8a audit's E: a type identity decided with no side (held, failed with its message), the TDS
                // equivalence with a time delta, the forAll-contains subset (held, refuted), a sorted rendered text
                "{| assertIs(Integer, Integer); assertIs(type(1), Integer);}",
                "{| assertIs(Integer, String);}",
                "{| assertTdsEquivalent(#TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > 0),"
                        + " #TDS\na,b\n1,x\n2,y\n#->filter(r|$r.a > 0), 0.1, 1.0);}",
                "{| assert([1, 2]->forAll(e|[1, 2, 3]->contains($e)));}",
                "{| assertFalse([1, 4]->forAll(e|[1, 2]->contains($e)));}",
                "{| assertEquals('a,b\\n1,x\\n2,y\\n', #TDS\na,b\n2,y\n1,x\n#->sort(~a->ascending())->toString());}")) {
            assertParity(backend, body, "select 1");
        }
    }

    /** The keyed-class pairs (X5: a keyed class compares by its key properties alone; M8a: the key trees, and the
     *  classes a polymorphic key slot's value may carry as its own, are read when the arm is planned): a same-class
     *  pair, a keyless abstract slot holding keyed subclass values (judged by the own class's keys), the multiset
     *  form, the negated form, and a failing pair with the walk's own message. */
    @ParameterizedTest
    @MethodSource("backends")
    void keyedClassPrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(backend.withStorelessRuntime(KEYED));
        // the outcome pinned ABSOLUTELY beside the parity on the platform backend (the M8a audit's S1): both roads
        // run the same judgment, so a key restriction that stopped applying would fail both alike and parity alone
        // would not see it -- the first body holds ONLY through the restriction (two construction sites, two
        // identities); another backend may refuse the construction itself (H2: no list encoding), the same on both
        // roads, so there the parity alone is asserted
        boolean platform = backend.type() == TestBackends.platform().type();
        for (var c : List.of(
                Map.entry("{| assertEquals(^x::Lit(value=1), ^x::Lit(value=1));}", "ok"),
                Map.entry("{| assertEquals(^x::Call(name='f', args=[^x::Lit(value=1), ^x::Lit(value=2)]),"
                        + " ^x::Call(name='f', args=[^x::Lit(value=1), ^x::Lit(value=2)]));}", "ok"),
                Map.entry("{| assertEquals(^x::Call(name='f', args=[^x::Lit(value=1)]),"
                        + " ^x::Call(name='f', args=[^x::Lit(value=2)]));}", "AssertFailed:"),
                Map.entry("{| assertSameElements([^x::Lit(value=1), ^x::Lit(value=2)],"
                        + " [^x::Lit(value=2), ^x::Lit(value=1)]);}", "ok"),
                Map.entry("{| assertNotEquals(^x::Lit(value=1), ^x::Lit(value=2));}", "ok"),
                // the same let-bound instance: decided with no side
                Map.entry("{| let f = ^x::Lit(value=1); assertIs($f, $f);}", "ok"))) {
            Run walked = assertParity(backend, ctx, RT, c.getKey(), "select 1", List.of(),
                    ExecuteOptions.JudgeMode.HOST);
            if (platform) {
                assertTrue(walked.outcome.startsWith(c.getValue()), c.getKey() + " -> " + walked.outcome);
            }
        }
        for (String body : List.of(
                // a class wire against the type's accepted classes: itself, its supertype's subtree, a sibling. Today
                // BOTH roads fail the first two ("expected ... to be an instance of Lit, actual: LinkedHashMap": a
                // constructed instance's wire judged by the pure rule, not its classifier) where Pure holds them -- a
                // pre-existing divergence posted as a register question (the plan's §9 "M8a" (f)), parity alone here
                "{| assertInstanceOf(^x::Lit(value=1), x::Lit);}",
                "{| assertInstanceOf(^x::Lit(value=1), x::Node);}",
                "{| assertInstanceOf(^x::Lit(value=1), x::Call);}",
                // a slot declared Any (the prelude's List.values, the universe arm of the planned key trees): today
                // both roads wall on the list's representation (toRepresentation of a list is not modeled), so the
                // arm has no passing witness here -- the corpora's Pair and List stamps measure it
                "{| assertEquals(list([^x::Lit(value=1), 'x']), list([^x::Lit(value=1), 'x']));}",
                "{| assertEquals(list([pair(1, 2), 'x']), list([pair(1, 2), 'x']));}",
                // assertEq over class values: eq's identity rule walls loudly on both roads, with one text
                "{| assertEq(^x::Lit(value=1), ^x::Lit(value=1));}")) {
            assertParity(backend, ctx, RT, body, "select 1", List.of());
        }
    }

    /** Keyed classes: a keyless abstract node, a literal keyed by its value, a call keyed by its name and its
     *  arguments -- a slot declared the keyless node, so an argument is judged by its own class's keys. */
    private static final String KEYED = """
            ###Pure
            Class x::Node { }
            Class x::Lit extends x::Node { <<equality.Key>> value: Integer[1]; }
            Class x::Call extends x::Node { <<equality.Key>> name: String[1]; <<equality.Key>> args: x::Node[*]; }
            """;

    /** A model of a class, a temporal class and a function, for the shapes the query-program rule keeps on the walk
     *  (classified only: none runs). */
    private static final String CLASSES = """
            ###Pure
            Class my::P { name: String[1]; age: Integer[0..1]; }
            Class <<meta::pure::profiles::temporal.businesstemporal>> my::T { name: String[1]; }
            """;

    @Test
    void theQueryShapesTheRuleKeepsOnTheWalk() {
        TestBackend backend = TestBackends.platform();
        ModelContext ctx = Compiler.compileModel(backend.withStorelessRuntime(CLASSES));
        for (String body : List.of(
                // an object answered (M5f's audit B2): a graph on the product's road
                "{| my::P.all();}",
                // a value through a class's chain (B1): the product's column facts
                "{| my::P.all()->map(p|$p.name);}",
                // a value statement before the last (B3): a check the walk holds and labels
                "{| 'abc'->toUpper(); 'end';}",
                // a relation let before the last (S3): the walk spends an alias on it
                "{| let t = #TDS\na\n1\n2\n#; $t->filter(r|$r.a > 1);}",
                // a to-one reader through a let (B4)
                "{| let t = #TDS\na\n1\n2\n#->toOne(); $t;}",
                // a temporal class read (S2): the exists-join form
                "{| my::T.all(%2020-01-01)->project(~[n:t|$t.name]);}",
                // an object built and answered (MV6's ids; a construction the compiler folds away, as
                // ^Pair(...).first, leaves none)
                "{| ^my::P(name='x');}")) {
            assertNull(BodyClass.of(typed(ctx, body), RT), body);
        }
    }

    @Test
    void theShapesTheRuleKeepsOnTheWalk() {
        TestBackend backend = TestBackends.platform();
        String c = connection(backend);
        // a computed value (the M5 audit's S3): the product's lowering would answer it
        assertWalked(backend, c + "'abc'->toUpper();\n" + effect("create table w1(i int)") + "true;}", false);
        // print of an effect (S4): its argument holds a test kind
        assertWalked(backend, c + effect("create table w2(i int)") + "print(" + EXEC
                + "'insert into w2 values (1)', $c, 0, 1000), 1);\ntrue;}", false);
        // a runtime that declares test data (B1): the two roads seed and memo differently
        assertWalked(backend, c + effect("insert into seed_t values (9)") + "true;}", true);
        // a computed effect argument (T9, M9)
        assertWalked(backend, c + EXEC + "'create table w3(i int)'->toUpper(), $c, 0, 1000);\ntrue;}", false);
        // an executeInDb result read: a raw grid (T8, M9)
        assertWalked(backend, c + "let r = " + EXEC + "'select 1', $c, 0, 1000);\n$r;}", false);
    }

    private static void assertWalked(TestBackend backend, String body, boolean seeded) {
        ModelContext ctx = Compiler.compileModel(model(backend, seeded));
        assertNull(BodyClass.of(typed(ctx, body), RT), body);
    }

    /** A side whose planning raises is a PLANNED RAISE at its assert's turn (R14; M6's audit, S1): the assert before it
     *  ran and reported, the body fails with the planning's own error, on both roads. */
    @ParameterizedTest
    @MethodSource("backends")
    void aSidesPlanningRaisesAtItsTurn(TestBackend backend) throws Exception {
        String body = "{| assertEquals(1, 1); assertEquals(#TDS\na\n1\n2\n#->filter(r|$r.a > 1),"
                + " #TDS\na\n1\n2\n#->limit('1'->parseInteger()));}";
        ModelContext ctx = Compiler.compileModel(model(backend, false));
        for (boolean router : List.of(false, true)) {
            Run run = run(backend, ctx, body, router, "select 1");
            assertEquals(true, run.outcome.startsWith("NotImplementedException: dynamic slicing bounds"),
                    (router ? "the router: " : "the walk: ") + run.outcome);
            assertEquals(1, run.verdicts.size(), "the first assert reported, the second raised: " + run.verdicts);
        }
    }

    /** A store, a mapping and a runtime with NO test data on {@code backend} (a seeded runtime is left on the walk):
     *  the frames' model, plus a seeded runtime over the same store for the frame-runtime case. The table is the
     *  harness's to create. */
    private static String frameModel(TestBackend backend) {
        return """
                Class x::Firm { name: String[1]; tagged(s: String[1]) { $this.name + $s }: String[1]; }
                Class <<meta::pure::profiles::temporal.businesstemporal>> x::TFirm { name: String[1]; }
                Class x::Key { <<equality.Key>> k: String[1]; v: Integer[1]; }
                // a user-written assert helper (toPostgresModel's assertConversion: lets, then an assert over class
                // values): the walk inlines the call and adjudicates its reduced root; M10c item 4 plans the same
                function x::assertSame(expected: x::Key[1], actual: x::Key[1]): Boolean[1]
                { let a = $actual; assertEquals($expected, $a); }
                function x::firms(): meta::pure::tds::TabularDataSet[1] { x::Firm.all()->project([f|$f.name], ['name']) }
                // the corpus's testRuntime(): a Runtime built in a helper, naming the store element (M10c item 7)
                function x::testRuntime(): meta::core::runtime::Runtime[1]
                { ^meta::core::runtime::Runtime(connectionStores = ^meta::core::runtime::ConnectionStore(element = x::DB,
                    connection = ^meta::external::store::relational::runtime::TestDatabaseConnection(
                      type = meta::relational::runtime::DatabaseType.%s))) }
                function x::testRuntimeFor(db: meta::relational::metamodel::Database[1]): meta::core::runtime::Runtime[1]
                { ^meta::core::runtime::Runtime(connectionStores = ^meta::core::runtime::ConnectionStore(element = $db,
                    connection = ^meta::external::store::relational::runtime::TestDatabaseConnection(
                      type = meta::relational::runtime::DatabaseType.%s))) }
                // the corpus's setupTestData (modelJoin): the connection picked by the runtime's qualified property, the
                // CSVs spelled as DDL and rows by the seed form, each statement sent through a map over executeInDb --
                // a helper the resolver's statement inliner unfolds into the body (M10c item 7c)
                function x::setupData(csvs: String[*], db: meta::relational::metamodel::Database[1],
                    runtime: meta::core::runtime::Runtime[1]): Boolean[1]
                { let connection = $runtime.connectionByElement($db)->cast(
                    @meta::external::store::relational::runtime::TestDatabaseConnection);
                  let sqls = $csvs->joinStrings('\\n-\\n')->meta::alloy::service::execution::setUpDataSQLs($db);
                  $sqls->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $connection));
                  true; }
                // the same helper ending in the map: unfolded, the map is the body's last statement (its answer)
                function x::seedOnly(csvs: String[*], db: meta::relational::metamodel::Database[1],
                    runtime: meta::core::runtime::Runtime[1]): Any[*]
                { let connection = $runtime.connectionByElement($db)->cast(
                    @meta::external::store::relational::runtime::TestDatabaseConnection);
                  let sqls = $csvs->joinStrings('\\n-\\n')->meta::alloy::service::execution::setUpDataSQLs($db);
                  $sqls->map(sql | meta::relational::metamodel::execute::executeInDb($sql, $connection)); }
                function x::aFirm(): x::Firm[1] { ^x::Firm(name='Cyl') }
                function x::oneExec(): Integer[1] { meta::pure::router::execute(|x::Firm.all()->project([f|$f.name],
                    ['name']), x::M, x::RT.runtimeValue, [])->size() }
                function x::execHelper(): meta::pure::mapping::Result<Any|*>[1] { meta::pure::router::execute(
                    |x::Firm.all()->project([f|$f.name], ['name']), x::M, x::RT.runtimeValue, []) }
                // the engine test library's engine-text asserts, in their own packages (the arms detect them by FQN
                // and splice the envelope's SQL read; the bodies here spell what the library's do: M10b's witnesses)
                function meta::relational::mapping::sqlRemoveFormatting(r: meta::pure::mapping::Result<Any|*>[1]): String[1]
                { $r.activities->at(0)->cast(@meta::relational::mapping::RelationalActivity).sql->toOne() }
                function meta::relational::functions::asserts::assertSameSQL(expected: String[1], actual: String[1]): Boolean[1]
                { assertEquals($expected, $actual) }
                function meta::relational::functions::asserts::assertSameSQL(expected: String[1],
                    r: meta::pure::mapping::Result<Any|*>[1]): Boolean[1]
                { assertEquals($expected, meta::relational::mapping::sqlRemoveFormatting($r)) }
                function meta::relational::functions::asserts::assertSameSQL(expected: String[1],
                    r: meta::relational::functions::sqlstring::SQLResult[1]): Boolean[1]
                { assertEquals($expected, meta::relational::mapping::sqlRemoveFormatting(
                    $r->cast(@meta::pure::mapping::Result<Any|*>))) }
                // the engine's body (h2Extension.pure): the H2 version read through executeInDb -- a raw grid the
                // program road never runs (the arm plans the texts from the arguments); the database planner's static
                // scan reads the arguments (M10c). Two spellings: the Result-actual (the splice of the frame's SQL) and
                // the corpus's String-actual (a plan text, a rendered SQL)
                function meta::relational::functions::sqlQueryToString::h2::assertEqualsH2Compatible(legacy: String[1],
                    upgraded: String[1], r: meta::pure::mapping::Result<Any|*>[1]): Boolean[1]
                { let h2Versions = meta::relational::functions::sqlQueryToString::h2::getH2Versions();
                  assertEquals(1, $h2Versions->size(), 'More than one H2 version found asserting test outputs');
                  if($h2Versions->toOne()->eq('1.4.200'),
                    | assertEquals($legacy, meta::relational::mapping::sqlRemoveFormatting($r)),
                    | assertEquals($upgraded, meta::relational::mapping::sqlRemoveFormatting($r))); }
                function meta::relational::functions::sqlQueryToString::h2::assertEqualsH2Compatible(legacy: String[1],
                    upgraded: String[1], actual: String[1]): Boolean[1]
                { let h2Versions = meta::relational::functions::sqlQueryToString::h2::getH2Versions();
                  assertEquals(1, $h2Versions->size(), 'More than one H2 version found asserting test outputs');
                  if($h2Versions->toOne()->eq('1.4.200'),
                    | assertEquals($legacy, $actual),
                    | assertEquals($upgraded, $actual)); }
                function meta::relational::functions::sqlQueryToString::h2::getH2Versions(): Any[*]
                { let conn = ^meta::external::store::relational::runtime::TestDatabaseConnection(
                    type=meta::relational::runtime::DatabaseType.H2);
                  let results = meta::relational::metamodel::execute::executeInDb('SELECT H2VERSION();', $conn);
                  $results.rows.values; }
                ###Relational
                Database x::DB (
                  Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32))
                  Table TFIRM ( milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                    ID INTEGER PRIMARY KEY, NAME VARCHAR(32), from_z DATE, thru_z DATE)
                )
                ###Mapping
                Mapping x::M (
                  *x::Firm: Relational { ~mainTable [x::DB] FIRM name: [x::DB] FIRM.NAME }
                  *x::TFirm: Relational { ~mainTable [x::DB] TFIRM name: [x::DB] TFIRM.NAME }
                )
                ###Connection
                RelationalDatabaseConnection x::Conn
                {
                    store: x::DB;
                    type: %s;
                    specification: LocalH2 { };
                    auth: DefaultH2;
                }
                RelationalDatabaseConnection x::ConnSeeded
                {
                    store: x::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupSqls: ['insert into FIRM values (9, \\'Seed\\')']; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ env: x::Conn ] ]; }
                Runtime x::RTSeeded { mappings: [x::M]; connections: [ x::DB: [ env: x::ConnSeeded ] ]; }
                """.formatted(backend.type().name(), backend.type().name(), backend.type().name(),
                        backend.type().name());
    }

    private static final List<String> FIRM_ROWS = List.of(
            "create table FIRM (ID integer primary key, NAME varchar(32))",
            "insert into FIRM values (1, 'Acme'), (2, 'Bolt')",
            "create table TFIRM (ID integer primary key, NAME varchar(32), from_z date, thru_z date)",
            "insert into TFIRM values (1, 'Old', date '2000-01-01', date '2010-01-01'),"
                    + " (2, 'New', date '2010-01-01', date '9999-12-31')");
    /** A frame over a business-temporal class (M9d): its eager run is the product's lowering of the temporal read. */
    private static final String EXECUTE_TEMPORAL = "meta::pure::router::execute(|x::TFirm.all(%2020-06-01)"
            + "->project([t|$t.name], ['name']), x::M, x::RT.runtimeValue, [])";
    private static final String EXECUTE = "meta::pure::router::execute(|x::Firm.all()->project([f|$f.name], ['name']),"
            + " x::M, x::RT.runtimeValue, [])";
    /** A class-rooted frame (the M9a audit's S2): the eager run is the product's lowering of an object read. */
    private static final String EXECUTE_CLASS = "meta::pure::router::execute(|x::Firm.all(), x::M, x::RT.runtimeValue,"
            + " [])";

    /** The frame programs (M9a): {@code let r = execute(...)} read by the asserts after it -- the corpus's idiom --
     *  run both ways: every statement (the frame's eager run under LET, the asserts' sides), the census, the verdicts,
     *  the answer. */
    @ParameterizedTest
    @MethodSource("backends")
    void framePrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        for (String body : List.of(
                // one frame, one assert over its rows
                "{| let r = " + EXECUTE + "; assertEquals(['Acme', 'Bolt'], $r.values.rows.values);}",
                // a frame read by two asserts, a value let reading it between them, a literal last
                "{| let r = " + EXECUTE + "; assertSize($r.values.rows, 2); let n = $r.values.rows->size();"
                        + " assertEquals(2, $n); true;}",
                // two frames, each read
                "{| let r = " + EXECUTE + "; let s = " + EXECUTE + "; assertEquals($r.values.rows->size(),"
                        + " $s.values.rows->size());}",
                // an assert before the frame, then the frame, then an assert that fails
                "{| assertEquals(1, 1); let r = " + EXECUTE + "; assertEquals(['Acme'], $r.values.rows.values);}",
                // the frame's rows filtered in the side
                "{| let r = " + EXECUTE + "; assertEquals(['Bolt'], $r.values->filter(x|$x.name == 'Bolt').rows"
                        + ".values);}",
                // M9b: an alias of the frame, the same frame under another name
                "{| let r = " + EXECUTE + "; let v = $r.values->at(0); assertSize($v.rows, 2);"
                        + " assertEquals(['Acme', 'Bolt'], $v.rows.values);}",
                // M9b: an inline execute read where it stands (the product's lazy splice)
                "{| assertSize(" + EXECUTE + ".values.rows, 2);}",
                // M9b: a database effect between the frame's let and its reader (the eager run before the effect, the
                // side after it)
                "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                        + "type=meta::relational::runtime::DatabaseType." + backend.type().name() + "); let r = "
                        + EXECUTE + "; assertSize($r.values.rows, 2); " + EXEC
                        + "'insert into FIRM values (3, \\'Cyl\\')', $c, 0, 1000); assertSize($r.values.rows, 3);}",
                // the M9a audit's S2: a class-rooted frame (the eager run an object read; its values the objects)
                "{| let r = " + EXECUTE_CLASS + "; assertSize($r.values, 2); assertEquals(['Acme', 'Bolt'],"
                        + " $r.values.name);}",
                // the M9a audit's S4: a frame read only through a value let; a failing first assert, then a reader
                "{| let r = " + EXECUTE + "; let n = $r.values.rows->size(); assertEquals(2, $n);}",
                "{| let r = " + EXECUTE + "; assertEquals(['Acme'], $r.values.rows.values); assertSize($r.values.rows,"
                        + " 2);}",
                // the M9b audit's F3: a frame's query through a plain helper (inlined on both roads; the rule's reach
                // into the helper finds no kind planned another way)
                "{| let r = meta::pure::router::execute(|x::firms(), x::M, x::RT.runtimeValue, []);"
                        + " assertSize($r.values.rows, 2);}",
                // F3: an alias of an alias; an alias read by a value let
                "{| let r = " + EXECUTE + "; let v = $r.values->at(0); let w = $v->toOne(); let n = $w.rows->size();"
                        + " assertEquals(2, $n); assertSize($w.rows, 2);}",
                // F3: an effect before the frame's let (the effect's script closed before the eager run)
                "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                        + "type=meta::relational::runtime::DatabaseType." + backend.type().name() + "); " + EXEC
                        + "'insert into FIRM values (3, \\'Cyl\\')', $c, 0, 1000); let r = " + EXECUTE
                        + "; assertSize($r.values.rows, 3);}",
                // F3: a print of a frame read between the asserts (inert on both roads)
                "{| let r = " + EXECUTE + "; assertSize($r.values.rows, 2); print($r.values.rows->size());"
                        + " assertSize($r.values.rows, 2);}",
                // the M9c audit's F7: a helper-wrapped execute bound by a let is a VALUE let both roads splice lazily
                // (the walk's standing divergence from upstream, which runs it at the let: recorded in the plan's §9)
                "{| let r = x::execHelper(); assertSize($r.values.rows, 2);}",
                "{| let r = " + EXECUTE + "; assertEquals($r.values.rows->size(), x::execHelper().values.rows"
                        + "->size());}",
                // the M9c audit's F8: the side kinds admitted since M9c -- an object literal's property, a temporal
                // class's extent, the activity comment through a tainted value let
                "{| let r = " + EXECUTE + "; assertEquals('Cyl', ^x::Firm(name='Cyl').name);"
                        + " assertSize($r.values.rows, 2);}",
                "{| let r = " + EXECUTE + "; assertEquals(1, x::TFirm.all(%2020-06-01)->from(x::M, x::RT)->size());"
                        + " assertSize($r.values.rows, 2);}",
                "{| let r = " + EXECUTE + "; let cm = $r.activities->at(0)->cast(@meta::relational::mapping::"
                        + "RelationalActivity).comment->toOne(); assert($cm->startsWith('-- \"executionTraceID\"'));}",
                // M9d: a frame over a business-temporal class, read by the asserts
                "{| let r = " + EXECUTE_TEMPORAL + "; assertSize($r.values.rows, 1); assertEquals(['New'],"
                        + " $r.values.rows.values);}",
                // M9d: the corpus's plan-handle idiom (`let p = executionPlan(...)`, its mapping a reference the rule
                // reads as the handle's query, never as a system-store read), the plan's SQL read from the handle's rows
                "{| let p = meta::pure::executionPlan::executionPlan(|x::Firm.all()->project([f|$f.name], ['name']),"
                        + " x::M, x::RT.runtimeValue, []); assertNotEmpty($p.rootExecutionNode.executionNodes);"
                        + " assert($p.rootExecutionNode.executionNodes->at(0)->cast(@meta::relational::mapping::"
                        + "SQLExecutionNode).sqlQuery->toOne()->startsWith('select'));}",
                // M9d: the corpus's value-let idiom over an inline execute (`let tds = execute(...).values->toOne()`)
                "{| let tds = " + EXECUTE + ".values->toOne(); assertEquals(2, $tds.rows->size());"
                        + " assertSameElements(['Acme', 'Bolt'], $tds.rows->map(r|$r.get('name')->toString()));}",
                // M9c: the frame's activities read -- the run's SQL (a planning-time render on both roads), through the
                // corpus's filter-and-cast idiom, through a value let, and inline
                "{| let r = " + EXECUTE + "; assertSize($r.activities, 1); assert($r.activities->at(0)->cast("
                        + "@meta::relational::mapping::RelationalActivity).sql->toOne()->startsWith('select'));}",
                "{| let r = " + EXECUTE + "; assertEquals(1, $r.activities->filter(a|$a->instanceOf("
                        + "meta::relational::mapping::RelationalActivity))->cast(@meta::relational::mapping::"
                        + "RelationalActivity).sql->size());}",
                "{| let r = " + EXECUTE + "; let a = $r.activities; assertSize($a, 1); let s = $r.activities->at(0)"
                        + "->cast(@meta::relational::mapping::RelationalActivity).sql->toOne();"
                        + " assert($s->startsWith('select'));}",
                "{| assertSize(" + EXECUTE + ".activities, 1);}",
                // the M9a audit's S3: the frame's own runtime argument declares test data the body's runtime does not:
                // neither road seeds it (the seeds are a from()'s; the frame's run reads the table as it stands)
                "{| let r = meta::pure::router::execute(|x::Firm.all()->project([f|$f.name], ['name']), x::M,"
                        + " x::RTSeeded.runtimeValue, []); assertSize($r.values.rows, 2);}")) {
            assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS);
        }
        // a body whose own runtime declares test data stays on the walk (the first rule of every class)
        assertNull(BodyClass.of(typed(ctx, "{| let r = " + EXECUTE + "; assertSize($r.values.rows, 2);}"),
                "x::RTSeeded", true));
    }

    /** The frame shapes M9 leaves on the walk, each by name: the eager inline execute, an effect let, a frame reading
     *  a frame, a kind planned another way inside a helper, a computed effect, an effect last. */
    @Test
    void theFrameShapesTheRuleKeepsOnTheWalk() {
        TestBackend backend = TestBackends.platform();
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        for (String body : List.of(
                // the one inline shape the splice runs at planning: size(execute(...))
                "{| assertEquals(1, " + EXECUTE + "->size());}",
                // an effect let (its value a raw grid)
                "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                        + "type=meta::relational::runtime::DatabaseType." + backend.type().name() + "); let x = " + EXEC
                        + "'insert into FIRM values (3, \\'Cyl\\')', $c, 0, 1000); let r = " + EXECUTE
                        + "; assertSize($r.values.rows, 3);}",
                // $r.values->at(k>0) over a relation frame: the walk raises it at the let (the M9a audit's S1), so
                // the rule names the shape and leaves it
                "{| let r = " + EXECUTE + "; let v = $r.values->at(1); assertSize($v.rows, 2);}",
                // a frame with no assert after it is no assert program
                "{| let r = " + EXECUTE + "; $r.values.rows->size();}",
                // the M9a audit's N1: a frame's query reading an earlier frame by name (the walk's run chain is
                // resolved with no splice, so the walk raises at the let where the program road would splice it)
                "{| let r = " + EXECUTE + "; let s = meta::pure::router::execute(|x::Firm.all()->filter(f|$f.name"
                        + " != $r.values.rows->size()->toString())->project([f|$f.name], ['name']), x::M,"
                        + " x::RT.runtimeValue, []); assertSize($s.values.rows, 2);}",
                // the M9b audit's F2: the same read through a value let tainted by the frame, and by an inline
                // execute
                "{| let r = " + EXECUTE + "; let n = $r.values.rows->size(); let s = meta::pure::router::execute("
                        + "|x::Firm.all()->filter(f|$f.name != $n->toString())->project([f|$f.name], ['name']), x::M,"
                        + " x::RT.runtimeValue, []); assertSize($s.values.rows, 2);}",
                "{| let n = " + EXECUTE + ".values.rows->size(); let s = meta::pure::router::execute("
                        + "|x::Firm.all()->filter(f|$f.name != $n->toString())->project([f|$f.name], ['name']), x::M,"
                        + " x::RT.runtimeValue, []); assertSize($s.values.rows, 2);}",
                // F2, through an alias and a let of a tainted let
                "{| let r = " + EXECUTE + "; let v = $r.values->at(0); let n = $v.rows->size(); let m = $n + 0;"
                        + " let s = meta::pure::router::execute(|x::Firm.all()->filter(f|$f.name != $m->toString())"
                        + "->project([f|$f.name], ['name']), x::M, x::RT.runtimeValue, []);"
                        + " assertSize($s.values.rows, 2);}",
                // the M9b audit's F1: a kind planned another way, or an execute, inside a helper the frame's query or
                // an assert calls (the rule's reach into the helper is blind to no callee: one memo per predicate)
                "{| let r = meta::pure::router::execute(|x::aFirm(), x::M, x::RT.runtimeValue, []);"
                        + " assertSize($r.values, 1);}",
                "{| let r = " + EXECUTE + "; assertEquals(1, x::oneExec()); assertSize($r.values.rows, 2);}",
                "{| let r = " + EXECUTE + "; let n = x::oneExec(); assertEquals(1, $n); assertSize($r.values.rows,"
                        + " 2);}",
                // F3: the eager inline shape bound by a let
                "{| let n = " + EXECUTE + "->size(); assertEquals(1, $n);}",
                // the M9c audit's F2: a store write inside a side runs where its side is planned, inside the walk's
                // transaction; a body it reaches stays on the walk
                "{| let r = " + EXECUTE + "; assertEquals(2, #>{x::DB.FIRM}#->filter(f|$f.ID <= 2)"
                        + "->write(#>{x::DB.FIRM}#)); assertSize($r.values.rows, 2);}",
                "{| assertEquals(2, #>{x::DB.FIRM}#->filter(f|$f.ID <= 2)->write(#>{x::DB.FIRM}#)); assertEquals(1, 2);}",
                // F3: an effect whose String argument reads a frame (not a literal), and an effect as the last
                // statement
                "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                        + "type=meta::relational::runtime::DatabaseType." + backend.type().name() + "); let r = "
                        + EXECUTE + "; " + EXEC + "'insert into FIRM values (3, \\'' + $r.values.rows->size()"
                        + "->toString() + '\\')', $c, 0, 1000); assertSize($r.values.rows, 3);}",
                "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                        + "type=meta::relational::runtime::DatabaseType." + backend.type().name() + "); let r = "
                        + EXECUTE + "; assertSize($r.values.rows, 2); " + EXEC
                        + "'insert into FIRM values (3, \\'Cyl\\')', $c, 0, 1000);}")) {
            assertNull(BodyClass.of(typed(ctx, body), "x::RT", true), body);
        }
    }

    /** M7: the assert programs under the DATABASE judge -- each assert's verdict statement deferred, the segment's
     *  fused statement composed at planning and run as one step at the segment's last assert, the rows judged by the
     *  batch's own root loop -- run both ways: the statements (the fused statement under BODY, a split-rung wrapper
     *  under FALLBACK), the verdict census, the verdicts, the answer. */
    @ParameterizedTest
    @MethodSource("backends")
    void assertProgramsUnderTheDatabaseJudge(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend, false));
        for (String body : List.of(
                // the M8b audit's should-fix 2: a FAILING negated family (the statement's constant message) and the
                // builder-folded polarities that fail (the statement's own expected/actual text), so the
                // double-negation regression class has a witness on every in-process backend
                "{| assertNotEquals(2, 1 + 1);}",
                "{| assertEmpty([1]);}",
                "{| assertFalse(1 < 2);}",
                "{| assertEquals(2, 1 + 1);}",
                "{| assertEquals(3, 1 + 1);}",
                "{| assertNotEquals(3, 1 + 1); 'after';}",
                "{| let n = 2; assertEquals(['a', 'b'], ['b', 'a']->sort()); assertEquals($n, 1 + 1);}",
                "{| assertSize([1, 2, 3], 3); assertEmpty([]); assertNotEmpty([1]); assert(1 < 2);"
                        + " assertFalse(1 > 2); true;}",
                "{| assertSameElements([1, 2], [2, 1]); assertContains([1, 2], 2);"
                        + " assertEqWithinTolerance(1.0, 1.05, 0.1);}",
                // an assert after a failing one: planned, judged after the first failure raised -- never
                "{| assertEquals(3, 1 + 1); assertEquals(2, 1 + 1);}",
                "{| assertEquals(1, 1); let x = 2; assertEquals($x, 2); 'done';}",
                // a relation's toString against a literal: a string equality the database judges (not the rendered
                // form the host reads: that is toCSV's, pinned walked below)
                "{| assertEquals('a\n1\n', #TDS\na\n1\n#->toString());}")) {
            assertParity(backend, ctx, RT, body, "select 1", List.of(), ExecuteOptions.JudgeMode.DATABASE);
        }
    }

    /** M7: the assert shapes whose DATABASE arm reads a side on the host at the assert's turn ({@code assertEq} over
     *  a class-kind side; since M10c item 2 a grid pair whose declared schemas differ -- in a width, a column's name
     *  or a column's declared kind -- which the host grid owner judges from the grids where the SQL form compares the
     *  row canons alone): the class admits the body and the database planner leaves it on the walk by its static
     *  gate, before any side is read; the grid owner's verdicts pinned. */
    @ParameterizedTest
    @MethodSource("backends")
    void theAssertShapesTheDatabasePlannerLeavesOnTheWalk(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        for (String body : List.of(
                "{| assertEq(^x::Firm(name='a'), ^x::Firm(name='a'));}",
                // equal cells under another column name: the names decide (false)
                "{| assertEquals(#TDS\na\n1\n#, #TDS\nb\n1\n#);}",
                // two empty grids of different widths: the widths decide (false)
                "{| assertEquals(#TDS\na\n1\n#->filter(r|$r.a > 5), #TDS\na,b\n1,x\n#->filter(r|$r.a > 5));}",
                // a Float-declared expected against an Integer-declared actual: the expected's kinds type both
                "{| assertEquals(#TDS\na\n1.5\n#, #TDS\na\n1\n#);}")) {
            assertEquals(BodyClass.ASSERT_PROGRAM, BodyClass.of(typed(ctx, body), "x::RT", true), body);
            Run walked = run(backend, ctx, "x::RT", body, false, "select 1", List.of(),
                    ExecuteOptions.JudgeMode.DATABASE);
            Run routed = run(backend, ctx, "x::RT", body, true, "select 1", List.of(),
                    ExecuteOptions.JudgeMode.DATABASE);
            assertEquals(walked.moved, routed.moved, "the router left the body on the walk: " + body);
            assertEquals(walked.outcome, routed.outcome, "outcome: " + body);
            assertEquals(walked.verdicts, routed.verdicts, "the verdicts: " + body);
            assertEquals(walked.census, routed.census, "the verdict census: " + body);
            assertEquals(walked.origins, routed.origins, "the statement-origin census (nothing read at planning): "
                    + body);
            if (body.contains("#TDS")) {
                assertTrue(walked.outcome.startsWith("AssertFailed"), "the grid owner's verdict: " + body + " -> "
                        + walked.outcome);
            }
        }
    }

    /** M10c item 2: the GRID PAIR (two relation-typed sides) and the RENDERED TEXT (a toCSV render of a grid against
     *  its golden) under the DATABASE judge are rows of the fused statement -- the grid pair {@code VerdictSql.gridPair}
     *  over the two planned grids (a row multiset unless the actual side is sorted: the host grid owner's own rule),
     *  the rendered text the golden brought to rows -- so the database planner admits a body whose grids' schemas are
     *  declared at planning; the outcomes pinned absolutely, the same on both roads. */
    @ParameterizedTest
    @MethodSource("backends")
    void gridPairsAndRenderedTextsUnderTheDatabaseJudge(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        String grid = "#TDS\na,b\n1,x\n2,y\n#";
        String csv = "->meta::relational::tests::csv::toCSV()";
        for (var c : List.of(
                java.util.Map.entry("{| assertEquals(" + grid + "->filter(r|$r.a > 1), " + grid
                        + "->filter(r|$r.a > 1));}", "ok"),
                java.util.Map.entry("{| assertEquals(" + grid + "->filter(r|$r.a > 1), " + grid
                        + "->filter(r|$r.a > 0));}", "AssertFailed"),
                java.util.Map.entry("{| assertNotEquals(" + grid + "->filter(r|$r.a > 1), " + grid
                        + "->filter(r|$r.a > 0));}", "ok"),
                // a defined order on both sides: the rows a multiset (the actual side is not sorted)
                java.util.Map.entry("{| assertEquals(#TDS\na,b\n2,y\n1,x\n#, " + grid + ");}", "ok"),
                // the actual side sorted: the rows compared in order
                java.util.Map.entry("{| assertEquals(#TDS\na,b\n2,y\n1,x\n#, " + grid + "->sort(asc('a')));}",
                        "AssertFailed"),
                // the rendered text against its golden, equal and not
                java.util.Map.entry("{| assertEquals('a\n1\n', #TDS\na\n1\n#" + csv + ");}", "ok"),
                java.util.Map.entry("{| assertEquals('a\n2\n', #TDS\na\n1\n#" + csv + ");}", "AssertFailed"),
                // a frame's grid rendered
                java.util.Map.entry("{| let r = " + EXECUTE + "; assertEquals('name\nAcme\nBolt\n',"
                        + " $r.values->toOne()" + csv + ");}", "ok"),
                // the rendered text under assertSameElements: the lines a multiset
                java.util.Map.entry("{| assertSameElements('a\n2\n1\n', #TDS\na\n1\n2\n#" + csv + ");}", "ok"))) {
            Run walked = assertParity(backend, ctx, "x::RT", c.getKey(), "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.DATABASE);
            assertTrue(walked.outcome.startsWith(c.getValue()), c.getKey() + " -> " + walked.outcome);
        }
    }

    /** M10c item 3: a LINEAGE TREE PRINT against its golden ({@code $scan->relationTreeAsString()} under
     *  {@code assertEquals}) is judged as LINES on both roads (the golden's lines a String collection, ours the prelude's
     *  rows over the scan's handle); the scan is a read of the system store, so a body whose only store read is the scan
     *  is system-routed and its program runs on the graph's system database under both judges. The golden is read off
     *  a body answering the print. */
    @ParameterizedTest
    @MethodSource("backends")
    void lineagePrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        // the system database seeded once per graph (systemRoutedPrograms)
        run(backend, ctx, "x::RT", "{| assertEquals('M', x::M.name);}", false, "select count(*) from FIRM", FIRM_ROWS);
        String scan = "let s = meta::pure::lineage::scanRelations::scanRelations({|x::Firm.all()->project([f|$f.name],"
                + " ['name'])}, x::M, []);";
        String print = "$s->meta::pure::lineage::scanRelations::relationTreeAsString()";
        String golden = text(run(backend, ctx, "x::RT", "{| " + scan + " " + print + ";}", false,
                "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        assertTrue(golden.startsWith("root\n") && golden.contains("------> (t) FIRM"), golden);
        String held = "{| " + scan + " assertEquals('" + pure(golden) + "', " + print + ");}";
        String failed = "{| " + scan + " assertEquals('" + pure(golden.replace("FIRM", "FIRMS")) + "', " + print + ");}";
        // an ordinary equality over the scan's rows (no print): the corpus's scanColumns bodies' shape
        String rows = "{| " + scan + " assertEquals(['FIRM'], $s.nodes->filter(n|$n.kind == 't').name);}";
        // the corpus's scanColumns chain (scanColumnsTests.pure: scanProperties -> buildPropertyTree -> scanColumns),
        // its golden read off a run
        String columns = "let p = {|x::Firm.all()->project([f|$f.name], ['name'])}.expressionSequence->at(0)"
                + "->evaluateAndDeactivate()->meta::pure::lineage::scanProperties::scanProperties("
                + "^meta::pure::functions::collection::List<meta::pure::lineage::scanProperties::PropertyPathNode>(), [], []);"
                + " let tree = $p.result->meta::pure::lineage::scanProperties::propertyTree::buildPropertyTree();"
                + " let f = $tree->meta::pure::lineage::scanColumns::scanColumns(x::M)->removeDuplicates();";
        String columnTexts = "$f->map(t|$t.column.owner->cast(@meta::relational::metamodel::relation::Table).name->toOne()"
                + " + '.' + $t.column.name->toOne() + ' <' + $t.context + '>')->sort()";
        String columnGolden = text(run(backend, ctx, "x::RT", "{| " + columns + " " + columnTexts
                + "->joinStrings(', ');}", false, "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        assertTrue(columnGolden.contains("FIRM.NAME"), columnGolden);
        StringBuilder goldenList = new StringBuilder("[");
        for (String one : columnGolden.split(", ")) {
            goldenList.append(goldenList.length() > 1 ? ", '" : "'").append(pure(one)).append("'");
        }
        String scanColumns = "{| " + columns + " assertEquals(" + goldenList + "], " + columnTexts + ");}";
        assertTrue(BodyClass.systemRouted(typed(ctx, held), "x::RT"), "system-routed: " + held);
        assertTrue(BodyClass.systemRouted(typed(ctx, scanColumns), "x::RT"), "system-routed: " + scanColumns);
        for (ExecuteOptions.JudgeMode mode : List.of(ExecuteOptions.JudgeMode.HOST, ExecuteOptions.JudgeMode.DATABASE)) {
            for (var c : List.of(java.util.Map.entry(held, "ok"), java.util.Map.entry(failed, "AssertFailed"),
                    java.util.Map.entry(rows, "ok"), java.util.Map.entry(scanColumns, "ok"))) {
                // the walk evaluates the scan's let on the body's session (one SESSION statement to set it up) and
                // routes the assert's side to the system session; the program has ONE target, the system database --
                // the one difference M10a makes for a body (systemRoutedPrograms' store-free case): every outcome,
                // verdict, answer and statement text the same
                Run walked = run(backend, ctx, "x::RT", c.getKey(), false, "select count(*) from FIRM", FIRM_ROWS, mode);
                Run moved = run(backend, ctx, "x::RT", c.getKey(), true, "select count(*) from FIRM", FIRM_ROWS, mode);
                assertTrue(walked.outcome.startsWith(c.getValue()), c.getKey() + " -> " + walked.outcome);
                assertEquals(walked.outcome, moved.outcome, "outcome: " + c.getKey());
                assertEquals(walked.answer, moved.answer, "the answer: " + c.getKey());
                assertEquals(walked.verdicts, moved.verdicts, "the verdicts: " + c.getKey());
                assertEquals(walked.census, moved.census, "the verdict census: " + c.getKey());
                assertEquals(walked.ledger, moved.ledger, "the raw-SQL ledger: " + c.getKey());
                assertEquals(walked.moved + 1, moved.moved, "the router moved the body: " + c.getKey());
                long[] w = originsOf(walked.origins);
                long[] m = originsOf(moved.origins);
                for (int i = 0; i < w.length; i++) {
                    long allowed = i == StatementOrigin.SESSION.ordinal() ? 1 : 0;
                    assertTrue(w[i] - m[i] >= 0 && w[i] - m[i] <= allowed, "the statement-origin census differs beyond"
                            + " the body session's setup: " + c.getKey() + "\n  walked: " + walked.statements
                            + "\n  moved:  " + moved.statements);
                }
                List<String> walkedSql = new ArrayList<>(walked.statements);
                walkedSql.removeIf(s -> s.startsWith("[session]"));
                assertEquals(walkedSql, moved.statements, "the statements sent, the body session's setup aside: "
                        + c.getKey());
            }
        }
    }

    /** M10c item 4: a USER-WRITTEN ASSERT HELPER at the root ({@code x::assertSame(expected, actual)}: lets, then an
     *  assert over class values, as toPostgresModel's {@code assertConversion}) is inlined and its reduced root planned
     *  as the assert it is on both roads, under that assert's name -- the walk's own rule (a class value asserted or a
     *  string entry read); the outcomes pinned absolutely under both judges. */
    @ParameterizedTest
    @MethodSource("backends")
    void helperAssertPrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        for (ExecuteOptions.JudgeMode mode : List.of(ExecuteOptions.JudgeMode.HOST, ExecuteOptions.JudgeMode.DATABASE)) {
            for (var c : List.of(
                    // the keys agree (v is no key): held
                    java.util.Map.entry("{| x::assertSame(^x::Key(k='a', v=1), ^x::Key(k='a', v=2));}", "ok"),
                    java.util.Map.entry("{| x::assertSame(^x::Key(k='a', v=1), ^x::Key(k='b', v=1));}", "AssertFailed"),
                    // after a let the helper reads
                    java.util.Map.entry("{| let e = ^x::Key(k='c', v=3); x::assertSame($e, ^x::Key(k='c', v=4));}",
                            "ok"))) {
                assertEquals(BodyClass.ASSERT_PROGRAM, BodyClass.of(typed(ctx, c.getKey()), "x::RT", true), c.getKey());
                Run walked = run(backend, ctx, "x::RT", c.getKey(), false, "select count(*) from FIRM", FIRM_ROWS, mode);
                if (walked.outcome.startsWith("DialectCapability")) {
                    // a keyed instance's canon needs the dialect's struct and list encodings: a dialect without them
                    // walls the pair on both roads alike (the walk's own answer, raised by the root loop at the flush).
                    // Under the DATABASE judge the planning pass defers the row (nothing rendered: PLANNED, the family
                    // counted), then the segment's close finds no member the dialect spells (VerdictBatch.plan's split
                    // rung catches the DialectCapability; DatabaseAsserts.close: no step) and gives the body back to
                    // the walk, which counts the family again -- M8's note in close, carried as a step by M10d. The
                    // outcome and the verdicts are the walk's. Pinned as that until M10d.
                    Run moved = run(backend, ctx, "x::RT", c.getKey(), true, "select count(*) from FIRM", FIRM_ROWS,
                            mode);
                    assertEquals(walked.outcome, moved.outcome, "outcome: " + c.getKey());
                    assertEquals(walked.verdicts, moved.verdicts, "the verdicts: " + c.getKey());
                    // the host road carries the wall as its step's raise (moved); the database road gives the body
                    // back (not moved)
                    assertEquals(mode == ExecuteOptions.JudgeMode.DATABASE ? walked.moved : walked.moved + 1,
                            moved.moved, "the body's road under the wall: " + c.getKey());
                    continue;
                }
                if (mode == ExecuteOptions.JudgeMode.DATABASE && c.getValue().equals("AssertFailed")) {
                    // the walk's own quirk for a helper whose deferred verdict fails: prepareValue answers the inlined
                    // assert's value (true) as the statement's value before the batch's flush judges it, where a
                    // statement-root assert answers nothing on a failure -- the program road answers nothing, as for
                    // every failing assert; every other boundary the same (the walk's deletion removes the quirk)
                    Run moved = run(backend, ctx, "x::RT", c.getKey(), true, "select count(*) from FIRM", FIRM_ROWS,
                            mode);
                    assertEquals(walked.outcome, moved.outcome, "outcome: " + c.getKey());
                    assertTrue(walked.outcome.startsWith("AssertFailed"), c.getKey() + " -> " + walked.outcome);
                    assertEquals(walked.verdicts, moved.verdicts, "the verdicts: " + c.getKey());
                    assertEquals(walked.census, moved.census, "the verdict census: " + c.getKey());
                    assertEquals(walked.origins, moved.origins, "the statement-origin census: " + c.getKey());
                    assertEquals(walked.moved + 1, moved.moved, "the router moved the body: " + c.getKey());
                    continue;
                }
                walked = assertParity(backend, ctx, "x::RT", c.getKey(), "select count(*) from FIRM", FIRM_ROWS, mode);
                assertTrue(walked.outcome.startsWith(c.getValue()), c.getKey() + " -> " + walked.outcome);
                if (backend.type().name().equals("DuckDB")) {
                    assertTrue(walked.outcome.startsWith(c.getValue()), "DuckDB judges: " + c.getKey() + " -> "
                            + walked.outcome);
                }
            }
        }
    }

    /** M10c item 5: the QUANTIFIED asserts on both roads -- the UNROLL form ({@code xs->map(x| ...asserts...)} over a
     *  literal collection, lets in the lambda or any assert family; {@code forAll} the same) one verdict step per
     *  element at the statement's place, each the walk's reduced statement planned as a root; the VECTOR forms
     *  ({@code xs->map(x|assert(pred, 'msg'))}: the predicate vector the database computes, every row the wanted
     *  truth) one step over all rows; under both judges, the outcomes pinned absolutely. */
    @ParameterizedTest
    @MethodSource("backends")
    void quantifiedPrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(model(backend, false));
        for (ExecuteOptions.JudgeMode mode : List.of(ExecuteOptions.JudgeMode.HOST, ExecuteOptions.JudgeMode.DATABASE)) {
            for (var c : List.of(
                    // the unroll: an assert family that is no simple predicate
                    java.util.Map.entry("{| [1, 2]->map(x|assertEquals($x, $x)); true;}", "ok"),
                    // the unroll with lets in the lambda
                    java.util.Map.entry("{| ['a', 'b']->map(s| let u = $s->toUpper(); assertEquals($u, $s->toUpper()););"
                            + " true;}", "ok"),
                    // the corpus's pairs (testCbrt, testPad): the element a pair, its sides read through the lambda
                    java.util.Map.entry("{| [pair('a', 'A'), pair('b', 'B')]->map(p| assertEquals($p.second,"
                            + " $p.first->toUpper()));}", "ok"),
                    java.util.Map.entry("{| [pair('a', 'A'), pair('b', 'X')]->map(p| assertEquals($p.second,"
                            + " $p.first->toUpper()));}", "AssertFailed"),
                    // forAll over asserts is the same unroll
                    java.util.Map.entry("{| [1, 2]->forAll(x| assertEquals($x, $x));}", "ok"),
                    // the vector form: the simple predicate with a literal message, judged over every row
                    java.util.Map.entry("{| [1, 2, 3]->map(x| assert($x > 0, 'positive'));}", "ok"),
                    java.util.Map.entry("{| [1, -2]->map(x| assert($x > 0, 'positive'));}", "AssertFailed"),
                    java.util.Map.entry("{| [1, 2]->map(x| assertFalse($x > 5, 'small'));}", "ok"),
                    // the forAll spelling of the simple predicate: the vector form too (the audit: it walled before)
                    java.util.Map.entry("{| [1, 2]->forAll(x| assert($x > 0, 'positive'));}", "ok"),
                    java.util.Map.entry("{| [1, -2]->forAll(x| assert($x > 0, 'positive'));}", "AssertFailed"),
                    // the vector contract over a row source, assertFalse: the vector carries the polarity (the audit:
                    // the host's own arm inverted it)
                    java.util.Map.entry("{| #TDS\nx\n1\n2\n#.rows->forAll(r| assertFalse($r.getInteger('x') > 5,"
                            + " 'small'));}", "ok"),
                    java.util.Map.entry("{| #TDS\nx\n1\n9\n#.rows->forAll(r| assertFalse($r.getInteger('x') > 5,"
                            + " 'small'));}", "AssertFailed"),
                    java.util.Map.entry("{| #TDS\nx\n1\n9\n#.rows->forAll(r| assert($r.getInteger('x') > 0,"
                            + " 'positive'));}", "ok"))) {
                assertEquals(BodyClass.ASSERT_PROGRAM, BodyClass.of(typed(ctx, c.getKey()), RT, true), c.getKey());
                Run walked = run(backend, ctx, RT, c.getKey(), false, "select 1", List.of(), mode);
                if (walked.outcome.startsWith("DialectCapability")) {
                    // the vector form's predicate vector unnests the collection: a dialect without an unnest placement
                    // walls it on both roads alike (the walk's own answer). The host road carries the raise as its
                    // step (moved); under the DATABASE judge the arm raises at planning as the first root of its
                    // segment and the body goes back to the walk after the planning pass counted it (M7's gap, item 4's
                    // (i); M10d carries it as the step). The outcome and the verdicts are the walk's.
                    Run moved = run(backend, ctx, RT, c.getKey(), true, "select 1", List.of(), mode);
                    assertEquals(walked.outcome, moved.outcome, "outcome: " + c.getKey());
                    assertEquals(walked.verdicts, moved.verdicts, "the verdicts: " + c.getKey());
                    assertEquals(mode == ExecuteOptions.JudgeMode.DATABASE ? walked.moved : walked.moved + 1,
                            moved.moved, "the body's road under the wall: " + c.getKey());
                    continue;
                }
                walked = assertParity(backend, ctx, RT, c.getKey(), "select 1", List.of(), mode);
                assertTrue(walked.outcome.startsWith(c.getValue()), c.getKey() + " -> " + walked.outcome);
            }
        }
    }

    /** M10c item 7: a let whose only reads of the system store are ELEMENT REFERENCES (the corpus's
     *  {@code let runtime = testRuntime()}: a store named inside a Runtime construction the helper builds; an inline
     *  literal; the mapping as a let too), consumed by the execute's orchestration argument, is no read: the body is
     *  not system-routed, it moves (the corpus's {@code .values} shape an assert program, the frame shape a frame
     *  program) and sends nothing to the system session on either road. A body with no user read is not system-routed
     *  by such a let either. A NAVIGATION of the element bound ({@code x::DB.schemas.tables.name}: a read of its row)
     *  is a system read even when the let is consumed inside the frame's lambda -- a system read beside a user read,
     *  the walk's (two targets; the audit's find). */
    @ParameterizedTest
    @MethodSource("backends")
    void consumedElementReferencePrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        String query = "|x::Firm.all()->project([f|$f.name], ['name'])";
        String inlineRuntime = "^meta::core::runtime::Runtime(connectionStores = ^meta::core::runtime::ConnectionStore("
                + "element = x::DB, connection = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                + "type = meta::relational::runtime::DatabaseType." + backend.type().name() + ")))";
        // the corpus's shape: the result's values bound (an assert program's value let)
        List<String> valueShapes = List.of(
                "{| let runtime = x::testRuntime(); let result = meta::pure::router::execute(" + query + ", x::M,"
                        + " $runtime, []).values; assertEquals(['Acme', 'Bolt'], $result.rows.values);}",
                "{| let runtime = " + inlineRuntime + "; let result = meta::pure::router::execute(" + query + ", x::M,"
                        + " $runtime, []).values; assertEquals(['Acme', 'Bolt'], $result.rows.values);}",
                // the mapping as a let too, as the corpus spells it
                "{| let mapping = x::M; let runtime = x::testRuntime(); let result = meta::pure::router::execute(" + query
                        + ", $mapping, $runtime, []).values; assertEquals(2, $result.rows->size());}",
                // item 7b: the runtime COPIED through a chain of lets (the XStore tests' idiom): a let read only by a
                // consumed let is consumed
                "{| let r0 = x::testRuntime(); let r = ^$r0(connectionStores = $r0.connectionStores);"
                        + " let result = meta::pure::router::execute(" + query + ", x::M, $r, []).values;"
                        + " assertEquals(['Acme', 'Bolt'], $result.rows.values);}",
                // the post-processor tests' idiom: the connection navigated out of the runtime and copied back in
                "{| let r0 = x::testRuntime(); let cs = $r0.connectionStores->at(0); let c = $cs.connection->cast("
                        + "@meta::external::store::relational::runtime::TestDatabaseConnection);"
                        + " let r = ^$r0(connectionStores = ^$cs(connection = $c));"
                        + " let result = meta::pure::router::execute(" + query + ", x::M, $r, []).values;"
                        + " assertEquals(['Acme', 'Bolt'], $result.rows.values);}");
        // the frame shape (a frame program)
        List<String> frameShapes = List.of(
                "{| let runtime = x::testRuntime(); let r = meta::pure::router::execute(" + query + ", x::M, $runtime,"
                        + " []); assertEquals(['Acme', 'Bolt'], $r.values.rows.values);}");
        for (BodyClass expected : List.of(BodyClass.ASSERT_PROGRAM, BodyClass.FRAME_PROGRAM)) {
            for (String body : expected == BodyClass.ASSERT_PROGRAM ? valueShapes : frameShapes) {
                assertFalse(BodyClass.systemRouted(typed(ctx, body), "x::RT"), "no system read: " + body);
                assertEquals(expected, BodyClass.of(typed(ctx, body), "x::RT", true), body);
                for (ExecuteOptions.JudgeMode mode : List.of(ExecuteOptions.JudgeMode.HOST,
                        ExecuteOptions.JudgeMode.DATABASE)) {
                    Run walked = assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS, mode);
                    assertEquals("ok", walked.outcome, body);
                    assertEquals(0, originsOf(walked.origins)[StatementOrigin.SYSTEM.ordinal()],
                            "nothing sent to the system session: " + body);
                }
            }
        }
        // no user read at all: the element references alone read nothing, so the let routes the body nowhere
        String none = "{| let runtime = x::testRuntime(); assertEquals(true, true);}";
        assertFalse(BodyClass.systemRouted(typed(ctx, none), "x::RT"), "an element reference routes nothing: " + none);
        // the chain's end read as a VALUE: the chain is evaluated, so the element it carries is a read (item 7b)
        String valued = "{| let r0 = x::testRuntime(); let r = ^$r0(connectionStores = $r0.connectionStores);"
                + " assertEquals(1, $r.connectionStores->size());}";
        assertTrue(BodyClass.systemRouted(typed(ctx, valued), "x::RT"), "a chain read as a value: " + valued);
        // item 7b's audit (HIGH-1): a let that only HANDS the runtime to a planning-time native carries nothing -- the
        // plan read as a value is no read of the store, and the body is not system-routed by it
        String handed = "{| let rt = x::testRuntime(); let p = meta::pure::executionPlan::executionPlan(" + query
                + ", x::M, $rt, []); assertNotEmpty($p.rootExecutionNode.executionNodes);}";
        assertFalse(BodyClass.systemRouted(typed(ctx, handed), "x::RT"), "a handed element routes nothing: " + handed);
        // (the body stays on the walk for its plan-object assert, the design note's F5 -- not for a system read)
        // the element's navigation (a read of its row) consumed inside the frame's lambda: a system read beside the
        // user read -- two targets, no class (BodyClass.elementNavigation)
        String navigation = "{| let t = x::DB.schemas.tables.name; let r = meta::pure::router::execute(|x::Firm.all()"
                + "->filter(f|$t->contains($f.name))->project([f|$f.name], ['name']), x::M, x::RT.runtimeValue, []);"
                + " assertEquals([], $r.values.rows.values);}";
        assertNull(BodyClass.of(typed(ctx, navigation), "x::RT", true), "a navigation is a read: " + navigation);
        // a real metamodel read beside the frame, each assert reading one store: the system assert on the graph's
        // system database, the frame's on the body's -- one program under the host judge (per-statement routing,
        // 2026-11-03); the database judge's groups on two sessions keep it on the walk, by name
        String mixed = "{| let r = meta::pure::router::execute(" + query + ", x::M, x::RT.runtimeValue, []);"
                + " assertEquals(['FIRM', 'TFIRM'], x::DB.schemas.tables.name->sort());"
                + " assertEquals(['Acme', 'Bolt'], $r.values.rows.values);}";
        assertMixedParity(backend, ctx, "x::RT", mixed, "select count(*) from FIRM", FIRM_ROWS);
        Run walked = run(backend, ctx, "x::RT", mixed, false, "select count(*) from FIRM", FIRM_ROWS,
                ExecuteOptions.JudgeMode.DATABASE);
        Run routed = run(backend, ctx, "x::RT", mixed, true, "select count(*) from FIRM", FIRM_ROWS,
                ExecuteOptions.JudgeMode.DATABASE);
        assertEquals(walked.moved, routed.moved, "under the database judge the mixed body stays on the walk: " + mixed);
        assertEquals(walked.outcome, routed.outcome, "outcome: " + mixed);
        assertEquals("ok", walked.outcome, mixed);
    }

    /** M10c item 7c: the corpus's setup helper -- a seed form mapped over executeInDb, unfolded into the body by the
     *  resolver's statement inliner -- planned as the effect script on the program road; the seed drops and recreates
     *  the table, so the rows are the CSV's. A data argument that does not fold stays on the walk. */
    @ParameterizedTest
    @MethodSource("backends")
    void seedFormEffectMapPrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        String body = "{| let firmCsv = 'default\\nFIRM\\nID,NAME\\n3,Cyl\\n'; let db = x::DB;"
                + " let runtime = x::testRuntimeFor($db); x::setupData([$firmCsv], $db, $runtime);"
                + " let result = " + EXECUTE.replace("x::RT.runtimeValue", "$runtime") + ";"
                + " assertEquals(['Cyl'], $result.values.rows.values);}";
        assertEquals(BodyClass.FRAME_PROGRAM, BodyClass.of(typed(ctx, body), "x::RT", true), body);
        for (ExecuteOptions.JudgeMode mode : List.of(ExecuteOptions.JudgeMode.HOST, ExecuteOptions.JudgeMode.DATABASE)) {
            Run walked = assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS, mode);
            assertEquals("ok", walked.outcome, body);
            assertEquals(0, originsOf(walked.origins)[StatementOrigin.SYSTEM.ordinal()],
                    "nothing sent to the system session: " + body);
        }
        // the data computed: the walk evaluates it as a statement; the program road refuses by name, so the body stays
        String computed = body.replace("[$firmCsv]", "[$firmCsv->toUpper()]");
        assertNull(BodyClass.of(typed(ctx, computed), "x::RT", true), "a computed seed stays: " + computed);
        // the map as the body's LAST statement, with no assert: an effect program whose answer is the last effect's
        String seedOnly = "{| let firmCsv = 'default\\nFIRM\\nID,NAME\\n3,Cyl\\n'; let db = x::DB;"
                + " let runtime = x::testRuntimeFor($db); x::seedOnly([$firmCsv], $db, $runtime);}";
        assertEquals(BodyClass.EFFECT_PROGRAM, BodyClass.of(typed(ctx, seedOnly), "x::RT", true), seedOnly);
        assertParity(backend, ctx, "x::RT", seedOnly, "select count(*) from FIRM", FIRM_ROWS,
                ExecuteOptions.JudgeMode.HOST);
    }

    /** M7: the frame programs under the DATABASE judge: a frame is never run at its let -- the asserts' sides reference
     *  its CTE inside the segment's fused statement, whose stamp is the frame's activity comment (a parameter the
     *  fused step binds; the walk's reserved comment) -- an effect between two asserts closes a segment. */
    @ParameterizedTest
    @MethodSource("backends")
    void framesUnderTheDatabaseJudge(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        for (String body : List.of(
                "{| let r = " + EXECUTE + "; assertEquals(['Acme', 'Bolt'], $r.values.rows.values);}",
                "{| let r = " + EXECUTE + "; assertSize($r.values.rows, 2); let n = $r.values.rows->size();"
                        + " assertEquals(2, $n); true;}",
                "{| let r = " + EXECUTE + "; let s = " + EXECUTE + "; assertEquals($r.values.rows->size(),"
                        + " $s.values.rows->size());}",
                "{| assertEquals(1, 1); let r = " + EXECUTE + "; assertEquals(['Acme'], $r.values.rows.values);}",
                "{| let r = " + EXECUTE + "; let v = $r.values->at(0); assertSize($v.rows, 2);"
                        + " assertEquals(['Acme', 'Bolt'], $v.rows.values);}",
                "{| assertSize(" + EXECUTE + ".values.rows, 2);}",
                // an effect between two asserts: two segments, each its fused statement, the effect's script between
                "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                        + "type=meta::relational::runtime::DatabaseType." + backend.type().name() + "); let r = "
                        + EXECUTE + "; assertSize($r.values.rows, 2); " + EXEC
                        + "'insert into FIRM values (3, \\'Cyl\\')', $c, 0, 1000); assertSize($r.values.rows, 3);}",
                "{| let r = " + EXECUTE_CLASS + "; assertSize($r.values, 2); assertEquals(['Acme', 'Bolt'],"
                        + " $r.values.name);}",
                // the activities: the SQL text, and the comment (the fused statement's own stamp)
                "{| let r = " + EXECUTE + "; assertSize($r.activities, 1); assert($r.activities->at(0)->cast("
                        + "@meta::relational::mapping::RelationalActivity).sql->toOne()->startsWith('select'));}",
                "{| let r = " + EXECUTE + "; assert($r.activities->at(0)->cast(@meta::relational::mapping::"
                        + "RelationalActivity).comment->toOne()->startsWith('-- \"executionTraceID\"'));"
                        + " assertEquals(62, $r.activities->at(0)->cast(@meta::relational::mapping::RelationalActivity)"
                        + ".comment->toOne()->length());}",
                "{| let p = meta::pure::executionPlan::executionPlan(|x::Firm.all()->project([f|$f.name], ['name']),"
                        + " x::M, x::RT.runtimeValue, []); assertNotEmpty($p.rootExecutionNode.executionNodes);}",
                "{| let r = " + EXECUTE_TEMPORAL + "; assertSize($r.values.rows, 1); assertEquals(['New'],"
                        + " $r.values.rows.values);}")) {
            assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.DATABASE);
        }
    }

    /** M10a: a SYSTEM-ROUTED assert program -- every store it reads is the graph's system metamodel store (an
     *  element's row, a mapping's class mappings, a store's tables), no user store -- runs its program on the graph's
     *  system database, the session the walk routes each of its sides to; under both judges. A body mixing a user
     *  store's read with a system read stays on the walk (two targets), by name. */
    @ParameterizedTest
    @MethodSource("backends")
    void systemRoutedPrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        // the graph's system database is opened and seeded ONCE per graph by whichever road first touches it (the
        // walk's route and the router's program share the one SystemDatabase): seeded here first, so neither run of a
        // pair carries the seeding's statements in its census -- a run-order fact, not a road difference
        run(backend, ctx, "x::RT", "{| assertEquals('M', x::M.name);}", false, "select count(*) from FIRM", FIRM_ROWS);
        for (String body : List.of(
                "{| assertEquals('M', x::M.name);}",
                "{| let m = x::M; assertEquals('M', $m.name); assertSize($m.classMappings, 2);}",
                "{| assertEquals(['FIRM', 'TFIRM'], x::DB.schemas.tables.name->sort());}",
                "{| assertEquals(1, x::DB.schemas->size()); 'done';}")) {
            assertTrue(BodyClass.systemRouted(typed(ctx, body), "x::RT"), "system-routed: " + body);
            assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.HOST);
            assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.DATABASE);
        }
        // a store-free assert beside a system read: the program has ONE target, so its statement runs on the system
        // session where the walk ran it on the body's -- the one difference M10a makes for a body (the M10a audit's A):
        // every outcome, verdict and answer the same; the walk alone sets the body session up (one SESSION statement),
        // and under the database judge it fuses the two asserts into two statements (one per session) where the
        // program fuses them into one
        String storeFree = "{| assertEquals('ABC', 'abc'->toUpper()); assertEquals('M', x::M.name);}";
        assertTrue(BodyClass.systemRouted(typed(ctx, storeFree), "x::RT"));
        for (ExecuteOptions.JudgeMode mode : List.of(ExecuteOptions.JudgeMode.HOST, ExecuteOptions.JudgeMode.DATABASE)) {
            Run walked = run(backend, ctx, "x::RT", storeFree, false, "select count(*) from FIRM", FIRM_ROWS, mode);
            Run moved = run(backend, ctx, "x::RT", storeFree, true, "select count(*) from FIRM", FIRM_ROWS, mode);
            assertEquals(walked.outcome, moved.outcome, "outcome: " + storeFree);
            assertEquals(walked.answer, moved.answer, "the answer: " + storeFree);
            assertEquals(walked.verdicts, moved.verdicts, "the verdicts: " + storeFree);
            assertEquals(walked.census, moved.census, "the verdict census: " + storeFree);
            assertEquals(walked.database, moved.database, "the database left: " + storeFree);
            long[] w = originsOf(walked.origins);
            long[] m = originsOf(moved.origins);
            for (int i = 0; i < w.length; i++) {
                long allowed = i == StatementOrigin.BODY.ordinal() || i == StatementOrigin.SESSION.ordinal() ? 1 : 0;
                assertTrue(w[i] - m[i] >= 0 && w[i] - m[i] <= allowed, "the statement-origin census differs beyond"
                        + " the body session's setup and the per-session fusion: " + storeFree + "\n  walked: "
                        + walked.statements + "\n  moved:  " + moved.statements);
            }
        }
        // a user store's read beside a system read, each in a statement of its own (per-statement routing,
        // 2026-11-03): the system statement on the graph's system database, the user's on the body's, one program --
        // under the host judge (the database judge's groups on two sessions keep such a body on the walk, by name);
        // and a query over the system store alone, its one statement on the system database
        for (String body : List.of(
                "{| assertEquals('M', x::M.name); assertSize(x::Firm.all(), 2);}",
                "{| let r = " + EXECUTE + "; let m = x::M; assertEquals('M', $m.name); assertSize($r.values.rows, 2);}",
                "{| let r = " + EXECUTE + "; assertEquals('M', x::M.name); assertSize($r.values.rows, 2);}",
                "{| meta::pure::mapping::Mapping.all()->project(~[n:m|$m.name]);}")) {
            assertMixedParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS);
        }
        // a frame whose query reads both stores once its let is spliced: one statement over two databases, the walk's
        String bothInOne = "{| let t = x::DB.schemas.tables.name; let r = " + EXECUTE.replace(
                "|x::Firm.all()->project", "|x::Firm.all()->filter(f|$t->contains($f.name))->project")
                + "; assertEquals([], $r.values.rows.values);}";
        assertNull(BodyClass.of(typed(ctx, bothInOne), "x::RT", true), bothInOne);
        // the test-data-generation lane's census carrier (getRelationalCSVDataFromQuery): the walk folds it to
        // instance literals in Java from the model, the program road has no fold -- the walk's, by name (M10a's third
        // cut: the corpus's three census tests moved on the strength of their system reads alone and walled at
        // lowering); in the corpus's shape: the let-bound mapping is consumed by the carrier (the carrier holds it as a
        // name), so since M10b's let rule the body reads no store at all and is not system-routed; the carrier's kind
        // alone keeps it on the walk
        String censusBody = "{| let query = {|x::Firm.all()->project([f|$f.name], ['n'])}; let mapping = x::M;"
                + " let d = meta::relational::testDataGeneration::getRelationalCSVDataFromQuery($query, $mapping);"
                + " assertEquals(1, $d.tables->size()); assertEquals('FIRM', $d.tables->at(0).table);}";
        assertFalse(BodyClass.systemRouted(typed(ctx, censusBody), "x::RT"), "a binding consumed: " + censusBody);
        assertNull(BodyClass.of(typed(ctx, censusBody), "x::RT", true), "the census carrier stays on the walk");
        assertEquals("ok", run(backend, ctx, "x::RT", censusBody, false, "select count(*) from FIRM", FIRM_ROWS,
                ExecuteOptions.JudgeMode.HOST).outcome, "the census body on the walk");
    }

    /** M10b: the ENGINE-TEXT asserts on the program road -- the two texts a verdict step's constant sides, a byte-equal
     *  text the verdict (the user's ruling of 2026-09-21), the appeal (the rows leg: our rows through the plan runner,
     *  the golden replayed on the referee) the harness's at the step on a failed text alone, as the walk decides them
     *  -- under both judges: the text equal, the text differing with the rows agreeing (MATCH), the rows differing
     *  (DIVERGED: the arm's own message), a foreign dialect (declined: the text is the contract), a frame's executed
     *  SQL through the envelope's read under the three spellings. The goldens are built from our own texts, read off
     *  bodies answering them. */
    @ParameterizedTest
    @MethodSource("backends")
    void engineTextPrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        String producer = "meta::relational::functions::sqlstring::toSQLString(|x::Firm.all()->project([f|$f.name],"
                + " ['name']), x::M, meta::relational::runtime::DatabaseType.H2, [])";
        String ours = text(run(backend, ctx, "x::RT", "{| " + producer + ";}", false, "select count(*) from FIRM",
                FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        assertTrue(ours.startsWith("select "), ours);
        String frameSql = text(run(backend, ctx, "x::RT", "{| let r = " + EXECUTE
                + "; meta::relational::mapping::sqlRemoveFormatting($r);}", false, "select count(*) from FIRM",
                FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        assertTrue(frameSql.startsWith("select "), frameSql);
        // another spelling of the same query (the rows agree), and the same spelling over other rows
        String sameRows = "SELECT" + ours.substring("select".length());
        String otherRows = ours + " where \"root\".ID = 1";
        int oursLeg = StatementOrigin.REFEREE_OURS.ordinal();
        for (ExecuteOptions.JudgeMode mode : List.of(ExecuteOptions.JudgeMode.HOST, ExecuteOptions.JudgeMode.DATABASE)) {
            // the text equal: the verdict, no replay (the user's ruling of 2026-09-21) -- pinned absolutely, so a
            // vacuous pass (the step handing the comparison over instead of judging it: found by the corpus before this
            // pin existed) cannot hide behind two roads agreeing
            for (String equal : List.of(
                    "{| meta::relational::functions::asserts::assertSameSQL('" + pure(ours) + "', " + producer + ");}",
                    "{| assertEquals('" + pure(ours) + "', " + producer + ");}",
                    "{| let r = " + EXECUTE + "; assertEquals('" + pure(frameSql) + "',"
                            + " meta::relational::mapping::sqlRemoveFormatting($r));}",
                    "{| let r = " + EXECUTE + "; meta::relational::functions::sqlQueryToString::h2"
                            + "::assertEqualsH2Compatible('legacy', '" + pure(frameSql) + "', $r);}",
                    // the corpus's spelling (M10c's 321): the actual a String the producer renders
                    "{| meta::relational::functions::sqlQueryToString::h2::assertEqualsH2Compatible('legacy', '"
                            + pure(ours) + "', " + producer + ");}",
                    "{| let r = " + EXECUTE + "; meta::relational::functions::asserts::assertSameSQL('"
                            + pure(frameSql) + "', $r);}")) {
                Run walked = assertParity(backend, ctx, "x::RT", equal, "select count(*) from FIRM", FIRM_ROWS, mode);
                assertEquals("ok", walked.outcome, "the text equal holds: " + equal);
                assertEquals(0, originsOf(walked.origins)[oursLeg], "no rows leg for an equal text: " + equal);
            }
            // the text differing, the rows agreeing: the appeal replays and the referee says MATCH -- the assert holds
            Run matched = assertParity(backend, ctx, "x::RT",
                    "{| meta::relational::functions::asserts::assertSameSQL('" + pure(sameRows) + "', " + producer + ");}",
                    "select count(*) from FIRM", FIRM_ROWS, mode);
            assertEquals("ok", matched.outcome, "the rows agree: the appeal holds");
            assertTrue(originsOf(matched.origins)[oursLeg] >= 1, "the rows leg ran: " + matched.statements);
            // the rows differing: the appeal fails the assert with the arm's own message
            Run diverged = assertParity(backend, ctx, "x::RT", "{| assertEquals('" + pure(otherRows) + "', " + producer
                    + ");}", "select count(*) from FIRM", FIRM_ROWS, mode);
            assertTrue(diverged.outcome.startsWith("AssertFailed") && diverged.outcome.contains("diverged"),
                    "the rows differ: the appeal fails the assert: " + diverged.outcome);
            // a frame's executed SQL against another spelling of it: the appeal reads the frame's rows (MATCH)
            Run frameMatched = assertParity(backend, ctx, "x::RT", "{| let r = " + EXECUTE
                    + "; meta::relational::functions::asserts::assertSameSQL('"
                    + pure("SELECT" + frameSql.substring("select".length())) + "', $r);}",
                    "select count(*) from FIRM", FIRM_ROWS, mode);
            assertEquals("ok", frameMatched.outcome, "the frame's rows agree: the appeal holds");
            assertTrue(frameMatched.verdicts.toString().contains("refereed MATCH"), frameMatched.verdicts.toString());
            // a dialect with a renderer and no referee database (DB2): a DECLINE decided before the texts are
            // compared, counted; the text is the contract
            String renderedElsewhere = producer.replace("DatabaseType.H2", "DatabaseType.DB2");
            Run declinedFails = assertParity(backend, ctx, "x::RT", "{| assertEquals('x', " + renderedElsewhere + ");}",
                    "select count(*) from FIRM", FIRM_ROWS, mode);
            assertTrue(declinedFails.outcome.startsWith("AssertFailed")
                    && declinedFails.outcome.contains("text is the contract"), declinedFails.outcome);
            assertTrue(declinedFails.verdicts.toString().contains("declined foreign-dialect:DB2"),
                    declinedFails.verdicts.toString());
            // a dialect the engine-text renderer does not spell: the routine RAISES when the text is planned. Under
            // the host judge the raise is planned at the assert's turn and the body moves; under the database judge
            // the raise would wall a batch with no row before it to carry the wall, so the static gate leaves the body
            // to the walk before anything is counted (the same raise, counted once) -- the one road difference
            String foreign = "{| assertEquals('x', " + producer.replace("DatabaseType.H2", "DatabaseType.Snowflake")
                    + ");}";
            if (mode == ExecuteOptions.JudgeMode.HOST) {
                assertParity(backend, ctx, "x::RT", foreign, "select count(*) from FIRM", FIRM_ROWS, mode);
            } else {
                assertWalkedAlike(backend, ctx, "x::RT", foreign, "select count(*) from FIRM", FIRM_ROWS, mode);
            }
        }
    }

    /** M10c item 12: a GRAPH FRAME -- a frame whose query is a graph fetch and its serialize -- planned on the program
     *  road (its eager run the same statement as the walk's): inline, let-bound, and the corpus's executeLegendQuery
     *  with its parameters bound from the pairs, read in a filter and in a graph tree's qualifier argument (the
     *  product's binding site reads a tree's arguments since this item: the XStore milestoning bodies); holding and
     *  failing, parity under both judges. The goldens are read off a run. */
    @ParameterizedTest
    @MethodSource("backends")
    void graphFramePrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        String gf = "meta::pure::graphFetch::execution::graphFetch(#{x::Firm{name}}#)"
                + "->meta::pure::graphFetch::execution::serialize(#{x::Firm{name}}#)";
        String tagged = "meta::pure::graphFetch::execution::graphFetch(#{x::Firm{tagged($s)}}#)"
                + "->meta::pure::graphFetch::execution::serialize(#{x::Firm{tagged($s)}}#)";
        String inline = "let r = meta::pure::router::execute(|x::Firm.all()->" + gf + ", x::M, x::RT.runtimeValue, []);";
        String letBound = "let func = {|x::Firm.all()->" + gf + "}; let r = meta::pure::router::execute($func, x::M,"
                + " x::RT.runtimeValue, []);";
        String filtered = "let func = {n: String[1]|x::Firm.all()->filter(f|$f.name == $n)->" + gf + "->from(x::M,"
                + " x::RT.runtimeValue)}; let r = meta::legend::executeLegendQuery($func, [pair('n', 'Acme')],"
                + " ^meta::pure::runtime::ExecutionContext(), []);";
        String qualified = "let func = {s: String[1]|x::Firm.all()->" + tagged + "->from(x::M, x::RT.runtimeValue)};"
                + " let r = meta::legend::executeLegendQuery($func, [pair('s', '!')],"
                + " ^meta::pure::runtime::ExecutionContext(), []);";
        for (var c : List.of(java.util.Map.entry(inline, "$r.values->toOne()"),
                java.util.Map.entry(letBound, "$r.values->toOne()"), java.util.Map.entry(filtered, "$r"),
                java.util.Map.entry(qualified, "$r"))) {
            String golden = text(run(backend, ctx, "x::RT", "{| " + c.getKey() + " " + c.getValue() + ";}", false,
                    "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
            assertTrue(golden.contains(c.getKey() == qualified ? "Acme!" : "Acme"), "the value bound: " + golden);
            for (ExecuteOptions.JudgeMode mode : List.of(ExecuteOptions.JudgeMode.HOST,
                    ExecuteOptions.JudgeMode.DATABASE)) {
                Run held = assertParity(backend, ctx, "x::RT", "{| " + c.getKey() + " assertEquals('" + pure(golden)
                        + "', " + c.getValue() + ");}", "select count(*) from FIRM", FIRM_ROWS, mode);
                assertEquals("ok", held.outcome, mode + ": " + c.getKey());
                Run failed = assertParity(backend, ctx, "x::RT", "{| " + c.getKey() + " assertEquals('" + pure(golden)
                        + "x', " + c.getValue() + ");}", "select count(*) from FIRM", FIRM_ROWS, mode);
                assertTrue(failed.outcome.startsWith("AssertFailed"), mode + ": " + failed.outcome);
            }
        }
        // two frames over ONE lambda with different pairs, each read: upstream reads each frame with its own binding;
        // the walk's frames share one environment's query lets, so the earlier frame's readers read the later binding
        // (`$r1` reads '?': the walk's defect) -- the walk's, by name, both roads alike (the item's audit F1)
        String twice = "let func = {s: String[1]|x::Firm.all()->" + tagged + "->from(x::M, x::RT.runtimeValue)};"
                + " let r1 = meta::legend::executeLegendQuery($func, [pair('s', '!')],"
                + " ^meta::pure::runtime::ExecutionContext(), []);"
                + " let r2 = meta::legend::executeLegendQuery($func, [pair('s', '?')],"
                + " ^meta::pure::runtime::ExecutionContext(), []);";
        String g1 = text(run(backend, ctx, "x::RT", "{| " + twice + " $r1;}", false, "select count(*) from FIRM",
                FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        String g2 = text(run(backend, ctx, "x::RT", "{| " + twice + " $r2;}", false, "select count(*) from FIRM",
                FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        assertTrue(g1.contains("Acme?") && g2.contains("Acme?"), "the walk's shared binding: " + g1 + " / " + g2);
        assertNull(BodyClass.of(typed(ctx, "{| " + twice + " assertEquals('" + pure(g1) + "', $r1);"
                + " assertEquals('" + pure(g2) + "', $r2);}"), "x::RT", true), "the walk's, by the class rule");
        // the PRODUCT's door: a query whose graph tree reads a let -- planned (an unbound variable before this item)
        typed(ctx, "{| let s = '!'; x::Firm.all()->" + tagged + ";}").plan("x::RT");
    }

    /** M10c item 11: a from()'s INLINE TEST DATA in a let a frame then runs -- the corpus's spelling (a runtime copied
     *  with {@code testDataSetupCsv}, a lambda whose from() names it, the frame executing the lambda): the test door
     *  seeds the let's sources before it, as the walk establishes them, and the frame reads the seeded rows; parity
     *  under the host judge, the seed statements included. */
    @ParameterizedTest
    @MethodSource("backends")
    void inlineSeedFramePrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        String seeded = "let runtime = x::testRuntime(); let cs = $runtime.connectionStores->at(0);"
                + " let c = $cs.connection->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);"
                + " let rtd = ^$runtime(connectionStores = ^$cs(connection = ^$c(testDataSetupCsv ="
                + " 'default\\nFIRM\\nID,NAME\\n1,Acme\\n2,Bolt\\n3,Cyl\\n-----\\n')));"
                + " let func = {|x::Firm.all()->project([f|$f.name], ['name'])->from(x::M, $rtd)};"
                + " let r = meta::pure::router::execute($func, x::M, x::RT.runtimeValue, []);";
        for (String body : List.of(
                "{| " + seeded + " assertSize($r.values.rows, 3);}",
                "{| " + seeded + " assertEquals(['Acme', 'Bolt', 'Cyl'], $r.values.rows.values->sort());}",
                "{| " + seeded + " assertSize($r.values.rows, 2);}")) {
            assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.HOST);
            assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.DATABASE);
        }
        Run held = run(backend, ctx, "x::RT", "{| " + seeded + " assertSize($r.values.rows, 3);}", true,
                "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST);
        assertEquals("ok", held.outcome, "the frame reads the seeded rows: " + held.statements);
        String rtd = "let runtime = x::testRuntime(); let cs = $runtime.connectionStores->at(0);"
                + " let c = $cs.connection->cast(@meta::external::store::relational::runtime::TestDatabaseConnection);"
                + " let rtd = ^$runtime(connectionStores = ^$cs(connection = ^$c(testDataSetupCsv ="
                + " 'default\\nFIRM\\nID,NAME\\n1,Acme\\n2,Bolt\\n3,Cyl\\n-----\\n')));";
        // the frame's own query naming the inline data: seeded at the frame's let, then its eager run
        assertParity(backend, ctx, "x::RT", "{| " + rtd + " let r = meta::pure::router::execute(|x::Firm.all()->project("
                + "[f|$f.name], ['name'])->from(x::M, $rtd), x::M, x::RT.runtimeValue, []); assertSize($r.values.rows,"
                + " 3);}", "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST);
        // the walk's, by name: a let-bound query whose kind is planned another way (an object built, read through the
        // let since this item), a seeded let after an assert (the seeds' order
        // against the fused statement and the open script, B1), an assert holding the inline data itself
        for (String stays : List.of(
                "{| " + rtd + " let func = {|x::Firm.all()->project([f|^x::Firm(name = $f.name).name], ['name'])"
                        + "->from(x::M, $rtd)}; let r = meta::pure::router::execute($func, x::M, x::RT.runtimeValue, []);"
                        + " assert(true);}",
                "{| let r0 = " + EXECUTE + "; assertSize($r0.values.rows, 2); " + seeded
                        + " assertSize($r.values.rows, 3);}",
                "{| " + rtd + " let r = " + EXECUTE + "; assertEquals(3, x::Firm.all()->project([f|$f.name], ['name'])"
                        + "->from(x::M, $rtd)->size());}")) {
            assertNull(BodyClass.of(typed(ctx, stays), "x::RT", true), "the walk's: " + stays);
        }
    }

    /** M10c item 10: an engine-text assert in a SYSTEM-ROUTED body (the body's one store read the system store's, the
     *  producer's query a routine argument), its arm planned on the body's environment and its sides on the program's:
     *  its text equal holds with no appeal; its text differing with agreeing rows appeals at the step and the rows leg
     *  reads the user's table on the body's session, the referee MATCHing -- as the walk's; our text a side the step
     *  reads (item 8: a plan's SQL through a navigation) names the program's one target, equal, appealed and diverging.
     *  The one difference M10a makes for a system-routed body stands (the walk alone sets the body session up). */
    @ParameterizedTest
    @MethodSource("backends")
    void systemRoutedEngineTextPrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        run(backend, ctx, "x::RT", "{| assertEquals('M', x::M.name);}", false, "select count(*) from FIRM", FIRM_ROWS);
        String producer = "meta::relational::functions::sqlstring::toSQLString(|x::Firm.all()->project([f|$f.name],"
                + " ['name']), x::M, meta::relational::runtime::DatabaseType.H2, [])";
        String ours = text(run(backend, ctx, "x::RT", "{| " + producer + ";}", false, "select count(*) from FIRM",
                FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        String equal = "{| assertEquals('M', x::M.name); assertEquals('" + pure(ours) + "', " + producer + ");}";
        String appealed = "{| assertEquals('M', x::M.name); assertEquals('"
                + pure("SELECT" + ours.substring("select".length())) + "', " + producer + ");}";
        String diverged = "{| assertEquals('M', x::M.name); assertEquals('" + pure(ours + " where \"root\".ID = 1")
                + "', " + producer + ");}";
        // our text a side the step reads (item 8: a plan handle's SQL through a navigation over the plan's rows) -- the
        // datetime plan tests' shape: the side planned on the program's environment names the program's one target
        String planLet = "let p = meta::pure::executionPlan::executionPlan(|x::Firm.all()->project([f|$f.name],"
                + " ['name']), x::M, x::RT.runtimeValue, []);";
        String nav = "$p.rootExecutionNode.executionNodes->at(0)->cast(@meta::relational::mapping::SQLExecutionNode)"
                + ".sqlQuery->toOne()";
        String planSql = text(run(backend, ctx, "x::RT", "{| " + planLet + " " + nav + ";}", false,
                "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        String deferred = "{| assertEquals('M', x::M.name); " + planLet + " assertEquals('" + pure(planSql) + "', "
                + nav + ");}";
        String deferredAppealed = "{| assertEquals('M', x::M.name); " + planLet + " assertEquals('"
                + pure("SELECT" + planSql.substring("select".length())) + "', " + nav + ");}";
        String deferredDiverged = "{| assertEquals('M', x::M.name); " + planLet + " assertEquals('"
                + pure(planSql + " where \"root\".ID = 1") + "', " + nav + ");}";
        for (var c : List.of(java.util.Map.entry(equal, "ok"), java.util.Map.entry(appealed, "ok"),
                java.util.Map.entry(diverged, "AssertFailed"), java.util.Map.entry(deferred, "ok"),
                java.util.Map.entry(deferredAppealed, "ok"), java.util.Map.entry(deferredDiverged, "AssertFailed"))) {
            String body = c.getKey();
            assertTrue(BodyClass.systemRouted(typed(ctx, body), "x::RT"), "system-routed: " + body);
            Run walked = run(backend, ctx, "x::RT", body, false, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.HOST);
            Run moved = run(backend, ctx, "x::RT", body, true, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.HOST);
            assertTrue(walked.outcome.startsWith(c.getValue()), body + " -> " + walked.outcome);
            assertEquals(walked.outcome, moved.outcome, "outcome: " + body);
            assertEquals(walked.answer, moved.answer, "the answer: " + body);
            assertEquals(walked.verdicts, moved.verdicts, "the verdicts: " + body);
            assertEquals(walked.census, moved.census, "the verdict census: " + body);
            assertEquals(walked.ledger, moved.ledger, "the raw-SQL ledger: " + body);
            assertEquals(walked.database, moved.database, "the database left: " + body);
            assertEquals(walked.moved + 1, moved.moved, "the router moved the body: " + body);
            long[] w = originsOf(walked.origins);
            long[] m = originsOf(moved.origins);
            for (int i = 0; i < w.length; i++) {
                long allowed = i == StatementOrigin.SESSION.ordinal() ? 1 : 0;
                assertTrue(w[i] - m[i] >= 0 && w[i] - m[i] <= allowed, "the statement-origin census differs beyond"
                        + " the body session's setup: " + body + "\n  walked: " + walked.statements
                        + "\n  moved:  " + moved.statements);
            }
            List<String> walkedSql = new ArrayList<>(walked.statements);
            walkedSql.removeIf(st -> st.startsWith("[session]"));
            List<String> movedSql = new ArrayList<>(moved.statements);
            movedSql.removeIf(st -> st.startsWith("[session]"));
            assertEquals(walkedSql, movedSql, "the statements sent, the body session's setup aside: " + body);
        }
        for (String a : List.of(appealed, deferredAppealed)) {
            Run matched = run(backend, ctx, "x::RT", a, true, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.HOST);
            assertTrue(matched.verdicts.toString().contains("refereed MATCH"), matched.verdicts.toString());
            assertTrue(originsOf(matched.origins)[StatementOrigin.REFEREE_OURS.ordinal()] >= 1,
                    "the rows leg read the user's table: " + matched.statements);
        }
        Run failed = run(backend, ctx, "x::RT", deferredDiverged, true, "select count(*) from FIRM", FIRM_ROWS,
                ExecuteOptions.JudgeMode.HOST);
        assertTrue(failed.outcome.contains("diverged"), "the appeal fails with the arm's message: " + failed.outcome);
    }

    /** M10c item 8: an engine-text assert whose OUR text is a side the plan runs -- a plan handle's SQL read through a
     *  navigation (inline, through a let, under the dual-golden root), a staged routine's text under {@code replace} --
     *  planned as the verdict step's ask and read there, the equality computed at the step; the appeal with the
     *  step's text (the rows leg MATCHing another spelling, failing diverging rows); the statement census agreeing
     *  because the step is sent under the origin the arm marked; the type gate declining a side that is no text on both
     *  roads with no probe statement; a golden that is no constant staying the walk's by name. */
    @ParameterizedTest
    @MethodSource("backends")
    void deferredTextPrograms(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        String planLet = "let p = meta::pure::executionPlan::executionPlan(|x::Firm.all()->project([f|$f.name],"
                + " ['name']), x::M, x::RT.runtimeValue, []);";
        String nav = "$p.rootExecutionNode.executionNodes->at(0)->cast(@meta::relational::mapping::SQLExecutionNode)"
                + ".sqlQuery->toOne()";
        String planSql = text(run(backend, ctx, "x::RT", "{| " + planLet + " " + nav + ";}", false,
                "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        assertTrue(planSql.startsWith("select "), planSql);
        String producer = "meta::relational::functions::sqlstring::toSQLString(|x::Firm.all()->project([f|$f.name],"
                + " ['name']), x::M, meta::relational::runtime::DatabaseType.H2, [])";
        String ours = text(run(backend, ctx, "x::RT", "{| " + producer + ";}", false, "select count(*) from FIRM",
                FIRM_ROWS, ExecuteOptions.JudgeMode.HOST));
        String replaced = ours.replace("\"root\"", "\"r\"");
        assertTrue(!ours.equals(replaced), ours);
        String replace = "->replace('\"root\"', '\"r\"')";
        int oursLeg = StatementOrigin.REFEREE_OURS.ordinal();
        // the text equal: the verdict at the step over the side the step ran, no appeal
        for (String equal : List.of(
                "{| " + planLet + " assertEquals('" + pure(planSql) + "', " + nav + ");}",
                "{| " + planLet + " let s = " + nav + "; assertEquals('" + pure(planSql) + "', $s);}",
                "{| " + planLet + " let s = " + nav + "; meta::relational::functions::sqlQueryToString::h2"
                        + "::assertEqualsH2Compatible('legacy', '" + pure(planSql) + "', $s);}",
                "{| assertEquals('" + pure(replaced) + "', " + producer + replace + ");}",
                "{| meta::relational::functions::asserts::assertSameSQL('" + pure(replaced) + "', " + producer
                        + replace + ");}")) {
            Run walked = assertParity(backend, ctx, "x::RT", equal, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.HOST);
            assertEquals("ok", walked.outcome, "the text equal holds: " + equal);
        }
        // our text's one statement runs under the origin the arm marks (REFEREE_OURS in the toSQLString arm): the
        // census agrees because the step is sent under that mark
        Run marked = assertParity(backend, ctx, "x::RT", "{| assertEquals('" + pure(replaced) + "', " + producer
                + replace + ");}", "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST);
        assertEquals(1, originsOf(marked.origins)[oursLeg], "our text read by one statement under the arm's mark: "
                + marked.statements);
        // the text differing, the rows agreeing: the appeal at the step replays and the referee says MATCH
        Run matched = assertParity(backend, ctx, "x::RT", "{| assertEquals('" + pure(ours) + "', " + producer
                + replace + ");}", "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST);
        assertEquals("ok", matched.outcome, "the rows agree: the appeal holds");
        assertTrue(matched.verdicts.toString().contains("refereed MATCH"), matched.verdicts.toString());
        Run planMatched = assertParity(backend, ctx, "x::RT", "{| " + planLet + " assertEquals('"
                + pure("SELECT" + planSql.substring("select".length())) + "', " + nav + ");}",
                "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST);
        assertEquals("ok", planMatched.outcome, "the plan's rows agree: the appeal holds");
        // the rows differing: the appeal fails the assert with the arm's own message, our text the step's
        Run diverged = assertParity(backend, ctx, "x::RT", "{| " + planLet + " assertEquals('"
                + pure(planSql + " where \"root\".ID = 1") + "', " + nav + ");}", "select count(*) from FIRM",
                FIRM_ROWS, ExecuteOptions.JudgeMode.HOST);
        assertTrue(diverged.outcome.startsWith("AssertFailed") && diverged.outcome.contains("diverged"),
                diverged.outcome);
        // the gate: a side the arm claims (a plan handle behind it) that is no text by its type -- the generic
        // equality on both roads, no probe statement
        Run counted = assertParity(backend, ctx, "x::RT", "{| " + planLet + " assertEquals(1,"
                + " $p.rootExecutionNode.executionNodes->size());}", "select count(*) from FIRM", FIRM_ROWS,
                ExecuteOptions.JudgeMode.HOST);
        assertEquals("ok", counted.outcome);
        assertEquals(0, originsOf(counted.origins)[oursLeg], "no probe of a side that is no text: "
                + counted.statements);
        // a golden that is no constant: the arms route on its text at planning -- the body stays the walk's, by name
        assertWalkedAlike(backend, ctx, "x::RT", "{| " + planLet + " let g = " + nav + "; assertEquals($g, "
                + producer + ");}", "select count(*) from FIRM", FIRM_ROWS, ExecuteOptions.JudgeMode.HOST);
        // under the database judge the batch row composes the text equality at planning, so a text the step would
        // read stays the walk's, by name, nothing read while planning (the item's audit, finding 1): the walk twice,
        // alike, the statements at the assert's turn
        for (String deferred : List.of(
                "{| " + planLet + " assertEquals('" + pure(planSql) + "', " + nav + ");}",
                "{| assertEquals('" + pure(replaced) + "', " + producer + replace + ");}")) {
            assertWalkedAlike(backend, ctx, "x::RT", deferred, "select count(*) from FIRM", FIRM_ROWS,
                    ExecuteOptions.JudgeMode.DATABASE);
        }
    }

    /** {@code body} is a moved class's, yet the router leaves it to the walk by a named rule of its judge's planner:
     *  the two runs are the walk's twice, alike in everything, and the router moved nothing. */
    private static void assertWalkedAlike(TestBackend backend, ModelContext ctx, String runtime, String body,
            String check, List<String> setup, ExecuteOptions.JudgeMode mode) throws Exception {
        assertNotNull(BodyClass.of(typed(ctx, body), runtime, true), "the body is a moved class's: " + body);
        Run walked = run(backend, ctx, runtime, body, false, check, setup, mode);
        Run routed = run(backend, ctx, runtime, body, true, check, setup, mode);
        assertEquals(walked.outcome, routed.outcome, "outcome: " + body);
        assertEquals(walked.verdicts, routed.verdicts, "the verdicts: " + body);
        assertEquals(walked.census, routed.census, "the verdict census: " + body);
        assertEquals(walked.origins, routed.origins, "the statement-origin census: " + body);
        // nothing sent while the router planned and refused: the statements are the walk's, in its order
        assertEquals(stampsAsParameters(walked.statements), stampsAsParameters(routed.statements),
                "the statements sent: " + body);
        assertEquals(walked.moved, routed.moved, "the router walked the body: " + body);
    }

    /** A text as a Pure string literal's body. */
    private static String pure(String text) {
        return text.replace("\\", "\\\\").replace("'", "\\'");
    }

    /** The one text a body answered, read off the capture's rendering ({@code ... row[<text>] end}). */
    private static String text(Run run) {
        assertEquals("ok", run.outcome, "the body answering a text ran: " + run.statements);
        String answer = run.answer;
        int from = answer.indexOf(" row[");
        int to = answer.lastIndexOf("] end");
        assertTrue(from >= 0 && to > from, "one text answered: " + answer + " (" + run.verdicts + ")");
        return answer.substring(from + " row[".length(), to);
    }

    /** M9c: a frame's activity COMMENT is the trace stamp of its eager run on both roads -- the walk's frame holds the
     *  run's comment as a literal, the pre-planned frame a parameter the run's check step binds (R17 C4) -- so the
     *  sides' texts differ there by design and every other boundary is the same; the moved side carries the stamp
     *  as a bound parameter, never as text. */
    @ParameterizedTest
    @MethodSource("backends")
    void theActivityCommentIsTheRunsStamp(TestBackend backend) throws Exception {
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        String body = "{| let r = " + EXECUTE + "; assert($r.activities->at(0)->cast(@meta::relational::mapping::"
                + "RelationalActivity).comment->toOne()->startsWith('-- \"executionTraceID\"'));"
                + " assertEquals(62, $r.activities->at(0)->cast(@meta::relational::mapping::RelationalActivity)"
                + ".comment->toOne()->length());}";
        assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS);
        Run walked = run(backend, ctx, "x::RT", body, false, "select count(*) from FIRM", FIRM_ROWS);
        Run moved = run(backend, ctx, "x::RT", body, true, "select count(*) from FIRM", FIRM_ROWS);
        assertEquals("ok", walked.outcome);
        assertTrue(walked.statements.stream().anyMatch(s -> STAMP_LITERAL.matcher(s).find()),
                "the walk's side spells the run's comment: " + walked.statements);
        assertFalse(moved.statements.stream().anyMatch(s -> STAMP_LITERAL.matcher(s).find()),
                "the moved side binds the comment, never spelling it: " + moved.statements);
    }

    /** The assert shapes the class admits and the host checks do not plan (M6's audit, S3 and N3): the router leaves
     *  the body on the walk, by a static rule ({@code AssertVerdicts.planHost} answers null before any side). */
    @ParameterizedTest
    @MethodSource("backends")
    void theAssertShapesTheHostChecksLeaveOnTheWalk(TestBackend backend) throws Exception {
        for (String body : List.of(
                // an instance test of a computed type (the walk's judgment would decline it after reading its side)
                "{| assertInstanceOf(1, type(2));}",
                // the identity of two values that are not elements
                "{| assertIs(1, 1);}",
                // a NESTED quantification (the engine's guard idiom): the walk unrolls it recursively; the planners
                // leave it (M10c item 5 plans the flat forms)
                "{| [1, 2]->map(i| [3]->map(j| assertEquals($i, $i))); true;}")) {
            ModelContext ctx = Compiler.compileModel(model(backend, false));
            assertEquals(BodyClass.ASSERT_PROGRAM, BodyClass.of(typed(ctx, body), RT, true), body);
            Run walked = run(backend, ctx, body, false, "select 1");
            Run routed = run(backend, ctx, body, true, "select 1");
            assertEquals(walked.moved, routed.moved, "the router left the body on the walk: " + body);
            assertEquals(walked.outcome, routed.outcome, "outcome: " + body);
            assertEquals(walked.verdicts, routed.verdicts, "the verdicts: " + body);
            assertEquals(walked.census, routed.census, "the verdict census: " + body);
        }
    }

    private static String model(TestBackend backend, boolean seeded) {
        if (!seeded) {
            return backend.withStorelessRuntime("");
        }
        return StorelessRuntime.declaring("", backend.type(),
                "specification: LocalH2 { testDataSetupSqls: [ 'create table if not exists seed_t(i int)',"
                        + " 'delete from seed_t', 'insert into seed_t values (1)' ]; };\n  auth: DefaultH2");
    }

    /** {@code q} as the execution door hands it to the router: each unread let the statement it binds. */
    private static TypedQuery unreadAsStatements(TypedQuery q) {
        return q.withBody(com.legend.planner.LetPlanner.unreadAsStatements(q.body(), q.resolved(), q.context()));
    }

    private static TypedQuery typed(ModelContext ctx, String body) {
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(body);
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : List.of(parsed);
        return Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(List.of()), ctx));
    }

    /** {@code body} (a moved class's) run on the walk and on the router, each on a fresh database: every boundary
     *  observed the same. */
    private static void assertParity(TestBackend backend, String body, String check) throws Exception {
        assertParity(backend, Compiler.compileModel(model(backend, false)), RT, body, check, List.of());
    }

    /** {@code body} on {@code ctx} and {@code runtime}, each run on a fresh database the harness first sends
     *  {@code setup} to (a table a frame reads; the harness sends its own SQL, as every test harness does). */
    private static void assertParity(TestBackend backend, ModelContext ctx, String runtime, String body, String check,
            List<String> setup) throws Exception {
        assertParity(backend, ctx, runtime, body, check, setup, ExecuteOptions.JudgeMode.HOST);
    }

    /** {@code body} (a moved class's under {@code mode}'s judge) run on the walk and on the router, each on a fresh
     *  database: every boundary observed the same. */
    private static Run assertParity(TestBackend backend, ModelContext ctx, String runtime, String body, String check,
            List<String> setup, ExecuteOptions.JudgeMode mode) throws Exception {
        assertNotNull(BodyClass.of(unreadAsStatements(typed(ctx, body)), runtime, true),
                "the body is a moved class's under one judge: " + body);
        Run walked = run(backend, ctx, runtime, body, false, check, setup, mode);
        Run moved = run(backend, ctx, runtime, body, true, check, setup, mode);
        assertEquals(walked.outcome, moved.outcome, "outcome: " + body);
        assertEquals(walked.ledger, moved.ledger, "the raw-SQL ledger: " + body);
        assertEquals(walked.answer, moved.answer, "the answer: " + body);
        assertEquals(walked.origins, moved.origins, "the statement-origin census: " + body + "\n  walked: "
                + walked.statements + "\n  moved:  " + moved.statements);
        assertEquals(walked.effects, moved.effects, "the effects census: " + body);
        assertEquals(walked.database, moved.database, "the database left: " + body);
        assertEquals(stampsAsParameters(walked.statements), stampsAsParameters(moved.statements),
                "the statements sent (origin and text, in order; a frame's trace stamp as the plan binds it): " + body);
        assertEquals(walked.verdicts, moved.verdicts, "the verdicts the listener heard: " + body + " (" + walked.outcome
                + ")");
        assertEquals(walked.census, moved.census, "the verdict census: " + body + " (" + walked.outcome + ")");
        assertEquals(walked.moved + 1, moved.moved, "the router moved the body: " + body);
        return walked;
    }

    /** {@code body}, a MIXED body (a statement on the graph's system database beside the body's own: per-statement
     *  routing, 2026-11-03), run on the walk and on the router: every boundary observed the same but WHEN the body
     *  session's own statements run -- the walk sends them before its first statement on that session, the program
     *  when it opens the session (one SESSION statement, its place in the order). The system database is seeded first,
     *  so neither run carries its once-per-graph seed. */
    private static void assertMixedParity(TestBackend backend, ModelContext ctx, String runtime, String body,
            String check, List<String> setup) throws Exception {
        run(backend, ctx, runtime, "{| assertEquals('M', x::M.name);}", false, check, setup);
        assertNotNull(BodyClass.of(unreadAsStatements(typed(ctx, body)), runtime, true), "a moved class's: " + body);
        Run walked = run(backend, ctx, runtime, body, false, check, setup, ExecuteOptions.JudgeMode.HOST);
        Run moved = run(backend, ctx, runtime, body, true, check, setup, ExecuteOptions.JudgeMode.HOST);
        assertEquals(walked.outcome, moved.outcome, "outcome: " + body);
        assertEquals(walked.ledger, moved.ledger, "the raw-SQL ledger: " + body);
        assertEquals(walked.answer, moved.answer, "the answer: " + body);
        assertEquals(walked.effects, moved.effects, "the effects census: " + body);
        assertEquals(walked.database, moved.database, "the database left: " + body);
        assertEquals(walked.verdicts, moved.verdicts, "the verdicts the listener heard: " + body);
        assertEquals(walked.census, moved.census, "the verdict census: " + body);
        assertEquals(walked.moved + 1, moved.moved, "the router moved the body: " + body);
        java.util.function.Function<List<String>, List<String>> noSession = l -> stampsAsParameters(l).stream()
                .filter(t -> !t.startsWith("[session]")).toList();
        assertEquals(noSession.apply(walked.statements), noSession.apply(moved.statements),
                "the statements sent but the body session's own: " + body);
        long[] w = originsOf(walked.origins);
        long[] m = originsOf(moved.origins);
        for (int i = 0; i < w.length; i++) {
            long allowed = i == StatementOrigin.SESSION.ordinal() ? 1 : 0;
            assertTrue(Math.abs(w[i] - m[i]) <= allowed, "the statement-origin census differs beyond the body"
                    + " session's own statement: " + body + "\n  walked: " + walked.origins + "\n  moved:  "
                    + moved.origins);
        }
    }

    /** The walk's spelling of a frame's trace stamp (the run's comment, a literal in the activity rows it registered
     *  after the run) replaced by {@code <stamp>}, and the plan's (the parameter the run's check step binds, R17 C4;
     *  M9c) likewise: the one designed difference in a side reading a frame's activities. */
    private static final java.util.regex.Pattern STAMP_LITERAL =
            java.util.regex.Pattern.compile("'-- \"executionTraceID\" : \"[0-9a-f-]{36}\"'");
    private static final java.util.regex.Pattern STAMP_PARAMETER =
            java.util.regex.Pattern.compile("CAST\\(\\? AS VARCHAR\\)|\\?");

    private static List<String> stampsAsParameters(List<String> statements) {
        List<String> out = new ArrayList<>(statements.size());
        for (String s : statements) {
            out.add(STAMP_PARAMETER.matcher(STAMP_LITERAL.matcher(s).replaceAll("<stamp>")).replaceAll("<stamp>"));
        }
        return out;
    }

    private record Run(String outcome, List<String> ledger, String answer, String origins, long effects,
            String database, long moved, List<String> statements, List<String> verdicts, String census) {
    }

    private static Run run(TestBackend backend, ModelContext ctx, String body, boolean router, String check)
            throws Exception {
        return run(backend, ctx, RT, body, router, check, List.of());
    }

    /** The statement-origin deltas a {@link Run} printed ({@code Arrays.toString}), as numbers again. */
    private static long[] originsOf(String printed) {
        String[] parts = printed.substring(1, printed.length() - 1).split(", ");
        long[] out = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = Long.parseLong(parts[i].trim());
        }
        return out;
    }

    private static Run run(TestBackend backend, ModelContext ctx, String runtime, String body, boolean router,
            String check, List<String> setup) throws Exception {
        return run(backend, ctx, runtime, body, router, check, setup, ExecuteOptions.JudgeMode.HOST);
    }

    private static Run run(TestBackend backend, ModelContext ctx, String runtime, String body, boolean router,
            String check, List<String> setup, ExecuteOptions.JudgeMode mode) throws Exception {
        try (Connection c = backend.open()) {
            try (Statement st = c.createStatement()) {
                for (String sql : setup) {
                    st.execute(sql);
                }
            }
            TypedQuery q = typed(ctx, body);
            RawSqlBoundary.Recorder recorder = new RawSqlBoundary.Recorder();
            ExecuteOptions options = ExecuteOptions.recording(recorder, null).withJudgeMode(mode);
            Capture capture = new Capture();
            Sessions.Session session = Sessions.borrowed(c);
            com.legend.judge.SqlReplayReferee referee = new StubReferee(c);
            long[] before = StatementOrigin.snapshot();
            long effectsBefore = Census.count(Census.Key.EFFECTS_IN_SCRIPT);
            long movedBefore = Census.count(Census.Key.BODIES_MOVED);
            String outcome = "ok";
            List<String> statements = new ArrayList<>();
            List<String> verdicts = new ArrayList<>();
            com.legend.judge.AssertListener listener = new com.legend.judge.AssertListener() {
                @Override
                public void verdict(String assertName, boolean pass, @com.legend.base.Nullable String detail) {
                    verdicts.add(assertName + " " + pass + " " + detail);
                }

                @Override
                public void unjudged(String assertName, String reason) {
                    verdicts.add(assertName + " unjudged " + reason);
                }

                @Override
                public void declined(String assertName, String reason) {
                    verdicts.add(assertName + " declined " + reason);
                }

                @Override
                public void refereed(String assertName, String outcome) {
                    verdicts.add(assertName + " refereed " + outcome);
                }
            };
            java.util.SortedMap<String, Long> censusBefore = new java.util.TreeMap<>(
                    com.legend.judge.CanonicalDivergence.sqlCensus());
            try (var observing = StatementOrigin.observing(statements)) {
                if (router) {
                    Execution.runProgram(q, runtime, Execution.Source.given(session), listener, referee, options,
                            capture::of);
                } else {
                    BodyDriver.execute(unreadAsStatements(q), runtime, backend.dialect().forServer(session.version()), session, listener,
                            referee, options, capture::of);
                }
            } catch (RuntimeException e) {
                outcome = e.getClass().getSimpleName() + ": " + e.getMessage();
            }
            long[] after = StatementOrigin.snapshot();
            long[] delta = new long[after.length];
            for (int i = 0; i < delta.length; i++) {
                delta[i] = after[i] - before[i];
            }
            List<String> ledger = new ArrayList<>();
            recorder.entries().forEach(e -> ledger.add(String.valueOf(e)));
            java.util.SortedMap<String, Long> census = new java.util.TreeMap<>();
            com.legend.judge.CanonicalDivergence.sqlCensus().forEach((k, v) -> {
                long d = v - censusBefore.getOrDefault(k, 0L);
                if (d != 0) {
                    census.put(k, d);
                }
            });
            return new Run(outcome, ledger, capture.toString(), Arrays.toString(delta),
                    Census.count(Census.Key.EFFECTS_IN_SCRIPT) - effectsBefore, database(c, check),
                    Census.count(Census.Key.BODIES_MOVED) - movedBefore, statements, verdicts, census.toString());
        }
    }

    private static String database(Connection c, String check) {
        StringBuilder rows = new StringBuilder();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(check)) {
            while (rs.next()) {
                rows.append(rs.getString(1)).append(',');
            }
        } catch (java.sql.SQLException e) {
            rows.append("refused: ").append(e.getMessage().lines().findFirst().orElse(""));
        }
        return rows.toString();
    }

    /** The caller's answer sink: the shape asked, the columns' facts and every row. */
    /** The referee of the engine-text witnesses: the golden replayed on the body's OWN connection (the texts the
     *  witnesses use run on every in-process backend), its rows against ours as sorted text multisets -- a test
     *  double of the SPI, not the harness's comparison policy (spec's {@code ReplayReferee}); a golden the connection
     *  refuses is a decline. The generator's fetch entries are not exercised by any witness here. */
    private static final class StubReferee implements com.legend.judge.SqlReplayReferee {
        private final Connection c;

        StubReferee(Connection c) {
            this.c = c;
        }

        @Override
        public RefereeRows rows(String sql) {
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
                int n = rs.getMetaData().getColumnCount();
                List<String> labels = new ArrayList<>();
                List<Integer> types = new ArrayList<>();
                for (int i = 1; i <= n; i++) {
                    labels.add(rs.getMetaData().getColumnLabel(i));
                    types.add(rs.getMetaData().getColumnType(i));
                }
                List<List<Object>> rows = new ArrayList<>();
                while (rs.next()) {
                    List<Object> cells = new ArrayList<>();
                    for (int i = 1; i <= n; i++) {
                        cells.add(rs.getObject(i));
                    }
                    rows.add(cells);
                }
                return new RefereeRows(labels, types, rows);
            } catch (java.sql.SQLException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public RowVerdict verifyFetchTexts(Sessions.Session session, String goldenSql, String ourSql) {
            throw new UnsupportedOperationException("the engine-text witnesses hold no generator fetch");
        }

        @Override
        public RowVerdict verifyFetchChain(Sessions.Session session, int hopIndex, String goldenSql, String ourSql,
                java.util.function.Supplier<FetchTranscript> transcript) {
            throw new UnsupportedOperationException("the engine-text witnesses hold no generator fetch");
        }

        @Override
        public RowVerdict verify(Sessions.Session session, String goldenSql, com.legend.judge.TypedSide ours,
                @com.legend.base.Nullable String mappingFqn, @com.legend.base.Nullable String rootClassFqn,
                ReplayFacts facts, ModelContext ctx) {
            List<String> golden = new ArrayList<>();
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(goldenSql)) {
                int n = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<String> cells = new ArrayList<>();
                    for (int i = 1; i <= n; i++) {
                        cells.add(String.valueOf(rs.getObject(i)));
                    }
                    golden.add(String.join("|", cells));
                }
            } catch (java.sql.SQLException e) {
                return RowVerdict.declined("golden execution: " + e.getMessage());
            }
            List<String> mine = new ArrayList<>();
            for (com.legend.judge.Row r : ours.rows()) {
                List<String> cells = new ArrayList<>();
                for (Object v : r.values()) {
                    cells.add(String.valueOf(v));
                }
                mine.add(String.join("|", cells));
            }
            java.util.Collections.sort(golden);
            java.util.Collections.sort(mine);
            return golden.equals(mine) ? RowVerdict.match()
                    : RowVerdict.diverged("golden rows " + golden + ", ours " + mine);
        }
    }

    private static final class Capture implements ResultSink {
        private final StringBuilder seen = new StringBuilder();

        ResultSink of(ExecutionPlan.TdsResult.Shape shape) {
            seen.append("shape=").append(shape).append(' ');
            return this;
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) {
            for (ExecutionPlan.Column c : columns) {
                seen.append(c.name()).append(':').append(c.type()).append(c.multiplicity()).append(' ');
            }
        }

        @Override
        public void batch(Batch batch) {
            for (int i = 0; i < batch.size(); i++) {
                seen.append(" row").append(batch.row(i));
            }
        }

        @Override
        public void end() {
            seen.append(" end");
        }

        @Override
        public String toString() {
            return seen.toString();
        }
    }
}
