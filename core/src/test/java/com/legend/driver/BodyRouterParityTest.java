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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
                        + " #TDS\na\n1\n2\n#->limit('1'->parseInteger()));}")) {
            assertParity(backend, body, "select 1");
        }
    }

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
                // a read of the system metamodel store (M4)
                "{| meta::pure::mapping::Mapping.all()->project(~[n:m|$m.name]);}",
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

    /** A store, a mapping and a runtime with NO test data on {@code backend} (a seeded runtime is left on the walk): the
     *  frames' model. The table is the harness's to create. */
    private static String frameModel(TestBackend backend) {
        return """
                Class x::Firm { name: String[1]; }
                ###Relational
                Database x::DB ( Table FIRM (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
                ###Mapping
                Mapping x::M ( *x::Firm: Relational { ~mainTable [x::DB] FIRM name: [x::DB] FIRM.NAME } )
                ###Connection
                RelationalDatabaseConnection x::Conn
                {
                    store: x::DB;
                    type: %s;
                    specification: LocalH2 { };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime x::RT { mappings: [x::M]; connections: [ x::DB: [ env: x::Conn ] ]; }
                """.formatted(backend.type().name());
    }

    private static final List<String> FIRM_ROWS = List.of("create table FIRM (ID integer primary key, NAME varchar(32))",
            "insert into FIRM values (1, 'Acme'), (2, 'Bolt')");
    private static final String EXECUTE = "meta::pure::router::execute(|x::Firm.all()->project([f|$f.name], ['name']),"
            + " x::M, x::RT.runtimeValue, [])";

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
                        + "'insert into FIRM values (3, \\'Cyl\\')', $c, 0, 1000); assertSize($r.values.rows, 3);}")) {
            assertParity(backend, ctx, "x::RT", body, "select count(*) from FIRM", FIRM_ROWS);
        }
    }

    /** The frame shapes M9a leaves on the walk, each by name: an alias of a frame, an inline execute inside an
     *  assert, a read of the frame's activities, a seeded runtime. */
    @Test
    void theFrameShapesTheRuleKeepsOnTheWalk() {
        TestBackend backend = TestBackends.platform();
        ModelContext ctx = Compiler.compileModel(frameModel(backend));
        for (String body : List.of(
                // a read of the frame's activities (the run's comment)
                "{| let r = " + EXECUTE + "; assertSize($r.activities, 1);}",
                // the one inline shape the splice runs at planning: size(execute(...))
                "{| assertEquals(1, " + EXECUTE + "->size());}",
                // an effect let (its value a raw grid)
                "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                        + "type=meta::relational::runtime::DatabaseType." + backend.type().name() + "); let x = " + EXEC
                        + "'insert into FIRM values (3, \\'Cyl\\')', $c, 0, 1000); let r = " + EXECUTE
                        + "; assertSize($r.values.rows, 3);}",
                // a frame with no assert after it is no assert program
                "{| let r = " + EXECUTE + "; $r.values.rows->size();}")) {
            assertNull(BodyClass.of(typed(ctx, body), "x::RT", true), body);
        }
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
                // a quantified assert (a map over asserts)
                "{| [1, 2]->map(x|assertEquals($x, $x)); true;}")) {
            ModelContext ctx = Compiler.compileModel(model(backend, false));
            assertEquals(BodyClass.ASSERT_PROGRAM, BodyClass.of(typed(ctx, body), RT, true), body);
            Run walked = run(backend, ctx, body, false, "select 1");
            Run routed = run(backend, ctx, body, true, "select 1");
            assertEquals(walked.moved, routed.moved, "the router left the body on the walk: " + body);
            assertEquals(walked.outcome, routed.outcome, "outcome: " + body);
            assertEquals(walked.verdicts, routed.verdicts, "the verdicts: " + body);
            assertEquals(walked.census, routed.census, "the verdict census: " + body);
        }
        // a type named as an argument is a packageable reference the class rule leaves on the walk (the query
        // program's list), so the class test by a type reference never reaches the host checks
        ModelContext ctx = Compiler.compileModel(model(backend, false));
        assertNull(BodyClass.of(typed(ctx, "{| assertInstanceOf(1, Integer); assertInstanceOf('a', String);}"), RT,
                true));
    }

    private static String model(TestBackend backend, boolean seeded) {
        if (!seeded) {
            return backend.withStorelessRuntime("");
        }
        return StorelessRuntime.declaring("", backend.type(),
                "specification: LocalH2 { testDataSetupSqls: [ 'create table if not exists seed_t(i int)',"
                        + " 'delete from seed_t', 'insert into seed_t values (1)' ]; };\n  auth: DefaultH2");
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
        assertNotNull(BodyClass.of(typed(ctx, body), runtime, true),
                "the body is a moved class's under the host judge: " + body);
        Run walked = run(backend, ctx, runtime, body, false, check, setup);
        Run moved = run(backend, ctx, runtime, body, true, check, setup);
        assertEquals(walked.outcome, moved.outcome, "outcome: " + body);
        assertEquals(walked.ledger, moved.ledger, "the raw-SQL ledger: " + body);
        assertEquals(walked.answer, moved.answer, "the answer: " + body);
        assertEquals(walked.origins, moved.origins, "the statement-origin census: " + body);
        assertEquals(walked.effects, moved.effects, "the effects census: " + body);
        assertEquals(walked.database, moved.database, "the database left: " + body);
        assertEquals(walked.statements, moved.statements, "the statements sent (origin and text, in order): " + body);
        assertEquals(walked.verdicts, moved.verdicts, "the verdicts the listener heard: " + body + " (" + walked.outcome
                + ")");
        assertEquals(walked.census, moved.census, "the verdict census: " + body + " (" + walked.outcome + ")");
        assertEquals(walked.moved + 1, moved.moved, "the router moved the body: " + body);
    }

    private record Run(String outcome, List<String> ledger, String answer, String origins, long effects,
            String database, long moved, List<String> statements, List<String> verdicts, String census) {
    }

    private static Run run(TestBackend backend, ModelContext ctx, String body, boolean router, String check)
            throws Exception {
        return run(backend, ctx, RT, body, router, check, List.of());
    }

    private static Run run(TestBackend backend, ModelContext ctx, String runtime, String body, boolean router,
            String check, List<String> setup) throws Exception {
        try (Connection c = backend.open()) {
            try (Statement st = c.createStatement()) {
                for (String sql : setup) {
                    st.execute(sql);
                }
            }
            TypedQuery q = typed(ctx, body);
            RawSqlBoundary.Recorder recorder = new RawSqlBoundary.Recorder();
            ExecuteOptions options = ExecuteOptions.recording(recorder, null);
            Capture capture = new Capture();
            Sessions.Session session = Sessions.borrowed(c);
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
            };
            java.util.SortedMap<String, Long> censusBefore = new java.util.TreeMap<>(
                    com.legend.judge.CanonicalDivergence.sqlCensus());
            try (var observing = StatementOrigin.observing(statements)) {
                if (router) {
                    Execution.runProgram(q, runtime, Execution.Source.given(session), listener, null, options,
                            capture::of);
                } else {
                    BodyDriver.execute(q, runtime, backend.dialect().forServer(session.version()), session, listener,
                            null, options, capture::of);
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
