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
        ModelContext ctx = Compiler.compileModel(model(backend, false));
        assertNotNull(BodyClass.of(typed(ctx, body), RT), "the body is a moved class's: " + body);
        Run walked = run(backend, ctx, body, false, check);
        Run moved = run(backend, ctx, body, true, check);
        assertEquals(walked.outcome, moved.outcome, "outcome: " + body);
        assertEquals(walked.ledger, moved.ledger, "the raw-SQL ledger: " + body);
        assertEquals(walked.answer, moved.answer, "the answer: " + body);
        assertEquals(walked.origins, moved.origins, "the statement-origin census: " + body);
        assertEquals(walked.effects, moved.effects, "the effects census: " + body);
        assertEquals(walked.database, moved.database, "the database left: " + body);
        assertEquals(walked.statements, moved.statements, "the statements sent (origin and text, in order): " + body);
        assertEquals(walked.moved + 1, moved.moved, "the router moved the body: " + body);
    }

    private record Run(String outcome, List<String> ledger, String answer, String origins, long effects,
            String database, long moved, List<String> statements) {
    }

    private static Run run(TestBackend backend, ModelContext ctx, String body, boolean router, String check)
            throws Exception {
        try (Connection c = backend.open()) {
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
            try (var observing = StatementOrigin.observing(statements)) {
                if (router) {
                    Execution.runProgram(q, RT, Execution.Source.given(session), null, null, options, capture::of);
                } else {
                    BodyDriver.execute(q, RT, backend.dialect().forServer(session.version()), session, null, null,
                            options, capture::of);
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
            return new Run(outcome, ledger, capture.toString(), Arrays.toString(delta),
                    Census.count(Census.Key.EFFECTS_IN_SCRIPT) - effectsBefore, database(c, check),
                    Census.count(Census.Key.BODIES_MOVED) - movedBefore, statements);
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
