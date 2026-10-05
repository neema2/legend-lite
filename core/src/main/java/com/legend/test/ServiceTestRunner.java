// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.test;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.PureModelContext;
import com.legend.exec.ExecutionResult;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import com.legend.model.DataDefinition;
import com.legend.model.ImportScope;
import com.legend.model.RuntimeDefinition;
import com.legend.model.ServiceDefinition;
import com.legend.protocol.Protocol;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.values.PureDateLiteral;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE SERVICE TEST-SUITE RUNNER — runs a {@code Service}'s {@code testSuites}
 * through the platform and returns one {@link Result} per atomic test. The
 * rules are the engine's testable framework's ({@code ServiceTestRunner} /
 * {@code TestRuntimeBuilder} / {@code TestAssertionEvaluator}, 4.145.0),
 * implemented here from the spec; docs/DEFERRED_TEST_EXECUTION.md is the
 * charter. The product surface: a user with test suites on services is the
 * caller; the stress corpus is one.
 *
 * <p>The engine's design, followed exactly: a suite runs against a TEST
 * RUNTIME — the service's mappings with every store bound to a connection
 * that CARRIES the suite's data — and the platform loads that data when it
 * establishes the connection. Here the test runtime is the platform's
 * execution overlay ({@link PureModelContext#withExecutionOverlay}): one
 * {@code LocalH2}-shaped connection whose {@code testDataSetupCsv} is the
 * suite's provisioned CSV, seeded by the platform's own establish step. This
 * runner opens and closes sessions and hands them to the platform; it
 * executes no SQL of its own (tenet #1), like {@link PureTestRunner}.
 *
 * <ul>
 *   <li><b>provisioning</b>: {@code data: [ connections: [ id: ... ] ]}
 *       addresses the runtime's connection by its id ({@code store: [ id:
 *       conn ]}); the compact form's resolvers address the STORE. A
 *       {@code Reference} resolves to the {@code ###Data} element's body;
 *       {@code Relational} CSV becomes the test connection's data. Any other
 *       data kind is SKIPPED, loudly;</li>
 *   <li><b>sessions</b> ({@link Sessions}): {@code FRESH_PER_TEST} opens a
 *       new session for every test — the engine's shape, a fresh database per
 *       run, seeded on establishment; {@code SHARED} keeps one seeded session
 *       per distinct provisioning for every test whose program has no
 *       statement effects (an effectful body still gets a private session);</li>
 *   <li><b>execution</b>: parameters bind as query VARIABLES ({@code let}
 *       statements ahead of the body — never text substitution); the body
 *       executes through the ONE production entry
 *       ({@link Compiler#executeResolved}) against the test runtime;</li>
 *   <li><b>serialization</b>: {@code PURE_TDSOBJECT} (and {@code RAW})
 *       render a tabular result as one object per row keyed by column; a
 *       graph result is the JSON envelope the platform produced
 *       ({@code {"builder":{"_type":"json"},"values":[...]}}, the engine's
 *       DEFAULT for a serialize root); a scalar or collection renders as
 *       its value(s); DEFAULT on a tabular result is SKIPPED, loudly;</li>
 *   <li><b>judgment</b>: {@code EqualToJson} through {@link TestAssertions}
 *       (null ≡ missing, unordered arrays, exact decimals). Other assertion
 *       kinds are SKIPPED, loudly — never a silent pass.</li>
 * </ul>
 */
public final class ServiceTestRunner implements AutoCloseable {

    public enum Status { PASS, FAIL, SKIPPED }

    /** How sessions are handed out. */
    public enum Sessions {
        /** A new session per test, seeded by the platform on establishment —
         *  the engine's own shape (a fresh test database per run). */
        FRESH_PER_TEST,
        /** One seeded session per distinct provisioning, shared by every
         *  read-only test; a body with statement effects gets its own. */
        SHARED
    }

    /** One atomic test's outcome. {@code reason} names the first failed
     *  assertion's difference, the skip's cause, or the phase that raised. */
    public record Result(String serviceFqn, String suiteId, String testId, Status status,
                         String reason, long millis) {
        public boolean pass() {
            return status == Status.PASS;
        }
    }

    private final PureModelContext ctx;
    private final PureTestRunner.Sessions opener;
    private final Sessions policy;
    /** The database the opener's sessions are: the test connection declares
     *  it, so the platform picks that dialect (an H2 session refuses a
     *  runtime whose connections declare anything else). */
    private final ConnectionDefinition.DatabaseType sessionType;
    /** test runtime name → the shared session (SHARED policy). */
    private final Map<String, Connection> shared = new LinkedHashMap<>();
    /** the runtime and its provisioning, BY VALUE → the test runtime (an
     *  overlay context + its runtime name). Value records, never a hash of
     *  the data: two suites whose CSVs differ but hash alike ("Aa"/"BB")
     *  used to share one runtime and the second ran on the first's rows
     *  (rebuild W0.6 push 8). */
    private final Map<RuntimeKey, TestRuntime> runtimes = new LinkedHashMap<>();

    /** One CSV table of a provisioning, by value (no source position: the
     *  same data declared twice, inline or through a {@code ###Data}
     *  reference, is the same provisioning). */
    private record CsvTableKey(String schema, String table, String values) {
    }

    /** A provisioning unit by value: the store and its tables. */
    private record ProvisionKey(String store, List<CsvTableKey> tables) {
    }

    /** A test runtime's identity: the service runtime and every provisioning. */
    private record RuntimeKey(String runtimeFqn, List<ProvisionKey> provisions) {
    }

    /** A suite's test runtime: the overlay context that resolves it, by name. */
    private record TestRuntime(PureModelContext ctx, String runtimeFqn) {
    }

    /** One test's computed answer, as its serialization format spells it,
     *  before any assertion judges it (rebuild D23: the rows a second engine
     *  is compared with). */
    public record Rows(String serviceFqn, String suiteId, String testId,
                       @com.legend.base.Nullable Object actual) {
    }

    /** Where every test's computed answer is offered before judging; null
     *  when nobody asked. */
    private final java.util.function.@com.legend.base.Nullable Consumer<Rows> rowsSink;

    public ServiceTestRunner(ModelContext ctx, PureTestRunner.Sessions opener, Sessions policy,
            ConnectionDefinition.DatabaseType sessionType) {
        this(ctx, opener, policy, sessionType, null);
    }

    public ServiceTestRunner(ModelContext ctx, PureTestRunner.Sessions opener, Sessions policy,
            ConnectionDefinition.DatabaseType sessionType,
            java.util.function.@com.legend.base.Nullable Consumer<Rows> rowsSink) {
        this.rowsSink = rowsSink;
        if (!(ctx instanceof PureModelContext pmc)) {
            throw new IllegalArgumentException("a service test runtime is an execution overlay"
                    + " on the compiled model; got " + ctx.getClass().getSimpleName());
        }
        this.ctx = pmc;
        this.opener = opener;
        this.policy = policy;
        this.sessionType = sessionType;
    }

    /** A provisioning unit (the plan's: {@link TestPlan#provisions}) by value. */
    private static ProvisionKey key(com.legend.testable.TestPlan.Provision p) {
        List<CsvTableKey> tables = new java.util.ArrayList<>(p.data().tables().size());
        for (Protocol.PRelationalCsvTable t : p.data().tables()) {
            tables.add(new CsvTableKey(t.schema(), t.table(), t.values()));
        }
        return new ProvisionKey(p.store(), List.copyOf(tables));
    }

    // ---- RUN -----------------------------------------------------------------

    /** Every atomic test of every suite of {@code svc}, in declaration order.
     *  A service without suites yields no result. */
    public List<Result> run(ServiceDefinition svc) {
        List<Result> out = new ArrayList<>();
        if (svc.testSuites() == null) {
            return out;
        }
        for (Protocol.PServiceTestSuite suite : svc.testSuites()) {
            for (Protocol.PServiceTestSuite.PSuiteTest test : suite.tests()) {
                long t0 = System.nanoTime();
                Result r;
                try {
                    r = runOne(svc, suite, test);
                } catch (com.legend.testable.TestPlan.Skip s) {
                    r = new Result(svc.qualifiedName(), suite.id(), test.id(), Status.SKIPPED,
                            String.valueOf(s.getMessage()), 0);
                } catch (RuntimeException | SQLException e) {
                    r = new Result(svc.qualifiedName(), suite.id(), test.id(), Status.FAIL,
                            "harness: " + e.getClass().getSimpleName() + ": "
                                    + PureTestRunner.whole(e.getMessage()), 0);
                }
                out.add(new Result(r.serviceFqn(), r.suiteId(), r.testId(), r.status(),
                        r.reason(), (System.nanoTime() - t0) / 1_000_000L));
            }
        }
        return out;
    }

    private Result runOne(ServiceDefinition svc, Protocol.PServiceTestSuite suite,
            Protocol.PServiceTestSuite.PSuiteTest test) throws SQLException {
        String runtimeFqn = com.legend.testable.TestPlan.runtimeOf(svc);
        RuntimeDefinition runtime = ctx.findRuntime(runtimeFqn).orElseThrow(
                () -> new com.legend.testable.TestPlan.Skip("runtime '" + runtimeFqn + "' is not in the model"));
        List<com.legend.testable.TestPlan.Provision> provisions = com.legend.testable.TestPlan.provisions(ctx, suite, runtime);
        TestRuntime rt = testRuntime(runtime, provisions);

        // the program: parameters as let-bound variables, then the body
        List<ValueSpecification> statements = new ArrayList<>();
        if (test.parameters() != null) {
            for (Protocol.PServiceTestSuite.PSuiteParam p : test.parameters()) {
                statements.add(new AppliedFunction("letFunction",
                        List.of(new CString(p.name()), p.value())));
            }
        }
        statements.addAll(body(svc.functionBody()));
        ValueSpecification resolved;
        try {
            resolved = Compiler.resolveQuery(statements, new ImportScope(List.of()), rt.ctx());
        } catch (RuntimeException e) {
            return fail(svc, suite, test, "resolve: " + PureTestRunner.whole(e.getMessage()));
        }
        boolean privateSession = policy == Sessions.FRESH_PER_TEST
                || Compiler.hasStatementEffects(resolved, rt.ctx());
        Connection conn = privateSession ? opener.open() : shared(rt.runtimeFqn());
        try {
            ExecutionResult result;
            try {
                result = Execution.executeResolved(resolved, rt.ctx(), rt.runtimeFqn(), conn);
            } catch (RuntimeException e) {
                if (System.getenv("LEGEND_LITE_STACKS") != null) {
                    e.printStackTrace();   // the same diagnostic switch the resolver walls honor
                }
                return fail(svc, suite, test, "execute: " + PureTestRunner.whole(e.getMessage()));
            }
            if (result == null) {
                return fail(svc, suite, test, "execute: the platform produced no result");
            }
            Object actual = serialize(result, test.serializationFormat());
            if (rowsSink != null) {
                rowsSink.accept(new Rows(svc.qualifiedName(), suite.id(), test.id(), actual));
            }
            for (Protocol.PTestAssertion a : test.assertions()) {
                String diff = judge(a, actual);
                if (diff != null) {
                    return fail(svc, suite, test, a.id() + ": " + diff);
                }
            }
            return new Result(svc.qualifiedName(), suite.id(), test.id(), Status.PASS,
                    test.assertions().size() + " assertion(s)", 0);
        } finally {
            if (privateSession) {
                conn.close();
            }
        }
    }

    private static Result fail(ServiceDefinition svc, Protocol.PServiceTestSuite suite,
            Protocol.PServiceTestSuite.PSuiteTest test, String reason) {
        return new Result(svc.qualifiedName(), suite.id(), test.id(), Status.FAIL, reason, 0);
    }

    private static List<ValueSpecification> body(ValueSpecification fb) {
        return fb instanceof LambdaFunction lf && lf.parameters().isEmpty()
                ? lf.body() : List.of(fb);
    }

    // ---- PROVISIONING → THE TEST RUNTIME -----------------------------------------

    /** The suite's TEST RUNTIME: the service runtime's mappings, its one
     *  provisioned store bound to a connection carrying the CSV as declared
     *  test data (the {@code LocalH2 { testDataSetupCSV }} shape the platform
     *  seeds on establishment). One per distinct provisioning; overlays are
     *  allocation-cheap views of the compiled model. */
    private TestRuntime testRuntime(RuntimeDefinition runtime, List<com.legend.testable.TestPlan.Provision> provisions) {
        RuntimeKey key = new RuntimeKey(runtime.qualifiedName(),
                provisions.stream().map(ServiceTestRunner::key).toList());
        TestRuntime cached = runtimes.get(key);
        if (cached != null) {
            return cached;
        }
        if (provisions.isEmpty()) {
            TestRuntime plain = new TestRuntime(ctx, runtime.qualifiedName());
            runtimes.put(key, plain);
            return plain;
        }
        String store = provisions.get(0).store();
        for (com.legend.testable.TestPlan.Provision p : provisions) {
            if (!p.store().equals(store)) {
                throw new com.legend.testable.TestPlan.Skip("provisioning spans several stores (" + store + ", " + p.store()
                        + "); the test runtime binds one");
            }
        }
        if (ctx.findDatabase(store).isEmpty()) {
            throw new com.legend.testable.TestPlan.Skip("store '" + store + "' is not a database in the model");
        }
        StringBuilder csv = new StringBuilder();
        for (com.legend.testable.TestPlan.Provision p : provisions) {
            for (Protocol.PRelationalCsvTable t : p.data().tables()) {
                if (csv.length() > 0) {
                    csv.append("\n-\n");
                }
                csv.append(t.schema()).append('\n').append(t.table()).append('\n')
                        .append(t.values());
            }
        }
        // the '$' sigil: a name no user can write, so the overlay shadows
        // nothing; the ordinal is unique within this runner (a hash of the
        // key was not: colliding keys shared a name and, under SHARED, a
        // session)
        String rtName = runtime.qualifiedName() + "$test$" + runtimes.size();
        String connName = rtName + "$conn";
        ConnectionDefinition conn = new ConnectionDefinition(connName, store, sessionType,
                new ConnectionSpecification.LocalH2(null, csv.toString(), null),
                new AuthenticationSpec.TestAuth());
        Map<String, String> ids = new LinkedHashMap<>();
        runtime.connectionIds().forEach((id, s) -> ids.put(id, s));
        RuntimeDefinition rt = new RuntimeDefinition(rtName, runtime.mappings(),
                Map.of(store, List.of(connName)), List.of(), List.of(), ids);
        TestRuntime built = new TestRuntime(ctx.withExecutionOverlay(rt, conn), rtName);
        runtimes.put(key, built);
        return built;
    }

    private Connection shared(String testRuntimeFqn) throws SQLException {
        Connection conn = shared.get(testRuntimeFqn);
        if (conn == null) {
            conn = opener.open();
            shared.put(testRuntimeFqn, conn);
        }
        return conn;
    }

    // ---- SERIALIZE + JUDGE ----------------------------------------------------

    /** The result as the JSON tree its serialization format produces. */
    static @com.legend.base.Nullable Object serialize(ExecutionResult result,
            @com.legend.base.Nullable String format) {
        String fmt = format == null ? "DEFAULT" : format;
        return switch (result) {
            case ExecutionResult.Graph g -> {
                Object tree = com.legend.sql.Json.parse(g.json());
                if (tree instanceof Map<?, ?> m && m.containsKey("builder")) {
                    yield tree;
                }
                Map<String, Object> env = new LinkedHashMap<>();
                env.put("builder", Map.of("_type", "json"));
                env.put("values", tree);
                yield env;
            }
            case ExecutionResult.Tabular t -> {
                if (!fmt.equals("PURE_TDSOBJECT") && !fmt.equals("RAW")) {
                    throw new com.legend.testable.TestPlan.Skip("serialization format " + fmt
                            + " of a tabular result is not rendered by this runner");
                }
                List<Object> rows = new ArrayList<>(t.rows().size());
                for (var row : t.rows()) {
                    Map<String, Object> o = new LinkedHashMap<>();
                    for (int i = 0; i < t.columns().size(); i++) {
                        o.put(t.columns().get(i).name(), cell(row.values().get(i)));
                    }
                    rows.add(o);
                }
                yield rows;
            }
            case ExecutionResult.Scalar s -> cell(s.value());
            case ExecutionResult.Collection c -> {
                List<Object> vs = new ArrayList<>(c.values().size());
                c.values().forEach(v -> vs.add(cell(v)));
                yield vs;
            }
            case ExecutionResult.TdsText tt -> throw new com.legend.testable.TestPlan.Skip(
                    "a TDS-text result is not rendered by this runner");
        };
    }

    /** A result cell as a JSON tree leaf (the engine's value transformer:
     *  dates as their engine string, numbers as numbers, the rest as-is). */
    private static @com.legend.base.Nullable Object cell(@com.legend.base.Nullable Object v) {
        return switch (v) {
            case null -> null;
            case PureDateLiteral d -> d.toEngineJson();   // the engine's JSON spelling: nanos + "+0000" on time-bearing values
            case Map<?, ?> m -> {
                Map<String, Object> o = new LinkedHashMap<>();
                m.forEach((k, x) -> o.put(String.valueOf(k), cell(x)));
                yield o;
            }
            case List<?> l -> {
                List<Object> o = new ArrayList<>(l.size());
                l.forEach(x -> o.add(cell(x)));
                yield o;
            }
            case Number n -> n;
            case Boolean b -> b;
            case String s -> s;
            default -> String.valueOf(v);
        };
    }

    private static @com.legend.base.Nullable String judge(Protocol.PTestAssertion a,
            @com.legend.base.Nullable Object actual) {
        // the plan's reading of the assertion: its expected JSON, or why this kind is not judged
        com.legend.testable.TestPlan.Assertion planned = com.legend.testable.TestPlan.assertion(a);
        String expectedJson = planned.expectedJson();
        if (expectedJson == null) {
            throw new com.legend.testable.TestPlan.Skip(String.valueOf(planned.skipped()));
        }
        return judgeJson(expectedJson, actual);
    }

    /** {@code EqualToJson}, the expected side as its text: null when equal, else the difference. */
    public static @com.legend.base.Nullable String judgeJson(String expectedJson, @com.legend.base.Nullable Object actual) {
        Object expected = null;
        String unreadable = null;
        try {
            expected = com.legend.sql.Json.parse(expectedJson);
        } catch (RuntimeException e) {
            unreadable = "expected JSON does not parse: " + e.getMessage();
        }
        return unreadable != null ? unreadable : TestAssertions.equalToJson(expected, actual);
    }

    // ---- SESSIONS -------------------------------------------------------------

    /** The shared sessions opened so far, by test runtime. */
    public Map<String, Connection> sessions() {
        return Collections.unmodifiableMap(shared);
    }

    @Override
    public void close() {
        for (Connection c : shared.values()) {
            try {
                c.close();
            } catch (SQLException ignored) {
                // a session that fails to close cannot poison the next
            }
        }
        shared.clear();
    }
}
