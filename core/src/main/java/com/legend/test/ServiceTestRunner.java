// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.test;

import com.legend.Compiler;
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

    /** A test the runner cannot judge: the reason travels as the result. */
    private static final class Skip extends RuntimeException {
        Skip(String why) {
            super(why);
        }
    }

    private final PureModelContext ctx;
    private final PureTestRunner.Sessions opener;
    private final Sessions policy;
    /** The database the opener's sessions are: the test connection declares
     *  it, so the platform picks that dialect (an H2 session refuses a
     *  runtime whose connections declare anything else). */
    private final ConnectionDefinition.DatabaseType sessionType;
    /** provisioning identity → the shared session (SHARED policy). */
    private final Map<String, Connection> shared = new LinkedHashMap<>();
    /** provisioning identity → the test runtime (an overlay context + its runtime name). */
    private final Map<String, TestRuntime> runtimes = new LinkedHashMap<>();

    /** A suite's test runtime: the overlay context that resolves it, by name. */
    private record TestRuntime(PureModelContext ctx, String runtimeFqn) {
    }

    public ServiceTestRunner(ModelContext ctx, PureTestRunner.Sessions opener, Sessions policy,
            ConnectionDefinition.DatabaseType sessionType) {
        if (!(ctx instanceof PureModelContext pmc)) {
            throw new IllegalArgumentException("a service test runtime is an execution overlay"
                    + " on the compiled model; got " + ctx.getClass().getSimpleName());
        }
        this.ctx = pmc;
        this.opener = opener;
        this.policy = policy;
        this.sessionType = sessionType;
    }

    /** One provisioning unit: the store and the CSV data that seeds it. */
    private record Provision(String store, Protocol.PRelationalCsvData data, String identity) {
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
                } catch (Skip s) {
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
        String runtimeFqn = runtimeOf(svc);
        RuntimeDefinition runtime = ctx.findRuntime(runtimeFqn).orElseThrow(
                () -> new Skip("runtime '" + runtimeFqn + "' is not in the model"));
        List<Provision> provisions = provisions(suite, runtime);
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
                result = Compiler.executeResolved(resolved, rt.ctx(), rt.runtimeFqn(), conn);
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

    private static String runtimeOf(ServiceDefinition svc) {
        if (svc.runtimeRef() != null) {
            return svc.runtimeRef();
        }
        if (svc.multiExecution() != null) {
            throw new Skip("multi-execution service: the test's keys select an environment,"
                    + " which this runner does not bind yet");
        }
        throw new Skip("service names no runtime");
    }

    private static List<ValueSpecification> body(ValueSpecification fb) {
        return fb instanceof LambdaFunction lf && lf.parameters().isEmpty()
                ? lf.body() : List.of(fb);
    }

    // ---- PROVISIONING → THE TEST RUNTIME -----------------------------------------

    private List<Provision> provisions(Protocol.PServiceTestSuite suite, RuntimeDefinition runtime) {
        List<Provision> out = new ArrayList<>();
        Protocol.PServiceTestSuite.PSuiteData data = suite.testData();
        if (data == null) {
            return out;
        }
        for (Protocol.PServiceTestSuite.PSuiteConnData cd : data.connectionsTestData()) {
            String store = runtime.connectionIds().get(cd.id());
            if (store == null) {
                if (runtime.connectionBindings().size() == 1) {
                    store = runtime.connectionBindings().keySet().iterator().next();
                } else {
                    throw new Skip("connection id '" + cd.id() + "' is not bound by runtime '"
                            + runtime.qualifiedName() + "'");
                }
            }
            addProvision(out, store, cd.data());
        }
        if (data.serviceTestData() != null) {
            for (Protocol.PServiceTestSuite.PResolverData rd : data.serviceTestData()) {
                if (rd.data() != null) {
                    addProvision(out, rd.elementPath(), rd.data());
                } else {
                    // referenceDataResolver: the element's own resolvers name the stores
                    DataDefinition dd = ctx.findData(rd.elementPath()).orElseThrow(
                            () -> new Skip("data element '" + rd.elementPath()
                                    + "' is not in the model"));
                    addResolvers(out, dd);
                }
            }
        }
        return out;
    }

    private void addResolvers(List<Provision> out, DataDefinition dd) {
        if (dd.body().resolvers().isEmpty()) {
            throw new Skip("data element '" + dd.qualifiedName()
                    + "' names no store to provision (no resolvers)");
        }
        for (Protocol.PDataResolver r : dd.body().resolvers()) {
            if (r.data() == null) {
                DataDefinition inner = ctx.findData(r.elementPointer().path()).orElseThrow(
                        () -> new Skip("data element '" + r.elementPointer().path()
                                + "' is not in the model"));
                addResolvers(out, inner);
            } else {
                addProvision(out, r.elementPointer().path(), r.data());
            }
        }
    }

    /** Resolves references down to a concrete value and records it. */
    private void addProvision(List<Provision> out, String store, Protocol.PEmbeddedDataValue v) {
        Protocol.PEmbeddedDataValue value = v;
        StringBuilder identity = new StringBuilder(store).append('=');
        while (value instanceof Protocol.PDataReference ref) {
            String path = ref.dataElement().path();
            identity.append("ref:").append(path).append('>');
            DataDefinition dd = ctx.findData(path).orElseThrow(
                    () -> new Skip("data element '" + path + "' is not in the model"));
            if (dd.body().value() == null) {
                // a resolver-form element: its own store keys apply
                addResolvers(out, dd);
                return;
            }
            value = dd.body().value();
        }
        if (!(value instanceof Protocol.PRelationalCsvData csv)) {
            throw new Skip("embedded data kind '"
                    + value.getClass().getSimpleName().substring(1)
                    + "' is not provisioned by this runner");
        }
        identity.append("csv:");
        for (Protocol.PRelationalCsvTable t : csv.tables()) {
            identity.append(t.schema()).append('.').append(t.table()).append(':')
                    .append(t.values().hashCode()).append(';');
        }
        out.add(new Provision(store, csv, identity.toString()));
    }

    /** The suite's TEST RUNTIME: the service runtime's mappings, its one
     *  provisioned store bound to a connection carrying the CSV as declared
     *  test data (the {@code LocalH2 { testDataSetupCSV }} shape the platform
     *  seeds on establishment). One per distinct provisioning; overlays are
     *  allocation-cheap views of the compiled model. */
    private TestRuntime testRuntime(RuntimeDefinition runtime, List<Provision> provisions) {
        StringBuilder key = new StringBuilder(runtime.qualifiedName()).append('|');
        provisions.forEach(p -> key.append(p.identity()).append('|'));
        TestRuntime cached = runtimes.get(key.toString());
        if (cached != null) {
            return cached;
        }
        if (provisions.isEmpty()) {
            TestRuntime plain = new TestRuntime(ctx, runtime.qualifiedName());
            runtimes.put(key.toString(), plain);
            return plain;
        }
        String store = provisions.get(0).store();
        for (Provision p : provisions) {
            if (!p.store().equals(store)) {
                throw new Skip("provisioning spans several stores (" + store + ", " + p.store()
                        + "); the test runtime binds one");
            }
        }
        if (ctx.findDatabase(store).isEmpty()) {
            throw new Skip("store '" + store + "' is not a database in the model");
        }
        StringBuilder csv = new StringBuilder();
        for (Provision p : provisions) {
            for (Protocol.PRelationalCsvTable t : p.data().tables()) {
                if (csv.length() > 0) {
                    csv.append("\n-\n");
                }
                csv.append(t.schema()).append('\n').append(t.table()).append('\n')
                        .append(t.values());
            }
        }
        // the '$' sigil: a name no user can write, so the overlay shadows nothing
        String rtName = runtime.qualifiedName() + "$test$" + Integer.toHexString(key.toString().hashCode());
        String connName = rtName + "$conn";
        ConnectionDefinition conn = new ConnectionDefinition(connName, store, sessionType,
                new ConnectionSpecification.LocalH2(null, csv.toString(), null),
                new AuthenticationSpec.TestAuth());
        Map<String, String> ids = new LinkedHashMap<>();
        runtime.connectionIds().forEach((id, s) -> ids.put(id, s));
        RuntimeDefinition rt = new RuntimeDefinition(rtName, runtime.mappings(),
                Map.of(store, List.of(connName)), List.of(), List.of(), ids);
        TestRuntime built = new TestRuntime(ctx.withExecutionOverlay(rt, conn), rtName);
        runtimes.put(key.toString(), built);
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
                    throw new Skip("serialization format " + fmt
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
            case ExecutionResult.TdsText tt -> throw new Skip(
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
        return switch (a.expected()) {
            case Protocol.PExternalFormatData ef -> {
                if (!"application/json".equalsIgnoreCase(ef.contentType())) {
                    throw new Skip("assertion '" + a.id() + "': content type '"
                            + ef.contentType() + "' is not judged by this runner");
                }
                Object expected;
                try {
                    expected = com.legend.sql.Json.parse(ef.data());
                } catch (RuntimeException e) {
                    yield "expected JSON does not parse: " + e.getMessage();
                }
                yield TestAssertions.equalToJson(expected, actual);
            }
            case Protocol.PEqualToValue eq -> throw new Skip("assertion '" + a.id()
                    + "': EqualTo (a spec value) is not judged by this runner");
            case Protocol.PRelationElement rel -> throw new Skip("assertion '" + a.id()
                    + "': a Relation assertion is not judged by this runner");
        };
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
