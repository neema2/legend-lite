// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testable;

import com.legend.compiler.element.ModelContext;
import com.legend.model.DataDefinition;
import com.legend.model.RuntimeDefinition;
import com.legend.model.ServiceDefinition;
import com.legend.protocol.Protocol;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A SERVICE'S TEST SUITES, PLANNED WITHOUT A DATABASE (Studio plan A4): for each atomic test, the
 * rows its runtime is provisioned with -- each CSV table and the store it seeds, resolved from
 * connection ids, {@code ###Data} references and resolvers by the engine's rules -- the runtime it
 * runs on, its serialization format and its assertions: everything but executing it.
 * {@code com.legend.test.ServiceTestRunner} executes the plan on a JDBC session; legend-lite's WebAssembly
 * planner hands it to the browser (Studio's test runner), which loads the tables into the tab's
 * DuckDB, runs the service's query there, and asks {@code com.legend.test.TestAssertions} for each judgment -- one
 * set of rules, written once (docs/DEFERRED_TEST_EXECUTION.md).
 *
 * <p>A test the plan cannot provide for carries the reason as {@code skipped}, never silently.
 */
public final class TestPlan {

    private TestPlan() {
    }

    /** A test the plan cannot provide for: the reason travels as its SKIPPED result. */
    public static final class Skip extends RuntimeException {
        public Skip(String why) {
            super(why);
        }
    }

    /** One CSV table of a test's provisioning, and the store (database) it seeds. */
    public record Table(String store, String schema, String table, String csv) {
    }

    /**
     * One assertion: an {@code EqualToJson}'s expected JSON text, or why it is not judged here
     * ({@code skipped}) -- the kinds {@code com.legend.test.ServiceTestRunner} does not judge either.
     */
    public record Assertion(String id, @com.legend.base.Nullable String expectedJson,
                            @com.legend.base.Nullable String skipped) {
    }

    /** One atomic test, planned: or {@code skipped}, with why. */
    public record Planned(String suiteId, String testId,
                          @com.legend.base.Nullable String skipped,
                          @com.legend.base.Nullable String runtime,
                          List<Table> tables, String format, List<Assertion> assertions) {
    }

    /** One provisioning unit: the store and the CSV data that seeds it. */
    public record Provision(String store, Protocol.PRelationalCsvData data) {
    }

    /** Every atomic test of every suite of {@code svc}, in declaration order. */
    public static List<Planned> of(ModelContext ctx, ServiceDefinition svc) {
        List<Planned> out = new ArrayList<>();
        if (svc.testSuites() == null) {
            return out;
        }
        for (Protocol.PServiceTestSuite suite : svc.testSuites()) {
            for (Protocol.PServiceTestSuite.PSuiteTest test : suite.tests()) {
                String format = test.serializationFormat() == null ? "DEFAULT" : test.serializationFormat();
                try {
                    String runtimeFqn = runtimeOf(svc);
                    RuntimeDefinition runtime = ctx.findRuntime(runtimeFqn).orElseThrow(
                            () -> new Skip("runtime '" + runtimeFqn + "' is not in the model"));
                    List<Table> tables = new ArrayList<>();
                    for (Provision p : provisions(ctx, suite, runtime)) {
                        for (Protocol.PRelationalCsvTable t : p.data().tables()) {
                            tables.add(new Table(p.store(), t.schema(), t.table(), t.values()));
                        }
                    }
                    List<Assertion> assertions = new ArrayList<>();
                    for (Protocol.PTestAssertion a : test.assertions()) {
                        assertions.add(assertion(a));
                    }
                    out.add(new Planned(suite.id(), test.id(), null, runtimeFqn, List.copyOf(tables), format,
                            List.copyOf(assertions)));
                } catch (Skip s) {
                    out.add(new Planned(suite.id(), test.id(), String.valueOf(s.getMessage()), null, List.of(), format,
                            List.of()));
                }
            }
        }
        return out;
    }

    /** An assertion as the plan carries it: the expected JSON, or why this kind is not judged. */
    public static Assertion assertion(Protocol.PTestAssertion a) {
        return switch (a.expected()) {
            case Protocol.PExternalFormatData ef -> "application/json".equalsIgnoreCase(ef.contentType())
                    ? new Assertion(a.id(), ef.data(), null)
                    : new Assertion(a.id(), null, "assertion '" + a.id() + "': content type '" + ef.contentType()
                            + "' is not judged by this runner");
            case Protocol.PEqualToValue eq -> new Assertion(a.id(), null, "assertion '" + a.id()
                    + "': EqualTo (a spec value) is not judged by this runner");
            case Protocol.PRelationElement rel -> new Assertion(a.id(), null, "assertion '" + a.id()
                    + "': a Relation assertion is not judged by this runner");
        };
    }

    /** The runtime a service's single execution names; a multi-execution service is not run yet. */
    public static String runtimeOf(ServiceDefinition svc) {
        if (svc.runtimeRef() != null) {
            return svc.runtimeRef();
        }
        if (svc.multiExecution() != null) {
            throw new Skip("multi-execution service: the test's keys select an environment,"
                    + " which this runner does not bind yet");
        }
        throw new Skip("service names no runtime");
    }

    // ---- PROVISIONING -----------------------------------------------------------

    /** A suite's provisioning: each connection's (or the compact form's store's) CSV data, references resolved. */
    public static List<Provision> provisions(ModelContext ctx, Protocol.PServiceTestSuite suite, RuntimeDefinition runtime) {
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
            addProvision(ctx, out, store, cd.data());
        }
        if (data.serviceTestData() != null) {
            for (Protocol.PServiceTestSuite.PResolverData rd : data.serviceTestData()) {
                if (rd.data() != null) {
                    addProvision(ctx, out, rd.elementPath(), rd.data());
                } else {
                    // referenceDataResolver: the element's own resolvers name the stores
                    DataDefinition dd = ctx.findData(rd.elementPath()).orElseThrow(
                            () -> new Skip("data element '" + rd.elementPath() + "' is not in the model"));
                    addResolvers(ctx, out, dd);
                }
            }
        }
        return out;
    }

    private static void addResolvers(ModelContext ctx, List<Provision> out, DataDefinition dd) {
        if (dd.body().resolvers().isEmpty()) {
            throw new Skip("data element '" + dd.qualifiedName() + "' names no store to provision (no resolvers)");
        }
        for (Protocol.PDataResolver r : dd.body().resolvers()) {
            if (r.data() == null) {
                DataDefinition inner = ctx.findData(r.elementPointer().path()).orElseThrow(
                        () -> new Skip("data element '" + r.elementPointer().path() + "' is not in the model"));
                addResolvers(ctx, out, inner);
            } else {
                addProvision(ctx, out, r.elementPointer().path(), r.data());
            }
        }
    }

    /** Resolves references down to a concrete value and records it. */
    private static void addProvision(ModelContext ctx, List<Provision> out, String store, Protocol.PEmbeddedDataValue v) {
        Protocol.PEmbeddedDataValue value = v;
        while (value instanceof Protocol.PDataReference ref) {
            String path = ref.dataElement().path();
            DataDefinition dd = ctx.findData(path).orElseThrow(() -> new Skip("data element '" + path + "' is not in the model"));
            if (dd.body().value() == null) {
                // a resolver-form element: its own store keys apply
                addResolvers(ctx, out, dd);
                return;
            }
            value = dd.body().value();
        }
        if (!(value instanceof Protocol.PRelationalCsvData csv)) {
            throw new Skip("embedded data kind '" + value.getClass().getSimpleName().substring(1)
                    + "' is not provisioned by this runner");
        }
        out.add(new Provision(store, csv));
    }

    // ---- THE PLAN AS JSON (the WebAssembly planner's answer) ----------------------

    /** The plan as the browser reads it: {@code [{suite, test, skipped?, runtime?, format, tables, assertions}]}. */
    public static List<Object> toJson(List<Planned> plan) {
        List<Object> out = new ArrayList<>(plan.size());
        for (Planned p : plan) {
            Map<String, Object> o = new LinkedHashMap<>();
            o.put("suite", p.suiteId());
            o.put("test", p.testId());
            if (p.skipped() != null) {
                o.put("skipped", p.skipped());
            }
            if (p.runtime() != null) {
                o.put("runtime", p.runtime());
            }
            o.put("format", p.format());
            List<Object> tables = new ArrayList<>();
            for (Table t : p.tables()) {
                Map<String, Object> tj = new LinkedHashMap<>();
                tj.put("store", t.store());
                tj.put("schema", t.schema());
                tj.put("table", t.table());
                tj.put("csv", t.csv());
                tables.add(tj);
            }
            o.put("tables", tables);
            List<Object> assertions = new ArrayList<>();
            for (Assertion a : p.assertions()) {
                Map<String, Object> aj = new LinkedHashMap<>();
                aj.put("id", a.id());
                if (a.expectedJson() != null) {
                    aj.put("expectedJson", a.expectedJson());
                }
                if (a.skipped() != null) {
                    aj.put("skipped", a.skipped());
                }
                assertions.add(aj);
            }
            o.put("assertions", assertions);
            out.add(o);
        }
        return out;
    }
}
