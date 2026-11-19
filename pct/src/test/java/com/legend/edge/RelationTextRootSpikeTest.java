// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.database.PlanStyle;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.result.Collector;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SPIKE (H11): A QUERY WHOSE ROOT IS A RELATION'S TEXT, answered as "the relation's rows, presented as text" -- the
 * door rewrite slice 1.3 names, through the product's public doors alone. The query is typed once to see its root;
 * where that is {@code relation::toString} or {@code toCSV}, its receiver is planned and run IN THE PRODUCT'S OWN
 * STYLE ({@code PlanStyle.PRODUCT}: no edge SQL at all), its typed rows collected, and {@link RelationText} writes the
 * text from the plan's columns and the rows. The cases are the PR #118 audit's EDGE-1 and EDGE-2
 * ({@code EdgeCasesGoldenTest} on claude/presentation-b1); each expected text is upstream's, read at the pin.
 */
class RelationTextRootSpikeTest {

    private static final String RELATION_TO_STRING = "meta::pure::functions::relation::toString";

    /** The query's answer: its root's relation run as rows and presented; a root that is no relation text refused. */
    static String presented(TestBackend backend, String query) throws Exception {
        String model = backend.withStorelessRuntime("Class s::P { name: String[1]; }\n");
        ModelContext ctx = Compiler.compileModel(model);
        ValueSpecification parsed = Compiler.parseQuery(query);
        // typed once, to read what the root IS (a typed fact, never the call's spelled name)
        TypedSpec root = Compiler.query(ctx, parsed).expression();
        if (root instanceof com.legend.compiler.spec.typed.TypedLambda l) {
            root = l.body().get(l.body().size() - 1);
        }
        if (!(root instanceof TypedNativeCall call)) {
            throw new IllegalStateException("the root is no call: " + root.getClass().getSimpleName());
        }
        String fqn = call.callee().qualifiedName();
        boolean csv = com.legend.compiler.element.type.PlatformTypes.TO_CSV.equals(fqn);
        if (!csv && !RELATION_TO_STRING.equals(fqn)) {
            throw new IllegalStateException("the root is no relation text: " + fqn);
        }
        // the door rewrite: the same lambda with its last statement's receiver in the print's place
        LambdaFunction lambda = (LambdaFunction) parsed;
        List<ValueSpecification> body = new ArrayList<>(lambda.body());
        AppliedFunction print = (AppliedFunction) body.get(body.size() - 1);
        body.set(body.size() - 1, print.parameters().get(0));
        TypedQuery rows = Compiler.query(ctx, new LambdaFunction(lambda.parameters(), body));
        Collector collector = new Collector();
        try (Connection c = backend.open(PlanStyle.PRODUCT)) {
            Execution.run(rows, StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(TestSessions.over(c)),
                    com.legend.exec.PlanSessions.of(TestSessions.over(c)),
                    com.legend.exec.ProductRun.Establish.DECLARED_SETUP, java.util.Set.of(), PlanStyle.PRODUCT,
                    new com.legend.result.Output.Rows(collector), com.legend.result.Batcher.DEFAULT_CAPACITY);
        }
        Collector.Collected got = collector.collected();
        List<RelationText.Col> cols = new ArrayList<>();
        for (ExecutionPlan.Column col : got.columns()) {
            System.out.println("[H11-root] " + backend.type() + " column " + col.name() + " type=" + col.type()
                    + " slot=" + col.slot() + " precision=" + col.precision() + " cells="
                    + got.rows().stream().map(r -> r.get(got.columns().indexOf(col)))
                            .map(v -> v == null ? "null" : v.getClass().getSimpleName() + ":" + v).toList());
            cols.add(new RelationText.Col(col.name(), col.type()));
        }
        return csv ? RelationText.csv(cols, got.rows(), false) : RelationText.tds(cols, got.rows());
    }

    private static final String CSV = "->meta::relational::tests::csv::toCSV()";

    @Test
    void theAuditsEdgeCasesAtAQuerysRoot_onDuckDbAndH2() throws Exception {
        Map<String, String> cases = new LinkedHashMap<>();   // the query -> upstream's text
        String[] values = {"0.00001", "0.0005", "25000000.0", "10000000000000000.0", "100000000000000000000.0"};
        for (String v : values) {
            // EDGE-1: legend-pure's Float print, a point always and never an exponent
            cases.put("|#TDS\n  x:Float\n  " + v + "\n#->toString()", "#TDS\n   x\n   " + v + "\n#");
            cases.put("|#TDS\n  x:Float\n  " + v + "\n#" + CSV, "x\n" + v + "\n");
        }
        // EDGE-2: toCSV's DateTime is legend-engine's SimpleDateTimeFormat, '%t{yyyy-MM-dd HH:mm:ss}'
        cases.put("|#TDS\n  id:Integer, at:DateTime\n  1, 2024-01-02T10:30:00.123+0000\n#" + CSV,
                "id,at\n1,2024-01-02 10:30:00\n");
        // the same value in a relation's toString: three subsecond digits and +0000 (TestTDS.java:209)
        cases.put("|#TDS\n  id:Integer, at:DateTime\n  1, 2024-01-02T10:30:00.123+0000\n#->toString()",
                "#TDS\n   id,at\n   1,2024-01-02T10:30:00.123+0000\n#");
        List<String> wrong = new ArrayList<>();
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
            TestBackend backend = TestBackends.of(type);
            for (Map.Entry<String, String> c : cases.entrySet()) {
                String got;
                try {
                    got = presented(backend, c.getKey());
                } catch (RuntimeException e) {
                    got = "THREW " + e.getClass().getSimpleName() + ": " + e.getMessage();
                }
                String line = type + " " + c.getKey().replace("\n", "\\n") + " => " + got.replace("\n", "\\n");
                System.out.println("[H11-root] " + (got.equals(c.getValue()) ? "EQUAL " : "DIFFERS ") + line);
                if (!got.equals(c.getValue())) {
                    wrong.add(line + " (upstream: " + c.getValue().replace("\n", "\\n") + ")");
                }
            }
        }
        assertEquals(List.of(), wrong);
    }
}
