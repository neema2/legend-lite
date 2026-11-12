// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.ModelContext;
import com.legend.json.Json;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.server.ModelInput;
import com.legend.server.PureV1Api;
import com.legend.sql.SqlQuery;
import com.legend.testdatagen.TestDataCensus;
import com.legend.testdatagen.TestDataGenerator;
import com.legend.testdatagen.TestDataPlanner;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE TEST-DATA GENERATOR'S PRODUCT DOORS (B2d.3e of the total-wall program, 2026-11-12; the design's §3.3,
 * docs/data-delivery-homework/TEST_DATA_GENERATOR_B2D3_DESIGN_2026_11_11.md): legend-engine's four routes
 * ({@code TestDataGenerationAPI} under {@code pure/v1/execution}, and the census's {@code TestDataGeneration} under
 * {@code pure/v1/testData/generation}), each read only. A route that reads the database composes ONE planning call --
 * the generator's statement ({@link TestDataPlanner}), the database building the answer's text in its one cell -- and
 * ONE run, the host's door ({@link PureV1Api.Runner}: the server's {@code Execution.runOnProjectDatabases}); the census
 * is a model fact, answered at planning with no run.
 * <ul>
 *   <li>{@code generateSeedData}: the seed's chosen keys of the query's starting table, as upstream's JSON
 *       ({@link TestDataPlanner#seedJson}; register S198's order).</li>
 *   <li>{@code generateTestData_WithSeed}: the test data from the given identifiers ({@link TestDataPlanner#route}),
 *       as {@code text/plain}: the routes' table order, the function's cells (R5, register S191).</li>
 *   <li>{@code generateTestData_WithDefaultSeed}: the same from the default seed, its keys the statement's first clause
 *       (one statement, where upstream runs two plans).</li>
 *   <li>{@code DONOTUSE_generateTestData}: the census ({@link TestDataCensus}) as upstream's embedded data.</li>
 * </ul>
 * Refused by name: a view in the query's relation tree on the two test-data routes ({@code generateTestData_WithSeed},
 * {@code generateTestData_WithDefaultSeed}), and a runtime given inline (an {@code engineRuntime}), each pending the
 * user's ruling (register S301's draft, B2d.3e). The seed route admits a view ROOT, whose main table's keys it answers,
 * as upstream's seed plan does ({@code planSeedDataGenerationWithParameterNameValuePairs}, TDG:806-808: a view root's
 * {@code mainTable()}; the B2d.3 audit's L8, 2026-11-12 -- until then this text said every route refused a view). The
 * query's {@code parameterValues} are bound by name, as {@code execute} binds them.
 */
final class TestDataRoutes {

    private TestDataRoutes() {
    }

    /** {@code pure/v1/execution/testDataGeneration/generateSeedData}: the seed's keys as upstream's JSON. */
    static PureV1Api.Answer seedData(String body, PureV1Api.Runner runner, PureV1Api.Models models) {
        return answer(500, "COMPILATION", () -> {
            Request r = Request.of(PureV1Api.request(body));
            return ModelInput.of(r.json().getObj("model")).with(models, ctx -> run(ctx,
                    TestDataPlanner.seedJson(ctx, seed(ctx, r, true)), r.runtime(), runner));
        });
    }

    /** {@code pure/v1/execution/testDataGeneration/generateTestData_WithSeed}: the test data from the given
     *  identifiers. */
    static PureV1Api.Answer withSeed(String body, PureV1Api.Runner runner, PureV1Api.Models models) {
        return testData(body, runner, models, false);
    }

    /** {@code pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed}: the test data from the default
     *  seed. */
    static PureV1Api.Answer withDefaultSeed(String body, PureV1Api.Runner runner, PureV1Api.Models models) {
        return testData(body, runner, models, true);
    }

    private static PureV1Api.Answer testData(String body, PureV1Api.Runner runner, PureV1Api.Models models,
            boolean defaultSeed) {
        PureV1Api.Answer a = answer(500, "COMPILATION", () -> {
            Request r = Request.of(PureV1Api.request(body));
            return ModelInput.of(r.json().getObj("model")).with(models, ctx -> {
                List<TestDataGenerator.TableRowIds> ids = defaultSeed ? List.of() : identifiers(r.json());
                // a view root's main table admitted, so a view is refused by the route's own rule (TestDataPlanner.route)
                TestDataPlanner.Seed seed = defaultSeed ? seed(ctx, r, true) : null;
                // a root no identifier names and no seed starts: upstream's error node, answered by unseeded below
                TestDataPlanner.Planned planned = TestDataPlanner.route(ctx, r.query(), r.mapping(), ids, seed,
                        r.json().getBoolOr("hashStrings", false));
                return run(ctx, planned.csv(), r.runtime(), runner);
            });
        });
        return a.status() == 200 ? new PureV1Api.Answer(200, a.json(), "text/plain") : a;
    }

    /** A route's root no identifier names ({@link TestDataPlanner.Unseeded}, raised at planning): upstream's error
     *  node's answer, its message, {@code Example:\n} and the table's identifiers -- none -- as the 500's text
     *  ({@code TestDataGeneration.manageTestDataResult}, :230-262). {@link #answer} answers it so. */
    static PureV1Api.Answer unseeded(TestDataPlanner.Unseeded e) {
        return answerOf(e);
    }

    /** The product's answer of {@code call} ({@link PureV1Api#answer}), a root no identifier names answered as upstream
     *  answers it ({@link #unseeded}): the edge's wrapping, so the product's answer knows nothing of upstream's error
     *  node (D1.0b, 2026-11-12; until then {@code PureV1Api.answer} called this class). */
    static PureV1Api.Answer answer(int status, @com.legend.base.Nullable String errorType,
            java.util.function.Supplier<String> call) {
        TestDataPlanner.Unseeded[] unseeded = {null};
        PureV1Api.Answer a = PureV1Api.answer(status, errorType, () -> {
            try {
                return call.get();
            } catch (TestDataPlanner.Unseeded e) {
                unseeded[0] = e;
                return "";
            }
        });
        return unseeded[0] != null ? unseeded(unseeded[0]) : a;
    }

    private static PureV1Api.Answer answerOf(TestDataPlanner.Unseeded e) {
        return new PureV1Api.Answer(500, e.getMessage() + "Example:\n" + Json.toCompact(identifiersOf(e)),
                "text/plain");
    }

    /** {@code pure/v1/testData/generation/DONOTUSE_generateTestData}: the census, upstream's embedded data
     *  ({@code TestDataGenerationService.generateEmbeddedData}) -- {@code {"data":[{"_type":"relationalCSVData",
     *  "tables":[{"schema":..,"table":..,"values":..},...]}]}}, a model fact: nothing runs. */
    static PureV1Api.Answer census(String body, PureV1Api.Models models) {
        return answer(400, "COMPILATION", () -> {
            Json.Obj request = PureV1Api.request(body);
            LambdaFunction query = resolved(request.getObj("query"));
            String mapping = request.getString("mapping");
            return ModelInput.of(request.getObj("model")).with(models, ctx -> {
                List<Map<String, Object>> tables = new ArrayList<>();
                for (List<String> t : TestDataCensus.rows(ctx, query, mapping)) {
                    Map<String, Object> table = new LinkedHashMap<>();
                    table.put("schema", t.get(0));
                    table.put("table", t.get(1));
                    table.put("values", t.get(2));
                    tables.add(table);
                }
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("_type", "relationalCSVData");
                data.put("tables", tables);
                return Json.toCompact(Map.of("data", List.of(data)));
            });
        });
    }

    /** A route's {@code ExecuteInput}: its query, its {@code parameterValues} bound by name as upstream binds them
     *  (TestDataGenerationAPI.java:84-90, :121-128, :161-168) -- as {@code execute} binds them
     *  ({@link PureV1Api#boundParameters}: a parameter with no value refused with upstream's words, a value for no
     *  parameter ignored; the seed route's own {@code parameters} field read by nothing, as upstream's), its mapping and
     *  its runtime (a runtime named; one given inline refused by name). */
    record Request(Json.Obj json, LambdaFunction query, String mapping, String runtime) {
        static Request of(Json.Obj json) {
            LambdaFunction query = resolved(PureV1Api.boundParameters(json.getObj("function"),
                    json.getArrOr("parameterValues", null)));
            Json.Obj rt = json.getObj("runtime");
            if (!rt.has("runtime")) {
                throw refused("a runtime given inline (" + rt.getStringOr("_type", "?") + "); name one (runtimePointer)");
            }
            return new Request(json, query, json.getString("mapping"), rt.getString("runtime"));
        }
    }

    /** A request's lambda, its names resolved (step D), as every planning entry takes a query. */
    private static LambdaFunction resolved(Json.Obj lambda) {
        return (LambdaFunction) com.legend.compiler.NameResolver.resolveQuery(ProtocolReader.lambda(lambda));
    }

    private static com.legend.error.NotImplementedException refused(String what) {
        return new com.legend.error.NotImplementedException("the test-data generator's route with " + what
                + ": not planned (B2d.3e)");
    }

    /** The seed of the request's query, lowered on its runtime through its mapping, whatever mappings the runtime
     *  declares ({@link TestDataPlanner#underMapping}, as upstream executes the seed's function with the given
     *  mapping). */
    private static TestDataPlanner.Seed seed(ModelContext ctx, Request r, boolean viewRoot) {
        return TestDataPlanner.seed(ctx, r.query(), r.mapping(), viewRoot, q -> com.legend.Compiler.query(ctx,
                TestDataPlanner.underMapping(q, r.mapping(), r.runtime(), com.legend.Compiler::parseQuery))
                .lower(r.runtime(), false).plan());
    }

    /** The request's {@code tableRowIdentifiers}: each table's identifiers, each a row's column-value pairs. */
    private static List<TestDataGenerator.TableRowIds> identifiers(Json.Obj request) {
        Json.Arr given = request.getArrOr("tableRowIdentifiers", null);
        List<TestDataGenerator.TableRowIds> out = new ArrayList<>();
        if (given == null) {
            return out;
        }
        for (Json.Node n : given.items()) {
            Json.Obj t = (Json.Obj) n;
            Json.Obj table = t.getObj("table");
            List<TestDataGenerator.RowId> rows = new ArrayList<>();
            Json.Arr ids = t.getArrOr("rowIdentifiers", null);
            for (Json.Node id : ids == null ? List.<Json.Node>of() : ids.items()) {
                List<String> cols = new ArrayList<>();
                List<Object> values = new ArrayList<>();
                Json.Arr pairs = ((Json.Obj) id).getArrOr("columnValuePairs", null);
                for (Json.Node p : pairs == null ? List.<Json.Node>of() : pairs.items()) {
                    Json.Obj pair = (Json.Obj) p;
                    cols.add(pair.getString("name"));
                    values.add(value(pair.get("value")));
                }
                rows.add(new TestDataGenerator.RowId(cols, values));
            }
            out.add(new TestDataGenerator.TableRowIds(table.getString("schema"), table.getString("table"), rows));
        }
        return out;
    }

    /** A column-value pair's value as upstream's primitive reader reads it: a text, a whole number, a decimal, a
     *  Boolean; any other refused by name. */
    private static Object value(Json.Node v) {
        return switch (v) {
            case Json.Str s -> s.value();
            case Json.Num n when n.isInteger() -> n.longValue();
            case Json.Num n -> n.decimalValue() != null ? n.decimalValue()
                    : java.math.BigDecimal.valueOf(n.doubleValue());
            case Json.Bool b -> b.value();
            default -> throw refused("a row identifier's value " + Json.toCompact(v));
        };
    }

    /** The route's example: the table's identifiers, none ({@link TestDataPlanner.Unseeded}). */
    private static Map<String, Object> identifiersOf(TestDataPlanner.Unseeded e) {
        Map<String, Object> table = new LinkedHashMap<>();
        table.put("_type", "table");
        table.put("table", e.table());
        table.put("schema", e.schema());
        table.put("database", e.database());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("table", table);
        out.put("rowIdentifiers", List.of());
        return out;
    }

    /** {@code statement}'s one cell, the database's text: the statement a value the query answers, planned and run by
     *  the host's door, its one cell written as it is. */
    private static String run(ModelContext ctx, SqlQuery statement, String runtime, PureV1Api.Runner runner) {
        com.legend.TypedQuery shape = com.legend.Compiler.query(ctx, "|''");
        com.legend.TypedQuery query = shape.withBody(List.of(com.legend.compiler.spec.GeneratorSplice.plannedValue(
                statement, shape.body().get(0).info())));
        java.io.StringWriter out = new java.io.StringWriter();
        try {
            runner.run(query, runtime, new com.legend.result.Output.Rows(new OneText(out)));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return out.toString();
    }

    /** The answer of a statement whose one row's one cell is a text -- an aggregate with no grouping, one row by
     *  construction: that text, written as it is. */
    private static final class OneText implements com.legend.result.ResultSink {
        private final java.io.Writer out;

        OneText(java.io.Writer out) {
            this.out = out;
        }

        @Override
        public void columns(List<com.legend.executionplan.ExecutionPlan.Column> columns) {
            if (columns.size() != 1) {
                throw new IllegalStateException("the generator's statement answers one column, not " + columns.size());
            }
        }

        @Override
        public void batch(com.legend.result.Batch batch) throws java.io.IOException {
            for (int r = 0; r < batch.size(); r++) {
                Object cell = batch.get(r, 0);
                if (!(cell instanceof String text)) {
                    throw new IllegalStateException("the generator's statement answers a text, not " + cell);
                }
                out.write(text);
            }
        }

        @Override
        public void end() {
        }
    }
}
