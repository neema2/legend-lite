// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.ModelContext;
import com.legend.json.Json;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.server.LiteV1Api;
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
 * ONE run, the host's door ({@link LiteV1Api.Runner}: the server's {@code Execution.runOnProjectDatabases}); the census
 * is a model fact, answered at planning with no run.
 * <ul>
 *   <li>{@code generateSeedData}: the seed's chosen keys of the query's starting table, as upstream's JSON
 *       ({@link TestDataPlanner#seedJson}; register S198's order).</li>
 *   <li>{@code generateTestData_WithSeed}: the test data from the given identifiers ({@link TestDataPlanner#plan} in
 *       the routes' form, {@link #ROUTE}), as {@code text/plain}: the routes' table order, the function's cells (R5,
 *       register S191).</li>
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
public final class TestDataRoutes {

    private TestDataRoutes() {
    }

    /** {@link EngineTestDataPrinting}'s one instance: the function's form under the compatibility rules. */
    static final TestDataPlanner.Printing FUNCTION = new EngineTestDataPrinting();

    /** The generator's text under the compatibility rules ({@code generateTestData}, the plan style's two-form answer
     *  in {@code PureText.UPSTREAM}; J7/K12 of the edge audit, 2026-11-17, kept beside the routes' form, since every
     *  class nested in {@code PureText} is an edge dialect): a timestamp as
     *  legend-engine's test-data CSV writes it (L9; census C1); the rest -- the refusal of a starting table no identifier
     *  names, the tables' and the columns' order, the header's names, the relation tree admitted -- the function's
     *  rules, lite's own ({@code LitePrinting}): upstream's words of the refusal are {@link PureMessages}'s, worded
     *  after the run (stage B's B1, 2026-11-20). */
    private static final class EngineTestDataPrinting implements com.legend.testdatagen.TestDataPlanner.Printing {
        private static final com.legend.testdatagen.TestDataPlanner.Printing LITES =
                com.legend.testdatagen.LitePrinting.INSTANCE;

        @Override
        public com.legend.sql.SqlExpr timestampText(com.legend.sql.SqlExpr x) {
            return EnginePlans.testDataTimestampText(x);
        }

        @Override
        public RuntimeException unidentified(com.legend.testdatagen.TestDataPlanner.NoRowIdentifiers facts) {
            return LITES.unidentified(facts);
        }

        @Override
        public java.util.List<com.legend.testdatagen.TestDataPlanner.Table> tableOrder(
                java.util.List<com.legend.testdatagen.TestDataPlanner.Table> tables) {
            return LITES.tableOrder(tables);
        }

        @Override
        public java.util.List<String> headerOrder(com.legend.model.DatabaseDefinition.TableDefinition def,
                java.util.List<String> cols) {
            return LITES.headerOrder(def, cols);
        }

        @Override
        public String headerName(com.legend.model.DatabaseDefinition.TableDefinition def, String col) {
            return LITES.headerName(def, col);
        }

        @Override
        public void checkTree(com.legend.compiler.element.ModelContext ctx,
                java.util.List<com.legend.lineage.ScanRelations.Rel> roots) {
            LITES.checkTree(ctx, roots);
        }
    }

    /**
     * THE SERVER ROUTES' FORM OF THE GENERATOR'S TEXT (J7/K12 of the edge audit, 2026-11-17: the routes' rules, which a
     * boolean chose inside the product's generator until then, are the edge's, handed to {@code TestDataPlanner.plan}).
     * Upstream's {@code TestDataGenerationAPI} runs the generator's PLAN form
     * ({@code planTestDataGenerationWithParameterValuePairs}) and writes its rows with
     * {@code TestDataGeneration.manageTestDataResult}: a timestamp as legend-engine's test-data CSV writes it (L9); a
     * root no identifier names and no seed starts, upstream's error node ({@link Unseeded}); the tables by schema then
     * table ({@code manageTestDataResult}, :282); each table's columns by their model names and its header their
     * declared names (the B2d.3 audit's M1); a view in the query's relation tree refused by name. Public for its tests
     * (the generator's own, which pin the routes' header beside the function's).
     */
    public static final TestDataPlanner.Printing ROUTE = new RouteForm();

    /** {@link #ROUTE}'s rules (J7/K12 of the edge audit, 2026-11-17: moved from testdatagen.TestDataPlanner's route
     *  branch, unchanged). */
    private static final class RouteForm implements TestDataPlanner.Printing {
        @Override
        public com.legend.sql.SqlExpr timestampText(com.legend.sql.SqlExpr x) {
            return EnginePlans.testDataTimestampText(x);
        }

        /** Upstream's plan form's error node (TDG:1244-1252), which the route answers as its 500's text
         *  ({@link #answer}). */
        @Override
        public RuntimeException unidentified(TestDataPlanner.NoRowIdentifiers facts) {
            return new Unseeded(PureMessages.unidentified(facts), facts.database(), facts.schema(), facts.table());
        }

        /** The server's routes' order: by schema and table, as upstream's writer sorts its blocks
         *  ({@code manageTestDataResult}, :282: {@code schemaName + '.' + relationName}) -- the model's names, a
         *  planning constant. */
        @Override
        public List<TestDataPlanner.Table> tableOrder(List<TestDataPlanner.Table> tables) {
            List<TestDataPlanner.Table> order = new ArrayList<>(tables);
            order.sort(java.util.Comparator.comparing(t -> t.schema() + "." + t.table()));
            return order;
        }

        /** {@code cols} of {@code def} in the server routes' order (the B2d.3 audit's M1): upstream's plan form sorts a
         *  table's columns by their model names ({@code sortBy(x | $x.name)}, TDG:885, :1043) -- Pure's string order,
         *  case-sensitive, a quoted name with its quotes -- before it aliases each, and the route writes them in that
         *  order. A planning constant: the model's names. */
        @Override
        public List<String> headerOrder(com.legend.model.DatabaseDefinition.TableDefinition def, List<String> cols) {
            List<String> cs = new ArrayList<>(cols);
            cs.sort(java.util.Comparator.comparing(c -> {
                // the model's name, a quoted name with its quotes (J40, 2026-11-18: the edge's own)
                com.legend.model.DatabaseDefinition.ColumnDefinition cd = TestDataGenerator.column(def, c);
                return cd.quoted() ? "\"" + cd.name() + "\"" : cd.name();
            }));
            return cs;
        }

        /** A column's name in the server routes' header (the B2d.3 audit's M1): upstream aliases it
         *  {@code $y.name->addQuotesIfNoQuotes()} (TDG:885) and its writer prints the label without its quotes, in
         *  double quotes where it holds a space ({@code SQLResultColumn.getQuotedLabelIfContainSpace},
         *  TestDataGeneration.java:306): the declared name, its case kept. */
        @Override
        public String headerName(com.legend.model.DatabaseDefinition.TableDefinition def, String col) {
            String name = TestDataGenerator.column(def, col).name();
            return name.contains(" ") ? "\"" + name + "\"" : name;
        }

        /** A view in the query's relation tree, refused by name before anything is fetched: upstream's plan form
         *  fetches a view's own rows {@code top 20} and a child view's main table through its parent (TDG:1023-1201),
         *  which lite does not plan (B2d.3e). */
        @Override
        public void checkTree(ModelContext ctx, List<com.legend.lineage.ScanRelations.Rel> roots) {
            for (com.legend.lineage.ScanRelations.Rel r : roots) {
                noView(ctx, r);
            }
        }

        private static void noView(ModelContext ctx, com.legend.lineage.ScanRelations.Rel rel) {
            if (com.legend.lineage.ScanRelations.isView(ctx, rel.db(), rel.table())) {
                throw new com.legend.error.NotImplementedException("testDataGen: the view '" + rel.table()
                        + "' in the query's relation tree on the server's route: not planned (B2d.3e; upstream's plan"
                        + " form fetches a view's own rows top 20 and a child view's main table through its parent,"
                        + " TDG:1023-1201)");
            }
            for (com.legend.lineage.ScanRelations.Rel c : rel.children()) {
                noView(ctx, c);
            }
        }
    }

    /**
     * No identifiers and no seed for a root on the server's route (B2d.3e; moved from testdatagen.TestDataPlanner,
     * J7/K12 of the edge audit, 2026-11-17): upstream's plan form's error node (TDG:1244-1252), which the route answers
     * as its 500's text -- the node's message, {@code Example:\n} and the table's identifiers as JSON
     * ({@code TestDataGeneration.manageTestDataResult}, :230-262). Upstream's example holds the table's first 5 keys,
     * read for it; planning reads no row, so the route's example holds none (register S300, as the function's). The
     * message, and the table the example names, are data: the route writes the JSON.
     */
    static final class Unseeded extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final String database;
        private final String schema;
        private final String table;

        Unseeded(String message, String database, String schema, String table) {
            super(message);
            this.database = database;
            this.schema = schema;
            this.table = table;
        }

        String database() {
            return database;
        }

        String schema() {
            return schema;
        }

        String table() {
            return table;
        }
    }

    /** {@code pure/v1/execution/testDataGeneration/generateSeedData}: the seed's keys as upstream's JSON. */
    static LiteV1Api.Answer seedData(String body, LiteV1Api.Runner runner, LiteV1Api.Models models) {
        return answer(500, "COMPILATION", () -> {
            Request r = Request.of(LiteV1Api.request(body));
            // doored first, before the seed scans the query's relations (the final audit of 9af16fbe58's J8)
            return ModelInput.of(r.json().getObj("model")).with(models, ctx -> run(doored(ctx, r),
                    seedJson(TestDataPlanner.seedFacts(ctx, seed(ctx, r, true)), TestDataRoutes::upstreamKeyText), r.runtime(), runner));
        });
    }

    /**
     * The seed as upstream's seed route answers it (B2d.3e; {@code generateSeedData},
     * {@code SeedDataGeneration.streamSeedDataResult}; moved from the product's testdatagen, J40 of the edge audit,
     * 2026-11-18): the JSON object {@code {"table":{"_type":"table","table":T,"schema":S,"database":D},
     * "rowIdentifiers":[{"columnValuePairs":[{"name":k,"value":v},...]},...]}}, built by the database -- each value its
     * JSON for the key's cell, the identifiers in the keys' order (R2, register S198) -- one statement, its one cell the
     * text. A key's name is its column's, in double quotes where it holds a space (upstream's
     * {@code getQuotedLabelIfContainSpace}); a timestamp key's value {@code timestampKey}'s text of its cell (the seed route's, upstream's: {@link #upstreamKeyText}).
     */
    public static SqlQuery seedJson(TestDataPlanner.SeedFacts seed,
            java.util.function.UnaryOperator<com.legend.sql.SqlExpr> timestampKey) {
        List<com.legend.sql.SqlExpr> pairs = new ArrayList<>(seed.keys().size());
        List<com.legend.sql.SqlExpr.JsonArrayAgg.Key> order = new ArrayList<>(seed.keys().size());
        for (TestDataPlanner.SeedKey k : seed.keys()) {
            com.legend.sql.SqlExpr value = k.timestamp() ? timestampKey.apply(k.cell()) : k.cell();
            pairs.add(new com.legend.sql.SqlExpr.JsonObject(List.of(new com.legend.sql.SqlExpr.StringLit("name"),
                    new com.legend.sql.SqlExpr.StringLit(k.name().contains(" ") ? "\"" + k.name() + "\"" : k.name()),
                    new com.legend.sql.SqlExpr.StringLit("value"), value)));
            order.add(new com.legend.sql.SqlExpr.JsonArrayAgg.Key(k.order(), false));
        }
        com.legend.sql.SqlExpr identifier = new com.legend.sql.SqlExpr.JsonObject(List.of(
                new com.legend.sql.SqlExpr.StringLit("columnValuePairs"), new com.legend.sql.SqlExpr.JsonArray(pairs)));
        com.legend.sql.SqlExpr table = new com.legend.sql.SqlExpr.JsonObject(List.of(
                new com.legend.sql.SqlExpr.StringLit("_type"), new com.legend.sql.SqlExpr.StringLit("table"),
                new com.legend.sql.SqlExpr.StringLit("table"), new com.legend.sql.SqlExpr.StringLit(seed.table()),
                new com.legend.sql.SqlExpr.StringLit("schema"), new com.legend.sql.SqlExpr.StringLit(seed.schema()),
                new com.legend.sql.SqlExpr.StringLit("database"), new com.legend.sql.SqlExpr.StringLit(seed.database())));
        com.legend.sql.SqlExpr answer = new com.legend.sql.SqlExpr.JsonObject(List.of(
                new com.legend.sql.SqlExpr.StringLit("table"), table, new com.legend.sql.SqlExpr.StringLit("rowIdentifiers"),
                new com.legend.sql.SqlExpr.JsonArrayAgg(identifier, order)));
        return new com.legend.sql.SqlSelect(List.of(new com.legend.sql.SqlSelect.Projection(new com.legend.sql.SqlExpr.Cast(
                answer, com.legend.sql.SqlType.Scalar.VARCHAR, com.legend.sql.SqlExpr.Reason.DECLARED), "seedData", null)),
                false, new com.legend.sql.SqlSource.Subselect(seed.rows(), "_k", null), null, List.of(), null, null,
                List.of(), null, null, List.of());
    }

    /** {@code pure/v1/execution/testDataGeneration/generateTestData_WithSeed}: the test data from the given
     *  identifiers. */
    static LiteV1Api.Answer withSeed(String body, LiteV1Api.Runner runner, LiteV1Api.Models models) {
        return testData(body, runner, models, false);
    }

    /** {@code pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed}: the test data from the default
     *  seed. */
    static LiteV1Api.Answer withDefaultSeed(String body, LiteV1Api.Runner runner, LiteV1Api.Models models) {
        return testData(body, runner, models, true);
    }

    private static LiteV1Api.Answer testData(String body, LiteV1Api.Runner runner, LiteV1Api.Models models,
            boolean defaultSeed) {
        LiteV1Api.Answer a = answer(500, "COMPILATION", () -> {
            Request r = Request.of(LiteV1Api.request(body));
            return ModelInput.of(r.json().getObj("model")).with(models, ctx -> {
                // every route doored first, before its query's relations are scanned (ScanRelations.relTree: a raw
                // read's query with no root, or several, was answered by the scan's words until the final audit of
                // 9af16fbe58's J8, 2026-11-14); the seed's entry refuses raw SQL inside itself too
                doored(ctx, r);
                List<TestDataGenerator.TableRowIds> ids = defaultSeed ? List.of() : identifiers(r.json());
                // a view root's main table admitted, so a view is refused by the route's own rule (ROUTE.checkTree)
                TestDataPlanner.Seed seed = defaultSeed ? seed(ctx, r, true) : null;
                // a root no identifier names and no seed starts: upstream's error node, answered by unseeded below; a
                // view is refused before it is fetched (ROUTE.checkTree), so its own fetch is never planned here
                TestDataPlanner.Planned planned = TestDataPlanner.plan(ctx, r.query(), r.mapping(), ids, null,
                        r.json().getBoolOr("hashStrings", false), (db, view, reads, tables) -> {
                            throw new IllegalStateException("a view on the server's route is refused before it is"
                                    + " fetched");
                        }, seed, ROUTE);
                return run(ctx, planned.csv(), r.runtime(), runner);
            });
        });
        return a.status() == 200 ? new LiteV1Api.Answer(200, a.json(), "text/plain") : a;
    }

    /** A timestamp key as upstream's seed route writes it ({@code ValueTransformer}, SeedDataGeneration.java:192:
     *  nine fraction digits and the GMT zone); a NULL stays NULL. Moved here from the generator
     *  (TestDataPlanner.upstreamKeyText) unchanged, F's separation census line B11, 2026-11-17. */
    private static com.legend.sql.SqlExpr upstreamKeyText(com.legend.sql.SqlExpr x) {
        return new com.legend.sql.SqlExpr.Case(java.util.List.of(new com.legend.sql.SqlExpr.Case.When(
                com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NULL, x),
                new com.legend.sql.SqlExpr.Cast(new com.legend.sql.SqlExpr.NullLit(), com.legend.sql.SqlType.Scalar.VARCHAR,
                        com.legend.sql.SqlExpr.Reason.DECLARED))),
                com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                        com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.STRFTIME, x,
                                new com.legend.sql.SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_NANO)),
                        new com.legend.sql.SqlExpr.StringLit("+0000")), com.legend.sql.SqlExpr.Reason.DECLARED);
    }

    /** A route's root no identifier names ({@link Unseeded}, raised at planning by {@link #ROUTE}): upstream's error
     *  node's answer, its message, {@code Example:\n} and the table's identifiers -- none -- as the 500's text
     *  ({@code TestDataGeneration.manageTestDataResult}, :230-262). {@link #answer} answers it so. */
    static LiteV1Api.Answer unseeded(Unseeded e) {
        return answerOf(e);
    }

    /** The product's answer of {@code call} ({@link LiteV1Api#answer}), a root no identifier names answered as upstream
     *  answers it ({@link #unseeded}): the edge's wrapping, so the product's answer knows nothing of upstream's error
     *  node (D1.0b, 2026-11-12; until then {@code LiteV1Api.answer} called this class). */
    static LiteV1Api.Answer answer(int status, @com.legend.base.Nullable String errorType,
            java.util.function.Supplier<String> call) {
        Unseeded[] unseeded = {null};
        LiteV1Api.Answer a = EngineEnvelope.answer(status, errorType, () -> {
            try {
                return call.get();
            } catch (Unseeded e) {
                unseeded[0] = e;
                return "";
            }
        }, PureMessages::compile);
        return unseeded[0] != null ? unseeded(unseeded[0]) : a;
    }

    private static LiteV1Api.Answer answerOf(Unseeded e) {
        return new LiteV1Api.Answer(500, e.getMessage() + "Example:\n" + Json.toCompact(identifiersOf(e)),
                "text/plain");
    }

    /** {@code pure/v1/testData/generation/DONOTUSE_generateTestData}: the census, upstream's embedded data
     *  ({@code TestDataGenerationService.generateEmbeddedData}) -- {@code {"data":[{"_type":"relationalCSVData",
     *  "tables":[{"schema":..,"table":..,"values":..},...]}]}}, a model fact: nothing runs. */
    static LiteV1Api.Answer census(String body, LiteV1Api.Models models) {
        return answer(400, "COMPILATION", () -> {
            Json.Obj request = LiteV1Api.request(body);
            LambdaFunction query = resolved(request.getObj("query"));
            String mapping = request.getString("mapping");
            return ModelInput.of(request.getObj("model")).with(models, ctx -> {
                // through the product's door before its scan, as the generator's other routes (the last list of
                // 2026-11-14's observation: the census scanned first; it plans and runs nothing either way)
                com.legend.planner.QueryDoor.refuseRawSql(com.legend.Compiler.query(ctx, query));
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
     *  ({@link ExecuteInputs#boundParameters}: a parameter with no value refused with upstream's words, a value for no
     *  parameter ignored; the seed route's own {@code parameters} field read by nothing, as upstream's), its mapping and
     *  its runtime (a runtime named; one given inline refused by name). */
    record Request(Json.Obj json, LambdaFunction query, String mapping, String runtime) {
        static Request of(Json.Obj json) {
            LambdaFunction query = resolved(ExecuteInputs.boundParameters(json.getObj("function"),
                    json.getArrOr("parameterValues", null)));
            Json.Obj rt = json.getObj("runtime");
            if (!rt.has("runtime")) {
                throw refused("a runtime given inline (" + rt.getStringOr("_type", "?") + "); name one (runtimePointer)");
            }
            return new Request(json, query, json.getString("mapping"), rt.getString("runtime"));
        }
    }

    /** {@code ctx}, once the request's query is through the product's door: a query reaching a statement's own SQL
     *  text ({@code executeInDb}'s read) is refused by name, as every product door refuses it (2026-11-13, the
     *  orchestrator's completion A of item 5: these routes scan the query's relations and lower its seed, apart from
     *  the door every execute passes, PlanMaker.plan's QueryDoor.refuseTestOnly). */
    private static ModelContext doored(ModelContext ctx, Request r) {
        com.legend.planner.QueryDoor.refuseRawSql(com.legend.Compiler.query(ctx, r.query()));
        return ctx;
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
     *  mapping). The seed's query is the one whose meaning chooses the keys, so it is planned as every query of this
     *  door is, with legend-engine's meaning, or refused by name ({@link PureV1Routes#asLegendEngine}, in the door's
     *  style; the user's ruling of 2026-11-20 ~07:55). The routes' other queries choose no row by a call's value:
     *  generateTestData_WithSeed's rows are the identifiers'. */
    private static TestDataPlanner.Seed seed(ModelContext ctx, Request r, boolean viewRoot) {
        return TestDataPlanner.seed(ctx, r.query(), r.mapping(), viewRoot, q -> {
            com.legend.TypedQuery typed = com.legend.Compiler.query(ctx,
                    TestDataPlanner.underMapping(q, r.mapping(), r.runtime(), com.legend.Compiler::parseQuery));
            return com.legend.planner.ProductStatements.lowerTestDataSeed(PureV1Routes.asLegendEngine(typed),
                    r.runtime(), PureText.PURE_V1);
        });
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

    /** The route's example: the table's identifiers, none ({@link Unseeded}). */
    private static Map<String, Object> identifiersOf(Unseeded e) {
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
    private static String run(ModelContext ctx, SqlQuery statement, String runtime, LiteV1Api.Runner runner) {
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
