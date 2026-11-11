// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The text plan (generatePlan, arrowPlan, the hosts' plans: {@code TypedQuery.plan}) renders from the execution plan
 * (B3, 2026-11-11, docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2): a query reading a {@code data:} source carries the
 * slot that binds its payload by digest and the statement's content holding it (the JSON pass, 2026-11-11: until then
 * the loads that made its session table and the release that dropped it), one reading a {@code file:} source the slot
 * that binds its path -- each exactly the execution plan's, never refused (register S188, temporary until B3, closed).
 * A text that carries either does not run as it is ({@code textAlone}): its plan's runner runs it.
 */
class TextPlanSourceTest {

    @Test
    void aLiveSourceIsCarriedByTheTextPlanAsItsExecutionPlanCarriesIt() {
        var ctx = Compiler.compileModel(DataDeliveryMatrixTest.model(DataDeliveryMatrixTest.bareConnection(
                DatabaseType.DuckDB), ""));
        // a data: payload: its statement's bound parameter, named by its digest, its text the statement's content (the
        // JSON pass, 2026-11-11: no session table, no load)
        var data = Compiler.query(ctx, "|sourceUrl('" + DataDeliveryMatrixTest.PAYLOAD + "')"
                + DataDeliveryMatrixTest.FROM_JSON);
        var text = data.plan("x::RT");
        assertEquals(data.executionPlan("x::RT").answerSql(), text.statement());
        assertFalse(text.textAlone(), text.sql());
        assertTrue(text.statement().slots().stream().anyMatch(s -> s.binding()
                instanceof com.legend.executionplan.ExecutionPlan.Binding.Payload), text.statement()::toString);
        assertEquals(java.util.List.of(), text.target().loads(), "no load: " + text.target().loads());
        assertFalse(text.statement().content().isEmpty(), "the payload is the statement's content: " + text.statement());
        assertFalse(text.sql().contains("data:"), "the payload is bound, never the statement's text: " + text.sql());
        // a host runs it (the tenets audit's M3, 2026-11-11): the payload its statement binds is no longer beyond its
        // text -- the host binds it from the plan carried beside the text
        assertEquals(null, text.beyondItsText(), text.sql());
        assertTrue(text.bindsPayload(), text.sql());
        // a file: source: its path a slot the run binds, never the statement's text
        String file = "file:///no/such/rows.json";
        var read = Compiler.query(ctx, "|sourceUrl('" + file + "')" + DataDeliveryMatrixTest.FROM_JSON);
        var fileText = read.plan("x::RT");
        assertEquals(read.executionPlan("x::RT").answerSql(), fileText.statement());
        assertFalse(fileText.textAlone(), fileText.sql());
        assertTrue(fileText.statement().slots().stream().anyMatch(s -> s.binding()
                .equals(new com.legend.executionplan.ExecutionPlan.Binding.Source(file))), fileText.statement()::toString);
        assertFalse(fileText.sql().contains("no/such"), "the path is bound, never written: " + fileText.sql());
        // a file: path a host does not bind: beyond its text still
        assertEquals("its statement binds a file: source's path", fileText.beyondItsText());
    }

    /** A CATALOG READ does not run as its text (the B checkpoint's tenets audit, LOW 8, 2026-11-11): its statement runs
     *  on the process's catalog, never the runtime's connection -- its plan's System target -- so arrowPlan, whose host
     *  runs the text itself, refuses it by name, on an H2 runtime (its catalog H2's in process) and a Postgres one (its
     *  catalog DuckDB's, whose text was answered as Postgres's) alike. generatePlan answers legend-engine's plan alone,
     *  nothing beside it (M5, the user's ruling of 2026-11-11: "yes, take out of compatibility plan"): lite's plan, its
     *  catalog target, reaches a lite host by lite's own routes. */
    @Test
    void aCatalogReadDoesNotRunAsItsText() {
        for (DatabaseType db : java.util.List.of(DatabaseType.H2, DatabaseType.Postgres)) {
            String model = DataDeliveryMatrixTest.model(DataDeliveryMatrixTest.connection(db,
                    DataDeliveryMatrixTest.localH2("testDataSetupCSV: ''")), "");
            var query = Compiler.query(Compiler.compileModel(model), "|" + DataDeliveryMatrixTest.CATALOG);
            assertTrue(query.plan("x::RT").target().database()
                    instanceof com.legend.executionplan.ExecutionPlan.Database.System, db.name());
            String lambda = com.legend.server.PureV1Api.grammarToJsonLambda("|" + DataDeliveryMatrixTest.CATALOG,
                    false).json();
            String request = "{\"clientVersion\":\"vX_X_X\",\"function\":" + lambda + ",\"model\":"
                    + com.legend.json.Json.toCompact(java.util.Map.of("_type", "text", "code", model))
                    + ",\"runtime\":{\"runtime\":\"x::RT\"},\"context\":{\"_type\":\"BaseExecutionContext\"}}";
            var generated = com.legend.server.PureV1Api.generatePlan(request);
            assertEquals(200, generated.status(), generated.json());
            assertTrue(SINGLE_EXECUTION_PLAN.containsAll(com.legend.json.Json.parseObject(generated.json()).fields()
                    .keySet()), db + ": legend-engine's plan alone: " + generated.json());
            var arrow = com.legend.server.PureV1Api.arrowPlan(request, java.util.List.of(model));
            assertTrue(arrow.json().contains("the system metamodel store"), db + ": " + arrow.json());
        }
    }

    /** The fields of legend-engine's SingleExecutionPlan (protocol-pure 4.145.0, SingleExecutionPlan.java:29-37, and
     *  ExecutionPlan's {@code _type}): its reader fails on any other top-level field (Jackson's
     *  FAIL_ON_UNKNOWN_PROPERTIES, no @JsonIgnoreProperties). */
    static final java.util.Set<String> SINGLE_EXECUTION_PLAN = java.util.Set.of("_type", "authDependent", "kerberos",
            "serializer", "templateFunctions", "rootExecutionNode", "globalImplementationSupport");

    /** generatePlan answers legend-engine's plan and nothing beside it (M5, the user's ruling of 2026-11-11: "yes, take
     *  out of compatibility plan"), what the text alone does not say -- a data: payload, a file: path, a pivot's keys --
     *  included; arrowPlan and the JSON execute's plan half, lite's own routes whose host runs the text itself, carry
     *  lite's plan, the payload its statement binds in its content (the tenets audit's M3, 2026-11-11), so the host
     *  binds it. */
    @Test
    void generatePlanAnswersTheEnginesPlanAlone_arrowPlanCarriesLitesPlan() {
        String model = DataDeliveryMatrixTest.model(DataDeliveryMatrixTest.bareConnection(DatabaseType.DuckDB), "");
        java.util.function.Function<String, String> request = text -> "{\"clientVersion\":\"vX_X_X\",\"function\":"
                + com.legend.server.PureV1Api.grammarToJsonLambda(text, false).json() + ",\"model\":"
                + com.legend.json.Json.toCompact(java.util.Map.of("_type", "text", "code", model))
                + ",\"runtime\":{\"runtime\":\"x::RT\"},\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        String payload = "|sourceUrl('" + DataDeliveryMatrixTest.PAYLOAD + "')" + DataDeliveryMatrixTest.FROM_JSON;
        String file = "|sourceUrl('file:///no/such/rows.json')" + DataDeliveryMatrixTest.FROM_JSON;
        String pivot = "|#TDS{\ndept, city, name\nA, NYC, x\nA, SAN, y\nB, NYC, z\n}#"
                + "->pivot(~[city], ~[names:r|$r.name:y|$y->joinStrings(',')])->from(x::RT)";
        for (String query : java.util.List.of(payload, file, pivot)) {
            var generated = com.legend.server.PureV1Api.generatePlan(request.apply(query));
            assertEquals(200, generated.status(), generated.json());
            var plan = com.legend.json.Json.parseObject(generated.json());
            assertTrue(SINGLE_EXECUTION_PLAN.containsAll(plan.fields().keySet()),
                    "a field legend-engine's reader refuses: " + plan.fields().keySet());
            assertFalse(generated.json().contains("legendLitePlan"), generated.json());
        }
        // a pivot whose columns the data names: the keyed plan lite runs, marked dynamic as upstream marks its own
        var keyed = com.legend.json.Json.parseObject(com.legend.server.PureV1Api.generatePlan(request.apply(pivot)).json());
        var sql = (com.legend.json.Json.Obj) keyed.getObj("rootExecutionNode").getArr("executionNodes").items().get(0);
        assertEquals("sql", sql.getString("_type"));
        assertEquals(com.legend.json.Json.parse("true"), sql.get("isResultColumnsDynamic"), com.legend.json.Json.toCompact(sql));
        // a statement whose columns are known is not marked
        var fixed = com.legend.json.Json.parseObject(com.legend.server.PureV1Api.generatePlan(request.apply(payload)).json());
        var fixedSql = (com.legend.json.Json.Obj) fixed.getObj("rootExecutionNode").getArr("executionNodes").items().get(0);
        assertFalse(fixedSql.has("isResultColumnsDynamic"), com.legend.json.Json.toCompact(fixedSql));
        String lambda = com.legend.server.PureV1Api.grammarToJsonLambda(payload, false).json();
        String requestText = "{\"clientVersion\":\"vX_X_X\",\"function\":" + lambda + ",\"model\":"
                + com.legend.json.Json.toCompact(java.util.Map.of("_type", "text", "code", model))
                + ",\"runtime\":{\"runtime\":\"x::RT\"},\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        var arrow = com.legend.server.PureV1Api.arrowPlan(requestText, java.util.List.of(model));
        assertEquals(200, arrow.status(), arrow.json());
        var arrowed = com.legend.json.Json.parseObject(arrow.json());
        assertTrue(arrowed.getString("sql").contains("?") && !arrowed.getString("sql").contains("data:"), arrow.json());
        var root = arrowed.getObj("plan").getObj("root").getObj("sql");
        assertTrue(com.legend.json.Json.toCompact(root.getArr("slots")).contains("\"payload\""), arrow.json());
        assertFalse(root.getObj("content").fields().isEmpty(), "the payload, the statement's content: " + arrow.json());
        // the JSON execute's plan half: its `run` the statement, the plan beside it to bind from
        var json = com.legend.server.PureV1Api.jsonPlan(requestText, java.util.List.of(model));
        assertEquals(200, json.status(), json.json());
        var planned = com.legend.json.Json.parseObject(json.json());
        assertEquals(planned.getString("sql"), planned.getString("run"));
        assertFalse(planned.getObj("plan").getObj("root").getObj("sql").getObj("content").fields().isEmpty(), json.json());
    }
}
