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
        // a file: source: its path a slot the run binds, never the statement's text
        String file = "file:///no/such/rows.json";
        var read = Compiler.query(ctx, "|sourceUrl('" + file + "')" + DataDeliveryMatrixTest.FROM_JSON);
        var fileText = read.plan("x::RT");
        assertEquals(read.executionPlan("x::RT").answerSql(), fileText.statement());
        assertFalse(fileText.textAlone(), fileText.sql());
        assertTrue(fileText.statement().slots().stream().anyMatch(s -> s.binding()
                .equals(new com.legend.executionplan.ExecutionPlan.Binding.Source(file))), fileText.statement()::toString);
        assertFalse(fileText.sql().contains("no/such"), "the path is bound, never written: " + fileText.sql());
    }

    /** generatePlan carries the execution plan its text renders from, where the text does not run alone (its loads and
     *  slots: lite's own field beside upstream's plan); arrowPlan, whose host runs the text itself, carries it too, and
     *  refuses by name what the host cannot run yet (a data: source's payload its statement binds: the JSON pass). */
    @Test
    void generatePlanAndArrowPlanCarryTheExecutionPlan() {
        String model = DataDeliveryMatrixTest.model(DataDeliveryMatrixTest.bareConnection(DatabaseType.DuckDB), "");
        String lambda = com.legend.server.PureV1Api.grammarToJsonLambda("|sourceUrl('" + DataDeliveryMatrixTest.PAYLOAD
                + "')" + DataDeliveryMatrixTest.FROM_JSON, false).json();
        String request = "{\"clientVersion\":\"vX_X_X\",\"function\":" + lambda + ",\"model\":"
                + com.legend.json.Json.toCompact(java.util.Map.of("_type", "text", "code", model))
                + ",\"runtime\":{\"runtime\":\"x::RT\"},\"context\":{\"_type\":\"BaseExecutionContext\"}}";
        var generated = com.legend.server.PureV1Api.generatePlan(request);
        assertEquals(200, generated.status(), generated.json());
        var carried = com.legend.json.Json.parseObject(generated.json()).getObj("legendLitePlan");
        assertEquals(com.legend.executionplan.PlanJson.FORMAT, carried.getString("format"));
        assertTrue(com.legend.json.Json.toCompact(carried).contains("\"payload\""),
                "the data: source's payload slot: " + generated.json());
        var arrow = com.legend.server.PureV1Api.arrowPlan(request, java.util.List.of(model));
        assertTrue(arrow.json().contains("its statement binds a data: source's payload"), arrow.json());
    }
}
