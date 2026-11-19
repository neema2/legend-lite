// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.json.Json;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE SQL TEXT A LEGEND-ENGINE REQUEST ASKS FOR, AFTER A2 (the compatibility program's stage A, ratified by the user
 * 2026-11-19; the compat owner's planted requests for A2's first PR). legend-engine's SQL text printers were deleted
 * under CP-4 (upstream's SQL text is never promised): a request that itself asks for SQL text answers lite's own, and
 * every other request is untouched. The pure/v1 route reached those printers through two hooks alone (census B2's
 * bytecode walk from {@code PureV1Routes.generatePlan}, A2-shape-note.md section 2): the upstream plan facet (a
 * plan-text or DDL native) and the activity-text two-form (a query reading an execute's {@code activities[].sql}).
 *
 * <p>One request of each shape, sent to pure/v1's execute and generatePlan as the edge golden net sends its own:
 * <ul>
 *   <li>{@code ordinary}: the net's own {@code tds} request, a plain relation query, which reaches neither hook. Its
 *   execute answer and its plan are BYTE-IDENTICAL to the net's lines, recorded before A2 (the net's
 *   {@code edge/pure_v1_answers.tsv}, current at main {@code b75d6c73a});</li>
 *   <li>{@code ddl}: a DDL native's text (the upstream plan facet): exactly what the edge's H2 dialect writes for the
 *   table ({@code PureText.dialect}), lite's own DDL;</li>
 *   <li>{@code activitySql}: an execute's activity SQL read back (the activity-text two-form): lite's H2 SQL of the
 *   inner query.</li>
 * </ul>
 * The two SQL-text requests are answered through generatePlan, their plans pinned as golden lines
 * ({@code edge/pure_v1_sql_text.tsv}, ids masked as the net masks them): pure/v1's execute refuses a scalar result by
 * name ("a SCALAR result's serialization is unprobed"), before A2 and after. With {@code LEGEND_A2_RECORD} set in the environment, the test prints the lines it computed
 * instead of comparing them, to record them.
 */
class PureV1SqlTextTest {

    private static final String RT = "->from(trades::h2::RT)";
    private static final String TRADES = "#>{trades::h2::DB.TRADES_SCHEMA.TRADES}#";

    /** name -> the lambda's grammar. */
    private static final Map<String, String> REQUESTS = new LinkedHashMap<>();

    static {
        // the net's "tds" request, as PureV1EdgeGoldenTest sends it
        REQUESTS.put("ordinary", "|" + TRADES + "->select(~[year, region])->sort([~year->ascending(),"
                + " ~region->ascending()])->limit(2)" + RT);
        REQUESTS.put("ddl", "|meta::relational::functions::toDDL::createTableStatement(trades::h2::DB, 'TRADES_SCHEMA',"
                + " 'TRADES')");
        REQUESTS.put("activitySql", "|meta::pure::router::execute(|" + TRADES + "->select(~[year])->limit(1),"
                + " trades::h2::M, trades::h2::RT.runtimeValue, []).activities->at(0)"
                + "->cast(@meta::relational::mapping::RelationalActivity).sql");
    }

    /** The net's model; with an empty mapping for the execute the activity request reads back. */
    private static String model(String request) throws IOException {
        String model = resource("upstream-api/trades-h2.pure");
        return request.equals("activitySql") ? model + "\n###Mapping\nMapping trades::h2::M\n(\n)\n" : model;
    }

    private static String resource(String name) throws IOException {
        try (InputStream in = PureV1SqlTextTest.class.getClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("no resource " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static final java.util.regex.Pattern IDS = java.util.regex.Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    private static String answer(java.util.function.Supplier<LiteV1Api.Answer> call) {
        try {
            LiteV1Api.Answer a = call.get();
            String json = a.json();
            try {
                json = Json.toCompact(Json.parseObject(json));
            } catch (RuntimeException notJson) {
                // an answer that is no JSON object is recorded as it is
            }
            return a.status() + "\t" + json;
        } catch (RuntimeException e) {
            return "THREW\t" + e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    private static String masked(String line) {
        return IDS.matcher(line).replaceAll("<id>").replace("\n", "\\n").replace("\r", "");
    }

    /** Each request's execute and generatePlan answers, by line name ({@code execute.<name>}, {@code plan.<name>}). */
    static Map<String, String> answers() throws IOException {
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<String, String> r : REQUESTS.entrySet()) {
            String model = Json.toCompact(Map.of("_type", "text", "code", model(r.getKey())));
            String function = com.legend.edge.PureV1Routes.grammarToJsonLambda(r.getValue(), false).json();
            // the SQL-text requests name their runtime as Studio's ExecuteInput does (a runtimePointer); the ordinary one
            // is the net's body exactly
            String runtime = r.getKey().equals("ordinary") ? ""
                    : ",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"trades::h2::RT\"}";
            String body = "{\"clientVersion\":\"vX_X_X\",\"function\":" + function + ",\"model\":" + model
                    + runtime + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}";
            if (r.getKey().equals("ordinary")) {
                out.put("execute." + r.getKey(), masked(answer(() -> com.legend.edge.PureV1Routes.execute(body,
                        PureV1EdgeGoldenTest.RECORDER))));
            }
            out.put("plan." + r.getKey(), masked(answer(() -> com.legend.edge.PureV1Routes.generatePlan(body))));
        }
        return out;
    }

    /** A golden file: one {@code name TAB answer} line each. */
    private static Map<String, String> golden(String name) throws IOException {
        Map<String, String> out = new LinkedHashMap<>();
        for (String line : resource(name).split("\n")) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            int tab = line.indexOf('\t');
            out.put(line.substring(0, tab), line.substring(tab + 1));
        }
        return out;
    }

    @Test
    void anOrdinaryRequestIsUntouchedAndASqlTextRequestAnswersLitesSql() throws IOException {
        Map<String, String> answers = answers();
        if (System.getenv("LEGEND_A2_RECORD") != null) {
            answers.forEach((k, v) -> System.out.println("[a2-sql-text]\t" + k + "\t" + v));
            return;
        }
        // the ordinary request: byte-identical to the net's lines for the same request, recorded before A2
        Map<String, String> net = golden("edge/pure_v1_answers.tsv");
        assertEquals(net.get("tds"), answers.get("execute.ordinary"), "the ordinary request's execute answer");
        assertEquals(net.get("plan.tds"), answers.get("plan.ordinary"), "the ordinary request's plan");
        assertFalse(answers.get("plan.ordinary").contains("THREW"), answers.get("plan.ordinary"));
        // the SQL-text requests: their golden lines
        Map<String, String> golden = golden("edge/pure_v1_sql_text.tsv");
        for (String k : java.util.List.of("plan.ddl", "plan.activitySql")) {
            assertEquals(golden.get(k), answers.get(k), k);
        }
        // the DDL asked for is exactly the edge's H2 dialect's, the SQL lite runs on this route
        var ctx = com.legend.Compiler.compileModel(model("ddl"));
        var table = ctx.findTableDefinition("trades::h2::DB", "TRADES_SCHEMA.TRADES").orElseThrow();
        String lite = com.legend.edge.PureText.dialect(com.legend.model.ConnectionDefinition.DatabaseType.H2)
                .render(com.legend.setup.Ddl.createTable(table, "TRADES_SCHEMA"));
        String plan = answers.get("plan.ddl");
        String value = Json.parseObject(plan.substring(plan.indexOf('\t') + 1)).getObj("rootExecutionNode")
                .getObj("values").getString("value");
        assertEquals(lite, value, "the DDL native's text");
        // the activity's SQL is lite's H2 text of the inner query, never legend-engine's ("root" aliases, lower case)
        String sql = answers.get("plan.activitySql");
        assertTrue(sql.contains("'SELECT \\\"t0\\\".\\\"YEAR\\\" AS \\\"year\\\"\\nFROM TRADES_SCHEMA.TRADES AS \\\"t0\\\"\\nLIMIT 1'"),
                sql);
        assertFalse(sql.contains("\\\"root\\\""), sql);
    }
}
