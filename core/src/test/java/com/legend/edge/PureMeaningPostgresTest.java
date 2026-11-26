// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.json.Json;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.server.LiteV1Api;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code generateTestData_WithSeed} ON A DATABASE WHERE LEGEND-ENGINE'S ANSWER IS NOT YET CHECKED (the reviewer's should
 * on 67fa4df51; the door's rule since the user's ruling of 2026-11-20 ~07:55). That route's rows are the identifiers'
 * a request names, so no call in its query chooses one, and the door does not refuse there (the route plans no seed;
 * the seed routes, whose rows a call does choose, plan theirs with legend-engine's meaning or refuse by name:
 * {@code PureMeaningEngineTest}). Run on the
 * embedded Postgres 16, a database not yet checked, as an unprivileged user ({@code --run_under}, docs/CLOUD_SESSIONS.md):
 * the same request with a flagged call answers the same CSV as without it, byte for byte, and names the identifiers'
 * row. The table is one every Postgres database holds, {@code information_schema.schemata}, so the test writes nothing:
 * its rows are the server's own schemas.
 */
class PureMeaningPostgresTest {

    private static String model(TestBackend postgres) {
        return String.join("\n",
                "###Relational\nDatabase t::DB ( Schema information_schema ( Table schemata ("
                        + " schema_name VARCHAR(64) PRIMARY KEY ) ) )",
                "###Pure\nClass t::Schema { name: String[1]; }",
                "###Mapping\nMapping t::M ( t::Schema: Relational { ~mainTable [t::DB]information_schema.schemata"
                        + " name: [t::DB]information_schema.schemata.schema_name } )",
                "###Connection\nRelationalDatabaseConnection t::Conn { store: t::DB; " + postgres.connectionBody() + " }",
                "###Runtime\nRuntime t::RT { mappings: [t::M]; connections: [ t::DB: [ c1: t::Conn ] ]; }",
                "");
    }

    /** A flagged call in the query's filter: legend-engine's answer for it is not yet checked on Postgres. */
    private static final String FLAGGED = "|t::Schema.all()->filter(s|$s.name == 'xpublic'->substring(1, 7))"
            + "->project([s|$s.name], ['n'])";

    private static final String CLEAN = "|t::Schema.all()->filter(s|$s.name == 'public')->project([s|$s.name], ['n'])";

    @Test
    void withSeedAnswersTheSameCsvWithAFlaggedCallAsWithout_onADatabaseNotYetChecked() throws Exception {
        TestBackend postgres = TestBackends.of(DatabaseType.Postgres);
        assertFalse(PureText.engineAnswers(DatabaseType.Postgres), "Postgres is not yet checked: the case this covers");
        try (Connection c = postgres.openWorkspace()) {
            LiteV1Api.Runner onPostgres = (query, runtime, output) -> com.legend.Execution.run(query, runtime,
                            com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)),
                            com.legend.exec.PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                            com.legend.exec.ProductRun.Establish.DECLARED_SETUP, java.util.Set.of(), PureText.PURE_V1, output,
                            com.legend.result.Batcher.DEFAULT_CAPACITY);
            String model = model(postgres);
            LiteV1Api.Answer flagged = withSeed(model, FLAGGED, onPostgres);
            LiteV1Api.Answer clean = withSeed(model, CLEAN, onPostgres);
            assertEquals(200, flagged.status(), flagged.json());
            assertEquals(200, clean.status(), clean.json());
            assertEquals(clean.json(), flagged.json());
            // the identifiers' row, and no other: the schema 'public', whatever the query's filter
            assertTrue(flagged.json().contains("public") && !flagged.json().contains("pg_catalog"), flagged.json());
        }
    }

    private static LiteV1Api.Answer withSeed(String modelText, String query, LiteV1Api.Runner runner) {
        String function = PureV1Routes.grammarToJsonLambda(query, true).json();
        Map<String, Object> model = new java.util.LinkedHashMap<>();
        model.put("_type", "text");
        model.put("code", modelText);
        String body = "{\"function\":" + function + ",\"mapping\":\"t::M\",\"runtime\":{\"_type\":\"runtimePointer\","
                + "\"runtime\":\"t::RT\"},\"model\":" + Json.toCompact(model) + ",\"tableRowIdentifiers\":"
                + "[{\"table\":{\"_type\":\"table\",\"table\":\"schemata\",\"schema\":\"information_schema\","
                + "\"database\":\"t::DB\"},\"rowIdentifiers\":[{\"columnValuePairs\":[{\"name\":\"schema_name\","
                + "\"value\":\"public\"}]}]}]}";
        return PureV1Routes.route("/api/pure/v1/execution/testDataGeneration/generateTestData_WithSeed", null, body,
                runner, LiteV1Api.Models.COMPILED);
    }
}
