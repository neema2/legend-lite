// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.leansql;

import com.legend.Compiler;
import com.legend.executionplan.PlanJson;
import com.legend.json.Json;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lean-SQL sets' capture ({@link QueryCaptureListener}, a test harness's observer; E-1, the user's ruling of
 * 2026-11-12), run with it ON (its own target, :query_capture_test, {@code -Dlegend.capture.queries}): it records a product query's model text, its query
 * text and its runtime's NAME, and nothing a connection resolves to; and it changes nothing the planner decides --
 * the recorded query planned again answers the same plan byte for byte, and channel A's DuckDB plans measure exactly
 * their pins, which a run with the switch OFF made (lean-sql/baseline.tsv).
 */
class QueryCaptureTest {

    private static final String MODEL = """
            Class m::P { name: String[1]; }
            ###Relational
            Database m::DB ( Table T ( NAME VARCHAR(20) ) )
            ###Mapping
            Mapping m::M ( m::P: Relational { ~mainTable [m::DB]T name: [m::DB]T.NAME } )
            ###Connection
            RelationalDatabaseConnection m::C
            {
              store: m::DB;
              type: Postgres;
              specification: Static { name: 'db'; host: 'localhost'; port: 5432; };
              auth: UserNamePassword { userNameVaultReference: 'vault::svc_user'; passwordVaultReference: 'vault::prod_pw'; };
            }
            ###Runtime
            Runtime m::RT { mappings: [ m::M ]; connections: [ m::DB: [ c: m::C ] ]; }
            """;

    private static final String QUERY = "|m::P.all()->project(~[name: p | $p.name])";

    private static Path file() {
        String option = System.getProperty(QueryCaptureListener.PROPERTY);
        assertTrue(option != null, "this target runs with the capture on");
        return Path.of(option);
    }

    @Test
    void theCaptureRecordsTextAndNamesAndChangesNoPlan() throws Exception {
        String planned = PlanJson.write(Compiler.query(Compiler.compileModel(MODEL), QUERY).executionPlan("m::RT"));
        Json.Obj recorded = null;
        for (String line : Files.readAllLines(file(), StandardCharsets.UTF_8)) {
            Json.Obj o = Json.parseObject(line);
            if (((Json.Str) o.get("query")).value().equals(QUERY)) {
                recorded = o;
            }
        }
        assertTrue(recorded != null, "the query was recorded");
        assertEquals(List.of("model", "query", "runtime"), List.copyOf(recorded.fields().keySet()));
        // the model as the caller wrote it: the credential's NAMES, never anything a vault resolves
        assertEquals(MODEL, ((Json.Str) recorded.get("model")).value());
        assertEquals("m::RT", ((Json.Str) recorded.get("runtime")).value());
        // planned again from what was recorded: the same plan, byte for byte
        String again = PlanJson.write(Compiler.query(Compiler.compileModel(((Json.Str) recorded.get("model")).value()),
                ((Json.Str) recorded.get("query")).value()).executionPlan(((Json.Str) recorded.get("runtime")).value()));
        assertEquals(planned, again);
    }

    /** With the switch on, channel A's DuckDB plans measure exactly the pins a run with the switch off made. */
    @Test
    void theSwitchOnPlansWhatTheSwitchOffPinned() throws Exception {
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> pinned = LeanSqlBaseline.read(Files.readString(
                com.legend.testing.Runfile.property("legend.lean_sql.baseline"), StandardCharsets.UTF_8));
        Map<LeanSqlBaseline.Key, LeanSqlBaseline.Measure> measured = LeanSqlBaseline.measure(
                Map.of("channel_a", com.legend.testing.Runfile.property("legend.lean_sql.channel_a")),
                t -> t == com.legend.database.Databases.PLATFORM);
        assertTrue(!measured.isEmpty());
        for (var e : measured.entrySet()) {
            LeanSqlBaseline.Measure pin = pinned.get(e.getKey());
            assertEquals(pin == null ? null : List.of(pin.statements(), pin.chars(), pin.plan()),
                    List.of(e.getValue().statements(), e.getValue().chars(), e.getValue().plan()), e.getKey().toString());
        }
    }
}
