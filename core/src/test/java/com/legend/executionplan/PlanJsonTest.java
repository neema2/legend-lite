// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan.Column;
import com.legend.executionplan.ExecutionPlan.Database;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.executionplan.ExecutionPlan.Parameter;
import com.legend.executionplan.ExecutionPlan.Sequence;
import com.legend.executionplan.ExecutionPlan.Servers;
import com.legend.executionplan.ExecutionPlan.SetupStep;
import com.legend.executionplan.ExecutionPlan.Slot;
import com.legend.executionplan.ExecutionPlan.Sql;
import com.legend.executionplan.ExecutionPlan.Target;
import com.legend.executionplan.ExecutionPlan.TdsResult;
import com.legend.model.AuthenticationSpec;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionSpecification;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The lite plan format reads back what it writes: every node kind (every connection specification and authentication:
 *  the model's codec, {@code model.ConnectionWireTest}, since the dialect program's Phase 6, 2026-10-16). */
class PlanJsonTest {

    private static Target target(ConnectionSpecification spec, AuthenticationSpec auth) {
        return new Target(new Database.Declared(new ConnectionDefinition("store::Conn", "store::DB", Databases.PLATFORM,
                spec, auth)), new Servers.Versions(List.of("2.1", "2.2")), List.of(),
                List.of(new SetupStep.Statement("create table T(ID INT)")));
    }

    @Test
    void everyNodeKindAndParameterReadsBack() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        ExecutionPlan plan = new ExecutionPlan(
                List.of(new Parameter("name", "String", new Multiplicity(1, 1), List.of()),
                        new Parameter("ids", "Integer", new Multiplicity(0, null), List.of()),
                        new Parameter("status", "model::Status", new Multiplicity(1, 1), List.of("ACTIVE", "CLOSED"))),
                new Sequence(List.of(
                        new TdsResult(TdsResult.Shape.RELATION, List.of(new Column("name", "String", new Multiplicity(1, 1), "VARCHAR", "VARCHAR(100)", null)),
                                new Sql("select NAME as \"name\" from T where NAME = ? and ID = ANY(?)",
                                        List.of(new Slot("name", new ExecutionPlan.Binding.One("VARCHAR", null)),
                                                new Slot("ids", new ExecutionPlan.Binding.Array("INTEGER"))), t, null)),
                        // a placeholder typed by its value (H2): its type hole, with each kind's spelling
                        new TdsResult(TdsResult.Shape.RELATION, List.of(new Column("x", "Float", new Multiplicity(1, 1), "DOUBLE", null, null)),
                                new Sql("select ID * CAST(? AS ) as x from T", List.of(new Slot("f",
                                        new ExecutionPlan.Binding.One("DECIMAL", new ExecutionPlan.TypeHole(22,
                                                java.util.Map.of(ExecutionPlan.ValueKind.DECIMAL,
                                                        new ExecutionPlan.TypeSpelling("NUMERIC",
                                                                ExecutionPlan.Digits.PRECISION_AND_SCALE),
                                                        ExecutionPlan.ValueKind.FLOATING,
                                                        new ExecutionPlan.TypeSpelling("DECFLOAT",
                                                                ExecutionPlan.Digits.PRECISION)),
                                                ExecutionPlan.ValueKind.DECIMAL)))), t, null)),
                        // a date-time's hole, a finer one cut to six digits (Postgres)
                        // (its written-precision fact too, format 17: the column's fourth fact round-trips; a value
                        // root's instant is MINIMAL, format 18)
                        new TdsResult(TdsResult.Shape.VALUE, List.of(new Column("t", "DateTime", new Multiplicity(0, 1), "TIMESTAMP", null,
                                ExecutionPlan.Precision.MINIMAL)),
                                new Sql("select CAST(? AS ) as t", List.of(new Slot("t",
                                        new ExecutionPlan.Binding.One("TIMESTAMP", new ExecutionPlan.TypeHole(17,
                                                java.util.Map.of(ExecutionPlan.ValueKind.DATE_TIME,
                                                        new ExecutionPlan.TypeSpelling("TIMESTAMP",
                                                                ExecutionPlan.Digits.NONE),
                                                        ExecutionPlan.ValueKind.DATE_TIME_NANOS,
                                                        new ExecutionPlan.TypeSpelling("TIMESTAMP",
                                                                ExecutionPlan.Digits.NONE, 6)),
                                                ExecutionPlan.ValueKind.DATE_TIME)))), t, null)),
                        new TdsResult(TdsResult.Shape.OBJECTS, List.of(new Column("value", "model::Person", new Multiplicity(0, null), "VARCHAR", null, null)),
                                new Sql("select json_object(...) from T", List.of(), t, null)),
                        // a write plan's steps (the writes design's leg 7): a change whose count a later slot binds,
                        // one whose count none reads, and a check
                        new ExecutionPlan.Update(new Sql("update T set ID = ? where ID > 0", List.of(new Slot("id",
                                new ExecutionPlan.Binding.One("BIGINT", null))), t, null), ExecutionPlan.COUNT),
                        new ExecutionPlan.Update(new Sql("drop table if exists S;", List.of(), t, null), null),
                        new ExecutionPlan.Check(new Sql("select 1 from T", List.of(), t, null)))));
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)));
    }

    @Test
    void bothSetupStepKindsReadBack_aNullCellStaysNull() {
        Target t = new Target(new Database.Declared(new ConnectionDefinition("store::Conn", "store::DB",
                Databases.PLATFORM, new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth())),
                new Servers.Every(), List.of("SET TimeZone='UTC'"),
                List.of(new SetupStep.Statement("create table T(ID INTEGER, NAME VARCHAR)"),
                        new SetupStep.Rows("legend_row_load", "create temporary table legend_row_load(c0 VARCHAR, c1 VARCHAR)",
                                "insert into T(ID, NAME) select c0, c1 from legend_row_load", "drop table legend_row_load",
                                List.of(List.of("1", "O'Brien"), Arrays.asList("2", null)))));
        ExecutionPlan plan = new ExecutionPlan(List.of(), new TdsResult(TdsResult.Shape.RELATION, List.of(), new Sql("select 1", List.of(), t, null)));
        ExecutionPlan back = PlanJson.read(PlanJson.write(plan));
        assertEquals(plan, back);
        SetupStep.Rows rows = (SetupStep.Rows) ((TdsResult) back.root()).sql().target().setup().get(1);
        assertEquals(null, rows.rows().get(1).get(1));
    }

    @Test
    void thePlatformsEngineReadsBack_withNoSetup() {
        Target t = new Target(new Database.Platform(Databases.PLATFORM), new Servers.Every(),
                List.of("SET TimeZone='UTC'"), List.of());
        ExecutionPlan plan = new ExecutionPlan(List.of(), new TdsResult(TdsResult.Shape.VALUE,
                List.of(new Column("value", "Integer", new Multiplicity(1, 1), "BIGINT", null, null)), new Sql("select 1", List.of(), t, null)));
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)));
    }

    @Test
    void aStatementWrittenForSomeServerVersionsNamesOne() {
        var refused = assertThrows(IllegalArgumentException.class, () -> new Servers.Versions(List.of()));
        assertTrue(refused.getMessage().contains("names at least one"), refused.getMessage());
    }

    @Test
    void rowsOfDifferentWidthsAreRefused() {
        var refused = assertThrows(IllegalArgumentException.class, () -> new SetupStep.Rows("s", "c", "i", "d",
                List.of(List.of("1", "a"), List.of("2"))));
        assertTrue(refused.getMessage().contains("a row of 1 cells among rows of 2"), refused.getMessage());
    }

    @Test
    void theTypedTreeIsNotWrittenYet() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        String json = PlanJson.write(new ExecutionPlan(List.of(), new TdsResult(TdsResult.Shape.RELATION, List.of(),
                new Sql("select 1", List.of(), t, null))));
        // 8 since 2026-10-20: the keyed result, per-key regions, a key's cell (the dynamic pivot's probe plan); 9 the
        // same day: the keyed result's cells named as its statements write them (keyFields, an output's field); 12
        // since 2026-10-27: a typed result's shape (a relation's rows, a value's elements, a graph fetch's objects); 13
        // the same day: a text result is Pure's TDS text alone; 14 the same day: no text result at all; 15 the same day:
        // the effect node (a statement sent for what it does, the body driver's effects on the plan); 16 the same day: a
        // keyed result's per-key columns placed among its fixed ones; 17 since 2026-10-28: a column's written-precision
        // fact (the result boundary program's R2, the start page's S1.4 item 1); 18 since 2026-10-29: the fact's
        // MINIMAL value, a value root's instant; 19 the same day: the LITERAL slot word retired (track L's L2)
        assertTrue(json.startsWith("{\"format\":\"legend-lite-plan\",\"version\":19,"), json);
    }

    @Test
    void anotherFormatIsRefusedByName() {
        var refused = assertThrows(IllegalArgumentException.class,
                () -> PlanJson.read("{\"_type\":\"simple\",\"rootExecutionNode\":{}}"));
        assertTrue(refused.getMessage().contains("not a legend-lite-plan v19 plan"), refused.getMessage());
    }

    @Test
    void aParameterWithoutItsEnumValuesIsRefused_neverReadAsNone() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        String json = PlanJson.write(new ExecutionPlan(List.of(new Parameter("s", "model::Status",
                new Multiplicity(1, 1), List.of("A"))), new TdsResult(TdsResult.Shape.RELATION, List.of(), new Sql("select 1", List.of(), t, null))))
                .replace(",\"enumValues\":[\"A\"]", "");
        assertThrows(RuntimeException.class, () -> PlanJson.read(json));
    }

    @Test
    void anEarlierVersionIsRefused() {
        for (int version = 1; version <= 17; version++) {
            int v = version;
            var refused = assertThrows(IllegalArgumentException.class, () -> PlanJson.read(
                    "{\"format\":\"legend-lite-plan\",\"version\":" + v + ",\"parameters\":[],\"root\":{}}"));
            assertTrue(refused.getMessage().contains("(format legend-lite-plan, version " + v + ")"),
                    refused.getMessage());
        }
    }
}
