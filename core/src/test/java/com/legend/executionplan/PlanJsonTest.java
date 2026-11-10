// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan.Column;
import com.legend.executionplan.ExecutionPlan.Database;
import com.legend.executionplan.ExecutionPlan.Multiplicity;
import com.legend.executionplan.ExecutionPlan.Parameter;
import com.legend.executionplan.ExecutionPlan.Sequence;
import com.legend.executionplan.ExecutionPlan.LoadStep;
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
        return com.legend.planner.LoadPlanner.withLoads(new Target(new Database.Declared(com.legend.planner.ConnectionPlans.of(new ConnectionDefinition("store::Conn", "store::DB", Databases.PLATFORM,
                spec, auth)), null), List.of(), List.of()),
                List.of(new LoadStep.Statement("create table T(ID INT)")));
    }

    @Test
    void everyNodeKindAndParameterReadsBack() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        ExecutionPlan plan = ExecutionPlan.reading(
                List.of(new Parameter("name", "String", new Multiplicity(1, 1), List.of()),
                        new Parameter("ids", "Integer", new Multiplicity(0, null), List.of()),
                        new Parameter("status", "model::Status", new Multiplicity(1, 1), List.of("ACTIVE", "CLOSED"))),
                new Sequence(List.of(
                        new TdsResult(TdsResult.Shape.RELATION, List.of(new Column("name", "String", new Multiplicity(1, 1), "VARCHAR", "VARCHAR(100)", null)),
                                new Sql("select NAME as \"name\" from T where NAME = ? and ID = ANY(?)",
                                        List.of(new Slot("name", new ExecutionPlan.Binding.One("VARCHAR", null)),
                                                new Slot("ids", new ExecutionPlan.Binding.Array("INTEGER"))), t, null)),
                        // a placeholder of a declared kind (H2): its fixed type in the text, bound bare (P4)
                        new TdsResult(TdsResult.Shape.RELATION, List.of(new Column("x", "Float", new Multiplicity(1, 1), "DOUBLE", null, null)),
                                new Sql("select ID * CAST(? AS FLOAT) as x from T", List.of(new Slot("f",
                                        new ExecutionPlan.Binding.One("DECIMAL", null))), t, null)),
                        // a date-time bound as its text, a finer digit cut to six (Postgres)
                        // (its written-precision fact too, format 17: the column's fourth fact round-trips; a value
                        // root's instant is MINIMAL, format 18)
                        new TdsResult(TdsResult.Shape.VALUE, List.of(new Column("t", "DateTime", new Multiplicity(0, 1), "TIMESTAMP", null,
                                ExecutionPlan.Precision.MINIMAL)),
                                new Sql("select CAST(? AS TIMESTAMP) as t", List.of(new Slot("t",
                                        new ExecutionPlan.Binding.One("TIMESTAMP", new ExecutionPlan.AsText(6)))), t,
                                        null)),
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
    void bothLoadStepKindsReadBack_theStatementsBlobInTheBundle() {
        Target t = com.legend.planner.LoadPlanner.withLoads(new Target(new Database.Declared(com.legend.planner.ConnectionPlans.of(new ConnectionDefinition("store::Conn", "store::DB",
                Databases.PLATFORM, new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth())), null),
                List.of("SET TimeZone='UTC'"), List.of()),
                com.legend.setup.LoadSteps.of(List.of(new LoadStep.Statement("create table T(ID INTEGER, NAME VARCHAR)")))
                        .then(com.legend.setup.LoadSteps.blob(List.of("insert into T values (1, 'O''Brien'), (2, NULL)"))));
        ExecutionPlan plan = ExecutionPlan.reading(List.of(), new TdsResult(TdsResult.Shape.RELATION, List.of(), new Sql("select 1", List.of(), t, null)));
        ExecutionPlan back = PlanJson.read(PlanJson.write(plan));
        assertEquals(plan, back);
        ExecutionPlan.Load load = ((TdsResult) back.root()).sql().target().loads().get(0);
        LoadStep.Statements blob = (LoadStep.Statements) load.steps().get(1);
        assertEquals(List.of("insert into T values (1, 'O''Brien'), (2, NULL)"),
                DataRef.Statements.decode(load.content().get(blob.ref().digest())));
    }

    @Test
    void thePlatformsEngineReadsBack_withNoSetup() {
        Target t = new Target(new Database.Platform(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null), List.of("SET TimeZone='UTC'"), List.of());
        ExecutionPlan plan = ExecutionPlan.reading(List.of(), new TdsResult(TdsResult.Shape.VALUE,
                List.of(new Column("value", "Integer", new Multiplicity(1, 1), "BIGINT", null, null)), new Sql("select 1", List.of(), t, null)));
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)));
    }

    @Test
    void theGraphsSystemDatabaseReadsBack() {
        // the test-is-the-product plan's M10a: a test body reading the metamodel store runs on the graph's system
        // database on its engine; the plan names it by type alone (the one who opens it holds the session)
        Target t = new Target(new Database.System(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), "a-graph"),
                List.of(), List.of());
        ExecutionPlan plan = ExecutionPlan.reading(List.of(), new TdsResult(TdsResult.Shape.RELATION, List.of(),
                new Sql("select 1", List.of(), t, null)));
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)));
        assertTrue(PlanJson.write(plan).contains("\"_type\":\"system\""), PlanJson.write(plan));
    }

    @Test
    void aStatementsStepIsAContentBlobOfStatements() {
        var refused = assertThrows(IllegalArgumentException.class, () -> new LoadStep.Statements(new DataRef(
                DataRef.Namespace.RESOURCE, "x.json", "0".repeat(64), 2, DataRef.Statements.JSON)));
        assertTrue(refused.getMessage().contains("a CONTENT blob of statements"), refused.getMessage());
        var empty = assertThrows(IllegalArgumentException.class, () -> DataRef.Statements.decode("[]"));
        assertTrue(empty.getMessage().contains("non-empty JSON array of strings"), empty.getMessage());
    }

    @Test
    void theTypedTreeIsNotWrittenYet() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        String json = PlanJson.write(ExecutionPlan.reading(List.of(), new TdsResult(TdsResult.Shape.RELATION, List.of(),
                new Sql("select 1", List.of(), t, null))));
        // 8 since 2026-10-20: the keyed result, per-key regions, a key's cell (the dynamic pivot's probe plan); 9 the
        // same day: the keyed result's cells named as its statements write them (keyFields, an output's field); 12
        // since 2026-10-27: a typed result's shape (a relation's rows, a value's elements, a graph fetch's objects); 13
        // the same day: a text result is Pure's TDS text alone; 14 the same day: no text result at all; 15 the same day:
        // the effect node (a statement sent for what it does, the body driver's effects on the plan); 16 the same day: a
        // keyed result's per-key columns placed among its fixed ones; 17 since 2026-10-28: a column's written-precision
        // fact (the result boundary program's R2, the start page's S1.4 item 1); 18 since 2026-10-29: the fact's
        // MINIMAL value, a value root's instant; 19 the same day: the LITERAL slot word retired (track L's L2); 20 since
        // 2026-10-30: the verdicts node (the planner-driver seam program's S1b); 21 the same day: a constant side's
        // folded flag (S2f); 22 the same day: a keyed result's key source, a statement's rows or its reported columns
        // (S3c, a raw grid's); 23 since 2026-10-31: the constant node (the test-is-the-product plan's M2, MV12); 24 the
        // same day: an effect's script, the seed step, a verdict group's savepoint (M3b, B12); 27 since 2026-11-08: the
        // verdict group and the branch leave for the test plan's own format (the plan-as-data design's step 3); 28 the
        // same day: the system database names its store by its content's key (step 7); 29 the same day: a declared or
        // platform database carries its project's identity (hole 7); 30 since 2026-11-09: whether the plan writes (slice 5);
        // 31 the same day: the load node and a file's load by reference (docs/DATA_IN_THE_PLAN_2026_11_09.md L1); 32
        // the same day: a reference's declared format, an object, and the one streamed delivery (L2); 33 the same day: a
        // PER_TRANSACTION load, the system catalog's registration (L4b); 38 since 2026-11-10: the model's data a load
        // step of statements, a content blob by digest, the rows step and format gone (A2)
        assertTrue(json.startsWith("{\"format\":\"legend-lite-plan\",\"version\":38,"), json);
    }

    @Test
    void anotherFormatIsRefusedByName() {
        var refused = assertThrows(IllegalArgumentException.class,
                () -> PlanJson.read("{\"_type\":\"simple\",\"rootExecutionNode\":{}}"));
        assertTrue(refused.getMessage().contains("not a legend-lite-plan v38 plan"), refused.getMessage());
    }

    @Test
    void aParameterWithoutItsEnumValuesIsRefused_neverReadAsNone() {
        Target t = target(new ConnectionSpecification.InMemory(), new AuthenticationSpec.TestAuth());
        String json = PlanJson.write(ExecutionPlan.reading(List.of(new Parameter("s", "model::Status",
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

    /** The load section (the M1 correctness audit's C2, 2026-11-09): several targets naming one load write it once and
     *  read back as one; a section naming an identity twice, a target naming an identity the section lacks, and two
     *  contents under one identity are each refused by name. */
    @Test
    void theLoadSectionWritesEachLoadOnce_andRefusesWhatIsNotOneLoadPerIdentity() {
        Target bare = new Target(new Database.Platform(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM),
                null), List.of(), List.of());
        Target t = com.legend.planner.LoadPlanner.withLoads(bare, List.of(new LoadStep.Statement("create table T(ID INT)")));
        String identity = t.loads().get(0).identity();
        ExecutionPlan plan = ExecutionPlan.reading(List.of(), new Sequence(List.of(
                new TdsResult(TdsResult.Shape.RELATION, List.of(), new Sql("select 1", List.of(), t, null)),
                new TdsResult(TdsResult.Shape.RELATION, List.of(), new Sql("select 2", List.of(), t, null)))));
        String json = PlanJson.write(plan);
        assertEquals(json.indexOf("create table T(ID INT)"), json.lastIndexOf("create table T(ID INT)"), json);
        assertTrue(json.contains("create table T(ID INT)"), json);
        assertEquals(plan, PlanJson.read(json));
        // the section naming one identity twice
        String section = json.substring(json.indexOf("\"loads\":[{"));
        String twice = json.replace("\"loads\":[{", "\"loads\":[" + section.substring("\"loads\":[".length(),
                section.length() - 2) + ",{");
        IllegalArgumentException dup = assertThrows(IllegalArgumentException.class, () -> PlanJson.read(twice));
        assertTrue(dup.getMessage().contains("names a load twice"), dup.getMessage());
        // a target naming a load the section lacks
        String dangling = json.replace("\"loads\":[\"" + identity + "\"]", "\"loads\":[\"load:absent\"]");
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> PlanJson.read(dangling));
        assertTrue(missing.getMessage().contains("does not hold"), missing.getMessage());
        // two contents under one identity, when the plan is written
        ExecutionPlan.Load other = new ExecutionPlan.Load(identity, ExecutionPlan.Load.Cadence.PER_ACQUISITION,
                List.of(new LoadStep.Statement("create table U(ID INT)")), bare, java.util.Map.of());
        ExecutionPlan clash = ExecutionPlan.reading(List.of(), new Sequence(List.of(
                new TdsResult(TdsResult.Shape.RELATION, List.of(), new Sql("select 1", List.of(), t, null)),
                new TdsResult(TdsResult.Shape.RELATION, List.of(), new Sql("select 2", List.of(),
                        new Target(bare.database(), bare.session(), List.of(other)), null)))));
        IllegalStateException differ = assertThrows(IllegalStateException.class, () -> PlanJson.write(clash));
        assertTrue(differ.getMessage().contains("two loads of one identity differ"), differ.getMessage());
    }
}
