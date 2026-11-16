// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.RaiseKind;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A PLAN DECLARES EVERY RAISE ITS STATEMENTS CAN SEND, ON EVERY DATABASE (audit B's item 1, 2026-11-16): the kinds are
 * recorded where the renderer writes a raise ({@code AnsiSqlRenderer.raiseMessage}), carried by each statement
 * ({@code ExecutionPlan.Sql#raises}) and gathered into the plan's {@code raises} ({@link PlanJson#raises}) -- typed, never
 * read back from a statement's text. Until 2026-11-16 the plan's raises were a scan of its texts for the raise's mark,
 * which missed a dialect that spells the mark in its own escape (BigQuery's), so exec refused the raise as undeclared.
 */
class PlanDeclaresItsRaisesTest {

    /** {@code query} on a connection of {@code type}: planning alone, no database. */
    private static ExecutionPlan planned(DatabaseType type, String query) {
        String model = """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, D INTEGER NOT NULL ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,D\\n1,0\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type.name());
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query).executionPlan("s::RT");
    }

    @Test
    void aDivisionsRaiseIsDeclaredOnEveryRelationalDatabase() {
        for (DatabaseType type : com.legend.database.Databases.relational()) {
            ExecutionPlan plan = planned(type, "|#>{s::DB.T}#->extend(~r: r|$r.ID / $r.D)->select(~r)");
            Map<String, String> raises = PlanJson.raises(plan);
            assertTrue(raises.containsKey(RaiseKind.DIVIDE_BY_ZERO.name()), type + ": " + raises);
            // the JSON carries each statement's kinds and the plan's map, and reads back the same
            ExecutionPlan back = PlanJson.read(PlanJson.write(plan));
            assertEquals(plan, back, type.toString());
            assertEquals(raises, PlanJson.raises(back), type.toString());
        }
    }

    /** Raising queries, each with the kind it must declare where a database plans it, or none where a database may
     *  check it itself (a text read as an Integer: the product's raise on some databases, the database's own error
     *  on others) -- whose marks are held to its declaration all the same. */
    private static final Map<String, java.util.Optional<RaiseKind>> RAISING = Map.of(
            "|#>{s::DB.T}#->extend(~r: r|$r.ID / $r.D)->select(~r)", java.util.Optional.of(RaiseKind.DIVIDE_BY_ZERO),
            "|#>{s::DB.T}#->map(r|$r.ID)->toOne()", java.util.Optional.of(RaiseKind.SIZE_NOT_MULTIPLICITY),
            "|#>{s::DB.T}#->extend(~r: r|$r.ID->toString()->parseInteger())->select(~r)", java.util.Optional.empty());

    /**
     * EVERY TOKEN A STATEMENT CAN RAISE IS IN ITS SITE TABLE, on every relational database and on MongoDB (the P2
     * checkpoint audit's A-S5, 2026-11-16; phase D's Phase 2, raises as SITES, the same day): for each raising query a
     * database plans, its kind is among the plan's raises, every token {@code LG#<n>} a statement's text carries is a
     * site that statement declares, and no text carries the mark outside a token. (A query a database refuses by name at planning
     * is that database's answer, not this test's.)
     */
    @Test
    void everyTokenAStatementCanRaiseIsInItsSiteTable() {
        java.util.List<String> wrong = new java.util.ArrayList<>();
        java.util.List<java.util.Map.Entry<String, ExecutionPlan>> plans = new java.util.ArrayList<>();
        for (DatabaseType type : com.legend.database.Databases.relational()) {
            for (Map.Entry<String, java.util.Optional<RaiseKind>> q : RAISING.entrySet()) {
                ExecutionPlan plan;
                try {
                    plan = planned(type, q.getKey());
                } catch (com.legend.sql.DialectCapability | com.legend.error.NotImplementedException refused) {
                    continue;
                }
                if (q.getValue().isPresent() && !PlanJson.raises(plan).containsKey(q.getValue().get().name())) {
                    wrong.add(type + " " + q.getKey() + ": " + q.getValue().get() + " undeclared " + PlanJson.raises(plan));
                }
                plans.add(Map.entry(type + " " + q.getKey(), plan));
            }
        }
        plans.add(Map.entry("a document store's division", documentStorePlanned(
                "#>{s::Store.people}#->extend(~r: x|$x.age / $x.zero)->select(~r)")));
        for (Map.Entry<String, ExecutionPlan> e : plans) {
            for (ExecutionPlan.Sql sql : statements(e.getValue().root())) {
                java.util.regex.Matcher m = java.util.regex.Pattern.compile("LG#([0-9]+)").matcher(sql.statement());
                while (m.find()) {
                    int n = Integer.parseInt(m.group(1));
                    if (n < 1 || n > sql.sites().size()) {
                        wrong.add(e.getKey() + ": raises " + m.group() + ", declares " + sql.sites());
                    }
                }
                if (sql.statement().replace(RaiseKind.TOKEN, "").indexOf(RaiseKind.MARK) >= 0) {
                    wrong.add(e.getKey() + ": the mark in its text outside a token: " + sql.statement());
                }
            }
        }
        assertEquals(java.util.List.of(), wrong);
    }

    /** Every statement of {@code node}, where the plan holds it. */
    private static java.util.List<ExecutionPlan.Sql> statements(ExecutionPlan.Node node) {
        java.util.List<ExecutionPlan.Sql> out = new java.util.ArrayList<>();
        switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().forEach(step -> out.addAll(statements(step)));
            case ExecutionPlan.TdsResult t -> out.add(t.sql());
            case ExecutionPlan.Keyed k -> {
                out.add(k.keys());
                out.add(k.body());
            }
            case ExecutionPlan.Update u -> out.add(u.sql());
            case ExecutionPlan.Check c -> out.add(c.sql());
            case ExecutionPlan.Effect f -> out.add(f.sql());
            case ExecutionPlan.Load l -> {
            }
            case ExecutionPlan.Constant c -> {
            }
        }
        return out;
    }

    /** {@code query} over a MongoDB store: planning alone, no server. */
    private static ExecutionPlan documentStorePlanned(String query) {
        String model = """
                ###MongoDB
                Database s::Store
                (
                  Collection people
                  (
                    jsonSchema: {
                      "bsonType": "object",
                      "properties": {
                        "age": { "bsonType": "long" },
                        "zero": { "bsonType": "long" }
                      },
                      "required": ["age", "zero"]
                    };
                  )
                )
                ###Connection
                MongoDBConnection s::Conn
                {
                  database: db;
                  store: s::Store;
                  serverURLs: [localhost:27017];
                  authentication: # UserPassword {
                    username: 'u';
                    password: SystemPropertiesSecret
                    {
                      systemPropertyName: 'p';
                    };
                  }#;
                }
                ###Runtime
                Runtime s::RT
                {
                  mappings: [];
                  connections: [ s::Store: [ c1: s::Conn ] ];
                }
                """;
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query).executionPlan("s::RT");
    }

    @Test
    void aPlanThatRaisesNothingDeclaresNothing() {
        for (DatabaseType type : com.legend.database.Databases.relational()) {
            assertEquals(Map.of(), PlanJson.raises(planned(type, "|#>{s::DB.T}#->select(~[ID])")), type.toString());
        }
    }
}
