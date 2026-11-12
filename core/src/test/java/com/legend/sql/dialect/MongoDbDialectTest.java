// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DialectCapability;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The MongoDb target's text (docs/MONGODB_PLAN_2026_10_16.md §5.1, §11): a query over a {@code ###MongoDB} store,
 * planned by the one compiler, renders an {@code aggregate} command whose stages are the select's clauses in order;
 * a parameter is a marker the connector replaces; what has no shape yet is refused by name.
 */
class MongoDbDialectTest {

    private static final String STORE = """
            ###MongoDB
            Database test::Store
            (
              Collection people
              (
                jsonSchema: {
                  "bsonType": "object",
                  "properties": {
                    "name": { "bsonType": "string" },
                    "age": { "bsonType": "long" },
                    "city": { "bsonType": "string" }
                  },
                  "required": ["name"]
                };
              )
            )
            """;

    /** The store with a MongoDBConnection to a server no test reaches: the plan is made, never run. */
    private static final String MODEL = STORE + """
            ###Connection
            MongoDBConnection test::Conn
            {
              database: db;
              store: test::Store;
              serverURLs: [localhost:27017];
              authentication: # UserPassword {
                username: 'legend';
                password: SystemPropertiesSecret
                {
                  systemPropertyName: 'legend.mongodb.password';
                };
              }#;
            }
            ###Runtime
            Runtime test::RT
            {
              mappings: [];
              connections: [ test::Store: [ session: test::Conn ] ];
            }
            """;

    /** {@code query}'s typed plan's statement: the pipeline the connector sends. */
    private static String statement(String query) {
        return Compiler.query(Compiler.compileModel(MODEL), query).executionPlan("test::RT").answerSql().statement();
    }

    @Test
    void aFilteredSortedSelectIsAPipelineOverTheCollection() {
        String s = statement("#>{test::Store.people}#->filter(x | $x.age > 30)->select(~[name, age])"
                + "->sort(~name->ascending())->limit(5)");
        assertTrue(s.startsWith("{\"aggregate\": \"people\", \"pipeline\": [{\"$project\": {\"_id\": 0, "), s);
        // the filter: the lowering's null rejection, then the three-valued comparison through its guard (D7); the
        // null test reads the value's type, missing or null (2026-10-20: $ne compares a missing value unequal to null)
        assertTrue(s.contains("{\"$match\": {\"$expr\": {\"$and\": [{\"$not\": [{\"$in\": [{\"$type\": \"$t0.age\"},"
                + " [\"missing\", \"null\"]]}]}, {\"$let\": {\"vars\": {\"g0\": \"$t0.age\", \"g1\": {\"$numberLong\":"
                + " \"30\"}}"), s);
        assertTrue(s.contains("{\"$gt\": [\"$$g0\", \"$$g1\"]}"), s);
        assertTrue(s.contains("{\"$limit\": 5}"), s);
        assertTrue(s.contains("{\"$sort\": {\"__null0\": 1, \"__sort0\": 1}}"), s);
        assertTrue(s.endsWith("], \"cursor\": {}}"), s);
    }

    @Test
    void aParameterIsAPipelineVariableTheCommandsLetBinds() {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(MODEL),
                "{n: Integer[1] | #>{test::Store.people}#->filter(x | $x.age > $n)->select(~[name])}")
                .executionPlan("test::RT");
        ExecutionPlan.Sql sql = plan.answerSql();
        assertEquals(1, sql.slots().size(), sql.statement());
        assertEquals("n", sql.slots().get(0).parameter());
        assertTrue(sql.statement().contains("\"$$lp1\""), sql.statement());
        assertTrue(!sql.statement().contains("$legendParam"), sql.statement());
    }

    /** A plan parameter's variable ({@code lp1}, {@code lp2}, ...) is the command's let's: a variable the pipeline binds
     *  itself is refused by name where it would take one of those names and hide the parameter's value. */
    @Test
    void aVariableThePipelineBindsNeverTakesAParametersName() {
        DialectCapability clash = org.junit.jupiter.api.Assertions.assertThrows(DialectCapability.class,
                () -> MongoJson.letVariable("lp1"));
        assertTrue(clash.getMessage().contains("'lp1'") && clash.getMessage().contains("a plan parameter's"),
                clash.getMessage());
        assertEquals("lpx", MongoJson.letVariable("lpx"));
        assertEquals("v_lp1", MongoJson.variable("lp1"));
        assertEquals("lp2", MongoJson.parameterVariable(2));
    }

    /** A parameter binds only through an aggregate command's let: an insert takes none, refused by name. */
    @Test
    void anInsertTakesNoParameter() {
        com.legend.sql.SqlExpr.PlanParam n = new com.legend.sql.SqlExpr.PlanParam("n",
                com.legend.sql.SqlExpr.PlanParam.Kind.OTHER, false, null,
                new com.legend.sql.TypeFact.Typed(com.legend.sql.SqlType.Scalar.BIGINT, false), null);
        DialectCapability refused = org.junit.jupiter.api.Assertions.assertThrows(DialectCapability.class,
                () -> new MongoDb().render(new com.legend.sql.SqlDml.InsertValues(null, "people",
                        com.legend.sql.NameQuoting.BARE, java.util.List.of("age"), java.util.Set.of(),
                        java.util.List.of(java.util.List.of(n)))));
        assertTrue(refused.getMessage().contains("plan parameter 'n' in an insert command"), refused.getMessage());
    }

    @Test
    void aGroupIsOneGroupStageReadThroughItsMarkers() {
        String s = statement("#>{test::Store.people}#->groupBy(~[city], ~[n: x | $x.name : y | $y->count()])");
        assertTrue(s.contains("{\"$group\": {\"_id\": {\"k0\": \"$"), s);
        assertTrue(s.contains("\"__agg0\": {\"$sum\": "), s);
        assertTrue(s.contains("\"$__agg0\""), s);
    }

    @Test
    void aStorelessExpressionRunsOnTheDatabaseAlone() {
        String s = statement("1 + 2");
        assertTrue(s.startsWith("{\"aggregate\": 1, \"pipeline\": [{\"$documents\": [{}]}"), s);
        assertTrue(s.contains("{\"$add\": [{\"$numberLong\": \"1\"}, {\"$numberLong\": \"2\"}]}"), s);
    }

    @Test
    void whatHasNoShapeIsRefusedByName() {
        // a digest has no operator in a pipeline (the burn-down's cross-lane pass, 2026-10-17: base64, refused here
        // before, is 8.0's conversion through binary data now; hashCode the hashed index key)
        var digest = assertThrows(DialectCapability.class, () -> statement(
                "#>{test::Store.people}#->extend(~h: x | $x.name->hash(meta::pure::functions::hash::HashType.MD5))"));
        assertTrue(digest.getMessage().contains("MD5 reached MongoDb"), digest.getMessage());
    }

    @Test
    void aHashIsTheHashedIndexKey() {
        String s = statement("#>{test::Store.people}#->extend(~h: x | $x.name->hashCode())");
        assertTrue(s.contains("{\"$toHashedIndexKey\": \"$_tds0.name\"}") || s.contains("$toHashedIndexKey"), s);
    }
}
