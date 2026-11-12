// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.database.Databases;
import com.legend.database.DialectSource;
import com.legend.edge.PureText;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE SPELLING HOOK (phase D's D1.0, the design the orchestrator approved 2026-11-12): a plan's dialect comes from the
 * door's {@link DialectSource} -- the product's own ({@code Databases::dialect}) or the edge's ({@link PureText}).
 * Pinned here: a plan made through one door is never served to the other (the orchestrator's addition 1: a memo of
 * anything a dialect wrote keys on its dialect); the edge's dialects are the product's own, subclassed, overriding ONLY
 * the arms the pin names (addition 2: shrink-only toward the floor compatibility needs -- D1.2 to D1.5 grow it, each
 * arm by the move that brings it, with its date).
 */
class EdgeSpellingHookTest {

    private static final String MODEL = """
            Class s::Item { id: Integer[1]; name: String[0..1]; }
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, NAME VARCHAR(20) ) )
            ###Mapping
            Mapping s::M ( *s::Item: Relational { ~mainTable [s::DB] T id: [s::DB] T.ID, name: [s::DB] T.NAME } )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: DuckDB;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,NAME\\n1,a\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: [ s::M ]; connections: [ s::DB: [ c: s::Conn ] ]; }
            """;

    private static final String QUERY = "s::Item.all()->project([x|$x.id, x|$x.name], ['id', 'name'])->from(s::M, s::RT)";

    /** A dialect source that spells differently from the product's, as the edge's will: a test's own, standing for the
     *  edge's once its arms move (every label marked, so the two texts tell apart). */
    static final class Lower extends com.legend.sql.dialect.DuckDb {
        @Override
        protected String aliasIdent(String name) {
            return "\"" + name + "_edge\"";
        }
    }

    private static final DialectSource LOWER = type -> type == DatabaseType.DuckDB ? new Lower()
            : Databases.dialect(type);

    private static String sql(ExecutionPlan plan) {
        return ((ExecutionPlan.TdsResult) plan.root()).sql().statement();
    }

    @Test
    void aPlanMadeThroughOneDoorIsNeverServedToTheOther() {
        var typed = Compiler.query(Compiler.compileModel(MODEL), QUERY);
        String product = PlanJson.write(typed.executionPlan("s::RT"));
        String spelled = PlanJson.write(typed.executionPlan("s::RT", Set.of(), LOWER));
        String productAgain = PlanJson.write(typed.executionPlan("s::RT"));
        assertNotEquals(product, spelled, "the spelling source's plan is its own");
        assertEquals(product, productAgain, "the product door's plan, after the other door planned the same query");
        assertTrue(sql(typed.executionPlan("s::RT", Set.of(), LOWER)).contains("_edge"), spelled);
        assertTrue(!sql(typed.executionPlan("s::RT")).contains("_edge"), product);
        // the project's database (memoized on the compiled model, by its dialect): the same, both ways round
        String projectSpelled = PlanJson.write(typed.projectPlan("s::RT", LOWER));
        String project = PlanJson.write(typed.projectPlan("s::RT"));
        assertNotEquals(project, projectSpelled);
        assertEquals(projectSpelled, PlanJson.write(typed.projectPlan("s::RT", LOWER)));
        assertEquals(project, PlanJson.write(typed.projectPlan("s::RT")));
    }

    @Test
    void theEdgesDialectsAreTheProductsSubclassed() {
        for (DatabaseType type : Databases.implemented()) {
            var product = Databases.dialect(type);
            var edge = PureText.dialect(type);
            assertEquals(product.getClass(), edge.getClass().getSuperclass(), type.name());
            assertEquals(product.getClass().getSimpleName(), edge.getClass().getSimpleName(), type.name());
        }
    }

    /** The arms each edge dialect overrides, by class. Each D step adds the arms it moves, dated; an arm that spells no
     *  upstream text is a defect. {@code raiseMessage} (2026-11-12, D1.1): legend-pure's sentence of a raise
     *  (edge.PureMessages), where the product writes the raise's kind. */
    private static final Set<String> D1_1 = Set.of("raiseMessage");

    private static final Map<String, Set<String>> OVERRIDES = new TreeMap<>(Map.ofEntries(
            Map.entry("BigQuery", D1_1), Map.entry("ClickHouse", D1_1), Map.entry("Databricks", D1_1),
            Map.entry("DuckDb", D1_1), Map.entry("H2", D1_1), Map.entry("MariaDb", D1_1),
            Map.entry("MongoDb", D1_1), Map.entry("MySql", D1_1), Map.entry("Postgres", D1_1),
            Map.entry("SQLite", D1_1), Map.entry("Snowflake", D1_1), Map.entry("Trino", D1_1)));

    @Test
    void theEdgesDialectsOverrideOnlyThePinnedArms() {
        Map<String, Set<String>> found = new TreeMap<>();
        for (Class<?> c : PureText.class.getDeclaredClasses()) {
            Set<String> arms = new TreeSet<>();
            Arrays.stream(c.getDeclaredMethods()).filter(m -> !m.isSynthetic()).forEach(m -> arms.add(m.getName()));
            found.put(c.getSimpleName(), arms);
        }
        assertEquals(OVERRIDES, found, "the edge's overridden arms (PureText): an arm moves here only with its D step");
        assertEquals(List.of(), Arrays.stream(PureText.class.getDeclaredClasses())
                .filter(c -> c.getDeclaredFields().length > 0).map(Class::getSimpleName).toList(),
                "an edge dialect holds no state of its own");
    }
}
