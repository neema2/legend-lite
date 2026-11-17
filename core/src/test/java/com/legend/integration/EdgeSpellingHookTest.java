// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.database.Databases;
import com.legend.database.PlanStyle;
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
 * door's {@link PlanStyle} -- the product's own ({@code Databases::dialect}) or the edge's ({@link PureText}).
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

    private static final PlanStyle LOWER = new PlanStyle() {
        @Override
        public com.legend.sql.dialect.SqlDialect dialect(DatabaseType type) {
            return type == DatabaseType.DuckDB ? new Lower() : Databases.dialect(type);
        }

        @Override
        public @com.legend.base.Nullable TraceCell traceCell() {
            return PlanStyle.PRODUCT.traceCell();
        }

        @Override
        public <F> F twoForm(Class<F> kind, F productForm) {
            return PlanStyle.PRODUCT.twoForm(kind, productForm);
        }

        @Override
        public String activitySql(Activity activity) {
            return PlanStyle.PRODUCT.activitySql(activity);
        }

        @Override
        public String graphKeyDate(String written) {
            return PlanStyle.PRODUCT.graphKeyDate(written);
        }

        @Override
        public String graphKeyString(String value) {
            return PlanStyle.PRODUCT.graphKeyString(value);
        }

        @Override
        public String errorWords(RuntimeException e) {
            return e.getMessage();
        }

        @Override
        public @com.legend.base.Nullable String siteWords(com.legend.sql.dialect.RaiseSite site) {
            return null;
        }

        @Override
        public boolean judgesEngineText() {
            return PlanStyle.PRODUCT.judgesEngineText();
        }

        @Override
        public <F> F facet(Class<F> kind) {
            return PlanStyle.PRODUCT.facet(kind);
        }

        @Override
        public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
            return PlanStyle.PRODUCT.facet(kind, refusal);
        }
    };

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

    /** S309 (RULED by the user 2026-11-14, DE1 (a)): the product's style writes none of legend-engine's text -- its
     *  upstream plan facet refused by name; the edge's answers it (O3). Since Phase 6's A (2026-11-17) the style names
     *  no engine-text dialect at all: the printers are the edge's ({@code PureText}), reached through the facet. */
    @Test
    void theProductRefusesEngineTextByNameAndTheEdgeAnswersIt() {
        var refused = org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.NotImplementedException.class,
                () -> PlanStyle.PRODUCT.facet(com.legend.planner.UpstreamPlans.class));
        assertTrue(refused.getMessage().contains("UpstreamPlans") && refused.getMessage().contains("compatibility edge's"),
                refused.getMessage());
        org.junit.jupiter.api.Assertions.assertNotNull(PureText.UPSTREAM.facet(com.legend.planner.UpstreamPlans.class));
        org.junit.jupiter.api.Assertions.assertNotNull(PureText.engineText(DatabaseType.H2));
        org.junit.jupiter.api.Assertions.assertNotNull(PureText.enginePlanText(DatabaseType.H2, true, null));
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

    /** D1.1's arm, and D1.2's (2026-11-15): {@code spellingPasses} and {@code floatToText}, legend-pure's print form of
     *  a Float ({@code FLOAT_SHORTEST_TEXT}, edge.PurePrintForms) where the product writes its six decimals. */
    private static final Set<String> D1_2 = Set.of("raiseMessage", "spellingPasses", "floatToText");

    /** D1.2's arms and MongoDB's {@code dateTimeFormat} (2026-11-15): legend-pure's {@code +0000} text of a date-time
     *  in its {@code $dateToString}s, where the product writes ISO with {@code Z}. */
    private static final Set<String> D1_2_MONGO = Set.of("raiseMessage", "spellingPasses", "floatToText",
            "dateTimeFormat");

    /** D1.2's arms and DuckDB's {@code scanOrderPasses} (W6 of docs/records/d-plan-2026-11-16.md, 2026-11-16):
     *  legend-engine's scan order ({@code edge.StableScanOrder}) for the corpus replay, where the product adds none. */
    private static final Set<String> D1_2_SCAN_ORDER = Set.of("raiseMessage", "spellingPasses", "floatToText",
            "scanOrderPasses");

    /** THE EDGE'S IN-QUERY VARIANTS: the decisions an edge dialect takes because legend-engine computes inside the
     *  query differently (the edge program's §2.3, kind B), each a listed row of its §6. {@code floatLiteralKind}
     *  (row 21, the orchestrator's ruling R2 of 2026-11-17, Phase 5b): a Float literal computes as legend-engine's bare
     *  literal, a DECIMAL, where the product's is the double (register S20); on the databases whose product writes the
     *  double and legend-engine the bare decimal; the user, 2026-11-17, approved 5b's landing ('Okay'). */
    static final Set<String> ROW_21 = Set.of("floatLiteralKind");

    /** Row 22 (the orchestrator's ruling (c) of 2026-11-17, under the user's 5b approval and the standing
     *  no-silent-approximation rule): SQLite's Decimal arithmetic -- the product refuses it, holding no exact decimal;
     *  the edge writes it as REAL arithmetic, as legend-engine would there. */
    static final Set<String> ROW_22 = Set.of("refusesDecimalArithmetic");

    private static Set<String> withRow22(Set<String> arms) {
        Set<String> out = new TreeSet<>(arms);
        out.addAll(ROW_22);
        return out;
    }

    private static Set<String> withRow21(Set<String> arms) {
        Set<String> out = new TreeSet<>(arms);
        out.addAll(ROW_21);
        return out;
    }

    /** DuckDB's and Postgres's raw arm too (Phase 6's B4 (ii), 2026-11-17): legend-engine's H2 text translated, where the
     *  product runs a raw statement as written. */
    private static final Set<String> D1_2_SCAN_ORDER_RAW = Set.of("raiseMessage", "spellingPasses", "floatToText",
            "scanOrderPasses", "raw");
    private static final Set<String> D1_2_RAW = Set.of("raiseMessage", "spellingPasses", "floatToText", "raw");

    private static final Map<String, Set<String>> OVERRIDES = new TreeMap<>(Map.ofEntries(
            Map.entry("BigQuery", D1_2), Map.entry("ClickHouse", D1_2), Map.entry("Databricks", withRow21(D1_2)),
            Map.entry("DuckDb", withRow21(D1_2_SCAN_ORDER_RAW)), Map.entry("H2", withRow21(D1_2)), Map.entry("MariaDb", D1_2),
            Map.entry("MongoDb", D1_2_MONGO), Map.entry("MySql", D1_2), Map.entry("Postgres", withRow21(D1_2_RAW)),
            Map.entry("SQLite", withRow22(D1_2)), Map.entry("Snowflake", D1_2), Map.entry("Trino", withRow21(D1_2))));

    /** The methods {@code c} declares that override a product dialect's method which is not a
     *  {@link com.legend.sql.dialect.SpellingArm}: a decision the edge must not take ({@code EdgeMayOverride}, used
     *  nowhere, went 2026-11-15: the D part 1 audit's nit). */
    static List<String> decisionsOverridden(Class<?> c) {
        List<String> out = new java.util.ArrayList<>();
        for (java.lang.reflect.Method m : c.getDeclaredMethods()) {
            if (m.isSynthetic() || java.lang.reflect.Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            java.lang.reflect.Method overridden = null;
            for (Class<?> up = c.getSuperclass(); up != null && overridden == null; up = up.getSuperclass()) {
                try {
                    overridden = up.getDeclaredMethod(m.getName(), m.getParameterTypes());
                } catch (NoSuchMethodException next) {
                    // keep climbing
                }
            }
            if (overridden == null || !overridden.isAnnotationPresent(com.legend.sql.dialect.SpellingArm.class)) {
                out.add(c.getSimpleName() + "." + m.getName());
            }
        }
        return out;
    }

    /** The edge overrides SPELLING, never a DECISION (the orchestrator's guard, 2026-11-12): each method an edge dialect
     *  declares overrides a product method marked {@code @SpellingArm}. */
    @Test
    void theEdgesDialectsOverrideSpellingOnly() {
        List<String> decisions = new java.util.ArrayList<>();
        for (Class<?> c : PureText.class.getDeclaredClasses()) {
            // a listed in-query variant (row 21) is the edge's by ruling, not a mode
            decisionsOverridden(c).stream().filter(d -> !ROW_21.contains(d.substring(d.indexOf('.') + 1))
                            && !ROW_22.contains(d.substring(d.indexOf('.') + 1)))
                    .forEach(decisions::add);
        }
        assertEquals(List.of(), decisions, "an edge dialect overriding a decision: planning would be a mode");
    }

    /** The rule bites an edge class overriding a decision ({@code edge.PlantedEdgeDecision}, a test class planted for
     *  this alone: the raise read-back). */
    @Test
    void theSpellingOnlyRuleBitesAPlantedDecision() {
        assertEquals(List.of("PlantedEdgeDecision.raiseReadBack"),
                decisionsOverridden(com.legend.edge.PlantedEdgeDecision.class));
    }

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
