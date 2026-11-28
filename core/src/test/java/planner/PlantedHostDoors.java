// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planner;

/**
 * Classes PLANTED in a host's package (the boundary's, {@code planner}) for tests alone (the GUARD stream's C.5,
 * 2026-11-17): each a defect one rule of {@code ArchitectureTest.hostDoorRules} refuses
 * ({@code ArchitectureTest.theWidenedRulesBiteAPlantedHostDoor}; the edge reach's is {@code PlantedHostEdgeReach}, in
 * the edge's test library). Test classes: the rules' real checks read the product's and the hosts' jars, never these.
 */
public final class PlantedHostDoors {

    private PlantedHostDoors() {
    }

    /** A host writing SQL as a dialect of its own (the interface, so no database is named). */
    public abstract static class Dialect implements com.legend.sql.dialect.SqlDialect {
    }

    /** A host picking a plan style through a door that takes one. */
    public static final class Spells {

        private Spells() {
        }

        public static com.legend.executionplan.ExecutionPlan plan(com.legend.TypedQuery typed) {
            return typed.executionPlan(null, java.util.Set.of(), com.legend.database.PlanStyle.PRODUCT);
        }
    }

    /** A host discovering a type by name. */
    public static final class Reflects {

        private Reflects() {
        }

        public static Class<?> find(String name) throws ClassNotFoundException {
            return Class.forName(name);
        }
    }

    /** A host implementing a style's facet. */
    public abstract static class Facet implements com.legend.planner.UpstreamPlans {
    }

    /** A host implementing the test planner's engine-text facet. */
    public abstract static class TestText implements com.legend.testplan.EngineTestText {
    }

    // (RelationText: gone 2026-11-25 with the lowering's relation-text facet, stage B's B2b S5c; a shrink)

    /** A host implementing the lowering's Pure-only-text facet. */
    public abstract static class PureOnlyText implements com.legend.lowering.PureOnlyTexts {
    }

    /** A host implementing the lowering's result-envelope facet. */
    public abstract static class ResultEnvelope implements com.legend.lowering.ResultEnvelopes {
    }

    /** A host implementing a plan style. */
    public abstract static class Style implements com.legend.database.PlanStyle {
    }
}
