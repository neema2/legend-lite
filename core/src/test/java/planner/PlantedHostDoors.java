// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planner;

/**
 * Classes PLANTED in a host's package (the boundary's, {@code planner}) for tests alone (the GUARD stream's C.5,
 * 2026-11-17): each a defect one of the five rules widened to the hosts' doors refuses
 * ({@code ArchitectureTest.theWidenedRulesBiteAPlantedHostDoor}). Test classes: the rules' real checks read the
 * product's and the hosts' jars, never these.
 */
public final class PlantedHostDoors {

    private PlantedHostDoors() {
    }

    /** A host writing SQL as a dialect of its own. */
    public abstract static class Dialect extends com.legend.sql.dialect.DuckDb {
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

    /** A host implementing a plan style. */
    public abstract static class Style implements com.legend.database.PlanStyle {
    }
}
