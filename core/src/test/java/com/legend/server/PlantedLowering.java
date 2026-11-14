// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.lowering.Lowerer;
import com.legend.lowering.PlatformRegistrations;
import com.legend.lowering.SeedableLets;
import com.legend.planner.BodyPlanner;
import com.legend.planner.SidePlanner;

/**
 * Classes PLANTED in the server's package to reach a lowering past the planners, for one test alone (the follow-up
 * audit's G1, 2026-11-14): {@code ArchitectureTest.LOWERING_IS_THE_PLANNERS} fails on each -- a Lowerer constructed by
 * a call and by a reference, and each public lowering static called or referenced. Test classes: the rule's real
 * checks read the product's classes and the hosts' doors, never these.
 */
public final class PlantedLowering {

    private PlantedLowering() {
    }

    /** A Lowerer constructed outside the planners. */
    public static final class Constructs {
        private Constructs() {
        }

        public static Lowerer lowerer() {
            return new Lowerer(PlatformRegistrations.catalogTable());
        }
    }

    /** The same, by the constructor's reference. */
    public static final class ConstructsByReference {
        private ConstructsByReference() {
        }

        public static java.util.function.Function<com.legend.platform.ImplementationTable, Lowerer> lowerer() {
            return Lowerer::new;
        }
    }

    /** The test-data generator's view fetch, called outside the planners. */
    public static final class FetchesAView {
        private FetchesAView() {
        }

        public static Object fetched() {
            return BodyPlanner.viewFetch("db", "view", java.util.Set.of(), java.util.Map.of(), null, null);
        }
    }

    /** A side's staging, by its reference. */
    public static final class StagesASide {
        private StagesASide() {
        }

        /** {@code staged}'s form as a function. */
        public interface Staging {
            SidePlanner.Staged staged(java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
                    com.legend.planner.PlanEnv env, com.legend.lowering.CanonRider rider, boolean identityLane,
                    boolean hostSide);
        }

        public static Staging staging() {
            return SidePlanner::staged;
        }
    }

    /** The seedable lets' prefix, called outside the planners. */
    public static final class SeedsLets {
        private SeedsLets() {
        }

        public static Object prefixed() {
            return SeedableLets.withSeedableLetPrefix(java.util.List.of(), java.util.Map.of(), null);
        }
    }
}
