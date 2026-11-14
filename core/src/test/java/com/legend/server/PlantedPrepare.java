// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.TypedQuery;
import com.legend.planner.ProductStatements;

/**
 * A class PLANTED in the server's package to prepare a product statement past the planners, for one test alone (the C4
 * audit's M5, 2026-11-13): {@code ArchitectureTest.PREPARE_IS_THE_PLANNERS} fails on it, by a call and by a
 * reference. A test class: the rule's real check reads the product's classes, never this one.
 */
public final class PlantedPrepare {

    private PlantedPrepare() {
    }

    /** A statement prepared outside the planners: the defect the rule refuses. */
    public static ProductStatements.Statement prepared(TypedQuery query) {
        return ProductStatements.prepare(query, query.body(), null, null, java.util.Set.of());
    }

    /** The same, by the method's reference, never called. */
    public static final class ByReference {
        private ByReference() {
        }

        /** {@code prepare}'s five-argument form as a function. */
        public interface Preparing {
            ProductStatements.Statement prepare(TypedQuery query, java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
                    String runtime, Integer mark, java.util.Set<com.legend.platform.Feature> features);
        }

        public static Preparing preparing() {
            return ProductStatements::prepare;
        }
    }
}
