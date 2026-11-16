// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compat;

/**
 * THE COMPATIBILITY FORMS A RUN IS HANDED (phase D's W9/W10, docs/records/d-plan-2026-11-16.md; the shape agreed by
 * D and E-Java, 2026-11-16): what legend-pure and legend-engine spell that the product never computes -- the engine's
 * plan text ({@link UpstreamText}, W9) and the judges' print forms ({@link JudgeForms}, W10: {@code %r} and Repr, the
 * {@code +0000} canon, the TDS null cell, a Float's canon and print). One handle with two narrow facets, so the test
 * road reads one thing.
 *
 * <p>It rides on the planning options, set once per run by the caller of a door -- the compatibility edge
 * ({@code com.legend.edge}, which implements it), or PCT's harness through the edge -- and read where the planner and
 * the test planner need a form. Never a static, never a JVM property. With no edge, {@link #PRODUCT}: each facet
 * answers lite's own forms, and upstream's appear only where the edge hands them in.
 *
 * <p>Records and interfaces only: this package depends on {@code base} and {@code sql} and nothing else, so the planner,
 * the test planner and the edge each read it without reaching one another (tools/deps/core-layers.txt).
 */
public interface CompatForms {

    /** The engine's plan text (W9): legend-engine's generatePlan and SQL-text spellings, the edge's. Its methods arrive
     *  with W9's rows, each a form the product's planner no longer names. */
    UpstreamText upstreamText();

    /** The judges' print forms (W10): legend-pure's {@code %r}, canon and print forms, the edge's. Its methods arrive
     *  with W10's rows. */
    JudgeForms judgeForms();

    /** The engine's plan text, one facet of {@link CompatForms}. */
    interface UpstreamText {
    }

    /** The judges' print forms, one facet of {@link CompatForms}. */
    interface JudgeForms {
    }

    /** Lite's own forms: what a run is handed where no edge is present (core's own tests, the product). */
    CompatForms PRODUCT = new CompatForms() {
        private final UpstreamText engine = new UpstreamText() {
        };
        private final JudgeForms judge = new JudgeForms() {
        };

        @Override
        public UpstreamText upstreamText() {
            return engine;
        }

        @Override
        public JudgeForms judgeForms() {
            return judge;
        }
    };
}
