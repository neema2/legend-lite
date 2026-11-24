// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.test;

import com.legend.ExecuteOptions;

/**
 * WHO A TEST'S BODY IS RUN FOR (B3 step 2b of the compatibility program, 2026-11-24; the review of step 2's M1: the plan
 * style, the run hook and the door were chosen apart, so a harness could admit a body for one audience and plan it for
 * another). One object gives the three together: the run's options, the hook that hears its appeals, and the door its
 * body (and each setup) passes after typing and before planning. The product's audience, {@link #PRODUCT}, is lite's
 * own: the options as given, no appeal heard, the body as written. A compatibility audience is its harness's (the
 * relational corpus's, {@code rcorpus.MinimalCorpus}), whose options carry upstream's plan style and whose door is the
 * edge's reading of a body ({@code edge.UpstreamTestBodies}, {@code edge.UpstreamPrints}); the product names no edge
 * class. PCT's channel B and the edge's test helper run bodies through the public test door without this runner, and
 * choose the three from one rule-set flag each (the review of B3 step 3's L2, 2026-11-24: recorded, not unified).
 */
public interface Audience {

    /** The options a body runs with: on the package's shared session ({@code shared}), or a private one. */
    ExecuteOptions options(boolean shared);

    /** The run that hears this audience's appeals: the product's hears none. */
    com.legend.testexec.EdgeRun edgeRun();

    /** {@code typed} as this audience reads it, before it is planned: the product's the body itself. */
    com.legend.TypedQuery admitted(com.legend.TypedQuery typed);

    /** {@code typed} (admitted; {@code facts} its facts) with the values this audience reads before the body substituted, read through
     *  {@code reads} on the body's own session once its setups have run: the product's the body itself. The edge's reads
     *  a relation's printed text (B3 step 2b, {@code edge.UpstreamPrints}). */
    com.legend.TypedQuery read(com.legend.TypedQuery typed, com.legend.testexecutionplan.BodyFacts facts, Reads reads);

    /** A query run on the body's session through lite's public test door, its answer written to {@code answer}: what
     *  an audience reads before a body (B3 step 2b, the user's option (B), 2026-11-24). */
    @FunctionalInterface
    interface Reads {
        void read(com.legend.TypedQuery query, com.legend.result.Output answer);
    }

    /** Lite's own audience: the options as given, no appeal heard, the body as written. */
    Audience PRODUCT = new Audience() {
        @Override
        public ExecuteOptions options(boolean shared) {
            return ExecuteOptions.NONE;
        }

        @Override
        public com.legend.testexec.EdgeRun edgeRun() {
            return com.legend.testexec.NoEdgeRun.NONE;
        }

        @Override
        public com.legend.TypedQuery admitted(com.legend.TypedQuery typed) {
            return typed;
        }

        @Override
        public com.legend.TypedQuery read(com.legend.TypedQuery typed, com.legend.testexecutionplan.BodyFacts facts,
                Reads reads) {
            return typed;
        }
    };
}
