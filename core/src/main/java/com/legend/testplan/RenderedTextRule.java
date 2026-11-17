// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * HOW THE DATABASE JUDGE PLANS A RENDERED TEXT'S EQUALITY (census C1, the orchestrator's ruling of 2026-11-17: the
 * style decides, never the rules): a two-form kind ({@code PlanStyle.twoForm}, {@link PlanStyleFacets#TWO_FORMS}).
 * The product's form, {@link Product#STRINGS}: the two strings are the verdict (the user, 2026-11-05: after a verdict
 * the harness may only explain); the edge's: the strings first, upstream's ruled line and row comparison (S131, S132)
 * where they differ, one verdict row. Signatures only, and the product's form.
 */
public interface RenderedTextRule {

    /** Lite's form: the two strings are the verdict. */
    enum Product implements RenderedTextRule {
        STRINGS;

        @Override
        public boolean ruledWhereTheyDiffer() {
            return false;
        }
    }

    /** Whether two rendered texts that differ are compared once more by the ruled line and row comparison. */
    boolean ruledWhereTheyDiffer();
}
