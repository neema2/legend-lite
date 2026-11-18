// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

/**
 * LEGEND-ENGINE'S PLAN VARIABLES IN A STATEMENT (J28 of the edge audit, 2026-11-17): legend-engine's plan text reads
 * another node's result set by its variable ({@code (${var}) as "alias"}, its VarSetPlaceHolder, spliced by FreeMarker
 * when the plan runs). The edge writes that read as a reference to a relation the statement does not define -- a
 * {@code SqlSource.Cte} with no {@code WITH} of its name -- so the product's MIR carries no plan-text-only variant: a
 * statement the product plans defines every relation it names, and only the edge's plan text reads one it does not.
 * The edge's text renderer and its result-column typing ask this class which names those are.
 */
public final class PlanVariables {

    private PlanVariables() {
    }

    /** The relations {@code query} reads by name and defines nowhere in itself: its plan variables. */
    public static java.util.Set<String> of(com.legend.sql.SqlQuery query) {
        java.util.Set<String> read = new java.util.LinkedHashSet<>();
        java.util.Set<String> defined = new java.util.HashSet<>();
        new com.legend.sql.SqlRewriter() {
            @Override
            protected com.legend.sql.SqlQuery with(com.legend.sql.SqlWith w) {
                w.ctes().forEach(c -> defined.add(c.name()));
                return w;
            }

            @Override
            protected com.legend.sql.SqlSource source(com.legend.sql.SqlSource s) {
                if (s instanceof com.legend.sql.SqlSource.Cte c) {
                    read.add(c.name());
                }
                return s;
            }
        }.rewrite(query);
        read.removeAll(defined);
        return java.util.Collections.unmodifiableSet(read);
    }
}
