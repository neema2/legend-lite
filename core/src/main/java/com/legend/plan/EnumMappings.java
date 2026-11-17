// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.compiler.element.ModelContext;

/**
 * A mapping's ENUMERATION MAPPING for an enum: a model lookup, not upstream's text (Phase 6, 2026-11-17: moved off
 * PlanText, legend-engine's plan text, which goes to the edge; the planner and the test planner read the mapping).
 */
public final class EnumMappings {

    private EnumMappings() {
    }

    /** The mapping's ENUMERATION-MAPPING for an enum FQN — read off the
     * NORMALIZED artifact, whose list is already include-flattened at
     * Phase E (exact match first, simple-name second — parsed mappings
     * may hold either spelling), or null. */
    public static com.legend.model.@com.legend.base.Nullable EnumerationMapping of(
            ModelContext ctx, String mappingFqn, String enumFqn) {
        return of(ctx, mappingFqn, enumFqn, new java.util.HashSet<>());
    }

    /** The mapping's own enumeration mappings first, then its INCLUDED
     * mappings' (real pure: an included mapping's enumeration mappings
     * are visible through the include — the enum-decoded rows leg over
     * a mapping that includes the store mapping). */
    private static com.legend.model.@com.legend.base.Nullable EnumerationMapping of(
            ModelContext ctx, String mappingFqn, String enumFqn,
            java.util.Set<String> seen) {
        if (!seen.add(mappingFqn)) {
            return null;
        }
        var md = ctx.findMapping(mappingFqn).orElse(null);
        if (md == null) {
            return null;
        }
        var ems = md.enumerationMappings();
        String simple = enumFqn.substring(enumFqn.lastIndexOf(':') + 1);
        for (var em : ems) {
            if (em.enumName().equals(enumFqn)) {
                return em;
            }
        }
        for (var em : ems) {
            if (em.enumName().equals(simple)
                    || em.enumName().endsWith("::" + simple)) {
                return em;
            }
        }
        for (var inc : md.includes()) {
            var em = of(ctx, inc.mappingPath(), enumFqn, seen);
            if (em != null) {
                return em;
            }
        }
        return null;
    }
}
