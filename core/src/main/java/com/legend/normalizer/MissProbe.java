// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

/**
 * F7.8 VERDICT funnel. The full-sweep census over every
 * {@code findClass(...).orElse(null)} default in this package split the
 * 33 sites in two:
 *
 * <ul>
 *   <li>23 sites NEVER fired — they now {@code orElseThrow} at the call
 *       site (an unresolvable class was the audit's feared silent
 *       semantic default; it is loud there now).</li>
 *   <li>the sites that fire LEGITIMATELY funnel through
 *       {@link #knownMiss}: the metamodel probes (the miss IS the answer
 *       — engine metamodel/protocol class names are not user classes)
 *       and owner classes a synthesis asks about before it knows they
 *       exist. (An earlier version of this note named a bare-superclass
 *       name-resolution gap; {@code NameResolver.resolveClass} resolves
 *       superclasses through the import scope, and
 *       {@code KnowledgeLayerTest} pins it — T4.1 step 3a.)</li>
 * </ul>
 */
final class MissProbe {

    private MissProbe() {
    }

    /** A censused, legitimate empty-answer site (see class doc). */
    static <T> @com.legend.base.Nullable T knownMiss(java.util.Optional<T> o) {
        return o.orElse(null);
    }

    /** B5 (docs/NORMALIZER_CLEAN_SHEET_HOMEWORK_2026_09_13.md): the 37 bare
     * {@code orElse(null)} sites of this package were censused on
     * 2026-09-15 over the DuckDB corpus lane and the core tests. The 21
     * that FIRED are "the miss is the answer" sites and read through this
     * funnel ({@code orElseGet(MissProbe::miss)}): AssociationSynthesis#2,
     * DeclaredCoercions#1–2, ImplicitInheritance#2, M2mRouteGuards#1,
     * MappingNormalizer#1–6, RelOpTranslator#1, RequiredNullableCensus#1–2,
     * StoreSubstitutionRewrite#1, UnionSynthesis#1–3, ViewRelation#1–3
     * (numbered in file order at census time). The 16 that never fired
     * are loud ({@link #neverFired}). */
    static <T> @com.legend.base.Nullable T miss() {
        return null;
    }

    /** A censused empty-answer site that NEVER fired: an empty answer here
     * is a real model gap, never a default to ride on. */
    static IllegalStateException neverFired(String site) {
        return new IllegalStateException("F7.8: empty answer at " + site
                + " (this default never fired on the 2026-09-15 B5 census; a miss here is a"
                + " real model gap)");
    }
}
