// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.model.KeyThread;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Phase E's per-MAPPING ledger: what the synthesis of ONE mapping learns
 * and records on the way &mdash; poisons, mixed-kind unions, union key
 * threads, the nullable census &mdash; stamped onto the compiled
 * {@link MappingDefinition} as {@link MappingDefinition.NormalizationFacts}
 * when the mapping's normalization completes (T4.1 step 2). Before, these
 * were five write channels into the shared model index, copied across the
 * E&rarr;F gate; now the artifact carries them and nothing is copied.
 *
 * <p>Rides the {@link Pipeline} so the deep emission sites reach it
 * without a parameter sweep; the closure-local mapped fact
 * rides along (read-only).
 */
final class MappingLedger {

    /** {@link com.legend.model.PoisonKey} &rarr; the reason the binding is
     * withheld (loud at use). Written ONLY through {@link #poison}: every
     * reason is kept (a second reason for one key appends), never
     * first-wins or last-wins (audit 2026-09-15 P1-1: three policies on
     * one map masked real causes behind the generic multi-set text). */
    final Map<com.legend.model.PoisonKey, String> poisons = new LinkedHashMap<>();

    /** Record {@code reason} against {@code key}; a prior reason is kept
     * and this one appended. */
    void poison(com.legend.model.PoisonKey key, String reason) {
        poisons.merge(key, reason, (a, b) -> a.equals(b) ? a : a + "; " + b);
    }
    /** The per-element errors a STRICT build surfaces (B4: the translator
     * records, the driver alone decides): a USER-model error the engine's
     * compiler rejects (a {@code ModelException}), or an association on
     * roadmap machinery — in element order; a MODULE build keeps them as
     * poisons only. */
    final List<RuntimeException> strictErrors = new java.util.ArrayList<>();
    /** class &rarr; member set ids of a MIXED-KIND Operation union. */
    final Map<String, List<String>> mixedUnions = new LinkedHashMap<>();
    /** operation set id &rarr; the member set ids its function's body
     * concatenates, in member order (the stack's arms as a FACT on the
     * binding — {@code ClassBinding.Operation.memberSetIds}). */
    final Map<String, List<String>> operationMembers = new LinkedHashMap<>();
    /** class &rarr; the primary-key threads of an Operation union's row. */
    final Map<String, List<KeyThread>> unionKeyThreads = new LinkedHashMap<>();
    /** The classes MAPPED for this mapping (engine R1: a set in the queried
     * mapping's closure — own pre-passed record and the included mappings'
     * pre-passed records, implicit operation sets included). Never a
     * graph-wide fact: a class another mapping happens to map is not
     * navigable here. */
    private final Set<String> mappedInClosure;

    MappingLedger(Set<String> mappedInClosure) {
        this.mappedInClosure = mappedInClosure;
    }

    /** Whether {@code classFqn} has a set in this mapping's closure. */
    boolean isMapped(String classFqn) {
        return mappedInClosure.contains(classFqn);
    }

    /** The mapped classes of {@code md}'s pre-passed closure. */
    static Set<String> mappedInClosure(ResolvedMapping md, Map<String, ResolvedMapping> resolved) {
        Set<String> out = new java.util.HashSet<>();
        for (com.legend.model.LegacyMappingDefinition m : UnionSynthesis.prePassedClosure(md, resolved)) {
            for (com.legend.model.ClassMapping cm : m.classMappings()) {
                out.add(cm.className());
            }
        }
        return java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(out));
    }

    /** The compiled mapping's facts: what this synthesis recorded, plus
     * the SURFACE facts Phase F reads off the artifact (T4.1 step 4b). */
    MappingDefinition.NormalizationFacts facts(LegacyMappingDefinition surface) {
        return new MappingDefinition.NormalizationFacts(
                poisons, mixedUnions, unionKeyThreads,
                MappingFacts.unionMembers(surface), MappingFacts.routedTargetClasses(surface));
    }
}
