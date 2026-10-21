// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.PropertyMapping;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * VALIDATION BEFORE SYNTHESIS (T4.1 step 5): the user-model checks Phase
 * E used to raise from inside a class's synthesis — a property mapping
 * naming a property its class does not declare, an M2M binding whose
 * {@code [source, target]} route is not benign — run over every set of
 * the pre-passed mapping before any synthesis, with §6's line held:
 * a STRICT build rejects them (the engine rejects them at compile time),
 * a MODULE build records them as per-set poisons and synthesis skips
 * the set (the binding is withheld; the reason raises at use). The
 * roadmap gaps ({@code NotImplementedException}) are not validation:
 * they stay per-set poisons in both builds.
 */
final class MappingValidation {

    private MappingValidation() {}

    /** The invalid sets of {@code md} BY SET ID, in declaration order, with
     * the engine's compile-time rejection of each — recorded, never thrown
     * here: the driver applies strict/module (B4). The key is the set's id,
     * never the object: later construction steps (the multi-hop injection)
     * rebuild the {@code ClassMapping} records, and an identity key lost the
     * verdict there (audit 2026-09-15 P0-1, proven by probe); declaration
     * order makes "a strict build rejects the first" a stable statement
     * (P0-2). Mapping-level errors (duplicate ids) still throw: they are the
     * mapping's, not a set's. */
    static Map<String, ModelException> run(ResolvedMapping r, ModelBuilder model) {
        LegacyMappingDefinition md = r.raw();
        // MAPPING-level errors the engine's compiler raises (R5): a class
        // mapping id taken by two distinct sets across the include closure,
        // (an include listed twice is rejected where the include graph is
        // first walked, StoreSubstitutionRewrite.resolveAllStores). Thrown in BOTH builds — a module build
        // walls the whole mapping through the pre-pass's own catch, exactly
        // as the engine rejects the mapping.
        MappingClosures.Closure closure = MappingClosures.of(model).closure(md.qualifiedName());
        List<String> dupIds = closure.duplicateIds();
        if (!dupIds.isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.MODEL,
                    "Duplicated class mappings found with ID " + dupIds
                    + " in mapping '" + md.qualifiedName() + "'", md.qualifiedName());
        }
        Map<String, ModelException> invalid = new LinkedHashMap<>();
        for (ClassMapping cm : md.classMappings()) {
            try {
                switch (cm) {
                    case ClassMapping.Relational rcm -> validatePmNames(rcm, model, md);
                    case ClassMapping.Pure pcm -> validateM2mRoutes(pcm, model, r);
                    case ClassMapping.Union u -> validateUnionMembers(u, r);
                    default -> { }
                }
            } catch (ModelException e) {
                invalid.put(ResolvedMapping.idOf(cm), e);
            }
        }
        return invalid;
    }

    /** An Operation union's every member set is a class mapping of the mapping or its includes, as legend-engine's
     * compiler requires (ClassMappingSecondPassBuilder: "Can't find class mapping"). */
    private static void validateUnionMembers(ClassMapping.Union u, ResolvedMapping r) {
        java.util.Set<String> ids = new java.util.HashSet<>(r.includedSets().keySet());
        for (ClassMapping cm : r.raw().classMappings()) {
            ids.add(ResolvedMapping.idOf(cm));
        }
        for (String member : u.memberSetIds()) {
            if (!ids.contains(member)) {
                throw new ModelException(LegendCompileException.Phase.MAPPING, "Can't find class mapping '" + member
                        + "' in mapping '" + r.raw().qualifiedName() + "'", r.raw().qualifiedName());
            }
        }
    }

    /** Every property mapping names a property its class declares or
     * inherits (engine parity: property lookup walks generalizations). */
    private static void validatePmNames(ClassMapping.Relational rcm,
            ModelBuilder model, LegacyMappingDefinition md) {
        ClassDefinition cd = MissProbe.knownMiss(model.knowledge().hierarchyClass(rcm.className()));
        if (cd == null) return;
        for (PropertyMapping pm : rcm.propertyMappings()) {
            if (pm instanceof PropertyMapping.LocalProperty) continue;
            if (model.knowledge().propertyType(cd, pm.propertyName()) == null) {
                throw new ModelException(LegendCompileException.Phase.MODEL,
                        "PropertyMapping '" + pm.propertyName() + "' references property "
                      + "not declared on class '" + rcm.className() + "'; mapping="
                      + md.qualifiedName());
            }
        }
    }

    /** Every routed M2M binding names a benign route. */
    private static void validateM2mRoutes(ClassMapping.Pure pcm,
            ModelBuilder model, ResolvedMapping md) {
        ClassDefinition tgt = MissProbe.knownMiss(model.knowledge().hierarchyClass(pcm.className()));
        for (ClassMapping.Pure.PropertyBinding pb : pcm.propertyBindings()) {
            M2mRouteGuards.requireBenignRoute(pb, pcm, tgt, md, model);
        }
    }
}
