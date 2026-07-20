// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.TypeExpression;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Audit-21a route guards for M2M (Pure) property bindings: the parsed
 * mappingLine heads are honored or poisoned by DESIGN — never dropped.
 */
final class M2mRouteGuards {

    private M2mRouteGuards() {
    }

    /**
     * Audit 21a HIGH: a Pure binding's {@code [targetSetId]} route selects a
     * SPECIFIC set of the routed class; emitting an unrouted
     * {@code NewInstanceCast} would root-route it — the audit-11 wrong-rows
     * shape (relational side: Join.targetSetId, classified per-PM) mirrored
     * on the M2M side. The route is benign only when root-routing is
     * IDENTICAL to honoring it: the routed set is the routed class's sole
     * set, or its root set. Anything else — including a set id that matches
     * nothing, or a route on a binding whose type is not a mapped class —
     * is a loud wall, never a silent drop. The {@code [src, tgt]} source id
     * must name this very set; a mismatch is a model shape we don't route.
     */
    static void requireBenignRoute(ClassMapping.Pure.PropertyBinding pb,
            ClassMapping.Pure pcm, ClassDefinition tgt,
            LegacyMappingDefinition md, ModelBuilder model) {
        if (pb.sourceSetId() == null && pb.targetSetId() == null) {
            return;
        }
        if (pb.sourceSetId() != null && !setIdMatches(pcm, pb.sourceSetId())) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "M2M binding '" + pb.propertyName() + "[" + pb.sourceSetId()
                  + ", " + pb.targetSetId() + "]' names source set '"
                  + pb.sourceSetId() + "' but is declared inside set '"
                  + pcm.setId() + "'; mapping=" + md.qualifiedName());
        }
        String routedClass = null;
        if (tgt != null
                && MappingNormalizer.findPropertyTypeDeep(tgt, pb.propertyName(), model)
                        instanceof TypeExpression.NameRef nr
                && model.findClass(nr.name()).isPresent()) {
            routedClass = nr.name();
        }
        if (routedClass != null) {
            List<LegacyMappingDefinition> closure = new ArrayList<>();
            MappingNormalizer.collectMappingClosure(md, model, closure, new HashSet<>());
            List<ClassMapping> sets = new ArrayList<>();
            for (LegacyMappingDefinition m : closure) {
                for (ClassMapping cm : m.classMappings()) {
                    if (cm.className().equals(routedClass)) {
                        sets.add(cm);
                    }
                }
            }
            final String tgtSet = pb.targetSetId();
            ClassMapping routed = sets.stream()
                    .filter(cm -> setIdMatches(cm, tgtSet))
                    .findFirst().orElse(null);
            if (routed != null && (sets.size() == 1 || routed.root())) {
                return; // root-routing == honoring the route
            }
        }
        throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                "M2M set-routed binding '" + pb.propertyName() + "["
              + pb.targetSetId() + "]' targets a non-root set"
              + (routedClass == null ? "" : " of '" + routedClass + "'")
              + "; per-set routing on the M2M side is a roadmap feature"
              + " (root-routing would produce wrong rows); mapping="
              + md.qualifiedName());
    }

    /**
     * Whether {@code cm} answers to set id {@code id}. An unlabeled mapping's
     * engine-default id is the class FQN with '_' for '::' (short name
     * accepted too — includes-era corpora spell it either way).
     */
    private static boolean setIdMatches(ClassMapping cm, String id) {
        if (id == null) {
            return false;
        }
        if (cm.setId() != null) {
            return cm.setId().equals(id);
        }
        String fqn = cm.className();
        // audit 23: EXACT engine-default id only (FQN with '_' for '::');
        // the short-class-name acceptance let two same-named classes in
        // different packages match each other's routes
        return id.equals(fqn.replace("::", "_"));
    }
}
