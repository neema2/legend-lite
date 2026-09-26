// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.LegacyMappingDefinition;
import com.legend.protocol.TypeExpression;
import java.util.ArrayList;
import java.util.List;

/**
 * Audit-21a route guards for M2M (Pure) property bindings: the parsed
 * mappingLine heads are honored or poisoned by DESIGN — never dropped.
 */
final class M2mRouteGuards {

    /** The M2M binding's STORAGE key: the declared property name, or —
     * for the SYNTHETIC milestoned sweep spelling ({@code
     * <base>AllVersions} on a temporal-class end, which real pure
     * GENERATES) — the BASE property (queries and fetch trees address
     * the declared name; the VALUE keeps its all-versions read). An
     * undeclared key throws (normalize-phase contract).  */
    static String m2mBindingKey(ClassMapping.Pure.PropertyBinding pb,
            @com.legend.base.Nullable ClassDefinition tgt,
            ResolvedMapping md,
            java.util.function.Predicate<String> declared) {
        if (tgt == null || declared.test(pb.propertyName())) {
            return pb.propertyName();
        }
        if (pb.propertyName().endsWith("AllVersions")) {
            String base = pb.propertyName().substring(0,
                    pb.propertyName().length() - "AllVersions".length());
            if (declared.test(base)) {
                return base;
            }
        }
        throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                "M2M PropertyBinding '" + pb.propertyName()
              + "' is not declared on class '" + tgt.qualifiedName()
              + "'; mapping=" + md.qualifiedName());
    }

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
            ClassMapping.Pure pcm, @com.legend.base.Nullable ClassDefinition tgt,
            ResolvedMapping md, ModelBuilder model) {
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
                && model.knowledge().propertyType(tgt, pb.propertyName())
                        instanceof TypeExpression.NameRef nr
                && model.knowledge().hierarchyClass(nr.name()).isPresent()) {
            routedClass = nr.name();
        }
        if (routedClass != null) {
            List<LegacyMappingDefinition> closure = md.closure();
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
                    .findFirst().orElseGet(MissProbe::miss);
            if (routed != null && (sets.size() == 1 || routed.root()
                    || pb.expression() instanceof com.legend.protocol.spec.Variable)) {
                // honored routes: the sole set, the root, or the
                // WHOLE-SOURCE identity binding ($src) — its cast carries
                // targetSetId (route A edit 3) and the graph consumer
                // dispatches by it (wholeSrcChild), so the named set is
                // honored, never root-collapsed
                return;
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
    private static boolean setIdMatches(ClassMapping cm, @com.legend.base.Nullable String id) {
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
        return com.legend.model.SetId.isDefault(id, fqn);
    }

    /** Mapping-LOCAL property field (+prodId: String[1]: expr — engine
     * local-property semantics; the XStore assoc-key idiom): typed by its
     * own value via the isLocal KeyExpression, composed as an extra
     * binding column. A local COLLIDING with a declared property stays
     * the designed poison (audit 21a: the name-keyed binding table
     * cannot hold both, and silent retargeting was the original bug). */
    static com.legend.protocol.spec.KeyExpression localField(
            com.legend.model.ClassMapping.Pure.PropertyBinding pb,
            com.legend.model.@com.legend.base.Nullable ClassDefinition tgt,
            ResolvedMapping md,
            com.legend.compiler.ModelBuilder model, boolean collides) {
        if (tgt != null && collides) {
            throw new com.legend.error.ModelException(
                    com.legend.error.LegendCompileException.Phase.NORMALIZE,
                    "M2M local mapping property '+" + pb.propertyName()
                  + "' collides with a declared property of '"
                  + tgt.qualifiedName() + "' — locals are distinct (engine)"
                  + " and the shared binding table cannot hold both;"
                  + " mapping=" + md.qualifiedName());
        }
        return new com.legend.protocol.spec.KeyExpression(
                pb.expression(), false, true);
    }
}
