// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.ClassMapping;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.model.PropertyMapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Phase E's PRE-PASS over every legacy mapping, run to completion for ALL
 * mappings before any synthesis (T4.1 step 2): the JSON-connection
 * identity sets (formerly the index's runtime cross-bake), the M2M cycle
 * check, the sets' own declared-key text, {@code extends} flattening,
 * implicit same-extent inheritance, import-scope store-ref qualification
 * and the implicit Operation sets for routed targets &mdash; ONE construction
 * per mapping (B3.3): every step rewrites the {@link ResolvedMapping} under
 * construction and asks its closure questions of that record. Whether a
 * class is mapped FOR a mapping is the ledger's closure-local answer
 * ({@link MappingLedger#isMapped}); no graph-wide fact exists, so a
 * mapping's synthesis never depends on which mappings normalized before it.
 */
final class MappingPrePass {

    private MappingPrePass() {}

    /** Pre-pass every legacy mapping of {@code parsed}, element order, then
     * build every mapping's RESOLVED record over the graph-wide facts. A
     * mapping whose pre-pass fails is walled under a tolerant build (and
     * absent from the result) or thrown under a strict one &mdash; exactly
     * the driver's own per-mapping discipline. */
    static Map<String, ResolvedMapping> run(ParsedModel parsed, ModelBuilder model,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink, LiftedViews views) {
        Map<String, ResolvedMapping> pre = new LinkedHashMap<>();
        for (PackageableElement el : parsed.elements()) {
            if (!(el instanceof LegacyMappingDefinition md)) {
                continue;
            }
            try {
                pre.put(md.qualifiedName(), MappingNormalizer.withElement(
                        md.qualifiedName(), () -> {
                            ResolvedMapping r = prePass(md, model, views);
                            // VALIDATION before synthesis (step 5): the
                            // translator records every invalid set; THE
                            // DRIVER'S policy (B4) — a strict build rejects
                            // the first IN DECLARATION ORDER, a module build
                            // poisons them (by set id: the record survives
                            // the construction steps that rebuild the sets)
                            Map<String, ModelException> invalid =
                                    MappingValidation.run(r, model);
                            if (wallSink == null && !invalid.isEmpty()) {
                                throw invalid.values().iterator().next();
                            }
                            Map<String, String> reasons = new LinkedHashMap<>();
                            invalid.forEach((setId, e) -> reasons.put(setId, String.valueOf(e.getMessage())));
                            return r.withInvalid(reasons);
                        }));
            } catch (ModelException e) {
                if (wallSink == null || e.element() == null) {
                    throw e;
                }
                wallSink.putIfAbsent(e.element(),
                        String.valueOf(e.getMessage()).split("\n")[0]);
            }
        }
        return pre;
    }

    private static ResolvedMapping prePass(LegacyMappingDefinition authored, ModelBuilder model,
            LiftedViews views) {
        LegacyMappingDefinition surface = MappingClosures.of(model).surface(authored);
        detectM2MCycles(surface);
        // the sets' OWN key text, captured BEFORE the extends pre-pass
        // merges the parent's in (metamodel facts, ClassBinding.declared)
        Map<String, MappingDefinition.ClassBinding.DeclaredKeys> declaredKeys = new HashMap<>();
        for (ClassMapping cm0 : surface.classMappings()) {
            if (cm0 instanceof ClassMapping.Relational r0) {
                declaredKeys.put(SetKeyFacts.setKey(r0), SetKeyFacts.declaredKeysOf(r0));
            }
        }
        // Pre-pass: flatten `extends [parentSetId]` by merging inherited
        // property mappings into each child mapping (child overrides on
        // property-name conflict; multi-level resolves recursively). See
        // docs/MAPPING_LEGACY_TO_FUNCTION.md §5.2.3.
        // ONE construction (B3.3): every step rewrites the record under
        // construction and asks its closure questions of that record
        ResolvedMapping r = new ResolvedMapping(surface, surface, declaredKeys, Map.of(),
                MappingClosures.of(model).closure(surface.qualifiedName()), views);
        r = r.withMapping(resolveExtends(r, model));
        r = r.withMapping(ImplicitInheritance.apply(r, model));
        // Pre-pass: IMPORT-SCOPE store-ref qualification (see
        // StoreSubstitutionRewrite.qualifyStoreRefs).
        r = r.withMapping(StoreSubstitutionRewrite.qualifyStoreRefs(r.raw(), model));
        // Pre-pass: implicit inheritance OPS for unmapped routed targets
        // (association ends, routed class-typed properties) — must precede
        // the multi-hop injection (op visibility).
        return r.withMapping(ImplicitInheritance.implicitOpsForRoutedTargets(r, model));
    }


    // ====================================================================
    // Pre-pass: extends flattening
    // ====================================================================

    /**
     * Flatten {@code extends [parentSetId]} by merging inherited property
     * mappings into each Relational child (child overrides on property
     * identity; multi-level resolves recursively). A Pure (M2M) child
     * must declare its own (the function form requires explicitness).
     */
    private static LegacyMappingDefinition resolveExtends(ResolvedMapping r,
                                                          ModelBuilder model) {
        LegacyMappingDefinition md = r.raw();
        boolean any = md.classMappings().stream().anyMatch(cm -> cm.extendsSetId() != null);
        if (!any) return md;
        // set-ids resolve within this mapping AND its includes (transitive,
        // own definitions win) — extends [set] across an include is the
        // union::extend corpus family's normal shape
        Map<String, ClassMapping> bySetId = new HashMap<>(r.visibleSets());
        List<ClassMapping> rewritten = new ArrayList<>(md.classMappings().size());
        for (ClassMapping cm : md.classMappings()) {
            if (cm.extendsSetId() == null) {
                rewritten.add(cm);
            } else if (cm instanceof ClassMapping.Relational rcm) {
                rewritten.add(flattenExtends(rcm, bySetId, new LinkedHashSet<>(), md));
            } else {
                // Pure (M2M) extends is not covered by §5.2.3; reject loudly
                // rather than silently ignore the inheritance (AGENTS.md: no
                // fallbacks).
                throw new NotImplementedException(
                        "Class mapping for '" + cm.className() + "' uses extends ["
                      + cm.extendsSetId() + "] on a non-Relational (Pure) mapping; "
                      + "only Relational extends is supported. Mapping="
                      + md.qualifiedName());
            }
        }
        return md.withClassMappings(rewritten);
    }

    /**
     * Recursively flatten one Relational child's {@code extends} chain into a
     * single binding carrying the merged property mappings (child overrides
     * parent on property-name conflict).
     */
    private static ClassMapping.Relational flattenExtends(ClassMapping.Relational child,
                                                         Map<String, ClassMapping> bySetId,
                                                         Set<String> chain,
                                                         LegacyMappingDefinition md) {
        String parentSetId = child.extendsSetId();
        if (!chain.add(parentSetId)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "Circular 'extends' chain in mapping '" + md.qualifiedName()
                  + "': " + String.join(" -> ", chain) + " -> " + parentSetId);
        }
        ClassMapping parent = bySetId.get(parentSetId);
        if (parent == null) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "Class mapping for '" + child.className() + "' extends ["
                  + parentSetId + "] but no class mapping with set id '" + parentSetId
                  + "' exists in mapping=" + md.qualifiedName());
        }
        if (!(parent instanceof ClassMapping.Relational parentRcm)) {
            throw new NotImplementedException(
                    "Class mapping for '" + child.className() + "' extends ["
                  + parentSetId + "] which is not a Relational mapping; only "
                  + "Relational extends is supported. Mapping=" + md.qualifiedName());
        }
        ClassMapping.Relational flatParent = parentRcm.extendsSetId() != null
                ? flattenExtends(parentRcm, bySetId, chain, md)
                : parentRcm;
        // Parent PMs first (declaration order), child overrides by property
        // IDENTITY = (name, targetSetId): a routed property's per-set
        // duplicates (employees[set1], employees[set2]) are DISTINCT
        // mappings — merging by name alone silently dropped all but the
        // last route (audit 11: the extends-of-union-Firm corpus family
        // then navigated one member only).
        LinkedHashMap<String, PropertyMapping> merged = new LinkedHashMap<>();
        for (PropertyMapping pm : flatParent.propertyMappings()) {
            merged.put(UnionSynthesis.pmIdentity(pm), pm);
        }
        for (PropertyMapping pm : child.propertyMappings()) {
            merged.put(UnionSynthesis.pmIdentity(pm), pm);
        }
        // prop[setId] routes do NOT inherit: the parent's set ids name the
        // PARENT mapping's sets — a child that re-unions its own members
        // (Person[mySet1] extends [set1]) can't resolve them, and the
        // name-keyed merge already collapsed multi-route PMs to one, whose
        // parent route would mis-read as a PARTIAL union route (wrong rows).
        // The child's own routes are authoritative; inherited multi-route
        // properties keep the merged single PM (equivalent-join shape).
        // Table-level attributes INHERIT-IF-ABSENT, child REPLACES (never
        // ANDs) — real legend-pure resolveFilter/resolveGroupBy/
        // resolveDistinct (platform_store_relational functions.pure:143-167)
        // and the pk priority ladder (:190-214). Hardcoding the child's
        // silently DROPPED a parent ~filter for filter-less children —
        // wrong ROWS, not an error (audit 17 bucket analysis).
        return new ClassMapping.Relational(
                child.className(), child.setId(), child.extendsSetId(), child.root(),
                child.mainTable() != null ? child.mainTable() : flatParent.mainTable(),
                child.filter() != null ? child.filter() : flatParent.filter(),
                child.distinct() || flatParent.distinct(),
                !child.groupBy().isEmpty() ? child.groupBy()
                        : flatParent.groupBy(),
                !child.primaryKey().isEmpty()
                        ? child.primaryKey() : flatParent.primaryKey(),
                new ArrayList<>(merged.values()), child.sourceUrl(),
                child.propertyTargetSets(), child.aggregation());
    }

    // ====================================================================
    // M2M cycle detection  —  rejects A.~src=B, B.~src=A (or longer) cycles
    // ====================================================================

    private static void detectM2MCycles(LegacyMappingDefinition md) {
        // Index PureClassMappings by target class FQN for fast walk.
        Map<String, ClassMapping.Pure> pureByTarget = new HashMap<>();
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Pure pcm) {
                pureByTarget.put(pcm.className(), pcm);
            }
        }
        for (ClassMapping.Pure root : pureByTarget.values()) {
            Set<String> visiting = new LinkedHashSet<>();
            walkM2MChain(root, pureByTarget, visiting, md);
        }
    }

    private static void walkM2MChain(ClassMapping.Pure pcm,
                                    Map<String, ClassMapping.Pure> pureByTarget,
                                    Set<String> visiting, LegacyMappingDefinition md) {
        if (!visiting.add(pcm.className())) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "Circular M2M ~src chain detected in mapping '"
                  + md.qualifiedName() + "': " + String.join(" -> ", visiting)
                  + " -> " + pcm.className());
        }
        ClassMapping.Pure next = pureByTarget.get(pcm.sourceClass());
        // SELF-SOURCED Pure mapping (~src X on X): the engine's identity/
        // pass-through idiom (XStore linkage, objectReference shared
        // mappings) — the source is the RAW upstream instance, never a
        // recursive route through the same set; a self-edge is a LEAF,
        // not a cycle (the corpus compiles these; only multi-set loops
        // are genuine ~src cycles)
        if (next != null && next != pcm) {
            walkM2MChain(next, pureByTarget, visiting, md);
        }
        visiting.remove(pcm.className());
    }
}
