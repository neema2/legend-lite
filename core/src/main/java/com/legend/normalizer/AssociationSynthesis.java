// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.DatabaseDefinition;
import com.legend.model.FunctionDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;
import com.legend.model.SynthHat;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
/**
 * Association-mapping realization: multi-hop end injection (Option A), per-pair entry gathering, predicate-function synthesis (doc 5.6.1). Split from MappingNormalizer (the Doors split).
 */
final class AssociationSynthesis {

    private AssociationSynthesis() {}

    /**
     * Option A ({@code docs/MAPPING_LEGACY_TO_FUNCTION.md} §5.6.1b): a multi-hop
     * association end is realized as a class-typed Join PM on the end's
     * owning class, flowing through the same join-chain machinery as a
     * class-typed property mapping ({@link #emitJoinChain}). This pre-pass
     * appends those Join PMs to the relevant {@link ClassMapping.Relational}
     * before class-mapping synthesis. Single-hop ends are left to the
     * standalone {@code legacyAssocPredicate} path (§5.6.1).
     */
    static LegacyMappingDefinition injectMultiHopAssociationPMs(ResolvedMapping pp,
                                                                 ModelBuilder model) {
        LegacyMappingDefinition md = pp.raw();
        ResolvedMapping view = pp;
        // INCLUDE-CLOSURE: the association entries, the owning class
        // mappings and the target unions may live in DIFFERENT mapping
        // definitions (multipleChainedJoins V4: top mapping = unions only,
        // includes carry the class sets and the pair entries). Navigation
        // behavior depends on the REQUESTING mapping's context, so the
        // top-level normalization injects — hoisting an included owner
        // set into this definition when needed (the hoisted copy shadows
        // the included one for this mapping's synthesis).
        List<LegacyMappingDefinition> closure = new ArrayList<>();
        closure.add(md);
        closure.addAll(MappingClosures.of(model).closure(md.qualifiedName()).mappings());
        List<AssociationMapping.Relational> rels = new ArrayList<>();
        for (LegacyMappingDefinition m : closure) {
            for (AssociationMapping am : m.associationMappings()) {
                if (am instanceof AssociationMapping.Relational r) {
                    rels.add(r);
                }
            }
        }
        if (rels.isEmpty()) return md;
        // className -> unqualified injections; className -> setId -> per-set
        // injections (pair entries bind to their SOURCE set only)
        Map<String, List<PropertyMapping>> byClass = new LinkedHashMap<>();
        Map<String, Map<String, List<PropertyMapping>>> bySet = new LinkedHashMap<>();
        for (AssociationMapping.Relational rel : rels) {
            AssociationDefinition ad = model.findAssociation(rel.associationName()).orElseThrow(() -> MissProbe.neverFired("AssociationSynthesis#1"));
            // per (source set, property): a pair group whose TARGET class
            // is union- or inheritance-mapped (or cannot anchor a
            // predicate) is a ROUTED group — ALL its entries, single-hop
            // included, inject as target-stamped Join PMs on their source
            // set: the member's own function carries the navigation and
            // the stack composes the arms' routes (legacy routes as
            // composition §11 A2). Only a plain pair between two
            // anchorable classes takes the predicate path.
            Set<String> routedUnionGroups = new HashSet<>();
            for (AssociationPropertyMapping apm : rel.propertyMappings()) {
                if (!(apm.body() instanceof PropertyMapping.Join join)
                        || apm.sourceSetId() == null) {
                    continue;
                }
                String target = associationTargetClass(ad, apm.propertyName());
                if (target == null) {
                    continue;
                }
                boolean unionTgt =
                        view.unionOf(target) != null;
                boolean inheritanceTgt = !unionTgt
                        && view.inheritanceOf(target)
                                != null;
                // an INHERITANCE-op target has NO set of its own class —
                // the predicate path cannot anchor it (no ~mainTable);
                // pair groups into it ALWAYS take the routed-PM injection
                // (FIX-A computes inheritance member ordinals)
                // The predicate path is IMPOSSIBLE when either end class
                // has no Relational set of its own class (union over
                // SUBCLASS sets: VehicleOwner union(airline,per1)) —
                // those groups force the routed-PM injection too.
                String owner0 = associationOwnerClass(ad, apm.propertyName());
                // the SAME anchor rule synthesizeAssociationMapping applies
                // (a class mapped only as an embedded block anchors on its
                // owner's table — hasMainTable alone misjudged those and
                // regressed 30 inheritance tests when tried, 2026-09-02)
                boolean bindingPossible = owner0 != null
                        && anchorTableOf(view, owner0, model) != null
                        && anchorTableOf(view, target, model) != null;
                // the OWNER side too: every arm of a union-mapped owner
                // carries its own navigation (the law: an operation on a
                // stack is the operation per arm, stacked)
                boolean opOwner = owner0 != null
                        && (view.unionOf(owner0) != null
                                || view.inheritanceOf(owner0) != null);
                // an end whose written id names a NON-ROOT set navigates to
                // THAT set (engine: the property mapping's source/target set
                // ids); the class-level predicate pairs the classes' ROOT
                // extents, so such a pair is routed onto its sets too
                String tgtSetId = join.targetSetId() != null
                        ? join.targetSetId() : apm.targetSetId();
                ClassMapping tgtSet = tgtSetId == null ? null : view.set(tgtSetId);
                ClassMapping srcSet = view.set(apm.sourceSetId());
                boolean nonRootEnd = (tgtSet != null && !view.isRootOrSole(tgtSet))
                        || (srcSet != null && !view.isRootOrSole(srcSet));
                if (!unionTgt && !inheritanceTgt && !opOwner && !nonRootEnd
                        && bindingPossible) {
                    continue;   // plain pair: the predicate path
                }
                // an end whose class cannot ANCHOR a predicate (an Operation-
                // mapped root with no set of its own — SetImplementation,
                // an abstract base) would leave the association silently
                // unbound: its entries inject as routed PMs on their sets
                routedUnionGroups.add(apm.sourceSetId() + "\u0000"
                        + apm.propertyName());
            }
            for (AssociationPropertyMapping apm : rel.propertyMappings()) {
                if (!(apm.body() instanceof PropertyMapping.Join join)) continue;
                boolean routedUnion = apm.sourceSetId() != null
                        && routedUnionGroups.contains(
                                apm.sourceSetId() + "\u0000" + apm.propertyName());
                if (join.joins().size() < 2 && !routedUnion) {
                    continue;   // single-hop -> predicate path
                }
                String owner = associationOwnerClass(ad, apm.propertyName());
                if (owner == null) continue;
                // a per-pair entry on a UNION-mapped owner lands on its
                // SOURCE SET's own record too (legacy routes as composition,
                // §11 A2): the member's function carries the navigation and
                // the stack composes it from the arms
                String tgtSet = join.targetSetId() != null
                        ? join.targetSetId() : apm.targetSetId();
                PropertyMapping.Join stamped = routedUnion && tgtSet != null
                        && join.targetSetId() == null
                        ? new PropertyMapping.Join(join.propertyName(),
                                join.database(), join.joins(), tgtSet)
                        : join;
                if (apm.sourceSetId() != null) {
                    bySet.computeIfAbsent(owner, k -> new LinkedHashMap<>())
                            .computeIfAbsent(apm.sourceSetId(),
                                    k -> new ArrayList<>()).add(stamped);
                } else {
                    byClass.computeIfAbsent(owner, k -> new ArrayList<>())
                            .add(stamped);
                }
            }
        }
        if (byClass.isEmpty() && bySet.isEmpty()) return md;
        List<ClassMapping> rewritten = new ArrayList<>(md.classMappings().size());
        Set<String> ownClasses = new HashSet<>();
        Set<String> ownMapped = new HashSet<>();
        Set<String> ownSetIds = new HashSet<>();
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Relational) {
                ownClasses.add(cm.className());
            }
            ownMapped.add(cm.className());
            ownSetIds.add(ResolvedMapping.idOf(cm));
            ClassMapping.Relational injectedCm = withInjectedPMs(cm, byClass, bySet,
                    lineageOf(cm, view));
            rewritten.add(injectedCm != null ? injectedCm : cm);
        }
        // HOIST: an owner set that lives only in an INCLUDED definition is
        // copied up with its injections (skipped when this definition
        // already maps the class through a set of its own; a class this
        // definition maps only as an OPERATION over included members hoists
        // the members, so each arm's own function carries the navigation)
        Set<String> hoisted = new HashSet<>();
        for (LegacyMappingDefinition m : closure) {
            if (m == md) continue;
            for (ClassMapping cm : m.classMappings()) {
                if (ownClasses.contains(cm.className())
                        || ownSetIds.contains(ResolvedMapping.idOf(cm))
                        || !hoisted.add(ResolvedMapping.idOf(cm))) {
                    continue;
                }
                ClassMapping.Relational injectedCm = withInjectedPMs(cm, byClass, bySet,
                        lineageOf(cm, view));
                if (injectedCm != null) {
                    // a hoisted member of a class THIS definition maps as an
                    // Operation is a member here: the Operation is the root
                    rewritten.add(injectedCm.root() && ownMapped.contains(cm.className())
                            ? withRoot(injectedCm, false) : injectedCm);
                }
            }
        }
        return md.withClassMappings(rewritten);
    }

    /** A set's EXTENDS lineage: its own id, then each ancestor's (a set
     * inherits the pair entries its ancestors own — engine
     * allSuperSetImplementationIds). */
    private static List<String> lineageOf(ClassMapping cm, ResolvedMapping view) {
        List<String> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        String cur = ResolvedMapping.idOf(cm);
        while (cur != null && seen.add(cur)) {
            out.add(cur);
            ClassMapping up = view.set(cur);
            cur = up instanceof ClassMapping.Relational ur ? ur.extendsSetId() : null;
        }
        return out;
    }

    private static ClassMapping.Relational withRoot(ClassMapping.Relational rcm,
            boolean root) {
        return new ClassMapping.Relational(
                rcm.className(), rcm.setId(), rcm.extendsSetId(), root,
                rcm.mainTable(), rcm.filter(), rcm.distinct(), rcm.groupBy(),
                rcm.primaryKey(), rcm.propertyMappings(), rcm.sourceUrl(),
                rcm.propertyTargetSets(), rcm.aggregation());
    }

    /** The class mapping with this class's/set's pending injections
     * appended; null when none apply. */
    private static ClassMapping.@com.legend.base.Nullable Relational withInjectedPMs(ClassMapping cm,
            Map<String, List<PropertyMapping>> byClass,
            Map<String, Map<String, List<PropertyMapping>>> bySet, List<String> lineage) {
        if (!(cm instanceof ClassMapping.Relational rcm)) return null;
        List<PropertyMapping> add = new ArrayList<>();
        List<PropertyMapping> forClass = byClass.get(rcm.className());
        if (forClass != null) add.addAll(forClass);
        // per-SET injections match by SET ID under any owner key: the
        // association end's owner may be a SUPERCLASS of the set's class
        // (ownedVehicles on VehicleOwner, set per1 maps Person — the
        // sourceSetId pins the exact set; set ids are unique in scope).
        // An ANCESTOR set's entries are inherited (extends lineage), a
        // property the set already maps (or a nearer ancestor's entry)
        // shadowing them.
        Set<String> have = new HashSet<>();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            have.add(pm.propertyName());
        }
        for (String id : lineage) {
            boolean own = id.equals(ResolvedMapping.idOf(rcm));
            for (Map<String, List<PropertyMapping>> sets : bySet.values()) {
                List<PropertyMapping> forSet = sets.get(id);
                if (forSet == null) continue;
                for (PropertyMapping pm : forSet) {
                    if (own || have.add(pm.propertyName())) {
                        add.add(pm);
                    }
                }
            }
            if (own) {
                for (PropertyMapping pm : add) {
                    have.add(pm.propertyName());
                }
            }
        }
        // EMBEDDED-set sources: an entry keyed <thisSetId>_<embProp> (or
        // the default <classFqnUnderscored>_<embProp>) belongs INSIDE this
        // set's embedded block — location[f1_address, loc] appends to
        // Firm[f1]'s address block, whose join chain then hoists through
        // the ordinary embedded sub-PM machinery.
        List<PropertyMapping> pms = new ArrayList<>(rcm.propertyMappings());
        boolean nested = false;
        String sid = ResolvedMapping.idOf(rcm);
        String classId = com.legend.model.SetId.defaultFor(rcm.className());
        for (Map<String, List<PropertyMapping>> anySets : bySet.values()) {
            for (var en : anySets.entrySet()) {
                String key = en.getKey();
                String prop = key.startsWith(sid + "_")
                        ? key.substring(sid.length() + 1)
                        : key.startsWith(classId + "_")
                                ? key.substring(classId.length() + 1) : null;
                if (prop == null) {
                    continue;
                }
                for (int i = 0; i < pms.size(); i++) {
                    if (pms.get(i) instanceof PropertyMapping.Embedded emb
                            && emb.propertyName().equals(prop)) {
                        List<PropertyMapping> sub =
                                new ArrayList<>(emb.propertyMappings());
                        sub.addAll(en.getValue());
                        pms.set(i, new PropertyMapping.Embedded(
                                emb.propertyName(), sub));
                        nested = true;
                    }
                }
            }
        }
        if (add.isEmpty() && !nested) return null;
        pms.addAll(add);
        return new ClassMapping.Relational(
                rcm.className(), rcm.setId(), rcm.extendsSetId(), rcm.root(),
                rcm.mainTable(), rcm.filter(), rcm.distinct(), rcm.groupBy(),
                rcm.primaryKey(), pms, rcm.sourceUrl(),
                rcm.propertyTargetSets(), rcm.aggregation());
    }


    /** Record WHY no predicate binding was emitted for {@code am}, under the
     * association's own poison key, so the query-side "association not
     * mapped" wall can say it (audit 2026-09-15 P3-3: these paths withheld
     * the binding with no reason and the user got a reasonless wall). Not an
     * error: the reason surfaces only if someone navigates. */
    private static void recordWithheld(ResolvedMapping md, AssociationMapping am,
            ModelBuilder model, MappingLedger ledger, String reason) {
        ledger.poison(new com.legend.model.PoisonKey.ForAssociation(
                resolveAssociation(model, md, am)
                        .map(a -> a.qualifiedName())
                        .orElse(am.associationName())),
                "no predicate binding was emitted: " + reason
                        + "; mapping=" + md.qualifiedName());
    }

    /** The property's OWN end class (the navigation target), mirror of
     * {@link #associationOwnerClass}. */
    static @com.legend.base.Nullable String associationTargetClass(AssociationDefinition ad, String propName) {
        if (ad.property1().propertyName().equals(propName)) {
            return MappingNormalizer.nameRefOrNull(ad.property1().targetClass());
        }
        if (ad.property2().propertyName().equals(propName)) {
            return MappingNormalizer.nameRefOrNull(ad.property2().targetClass());
        }
        return null;
    }

    /**
     * The class that <em>owns</em> association property {@code propName}: in
     * {@code Association(p1: B, p2: A)}, property {@code p1} is declared on the
     * class {@code p2} points at (and vice versa). Returns {@code null} if
     * {@code propName} is neither end, or the opposite end is non-NameRef.
     */
    static @com.legend.base.Nullable String associationOwnerClass(AssociationDefinition ad, String propName) {
        if (ad.property1().propertyName().equals(propName)) {
            return MappingNormalizer.nameRefOrNull(ad.property2().targetClass());
        }
        if (ad.property2().propertyName().equals(propName)) {
            return MappingNormalizer.nameRefOrNull(ad.property1().targetClass());
        }
        return null;
    }

    /** The mapped association, resolved through the MAPPING's import
     * scope when the header spells a SIMPLE name (the engine grammar:
     * {@code Driver : Relational { AssociationMapping (...) }} inside a
     * file importing the model package). */
    static java.util.Optional<AssociationDefinition> resolveAssociation(
            ModelBuilder model, ResolvedMapping md,
            AssociationMapping am) {
        String name = am.associationName();
        var direct = model.findAssociation(name);
        if (direct.isPresent() || name.contains("::")) {
            return direct;
        }
        for (String pkg : model.importsOf(md.qualifiedName()).wildcards()) {
            var hit = model.findAssociation(pkg + "::" + name);
            if (hit.isPresent()) {
                return hit;
            }
        }
        // same-package fallback (an unimported sibling)
        int cut = md.qualifiedName().lastIndexOf("::");
        return cut < 0 ? java.util.Optional.empty()
                : model.findAssociation(
                        md.qualifiedName().substring(0, cut) + "::" + name);
    }

    static @com.legend.base.Nullable FunctionDefinition synthesizeAssociationMapping(ResolvedMapping md,
                                                                  AssociationMapping am,
                                                                  ModelBuilder model,
                                                                  MappingLedger ledger) {
        AssociationDefinition ad0 = resolveAssociation(model, md, am)
                .orElseGet(MissProbe::miss);
        if (am instanceof AssociationMapping.ModelJoin mj && ad0 != null) {
            return MappingNormalizer.synthesizeModelJoinMapping(md, mj, model,
                    ad0.property1().targetClassFqn(),
                    ad0.property2().targetClassFqn());
        }
        if (am instanceof AssociationMapping.Cross xs && ad0 != null) {
            return MappingNormalizer.synthesizeXStoreMapping(md, xs, model,
                    ad0.property1().targetClassFqn(),
                    ad0.property2().targetClassFqn());
        }
        if (!(am instanceof AssociationMapping.Relational rel)) {
            throw new NotImplementedException(
                    "Association mapping kind " + am.getClass().getSimpleName()
                  + " not supported; mapping=" + md.qualifiedName());
        }
        AssociationDefinition ad = resolveAssociation(model, md, am)
                .orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE, 
                        "AssociationMapping references unknown association '"
                      + am.associationName() + "'; mapping=" + md.qualifiedName()));
        String classA = ad.property1().targetClassFqn();
        String classB = ad.property2().targetClassFqn();

        if (rel.propertyMappings().isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "AssociationMapping for '" + am.associationName()
                  + "' has no property mappings; mapping=" + md.qualifiedName());
        }
        // Pick the FIRST property mapping as the primary; multi-PM
        // disambiguation by [srcSetId, tgtSetId] could differentiate
        // direction-specific joins but the predicate condition is the
        // same regardless (it describes the association in one place).
        AssociationPropertyMapping firstAm = rel.propertyMappings().get(0);
        if (!(firstAm.body() instanceof PropertyMapping.Join firstJoin)) {
            throw new NotImplementedException(
                    "AssociationMapping property body kind "
                  + firstAm.body().getClass().getSimpleName()
                  + " not supported (only Join bodies are bridged); mapping="
                  + md.qualifiedName()
                  + ". See docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.");
        }
        // Multi-hop association: realized as per-end navigation injected into
        // the class realizing functions (Option A; see
        // docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b). A (A,B)->Boolean predicate
        // cannot bind the intermediate row, so no standalone predicate is
        // emitted — return null and let injectMultiHopAssociationPMs handle it.
        if (firstJoin.joins().size() >= 2) {
            return null;
        }
        // First-PM-wins is RETAINED (audit 23 probed-and-reverted): a
        // direction-agreement wall broke testSimpleQueryToAssociationMapping
        // + testProjectThroughAssoWithAssociationMapping — classic
        // association mappings legitimately spell the two directions with
        // DIFFERENT (inverse-equivalent) joins, and the first join's
        // predicate is row-correct for both (engine joins are
        // direction-neutral). Residual: two directions with genuinely
        // NON-equivalent joins would still take the first silently.
        // An end class with NO ~mainTable mapping (its properties live only
        // as Join PMs on the other end) cannot anchor a standalone
        // (A,B)->Boolean predicate — no binding is emitted, and NAVIGATING
        // the association stays loud at resolve time ("association not
        // mapped in mapping"). Declaring it is not an error.
        // ENGINE RULE: an end written with a set id (prop[sourceId, targetId])
        // anchors on THAT set (RelationalValidator.validateAssociationId); an
        // end without one keeps the class rule below
        String srcId = firstAm.sourceSetId();
        String tgtId = firstJoin.targetSetId() != null
                ? firstJoin.targetSetId() : firstAm.targetSetId();
        boolean onProperty1 = ad.property1().propertyName().equals(firstAm.propertyName());
        EndAnchors anchors = endAnchors(md, classA, classB,
                onProperty1 ? tgtId : srcId, onProperty1 ? srcId : tgtId, firstJoin, model);
        if (anchors == null) {
            // WITHHELD, with the reason recorded: navigating this association
            // is loud at resolve time, and before audit 2026-09-15 P3-3 the
            // wall had NO reason to read — the binding simply was not there
            recordWithheld(md, am, model, ledger, "end class '"
                    + (anchorTableOf(md, classA, model) == null ? classA : classB)
                    + "' has no table to anchor a (source, target) predicate on"
                    + " (its properties live only as Join property mappings on the"
                    + " other end, or neither end's set is visible from this"
                    + " mapping); declaring the association is not an error, but"
                    + " navigating it has no step");
            return null;
        }
        // an OPERATION-mapped end (union / inheritance) has no one table
        // to anchor on: its pairs inject onto the member sets (above), and
        // a class-level predicate anchored on one member's table would join
        // that member alone — no predicate; a navigation without a step is
        // loud at demand
        if (md.unionOf(classA) != null || md.inheritanceOf(classA) != null
                || md.unionOf(classB) != null || md.inheritanceOf(classB) != null) {
            recordWithheld(md, am, model, ledger, "an end class is OPERATION-mapped"
                    + " (union or inheritance), so there is no single table to anchor a"
                    + " (source, target) predicate on — the pair entries inject onto the"
                    + " member sets instead; a navigation that finds no step here is loud");
            return null;
        }

        Variable srcRow = new Variable("srcRow");
        Variable tgtRow = new Variable("tgtRow");
        // A SELF-JOIN between two DIFFERENT classes over ONE table (the
        // store's single-table hierarchy: TableAlias -> Table, both rows of
        // relational_elements): the join's table-named side is the first
        // line's SOURCE set and {target} its target set — when that source
        // set maps classB, {target} is classA's row. Same-class
        // self-associations keep the pinned convention (property1's
        // destination on tgtRow; the resolver reverses by property name).
        boolean targetIsA = false;
        if (!classA.equals(classB) && firstAm.sourceSetId() != null) {
            ClassMapping srcSet = md.set(firstAm.sourceSetId());
            targetIsA = srcSet != null && srcSet.className().equals(classB);
        }
        ValueSpecification predicateBody = buildAssocPredicateBody(firstJoin, classA,
                classB, srcRow, tgtRow, am.associationName(), md, model, targetIsA,
                anchors.a().table(), anchors.b().table());
        // predicateBody's tgtRow reads the JOIN's landing table; the call
        // declares tgtRow's row type as classB's ~mainTable. Those must be
        // the SAME table or the lambda would silently mistype (checked
        // inside buildAssocPredicateBody, which knows the landing table).

        Variable a = new Variable("a");
        Variable b = new Variable("b");
        // The adapter lambda speaks ROW scope; its row types are knowable
        // right here (the two ends' ~mainTable), so say them: the src/tgt
        // Relation args bind the signature's S,T and the lambda's columns
        // type through the ordinary kernel — no Any punt, no bespoke
        // checker. The resolver reads the tables from the CALL instead of
        // re-deriving them from the classes' mappings.
        ValueSpecification body = new AppliedFunction(Pure.Lite.LEGACY_ASSOC_PREDICATE, List.of(
                a, b,
                ViewRelation.sourceRefFor(anchors.a(), model, md),
                ViewRelation.sourceRefFor(anchors.b(), model, md),
                new LambdaFunction(List.of(srcRow, tgtRow),
                                         List.of(predicateBody))));

        FunctionDefinition.ParameterDefinition pA = new FunctionDefinition.ParameterDefinition(
                "a", new TypeExpression.NameRef(classA), Multiplicity.Concrete.PURE_ONE);
        FunctionDefinition.ParameterDefinition pB = new FunctionDefinition.ParameterDefinition(
                "b", new TypeExpression.NameRef(classB), Multiplicity.Concrete.PURE_ONE);
        return new FunctionDefinition(
                SynthFqn.mappingAssoc(md.qualifiedName(), am.associationName()),
                List.of(), List.of(), List.of(pA, pB),
                new TypeExpression.NameRef("meta::pure::metamodel::type::Boolean"),
                Multiplicity.Concrete.PURE_ONE,
                List.of(body),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.ASSOC, md.qualifiedName(), am.associationName(),
                        firstAm.propertyName()));
    }

    /**
     * Build the predicate body for an AssociationMapping. Single-hop
     * joins translate to the join condition directly over (srcRow,
     * tgtRow). Multi-hop joins chain conditions through intermediate
     * row bindings: each hop's predicate is conjoined with the next
     * via {@code and(...)}, with intermediate rows resolved by named
     * binding through the chain alias scope.
     */
    static ValueSpecification buildAssocPredicateBody(PropertyMapping.Join join,
                                                             String classA, String classB,
                                                             Variable srcRow, Variable tgtRow,
                                                             String associationName,
                                                             ResolvedMapping md,
                                                             ModelBuilder model) {
        return buildAssocPredicateBody(join, classA, classB, srcRow, tgtRow,
                associationName, md, model, false,
                anchorNameOf(md, classA, model), anchorNameOf(md, classB, model));
    }

    /** The two ends' predicate anchor tables: each end's own set's table
     * when this mapping's closure sees it; otherwise — an end class mapped
     * OUTSIDE the closure (the stress corpus' reporting::BookHasRollup:
     * the association is mapped in the rollup mapping, positions::Book in
     * the positions mapping, and only the queried mapping includes both;
     * the engine resolves an association's ends under the QUERIED
     * mapping) — the table the single-hop join names for that end, which
     * is the same table any including mapping's set would bind. Null
     * when neither end is visible (there is nothing to orient the join
     * by) or the join does not name exactly one other table. */
    /** The two ends' anchor tables, {@code a} for classA and {@code b} for classB. */
    record EndAnchors(LegacyMappingDefinition.TableReference a,
            LegacyMappingDefinition.TableReference b) {
    }

    private static @com.legend.base.Nullable EndAnchors endAnchors(
            ResolvedMapping md, String classA, String classB,
            @com.legend.base.Nullable String idA, @com.legend.base.Nullable String idB,
            PropertyMapping.Join join, ModelBuilder model) {
        LegacyMappingDefinition.TableReference a = endAnchor(md, classA, idA, model);
        LegacyMappingDefinition.TableReference b = endAnchor(md, classB, idB, model);
        if (a != null && b != null) {
            return new EndAnchors(a, b);
        }
        if ((a == null && b == null) || join.joins().size() != 1) {
            return null;
        }
        JoinChainElement hop = join.joins().get(0);
        String db = hop.databaseName() != null ? hop.databaseName() : join.database();
        DatabaseDefinition.JoinDefinition jd = model.findJoin(db, hop.joinName())
                .orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE,
                        "AssociationMapping join '" + hop.joinName() + "' not found in db '" + db
                        + "'; mapping=" + md.qualifiedName()));
        LegacyMappingDefinition.TableReference known =
                java.util.Objects.requireNonNull(a != null ? a : b);
        String other;
        if (MappingNormalizer.containsTargetColumnRef(jd.operation())) {
            other = known.table();   // a self-join: {target} is the same table
        } else {
            Set<String> tables = new java.util.LinkedHashSet<>();
            RelOpTranslator.collectTablesIn(jd.operation(), tables);
            tables.remove(known.table());
            if (tables.size() != 1) {
                return null;
            }
            other = tables.iterator().next();
        }
        LegacyMappingDefinition.TableReference derived =
                new LegacyMappingDefinition.TableReference(db, other);
        return a != null ? new EndAnchors(a, derived)
                : new EndAnchors(derived, java.util.Objects.requireNonNull(b));
    }

    /** {@code targetIsA}: on a self-join, {@code {target}} is classA's row
     * (the table-named side classB's) — see synthesizeAssociationMapping. */
    static ValueSpecification buildAssocPredicateBody(PropertyMapping.Join join,
                                                             String classA, String classB,
                                                             Variable srcRow, Variable tgtRow,
                                                             String associationName,
                                                             ResolvedMapping md,
                                                             ModelBuilder model,
                                                             boolean targetIsA,
                                                             String sourceTable,
                                                             String classBTable) {
        if (join.joins().isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "AssociationMapping for '" + associationName
                  + "' has empty join chain; mapping=" + md.qualifiedName());
        }
        if (join.joins().size() == 1) {
            JoinChainElement hop = join.joins().get(0);
            String hopDb = hop.databaseName() != null ? hop.databaseName() : join.database();
            DatabaseDefinition.JoinDefinition jd = model.findJoin(hopDb, hop.joinName())
                    .orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE, 
                            "AssociationMapping join '" + hop.joinName()
                          + "' not found in db '" + hopDb + "'; association='"
                          + associationName + "', mapping=" + md.qualifiedName()));
            // The synthesized legacyAssocPredicate call declares tgtRow's row
            // type as classB's ~mainTable; the join must actually land there,
            // or the lambda's column reads would silently mistype.
            RelationalOperation cond2 = MappingNormalizer.resolveViewRefsInJoin(
                    jd.operation(), hopDb, sourceTable, model, md,
                    model.findView(hopDb, sourceTable).isPresent() ? sourceTable : null,
                    null,
                    model.findView(hopDb, classBTable).isPresent() ? classBTable : null,
                    /*anySide*/ true);
            String targetTable = MappingNormalizer.determineTargetTable(cond2, sourceTable,
                    hop.joinName(), associationName, 1, md.qualifiedName());
            if (!targetTable.equals(classBTable)) {
                // a join landing on classB's OWN main source is fine even
                // when that source is a VIEW — the class-source override
                // already expands it, so tgtRow IS the view relation's row
                // (declared column names); any OTHER view target stays a
                // named wall
                MappingNormalizer.requireNonViewTarget(targetTable, hopDb,
                        hop.joinName(), model, md);
            }
            if (!targetTable.equals(classBTable)) {
                throw new NotImplementedException(
                        "AssociationMapping join '" + hop.joinName() + "' lands on table '"
                      + targetTable + "' but the target end class '" + classB
                      + "' is mapped to ~mainTable '" + classBTable + "'; an "
                      + "association joining through a non-mainTable row is not "
                      + "supported. Association='" + associationName + "', mapping="
                      + md.qualifiedName());
            }
            boolean swap = targetIsA && MappingNormalizer.containsTargetColumnRef(cond2);
            Variable tableSide = swap ? tgtRow : srcRow;
            Variable targetSide = swap ? srcRow : tgtRow;
            Map<String, ValueSpecification> condScope = new LinkedHashMap<>();
            condScope.put(sourceTable, tableSide);
            if (!targetTable.equals(sourceTable)) condScope.put(targetTable, targetSide);
            // translate the SAME view-resolved tree the target was picked
            // from — raw refs name pre-resolution tables (T1.10)
            return RelOpTranslator.translate(cond2, condScope, targetSide, /*rowBind*/ null, RelOpTranslator.PipelineView.NONE);
        }
        // Unreachable: multi-hop associations are intercepted in
        // synthesizeAssociationMapping (returns null) and realized as per-end
        // navigation injected into the class realizing functions (Option A;
        // see docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b). A (A,B)->Boolean
        // predicate cannot bind the intermediate row(s). This guard fires only
        // if that interception is bypassed — a compiler invariant violation.
        throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                "Multi-hop AssociationMapping for '" + associationName + "' ("
              + join.joins().size() + " join hops) reached the predicate "
              + "builder; it should have been handled by per-end injection. "
              + "Mapping=" + md.qualifiedName());
    }

    /** The predicate ANCHOR table for an association end: the class's own
     * root/sole set's ~mainTable, or — for a class mapped ONLY as an
     * EMBEDDED block — the OWNING set's main table (engine: an embedded
     * set implementation shares its owner's table; Join Firm_Organizations
     * anchors on PERSON_FIRM_DENORM). Null when neither resolves. */
    static LegacyMappingDefinition.@com.legend.base.Nullable TableReference
            anchorTableOf(ResolvedMapping md, String classFqn,
            ModelBuilder model) {
        LegacyMappingDefinition.TableReference own = MappingNormalizer.mainTableOrNull(md, classFqn);
        if (own != null) {
            return own;
        }
        List<LegacyMappingDefinition> closure = new ArrayList<>();
        closure.addAll(md.closure());
        for (LegacyMappingDefinition m : closure) {
            for (ClassMapping cm : m.classMappings()) {
                if (!(cm instanceof ClassMapping.Relational rcm)) {
                    continue;
                }
                for (PropertyMapping pm : rcm.propertyMappings()) {
                    if (!(pm instanceof PropertyMapping.Embedded
                            || pm instanceof PropertyMapping
                                    .OtherwiseEmbedded)) {
                        continue;
                    }
                    ClassDefinition owner =
                            model.knowledge().hierarchyClass(rcm.className()).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at AssociationSynthesis#1 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + rcm.className()));
                    TypeExpression pt = owner == null ? null
                            : model.knowledge().propertyType(owner, pm.propertyName());
                    if (pt instanceof TypeExpression.NameRef nr
                            && nr.name().equals(classFqn)) {
                        LegacyMappingDefinition.TableReference mt =
                                rcm.mainTable() != null ? rcm.mainTable()
                                        : MappingNormalizer
                                                .inferMainTableQuiet(rcm);
                        if (mt != null) {
                            return mt;
                        }
                    }
                }
            }
        }
        return null;
    }

    /** One end's anchor: the set its id names when written and resolvable
     * (the engine's rule), else the class's ({@link #anchorTableOf}). An
     * unresolvable id is only a WARNING in the engine
     * (validateAssociationId's useWarning), so the class rule stands in. */
    private static LegacyMappingDefinition.@com.legend.base.Nullable TableReference endAnchor(
            ResolvedMapping md, String classFqn, @com.legend.base.Nullable String setId,
            ModelBuilder model) {
        if (setId != null) {
            LegacyMappingDefinition.TableReference byId = anchorOfSet(md, setId);
            if (byId != null) {
                return byId;
            }
        }
        return anchorTableOf(md, classFqn, model);
    }

    /** The table of the set {@code setId} names: a Relational set's
     * ~mainTable, or — an EMBEDDED set, id {@code <ownerId>_<property>} —
     * its owner's (engine findMainTableAlias: an embedded set shares its
     * owner's main table). Null when the id names no such set. */
    private static LegacyMappingDefinition.@com.legend.base.Nullable TableReference anchorOfSet(
            ResolvedMapping md, String setId) {
        if (md.set(setId) instanceof ClassMapping.Relational rcm) {
            return mainTableOfSet(rcm);
        }
        for (int cut = setId.lastIndexOf('_'); cut > 0; cut = setId.lastIndexOf('_', cut - 1)) {
            String prop = setId.substring(cut + 1);
            if (md.set(setId.substring(0, cut)) instanceof ClassMapping.Relational owner
                    && owner.propertyMappings().stream().anyMatch(pm ->
                            (pm instanceof PropertyMapping.Embedded
                                    || pm instanceof PropertyMapping.OtherwiseEmbedded)
                                    && pm.propertyName().equals(prop))) {
                return mainTableOfSet(owner);
            }
        }
        return null;
    }

    private static LegacyMappingDefinition.@com.legend.base.Nullable TableReference mainTableOfSet(
            ClassMapping.Relational rcm) {
        return rcm.mainTable() != null ? rcm.mainTable()
                : MappingNormalizer.inferMainTableQuiet(rcm);
    }

    /** {@link #anchorTableOf}'s table NAME — loud when unresolvable (the
     * synthesis gate already null-checked). */
    private static String anchorNameOf(ResolvedMapping md,
            String classFqn, ModelBuilder model) {
        return java.util.Objects.requireNonNull(
                anchorTableOf(md, classFqn, model),
                () -> "no predicate anchor for '" + classFqn + "' in "
                        + md.qualifiedName()).table();
    }
}
