package com.legend.normalizer;

import com.legend.parser.NormalizedModel;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.parser.Multiplicity;
import com.legend.parser.ParsedModel;
import com.legend.parser.TypeExpression;
import com.legend.parser.element.AssociationDefinition;
import com.legend.parser.element.AssociationMapping;
import com.legend.parser.element.AssociationPropertyMapping;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.ClassMapping;
import com.legend.parser.element.ComparisonOp;
import com.legend.parser.element.DatabaseDefinition;
import com.legend.parser.element.EnumerationMapping;
import com.legend.parser.element.FilterMapping;
import com.legend.parser.element.FilterPointer;
import com.legend.parser.element.FunctionDefinition;
import com.legend.parser.element.JoinChainElement;
import com.legend.parser.element.LogicalOp;
import com.legend.parser.element.LegacyMappingDefinition;
import com.legend.parser.element.MappingDefinition;
import com.legend.parser.element.Realization;
import com.legend.parser.element.PackageableElement;
import com.legend.parser.element.PropertyMapping;
import com.legend.parser.element.SynthHat;
import com.legend.parser.element.RelationalOperation;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CBoolean;
import com.legend.parser.spec.CFloat;
import com.legend.parser.spec.CInteger;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.ColSpec;
import com.legend.parser.spec.ColSpecArray;
import com.legend.parser.spec.EnumValue;
import com.legend.parser.spec.KeyExpression;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.NewInstance;
import com.legend.parser.spec.NewInstanceCast;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.TypeAnnotation;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Legacy Mapping DSL desugarer. Translates every legacy
 * {@link LegacyMappingDefinition} into clean-sheet function form per
 * {@code docs/MAPPING_LEGACY_TO_FUNCTION.md} with full feature parity to
 * engine's {@code com.gs.legend.compiler.MappingNormalizer}.
 *
 * <h2>Per class mapping</h2>
 * Synthesizes one {@code Class[*]}-returning function per
 * {@link ClassMapping}. Three Relational source-path variants:
 * <ul>
 *   <li><b>JSON-source</b> ({@code sourceUrl != null}): emit
 *       {@code sourceUrl(url)} as source; bind class properties via
 *       {@code $row.data->get('propName', @Type)} (engine parity:
 *       {@code RelationalMapping.variantIdentity}).</li>
 *   <li><b>View-backed</b> ({@code mainTable} resolves to a
 *       {@link DatabaseDefinition.ViewDefinition}): expand view as
 *       macro &mdash; rewrite PMs through view column mappings, apply
 *       view-level {@code ~filter}/{@code ~distinct}/{@code ~groupBy},
 *       then continue with the table pipeline.</li>
 *   <li><b>Table-backed</b> (default): {@code tableReference(db, t)}
 *       as source.</li>
 * </ul>
 *
 * <p>Common pipeline structure:
 * <pre>
 *   &lt;source&gt;
 *     -&gt; join(~alias: tableReference(...), {s,t | cond})*           // intermediate hops + JoinNav hoisting
 *     -&gt; legacyNavigate(~slot: getAll(T), {sr,tr | cond})*          // class-typed final hops + OE fallbacks
 *     -&gt; filter(r | &lt;inlined predicate&gt;)?                       // ~filter Direct + JoinMediated
 *     -&gt; groupBy(~[keys], ~[aggs])?                                  // ~groupBy
 *     -&gt; distinct()?                                                 // ~distinct
 *     -&gt; map(r | ^Class(&lt;fields&gt;))
 * </pre>
 *
 * <h2>Per association mapping</h2>
 * Synthesizes one {@code (A[1], B[1]) -> Boolean[1]} predicate
 * function whose body uses {@code legacyAssocPredicate} to bridge the
 * physical-column lambda to instance-level parameters
 * (doc &sect;5.6.1). Multi-hop AssociationMappings emit chained
 * predicates with intermediate row bindings.
 *
 * <h2>Helpers emitted</h2>
 * <ul>
 *   <li>{@code legacyNavigate} &mdash; pipeline step, symmetric to
 *       clean-sheet {@code navigate}. Binds a named slot to a class
 *       instance reached via a physical-column predicate
 *       ({@code docs/MAPPING_LEGACY_TO_FUNCTION.md} &sect;2.1).</li>
 *   <li>{@code legacyAssocPredicate} &mdash; row-extraction adapter
 *       for AssociationMapping predicate function bodies
 *       (doc &sect;2.2).</li>
 * </ul>
 *
 * <h2>Hand-written clean-sheet mappings</h2>
 * Bypass this phase: their realizing functions are already Pure
 * expressions written by the user.
 */
public final class MappingNormalizer {

    private MappingNormalizer() {}

    /**
     * Canonical Pure aggregate function names recognized for {@code ~groupBy}
     * decomposition. Matched case-sensitively against the parsed function name
     * (Pure function names are case-sensitive) and emitted verbatim, so the
     * emitted aggregate call resolves to the same-named native (e.g. {@code stdDev}
     * must keep its camelCase to match the catalog entry, not be lowercased).
     */
    private static final Set<String> AGGREGATE_FNS = Set.of(
            "sum", "count", "avg", "min", "max", "stdDev", "variance");

    // ====================================================================
    // Entry point
    // ====================================================================

    /**
     * Desugar legacy mapping DSL (Phase E.1) into synthesized realizing
     * functions, threading a pre-built resolution index.
     *
     * <p>{@code model} MUST be {@code ModelBuilder.from(parsed)} for the same
     * {@code parsed}: it is the shared resolution view
     * (findClass/findAssociation/findJoin/findFilter) and supplies the
     * cross-baked mappings (findMapping picks up JsonModelConnection synthetic
     * class mappings injected during {@code ModelBuilder.from}). The index is
     * owned by the step-5 entry {@link ModelNormalizer} (or a test), never
     * self-built here, so this phase is a pure function of {@code (parsed, model)}
     * and every caller — prod and test — exercises this single code path.
     */
    public static NormalizedModel normalize(ParsedModel parsed, ModelBuilder model) {
        Objects.requireNonNull(parsed, "parsed");
        Objects.requireNonNull(model, "model");
        List<PackageableElement> out = new ArrayList<>(parsed.elements().size());
        List<FunctionDefinition> lifted = new ArrayList<>();
        for (PackageableElement el : parsed.elements()) {
            // The legacy mapping we read from `parsed` may have been
            // cross-baked (e.g., by JsonModelConnection bindings) in
            // ModelBuilder.from. Re-fetch the latest legacy surface from the
            // resolution index to pick up any synthetic class mappings.
            if (el instanceof LegacyMappingDefinition md) {
                LegacyMappingDefinition latest =
                        model.findLegacyMapping(md.qualifiedName()).orElse(md);
                // Rewrite legacy surface -> canonical binding table; the legacy
                // record does NOT flow past Phase E (CLEAN_SHEET_INVERSION §1.5).
                out.add(normalizeMapping(latest, model, lifted));
            } else if (el instanceof MappingDefinition canonical) {
                // Clean-sheet (Door 1/3) mapping: function-ref bindings pass
                // through; inline expression bindings (Door 3) are lambda-lifted
                // here (CLEAN_SHEET_INVERSION §5.3), so no Inline survives Phase E.
                out.add(liftInlineBindings(canonical, model, lifted));
            } else {
                out.add(el);
            }
        }
        // Lifted realizing functions are ordinary top-level elements
        // (docs/CLEAN_SHEET_INVERSION.md §1) — appended after the
        // structural elements, never stored on the mapping record.
        out.addAll(lifted);
        return new NormalizedModel(out, parsed.imports());
    }

    /**
     * Rewrite one legacy mapping surface into the canonical
     * {@link MappingDefinition} binding table, lifting each class/association
     * transform into an ordinary top-level {@link FunctionDefinition}
     * (appended to {@code lifted}). Each binding references its realizing
     * function by the function's own FQN — the same string the lift produced,
     * so binding and function agree by construction (no regeneration).
     */
    private static MappingDefinition normalizeMapping(LegacyMappingDefinition md,
                                                     ModelBuilder model,
                                                     List<FunctionDefinition> lifted) {
        detectM2MCycles(md, model);

        // Pre-pass: flatten `extends [parentSetId]` by merging inherited
        // property mappings into each child mapping (child overrides on
        // property-name conflict; multi-level resolves recursively). See
        // docs/MAPPING_LEGACY_TO_FUNCTION.md §5.2.3.
        md = resolveExtends(md);

        // Pre-pass: inject MULTI-HOP association ends as class-typed Join PMs
        // into their owning class mappings (Option A; see
        // docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b). Single-hop ends keep the
        // §5.6.1 standalone predicate.
        md = injectMultiHopAssociationPMs(md, model);

        List<MappingDefinition.ClassBinding> classBindings =
                new ArrayList<>(md.classMappings().size());
        for (ClassMapping cm : md.classMappings()) {
            FunctionDefinition fn = synthesizeClassMapping(md, cm, model);
            lifted.add(fn);
            classBindings.add(new MappingDefinition.ClassBinding(
                    cm.className(),
                    cm instanceof ClassMapping.Pure
                            ? MappingDefinition.Kind.PURE
                            : MappingDefinition.Kind.RELATIONAL,
                    cm.setId(), cm.extendsSetId(), cm.root(),
                    fn.qualifiedName()));
        }
        List<MappingDefinition.AssociationBinding> assocBindings =
                new ArrayList<>(md.associationMappings().size());
        for (AssociationMapping am : md.associationMappings()) {
            // null => multi-hop association, realized by per-end navigation
            // injected above; no standalone predicate function, hence no binding.
            FunctionDefinition fn = synthesizeAssociationMapping(md, am, model);
            if (fn != null) {
                lifted.add(fn);
                assocBindings.add(new MappingDefinition.AssociationBinding(
                        am.associationName(), fn.qualifiedName()));
            }
        }
        // includes survive the rewrite unchanged — one shared MappingInclude type.
        return new MappingDefinition(
                md.qualifiedName(),
                md.includes(),
                classBindings,
                assocBindings,
                md.enumerationMappings(),
                md.testSuitesSource());
    }

    // ====================================================================
    // Door 3 — lift inline expression bindings in a clean-sheet mapping
    // ====================================================================

    /**
     * Lambda-lift every inline binding ({@link Realization.Inline})
     * in a clean-sheet mapping into an ordinary top-level function (appended to
     * {@code lifted}) and rewrite the binding to a function ref. Function-ref
     * bindings pass through untouched. Post-condition: no {@code Inline}
     * survives (CLEAN_SHEET_INVERSION §5.3 / §7.4). Reference-equality preserved
     * when the mapping has no inline bindings.
     */
    private static MappingDefinition liftInlineBindings(MappingDefinition md,
                                                       ModelBuilder model,
                                                       List<FunctionDefinition> lifted) {
        boolean changed = false;
        List<MappingDefinition.ClassBinding> classBindings =
                new ArrayList<>(md.classBindings().size());
        for (MappingDefinition.ClassBinding cb : md.classBindings()) {
            if (cb.realization() instanceof Realization.Inline inl) {
                String fnFqn = SynthFqn.mappingClass(md.qualifiedName(), cb.classFqn());
                lifted.add(liftClassInline(md, cb, inl, fnFqn));
                classBindings.add(new MappingDefinition.ClassBinding(
                        cb.classFqn(), cb.kind(), cb.setId(), cb.extendsSetId(), cb.root(), fnFqn));
                changed = true;
            } else {
                classBindings.add(cb);
            }
        }
        List<MappingDefinition.AssociationBinding> assocBindings =
                new ArrayList<>(md.associationBindings().size());
        for (MappingDefinition.AssociationBinding ab : md.associationBindings()) {
            if (ab.realization() instanceof Realization.Inline inl) {
                String fnFqn = SynthFqn.mappingAssoc(md.qualifiedName(), ab.associationFqn());
                lifted.add(liftAssocInline(md, ab, inl, fnFqn, model));
                assocBindings.add(new MappingDefinition.AssociationBinding(
                        ab.associationFqn(), fnFqn));
                changed = true;
            } else {
                assocBindings.add(ab);
            }
        }
        if (!changed) return md;
        return new MappingDefinition(md.qualifiedName(), md.includes(),
                classBindings, assocBindings, md.enumerationMappings(), md.testSuitesSource());
    }

    /**
     * A class inline body lifts to a param-less {@code (): Class[*]} function
     * whose body is the user's expression verbatim. Kind-agnostic: Relational
     * and Pure differ only in what the body starts from, which the lift never
     * inspects.
     */
    private static FunctionDefinition liftClassInline(MappingDefinition md,
                                                     MappingDefinition.ClassBinding cb,
                                                     Realization.Inline inl,
                                                     String fnFqn) {
        return new FunctionDefinition(
                fnFqn, List.of(), List.of(), List.of(),
                new TypeExpression.NameRef(cb.classFqn()),
                Multiplicity.Concrete.ZERO_MANY,
                inl.body(),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.CLASS, md.qualifiedName(), cb.classFqn()));
    }

    /**
     * An association inline body is a single {@code (source, target) -> Boolean}
     * lambda; it lifts to a two-parameter {@code Boolean[1]} predicate. The
     * param <em>names</em> come from the user's lambda; the param <em>types</em>
     * come from the association's two ends (looked up in the model), matching
     * the legacy predicate signature.
     */
    private static FunctionDefinition liftAssocInline(MappingDefinition md,
                                                     MappingDefinition.AssociationBinding ab,
                                                     Realization.Inline inl,
                                                     String fnFqn,
                                                     ModelBuilder model) {
        if (inl.body().size() != 1 || !(inl.body().get(0) instanceof LambdaFunction lam)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "inline association predicate for '" + ab.associationFqn()
                  + "' must be a single (source, target) -> Boolean lambda; mapping="
                  + md.qualifiedName());
        }
        if (lam.parameters().size() != 2) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "inline association predicate for '" + ab.associationFqn()
                  + "' must take exactly 2 parameters (source, target); got "
                  + lam.parameters().size() + "; mapping=" + md.qualifiedName());
        }
        AssociationDefinition ad = model.findAssociation(ab.associationFqn())
                .orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                        "inline association predicate references unknown association '"
                      + ab.associationFqn() + "'; mapping=" + md.qualifiedName()));
        String classA = associationEndClass(ad.property1().targetClass(),
                "association '" + ab.associationFqn() + "' end1");
        String classB = associationEndClass(ad.property2().targetClass(),
                "association '" + ab.associationFqn() + "' end2");
        Variable p0 = lam.parameters().get(0);
        Variable p1 = lam.parameters().get(1);
        var paramA = new FunctionDefinition.ParameterDefinition(
                p0.name(), new TypeExpression.NameRef(classA), Multiplicity.Concrete.PURE_ONE);
        var paramB = new FunctionDefinition.ParameterDefinition(
                p1.name(), new TypeExpression.NameRef(classB), Multiplicity.Concrete.PURE_ONE);
        return new FunctionDefinition(
                fnFqn, List.of(), List.of(), List.of(paramA, paramB),
                new TypeExpression.NameRef("meta::pure::metamodel::type::Boolean"),
                Multiplicity.Concrete.PURE_ONE,
                lam.body(),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.ASSOC, md.qualifiedName(), ab.associationFqn()));
    }

    // ====================================================================
    // Pre-pass: flatten `extends [parentSetId]`  —  doc §5.2.3
    // ====================================================================

    /**
     * Resolve {@code extends [parentSetId]} on Relational class mappings by
     * merging the parent's property mappings into the child
     * ({@code docs/MAPPING_LEGACY_TO_FUNCTION.md} §5.2.3):
     * <ol>
     *   <li>Resolve the parent binding by {@code setId} within this mapping.</li>
     *   <li>Concatenate parent + child property mappings, child winning on
     *       property-name conflict.</li>
     *   <li>Multi-level {@code extends} resolves recursively before merging.</li>
     *   <li>The {@code extends} annotation is preserved on the binding for
     *       query-time set-ID dispatch.</li>
     * </ol>
     * The parent's {@code ~mainTable} is <em>not</em> auto-copied; the child
     * must declare its own (the function form requires explicitness).
     */
    private static LegacyMappingDefinition resolveExtends(LegacyMappingDefinition md) {
        boolean any = md.classMappings().stream().anyMatch(cm -> cm.extendsSetId() != null);
        if (!any) return md;
        Map<String, ClassMapping> bySetId = new HashMap<>();
        for (ClassMapping cm : md.classMappings()) {
            if (cm.setId() != null) bySetId.put(cm.setId(), cm);
        }
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
                throw new com.legend.error.NotImplementedException(
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
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "Circular 'extends' chain in mapping '" + md.qualifiedName()
                  + "': " + String.join(" -> ", chain) + " -> " + parentSetId);
        }
        ClassMapping parent = bySetId.get(parentSetId);
        if (parent == null) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "Class mapping for '" + child.className() + "' extends ["
                  + parentSetId + "] but no class mapping with set id '" + parentSetId
                  + "' exists in mapping=" + md.qualifiedName());
        }
        if (!(parent instanceof ClassMapping.Relational parentRcm)) {
            throw new com.legend.error.NotImplementedException(
                    "Class mapping for '" + child.className() + "' extends ["
                  + parentSetId + "] which is not a Relational mapping; only "
                  + "Relational extends is supported. Mapping=" + md.qualifiedName());
        }
        ClassMapping.Relational flatParent = parentRcm.extendsSetId() != null
                ? flattenExtends(parentRcm, bySetId, chain, md)
                : parentRcm;
        // Parent PMs first (declaration order), child overrides by property name.
        LinkedHashMap<String, PropertyMapping> merged = new LinkedHashMap<>();
        for (PropertyMapping pm : flatParent.propertyMappings()) {
            merged.put(pm.propertyName(), pm);
        }
        for (PropertyMapping pm : child.propertyMappings()) {
            merged.put(pm.propertyName(), pm);
        }
        return new ClassMapping.Relational(
                child.className(), child.setId(), child.extendsSetId(), child.root(),
                child.mainTable(), child.filter(), child.distinct(), child.groupBy(),
                child.primaryKey(), new ArrayList<>(merged.values()), child.sourceUrl());
    }

    // ====================================================================
    // Pre-pass: inject multi-hop association ends as class-typed Join PMs
    // ====================================================================

    /**
     * Option A ({@code docs/MAPPING_LEGACY_TO_FUNCTION.md} §5.6.1b): a multi-hop
     * association end is realized as a class-typed Join PM on the end's
     * owning class, flowing through the same join-chain machinery as a
     * class-typed property mapping ({@link #emitJoinChain}). This pre-pass
     * appends those Join PMs to the relevant {@link ClassMapping.Relational}
     * before class-mapping synthesis. Single-hop ends are left to the
     * standalone {@code legacyAssocPredicate} path (§5.6.1).
     */
    private static LegacyMappingDefinition injectMultiHopAssociationPMs(LegacyMappingDefinition md,
                                                                 ModelBuilder model) {
        if (md.associationMappings().isEmpty()) return md;
        Map<String, List<PropertyMapping>> injected = new LinkedHashMap<>();
        for (AssociationMapping am : md.associationMappings()) {
            if (!(am instanceof AssociationMapping.Relational rel)) continue;
            AssociationDefinition ad = model.findAssociation(am.associationName()).orElse(null);
            if (ad == null) continue;
            for (AssociationPropertyMapping apm : rel.propertyMappings()) {
                if (!(apm.body() instanceof PropertyMapping.Join join)) continue;
                if (join.joins().size() < 2) continue;   // single-hop -> predicate path
                String owner = associationOwnerClass(ad, apm.propertyName());
                if (owner == null) continue;
                injected.computeIfAbsent(owner, k -> new ArrayList<>()).add(join);
            }
        }
        if (injected.isEmpty()) return md;
        List<ClassMapping> rewritten = new ArrayList<>(md.classMappings().size());
        for (ClassMapping cm : md.classMappings()) {
            List<PropertyMapping> add = injected.get(cm.className());
            if (add != null && cm instanceof ClassMapping.Relational rcm) {
                List<PropertyMapping> pms = new ArrayList<>(rcm.propertyMappings());
                pms.addAll(add);
                rewritten.add(new ClassMapping.Relational(
                        rcm.className(), rcm.setId(), rcm.extendsSetId(), rcm.root(),
                        rcm.mainTable(), rcm.filter(), rcm.distinct(), rcm.groupBy(),
                        rcm.primaryKey(), pms, rcm.sourceUrl()));
            } else {
                rewritten.add(cm);
            }
        }
        return md.withClassMappings(rewritten);
    }

    /**
     * The class that <em>owns</em> association property {@code propName}: in
     * {@code Association(p1: B, p2: A)}, property {@code p1} is declared on the
     * class {@code p2} points at (and vice versa). Returns {@code null} if
     * {@code propName} is neither end, or the opposite end is non-NameRef.
     */
    private static String associationOwnerClass(AssociationDefinition ad, String propName) {
        if (ad.property1().propertyName().equals(propName)) {
            return nameRefOrNull(ad.property2().targetClass());
        }
        if (ad.property2().propertyName().equals(propName)) {
            return nameRefOrNull(ad.property1().targetClass());
        }
        return null;
    }

    private static String nameRefOrNull(TypeExpression t) {
        return t instanceof TypeExpression.NameRef nr ? nr.name() : null;
    }

    // Lifted-function FQNs are owned by SynthFqn (the single naming authority,
    // docs/CLEAN_SHEET_INVERSION.md §3): SynthFqn.mappingClass / mappingAssoc.

    // ====================================================================
    // M2M cycle detection  —  rejects A.~src=B, B.~src=A (or longer) cycles
    // ====================================================================

    private static void detectM2MCycles(LegacyMappingDefinition md, ModelBuilder model) {
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
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "Circular M2M ~src chain detected in mapping '"
                  + md.qualifiedName() + "': " + String.join(" -> ", visiting)
                  + " -> " + pcm.className());
        }
        ClassMapping.Pure next = pureByTarget.get(pcm.sourceClass());
        if (next != null) walkM2MChain(next, pureByTarget, visiting, md);
        visiting.remove(pcm.className());
    }

    // ====================================================================
    // Class mapping synthesis (top-level dispatch)
    // ====================================================================

    private static FunctionDefinition synthesizeClassMapping(LegacyMappingDefinition md,
                                                            ClassMapping cm,
                                                            ModelBuilder model) {
        ValueSpecification body = switch (cm) {
            case ClassMapping.Pure pcm       -> synthM2M(md, pcm, model, new HashSet<>());
            case ClassMapping.Relational rcm -> synthRelational(md, rcm, model);
        };
        return new FunctionDefinition(
                SynthFqn.mappingClass(md.qualifiedName(), cm.className()),
                List.of(), List.of(), List.of(),
                new TypeExpression.NameRef(cm.className()),
                Multiplicity.Concrete.ZERO_MANY,
                List.of(body),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.CLASS, md.qualifiedName(), cm.className()));
    }

    // ====================================================================
    // M2M (ClassMapping.Pure)  —  doc §5.5
    // ====================================================================

    private static ValueSpecification synthM2M(LegacyMappingDefinition md,
                                              ClassMapping.Pure pcm,
                                              ModelBuilder model,
                                              Set<String> cycleStack) {
        cycleStack.add(pcm.className());
        try {
            // Source: SourceClass.all() — emitted as getAll(SourceClass).
            ValueSpecification source = new AppliedFunction("getAll",
                    List.of(new PackageableElementPtr(pcm.sourceClass())));
            if (pcm.filter() != null) {
                source = new AppliedFunction("filter", List.of(source,
                        new LambdaFunction(List.of(new Variable("src")),
                                           List.of(pcm.filter()))));
            }
            // Terminal: map(src | ^Class(...)).
            Variable srcBind = new Variable("src");
            Map<String, KeyExpression> fields = new LinkedHashMap<>();
            ClassDefinition tgt = model.findClass(pcm.className()).orElse(null);
            for (ClassMapping.Pure.PropertyBinding pb : pcm.propertyBindings()) {
                if (tgt != null && findPropertyTypeDeep(tgt, pb.propertyName(), model) == null) {
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                            "M2M PropertyBinding '" + pb.propertyName()
                          + "' is not declared on class '" + tgt.qualifiedName()
                          + "'; mapping=" + md.qualifiedName());
                }
                fields.put(pb.propertyName(),
                        new KeyExpression(m2mPropertyValue(pb, tgt, md, model, cycleStack)));
            }
            return new AppliedFunction("map", List.of(source,
                    new LambdaFunction(List.of(srcBind),
                                       List.of(buildNewInstance(pcm.className(), fields)))));
        } finally {
            cycleStack.remove(pcm.className());
        }
    }

    private static ValueSpecification m2mPropertyValue(
            ClassMapping.Pure.PropertyBinding pb, ClassDefinition tgt,
            LegacyMappingDefinition md, ModelBuilder model, Set<String> cycleStack) {
        if (tgt == null) return pb.expression();
        TypeExpression propType = findPropertyTypeDeep(tgt, pb.propertyName(), model);
        if (!(propType instanceof TypeExpression.NameRef nr)) return pb.expression();
        String innerFqn = nr.name();
        if (model.findClass(innerFqn).isEmpty()) return pb.expression();
        if (!model.isMappedClass(innerFqn)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "M2M class-typed property '" + pb.propertyName() + "' on '"
                  + tgt.qualifiedName() + "' targets unmapped class '" + innerFqn
                  + "'; map '" + innerFqn + "' or use Embedded. Mapping="
                  + md.qualifiedName());
        }
        if (!cycleStack.add(innerFqn)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "Cycle materializing M2M class-typed property; class "
                  + innerFqn + " recurses. Stack=" + cycleStack);
        }
        try {
            return new NewInstanceCast(innerFqn, List.of(), pb.expression());
        } finally {
            cycleStack.remove(innerFqn);
        }
    }

    // ====================================================================
    // Relational dispatch:  JsonSource | View-backed | Table-backed
    // ====================================================================

    private static ValueSpecification synthRelational(LegacyMappingDefinition md,
                                                     ClassMapping.Relational rcm,
                                                     ModelBuilder model) {
        // JSON-source: synthesized by ModelBuilder cross-baking from a
        // RuntimeDefinition's JsonModelConnection. mainTable is null;
        // sourceUrl carries the inline VARIANT subquery source.
        if (rcm.sourceUrl() != null) {
            return synthJsonSourceMapping(md, rcm, model);
        }
        if (rcm.mainTable() == null) {
            throw new com.legend.error.NotImplementedException(
                    "Relational mapping without ~mainTable or sourceUrl not supported; class="
                  + rcm.className() + ", mapping=" + md.qualifiedName()
                  + ". See docs/MAPPING_LEGACY_TO_FUNCTION.md §5.2.3.");
        }
        DatabaseDefinition.ViewDefinition view = model.findView(
                rcm.mainTable().database(), rcm.mainTable().table()).orElse(null);
        if (view != null) {
            return synthViewBackedMapping(md, rcm, view, model);
        }
        return synthTableBackedMapping(md, rcm, model);
    }

    // ====================================================================
    // JSON-source mapping  —  RelationalMapping.variantIdentity parity
    // ====================================================================

    /**
     * Synth body for a JSON-backed class:
     * <pre>
     *   sourceUrl('data:application/json,...')
     *     -&gt; map(row | ^Class(
     *           propA = $row.data-&gt;get('propA', @TypeA),
     *           propB = $row.data-&gt;get('propB', @TypeB),
     *           ...))
     * </pre>
     * Property bindings are derived from the class's declared
     * properties; the class mapping itself carries no PMs (the cross-
     * bake from ModelBuilder synthesizes an empty PM list).
     */
    private static ValueSpecification synthJsonSourceMapping(LegacyMappingDefinition md,
                                                            ClassMapping.Relational rcm,
                                                            ModelBuilder model) {
        ValueSpecification source = new AppliedFunction("sourceUrl",
                List.of(new CString(rcm.sourceUrl())));
        Variable rowBind = new Variable("row");
        ClassDefinition cd = model.findClass(rcm.className()).orElseThrow(() ->
                new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, "JSON-source mapping references unknown class '"
                        + rcm.className() + "'; mapping=" + md.qualifiedName()));
        Map<String, KeyExpression> fields = new LinkedHashMap<>();
        for (ClassDefinition.PropertyDefinition prop : cd.properties()) {
            // get($row.data, 'propName') — 2-arg variant access. The only
            // `get` native is get(Variant[1], Any[1]):Variant[0..1]; the
            // single VARIANT `data` column is fanned into property values
            // by key (engine parity: MappingNormalizer.synthesizeExpressionAccess).
            ValueSpecification get = new AppliedFunction("get", List.of(
                    new AppliedProperty(rowBind, "data"), new CString(prop.name())));
            // to(get(...), @Type) — typed text-extraction + cast. Engine uses
            // `to` (not `cast`) so the Variant text access (->>) strips JSON
            // string quotes before casting to the declared property type.
            ValueSpecification value = (prop.type() instanceof TypeExpression.NameRef nr)
                    ? new AppliedFunction("to", List.of(get,
                            new TypeAnnotation.Named(new TypeExpression.NameRef(nr.name()))))
                    : get;
            fields.put(prop.name(), new KeyExpression(value));
        }
        return new AppliedFunction("map", List.of(source,
                new LambdaFunction(List.of(rowBind),
                        List.of(buildNewInstance(rcm.className(), fields)))));
    }

    // ====================================================================
    // View-backed mapping  —  doc §5.3.7
    // ====================================================================

    /**
     * Expand the view as a macro, faithful to the engine
     * ({@code PureModelBuilder.inferViewMainTable} +
     * {@code MappingNormalizer.resolvePropertyMappingsThroughView}):
     * <ol>
     *   <li>Infer the view's single underlying physical table from its
     *       non-join column expressions ({@link #inferViewMainTable}).
     *       That physical table &mdash; <em>not</em> the view name &mdash;
     *       is the pipeline's {@code tableReference} source.</li>
     *   <li>Resolve user PMs against the view's column mappings: a PM that
     *       references a view column is rewritten to the physical
     *       expression behind it (column / join-terminal / dyna); PMs
     *       unrelated to the view pass through.</li>
     *   <li>Run the rewritten PMs through the table-backed pipeline over
     *       the inferred physical table.</li>
     *   <li>View-level {@code ~filter}/{@code ~distinct}/{@code ~groupBy}
     *       are layered before user-level directives (the view filter
     *       sequences first).</li>
     * </ol>
     */
    private static ValueSpecification synthViewBackedMapping(LegacyMappingDefinition md,
                                                            ClassMapping.Relational rcm,
                                                            DatabaseDefinition.ViewDefinition view,
                                                            ModelBuilder model) {
        String mainDb = rcm.mainTable().database();
        // Engine parity: the view resolves to a single physical root table,
        // which (not the view name) is the source relation.
        String physicalTable = inferViewMainTable(view, rcm.mainTable().table(), md);
        // Resolve view column expressions: name -> RelationalOperation.
        Map<String, RelationalOperation> viewCols = new LinkedHashMap<>();
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
            viewCols.put(vc.name(), vc.expression());
        }
        // Rewrite each user PM that references a view column to the physical
        // expression behind it. PMs unrelated to the view pass through.
        List<PropertyMapping> rewrittenPms = new ArrayList<>(rcm.propertyMappings().size());
        for (PropertyMapping pm : rcm.propertyMappings()) {
            rewrittenPms.add(rewritePmThroughView(pm, viewCols, mainDb, physicalTable));
        }
        // Merge view-level directives with the mapping-level ones. View
        // filter sequences BEFORE the mapping filter; view ~distinct ORs
        // with the mapping ~distinct; view ~groupBy concatenates with the
        // mapping ~groupBy (engine semantics).
        FilterMapping mergedFilter = view.filter() != null ? view.filter() : rcm.filter();
        boolean mergedDistinct = rcm.distinct() || view.distinct();
        List<RelationalOperation> mergedGroupBy = new ArrayList<>(view.groupByColumns());
        mergedGroupBy.addAll(rcm.groupBy());
        ClassMapping.Relational effective = new ClassMapping.Relational(
                rcm.className(), rcm.setId(), rcm.extendsSetId(), rcm.root(),
                new LegacyMappingDefinition.TableReference(mainDb, physicalTable),
                mergedFilter, mergedDistinct, mergedGroupBy, rcm.primaryKey(),
                rewrittenPms, null);
        ValueSpecification body = synthTableBackedMapping(md, effective, model);
        // When BOTH a view filter and a mapping filter exist, the pipeline
        // above applied only the view filter (effective.filter). Apply the
        // mapping filter too &mdash; pre-map, after the view filter &mdash;
        // matching the engine's view-then-mapping filter sequencing.
        if (view.filter() != null && rcm.filter() != null && view.filter() != rcm.filter()) {
            body = layerMappingFilterPreMap(body, rcm, physicalTable, model, md);
        }
        return body;
    }

    /**
     * Rewrite a PM whose column reference points at a view column
     * into a PM that references the underlying physical expression.
     * Unrelated PMs pass through unchanged.
     */
    private static PropertyMapping rewritePmThroughView(PropertyMapping pm,
                                                       Map<String, RelationalOperation> viewCols,
                                                       String dbFqn, String mainTable) {
        return switch (pm) {
            case PropertyMapping.Column col when viewCols.containsKey(col.column()) ->
                    rewriteColumnPmAsViewExpr(col, viewCols.get(col.column()), dbFqn, mainTable);
            case PropertyMapping.LocalProperty lp -> new PropertyMapping.LocalProperty(
                    lp.propertyName(), lp.type(), lp.multiplicity(),
                    rewritePmThroughView(lp.body(), viewCols, dbFqn, mainTable));
            case PropertyMapping.Embedded emb -> {
                List<PropertyMapping> rewrittenSubs = new ArrayList<>();
                for (PropertyMapping sub : emb.propertyMappings()) {
                    rewrittenSubs.add(rewritePmThroughView(sub, viewCols, dbFqn, mainTable));
                }
                yield new PropertyMapping.Embedded(emb.propertyName(), rewrittenSubs);
            }
            // Non-view Column, Join, JoinTerminalColumn, EnumeratedColumn,
            // Expression, InlineEmbedded, OtherwiseEmbedded: pass through
            // unchanged (view rewriting is at column granularity).
            case PropertyMapping.Column col -> col;
            case PropertyMapping.Join j -> j;
            case PropertyMapping.JoinTerminalColumn jtc -> jtc;
            case PropertyMapping.EnumeratedColumn ec -> ec;
            case PropertyMapping.Expression expr -> expr;
            case PropertyMapping.InlineEmbedded ie -> ie;
            case PropertyMapping.OtherwiseEmbedded oe -> oe;
        };
    }

    /**
     * Rewrite a {@code prop: V.col} PM as the underlying view-column
     * expression. The expression can be a simple ColumnRef, a
     * JoinNavigation, or a complex DynaFunction; each maps to a
     * different PM kind.
     */
    private static PropertyMapping rewriteColumnPmAsViewExpr(PropertyMapping.Column col,
                                                            RelationalOperation expr,
                                                            String dbFqn,
                                                            String mainTable) {
        if (expr instanceof RelationalOperation.ColumnRef cr) {
            return new PropertyMapping.Column(col.propertyName(),
                    cr.databaseName() != null ? cr.databaseName() : dbFqn,
                    cr.table(), cr.column());
        }
        if (expr instanceof RelationalOperation.JoinNavigation jn) {
            if (jn.terminal() instanceof RelationalOperation.ColumnRef tcr) {
                return new PropertyMapping.JoinTerminalColumn(col.propertyName(),
                        jn.databaseName() != null ? jn.databaseName() : dbFqn,
                        jn.chain(), tcr);
            }
            // JoinNav with non-column terminal becomes an Expression
            // PM whose body is the JoinNav (will be hoisted).
            return new PropertyMapping.Expression(col.propertyName(), jn);
        }
        // Complex expression: lift into an Expression PM.
        return new PropertyMapping.Expression(col.propertyName(), expr);
    }

    /**
     * Apply the mapping-level {@code ~filter} as a <em>pre-map</em> filter
     * step, layered after the view filter the inner pipeline already applied.
     * The inner pipeline's terminal is {@code map(source, lambda)}; the filter
     * is injected on {@code source} so it filters table rows (engine parity),
     * not materialized class instances.
     */
    private static ValueSpecification layerMappingFilterPreMap(ValueSpecification body,
                                                              ClassMapping.Relational rcm,
                                                              String physicalTable,
                                                              ModelBuilder model,
                                                              LegacyMappingDefinition md) {
        FilterMapping fm = rcm.filter();
        if (!(fm instanceof FilterMapping.Direct direct)) {
            // A JoinMediated mapping filter over a view that already carries
            // its own filter would need the mapping-filter join chain hoisted
            // at this outer level; not wired. Refuse loudly rather than
            // silently drop the mapping filter (AGENTS.md: no fallbacks).
            throw new com.legend.error.NotImplementedException(
                    "View-backed class '" + rcm.className() + "' has both a view "
                  + "filter and a JoinMediated mapping ~filter; layering a "
                  + "JoinMediated filter over a filtered view is not supported. "
                  + "Mapping=" + md.qualifiedName());
        }
        if (!(body instanceof AppliedFunction map) || !"map".equals(map.function())
                || map.parameters().size() != 2) {
            throw new IllegalStateException(
                    "Expected a map(source, lambda) terminal for view-backed class '"
                  + rcm.className() + "'; mapping=" + md.qualifiedName());
        }
        String dbFqn = switch (direct.filter()) {
            case FilterPointer.Cross c -> c.db();
            case FilterPointer.Local l -> rcm.mainTable().database();
        };
        DatabaseDefinition.FilterDefinition fd = model.findFilter(dbFqn, direct.filter().name())
                .orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                        "~filter '" + direct.filter().name() + "' not found in db '"
                      + dbFqn + "'; class=" + rcm.className() + ", mapping="
                      + md.qualifiedName()));
        ValueSpecification source = map.parameters().get(0);
        ValueSpecification mapLambda = map.parameters().get(1);
        Variable rowBind = new Variable("row");
        Map<String, ValueSpecification> scope = Map.of(physicalTable, rowBind);
        ValueSpecification cond = translateRelOp(fd.condition(), scope, null, rowBind, null);
        ValueSpecification filtered = new AppliedFunction("filter",
                List.of(source, new LambdaFunction(List.of(rowBind), List.of(cond))));
        return new AppliedFunction("map", List.of(filtered, mapLambda));
    }

    /**
     * Infer the view's single underlying physical table by scanning its
     * non-join column expressions for {@link RelationalOperation.ColumnRef}
     * tables (engine parity: {@code PureModelBuilder.inferViewMainTable}).
     * Columns whose expression navigates a join are skipped &mdash; they
     * reference joined tables, not the view's root. Exactly one root table
     * must remain, else fail loudly.
     */
    private static String inferViewMainTable(DatabaseDefinition.ViewDefinition view,
                                            String viewName, LegacyMappingDefinition md) {
        Set<String> tables = new LinkedHashSet<>();
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
            RelationalOperation expr = vc.expression();
            List<JoinNavSpec> navs = new ArrayList<>();
            collectJoinNavigations(expr, navs);
            if (!navs.isEmpty()) continue;   // joined column — not the view's root table
            collectTablesIn(expr, tables);
        }
        if (tables.isEmpty()) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "View '" + viewName + "': cannot infer underlying main table — no "
                  + "non-join column references found; mapping=" + md.qualifiedName());
        }
        if (tables.size() > 1) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "View '" + viewName + "' references multiple root tables " + tables
                  + "; a view must resolve to a single root table. Mapping="
                  + md.qualifiedName());
        }
        return tables.iterator().next();
    }

    // ====================================================================
    // Table-backed mapping  —  the main pipeline synthesis
    // ====================================================================

    /**
     * Pipeline state: accumulated AST plus aliases for hops that have
     * been emitted. Each alias corresponds to a sub-row (clean
     * {@code join} slot) or a class-instance slot ({@code legacyNavigate}).
     */
    private static final class Pipeline {
        ValueSpecification expr;
        final Map<String, String> aliasToTargetTable = new LinkedHashMap<>();
        final Set<String> classSlots = new HashSet<>();
        // Structural identity of each emitted physical sub-row hop: the
        // ordered list of join names from the main table maps to the slot
        // name actually used in the pipeline. This is the dedup key (so a
        // chain [A, B] never collides with a single join literally named
        // "A__B") AND the lookup readers use to recover the slot name,
        // rather than re-flattening the hop list (which is lossy).
        final Map<List<String>, String> pathToSlot = new LinkedHashMap<>();
        // Physical (non-class) target tables reached by MORE THAN ONE distinct
        // sub-row slot and which are NOT the main table. A bare column ref to
        // such a table (in a filter/expression/groupBy/column PM) cannot
        // identify which sub-row is meant, so it is left unbound in the row
        // scope and a read fails loudly (see columnRead / translateRelOp)
        // instead of silently resolving to an arbitrary sub-row. Pin the
        // intended sub-row with a join-terminal column (| T.COL) instead.
        final Set<String> ambiguousTables = new HashSet<>();
        Pipeline(ValueSpecification expr) { this.expr = expr; }
    }

    private static ValueSpecification synthTableBackedMapping(LegacyMappingDefinition md,
                                                             ClassMapping.Relational rcm,
                                                             ModelBuilder model) {
        validatePmNames(rcm, model, md);

        String mainDb    = rcm.mainTable().database();
        String mainTable = rcm.mainTable().table();
        Variable rowBind = new Variable("row");

        Pipeline p = new Pipeline(new AppliedFunction("tableReference",
                List.of(new CString(mainDb), new CString(mainTable))));

        // Pass 1: structural chain emission (Join, JoinTerminalColumn,
        // LocalProperty-wrapping-JTC). Class-typed Join PMs to mapped
        // targets emit a final-hop legacyNavigate.
        for (PropertyMapping pm : rcm.propertyMappings()) {
            emitHopsForStructuralPm(p, pm, rcm.className(), mainDb, mainTable,
                    rowBind, model, md);
        }
        // Pass 2: hoist JoinNavigation chains nested inside Expression
        // bodies, LocalProperty wrappers, Embedded sub-PMs,
        // OtherwiseEmbedded eager/fallback bodies, groupBy keys, and
        // Direct filter conditions. Each unique chain becomes a clean
        // join(...) step; dedup via aliasToTargetTable.
        List<JoinNavSpec> nested = new ArrayList<>();
        collectJoinNavigationsInPms(rcm.propertyMappings(), nested);
        for (RelationalOperation key : rcm.groupBy()) {
            collectJoinNavigations(key, nested);
        }
        if (rcm.filter() instanceof FilterMapping.Direct dfilt) {
            String dbFqn = switch (dfilt.filter()) {
                case FilterPointer.Cross c -> c.db();
                case FilterPointer.Local l -> mainDb;
            };
            model.findFilter(dbFqn, dfilt.filter().name()).ifPresent(fd ->
                    collectJoinNavigations(fd.condition(), nested));
        }
        for (JoinNavSpec spec : nested) {
            emitJoinChain(p, spec.chain(), spec.chainDb(),
                    /* propName */ null, rcm.className(),
                    mainDb, mainTable, rowBind, model, md,
                    /* classTypedTerminus */ false);
        }
        // Pass 3: JoinMediated filter's join chain (extends pipeline).
        if (rcm.filter() instanceof FilterMapping.JoinMediated jm) {
            emitJoinChain(p, jm.joins(), jm.sourceDb(),
                    /* propName */ null, rcm.className(),
                    mainDb, mainTable, rowBind, model, md,
                    /* classTypedTerminus */ false);
        }

        // Apply ~filter (Direct inlined or JoinMediated chain-anchored).
        if (rcm.filter() != null) {
            p.expr = applyFilter(p.expr, rcm, rowBind, mainDb, mainTable, p, model, md);
        }

        // Apply ~groupBy (with aggregate decomposition).
        if (!rcm.groupBy().isEmpty()) {
            p.expr = applyGroupBy(p.expr, rcm, rowBind, mainTable, p, md);
        }

        // ~primaryKey is intentionally NOT lowered into the realizing
        // function. In the engine it is object-identity metadata used at
        // graph-fetch time (PK columns/getters to correlate rows to objects
        // and dedup/merge object graphs), never a row-level DISTINCT in the
        // query (HelperRelationalBuilder.processRelationalClassMapping /
        // processRelationalPrimaryKey). lite has no graph-fetch consumer yet,
        // so ~primaryKey is parsed (rcm.primaryKey()) but currently a no-op
        // here. Lowering it to a `distinct`/`distinctBy` step would diverge
        // from engine semantics; see docs/MAPPING_LEGACY_TO_FUNCTION.md §5.3.6.

        // Apply ~distinct.
        if (rcm.distinct()) {
            p.expr = new AppliedFunction("distinct", List.of(p.expr));
        }

        // Terminal: map(row | ^Class(...)).
        Map<String, ValueSpecification> tableScope = new LinkedHashMap<>();
        tableScope.put(mainTable, rowBind);
        seedAliasScope(tableScope, p, rowBind, mainTable);

        Map<String, KeyExpression> fields = new LinkedHashMap<>();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            CtorField cf = translatePmToField(pm, rowBind, tableScope, mainTable, p,
                    rcm.className(), md, model, !rcm.groupBy().isEmpty());
            fields.put(cf.name(), new KeyExpression(cf.value(), false, cf.isLocal()));
        }
        return new AppliedFunction("map", List.of(p.expr,
                new LambdaFunction(List.of(rowBind),
                        List.of(buildNewInstance(rcm.className(), fields)))));
    }

    private static void validatePmNames(ClassMapping.Relational rcm,
                                       ModelBuilder model, LegacyMappingDefinition md) {
        ClassDefinition cd = model.findClass(rcm.className()).orElse(null);
        if (cd == null) return;
        for (PropertyMapping pm : rcm.propertyMappings()) {
            if (pm instanceof PropertyMapping.LocalProperty) continue;
            // Resolve through the superclass chain: a PM may target an
            // inherited property (engine parity: property lookup walks
            // generalizations).
            if (findPropertyTypeDeep(cd, pm.propertyName(), model) == null) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                        "PropertyMapping '" + pm.propertyName() + "' references property "
                      + "not declared on class '" + rcm.className() + "'; mapping="
                      + md.qualifiedName());
            }
        }
    }

    // ====================================================================
    // Hop emission  —  Pass 1 (structural) and Pass 2 (nested JoinNav)
    // ====================================================================

    private static void emitHopsForStructuralPm(Pipeline p, PropertyMapping pm,
                                               String ownerClassFqn, String mainDb,
                                               String mainTable, Variable rowBind,
                                               ModelBuilder model, LegacyMappingDefinition md) {
        switch (pm) {
            case PropertyMapping.Join j -> emitJoinChain(p, j.joins(), j.database(),
                    j.propertyName(), ownerClassFqn, mainDb, mainTable,
                    rowBind, model, md, /*classTypedTerminus*/ true);
            case PropertyMapping.JoinTerminalColumn jtc -> emitJoinChain(p,
                    jtc.joins(), jtc.database(), /*propName*/ null, ownerClassFqn,
                    mainDb, mainTable, rowBind, model, md, /*classTypedTerminus*/ false);
            case PropertyMapping.LocalProperty lp -> emitHopsForStructuralPm(p, lp.body(),
                    ownerClassFqn, mainDb, mainTable, rowBind, model, md);
            case PropertyMapping.OtherwiseEmbedded oe -> emitOtherwiseEmbeddedHop(p, oe,
                    ownerClassFqn, mainDb, mainTable, rowBind, model, md);
            default -> { /* Column / Enum / Expression / Embedded / InlineEmbedded:
                            nested JoinNav handled in Pass 2 */ }
        }
    }

    /**
     * Emit the OtherwiseEmbedded fallback's pipeline step: a
     * {@code legacyNavigate(~<propName>: getAll(Target), {sr,tr|cond})}
     * binding the fallback class instance as a named slot. The map
     * terminal then composes {@code otherwise(^Inner(...), $row.slot)}.
     */
    private static void emitOtherwiseEmbeddedHop(Pipeline p,
                                                PropertyMapping.OtherwiseEmbedded oe,
                                                String ownerClassFqn, String mainDb,
                                                String mainTable, Variable rowBind,
                                                ModelBuilder model, LegacyMappingDefinition md) {
        if (!(oe.fallback() instanceof PropertyMapping.Join joinFallback)) {
            throw new com.legend.error.NotImplementedException(
                    "OtherwiseEmbedded PM '" + oe.propertyName() + "' fallback kind "
                  + oe.fallback().getClass().getSimpleName()
                  + " not supported (Join only). Mapping=" + md.qualifiedName());
        }
        ClassDefinition owner = model.findClass(ownerClassFqn).orElseThrow();
        TypeExpression propType = findPropertyTypeDeep(owner, oe.propertyName(), model);
        if (!(propType instanceof TypeExpression.NameRef nr)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "OtherwiseEmbedded PM '" + oe.propertyName()
                  + "' has non-class property type; mapping=" + md.qualifiedName());
        }
        String targetClassFqn = nr.name();
        if (!model.isMappedClass(targetClassFqn)) {
            throw new com.legend.error.NotImplementedException(
                    "OtherwiseEmbedded PM '" + oe.propertyName() + "' target class '"
                  + targetClassFqn + "' is not mapped; mapping=" + md.qualifiedName());
        }
        // Emit the fallback Join chain as a legacyNavigate step under
        // a slot named after the property. The terminal `map` reads
        // $row.<propName> and composes with the partial ^Inner(...).
        emitJoinChain(p, joinFallback.joins(), joinFallback.database(),
                oe.propertyName(), ownerClassFqn, mainDb, mainTable,
                rowBind, model, md, /*classTypedTerminus*/ true);
    }

    /**
     * Emit one join chain. Intermediate hops are clean-sheet
     * {@code join} steps. The final hop is a {@code legacyNavigate}
     * iff {@code classTypedTerminus} is true AND the property's
     * declared target class is mapped; otherwise the final hop is
     * also a clean {@code join} binding a physical sub-row.
     *
     * <p>Shared chain prefixes across PMs dedup via
     * {@link Pipeline#aliasToTargetTable}.
     */
    private static void emitJoinChain(Pipeline p, List<JoinChainElement> hops,
                                     String chainDb, String propName,
                                     String ownerClassFqn, String mainDb,
                                     String mainTable, Variable rowBind,
                                     ModelBuilder model, LegacyMappingDefinition md,
                                     boolean classTypedTerminus) {
        String targetClassFqn = null;
        if (classTypedTerminus && propName != null) {
            targetClassFqn = classTypedTargetIfMapped(ownerClassFqn, propName, model);
        }
        int lastIdx = hops.size() - 1;
        List<String> prefixPath = new ArrayList<>();
        String prevAlias = null;
        String prevTable = mainTable;
        for (int i = 0; i < hops.size(); i++) {
            JoinChainElement hop = hops.get(i);
            prefixPath.add(hop.joinName());
            boolean isLastHop = i == lastIdx;
            boolean emitNavigate = isLastHop && targetClassFqn != null;

            // Dedup. Physical sub-row hops are identified by their STRUCTURED
            // prefix path (the ordered join names from the main table), so a
            // chain [A, B] and a single join literally named "A__B" stay
            // distinct. Class-instance hops dedup on the property name, since
            // their slot is the property, not a flattened chain.
            List<String> pathKey = emitNavigate ? null : List.copyOf(prefixPath);
            if (emitNavigate) {
                if (p.aliasToTargetTable.containsKey(propName)) {
                    prevTable = p.aliasToTargetTable.get(propName);
                    prevAlias = propName;
                    continue;
                }
            } else {
                String existing = p.pathToSlot.get(pathKey);
                if (existing != null) {
                    prevTable = p.aliasToTargetTable.get(existing);
                    prevAlias = existing;
                    continue;
                }
            }
            // The slot name read back in generated Pure ($row.<slot>). For a
            // class hop it is the property name; for a physical hop it is the
            // human-readable "__"-joined path, disambiguated only if that name
            // would clash with a different slot (the structured pathKey, not
            // the name, is the identity readers resolve through slotFor).
            String slotAlias = emitNavigate ? propName : uniqueSlotName(p, pathKey);

            String hopDb = hop.databaseName() != null ? hop.databaseName()
                    : (chainDb != null ? chainDb : mainDb);
            DatabaseDefinition.JoinDefinition jd = model.findJoin(hopDb, hop.joinName())
                    .orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                            "Join '" + hop.joinName() + "' not found in db '"
                          + hopDb + "'; PM='" + propName + "', mapping="
                          + md.qualifiedName()));
            String targetTable = determineTargetTable(jd.operation(), prevTable,
                    hop.joinName(), propName == null ? "<nested>" : propName,
                    i + 1, md.qualifiedName());

            Variable s = new Variable("s");
            Variable t = new Variable("t");
            Map<String, ValueSpecification> condScope = new LinkedHashMap<>();
            condScope.put(prevTable, prevAlias == null
                    ? s : new AppliedProperty(s, prevAlias));
            if (!targetTable.equals(prevTable)) condScope.put(targetTable, t);
            ValueSpecification cond = translateRelOp(jd.operation(), condScope, t,
                    /*rowBind*/ null, /*pipeline*/ null);
            LambdaFunction condLambda = new LambdaFunction(List.of(s, t), List.of(cond));

            if (emitNavigate) {
                ColSpec slot = new ColSpec(slotAlias,
                        new LambdaFunction(List.of(), List.of(new AppliedFunction(
                                "getAll", List.of(new PackageableElementPtr(targetClassFqn))))),
                        null);
                p.expr = new AppliedFunction("legacyNavigate",
                        List.of(p.expr, slot, condLambda));
                p.classSlots.add(slotAlias);
            } else {
                ColSpec slot = new ColSpec(slotAlias,
                        new LambdaFunction(List.of(), List.of(new AppliedFunction(
                                "tableReference",
                                List.of(new CString(hopDb), new CString(targetTable))))),
                        null);
                p.expr = new AppliedFunction("join",
                        List.of(p.expr, slot, condLambda));
            }
            p.aliasToTargetTable.put(slotAlias, targetTable);
            if (!emitNavigate) p.pathToSlot.put(pathKey, slotAlias);
            prevTable = targetTable;
            prevAlias = slotAlias;
        }
    }

    /**
     * Pick the slot identifier for a freshly-emitted physical sub-row hop.
     * The default is the human-readable {@code "__"}-joined path. If a
     * <em>different</em> path already produced that exact name (a genuine
     * collision — e.g. chain {@code [A, B]} vs a single join named
     * {@code "A__B"}), a deterministic suffix is appended until the name is
     * free. The structured {@link Pipeline#pathToSlot} key, not this name,
     * is the dedup identity; readers recover the name via {@link #slotFor}.
     */
    private static String uniqueSlotName(Pipeline p, List<String> path) {
        String base = String.join("__", path);
        if (!p.aliasToTargetTable.containsKey(base)) return base;
        int n = 2;
        String candidate;
        do {
            candidate = base + "__" + n++;
        } while (p.aliasToTargetTable.containsKey(candidate));
        return candidate;
    }

    /** Ordered join names of a chain — the structured {@link Pipeline#pathToSlot} key. */
    private static List<String> joinPath(List<JoinChainElement> hops) {
        List<String> names = new ArrayList<>(hops.size());
        for (JoinChainElement h : hops) names.add(h.joinName());
        return names;
    }

    /**
     * Recover the pipeline slot name a physical chain was emitted under.
     * Resolves through the structured path registry (collision-proof);
     * falls back to the flattened name only if the chain was never emitted
     * (defensive — emitted chains are always registered).
     */
    private static String slotFor(Pipeline p, List<JoinChainElement> hops) {
        List<String> key = joinPath(hops);
        String slot = p.pathToSlot.get(key);
        return slot != null ? slot : String.join("__", key);
    }

    private static String classTypedTargetIfMapped(String ownerClassFqn,
                                                  String propName, ModelBuilder model) {
        ClassDefinition owner = model.findClass(ownerClassFqn).orElse(null);
        if (owner == null) return null;
        TypeExpression propType = findPropertyTypeDeep(owner, propName, model);
        if (!(propType instanceof TypeExpression.NameRef nr)) return null;
        String tgt = nr.name();
        return model.isMappedClass(tgt) ? tgt : null;
    }

    // ====================================================================
    // JoinNavigation collection (Pass 2 hoisting source)
    // ====================================================================

    private record JoinNavSpec(List<JoinChainElement> chain, String chainDb) {}

    private static void collectJoinNavigationsInPms(List<PropertyMapping> pms,
                                                   List<JoinNavSpec> out) {
        for (PropertyMapping pm : pms) {
            switch (pm) {
                case PropertyMapping.Expression expr -> collectJoinNavigations(expr.expression(), out);
                case PropertyMapping.LocalProperty lp -> collectJoinNavigationsInPms(List.of(lp.body()), out);
                case PropertyMapping.Embedded emb -> collectJoinNavigationsInPms(emb.propertyMappings(), out);
                case PropertyMapping.OtherwiseEmbedded oe ->
                        collectJoinNavigationsInPms(oe.embedded(), out);
                case PropertyMapping.JoinTerminalColumn jtc ->
                        collectJoinNavigations(jtc.terminalColumn(), out);
                // Join / Column / EnumeratedColumn / InlineEmbedded:
                // Join handled by Pass 1; the others don't carry JoinNav.
                case PropertyMapping.Join ignored -> { }
                case PropertyMapping.Column ignored -> { }
                case PropertyMapping.EnumeratedColumn ignored -> { }
                case PropertyMapping.InlineEmbedded ignored -> { }
            }
        }
    }

    private static void collectJoinNavigations(RelationalOperation op,
                                              List<JoinNavSpec> out) {
        switch (op) {
            case RelationalOperation.JoinNavigation jn -> {
                out.add(new JoinNavSpec(jn.chain(), jn.databaseName()));
                if (jn.terminal() != null) collectJoinNavigations(jn.terminal(), out);
            }
            case RelationalOperation.FunctionCall fc ->
                    fc.args().forEach(a -> collectJoinNavigations(a, out));
            case RelationalOperation.Comparison cmp -> {
                collectJoinNavigations(cmp.left(), out);
                collectJoinNavigations(cmp.right(), out);
            }
            case RelationalOperation.BooleanOp bo -> {
                collectJoinNavigations(bo.left(), out);
                collectJoinNavigations(bo.right(), out);
            }
            case RelationalOperation.IsNull n   -> collectJoinNavigations(n.operand(), out);
            case RelationalOperation.IsNotNull n -> collectJoinNavigations(n.operand(), out);
            case RelationalOperation.Group g    -> collectJoinNavigations(g.inner(), out);
            case RelationalOperation.ArrayLiteral a ->
                    a.elements().forEach(e -> collectJoinNavigations(e, out));
            case RelationalOperation.ColumnRef ignored -> { }
            case RelationalOperation.TargetColumnRef ignored -> { }
            case RelationalOperation.Literal ignored -> { }
        }
    }

    // ====================================================================
    // PropertyMapping → constructor field (terminal projection)
    // ====================================================================

    private record CtorField(String name, ValueSpecification value, boolean isLocal) {}

    private static CtorField translatePmToField(PropertyMapping pm, Variable rowBind,
                                               Map<String, ValueSpecification> tableScope,
                                               String defaultTable, Pipeline pipeline,
                                               String ownerClassFqn, LegacyMappingDefinition md,
                                               ModelBuilder model,
                                               boolean underGroupBy) {
        // Under ~groupBy, every PM (key-matching or aggregate) reads
        // from the post-groupBy row by its own property name (the
        // groupBy/agg ColSpec was named after the PM).
        if (underGroupBy) {
            return new CtorField(pm.propertyName(),
                    new AppliedProperty(rowBind, pm.propertyName()), false);
        }
        return switch (pm) {
            case PropertyMapping.Column col -> new CtorField(col.propertyName(),
                    columnRead(col.table(), col.column(), tableScope, defaultTable, pipeline),
                    false);
            case PropertyMapping.EnumeratedColumn ec -> new CtorField(ec.propertyName(),
                    translateEnumeratedColumn(ec, tableScope, defaultTable, md, pipeline),
                    false);
            case PropertyMapping.Expression expr -> new CtorField(expr.propertyName(),
                    translateRelOp(expr.expression(), tableScope, null, rowBind, pipeline),
                    false);
            case PropertyMapping.Join j -> {
                String targetIfMapped = classTypedTargetIfMapped(ownerClassFqn,
                        j.propertyName(), model);
                String slot = targetIfMapped != null
                        ? j.propertyName()
                        : slotFor(pipeline, j.joins());
                yield new CtorField(j.propertyName(),
                        new AppliedProperty(rowBind, slot), false);
            }
            case PropertyMapping.JoinTerminalColumn jtc -> {
                String alias = slotFor(pipeline, jtc.joins());
                ValueSpecification subRow = new AppliedProperty(rowBind, alias);
                Map<String, ValueSpecification> scope = new LinkedHashMap<>(tableScope);
                String terminalTable = pipeline.aliasToTargetTable.get(alias);
                if (terminalTable != null) scope.put(terminalTable, subRow);
                yield new CtorField(jtc.propertyName(),
                        translateRelOp(jtc.terminalColumn(), scope, null, rowBind, pipeline),
                        false);
            }
            case PropertyMapping.LocalProperty lp -> {
                CtorField inner = translatePmToField(lp.body(), rowBind, tableScope,
                        defaultTable, pipeline, ownerClassFqn, md, model, false);
                yield new CtorField(lp.propertyName(), inner.value(), true);
            }
            case PropertyMapping.Embedded emb -> new CtorField(emb.propertyName(),
                    materializeEmbedded(emb.propertyName(), emb.propertyMappings(),
                            rowBind, tableScope, defaultTable, pipeline,
                            ownerClassFqn, md, model, new HashSet<>()),
                    false);
            case PropertyMapping.InlineEmbedded ie -> new CtorField(ie.propertyName(),
                    materializeInlineEmbedded(ie, rowBind, tableScope, defaultTable,
                            pipeline, ownerClassFqn, md, model),
                    false);
            case PropertyMapping.OtherwiseEmbedded oe -> new CtorField(oe.propertyName(),
                    materializeOtherwiseEmbedded(oe, rowBind, tableScope, defaultTable,
                            pipeline, ownerClassFqn, md, model),
                    false);
        };
    }

    // ====================================================================
    // Embedded materialization  —  doc §5.4.7
    // ====================================================================

    private static ValueSpecification materializeEmbedded(
            String propName, List<PropertyMapping> subPms, Variable rowBind,
            Map<String, ValueSpecification> tableScope, String defaultTable,
            Pipeline pipeline, String ownerClassFqn, LegacyMappingDefinition md,
            ModelBuilder model, Set<String> cycleStack) {
        ClassDefinition owner = model.findClass(ownerClassFqn).orElse(null);
        if (owner == null) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "Embedded PM '" + propName + "' on '" + ownerClassFqn
                  + "' but owner class unknown; mapping=" + md.qualifiedName());
        }
        TypeExpression propType = findPropertyTypeDeep(owner, propName, model);
        if (!(propType instanceof TypeExpression.NameRef nr)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "Embedded PM '" + propName + "' on '" + ownerClassFqn
                  + "' has non-class property type; mapping=" + md.qualifiedName());
        }
        String innerFqn = nr.name();
        if (!cycleStack.add(innerFqn)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "Cycle materializing Embedded; class " + innerFqn
                  + " recurses via '" + propName + "' on '" + ownerClassFqn + "'");
        }
        try {
            Map<String, KeyExpression> fields = new LinkedHashMap<>();
            for (PropertyMapping sub : subPms) {
                // Class-typed Join sub-PMs inside a value-position Embedded
                // would require hoisting a navigate step keyed to the
                // embedded slot — not wired for the embedded context. Reject
                // with a clear diagnostic rather than silently reading an
                // unemitted slot.
                if (sub instanceof PropertyMapping.Join) {
                    throw new com.legend.error.NotImplementedException(
                            "Embedded sub-PM '" + sub.propertyName() + "' on '"
                          + propName + "' is a class-typed Join; nested Join PMs "
                          + "inside a value-position Embedded are not supported "
                          + "(would need a hoisted navigate keyed to the embedded "
                          + "slot). Mapping=" + md.qualifiedName());
                }
                CtorField cf = translatePmToField(sub, rowBind, tableScope,
                        defaultTable, pipeline, innerFqn, md, model, false);
                fields.put(cf.name(),
                        new KeyExpression(cf.value(), false, cf.isLocal()));
            }
            return buildNewInstance(innerFqn, fields);
        } finally {
            cycleStack.remove(innerFqn);
        }
    }

    // ====================================================================
    // OtherwiseEmbedded materialization  —  doc §5.4.9
    //
    // The pipeline step (legacyNavigate binding the fallback slot) was
    // emitted in Pass 1 via emitOtherwiseEmbeddedHop. Here we build the
    // ctor field that composes ^Inner(<embedded subs>) with the slot.
    // ====================================================================

    private static ValueSpecification materializeOtherwiseEmbedded(
            PropertyMapping.OtherwiseEmbedded oe, Variable rowBind,
            Map<String, ValueSpecification> tableScope, String defaultTable,
            Pipeline pipeline, String ownerClassFqn, LegacyMappingDefinition md,
            ModelBuilder model) {
        ValueSpecification partial = materializeEmbedded(oe.propertyName(),
                oe.embedded(), rowBind, tableScope, defaultTable, pipeline,
                ownerClassFqn, md, model, new HashSet<>());
        ValueSpecification fallback = new AppliedProperty(rowBind, oe.propertyName());
        return new AppliedFunction("otherwise", List.of(partial, fallback));
    }

    // ====================================================================
    // InlineEmbedded materialization  —  doc §5.4.8
    //
    // Splice the referenced class mapping's PMs as embedded sub-mappings
    // of the current class. The referenced ClassMapping.Relational is
    // located by setId within the enclosing LegacyMappingDefinition.
    // ====================================================================

    private static ValueSpecification materializeInlineEmbedded(
            PropertyMapping.InlineEmbedded ie, Variable rowBind,
            Map<String, ValueSpecification> tableScope, String defaultTable,
            Pipeline pipeline, String ownerClassFqn, LegacyMappingDefinition md,
            ModelBuilder model) {
        ClassMapping.Relational referenced = null;
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Relational rcm
                    && Objects.equals(rcm.setId(), ie.setId())) {
                referenced = rcm;
                break;
            }
        }
        if (referenced == null) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "InlineEmbedded PM '" + ie.propertyName()
                  + "' references unknown setId '" + ie.setId()
                  + "' in mapping=" + md.qualifiedName());
        }
        return materializeEmbedded(ie.propertyName(),
                referenced.propertyMappings(), rowBind, tableScope, defaultTable,
                pipeline, ownerClassFqn, md, model, new HashSet<>());
    }

    // ====================================================================
    // ~filter (Direct + JoinMediated)  —  doc §5.3.2, §5.3.3
    // ====================================================================

    private static ValueSpecification applyFilter(ValueSpecification source,
                                                 ClassMapping.Relational rcm,
                                                 Variable rowBind, String mainDb,
                                                 String mainTable, Pipeline p,
                                                 ModelBuilder model, LegacyMappingDefinition md) {
        return switch (rcm.filter()) {
            case FilterMapping.Direct direct ->
                    applyDirectFilter(source, rcm, rowBind, mainDb, mainTable, p,
                            model, direct, md);
            case FilterMapping.JoinMediated jm ->
                    applyJoinMediatedFilter(source, rcm, rowBind, mainDb, mainTable,
                            p, model, jm, md);
        };
    }

    private static ValueSpecification applyDirectFilter(ValueSpecification source,
                                                       ClassMapping.Relational rcm,
                                                       Variable rowBind, String mainDb,
                                                       String mainTable, Pipeline p,
                                                       ModelBuilder model,
                                                       FilterMapping.Direct direct,
                                                       LegacyMappingDefinition md) {
        String dbFqn = switch (direct.filter()) {
            case FilterPointer.Cross c -> c.db();
            case FilterPointer.Local l -> mainDb;
        };
        DatabaseDefinition.FilterDefinition fd = model.findFilter(
                dbFqn, direct.filter().name()).orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                "~filter '" + direct.filter().name() + "' not found in db '"
              + dbFqn + "'; class=" + rcm.className() + ", mapping="
              + md.qualifiedName()));
        Map<String, ValueSpecification> scope = new LinkedHashMap<>();
        scope.put(mainTable, rowBind);
        seedAliasScope(scope, p, rowBind, mainTable);
        ValueSpecification cond = translateRelOp(fd.condition(), scope, null, rowBind, p);
        return new AppliedFunction("filter", List.of(source,
                new LambdaFunction(List.of(rowBind), List.of(cond))));
    }

    private static ValueSpecification applyJoinMediatedFilter(ValueSpecification source,
                                                             ClassMapping.Relational rcm,
                                                             Variable rowBind, String mainDb,
                                                             String mainTable, Pipeline p,
                                                             ModelBuilder model,
                                                             FilterMapping.JoinMediated jm,
                                                             LegacyMappingDefinition md) {
        String dbFqn = switch (jm.filter()) {
            case FilterPointer.Cross c -> c.db();
            case FilterPointer.Local l -> jm.sourceDb();
        };
        DatabaseDefinition.FilterDefinition fd = model.findFilter(
                dbFqn, jm.filter().name()).orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                "~filter '" + jm.filter().name() + "' not found in db '"
              + dbFqn + "'; class=" + rcm.className() + ", mapping="
              + md.qualifiedName()));
        // The chain was emitted as join hops in Pass 3 of
        // synthTableBackedMapping. The terminal-table row path is at
        // $row.<terminalAlias>. Build scope that maps the filter's
        // condition tables (typically the chain's terminal table) to
        // the appropriate row path.
        String terminalAlias = slotFor(p, jm.joins());
        ValueSpecification terminalRow = new AppliedProperty(rowBind, terminalAlias);
        Map<String, ValueSpecification> scope = new LinkedHashMap<>();
        scope.put(mainTable, rowBind);
        seedAliasScope(scope, p, rowBind, mainTable);
        String terminalTable = p.aliasToTargetTable.get(terminalAlias);
        if (terminalTable != null) scope.putIfAbsent(terminalTable, terminalRow);
        ValueSpecification cond = translateRelOp(fd.condition(), scope, terminalRow,
                rowBind, p);
        return new AppliedFunction("filter", List.of(source,
                new LambdaFunction(List.of(rowBind), List.of(cond))));
    }

    // ====================================================================
    // ~groupBy with aggregate fn1/fn2 decomposition  —  doc §5.3.5
    // ====================================================================

    private static ValueSpecification applyGroupBy(ValueSpecification source,
                                                  ClassMapping.Relational rcm,
                                                  Variable rowBind, String mainTable,
                                                  Pipeline p, LegacyMappingDefinition md) {
        Map<String, ValueSpecification> scope = new LinkedHashMap<>();
        scope.put(mainTable, rowBind);
        seedAliasScope(scope, p, rowBind, mainTable);

        List<RelationalOperation> keyOps = rcm.groupBy();
        String[] keyNames = new String[keyOps.size()];
        for (int i = 0; i < keyOps.size(); i++) {
            String base = keyBaseName(keyOps.get(i));
            keyNames[i] = base != null ? "k" + i + "__" + base : "k" + i;
        }
        // Walk PMs once: align non-agg PMs to keys by structural
        // equality, collect aggregate PMs, reject orphan formulas.
        Set<Integer> claimedKeys = new HashSet<>();
        List<PropertyMapping> aggPms = new ArrayList<>();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            if (isAggregatePm(pm)) { aggPms.add(pm); continue; }
            RelationalOperation pmOp = pmAsRelationalOp(pm);
            if (pmOp == null) {
                throw new com.legend.error.NotImplementedException(
                        "PropertyMapping '" + pm.getClass().getSimpleName()
                      + "' for property '" + pm.propertyName()
                      + "' is not supported under ~groupBy (only Column, Expression, "
                      + "JoinTerminalColumn, and aggregate Expression PMs are allowed). "
                      + "Mapping=" + md.qualifiedName());
            }
            int matchIdx = -1;
            for (int i = 0; i < keyOps.size(); i++) {
                if (!claimedKeys.contains(i) && Objects.equals(pmOp, keyOps.get(i))) {
                    matchIdx = i; break;
                }
            }
            if (matchIdx < 0) {
                throw new com.legend.error.NotImplementedException(
                        "PM '" + pm.propertyName() + "' is a per-row expression that is "
                      + "neither an aggregate nor a declared ~groupBy key; ~groupBy "
                      + "mappings forbid per-row formulas outside the key list. Mapping="
                      + md.qualifiedName());
            }
            claimedKeys.add(matchIdx);
            keyNames[matchIdx] = pm.propertyName();
        }
        // Build key ColSpecs.
        List<ColSpec> keyCols = new ArrayList<>(keyOps.size());
        for (int i = 0; i < keyOps.size(); i++) {
            ValueSpecification keyValue = translateRelOp(keyOps.get(i), scope, null,
                    rowBind, p);
            keyCols.add(new ColSpec(keyNames[i],
                    new LambdaFunction(List.of(rowBind), List.of(keyValue)), null));
        }
        // Build aggregate ColSpecs with fn1 (selector) + fn2 (aggregate).
        List<ColSpec> aggCols = new ArrayList<>(aggPms.size());
        for (PropertyMapping pm : aggPms) {
            RelationalOperation.FunctionCall fc = (RelationalOperation.FunctionCall)
                    ((PropertyMapping.Expression) pm).expression();
            if (fc.args().size() != 1) {
                throw new com.legend.error.NotImplementedException(
                        "Aggregate PM '" + pm.propertyName() + "' uses '" + fc.name()
                      + "' with " + fc.args().size() + " args; only single-argument "
                      + "aggregates lift to the two-stage AggColSpec form. Mapping="
                      + md.qualifiedName());
            }
            ValueSpecification selector = translateRelOp(fc.args().get(0), scope, null,
                    rowBind, p);
            Variable vals = new Variable("vals");
            ValueSpecification aggBody = new AppliedFunction(
                    fc.name(), List.of(vals));
            aggCols.add(new ColSpec(pm.propertyName(),
                    new LambdaFunction(List.of(rowBind), List.of(selector)),
                    new LambdaFunction(List.of(vals), List.of(aggBody))));
        }
        return new AppliedFunction("groupBy", List.of(source,
                new ColSpecArray(keyCols), new ColSpecArray(aggCols)));
    }

    private static boolean isAggregatePm(PropertyMapping pm) {
        if (pm instanceof PropertyMapping.Expression expr
                && expr.expression() instanceof RelationalOperation.FunctionCall fc) {
            return AGGREGATE_FNS.contains(fc.name());
        }
        return false;
    }

    private static RelationalOperation pmAsRelationalOp(PropertyMapping pm) {
        if (pm instanceof PropertyMapping.Column col) {
            return new RelationalOperation.ColumnRef(col.database(), col.table(), col.column());
        }
        if (pm instanceof PropertyMapping.Expression expr) {
            return expr.expression();
        }
        if (pm instanceof PropertyMapping.JoinTerminalColumn jtc) {
            return new RelationalOperation.JoinNavigation(jtc.database(),
                    jtc.joins(), jtc.terminalColumn());
        }
        return null;
    }

    private static String keyBaseName(RelationalOperation op) {
        if (op instanceof RelationalOperation.ColumnRef cr) return cr.column();
        if (op instanceof RelationalOperation.TargetColumnRef tr) return tr.column();
        return null;
    }

    // ====================================================================
    // AssociationMapping → predicate function  —  doc §5.6.1
    // ====================================================================

    private static FunctionDefinition synthesizeAssociationMapping(LegacyMappingDefinition md,
                                                                  AssociationMapping am,
                                                                  ModelBuilder model) {
        if (!(am instanceof AssociationMapping.Relational rel)) {
            throw new com.legend.error.NotImplementedException(
                    "Association mapping kind " + am.getClass().getSimpleName()
                  + " not supported; mapping=" + md.qualifiedName());
        }
        AssociationDefinition ad = model.findAssociation(am.associationName())
                .orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                        "AssociationMapping references unknown association '"
                      + am.associationName() + "'; mapping=" + md.qualifiedName()));
        String classA = associationEndClass(ad.property1().targetClass(),
                "association '" + am.associationName() + "' end1");
        String classB = associationEndClass(ad.property2().targetClass(),
                "association '" + am.associationName() + "' end2");

        if (rel.propertyMappings().isEmpty()) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "AssociationMapping for '" + am.associationName()
                  + "' has no property mappings; mapping=" + md.qualifiedName());
        }
        // Pick the FIRST property mapping as the primary; multi-PM
        // disambiguation by [srcSetId, tgtSetId] could differentiate
        // direction-specific joins but the predicate condition is the
        // same regardless (it describes the association in one place).
        AssociationPropertyMapping firstAm = rel.propertyMappings().get(0);
        if (!(firstAm.body() instanceof PropertyMapping.Join firstJoin)) {
            throw new com.legend.error.NotImplementedException(
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

        Variable srcRow = new Variable("srcRow");
        Variable tgtRow = new Variable("tgtRow");
        ValueSpecification predicateBody = buildAssocPredicateBody(firstJoin, classA,
                classB, srcRow, tgtRow, am.associationName(), md, model);

        Variable a = new Variable("a");
        Variable b = new Variable("b");
        ValueSpecification body = new AppliedFunction("legacyAssocPredicate", List.of(
                a, b, new LambdaFunction(List.of(srcRow, tgtRow),
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
                        SynthHat.ASSOC, md.qualifiedName(), am.associationName()));
    }

    /**
     * Build the predicate body for an AssociationMapping. Single-hop
     * joins translate to the join condition directly over (srcRow,
     * tgtRow). Multi-hop joins chain conditions through intermediate
     * row bindings: each hop's predicate is conjoined with the next
     * via {@code and(...)}, with intermediate rows resolved by named
     * binding through the chain alias scope.
     */
    private static ValueSpecification buildAssocPredicateBody(PropertyMapping.Join join,
                                                             String classA, String classB,
                                                             Variable srcRow, Variable tgtRow,
                                                             String associationName,
                                                             LegacyMappingDefinition md,
                                                             ModelBuilder model) {
        if (join.joins().isEmpty()) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "AssociationMapping for '" + associationName
                  + "' has empty join chain; mapping=" + md.qualifiedName());
        }
        String sourceTable = mainTableOf(md, classA);
        if (join.joins().size() == 1) {
            JoinChainElement hop = join.joins().get(0);
            String hopDb = hop.databaseName() != null ? hop.databaseName() : join.database();
            DatabaseDefinition.JoinDefinition jd = model.findJoin(hopDb, hop.joinName())
                    .orElseThrow(() -> new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                            "AssociationMapping join '" + hop.joinName()
                          + "' not found in db '" + hopDb + "'; association='"
                          + associationName + "', mapping=" + md.qualifiedName()));
            String targetTable = determineTargetTable(jd.operation(), sourceTable,
                    hop.joinName(), associationName, 1, md.qualifiedName());
            Map<String, ValueSpecification> condScope = new LinkedHashMap<>();
            condScope.put(sourceTable, srcRow);
            if (!targetTable.equals(sourceTable)) condScope.put(targetTable, tgtRow);
            return translateRelOp(jd.operation(), condScope, tgtRow, /*rowBind*/ null, /*pipeline*/ null);
        }
        // Unreachable: multi-hop associations are intercepted in
        // synthesizeAssociationMapping (returns null) and realized as per-end
        // navigation injected into the class realizing functions (Option A;
        // see docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b). A (A,B)->Boolean
        // predicate cannot bind the intermediate row(s). This guard fires only
        // if that interception is bypassed — a compiler invariant violation.
        throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                "Multi-hop AssociationMapping for '" + associationName + "' ("
              + join.joins().size() + " join hops) reached the predicate "
              + "builder; it should have been handled by per-end injection. "
              + "Mapping=" + md.qualifiedName());
    }

    private static String associationEndClass(TypeExpression t, String context) {
        if (t instanceof TypeExpression.NameRef nr) return nr.name();
        throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                context + " has non-NameRef target class type: "
              + t.getClass().getSimpleName());
    }

    private static String mainTableOf(LegacyMappingDefinition md, String classFqn) {
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Relational rcm
                    && classFqn.equals(rcm.className())
                    && rcm.mainTable() != null) {
                return rcm.mainTable().table();
            }
        }
        throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                "No ~mainTable for class '" + classFqn + "' in mapping="
              + md.qualifiedName() + " (required to synthesize AssociationMapping)");
    }

    // ====================================================================
    // EnumeratedColumn  —  doc §5.4.2
    // ====================================================================

    private static ValueSpecification translateEnumeratedColumn(
            PropertyMapping.EnumeratedColumn ec,
            Map<String, ValueSpecification> tableScope,
            String defaultTable, LegacyMappingDefinition md, Pipeline p) {
        EnumerationMapping em = null;
        for (EnumerationMapping cand : md.enumerationMappings()) {
            if (cand.mappingId().equals(ec.enumMappingId())) { em = cand; break; }
        }
        if (em == null) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "EnumeratedColumn '" + ec.propertyName() + "' references unknown "
                  + "enum mapping '" + ec.enumMappingId() + "'; mapping="
                  + md.qualifiedName());
        }
        ValueSpecification colRead = columnRead(ec.table(), ec.column(),
                tableScope, defaultTable, p);
        ValueSpecification tail = new PureCollection(List.of());
        List<EnumerationMapping.EnumValueMapping> values = em.valueMappings();
        for (int i = values.size() - 1; i >= 0; i--) {
            EnumerationMapping.EnumValueMapping ev = values.get(i);
            ValueSpecification disj = null;
            for (EnumerationMapping.SourceValue sv : ev.sourceValues()) {
                ValueSpecification srcLit;
                if (sv instanceof EnumerationMapping.SourceValue.StringValue s) {
                    srcLit = new CString(s.value());
                } else if (sv instanceof EnumerationMapping.SourceValue.IntegerValue i2) {
                    srcLit = new CInteger(i2.value());
                } else if (sv instanceof EnumerationMapping.SourceValue.EnumRef er) {
                    srcLit = new EnumValue(er.enumPath(), er.enumValueName());
                } else {
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, "Unhandled SourceValue: " + sv);
                }
                ValueSpecification eq = new AppliedFunction("equal",
                        List.of(colRead, srcLit));
                disj = disj == null ? eq
                        : new AppliedFunction("or", List.of(disj, eq));
            }
            if (disj == null) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                        "EnumerationMapping '" + ec.enumMappingId() + "' value '"
                      + ev.enumValue() + "' declares no source values; cannot build "
                      + "a match condition for property '" + ec.propertyName()
                      + "'. Mapping=" + md.qualifiedName());
            }
            ValueSpecification then = new EnumValue(em.enumName(), ev.enumValue());
            tail = new AppliedFunction("if", List.of(disj,
                    new LambdaFunction(List.of(), List.of(then)),
                    new LambdaFunction(List.of(), List.of(tail))));
        }
        return tail;
    }

    // ====================================================================
    // Low-level helpers
    // ====================================================================

    private static void seedAliasScope(Map<String, ValueSpecification> scope,
                                      Pipeline p, Variable rowBind, String mainTable) {
        // Count physical (non-class) sub-rows per target table.
        Map<String, Integer> perTable = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : p.aliasToTargetTable.entrySet()) {
            if (p.classSlots.contains(e.getKey())) continue;
            perTable.merge(e.getValue(), 1, Integer::sum);
        }
        for (Map.Entry<String, String> e : p.aliasToTargetTable.entrySet()) {
            if (p.classSlots.contains(e.getKey())) continue;
            String table = e.getValue();
            // A NON-main table reached by more than one physical sub-row is
            // ambiguous for bare column refs: leave it unbound and record it
            // so reads fail loudly rather than picking an arbitrary sub-row.
            // (The main table is exempt: a bare ref means the top row by
            // convention; its sub-rows are reached via their own slots.)
            if (!table.equals(mainTable) && perTable.get(table) > 1) {
                p.ambiguousTables.add(table);
                continue;
            }
            scope.putIfAbsent(table, new AppliedProperty(rowBind, e.getKey()));
        }
    }

    private static String determineTargetTable(RelationalOperation cond, String sourceTable,
                                              String joinName, String ownerLabel,
                                              int hopIndex, String mappingFqn) {
        if (containsTargetColumnRef(cond)) return sourceTable;
        Set<String> tables = new LinkedHashSet<>();
        collectTablesIn(cond, tables);
        tables.remove(sourceTable);
        if (tables.size() == 1) return tables.iterator().next();
        if (tables.isEmpty()) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "Join '" + joinName + "' references no table other than source '"
                  + sourceTable + "' and has no {target} marker; owner=" + ownerLabel
                  + ", hop " + hopIndex + ", mapping=" + mappingFqn);
        }
        throw new com.legend.error.NotImplementedException(
                "Join '" + joinName + "' references multiple non-source tables "
              + tables + "; multi-table joins not supported. owner=" + ownerLabel
              + ", hop " + hopIndex + ", mapping=" + mappingFqn);
    }

    private static boolean containsTargetColumnRef(RelationalOperation op) {
        return switch (op) {
            case RelationalOperation.TargetColumnRef ignored -> true;
            case RelationalOperation.ColumnRef ignored       -> false;
            case RelationalOperation.Literal ignored         -> false;
            case RelationalOperation.FunctionCall fc         ->
                    fc.args().stream().anyMatch(MappingNormalizer::containsTargetColumnRef);
            case RelationalOperation.Comparison c            ->
                    containsTargetColumnRef(c.left()) || containsTargetColumnRef(c.right());
            case RelationalOperation.BooleanOp b             ->
                    containsTargetColumnRef(b.left()) || containsTargetColumnRef(b.right());
            case RelationalOperation.IsNull n                -> containsTargetColumnRef(n.operand());
            case RelationalOperation.IsNotNull n             -> containsTargetColumnRef(n.operand());
            case RelationalOperation.Group g                 -> containsTargetColumnRef(g.inner());
            case RelationalOperation.ArrayLiteral a          ->
                    a.elements().stream().anyMatch(MappingNormalizer::containsTargetColumnRef);
            case RelationalOperation.JoinNavigation ignored -> throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "JoinNavigation inside join condition");
        };
    }

    private static void collectTablesIn(RelationalOperation op, Set<String> sink) {
        switch (op) {
            case RelationalOperation.ColumnRef cr            -> sink.add(cr.table());
            case RelationalOperation.TargetColumnRef ignored -> { }
            case RelationalOperation.Literal ignored         -> { }
            case RelationalOperation.FunctionCall fc         -> fc.args().forEach(a -> collectTablesIn(a, sink));
            case RelationalOperation.Comparison c            -> { collectTablesIn(c.left(), sink); collectTablesIn(c.right(), sink); }
            case RelationalOperation.BooleanOp b             -> { collectTablesIn(b.left(), sink); collectTablesIn(b.right(), sink); }
            case RelationalOperation.IsNull n                -> collectTablesIn(n.operand(), sink);
            case RelationalOperation.IsNotNull n             -> collectTablesIn(n.operand(), sink);
            case RelationalOperation.Group g                 -> collectTablesIn(g.inner(), sink);
            case RelationalOperation.ArrayLiteral a          -> a.elements().forEach(e -> collectTablesIn(e, sink));
            case RelationalOperation.JoinNavigation ignored  -> throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "JoinNavigation inside expression");
        }
    }

    private static ValueSpecification columnRead(String table, String column,
                                                Map<String, ValueSpecification> tableScope,
                                                String defaultTable, Pipeline p) {
        ValueSpecification base = tableScope.get(table);
        if (base == null && p != null && p.ambiguousTables.contains(table)) {
            throw ambiguousTableRef(table, column);
        }
        if (base == null) base = tableScope.get(defaultTable);
        if (base == null) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                    "No row variable in scope for table '" + table
                  + "'; available=" + tableScope.keySet());
        }
        return new AppliedProperty(base, column);
    }

    /**
     * A bare column reference to a table the join chain reaches through more
     * than one path is irreducibly ambiguous: the legacy DSL addresses by
     * table name, which cannot pick between two sub-rows of the same table.
     * Fail loudly with guidance rather than resolve to an arbitrary sub-row.
     */
    private static com.legend.error.ModelException ambiguousTableRef(String table, String column) {
        return new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                "Ambiguous column reference '" + table + "." + column + "': the join "
              + "chain reaches table '" + table + "' through more than one path, so a "
              + "bare column reference cannot identify which sub-row is meant. Pin the "
              + "intended sub-row with a join-terminal column (e.g. @SomeJoin | "
              + table + "." + column + ").");
    }

    /**
     * Translate a {@link RelationalOperation} into a Pure value
     * expression. Nested {@link RelationalOperation.JoinNavigation}
     * nodes resolve via the hoisted prelude: each chain has been
     * emitted as a clean {@code join(~alias, ...)} step in Pass 2, so
     * its sub-row is reachable as {@code $row.<alias>}, and the
     * terminal column (if any) reads from that sub-row.
     */
    private static ValueSpecification translateRelOp(RelationalOperation op,
                                                    Map<String, ValueSpecification> tableScope,
                                                    ValueSpecification targetVarOrNull,
                                                    Variable rowBindOrNull,
                                                    Pipeline pipelineOrNull) {
        return switch (op) {
            case RelationalOperation.ColumnRef ref -> {
                ValueSpecification path = tableScope.get(ref.table());
                if (path == null && pipelineOrNull != null
                        && pipelineOrNull.ambiguousTables.contains(ref.table())) {
                    throw ambiguousTableRef(ref.table(), ref.column());
                }
                if (path == null) {
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                            "ColumnRef references table '" + ref.table()
                          + "' not in scope; available=" + tableScope.keySet());
                }
                yield new AppliedProperty(path, ref.column());
            }
            case RelationalOperation.TargetColumnRef tref -> {
                if (targetVarOrNull == null) {
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                            "TargetColumnRef {target}." + tref.column()
                          + " outside a join condition context");
                }
                yield new AppliedProperty(targetVarOrNull, tref.column());
            }
            case RelationalOperation.Literal lit -> literalToValueSpec(lit.value());
            case RelationalOperation.FunctionCall call -> new AppliedFunction(
                    call.name(),
                    call.args().stream()
                            .map(a -> translateRelOp(a, tableScope, targetVarOrNull,
                                    rowBindOrNull, pipelineOrNull))
                            .toList());
            case RelationalOperation.Comparison cmp -> new AppliedFunction(
                    comparisonFn(cmp.op()),
                    List.of(translateRelOp(cmp.left(),  tableScope, targetVarOrNull,
                                    rowBindOrNull, pipelineOrNull),
                            translateRelOp(cmp.right(), tableScope, targetVarOrNull,
                                    rowBindOrNull, pipelineOrNull)));
            case RelationalOperation.BooleanOp bo -> new AppliedFunction(
                    bo.op() == LogicalOp.AND ? "and" : "or",
                    List.of(translateRelOp(bo.left(),  tableScope, targetVarOrNull,
                                    rowBindOrNull, pipelineOrNull),
                            translateRelOp(bo.right(), tableScope, targetVarOrNull,
                                    rowBindOrNull, pipelineOrNull)));
            case RelationalOperation.IsNull n -> new AppliedFunction("isNull",
                    List.of(translateRelOp(n.operand(), tableScope, targetVarOrNull,
                            rowBindOrNull, pipelineOrNull)));
            case RelationalOperation.IsNotNull n -> new AppliedFunction("isNotNull",
                    List.of(translateRelOp(n.operand(), tableScope, targetVarOrNull,
                            rowBindOrNull, pipelineOrNull)));
            case RelationalOperation.Group g ->
                    translateRelOp(g.inner(), tableScope, targetVarOrNull,
                            rowBindOrNull, pipelineOrNull);
            case RelationalOperation.ArrayLiteral arr -> new PureCollection(
                    arr.elements().stream()
                            .map(e -> translateRelOp(e, tableScope, targetVarOrNull,
                                    rowBindOrNull, pipelineOrNull))
                            .toList());
            case RelationalOperation.JoinNavigation jn -> {
                // The chain has been hoisted into the pipeline as a
                // join(~alias, ...) step. Its sub-row is $row.<alias>;
                // the terminal (if any) reads from that sub-row's
                // table scope.
                if (rowBindOrNull == null || pipelineOrNull == null) {
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, 
                            "Nested JoinNavigation in scope without pipeline; "
                          + "JoinNav inside association predicates or join "
                          + "conditions is not supported.");
                }
                String alias = slotFor(pipelineOrNull, jn.chain());
                ValueSpecification subRow = new AppliedProperty(rowBindOrNull, alias);
                if (jn.terminal() == null) yield subRow;
                String terminalTable = pipelineOrNull.aliasToTargetTable.get(alias);
                Map<String, ValueSpecification> innerScope = new LinkedHashMap<>(tableScope);
                if (terminalTable != null) innerScope.put(terminalTable, subRow);
                yield translateRelOp(jn.terminal(), innerScope, targetVarOrNull,
                        rowBindOrNull, pipelineOrNull);
            }
        };
    }

    private static ValueSpecification literalToValueSpec(Object value) {
        if (value instanceof String s)  return new CString(s);
        if (value instanceof Long l)    return new CInteger(l);
        if (value instanceof Integer i) return new CInteger((long) i);
        if (value instanceof Double d)  return new CFloat(d);
        if (value instanceof Boolean b) return new CBoolean(b);
        throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.NORMALIZE, "Unsupported literal type: "
                + (value == null ? "null" : value.getClass().getName()));
    }

    private static String comparisonFn(ComparisonOp op) {
        return switch (op) {
            case EQ  -> "equal";
            case NEQ -> "notEqual";
            case LT  -> "lessThan";
            case LTE -> "lessThanEqual";
            case GT  -> "greaterThan";
            case GTE -> "greaterThanEqual";
        };
    }

    private static ValueSpecification buildNewInstance(String classFqn,
                                                      Map<String, KeyExpression> fields) {
        return new AppliedFunction("new", List.of(
                new PackageableElementPtr(classFqn),
                new NewInstance(classFqn, List.of(),
                        Collections.unmodifiableMap(new LinkedHashMap<>(fields)))));
    }

    private static TypeExpression findPropertyType(ClassDefinition cd, String propName) {
        for (ClassDefinition.PropertyDefinition p : cd.properties()) {
            if (p.name().equals(propName)) return p.type();
        }
        return null;
    }

    /**
     * Resolve a property's declared type on {@code cd} or any of its
     * superclasses (depth-first over {@link ClassDefinition#superClasses()}).
     * Returns {@code null} if the property is declared nowhere in the
     * generalization chain. A {@code visited} guard tolerates malformed
     * cyclic {@code extends} graphs without looping.
     */
    private static TypeExpression findPropertyTypeDeep(ClassDefinition cd, String propName,
                                                      ModelBuilder model) {
        TypeExpression own = findPropertyTypeDeep(cd, propName, model, new HashSet<>());
        if (own != null) return own;
        // Association properties are class properties semantically. This lets
        // injected per-end association property mappings (Option A; see
        // docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b) resolve their terminus class
        // through validatePmNames / classTypedTargetIfMapped / emitJoinChain.
        if (cd == null) return null;
        return model.findAssociationProperty(cd.qualifiedName(), propName).orElse(null);
    }

    private static TypeExpression findPropertyTypeDeep(ClassDefinition cd, String propName,
                                                      ModelBuilder model, Set<String> visited) {
        if (cd == null || !visited.add(cd.qualifiedName())) return null;
        TypeExpression own = findPropertyType(cd, propName);
        if (own != null) return own;
        for (TypeExpression sup : cd.superClasses()) {
            if (sup instanceof TypeExpression.NameRef nr) {
                ClassDefinition superCd = model.findClass(nr.name()).orElse(null);
                TypeExpression inherited = findPropertyTypeDeep(superCd, propName, model, visited);
                if (inherited != null) return inherited;
            }
        }
        return null;
    }
}
