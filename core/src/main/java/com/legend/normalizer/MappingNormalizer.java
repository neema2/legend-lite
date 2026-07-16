package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.Multiplicity;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.model.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.ComparisonOp;
import com.legend.model.DatabaseDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.EnumerationMapping;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.Function;
import com.legend.model.FunctionDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.LogicalOp;
import com.legend.model.MappingDefinition;
import com.legend.model.MappingInclude;
import com.legend.model.PackageableElement;
import com.legend.model.PropertyMapping;
import com.legend.model.Realization;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;
import com.legend.model.SynthHat;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.CBoolean;
import com.legend.model.spec.CDate;
import com.legend.model.spec.CDecimal;
import com.legend.model.spec.CFloat;
import com.legend.model.spec.CInteger;
import com.legend.model.spec.CString;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.EnumValue;
import com.legend.model.spec.KeyExpression;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.NewInstance;
import com.legend.model.spec.NewInstanceCast;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.TypeAnnotation;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;
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
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    static final Set<String> AGGREGATE_FNS = Set.of(
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
        return normalize(parsed, model, null);
    }

    /** TOLERANT variant (module compile): a non-null {@code wallSink}
     * collects per-mapping normalization walls (element FQN &rarr; first
     * error line) and EXCLUDES those mappings instead of throwing. */
    public static NormalizedModel normalize(ParsedModel parsed, ModelBuilder model,
            java.util.Map<String, String> wallSink) {
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
                try {
                    out.add(withElement(md.qualifiedName(),
                            () -> normalizeMapping(latest, model, lifted,
                                    wallSink != null)));
                } catch (ModelException e) {
                    if (wallSink == null || e.element() == null) {
                        throw e;
                    }
                    wallSink.putIfAbsent(e.element(),
                            String.valueOf(e.getMessage()).split("\n")[0]);
                }
            } else if (el instanceof MappingDefinition canonical) {
                // Clean-sheet (Door 1/3) mapping: function-ref bindings pass
                // through; inline expression bindings (Door 3) are lambda-lifted
                // here (CLEAN_SHEET_INVERSION §5.3), so no Inline survives Phase E.
                try {
                    out.add(withElement(canonical.qualifiedName(),
                            () -> liftInlineBindings(canonical, model, lifted)));
                } catch (ModelException e) {
                    if (wallSink == null || e.element() == null) {
                        throw e;
                    }
                    wallSink.putIfAbsent(e.element(),
                            String.valueOf(e.getMessage()).split("\n")[0]);
                }
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
    /**
     * Attach the ELEMENT FQN to any {@link com.legend.error.ModelException}
     * escaping {@code work} — ONE wrap covers every throw inside a mapping's
     * normalization, so the driver can decorate with the element's
     * {@code [line:col]} (positions wave).
     */
    private static <T> T withElement(String elementFqn, Supplier<T> work) {
        try {
            return work.get();
        } catch (ModelException e) {
            if (e.element() != null) {
                throw e;
            }
            throw new ModelException(e.phase(), e.getMessage(), elementFqn);
        } catch (com.legend.error.NotImplementedException
                | com.legend.error.MappingResolutionException e) {
            // DELIBERATE walls get element attribution too — a module
            // compile's drop-and-wall needs the identity. Genuine bugs
            // (NPE, ISE) stay RAW: they must fail the build, never
            // silently wall an element away.
            throw new ModelException(
                    com.legend.error.LegendCompileException.Phase.NORMALIZE,
                    e.getMessage(), elementFqn);
        }
    }

    private static MappingDefinition normalizeMapping(LegacyMappingDefinition md,
                                                     ModelBuilder model,
                                                     List<FunctionDefinition> lifted,
                                                     boolean tolerant) {
        detectM2MCycles(md, model);

        // Pre-pass: flatten `extends [parentSetId]` by merging inherited
        // property mappings into each child mapping (child overrides on
        // property-name conflict; multi-level resolves recursively). See
        // docs/MAPPING_LEGACY_TO_FUNCTION.md §5.2.3.
        md = resolveExtends(md, model);

        // Pre-pass: inject MULTI-HOP association ends as class-typed Join PMs
        // into their owning class mappings (Option A; see
        // docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b). Single-hop ends keep the
        // §5.6.1 standalone predicate.
        md = AssociationSynthesis.injectMultiHopAssociationPMs(md, model);

        // A class mapped through MULTIPLE set IDs synthesizes its ROOT set
        // only — .all() dispatches to the root; non-root sets await the H5
        // set-ID dispatch story (ModelBuilder's R2 already guaranteed the
        // one-root shape).
        Map<String, Long> mappingsPerClass = new HashMap<>();
        for (ClassMapping cm : md.classMappings()) {
            mappingsPerClass.merge(cm.className(), 1L, Long::sum);
        }
        List<MappingDefinition.ClassBinding> classBindings =
                new ArrayList<>(md.classMappings().size());
        Set<String> unionRooted = new HashSet<>();
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Union) {
                unionRooted.add(cm.className());
            }
        }
        for (ClassMapping cm : md.classMappings()) {
            if (mappingsPerClass.get(cm.className()) > 1 && !cm.root()) {
                if (!unionRooted.contains(cm.className())) {
                    // multi-set class without a UNION root: .all() dispatch
                    // is undefined — recorded as a poison so the 0-binder
                    // error explains itself, never silent
                    model.mappingPoisons.putIfAbsent(
                            md.qualifiedName() + "::" + cm.className(),
                            "class is mapped through multiple set IDs; .all() over"
                                    + " multi-set mappings (implicit union) is a"
                                    + " roadmap feature");
                }
                continue;
            }
            FunctionDefinition fn;
            try {
                fn = synthesizeClassMapping(md, cm, model);
            } catch (NotImplementedException
                    | ModelException e) {
                // PER-CLASS fault isolation: one class mapping using a
                // roadmap feature must not sink the whole mapping. The
                // binding is withheld; fetching THIS class raises the
                // recorded reason (loud at use, never silent).
                // DELIBERATE TRADE (audit 6, adjudicated): ModelException
                // here means a USER-model error the real engine rejects at
                // compile time; we defer it to query time so the rest of a
                // partially-broken model stays loadable/queryable. The full
                // message rides on the poison and surfaces via
                // StoreResolver's 0-binder error.
                model.mappingPoisons.put(md.qualifiedName() + "::" + cm.className(),
                        String.valueOf(e.getMessage()));
                continue;
            }
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
            FunctionDefinition fn;
            try {
                fn = AssociationSynthesis.synthesizeAssociationMapping(md, am, model);
            } catch (NotImplementedException | ModelException e) {
                // PER-ASSOCIATION fault isolation (mirrors the per-class arm
                // above): one XStore/ModelJoin association on roadmap
                // machinery must not sink the whole mapping — the class
                // bindings stay queryable; navigating THIS association
                // raises the recorded reason via the poison channel.
                // TOLERANT (module) builds only — a STRICT build must
                // reject what the engine's compiler rejects (audit 17).
                if (!tolerant) {
                    throw e;
                }
                model.mappingPoisons.putIfAbsent(
                        md.qualifiedName() + "::" + am.associationName(),
                        String.valueOf(e.getMessage()));
                continue;
            }
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
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "inline association predicate for '" + ab.associationFqn()
                  + "' must be a single (source, target) -> Boolean lambda; mapping="
                  + md.qualifiedName());
        }
        if (lam.parameters().size() != 2) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "inline association predicate for '" + ab.associationFqn()
                  + "' must take exactly 2 parameters (source, target); got "
                  + lam.parameters().size() + "; mapping=" + md.qualifiedName());
        }
        AssociationDefinition ad = model.findAssociation(ab.associationFqn())
                .orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE, 
                        "inline association predicate references unknown association '"
                      + ab.associationFqn() + "'; mapping=" + md.qualifiedName()));
        String classA = ad.property1().targetClassFqn();
        String classB = ad.property2().targetClassFqn();
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
     * The set-implementation ID of a class mapping: the explicit
     * {@code [id]}, else the class FQN with {@code ::} &rarr; {@code _}
     * &mdash; the engine's default ({@code HelperMappingBuilder
     * .getClassMappingId}); corpus mappings reference singleton sets by
     * that implicit spelling ({@code extends [meta_relational_tests_model
     * _simple_Firm]}).
     */
    static String setIdOf(ClassMapping cm) {
        return cm.setId() != null ? cm.setId() : cm.className().replace("::", "_");
    }

    /**
     * Rewrite the condition's target-side reads ({@code $t.COL}) to the
     * member-suffixed spelling ({@code $t.COL_<ord>}), collecting
     * physical&rarr;suffixed names. Closed vocabulary, loud default —
     * join conditions are comparisons/boolean ops over column reads and
     * literals; anything else is a shape this route cannot suffix yet.
     */
    static ValueSpecification suffixTargetReads(ValueSpecification n,
            Variable t, int ord, Map<String, String> out) {
        if (n instanceof AppliedProperty ap
                && ap.receiver() instanceof Variable v
                && v.name().equals(t.name())) {
            String suffixed = ap.property() + "_" + ord;
            out.put(ap.property(), suffixed);
            return new AppliedProperty(v, suffixed);
        }
        return switch (n) {
            case AppliedFunction af -> new AppliedFunction(af.function(),
                    af.parameters().stream().map(x ->
                            suffixTargetReads(x, t, ord, out)).toList());
            case AppliedProperty ap -> new AppliedProperty(
                    suffixTargetReads(ap.receiver(), t, ord, out), ap.property());
            case Variable v -> v;
            case CString ignored -> n;
            case CInteger ignored -> n;
            case CFloat ignored -> n;
            case CDecimal ignored -> n;
            case CBoolean ignored -> n;
            case CDate ignored -> n;
            case PureCollection pc -> new PureCollection(pc.values().stream()
                    .map(x -> suffixTargetReads(x, t, ord, out)).toList());
            default -> throw new NotImplementedException(
                    "partial-union route join condition carries a "
                    + n.getClass().getSimpleName()
                    + " — not suffixable yet");
        };
    }

    /**
     * The ordinal of {@code setId} among the member sets of the Union
     * operation mapping {@code prop}'s declared target class — the position
     * of that member's thread in the synthesized left-deep concatenate
     * (and the engine's {@code _N} key-column suffix). {@code null} when
     * the property isn't class-typed or no local Union covers the set.
     */
    /**
     * The set-implementation with effective id {@code setId}, resolved in
     * {@code md} and its includes transitively (engine {@code
     * classMappingById} is include-recursive — audit 11: own-mapping-only
     * lookup dropped routes to included sets). Own definitions win.
     */
    static ClassMapping findSetById(LegacyMappingDefinition md,
            ModelBuilder model, String setId) {
        for (ClassMapping cm : md.classMappings()) {
            if (setId.equals(setIdOf(cm))) {
                return cm;
            }
        }
        Map<String, ClassMapping> included = new HashMap<>();
        collectIncludedSetIds(md, model, included, new HashSet<>());
        return included.get(setId);
    }

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
    private static LegacyMappingDefinition resolveExtends(LegacyMappingDefinition md,
                                                          ModelBuilder model) {
        boolean any = md.classMappings().stream().anyMatch(cm -> cm.extendsSetId() != null);
        if (!any) return md;
        // set-ids resolve within this mapping AND its includes (transitive,
        // own definitions win) — extends [set] across an include is the
        // union::extend corpus family's normal shape
        Map<String, ClassMapping> bySetId = new HashMap<>();
        collectIncludedSetIds(md, model, bySetId, new HashSet<>());
        for (ClassMapping cm : md.classMappings()) {
            bySetId.put(setIdOf(cm), cm);
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
                throw new NotImplementedException(
                        "Class mapping for '" + cm.className() + "' uses extends ["
                      + cm.extendsSetId() + "] on a non-Relational (Pure) mapping; "
                      + "only Relational extends is supported. Mapping="
                      + md.qualifiedName());
            }
        }
        return md.withClassMappings(rewritten);
    }

    /** Set-ids of {@code md}'s includes, transitively (nearer include wins). */
    static void collectIncludedSetIds(LegacyMappingDefinition md,
            ModelBuilder model, Map<String, ClassMapping> bySetId,
            Set<String> seen) {
        for (MappingInclude inc : md.includes()) {
            if (!seen.add(inc.mappingPath())) {
                continue;
            }
            LegacyMappingDefinition included =
                    model.findLegacyMapping(inc.mappingPath()).orElse(null);
            if (included == null) {
                continue;   // unresolvable include is its own loud problem elsewhere
            }
            collectIncludedSetIds(included, model, bySetId, seen);
            for (ClassMapping cm : included.classMappings()) {
                bySetId.put(setIdOf(cm), cm);
            }
        }
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
                child.groupBy() != null ? child.groupBy() : flatParent.groupBy(),
                child.primaryKey() != null && !child.primaryKey().isEmpty()
                        ? child.primaryKey() : flatParent.primaryKey(),
                new ArrayList<>(merged.values()), child.sourceUrl(),
                child.propertyTargetSets());
    }

    // ====================================================================
    // Pre-pass: inject multi-hop association ends as class-typed Join PMs
    // ====================================================================

    static String nameRefOrNull(TypeExpression t) {
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
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
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

    /** Orientation-normal form for XStore conditions: {@code ==} is
     * commutative — the two direction lines write the SAME predicate with
     * swapped operands ({@code $this.id == $that.firmId} vs
     * {@code $this.firmId == $that.id}); put the {@code srcRow}-rooted
     * operand first so equal directions COMPARE equal (audit: the
     * direction-specific wall fired on pure commutation). */
    private static ValueSpecification canonicalizeEqualOperands(
            ValueSpecification v, String srcVar) {
        if (v instanceof AppliedFunction af) {
            List<ValueSpecification> ps = af.parameters().stream()
                    .map(x -> canonicalizeEqualOperands(x, srcVar)).toList();
            if (("equal".equals(af.function()) || "==".equals(af.function()))
                    && ps.size() == 2
                    && !rootedAt(ps.get(0), srcVar)
                    && rootedAt(ps.get(1), srcVar)) {
                ps = List.of(ps.get(1), ps.get(0));
            }
            return new AppliedFunction(af.function(), ps);
        }
        return v;
    }

    private static boolean rootedAt(ValueSpecification v, String var) {
        if (v instanceof Variable x) {
            return x.name().equals(var);
        }
        if (v instanceof AppliedProperty ap) {
            return rootedAt(ap.receiver(), var);
        }
        if (v instanceof AppliedFunction af && af.parameters().size() == 1) {
            return rootedAt(af.parameters().get(0), var);
        }
        return false;
    }

    private static FunctionDefinition synthesizeClassMapping(LegacyMappingDefinition md,
                                                            ClassMapping cm,
                                                            ModelBuilder model) {
        // prop[setId] routing is classified PER-PM (Join.targetSetId) inside
        // synthTableBackedParts — the name-keyed propertyTargetSets map
        // cannot distinguish same-named duplicates (audit 11: textual PM
        // order silently decided the outcome), so no map-driven pre-rewrite
        // happens here.
        // GROUPED sets carry only key + aggregate properties in the grouped
        // ROW — a class-typed Join PM is navigation, never projection
        // (stage 1, audit-17 bucket analysis): withhold it BEFORE synthesis
        // so neither the ctor nor the slot injection references it; a
        // query touching the property raises the ordinary not-mapped error.
        if (cm instanceof ClassMapping.Relational g && !g.groupBy().isEmpty()
                && g.propertyMappings().stream()
                        .anyMatch(x -> x instanceof PropertyMapping.Join)) {
            cm = new ClassMapping.Relational(
                    g.className(), g.setId(), g.extendsSetId(), g.root(),
                    g.mainTable(), g.filter(), g.distinct(), g.groupBy(),
                    g.primaryKey(),
                    g.propertyMappings().stream()
                            .filter(x -> !(x instanceof PropertyMapping.Join))
                            .toList(),
                    g.sourceUrl(), g.propertyTargetSets());
        }
        ValueSpecification body = switch (cm) {
            case ClassMapping.Pure pcm       -> synthM2M(md, pcm, model, new HashSet<>());
            case ClassMapping.Relational rcm -> synthRelational(md, rcm, model);
            case ClassMapping.Union u        -> UnionSynthesis.synthUnion(md, u, model);
            case ClassMapping.Inheritance ih -> UnionSynthesis.synthInheritance(md, ih, model);
            case ClassMapping.RelationFunction rf -> synthRelationFunction(md, rf, model);
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

    /**
     * Relation({@code ~func}) class mapping: the class's extent is the
     * RELATION the referenced zero-arg function returns; property bindings
     * read its columns by name. The function's single body expression
     * β-inlines as the pipeline (it is zero-arg by grammar), then the
     * standard map terminal projects the constructor:
     * {@code <fn body> -> map(row | ^Class(prop = $row.COL, ...))}.
     * Mapping-local ({@code +}) columns are XStore association keys — not
     * class properties; they are omitted from the constructor and consumed
     * by XStore association support when it lands.
     */
    private static ValueSpecification synthRelationFunction(
            LegacyMappingDefinition md,
            ClassMapping.RelationFunction rf, ModelBuilder model) {
        ValueSpecification pipeline = relationFunctionPipeline(rf, model);
        Variable row = new Variable("rf_row");
        Map<String, KeyExpression> fields = new LinkedHashMap<>();
        putRelationCols(fields, rf.columns(), row, rf.className(), md, model);
        return new AppliedFunction("map", List.of(pipeline,
                new LambdaFunction(List.of(row),
                        List.of(buildNewInstanceToOne(rf.className(), fields, model)))));
    }

    /** Bindings for a Relation mapping's column list — EMBEDDED blocks
     * ({@code prop ( sub: COL, ... )}) synthesize an inner instance over
     * the SAME row (the Relational-kind embedded emission's shape),
     * recursively. */
    private static void putRelationCols(Map<String, KeyExpression> fields,
            List<ClassMapping.RelationFunction.Col> cols, Variable row,
            String ownerClassFqn, LegacyMappingDefinition md,
            ModelBuilder model) {
        for (ClassMapping.RelationFunction.Col c : cols) {
            if (c.local()) {
                continue;
            }
            if (c.inlineSetId() != null) {
                // INLINE-embedded (prop () Inline [set]): the SIBLING set's
                // column list gives the sub-object's shape; its bindings
                // read THIS row (engine inline semantics: reuse the set's
                // mapping shape in place)
                ClassMapping.RelationFunction sibling = null;
                for (ClassMapping cm : md.classMappings()) {
                    if (cm instanceof ClassMapping.RelationFunction rf2
                            && c.inlineSetId().equals(setIdOf(rf2))) {
                        sibling = rf2;
                        break;
                    }
                }
                if (sibling == null) {
                    throw new ModelException(
                            LegendCompileException.Phase.NORMALIZE,
                            "inline-embedded property '" + c.property()
                            + "' names set '" + c.inlineSetId()
                            + "' which is not a Relation set of mapping "
                            + md.qualifiedName());
                }
                Map<String, KeyExpression> inner = new LinkedHashMap<>();
                putRelationCols(inner, sibling.columns(), row,
                        sibling.className(), md, model);
                fields.put(c.property(), new KeyExpression(
                        buildNewInstanceToOne(sibling.className(), inner, model),
                        false, false));
                continue;
            }
            if (c.isEmbedded()) {
                ClassDefinition owner = model.findClass(ownerClassFqn)
                        .orElseThrow(() -> new ModelException(
                                LegendCompileException.Phase.NORMALIZE,
                                "Relation mapping embedded property '"
                                + c.property() + "': unknown owner class '"
                                + ownerClassFqn + "'"));
                TypeExpression t = findPropertyTypeDeep(owner, c.property(),
                        model);
                if (!(t instanceof TypeExpression.NameRef nr)) {
                    throw new ModelException(
                            LegendCompileException.Phase.NORMALIZE,
                            "Relation mapping embedded property '"
                            + c.property() + "' of '" + ownerClassFqn
                            + "' has non-class type — cannot embed");
                }
                Map<String, KeyExpression> inner = new LinkedHashMap<>();
                putRelationCols(inner, c.embedded(), row, nr.name(), md, model);
                fields.put(c.property(), new KeyExpression(
                        buildNewInstanceToOne(nr.name(), inner, model),
                        false, false));
                continue;
            }
            ValueSpecification read = new AppliedProperty(row, c.column());
            if (c.enumMappingId() != null) {
                // enum-decoded column: the same source-value decode chain
                // every other enum-mapped read synthesizes
                read = translateEnumeratedSource(c.property(), c.enumMappingId(),
                        read, md, ownerClassFqn, model);
            }
            fields.put(c.property(), new KeyExpression(read, false, false));
        }
    }

    /** Resolve a Relation mapping's {@code ~func} ref and inline its body. */
    static ValueSpecification relationFunctionPipeline(
            ClassMapping.RelationFunction rf, ModelBuilder model) {
        String ref = rf.funcRef();
        List<Function> fns = model.findFunction(ref);
        if (fns.isEmpty()) {
            // the MANGLED spelling f__Relation_1_ encodes the signature in
            // the name — strip the suffix and retry
            Matcher mangled = Pattern
                    .compile("^(.*?)__[A-Za-z0-9$]+_(?:\\d+|MANY)_$").matcher(ref);
            if (mangled.matches()) {
                fns = model.findFunction(mangled.group(1));
            }
        }
        if (fns.size() != 1
                || !(fns.get(0) instanceof FunctionDefinition fn)) {
            throw new NotImplementedException(
                    "Relation mapping for '" + rf.className() + "': ~func '"
                    + ref + "' resolves to " + fns.size() + " function(s)");
        }
        if (!fn.parameters().isEmpty() || fn.body().size() != 1) {
            throw new NotImplementedException(
                    "Relation mapping ~func '" + fn.qualifiedName()
                    + "' must be a zero-arg single-expression function");
        }
        return fn.body().get(0);
    }

    /**
     * XStore association over two Relation-function class mappings: the end
     * expression ({@code $this.id == $that.firmId}) rewrites property reads
     * to the two relations' COLUMN reads (mapping-local {@code +} columns
     * included — that is what they exist for) and rides the SAME
     * {@code legacyAssocPredicate(a, b, srcRel, tgtRel, {s,t|cond})}
     * emission the table-backed path uses: the relation args type the
     * lambda's rows through the ordinary kernel, and the resolver reads
     * the oriented condition off the call.
     */
    static FunctionDefinition synthesizeXStoreMapping(LegacyMappingDefinition md,
            AssociationMapping.Cross xs, ModelBuilder model,
            String classA, String classB) {
        AssociationDefinition ad = model.findAssociation(xs.associationName()).orElseThrow();
        // the line's [srcSet, tgtSet] ids select the sets: srcSet = the
        // OWNING end's, tgtSet = the line's target end's (engine
        // PropertyMappingBuilder uses them the same way)
        String setA = null;
        String setB = null;
        if (!xs.propertyMappings2().isEmpty()) {
            var l0 = xs.propertyMappings2().get(0);
            boolean p1 = l0.propertyName().equals(ad.property1().propertyName());
            setA = p1 ? l0.targetSetId() : l0.sourceSetId();
            setB = p1 ? l0.sourceSetId() : l0.targetSetId();
        }
        XEnd endA = xstoreEndOf(md, classA, setA, model);
        XEnd endB = xstoreEndOf(md, classB, setB, model);
        ClassMapping.RelationFunction rfA = endA.colsView();
        ClassMapping.RelationFunction rfB = endB.colsView();
        if (xs.propertyMappings2().isEmpty()) {
            throw new ModelException(
                    LegendCompileException.Phase.NORMALIZE,
                    "XStore mapping for '" + xs.associationName()
                    + "' has no property lines; mapping=" + md.qualifiedName());
        }
        Variable srcRow = new Variable("srcRow");
        Variable tgtRow = new Variable("tgtRow");
        boolean selfAssoc = classA.equals(classB);
        // ORIENTATION (the resolver's associationJoin contract): for
        // DISTINCT end classes the cond binds (srcRow=classA-row,
        // tgtRow=classB-row), so a property1 line's $that (the property1
        // destination = classA) maps to srcRow. A SELF-association cannot
        // orient by class — the pinned convention is "property1's
        // destination on tgtRow" (the table-backed emission binds {target}
        // there), which is the INVERSE mapping (audit 8 S1).
        List<ValueSpecification> conds = new ArrayList<>();
        for (AssociationMapping.Cross.XStoreProperty cand : xs.propertyMappings2()) {
            boolean isProp1 = cand.propertyName().equals(ad.property1().propertyName());
            if (!isProp1 && !cand.propertyName().equals(ad.property2().propertyName())) {
                throw new ModelException(
                        LegendCompileException.Phase.NORMALIZE,
                        "XStore line '" + cand.propertyName() + "' matches"
                        + " neither end of association '" + xs.associationName()
                        + "'; mapping=" + md.qualifiedName());
            }
            Variable thatRow;
            if (selfAssoc) {
                thatRow = isProp1 ? tgtRow : srcRow;
            } else {
                thatRow = isProp1 ? srcRow : tgtRow;
            }
            Variable thisRow = thatRow == srcRow ? tgtRow : srcRow;
            ClassMapping.RelationFunction thatRf = isProp1 ? rfA : rfB;
            ClassMapping.RelationFunction thisRf = isProp1 ? rfB : rfA;
            conds.add(canonicalizeEqualOperands(rewriteXStoreReads(
                    cand.expression(),
                    thisRow, thisRf, thatRow, thatRf, xs.associationName(), md),
                    srcRow.name()));
        }
        // the engine compiles each direction's expression independently;
        // our single-predicate emission requires them to AGREE — loud when
        // a model gives the two directions different conditions (audit S6)
        ValueSpecification cond = conds.get(0);
        for (ValueSpecification c : conds) {
            if (!c.equals(cond)) {
                throw new NotImplementedException(
                        "XStore association '" + xs.associationName()
                        + "' has direction-specific conditions; a single"
                        + " shared predicate is required for now (mapping="
                        + md.qualifiedName() + ")");
            }
        }
        Variable a = new Variable("a");
        Variable b = new Variable("b");
        ValueSpecification body = new AppliedFunction("legacyAssocPredicate", List.of(
                a, b,
                endA.pipeline(),
                endB.pipeline(),
                new LambdaFunction(List.of(srcRow, tgtRow), List.of(cond))));
        FunctionDefinition.ParameterDefinition pA = new FunctionDefinition.ParameterDefinition(
                "a", new TypeExpression.NameRef(classA), Multiplicity.Concrete.PURE_ONE);
        FunctionDefinition.ParameterDefinition pB = new FunctionDefinition.ParameterDefinition(
                "b", new TypeExpression.NameRef(classB), Multiplicity.Concrete.PURE_ONE);
        return new FunctionDefinition(
                SynthFqn.mappingAssoc(md.qualifiedName(), xs.associationName()),
                List.of(), List.of(), List.of(pA, pB),
                new TypeExpression.NameRef("meta::pure::metamodel::type::Boolean"),
                Multiplicity.Concrete.PURE_ONE,
                List.of(body),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.ASSOC, md.qualifiedName(), xs.associationName()));
    }

    /**
     * ModelJoin association: the typed lambda's params name the two end
     * classes; the condition rewrites property reads to the Relation
     * mappings' columns and rides the legacyAssocPredicate emission (same
     * contract as {@link #synthesizeXStoreMapping}). Param-to-end matching
     * is by DECLARED TYPE (the corpus writes them fully qualified).
     */
    static FunctionDefinition synthesizeModelJoinMapping(LegacyMappingDefinition md,
            AssociationMapping.ModelJoin mj, ModelBuilder model,
            String classA, String classB) {
        AssociationDefinition ad2 = model.findAssociation(mj.associationName()).orElseThrow();
        ClassMapping.RelationFunction rfA = relationFunctionMappingOf(md, classA, null);
        ClassMapping.RelationFunction rfB = relationFunctionMappingOf(md, classB, null);
        if (mj.lambda().parameters().size() != 2) {
            throw new NotImplementedException(
                    "ModelJoin for '" + mj.associationName() + "' needs a"
                    + " 2-param lambda; got " + mj.lambda().parameters().size());
        }
        Variable p0 = mj.lambda().parameters().get(0);
        Variable p1 = mj.lambda().parameters().get(1);
        String t0 = p0.type() instanceof TypeExpression.NameRef nr0 ? nr0.name() : null;
        String t1 = p1.type() instanceof TypeExpression.NameRef nr1 ? nr1.name() : null;
        String aVar;
        String bVar;
        // pairing: by EXACT type FQN when the ends differ; a SELF-association
        // is type-ambiguous, so the engine mandates NAME pairing (params
        // named after the association properties — HelperMappingBuilder
        // resolveLambdaParamNames) and errors otherwise. No suffix matching.
        if (!classA.equals(classB) && classA.equals(t0) && classB.equals(t1)) {
            aVar = p0.name();
            bVar = p1.name();
        } else if (!classA.equals(classB) && classA.equals(t1) && classB.equals(t0)) {
            aVar = p1.name();
            bVar = p0.name();
        } else if (p0.name().equals(ad2.property1().propertyName())
                && p1.name().equals(ad2.property2().propertyName())) {
            aVar = p0.name();
            bVar = p1.name();
        } else if (p1.name().equals(ad2.property1().propertyName())
                && p0.name().equals(ad2.property2().propertyName())) {
            aVar = p1.name();
            bVar = p0.name();
        } else {
            throw new NotImplementedException(
                    "ModelJoin for '" + mj.associationName() + "': lambda params ["
                    + p0.name() + ": " + t0 + ", " + p1.name() + ": " + t1
                    + "] pair with the association ends neither by exact type"
                    + " nor by the engine's property-name rule");
        }
        Variable srcRow = new Variable("srcRow");
        Variable tgtRow = new Variable("tgtRow");
        Map<String, Variable> rowByVar = Map.of(aVar, srcRow, bVar, tgtRow);
        Map<String, ClassMapping.RelationFunction> rfByVar = Map.of(aVar, rfA, bVar, rfB);
        ValueSpecification cond = rewriteRelationReads(
                mj.lambda().body().get(mj.lambda().body().size() - 1),
                rowByVar, rfByVar, mj.associationName(), md);
        Variable a = new Variable("a");
        Variable b = new Variable("b");
        ValueSpecification body = new AppliedFunction("legacyAssocPredicate", List.of(
                a, b,
                relationFunctionPipeline(rfA, model),
                relationFunctionPipeline(rfB, model),
                new LambdaFunction(List.of(srcRow, tgtRow), List.of(cond))));
        FunctionDefinition.ParameterDefinition pA = new FunctionDefinition.ParameterDefinition(
                "a", new TypeExpression.NameRef(classA), Multiplicity.Concrete.PURE_ONE);
        FunctionDefinition.ParameterDefinition pB = new FunctionDefinition.ParameterDefinition(
                "b", new TypeExpression.NameRef(classB), Multiplicity.Concrete.PURE_ONE);
        return new FunctionDefinition(
                SynthFqn.mappingAssoc(md.qualifiedName(), mj.associationName()),
                List.of(), List.of(), List.of(pA, pB),
                new TypeExpression.NameRef("meta::pure::metamodel::type::Boolean"),
                Multiplicity.Concrete.PURE_ONE,
                List.of(body),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.ASSOC, md.qualifiedName(), mj.associationName()));
    }

    /**
     * The Relation-function class mapping for {@code classFqn} in {@code md}
     * — by SET ID when the association line names one; otherwise the class's
     * sole set (two sets without an id is ambiguous — loud, never first-wins).
     */
    private static ClassMapping.RelationFunction relationFunctionMappingOf(
            LegacyMappingDefinition md, String classFqn, String setId) {
        List<ClassMapping.RelationFunction> hits = new ArrayList<>();
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.RelationFunction rf
                    && rf.className().equals(classFqn)
                    && (setId == null || setId.equals(setIdOf(rf)))) {
                hits.add(rf);
            }
        }
        if (hits.size() != 1) {
            throw new NotImplementedException(
                    "XStore/ModelJoin association end class '" + classFqn
                    + "' resolves to " + hits.size() + " Relation(~func) set(s)"
                    + (setId != null ? " for set id '" + setId + "'" : "")
                    + " in '" + md.qualifiedName() + "'");
        }
        return hits.get(0);
    }

    /** An XStore END resolved to its anchor pipeline and column view —
     * a Relation(~func) set directly, or a RELATIONAL set converted
     * (Column/local PMs -> cols, ~mainTable -> pipeline): MIXED-kind
     * mappings bridge the two (relation-family MixedMapping). */
    private record XEnd(ValueSpecification pipeline,
                        ClassMapping.RelationFunction colsView) {}

    private static XEnd xstoreEndOf(LegacyMappingDefinition md,
            String classFqn, String setId, ModelBuilder model) {
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.RelationFunction rf
                    && rf.className().equals(classFqn)
                    && (setId == null || setId.equals(setIdOf(rf)))) {
                return new XEnd(relationFunctionPipeline(rf, model), rf);
            }
        }
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Relational rcm
                    && rcm.className().equals(classFqn)
                    && (setId == null || setId.equals(setIdOf(rcm)))) {
                List<ClassMapping.RelationFunction.Col> cols = new ArrayList<>();
                for (PropertyMapping pm : rcm.propertyMappings()) {
                    if (pm instanceof PropertyMapping.Column c) {
                        cols.add(new ClassMapping.RelationFunction.Col(
                                c.propertyName(), c.column(), false));
                    } else if (pm instanceof PropertyMapping.LocalProperty lp
                            && lp.body() instanceof PropertyMapping.Column lc) {
                        cols.add(new ClassMapping.RelationFunction.Col(
                                lp.propertyName(), lc.column(), true));
                    }
                }
                return new XEnd(mainTableRefOf(md, classFqn, model),
                        new ClassMapping.RelationFunction(classFqn,
                                setIdOf(rcm), null, rcm.root(),
                                "<relational>", cols));
            }
        }
        throw new NotImplementedException(
                "XStore/ModelJoin association end class '" + classFqn
                + "' resolves to no Relation or Relational set"
                + (setId != null ? " for set id '" + setId + "'" : "")
                + " in '" + md.qualifiedName() + "'");
    }

    /** {@code $this.p}/{@code $that.p} → column reads on the two relation rows. */
    private static ValueSpecification rewriteXStoreReads(ValueSpecification v,
            Variable thisRow, ClassMapping.RelationFunction thisRf,
            Variable thatRow, ClassMapping.RelationFunction thatRf,
            String assocName, LegacyMappingDefinition md) {
        return rewriteRelationReads(v,
                Map.of("this", thisRow, "that", thatRow),
                Map.of("this", thisRf, "that", thatRf), assocName, md);
    }

    /** {@code $var.prop} → the var's Relation mapping's COLUMN read. */
    private static ValueSpecification rewriteRelationReads(ValueSpecification v,
            Map<String, Variable> rowByVar,
            Map<String, ClassMapping.RelationFunction> rfByVar,
            String assocName, LegacyMappingDefinition md) {
        if (v instanceof AppliedProperty ap
                && ap.receiver() instanceof Variable var
                && rowByVar.containsKey(var.name())) {
            ClassMapping.RelationFunction rf = rfByVar.get(var.name());
            for (ClassMapping.RelationFunction.Col c : rf.columns()) {
                if (c.property().equals(ap.property()) && c.column() != null) {
                    return new AppliedProperty(rowByVar.get(var.name()), c.column());
                }
            }
            throw new NotImplementedException(
                    "association '" + assocName + "': $" + var.name() + "."
                    + ap.property() + " has no column binding on the Relation"
                    + " mapping of '" + rf.className() + "' (mapping="
                    + md.qualifiedName() + ")");
        }
        return switch (v) {
            case AppliedFunction af -> new AppliedFunction(af.function(),
                    af.parameters().stream().map(x -> rewriteRelationReads(x,
                            rowByVar, rfByVar, assocName, md)).toList());
            case AppliedProperty ap2 -> new AppliedProperty(
                    rewriteRelationReads(ap2.receiver(), rowByVar, rfByVar,
                            assocName, md), ap2.property());
            case PureCollection pc -> new PureCollection(
                    pc.values().stream().map(x -> rewriteRelationReads(x,
                            rowByVar, rfByVar, assocName, md)).toList());
            case LambdaFunction lf2 -> new LambdaFunction(lf2.parameters(),
                    lf2.body().stream().map(x -> rewriteRelationReads(x,
                            rowByVar, rfByVar, assocName, md)).toList());
            default -> v;
        };
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
                    throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
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
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "M2M class-typed property '" + pb.propertyName() + "' on '"
                  + tgt.qualifiedName() + "' targets unmapped class '" + innerFqn
                  + "'; map '" + innerFqn + "' or use Embedded. Mapping="
                  + md.qualifiedName());
        }
        if (!cycleStack.add(innerFqn)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
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

    /** {@code md} plus its includes, transitively. */
    static void collectMappingClosure(LegacyMappingDefinition md,
            ModelBuilder model, List<LegacyMappingDefinition> out,
            Set<String> seen) {
        if (!seen.add(md.qualifiedName())) {
            return;
        }
        out.add(md);
        for (MappingInclude inc : md.includes()) {
            model.findLegacyMapping(inc.mappingPath())
                    .ifPresent(m -> collectMappingClosure(m, model, out, seen));
        }
    }

    /** Column names of {@code table} referenced anywhere in the condition. */
    static void collectColumnsOfTable(RelationalOperation cond,
            String table, Set<String> out) {
        switch (cond) {
            case RelationalOperation.ColumnRef c -> {
                if (table.equals(c.table())) {
                    out.add(c.column());
                }
            }
            case RelationalOperation.Comparison c -> {
                collectColumnsOfTable(c.left(), table, out);
                collectColumnsOfTable(c.right(), table, out);
            }
            case RelationalOperation.BooleanOp b -> {
                collectColumnsOfTable(b.left(), table, out);
                collectColumnsOfTable(b.right(), table, out);
            }
            case RelationalOperation.Group g ->
                    collectColumnsOfTable(g.inner(), table, out);
            case RelationalOperation.IsNull n ->
                    collectColumnsOfTable(n.operand(), table, out);
            case RelationalOperation.IsNotNull n ->
                    collectColumnsOfTable(n.operand(), table, out);
            case RelationalOperation.FunctionCall f ->
                    f.args().forEach(x -> collectColumnsOfTable(x, table, out));
            default -> {
            }
        }
    }

    /** Whether the class (or a superclass) is BITEMPORAL. */
    static boolean isBitemporalClass(String classFqn, ModelBuilder model) {
        return isBitemporalClass(classFqn, model, new HashSet<>());
    }

    static boolean isBitemporalClass(String classFqn, ModelBuilder model,
            Set<String> visited) {
        if (!visited.add(classFqn)) {
            return false;
        }
        ClassDefinition cd = model.findClass(classFqn).orElse(null);
        if (cd == null) {
            return false;
        }
        for (var st : cd.stereotypes()) {
            if (("temporal".equals(st.profileName())
                    || "meta::pure::profiles::temporal".equals(st.profileName()))
                    && "bitemporal".equals(st.stereotypeName())) {
                return true;
            }
        }
        for (TypeExpression sup : cd.superClasses()) {
            if (sup instanceof TypeExpression.NameRef nr
                    && isBitemporalClass(nr.name(), model, visited)) {
                return true;
            }
        }
        return false;
    }

    /** Whether the class (or a superclass) carries a temporal stereotype. */
    private static boolean isTemporalClass(String classFqn, ModelBuilder model) {
        return isTemporalClass(classFqn, model, new HashSet<>());
    }

    private static boolean isTemporalClass(String classFqn, ModelBuilder model,
            Set<String> visited) {
        if (!visited.add(classFqn)) {
            return false;   // superclass cycle guard
        }
        ClassDefinition cd = model.findClass(classFqn).orElse(null);
        if (cd == null) {
            return false;
        }
        for (var st : cd.stereotypes()) {
            if (("temporal".equals(st.profileName())
                    || "meta::pure::profiles::temporal".equals(st.profileName()))
                    && Set.of("businesstemporal", "processingtemporal",
                            "bitemporal").contains(st.stereotypeName())) {
                return true;
            }
        }
        for (TypeExpression sup : cd.superClasses()) {
            if (sup instanceof TypeExpression.NameRef nr
                    && isTemporalClass(nr.name(), model, visited)) {
                return true;
            }
        }
        return false;
    }

    /**
     * A typed NULL for a member thread that does not carry {@code col}:
     * cast to the pure kind of the ROUTED member's physical column (the
     * threads must agree on the concatenate schema).
     */
    static ValueSpecification nullOfPhysicalKind(
            ClassMapping.Relational routedMember, String col,
            LegacyMappingDefinition md, ModelBuilder model) {
        DatabaseDefinition.ColumnDefinition cd = findPhysicalColumn(
                routedMember.mainTable().database(),
                routedMember.mainTable().table(), col, model);
        String kind = cd == null ? null : pureKindOf(cd.dataType());
        if (kind == null) {
            throw new NotImplementedException(
                    "union navigation key column '" + col + "' of table '"
                    + routedMember.mainTable().table() + "' has no derivable"
                    + " pure kind; mapping=" + md.qualifiedName());
        }
        return new AppliedFunction("cast", List.of(
                new PureCollection(List.of()),
                new TypeAnnotation.Named(
                        new TypeExpression.NameRef(kind))));
    }

    /**
     * A typed NULL for a union thread that does not map {@code prop}:
     * {@code []->cast(@DeclaredType)} &mdash; the empty collection carries
     * SQL NULL through the erasure lowering, the cast types the column so
     * the concatenate's branches agree.
     */
    static ValueSpecification nullOfDeclaredType(ClassDefinition owner,
            String prop, ModelBuilder model) {
        TypeExpression dt = owner == null ? null
                : findPropertyTypeDeep(owner, prop, model);
        String castTo = dt instanceof TypeExpression.NameRef nr
                ? nr.name() : "String";
        return new AppliedFunction("cast", List.of(
                new PureCollection(List.of()),
                new TypeAnnotation.Named(
                        new TypeExpression.NameRef(castTo))));
    }

    /** The declared multiplicity of {@code prop} on {@code owner} (chain walk). */
    private static Multiplicity findPropertyDeclared(
            ClassDefinition owner, String prop, ModelBuilder model) {
        for (ClassDefinition.PropertyDefinition pd
                : owner.properties()) {
            if (pd.name().equals(prop)) {
                return pd.multiplicity();
            }
        }
        for (TypeExpression sup : owner.superClasses()) {
            if (sup instanceof TypeExpression.NameRef nr) {
                ClassDefinition sc = model.findClass(nr.name()).orElse(null);
                if (sc != null) {
                    Multiplicity m = findPropertyDeclared(sc, prop, model);
                    if (m != null) {
                        return m;
                    }
                }
            }
        }
        return null;
    }

    static ValueSpecification synthRelational(LegacyMappingDefinition md,
                                                     ClassMapping.Relational rcm,
                                                     ModelBuilder model) {
        // JSON-source: synthesized by ModelBuilder cross-baking from a
        // RuntimeDefinition's JsonModelConnection. mainTable is null;
        // sourceUrl carries the inline VARIANT subquery source.
        if (rcm.sourceUrl() != null) {
            return synthJsonSourceMapping(md, rcm, model);
        }
        if (rcm.mainTable() == null) {
            // real engine INFERS the main table when ~mainTable is absent:
            // the table of the first direct column binding (corpus mappings
            // rarely spell ~mainTable). No column binding to infer from
            // stays a loud wall.
            LegacyMappingDefinition.TableReference inferred = inferMainTable(rcm);
            if (inferred == null) {
                throw new NotImplementedException(
                        "Relational mapping without ~mainTable, sourceUrl, or an"
                      + " inferable column binding; class="
                      + rcm.className() + ", mapping=" + md.qualifiedName()
                      + ". See docs/MAPPING_LEGACY_TO_FUNCTION.md §5.2.3.");
            }
            rcm = new ClassMapping.Relational(rcm.className(), rcm.setId(),
                    rcm.extendsSetId(), rcm.root(), inferred, rcm.filter(),
                    rcm.distinct(), rcm.groupBy(), rcm.primaryKey(),
                    rcm.propertyMappings(), rcm.sourceUrl(),
                    rcm.propertyTargetSets());
        }
        DatabaseDefinition.ViewDefinition view = model.findView(
                rcm.mainTable().database(), rcm.mainTable().table()).orElse(null);
        if (view != null) {
            return synthViewBackedMapping(md, rcm, view, model);
        }
        return synthTableBackedMapping(md, rcm, model);
    }

    /**
     * The engine's inferred main table when {@code ~mainTable} is absent:
     * every property mapping's DIRECT (non-join) table must agree on ONE
     * table (RelationalCompilerExtension collects all aliases and errors on
     * more than one distinct table — "Please specify a main table"). First
     * table wins only when it is the SOLE table; disagreement is loud.
     */
    static LegacyMappingDefinition.TableReference inferMainTable(
            ClassMapping.Relational rcm) {
        List<LegacyMappingDefinition.TableReference> refs = new ArrayList<>();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            collectMainTables(pm, refs);
        }
        // the DATABASE is part of table identity: [db1]T and [db2]T are
        // different tables — table-name-only dedup would silently pick
        // refs.get(0)'s database. Null (scope-block) refs inherit the
        // explicit one, so only NON-NULL databases enter the check.
        Set<String> dbs = new LinkedHashSet<>();
        refs.forEach(r -> {
            if (r.database() != null) {
                dbs.add(r.database());
            }
        });
        if (dbs.size() > 1) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "Inconsistent database definitions for the mapping of class '"
                  + rcm.className() + "': " + dbs);
        }
        Set<String> names = new LinkedHashSet<>();
        refs.forEach(r -> names.add(canonicalTable(r.table())));
        if (names.size() > 1) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "Can't find the main table for class '" + rcm.className()
                  + "': property mappings span tables " + names
                  + ". Please specify a main table using the ~mainTable directive.");
        }
        if (refs.isEmpty()) {
            return null;
        }
        // return the CANONICAL name: scope([db]default.personTable) columns
        // record 'default.personTable' while join conditions and the store
        // lookup say 'personTable' — the default-schema prefix is spelling,
        // not identity
        LegacyMappingDefinition.TableReference first = refs.get(0);
        return new LegacyMappingDefinition.TableReference(first.database(),
                canonicalTable(first.table()));
    }

    /** 'default.T' and 'T' are the same table — the default-schema prefix
     * is spelling, not identity. THE one canonicalization site (audit 15:
     * RelOpTranslator spelled it independently). */
    static String canonicalTable(String table) {
        return table.startsWith("default.")
                ? table.substring("default.".length()) : table;
    }

    /** {@link #inferMainTable} as a PROBE: null on ambiguity instead of loud. */
    private static LegacyMappingDefinition.TableReference inferMainTableQuiet(
            ClassMapping.Relational rcm) {
        try {
            return inferMainTable(rcm);
        } catch (ModelException e) {
            return null;
        }
    }

    private static void collectMainTables(PropertyMapping pm,
            List<LegacyMappingDefinition.TableReference> sink) {
        switch (pm) {
            case PropertyMapping.Column c ->
                    sink.add(new LegacyMappingDefinition.TableReference(c.database(), c.table()));
            case PropertyMapping.EnumeratedColumn ec ->
                    sink.add(new LegacyMappingDefinition.TableReference(ec.database(), ec.table()));
            case PropertyMapping.Embedded emb ->
                    emb.propertyMappings().forEach(inner -> collectMainTables(inner, sink));
            case PropertyMapping.LocalProperty lp -> collectMainTables(lp.body(), sink);
            case PropertyMapping.Expression ex -> {
                // a computed column reads its columns off the main table —
                // unless it navigates a join (those reference OTHER tables)
                List<JoinChainEmission.JoinNavSpec> navs = new ArrayList<>();
                JoinChainEmission.collectJoinNavigations(ex.expression(), navs);
                if (navs.isEmpty()) {
                    collectExprTables(ex.expression(), sink);
                }
            }
            // DELIBERATE non-contributors (audit 15: exhaustive, no default —
            // a new PM kind must state its main-table stance here): joins
            // and join terminals reference OTHER tables; enum expressions
            // and inline/otherwise embeddeds carry no direct column.
            case PropertyMapping.Join ignored -> { }
            case PropertyMapping.JoinTerminalColumn ignored -> { }
            case PropertyMapping.EnumeratedExpression ignored -> { }
            case PropertyMapping.InlineEmbedded ignored -> { }
            // NOTE the asymmetry with Embedded (which recurses): the old
            // open default never recursed OtherwiseEmbedded — preserved
            // as-is; changing main-table inference needs its own probe.
            case PropertyMapping.OtherwiseEmbedded ignored -> { }
        }
    }

    private static void collectExprTables(RelationalOperation op,
            List<LegacyMappingDefinition.TableReference> sink) {
        if (op instanceof RelationalOperation.ColumnRef cr
                && cr.databaseName() != null && !cr.databaseName().isEmpty()) {
            sink.add(new LegacyMappingDefinition.TableReference(cr.databaseName(), cr.table()));
            return;
        }
        switch (op) {
            case RelationalOperation.FunctionCall fc ->
                    fc.args().forEach(a -> collectExprTables(a, sink));
            case RelationalOperation.Comparison c -> {
                collectExprTables(c.left(), sink);
                collectExprTables(c.right(), sink);
            }
            case RelationalOperation.BooleanOp b -> {
                collectExprTables(b.left(), sink);
                collectExprTables(b.right(), sink);
            }
            case RelationalOperation.IsNull n ->
                    collectExprTables(n.operand(), sink);
            case RelationalOperation.IsNotNull n ->
                    collectExprTables(n.operand(), sink);
            case RelationalOperation.Group g ->
                    collectExprTables(g.inner(), sink);
            case RelationalOperation.ArrayLiteral a ->
                    a.elements().forEach(e -> collectExprTables(e, sink));
            // DELIBERATE non-contributors (audit 15: exhaustive, no default):
            // literals/type refs carry no table; a bare/target column ref
            // without a database qualifier cannot name one; join navigations
            // reference OTHER tables by construction.
            case RelationalOperation.ColumnRef ignored -> { }
            case RelationalOperation.TargetColumnRef ignored -> { }
            case RelationalOperation.Literal ignored -> { }
            case RelationalOperation.TypeRef ignored -> { }
            case RelationalOperation.JoinNavigation ignored -> { }
        }
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
                new ModelException(LegendCompileException.Phase.NORMALIZE, "JSON-source mapping references unknown class '"
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
                        List.of(buildNewInstanceToOne(rcm.className(), fields, model)))));
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
        // A GROUPED view cannot flatten into the class mapping: the merged
        // ~groupBy would run AFTER navigate injection and discard the
        // navigation slots (V1b, probe-confirmed on AccountPnl). The view
        // is a ROW-DEFINING SUBSELECT — it becomes the pipeline SOURCE
        // (columns projected by NAME, ~filter/~groupBy/~distinct inside),
        // PMs read view columns VERBATIM, and the view name is the source
        // row scope. Plain views keep the flattening path below (it also
        // handles join-navigating view columns, which the subselect
        // expansion walls loudly).
        if (!view.groupByColumns().isEmpty()) {
            ValueSpecification viewSource = ViewRelation.viewRelationExpr(
                    view, rcm.mainTable().table(), mainDb, model, md);
            ClassMapping.Relational overView = new ClassMapping.Relational(
                    rcm.className(), rcm.setId(), rcm.extendsSetId(), rcm.root(),
                    rcm.mainTable(), rcm.filter(), rcm.distinct(), rcm.groupBy(),
                    rcm.primaryKey(), rcm.propertyMappings(), null,
                    rcm.propertyTargetSets());
            return synthTableBackedMapping(md, overView, model,
                    /*backingView*/ null, viewSource);
        }
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
                rewrittenPms, null, rcm.propertyTargetSets());
        ValueSpecification body = synthTableBackedMapping(md, effective, model,
                rcm.mainTable().table());
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
            case PropertyMapping.EnumeratedExpression ee -> ee;
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
            throw new NotImplementedException(
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
                .orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE, 
                        "~filter '" + direct.filter().name() + "' not found in db '"
                      + dbFqn + "'; class=" + rcm.className() + ", mapping="
                      + md.qualifiedName()));
        ValueSpecification source = map.parameters().get(0);
        ValueSpecification mapLambda = map.parameters().get(1);
        Variable rowBind = new Variable("row");
        Map<String, ValueSpecification> scope = Map.of(physicalTable, rowBind);
        ValueSpecification cond = RelOpTranslator.translate(fd.condition(), scope, null, rowBind, RelOpTranslator.PipelineView.NONE);
        ValueSpecification filtered = filterBelowAggregation(source,
                new LambdaFunction(List.of(rowBind), List.of(cond)));
        return new AppliedFunction("map", List.of(filtered, mapLambda));
    }

    /**
     * Insert the mapping filter BENEATH the class pipeline's groupBy /
     * distinct nodes: the engine evaluates the view filter AND the mapping
     * filter both in WHERE position, before aggregation. Filtering the
     * grouped relation silently turned {@code WHERE amount > 10} into
     * {@code HAVING sum(amount) > 10} whenever a grouped output column
     * kept the filtered physical name (audit 18 finding 6). Only reached
     * from the flattening path, where every groupBy/distinct in the source
     * chain is class-pipeline-level (grouped views return earlier with the
     * view as an opaque source subselect).
     */
    private static ValueSpecification filterBelowAggregation(
            ValueSpecification src, LambdaFunction pred) {
        // distinct commutes with a row-level predicate — descend through it
        // only to reach a groupBy beneath (the pinned canonical form keeps
        // the mapping filter ABOVE a bare distinct)
        boolean descend = src instanceof AppliedFunction af
                && ("groupBy".equals(af.function())
                        || ("distinct".equals(af.function())
                                && chainHasGroupBy(af.parameters().get(0))));
        if (descend) {
            AppliedFunction af = (AppliedFunction) src;
            List<ValueSpecification> ps = new ArrayList<>(af.parameters());
            ps.set(0, filterBelowAggregation(ps.get(0), pred));
            return new AppliedFunction(af.function(), ps);
        }
        return new AppliedFunction("filter", List.of(src, pred));
    }

    private static boolean chainHasGroupBy(ValueSpecification v) {
        while (v instanceof AppliedFunction af && !af.parameters().isEmpty()) {
            if ("groupBy".equals(af.function())) {
                return true;
            }
            v = af.parameters().get(0);
        }
        return false;
    }

    /**
     * Infer the view's single underlying physical table by scanning its
     * non-join column expressions for {@link RelationalOperation.ColumnRef}
     * tables (engine parity: {@code PureModelBuilder.inferViewMainTable}).
     * Columns whose expression navigates a join are skipped &mdash; they
     * reference joined tables, not the view's root. Exactly one root table
     * must remain, else fail loudly.
     */
    static String inferViewMainTable(DatabaseDefinition.ViewDefinition view,
                                            String viewName, LegacyMappingDefinition md) {
        Set<String> tables = new LinkedHashSet<>();
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
            RelationalOperation expr = vc.expression();
            List<JoinChainEmission.JoinNavSpec> navs = new ArrayList<>();
            JoinChainEmission.collectJoinNavigations(expr, navs);
            if (!navs.isEmpty()) continue;   // joined column — not the view's root table
            RelOpTranslator.collectTablesIn(expr, tables);
        }
        if (tables.isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "View '" + viewName + "': cannot infer underlying main table — no "
                  + "non-join column references found; mapping=" + md.qualifiedName());
        }
        if (tables.size() > 1) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "View '" + viewName + "' references multiple root tables " + tables
                  + "; a view must resolve to a single root table. Mapping="
                  + md.qualifiedName());
        }
        return tables.iterator().next();
    }

    // ====================================================================
    // Table-backed mapping  —  the main pipeline synthesis
    // ====================================================================

    /** A synthesized relational body BEFORE its map terminal composes. */
    record RelationalParts(ValueSpecification pipeline, Variable rowBind,
                           Map<String, KeyExpression> fields) {
    }

    private static ValueSpecification synthTableBackedMapping(LegacyMappingDefinition md,
                                                              ClassMapping.Relational rcm,
                                                              ModelBuilder model) {
        return synthTableBackedMapping(md, rcm, model, null);
    }

    private static ValueSpecification synthTableBackedMapping(LegacyMappingDefinition md,
                                                              ClassMapping.Relational rcm,
                                                              ModelBuilder model,
                                                              String backingView) {
        return synthTableBackedMapping(md, rcm, model, backingView, null);
    }

    private static ValueSpecification synthTableBackedMapping(LegacyMappingDefinition md,
                                                              ClassMapping.Relational rcm,
                                                              ModelBuilder model,
                                                              String backingView,
                                                              ValueSpecification sourceOverride) {
        RelationalParts parts = synthTableBackedParts(md, rcm, model, backingView,
                sourceOverride);
        return new AppliedFunction("map", List.of(parts.pipeline(),
                new LambdaFunction(List.of(parts.rowBind()),
                        List.of(buildNewInstanceToOne(rcm.className(), parts.fields(), model)))));
    }

    static RelationalParts synthTableBackedParts(LegacyMappingDefinition md,
                                                             ClassMapping.Relational rcm,
                                                             ModelBuilder model,
                                                              String backingView) {
        return synthTableBackedParts(md, rcm, model, backingView, null);
    }

    /**
     * {@code sourceOverride}: a NON-TABLE source relation (a grouped view's
     * subselect — V1b): the pipeline starts there instead of
     * {@code tableReference(mainTable)}, and {@code rcm.mainTable().table()}
     * names the SOURCE ROW SCOPE (the view name) that column PMs and join
     * conditions resolve against.
     */
    static RelationalParts synthTableBackedParts(LegacyMappingDefinition md,
                                                             ClassMapping.Relational rcm,
                                                             ModelBuilder model,
                                                              String backingView,
                                                              ValueSpecification sourceOverride) {
        validatePmNames(rcm, model, md);

        // A mapping ~filter with an EXPLICIT (INNER) join type row-explodes:
        // the engine swaps the main table for a subselect that joins the
        // filter chain, applies the condition, and projects every base
        // column under its original name (getRelationalElementWithInnerJoin,
        // pureToSQLQuery.pure:5061-5074) — one row PER MATCHING CHILD
        // survives (testInnerJoinClassMappingFilterWithChainedJoins expects
        // Firm X x4). The exists-shaped filter route below keeps one row
        // per parent, so it cannot serve this form.
        if (sourceOverride == null
                && rcm.filter() instanceof FilterMapping.JoinMediated jmi
                && jmi.joinType() != null) {
            ValueSpecification innerSrc = JoinChainEmission.innerFilteredSource(rcm, jmi, model, md);
            ClassMapping.Relational noFilter = new ClassMapping.Relational(
                    rcm.className(), rcm.setId(), rcm.extendsSetId(), rcm.root(),
                    rcm.mainTable(), null, rcm.distinct(), rcm.groupBy(),
                    rcm.primaryKey(), rcm.propertyMappings(), null,
                    rcm.propertyTargetSets());
            return synthTableBackedParts(md, noFilter, model, backingView, innerSrc);
        }

        String mainDb    = rcm.mainTable().database();
        String mainTable = rcm.mainTable().table();
        Variable rowBind = new Variable("row");

        // Query-parser parity (H1): the database is a PackageableElementPtr,
        // the table a string — the same shapes #>{db.TABLE}# produces, so
        // TableReferenceChecker serves both surfaces.
        Pipeline p = new Pipeline(sourceOverride != null ? sourceOverride
                : new AppliedFunction("tableReference",
                        List.of(new PackageableElementPtr(mainDb), new CString(mainTable))),
                backingView);
        UnionSynthesis.classifyUnionRoutes(md, rcm, model, p);

        // Pass 1: structural chain emission (Join, JoinTerminalColumn,
        // LocalProperty-wrapping-JTC). Class-typed Join PMs to mapped
        // targets emit a final-hop legacyNavigate.
        for (PropertyMapping pm : rcm.propertyMappings()) {
            JoinChainEmission.emitHopsForStructuralPm(p, pm, rcm.className(), mainDb, mainTable,
                    rowBind, model, md);
        }
        // Pass 2: hoist JoinNavigation chains nested inside Expression
        // bodies, LocalProperty wrappers, Embedded sub-PMs,
        // OtherwiseEmbedded eager/fallback bodies, groupBy keys, and
        // Direct filter conditions. Each unique chain becomes a clean
        // join(...) step; dedup via aliasToTargetTable.
        List<JoinChainEmission.JoinNavSpec> nested = new ArrayList<>();
        JoinChainEmission.collectJoinNavigationsInPms(rcm.propertyMappings(), nested);
        for (RelationalOperation key : rcm.groupBy()) {
            JoinChainEmission.collectJoinNavigations(key, nested);
        }
        if (rcm.filter() instanceof FilterMapping.Direct dfilt) {
            String dbFqn = switch (dfilt.filter()) {
                case FilterPointer.Cross c -> c.db();
                case FilterPointer.Local l -> mainDb;
            };
            model.findFilter(dbFqn, dfilt.filter().name()).ifPresent(fd ->
                    JoinChainEmission.collectJoinNavigations(fd.condition(), nested));
        }
        for (JoinChainEmission.JoinNavSpec spec : nested) {
            JoinChainEmission.emitJoinChain(p, spec.chain(), spec.chainDb(),
                    /* propName */ null, rcm.className(),
                    mainDb, mainTable, rowBind, model, md,
                    /* classTypedTerminus */ false);
        }
        // Pass 3: JoinMediated filter's join chain (extends pipeline).
        if (rcm.filter() instanceof FilterMapping.JoinMediated jm) {
            JoinChainEmission.emitJoinChain(p, jm.joins(), jm.sourceDb(),
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

        // Apply ~distinct. Engine semantics: DISTINCT over the MAPPED
        // columns, not the raw physical row (the table's unmapped PK would
        // defeat the dedup) — the source narrows to a select of exactly the
        // columns the PMs consume. Only for plain column/expression PMs on
        // the main table; slot-carrying distinct mappings stay the
        // H3-pending wall downstream.
        if (rcm.distinct()) {
            Set<String> mappedCols = new LinkedHashSet<>();
            boolean plainColumns = p.aliasToTargetTable.isEmpty();
            for (PropertyMapping pm : rcm.propertyMappings()) {
                plainColumns &= collectMappedColumns(pm, mappedCols);
            }
            if (plainColumns && !mappedCols.isEmpty()) {
                List<ColSpec> cols = mappedCols.stream()
                        .map(c -> new ColSpec(c, null, null)).toList();
                p.expr = new AppliedFunction("select",
                        List.of(p.expr, new ColSpecArray(cols)));
                p.expr = new AppliedFunction("distinct", List.of(p.expr));
            } else if (!mappedCols.isEmpty()) {
                // SLOT-CARRYING ~distinct: dedup by the mapped MAIN-TABLE
                // columns (the unmapped PK must not defeat the dedup —
                // engine dedups the mapped row) PLUS the slot pseudo-columns
                // (so slot reads above the distinct still type-check); the
                // materializer swaps each demanded slot for its prefixed
                // physical columns (join-equality makes them dependent,
                // dedup-neutral) and drops the undemanded ones.
                List<ColSpec> cols = new ArrayList<>(mappedCols.stream()
                        .map(c -> new ColSpec(c, null, null)).toList());
                for (String alias : p.aliasToTargetTable.keySet()) {
                    cols.add(new ColSpec(alias, null, null));
                }
                p.expr = new AppliedFunction("distinct",
                        List.of(p.expr, new ColSpecArray(cols)));
            } else {
                p.expr = new AppliedFunction("distinct", List.of(p.expr));
            }
        }

        // Terminal: map(row | ^Class(...)).
        Map<String, ValueSpecification> tableScope = new LinkedHashMap<>();
        tableScope.put(mainTable, rowBind);
        seedAliasScope(tableScope, p, rowBind, mainTable);

        Map<String, KeyExpression> fields = new LinkedHashMap<>();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            if (p.droppedRoutedProps.contains(pm.propertyName())) {
                continue;   // dropped route: no binding (loud at demand)
            }
            CtorField cf = translatePmToField(pm, rowBind, tableScope, mainTable, p,
                    rcm.className(), md, model, !rcm.groupBy().isEmpty());
            fields.put(cf.name(), new KeyExpression(cf.value(), false, cf.isLocal()));
        }
        return new RelationalParts(p.expr, rowBind, fields);
    }

    /**
     * The physical COLUMNS a plain PM consumes (the ~distinct projection
     * set). False = the PM is not a plain main-table read (join/embedded) —
     * the caller skips the narrowing select.
     */
    private static boolean collectMappedColumns(PropertyMapping pm, Set<String> sink) {
        switch (pm) {
            case PropertyMapping.Column c -> sink.add(c.column());
            case PropertyMapping.EnumeratedColumn ec -> sink.add(ec.column());
            case PropertyMapping.Expression e -> collectExprColumns(e.expression(), sink);
            case PropertyMapping.EnumeratedExpression ee ->
                    collectExprColumns(ee.expression(), sink);
            case PropertyMapping.LocalProperty lp -> {
                return collectMappedColumns(lp.body(), sink);
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    private static void collectExprColumns(RelationalOperation op,
            Set<String> sink) {
        switch (op) {
            case RelationalOperation.ColumnRef cr -> sink.add(cr.column());
            case RelationalOperation.FunctionCall fc ->
                    fc.args().forEach(a -> collectExprColumns(a, sink));
            case RelationalOperation.Comparison c -> {
                collectExprColumns(c.left(), sink);
                collectExprColumns(c.right(), sink);
            }
            case RelationalOperation.BooleanOp b -> {
                collectExprColumns(b.left(), sink);
                collectExprColumns(b.right(), sink);
            }
            case RelationalOperation.IsNull n ->
                    collectExprColumns(n.operand(), sink);
            case RelationalOperation.IsNotNull n ->
                    collectExprColumns(n.operand(), sink);
            case RelationalOperation.Group g ->
                    collectExprColumns(g.inner(), sink);
            case RelationalOperation.ArrayLiteral a ->
                    a.elements().forEach(e -> collectExprColumns(e, sink));
            default -> { }
        }
    }

    /**
     * Engine semantics for EXPRESSION property mappings: the dynafunction's
     * result coerces to the property's declared type at the SQL boundary
     * (abs(...) types Number, the property says Float — the engine compiles
     * and the database delivers the declared kind). NUMERIC declared types
     * wrap in cast(@Declared); everything else passes through untouched so
     * genuine kind errors stay loud.
     */
    static ValueSpecification coerceToDeclaredNumeric(ValueSpecification value,
            String propName, String ownerClassFqn, ModelBuilder model) {
        String simple = declaredPlatformKind(propName, ownerClassFqn, model);
        if (simple == null || !Set.of("Float", "Integer", "Decimal",
                "Number", "DateTime", "StrictDate", "Date").contains(simple)) {
            return value;
        }
        return new AppliedFunction("cast", List.of(value,
                new TypeAnnotation.Named(
                        new TypeExpression.NameRef(simple))));
    }

    /**
     * The declared property type's PLATFORM primitive simple name, or null
     * for class/enum-typed and shadowed names. Identified exactly: the bare
     * spelling (not shadowed by a user class — {@code m::Number} must never
     * coerce) or the full platform FQN. Suffix-matching is the banned idiom.
     */
    private static String declaredPlatformKind(String propName, String ownerClassFqn,
            ModelBuilder model) {
        ClassDefinition owner = model.findClass(ownerClassFqn).orElse(null);
        TypeExpression t = owner == null ? null
                : findPropertyTypeDeep(owner, propName, model);
        String name = t instanceof TypeExpression.NameRef nr ? nr.name() : null;
        if (name == null) {
            return null;
        }
        if (!name.contains("::") && model.findClass(name).isEmpty()) {
            return name;
        }
        if (name.startsWith("meta::pure::metamodel::type::")) {
            return name.substring("meta::pure::metamodel::type::".length());
        }
        return null;
    }

    /**
     * A plain column PM whose PHYSICAL kind disagrees with the declared
     * property type casts at the boundary ({@code id: String[1]} over an
     * INTEGER column — engine relational execution coerces to the property
     * type on the wire). Matching kinds and subsuming declarations
     * ({@code Number} over any numeric, {@code Date} over any temporal)
     * emit NO cast; unknown columns and non-primitive declarations pass
     * through (the type checker stays the loud arbiter).
     */
    /** {@code typeAsDeclared(read, @Declared)} when the terminal column's
     * physical kind differs from the declared platform kind — the TYPE-ONLY
     * assertion (no SQL); matching kinds pass through untouched. */
    private static ValueSpecification declaredAssertion(ValueSpecification read,
            PropertyMapping.JoinTerminalColumn jtc, String ownerClassFqn,
            ModelBuilder model) {
        String declared = declaredPlatformKind(jtc.propertyName(),
                ownerClassFqn, model);
        if (declared == null
                || !(jtc.terminalColumn()
                        instanceof RelationalOperation.ColumnRef cr)) {
            return read;
        }
        String db = cr.databaseName() != null ? cr.databaseName()
                : jtc.database();
        if (db == null) {
            return read;
        }
        DatabaseDefinition.ColumnDefinition cd = findPhysicalColumn(
                db, cr.table(), cr.column(), model);
        String colKind = cd == null ? null : pureKindOf(cd.dataType());
        if (colKind == null || colKind.equals(declared)) {
            return read;
        }
        return new AppliedFunction("typeAsDeclared", List.of(read,
                new TypeAnnotation.Named(
                        new TypeExpression.NameRef(declared))));
    }

    private static ValueSpecification coerceColumnToDeclared(ValueSpecification read,
            PropertyMapping.Column col,
            String ownerClassFqn, ModelBuilder model) {
        String declared = declaredPlatformKind(col.propertyName(), ownerClassFqn, model);
        if (declared == null) {
            return read;
        }
        // scope-block columns carry no [db] — skip (checker stays loud)
        String db = col.database();
        DatabaseDefinition.ColumnDefinition cd =
                findPhysicalColumn(db, col.table(), col.column(), model);
        String colKind = cd == null ? null : pureKindOf(cd.dataType());
        if (colKind == null || colKind.equals(declared)) {
            return read;
        }
        // Only conversions the engine's runtime transformer actually
        // performs (Boolean, Date-family) or that are lossless (*->String,
        // DATE widening to DateTime) may cast. NARROWING casts (Integer
        // over DECIMAL, StrictDate over TIMESTAMP) match no engine
        // behavior — they'd silently truncate values the engine preserves
        // (audit 8 S4); those fall through uncast and the type checker
        // stays the loud arbiter.
        boolean emit = switch (declared) {
            case "String" -> true;
            case "Boolean" -> true;
            case "DateTime" -> "StrictDate".equals(colKind);
            default -> false;
        };
        if (!emit) {
            return read;
        }
        return new AppliedFunction("cast", List.of(read,
                new TypeAnnotation.Named(
                        new TypeExpression.NameRef(declared))));
    }

    /** The pure primitive kind a physical SQL type reads as, or null. */
    static String pureKindOf(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.Varchar v -> "String";
            case RelationalDataType.Char_ c -> "String";
            case RelationalDataType.BigInt b -> "Integer";
            case RelationalDataType.SmallInt s -> "Integer";
            case RelationalDataType.TinyInt s -> "Integer";
            case RelationalDataType.Integer_ i -> "Integer";
            case RelationalDataType.Float_ f -> "Float";
            case RelationalDataType.Double_ d -> "Float";
            case RelationalDataType.Real r -> "Float";
            case RelationalDataType.Decimal d -> "Decimal";
            case RelationalDataType.Numeric n -> "Decimal";
            case RelationalDataType.Bool b -> "Boolean";
            case RelationalDataType.Bit b -> "Boolean";
            case RelationalDataType.Timestamp ts -> "DateTime";
            case RelationalDataType.Date_ d -> "StrictDate";
            default -> null;
        };
    }

    /** The column's declared SQL type — schema-aware, include-walking. */
    static DatabaseDefinition.ColumnDefinition findPhysicalColumn(
            String dbFqn, String table, String column, ModelBuilder model) {
        if (dbFqn == null || table == null) {
            return null;
        }
        String t = canonicalTable(table);
        String schema = null;
        int dot = t.indexOf('.');
        if (dot > 0) {
            schema = t.substring(0, dot);
            t = t.substring(dot + 1);
        }
        return findPhysicalColumn(dbFqn, schema, t, column, model,
                new HashSet<>());
    }

    static DatabaseDefinition.ColumnDefinition findPhysicalColumn(
            String dbFqn, String schema, String table, String column,
            ModelBuilder model, Set<String> seen) {
        if (!seen.add(dbFqn)) {
            return null;
        }
        DatabaseDefinition db = model.findDatabase(dbFqn).orElse(null);
        if (db == null) {
            return null;
        }
        List<DatabaseDefinition.TableDefinition> tables = new ArrayList<>(db.tables());
        for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
            if (schema == null || s.name().equals(schema)) {
                tables.addAll(s.tables());
            }
        }
        for (DatabaseDefinition.TableDefinition td : tables) {
            if (td.name().equalsIgnoreCase(table)) {
                for (DatabaseDefinition.ColumnDefinition cd : td.columns()) {
                    if (cd.name().equalsIgnoreCase(column)) {
                        return cd;
                    }
                }
            }
        }
        for (String inc : db.includes()) {
            DatabaseDefinition.ColumnDefinition hit =
                    findPhysicalColumn(inc, schema, table, column, model, seen);
            if (hit != null) {
                return hit;
            }
        }
        return null;
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
                throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                        "PropertyMapping '" + pm.propertyName() + "' references property "
                      + "not declared on class '" + rcm.className() + "'; mapping="
                      + md.qualifiedName());
            }
        }
    }

    // ====================================================================
    // Hop emission  —  Pass 1 (structural) and Pass 2 (nested JoinNav)
    // ====================================================================

    // ====================================================================
    // JoinNavigation collection (Pass 2 hoisting source)
    // ====================================================================

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
            case PropertyMapping.EnumeratedExpression ee -> new CtorField(ee.propertyName(),
                    translateEnumeratedSource(ee.propertyName(), ee.enumMappingId(),
                            RelOpTranslator.translate(ee.expression(), tableScope, null,
                                    rowBind, pipeline.view()),
                            md, ownerClassFqn, model),
                    false);
            case PropertyMapping.Column col -> new CtorField(col.propertyName(),
                    coerceColumnToDeclared(
                            RelOpTranslator.columnRead(col.table(), col.column(), tableScope, defaultTable, pipeline.view()),
                            col, ownerClassFqn, model),
                    false);
            case PropertyMapping.EnumeratedColumn ec -> new CtorField(ec.propertyName(),
                    translateEnumeratedColumn(ec, tableScope, defaultTable, md, pipeline,
                            ownerClassFqn, model),
                    false);
            case PropertyMapping.Expression expr -> new CtorField(expr.propertyName(),
                    coerceToDeclaredNumeric(
                            RelOpTranslator.translate(expr.expression(), tableScope, null,
                                    rowBind, pipeline.view()),
                            expr.propertyName(), ownerClassFqn, model),
                    false);
            case PropertyMapping.Join j -> {
                String targetIfMapped = JoinChainEmission.classTypedTargetIfMapped(ownerClassFqn,
                        j.propertyName(), model);
                String slot = targetIfMapped != null
                        ? pipeline.navSlotByProp.getOrDefault(
                                j.propertyName(), j.propertyName())
                        : JoinChainEmission.slotFor(pipeline, j.joins());
                yield new CtorField(j.propertyName(),
                        new AppliedProperty(rowBind, slot), false);
            }
            case PropertyMapping.JoinTerminalColumn jtc -> {
                String alias = JoinChainEmission.slotFor(pipeline, jtc.joins());
                ValueSpecification subRow = new AppliedProperty(rowBind, alias);
                Map<String, ValueSpecification> scope = new LinkedHashMap<>(tableScope);
                String terminalTable = pipeline.aliasToTargetTable.get(alias);
                if (terminalTable != null) scope.put(terminalTable, subRow);
                ValueSpecification read = RelOpTranslator.translate(
                        jtc.terminalColumn(), scope, null, rowBind, pipeline.view());
                yield new CtorField(jtc.propertyName(),
                        jtc.enumMapped()
                                ? translateEnumeratedSource(jtc.propertyName(),
                                        jtc.enumMappingId(), read, md,
                                        ownerClassFqn, model)
                                // engine parity: a binding read TYPES AS
                                // the DECLARED property with NO SQL cast
                                // (Integer property over a DOUBLE column —
                                // the engine's rows are the raw doubles);
                                // wrapped only on a genuine kind mismatch
                                : declaredAssertion(read, jtc,
                                        ownerClassFqn, model),
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
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "Embedded PM '" + propName + "' on '" + ownerClassFqn
                  + "' but owner class unknown; mapping=" + md.qualifiedName());
        }
        TypeExpression propType = findPropertyTypeDeep(owner, propName, model);
        if (!(propType instanceof TypeExpression.NameRef nr)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "Embedded PM '" + propName + "' on '" + ownerClassFqn
                  + "' has non-class property type; mapping=" + md.qualifiedName());
        }
        String innerFqn = nr.name();
        if (!cycleStack.add(innerFqn)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "Cycle materializing Embedded; class " + innerFqn
                  + " recurses via '" + propName + "' on '" + ownerClassFqn + "'");
        }
        try {
            Map<String, KeyExpression> fields = new LinkedHashMap<>();
            for (PropertyMapping sub : subPms) {
                // Join sub-PMs read the slot Pass 1 hoisted into the TOP
                // pipeline (the embedded instance shares the owner's row) —
                // translatePmToField's Join arm resolves it via innerFqn.
                // An UNMAPPED target class has no instance to bind: wall.
                if (sub instanceof PropertyMapping.Join j
                        && JoinChainEmission.classTypedTargetIfMapped(innerFqn, j.propertyName(), model) == null) {
                    throw new NotImplementedException(
                            "Embedded sub-PM '" + j.propertyName() + "' on '"
                          + propName + "' is a class-typed Join to an UNMAPPED"
                          + " target class — no instance to bind. Mapping="
                          + md.qualifiedName());
                }
                CtorField cf = translatePmToField(sub, rowBind, tableScope,
                        defaultTable, pipeline, innerFqn, md, model, false);
                fields.put(cf.name(),
                        new KeyExpression(cf.value(), false, cf.isLocal()));
            }
            return buildNewInstanceToOne(innerFqn, fields, model);
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
                    && Objects.equals(setIdOf(rcm), ie.setId())) {
                referenced = rcm;
                break;
            }
        }
        if (referenced == null) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
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
                dbFqn, direct.filter().name()).orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE, 
                "~filter '" + direct.filter().name() + "' not found in db '"
              + dbFqn + "'; class=" + rcm.className() + ", mapping="
              + md.qualifiedName()));
        Map<String, ValueSpecification> scope = new LinkedHashMap<>();
        scope.put(mainTable, rowBind);
        seedAliasScope(scope, p, rowBind, mainTable);
        ValueSpecification cond = RelOpTranslator.translate(fd.condition(), scope, null, rowBind, p.view());
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
                dbFqn, jm.filter().name()).orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE, 
                "~filter '" + jm.filter().name() + "' not found in db '"
              + dbFqn + "'; class=" + rcm.className() + ", mapping="
              + md.qualifiedName()));
        // The chain was emitted as join hops in Pass 3 of
        // synthTableBackedMapping. The terminal-table row path is at
        // $row.<terminalAlias>. Build scope that maps the filter's
        // condition tables (typically the chain's terminal table) to
        // the appropriate row path.
        String terminalAlias = JoinChainEmission.slotFor(p, jm.joins());
        ValueSpecification terminalRow = new AppliedProperty(rowBind, terminalAlias);
        Map<String, ValueSpecification> scope = new LinkedHashMap<>();
        scope.put(mainTable, rowBind);
        seedAliasScope(scope, p, rowBind, mainTable);
        String terminalTable = p.aliasToTargetTable.get(terminalAlias);
        if (terminalTable != null) scope.putIfAbsent(terminalTable, terminalRow);
        ValueSpecification cond = RelOpTranslator.translate(fd.condition(), scope, terminalRow,
                rowBind, p.view());
        // The absorption theory (LEFT slot + WHERE ≡ INNER) was REFUTED by
        // the corpus referee: an (INNER) filter through a TO-MANY chain
        // ROW-EXPLODES the parent (testInnerJoinClassMappingFilterWith-
        // ChainedJoins expects Firm X x4 — one per matching Person row);
        // our slot emission keeps one row per parent. Loud until the
        // row-exploding emission is built.
        if (jm.joinType() != null) {
            throw new NotImplementedException("mapping ~filter with an"
                    + " explicit (" + jm.joinType() + ") join type"
                    + " row-explodes through to-many chains — not built yet;"
                    + " class=" + rcm.className() + ", mapping="
                    + md.qualifiedName());
        }
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
            // A JOIN PM (class-typed navigation) is not part of the grouped
            // ROW — the engine navigates grouped sets through the join
            // machinery, never the grouped projection. Stage 1 (audit-17
            // bucket analysis): WITHHOLD the property instead of sinking
            // the whole class mapping — queries that never touch it run;
            // touching it raises the ordinary not-mapped error, loud.
            if (pm instanceof PropertyMapping.Join) {
                continue;
            }
            RelationalOperation pmOp = pmAsRelationalOp(pm);
            if (pmOp == null) {
                throw new NotImplementedException(
                        "PropertyMapping '" + pm.getClass().getSimpleName()
                      + "' for property '" + pm.propertyName()
                      + "' is not supported under ~groupBy (only Column, Expression, "
                      + "JoinTerminalColumn, and aggregate Expression PMs are allowed). "
                      + "Mapping=" + md.qualifiedName());
            }
            int matchIdx = -1;
            for (int i = 0; i < keyOps.size(); i++) {
                if (!claimedKeys.contains(i) && groupByOpsMatch(pmOp, keyOps.get(i))) {
                    matchIdx = i; break;
                }
            }
            if (matchIdx < 0) {
                throw new NotImplementedException(
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
            ValueSpecification keyValue = RelOpTranslator.translate(keyOps.get(i), scope, null,
                    rowBind, p.view());
            keyCols.add(new ColSpec(keyNames[i],
                    new LambdaFunction(List.of(rowBind), List.of(keyValue)), null));
        }
        // Build aggregate ColSpecs with fn1 (selector) + fn2 (aggregate).
        List<ColSpec> aggCols = new ArrayList<>(aggPms.size());
        for (PropertyMapping pm : aggPms) {
            RelationalOperation.FunctionCall fc = (RelationalOperation.FunctionCall)
                    ((PropertyMapping.Expression) pm).expression();
            if (fc.args().size() != 1) {
                throw new NotImplementedException(
                        "Aggregate PM '" + pm.propertyName() + "' uses '" + fc.name()
                      + "' with " + fc.args().size() + " args; only single-argument "
                      + "aggregates lift to the two-stage AggColSpec form. Mapping="
                      + md.qualifiedName());
            }
            ValueSpecification selector = RelOpTranslator.translate(fc.args().get(0), scope, null,
                    rowBind, p.view());
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

    /**
     * Structural equality MODULO the database qualifier: a PM rewritten
     * through a view carries the mapping's db FQN while the view's own
     * ~groupBy keys parse unqualified — same table+column IS the same key.
     */
    static boolean groupByOpsMatch(RelationalOperation a, RelationalOperation b) {
        if (a instanceof RelationalOperation.ColumnRef ca
                && b instanceof RelationalOperation.ColumnRef cb) {
            // The qualifier is ignored only when ONE side lacks it (the
            // view-rewrite stamps the mapping's db onto PMs while view keys
            // parse unqualified); two EXPLICIT different dbs never match
            // (same-named tables across included dbs; audit).
            boolean dbOk = ca.databaseName() == null || cb.databaseName() == null
                    || ca.databaseName().equals(cb.databaseName());
            return dbOk && Objects.equals(ca.table(), cb.table())
                    && Objects.equals(ca.column(), cb.column());
        }
        return Objects.equals(a, b);
    }

    private static String keyBaseName(RelationalOperation op) {
        if (op instanceof RelationalOperation.ColumnRef cr) return cr.column();
        if (op instanceof RelationalOperation.TargetColumnRef tr) return tr.column();
        return null;
    }

    // ====================================================================
    // AssociationMapping → predicate function  —  doc §5.6.1
    // ====================================================================

    static boolean hasMainTable(LegacyMappingDefinition md, String classFqn,
            ModelBuilder model) {
        for (ClassMapping.Relational rcm
                : relationalMappingsInClosure(md, model, classFqn)) {
            if (rcm.mainTable() != null || inferMainTableQuiet(rcm) != null) {
                return true;
            }
        }
        return false;
    }

    /** {@code classFqn}'s Relational class mappings across the INCLUDE
     * CLOSURE, own mapping first (union V3: assoc mappings routinely live
     * in a mapping that only INCLUDES the class-mapping definitions). */
    private static List<ClassMapping.Relational> relationalMappingsInClosure(
            LegacyMappingDefinition md, ModelBuilder model, String classFqn) {
        List<LegacyMappingDefinition> closure = new ArrayList<>();
        collectMappingClosure(md, model, closure, new LinkedHashSet<>());
        List<ClassMapping.Relational> out = new ArrayList<>();
        for (LegacyMappingDefinition m : closure) {
            for (ClassMapping cm : m.classMappings()) {
                if (cm instanceof ClassMapping.Relational rcm
                        && classFqn.equals(rcm.className())) {
                    out.add(rcm);
                }
            }
        }
        return out;
    }

    /** {@code classFqn}'s ~mainTable declaration in {@code md} (loud if absent). */
    private static LegacyMappingDefinition.TableReference mainTableDefOf(
            LegacyMappingDefinition md, String classFqn, ModelBuilder model) {
        // The ROOT set's table — with multiple set IDs, .all() and every
        // synthesized association predicate anchor on the root; taking the
        // FIRST declared set bound predicates to the wrong table whenever a
        // non-root set was declared first (audit). Include-closure aware.
        LegacyMappingDefinition.TableReference first = null;
        for (ClassMapping.Relational rcm
                : relationalMappingsInClosure(md, model, classFqn)) {
            {
                LegacyMappingDefinition.TableReference mt = rcm.mainTable() != null
                        ? rcm.mainTable() : inferMainTableQuiet(rcm);
                if (mt == null) {
                    continue;
                }
                if (rcm.root()) {
                    return mt;
                }
                if (first == null) {
                    first = mt;
                }
            }
        }
        if (first != null) {
            return first;
        }
        throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                "No ~mainTable for class '" + classFqn + "' in mapping="
              + md.qualifiedName() + " (required to synthesize AssociationMapping)");
    }

    /** The {@code #>{db.T}#}-shaped source of {@code classFqn}'s ~mainTable row. */
    static ValueSpecification mainTableRefOf(LegacyMappingDefinition md, String classFqn,
            ModelBuilder model) {
        LegacyMappingDefinition.TableReference ref = mainTableDefOf(md, classFqn, model);
        return new AppliedFunction("tableReference", List.of(
                new PackageableElementPtr(ref.database()),
                new CString(ref.table())));
    }

    static String mainTableOf(LegacyMappingDefinition md, String classFqn,
            ModelBuilder model) {
        return mainTableDefOf(md, classFqn, model).table();
    }

    // ====================================================================
    // EnumeratedColumn  —  doc §5.4.2
    // ====================================================================

    private static ValueSpecification translateEnumeratedColumn(
            PropertyMapping.EnumeratedColumn ec,
            Map<String, ValueSpecification> tableScope,
            String defaultTable, LegacyMappingDefinition md, Pipeline p,
            String ownerClassFqn, ModelBuilder model) {
        ValueSpecification colRead = RelOpTranslator.columnRead(ec.table(), ec.column(),
                tableScope, defaultTable, p == null ? RelOpTranslator.PipelineView.NONE : p.view());
        return translateEnumeratedSource(ec.propertyName(), ec.enumMappingId(),
                colRead, md, ownerClassFqn, model);
    }

    /** This mapping's enum mappings PLUS its includes' (transitively; own first). */
    private static List<EnumerationMapping> enumerationMappingsWithIncludes(
            LegacyMappingDefinition md, ModelBuilder model) {
        List<EnumerationMapping> out = new ArrayList<>(md.enumerationMappings());
        Set<String> seen = new HashSet<>();
        collectIncludedEnumMappings(md, model, out, seen);
        return out;
    }

    private static void collectIncludedEnumMappings(LegacyMappingDefinition md,
            ModelBuilder model, List<EnumerationMapping> out, Set<String> seen) {
        for (MappingInclude inc : md.includes()) {
            if (!seen.add(inc.mappingPath())) {
                continue;
            }
            LegacyMappingDefinition included =
                    model.findLegacyMapping(inc.mappingPath()).orElse(null);
            if (included == null) {
                continue;
            }
            out.addAll(included.enumerationMappings());
            collectIncludedEnumMappings(included, model, out, seen);
        }
    }

    /**
     * The enum-decode if/equal chain over ANY source read — a column or a
     * translated expression ({@code role: EnumerationMapping M : case(...)},
     * constants included). Each mapped enum value tests its source values
     * in turn; no match yields {@code []}.
     */
    static ValueSpecification translateEnumeratedSource(
            String propertyName, String enumMappingId, ValueSpecification sourceRead,
            LegacyMappingDefinition md, String ownerClassFqn, ModelBuilder model) {
        EnumerationMapping em = null;
        List<EnumerationMapping> ems = enumerationMappingsWithIncludes(md, model);
        if (enumMappingId != null) {
            // engine getEnumerationMappingId (HelperMappingBuilder:348-351):
            // an anonymous enum mapping's IMPLICIT id is its enumeration FQN
            // with :: -> _ — references by that spelling resolve
            for (EnumerationMapping cand : ems) {
                String candId = cand.mappingId() != null ? cand.mappingId()
                        : cand.enumName().replace("::", "_");
                if (enumMappingId.equals(candId)) { em = cand; break; }
            }
        } else {
            // ANONYMOUS reference — resolved by the PROPERTY's declared enum
            // type. Names are FQNs here (NameResolver runs before the
            // normalizer). Two mappings for the SAME enum need the id
            // spelled — loud, never arbitrary.
            ClassDefinition owner = model.findClass(ownerClassFqn).orElse(null);
            TypeExpression propType = owner == null ? null
                    : findPropertyTypeDeep(owner, propertyName, model);
            String enumFqn = propType instanceof TypeExpression.NameRef nr ? nr.name() : null;
            for (EnumerationMapping cand : ems) {
                if (cand.enumName().equals(enumFqn)) {
                    if (em != null) {
                        throw new ModelException(
                                LegendCompileException.Phase.NORMALIZE,
                                "enum-mapped property '" + propertyName + "' uses an"
                              + " anonymous EnumerationMapping but '" + enumFqn
                              + "' has more than one — name the mapping id; mapping="
                              + md.qualifiedName());
                    }
                    em = cand;
                }
            }
        }
        if (em == null) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    enumMappingId != null
                            ? "enum-mapped property '" + propertyName + "' references unknown "
                                    + "enum mapping '" + enumMappingId + "'; mapping="
                                    + md.qualifiedName()
                            : "enum-mapped property '" + propertyName + "' uses an anonymous"
                                    + " EnumerationMapping but no mapping for the property's"
                                    + " enum type exists (or the property is not enum-typed);"
                                    + " mapping=" + md.qualifiedName());
        }
        ValueSpecification tail = new PureCollection(List.of());
        List<EnumerationMapping.EnumValueMapping> values = em.valueMappings();
        // An entry naming a NON-EXISTENT enum value is a COMPILE error —
        // the real engine resolves every entry against the enumeration
        // (HelperMappingBuilder.processEnumMapping); silently skipping it
        // turned typos into NULL rows in [1] slots (audit).
        List<String> knownValues = model.findEnum(em.enumName())
                .map(EnumDefinition::values).orElse(null);
        for (int i = values.size() - 1; i >= 0; i--) {
            EnumerationMapping.EnumValueMapping ev = values.get(i);
            if (knownValues != null && !knownValues.contains(ev.enumValue())) {
                throw new ModelException(
                        LegendCompileException.Phase.NORMALIZE,
                        "EnumerationMapping '" + em.mappingId() + "' maps value '"
                                + ev.enumValue() + "' which enumeration '"
                                + em.enumName() + "' does not declare");
            }
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
                    throw new ModelException(LegendCompileException.Phase.NORMALIZE, "Unhandled SourceValue: " + sv);
                }
                ValueSpecification eq = new AppliedFunction("equal",
                        List.of(sourceRead, srcLit));
                disj = disj == null ? eq
                        : new AppliedFunction("or", List.of(disj, eq));
            }
            if (disj == null) {
                throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                        "EnumerationMapping '" + enumMappingId + "' value '"
                      + ev.enumValue() + "' declares no source values; cannot build "
                      + "a match condition for property '" + propertyName
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

    static void seedAliasScope(Map<String, ValueSpecification> scope,
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

    /**
     * Substitute view-column refs whose view's physical root IS the source
     * relation. {@code onlyView} non-null = PASS-1 mode: substitute only
     * that view (the class's backing view — its row semantics already live
     * in the class pipeline), no guards; refs to other views pass through
     * so the caller can recognize a view TARGET.
     */
    static RelationalOperation resolveViewRefsInJoin(RelationalOperation op,
            String db, String sourceTable, ModelBuilder model, LegacyMappingDefinition md,
            String backingView, String onlyView) {
        return switch (op) {
            case RelationalOperation.ColumnRef cr -> {
                if (onlyView != null && !cr.table().equals(onlyView)) {
                    yield cr;
                }
                var view = model.findView(cr.databaseName() != null ? cr.databaseName() : db,
                        cr.table()).orElse(null);
                if (view == null) {
                    yield cr;
                }
                String phys = inferViewMainTable(view, cr.table(), md);
                if (!phys.equals(sourceTable)) {
                    yield cr;
                }
                if ((view.filter() != null || !view.groupByColumns().isEmpty()
                        || view.distinct()) && !cr.table().equals(backingView)
                        && onlyView == null) {
                    // substituting the column expression alone would DROP the
                    // view's row semantics (filter/distinct/groupBy) — the
                    // join would match rows the view excludes. EXEMPT: the
                    // class's OWN backing view (its pipeline already applies
                    // those semantics; the condition only needs the columns).
                    throw new NotImplementedException(
                            "Join references view '" + cr.table() + "' with "
                          + (view.filter() != null ? "~filter" : view.distinct()
                                  ? "~distinct" : "~groupBy")
                          + " semantics as its source side; joins over"
                          + " non-plain views are a roadmap feature. mapping="
                          + md.qualifiedName());
                }
                for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc
                        : view.columnMappings()) {
                    if (vc.name().equals(cr.column())) {
                        yield vc.expression();
                    }
                }
                yield cr;
            }
            case RelationalOperation.Comparison c -> new RelationalOperation.Comparison(
                    resolveViewRefsInJoin(c.left(), db, sourceTable, model, md, backingView, onlyView), c.op(),
                    resolveViewRefsInJoin(c.right(), db, sourceTable, model, md, backingView, onlyView));
            case RelationalOperation.BooleanOp b -> new RelationalOperation.BooleanOp(
                    resolveViewRefsInJoin(b.left(), db, sourceTable, model, md, backingView, onlyView), b.op(),
                    resolveViewRefsInJoin(b.right(), db, sourceTable, model, md, backingView, onlyView));
            case RelationalOperation.Group g -> new RelationalOperation.Group(
                    resolveViewRefsInJoin(g.inner(), db, sourceTable, model, md, backingView, onlyView));
            case RelationalOperation.IsNull n -> new RelationalOperation.IsNull(
                    resolveViewRefsInJoin(n.operand(), db, sourceTable, model, md, backingView, onlyView));
            case RelationalOperation.IsNotNull n -> new RelationalOperation.IsNotNull(
                    resolveViewRefsInJoin(n.operand(), db, sourceTable, model, md, backingView, onlyView));
            case RelationalOperation.FunctionCall f -> new RelationalOperation.FunctionCall(
                    f.name(), f.args().stream()
                            .map(a -> resolveViewRefsInJoin(a, db, sourceTable, model, md, backingView, onlyView))
                            .toList());
            default -> op;
        };
    }

    /**
     * A join landing ON a view has no physical target relation yet — wall it
     * AT SYNTH TIME so the failure stays inside the per-class poison catch.
     * Without this, the synth body carries an unknown-table tableReference
     * whose type-check failure (phase F, OUTSIDE the catch) sinks the WHOLE
     * mapping for every class. Views as join targets = roadmap slice.
     */
    static void requireNonViewTarget(String targetTable, String db,
            String joinName, ModelBuilder model, LegacyMappingDefinition md) {
        if (model.findView(db, targetTable).isPresent()) {
            throw new NotImplementedException(
                    "Join '" + joinName + "' targets view '" + targetTable
                  + "'; views as JOIN TARGETS are a roadmap feature (the view"
                  + " must expand as a relation at the join hop). mapping="
                  + md.qualifiedName());
        }
    }

    static String determineTargetTable(RelationalOperation cond, String sourceTable,
                                              String joinName, String ownerLabel,
                                              int hopIndex, String mappingFqn) {
        if (containsTargetColumnRef(cond)) return sourceTable;
        Set<String> tables = new LinkedHashSet<>();
        RelOpTranslator.collectTablesIn(cond, tables);
        tables.remove(sourceTable);
        if (tables.size() == 1) return tables.iterator().next();
        if (tables.isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "Join '" + joinName + "' references no table other than source '"
                  + sourceTable + "' and has no {target} marker; owner=" + ownerLabel
                  + ", hop " + hopIndex + ", mapping=" + mappingFqn);
        }
        throw new NotImplementedException(
                "Join '" + joinName + "' references multiple non-source tables "
              + tables + "; multi-table joins not supported. owner=" + ownerLabel
              + ", hop " + hopIndex + ", mapping=" + mappingFqn);
    }

    private static boolean containsTargetColumnRef(RelationalOperation op) {
        return switch (op) {
            case RelationalOperation.TargetColumnRef ignored -> true;
            case RelationalOperation.ColumnRef ignored       -> false;
            case RelationalOperation.Literal ignored         -> false;
            case RelationalOperation.TypeRef ignored         -> false;
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
            case RelationalOperation.JoinNavigation ignored -> throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "JoinNavigation inside join condition");
        };
    }

    /**
     * A bare column reference to a table the join chain reaches through more
     * than one path is irreducibly ambiguous: the legacy DSL addresses by
     * table name, which cannot pick between two sub-rows of the same table.
     * Fail loudly with guidance rather than resolve to an arbitrary sub-row.
     */

    /**
     * Translate a {@link RelationalOperation} into a Pure value
     * expression. Nested {@link RelationalOperation.JoinNavigation}
     * nodes resolve via the hoisted prelude: each chain has been
     * emitted as a clean {@code join(~alias, ...)} step in Pass 2, so
     * its sub-row is reachable as {@code $row.<alias>}, and the
     * terminal column (if any) reads from that sub-row.
     */

    private static ValueSpecification buildNewInstance(String classFqn,
                                                      Map<String, KeyExpression> fields) {
        return new AppliedFunction("new", List.of(
                new PackageableElementPtr(classFqn),
                new NewInstance(classFqn, List.of(),
                        Collections.unmodifiableMap(new LinkedHashMap<>(fields)))));
    }

    /**
     * {@link #buildNewInstance(String, Map)} for STORE-backed emissions
     * (relational columns, variant/JSON reads): values bound to a
     * {@code [1]}-declared property are wrapped in {@code toOne(...)}.
     *
     * <p>A store read is statically {@code [0..1]} (a nullable column, a
     * variant key access), and real pure's {@code NewValidator} demands
     * full multiplicity subsumption on {@code ^new(...)} &mdash; hand-written
     * pure must spell {@code ->toOne()} to bind such a value to a
     * {@code [1]} property. The synthesized body says the same thing
     * explicitly: the MAPPING is the assertion that the read is to-one,
     * and the residual null-check is {@code toOne}'s runtime semantics.
     * The m2m (PureInstanceSetImplementation) path deliberately does NOT
     * auto-wrap: there the lambda is user-written pure and real engine
     * makes the user write the coercion.
     *
     * <p>NOT wrapped: {@code navigate}/{@code legacyNavigate} values
     * (statically {@code T[*]} by design; conformance is the Phase H
     * resolver's question) and nested {@code new} (already {@code [1]}).
     */
    static String simpleTypeName(String name) {
        int idx = name.lastIndexOf("::");
        return idx < 0 ? name : name.substring(idx + 2);
    }

    private static final Set<String> PRIMITIVE_TYPE_NAMES = Set.of(
            "Integer", "String", "Float", "Boolean", "Decimal", "Number",
            "StrictDate", "DateTime", "Date");

    static ValueSpecification buildNewInstanceToOne(String classFqn,
                                                            Map<String, KeyExpression> fields,
                                                            ModelBuilder model) {
        ClassDefinition cd = model.findClass(classFqn).orElse(null);
        Map<String, KeyExpression> wrapped = new LinkedHashMap<>();
        fields.forEach((name, key) -> {
            ClassDefinition.PropertyDefinition prop =
                    cd == null ? null : findPropertyDefDeep(cd, name, model, new HashSet<>());
            boolean toOneDeclared = prop != null
                    && prop.multiplicity() instanceof Multiplicity.Concrete c
                    && c.lowerBound() == 1 && Integer.valueOf(1).equals(c.upperBound());
            ValueSpecification v = key.value();
            // An UNTYPED variant read (PAYLOAD->get('price')) bound to a
            // PRIMITIVE-declared property coerces by EMISSION — the same
            // to(get, @Type) the typed-get spelling and the JSON-source
            // synthesizer produce; the declared property type IS the type.
            // EXACT primitive identification: the bare spelling or the
            // platform FQN — a user class named model::Integer must not be
            // coerced (audit; the exact-FQN rule).
            String ptName = prop != null
                    && prop.type() instanceof TypeExpression.NameRef ptn
                    ? ptn.name() : null;
            String primitiveName = ptName == null ? null
                    : PRIMITIVE_TYPE_NAMES.contains(ptName) ? ptName
                    : ptName.startsWith("meta::pure::metamodel::type::")
                            && PRIMITIVE_TYPE_NAMES.contains(simpleTypeName(ptName))
                            ? simpleTypeName(ptName) : null;
            if (v instanceof AppliedFunction gf && gf.function().equals("get")
                    && primitiveName != null) {
                v = new AppliedFunction("to", List.of(v,
                        new TypeAnnotation.Named(
                                new TypeExpression.NameRef(primitiveName))));
            }
            boolean exempt = v instanceof AppliedFunction af
                    && (af.function().equals("navigate")
                        || af.function().equals("legacyNavigate")
                        || af.function().equals("otherwise")
                        || af.function().equals("new"));
            wrapped.put(name, toOneDeclared && !exempt
                    ? new KeyExpression(new AppliedFunction("toOne", List.of(v)),
                            key.isAdd(), key.isLocal())
                    : new KeyExpression(v, key.isAdd(), key.isLocal()));
        });
        return buildNewInstance(classFqn, wrapped);
    }

    private static ClassDefinition.PropertyDefinition findPropertyDefDeep(
            ClassDefinition cd, String propName, ModelBuilder model, Set<String> visited) {
        if (cd == null || !visited.add(cd.qualifiedName())) return null;
        for (ClassDefinition.PropertyDefinition p : cd.properties()) {
            if (p.name().equals(propName)) return p;
        }
        for (TypeExpression sup : cd.superClasses()) {
            if (sup instanceof TypeExpression.NameRef nr) {
                ClassDefinition.PropertyDefinition inherited = findPropertyDefDeep(
                        model.findClass(nr.name()).orElse(null), propName, model, visited);
                if (inherited != null) return inherited;
            }
        }
        return null;
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
    static TypeExpression findPropertyTypeDeep(ClassDefinition cd, String propName,
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

    static TypeExpression findPropertyTypeDeep(ClassDefinition cd, String propName,
                                                      ModelBuilder model, Set<String> visited) {
        if (cd == null || !visited.add(cd.qualifiedName())) return null;
        TypeExpression own = findPropertyType(cd, propName);
        if (own != null) return own;
        // ASSOCIATION properties are inherited too (PersonWithConstraints
        // extends Person reaches Person's 'firm' end) — consult them per
        // class on the chain, not just the declared class.
        TypeExpression assoc = model.findAssociationProperty(cd.qualifiedName(), propName)
                .orElse(null);
        if (assoc != null) return assoc;
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
