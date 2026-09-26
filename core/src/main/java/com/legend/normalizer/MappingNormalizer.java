package com.legend.normalizer;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.protocol.Multiplicity;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.model.PoisonKey;
import com.legend.protocol.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.ClassDefinition;
import com.legend.model.CleanSheetMappingDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.DatabaseDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.EnumerationMapping;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.Function;
import com.legend.model.FunctionDefinition;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.MappingInclude;
import com.legend.model.PackageableElement;
import com.legend.model.PropertyMapping;
import com.legend.protocol.Realization;
import com.legend.model.RelationalOperation;
import com.legend.model.SynthHat;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
/**
 * Legacy Mapping DSL desugarer. Translates every legacy
 * {@link LegacyMappingDefinition} into clean-sheet function form per
 * {@code docs/MAPPING_LEGACY_TO_FUNCTION.md}. The engine has no such
 * translator: it compiles the legacy DSL into its mapping metamodel
 * ({@code HelperRelationalBuilder}, {@code RelationalCompilerExtension})
 * and generates SQL from that metamodel ({@code pureToSQLQuery.pure}).
 * Every rule here that shapes rows cites the engine line it follows
 * (docs/TRANSLATOR_AUDIT_2026_09_15.md); a rule without a line is ours.
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

    // ====================================================================
    // Entry point
    // ====================================================================

    /** Desugar legacy mapping DSL (E.1) into synthesized realizing
     * functions. {@code model} MUST be {@code ModelBuilder.from(parsed)}
     * for the SAME parsed (shared resolution view incl. cross-baked
     * mappings); the index is owned by {@link ModelNormalizer}/tests —
     * this phase is a pure function of {@code (parsed, model)}. */
    public static NormalizedModel normalize(ParsedModel parsed, ModelBuilder model) {
        return normalize(parsed, model, null);
    }

    /** TOLERANT variant (module compile): a non-null {@code wallSink}
     * collects per-mapping normalization walls (element FQN &rarr; first
     * error line) and EXCLUDES those mappings instead of throwing. */
    public static NormalizedModel normalize(ParsedModel parsed, ModelBuilder model,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink) {
        LiftedViews views = new LiftedViews(parsed, model);
        views.liftAll(wallSink);
        return normalize(parsed, model, wallSink, views);
    }

    /** {@code views}: the lifted view bodies (E.5), built by the caller BEFORE
     *  this phase and read by every view-expansion site through the mapping
     *  handle — never computed here twice. */
    static NormalizedModel normalize(ParsedModel parsed, ModelBuilder model,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink, LiftedViews views) {
        Objects.requireNonNull(parsed, "parsed");
        Objects.requireNonNull(model, "model");
        List<PackageableElement> out = new ArrayList<>(parsed.elements().size());
        List<FunctionDefinition> lifted = new ArrayList<>();
        java.util.Map<String, LegacyMappingDefinition> legacySurfaces =
                new java.util.LinkedHashMap<>();
        // store substitutions resolved ONCE for every mapping, include order (stamped on each)
        java.util.Map<String, java.util.Map<String, String>> resolvedStores =
                StoreSubstitutionRewrite.resolveAllStores(parsed.elements().stream()
                        .filter(LegacyMappingDefinition.class::isInstance)
                        .map(LegacyMappingDefinition.class::cast).toList(), model);
        // EVERY mapping's pre-pass runs to completion first (JSON identity
        // sets, cycles, declared keys, extends, implicit inheritance, store
        // refs, implicit ops), then the graph-wide mapped-class fact is
        // fixed — a mapping's synthesis never depends on which mappings
        // normalized before it (T4.1 step 2, verified item 1).
        java.util.Map<String, ResolvedMapping> resolved =
                MappingPrePass.run(parsed, model, wallSink, views);
        for (PackageableElement el : parsed.elements()) {
            if (el instanceof LegacyMappingDefinition md) {
                ResolvedMapping pp = resolved.get(md.qualifiedName());
                if (pp == null) {
                    continue;   // walled by the pre-pass (tolerant build)
                }
                legacySurfaces.put(md.qualifiedName(), pp.surface());
                // Rewrite legacy surface -> canonical binding table; the legacy
                // record does NOT flow past Phase E (CLEAN_SHEET_INVERSION §1.5).
                // What this mapping's synthesis learns rides its own ledger,
                // stamped on the compiled mapping — never the shared index.
                MappingLedger ledger = new MappingLedger(
                        MappingLedger.mappedInClosure(pp, resolved));
                try {
                    out.add(withElement(md.qualifiedName(), () -> {
                        MappingDefinition m = normalizeMapping(pp, model, lifted,
                                resolvedStores.getOrDefault(md.qualifiedName(),
                                        java.util.Map.of()), ledger);
                        // THE DRIVER'S policy (B4): a STRICT build rejects
                        // what the engine's compiler rejects — the first
                        // recorded error; a MODULE build keeps the poisons
                        if (wallSink == null && !ledger.strictErrors.isEmpty()) {
                            throw ledger.strictErrors.get(0);
                        }
                        return m;
                    }));
                } catch (ModelException e) {
                    if (wallSink == null || e.element() == null) {
                        throw e;
                    }
                    wallSink.putIfAbsent(e.element(),
                            String.valueOf(e.getMessage()).split("\n")[0]);
                }
            } else if (el instanceof CleanSheetMappingDefinition canonical) {
                // Clean-sheet (Door 1/3) mapping: the pre-E surface tree is
                // translated to the compiled binding table here — inline
                // bodies lambda-lift, ref bindings keep the user's FQN, and
                // EVERY relational binding stamps its source
                // (CLEAN_SHEET_INVERSION §5.3; census 2026-08-30).
                try {
                    out.add(withElement(canonical.qualifiedName(),
                            () -> cleanSheetToCanonical(canonical, model, lifted)));
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
        return new NormalizedModel(out, parsed.imports(), legacySurfaces);
    }

    /**
     * Attach the ELEMENT FQN to any {@link com.legend.error.ModelException}
     * escaping {@code work} — ONE wrap covers every throw inside a mapping's
     * normalization, so the driver can decorate with the element's
     * {@code [line:col]} (positions wave).
     */
    static <T> T withElement(String elementFqn, Supplier<T> work) {
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

    private static MappingDefinition normalizeMapping(ResolvedMapping pp,
                                                     ModelBuilder model,
                                                     List<FunctionDefinition> lifted,
                                                     java.util.Map<String, String> resolvedStores,
                                                     MappingLedger ledger) {
        // the pre-pass (MappingPrePass) already ran for every mapping
        Map<String, MappingDefinition.ClassBinding.DeclaredKeys> declaredKeys = pp.declaredKeys();
        // Pre-pass: inject MULTI-HOP association ends as class-typed Join
        // PMs (Option A, docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b).
        ResolvedMapping md = pp.withMapping(AssociationSynthesis.injectMultiHopAssociationPMs(pp, model));

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
        Set<String> mixedUnionRooted = new HashSet<>();
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Union un) {
                unionRooted.add(un.className());
                // MIXED-KIND (route b): resolver arms need PER-SET bindings
                for (String sid : un.memberSetIds()) {
                    if (md.set(sid) instanceof ClassMapping.Pure) {
                        mixedUnionRooted.add(un.className());
                        break;
                    }
                }
            }
        }
        for (ClassMapping cm : md.classMappings()) {
            if (java.util.Objects.requireNonNull(mappingsPerClass.get(cm.className())) > 1 && !cm.root()) {
                boolean unionMember = unionRooted.contains(cm.className());
                // EVERY non-root set realizes as its own function and binds
                // by set id (legacy routes as composition, §11 leg 1): a
                // union member too — a route names the member's function,
                // and the union is those functions stacked
                {
                    if (!unionMember) {
                        // multi-set class without a UNION root: .all() is
                        // undefined (poisoned); the SET itself still
                        // realizes (H5) via the set-discriminated binding.
                        ledger.poison(new PoisonKey.ForClass(cm.className()),
                                "class is mapped through multiple set IDs;"
                                        + " .all() over multi-set mappings"
                                        + " (implicit union) is a roadmap"
                                        + " feature");
                    }
                    String invalidSet = pp.invalidReason(cm);
                    if (invalidSet != null) {
                        ledger.poison(new PoisonKey.ForSet(cm.className(),
                                ResolvedMapping.idOf(cm)), invalidSet);
                        continue;
                    }
                    try {
                        FunctionDefinition setFn =
                                synthesizeClassMapping(md, cm, model, true, ledger);
                        lifted.add(setFn);
                        classBindings.add(cm instanceof ClassMapping.Relational rSrc
                                ? new MappingDefinition.ClassBinding.Relational(
                                        cm.className(), cm.setId(),
                                        cm.extendsSetId(), /*root*/ false,
                                        setFn.qualifiedName(),
                                        declaredPrimaryKeyColumns(cm),
                                        declaredKeys.getOrDefault(SetKeyFacts.setKey(rSrc),
                                                MappingDefinition.ClassBinding.DeclaredKeys.NONE),
                                        relationalSourceOf(rSrc),
                            List.of(), propertyPinsOf(rSrc))
                                : isOperation(cm)
                                ? new MappingDefinition.ClassBinding.Operation(
                                        cm.className(), cm.setId(),
                                        cm.extendsSetId(), /*root*/ false,
                                        setFn.qualifiedName(),
                                        declaredPrimaryKeyColumns(cm),
                                        cm instanceof ClassMapping.Inheritance,
                                        ledger.operationMembers.getOrDefault(
                                                ResolvedMapping.idOf(cm), List.of()))
                                : new MappingDefinition.ClassBinding.Pure(
                                        cm.className(), cm.setId(),
                                        cm.extendsSetId(), /*root*/ false,
                                        setFn.qualifiedName(),
                                        declaredPrimaryKeyColumns(cm)));
                    } catch (NotImplementedException | ModelException e) {
                        // per-SET fault isolation, same line as per-class
                        if (e instanceof ModelException) {
                            ledger.strictErrors.add(e);
                        }
                        ledger.poison(new PoisonKey.ForSet(cm.className(),
                                ResolvedMapping.idOf(cm)), String.valueOf(e.getMessage()));
                    }
                }
                continue;
            }
            String invalid = pp.invalidReason(cm);
            if (invalid != null) {
                ledger.poison(new PoisonKey.ForClass(cm.className()), invalid);
                continue;
            }
            FunctionDefinition fn;
            try {
                fn = synthesizeClassMapping(md, cm, model, false, ledger);
            } catch (NotImplementedException
                    | ModelException e) {
                // §6's line (step 6): a USER-model error the engine rejects
                // at compile time is rejected by a STRICT build too (the
                // driver throws the recorded error); only a MODULE build
                // defers it, and a ROADMAP gap defers in both
                if (e instanceof ModelException) {
                    ledger.strictErrors.add(e);
                }
                // PER-CLASS fault isolation: one class mapping using a
                // roadmap feature must not sink the whole mapping. The
                // binding is withheld; fetching THIS class raises the
                // recorded reason (loud at use, never silent).
                // The full message rides on the poison and surfaces via
                // StoreResolver's 0-binder error.
                ledger.poison(new PoisonKey.ForClass(cm.className()), String.valueOf(e.getMessage()));
                continue;
            }
            lifted.add(fn);
            if (cm instanceof ClassMapping.Relational aggMain
                    && aggMain.aggregation() != null) {
                AggregateViewLift.lift(md, aggMain, model, lifted, classBindings, declaredKeys, ledger);
            }
            classBindings.add(cm instanceof ClassMapping.Relational rSrc
                    ? new MappingDefinition.ClassBinding.Relational(
                            cm.className(), cm.setId(), cm.extendsSetId(),
                            cm.root(), fn.qualifiedName(),
                            declaredPrimaryKeyColumns(cm),
                            declaredKeys.getOrDefault(SetKeyFacts.setKey(rSrc),
                                    MappingDefinition.ClassBinding.DeclaredKeys.NONE),
                            relationalSourceOf(rSrc),
                            AggregateViewLift.facts(rSrc), propertyPinsOf(rSrc))
                    : isOperation(cm)
                    ? new MappingDefinition.ClassBinding.Operation(
                            cm.className(), cm.setId(), cm.extendsSetId(),
                            cm.root(), fn.qualifiedName(),
                            declaredPrimaryKeyColumns(cm),
                            cm instanceof ClassMapping.Inheritance,
                            ledger.operationMembers.getOrDefault(
                                    ResolvedMapping.idOf(cm), List.of()))
                    : new MappingDefinition.ClassBinding.Pure(
                            cm.className(), cm.setId(), cm.extendsSetId(),
                            cm.root(), fn.qualifiedName(),
                            declaredPrimaryKeyColumns(cm)));
        }
        // ENGINE ROUTER PARITY (include direction): union/inheritance route
        // classification happens in the QUERIED mapping's closure. A class
        // mapped in an INCLUDED mapping whose set-routed property targets a
         List<MappingDefinition.AssociationBinding> assocBindings =
                new ArrayList<>(md.associationMappings().size());
        for (AssociationMapping am : md.associationMappings()) {
            // null => multi-hop association (per-end navigation above).
            FunctionDefinition fn;
            try {
                fn = AssociationSynthesis.synthesizeAssociationMapping(md, am, model, ledger);
            } catch (NotImplementedException | ModelException e) {
                // PER-ASSOCIATION fault isolation (mirrors the per-class arm
                // above): one XStore/ModelJoin association on roadmap
                // machinery must not sink the whole mapping — the class
                // bindings stay queryable; navigating THIS association
                // raises the recorded reason via the poison channel.
                // TOLERANT (module) builds only — a STRICT build must
                // reject what the engine's compiler rejects (audit 17): the
                // driver throws the recorded error
                ledger.strictErrors.add(e);
                ledger.poison(new PoisonKey.ForAssociation(
                        AssociationSynthesis.resolveAssociation(model, md, am)
                                .map(a -> a.qualifiedName())
                                .orElse(am.associationName())),
                        String.valueOf(e.getMessage()));
                continue;
            }
            if (fn != null) {
                lifted.add(fn);
                assocBindings.add(new MappingDefinition.AssociationBinding(
                        AssociationSynthesis.resolveAssociation(model, md, am)
                                .map(a -> a.qualifiedName())
                                .orElse(am.associationName()), fn.qualifiedName()));
            }
        }
        // includes survive the rewrite unchanged — one shared MappingInclude type.
        // Enumeration mappings are FLATTENED here (own + includes',
        // transitively): the fact rides the compiled artifact, so no
        // post-compile consumer re-derives it from the legacy surface.
        return new MappingDefinition(
                md.qualifiedName(),
                md.includes(),
                classBindings,
                assocBindings,
                md.enumerationMappingsWithIncludes(),
                md.testSuitesSource(),
                resolvedStores,
                ledger.facts(pp.surface()));
    }

    // ====================================================================
    // Door 3 — lift inline expression bindings in a clean-sheet mapping
    // ====================================================================

    /**
     * The clean-sheet door's Phase-E translation: every binding of the
     * pre-E surface ({@link CleanSheetMappingDefinition}) becomes a
     * COMPILED binding. Inline bodies lambda-lift into ordinary top-level
     * functions (appended to {@code lifted}) and stamp their source from
     * the lifted body's root; function-REF bindings keep the user's FQN
     * and stamp from the REFERENCED function's body (available here —
     * functions register at resolution time). Post-condition: the
     * compiled artifact carries function FQNs and STAMPED sources only
     * (CLEAN_SHEET_INVERSION §5.3 / §7.4; no Inline, no Undeclared).
     * Underivable sources THROW (door symmetry with the legacy
     * main-table wall), riding the per-element wall sink in tolerant
     * builds.
     */
    private static MappingDefinition cleanSheetToCanonical(
            CleanSheetMappingDefinition md,
            ModelBuilder model,
            List<FunctionDefinition> lifted) {
        List<MappingDefinition.ClassBinding> classBindings =
                new ArrayList<>(md.classBindings().size());
        for (CleanSheetMappingDefinition.ClassBinding cb : md.classBindings()) {
            String fnFqn;
            List<ValueSpecification> srcBody = null;
            if (cb.realization() instanceof Realization.Inline inl) {
                fnFqn = SynthFqn.mappingClass(md.qualifiedName(), cb.classFqn());
                lifted.add(liftClassInline(md, cb, inl, fnFqn));
                srcBody = inl.body();
            } else {
                fnFqn = ((Realization.Ref) cb.realization()).functionFqn();
            }
            if (cb.kind() == CleanSheetMappingDefinition.Kind.RELATIONAL) {
                // ref bindings stamp from the REFERENCED function's body
                // (functions register at resolution time) — construction-
                // time derivation, fetched only for the relational kind
                java.util.Set<String> seen = new java.util.HashSet<>();
                if (srcBody == null) {
                    srcBody = refBody(fnFqn, md.qualifiedName(),
                            cb.classFqn(), model);
                    seen.add(fnFqn);
                }
                // a FUNCTION-FORM binding declares no key text (no
                // ~distinct / ~groupBy / ~primaryKey): its key facts are
                // the body's — not stamped here (grow by witness)
                classBindings.add(new MappingDefinition.ClassBinding.Relational(
                        cb.classFqn(), cb.setId(), cb.extendsSetId(),
                        cb.root(), fnFqn, cb.primaryKeyColumns(),
                        MappingDefinition.ClassBinding.DeclaredKeys.NONE,
                        inlineRootSource(srcBody, model, md.qualifiedName(),
                                cb.classFqn(), seen),
                            List.of(), java.util.Map.of()));
            } else {
                classBindings.add(new MappingDefinition.ClassBinding.Pure(
                        cb.classFqn(), cb.setId(), cb.extendsSetId(),
                        cb.root(), fnFqn, cb.primaryKeyColumns()));
            }
        }
        List<MappingDefinition.AssociationBinding> assocBindings =
                new ArrayList<>(md.associationBindings().size());
        for (CleanSheetMappingDefinition.AssociationBinding ab : md.associationBindings()) {
            if (ab.realization() instanceof Realization.Inline inl) {
                String fnFqn = SynthFqn.mappingAssoc(md.qualifiedName(), ab.associationFqn());
                lifted.add(liftAssocInline(md, ab, inl, fnFqn, model));
                assocBindings.add(new MappingDefinition.AssociationBinding(
                        ab.associationFqn(), fnFqn));
            } else {
                assocBindings.add(new MappingDefinition.AssociationBinding(
                        ab.associationFqn(),
                        ((Realization.Ref) ab.realization()).functionFqn()));
            }
        }
        return new MappingDefinition(md.qualifiedName(), md.includes(),
                classBindings, assocBindings, md.enumerationMappings(), md.testSuitesSource());
    }

    /** A function-REF binding's realizing body, fetched for the stamp —
     * construction-time derivation from the referenced function (never a
     * consumption-time walk). Loud when the function is unknown or
     * body-less. */
    private static List<ValueSpecification> refBody(String fnFqn,
            String mappingFqn, String classFqn, ModelBuilder model) {
        var fns = model.findFunction(fnFqn);
        for (var f : fns) {
            if (f instanceof FunctionDefinition fd) {
                return fd.body();
            }
        }
        throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                "clean-sheet binding for '" + classFqn + "' references '"
                        + fnFqn + "' which is not a user function with a body;"
                        + " mapping=" + mappingFqn);
    }

    /** The DOOR-1 stamp: a clean-sheet inline binding's root table
     * reference, recognized at LIFT (the binding's construction moment)
     * by leftmost descent to the {@code #>{db.T}#} head the parser
     * emitted structurally (AppliedFunction "tableReference"). The
     * convergence tenet demands both doors produce the SAME binding
     * table, so this mirrors the legacy {@code relationalSourceOf}.
     * FOLLOWS user-function chains: a root that is a call to a
     * registered user function recurses into that function's body
     * (cycle-guarded) — a mapping may share its source through
     * arbitrarily chained functions and still stamp the Table the
     * chain bottoms out at. A chain that never reaches a store access
     * THROWS (user ruling 2026-08-30: no unknown variant; declare the
     * binding Pure or root it at a store access), riding the
     * per-element wall sink in tolerant builds. */
    private static MappingDefinition.RelationalSource
            inlineRootSource(List<ValueSpecification> body, ModelBuilder model,
                    String mappingFqn, String classFqn,
                    java.util.Set<String> seen) {
        ValueSpecification v = body.size() == 1 ? body.get(0) : null;
        while (v instanceof com.legend.protocol.spec.AppliedFunction af) {
            if ("tableReference".equals(af.function())) {
                break;
            }
            List<ValueSpecification> chained = userFunctionBody(af.function(), model);
            if (chained != null) {
                if (!seen.add(af.function())) {
                    throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                            "clean-sheet relational binding for '" + classFqn
                                    + "' has a CYCLIC source-function chain at '"
                                    + af.function() + "'; mapping=" + mappingFqn);
                }
                return inlineRootSource(chained, model, mappingFqn, classFqn, seen);
            }
            if (af.parameters().isEmpty()) {
                break;
            }
            v = af.parameters().get(0);
        }
        if (v instanceof com.legend.protocol.spec.AppliedFunction tr
                && "tableReference".equals(tr.function())
                && tr.parameters().size() == 2
                && tr.parameters().get(0)
                        instanceof com.legend.protocol.spec.PackageableElementPtr db
                && tr.parameters().get(1)
                        instanceof com.legend.protocol.spec.CString t) {
            return new MappingDefinition.RelationalSource.Table(
                    db.fullPath(), t.value(), false, List.of());
        }
        throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                "clean-sheet relational binding for '" + classFqn
                        + "' has no derivable physical source (the body's root"
                        + " is neither a store access nor a user-function"
                        + " chain reaching one); declare the binding Pure or"
                        + " root it at #>{db.TABLE}#; mapping=" + mappingFqn);
    }

    /** The registered user function's body for a call root, else null
     * (natives/combinators like {@code map} resolve to nothing here —
     * user functions arrive FQN'd from name resolution). */
    private static @com.legend.base.Nullable List<ValueSpecification> userFunctionBody(
            String calleeFqn, ModelBuilder model) {
        for (var f : model.findFunction(calleeFqn)) {
            if (f instanceof FunctionDefinition fd) {
                return fd.body();
            }
        }
        return null;
    }

    /**
     * A class inline body lifts to a param-less {@code (): Class[*]} function
     * whose body is the user's expression verbatim. Kind-agnostic: Relational
     * and Pure differ only in what the body starts from, which the lift never
     * inspects.
     */
    private static FunctionDefinition liftClassInline(CleanSheetMappingDefinition md,
                                                     CleanSheetMappingDefinition.ClassBinding cb,
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
    private static FunctionDefinition liftAssocInline(CleanSheetMappingDefinition md,
                                                     CleanSheetMappingDefinition.AssociationBinding ab,
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




    // ====================================================================
    // Pre-pass: inject multi-hop association ends as class-typed Join PMs
    // ====================================================================

    static @com.legend.base.Nullable String nameRefOrNull(TypeExpression t) {
        return t instanceof TypeExpression.NameRef nr ? nr.name() : null;
    }

    // Lifted-function FQNs are owned by SynthFqn (the single naming authority,
    // docs/CLEAN_SHEET_INVERSION.md §3): SynthFqn.mappingClass / mappingAssoc.

    // ====================================================================
    // Class mapping synthesis (top-level dispatch)
    // ====================================================================

    /** Orientation-normal form for XStore conditions: {@code ==} is
     * commutative — the two direction lines write the SAME predicate with
     * swapped operands ({@code $this.id == $that.firmId} vs
     * {@code $this.firmId == $that.id}); put the {@code srcRow}-rooted
     * operand first so equal directions COMPARE equal (audit: the
     * direction-specific wall fired on pure commutation). */
    static ValueSpecification canonicalizeEqualOperands(
            ValueSpecification v, String srcVar) {
        if (v instanceof AppliedFunction af) {
            List<ValueSpecification> ps = af.parameters().stream()
                    .map(x -> canonicalizeEqualOperands(x, srcVar)).toList();
            if (com.legend.compiler.ResolvedNames.names(af, com.legend.builtin.Pure.EQUAL__ANY_MANY__ANY_MANY.qualifiedName())
                    && ps.size() == 2
                    && !rootedAt(ps.get(0), srcVar)
                    && rootedAt(ps.get(1), srcVar)) {
                ps = List.of(ps.get(1), ps.get(0));
            }
            // AND/OR are commutative: the two direction lines may order
            // conjuncts differently ((eq && date) vs (date && eq) —
            // crossMapping2); a deterministic operand order makes pure
            // commutation compare equal (the direction-specific wall stays
            // for genuinely different predicates)
            if ((com.legend.compiler.ResolvedNames.names(af, com.legend.builtin.Pure.AND__BOOLEAN_1__BOOLEAN_1.qualifiedName())
                    || com.legend.compiler.ResolvedNames.names(af, com.legend.builtin.Pure.OR__BOOLEAN_1__BOOLEAN_1.qualifiedName()))
                    && ps.size() == 2
                    && ps.get(0).toString().compareTo(ps.get(1).toString()) > 0) {
                ps = List.of(ps.get(1), ps.get(0));
            }
            return af.withParameters(ps);
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
        // the property-space local marker legacyLocalProperty($row, 'p')
        // is rooted at its row argument (XStorePureEnds emission)
        if (v instanceof AppliedFunction af2 && af2.parameters().size() == 2
                && Pure.Lite.LEGACY_LOCAL_PROPERTY.equals(af2.function())) {
            return rootedAt(af2.parameters().get(0), var);
        }
        return false;
    }

    /** A union or inheritance operation: its function is a composition of
     * the members' functions and binds under the engine's own kind. */
    private static boolean isOperation(ClassMapping cm) {
        return cm instanceof ClassMapping.Union || cm instanceof ClassMapping.Inheritance;
    }

    static FunctionDefinition synthesizeClassMapping(ResolvedMapping md,
                                                            ClassMapping cm,
                                                            ModelBuilder model,
                                                            boolean setDiscriminated,
                                                            MappingLedger ledger) {
        // prop[setId] routing is classified PER-PM (Join.targetSetId) inside
        // synthTableBackedParts — the name-keyed propertyTargetSets map
        // cannot distinguish same-named duplicates (audit 11: textual PM
        // order silently decided the outcome), so no map-driven pre-rewrite
        // happens here.
        ValueSpecification body = switch (cm) {
            case ClassMapping.Pure pcm       -> synthM2M(md, pcm, model, ledger);
            case ClassMapping.Relational rcm -> synthRelational(md, rcm, model, ledger);
            case ClassMapping.Union u        -> UnionSynthesis.synthUnion(md, u, model, ledger);
            case ClassMapping.Inheritance ih -> UnionSynthesis.synthInheritance(md, ih, model, ledger);
            case ClassMapping.RelationFunction rf -> synthRelationFunction(md, rf, model);
        };
        return new FunctionDefinition(
                setDiscriminated
                        ? SynthFqn.mappingClassSet(md.qualifiedName(),
                                cm.className(), ResolvedMapping.idOf(cm))
                        : SynthFqn.mappingClass(md.qualifiedName(), cm.className()),
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
            ResolvedMapping md,
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
    static void putRelationCols(Map<String, KeyExpression> fields,
            List<ClassMapping.RelationFunction.Col> cols, Variable row,
            String ownerClassFqn, ResolvedMapping md,
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
                            && c.inlineSetId().equals(ResolvedMapping.idOf(rf2))) {
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
                ClassDefinition owner = model.knowledge().hierarchyClass(ownerClassFqn)
                        .orElseThrow(() -> new ModelException(
                                LegendCompileException.Phase.NORMALIZE,
                                "Relation mapping embedded property '"
                                + c.property() + "': unknown owner class '"
                                + ownerClassFqn + "'"));
                TypeExpression t = model.knowledge().propertyType(owner, c.property());
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
            // a ROW-EXPRESSION binding (explicit-src form) rebinds $src to THIS row
            ValueSpecification read = c.expr() != null
                    ? ClassMapping.RelationFunction.Col.bindSrc(c.expr(), row)
                    : new AppliedProperty(row, java.util.Objects.requireNonNull(
                            c.column(), "column read on an embedded ~func col"));
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
        if (rf.funcRef() == null) {
            // ~src inline expression IS the pipeline — no resolution step
            // (engine #4941: _relationFunction holds the lambda itself)
            return java.util.Objects.requireNonNull(rf.inlineSource());
        }
        String ref = rf.funcRef();
        List<Function> fns = model.findFunction(ref);
        if (fns.isEmpty()) {
            // the signature-id spelling f__Relation_1_ names one overload,
            // registered under that exact id
            fns = model.findFunctionById(ref);
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
    static FunctionDefinition synthesizeXStoreMapping(ResolvedMapping md,
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
        XStorePureEnds.XEnd endA = XStorePureEnds.xstoreEndOf(md, classA, setA, model);
        XStorePureEnds.XEnd endB = XStorePureEnds.xstoreEndOf(md, classB, setB, model);
        if (endA.pure() || endB.pure() || endA.lossyView() || endB.lossyView()) {
            // route A (docs/XSTORE_LEG.md): a Pure-set end has no relation
            // at normalize time — property-space emission, sets pinned by id.
            // A table-backed end whose column view is LOSSY takes the same
            // route (batch 110): the view drops expression-bound and
            // join-chain +props, while the resolver's substitution through
            // the set's bindings carries them (the engine compiles the
            // property mapping's own relational operation into the
            // condition). An exact view keeps the column-space emission.
            return XStorePureEnds.synthesize(md, xs, ad, classA, classB,
                    endA, endB);
        }
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
            conds.add(RelationReads.xstore(
                    cand.expression(),
                    thisRow, thisRf, thatRow, thatRf, xs.associationName(),
                    md, model));
        }
        // the two directions must AGREE (audit S6) — canonicalization
        // serves ONLY that comparison; the EMITTED cond keeps the FIRST
        // line's AUTHORED operand order (engine golden spells
        // $this.entityIdFk == $that.entityId verbatim, never re-ordered)
        ValueSpecification cond = conds.get(0);
        ValueSpecification canon0 = canonicalizeEqualOperands(cond, srcRow.name());
        for (ValueSpecification c : conds) {
            if (!canonicalizeEqualOperands(c, srcRow.name()).equals(canon0)) {
                throw new NotImplementedException(
                        "XStore association '" + xs.associationName()
                        + "' has direction-specific conditions; a single"
                        + " shared predicate is required for now (mapping="
                        + md.qualifiedName() + ")");
            }
        }
        Variable a = new Variable("a");
        Variable b = new Variable("b");
        ValueSpecification body = new AppliedFunction(Pure.Lite.LEGACY_ASSOC_PREDICATE, List.of(
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
    static FunctionDefinition synthesizeModelJoinMapping(ResolvedMapping md,
            AssociationMapping.ModelJoin mj, ModelBuilder model,
            String classA, String classB) {
        AssociationDefinition ad2 = model.findAssociation(mj.associationName()).orElseThrow();
        // ends resolve like the XStore path: a Relation(~func) set
        // directly, or a TABLE-backED Relational set converted to its
        // column view (the testRelational*/mixed sub-family)
        XStorePureEnds.XEnd endA = XStorePureEnds.xstoreEndOf(md, classA, null, model);
        XStorePureEnds.XEnd endB = XStorePureEnds.xstoreEndOf(md, classB, null, model);
        if (endA.pure() || endB.pure()) {
            throw new NotImplementedException(
                    "ModelJoin association '" + mj.associationName()
                    + "' has a Pure-set end — the property-space route"
                    + " covers XStore only so far (mapping="
                    + md.qualifiedName() + ")");
        }
        ClassMapping.RelationFunction rfA = endA.colsView();
        ClassMapping.RelationFunction rfB = endB.colsView();
        if (mj.lambda().parameters().size() != 2) {
            throw new NotImplementedException(
                    "ModelJoin for '" + mj.associationName() + "' needs a"
                    + " 2-param lambda; got " + mj.lambda().parameters().size());
        }
        Variable p0 = mj.lambda().parameters().get(0);
        Variable p1 = mj.lambda().parameters().get(1);
        String t0 = p0.type() instanceof TypeExpression.NameRef nr0 ? nr0.name() : null;
        String t1 = p1.type() instanceof TypeExpression.NameRef nr1 ? nr1.name() : null;
        String[] pair = ModelJoinNesting.pairEndVars(mj.associationName(), ad2,
                classA, classB, p0, p1, t0, t1);
        String aVar = pair[0];
        String bVar = pair[1];
        Variable srcRow = new Variable("srcRow");
        Variable tgtRow = new Variable("tgtRow");
        Map<String, Variable> rowByVar = Map.of(aVar, srcRow, bVar, tgtRow);
        Map<String, ClassMapping.RelationFunction> rfByVar = Map.of(aVar, rfA, bVar, rfB);
        ModelJoinNesting.Composed nh = ModelJoinNesting.compose(md, model,
                mj, ad2, classA, classB, aVar, bVar, rfByVar,
                endA.pipeline(), endB.pipeline());
        ValueSpecification pipeA = nh.pipeA();
        ValueSpecification pipeB = nh.pipeB();
        Map<String, Map<String, Map<String, String>>> nestedCols =
                nh.nestedCols();
        ValueSpecification cond = RelationReads.rewrite(
                mj.lambda().body().get(mj.lambda().body().size() - 1),
                rowByVar, rfByVar, mj.associationName(), md, nestedCols,
                model);
        Variable a = new Variable("a");
        Variable b = new Variable("b");
        ValueSpecification body = new AppliedFunction(Pure.Lite.LEGACY_ASSOC_PREDICATE, List.of(
                a, b,
                pipeA,
                pipeB,
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


    // ====================================================================
    // M2M (ClassMapping.Pure)  —  doc §5.5
    // ====================================================================

    private static ValueSpecification synthM2M(ResolvedMapping md,
                                              ClassMapping.Pure pcm,
                                              ModelBuilder model,
                                              MappingLedger ledger) {
        // Source: SourceClass.all() — emitted as getAll(SourceClass).
        String srcFqn = pcm.sourceClass();
        if (srcFqn == null) {
            // A Pure mapping with no ~src is legal to WRITE and legal to
            // compile (engine leaves _srcClass null), but the M2M
            // pipeline is "every instance of the source, mapped" — with
            // no source there is no extent to iterate, so there is
            // nothing this could execute. Refuse where the meaning runs
            // out, not where the syntax does.
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "Pure class mapping for '" + pcm.className()
                    + "' in mapping '" + md.qualifiedName()
                    + "' declares no ~src, so it has no source extent to"
                    + " map from — such a mapping can be compiled and"
                    + " analysed but cannot produce rows");
        }
        ValueSpecification source = new AppliedFunction("getAll",
                List.of(new PackageableElementPtr(srcFqn)));
        if (pcm.filter() != null) {
            source = new AppliedFunction("filter", List.of(source,
                    new LambdaFunction(List.of(new Variable("src")),
                                       List.of(pcm.filter()))));
        }
        // Terminal: map(src | ^Class(...)).
        Variable srcBind = new Variable("src");
        Map<String, KeyExpression> fields = new LinkedHashMap<>();
        ClassDefinition tgt = MissProbe.knownMiss(model.knowledge().hierarchyClass(pcm.className()));
        for (ClassMapping.Pure.PropertyBinding pb : pcm.propertyBindings()) {
            // Audit 21a: the parsed mappingLine heads are honored or
            // poisoned by DESIGN — never dropped. A local (+prop) is
            // checked FIRST so a name collision with a real/inherited/
            // association property can never silently retarget it (the
            // engine keeps local mapping properties distinct).
            if (pb.local()) {
                // mapping-LOCAL property (the XStore assoc-key idiom):
                // composes as an extra isLocal binding column;
                // collision with a declared property stays the audit
                // 21a poison (M2mRouteGuards.localField)
                fields.put(pb.propertyName(), M2mRouteGuards.localField(
                        pb, tgt, md, model,
                        model.knowledge().propertyType(tgt, pb.propertyName()) != null));
                continue;
            }
            if (pb.explode()) {
                throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                        "M2M explosion '" + pb.propertyName() + "*' is a"
                      + " roadmap feature (index-aligned zip fan-out — one"
                      + " target instance per source element); mapping="
                      + md.qualifiedName());
            }
            if (pb.enumMappingId() != null) {
                // parsed and RECORDED (the mft/testExplosion corpus
                // families) — dropping the transformer would read raw
                // source values as enum names, silently wrong
                throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                        "M2M enum transformer 'EnumerationMapping "
                      + pb.enumMappingId() + "' on '" + pb.propertyName()
                      + "' is a roadmap feature (source-value decode on"
                      + " the M2M read); mapping=" + md.qualifiedName());
            }
            String keyName = M2mRouteGuards.m2mBindingKey(pb, tgt, md,
                    b -> model.knowledge().propertyType(tgt, b) != null);
            fields.put(keyName,
                    new KeyExpression(m2mPropertyValue(pb, tgt, md, model, ledger), false, false));
        }
        return new AppliedFunction("map", List.of(source,
                new LambdaFunction(List.of(srcBind),
                                   List.of(buildNewInstance(pcm.className(), fields)))));
    }

    private static ValueSpecification m2mPropertyValue(
            ClassMapping.Pure.PropertyBinding pb, @com.legend.base.Nullable ClassDefinition tgt,
            ResolvedMapping md, ModelBuilder model, MappingLedger ledger) {
        if (tgt == null) return pb.expression();
        TypeExpression propType = model.knowledge().propertyType(tgt, pb.propertyName());
        if (propType == null && pb.propertyName().endsWith("AllVersions")) {
            propType = model.knowledge().propertyType(tgt, pb.propertyName().substring(0,
                    pb.propertyName().length() - "AllVersions".length()));
        }
        if (!(propType instanceof TypeExpression.NameRef nr)) return pb.expression();
        String innerFqn = nr.name();
        if (model.knowledge().hierarchyClass(innerFqn).isEmpty()) return pb.expression();
        if (!ledger.isMapped(innerFqn)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "M2M class-typed property '" + pb.propertyName() + "' on '"
                  + tgt.qualifiedName() + "' targets unmapped class '" + innerFqn
                  + "'; map '" + innerFqn + "' or use Embedded. Mapping="
                  + md.qualifiedName());
        }
        // audit 21a heads honored: the line's [targetSetId] route rides
        // the cast — the whole-$src graph child dispatches by it. No
        // recursion happens here (the cast DEFERS the child to the graph),
        // so there is nothing to guard: the old class-keyed "cycle" check
        // could only trip on the OWNING class and rejected the legal
        // self-reference `Person.manager: Person` (audit 2026-09-15 P0-3);
        // the ~src-chain cycle is the pre-pass's detectM2MCycles.
        return new NewInstanceCast(innerFqn, List.of(), pb.expression(),
                pb.targetSetId());
    }

    // ====================================================================
    // Relational dispatch:  JsonSource | View-backed | Table-backed
    // ====================================================================



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

    /** Column names read through the {@code {target}} placeholder — the
     * DESTINATION side of a self-join hop ({@code OneTable.ID =
     * {target}.personId}). Inbound union-route keys live on this side for
     * self-join members; a plain table-named collect can't see them. */
    static void collectTargetColumns(RelationalOperation cond, Set<String> out) {
        switch (cond) {
            case RelationalOperation.TargetColumnRef t -> out.add(t.column());
            case RelationalOperation.Comparison c -> {
                collectTargetColumns(c.left(), out);
                collectTargetColumns(c.right(), out);
            }
            case RelationalOperation.BooleanOp b -> {
                collectTargetColumns(b.left(), out);
                collectTargetColumns(b.right(), out);
            }
            case RelationalOperation.Group g ->
                    collectTargetColumns(g.inner(), out);
            case RelationalOperation.IsNull n ->
                    collectTargetColumns(n.operand(), out);
            case RelationalOperation.IsNotNull n ->
                    collectTargetColumns(n.operand(), out);
            case RelationalOperation.FunctionCall f ->
                    f.args().forEach(x -> collectTargetColumns(x, out));
            default -> {
            }
        }
    }



    /** The declared multiplicity of {@code prop} on {@code owner} (chain walk). */

    static ValueSpecification synthRelational(ResolvedMapping md,
                                                     ClassMapping.Relational rcm,
                                                     ModelBuilder model,
                                                     MappingLedger ledger) {
        // JSON-source: synthesized by ModelBuilder cross-baking from a
        // RuntimeDefinition's JsonModelConnection. mainTable is null;
        // sourceUrl carries the inline VARIANT subquery source.
        if (rcm.sourceUrl() != null) {
            return synthJsonSourceMapping(md, rcm, model);
        }
        if (rcm.mainTable() == null) {
            LegacyMappingDefinition.TableReference inferred = resolvedMainTable(rcm);
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
                    rcm.propertyTargetSets(), rcm.aggregation());
        }
        var vMain = java.util.Objects.requireNonNull(rcm.mainTable(),
                "table-backed set without ~mainTable");
        DatabaseDefinition.ViewDefinition view = model.findView(
                vMain.database(), vMain.table()).orElseGet(MissProbe::miss);
        if (view != null) {
            return synthViewBackedMapping(md, rcm, view, model, ledger);
        }
        return synthTableBackedMapping(md, rcm, model, ledger, null);
    }

    /**
     * The RESOLVED main source of a relational set: explicit
     * {@code ~mainTable}, or the engine-parity inference when absent
     * (real engine: the table of the first direct column binding when
     * all agree; corpus mappings rarely spell {@code ~mainTable}). Null
     * for JSON-source sets and for sets with nothing to infer from (the
     * synthesis walls loudly on the latter). THE one resolution &mdash;
     * both the function synthesis and the {@code ClassBinding} stamp
     * read this, so the derivation logic exists in exactly one place.
     */
    static LegacyMappingDefinition.@com.legend.base.Nullable TableReference
            resolvedMainTable(ClassMapping.Relational rcm) {
        if (rcm.sourceUrl() != null) {
            return null;
        }
        return rcm.mainTable() != null ? rcm.mainTable() : inferMainTable(rcm);
    }

    /** The {@code ClassBinding} STAMP for a relational set — cached
     * answers computed by the SAME resolution the function synthesis
     * uses ({@link #resolvedMainTable}); consumers read them verbatim
     * (the {@code RelationalSource} razor). TOTAL: JSON-source sets
     * stamp {@code Json}; a set whose synthesis is about to wall stamps
     * {@code Undeclared} (unreachable on a lifted binding — the wall
     * poisons first). */
    static MappingDefinition.RelationalSource
            relationalSourceOf(ClassMapping.Relational rcm) {
        if (rcm.sourceUrl() != null) {
            return new MappingDefinition.RelationalSource.Json(rcm.sourceUrl());
        }
        var main = resolvedMainTable(rcm);
        if (main == null) {
            // unreachable on a lifted binding: synthRelational THROWS on
            // exactly this condition before the binding is constructed
            throw new IllegalStateException(
                    "stamping a set whose synthesis must have walled: "
                            + rcm.className());
        }
        List<MappingDefinition.EnumColumn> ecs = new ArrayList<>();
        for (var pm : rcm.propertyMappings()) {
            if (pm instanceof PropertyMapping.EnumeratedColumn ec
                    && ec.enumMappingId() != null) {
                ecs.add(new MappingDefinition.EnumColumn(
                        ec.table(), ec.column(), ec.enumMappingId()));
            }
        }
        return new MappingDefinition.RelationalSource.Table(main.database(),
                main.table(), rcm.aggregationAwareMain(), ecs);
    }

    /**
     * The engine's inferred main table when {@code ~mainTable} is absent:
     * every property mapping's DIRECT (non-join) table must agree on ONE
     * table (RelationalCompilerExtension collects all aliases and errors on
     * more than one distinct table — "Please specify a main table"). First
     * table wins only when it is the SOLE table; disagreement is loud.
     */
    static LegacyMappingDefinition.@com.legend.base.Nullable TableReference inferMainTable(
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
        return com.legend.compiler.KnowledgeLayer.canonicalTable(table);
    }

    /** {@link #inferMainTable} as a PROBE: null on ambiguity instead of loud. */
    static LegacyMappingDefinition.@com.legend.base.Nullable TableReference inferMainTableQuiet(
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
            // a computed column's DIRECT column references count; a join
            // navigation inside it contributes nothing (collectExprTables) —
            // the engine's alias map (HelperRelationalBuilder.java:1172) takes
            // every direct TableAliasColumn and processes a join's terminal
            // with a fresh map (:1182)
            case PropertyMapping.Expression ex -> collectExprTables(ex.expression(), sink);
            // DELIBERATE non-contributors (audit 15: exhaustive, no default —
            // a new PM kind must state its main-table stance here): joins
            // and join terminals reference OTHER tables; enum expressions
            // and inline/otherwise embeddeds carry no direct column.
            case PropertyMapping.Join ignored -> { }
            case PropertyMapping.JoinTerminalColumn ignored -> { }
            case PropertyMapping.EnumeratedExpression ignored -> { }
            case PropertyMapping.InlineEmbedded ignored -> { }
            // an otherwise-embedded block's own property mappings read the
            // owner's row exactly like a plain embedded block (the engine
            // processes both with the class mapping's alias map)
            case PropertyMapping.OtherwiseEmbedded oe ->
                    oe.embedded().forEach(inner -> collectMainTables(inner, sink));
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
            case RelationalOperation.Lambda lam -> collectExprTables(lam.body(), sink);
            case RelationalOperation.LambdaParam ignored -> { }
            // DELIBERATE non-contributors (audit 15: exhaustive, no default):
            // literals/type refs carry no table; a bare/target column ref
            // without a database qualifier cannot name one; join navigations
            // reference OTHER tables by construction.
            case RelationalOperation.ColumnRef ignored -> { }
            case RelationalOperation.TargetColumnRef ignored -> { }
            case RelationalOperation.Literal ignored -> { }
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
    private static ValueSpecification synthJsonSourceMapping(ResolvedMapping md,
                                                            ClassMapping.Relational rcm,
                                                            ModelBuilder model) {
        ValueSpecification source = new AppliedFunction(Pure.Lite.SOURCE_URL,
                List.of(new CString(java.util.Objects.requireNonNull(rcm.sourceUrl(),
                        "sourceUrl-backed set without a source url"))));
        Variable rowBind = new Variable("row");
        ClassDefinition cd = model.knowledge().hierarchyClass(rcm.className()).orElseThrow(() ->
                new ModelException(LegendCompileException.Phase.NORMALIZE, "JSON-source mapping references unknown class '"
                        + rcm.className() + "'; mapping=" + md.qualifiedName()));
        Map<String, KeyExpression> fields = new LinkedHashMap<>();
        for (ClassDefinition.PropertyDefinition prop : cd.properties()) {
            // get($row.data, 'propName') — 2-arg variant access. The only
            // `get` native is get(Variant[1], Any[1]):Variant[0..1]; the
            // single VARIANT `data` column is fanned into property values
            // by key (ours: the engine reads a JsonModelConnection in memory
            // and generates no SQL for it; this is the SQL-side equivalent).
            ValueSpecification get = new AppliedFunction("get", List.of(
                    new AppliedProperty(rowBind, "data"), new CString(prop.name())));
            // to(get(...), @Type) — typed text-extraction + cast. Engine uses
            // `to` (not `cast`) so the Variant text access (->>) strips JSON
            // string quotes before casting to the declared property type.
            ValueSpecification value = (prop.type() instanceof TypeExpression.NameRef nr)
                    ? new AppliedFunction("to", List.of(get,
                            new TypeAnnotation.Named(new TypeExpression.NameRef(nr.name()))))
                    : get;
            fields.put(prop.name(), new KeyExpression(value, false, false));
        }
        return new AppliedFunction("map", List.of(source,
                new LambdaFunction(List.of(rowBind),
                        List.of(buildNewInstanceToOne(rcm.className(), fields, model)))));
    }

    // ====================================================================
    // View-backed mapping  —  doc §5.3.7
    // ====================================================================

    /**
     * A view-backed set: the view is the set's FRAME (a subselect —
     * {@code pureToSQLQuery.pure:5187 ViewSelectSQLQuery}), the set's own
     * references to the view's root table resolve through the frame
     * ({@link ViewRelation#frameRewrite}), its ~filter / ~distinct /
     * ~groupBy apply over the frame's rows. (Historical shape of the
     * deleted flattening fallback, for the record:)
     * <ol>
     *   <li>Infer the view's single underlying physical table from its
     *       non-join column expressions ({@code ModelBuilder.viewMainTable}).
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
    private static ValueSpecification synthViewBackedMapping(ResolvedMapping md,
                                                            ClassMapping.Relational rcm,
                                                            DatabaseDefinition.ViewDefinition view,
                                                            ModelBuilder model,
                                                            MappingLedger ledger) {
        String mainDb = java.util.Objects.requireNonNull(rcm.mainTable(),
                "view-backed set without ~mainTable").database();
        // a view reached as a relation is an IDENTITY-CARRYING FRAME — a
        // row-defining subselect as the pipeline SOURCE (its ~filter /
        // ~groupBy / ~distinct inside); PMs read view columns VERBATIM
        // (engine: ViewSelectSQLQuery extends TABLE; a view never flattens)
        // the view is the set's FRAME (its subselect); every reference the set
        // makes to the view's root table resolves to the declared column that
        // carries it (ViewRelation.frameRewrite — loud when none does); joins
        // depart from the view by name. No other emission exists (the
        // flattening fallback of docs/TRANSLATOR_AUDIT_2026_09_15.md F1 was
        // ours alone and is gone).
        String viewName = rcm.mainTable().table();
        ValueSpecification viewSource = md.views().body(view);
        ClassMapping.Relational overView = ViewRelation.throughFrame(rcm, view, viewName, md);
        return synthTableBackedMapping(md, overView, model, ledger, viewSource);
    }



    // ====================================================================
    // Table-backed mapping  —  the main pipeline synthesis
    // ====================================================================

    /** A synthesized relational body BEFORE its map terminal composes. */
    record RelationalParts(ValueSpecification pipeline, Variable rowBind,
                           Map<String, KeyExpression> fields) {
    }

    private static ValueSpecification synthTableBackedMapping(ResolvedMapping md,
                                                              ClassMapping.Relational rcm,
                                                              ModelBuilder model,
                                                              MappingLedger ledger,
                                                              @com.legend.base.Nullable ValueSpecification sourceOverride) {
        RelationalParts parts = synthTableBackedParts(md, rcm, model, ledger, sourceOverride);
        return new AppliedFunction("map", List.of(parts.pipeline(),
                new LambdaFunction(List.of(parts.rowBind()),
                        List.of(buildNewInstanceToOne(rcm.className(), parts.fields(), model)))));
    }

    /**
     * {@code sourceOverride}: a NON-TABLE source relation (a grouped view's
     * subselect — V1b): the pipeline starts there instead of
     * {@code tableReference(mainTable)}, and {@code rcm.mainTable().table()}
     * names the SOURCE ROW SCOPE (the view name) that column PMs and join
     * conditions resolve against.
     */
    static RelationalParts synthTableBackedParts(ResolvedMapping md,
                                                             ClassMapping.Relational rcm,
                                                             ModelBuilder model, MappingLedger ledger,
                                                              @com.legend.base.Nullable ValueSpecification sourceOverride) {
        // A mapping ~filter with an EXPLICIT (INNER) join type row-explodes:
        // the engine swaps the main table for a subselect that joins the
        // filter chain, applies the condition, and projects every base
        // column under its original name (getRelationalElementWithInnerJoin,
        // pureToSQLQuery.pure:5077; chosen at :5101) — one row PER MATCHING CHILD
        // survives (testInnerJoinClassMappingFilterWithChainedJoins expects
        // Firm X x4). The exists-shaped filter route below keeps one row
        // per parent, so it cannot serve this form.
        // (the recursion below passes the filtered source with the filter
        // removed, so a frame source with an INNER filter takes this path
        // once — the frame is rebuilt view-aware inside innerFilteredSource)
        if (rcm.filter() instanceof FilterMapping.JoinMediated jmi
                && jmi.joinType() != null) {
            ValueSpecification innerSrc = JoinChainEmission.innerFilteredSource(rcm, jmi, model, md, ledger);
            ClassMapping.Relational noFilter = new ClassMapping.Relational(
                    rcm.className(), rcm.setId(), rcm.extendsSetId(), rcm.root(),
                    rcm.mainTable(), null, rcm.distinct(), rcm.groupBy(),
                    rcm.primaryKey(), rcm.propertyMappings(), null,
                    rcm.propertyTargetSets(), rcm.aggregation());
            return synthTableBackedParts(md, noFilter, model, ledger, innerSrc);
        }

        var mMain = java.util.Objects.requireNonNull(rcm.mainTable(),
                "relational set without ~mainTable");
        String mainDb    = mMain.database();
        String mainTable = canonicalTable(mMain.table());
        Variable rowBind = new Variable("row");

        // Query-parser parity (H1): the database is a PackageableElementPtr,
        // the table a string — the same shapes #>{db.TABLE}# produces, so
        // TableReferenceChecker serves both surfaces.
        Pipeline p = new Pipeline(sourceOverride != null ? sourceOverride
                : new AppliedFunction("tableReference",
                        List.of(new PackageableElementPtr(mainDb), new CString(mainTable))),
                ledger, md.views());
        UnionSynthesis.classifyUnionRoutes(md, rcm, model, p);

        // Pass 1: structural chain emission (Join, JoinTerminalColumn,
        // LocalProperty-wrapping-JTC). Class-typed Join PMs to mapped
        // targets emit a final-hop legacyNavigate. On a ~groupBy class the
        // Join PMs DEFER: their navigate lands AFTER the groupBy (stage 2 —
        // the engine navigates a grouped set through the join over the
        // GROUPED subselect, on the group-key columns).
        boolean grouped = !rcm.groupBy().isEmpty();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            if (grouped && pm instanceof PropertyMapping.Join) {
                continue;
            }
            JoinChainEmission.emitHopsForStructuralPm(p, pm, rcm.className(), mainDb, mainTable,
                    rowBind, model, md);
        }
        // Pass 2: hoist JoinNavigation chains nested inside Expression
        // bodies, LocalProperty wrappers, Embedded sub-PMs,
        // OtherwiseEmbedded eager/fallback bodies, groupBy keys, and
        // Direct filter conditions. Each unique chain becomes a clean
        // join(...) step; dedup via aliasToTargetTable.
        List<JoinChainEmission.JoinNavSpec> nested = new ArrayList<>();
        JoinChainEmission.collectJoinNavigationsInPms(rcm.propertyMappings(), nested, md);
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
            p.expr = GroupBySynthesis.applyGroupBy(p.expr, rcm, rowBind, mainTable, p, md);
            // stage 2: the deferred class-typed Join PMs navigate the
            // GROUPED relation — the join condition's source-side reads
            // redirect to the grouped OUTPUT columns (a condition column
            // that is not a group key is loud: a grouped set cannot be
            // navigated on a non-key).
            Map<String, String> groupedNames =
                    GroupBySynthesis.groupedKeyColumnNames(rcm, mainTable);
            for (PropertyMapping pm : rcm.propertyMappings()) {
                if (!(pm instanceof PropertyMapping.Join j)) {
                    continue;
                }
                if (j.joins().size() != 1) {
                    throw new NotImplementedException("multi-hop Join PM '"
                            + j.propertyName() + "' on a ~groupBy class is"
                            + " not supported yet; mapping="
                            + md.qualifiedName());
                }
                JoinChainEmission.emitHopsForStructuralPm(p, j,
                        rcm.className(), mainDb, mainTable, rowBind, model, md);
                p.expr = GroupBySynthesis.renameGroupedNavCond(p.expr, groupedNames,
                        j.propertyName(), md);
            }
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
            } else if (!mappedCols.isEmpty() || !p.aliasToTargetTable.isEmpty()) {
                // SLOT-CARRYING ~distinct: dedup by the mapped MAIN-TABLE
                // columns (the unmapped PK must not defeat the dedup —
                // engine dedups the mapped row) PLUS the slot pseudo-columns
                // (so slot reads above the distinct still type-check); the
                // materializer swaps each demanded slot for its prefixed
                // physical columns (join-equality makes them dependent,
                // dedup-neutral) and drops the undemanded ones. A set whose
                // properties are ALL join reads dedups by the slots alone —
                // never by the raw row, whose unique key defeats the dedup
                // (audit 2026-09-15 P0-4, proven by probe).
                List<ColSpec> cols = new ArrayList<>(mappedCols.stream()
                        .map(c -> new ColSpec(c, null, null)).toList());
                for (String alias : p.aliasToTargetTable.keySet()) {
                    cols.add(new ColSpec(alias, null, null));
                }
                p.expr = new AppliedFunction("distinct",
                        List.of(p.expr, new ColSpecArray(cols)));
            } else {
                // no mapped column and no slot: nothing to dedup BY. Loud,
                // never distinct over the physical row.
                throw new NotImplementedException("~distinct on '" + rcm.className()
                        + "' maps no main-table column and carries no join slot —"
                        + " nothing to dedup by; mapping=" + md.qualifiedName());
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

    /** The MAIN-TABLE columns a property mapping reads into {@code sink};
     * TRUE when the mapping is a plain read (no join slot). An embedded
     * block contributes its sub-mappings' columns (they read the owner's
     * row); its otherwise-fallback join and every join-carrying mapping
     * answer FALSE — their reads ride a slot the dedup lists beside the
     * columns. */
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
            case PropertyMapping.Embedded emb -> {
                boolean plain = true;
                for (PropertyMapping sub : emb.propertyMappings()) {
                    plain &= collectMappedColumns(sub, sink);
                }
                return plain;
            }
            case PropertyMapping.OtherwiseEmbedded oe -> {
                for (PropertyMapping sub : oe.embedded()) {
                    collectMappedColumns(sub, sink);
                }
                return false;
            }
            case PropertyMapping.Join ignored -> {
                return false;
            }
            case PropertyMapping.JoinTerminalColumn ignored -> {
                return false;
            }
            case PropertyMapping.InlineEmbedded ignored -> {
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
            default -> op.children().forEach(x -> collectExprColumns(x, sink));
        }
    }

    /** The set PINS a relational set's own property mappings declare
     * (property -> target set id): the {@code prop[setId]} Join PMs and
     * the stamped property target sets. A fact on the binding, read at
     * query time (engine R6). */
    static Map<String, List<String>> propertyPinsOf(ClassMapping.Relational rcm) {
        Map<String, List<String>> pins = new LinkedHashMap<>();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            PropertyMapping body = pm instanceof PropertyMapping.LocalProperty lp ? lp.body() : pm;
            if (body instanceof PropertyMapping.Join j && j.targetSetId() != null) {
                List<String> l = pins.computeIfAbsent(j.propertyName(), k -> new ArrayList<>());
                if (!l.contains(j.targetSetId())) {
                    l.add(j.targetSetId());
                }
            }
            if (body instanceof PropertyMapping.OtherwiseEmbedded oe) {
                String pin = oe.fallback() instanceof PropertyMapping.Join fj
                        && fj.targetSetId() != null ? fj.targetSetId() : oe.fallbackSetId();
                List<String> l = pins.computeIfAbsent(oe.propertyName(), k -> new ArrayList<>());
                if (!l.contains(pin)) {
                    l.add(pin);
                }
            }
        }
        for (var e : rcm.propertyTargetSets().entrySet()) {
            List<String> l = pins.computeIfAbsent(e.getKey(), k -> new ArrayList<>());
            if (!l.contains(e.getValue())) {
                l.add(e.getValue());
            }
        }
        return pins;
    }

    /** The ~primaryKey column NAMES (engine resolvePrimaryKey: declared
     * mapping identity first; the table's PK is only the fallback and is
     * resolved by the CONSUMER, which has store access). Only plain
     * ColumnRef entries carry a name — expression keys contribute none. */
    static List<String> declaredPrimaryKeyColumns(ClassMapping cm) {
        if (!(cm instanceof ClassMapping.Relational rcm)) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (RelationalOperation op : rcm.primaryKey()) {
            if (op instanceof RelationalOperation.ColumnRef cr) {
                out.add(cr.column());
            }
        }
        return out;
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
                                               String ownerClassFqn, ResolvedMapping md,
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
                    DeclaredCoercions.coerceColumnToDeclared(
                            RelOpTranslator.columnRead(col.table(), col.column(), tableScope, defaultTable, pipeline.view()),
                            col, ownerClassFqn, model, pipeline.ledger()),
                    false);
            case PropertyMapping.EnumeratedColumn ec -> new CtorField(ec.propertyName(),
                    translateEnumeratedColumn(ec, tableScope, defaultTable, md, pipeline,
                            ownerClassFqn, model),
                    false);
            case PropertyMapping.Expression expr -> new CtorField(expr.propertyName(),
                    DeclaredCoercions.coerceToDeclaredNumeric(
                            RelOpTranslator.translate(expr.expression(), tableScope, null,
                                    rowBind, pipeline.view()),
                            expr.propertyName(), ownerClassFqn, model),
                    false);
            case PropertyMapping.Join j -> {
                String targetIfMapped = JoinChainEmission.classTypedTarget(ownerClassFqn,
                        j.propertyName(), model);
                String slot = targetIfMapped != null
                        ? JoinChainEmission.navSlotFor(pipeline, j.propertyName())
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
                                : DeclaredCoercions.declaredAssertion(read, jtc,
                                        ownerClassFqn, model, pipeline.ledger()),
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
                            ownerClassFqn, md, model, null),
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

    /** {@code innerOverride} non-null pins the inner class — an Inline
     * splice materializes the REFERENCED set's class (a subclass of the
     * declared prop type; its own props aren't on the declared class).
     * An authored Embedded block cannot recurse (its text is finite); the
     * only cycle is through Inline set references, guarded by set id in
     * {@link #materializeInlineEmbedded} on the pipeline (the old
     * class-keyed stack was handed a fresh set by every caller and could
     * never fire — audit 2026-09-15 P0-3). */
    private static ValueSpecification materializeEmbedded(
            String propName, List<PropertyMapping> subPms, Variable rowBind,
            Map<String, ValueSpecification> tableScope, String defaultTable,
            Pipeline pipeline, String ownerClassFqn, ResolvedMapping md,
            ModelBuilder model, @com.legend.base.Nullable String innerOverride) {
        ClassDefinition owner = MissProbe.knownMiss(model.knowledge().hierarchyClass(ownerClassFqn));
        if (owner == null) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "Embedded PM '" + propName + "' on '" + ownerClassFqn
                  + "' but owner class unknown; mapping=" + md.qualifiedName());
        }
        String innerFqn = innerOverride != null ? innerOverride
                : model.knowledge().propertyType(owner, propName)
                        instanceof TypeExpression.NameRef nr ? nr.name() : null;
        if (innerFqn == null) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "Embedded PM '" + propName + "' on '" + ownerClassFqn
                  + "' has non-class property type; mapping=" + md.qualifiedName());
        }
        Map<String, KeyExpression> fields = new LinkedHashMap<>();
        for (PropertyMapping sub : subPms) {
            // Join sub-PMs read the slot Pass 1 hoisted into the TOP
            // pipeline (the embedded instance shares the owner's row) —
            // translatePmToField's Join arm resolves it via innerFqn.
            // An UNMAPPED target class has no instance to bind: wall.
            if (sub instanceof PropertyMapping.Join j
                    && JoinChainEmission.classTypedTargetIfMapped(innerFqn, j.propertyName(),
                            model, pipeline.ledger()) == null) {
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
            Pipeline pipeline, String ownerClassFqn, ResolvedMapping md,
            ModelBuilder model) {
        ValueSpecification partial = materializeEmbedded(oe.propertyName(),
                oe.embedded(), rowBind, tableScope, defaultTable, pipeline,
                ownerClassFqn, md, model, null);
        ValueSpecification fallback = new AppliedProperty(rowBind, oe.propertyName());
        return new AppliedFunction(Pure.Lite.OTHERWISE, List.of(partial, fallback));
    }

    // InlineEmbedded (§5.4.8): splice the referenced set's PMs inline.

    private static ValueSpecification materializeInlineEmbedded(
            PropertyMapping.InlineEmbedded ie, Variable rowBind,
            Map<String, ValueSpecification> tableScope, String defaultTable,
            Pipeline pipeline, String ownerClassFqn, ResolvedMapping md,
            ModelBuilder model) {
        // the referenced set may live in an INCLUDED mapping (engine
        // resolves Inline set ids across the include closure —
        // testMappingEmbeddedTargetIdsWithIncludes). EXACTLY ONE match:
        // the engine asserts it ("Found too many or not enough matches",
        // mappingExtension.pure) — taking the first silently resolved two
        // sets sharing an id across two included mappings by closure order
        // (audit 2026-09-15 P3-2).
        List<ClassMapping.Relational> matches = new ArrayList<>();
        for (LegacyMappingDefinition m : md.closure()) {
            for (ClassMapping cm : m.classMappings()) {
                if (cm instanceof ClassMapping.Relational rcm
                        && Objects.equals(ResolvedMapping.idOf(rcm), ie.setId())) {
                    matches.add(rcm);
                }
            }
        }
        if (matches.isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "InlineEmbedded PM '" + ie.propertyName()
                  + "' references unknown setId '" + ie.setId()
                  + "' in mapping=" + md.qualifiedName());
        }
        if (matches.size() > 1) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "InlineEmbedded PM '" + ie.propertyName() + "' references setId '"
                  + ie.setId() + "', which " + matches.size() + " class mappings of the"
                  + " include closure declare (" + matches.stream()
                          .map(ClassMapping::className).distinct().toList()
                  + "); the engine requires exactly one match. Mapping="
                  + md.qualifiedName());
        }
        ClassMapping.Relational referenced = matches.get(0);
        // the referenced set's class must BE the declared property type or a
        // subtype of it (engine RelationalInstanceSetImplementationValidator:
        // an Inline reference to an unrelated class is rejected there, and
        // the splice would otherwise materialize a foreign class's PMs)
        ClassDefinition inlineOwner = MissProbe.knownMiss(
                model.knowledge().hierarchyClass(ownerClassFqn));
        TypeExpression declared = inlineOwner == null ? null
                : model.knowledge().propertyType(inlineOwner, ie.propertyName());
        if (declared instanceof TypeExpression.NameRef dn
                && !model.knowledge().isSubtype(referenced.className(), dn.name())) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "InlineEmbedded PM '" + ie.propertyName() + "' references set '"
                  + ie.setId() + "' of class '" + referenced.className()
                  + "', which is not '" + dn.name() + "' or a subclass of it; mapping="
                  + md.qualifiedName());
        }
        // THE cycle guard: an Inline reference is the only way an embedded
        // materialization can recurse (set a splices set b splices set a).
        // Keyed by SET ID on the pipeline — the one object every level
        // shares — so a legal re-use of one CLASS at two paths never trips.
        if (!pipeline.inlineStack.add(ie.setId())) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "Cycle materializing Inline embedded set '" + ie.setId() + "' via '"
                  + ie.propertyName() + "' on '" + ownerClassFqn + "': the splice chain "
                  + pipeline.inlineStack + " returns to it; mapping=" + md.qualifiedName());
        }
        try {
            return materializeEmbedded(ie.propertyName(),
                    referenced.propertyMappings(), rowBind, tableScope, defaultTable,
                    pipeline, ownerClassFqn, md, model, referenced.className());
        } finally {
            pipeline.inlineStack.remove(ie.setId());
        }
    }

    // ====================================================================
    // ~filter (Direct + JoinMediated)  —  doc §5.3.2, §5.3.3
    // ====================================================================

    private static ValueSpecification applyFilter(ValueSpecification source,
                                                 ClassMapping.Relational rcm,
                                                 Variable rowBind, String mainDb,
                                                 String mainTable, Pipeline p,
                                                 ModelBuilder model, ResolvedMapping md) {
        return switch (rcm.filter()) {
            case null -> source;
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
                                                       ResolvedMapping md) {
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
        // a set over a VIEW: the filter's references to the view's root table
        // resolve through the frame; a set over a TABLE whose filter names a
        // view's columns reads the view's expressions (the base table in scope)
        RelationalOperation fcond = model.findView(mainDb, mainTable).isPresent()
                ? ViewRelation.frameRewriteIfView(fd.condition(), mainDb, mainTable, md, model)
                : ViewRelation.inlineViewRefs(fd.condition(), dbFqn, model);
        ValueSpecification cond = RelOpTranslator.translate(fcond, scope, null, rowBind, p.view());
        return new AppliedFunction("filter", List.of(source,
                new LambdaFunction(List.of(rowBind), List.of(cond))));
    }

    private static ValueSpecification applyJoinMediatedFilter(ValueSpecification source,
                                                             ClassMapping.Relational rcm,
                                                             Variable rowBind, String mainDb,
                                                             String mainTable, Pipeline p,
                                                             ModelBuilder model,
                                                             FilterMapping.JoinMediated jm,
                                                             ResolvedMapping md) {
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
        ValueSpecification cond = RelOpTranslator.translate(
                ViewRelation.frameRewriteIfView(fd.condition(), mainDb, mainTable, md, model),
                scope, terminalRow, rowBind, p.view());
        // an EXPLICIT (INNER) filter never reaches here: synthTableBackedParts
        // intercepts it first and builds the row-exploding source
        // (innerFilteredSource) — the wall that stood here described an
        // emission that WAS built (audit 2026-09-15 P6)
        return new AppliedFunction("filter", List.of(source,
                new LambdaFunction(List.of(rowBind), List.of(cond))));
    }

    // ====================================================================
    // ~groupBy with aggregate fn1/fn2 decomposition  —  doc §5.3.5
    // ====================================================================

    // ====================================================================
    // AssociationMapping → predicate function  —  doc §5.6.1
    // ====================================================================

    static boolean hasMainTable(ResolvedMapping md, String classFqn,
            ModelBuilder model) {
        return mainTableOrNull(md, classFqn) != null;
    }

    /** {@code classFqn}'s ~mainTable in {@code md}'s closure, null when no
     * Relational set of the class has one. The ROOT set's table — with
     * multiple set IDs, .all() and every synthesized association predicate
     * anchor on the root; taking the FIRST declared set bound predicates to
     * the wrong table whenever a non-root set was declared first (audit) —
     * else the first set's that has one. */
    static LegacyMappingDefinition.@com.legend.base.Nullable TableReference mainTableOrNull(
            ResolvedMapping md, @com.legend.base.Nullable String classFqn) {
        LegacyMappingDefinition.TableReference first = null;
        for (ClassMapping.Relational rcm : md.relationalSets(classFqn)) {
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
        return first;
    }

    /** {@code classFqn}'s ~mainTable declaration in {@code md} (loud if absent). */
    static LegacyMappingDefinition.TableReference mainTableDefOf(
            ResolvedMapping md, @com.legend.base.Nullable String classFqn, ModelBuilder model) {
        LegacyMappingDefinition.TableReference mt = mainTableOrNull(md, classFqn);
        if (mt != null) {
            return mt;
        }
        throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                "No ~mainTable for class '" + classFqn + "' in mapping="
              + md.qualifiedName() + " (required to synthesize AssociationMapping)");
    }

    /** The {@code #>{db.T}#}-shaped source of {@code classFqn}'s ~mainTable row. */
    static String mainTableOf(ResolvedMapping md,
            @com.legend.base.Nullable String classFqn,
            ModelBuilder model) {
        return mainTableDefOf(md, classFqn, model).table();
    }

    // ====================================================================
    // EnumeratedColumn  —  doc §5.4.2
    // ====================================================================

    private static ValueSpecification translateEnumeratedColumn(
            PropertyMapping.EnumeratedColumn ec,
            Map<String, ValueSpecification> tableScope,
            String defaultTable, ResolvedMapping md, Pipeline p,
            String ownerClassFqn, ModelBuilder model) {
        ValueSpecification colRead = RelOpTranslator.columnRead(ec.table(), ec.column(),
                tableScope, defaultTable, p == null ? RelOpTranslator.PipelineView.NONE : p.view());
        return translateEnumeratedSource(ec.propertyName(), ec.enumMappingId(),
                colRead, md, ownerClassFqn, model);
    }

    /**
     * The enum-decode if/equal chain over ANY source read — a column or a
     * translated expression ({@code role: EnumerationMapping M : case(...)},
     * constants included). Each mapped enum value tests its source values
     * in turn; no match yields {@code []}.
     */
    static ValueSpecification translateEnumeratedSource(
            String propertyName, @com.legend.base.Nullable String enumMappingId, ValueSpecification sourceRead,
            ResolvedMapping md, String ownerClassFqn, ModelBuilder model) {
        EnumerationMapping em = null;
        List<EnumerationMapping> ems =
                md.enumerationMappingsWithIncludes();
        if (enumMappingId != null) {
            // engine getEnumerationMappingId (HelperMappingBuilder:348-351):
            // an anonymous enum mapping's IMPLICIT id is its enumeration FQN
            // with :: -> _ — references by that spelling resolve
            for (EnumerationMapping cand : ems) {
                String candId = com.legend.model.SetId.of(cand.mappingId(), cand.enumName());
                if (enumMappingId.equals(candId)) { em = cand; break; }
            }
        } else {
            // ANONYMOUS reference — resolved by the PROPERTY's declared enum
            // type. Names are FQNs here (NameResolver runs before the
            // normalizer). Two mappings for the SAME enum need the id
            // spelled — loud, never arbitrary.
            ClassDefinition owner = model.knowledge().hierarchyClass(ownerClassFqn).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at MappingNormalizer#8 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + ownerClassFqn));
            TypeExpression propType = owner == null ? null
                    : model.knowledge().propertyType(owner, propertyName);
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
                .map(EnumDefinition::values).orElseGet(MissProbe::miss);
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
            if (!table.equals(mainTable) && java.util.Objects.requireNonNull(perTable.get(table)) > 1) {
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
            String db, @com.legend.base.Nullable String sourceTable,
            ModelBuilder model, @com.legend.base.Nullable ResolvedMapping md,
            @com.legend.base.Nullable String backingView,
            @com.legend.base.Nullable String onlyView) {
        return resolveViewRefsInJoin(op, db, sourceTable, model, md,
                backingView, onlyView, null, false);
    }

    /** {@code anySide}: substitute plain-view refs on EITHER side of the
     * condition (remediation T1.10 — the association's REVERSE end joins
     * through a view over the TARGET's physical table); frame refs
     * (source frame, backing view, {@code keepTargetView} = a
     * view-mapped target class's own frame) stay VERBATIM — frame rows
     * carry the declared view columns. */
    static RelationalOperation resolveViewRefsInJoin(RelationalOperation op,
            String db, @com.legend.base.Nullable String sourceTable,
            ModelBuilder model, @com.legend.base.Nullable ResolvedMapping md,
            @com.legend.base.Nullable String backingView,
            @com.legend.base.Nullable String onlyView,
            @com.legend.base.Nullable String keepTargetView,
            boolean anySide) {
        return switch (op) {
            case RelationalOperation.ColumnRef cr -> {
                if (onlyView != null && !cr.table().equals(onlyView)) {
                    yield cr;
                }
                var view = model.findView(cr.databaseName() != null ? cr.databaseName() : db,
                        cr.table()).orElseGet(MissProbe::miss);
                if (view == null) {
                    yield cr;
                }
                String crDb = cr.databaseName() != null ? cr.databaseName() : db;
                String phys = model.viewMainTable(crDb, view);
                if (anySide
                        ? (cr.table().equals(backingView)
                                || cr.table().equals(sourceTable)
                                || cr.table().equals(keepTargetView))
                        : !viewChainReaches(phys, sourceTable, crDb, md, model)) {
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
                          + " non-plain views are a roadmap feature. "
                          + ViewRelation.owner(md, db));
                }
                for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc
                        : view.columnMappings()) {
                    if (vc.name().equals(cr.column())) {
                        // a view-on-view column substitutes to ANOTHER view's
                        // column (ProductTableViewNested.id -> ProductTableView
                        // .id) — re-resolve so the chain flattens to the
                        // physical root. Pass-1 (onlyView) stays one-layer:
                        // its exact-name contract addresses the backing view
                        // alone.
                        yield onlyView == null
                                ? resolveViewRefsInJoin(vc.expression(), db,
                                        sourceTable, model, md, backingView,
                                        null, keepTargetView, anySide)
                                : vc.expression();
                    }
                }
                yield cr;
            }
            case RelationalOperation.Comparison c -> new RelationalOperation.Comparison(
                    resolveViewRefsInJoin(c.left(), db, sourceTable, model, md, backingView, onlyView, keepTargetView, anySide), c.op(),
                    resolveViewRefsInJoin(c.right(), db, sourceTable, model, md, backingView, onlyView, keepTargetView, anySide));
            case RelationalOperation.BooleanOp b -> new RelationalOperation.BooleanOp(
                    resolveViewRefsInJoin(b.left(), db, sourceTable, model, md, backingView, onlyView, keepTargetView, anySide), b.op(),
                    resolveViewRefsInJoin(b.right(), db, sourceTable, model, md, backingView, onlyView, keepTargetView, anySide));
            case RelationalOperation.Group g -> new RelationalOperation.Group(
                    resolveViewRefsInJoin(g.inner(), db, sourceTable, model, md, backingView, onlyView, keepTargetView, anySide));
            case RelationalOperation.IsNull n -> new RelationalOperation.IsNull(
                    resolveViewRefsInJoin(n.operand(), db, sourceTable, model, md, backingView, onlyView, keepTargetView, anySide));
            case RelationalOperation.IsNotNull n -> new RelationalOperation.IsNotNull(
                    resolveViewRefsInJoin(n.operand(), db, sourceTable, model, md, backingView, onlyView, keepTargetView, anySide));
            case RelationalOperation.FunctionCall f -> new RelationalOperation.FunctionCall(
                    f.name(), f.args().stream()
                            .map(a -> resolveViewRefsInJoin(a, db, sourceTable, model, md, backingView, onlyView, keepTargetView, anySide))
                            .toList());
            default -> op.mapChildren(x -> resolveViewRefsInJoin(x, db,
                    sourceTable, model, md, backingView, onlyView,
                    keepTargetView, anySide));
        };
    }

    /** Whether {@code start} IS {@code sourceTable} or reaches it walking
     * DOWN a view-on-view chain (each layer's inferred main table). The
     * source-side test of {@link #resolveViewRefsInJoin} must see through
     * stacked views — one-layer equality missed ProductTableViewNested
     * (over ProductTableView over ProductTable) as the pipeline's own row. */
    private static boolean viewChainReaches(String start,
            @com.legend.base.Nullable String sourceTable, String db,
            @com.legend.base.Nullable ResolvedMapping md, ModelBuilder model) {
        String walk = start;
        java.util.Set<String> seen = new java.util.HashSet<>();
        while (seen.add(walk)) {
            if (walk.equals(sourceTable)) {
                return true;
            }
            DatabaseDefinition.ViewDefinition v =
                    model.findView(db, walk).orElseGet(MissProbe::miss);
            if (v == null) {
                return false;
            }
            walk = model.viewMainTable(db, v);
        }
        return false;
    }

    /**
     * A join landing ON a view has no physical target relation yet — wall it
     * AT SYNTH TIME so the failure stays inside the per-class poison catch.
     * Without this, the synth body carries an unknown-table tableReference
     * whose type-check failure (phase F, OUTSIDE the catch) sinks the WHOLE
     * mapping for every class. Views as join targets = roadmap slice.
     */
    static void requireNonViewTarget(String targetTable, String db,
            String joinName, ModelBuilder model, ResolvedMapping md) {
        if (model.findView(db, targetTable).isPresent()) {
            throw new NotImplementedException(
                    "Join '" + joinName + "' targets view '" + targetTable
                  + "'; views as JOIN TARGETS are a roadmap feature (the view"
                  + " must expand as a relation at the join hop). mapping="
                  + md.qualifiedName());
        }
    }

    static String determineTargetTable(RelationalOperation cond, @com.legend.base.Nullable String sourceTable,
                                              String joinName, @com.legend.base.Nullable String ownerLabel,
                                              int hopIndex, String mappingFqn) {
        if (containsTargetColumnRef(cond)) {
            return java.util.Objects.requireNonNull(sourceTable,
                    "target-column-ref join condition with unknown source table");
        }
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

    static boolean containsTargetColumnRef(RelationalOperation op) {
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
            case RelationalOperation.Lambda lam              -> containsTargetColumnRef(lam.body());
            case RelationalOperation.LambdaParam ignored     -> false;
            case RelationalOperation.JoinNavigation ignored -> throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "JoinNavigation inside join condition");
        };
    }

    /**
     * Translate a {@link RelationalOperation} into a Pure value
     * expression. Nested {@link RelationalOperation.JoinNavigation}
     * nodes resolve via the hoisted prelude: each chain has been
     * emitted as a clean {@code join(~alias, ...)} step in Pass 2, so
     * its sub-row is reachable as {@code $row.<alias>}, and the
     * terminal column (if any) reads from that sub-row.
     */

    private static ValueSpecification buildNewInstance(@com.legend.base.Nullable String classFqn,
                                                      Map<String, KeyExpression> fields) {
        String fqnNN = java.util.Objects.requireNonNull(classFqn,
                "instance construction without a target class");
        return new AppliedFunction("new", List.of(
                new PackageableElementPtr(fqnNN),
                new NewInstance(fqnNN, List.of(),
                        fields.entrySet().stream().map(e ->
                                new com.legend.protocol.spec.NewInstance
                                        .KeyBinding(e.getKey(), e.getValue()))
                                .toList())));
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

    static ValueSpecification buildNewInstanceToOne(@com.legend.base.Nullable String classFqn,
                                                            Map<String, KeyExpression> fields,
                                                            ModelBuilder model) {
        ClassDefinition cd = MissProbe.knownMiss(model.knowledge().hierarchyClass(classFqn));
        Map<String, KeyExpression> wrapped = new LinkedHashMap<>();
        fields.forEach((name, key) -> {
            ClassDefinition.PropertyDefinition prop =
                    cd == null ? null : model.knowledge().propertyDef(cd, name);
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
            if (v instanceof AppliedFunction gf && com.legend.builtin.NativeFn.RowGetter.GET.property().equals(gf.function())
                    && primitiveName != null) {
                v = new AppliedFunction("to", List.of(v,
                        new TypeAnnotation.Named(
                                new TypeExpression.NameRef(primitiveName))));
            }
            if ("Boolean".equals(primitiveName)) {
                v = booleanizeCaseLiterals(v);
            }
            boolean exempt = v instanceof AppliedFunction af
                    && (com.legend.platform.CoreFn.of(af.function()).orElseGet(MissProbe::miss) == com.legend.platform.CoreFn.NAVIGATE
                        || af.function().equals(Pure.Lite.LEGACY_NAVIGATE)
                        || af.function().equals(Pure.Lite.OTHERWISE)
                        || AppliedFunction.isNew(af));
            wrapped.put(name, toOneDeclared && !exempt
                    ? new KeyExpression(new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(v)),
                            key.isAdd(), key.isLocal())
                    : new KeyExpression(v, key.isAdd(), key.isLocal()));
        });
        return buildNewInstance(classFqn, wrapped);
    }

    /** {@code case(cond,'true','false')} bound to a Boolean property
     * coerces by EMISSION (engine keeps the strings in SQL and decodes
     * at the TDS reader; our tenet types the value in the plan):
     * if-branch and bare 'true'/'false' literals become boolean
     * literals. Any OTHER string stays — the checker's conformance
     * error remains the loud path (never weakened). */
    private static ValueSpecification booleanizeCaseLiterals(
            ValueSpecification v) {
        if (v instanceof CString cs) {
            if (cs.value().equals("true")) {
                return new CBoolean(true);
            }
            if (cs.value().equals("false")) {
                return new CBoolean(false);
            }
            return v;
        }
        if (v instanceof AppliedFunction af
                && com.legend.compiler.ResolvedNames.names(af,
                        Pure.IF__BOOLEAN_1__FUNCTION_1__FUNCTION_1.qualifiedName())
                && af.parameters().size() == 3) {
            List<ValueSpecification> ps = new ArrayList<>(af.parameters());
            for (int i = 1; i <= 2; i++) {
                if (ps.get(i) instanceof LambdaFunction lf
                        && !lf.body().isEmpty()) {
                    List<ValueSpecification> b = new ArrayList<>(lf.body());
                    b.set(b.size() - 1,
                            booleanizeCaseLiterals(b.get(b.size() - 1)));
                    ps.set(i, new LambdaFunction(lf.parameters(), b));
                }
            }
            return af.withParameters(ps);
        }
        return v;
    }





}
