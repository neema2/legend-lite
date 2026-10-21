package com.legend.compiler.element;

import com.legend.protocol.DerivedPropertyDefinition;

import com.legend.protocol.ConstraintDefinition;

import com.legend.protocol.ParameterDefinition;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.model.ClassDefinition;
import com.legend.model.Function;
import com.legend.model.MappingDefinition;
import com.legend.protocol.Realization;

import java.util.List;

/**
 * THE eager reference-safety pass (F.a + F.b unified): every reference a model
 * element makes — type names, realizing functions, mapping bindings,
 * association ends — must exist (and, for behavior hats, have the right
 * structural shape), checked ONCE, whole-model, at context construction. An
 * invalid model never becomes a queryable context, even if nothing ever
 * demands the bad element. Knowledge-level only: classification and symbol
 * lookups, no {@code Typed*} materialization; bodies are Phase G.
 *
 * <p>ONE pass because these checks share their nature (dangling references),
 * their time (construction), and their input (the whole model) — previously
 * scattered across a lazy check inside the class compiler (which missed
 * undemanded elements entirely — found by the pipeline stage-failure suite)
 * and a separate mapping-binding walk.
 */
final class ModelIntegrity {

    private ModelIntegrity() {
    }

    static void check(ModelBuilder model, TypeClassifier classifier, FunctionCompiler functions) {
        check(model, classifier, functions, null, null);
    }

    /** TOLERANT variant (module compile): a non-null {@code wallSink}
     * collects EVERY failing element's first error line in one pass instead
     * of throwing on the first — the caller drops them and re-runs.
     *
     * <p>{@code prior}: a layer already checked on its own (the boot layer,
     * once per process). Its elements' OWN checks are not repeated — nothing
     * a graph adds can change them (a graph element shadowing a boot FQN is
     * a duplicate, reported below) — while every check that spans the two
     * layers still runs: duplicate element names, a graph function repeating
     * a boot signature, and every graph element's references into the boot
     * layer. */
    static void check(ModelBuilder model, TypeClassifier classifier,
            FunctionCompiler functions,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink,
            PureModelContext.@com.legend.base.Nullable CheckedLayer prior) {
        java.util.function.Predicate<Object> fresh = prior == null ? el -> true
                : el -> !prior.contains(el);
        // D6b: element-identity first. A layer's own duplicates were refused
        // before Phase E read the index (Compiler.normalizeLayer); what is
        // new here is a graph element shadowing a boot FQN, recorded when the
        // boot layer's elements joined this index at the gate (2026-10-11)
        refuseDuplicates(model, wallSink);
        model.classes().filter(fresh).forEach(cd -> withElement(cd.qualifiedName(),
                () -> checkClass(cd, classifier, functions, model), wallSink));
        checkInheritanceAcyclic(model, classifier, wallSink, fresh);
        // a function's wall is under its KEY, its id (ParsedModel.keyOf): the
        // overload the signature belongs to, with its own file and position
        model.functions().filter(fresh).forEach(f -> withElement(com.legend.model.FunctionId.of(f).qualified(),
                () -> checkFunction(f, classifier, model), wallSink));
        checkDuplicateSignatures(model, wallSink, fresh,
                prior == null ? java.util.Set.of() : prior.signatureKeys());
        model.associations().filter(fresh).forEach(a -> withElement(a.qualifiedName(), () -> {
            classifier.classify(a.property1().targetClass(), List.of());
            classifier.classify(a.property2().targetClass(), List.of());
            classifier.requireArguments(a.property1().targetClass(), List.of());
            classifier.requireArguments(a.property2().targetClass(), List.of());
            requireAnnotations(a.stereotypes(), a.taggedValues(), model);
            requireAnnotations(a.property1().stereotypes(), a.property1().taggedValues(), model);
            requireAnnotations(a.property2().stereotypes(), a.property2().taggedValues(), model);
            for (DerivedPropertyDefinition dp : a.derivedProperties()) {
                requireAnnotations(dp, model);
            }
        }, wallSink));
        // the property-graph profile's rule over the edge classes (docs/CYPHER_STUDY_2026_10_15.md §5.6): a
        // misdeclared edge is its class's error, one wall per class (the error names the class declaring the
        // fault, which an inherited one may be: its own wall then)
        GraphEdges.Source graphSource = GraphEdges.sourceOf(model);
        GraphEdges.Index graphIndex = GraphEdges.Index.of(graphSource);
        model.classes().filter(fresh).filter(cd -> !cd.isNative()).forEach(cd -> withElement(cd.qualifiedName(),
                () -> GraphEdges.readOne(graphSource, graphIndex, cd), wallSink));
        model.enums().filter(fresh).forEach(ed -> withElement(ed.qualifiedName(), () -> {
            checkEnum(ed);
            requireAnnotations(ed.stereotypes(), ed.taggedValues(), model);
            for (com.legend.model.EnumDefinition.ValueAnnotations va : ed.valueAnnotations()) {
                requireAnnotations(va.stereotypes(), va.taggedValues(), model);
            }
        }, wallSink));
        model.databases().filter(fresh).forEach(db -> withElement(db.qualifiedName(),
                () -> checkDatabase(db, model), wallSink));
        model.mappings().filter(fresh).forEach(md -> withElement(md.qualifiedName(),
                () -> checkMapping(md, model, classifier, functions), wallSink));
        // the elements that only NAME others (2026-10-21, ReferenceIntegrityTest): each name is an element of its
        // kind, as legend-engine's compiler resolves them (PureModel.getMapping / getRuntime / getConnection /
        // getStore, its own words)
        model.runtimes().filter(fresh).forEach(rt -> withElement(rt.qualifiedName(),
                () -> checkRuntime(rt, model), wallSink));
        model.connections().filter(fresh).forEach(c -> withElement(c.qualifiedName(),
                () -> requireStore(c.storeName(), model), wallSink));
        model.services().filter(fresh).forEach(sv -> withElement(sv.qualifiedName(),
                () -> checkService(sv, model), wallSink));
        model.dataSpaces().filter(fresh).forEach(ds -> withElement(ds.qualifiedName(), () -> {
            for (com.legend.model.DataSpaceDefinition.ExecutionContext ec : ds.executionContexts()) {
                requireMapping(ec.mapping(), model);
                requireRuntime(ec.defaultRuntime(), model);
            }
        }, wallSink));
    }

    private static void checkRuntime(com.legend.model.RuntimeDefinition rt, ModelBuilder model) {
        for (String mapping : rt.mappings()) {
            requireMapping(mapping, model);
        }
        for (java.util.Map.Entry<String, List<String>> binding : rt.connectionBindings().entrySet()) {
            requireStore(binding.getKey(), model);
            for (String connection : binding.getValue()) {
                if (model.findConnection(connection).isEmpty() && !model.isModelConnection(connection)
                        && !model.hasElement(connection)) {
                    throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                            "Can't find connection '" + connection + "'");
                }
            }
        }
    }

    private static void checkService(com.legend.model.ServiceDefinition sv, ModelBuilder model) {
        requireMapping(sv.mappingRef(), model);
        requireRuntime(sv.runtimeRef(), model);
        if (sv.multiExecution() != null) {
            for (com.legend.model.ServiceDefinition.KeyedExecution kx : sv.multiExecution().executions()) {
                requireMapping(kx.mapping(), model);
                requireRuntime(kx.runtime(), model);
            }
        }
    }

    /**
     * Every stereotype and tag names a profile the model has, and one that profile declares -- legend-engine's
     * PureModel.getProfile / getStereotype / getTag, in its words. The platform's own profiles (legend-pure's, carried
     * by the prelude) are the model's like any other (2026-10-21; before, no annotation was checked, and a bare
     * {@code temporal} stood for the platform's profile unresolved).
     */
    private static void requireAnnotations(List<com.legend.model.StereotypeApplication> stereotypes,
            List<com.legend.model.TaggedValue> taggedValues, ModelBuilder model) {
        for (com.legend.model.StereotypeApplication st : stereotypes) {
            if (!profile(st.profileName(), model).stereotypes().contains(st.stereotypeName())) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                        "Can't find stereotype '" + st.stereotypeName() + "' in profile '" + st.profileName() + "'");
            }
        }
        for (com.legend.model.TaggedValue tv : taggedValues) {
            if (!profile(tv.profileName(), model).tags().contains(tv.tagName())) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                        "Can't find tag '" + tv.tagName() + "' in profile '" + tv.profileName() + "'");
            }
        }
    }

    /** A derived property's annotations (carried as their protocol records), checked as any annotation is. */
    private static void requireAnnotations(DerivedPropertyDefinition dp, ModelBuilder model) {
        List<com.legend.model.StereotypeApplication> stereotypes = new java.util.ArrayList<>();
        for (com.legend.protocol.Protocol.PStereotype st : dp.stereotypes()) {
            stereotypes.add(new com.legend.model.StereotypeApplication(st.profile(), st.value()));
        }
        List<com.legend.model.TaggedValue> taggedValues = new java.util.ArrayList<>();
        for (com.legend.protocol.Protocol.PTaggedValue tv : dp.taggedValues()) {
            taggedValues.add(new com.legend.model.TaggedValue(tv.tag().profile(), tv.tag().value(), tv.value()));
        }
        requireAnnotations(stereotypes, taggedValues, model);
    }

    private static com.legend.model.ProfileDefinition profile(String fqn, ModelBuilder model) {
        return model.findProfile(fqn).orElseThrow(() -> new com.legend.error.ModelException(
                com.legend.error.LegendCompileException.Phase.MODEL, "Can't find the profile '" + fqn + "'"));
    }

    /** A named mapping exists; {@code null} is a reference not written (an optional one). */
    private static void requireMapping(@com.legend.base.Nullable String mapping, ModelBuilder model) {
        if (mapping != null && model.findMapping(mapping).isEmpty()) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                    "Can't find mapping '" + mapping + "'");
        }
    }

    /** A named runtime exists ({@code null}: none written, or an inline one). */
    private static void requireRuntime(@com.legend.base.Nullable String runtime, ModelBuilder model) {
        if (runtime != null && model.findRuntime(runtime).isEmpty()) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                    "Can't find runtime '" + runtime + "'");
        }
    }

    /** A named store exists: the model store, a database, or another store element ({@code null}: none written). */
    private static void requireStore(@com.legend.base.Nullable String store, ModelBuilder model) {
        if (store != null && !"ModelStore".equals(store) && model.findDatabase(store).isEmpty()
                && !model.hasElement(store)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                    "The store '" + store + "' can't be found.");
        }
    }

    /** Every FQN registered more than once in {@code model}'s shared element
     * namespace ({@link ModelBuilder#duplicateElements}) is refused with the
     * engine's "Duplicated element" (D6b): thrown in a strict build, walled
     * under the FQN in a tolerant one. Run BEFORE Phase E reads the index
     * (so a duplicate's refusal is ITS reason, never a downstream confusion
     * from whichever definition won the slot: a second mapping's include
     * ordering, a loser's own shape) and again at the gate, where the boot
     * layer's elements join the index. */
    static void refuseDuplicates(ModelBuilder model,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink) {
        model.duplicateElements().forEach((fqn, msg) ->
                withElement(fqn, () -> {
                    throw new com.legend.error.ModelException(
                            com.legend.error.LegendCompileException.Phase.MODEL, msg);
                }, wallSink));
    }

    /** Attach the element FQN to escaping ModelExceptions (positions wave). */
    private static void withElement(String elementFqn, Runnable work,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink) {
        try {
            work.run();
        } catch (com.legend.error.ModelException e) {
            if (wallSink != null) {
                // POISON-NOT-DROP: record the first reason and keep the
                // element — use-time compilation re-fails loudly
                wallSink.putIfAbsent(e.element() != null ? e.element() : elementFqn,
                        String.valueOf(e.getMessage()).split("\n")[0]);
                return;
            }
            if (e.element() != null) {
                throw e;
            }
            throw new com.legend.error.ModelException(e.phase(), e.getMessage(), elementFqn);
        }
    }

    /** Class references: property/derived types + realizer functions + constraint shapes.
     * D6b additions: duplicate stored-property names (engine parity:
     * "Found duplicated property") and EAGER multiplicity-bound
     * validation — the {@code Bounded} constructor guard otherwise fires
     * lazily at first class demand as a raw, unattributed
     * {@code IllegalArgumentException}. (Derived-property NAME dups stay
     * accepted: qualified properties legally overload.) */
    private static void checkClass(ClassDefinition cd, TypeClassifier classifier,
                                   FunctionCompiler functions, ModelBuilder model) {
        List<String> typeParams = cd.typeParams();
        requireAnnotations(cd.stereotypes(), cd.taggedValues(), model);
        for (ClassDefinition.PropertyDefinition pd : cd.properties()) {
            requireAnnotations(pd.stereotypes(), pd.taggedValues(), model);
        }
        for (com.legend.protocol.TypeExpression sup : cd.superClasses()) {
            // a supertype is a type that exists (its head, each argument, a generic's arity: classify) and a class:
            // legend-engine's words (ClassCompilerExtension: "Invalid supertype", the class named by its path), which
            // legend-pure refuses too (TypeValidator: "Invalid generalization")
            classifier.classify(sup, typeParams);
            classifier.requireArguments(sup, typeParams);
            String supFqn = TypeClassifier.headFqn(sup);
            // its kind, not its catalog entry: the native catalog lists the primitives among its classes
            if (!(classifier.findType(supFqn).orElseThrow() instanceof com.legend.compiler.element.type.Type.ClassType)) {
                throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.MODEL,
                        "Invalid supertype: '" + cd.qualifiedName() + "' cannot extend '" + supFqn
                                + "' as it is not a class.");
            }
        }
        java.util.Set<String> propertyNames = new java.util.HashSet<>();
        for (ClassDefinition.PropertyDefinition pd : cd.properties()) {
            if (!propertyNames.add(pd.name())) {
                throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.MODEL,
                        "Found duplicated property '" + pd.name()
                                + "' in class '" + cd.qualifiedName() + "'");
            }
            classifier.classify(pd.type(), typeParams);
            classifier.requireArguments(pd.type(), typeParams);
            requireValidBounds(pd.multiplicity(),
                    "property '" + pd.name() + "' of " + cd.qualifiedName());
            if (pd.hasDefault() && pd.multiplicity() instanceof com.legend.protocol.Multiplicity.Concrete m
                    && m.lowerBound() == 0) {
                // legend-pure's PropertyValidator, in its words (its default value's TYPE is the body phase's: the
                // lifted <class>$default$<property>, ModelNormalizer.liftDefaults)
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                        "Default values are supported only for mandatory fields, and property '" + pd.name()
                                + "' is optional.");
            }
        }
        for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
            requireAnnotations(dp, model);
            classifier.classify(dp.type(), typeParams);
            classifier.requireArguments(dp.type(), typeParams);
            requireValidBounds(dp.multiplicity(),
                    "derived property '" + dp.name() + "' of " + cd.qualifiedName());
            for (ParameterDefinition p : dp.parameters()) {
                classifier.classify(p.type(), typeParams);
                classifier.requireArguments(p.type(), typeParams);
                requireValidBounds(p.multiplicity(),
                        "parameter '" + p.name() + "' of derived property '"
                                + dp.name() + "' of " + cd.qualifiedName());
            }
            functions.requireFunction(
                    realizedFqn(dp.realization(), SynthFqn.prop(cd.qualifiedName(), dp.name())),
                    "derived property '" + dp.name() + "' of " + cd.qualifiedName());
        }
        for (ConstraintDefinition con : cd.constraints()) {
            String fqn = realizedFqn(con.realization(),
                    SynthFqn.constraint(cd.qualifiedName(), con.name()));
            String site = "constraint '" + con.name() + "' of " + cd.qualifiedName();
            functions.requireFunction(fqn, site);
            functions.requireShape(fqn, FunctionCompiler::returnsBooleanOne,
                    site, "returning Boolean[1]");
        }
    }

    /** Two definitions with the SAME dispatch identity
     * ({@link Function#signatureKey()} — name + canonical parameter
     * spellings) can never be told apart at a call site: real pure
     * rejects the second definition; silently letting one win answers
     * calls with an arbitrary body. */
    private static void checkDuplicateSignatures(ModelBuilder model,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink,
            java.util.function.Predicate<Object> fresh, java.util.Set<com.legend.model.FunctionId> priorKeys) {
        java.util.Set<com.legend.model.FunctionId> seen = new java.util.HashSet<>();
        for (Function f : model.functions().filter(fresh).toList()) {
            com.legend.model.FunctionId key = com.legend.model.FunctionId.of(f);
            if (priorKeys.contains(key) || !seen.add(key)) {
                withElement(key.qualified(), () -> {
                    throw new com.legend.error.ModelException(
                            com.legend.error.LegendCompileException.Phase.MODEL,
                            "function '" + f.qualifiedName()
                                    + "' is defined more than once with the same"
                                    + " signature — calls would be ambiguous");
                }, wallSink);
            }
        }
    }

    /** Function signature references (params + return). */
    private static void checkFunction(Function f, TypeClassifier classifier, ModelBuilder model) {
        requireAnnotations(f.stereotypes(), f.taggedValues(), model);
        // a declaration's types name their classes' arguments; lite's own synthesized signatures name a mapped class
        // bare (TypeClassifier.requireArguments)
        boolean declared = !(f instanceof com.legend.model.FunctionDefinition fd && fd.isSynthesized());
        for (var p : f.parameters()) {
            classifier.classify(p.type(), f.typeParameters());
            if (declared) {
                classifier.requireArguments(p.type(), f.typeParameters());
            }
        }
        classifier.classify(f.returnType(), f.typeParameters());
        if (declared) {
            classifier.requireArguments(f.returnType(), f.typeParameters());
        }
    }

    /**
     * F.b mapping-binding integrity: every class binding names a real class
     * realized by a real {@code (): Class[*]} function; every association
     * binding names a real association realized by a real
     * {@code (Source, Target): Boolean[1]} predicate.
     */
    private static void checkMapping(MappingDefinition md, ModelBuilder model,
                                     TypeClassifier classifier, FunctionCompiler functions) {
        // an include's store substitutions name databases (legend-engine resolves both sides,
        // HelperRelationalBuilder.resolveDatabase); the included mapping itself is the normalizer's to find
        for (com.legend.model.MappingInclude inc : md.includes()) {
            for (com.legend.model.MappingInclude.StoreSubstitution sub : inc.substitutions()) {
                for (String store : List.of(sub.originalStore(), sub.replacementStore())) {
                    if (model.findDatabase(store).isEmpty()) {
                        throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MAPPING,
                                "Can't find database '" + store + "'");
                    }
                }
            }
        }
        for (MappingDefinition.ClassBinding cb : md.classBindings()) {
            if (!classifier.isClassFqn(cb.classFqn())) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, "mapping '" + md.qualifiedName()
                      + "' binds unknown class '" + cb.classFqn() + "'");
            }
            String site = "mapping '" + md.qualifiedName()
                    + "' class binding for '" + cb.classFqn() + "'";
            functions.requireFunction(cb.functionFqn(), site);
            functions.requireShape(cb.functionFqn(),
                    f -> f.parameters().isEmpty() && functions.returnsClassMany(f),
                    site, "of the form (): Class[*]");
        }
        for (MappingDefinition.AssociationBinding ab : md.associationBindings()) {
            if (model.findAssociation(ab.associationFqn()).isEmpty()) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, "mapping '" + md.qualifiedName()
                      + "' binds unknown association '" + ab.associationFqn() + "'");
            }
            String site = "mapping '" + md.qualifiedName()
                    + "' association binding for '" + ab.associationFqn() + "'";
            functions.requireFunction(ab.predicateFunctionFqn(), site);
            functions.requireShape(ab.predicateFunctionFqn(),
                    f -> f.parameters().size() == 2 && FunctionCompiler.returnsBooleanOne(f),
                    site, "of the form (source, target): Boolean[1]");
        }
    }

    private static String realizedFqn(Realization r, String liftedFqn) {
        return r instanceof Realization.Ref ref ? ref.functionFqn() : liftedFqn;
    }

    // ====================================================================
    // D6b frontend-leniency batch: checks below reject models the engine
    // rejects at compile but that previously slid through to a lazy crash
    // (StackOverflow on cyclic extends), a silent last-wins, or wrong SQL.
    // ====================================================================

    /** Protocol multiplicity bounds must be well-formed ({@code [2..1]}
     * previously surfaced lazily as a bare IllegalArgumentException). */
    private static void requireValidBounds(
            com.legend.protocol.Multiplicity m, String site) {
        try {
            TypeClassifier.multiplicity(m);
        } catch (IllegalArgumentException e) {
            throw new com.legend.error.ModelException(
                    com.legend.error.LegendCompileException.Phase.MODEL,
                    site + ": invalid multiplicity — " + e.getMessage());
        }
    }

    /** Engine parity: "Found duplicated value 'X' in enumeration 'E'". */
    private static void checkEnum(com.legend.model.EnumDefinition ed) {
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (String v : ed.values()) {
            if (!seen.add(v)) {
                throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.MODEL,
                        "Found duplicated value '" + v + "' in enumeration '"
                                + ed.qualifiedName() + "'");
            }
        }
    }

    /** Inheritance must be acyclic — a cycle previously compiled fine and
     * blew the stack only when the class was DEMANDED. Walks resolved
     * supers ({@code headFqn} + classDef lookup); unresolvable heads are
     * the classify checks' concern, not this walk's. */
    private static void checkInheritanceAcyclic(ModelBuilder model,
            TypeClassifier classifier,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink,
            java.util.function.Predicate<Object> fresh) {
        // a prior layer cannot reach a graph class (it was checked alone),
        // so every cycle through a graph class starts at one
        java.util.Set<String> acyclic = new java.util.HashSet<>();
        model.classes().filter(fresh).forEach(cd -> withElement(cd.qualifiedName(),
                () -> walkSupers(cd, classifier,
                        new java.util.LinkedHashSet<>(), acyclic), wallSink));
    }

    private static void walkSupers(ClassDefinition cd, TypeClassifier classifier,
            java.util.LinkedHashSet<String> path, java.util.Set<String> acyclic) {
        String fqn = cd.qualifiedName();
        if (acyclic.contains(fqn)) {
            return;
        }
        if (!path.add(fqn)) {
            throw new com.legend.error.ModelException(
                    com.legend.error.LegendCompileException.Phase.MODEL,
                    "Inheritance cycle: "
                            + String.join(" -> ", path) + " -> " + fqn);
        }
        for (com.legend.protocol.TypeExpression sup : cd.superClasses()) {
            String supFqn;
            try {
                supFqn = TypeClassifier.headFqn(sup);
            } catch (com.legend.error.ModelException e) {
                continue; // malformed head — checkClass's classify reports it
            }
            classifier.classDef(supFqn)
                    .ifPresent(sc -> walkSupers(sc, classifier, path, acyclic));
        }
        path.remove(fqn);
        acyclic.add(fqn);
    }

    /**
     * Ghost store cross-refs: every {@code ColumnRef} inside a join or
     * filter condition must name a table (or view) declared in the
     * database's include closure, and a column declared ON that table —
     * previously accepted and shipped as SQL that failed (or silently
     * misbehaved) in the database. Conservative by design: {@code
     * &lcub;target&rcub;} refs and refs whose home cannot be resolved
     * unambiguously (explicit foreign {@code databaseName}) are SKIPPED,
     * never false-positived.
     */
    private static void checkDatabase(com.legend.model.DatabaseDefinition db,
            ModelBuilder model) {
        for (String include : db.includes()) {
            if (model.findDatabase(include).isEmpty()) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL,
                        "Can't find database '" + include + "'");
            }
        }
        for (var j : db.joins()) {
            checkStoreRefs(db, model, j.operation(), "join '" + j.name() + "'");
        }
        for (var f : db.filters()) {
            checkStoreRefs(db, model, f.condition(), "filter '" + f.name() + "'");
        }
        for (var f : db.multiGrainFilters()) {
            checkStoreRefs(db, model, f.condition(),
                    "multigrain filter '" + f.name() + "'");
        }
        for (var td : db.tables()) {
            checkMilestoningColumnTypes(db, td);
        }
    }

    /**
     * A milestoning date column must hold a date: one declared OTHER or DISTINCT is a String READ AS
     * TEXT (StoreCompiler.columnType), and comparing dates as text is silently wrong -- refused, by name
     * (docs/STORE_TYPES_HOMEWORK_2026_10_02.md, decision 5).
     */
    private static void checkMilestoningColumnTypes(com.legend.model.DatabaseDefinition db,
            com.legend.model.DatabaseDefinition.TableDefinition td) {
        var m = td.milestoning();
        if (m == null) {
            return;
        }
        var b = m.business();
        var p = m.processing();
        for (String mc : new String[] {
                b == null ? null : b.from(), b == null ? null : b.thru(),
                b == null ? null : b.snapshotDate(),
                p == null ? null : p.in(), p == null ? null : p.out(),
                p == null ? null : p.snapshotDate()}) {
            for (var c : td.columns()) {
                if (c.name().equals(mc) && (c.dataType() instanceof com.legend.model.RelationalDataType.Other
                        || c.dataType() instanceof com.legend.model.RelationalDataType.Distinct)) {
                    throw new com.legend.error.ModelException(
                            com.legend.error.LegendCompileException.Phase.MODEL,
                            "The milestoning column '" + mc + "' of table '" + td.name() + "' in database '"
                                    + db.qualifiedName() + "' is declared " + c.dataType().getClass().getSimpleName()
                                    + ": a milestoning date must be a DATE or TIMESTAMP column");
                }
            }
        }
    }

    private static void checkStoreRefs(com.legend.model.DatabaseDefinition db,
            ModelBuilder model, com.legend.model.RelationalOperation op,
            String site) {
        if (op instanceof com.legend.model.RelationalOperation.ColumnRef cr) {
            // An explicit FOREIGN database qualifier resolves through
            // model-level lookup subtleties this pass doesn't own — skip.
            if (cr.databaseName() == null
                    || cr.databaseName().equals(db.qualifiedName())) {
                checkColumnRef(db, model, cr, site);
            }
        }
        for (com.legend.model.RelationalOperation child : op.children()) {
            checkStoreRefs(db, model, child, site);
        }
    }

    private static void checkColumnRef(com.legend.model.DatabaseDefinition db,
            ModelBuilder model,
            com.legend.model.RelationalOperation.ColumnRef cr, String site) {
        var table = model.findTableDefinition(db.qualifiedName(), cr.table());
        if (table.isPresent()) {
            if (!hasColumn(table.get(), cr.column())) {
                throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.MODEL,
                        "The column '" + cr.column()
                                + "' can't be found in the table '"
                                + table.get().name() + "' (" + site
                                + " of database '" + db.qualifiedName() + "')");
            }
            return;
        }
        var view = model.findView(db.qualifiedName(), cr.table());
        if (view.isPresent()) {
            boolean hit = view.get().columnMappings().stream()
                    .anyMatch(cm -> cm.name().equals(cr.column()));
            if (!hit) {
                throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.MODEL,
                        "The column '" + cr.column()
                                + "' can't be found in the view '"
                                + view.get().name() + "' (" + site
                                + " of database '" + db.qualifiedName() + "')");
            }
            return;
        }
        throw new com.legend.error.ModelException(
                com.legend.error.LegendCompileException.Phase.MODEL,
                "The table '" + cr.table()
                        + "' can't be found in the database '"
                        + db.qualifiedName() + "' (" + site + ")");
    }

    /** Declared columns + milestoning-declared temporal columns, by bare name
     * (a reference and a declaration both keep the bare name; quoting is a
     * spelling — model.RelationalIdentifier). */
    private static boolean hasColumn(
            com.legend.model.DatabaseDefinition.TableDefinition td, String name) {
        for (var c : td.columns()) {
            if (c.name().equals(name)) {
                return true;
            }
        }
        var m = td.milestoning();
        if (m != null) {
            var b = m.business();
            var p = m.processing();
            for (String mc : new String[] {
                    b == null ? null : b.from(), b == null ? null : b.thru(),
                    b == null ? null : b.snapshotDate(),
                    p == null ? null : p.in(), p == null ? null : p.out(),
                    p == null ? null : p.snapshotDate()}) {
                if (name.equals(mc)) {
                    return true;
                }
            }
        }
        return false;
    }

}
