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
        // D6b: element-identity first, so a duplicated FQN poisons with
        // ITS reason rather than a downstream confusion from whichever
        // definition happened to win the last-wins slot.
        model.duplicateElements().forEach((fqn, msg) ->
                withElement(fqn, () -> {
                    throw new com.legend.error.ModelException(
                            com.legend.error.LegendCompileException.Phase.MODEL, msg);
                }, wallSink));
        model.classes().filter(fresh).forEach(cd -> withElement(cd.qualifiedName(),
                () -> checkClass(cd, classifier, functions), wallSink));
        checkInheritanceAcyclic(model, classifier, wallSink, fresh);
        model.functions().filter(fresh).forEach(f -> withElement(f.qualifiedName(),
                () -> checkFunction(f, classifier), wallSink));
        checkDuplicateSignatures(model, wallSink, fresh,
                prior == null ? java.util.Set.of() : prior.signatureKeys());
        model.associations().filter(fresh).forEach(a -> withElement(a.qualifiedName(), () -> {
            classifier.classify(a.property1().targetClass(), List.of());
            classifier.classify(a.property2().targetClass(), List.of());
        }, wallSink));
        model.enums().filter(fresh).forEach(ed -> withElement(ed.qualifiedName(),
                () -> checkEnum(ed), wallSink));
        model.databases().filter(fresh).forEach(db -> withElement(db.qualifiedName(),
                () -> checkDatabase(db, model), wallSink));
        model.mappings().filter(fresh).forEach(md -> withElement(md.qualifiedName(),
                () -> checkMapping(md, model, classifier, functions), wallSink));
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
                                   FunctionCompiler functions) {
        List<String> typeParams = cd.typeParams();
        java.util.Set<String> propertyNames = new java.util.HashSet<>();
        for (ClassDefinition.PropertyDefinition pd : cd.properties()) {
            if (!propertyNames.add(pd.name())) {
                throw new com.legend.error.ModelException(
                        com.legend.error.LegendCompileException.Phase.MODEL,
                        "Found duplicated property '" + pd.name()
                                + "' in class '" + cd.qualifiedName() + "'");
            }
            classifier.classify(pd.type(), typeParams);
            requireValidBounds(pd.multiplicity(),
                    "property '" + pd.name() + "' of " + cd.qualifiedName());
        }
        for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
            classifier.classify(dp.type(), typeParams);
            requireValidBounds(dp.multiplicity(),
                    "derived property '" + dp.name() + "' of " + cd.qualifiedName());
            for (ParameterDefinition p : dp.parameters()) {
                classifier.classify(p.type(), typeParams);
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
                withElement(f.qualifiedName(), () -> {
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
    private static void checkFunction(Function f, TypeClassifier classifier) {
        for (var p : f.parameters()) {
            classifier.classify(p.type(), f.typeParameters());
        }
        classifier.classify(f.returnType(), f.typeParameters());
    }

    /**
     * F.b mapping-binding integrity: every class binding names a real class
     * realized by a real {@code (): Class[*]} function; every association
     * binding names a real association realized by a real
     * {@code (Source, Target): Boolean[1]} predicate.
     */
    private static void checkMapping(MappingDefinition md, ModelBuilder model,
                                     TypeClassifier classifier, FunctionCompiler functions) {
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
