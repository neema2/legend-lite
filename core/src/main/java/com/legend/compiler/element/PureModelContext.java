package com.legend.compiler.element;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.element.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The concrete {@link ModelContext} for a single compilation unit &mdash; the
 * Phase-F <strong>façade</strong>, realized per the north-star tenet
 * (<i>Total Knowledge, Demand-Driven Work</i>; {@code docs/TENETS.md}). It owns
 * the memo caches and the lookup surface, and nothing else: each element kind
 * compiles in its own compiler (the Phase-G checker layout, mirrored) &mdash;
 * {@link TypeClassifier} is the shared name&rarr;kind kernel,
 * {@link FunctionCompiler}/{@link ClassCompiler} materialize {@code Typed*}
 * records on demand, {@link ModelIntegrity} runs the eager F.a/F.b
 * reference-safety pass at construction, {@link StoreCompiler} resolves table schemas.
 *
 * <ul>
 *   <li><b>{@link #findType(String)} &mdash; Knowledge, cheap.</b> Kind
 *       classification by FQN existence; builds no structure. Unknown FQN
 *       returns empty &mdash; classifying call sites turn that into a compile
 *       error (AGENTS.md invariant 4), never a fallback.</li>
 *   <li><b>{@link #findClass}/{@link #findEnum}/{@link #findFunction} &mdash;
 *       Work, lazy.</b> Materialized on demand and memoized; type references
 *       inside are FQN-only kinds, never forcing the referent (invariant 5).</li>
 * </ul>
 *
 * <p>One-shot and immutable after construction modulo the memo caches; the
 * backing {@link ModelBuilder} is read-only. Not thread-safe (plain maps);
 * a single compile is single-threaded.
 */
public final class PureModelContext implements ModelContext {

    private final ModelBuilder model;
    private final TypeClassifier classifier;
    private final FunctionCompiler functions;
    private final ClassCompiler classes;

    // Demand-driven materialization caches (Work; memoized).
    private final Map<String, TypedClass> classCache = new HashMap<>();
    private final Map<String, TypedEnum> enumCache = new HashMap<>();
    private final Map<String, List<TypedFunction>> functionCache = new HashMap<>();

    public PureModelContext(ModelBuilder model) {
        this.model = Objects.requireNonNull(model, "model");
        this.classifier = new TypeClassifier(model);
        this.functions = new FunctionCompiler(model, classifier);
        this.classes = new ClassCompiler(classifier, functions);
        // F.a + F.b: THE eager reference-safety pass — every reference every
        // element makes (types, realizers, mapping bindings, association ends)
        // is checked once, whole-model, before this context exists.
        ModelIntegrity.check(model, classifier, this.functions);
    }

    /**
     * Build from a Phase-E {@link com.legend.parser.NormalizedModel}. The
     * parameter type is the phase gate ({@code docs/CLEAN_SHEET_INVERSION.md}
     * &sect;4): Phase F demands a normalized model at the signature level, so
     * an un-normalized {@code ParsedModel} cannot reach element compilation.
     */
    public static PureModelContext from(com.legend.parser.NormalizedModel normalized) {
        // THE Phase-E -> Phase-F gate: element compilation demands a
        // normalized model AT THE SIGNATURE LEVEL. (ModelBuilder itself is
        // phase-agnostic indexing and must not depend on the normalizer —
        // that was the compiler<->normalizer package cycle.)
        return new PureModelContext(ModelBuilder.from(new com.legend.parser.ParsedModel(
                normalized.elements(), normalized.imports())));
    }

    @Override
    public Optional<Type> findType(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        return classifier.findType(fqn);
    }

    @Override
    public Optional<TypedClass> findClass(String fqn) {
        TypedClass cached = classCache.get(fqn);
        if (cached != null) {
            return Optional.of(cached);
        }
        return classifier.classDef(fqn).map(def -> {
            TypedClass typed = classes.compile(def);
            classCache.put(fqn, typed);
            return typed;
        });
    }

    @Override
    public Optional<TypedEnum> findEnum(String fqn) {
        TypedEnum cached = enumCache.get(fqn);
        if (cached != null) {
            return Optional.of(cached);
        }
        // A typed enum is its name + values — zero compilation logic, so no
        // ceremonial EnumCompiler (same judgment as trivial CoreFn arms).
        return classifier.enumDef(fqn).map(def -> {
            TypedEnum typed = new TypedEnum(def.qualifiedName(), def.values());
            enumCache.put(fqn, typed);
            return typed;
        });
    }

    @Override
    public List<TypedFunction> findFunction(String fqn) {
        List<TypedFunction> cached = functionCache.get(fqn);
        if (cached != null) {
            return cached;
        }
        List<TypedFunction> result = functions.compileAll(fqn);
        functionCache.put(fqn, result);
        return result;
    }

    @Override
    public Optional<Property> findProperty(String classFqn, String name) {
        Optional<TypedClass> tc = findClass(classFqn);
        if (tc.isEmpty()) {
            return Optional.empty();
        }
        for (Property p : tc.get().properties()) {
            if (p.name().equals(name)) {
                return Optional.of(p);
            }
        }
        for (String superFqn : tc.get().superClassFqns()) {
            Optional<Property> inherited = findProperty(superFqn, name);
            if (inherited.isPresent()) {
                return inherited;
            }
        }
        // The contract's third leg: ASSOCIATION-INJECTED navigation properties
        // resolve at lookup time from the association index — never stored on
        // TypedClass (Property doc §5 discipline 3). Superclass-declared
        // associations are found through the recursion above.
        return model.findAssociationEnd(classFqn, name).map(end ->
                new Property.Stored(end.propertyName(),
                        classifier.classify(end.targetClass(), java.util.List.of()),
                        TypeClassifier.multiplicity(end.multiplicity())));
    }

    @Override
    public java.util.Optional<com.legend.parser.element.MappingDefinition> findMapping(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        return model.findMapping(fqn);
    }

    @Override
    public java.util.Optional<com.legend.parser.element.RuntimeDefinition> findRuntime(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        return model.findRuntime(fqn);
    }

    @Override
    public boolean isExecutionContextElement(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        return model.findMapping(fqn).isPresent()
                || model.findLegacyMapping(fqn).isPresent()
                || model.findRuntime(fqn).isPresent()
                || model.findConnection(fqn).isPresent()
                || model.findDatabase(fqn).isPresent();
    }

    @Override
    public Optional<Type.RelationType> findTable(String dbFqn, String name) {
        Objects.requireNonNull(dbFqn, "dbFqn");
        Objects.requireNonNull(name, "name");
        return model.findDatabase(dbFqn)
                .flatMap(db -> StoreCompiler.resolveTable(db, name));
    }
}
