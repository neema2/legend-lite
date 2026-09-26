package com.legend.compiler.element;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.element.type.Type;

import java.util.ArrayList;
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
    /** The platform's registrations — the implementation table's input beside the declarations. */
    private final com.legend.platform.Registrations registrations;
    private com.legend.platform.@com.legend.base.Nullable DeclarationTable declarations;
    private com.legend.platform.@com.legend.base.Nullable ImplementationTable implementations;
    private final TypeClassifier classifier;
    private final FunctionCompiler functions;
    private final ClassCompiler classes;

    // Demand-driven materialization caches (Work; memoized).
    private final Map<String, TypedClass> classCache;
    private final Map<String, TypedEnum> enumCache;
    private final Map<String, List<TypedFunction>> functionCache;
    /** {@link #derived}: graph-lifetime facts, shared with overlays. */
    private final Map<Class<?>, Object> derivedCache;
    /** DRIVER-SUPPLIED execution elements (PHASE_K_EXECUTION.md §4):
     * an overlay VIEW resolves exactly these two fqns to the supplied
     * records; null on ordinary contexts. */
    private final com.legend.model.@com.legend.base.Nullable RuntimeDefinition overlayRuntime;
    private final com.legend.model.@com.legend.base.Nullable ConnectionDefinition overlayConnection;

    public PureModelContext(ModelBuilder model, com.legend.platform.Registrations registrations) {
        this(model, null, registrations);
    }

    /** TOLERANT integrity (module compile): a non-null {@code wallSink}
     * collects EVERY failing element in one pass; the caller drops them
     * and rebuilds — the strict form throws on the first. */
    public PureModelContext(ModelBuilder model,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink,
            com.legend.platform.Registrations registrations) {
        this(model, wallSink, null, registrations);
    }

    private PureModelContext(ModelBuilder model,
            java.util.@com.legend.base.Nullable Map<String, String> wallSink,
            @com.legend.base.Nullable CheckedLayer prior,
            com.legend.platform.Registrations registrations) {
        this.model = Objects.requireNonNull(model, "model");
        this.registrations = Objects.requireNonNull(registrations, "registrations");
        this.classifier = new TypeClassifier(model);
        this.functions = new FunctionCompiler(model, classifier);
        this.classes = new ClassCompiler(classifier, functions);
        this.classCache = new HashMap<>();
        this.enumCache = new HashMap<>();
        this.functionCache = new HashMap<>();
        this.derivedCache = new java.util.concurrent.ConcurrentHashMap<>();
        this.overlayRuntime = null;
        this.overlayConnection = null;
        // F.a + F.b: THE eager reference-safety pass — every reference every
        // element makes (types, realizers, mapping bindings, association ends)
        // is checked once, whole-model, before this context exists.
        ModelIntegrity.check(model, classifier, this.functions, wallSink, prior);
    }

    /**
     * A layer whose elements passed the integrity check ON THEIR OWN (the
     * boot layer, once per process): the element instances, by identity,
     * and their functions' dispatch signatures. A graph built over it checks
     * only its own elements and the rules that span the two
     * ({@link ModelIntegrity#check}).
     */
    public static final class CheckedLayer {
        private final java.util.Set<Object> elements =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        private final java.util.Set<com.legend.model.FunctionId> signatureKeys = new java.util.HashSet<>();

        boolean contains(Object element) {
            return elements.contains(element);
        }

        java.util.Set<com.legend.model.FunctionId> signatureKeys() {
            return signatureKeys;
        }
    }

    /** Check {@code normalized}'s layer on its own, STRICTLY (a failure is a
     * platform bug and throws), over the index Phase E built for it. */
    public static CheckedLayer checkLayer(com.legend.model.NormalizedModel normalized,
            ModelBuilder index, com.legend.platform.Registrations registrations) {
        PureModelContext alone = from(normalized, index, registrations);
        CheckedLayer out = new CheckedLayer();
        alone.model.classes().forEach(out.elements::add);
        alone.model.functions().forEach(f -> {
            out.elements.add(f);
            out.signatureKeys.add(com.legend.model.FunctionId.of(f));
        });
        alone.model.associations().forEach(out.elements::add);
        alone.model.enums().forEach(out.elements::add);
        alone.model.databases().forEach(out.elements::add);
        alone.model.mappings().forEach(out.elements::add);
        return out;
    }

    /**
     * Build from a Phase-E {@link com.legend.model.NormalizedModel} over THE
     * graph's one index. The parameter type is the phase gate
     * ({@code docs/CLEAN_SHEET_INVERSION.md} &sect;4): Phase F demands a
     * normalized model at the signature level, so an un-normalized
     * {@code ParsedModel} cannot reach element compilation.
     */
    public static PureModelContext from(com.legend.model.NormalizedModel normalized,
            ModelBuilder index, com.legend.platform.Registrations registrations) {
        return from(normalized, index, null, null, registrations);
    }

    /** {@link #from} with a tolerant integrity wall sink (module compile). */
    public static PureModelContext from(com.legend.model.NormalizedModel normalized,
            ModelBuilder index, java.util.@com.legend.base.Nullable Map<String, String> wallSink,
            com.legend.platform.Registrations registrations) {
        return from(normalized, index, wallSink, null, registrations);
    }

    /** {@link #from} over a graph that includes an already-checked layer. */
    public static PureModelContext from(com.legend.model.NormalizedModel normalized,
            ModelBuilder index, java.util.@com.legend.base.Nullable Map<String, String> wallSink,
            @com.legend.base.Nullable CheckedLayer prior, com.legend.platform.Registrations registrations) {
        // THE Phase-E -> Phase-F gate (T4.1 step 2): the index Phase E read
        // gains Phase E's products — the compiled mappings (their facts
        // stamped on them), the lifted functions — and the boot layer's
        // prepared elements; the pass-through structural elements are
        // already there and are skipped. Nothing is re-indexed.
        index.add(normalized.elements());
        // the pre-Door-1 mapping surfaces ride as an ANALYSIS archive
        // (static lineage #44) — F+ compilation never reads them
        normalized.legacySurfaces().values()
                .forEach(index::retainLegacySurface);
        return new PureModelContext(index, wallSink, prior, registrations);
    }

    @Override
    public Optional<Type> findType(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        return classifier.findType(fqn);
    }

    /** Each asked class's REACH through its compiled supers (itself
     * included; a class that does not compile is a dead end) — the
     * hierarchy's closure, walked once per class, so a subtype test is a
     * membership test. Was: a memo of (child, parent) answers keyed by a
     * string built per call, growing with the square of the classes (it
     * filled an 8 GB heap on a 20K-class model's queries). */
    private static final class Reach {
        final java.util.Map<String, Walked> of =
                new java.util.concurrent.ConcurrentHashMap<>();
    }

    /** A class's reach, and the first class on it that failed to compile
     * (a poisoned or unknown super): its supers are not walked. */
    private record Walked(java.util.Set<String> classes,
            com.legend.error.@com.legend.base.Nullable LegendCompileException failure) {
    }

    @Override
    public java.util.Set<String> subtree(String baseFqn) {
        return model.knowledge().subtree(baseFqn);
    }

    @Override
    public boolean isSubtype(String childFqn, String parentFqn) {
        if (childFqn.equals(parentFqn)) {
            return true;
        }
        Walked r = reach(childFqn);
        // Nil is the BOTTOM type — a subtype of every type (ModelContext's
        // walk: reaching it answers true whatever the parent)
        if (r.classes().contains(parentFqn)
                || r.classes().contains(com.legend.compiler.element.type.PlatformTypes.NIL)) {
            return true;
        }
        // a NO walked everything reachable, so a class that failed to
        // compile on the way is this question's failure (as it always was)
        if (r.failure() != null) {
            throw r.failure();
        }
        return false;
    }

    private Walked reach(String cls) {
        Reach memo = derived(Reach.class, c -> new Reach());
        Walked known = memo.of.get(cls);
        if (known != null) {
            return known;
        }
        java.util.Set<String> out = new java.util.HashSet<>();
        com.legend.error.LegendCompileException failure = null;
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>();
        work.add(cls);
        while (!work.isEmpty()) {
            String cur = work.poll();
            if (!out.add(cur) || cur.equals(com.legend.compiler.element.type.PlatformTypes.NIL)) {
                continue;
            }
            try {
                findClass(cur).ifPresent(tc -> work.addAll(tc.superClassFqns()));
            } catch (com.legend.error.LegendCompileException e) {
                if (failure == null) {
                    failure = e;
                }
            }
        }
        Walked walked = new Walked(java.util.Set.copyOf(out), failure);
        memo.of.putIfAbsent(cls, walked);
        return walked;
    }

    @Override
    public <T> T derived(Class<T> key,
            java.util.function.Function<ModelContext, T> derive) {
        // get-then-put, not computeIfAbsent: a derivation may itself read
        // another derived fact (ConcurrentHashMap forbids the recursion);
        // a rare double derivation of a pure function is harmless. The
        // derivation sees the BASE graph, never an overlay view.
        Object hit = derivedCache.get(key);
        if (hit == null) {
            hit = Objects.requireNonNull(derive.apply(this), "derived fact");
            derivedCache.putIfAbsent(key, hit);
            hit = derivedCache.get(key);
        }
        return key.cast(hit);
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
    public Optional<com.legend.model.ClassDefinition>
            findClassDefinition(String fqn) {
        return classifier.classDef(fqn);
    }

    @Override
    public Optional<com.legend.model.LegacyMappingDefinition>
            findLegacyMapping(String fqn) {
        return model.findLegacyMapping(fqn);
    }

    /** The name resolver's candidate universe as a LIVE VIEW: membership
     * answers from the model's symbol table and the platform's constant
     * type set, nothing materialized per query (leg 6e — the corpus lane
     * rebuilt every element name per query). Iteration materializes
     * {@link #elementFqns()}; the resolver only asks {@code contains}. */
    @Override
    public java.util.Set<String> resolutionUniverse() {
        java.util.Set<String> platform = com.legend.compiler.NameResolver.platformFqns();
        java.util.Set<String> extensions = model.primitiveExtensionFqns();
        return new java.util.AbstractSet<>() {
            @Override
            public boolean contains(Object o) {
                return o instanceof String s
                        && (platform.contains(s) || extensions.contains(s) || model.hasElement(s)
                                // a graph function's SIGNATURE ID is its element
                                // name upstream: registered with the function
                                || model.hasFunctionId(s));
            }

            @Override
            public java.util.Iterator<String> iterator() {
                java.util.Set<String> all = new java.util.HashSet<>(elementFqns());
                all.addAll(platform);
                return java.util.Collections.unmodifiableSet(all).iterator();
            }

            @Override
            public int size() {
                java.util.Set<String> all = new java.util.HashSet<>(elementFqns());
                all.addAll(platform);
                return all.size();
            }
        };
    }

    @Override
    public java.util.Set<String> elementFqns() {
        java.util.Set<String> out = new java.util.HashSet<>();
        model.classes().forEach(e -> out.add(e.qualifiedName()));
        model.enums().forEach(e -> out.add(e.qualifiedName()));
        model.measures().forEach(e -> out.add(e.qualifiedName()));
        model.associations().forEach(e -> out.add(e.qualifiedName()));
        model.mappings().forEach(e -> out.add(e.qualifiedName()));
        model.legacyMappings().forEach(e -> out.add(e.qualifiedName()));
        model.databases().forEach(e -> out.add(e.qualifiedName()));
        model.runtimes().forEach(e -> out.add(e.qualifiedName()));
        model.functions().forEach(e -> out.add(e.qualifiedName()));
        // primitive extensions (Primitive X extends Integer) — without
        // this a simple @ExtendedInteger under a wildcard import never
        // qualifies, and findPrimitiveExtension's EXACT-FQN lookup
        // (its documented precondition: "extensions are in knownFqns")
        // never matches (leg 7b R0)
        out.addAll(model.primitiveExtensionFqns());
        // platform-native enums resolve like parsed ones — an unqualified
        // DatabaseType.H2 under `import meta::relational::runtime::*`
        // must qualify (findEnum already falls back to the native
        // catalog; name resolution has to see the same surface)
        com.legend.builtin.Pure.allNativeEnums()
                .forEach(e -> out.add(e.qualifiedName()));
        return out;
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
    public List<TypedFunction> findFunctionById(String qualifiedId) {
        List<com.legend.model.Function> defs = new ArrayList<>(model.findFunctionById(qualifiedId));
        com.legend.model.NativeFunctionDefinition n = com.legend.builtin.Pure.nativeFunctionById(qualifiedId);
        if (n != null) {
            defs.add(n);
        }
        List<TypedFunction> out = new ArrayList<>(defs.size());
        for (com.legend.model.Function d : defs) {
            for (TypedFunction tf : findFunction(d.qualifiedName())) {
                if (d.equals(tf.definition())) {
                    out.add(tf);
                }
            }
        }
        return out;
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
    public java.util.Optional<com.legend.model.MeasureDefinition> findMeasure(String fqn) {
        return model.findMeasure(fqn);
    }

    @Override
    public boolean isPackage(String fqn) {
        if (packages.isEmpty()) {
            for (String el : elementFqns()) {
                String[] segs = el.split("::");
                StringBuilder prefix = new StringBuilder();
                for (int i = 0; i < segs.length - 1; i++) {
                    if (i > 0) {
                        prefix.append("::");
                    }
                    prefix.append(segs[i]);
                    packages.add(prefix.toString());
                }
            }
        }
        return packages.contains(fqn);
    }

    /** The package index — every proper prefix of an element FQN, built once. */
    private final java.util.Set<String> packages = new java.util.HashSet<>();

    @Override
    public java.util.Optional<com.legend.model.ProfileDefinition> findProfile(String fqn) {
        return model.findProfile(fqn);
    }

    @Override
    public java.util.Optional<com.legend.model.MappingDefinition> findMapping(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        return model.findMapping(fqn);
    }

    /** T4.1 step 4b: the SURFACE facts Phase F reads off the compiled
     * mapping — an Operation union's member classes, a class-typed
     * property's routed target class — stamped at Phase E from the
     * authored mapping's surface; nothing here re-reads a legacy record. */
    @Override
    public java.util.@com.legend.base.Nullable List<String> unionMemberClasses(
            String mappingFqn, String classFqn) {
        return model.findMapping(mappingFqn)
                .map(md -> md.facts().unionMembers().get(classFqn)).orElse(null);
    }

    @Override
    public @com.legend.base.Nullable String routedTargetClass(String mappingFqn,
            String ownerClass, String prop) {
        return model.findMapping(mappingFqn)
                .map(md -> md.facts().routedTargetClasses().getOrDefault(ownerClass, java.util.Map.of())
                        .get(prop))
                .orElse(null);
    }

    @Override
    public java.util.@com.legend.base.Nullable List<String> mixedUnionMembers(String mappingFqn,
            String classFqn) {
        return model.findMapping(mappingFqn)
                .map(md -> md.facts().mixedUnions().get(classFqn)).orElse(null);
    }

    @Override
    public java.util.@com.legend.base.Nullable List<com.legend.model.KeyThread> unionKeyThreads(
            String mappingFqn, String classFqn) {
        return model.findMapping(mappingFqn)
                .map(md -> md.facts().unionKeyThreads().get(classFqn)).orElse(null);
    }

    /** The compiled mapping's stamped facts (T4.1 step 2): the poison
     * ledger, mixed unions and key threads are read HERE, off the artifact
     * Phase E produced — never from a side channel on the index. */
    @Override
    public java.util.Optional<String> mappingPoison(String mappingFqn, String classFqn) {
        return model.findMapping(mappingFqn)
                .map(md -> md.facts().poisons().get(new com.legend.model.PoisonKey.ForClass(classFqn)));
    }

    @Override
    public java.util.Optional<String> mappingSetPoison(String mappingFqn, String classFqn,
            String setId) {
        return model.findMapping(mappingFqn)
                .map(md -> md.facts().poisons().get(
                        new com.legend.model.PoisonKey.ForSet(classFqn, setId)));
    }

    @Override
    public java.util.Optional<String> mappingAssociationPoison(String mappingFqn,
            String associationFqn) {
        return model.findMapping(mappingFqn)
                .map(md -> md.facts().poisons().get(
                        new com.legend.model.PoisonKey.ForAssociation(associationFqn)));
    }

    public java.util.Optional<com.legend.model.RuntimeDefinition> findRuntime(
            @com.legend.base.Nullable String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        if (overlayRuntime != null
                && overlayRuntime.qualifiedName().equals(fqn)) {
            return java.util.Optional.of(overlayRuntime);
        }
        return model.findRuntime(fqn);
    }

    @Override
    public java.util.Optional<com.legend.model.DataDefinition> findData(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        return model.findData(fqn);
    }

    /** Overlay view: SHARES the base context wholesale (model, compilers,
     * memo caches) and adds the driver-supplied runtime + connection. */
    private PureModelContext(PureModelContext base,
            com.legend.model.RuntimeDefinition runtime,
            com.legend.model.ConnectionDefinition connection) {
        this.model = base.model;
        this.classifier = base.classifier;
        this.functions = base.functions;
        this.classes = base.classes;
        this.classCache = base.classCache;
        this.enumCache = base.enumCache;
        this.functionCache = base.functionCache;
        this.derivedCache = base.derivedCache;
        this.overlayRuntime = Objects.requireNonNull(runtime, "runtime");
        this.overlayConnection = Objects.requireNonNull(connection, "connection");
        this.registrations = base.registrations;
    }

    /**
     * DRIVER-SUPPLIED execution context (PHASE_K_EXECUTION.md §4): the
     * runtime and its connection arrive as API records — a harness's
     * per-test dispatch context, a service's ambient runtime — never as
     * model text. The returned VIEW resolves exactly those two fqns;
     * everything else (elements, typed caches) is this context, shared,
     * so overlays are allocation-cheap per call. ADD, never SHADOW: an
     * overlay must not redefine an element the MODEL declares.
     */
    public PureModelContext withExecutionOverlay(
            com.legend.model.RuntimeDefinition runtime,
            com.legend.model.ConnectionDefinition connection) {
        if (model.findRuntime(runtime.qualifiedName()).isPresent()
                || model.findConnection(connection.qualifiedName()).isPresent()) {
            throw new IllegalArgumentException("execution overlay would"
                    + " shadow a model-declared element: '"
                    + runtime.qualifiedName() + "' / '"
                    + connection.qualifiedName() + "'");
        }
        // EAGER ref check — stronger than model text (ModelIntegrity does
        // not verify runtime refs): a typo'd mapping fails HERE by name,
        // never mid-query
        for (String m : runtime.mappings()) {
            if (findMapping(m).isEmpty() && findLegacyMapping(m).isEmpty()) {
                throw new IllegalArgumentException("execution overlay"
                        + " runtime '" + runtime.qualifiedName()
                        + "' names unknown mapping '" + m + "'");
            }
        }
        return new PureModelContext(this, runtime, connection);
    }

    @Override
    public com.legend.platform.DeclarationTable declarations() {
        com.legend.platform.DeclarationTable d = declarations;
        if (d == null) {
            d = com.legend.platform.DeclarationTable.of(java.util.stream.Stream.concat(
                    com.legend.builtin.Pure.all().stream(), model.functions()).toList());
            declarations = d;
        }
        return d;
    }

    @Override
    public com.legend.platform.ImplementationTable implementations() {
        com.legend.platform.ImplementationTable t = implementations;
        if (t == null) {
            t = com.legend.platform.ImplementationTable.build(declarations(), registrations);
            implementations = t;
        }
        return t;
    }

    @Override
    public java.util.Optional<com.legend.model.ConnectionDefinition> findConnection(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        if (overlayConnection != null
                && overlayConnection.qualifiedName().equals(fqn)) {
            return java.util.Optional.of(overlayConnection);
        }
        return model.findConnection(fqn);
    }

    @Override
    public boolean isModelConnection(String fqn) {
        return model.isModelConnection(fqn);
    }

    @Override
    public java.util.Optional<com.legend.model.AssociationDefinition> findAssociationOf(
            String ownerClassFqn, String propName) {
        return model.findAssociationOf(ownerClassFqn, propName);
    }

    @Override
    public java.util.Optional<com.legend.model.AssociationDefinition.AssociationEndDefinition>
            findAssociationEnd(String ownerClassFqn, String propName) {
        return model.findAssociationEnd(ownerClassFqn, propName);
    }

    @Override
    public boolean isExecutionContextElement(String fqn) {
        Objects.requireNonNull(fqn, "fqn");
        return model.findMapping(fqn).isPresent()
                || model.findLegacyMapping(fqn).isPresent()
                || findRuntime(fqn).isPresent()
                || findConnection(fqn).isPresent()
                || model.findDatabase(fqn).isPresent();
    }

    @Override
    public java.util.List<com.legend.model.FunctionDefinition>
            findFunctionDefinitions(String fqn) {
        java.util.List<com.legend.model.FunctionDefinition> out = new java.util.ArrayList<>(1);
        for (com.legend.model.Function f : model.findFunction(fqn)) {
            if (f instanceof com.legend.model.FunctionDefinition fd) {
                out.add(fd);
            }
        }
        return out;
    }

    @Override
    public Optional<com.legend.model.FunctionDefinition>
            findFunctionDefinition(String fqn) {
        for (com.legend.model.Function f : model.findFunction(fqn)) {
            if (f instanceof com.legend.model.FunctionDefinition fd) {
                return Optional.of(fd);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<com.legend.model.DatabaseDefinition.TableDefinition>
            findOwnTableDefinition(String dbFqn, String name) {
        return model.findOwnTableDefinition(dbFqn, name);
    }

    @Override
    public Optional<com.legend.model.DatabaseDefinition.ViewDefinition> findView(String dbFqn, String name) {
        return model.findView(dbFqn, name);
    }

    @Override
    public String viewMainTable(String dbFqn, com.legend.model.DatabaseDefinition.ViewDefinition view) {
        return model.viewMainTable(dbFqn, view);
    }

    @Override
    public Optional<com.legend.model.DatabaseDefinition.ViewDefinition> findView(String dbFqn,
            @com.legend.base.Nullable String schema, String name) {
        return model.findView(dbFqn, schema, name);
    }

    @Override
    public Optional<TypedFunction> findViewFunction(String dbFqn, String name) {
        return model.viewLift(dbFqn, name)
                .flatMap(lift -> findFunction(lift.fqn()).stream().findFirst());
    }

    @Override
    public Optional<Type.RelationType> findTable(String dbFqn, String name) {
        Objects.requireNonNull(dbFqn, "dbFqn");
        Objects.requireNonNull(name, "name");
        return model.findTableDefinition(dbFqn, name).map(StoreCompiler::tableSchema);
    }

    @Override
    public Optional<com.legend.model.DatabaseDefinition.TableDefinition.Milestoning>
            findTableMilestoning(String dbFqn, String name) {
        return model.findDatabase(dbFqn)
                .flatMap(db -> milestoningWithIncludes(db, name, new java.util.HashSet<>()));
    }

    /** The graph's [1]-over-nullable census: the union of every compiled
     * mapping's stamped rows, bucket by bucket (memoized: the mappings are
     * immutable once the gate has passed). */


    private Optional<com.legend.model.DatabaseDefinition.TableDefinition.Milestoning>
            milestoningWithIncludes(com.legend.model.DatabaseDefinition db,
                    String name, java.util.Set<String> seen) {
        var own = model.findOwnTableDefinition(db.qualifiedName(), name)
                .map(com.legend.model.DatabaseDefinition.TableDefinition::milestoning);
        if (own.isPresent() && own.get() != null) {
            return Optional.of(own.get());
        }
        for (String include : db.includes()) {
            if (!seen.add(include)) {
                continue;
            }
            var inc = model.findDatabase(include)
                    .flatMap(d -> milestoningWithIncludes(d, name, seen));
            if (inc.isPresent()) {
                return inc;
            }
        }
        return Optional.empty();
    }

    /** The classifiers the metamodel store TRACKS (METAMODEL_STORE_HANDOFF.md
     * §3): a constant of the registry, so "is this classifier tracked?" is
     * a membership test — the resolver asks it on every element reference
     * (leg 6e: the corpus lane's profile — over half its samples were the
     * extent below being built and thrown away for that yes/no). */
    private static final java.util.Set<String> TRACKED_CLASSIFIERS = java.util.Set.of(
            com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS,
            com.legend.compiler.element.type.PlatformTypes.ENUMERATION,
            "meta::pure::metamodel::relationship::Association",
            com.legend.compiler.element.type.PlatformTypes.MAPPING,
            com.legend.compiler.element.type.PlatformTypes.DATABASE);

    @Override
    public boolean tracksClassifier(String classifierFqn) {
        return TRACKED_CLASSIFIERS.contains(classifierFqn);
    }

    @Override
    public java.util.@com.legend.base.Nullable List<String> classifierInstances(
            String classifierFqn) {
        // the registry table (METAMODEL_STORE_HANDOFF.md §3): tracked
        // classifiers answer their extent (the seeds read it), everything
        // else is null (a user class — the store lane owns it); the
        // tracked set above IS this dispatch's domain — keep them equal
        java.util.stream.Stream<String> fqns;
        if (com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS
                .equals(classifierFqn)) {
            fqns = java.util.stream.Stream.concat(
                    model.classes().map(c -> c.qualifiedName()),
                    com.legend.builtin.Pure.allNativeClasses().stream()
                            .map(c -> c.qualifiedName()));
        } else if (com.legend.compiler.element.type.PlatformTypes.ENUMERATION
                .equals(classifierFqn)) {
            fqns = java.util.stream.Stream.concat(
                    model.enums().map(e -> e.qualifiedName()),
                    com.legend.builtin.Pure.allNativeEnums().stream()
                            .map(e -> e.qualifiedName()));
        } else if ("meta::pure::metamodel::relationship::Association"
                .equals(classifierFqn)) {
            fqns = model.associations().map(a -> a.qualifiedName());
        } else if (com.legend.compiler.element.type.PlatformTypes.MAPPING
                .equals(classifierFqn)) {
            fqns = model.mappings().map(m -> m.qualifiedName());
        } else if (com.legend.compiler.element.type.PlatformTypes.DATABASE
                .equals(classifierFqn)) {
            // the store extent (metamodel-store tables seed from it)
            fqns = model.databases().map(d -> d.qualifiedName());
        } else {
            return null;
        }
        return fqns.distinct().sorted().toList();
    }

    @Override
    public java.util.Set<String> functionFqns() {
        java.util.Set<String> out = new java.util.HashSet<>();
        model.functions().forEach(f -> out.add(f.qualifiedName()));
        return out;
    }

    @Override
    public boolean isDatabase(String fqn) {
        return model.findDatabase(fqn).isPresent();
    }

    @Override
    public Optional<com.legend.model.DatabaseDefinition>
            findDatabase(@com.legend.base.Nullable String dbFqn) {
        return model.findDatabase(dbFqn);
    }

    @Override
    public Optional<com.legend.model.DatabaseDefinition.JoinDefinition>
            findJoinDefinition(@com.legend.base.Nullable String dbFqn, String joinName) {
        return model.findJoin(dbFqn, joinName);
    }

    @Override
    public Optional<com.legend.model.DatabaseDefinition.TableDefinition>
            findTableDefinition(String dbFqn, String name) {
        return model.findTableDefinition(dbFqn, name);
    }

}
