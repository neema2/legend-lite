package com.legend.compiler.element;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.protocol.TypeExpression;
import com.legend.model.Function;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Phase F's <strong>function-element</strong> compiler: everything about
 * function symbols at element-compile time &mdash; the SINGLE native+user
 * overload merge point ({@link #functionsAt}), signature compilation
 * ({@link #compile}), and the F.a/F.c reference-safety primitives
 * ({@link #requireFunction} existence, {@link #requireShape} structural shape)
 * that the class and mapping compilers consume. Bodies are Phase G.
 */
final class FunctionCompiler {

    private final ModelBuilder model;
    private final TypeClassifier classifier;

    FunctionCompiler(ModelBuilder model, TypeClassifier classifier) {
        this.model = model;
        this.classifier = classifier;
    }

    /** THE native+user overload merge — every "functions at this FQN" question routes here. */
    List<Function> functionsAt(String fqn) {
        if (!fqn.contains("::")) {
            // A BARE name the resolver could not qualify: THE RULE (BareNames,
            // untangle 4b.2) names the declarations it may denote — engine
            // surface, core group, the form's own — and each is looked up
            // exactly like a qualified call, natives and model alike, under
            // the same platform-owned gate. No bare index, no courtesy list.
            List<Function> all = new ArrayList<>();
            for (String candidate : com.legend.compiler.BareNames.fqns(fqn)) {
                for (Function f : functionsAt(candidate)) {
                    if (!all.contains(f)) {
                        all.add(f);
                    }
                }
            }
            if (com.legend.builtin.DecisionProbe.INSTALLED != null) {
                com.legend.builtin.DecisionProbe.overloads(fqn, all, model, model.functions());
            }
            return all;
        }
        List<Function> all = new ArrayList<>(Pure.nativeFunctionsAt(fqn));
        // (the on-demand lift of NATIVE-CATALOG classes' derived properties
        // is GONE — batch 167, HAND_SHAPE_DIVERGENCE §4 step 5: the catalog
        // holds the primitives alone, every class with a body is a module
        // or graph class and lifts in ModelNormalizer E.2)
        // platform-owned FQNs: the native IS the definition; the corpus's
        // own M3-reflective bodies (toDDL.pure) never join the overload set.
        // The suppression is NOT silent — stderr once per FQN (audit 17;
        // a structured wall channel does not reach this layer yet).
        // a platform-IMPLEMENTED derived property (the row accessors — upstream
        // declares them as qualified properties, batch 5 leg 4) keeps its lifted
        // definition as the TYPING source: the call is never inlined
        // (UserCallInliner) and RowGetters lowers it by property name
        if (!com.legend.compiler.element.type.PlatformTypes
                .isPlatformOwnedFunction(fqn)) {
            addModelOverloads(all, model, fqn);
        } else if (!model.findFunction(fqn).isEmpty()
                && SUPPRESSED_ONCE.add(fqn)) {
            System.err.println("[legend-lite] platform-owned function '" + fqn
                    + "': " + model.findFunction(fqn).size()
                    + " user definition(s) suppressed (native is the definition)");
        }
        if (com.legend.builtin.DecisionProbe.INSTALLED != null) {
            com.legend.builtin.DecisionProbe.overloads(fqn, all, model, model.functions());
        }
        return all;
    }

    /** Model overloads join the set EXCEPT {@code <<PCT.function>>}
     * redefinitions of natives the registry owns: that stereotype is the
     * engine's own marker for "the platform function under conformance",
     * and for those the NATIVE is the definition (tenet #2 — the
     * reference pure body is the SPEC, never our implementation; witness:
     * core_functions_standard redefines or/and/max... whose inlined fold
     * bodies produced wrong SQL, chB-std testOr). A PCT.function with NO
     * registered native keeps its body — the model IS the implementation
     * (timeBucket, covarSample). Same stderr-once channel as the
     * platform-owned rule. */
    private static void addModelOverloads(
            List<Function> all, ModelBuilder model, String fqn) {
        for (Function def : model.findFunction(fqn)) {
            boolean pctFunction = def
                    instanceof com.legend.model.FunctionDefinition fd
                    && fd.stereotypes() != null
                    && fd.stereotypes().stream().anyMatch(st ->
                            "function".equals(st.stereotypeName())
                            && com.legend.compiler.element.type.PlatformTypes.isProfile(
                                    st.profileName(),
                                    com.legend.compiler.element.type.PlatformTypes.PCT_PROFILE));
            if (pctFunction && !com.legend.builtin.Pure.nativeFunctionsAt(fqn).isEmpty()) {
                if (SUPPRESSED_ONCE.add(fqn)) {
                    System.err.println("[legend-lite] PCT.function '" + fqn
                            + "' suppressed (native is the definition)");
                }
                continue;
            }
            all.add(def);
        }
    }

    private static final java.util.Set<String> SUPPRESSED_ONCE =
            java.util.Collections.newSetFromMap(
                    new java.util.concurrent.ConcurrentHashMap<>());

    /** Pure existence check — symbol-table lookup only, no compilation. */
    boolean exists(String fqn) {
        return !functionsAt(fqn).isEmpty();
    }

    /** Compile every overload at {@code fqn} to its typed signature. */
    List<TypedFunction> compileAll(String fqn) {
        List<Function> defs = functionsAt(fqn);
        List<TypedFunction> typed = new ArrayList<>(defs.size());
        RuntimeException first = null;
        for (Function f : defs) {
            try {
                typed.add(compile(f));
            } catch (RuntimeException e) {
                // DROP-AT-OVERLOAD (honest name — audit 17): a tolerant
                // module build keeps signature-broken functions in the
                // MODEL, but candidate collection omits them, so a call
                // whose engine-correct target is the broken overload can
                // silently re-dispatch to a healthy sibling. The fix is a
                // poison SENTINEL that participates in scoring and throws
                // when it wins (tracked, task #56). A STRICT build never
                // gets here — model integrity fails first.
                if (first == null) {
                    first = e;
                }
            }
        }
        if (typed.isEmpty() && first != null) {
            throw first;   // ALL overloads broken: surface the real reason
        }
        return List.copyOf(typed);
    }

    /** One parser function definition &rarr; its {@link TypedFunction} signature record. */
    TypedFunction compile(Function f) {
        List<String> typeParams = f.typeParameters();
        List<TypedParameter> params = new ArrayList<>(f.parameters().size());
        for (FunctionDefinition.ParameterDefinition p : f.parameters()) {
            params.add(new TypedParameter(
                    p.name(),
                    classifier.classify(p.type(), typeParams),
                    TypeClassifier.multiplicity(p.multiplicity())));
        }
        Optional<List<com.legend.protocol.spec.ValueSpecification>> body = f instanceof FunctionDefinition fd
                ? Optional.of(fd.body())
                : Optional.empty();
        return new TypedFunction(
                f.qualifiedName(),
                typeParams,
                f.multiplicityParameters(),
                params,
                classifier.classify(f.returnType(), typeParams),
                TypeClassifier.multiplicity(f.returnMultiplicity()),
                body,
                f instanceof NativeFunctionDefinition,
                f);
    }

    /**
     * F.a binding integrity: the FQN a structural element binds to must resolve
     * to a real function. Reference safety (eager + total); the body is Phase G.
     * The error speaks in the user-facing site, never the {@code $}-FQN.
     */
    void requireFunction(String fqn, String site) {
        if (!exists(fqn)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, 
                    site + " binds to unknown function '" + fqn + "'");
        }
    }

    /**
     * F.c structural shape check: at least one overload at {@code fqn} satisfies
     * {@code shape}. Existence is assumed already checked (F.a/F.b). Full
     * type-assignability (subtyping) and bodies are Phase G.
     */
    void requireShape(String fqn, Predicate<Function> shape, String site, String expected) {
        if (functionsAt(fqn).stream().noneMatch(shape)) {
            throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, 
                    site + " must be realized by a function " + expected);
        }
    }

    // ----- F.c shape predicates (structural only — no subtyping, that's G) -----

    /** Returns exactly {@code Boolean[1]} (exact — Boolean is a primitive, no subtypes). */
    static boolean returnsBooleanOne(Function f) {
        return named(f.returnType(), Pure.BOOLEAN.qualifiedName())
                && com.legend.protocol.Multiplicity.Concrete.PURE_ONE.equals(f.returnMultiplicity());
    }

    /** Returns some class type with multiplicity {@code [*]} (kind check — which
     *  class is right needs subtyping, deferred to G). */
    boolean returnsClassMany(Function f) {
        return f.returnType() instanceof TypeExpression.NameRef nr && classifier.isClassFqn(nr.name())
                && com.legend.protocol.Multiplicity.Concrete.ZERO_MANY.equals(f.returnMultiplicity());
    }

    private static boolean named(TypeExpression t, String fqn) {
        return t instanceof TypeExpression.NameRef nr && nr.name().equals(fqn);
    }
}
