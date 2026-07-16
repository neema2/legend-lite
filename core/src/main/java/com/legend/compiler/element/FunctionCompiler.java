package com.legend.compiler.element;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.model.TypeExpression;
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
        List<Function> all = new ArrayList<>(Pure.nativeFunctionsAt(fqn));
        // platform-owned FQNs: the native IS the definition; the corpus's
        // own M3-reflective bodies (toDDL.pure) never join the overload set.
        // The suppression is NOT silent — stderr once per FQN (audit 17;
        // a structured wall channel does not reach this layer yet).
        if (!com.legend.compiler.element.type.PlatformTypes
                .isPlatformOwnedFunction(fqn)) {
            all.addAll(model.findFunction(fqn));
        } else if (!model.findFunction(fqn).isEmpty()
                && SUPPRESSED_ONCE.add(fqn)) {
            System.err.println("[legend-lite] platform-owned function '" + fqn
                    + "': " + model.findFunction(fqn).size()
                    + " user definition(s) suppressed (native is the definition)");
        }
        return all;
    }

    private static final java.util.Set<String> SUPPRESSED_ONCE =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Pure existence check — symbol-table lookup only, no compilation. */
    boolean exists(String fqn) {
        return !Pure.nativeFunctionsAt(fqn).isEmpty() || !model.findFunction(fqn).isEmpty();
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
                // POISON-NOT-DROP at the overload level: a tolerant module
                // build keeps signature-broken functions in the model; one
                // such overload must not break candidate collection for
                // its healthy siblings (natives registered at the same
                // FQN included). A STRICT build never gets here — model
                // integrity fails first.
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
        Optional<List<com.legend.model.spec.ValueSpecification>> body = f instanceof FunctionDefinition fd
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
                && com.legend.model.Multiplicity.Concrete.PURE_ONE.equals(f.returnMultiplicity());
    }

    /** Returns some class type with multiplicity {@code [*]} (kind check — which
     *  class is right needs subtyping, deferred to G). */
    boolean returnsClassMany(Function f) {
        return f.returnType() instanceof TypeExpression.NameRef nr && classifier.isClassFqn(nr.name())
                && com.legend.model.Multiplicity.Concrete.ZERO_MANY.equals(f.returnMultiplicity());
    }

    private static boolean named(TypeExpression t, String fqn) {
        return t instanceof TypeExpression.NameRef nr && nr.name().equals(fqn);
    }
}
