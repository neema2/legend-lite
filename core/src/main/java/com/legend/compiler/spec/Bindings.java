package com.legend.compiler.spec;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Mutable accumulator of the type-variable and multiplicity-variable bindings
 * solved during a single overload's unification (engine {@code AbstractChecker}
 * {@code Bindings}). A native call instantiates its signature's variables fresh
 * into one of these, unifies each argument against the corresponding parameter
 * to fill it, then {@code resolve}s the return type against it
 * (PHASE_G_SPEC_COMPILER.md §3.2&ndash;3.3).
 *
 * <p>A user (monomorphic) call uses an empty {@code Bindings} &mdash; its
 * signature has no free variables, so nothing is ever bound; resolution is the
 * identity. This is the degenerate case of the same machinery (§2).
 */
public final class Bindings {

    private final Map<String, Type> types = new LinkedHashMap<>();
    private final Map<String, Multiplicity> mults = new LinkedHashMap<>();

    public boolean hasType(String var) {
        return types.containsKey(var);
    }

    public boolean hasMult(String var) {
        return mults.containsKey(var);
    }

    public Optional<Type> type(String var) {
        return Optional.ofNullable(types.get(var));
    }

    public Optional<Multiplicity> mult(String var) {
        return Optional.ofNullable(mults.get(var));
    }

    public void bindType(String var, Type t) {
        types.put(var, t);
    }

    public void bindMult(String var, Multiplicity m) {
        mults.put(var, m);
    }

    /**
     * An independent copy &mdash; for signature variables that solve <em>per
     * element</em> (an {@code AggColSpecArray}'s {@code K}/{@code V} are shared only
     * syntactically: each aggregate column's map type is its own). The copy sees
     * everything solved so far; its new bindings do not leak back.
     */
    public Bindings copy() {
        Bindings c = new Bindings();
        c.types.putAll(types);
        c.mults.putAll(mults);
        return c;
    }
}
