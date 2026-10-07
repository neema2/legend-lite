// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * HOW WELL A CANDIDATE FITS A CALL, ordered as legend-pure orders it: a port of m3's
 * {@code FunctionMatch}, {@code GenericTypeMatch}, {@code TypeMatch} and {@code MultiplicityMatch}
 * (legend-pure-m3-core, {@code compiler/postprocessing/functionmatch} and {@code navigation/…/match}),
 * with the type distances read off m3's {@code C3Linearization}. Lower is better.
 *
 * <p>A candidate's match is its parameters' type matches LEFT TO RIGHT, then their multiplicity
 * matches left to right; the first difference decides ({@code FunctionMatch.compareTo}). Nothing is
 * added up. Whether a candidate accepts the arguments at all is the kernel's acceptance test,
 * unchanged; this class only orders the candidates that accept.
 *
 * <p>Known departures from m3 (each documented in {@code InferenceKernel} where it is made):
 * <ul>
 *   <li>{@code Any} ranks with the type parameters in the primary ranking (legend-pure matches the
 *       calls inside a lambda before their arguments are typed); m3's literal order, {@code Any} a
 *       concrete class, settles a tie that leaves, in {@code resolveOverload} only — the lenient
 *       pass ({@code rankNonLambda}) leaves a tie to declaration order.</li>
 *   <li>A fit only this platform's acceptance admits ranks at one platform-rule distance, after
 *       every class generalization and before a type parameter: a relation into
 *       {@code TabularDataSet}; a bare function type against a carrier formal, or a carrier value
 *       against a bare function-type formal (m3 matches a function type only to a function type
 *       and to {@code Any}); a carrier the value's class does not extend; a value's function
 *       parameter narrower than the formal's.</li>
 *   <li>A relation-type formal ranks as a relation match without comparing its columns (m3's
 *       {@code RelationTypeMatch} compares their types and multiplicities); a type-operation formal
 *       ({@code T+V}) ranks as an untyped match (m3: non-concrete).</li>
 *   <li>Type arguments are compared position by position when the counts agree; m3 first maps the
 *       value's arguments to the formal's class through the hierarchy.</li>
 * </ul>
 */
final class FunctionMatch implements Comparable<FunctionMatch> {

    private final List<TypeFit> types;
    private final List<MultFit> multiplicities;

    FunctionMatch(List<TypeFit> types, List<MultFit> multiplicities) {
        this.types = List.copyOf(types);
        this.multiplicities = List.copyOf(multiplicities);
    }

    @Override
    public int compareTo(FunctionMatch other) {
        int size = types.size();
        if (other.types.size() != size) {
            return Integer.compare(size, other.types.size());
        }
        for (int i = 0; i < size; i++) {
            int c = types.get(i).compareTo(other.types.get(i));
            if (c != 0) {
                return c;
            }
        }
        for (int i = 0; i < size; i++) {
            int c = multiplicities.get(i).compareTo(other.multiplicities.get(i));
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof FunctionMatch m && types.equals(m.types) && multiplicities.equals(m.multiplicities);
    }

    @Override
    public int hashCode() {
        return types.hashCode() * 43 + multiplicities.hashCode();
    }

    @Override
    public String toString() {
        return "FunctionMatch" + types + multiplicities;
    }

    /** The kinds of raw type match, in legend-pure's order (m3 {@code TypeMatch}): a concrete
     *  match by hierarchy distance beats a type parameter, which beats a relation-type or
     *  function-type match, which beat an empty ({@code Nil}) value, which beats an untyped one. */
    enum Kind { SIMPLE, NON_CONCRETE, RELATION, FUNCTION, BOTTOM, NULL }

    /**
     * One parameter's type match (m3 {@code GenericTypeMatch} over {@code TypeMatch}): the raw type
     * match (its kind; for {@link Kind#SIMPLE} the distance, the formal's position in the argument
     * type's C3 linearization; for a relation or function type its inner matches), then the type
     * arguments' matches, then the multiplicity arguments'.
     */
    record TypeFit(Kind kind, int distance, List<TypeFit> inner, List<MultFit> innerMultiplicities,
            List<TypeFit> arguments, List<MultFit> multiplicityArguments) implements Comparable<TypeFit> {

        static final TypeFit EXACT = simple(0);
        static final TypeFit NON_CONCRETE = of(Kind.NON_CONCRETE);
        static final TypeFit BOTTOM = of(Kind.BOTTOM);
        static final TypeFit NULL = of(Kind.NULL);

        TypeFit {
            inner = List.copyOf(inner);
            innerMultiplicities = List.copyOf(innerMultiplicities);
            arguments = List.copyOf(arguments);
            multiplicityArguments = List.copyOf(multiplicityArguments);
        }

        static TypeFit simple(int distance) {
            return new TypeFit(Kind.SIMPLE, distance, List.of(), List.of(), List.of(), List.of());
        }

        static TypeFit of(Kind kind) {
            return new TypeFit(kind, 0, List.of(), List.of(), List.of(), List.of());
        }

        /** A raw match carrying its type arguments' and multiplicity arguments' matches. */
        TypeFit withArguments(List<TypeFit> args, List<MultFit> multArgs) {
            return new TypeFit(kind, distance, inner, innerMultiplicities, args, multArgs);
        }

        @Override
        public int compareTo(TypeFit other) {
            int c = Integer.compare(kind.ordinal(), other.kind.ordinal());
            if (c != 0) {
                return c;
            }
            if (kind == Kind.SIMPLE) {
                c = Integer.compare(distance, other.distance);
            } else if (kind == Kind.RELATION || kind == Kind.FUNCTION) {
                c = compareLists(inner, other.inner);
                if (c == 0) {
                    c = compareLists(innerMultiplicities, other.innerMultiplicities);
                }
            }
            if (c != 0) {
                return c;
            }
            c = compareLists(arguments, other.arguments);
            return c != 0 ? c : compareLists(multiplicityArguments, other.multiplicityArguments);
        }
    }

    /**
     * One parameter's multiplicity match (m3 {@code MultiplicityMatch}): exact, then a multiplicity
     * parameter ({@code m}), then the other concrete fits by how far apart the upper bounds are,
     * then the lower bounds ({@link Integer#MAX_VALUE} for an unbounded gap); an untyped value last.
     */
    record MultFit(int rank, int upperDistance, int lowerDistance) implements Comparable<MultFit> {

        static final MultFit EXACT = new MultFit(0, 0, 0);
        static final MultFit NON_CONCRETE = new MultFit(1, 0, 0);
        static final MultFit NULL = new MultFit(3, 0, 0);
        /** The widest concrete fit: what m3 gives a multiplicity-parameter value against {@code [*]}. */
        static final MultFit WIDEST = new MultFit(2, Integer.MAX_VALUE, Integer.MAX_VALUE);

        static MultFit concrete(int lowerDistance, int upperDistance) {
            return lowerDistance == 0 && upperDistance == 0 ? EXACT : new MultFit(2, upperDistance, lowerDistance);
        }

        @Override
        public int compareTo(MultFit other) {
            int c = Integer.compare(rank, other.rank);
            if (c != 0) {
                return c;
            }
            c = Integer.compare(upperDistance, other.upperDistance);
            return c != 0 ? c : Integer.compare(lowerDistance, other.lowerDistance);
        }
    }

    /** m3 {@code GenericTypeMatch.compareMatchLists}: element by element, then the shorter list first. */
    static <T extends Comparable<T>> int compareLists(List<T> these, List<T> those) {
        for (int i = 0, n = Math.min(these.size(), those.size()); i < n; i++) {
            int c = these.get(i).compareTo(those.get(i));
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(these.size(), those.size());
    }

    /**
     * The C3 linearization of a type's generalizations (m3 {@code C3Linearization}), the type itself
     * first and {@code Any} last; memoized. A class with no declared generalization generalizes
     * {@code Any}, as m3's post-processing makes it. A cyclic or inconsistent hierarchy (which m3
     * refuses to compile) falls back to breadth-first order; a class that fails to compile makes the
     * ranking throw (nothing here catches it).
     */
    static final class Linearizer {

        private final Function<String, List<String>> directGeneralizations;
        private final String top;
        private final Map<String, List<String>> memo = new HashMap<>();

        Linearizer(Function<String, List<String>> directGeneralizations, String top) {
            this.directGeneralizations = directGeneralizations;
            this.top = top;
        }

        /** The position of {@code general} in {@code specific}'s linearization; -1 when it is not there. */
        int distance(String specific, String general) {
            return linearization(specific).indexOf(general);
        }

        List<String> linearization(String fqn) {
            List<String> known = memo.get(fqn);
            if (known != null) {
                return known;
            }
            List<String> out = c3(fqn, new LinkedHashSet<>());
            if (out == null) {
                out = breadthFirst(fqn);
            }
            if (!out.contains(top)) {
                out = new ArrayList<>(out);
                out.add(top);
            }
            out = List.copyOf(out);
            memo.put(fqn, out);
            return out;
        }

        private List<String> generalizationsOf(String fqn) {
            List<String> direct = directGeneralizations.apply(fqn);
            if (direct.isEmpty() && !fqn.equals(top)) {
                return List.of(top);
            }
            return direct;
        }

        /** The C3 merge; null on a cycle or a merge conflict. */
        private @com.legend.base.Nullable List<String> c3(String fqn, LinkedHashSet<String> stack) {
            List<String> known = memo.get(fqn);
            if (known != null) {
                return known;
            }
            if (!stack.add(fqn)) {
                return null;
            }
            try {
                List<String> generals = generalizationsOf(fqn);
                if (generals.isEmpty()) {
                    return List.of(fqn);
                }
                List<List<String>> queues = new ArrayList<>();
                queues.add(new ArrayList<>(List.of(fqn)));
                for (String g : generals) {
                    List<String> lin = c3(g, stack);
                    if (lin == null) {
                        return null;
                    }
                    queues.add(new ArrayList<>(lin));
                }
                queues.add(new ArrayList<>(generals));
                List<String> result = new ArrayList<>();
                while (!queues.isEmpty()) {
                    String next = null;
                    for (List<String> q : queues) {
                        String candidate = q.get(0);
                        boolean inTail = false;
                        for (List<String> other : queues) {
                            if (other != q && other.indexOf(candidate) > 0) {
                                inTail = true;
                                break;
                            }
                        }
                        if (!inTail) {
                            next = candidate;
                            break;
                        }
                    }
                    if (next == null) {
                        return null;
                    }
                    result.add(next);
                    for (java.util.Iterator<List<String>> it = queues.iterator(); it.hasNext();) {
                        List<String> q = it.next();
                        if (q.get(0).equals(next)) {
                            q.remove(0);
                        }
                        if (q.isEmpty()) {
                            it.remove();
                        }
                    }
                }
                return result;
            } finally {
                stack.remove(fqn);
            }
        }

        private List<String> breadthFirst(String fqn) {
            List<String> out = new ArrayList<>(List.of(fqn));
            for (int i = 0; i < out.size(); i++) {
                for (String g : generalizationsOf(out.get(i))) {
                    if (!out.contains(g)) {
                        out.add(g);
                    }
                }
            }
            return out;
        }
    }
}
