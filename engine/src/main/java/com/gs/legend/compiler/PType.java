package com.gs.legend.compiler;

import java.util.List;

/**
 * Pure type model — the structured representation of types parsed from
 * native function signature strings.
 *
 * <p>
 * Designed to model the full range of Pure types:
 * <ul>
 *   <li>{@code String}, {@code Integer} → {@link Concrete}</li>
 *   <li>{@code T}, {@code V} → {@link TypeVar}</li>
 *   <li>{@code Relation<T>}, {@code Map<K,V>} → {@link Parameterized}</li>
 *   <li>{@code ColSpec<Z⊆T>} → {@link Parameterized} with {@link Constraint}</li>
 *   <li>{@code {T[1]->Boolean[1]}} → {@link FunctionType}</li>
 *   <li>{@code (name:String[1], age:Integer[1])} → {@link RelationTypeVar}</li>
 * </ul>
 *
 * <p>
 * All types are immutable records. The sealed hierarchy ensures exhaustive
 * pattern matching in switch expressions.
 */
public sealed interface PType {

    /**
     * A concrete named type like {@code String}, {@code Integer}, {@code Boolean},
     * {@code Date}, {@code Number}, {@code Any}, {@code JoinKind}, {@code DurationUnit}.
     */
    record Concrete(String name) implements PType {
        @Override public String toString() { return name; }
    }

    /**
     * A type variable like {@code T}, {@code V}, {@code K}.
     * Bound during overload resolution.
     */
    record TypeVar(String name) implements PType {
        @Override public String toString() { return name; }
    }

    /**
     * A parameterized type like {@code Relation<T>}, {@code ColSpec<Z⊆T>},
     * {@code AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>}.
     *
     * @param rawType    The outer type name (e.g., "Relation", "ColSpec")
     * @param typeArgs   Positional type arguments
     * @param constraints Schema constraints (e.g., Z⊆T in ColSpec)
     */
    record Parameterized(
            String rawType,
            List<PType> typeArgs,
            List<Constraint> constraints
    ) implements PType {
        public Parameterized(String rawType, List<PType> typeArgs) {
            this(rawType, typeArgs, List.of());
        }

        @Override
        public String toString() {
            var sb = new StringBuilder(rawType).append('<');
            for (int i = 0; i < typeArgs.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(typeArgs.get(i));
            }
            if (!constraints.isEmpty()) {
                for (var c : constraints) sb.append(',').append(c);
            }
            return sb.append('>').toString();
        }
    }

    /**
     * A function type like {@code {T[1]->Boolean[1]}} or
     * {@code {Relation<T>[1],_Window<T>[1],T[1]->Any[0..1]}}.
     *
     * @param paramTypes  Parameter types (with multiplicities)
     * @param returnType  The return type
     * @param returnMult  The return multiplicity
     */
    record FunctionType(
            List<Param> paramTypes,
            PType returnType,
            Mult returnMult
    ) implements PType {
        @Override
        public String toString() {
            var sb = new StringBuilder("{");
            for (int i = 0; i < paramTypes.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(paramTypes.get(i));
            }
            sb.append("->").append(returnType).append('[').append(returnMult).append(']');
            return sb.append('}').toString();
        }
    }

    /**
     * An inline relation type variable like {@code (name:String[1], age:Integer[1])}.
     * Used in extend window lambdas where the row type is spelled out.
     */
    record RelationTypeVar(List<Column> columns) implements PType {
        public record Column(String name, PType type, Mult mult) {}
    }

    // ===== Supporting types =====

    /**
     * A function parameter with name, type, and multiplicity.
     */
    record Param(String name, PType type, Mult mult) {
        @Override
        public String toString() {
            return name + ":" + type + "[" + mult + "]";
        }
    }

    /**
     * A schema constraint like {@code Z⊆T} (subset) or {@code Z=(?:K)} (type-match).
     */
    sealed interface Constraint {
        /** Z⊆T — Z is a subset of T's schema. */
        record Subset(String var, String ofVar) implements Constraint {
            @Override public String toString() { return var + "⊆" + ofVar; }
        }

        /** Z=(?:K) — Z has the same type as K (type-match in rename). */
        record TypeMatch(String var, String matchVar) implements Constraint {
            @Override public String toString() { return var + "=(?:" + matchVar + ")"; }
        }
    }
}
