package com.gs.legend.compiler;

import com.gs.legend.model.m3.Multiplicity;

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
 *   <li>{@code T-Z+V}, {@code Z⊆T} → {@link SchemaAlgebra}</li>
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

        /**
         * Bridges parse-time type to compile-time type.
         * Maps this concrete Pure type name to the corresponding {@link com.gs.legend.model.m3.Type}.
         * Returns null if the name doesn't map to a known Type (e.g., "JoinKind", "DurationUnit").
         */
        public com.gs.legend.model.m3.Type toGenericType() {
            if ("Decimal".equals(name)) return com.gs.legend.model.m3.Type.DEFAULT_DECIMAL;
            // Null signals a signature-layer marker like JoinKind / DurationUnit.
            // Primitive.lookup() stays narrow until PType is deleted in chunk 2.5d.
            return com.gs.legend.model.m3.Type.Primitive.lookup(name).orElse(null);
        }
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
     * @param typeArgs   Positional type arguments (may include SchemaAlgebra nodes)
     */
    record Parameterized(
            String rawType,
            List<PType> typeArgs
    ) implements PType {
        @Override
        public String toString() {
            var sb = new StringBuilder(rawType).append('<');
            for (int i = 0; i < typeArgs.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(typeArgs.get(i));
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
            Multiplicity returnMult
    ) implements PType {
        @Override
        public String toString() {
            var sb = new StringBuilder("{");
            for (int i = 0; i < paramTypes.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(paramTypes.get(i));
            }
            sb.append("->").append(returnType).append(returnMult);
            return sb.append('}').toString();
        }
    }

    /**
     * An inline relation type variable like {@code (name:String[1], age:Integer[1])}.
     * Used in extend window lambdas where the row type is spelled out.
     */
    record RelationTypeVar(List<Column> columns) implements PType {
        public record Column(String name, PType type, Multiplicity mult) {}
    }

    /**
     * Schema type algebra — binary tree matching legend-pure's GenericTypeOperation.
     *
     * <p>Operations:
     * <ul>
     *   <li>{@code Union}      — T+Z (add columns)</li>
     *   <li>{@code Difference}  — T-Z (remove columns)</li>
     *   <li>{@code Subset}     — Z⊆T (Z is column subset of T)</li>
     *   <li>{@code Equal}      — Z=K (type match)</li>
     * </ul>
     *
     * <p>Example: {@code T-Z+V} →
     * {@code SchemaAlgebra(SchemaAlgebra(TypeVar("T"), TypeVar("Z"), Difference), TypeVar("V"), Union)}
     */
    record SchemaAlgebra(PType left, PType right, OpType op) implements PType {
        public enum OpType { Union, Difference, Subset, Equal }

        @Override
        public String toString() {
            return switch (op) {
                case Union -> left + "+" + right;
                case Difference -> left + "-" + right;
                case Subset -> left + "⊆" + right;
                case Equal -> left + "=" + right;
            };
        }
    }

    // ===== Supporting types =====

    /**
     * A function parameter with name, type, and multiplicity.
     */
    record Param(String name, PType type, Multiplicity mult) {
        @Override
        public String toString() {
            return name + ":" + type + mult;
        }
    }
}
