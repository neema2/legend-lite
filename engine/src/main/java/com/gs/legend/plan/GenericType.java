package com.gs.legend.plan;

import com.gs.legend.model.m3.Multiplicity;

import java.util.List;
import java.util.Objects;

/**
 * Rich type system for the IR, replacing the flat PureType enum.
 * Supports type hierarchy, generics (List&lt;Integer&gt;), class types, and enum types.
 *
 * In Pure: Integer[*] = type Integer + multiplicity [*].
 * Multiplicity is carried on {@link Parameterized} for root return types;
 * other variants default to null (treated as [1] / scalar).
 */
public sealed interface GenericType
        permits GenericType.Primitive, GenericType.Parameterized,
                GenericType.ClassType, GenericType.EnumType,
                GenericType.PrecisionDecimal, GenericType.Relation,
                GenericType.Tuple {

    String typeName();

    /**
     * Primitive types with subtype hierarchy.
     *
     * Hierarchy:
     *   ANY
     *   ├── NUMBER
     *   │   ├── INTEGER
     *   │   │   ├── INT64
     *   │   │   └── INT128
     *   │   ├── FLOAT
     *   │   └── DECIMAL  (see also PrecisionDecimal for parameterized form)
     *   ├── STRING
     *   ├── BOOLEAN
     *   ├── DATE
     *   │   ├── STRICT_DATE
     *   │   └── DATE_TIME
     *   ├── STRICT_TIME
     *   └── JSON
     *   NIL (bottom type, subtype of everything)
     */
    enum Primitive implements GenericType {
        ANY,
        NIL,
        NUMBER, INTEGER, INT64, INT128, FLOAT, DECIMAL,
        STRING,
        BOOLEAN,
        DATE, STRICT_DATE, DATE_TIME, STRICT_TIME,
        JSON;

        @Override
        public String typeName() {
            return switch (this) {
                case ANY -> "Any";
                case NIL -> "Nil";
                case NUMBER -> "Number";
                case INTEGER, INT64, INT128 -> "Integer";
                case FLOAT -> "Float";
                case DECIMAL -> "Decimal";
                case STRING -> "String";
                case BOOLEAN -> "Boolean";
                case DATE -> "Date";
                case STRICT_DATE -> "StrictDate";
                case DATE_TIME -> "DateTime";
                case STRICT_TIME -> "StrictTime";
                case JSON -> "JSON";
            };
        }

        /**
         * @return true if this type is a subtype of (or equal to) the given type.
         */
        public boolean isSubtypeOf(Primitive other) {
            if (this == other) return true;
            if (this == NIL) return true;         // NIL is subtype of everything
            if (other == ANY) return true;         // everything is subtype of ANY
            return switch (this) {
                case INTEGER, FLOAT, DECIMAL -> other == NUMBER;
                case INT64, INT128 -> other == INTEGER || other == NUMBER;
                case STRICT_DATE, DATE_TIME -> other == DATE;
                default -> false;
            };
        }

        public boolean isNumeric() {
            return this == INTEGER || this == INT64 || this == INT128
                    || this == FLOAT || this == DECIMAL || this == NUMBER;
        }

        public boolean isTemporal() {
            return this == DATE || this == STRICT_DATE || this == DATE_TIME || this == STRICT_TIME;
        }

        /**
         * Maps a Pure type name string to the corresponding Primitive.
         */
        /** True if this is an integer type (INTEGER, INT64, or INT128). */
        public boolean isInteger() {
            return this == INTEGER || this == INT64 || this == INT128;
        }

        public static Primitive fromTypeName(String name) {
            // Handle qualified names: meta::pure::metamodel::variant::Variant -> Variant
            String simpleName = name.contains("::") ? name.substring(name.lastIndexOf("::") + 2) : name;
            return switch (simpleName) {
                case "Integer" -> INTEGER;
                case "Float" -> FLOAT;
                case "Decimal" -> DECIMAL;
                case "Number" -> NUMBER;
                case "String" -> STRING;
                case "Boolean" -> BOOLEAN;
                case "Date" -> DATE;
                case "StrictDate" -> STRICT_DATE;
                case "DateTime" -> DATE_TIME;
                case "StrictTime" -> STRICT_TIME;
                case "Variant" -> JSON;
                case "Any" -> ANY;
                default -> throw new IllegalArgumentException(
                        "Unknown primitive type: '" + simpleName + "'. Use GenericType.fromType() for class/enum types.");
            };
        }
    }

    /**
     * Parameterized types: List&lt;Integer&gt;, Pair&lt;String, Integer&gt;, etc.
     *
     * @param multiplicity Pure-level multiplicity (null = unset, treated as [1]).
     *                     Stamped by TypeChecker on root return types so the execution
     *                     layer knows whether the SQL produced N scalar rows (MANY)
     *                     or 1 row with an opaque array value (ONE/null).
     */
    record Parameterized(String rawType, List<GenericType> typeArgs, Multiplicity multiplicity) implements GenericType {
        public Parameterized {
            Objects.requireNonNull(rawType);
            Objects.requireNonNull(typeArgs);
            typeArgs = List.copyOf(typeArgs);
        }

        /** Convenience constructor without multiplicity (defaults to null). */
        public Parameterized(String rawType, List<GenericType> typeArgs) {
            this(rawType, typeArgs, null);
        }

        @Override
        public String typeName() {
            return rawType;
        }

        /**
         * @return The element type for single-parameter collections (e.g., List&lt;Integer&gt; → Integer).
         */
        public GenericType elementType() {
            if (typeArgs.isEmpty()) throw new IllegalStateException(
                    "Parameterized type '" + rawType + "' has no type arguments — cannot extract element type");
            return typeArgs.getFirst();
        }
    }

    /**
     * User-defined class types: Person, Firm, etc.
     */
    record ClassType(String qualifiedName) implements GenericType {
        public ClassType {
            Objects.requireNonNull(qualifiedName);
        }

        @Override
        public String typeName() {
            int lastColon = qualifiedName.lastIndexOf("::");
            return lastColon >= 0 ? qualifiedName.substring(lastColon + 2) : qualifiedName;
        }
    }

    /**
     * Enum types: MyEnum, etc.
     */
    record EnumType(String qualifiedName) implements GenericType {
        public EnumType {
            Objects.requireNonNull(qualifiedName);
        }

        @Override
        public String typeName() {
            int lastColon = qualifiedName.lastIndexOf("::");
            return lastColon >= 0 ? qualifiedName.substring(lastColon + 2) : qualifiedName;
        }
    }

    /**
     * Decimal type with explicit precision and scale: DECIMAL(38, 18).
     * Subtype of DECIMAL and NUMBER in the type hierarchy.
     * Used when the compiler knows the exact precision (e.g., integer-valued decimals).
     */
    record PrecisionDecimal(int precision, int scale) implements GenericType {
        @Override
        public String typeName() {
            return "Decimal(" + precision + "," + scale + ")";
        }
    }

    /**
     * Universal default for unparameterized Decimal types.
     * Pure's Decimal maps to Java's BigDecimal (arbitrary precision).
     * DECIMAL(38,18) is the closest SQL approximation: max total width (38 digits),
     * 18 digits after the decimal point. Uses INT128 in DuckDB.
     */
    PrecisionDecimal DEFAULT_DECIMAL = new PrecisionDecimal(38, 18);

    /**
     * Relational type: Relation<(col1:Type1, col2:Type2, ...)>.
     * Represents tabular query results with a column schema.
     */
    record Relation(Schema schema) implements GenericType {
        public Relation {
            java.util.Objects.requireNonNull(schema, "Relation schema must not be null");
        }

        @Override
        public String typeName() { return "Relation"; }

        /**
         * Tracks available columns and their types through a relation compilation
         * pipeline. Immutable — every mutation returns a new {@code Schema}.
         * Column order is preserved (insertion order via {@link java.util.LinkedHashMap}).
         *
         * @param columns Ordered map of column name to {@link GenericType}
         * @param dynamicPivotColumns Specs for data-dependent pivot columns; empty for non-pivot queries
         */
        public record Schema(java.util.Map<String, GenericType> columns,
                             java.util.List<DynamicPivotColumn> dynamicPivotColumns) {

            public record DynamicPivotColumn(String aliasSuffix, GenericType returnType) {
                public static final String SEPARATOR = "__|__";
            }

            public Schema {
                columns = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(columns));
                dynamicPivotColumns = dynamicPivotColumns != null
                        ? java.util.List.copyOf(dynamicPivotColumns) : java.util.List.of();
            }

            public static Schema withoutPivot(java.util.Map<String, GenericType> columns) {
                return new Schema(columns, java.util.List.of());
            }

            public Schema withColumn(String name, GenericType type) {
                var newCols = new java.util.LinkedHashMap<>(columns);
                newCols.put(name, type);
                return new Schema(newCols, dynamicPivotColumns);
            }

            public Schema withColumns(java.util.Map<String, GenericType> additionalColumns) {
                var newCols = new java.util.LinkedHashMap<>(columns);
                newCols.putAll(additionalColumns);
                return new Schema(newCols, dynamicPivotColumns);
            }

            public Schema onlyColumns(java.util.List<String> names) {
                var newCols = new java.util.LinkedHashMap<String, GenericType>();
                for (String name : names) {
                    GenericType type = columns.get(name);
                    if (type == null) {
                        throw new com.gs.legend.compiler.PureCompileException(
                                "Column '" + name + "' does not exist. Available columns: " + columns.keySet());
                    }
                    newCols.put(name, type);
                }
                return new Schema(newCols, dynamicPivotColumns);
            }

            public Schema withoutColumns(java.util.Set<String> names) {
                var newCols = new java.util.LinkedHashMap<>(columns);
                names.forEach(newCols::remove);
                return new Schema(newCols, dynamicPivotColumns);
            }

            public Schema renameColumn(String oldName, String newName) {
                GenericType type = columns.get(oldName);
                if (type == null) {
                    throw new com.gs.legend.compiler.PureCompileException(
                            "Cannot rename: column '" + oldName + "' does not exist. Available: " + columns.keySet());
                }
                var newCols = new java.util.LinkedHashMap<String, GenericType>();
                for (var entry : columns.entrySet()) {
                    if (entry.getKey().equals(oldName)) {
                        newCols.put(newName, entry.getValue());
                    } else {
                        newCols.put(entry.getKey(), entry.getValue());
                    }
                }
                return new Schema(newCols, dynamicPivotColumns);
            }

            public Schema merge(Schema other) {
                var newCols = new java.util.LinkedHashMap<>(columns);
                newCols.putAll(other.columns());
                return new Schema(newCols, dynamicPivotColumns);
            }

            public void assertHasColumn(String name) {
                if (!columns.containsKey(name)) {
                    throw new com.gs.legend.compiler.PureCompileException(
                            "Column '" + name + "' does not exist. Available columns: " + columns.keySet());
                }
            }

            public void assertHasColumns(java.util.List<String> names) {
                var missing = names.stream().filter(n -> !columns.containsKey(n)).toList();
                if (!missing.isEmpty()) {
                    throw new com.gs.legend.compiler.PureCompileException(
                            "Columns " + missing + " do not exist. Available columns: " + columns.keySet());
                }
            }

            public GenericType getColumnType(String name) { return columns.get(name); }

            public GenericType requireColumnType(String name) {
                GenericType type = columns.get(name);
                if (type == null) {
                    throw new com.gs.legend.compiler.PureCompileException(
                            "Column '" + name + "' does not exist. Available columns: " + columns.keySet());
                }
                return type;
            }

            public GenericType requireColumn(String name) { return requireColumnType(name); }
            public boolean hasColumn(String name) { return columns.containsKey(name); }
            public int size() { return columns.size(); }
            public java.util.List<String> columnNames() { return java.util.List.copyOf(columns.keySet()); }

            public static Schema empty() { return new Schema(java.util.Map.of(), java.util.List.of()); }

            public static Schema of(java.util.Map<String, GenericType> columns) {
                return new Schema(columns, java.util.List.of());
            }

            public static Schema of(java.util.List<String> names, java.util.List<GenericType> types) {
                if (names.size() != types.size()) {
                    throw new IllegalArgumentException("Column names and types must have the same size");
                }
                var cols = new java.util.LinkedHashMap<String, GenericType>();
                for (int i = 0; i < names.size(); i++) {
                    cols.put(names.get(i), types.get(i));
                }
                return new Schema(cols, java.util.List.of());
            }

            public static Schema ofSingle(String name, GenericType type) {
                return new Schema(java.util.Map.of(name, type), java.util.List.of());
            }

            @Override
            public String toString() {
                var sb = new StringBuilder("Relation<(");
                boolean first = true;
                for (var entry : columns.entrySet()) {
                    if (!first) sb.append(", ");
                    sb.append(entry.getKey()).append(":").append(entry.getValue().typeName());
                    first = false;
                }
                sb.append(")>");
                return sb.toString();
            }
        }
    }

    /**
     * Tuple type: the row schema that T binds to in Relation<T>.
     * In relational algebra, a Relation is a set of Tuples.
     * This is NOT a Relation — it represents a single row's column structure.
     * Used when offset functions (lead/lag/nth/first) return T[0..1].
     */
    record Tuple(Relation.Schema schema) implements GenericType {
        public Tuple {
            java.util.Objects.requireNonNull(schema, "Tuple schema must not be null");
        }

        @Override
        public String typeName() { return "Tuple"; }
    }

    // ========== Factory methods ==========

    static GenericType listOf(GenericType elementType) {
        return new Parameterized("List", List.of(elementType));
    }

    /** Creates a List type with explicit multiplicity. */
    static GenericType listOf(GenericType elementType, Multiplicity multiplicity) {
        return new Parameterized("List", List.of(elementType), multiplicity);
    }

    static GenericType pairOf(GenericType first, GenericType second) {
        return new Parameterized("Pair", List.of(first, second));
    }

    /**
     * Maps a Pure type name string to the best GenericType.
     * Only use for known primitive type names. For property types, use fromType(Type) instead.
     */
    static GenericType fromTypeName(String name) {
        // Handle qualified names: strip package prefix for primitive check
        String simpleName = name.contains("::") ? name.substring(name.lastIndexOf("::") + 2) : name;
        return switch (simpleName) {
            case "Integer" -> Primitive.INTEGER;
            case "Float" -> Primitive.FLOAT;
            case "Decimal" -> DEFAULT_DECIMAL;
            case "Number" -> Primitive.NUMBER;
            case "String" -> Primitive.STRING;
            case "Boolean" -> Primitive.BOOLEAN;
            case "Date" -> Primitive.DATE;
            case "StrictDate" -> Primitive.STRICT_DATE;
            case "DateTime" -> Primitive.DATE_TIME;
            case "StrictTime" -> Primitive.STRICT_TIME;
            case "Variant" -> Primitive.JSON;
            case "Any" -> Primitive.ANY;
            case "Nil" -> Primitive.NIL;
            // Non-primitive types (Relation, classes, enums) → ClassType
            default -> new ClassType(name);
        };
    }

    /**
     * Converts a Pure m3 Type to a GenericType, preserving class/enum identity.
     * This is the preferred conversion — avoids the lossy string path through fromTypeName.
     */
    static GenericType fromType(com.gs.legend.model.m3.Type type) {
        return switch (type) {
            case com.gs.legend.model.m3.PrimitiveType pt -> Primitive.fromTypeName(pt.typeName());
            case com.gs.legend.model.m3.PureClass pc -> new ClassType(pc.qualifiedName());
            case com.gs.legend.model.m3.PureEnumType et -> new EnumType(et.qualifiedName());
        };
    }

    // ========== Helper methods ==========

    /**
     * @return true if this type needs CAST for arithmetic operations.
     */
    default boolean needsNumericCast() {
        return this == Primitive.STRING || this == Primitive.JSON;
    }

    /**
     * @return true if this type is numeric.
     */
    default boolean isNumeric() {
        return (this instanceof Primitive p && p.isNumeric())
                || this instanceof PrecisionDecimal;
    }

    /**
     * @return true if this type is temporal.
     */
    default boolean isTemporal() {
        return this instanceof Primitive p && p.isTemporal();
    }

    /**
     * @return true if this type is a date (DATE, STRICT_DATE, DATE_TIME — not STRICT_TIME).
     */
    default boolean isDate() {
        return this instanceof Primitive p &&
                (p == Primitive.DATE || p == Primitive.STRICT_DATE || p == Primitive.DATE_TIME);
    }

    /**
     * @return true if this type is JSON-related.
     */
    default boolean isJson() {
        return this == Primitive.JSON || this.isList();
    }

    /**
     * Pure-level multiplicity of this type expression.
     * Non-null only on Parameterized types that have been stamped by the compiler.
     * Null means unset (treated as [1] / scalar).
     *
     * <p>Parameterized types override this via their record accessor.
     */
    default Multiplicity multiplicity() {
        return null;
    }

    /**
     * @return true if this is a parameterized List type.
     */
    default boolean isList() {
        return this instanceof Parameterized p && "List".equals(p.rawType());
    }

    /**
     * @return The element type if this is a List, or this type itself otherwise.
     */
    default GenericType elementType() {
        if (this instanceof Parameterized p && "List".equals(p.rawType())) {
            return p.elementType();
        }
        return this;
    }

    /**
     * @return true if this is a Primitive type.
     */
    default boolean isPrimitive() {
        return this instanceof Primitive;
    }

    /**
     * @return The Primitive if this is one, null otherwise.
     */
    default Primitive asPrimitive() {
        return this instanceof Primitive p ? p : null;
    }
}
