package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Rich type system for the IR, replacing the flat PureType enum.
 * Supports type hierarchy, generics (List&lt;Integer&gt;), class types, and enum types.
 *
 * In Pure: Integer[*] = type Integer + multiplicity [*].
 * Multiplicity is tracked separately via m3.Multiplicity.
 */
public sealed interface GenericType
        permits GenericType.Primitive, GenericType.Parameterized,
                GenericType.ClassType, GenericType.EnumType {

    String typeName();

    /**
     * Primitive types with subtype hierarchy.
     *
     * Hierarchy:
     *   ANY
     *   ├── NUMBER
     *   │   ├── INTEGER
     *   │   ├── FLOAT
     *   │   └── DECIMAL
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
        DEFERRED,
        NUMBER, INTEGER, FLOAT, DECIMAL,
        STRING,
        BOOLEAN,
        DATE, STRICT_DATE, DATE_TIME, STRICT_TIME,
        JSON;

        @Override
        public String typeName() {
            return switch (this) {
                case ANY -> "Any";
                case NIL -> "Nil";
                case DEFERRED -> "Deferred";
                case NUMBER -> "Number";
                case INTEGER -> "Integer";
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
            if (this == DEFERRED || other == DEFERRED) return false; // DEFERRED is not in the type hierarchy
            return switch (this) {
                case INTEGER, FLOAT, DECIMAL -> other == NUMBER;
                case STRICT_DATE, DATE_TIME -> other == DATE;
                default -> false;
            };
        }

        public boolean isNumeric() {
            return this == INTEGER || this == FLOAT || this == DECIMAL || this == NUMBER;
        }

        public boolean isTemporal() {
            return this == DATE || this == STRICT_DATE || this == DATE_TIME || this == STRICT_TIME;
        }

        /**
         * Maps a Pure type name string to the corresponding Primitive.
         */
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
     */
    record Parameterized(String rawType, List<GenericType> typeArgs) implements GenericType {
        public Parameterized {
            Objects.requireNonNull(rawType);
            Objects.requireNonNull(typeArgs);
            typeArgs = List.copyOf(typeArgs);
        }

        @Override
        public String typeName() {
            return rawType;
        }

        /**
         * @return The element type for single-parameter collections (e.g., List&lt;Integer&gt; → Integer).
         */
        public GenericType elementType() {
            if (typeArgs.isEmpty()) return Primitive.ANY;
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

    // ========== Constants ==========

    /** List with unknown element type — temporary until full type propagation. */
    static GenericType LIST_ANY() {
        return new Parameterized("List", List.of(Primitive.ANY));
    }

    // ========== Factory methods ==========

    static GenericType listOf(GenericType elementType) {
        return new Parameterized("List", List.of(elementType));
    }

    static GenericType pairOf(GenericType first, GenericType second) {
        return new Parameterized("Pair", List.of(first, second));
    }

    /**
     * Maps a Pure type name string to the best GenericType.
     * Only use for known primitive type names. For property types, use fromType(Type) instead.
     */
    static GenericType fromTypeName(String name) {
        return Primitive.fromTypeName(name);
    }

    /**
     * Converts a Pure m3 Type to a GenericType, preserving class/enum identity.
     * This is the preferred conversion — avoids the lossy string path through fromTypeName.
     */
    static GenericType fromType(org.finos.legend.pure.m3.Type type) {
        return switch (type) {
            case org.finos.legend.pure.m3.PrimitiveType pt -> Primitive.fromTypeName(pt.typeName());
            case org.finos.legend.pure.m3.PureClass pc -> new ClassType(pc.qualifiedName());
            case org.finos.legend.pure.m3.PureEnumType et -> new EnumType(et.qualifiedName());
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
        return this instanceof Primitive p && p.isNumeric();
    }

    /**
     * @return true if this type is temporal.
     */
    default boolean isTemporal() {
        return this instanceof Primitive p && p.isTemporal();
    }

    /**
     * @return true if this type is JSON-related.
     */
    default boolean isJson() {
        return this == Primitive.JSON || this.isList();
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
