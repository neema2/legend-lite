package com.gs.legend.model.m3;

import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.def.EnumDefinition;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Unified sealed hierarchy for type <em>expressions</em> in the Pure type system.
 *
 * <p>Introduced in Phase B chunk 2.5a as the single replacement for seven overlapping
 * representations: {@code compiler.PType}, {@code plan.Type}, {@code m3.TypeRef},
 * {@code m3.PrimitiveType}, {@code compiled.TypeRef}, plus the old {@code m3.Type}
 * (now {@link TypeDecl}) and raw {@code String} type names.
 *
 * <p>Orthogonality invariants:
 * <ul>
 *   <li>A {@code Type} carries <em>no</em> multiplicity. Multiplicity lives on
 *       {@link Parameter}, {@code Property}, {@code ExpressionType}, and {@code TypeInfo}
 *       — never inlined on the type itself.</li>
 *   <li>Declarations ({@link PureClass}, {@link PureEnumType}) live on the separate
 *       {@link TypeDecl} hierarchy (Java's {@code TypeElement} vs {@code TypeMirror} split).
 *       A {@code Type.ClassType} is a <em>reference</em> to a class declaration;
 *       the {@link PureClass} itself is the declaration.</li>
 *   <li>{@link #isSubtypeOf(Type)} is polymorphic — callers never need to pre-normalize
 *       (e.g., the old {@code PrecisionDecimal → DECIMAL} copy-paste is gone).</li>
 * </ul>
 *
 * <p>See {@code .windsurf/plans/phase-b-type-sweep-findings-c0954a.md} for the full
 * audit findings and staged consolidation plan.
 */
public sealed interface Type {

    /**
     * Human-readable name for errors, debugging, and display.
     * For primitives this is the Pure-level name (e.g., {@code "Integer"}). For
     * classes/enums this is the simple name extracted from the FQN.
     */
    String typeName();

    /**
     * {@code true} if this type is a subtype of (or equal to) {@code other}.
     *
     * <p>Default implementation: identity or {@link Primitive#ANY}. Variants override
     * for their own rules (primitive hierarchy, PrecisionDecimal ↔ DECIMAL, etc.).
     */
    default boolean isSubtypeOf(Type other) {
        return this.equals(other) || (other instanceof Primitive p && p == Primitive.ANY);
    }

    // ============================================================
    //  Convenience classification predicates (default methods)
    // ============================================================

    /** @return {@code true} if this is a {@link Primitive}. */
    default boolean isPrimitive() {
        return this instanceof Primitive;
    }

    /** @return the {@link Primitive} if this is one, else {@code null}. */
    default Primitive asPrimitive() {
        return this instanceof Primitive p ? p : null;
    }

    /** @return {@code true} for numeric primitives and {@link PrecisionDecimal}. */
    default boolean isNumeric() {
        return (this instanceof Primitive p && p.isNumeric())
                || this instanceof PrecisionDecimal;
    }

    /** @return {@code true} for temporal primitives (DATE, STRICT_DATE, DATE_TIME, STRICT_TIME). */
    default boolean isTemporal() {
        return this instanceof Primitive p && p.isTemporal();
    }

    /** @return {@code true} for date-kind primitives (DATE, STRICT_DATE, DATE_TIME — not STRICT_TIME). */
    default boolean isDate() {
        return this instanceof Primitive p
                && (p == Primitive.DATE || p == Primitive.STRICT_DATE || p == Primitive.DATE_TIME);
    }

    // ============================================================
    //  Static combinators
    // ============================================================

    /**
     * Closest common supertype. Both primitives walk the
     * {@linkplain Primitive#commonSupertype primitive lattice};
     * {@link PrecisionDecimal} is treated as {@link Primitive#DECIMAL} for the walk.
     * Mixed or unrelated types collapse to {@link Primitive#ANY}.
     */
    static Type commonSupertype(Type a, Type b) {
        if (a.equals(b)) return a;
        Primitive pa = toPrimitive(a);
        Primitive pb = toPrimitive(b);
        if (pa != null && pb != null) {
            return Primitive.commonSupertype(pa, pb);
        }
        return Primitive.ANY;
    }

    private static Primitive toPrimitive(Type t) {
        if (t instanceof Primitive p) return p;
        if (t instanceof PrecisionDecimal) return Primitive.DECIMAL;
        return null;
    }

    // ============================================================
    //  Kind-aware String -> Type resolver (the ONLY resolver)
    // ============================================================

    /**
     * Canonical String → {@link Type} resolver. Tries the three kinds in order:
     *
     * <ol>
     *   <li>{@link Primitive} via {@link Primitive#lookup}</li>
     *   <li>Class via {@link ModelContext#findClass}</li>
     *   <li>Enum via {@link ModelContext#findEnum}</li>
     * </ol>
     *
     * <p><strong>This is the one and only kind-aware resolver.</strong> It replaces the
     * seven scattered String→Type resolvers in the legacy code:
     * {@code Type.fromTypeName} (leaky default to ClassType),
     * {@code Type.Primitive.fromTypeName} (throwing, used for control flow),
     * {@code m3.PrimitiveType.fromName} (case-insensitive, inconsistent),
     * {@code TdsChecker} / {@code NewChecker} / {@code CastChecker} locals, and the
     * {@code parseMultiplicity} trio.
     *
     * <p><strong>Never returns a leaky default.</strong> If {@code name} is not a
     * primitive, class, or enum in the provided {@link ModelContext}, throws
     * {@link IllegalStateException}.
     *
     * <p>Callers that already know the kind from parse context (e.g., the parser just
     * saw a {@code Class} keyword) should construct the variant directly
     * ({@code new ClassType(fqn)}) rather than invoke this method — this resolver is
     * for cases where the kind must be inferred from the model.
     *
     * @param name Simple or fully qualified type name
     * @param ctx  Model context for class / enum lookup
     * @return The resolved {@link Type}
     * @throws IllegalStateException if {@code name} is not a known primitive, class, or enum
     */
    static Type resolve(String name, ModelContext ctx) {
        Objects.requireNonNull(name, "Type name cannot be null");
        Objects.requireNonNull(ctx, "ModelContext cannot be null");

        Optional<Primitive> primitive = Primitive.lookup(name);
        if (primitive.isPresent()) {
            return primitive.get();
        }

        Optional<PureClass> cls = ctx.findClass(name);
        if (cls.isPresent()) {
            return new ClassType(cls.get().qualifiedName());
        }

        Optional<EnumDefinition> enm = ctx.findEnum(name);
        if (enm.isPresent()) {
            return new EnumType(enm.get().qualifiedName());
        }

        throw new IllegalStateException(
                "Unknown type: '" + name + "'. Not a primitive, class, or enum in the current model context.");
    }

    // ============================================================
    //  Primitive scalar types with subtype hierarchy
    // ============================================================

    /**
     * Primitive types.
     *
     * <pre>
     *   ANY
     *   ├── NUMBER
     *   │   ├── INTEGER
     *   │   │   ├── INT64
     *   │   │   └── INT128
     *   │   ├── FLOAT
     *   │   └── DECIMAL   (parameterized form: {@link PrecisionDecimal})
     *   ├── STRING
     *   ├── BOOLEAN
     *   ├── DATE
     *   │   ├── STRICT_DATE
     *   │   └── DATE_TIME
     *   ├── STRICT_TIME
     *   └── JSON
     *   NIL (bottom — subtype of every type)
     * </pre>
     */
    enum Primitive implements Type {
        ANY,
        NIL,
        NUMBER, INTEGER, INT64, INT128, FLOAT, DECIMAL,
        STRING,
        BOOLEAN,
        DATE, STRICT_DATE, DATE_TIME, STRICT_TIME,
        JSON;

        /**
         * Canonical Pure-level name for this primitive. Single source of truth —
         * replaces the duplicated {@code case "Integer" -> ...} switches scattered
         * across {@code Type}, {@code m3.PrimitiveType}, {@code SqlDataType}, etc.
         */
        public String pureName() {
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

        @Override
        public String typeName() {
            return pureName();
        }

        /**
         * Resolves a Pure type name to its {@link Primitive} or returns empty.
         * Accepts simple names ({@code "Integer"}) and FQNs
         * ({@code "meta::pure::metamodel::variant::Variant"}).
         *
         * <p><strong>Never throws, never falls back to another kind.</strong> Replaces
         * the three leaky / throwing resolvers in the legacy type system:
         * {@code Type.Primitive.fromTypeName} (threw), {@code Type.fromTypeName}
         * (silently fell back to {@code ClassType}), and {@code m3.PrimitiveType.fromName}
         * (case-insensitive lookup, threw). If the caller needs class/enum resolution,
         * they dispatch to {@link com.gs.legend.model.ModelContext} explicitly.
         */
        public static Optional<Primitive> lookup(String name) {
            if (name == null) return Optional.empty();
            String simple = SymbolTable.extractSimpleName(name);
            return switch (simple) {
                case "Any" -> Optional.of(ANY);
                case "Nil" -> Optional.of(NIL);
                case "Number" -> Optional.of(NUMBER);
                case "Integer" -> Optional.of(INTEGER);
                case "Float" -> Optional.of(FLOAT);
                case "Decimal" -> Optional.of(DECIMAL);
                case "String" -> Optional.of(STRING);
                case "Boolean" -> Optional.of(BOOLEAN);
                case "Date" -> Optional.of(DATE);
                case "StrictDate" -> Optional.of(STRICT_DATE);
                case "DateTime" -> Optional.of(DATE_TIME);
                case "StrictTime" -> Optional.of(STRICT_TIME);
                case "Variant", "JSON" -> Optional.of(JSON);
                default -> Optional.empty();
            };
        }

        /**
         * Direct parent in the primitive hierarchy. ANY is the root (returns itself).
         */
        public Primitive parent() {
            return switch (this) {
                case ANY -> ANY;
                case NIL -> ANY;
                case NUMBER, STRING, BOOLEAN, DATE, STRICT_TIME, JSON -> ANY;
                case INTEGER, FLOAT, DECIMAL -> NUMBER;
                case INT64, INT128 -> INTEGER;
                case STRICT_DATE, DATE_TIME -> DATE;
            };
        }

        /** Lowest common ancestor of two primitive types. */
        public static Primitive commonSupertype(Primitive a, Primitive b) {
            if (a == b) return a;
            if (a.isSubtypeOf(b)) return b;
            if (b.isSubtypeOf(a)) return a;
            Primitive cursor = a;
            while (cursor != ANY) {
                cursor = cursor.parent();
                if (b.isSubtypeOf(cursor)) return cursor;
            }
            return ANY;
        }

        public boolean isNumeric() {
            return this == NUMBER || this == INTEGER || this == INT64 || this == INT128
                    || this == FLOAT || this == DECIMAL;
        }

        public boolean isTemporal() {
            return this == DATE || this == STRICT_DATE || this == DATE_TIME || this == STRICT_TIME;
        }

        public boolean isInteger() {
            return this == INTEGER || this == INT64 || this == INT128;
        }

        /**
         * Primitive-aware subtype check. Handles the primitive lattice; the
         * {@code PrecisionDecimal ↔ DECIMAL} relation is handled inside
         * {@link PrecisionDecimal#isSubtypeOf}.
         */
        @Override
        public boolean isSubtypeOf(Type other) {
            if (other instanceof Primitive p) {
                if (this == p) return true;
                if (this == NIL) return true;                          // NIL ⊂ every type
                if (p == ANY) return true;                             // every type ⊂ ANY
                return switch (this) {
                    case INTEGER, FLOAT, DECIMAL -> p == NUMBER;
                    case INT64, INT128 -> p == INTEGER || p == NUMBER;
                    case STRICT_DATE, DATE_TIME -> p == DATE;
                    default -> false;
                };
            }
            return false;
        }
    }

    // ============================================================
    //  Nominal types: references to declared classes / enums
    // ============================================================

    /**
     * Reference to a user-defined class by fully qualified name.
     * The corresponding declaration is {@link PureClass}.
     */
    record ClassType(String qualifiedName) implements Type {
        public ClassType {
            Objects.requireNonNull(qualifiedName, "ClassType qualifiedName cannot be null");
        }

        @Override
        public String typeName() {
            return SymbolTable.extractSimpleName(qualifiedName);
        }
    }

    /**
     * Reference to a user-defined enum by fully qualified name.
     * The corresponding declaration is {@link PureEnumType}.
     */
    record EnumType(String qualifiedName) implements Type {
        public EnumType {
            Objects.requireNonNull(qualifiedName, "EnumType qualifiedName cannot be null");
        }

        @Override
        public String typeName() {
            return SymbolTable.extractSimpleName(qualifiedName);
        }
    }

    // ============================================================
    //  Decimal with precision / scale
    // ============================================================

    /**
     * Decimal with explicit precision and scale: {@code DECIMAL(p, s)}. Subtype of
     * {@link Primitive#DECIMAL} and {@link Primitive#NUMBER}.
     *
     * <p>This variant handles subtyping against {@code DECIMAL} natively — callers
     * never need to "normalize" {@code PrecisionDecimal} to {@code DECIMAL} before
     * checking subtype relations (unlike the legacy {@code Type.PrecisionDecimal}
     * which required 5 scattered normalization copies).
     */
    record PrecisionDecimal(int precision, int scale) implements Type {
        @Override
        public String typeName() {
            return "Decimal(" + precision + "," + scale + ")";
        }

        @Override
        public boolean isSubtypeOf(Type other) {
            if (this.equals(other)) return true;
            if (other instanceof Primitive p) {
                return p == Primitive.DECIMAL || p == Primitive.NUMBER || p == Primitive.ANY;
            }
            return false;
        }
    }

    /**
     * Default precision/scale for Pure's unparameterized {@code Decimal} type.
     * Matches {@code DECIMAL(38, 18)} — the widest SQL decimal compatible with DuckDB's INT128.
     */
    PrecisionDecimal DEFAULT_DECIMAL = new PrecisionDecimal(38, 18);

    // ============================================================
    //  Parameterized types: List<T>, Pair<K,V>, Map<K,V>, Relation<T>, etc.
    // ============================================================

    /**
     * Parameterized type like {@code List<T>}, {@code Pair<K,V>}, {@code Relation<T>},
     * or native-signature-only pseudo-types like {@code ColSpec<Z⊆T>}.
     *
     * <p><strong>Invariant:</strong> a Pure property declared as {@code T[*]} is
     * {@code (Type=T, Multiplicity=MANY)} on the owning {@link Parameter} / {@code Property}
     * — never {@code List<T>}. Using {@code Parameterized("List", [T])} as a proxy for
     * collection multiplicity is forbidden.
     */
    record Parameterized(String rawType, List<Type> typeArgs) implements Type {
        public Parameterized {
            Objects.requireNonNull(rawType, "Parameterized rawType cannot be null");
            Objects.requireNonNull(typeArgs, "Parameterized typeArgs cannot be null");
            typeArgs = List.copyOf(typeArgs);
        }

        @Override
        public String typeName() {
            return rawType;
        }

        /** First type argument (single-element collections, e.g., {@code List<T>.elementType() == T}). */
        public Type elementType() {
            if (typeArgs.isEmpty()) {
                throw new IllegalStateException(
                        "Parameterized '" + rawType + "' has no type arguments — cannot extract element type");
            }
            return typeArgs.get(0);
        }
    }

    // ============================================================
    //  Function type: {T[1] -> R[1]}, {A[1], B[*] -> C[0..1]}, etc.
    // ============================================================

    /**
     * Structured function type: parameters + return type + return multiplicity.
     * Used for lambda signatures, function references, and the {@code Function<{...}>}
     * parameter type in native function signatures.
     */
    record FunctionType(List<Parameter> params, Type returnType, Multiplicity returnMult) implements Type {
        public FunctionType {
            Objects.requireNonNull(params, "FunctionType params cannot be null");
            Objects.requireNonNull(returnType, "FunctionType returnType cannot be null");
            Objects.requireNonNull(returnMult, "FunctionType returnMult cannot be null");
            params = List.copyOf(params);
        }

        @Override
        public String typeName() {
            return "Function";
        }
    }

    // ============================================================
    //  Relation: tabular data with a column schema
    // ============================================================

    /**
     * Relational type: {@code Relation<(col1:Type1, col2:Type2, ...)>}. Carries the
     * resolved column {@link Schema}.
     */
    record Relation(Schema schema) implements Type {
        public Relation {
            Objects.requireNonNull(schema, "Relation schema cannot be null");
        }

        @Override
        public String typeName() {
            return "Relation";
        }
    }

    /**
     * Row schema that {@code T} binds to in {@code Relation<T>}. A {@code Relation} is a
     * set of {@code Tuple}s. Used by offset functions (lead/lag/nth/first) that return
     * a single row of a relation.
     */
    record Tuple(Schema schema) implements Type {
        public Tuple {
            Objects.requireNonNull(schema, "Tuple schema cannot be null");
        }

        @Override
        public String typeName() {
            return "Tuple";
        }
    }

    // ============================================================
    //  Native-signature-only types: TypeVar, SchemaAlgebra, RelationTypeVar
    // ============================================================

    /**
     * Type variable like {@code T}, {@code V}, {@code K}. Bound during native function
     * signature unification. Not a type expression in user code — only appears inside
     * parsed native function signatures.
     */
    record TypeVar(String name) implements Type {
        public TypeVar {
            Objects.requireNonNull(name, "TypeVar name cannot be null");
        }

        @Override
        public String typeName() {
            return name;
        }
    }

    /**
     * Schema algebra — {@code T+V} (union), {@code T-Z} (difference), {@code Z⊆T}
     * (subset), {@code Z=K} (equal). Appears in native function signatures for
     * schema-transforming operations (extend, join, pivot).
     */
    record SchemaAlgebra(Type left, Op op, Type right) implements Type {
        public enum Op { UNION, DIFFERENCE, SUBSET, EQUAL }

        public SchemaAlgebra {
            Objects.requireNonNull(left, "SchemaAlgebra left cannot be null");
            Objects.requireNonNull(right, "SchemaAlgebra right cannot be null");
            Objects.requireNonNull(op, "SchemaAlgebra op cannot be null");
        }

        @Override
        public String typeName() {
            return left.typeName() + " " + op + " " + right.typeName();
        }
    }

    /**
     * Inline relation-type variable: {@code (name:String[1], age:Integer[1])}.
     * Used in extend/window lambdas where the row type is spelled out structurally
     * rather than referenced via {@link TypeVar}.
     */
    record RelationTypeVar(List<Column> columns) implements Type {
        public RelationTypeVar {
            Objects.requireNonNull(columns, "RelationTypeVar columns cannot be null");
            columns = List.copyOf(columns);
        }

        public record Column(String name, Type type, Multiplicity multiplicity) {
            public Column {
                Objects.requireNonNull(name, "Column name cannot be null");
                Objects.requireNonNull(type, "Column type cannot be null");
                Objects.requireNonNull(multiplicity, "Column multiplicity cannot be null");
            }
        }

        @Override
        public String typeName() {
            return "RelationTypeVar";
        }
    }

    // ============================================================
    //  Function reference: a qualified name pointing to a function definition
    // ============================================================

    /**
     * Reference to a function by its fully qualified (mangled) name. In Pure,
     * function references are first-class values with type {@code Function<...>}.
     */
    record FunctionReference(String qualifiedPath) implements Type {
        public FunctionReference {
            Objects.requireNonNull(qualifiedPath, "FunctionReference qualifiedPath cannot be null");
        }

        @Override
        public String typeName() {
            return "Function";
        }
    }

    // ============================================================
    //  Supporting records: Parameter, Schema
    // ============================================================

    /**
     * A named (type, multiplicity) pair used for function parameters. Keeps
     * multiplicity orthogonal to the type expression.
     */
    record Parameter(String name, Type type, Multiplicity multiplicity) {
        public Parameter {
            Objects.requireNonNull(name, "Parameter name cannot be null");
            Objects.requireNonNull(type, "Parameter type cannot be null");
            Objects.requireNonNull(multiplicity, "Parameter multiplicity cannot be null");
        }
    }

    /**
     * Column schema shared by {@link Relation} and {@link Tuple}. Immutable;
     * column order is preserved via {@link LinkedHashMap}.
     *
     * <p>Shape-compatible with the legacy {@code Type.Schema} to make
     * the chunk 2.5b migration a mechanical type-name swap. Intentionally simpler than
     * legend-pure's {@code RelationType} + {@code Column<U,V>[m]} metamodel — in
     * particular, we do not yet track per-column multiplicity, name wildcards, or
     * column stereotypes/taggedValues. Those features are not exercised by legend-lite
     * today and would cascade across {@code ExtendChecker} / {@code ProjectChecker} /
     * dozens of callers; they are deferred to a future chunk when actually needed.
     *
     * <p>{@code dynamicPivotColumns} is legend-lite-specific (not in Pure metamodel) —
     * captures column specs for pivot queries where names depend on runtime data.
     *
     * @param columns             Ordered map of column name to column type
     * @param dynamicPivotColumns Specs for data-dependent pivot columns; empty for non-pivot
     */
    record Schema(Map<String, Type> columns,
                  List<DynamicPivotColumn> dynamicPivotColumns) {

        /**
         * Dynamic pivot column spec: the column name has an {@code aliasSuffix} appended
         * to a data-derived prefix, and the value has {@code returnType}. Used by
         * {@code pivot()} to describe columns whose final names aren't known until
         * query execution.
         */
        public record DynamicPivotColumn(String aliasSuffix, Type returnType) {
            public static final String SEPARATOR = "__|__";
        }

        public Schema {
            Objects.requireNonNull(columns, "Schema columns cannot be null");
            columns = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(columns));
            dynamicPivotColumns = dynamicPivotColumns != null
                    ? List.copyOf(dynamicPivotColumns) : List.of();
        }

        // ----- Factories -----

        public static Schema empty() {
            return new Schema(Map.of(), List.of());
        }

        public static Schema withoutPivot(Map<String, Type> columns) {
            return new Schema(columns, List.of());
        }

        public static Schema of(Map<String, Type> columns) {
            return new Schema(columns, List.of());
        }

        public static Schema of(List<String> names, List<Type> types) {
            if (names.size() != types.size()) {
                throw new IllegalArgumentException(
                        "Column names and types must have the same size");
            }
            var cols = new LinkedHashMap<String, Type>();
            for (int i = 0; i < names.size(); i++) {
                cols.put(names.get(i), types.get(i));
            }
            return new Schema(cols, List.of());
        }

        public static Schema ofSingle(String name, Type type) {
            return new Schema(Map.of(name, type), List.of());
        }

        // ----- Structural accessors -----

        public int size() {
            return columns.size();
        }

        public boolean hasColumn(String name) {
            return columns.containsKey(name);
        }

        public Type getColumnType(String name) {
            return columns.get(name);
        }

        public Type requireColumnType(String name) {
            Type type = columns.get(name);
            if (type == null) {
                throw new PureCompileException(
                        "Column '" + name + "' does not exist. Available columns: " + columns.keySet());
            }
            return type;
        }

        /** Alias for {@link #requireColumnType(String)} — matches legacy naming. */
        public Type requireColumn(String name) {
            return requireColumnType(name);
        }

        public List<String> columnNames() {
            return List.copyOf(columns.keySet());
        }

        // ----- Assertions -----

        public void assertHasColumn(String name) {
            if (!columns.containsKey(name)) {
                throw new PureCompileException(
                        "Column '" + name + "' does not exist. Available columns: " + columns.keySet());
            }
        }

        public void assertHasColumns(List<String> names) {
            var missing = names.stream().filter(n -> !columns.containsKey(n)).toList();
            if (!missing.isEmpty()) {
                throw new PureCompileException(
                        "Columns " + missing + " do not exist. Available columns: " + columns.keySet());
            }
        }

        // ----- Transformations (all return new immutable Schemas) -----

        public Schema withColumn(String name, Type type) {
            var newCols = new LinkedHashMap<>(columns);
            newCols.put(name, type);
            return new Schema(newCols, dynamicPivotColumns);
        }

        public Schema withColumns(Map<String, Type> additionalColumns) {
            var newCols = new LinkedHashMap<>(columns);
            newCols.putAll(additionalColumns);
            return new Schema(newCols, dynamicPivotColumns);
        }

        public Schema onlyColumns(List<String> names) {
            var newCols = new LinkedHashMap<String, Type>();
            for (String name : names) {
                Type type = columns.get(name);
                if (type == null) {
                    throw new PureCompileException(
                            "Column '" + name + "' does not exist. Available columns: " + columns.keySet());
                }
                newCols.put(name, type);
            }
            return new Schema(newCols, dynamicPivotColumns);
        }

        public Schema withoutColumns(java.util.Set<String> names) {
            var newCols = new LinkedHashMap<>(columns);
            names.forEach(newCols::remove);
            return new Schema(newCols, dynamicPivotColumns);
        }

        public Schema renameColumn(String oldName, String newName) {
            Type type = columns.get(oldName);
            if (type == null) {
                throw new PureCompileException(
                        "Cannot rename: column '" + oldName + "' does not exist. Available: " + columns.keySet());
            }
            var newCols = new LinkedHashMap<String, Type>();
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
            var newCols = new LinkedHashMap<>(columns);
            newCols.putAll(other.columns());
            return new Schema(newCols, dynamicPivotColumns);
        }

        @Override
        public String toString() {
            var sb = new StringBuilder("(");
            boolean first = true;
            for (var entry : columns.entrySet()) {
                if (!first) sb.append(", ");
                sb.append(entry.getKey()).append(":").append(entry.getValue().typeName());
                first = false;
            }
            return sb.append(')').toString();
        }
    }
}
