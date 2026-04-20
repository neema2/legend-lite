package com.gs.legend.model.m3;

import com.gs.legend.compiler.PureCompileException;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.SymbolTable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Unified sealed hierarchy for type <em>expressions</em> in the Pure type system.
 *
 * <p>Single replacement for the legacy overlapping representations that used to live
 * as {@code plan.Type}, {@code m3.TypeRef}, {@code m3.PrimitiveType},
 * {@code compiled.TypeRef}, and raw {@code String} type names.
 *
 * <p>Orthogonality invariants:
 * <ul>
 *   <li>A {@code Type} carries <em>no</em> multiplicity. Multiplicity lives on
 *       {@link Parameter}, {@code Property}, {@code ExpressionType}, and {@code TypeInfo}
 *       — never inlined on the type itself.</li>
 *   <li>Declarations ({@link PureClass}, {@link PureEnum}) are separate records, not
 *       Type variants. A {@code Type.ClassType} is a <em>reference</em> to a class
 *       declaration; the {@link PureClass} itself is the declaration. Primitives are
 *       the exception — {@link Primitive} is both the declaration AND a Type variant,
 *       because the decl is inherently lightweight and always available.</li>
 *   <li>{@link #isSubtypeOf(Type)} is polymorphic — callers never need to pre-normalize
 *       (e.g., the old {@code PrecisionDecimal → DECIMAL} copy-paste is gone).</li>
 * </ul>
 */
public sealed interface Type permits
        Primitive, LClass,
        Type.NameRef, Type.ClassType, Type.EnumType,
        Type.PrecisionDecimal, Type.GenericType,
        Type.FunctionType, Type.Relation, Type.Tuple,
        Type.TypeVar, Type.SchemaAlgebra, Type.RelationTypeVar,
        Type.FunctionReference {

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

    /**
     * Extracts the {@link LClass} identity of a Type, if any. Returns the {@code LClass} whether
     * {@code t} is a bare zero-arg variant ({@code LClass.RANGE}) or wrapped in a
     * {@link GenericType} with an {@code LClass} rawType ({@code GenericType(LClass.RELATION, [T])}).
     * Returns {@code null} for user-defined class types, primitives, type variables, and other
     * non-platform kinds.
     */
    static LClass asLClass(Type t) {
        if (t instanceof LClass lc) return lc;
        if (t instanceof GenericType g && g.rawType() instanceof LClass lc) return lc;
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
        return ctx.findType(name).orElseThrow(() -> new IllegalStateException(
                "Unknown type: '" + name + "'. Not a primitive, class, or enum in the current model context."));
    }

    // ============================================================
    //  Nominal types: references to declared classes / enums
    // ============================================================

    /**
     * Pre-classification pointer: a fully qualified name whose Kind (primitive / class /
     * enum) has not yet been resolved. Produced by the parser for user source where the
     * target declaration may not be registered yet. The type checker is responsible for
     * resolving {@code NameRef} to a classified variant ({@link Primitive}, {@link ClassType},
     * or {@link EnumType}) via {@link ModelContext#findType} before emitting TypeInfo.
     *
     * <p><strong>Invariant:</strong> no {@code NameRef} flows past the type checker. If
     * downstream code (PlanGenerator, dialects, serializers) ever encounters a {@code NameRef},
     * the type checker failed to resolve it — that's a compiler bug.
     *
     * <p>Intentionally minimal: holds only the FQN. All other information (Kind, body) is
     * looked up on demand through the model context.
     */
    record NameRef(String qualifiedName) implements Type {
        public NameRef {
            Objects.requireNonNull(qualifiedName, "NameRef qualifiedName cannot be null");
        }

        @Override
        public String typeName() {
            return SymbolTable.extractSimpleName(qualifiedName);
        }
    }

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
     * The corresponding declaration is {@link PureEnum}.
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
     * Structured generic type — {@code rawType<arg1, arg2, ...>} where {@code rawType} is itself
     * a {@link Type}, not a raw {@link String}. The replacement for {@link Parameterized} being
     * rolled out in phase 2.5e.
     *
     * <p><strong>Lifecycle:</strong>
     * <ul>
     *   <li>Before {@code NameResolver}: {@code rawType} is a {@link NameRef} holding the unresolved
     *       (possibly simple) name from user source, symmetric with leaf name references.</li>
     *   <li>After {@code NameResolver}: {@code rawType} is a classified variant — typically
     *       {@link ClassType} for builtin / user class generics ({@code Relation<T>}, {@code Pair<U,V>}),
     *       or another {@link Type} kind when native signatures use e.g. {@link Primitive#ANY} as
     *       the base.</li>
     * </ul>
     *
     * <p><strong>Invariant:</strong> no {@code GenericType} with a {@link NameRef} rawType flows past
     * the type checker. A residual {@code NameRef} means import resolution failed — that's a
     * compiler bug, not a runtime-tolerable state.
     *
     * <p>Replaces the string-keyed dispatch pattern of {@link Parameterized}: structural-match
     * sites now compare {@code rawType instanceof ClassType ct && ct.qualifiedName().equals(FQN)}
     * instead of {@code rawType.equals("Relation")}.
     */
    record GenericType(Type rawType, List<Type> typeArgs) implements Type {
        public GenericType {
            Objects.requireNonNull(rawType, "GenericType rawType cannot be null");
            Objects.requireNonNull(typeArgs, "GenericType typeArgs cannot be null");
            typeArgs = List.copyOf(typeArgs);
        }

        @Override
        public String typeName() {
            return rawType.typeName();
        }

        /** First type argument. Throws if empty. */
        public Type elementType() {
            if (typeArgs.isEmpty()) {
                throw new IllegalStateException(
                        "GenericType '" + rawType.typeName() + "' has no type arguments — cannot extract element type");
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
     * A named (type, multiplicity) triple used for function parameters. Keeps
     * multiplicity orthogonal to the type expression.
     *
     * <p>{@code name} is non-null but may be empty ({@code ""}) when the parameter
     * originates from a function <em>type</em> literal such as {@code {Integer[1]->Boolean[1]}},
     * where Pure's grammar provides no identifier. This matches legend-pure's own
     * metamodel encoding (see {@code AntlrContextToM3CoreInstance.typeFunctionTypePureType}
     * which constructs {@code VariableExpressionInstance(..., "")}). A function
     * <em>definition</em> parameter always has a non-empty name.
     */
    record Parameter(String name, Type type, Multiplicity multiplicity) {
        public Parameter {
            Objects.requireNonNull(name, "Parameter name cannot be null (use \"\" for anonymous function-type params)");
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
