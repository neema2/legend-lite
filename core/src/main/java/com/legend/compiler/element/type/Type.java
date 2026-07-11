package com.legend.compiler.element.type;

import com.legend.builtin.Pure;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The kinded, post-classification type &mdash; Phase F output. Mirrors
 * {@code engine.m3.Type}. Pure-data; holds FQN <strong>strings</strong> only
 * (never live element refs), so it serializes to {@code .legend} unchanged
 * (core/README invariant 11).
 *
 * <h2>Relationship to {@link com.legend.parser.TypeExpression}</h2>
 *
 * <p>{@code TypeExpression} is the <em>pre-classification</em> parser AST
 * (bare/FQN name heads). {@code Type} is what {@code ElementCompiler} produces
 * by walking a {@code TypeExpression} against
 * {@link com.legend.compiler.element.ModelContext#findType}, the single place a
 * name becomes a kind:
 *
 * <pre>
 *   NameRef("…::Integer")    -&gt; Primitive.INTEGER
 *   NameRef("model::Person") -&gt; ClassType
 *   NameRef("…::Month")      -&gt; EnumType
 *   NameRef("T")             -&gt; TypeVar
 *   Generic("Relation", […]) -&gt; GenericType(Pure.RELATION fqn, [Type…])
 *   TE.FunctionType          -&gt; Type.FunctionType
 *   TE.SchemaAlgebra         -&gt; Type.SchemaAlgebra
 * </pre>
 *
 * <h2>Variants</h2>
 * <ul>
 *   <li><b>scalar leaves</b> &mdash; {@link Primitive}, {@link PrecisionDecimal}</li>
 *   <li><b>nominal</b> &mdash; {@link ClassType}, {@link EnumType} (FQN-only)</li>
 *   <li><b>variables &amp; application</b> &mdash; {@link TypeVar}, {@link GenericType}
 *       (required to classify the stdlib generics in {@code builtin/Pure})</li>
 *   <li><b>structural</b> &mdash; {@link FunctionType}, {@link RelationType}</li>
 *   <li><b>schema algebra</b> &mdash; {@link SchemaAlgebra} ({@code T+V}, {@code T-Z}, …)</li>
 * </ul>
 */
public sealed interface Type permits
        Type.Primitive, Type.PrecisionDecimal,
        Type.ClassType, Type.EnumType,
        Type.TypeVar, Type.GenericType,
        Type.FunctionType, Type.RelationType,
        Type.SchemaAlgebra {

    /** Human-readable rendering, e.g. {@code "Integer"}, {@code "Decimal(38,2)"}, {@code "Relation<T>"}. */
    String typeName();

    // ====================================================================
    // Scalar leaves
    // ====================================================================

    /**
     * Built-in primitive &mdash; the kinded <em>recognition tag</em> (Roslyn
     * {@code SpecialType} / Calcite {@code SqlTypeName} shape). The FQN of each
     * constant is <strong>sourced from the corresponding {@link Pure} native
     * class</strong>, not re-typed, keeping {@code Pure} the single source of
     * truth for the strings.
     *
     * <p>The primitive <em>lattice</em> (Integer &lt; Number &lt; Any) is the
     * {@code extends} chain already declared in {@code Pure.java} and is walked
     * via {@code ModelContext.isSubtype}; it is intentionally <strong>not</strong>
     * re-encoded here. This tag exists only for cheap exhaustive dispatch and
     * {@link #family()} grouping (coercion / SQL-dialect mapping).
     */
    enum Primitive implements Type {
        NUMBER(Pure.NUMBER, Family.NUMERIC),
        INTEGER(Pure.INTEGER, Family.NUMERIC),
        FLOAT(Pure.FLOAT, Family.NUMERIC),
        DECIMAL(Pure.DECIMAL, Family.NUMERIC),
        STRING(Pure.STRING, Family.TEXT),
        BOOLEAN(Pure.BOOLEAN, Family.BOOLEAN),
        BYTE(Pure.BYTE, Family.BINARY),
        DATE(Pure.DATE, Family.TEMPORAL),
        STRICT_DATE(Pure.STRICT_DATE, Family.TEMPORAL),
        DATE_TIME(Pure.DATE_TIME, Family.TEMPORAL),
        LATEST_DATE(Pure.LATEST_DATE, Family.TEMPORAL),
        STRICT_TIME(Pure.STRICT_TIME, Family.TEMPORAL);

        /** Coarse type-family for coercion + dialect mapping (Calcite {@code SqlTypeFamily} role). */
        public enum Family { NUMERIC, TEXT, BOOLEAN, TEMPORAL, BINARY }

        private final String qualifiedName;
        private final Family family;

        Primitive(com.legend.parser.element.ClassDefinition source, Family family) {
            this.qualifiedName = source.qualifiedName();
            this.family = family;
        }

        /** Fully qualified name, e.g. {@code "meta::pure::metamodel::type::Integer"}. */
        public String qualifiedName() {
            return qualifiedName;
        }

        public Family family() {
            return family;
        }

        public boolean isNumeric() {
            return family == Family.NUMERIC;
        }

        public boolean isTemporal() {
            return family == Family.TEMPORAL;
        }

        @Override
        public String typeName() {
            return qualifiedName.contains("::")
                    ? qualifiedName.substring(qualifiedName.lastIndexOf("::") + 2)
                    : qualifiedName;
        }

        private static final Map<String, Primitive> BY_FQN;
        static {
            Map<String, Primitive> m = new LinkedHashMap<>();
            for (Primitive p : values()) m.put(p.qualifiedName, p);
            // meta::pure::precisePrimitives: width-annotated SUBTYPES of the
            // base primitives (real precisePrimitives.pure: 'Primitive Int
            // extends Integer'). Value semantics are the base's — the width
            // is storage annotation; a dedicated width-carrying Type can
            // replace this aliasing when SQL narrowing parity is built.
            String pp = "meta::pure::precisePrimitives::";
            for (String n : new String[]{"TinyInt", "UTinyInt", "SmallInt",
                    "USmallInt", "Int", "UInt", "BigInt", "UBigInt"}) {
                m.put(pp + n, INTEGER);
            }
            m.put(pp + "Float4", FLOAT);
            m.put(pp + "Double", FLOAT);
            BY_FQN = Map.copyOf(m);
        }

        /** Looks up a built-in primitive by FQN; empty if {@code fqn} is not a known primitive. */
        public static Optional<Primitive> findByFqn(String fqn) {
            return Optional.ofNullable(BY_FQN.get(fqn));
        }
    }

    /**
     * Decimal with explicit precision and scale: {@code DECIMAL(p, s)}.
     *
     * <p><strong>Subtyping is baked in here</strong> (subtype of
     * {@link Primitive#DECIMAL}, {@link Primitive#NUMBER}) so callers never
     * "normalize {@code PrecisionDecimal} &rarr; {@code DECIMAL}" before a
     * comparison &mdash; the explicit fix for the engine scar where the legacy
     * form "required 5 scattered normalization copies". Any normalization that
     * is unavoidable happens in the <em>one</em> subtype routine
     * ({@code ModelContext.isSubtype}), via {@link #basePrimitive()}.
     */
    record PrecisionDecimal(int precision, int scale) implements Type {

        public PrecisionDecimal {
            if (precision < 0) {
                throw new IllegalArgumentException("precision must be >= 0, got " + precision);
            }
            if (scale < 0 || scale > precision) {
                throw new IllegalArgumentException(
                        "scale must be in [0, precision], got scale=" + scale + ", precision=" + precision);
            }
        }

        /**
         * Default precision/scale for Pure's unparameterized {@code Decimal}.
         * {@code DECIMAL(38, 18)} &mdash; widest SQL decimal compatible with a
         * 128-bit backing integer (engine parity).
         */
        public static final PrecisionDecimal DEFAULT_DECIMAL = new PrecisionDecimal(38, 18);

        /** Widest SQL {@code DECIMAL} precision compatible with a 128-bit backing integer. */
        public static final int MAX_PRECISION = 38;

        /**
         * Floor scale preserved when clamping an overflowing precision &mdash;
         * Spark's {@code MINIMUM_ADJUSTED_SCALE}. Keeps at least 6 fractional
         * digits rather than losing them all to the integer part.
         */
        public static final int MIN_ADJUSTED_SCALE = 6;

        /** The scalar kind any precision-decimal collapses to for subtyping: {@link Primitive#DECIMAL}. */
        public Primitive basePrimitive() {
            return Primitive.DECIMAL;
        }

        // ----------------------------------------------------------------
        // Decimal arithmetic precision/scale derivation &mdash; the universal
        // MS-SQL &rarr; Hive &rarr; Spark &rarr; Calcite lineage (a LegendLite
        // datatype rule, owned here; no SQL imports). Every result is clamped
        // to {@link #MAX_PRECISION} via Spark's {@code adjustPrecisionScale}.
        // See docs/PHASE_G_SPEC_COMPILER.md §8.
        // ----------------------------------------------------------------

        /** Result type of {@code this + other}: prec {@code max(s1,s2)+max(p1-s1,p2-s2)+1}, scale {@code max(s1,s2)}. */
        public PrecisionDecimal plus(PrecisionDecimal other) {
            int p1 = precision, s1 = scale, p2 = other.precision, s2 = other.scale;
            int rScale = Math.max(s1, s2);
            int rPrec = Math.max(s1, s2) + Math.max(p1 - s1, p2 - s2) + 1;
            return adjust(rPrec, rScale);
        }

        /** Result type of {@code this - other} &mdash; identical derivation to {@link #plus}. */
        public PrecisionDecimal minus(PrecisionDecimal other) {
            return plus(other);
        }

        /** Result type of {@code this * other}: prec {@code p1+p2+1}, scale {@code s1+s2}. */
        public PrecisionDecimal times(PrecisionDecimal other) {
            return adjust(precision + other.precision + 1, scale + other.scale);
        }

        /** Result type of {@code this / other}: scale {@code max(6, s1+p2+1)}, prec {@code p1-s1+s2+scale}. */
        public PrecisionDecimal dividedBy(PrecisionDecimal other) {
            int p1 = precision, s1 = scale, p2 = other.precision, s2 = other.scale;
            int rScale = Math.max(MIN_ADJUSTED_SCALE, s1 + p2 + 1);
            int rPrec = p1 - s1 + s2 + rScale;
            return adjust(rPrec, rScale);
        }

        /**
         * Spark's {@code adjustPrecisionScale}: when a derived precision exceeds
         * {@link #MAX_PRECISION}, cap precision at 38 and set the scale to
         * {@code max(38 - integerDigits, min(scale, MIN_ADJUSTED_SCALE))}. So the
         * floor is {@code min(scale, 6)}, not a flat 6: an input scale already below
         * 6 is preserved, otherwise at least 6 fractional digits are kept. Worked:
         * {@code (49,15) -> (38,6)} (floor 6 wins); {@code (49,5) -> (38,5)} (the
         * original scale 5 is below the floor and is kept).
         */
        private static PrecisionDecimal adjust(int prec, int scale) {
            if (prec <= MAX_PRECISION) {
                return new PrecisionDecimal(prec, scale);
            }
            int intDigits = prec - scale;
            int reducedScale = Math.max(MAX_PRECISION - intDigits, Math.min(scale, MIN_ADJUSTED_SCALE));
            return new PrecisionDecimal(MAX_PRECISION, reducedScale);
        }

        @Override
        public String typeName() {
            return "Decimal(" + precision + "," + scale + ")";
        }
    }

    // ====================================================================
    // Nominal (FQN-only)
    // ====================================================================

    /** Reference to a (user or native) class by FQN. */
    record ClassType(String fqn) implements Type {
        public ClassType {
            Objects.requireNonNull(fqn, "fqn");
        }

        @Override
        public String typeName() {
            return fqn;
        }
    }

    /** Reference to an enumeration by FQN. */
    record EnumType(String fqn) implements Type {
        public EnumType {
            Objects.requireNonNull(fqn, "fqn");
        }

        @Override
        public String typeName() {
            return fqn;
        }
    }

    // ====================================================================
    // Variables & application (stdlib generics)
    // ====================================================================

    /** Generic type parameter, e.g. {@code T}, {@code V}, {@code U}. */
    record TypeVar(String name) implements Type {
        public TypeVar {
            Objects.requireNonNull(name, "name");
        }

        @Override
        public String typeName() {
            return name;
        }
    }

    /**
     * Generic application: {@code rawFqn<arg, ...>}, e.g. {@code Relation<T>},
     * {@code List<T>}, {@code Function<F>}, {@code ColSpec<T>}. {@code rawFqn}
     * points at the parameterized native class (e.g. {@code Pure.RELATION}).
     */
    record GenericType(String rawFqn, List<Type> arguments) implements Type {
        public GenericType {
            Objects.requireNonNull(rawFqn, "rawFqn");
            Objects.requireNonNull(arguments, "arguments");
            arguments = List.copyOf(arguments);
        }

        @Override
        public String typeName() {
            String simple = rawFqn.contains("::")
                    ? rawFqn.substring(rawFqn.lastIndexOf("::") + 2)
                    : rawFqn;
            return simple + "<"
                    + arguments.stream().map(Type::typeName).collect(Collectors.joining(", "))
                    + ">";
        }
    }

    // ====================================================================
    // Structural
    // ====================================================================

    /** Function type: {@code {ParamType[mult], ... -> ResultType[mult]}}. */
    record FunctionType(List<Param> params, Param result) implements Type {
        public FunctionType {
            Objects.requireNonNull(params, "params");
            Objects.requireNonNull(result, "result");
            params = List.copyOf(params);
        }

        @Override
        public String typeName() {
            return "{"
                    + params.stream().map(Param::text).collect(Collectors.joining(", "))
                    + " -> " + result.text() + "}";
        }
    }

    /**
     * Inline relation type literal: {@code (col:Type[mult], ...)}.
     *
     * <p>{@code dynamicColumns} (engine-lite's {@code DynamicPivotColumn}; not in the
     * Pure metamodel) are a pivot output's aggregate TEMPLATES: the pivoted columns
     * are data-dependent (one per distinct pivot value, named
     * {@code <value>__|__<template-name>}), so they cannot appear in {@link #columns()},
     * but every one of them carries its template's type. Empty everywhere except a
     * pivot's schema; checkers read {@link #columns()} only — the templates are
     * consumed at the execution boundary, where the data-derived names first exist.
     */
    record RelationType(List<Column> columns, List<Column> dynamicColumns) implements Type {

        /** Separator between a pivoted data value and its aggregate-template name. */
        public static final String PIVOT_SEPARATOR = "__|__";

        public RelationType(List<Column> columns) {
            this(columns, List.of());
        }

        public RelationType {
            Objects.requireNonNull(columns, "columns");
            Objects.requireNonNull(dynamicColumns, "dynamicColumns");
            columns = List.copyOf(columns);
            dynamicColumns = List.copyOf(dynamicColumns);
            // Column names are unique BY CONSTRUCTION (real legend-pure errors on
            // duplicates; engine-lite's map-keyed schema silently last-wins — both
            // classes of silent wrongness become unrepresentable here). Checker-level
            // sites (schema UNION, colspec arrays) pre-check for friendlier messages.
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (Column c : columns) {
                if (!seen.add(c.name())) {
                    throw new IllegalArgumentException(
                            "duplicate column '" + c.name() + "' in relation type");
                }
            }
            java.util.Set<String> dynSeen = new java.util.HashSet<>();
            for (Column c : dynamicColumns) {
                if (!dynSeen.add(c.name())) {
                    throw new IllegalArgumentException(
                            "duplicate dynamic (pivot template) column '" + c.name() + "'");
                }
            }
        }

        @Override
        public String typeName() {
            return "("
                    + columns.stream().map(Column::text).collect(Collectors.joining(", "))
                    + ")";
        }
    }

    // ====================================================================
    // Schema algebra
    // ====================================================================

    /** Schema-algebra operation on type expressions: {@code T+V}, {@code T-Z}, {@code Z⊆T}, {@code Z=K}. */
    record SchemaAlgebra(Type left, Op op, Type right) implements Type {
        public SchemaAlgebra {
            Objects.requireNonNull(left, "left");
            Objects.requireNonNull(op, "op");
            Objects.requireNonNull(right, "right");
        }

        @Override
        public String typeName() {
            return left.typeName() + op.symbol() + right.typeName();
        }
    }

    /** Schema-algebra operator. Names match engine's {@code Type.SchemaAlgebra.Op}. */
    enum Op {
        EQUAL("="), UNION("+"), DIFFERENCE("-"), SUBSET("\u2286");

        private final String symbol;

        Op(String symbol) {
            this.symbol = symbol;
        }

        public String symbol() {
            return symbol;
        }
    }

    // ====================================================================
    // Shared sub-records
    // ====================================================================

    /** A type with its multiplicity, used inside {@link FunctionType}. */
    record Param(Type type, Multiplicity multiplicity) {
        public Param {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(multiplicity, "multiplicity");
        }

        /** Rendering, e.g. {@code Integer[1]}. */
        public String text() {
            return type.typeName() + multiplicity.text();
        }
    }

    /** A column in a {@link RelationType}: name + type + multiplicity. */
    record Column(String name, Type type, Multiplicity multiplicity) {
        public Column {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(multiplicity, "multiplicity");
        }

        /** Rendering, e.g. {@code price:Decimal(38,2)[1]}. */
        public String text() {
            return name + ":" + type.typeName() + multiplicity.text();
        }
    }
}
