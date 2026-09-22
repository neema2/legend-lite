package com.legend.compiler.element.type;

import com.legend.builtin.Pure;
import com.legend.sql.SqlType;

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
 * <h2>Relationship to {@link com.legend.protocol.TypeExpression}</h2>
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

        Primitive(com.legend.model.ClassDefinition source, Family family) {
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
    record GenericType(String rawFqn, List<Type> arguments,
            List<Multiplicity> multArguments) implements Type {
        public GenericType {
            Objects.requireNonNull(rawFqn, "rawFqn");
            Objects.requireNonNull(arguments, "arguments");
            Objects.requireNonNull(multArguments, "multArguments");
            arguments = List.copyOf(arguments);
            multArguments = List.copyOf(multArguments);
        }

        /** The overwhelmingly common shape — no multiplicity
         * arguments (engine parity: only Result&lt;T|m&gt;-style
         * classes carry them; every existing construction site keeps
         * this arity). */
        public GenericType(String rawFqn, List<Type> arguments) {
            this(rawFqn, arguments, List.of());
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
    // ====================================================================
    // THE RELATION REPRESENTATION (Row-vs-Relation, reference-faithful)
    // ====================================================================
    // Real pure's own spelling, adopted VERBATIM (Row-vs-Relation split,
    // the successor arc of STAMP_DISCIPLINE_PROGRAM): a TABLE value's
    // type is {@code GenericType(Relation, [schema])} — the signature
    // form {@code Relation<(name:String[1])>} — preserved through
    // resolution (the historical G-α erasure to a bare struct is
    // DELETED, not inverted). A bare {@link RelationType} is the SCHEMA
    // STRUCT, and a schema-typed VALUE is ONE ROW — pure's own pun: the
    // {@code T} of {@code Relation<T>} is the schema AND the row type
    // ({@code lead<T>(w:Relation<T>[1], r:T[1]):T[0..1]} — container vs
    // element, declared in every relation signature). With the wrapper
    // preserved, "am I holding a row or a table?" is read off the TYPE.

    /** THE table-type mint: {@code Relation<schema>}, pure's spelling. */
    static GenericType relation(RelationType schema) {
        return new GenericType(com.legend.compiler.element.type.PlatformTypes.RELATION, List.of(schema));
    }

    /** Whether {@code t} is a relation (table) type — the wrapped form,
     * whatever the argument's resolution state (a {@code Relation<T>}
     * with an unsolved {@code T} is still a table type). */
    static boolean isRelation(Type t) {
        return t instanceof GenericType g
                && com.legend.compiler.element.type.PlatformTypes.RELATION_CARRIERS.contains(g.rawFqn())
                && g.arguments().size() == 1;
    }

    /** The CLASS a value of type {@code t} is an instance of: a
     * {@link ClassType}'s fqn, or a PARAMETERIZED class's raw fqn — the
     * spec declares its metaclass-typed properties generic
     * ({@code SetImplementation.class : Class<Any>[1]},
     * {@code PropertyMapping.property : Property<Nil,Any|*>[1]}, mapping
     * leg B) and such a value IS a row of the raw class, exactly as a bare
     * {@code Class[1]} was. Null for every other type, the two carrier
     * families included: a {@code Relation<T>} is a table, a function
     * carrier is a lambda — neither is a row. */
    static @com.legend.base.Nullable String classFqn(Type t) {
        return switch (t) {
            case ClassType c -> c.fqn();
            case GenericType g when !isRelation(g)
                    && !com.legend.compiler.element.type.PlatformTypes.isValueCarrier(g)
                    -> g.rawFqn();
            default -> null;
        };
    }

    /** {@link #classFqn} as a {@link ClassType}: the bare type itself, a
     * parameterized class RAW — THE store resolver's reading of a value's
     * type ("is this a row, and of which class"): the resolver works on
     * raw classes, type arguments are the kernel's business. Null when
     * the value is not a row (see classFqn). */
    static @com.legend.base.Nullable ClassType asClassType(Type t) {
        String fqn = classFqn(t);
        return fqn == null ? null : t instanceof ClassType c ? c : new ClassType(fqn);
    }

    /** The schema of a TABLE type ({@code Relation<schema>}), or null if
     * {@code t} is not a resolved table type. THE "is this a table?"
     * reader — a bare {@link RelationType} is a schema/row, never a
     * table, and returns null here. */
    static @com.legend.base.Nullable RelationType relationSchema(Type t) {
        return t instanceof GenericType g
                && com.legend.compiler.element.type.PlatformTypes.RELATION_CARRIERS.contains(g.rawFqn())
                && g.arguments().size() == 1
                && g.arguments().get(0) instanceof RelationType r ? r : null;
    }

    /** The schema VIEW of a relation-ish type: a wrapped table yields its
     * schema, a bare struct (schema literal / row value) yields itself.
     * For signature-tolerant consumers (colspec rows, declared struct
     * params); table-only readers use {@link #relationSchema}. */
    static @com.legend.base.Nullable RelationType schemaView(Type t) {
        RelationType wrapped = relationSchema(t);
        if (wrapped != null) {
            return wrapped;
        }
        return t instanceof RelationType r ? r : null;
    }

    /** A RELATION-ROOTED value BY TYPE: a table (wrapped
     * {@code Relation<T>}), or a ROW COLLECTION — a bare struct with a
     * many stamp, the {@code .rows} view (engine: {@code TDSRow[*]}).
     * A bare struct with an at-most-one stamp is ONE row and is NOT
     * relation-rooted. No tree walking — the type and stamp decide. */
    static boolean relationValued(ExprType info) {
        return isRelation(info.type())
                || (info.type() instanceof RelationType
                        && info.multiplicity().isMany());
    }

    /** The schema of a node KNOWN to be a table (the resolver/lowering
     * pipeline-cast idiom) — loud on anything else, a bare struct
     * included: a bare struct is a ROW, and a pipeline typed bare is a
     * missed mint, not a table. */
    static RelationType requireRelationSchema(Type t) {
        RelationType schema = relationSchema(t);
        if (schema == null) {
            throw new IllegalStateException("expected a table type"
                    + " (Relation<schema>), got " + t.typeName());
        }
        return schema;
    }

    record RelationType(List<Column> columns, List<Column> dynamicColumns) implements Type {

        /** Separator between a pivoted data value and its aggregate-template name. */
        public static final String PIVOT_SEPARATOR = "__|__";

        /** The LATE-BOUND schema wildcard (One-Platform Plan Phase 1c):
         * a raw-SQL grid's columns first exist at execution — the
         * dynamic-pivot rule ({@link #dynamicColumns()} carry names the
         * static schema cannot enumerate). One template named {@code *}
         * typed {@code Any[0..1]} marks the WHOLE schema late-bound:
         * by-name reads trust their name (the pivot claim-any rule),
         * and the execution boundary stamps the real columns before
         * lowering. */
        public static final String LATE_BOUND_WILDCARD = "*";

        /** A relation whose columns are late-bound (raw-SQL grids). */
        public static RelationType lateBound() {
            return new RelationType(java.util.List.of(), java.util.List.of(
                    new Column(LATE_BOUND_WILDCARD,
                            new ClassType(PlatformTypes.ANY),
                            Multiplicity.Bounded.ZERO_ONE)));
        }

        /** True iff this schema is the late-bound wildcard (columns
         * unknown until the execution boundary stamps them). */
        public boolean isLateBound() {
            return columns().isEmpty() && dynamicColumns().size() == 1
                    && dynamicColumns().get(0).name()
                            .equals(LATE_BOUND_WILDCARD);
        }

        /** THE PIVOT-COLUMN MATCHING RULE (one owner — the exec egress
         * and the lowering's deferred-TDS resolver both consume it): a
         * statically known name matches by NAME; a
         * {@code <value>__|__<template>} name inherits its aggregate
         * TEMPLATE's type; a suffixed name matching NO template while
         * templates are present is a naming-contract bug — loud, never
         * guessed. Null = no static/template match (each caller owns
         * its fallback: the egress decodes the SQL type, the deferred
         * resolver walls). */
        /** ENGINE-VERBATIM presentation (pureToSQLQuery.pure:2985
         * mayQuotePivotColNames): a column name containing the pivot
         * separator that is not already quote-wrapped presents WITH
         * literal single quotes as part of the NAME — the physical SQL
         * column stays bare (Fold.pivotIdentity is the reference-side
         * inverse). */
        public static String presentPivotName(String physical) {
            return physical.contains(PIVOT_SEPARATOR)
                    && !(physical.startsWith("'") && physical.endsWith("'"))
                    ? "'" + physical + "'"
                    : physical;
        }

        public @com.legend.base.Nullable Type pivotColumnType(String rawName) {
            // quote-tolerant: the PRESENTED name carries literal quotes
            // (presentPivotName); matching runs on the bare spelling
            String name = rawName.length() >= 2 && rawName.startsWith("'")
                    && rawName.endsWith("'")
                    && rawName.contains(PIVOT_SEPARATOR)
                    ? rawName.substring(1, rawName.length() - 1)
                    : rawName;
            var byName = columns().stream()
                    .filter(c -> c.name().equals(name)
                            || c.name().equals(rawName)).findFirst();
            if (byName.isPresent()) {
                return byName.get().type();
            }
            int sep = name.lastIndexOf(PIVOT_SEPARATOR);
            if (sep >= 0 && !dynamicColumns().isEmpty()) {
                String template = name.substring(
                        sep + PIVOT_SEPARATOR.length());
                return dynamicColumns().stream()
                        .filter(c -> c.name().equals(template)).findFirst()
                        .map(Column::type)
                        .orElseThrow(() -> new IllegalStateException(
                                "pivot column '" + name + "' matches no"
                                + " aggregate template "
                                + dynamicColumns().stream()
                                        .map(Column::name).toList()));
            }
            return null;
        }

        /** THE TRUST-NAME RULE (the one place it is defined): a by-name
         * read over a late-bound schema is trusted — typed
         * {@code Any[0..1]}, resolved by the database (pivot's claim-any
         * rule). Callers gate on {@link #isLateBound()}. */
        public static Column trustedColumn(String name) {
            return new Column(name, new ClassType(PlatformTypes.ANY),
                    Multiplicity.Bounded.ZERO_ONE);
        }

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

    /** {@code dataTypeTransformer}'s identity arm: a declaration that leaves
     * the cell's kind to the WIRE — String, unrefined Number, Any (a numeric,
     * Boolean or temporal declaration converts the wire cell instead). */
    static boolean wireDecided(Type declared) {
        return declared == Primitive.STRING || declared == Primitive.NUMBER
                || (declared instanceof ClassType ct && PlatformTypes.isAny(ct));
    }

    /** The pure kind a wire (SQL) type spells — a WIRE fact read off a
     * planned output or a value-built layout, never a stamp echo. */
    public static @com.legend.base.Nullable Type kindOfSqlType(SqlType t) {
        if (t == SqlType.Scalar.BIGINT || t == SqlType.Scalar.INTEGER
                || t == SqlType.Scalar.HUGEINT) {
            return Type.Primitive.INTEGER;
        }
        if (t == SqlType.Scalar.DOUBLE) {
            return Type.Primitive.FLOAT;
        }
        if (t == SqlType.Scalar.BOOLEAN) {
            return Type.Primitive.BOOLEAN;
        }
        if (t == SqlType.Scalar.VARCHAR) {
            return Type.Primitive.STRING;
        }
        if (t instanceof SqlType.Decimal) {
            return Type.Primitive.DECIMAL;
        }
        if (t == SqlType.Scalar.DATE) {
            return Type.Primitive.STRICT_DATE;
        }
        if (t == SqlType.Scalar.TEMPORAL_TEXT) {
            // the precision-faithful temporal-text carrier (partials,
            // written subsecond digits): a temporal value, kind by its
            // declaration (the literal spells every temporal %-prefixed)
            return Type.Primitive.DATE;
        }
        if (t == SqlType.Scalar.TIMESTAMP || t == SqlType.Scalar.TIMESTAMPTZ) {
            // a DateTime literal with its +0000 lowers time-zoned — the
            // same DateTime kind (the 4 `[%2016-…+0000, …]` peers)
            return Type.Primitive.DATE_TIME;
        }
        return null;
    }
}
