package com.gs.legend.model.m3;

import com.gs.legend.model.SymbolTable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Pure primitive type (Integer, String, Boolean, Date, etc.) — both the runtime
 * declaration AND the {@link Type} variant. Primitives are always available
 * (loaded at JVM init as static final singletons on this class) and inherently
 * lightweight — their entire definition is an FQN, simple name, direct parent FQNs,
 * and three classification flags — so a single record serves both roles. User-defined
 * classes and enums, in contrast, keep separate declarations ({@link PureClass} /
 * {@link PureEnum}) and lightweight Type-layer references ({@code Type.ClassType} /
 * {@code Type.EnumType}) to preserve lazy / cross-project loading.
 *
 * <p>Each built-in primitive is a singleton exposed as a public static final field
 * on this class ({@link #INTEGER}, {@link #STRING}, etc.). Identity is FQN-based;
 * two {@code Primitive} instances are equal iff their {@link #qualifiedName} matches,
 * and the singletons are the only instances constructed in normal operation, so
 * reference equality ({@code ==}) also works for the built-ins.
 *
 * @param qualifiedName  Fully qualified name (e.g., {@code "meta::pure::metamodel::type::Integer"}).
 *                       Named for symmetry with {@code ClassDefinition.qualifiedName()} and
 *                       {@code EnumDefinition.qualifiedName()}, even though {@code Primitive}
 *                       lives on the runtime layer and doesn't implement {@code PackageableElement}.
 * @param pureName       Simple Pure-level display name (e.g., {@code "Integer"}).
 * @param superTypeFqns  FQNs of direct supertypes (e.g., Integer → {@code ["...Number"]}).
 *                       Empty list for roots ({@link #ANY}, {@link #NIL}).
 * @param numeric        {@code true} for Number / Integer / Int64 / Int128 / Float / Decimal.
 * @param temporal       {@code true} for Date / StrictDate / DateTime / StrictTime.
 * @param date           {@code true} for Date / StrictDate / DateTime (subset of temporal).
 */
public record Primitive(
        String qualifiedName,
        String pureName,
        List<String> superTypeFqns,
        boolean numeric,
        boolean temporal,
        boolean date) implements Type {

    public Primitive {
        Objects.requireNonNull(qualifiedName, "Primitive qualifiedName cannot be null");
        Objects.requireNonNull(pureName, "Primitive pureName cannot be null");
        superTypeFqns = superTypeFqns == null ? List.of() : List.copyOf(superTypeFqns);
    }

    // ============================================================
    //  Singleton built-in primitives
    //
    //  Declaration order matters: each singleton's superTypeFqns references parents
    //  already declared above. All 15 singletons are interned — reference equality
    //  works for the built-ins.
    //
    //  Lattice:
    //    ANY
    //    ├── NUMBER
    //    │   ├── INTEGER
    //    │   │   ├── INT64
    //    │   │   └── INT128
    //    │   ├── FLOAT
    //    │   └── DECIMAL   (parameterized form: Type.PrecisionDecimal)
    //    ├── STRING
    //    ├── BOOLEAN
    //    ├── DATE
    //    │   ├── STRICT_DATE
    //    │   └── DATE_TIME
    //    ├── STRICT_TIME
    //    └── VARIANT (a.k.a. JSON)
    //    NIL (bottom — subtype of every type)
    // ============================================================

    private static final String TYPE_PKG    = "meta::pure::metamodel::type";
    private static final String VARIANT_PKG = "meta::pure::metamodel::variant";

    public static final Primitive ANY         = new Primitive(TYPE_PKG    + "::Any",        "Any",        List.of(),                           false, false, false);
    public static final Primitive NIL         = new Primitive(TYPE_PKG    + "::Nil",        "Nil",        List.of(ANY.qualifiedName),          false, false, false);
    public static final Primitive NUMBER      = new Primitive(TYPE_PKG    + "::Number",     "Number",     List.of(ANY.qualifiedName),          true,  false, false);
    public static final Primitive INTEGER     = new Primitive(TYPE_PKG    + "::Integer",    "Integer",    List.of(NUMBER.qualifiedName),       true,  false, false);
    public static final Primitive INT64       = new Primitive(TYPE_PKG    + "::Int64",      "Integer",    List.of(INTEGER.qualifiedName),      true,  false, false);
    public static final Primitive INT128      = new Primitive(TYPE_PKG    + "::Int128",     "Integer",    List.of(INTEGER.qualifiedName),      true,  false, false);
    public static final Primitive FLOAT       = new Primitive(TYPE_PKG    + "::Float",      "Float",      List.of(NUMBER.qualifiedName),       true,  false, false);
    public static final Primitive DECIMAL     = new Primitive(TYPE_PKG    + "::Decimal",    "Decimal",    List.of(NUMBER.qualifiedName),       true,  false, false);
    public static final Primitive STRING      = new Primitive(TYPE_PKG    + "::String",     "String",     List.of(ANY.qualifiedName),          false, false, false);
    public static final Primitive BOOLEAN     = new Primitive(TYPE_PKG    + "::Boolean",    "Boolean",    List.of(ANY.qualifiedName),          false, false, false);
    public static final Primitive DATE        = new Primitive(TYPE_PKG    + "::Date",       "Date",       List.of(ANY.qualifiedName),          false, true,  true);
    public static final Primitive STRICT_DATE = new Primitive(TYPE_PKG    + "::StrictDate", "StrictDate", List.of(DATE.qualifiedName),         false, true,  true);
    public static final Primitive DATE_TIME   = new Primitive(TYPE_PKG    + "::DateTime",   "DateTime",   List.of(DATE.qualifiedName),         false, true,  true);
    public static final Primitive STRICT_TIME = new Primitive(TYPE_PKG    + "::StrictTime", "StrictTime", List.of(ANY.qualifiedName),          false, true,  false);
    public static final Primitive VARIANT     = new Primitive(VARIANT_PKG + "::Variant",    "JSON",       List.of(ANY.qualifiedName),          false, false, false);

    /** All built-in primitives in lattice order (roots first; children follow their parents). */
    public static final List<Primitive> ALL = List.of(
            ANY, NIL, NUMBER, INTEGER, INT64, INT128, FLOAT, DECIMAL,
            STRING, BOOLEAN, DATE, STRICT_DATE, DATE_TIME, STRICT_TIME, VARIANT);

    /** FQN → Primitive lookup. */
    private static final Map<String, Primitive> BY_FQN;
    static {
        Map<String, Primitive> m = new LinkedHashMap<>();
        for (Primitive p : ALL) m.put(p.qualifiedName, p);
        BY_FQN = Map.copyOf(m);
    }

    /**
     * Looks up a built-in primitive by fully qualified name. Returns empty if the FQN is
     * not a known Pure primitive. FQN-only — simple-name resolution is the caller's job
     * (via {@code ImportScope}).
     */
    public static Optional<Primitive> findByFqn(String fqn) {
        return Optional.ofNullable(BY_FQN.get(fqn));
    }

    // ============================================================
    //  Instance methods
    // ============================================================

    /** Simple name extracted from the qualified name (e.g., "Integer" from "meta::...::Integer"). */
    public String simpleName() {
        return SymbolTable.extractSimpleName(qualifiedName);
    }

    /** {@inheritDoc} Returns the Pure-level name (e.g., {@code "Integer"}). */
    @Override
    public String typeName() {
        return pureName;
    }

    /** Alias for {@link #numeric()} — matches legacy API naming. */
    public boolean isNumeric() {
        return numeric;
    }

    /** Alias for {@link #temporal()} — matches legacy API naming. */
    public boolean isTemporal() {
        return temporal;
    }

    /** Alias for {@link #date()} — matches legacy API naming. */
    public boolean isDate() {
        return date;
    }

    /** {@code true} for {@link #INTEGER}, {@link #INT64}, {@link #INT128}. */
    public boolean isInteger() {
        return this == INTEGER || this == INT64 || this == INT128;
    }

    /**
     * Direct parent in the primitive hierarchy — the first element of
     * {@link #superTypeFqns}, resolved against the built-in catalog. {@link #ANY} has
     * no parent and returns itself.
     */
    public Primitive parent() {
        if (superTypeFqns.isEmpty()) return this;  // ANY
        return BY_FQN.get(superTypeFqns.get(0));
    }

    /**
     * Lowest common ancestor of two primitive types. Walks {@code a}'s parent chain
     * until {@code b.isSubtypeOf(cursor)} — returns that ancestor, or {@link #ANY} if
     * none found.
     */
    public static Primitive commonSupertype(Primitive a, Primitive b) {
        if (a.equals(b)) return a;
        if (a.isSubtypeOf(b)) return b;
        if (b.isSubtypeOf(a)) return a;
        Primitive cursor = a;
        while (cursor != ANY) {
            cursor = cursor.parent();
            if (b.isSubtypeOf(cursor)) return cursor;
        }
        return ANY;
    }

    /**
     * {@code true} if this primitive is a subtype of (or equal to) {@code other}.
     *
     * <p>Primitive-specific rules:
     * <ul>
     *   <li>A primitive is always a subtype of itself (FQN equality).</li>
     *   <li>{@link #NIL} is a subtype of every type (bottom).</li>
     *   <li>For another {@link Primitive}, walks {@link #superTypeFqns} recursively —
     *       short chains (max depth 3) so no precomputed cache needed.</li>
     *   <li>Against non-primitive Types, falls through to the {@link Type#isSubtypeOf}
     *       default (equality or {@code ANY}-supertype check).</li>
     * </ul>
     */
    @Override
    public boolean isSubtypeOf(Type other) {
        if (this.equals(other)) return true;
        if (this == NIL) return true;
        if (other instanceof Primitive op) {
            if (op == ANY) return true;
            for (String parentFqn : superTypeFqns) {
                Primitive parent = BY_FQN.get(parentFqn);
                if (parent != null && parent.isSubtypeOf(op)) return true;
            }
            return false;
        }
        return Type.super.isSubtypeOf(other);
    }
}
