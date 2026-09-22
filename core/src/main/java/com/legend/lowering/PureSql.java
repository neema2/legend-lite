package com.legend.lowering;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlType;
import java.util.List;
/**
 * THE Pure→SQL type boundary (LEGEND_SQL_VISION.md): the one place Pure's
 * type system meets the SQL layer's. Pure Integer is 64-bit → BIGINT is
 * decided HERE, by the frontend — not by any dialect.
 */
final class PureSql {

    private PureSql() {
    }

    /** The raise emission with source provenance: {@code ERROR(message[,
     * 'line:col'])}. Interpreted pure's {@code AssertError} hands the matcher
     * the RAISING expression's source info; here the raiser is SQL, so the
     * position (the call-name token — the parser's named-call span) rides the
     * ERROR call as a literal second arg. Renderers thread it INSIDE the
     * U+001F provenance envelope behind a U+001E divider and
     * {@code RaisedErrors} parses it back off — production text stays clean
     * (the funnel strips the whole envelope). A synthesized call (no span)
     * raises position-free, exactly as before. */
    static com.legend.sql.SqlExpr raise(com.legend.sql.SqlExpr message,
            com.legend.protocol.@com.legend.base.Nullable SourceInfo pos) {
        return pos == null
                ? com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR, message)
                : com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR, message,
                        new com.legend.sql.SqlExpr.StringLit(
                                pos.startLine() + ":" + pos.startColumn()));
    }

    /**
     * One list element as TEXT (makeString): JSON-carried elements
     * extract through the dialect's unquoting cast —
     * Cast(VARIANT_GET(x,'$')), which DuckDB renders {@code (x ->> '$')}
     * — so a String element prints WITHOUT its JSON quotes; plain
     * scalars cast directly. The JSON decision follows the LOWERED shape
     * (an ArrayLit of TO_VARIANT elements = the heterogeneous-literal
     * wrap; a typed Variant value), never the pure type alone: row-cell
     * collections ($r.values) type Any too but lower as PLAIN lists,
     * and unquoting those dies on non-JSON values.
     */
    static com.legend.sql.SqlExpr elementText(
            com.legend.compiler.spec.typed.TypedSpec listArg,
            com.legend.sql.SqlExpr loweredList,
            com.legend.sql.SqlExpr elem) {
        // a LITERAL-carried list (M4 re-land, the §2R residual-row cure:
        // testSqlRealiasViews/testViewAll/testViewSimpleFilter): each
        // element's text is the spelling->PRINT projection — quotes
        // unwrap, % strips — via the ONE grammar owner; the stored type
        // fact is the decision
        if (loweredList.type() instanceof com.legend.sql.TypeFact.Typed lt
                && lt.type() instanceof com.legend.sql.SqlType.Array la
                && la.element() == SqlType.Scalar.LITERAL) {
            return LiteralSpelling.printForm(elem);
        }
        boolean json = (loweredList instanceof com.legend.sql.SqlExpr.ArrayLit al
                        && !al.elements().isEmpty()
                        && al.elements().stream().allMatch(
                                MixedEncoding::variantWrapped))
                || (listArg.info().type() instanceof Type.ClassType ct
                        && PlatformTypes.isVariant(ct));
        return json
                ? new com.legend.sql.SqlExpr.Cast(
                        com.legend.sql.SqlExpr.Call.of(
                                com.legend.sql.SqlFn.VARIANT_GET, elem,
                                new com.legend.sql.SqlExpr.StringLit("$")),
                        SqlType.Scalar.VARCHAR)
                : new com.legend.sql.SqlExpr.Cast(elem, SqlType.Scalar.VARCHAR);
    }

    /**
     * EXHAUSTIVE over sealed {@link Type} and the {@code Primitive} enum — no
     * default arms (root package-info invariant): a new Pure type demands an
     * explicit decision here, at compile time. Unsupported kinds throw with
     * their own arm so the message names the exact kind.
     */
    /** THE primitive-carrier table — the ONE owner of "which Pure
     * primitives have an SQL carrier, and which" ({@code type()}
     * throws where this is null; {@code carrierOrNull} passes the
     * null through — the audit's two-owners fix). */
    private static @com.legend.base.Nullable SqlType primitiveCarrier(
            Type.Primitive p) {
        return switch (p) {
            case STRING -> SqlType.Scalar.VARCHAR;
            case INTEGER -> SqlType.Scalar.BIGINT;
            case FLOAT, NUMBER -> SqlType.Scalar.DOUBLE;
            case BOOLEAN -> SqlType.Scalar.BOOLEAN;
            case DECIMAL -> new SqlType.Decimal(38, 18);
            case STRICT_DATE -> SqlType.Scalar.DATE;
            // %latest IS the fixed engine sentinel timestamp
            // ('9999-12-31 00:00:00.0000', Lowerer's value fold) —
            // its SQL kind is TIMESTAMP wherever a type is demanded
            // (the toSQLString re-render path types projections
            // BEFORE values fold; milestoning PREDICATES never get
            // here — TemporalFrame owns them).
            // F5.4: STRICT_DATE carries its kind as SQL DATE (the
            // driver returns java.sql.Date — the fact rides the
            // COLUMN type); an ABSTRACT Date slot rides TIMESTAMP
            // and decodes as a DateTime, faithfully — the Executor's
            // midnight-narrowing compensation is DELETED BY PROOF
            // (zero firings across the DuckDB corpus, the H2 corpus,
            // and the full PCT suite).
            case DATE_TIME, DATE, LATEST_DATE -> SqlType.Scalar.TIMESTAMP;
            case BYTE, STRICT_TIME -> null;
        };
    }

    static SqlType type(Type t) {
        return switch (t) {
            case Type.Primitive p -> {
                SqlType c = primitiveCarrier(p);
                if (c == null) {
                    throw new IllegalStateException(
                            "no SQL type for Pure primitive " + p
                            + " at the lowering boundary");
                }
                yield c;
            }
            case Type.PrecisionDecimal d -> new SqlType.Decimal(d.precision(), d.scale());
            case Type.ClassType ct -> {
                if (PlatformTypes.isVariant(ct)) {
                    yield SqlType.Scalar.JSON;
                }
                if (PlatformTypes.isAny(ct)) {
                    // Any = a heterogeneous VALUE position ([1,'a'] roots):
                    // the carrier is variant JSON — the one SQL type that
                    // keeps each element's own runtime kind (engine-lite's
                    // VARIANT-wrapping; the real engine refuses: 'Any is not
                    // managed yet!' and excludes these from relational PCT).
                    yield SqlType.Scalar.JSON;
                }
                if (PlatformTypes.isNil(ct)) {
                    // Nil is the BOTTOM type — it types only []-born values, whose
                    // sole inhabitant is emptiness. The cell is always SQL NULL;
                    // VARCHAR is the carrier of an always-null column.
                    yield SqlType.Scalar.VARCHAR;
                }
                throw new IllegalStateException("no SQL type for Pure class "
                        + ct.fqn() + " at the lowering boundary (class values do not"
                        + " reach SQL until Phase H lowers their sources)");
            }
            case Type.EnumType e -> SqlType.Scalar.VARCHAR;
            case Type.TypeVar v -> throw new IllegalStateException(
                    "unresolved type variable " + v.typeName() + " reached the lowering boundary");
            case Type.GenericType g -> {
                // List<T> is the platform's list CARRIER — an SQL array of
                // the element type (real pure's list() over slice/etc.).
                if (PlatformTypes.isListCarrier(g)) {
                    yield new SqlType.Array(type(g.arguments().get(0)));
                }
                // Pair<U,V> travels as STRUCT(first, second); Map<U,V> as MAP.
                if (PlatformTypes.isPairCarrier(g)) {
                    yield new SqlType.Struct(List.of(
                            new SqlType.Struct.Field("first", type(g.arguments().get(0))),
                            new SqlType.Struct.Field("second", type(g.arguments().get(1)))));
                }
                if (PlatformTypes.isMapCarrier(g)) {
                    yield new SqlType.Map(type(g.arguments().get(0)),
                            type(g.arguments().get(1)));
                }
                // Class<X> VALUES travel as canonical simple-name strings
                // (the type() fold / columnsMeta convention) — the scalar
                // arm renders them as StringLit, so the wire type is text
                if (g.rawFqn().equals(
                        com.legend.compiler.element.type.PlatformTypes.CLASS_METACLASS)) {
                    yield SqlType.Scalar.VARCHAR;
                }
                throw new IllegalStateException(
                        "no SQL type for generic " + g.typeName() + " at the lowering boundary");
            }
            case Type.FunctionType f -> throw new IllegalStateException(
                    "a function value has no SQL type (" + f.typeName() + ")");
            // a bare struct is a ROW value — no bare SQL carrier yet
            // (the split's lowering leg): loud, per this switch's own
            // contract. A TABLE (wrapped Relation<T>) lands in the
            // GenericType arm's loud fallback — a relation is a SOURCE,
            // not a scalar SQL type.
            case Type.RelationType r -> throw new com.legend.error
                    .NotImplementedException("a row value has no SQL"
                    + " carrier yet (Row-vs-Relation split): "
                    + r.typeName());
            case Type.SchemaAlgebra a -> throw new IllegalStateException(
                    "unresolved schema algebra " + a.typeName() + " reached the lowering boundary");
        };
    }

    /** A value in LIST position: the STAMP decides the wrap (a
     * non-many value — designed one-value carriers included — wraps as
     * one element; NULL = empty passes). Moved from the dissolved
     * ListShapes. */
    static com.legend.sql.SqlExpr asList(com.legend.sql.SqlExpr e,
            boolean many) {
        return many || e instanceof com.legend.sql.SqlExpr.NullLit
                ? e : new com.legend.sql.SqlExpr.ArrayLit(
                        java.util.List.of(e));
    }

    /** THE CONFORM-BY-EMISSION LIST DOOR (§4bZ-U leg 2): a
     * list-position value whose stored fact is UNKNOWN or the NULL
     * value CASTS to the array of its PURE element type — the rule
     * site holds the type, the emission conforms, and the empty/NULL
     * carriers then bind and type through every list op. Typed values
     * (variant JSON, LITERAL carriers included) pass untouched — their
     * list-ness is the carrier's own contract; an element kind with no
     * SQL carrier stays honestly unwrapped. */
    static com.legend.sql.SqlExpr typedList(com.legend.sql.SqlExpr list,
            Type elemPure) {
        if (!(list.type() instanceof com.legend.sql.TypeFact.Unknown)
                && !(list.type() instanceof com.legend.sql.TypeFact.Bottom)) {
            return list;
        }
        SqlType t = carrierOrNull(elemPure);
        return t == null ? list
                : new com.legend.sql.SqlExpr.Cast(list,
                        new com.legend.sql.SqlType.Array(t));
    }

    /** The element's SQL carrier when one certainly exists — an
     * eligibility CHECK, never a caught wall: carrier-bearing
     * primitives ({@link #primitiveCarrier}, the ONE owner {@code
     * type()} also reads), precision decimals, and the designed class
     * carriers (variant/Any/Nil). Everything else null — the door
     * stays shut. */
    private static @com.legend.base.Nullable SqlType carrierOrNull(Type t) {
        if (t instanceof Type.Primitive p) {
            return primitiveCarrier(p);
        }
        if (t instanceof Type.PrecisionDecimal) {
            return type(t);
        }
        if (t instanceof Type.ClassType ct
                && (PlatformTypes.isVariant(ct) || PlatformTypes.isAny(ct)
                        || PlatformTypes.isNil(ct))) {
            return type(t);
        }
        return null;
    }

    /** An if-branch is a 0-param SINGLE-expression thunk; its body is
     * the value. Moved from the dissolved ListShapes. */
    static com.legend.compiler.spec.typed.TypedSpec thunkBody(
            com.legend.compiler.spec.typed.TypedSpec branch) {
        if (branch instanceof com.legend.compiler.spec.typed.TypedLambda l) {
            if (l.body().size() != 1) {
                throw new IllegalStateException("if-branch thunk has "
                        + l.body().size() + " statements; a last-statement"
                        + " pick would silently drop the rest");
            }
            return l.body().get(0);
        }
        return branch;
    }

    static boolean nullable(Multiplicity m) {
        return !(m instanceof Multiplicity.Bounded b) || b.lower() == 0;
    }
}
