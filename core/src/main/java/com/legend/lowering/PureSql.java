package com.legend.lowering;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlType;

/**
 * THE Pure→SQL type boundary (LEGEND_SQL_VISION.md): the one place Pure's
 * type system meets the SQL layer's. Pure Integer is 64-bit → BIGINT is
 * decided HERE, by the frontend — not by any dialect.
 */
final class PureSql {

    private PureSql() {
    }

    /**
     * EXHAUSTIVE over sealed {@link Type} and the {@code Primitive} enum — no
     * default arms (root package-info invariant): a new Pure type demands an
     * explicit decision here, at compile time. Unsupported kinds throw with
     * their own arm so the message names the exact kind.
     */
    static SqlType type(Type t) {
        return switch (t) {
            case Type.Primitive p -> switch (p) {
                case STRING -> SqlType.Scalar.VARCHAR;
                case INTEGER -> SqlType.Scalar.BIGINT;
                case FLOAT, NUMBER -> SqlType.Scalar.DOUBLE;
                case BOOLEAN -> SqlType.Scalar.BOOLEAN;
                case DECIMAL -> new SqlType.Decimal(38, 18);
                case STRICT_DATE -> SqlType.Scalar.DATE;
                case DATE_TIME, DATE -> SqlType.Scalar.TIMESTAMP;
                case BYTE, LATEST_DATE, STRICT_TIME -> throw new IllegalStateException(
                        "no SQL type for Pure primitive " + p + " at the lowering boundary");
            };
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
                throw new IllegalStateException(
                        "no SQL type for generic " + g.typeName() + " at the lowering boundary");
            }
            case Type.FunctionType f -> throw new IllegalStateException(
                    "a function value has no SQL type (" + f.typeName() + ")");
            case Type.RelationType r -> throw new IllegalStateException(
                    "a relation is a SOURCE, not a scalar SQL type (" + r.typeName() + ")");
            case Type.SchemaAlgebra a -> throw new IllegalStateException(
                    "unresolved schema algebra " + a.typeName() + " reached the lowering boundary");
        };
    }

    static boolean nullable(Multiplicity m) {
        return !(m instanceof Multiplicity.Bounded b) || b.lower() == 0;
    }
}
