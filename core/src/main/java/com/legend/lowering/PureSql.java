package com.legend.lowering;

import com.legend.compiler.element.type.Multiplicity;
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

    static SqlType type(Type t) {
        if (t instanceof Type.Primitive p) {
            return switch (p) {
                case STRING -> SqlType.Scalar.VARCHAR;
                case INTEGER -> SqlType.Scalar.BIGINT;
                case FLOAT, NUMBER -> SqlType.Scalar.DOUBLE;
                case BOOLEAN -> SqlType.Scalar.BOOLEAN;
                case DECIMAL -> new SqlType.Decimal(38, 18);
                case STRICT_DATE -> SqlType.Scalar.DATE;
                case DATE_TIME, DATE -> SqlType.Scalar.TIMESTAMP;
                default -> throw new IllegalStateException(
                        "no SQL type for Pure primitive " + p + " at the lowering boundary");
            };
        }
        if (t instanceof Type.PrecisionDecimal d) {
            return new SqlType.Decimal(d.precision(), d.scale());
        }
        if (t instanceof Type.ClassType ct && ct.fqn().endsWith("::Variant")) {
            return SqlType.Scalar.JSON;
        }
        throw new IllegalStateException(
                "no SQL type for Pure type " + t.typeName() + " at the lowering boundary");
    }

    static boolean nullable(Multiplicity m) {
        return !(m instanceof Multiplicity.Bounded b) || b.lower() == 0;
    }
}
