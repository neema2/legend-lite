// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/**
 * DECIMAL KIND PRESERVATION through scale-carrying natives (X-audit
 * wire-kind drift, extracted from {@link Scalars} at the file guard):
 * the engine's {@code round(Decimal, scale):Decimal} and
 * {@code divide(Decimal, Decimal, scale):Decimal} keep the Decimal
 * kind, but the SQL round returns DOUBLE — a CONSTANT scale restores
 * DECIMAL-at-that-scale; a non-constant scale stays the SQL kind
 * (census-visible, never a guess). {@code toDecimal} keeps the VALUE's
 * own scale — an INTEGER input is a scale-0 Decimal, never the blanket
 * (38,18) fabrication.
 */
final class DecimalKindRules {

    private DecimalKindRules() {
    }

    static SqlExpr round(TypedNativeCall n, List<SqlExpr> args) {
        if (args.size() == 1) {
            return new SqlExpr.Cast(new SqlExpr.Call(SqlFn.ROUND, args),
                    SqlType.Scalar.BIGINT);
        }
        // round with a scale is HALF_UP, a Decimal's and a Float's alike (legend-pure's CoreHelper.round(BigDecimal, long),
        // setScale(scale, HALF_UP), and round(Double, long) through BigDecimal.valueOf; the interpreter's RoundWithScale):
        // C5-J's batch, 1(a), 2026-11-15 -- until then SqlFn.ROUND, which every dialect writes half-even (round(Number)
        // alone is pure's half-even). Each dialect reads a double through its shortest text (ROUND_HALF_UP's arms)
        return decimalScaleCast(n, 1,
                new SqlExpr.Call(SqlFn.ROUND_HALF_UP, args));
    }

    static SqlExpr divide(TypedNativeCall n, List<SqlExpr> args) {
        if (args.size() != 3) {
            // Part-1 fix (2026-08-26): INTEGER / INTEGER by zero RAISES
            // in real pure — the interpreted accumulator divides
            // integers through BigDecimal (ArithmeticException;
            // NumericAccumulator receipts); only the DOUBLE lane yields
            // Infinity. The rem idiom guards the integer pair;
            // Float/Decimal lanes keep their lane semantics.
            // A divisor written as a literal other than 0 needs no guard: its spelled token is compared, nothing
            // computed (2026-10-30, //datacube:timeseries_sql_measure: Wilder's (w - 1) / w guarded at each of RSI's
            // 64 terms, 122 bytes each).
            if (args.size() == 2
                    && n.args().get(0).info().type()
                            == Type.Primitive.INTEGER
                    && n.args().get(1).info().type()
                            == Type.Primitive.INTEGER
                    && !(args.get(1) instanceof SqlExpr.IntLit k && k.value() != 0)) {
                return Scalars.guarded(
                        SqlExpr.Call.of(SqlFn.EQUAL, args.get(1),
                                new SqlExpr.IntLit(0)),
                        new SqlExpr.StringLit("Division by zero"),
                        new SqlExpr.Call(SqlFn.DIVIDE, args));
            }
            return new SqlExpr.Call(SqlFn.DIVIDE, args);
        }
        // the exact quotient rounded HALF_UP, legend-pure's left.divide(right, scale, HALF_UP) (CompiledSupport.divideDecimal,
        // the interpreter's DivideDecimal): C5-J's batch, 1(b), 2026-11-15 -- until then DIVIDE, which forces float, then
        // rounded (2.01 / 2 at 2 places was 1.00 on DuckDB, 12345678901234567.89 / 1 lost its cents). A scale known only
        // at run time keeps that form until it is ruled (posted with the batch's results)
        if (args.get(2) instanceof SqlExpr.IntLit) {
            return decimalScaleCast(n, 2, SqlExpr.Call.of(SqlFn.DECIMAL_DIVIDE, args.get(0), args.get(1), args.get(2)));
        }
        return decimalScaleCast(n, 2,
                new SqlExpr.Call(SqlFn.ROUND_HALF_UP, List.of(
                        SqlExpr.Call.of(SqlFn.DIVIDE, args.get(0),
                                args.get(1)), args.get(2))));
    }

    static SqlExpr divideRound(TypedNativeCall n, List<SqlExpr> args) {
        return decimalScaleCast(n, 2,
                new SqlExpr.Call(SqlFn.ROUND, List.of(
                        SqlExpr.Call.of(SqlFn.DIVIDE, args.get(0),
                                args.get(1)), args.get(2))));
    }

    static SqlExpr toDecimal(TypedNativeCall n, List<SqlExpr> args) {
        return switch (args.get(0)) {
            // a bare integral literal types INTEGER — cast keeps it a
            // scale-0 DECIMAL (8D)
            case SqlExpr.IntLit i ->
                    new SqlExpr.Cast(i, new SqlType.Decimal(38, 0));
            case SqlExpr.DecimalLit d -> d;
            case SqlExpr.FloatLit fl -> new SqlExpr.DecimalLit(
                    java.math.BigDecimal.valueOf(fl.value()));
            // a Float as Pure makes a Decimal of it, the decimal its shortest text reads as (CoreHelper.toDecimal:
            // new BigDecimal(number.toString())): C5-J's batch, 1(c), 2026-11-15 -- until then a cast of the double,
            // which some databases take from its binary expansion
            default -> n.args().get(0).info().type() == Type.Primitive.FLOAT
                    ? SqlExpr.Call.of(SqlFn.FLOAT_TO_DECIMAL, args.get(0), new SqlExpr.IntLit(18))
                    : new SqlExpr.Cast(args.get(0),
                            n.args().get(0).info().type() == Type.Primitive.INTEGER
                                    ? new SqlType.Decimal(38, 0)
                                    : new SqlType.Decimal(38, 18));
        };
    }

    private static SqlExpr decimalScaleCast(TypedNativeCall n,
            int scaleArgIndex, SqlExpr computed) {
        if (isDecimalStamp(n.info().type())
                && n.args().get(scaleArgIndex) instanceof TypedCInteger k) {
            return new SqlExpr.Cast(computed,
                    new SqlType.Decimal(38, k.value().intValue()));
        }
        return computed;
    }

    static boolean isDecimalStamp(Type t) {
        return t == Type.Primitive.DECIMAL
                || t instanceof Type.PrecisionDecimal;
    }
}
