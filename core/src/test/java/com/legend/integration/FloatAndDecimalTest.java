// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * FLOAT OR DECIMAL: WHICH TO USE WHEN (the user's request, 2026-11-17; register S20, built in Phase 5b). The same
 * calculations four ways, side by side: a Float column, a Float literal, a Decimal column, a Decimal literal.
 * <ul>
 *   <li><b>Float</b> is a binary double, IEEE arithmetic everywhere, as legend-pure's compiled runtime and
 *       legend-engine compute it: use it for MEASUREMENTS, where a nearest double is the value
 *       ({@code 0.1 + 0.2} is {@code 0.30000000000000004}). A Float literal and a Float column answer alike.</li>
 *   <li><b>Decimal</b> is exact: use it for MONEY and exact math ({@code 0.1D + 0.2D} is {@code 0.3}). A Decimal
 *       literal and a Decimal column answer alike.</li>
 * </ul>
 * {@code round} with a scale is marked: lite rounds half-even where legend-pure rounds half-up over the shortest
 * decimal (F47, open until it lands): its lines pin today's answer and name the gap.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class FloatAndDecimalTest {

    /** A calculation over two operands {@code a} and {@code b}, written once with {@code $a}/{@code $b} standing for
     *  them: a literal substitutes the values, a column reads them from a one-row relation's columns. */
    private record Calc(String name, String expr, String a, String b) {
    }

    private static final List<Calc> CALCS = List.of(
            new Calc("sum", "$a + $b", "0.1", "0.2"),
            new Calc("equality", "($a + $b) == 0.3", "0.1", "0.2"),
            new Calc("remainder", "rem($a, $b)", "0.3", "0.1"),
            new Calc("product", "$a * $b", "123456789.123456789", "3.0"),
            new Calc("round", "round($a)", "2.5", "0"),
            new Calc("roundScale", "round($a, 1)", "0.25", "0"));

    @Test
    void floatIsTheDouble_decimalIsExact_aLiteralAndAColumnAlike() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        List<String> wrong = new ArrayList<>();
        try (Connection c = backend.open()) {
            for (Calc calc : CALCS) {
                Object floatLiteral = run(model, c, literal(calc, ""));
                Object floatColumn = run(model, c, column(calc, "Float", ""));
                Object decimalLiteral = run(model, c, literal(calc, "D"));
                Object decimalColumn = run(model, c, column(calc, "Decimal", "D"));
                System.out.println("[float and decimal] " + backend.name() + " " + calc.name() + ": Float literal "
                        + floatLiteral + ", Float column " + floatColumn + ", Decimal literal " + decimalLiteral
                        + ", Decimal column " + decimalColumn);
                Object floatWant = floatAnswer(calc);
                Object decimalWant = decimalAnswer(calc);
                check(wrong, backend.name() + " " + calc.name() + " Float literal", floatWant, floatLiteral);
                check(wrong, backend.name() + " " + calc.name() + " Float column", floatWant, floatColumn);
                check(wrong, backend.name() + " " + calc.name() + " Decimal literal", decimalWant, decimalLiteral);
                check(wrong, backend.name() + " " + calc.name() + " Decimal column", decimalWant, decimalColumn);
            }
        }
        assertEquals(List.of(), wrong, backend.name());
    }

    /** The double's answer: Java's own arithmetic on the doubles, legend-pure's compiled runtime. */
    private static Object floatAnswer(Calc calc) {
        double a = Double.parseDouble(calc.a());
        double b = Double.parseDouble(calc.b());
        return switch (calc.name()) {
            case "sum" -> a + b;
            case "equality" -> a + b == 0.3;
            case "remainder" -> a % b;
            case "product" -> a * b;
            // Pure's round(Number): half-even, 2.5 is 2
            case "round" -> 2L;
            // F47: legend-pure answers 0.3 (half-up over the shortest decimal); lite rounds half-even, 0.2
            case "roundScale" -> 0.2;
            default -> throw new IllegalStateException(calc.name());
        };
    }

    /** The exact answer: BigDecimal arithmetic on the written digits. */
    private static Object decimalAnswer(Calc calc) {
        BigDecimal a = new BigDecimal(calc.a());
        BigDecimal b = new BigDecimal(calc.b());
        return switch (calc.name()) {
            case "sum" -> a.add(b);
            case "equality" -> a.add(b).compareTo(new BigDecimal("0.3")) == 0;
            case "remainder" -> a.remainder(b);
            case "product" -> a.multiply(b);
            case "round" -> 2L;
            // F47: legend-pure answers 0.3 (HALF_UP); lite rounds half-even, 0.2
            case "roundScale" -> new BigDecimal("0.2");
            default -> throw new IllegalStateException(calc.name());
        };
    }

    /** {@code calc} over literals, each written with {@code suffix} ({@code D} for a Decimal). */
    private static String literal(Calc calc, String suffix) {
        return "|" + calc.expr().replace("$a", calc.a() + suffix).replace("$b", calc.b() + suffix)
                .replace("== 0.3", "== 0.3" + suffix);
    }

    /** {@code calc} over a one-row relation whose columns {@code a} and {@code b} are of {@code type}. */
    private static String column(Calc calc, String type, String suffix) {
        return "|#TDS\n  a:" + type + ", b:" + type + "\n  " + calc.a() + ", " + calc.b() + "\n#->extend(~v: r|"
                + calc.expr().replace("$a", "$r.a->toOne()").replace("$b", "$r.b->toOne()")
                        .replace("== 0.3", "== 0.3" + suffix)
                + ")->select(~v)->from(" + StorelessRuntime.RUNTIME + ")";
    }

    private static Object run(String model, Connection c, String query) {
        try {
            return Lean.run(model, query, StorelessRuntime.RUNTIME, c).rows().get(0).get(0);
        } catch (RuntimeException refused) {
            return refused;
        }
    }

    /** {@code got} is {@code want} by value: a number compared exactly (a double as the double, a decimal by
     *  {@code compareTo}), a Boolean as itself; a refusal is never an answer. */
    private static void check(List<String> wrong, String label, Object want, Object got) {
        boolean same;
        if (got instanceof Throwable || got == null) {
            same = false;
        } else if (want instanceof Double d) {
            same = got instanceof Number n && n.doubleValue() == d;
        } else if (want instanceof BigDecimal d) {
            same = got instanceof Number n && new BigDecimal(n.toString()).compareTo(d) == 0;
        } else if (want instanceof Long l) {
            same = got instanceof Number n && n.doubleValue() == l;
        } else {
            same = want.equals(got);
        }
        if (!same) {
            wrong.add(label + ": want " + want + ", got " + (got instanceof Throwable t
                    ? t.getClass().getSimpleName() + ": " + t.getMessage() : got));
        }
    }
}
