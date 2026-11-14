// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A Decimal's or a Number's text is its plain digits on every shape that carries one (the tenets audit's M-6, A6,
 * 2026-11-10): the Lowerer marks the number-to-text by the operand's Pure type ({@code SqlFn.DECIMAL_TO_TEXT}), and H2
 * writes it with its own number printer for every such operand -- until A6 its dialect walked the expression's shape
 * ({@code decfloatShaped}) and missed an {@code if}, a {@code max}, a let and a computed column, where {@code 100}
 * printed {@code 1E+2}. Each shape's text is the value's plain digits (no exponent), its number the value's.
 */
class DecimalTextTest {

    static final List<String> SHAPES = List.of(
            "{n: Decimal[1]|$n->toString()}",
            "{n: Decimal[1]|if(true, |$n, |$n)->toString()}",
            "{n: Decimal[1]|max($n, $n)->toString()}",
            "{n: Decimal[1]|let x = $n; $x->toString();}",
            "{n: Decimal[1]|#>{s::DB.T}#->filter(r|$r.ID == 1)->extend(~d: r|$n)->extend(~s: r|$r.d->toString())"
                    + "->select(~s)}");

    @Test
    void aDecimalsTextIsItsPlainDigitsOnEveryShape_onH2AndDuckDb() throws Exception {
        for (String value : List.of("100", "1000", "2.5", "-0.5", "0.000001")) {
            for (String shape : SHAPES) {
                Map<String, ?> values = Map.of("n", new BigDecimal(value));
                for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2)) {
                    String text;
                    try {
                        text = String.valueOf(ZonedDateTimeParameterTest.answer(type, null, shape, values));
                    } catch (RuntimeException e) {
                        throw new AssertionError(type + ": " + shape + " of " + value + ": " + e, e);
                    }
                    assertEquals(-1, text.indexOf('E'), type + ": " + shape + " of " + value + " printed " + text);
                    assertEquals(0, new BigDecimal(value).compareTo(new BigDecimal(text)),
                            type + ": " + shape + " of " + value + " printed " + text);
                }
            }
        }
    }
}
