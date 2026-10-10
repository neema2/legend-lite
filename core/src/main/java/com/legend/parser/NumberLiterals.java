// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.parser;

import com.legend.lexer.TokenStream;
import com.legend.protocol.SourceInfo;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.ValueSpecification;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Numeric literal TEXT to its protocol literal — the dialect-split rules
 * {@link SpecParser} applies to INTEGER / FLOAT / DECIMAL tokens, split out
 * of the parser for size (CodeShape guard, batch 150). Every rule below is
 * oracle-verified (2026-08-12) and unchanged.
 */
final class NumberLiterals {

    private NumberLiterals() {
    }

    /**
     * INTEGER token &rarr; {@link CInteger}. Narrows to {@link Long} when
     * the value fits in 64 signed bits, else falls back to
     * {@link BigInteger} so overflow is preserved exactly (matches the
     * engine record contract). The BigInteger widening is a DECLARED lite
     * extension: the ENGINE surface refuses it (G6 run 1081 caught the
     * over-wide legendStrict gate).
     */
    static CInteger integer(String text, SourceInfo span, Dialect dialect, TokenStream tokens, int tok) {
        try {
            return new CInteger(Long.parseLong(text), span);
        } catch (NumberFormatException overflow) {
            if (dialect.refusesLiteExtensions()) {
                throw TokenStreamCursor.throwAt(tokens, tok, "Unexpected token '" + text + "'");
            }
            return new CInteger(new BigInteger(text), span);
        }
    }

    /**
     * FLOAT token &rarr; a DIALECT-SPLIT literal: the ENGINE/LITE surfaces
     * build {@link CFloat} unconditionally, like {@code DomainParseTreeWalker}
     * ({@code 1.0000000000000001} is float {@code 1.0} on the wire, probed);
     * LEGEND_PLATFORM keeps legend-pure's EXECUTION semantics — the
     * interpreted runtime's Float IS BigDecimal-backed (FloatCoreInstance
     * parses the SOURCE TEXT), so a precision-losing literal keeps its
     * digits AND its Float label (the PCT reference testBigFloatAbs asserts
     * the decimal-exact value). An optional {@code f}/{@code F} suffix is
     * Pure's, not Java's.
     */
    static ValueSpecification floating(String text, SourceInfo span, Dialect dialect) {
        if (!text.isEmpty()) {
            char last = text.charAt(text.length() - 1);
            if (last == 'f' || last == 'F') {
                text = text.substring(0, text.length() - 1);
            }
        }
        double d = Double.parseDouble(text);
        if (!dialect.refusesLiteExtensions()) {
            BigDecimal exact = new BigDecimal(text);
            if (exact.compareTo(BigDecimal.valueOf(d)) != 0) {
                return new CFloat(d, exact, span);
            }
        }
        return new CFloat(d, span);
    }

    /**
     * DECIMAL token &rarr; {@link CDecimal}. The lexer admits both
     * {@code 42d} and {@code 3.14d}; the {@code d}/{@code D} suffix is
     * stripped before {@link BigDecimal} parses the text.
     */
    static CDecimal decimal(String text, SourceInfo span) {
        char last = text.charAt(text.length() - 1);
        if (last == 'd' || last == 'D') {
            text = text.substring(0, text.length() - 1);
        }
        return new CDecimal(new BigDecimal(text), text, span);
    }
}
