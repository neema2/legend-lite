// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/**
 * LITE'S OWN TEXT of a value in an assert's message, in SQL (the user's judge model, 2026-11-17): the database judge's
 * half of the sentence table ({@code error.AssertSentence}), the host's being {@code testexec.LiteWords}; the two
 * judges write one text. Not legend-pure's print form: that is {@link Repr}'s, and leaves the product with the edge.
 */
final class LiteText {

    private LiteText() {
    }

    /** A value's text in an assert's message under LITE'S OWN RULES (the user's judge model, 2026-11-17: the host
     *  judge's {@code testexec.LiteWords.value} in SQL, the two judges one text): a String quoted JSON-style (a
     *  backslash, a quote, a newline, a tab and a carriage return escaped); a Float at full round-trip precision
     *  ({@code FLOAT_SHORTEST_TEXT}, lite's canonical text of a Float -- the product writes it here, the audit's K1,
     *  2026-11-17, {@code LiteFloatTextTest}: a message is a diagnostic); a DateTime ISO 8601 in UTC, six fraction digits and Z (S312, {@code DATETIME_TO_TEXT});
     *  a date its day; a Decimal its plain digits ({@code DECIMAL_TO_TEXT}); every other primitive its toString. */
    static SqlExpr of(Type t, SqlExpr x) {
        if (t == Type.Primitive.STRING) {
            SqlExpr esc = new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
            String[][] pairs = {{"\\", "\\\\"}, {"\"", "\\\""}, {"\n", "\\n"}, {"\t", "\\t"},
                    {"\r", "\\r"}};
            for (String[] p : pairs) {
                esc = SqlExpr.Call.of(SqlFn.REPLACE, esc, new SqlExpr.StringLit(p[0]), new SqlExpr.StringLit(p[1]));
            }
            return Scalars.cat(new SqlExpr.StringLit("\""), Scalars.cat(esc, new SqlExpr.StringLit("\"")));
        }
        if (t == Type.Primitive.DATE_TIME) {
            // a value carried as its text (a literal's channel) read as the timestamp it names first
            boolean timestamp = x.type() instanceof com.legend.sql.TypeFact.Typed ty
                    && (ty.type() == SqlType.Scalar.TIMESTAMP || ty.type() == SqlType.Scalar.TIMESTAMPTZ);
            return SqlExpr.Call.of(SqlFn.DATETIME_TO_TEXT, timestamp ? x
                    : new SqlExpr.Cast(x, SqlType.Scalar.TIMESTAMP, SqlExpr.Reason.JUDGE));
        }
        if (t == Type.Primitive.STRICT_DATE || t == Type.Primitive.DATE) {
            return new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        }
        if (t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
            // a value the store read rides the value lane's D-suffixed text carrier (LiteralSpelling.wireValueEgress:
            // 1D, 0.3D), which lite's text drops, as the host writes the BigDecimal that carrier decodes to (PR #118 fix
            // round, 2026-11-18, G2: the database judge wrote "got 1D" where the host wrote "got 1 (Decimal)"). RESIDUE
            // for the presentation program ("one plan, two presentations"): the real cause is the product's store-read
            // carrier spelling a Decimal as legend-pure's %r form ("1D", DECIMAL_TEXT) inside the product; this strip
            // touches only that trailing D (AssertBarTest: a sign, a written scale, kept)
            return LiteralSpelling.withoutDecimalMark(Scalars.pureToString(Styled.PRODUCT, Type.Primitive.DECIMAL, x));
        }
        if (t == Type.Primitive.FLOAT) {
            // a message's Float at full round-trip precision (the orchestrator's ruling, 2026-11-17): FLOAT_SHORTEST_TEXT, the
            // host's CellText.carriedFloat; a value the store holds as an integer its digits
            return LiteralSpelling.heldAsInteger(x) ? new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE)
                    : javaExponent(SqlExpr.Call.of(SqlFn.FLOAT_SHORTEST_TEXT, x));
        }
        return Scalars.pureToString(Styled.PRODUCT, t, x);
    }

    /** {@code text}, a Float's FLOAT_SHORTEST_TEXT text, its exponent form (a magnitude past DECIMAL(38,18), each database
     *  spelling it its own way: {@code 1e-300}, {@code 1.0E-300}) written as Java writes one, so every database's
     *  message text is the host's ({@code LiteWords.messageFloat}): the mantissa with a point, {@code E}, the
     *  exponent's digits with no {@code +} and no leading zero. A text with no exponent as it is. */
    private static SqlExpr javaExponent(SqlExpr text) {
        SqlExpr t = new SqlExpr.Cast(text, SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        SqlExpr lower = SqlExpr.Call.of(SqlFn.LOWER, t);
        SqlExpr at = SqlExpr.Call.of(SqlFn.STRPOS, lower, new SqlExpr.StringLit("e"));
        SqlExpr mantissa = SqlExpr.Call.of(SqlFn.SUBSTRING, t, new SqlExpr.IntLit(1),
                SqlExpr.Call.of(SqlFn.MINUS, at, new SqlExpr.IntLit(1)));
        SqlExpr exponent = SqlExpr.Call.of(SqlFn.SUBSTRING, t, SqlExpr.Call.of(SqlFn.PLUS, at, new SqlExpr.IntLit(1)),
                SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, t), at));
        SqlExpr pointed = new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, mantissa, new SqlExpr.StringLit(".")), new SqlExpr.IntLit(0)), mantissa)),
                SqlExpr.Call.of(SqlFn.CONCAT, mantissa, new SqlExpr.StringLit(".0")), SqlExpr.Reason.JUDGE);
        // the exponent's digits read as an integer and written back: no '+', no leading zero
        SqlExpr digits = new SqlExpr.Cast(new SqlExpr.Cast(exponent, SqlType.Scalar.BIGINT, SqlExpr.Reason.JUDGE),
                SqlType.Scalar.VARCHAR, SqlExpr.Reason.JUDGE);
        // NaN and the infinities hold no 'e' a number's exponent is (Infinity's is past its first letter)
        SqlExpr numeric = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.GREATER, at, new SqlExpr.IntLit(1)),
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.STRPOS, lower, new SqlExpr.StringLit("inf")),
                        new SqlExpr.IntLit(0)));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(numeric,
                SqlExpr.Call.of(SqlFn.CONCAT, pointed, new SqlExpr.StringLit("E"), digits))), t, SqlExpr.Reason.JUDGE);
    }
}
