// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;

import java.util.List;

/**
 * The MODERN-H2 profile (2.3+, probed on 2.4.240) — {@link H2} plus the
 * typed-JSON navigation newer H2 grew: {@code (j)."field"} (the key
 * QUOTES — unquoted identifiers upcase and silently miss) and
 * {@code (j)[i]} with ONE-BASED indexes (the reference IR convention is
 * zero-based — the spelling shifts). Navigation reads TYPED JSON only:
 * the JSON_ARRAY / JSON_ARRAYAGG / JSON_OBJECT carriers and JSON
 * columns all feed it (probed); a CAST from VARCHAR does not. Selected
 * by CONNECTED VERSION at the driver seam ({@code Compiler.dialectOf})
 * — the 2.1.214 engine-parity target keeps {@link H2}'s walls.
 */
public class H2Modern extends H2 {

    /** {@code (x)."key"} / {@code (x)[i+1]} — dynamic keys have no
     * spelling (field access is an identifier) and fall to the wall. */
    @Override
    protected String variantGet(List<SqlExpr> args) {
        if (args.get(1) instanceof SqlExpr.StringLit key) {
            return "(" + expr(args.get(0), 7) + ").\""
                    + key.value().replace("\"", "\"\"") + "\"";
        }
        if (args.get(1) instanceof SqlExpr.IntLit ix) {
            return "(" + expr(args.get(0), 7) + ")[" + (ix.value() + 1) + "]";
        }
        return super.variantGet(args);
    }

    /** Struct values ride the JSON-object carrier here: literal ->
     * JSON_OBJECT (canonical field order), field read -> navigation. */
    @Override
    protected String structLit(SqlExpr.StructLit s) {
        StringBuilder sb = new StringBuilder("JSON_OBJECT(");
        for (int i = 0; i < s.fields().size(); i++) {
            SqlExpr.StructLit.Field f = s.fields().get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(stringLit(f.name())).append(": ")
                    .append(expr(f.value(), 0));
        }
        return sb.append(")").toString();
    }

    @Override
    protected String structGet(SqlExpr.StructGet g) {
        return "(" + expr(g.source(), 7) + ").\""
                + g.field().replace("\"", "\"\"") + "\"";
    }

    /** {@code CARDINALITY} counts JSON-array elements on 2.3+ (probed:
     * 3 for '[1,2,3]') — the LIST_LENGTH spelling. HERE, not in
     * listCall: the inherited 'len' Spellings row short-circuits ahead
     * of the idiom dispatch. The Array-cast wrapper only re-types and
     * unwraps. */
    @Override
    protected String call(SqlExpr.Call c, int parentPrec) {
        if (c.fn() == com.legend.sql.SqlFn.LIST_LENGTH
                && c.args().size() == 1) {
            SqlExpr arg = c.args().get(0);
            if (arg instanceof SqlExpr.Cast ac
                    && ac.target() instanceof com.legend.sql.SqlType.Array) {
                arg = ac.value();
            }
            return "CARDINALITY(" + expr(arg, 0) + ")";
        }
        return super.call(c, parentPrec);
    }

    /** A cast TO JSON is the PARSE intent (DuckDB's CAST parses) — but
     * H2's CAST from VARCHAR QUOTES the text as a JSON string instead
     * (probed: CAST('[10,20]' AS JSON) -> "\"[10,20]\""). The parsing
     * spellings: {@code JSON '...'} for literals, {@code (x FORMAT
     * JSON)} for dynamic text (both probed to navigate). TO_VARIANT
     * keeps the quoting CAST — that IS toVariant's string semantics. */
    private @com.legend.base.Nullable String jsonParseCast(SqlExpr.Cast c) {
        if (c.target() != com.legend.sql.SqlType.Scalar.JSON) {
            return null;
        }
        if (c.value() instanceof SqlExpr.StringLit sl) {
            return "JSON " + stringLit(sl.value());
        }
        return "(" + expr(c.value(), 0) + " FORMAT JSON)";
    }

    /** Scalar casts over a navigation extract TEXT first: H2 rejects
     * JSON -> number/boolean casts outright and its VARCHAR cast keeps
     * the JSON quoting (probed) — TRIM strips it; numbers and booleans
     * carry no quotes and convert cleanly. */
    @Override
    protected String variantAwareCast(SqlExpr.Cast c) {
        String parse = jsonParseCast(c);
        if (parse != null) {
            return parse;
        }
        boolean navigation = c.value() instanceof SqlExpr.Call call
                && (call.fn() == com.legend.sql.SqlFn.VARIANT_GET
                        || call.fn() == com.legend.sql.SqlFn.VARIANT_ELEMENTS)
                || c.value() instanceof SqlExpr.StructGet;
        if (navigation
                && c.target() instanceof com.legend.sql.SqlType.Scalar sc
                && sc != com.legend.sql.SqlType.Scalar.JSON) {
            return "CAST(TRIM(BOTH '\"' FROM CAST(" + expr(c.value(), 0)
                    + " AS VARCHAR)) AS " + castTypeName(c.target()) + ")";
        }
        return super.variantAwareCast(c);
    }
}
