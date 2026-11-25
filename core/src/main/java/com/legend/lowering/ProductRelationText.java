// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A RELATION'S {@code toString()}, COMPUTED BY THE DATABASE IN THE PRODUCT (stage B's B2, B2b S5a, 2026-11-24; the
 * user's rulings of 2026-11-19 ~18:05, "Agree with all four table printing recommendations", and ~18:36, "Null is
 * good"; built from H14's spike, docs/records/compat-homework/H14-slice-shape-note.md on
 * claude/compat-table-text-db-spike). Like any other function, the database computes the text (tenet 1): one scalar
 * subquery, a {@code STRING_AGG} of the relation's rows in the relation's own order. The layout is legend-pure's
 * {@code #TDS} text -- {@code #TDS}, the names line, one {@code '   '}-prefixed line per row with its cells joined
 * {@code ,}, {@code #}; a name quoted unless {@code ^[a-zA-Z0-9_]+$} -- and each cell is the PRODUCT's own text node
 * of its value on every plan style ({@link Styled#PRODUCT}: the cells of a text used further in a query are lite's
 * text, the user's ~18:12 ruling, "Yes record it"; until B2b's S6, under the edge's style the edge dialect's value
 * passes re-spell a Float and a DateTime cell as they re-spell any such text there -- a window recorded in
 * docs/records/compat-b2-progress.md's S5a, pinned by FloatTextTest): an empty cell {@code null} ({@code 'null'} under a Variant), a
 * Variant's text and a String holding a brace or a bracket quoted with {@code \} and {@code '} escaped, every other
 * cell its scalar {@code toString()}. Where a relation's text is the program's last step, legend-pure's and
 * legend-engine's doors print legend-pure's text in Java from the rows instead ({@code edge.PrintedRelations}, B3's
 * lift). No plan-style facet, no hook, no mode. A shape it cannot write is refused by name.
 */
final class ProductRelationText {

    private ProductRelationText() {
    }

    /** {@code tc}, {@code toString(rel)} or {@code toString(rel, false)}, as one scalar subquery. */
    static SqlExpr of(TypedNativeCall tc, Function<TypedSpec, SqlSelect> relation, String alias) {
        if (tc.args().size() == 2 && (!(tc.args().get(1) instanceof TypedCBoolean b) || b.value())) {
            throw new com.legend.error.NotImplementedException("a relation's toString with its types and"
                    + " multiplicities (toString(rel, true)) is not written by the product");
        }
        TypedSpec rel = tc.args().get(0);
        if (Type.relationSchema(rel.info().type()) instanceof Type.RelationType late && late.isLateBound()) {
            throw new com.legend.error.NotImplementedException("a relation's toString over a raw grid (executeInDb):"
                    + " its columns exist only when the plan runs");
        }
        Map<String, Type.Column> byName = new HashMap<>();
        if (Type.relationSchema(rel.info().type()) instanceof Type.RelationType rt) {
            if (!rt.dynamicColumns().isEmpty()) {
                throw new com.legend.error.NotImplementedException("a relation's toString over a pivot whose columns"
                        + " nothing names: its columns exist only once the database is asked");
            }
            for (Type.Column col : rt.columns()) {
                byName.put(col.name(), col);
            }
        }
        SqlSelect inner = relation.apply(rel);
        List<OutputCol> cols = inner.outputs();
        if (cols.isEmpty()) {
            throw new IllegalStateException("a relation's toString over a relation of no columns");
        }
        Render.Lifted lifted = Render.liftOrder(inner, alias, "a relation's toString");
        if (lifted == null) {
            throw new com.legend.error.NotImplementedException("a relation's toString over a DISTINCT or star select"
                    + " ordered by a key that is not one of its columns");
        }
        StringBuilder header = new StringBuilder("#TDS\n   ");
        SqlExpr line = new SqlExpr.StringLit("   ");
        for (int i = 0; i < cols.size(); i++) {
            OutputCol oc = cols.get(i);
            Type.Column typed = byName.get(oc.name());
            if (typed == null) {
                throw new IllegalStateException("a relation's toString: the output column '" + oc.name()
                        + "' has no typed relation column");
            }
            header.append(i == 0 ? "" : ",").append(name(oc.name()));
            SqlExpr c = cell(SqlExpr.Column.of(alias, oc), typed.type(), oc.type());
            line = i == 0 ? Scalars.cat(line, c) : Scalars.cat(line, new SqlExpr.StringLit(","), c);
        }
        header.append('\n');
        SqlExpr rows = SqlExpr.Call.of(SqlFn.COALESCE,
                new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG, List.of(line, new SqlExpr.StringLit("\n")), false,
                        lifted.keys()),
                new SqlExpr.StringLit(""));
        SqlExpr text = Scalars.cat(new SqlExpr.StringLit(header.toString()), rows, new SqlExpr.StringLit("\n#"));
        SqlSelect sel = SqlSelect.starOf(new SqlSource.Subselect(lifted.source(), alias, null))
                .withProjections(List.of(new SqlSelect.Projection(text, "tds",
                        new OutputCol("tds", SqlType.Scalar.VARCHAR, false))));
        return new SqlExpr.ScalarSubquery(sel);
    }

    /** A column's name in the header: trimmed, quoted unless it starts with a quote or is {@code ^[a-zA-Z0-9_]+$}. A
     *  typed fact of the plan, written at planning (a fold of facts fixed by the types). */
    private static String name(String raw) {
        String n = raw.trim();
        return n.startsWith("'") || n.matches("^[a-zA-Z0-9_]+$") ? n : "'" + n + "'";
    }

    /** A cell: an empty one {@code null} ({@code 'null'} under a Variant column); a Variant's text quoted, a String
     *  holding a brace or a bracket quoted ({@code \} and {@code '} escaped); any other its value's product text. */
    private static SqlExpr cell(SqlExpr c, Type t, SqlType slot) {
        if (slot instanceof SqlType.Tagged) {
            throw new com.legend.error.NotImplementedException("a relation's toString over a mixed column: its"
                    + " cells' kinds differ by row");
        }
        boolean variant = t instanceof Type.ClassType vc && PlatformTypes.isVariant(vc);
        SqlExpr rendered;
        if (variant) {
            rendered = quoted(valueText(t, c));
        } else if (t == Type.Primitive.STRING) {
            SqlExpr braced = SqlExpr.Call.of(SqlFn.OR, contains(c, "{"), contains(c, "["));
            rendered = new SqlExpr.Case(List.of(new SqlExpr.Case.When(braced, quoted(c))), c, SqlExpr.Reason.PRINT);
        } else {
            rendered = valueText(t, c);
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, c),
                new SqlExpr.StringLit(variant ? "'null'" : "null"))), rendered, SqlExpr.Reason.PRINT);
    }

    /** The product's text of a value of type {@code t}: the scalar toString rule's arms ({@link Scalars}), under the
     *  product's style. */
    private static SqlExpr valueText(Type t, SqlExpr x) {
        if (t == Type.Primitive.DATE_TIME) {
            return Styled.PRODUCT.dateTimes().dateTimeText(x, null);
        }
        if (t == Type.Primitive.FLOAT) {
            return Scalars.floatRepr(x);
        }
        return Scalars.pureToString(Styled.PRODUCT, t, x);
    }

    private static SqlExpr quoted(SqlExpr s) {
        return Scalars.cat(new SqlExpr.StringLit("'"),
                SqlExpr.Call.of(SqlFn.REPLACE, SqlExpr.Call.of(SqlFn.REPLACE, s, new SqlExpr.StringLit("\\"),
                        new SqlExpr.StringLit("\\\\")), new SqlExpr.StringLit("'"), new SqlExpr.StringLit("\\'")),
                new SqlExpr.StringLit("'"));
    }

    private static SqlExpr contains(SqlExpr s, String needle) {
        return SqlExpr.Call.of(SqlFn.GREATER, SqlExpr.Call.of(SqlFn.STRPOS, s, new SqlExpr.StringLit(needle)),
                new SqlExpr.IntLit(0));
    }
}
