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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A RELATION'S TEXT, COMPUTED BY THE DATABASE IN THE PRODUCT (stage B's B2): its {@code toString()} (B2b S5a,
 * 2026-11-24; the user's rulings of 2026-11-19 ~18:05, "Agree with all four table printing recommendations", and ~18:36,
 * "Null is good"; built from H14's spike, docs/records/compat-homework/H14-slice-shape-note.md on
 * claude/compat-table-text-db-spike) and the relational helper's {@code toCSV} (B2b S5b, 2026-11-25; the user's ruling
 * of 2026-11-19 ~20:05, "toCSV yes toString or another easy database sql that makes sense"; a date-time cell lite's
 * DateTime text, S312, relayed by the orchestrator 2026-11-25). Like any other function, the database computes the text
 * (tenet 1): one scalar subquery, a {@code STRING_AGG} of the relation's rows in the relation's own order. Each cell is
 * the PRODUCT's own text node of its value on every plan style ({@link Styled#PRODUCT}: the cells of a text used further
 * in a query are lite's text, the user's ~18:12 ruling, "Yes record it"; until B2b's S6, under the edge's style the edge
 * dialect's value passes re-spell a Float and a DateTime cell as they re-spell any such text there -- a window recorded
 * in docs/records/compat-b2-progress.md's S5a, pinned by FloatTextTest).
 * <ul>
 *   <li>{@code toString}: legend-pure's {@code #TDS} layout -- {@code #TDS}, the names line, one {@code '   '}-prefixed
 *       line per row with its cells joined {@code ,}, {@code #}; a name quoted unless {@code ^[a-zA-Z0-9_]+$}; an empty
 *       cell {@code null} ({@code 'null'} under a Variant); a Variant's text and a String holding a brace or a bracket
 *       quoted with {@code \} and {@code '} escaped; every other cell its scalar {@code toString()}.</li>
 *   <li>{@code toCSV}: the helper's layout -- the names joined {@code ,}, then one line per row with its cells joined
 *       {@code ,}, a newline after the names and after every row, each row's its own (no rows: the header line alone,
 *       where the helper writes a second newline, so a one-column relation's one null row is told from none: the
 *       user's ruling of 2026-11-25, Q-C, "Okay to both csv ones"; register S420); a name holding a comma, a quote or
 *       a line break quoted, its quotes doubled. Lite's own cells (the user's ruling of
 *       2026-11-25, "For csv we can do empty cell for null and "" or '' empty string and the toCSV(true) do null for
 *       null and quoted strings?"; register S420): under {@code toCSV(t)} a null is an empty field, an empty String
 *       {@code ""}, any other cell quoted where it holds a comma, a quote or a line break; under {@code toCSV(t, true)}
 *       a null is the word {@code null} and every String or Variant cell is quoted ({@code ""}, {@code "null"}), a
 *       number, a Boolean, a date or an enumeration as under {@code toCSV(t)}; the four-argument form, with formats,
 *       is refused by name (the orchestrator's rulings of 2026-11-25, Q-A and Q-B). A raw grid's CSV reads the grid's
 *       report when the plan runs, its names and its cells' texts (the planner-driver seam program's S3c, 2026-10-30, moved here from
 *       the edge with S5b: the keyed plan admits an integer's and a text's columns alone, whose database text is the
 *       product's).</li>
 * </ul>
 * Under {@code toCSV(t, true)} an enumeration's value named {@code null} is written {@code null}, as a null is: a
 * known collision, kept by the user's ruling of 2026-11-25 (Q-D, "Okay to both csv ones"; S420). Where a relation's
 * text is the program's last step, legend-pure's and legend-engine's doors print legend-pure's text in Java from the
 * rows instead ({@code edge.PrintedRelations}, B3's lift), for a relation whose columns the compiler knows. A raw grid's
 * {@code toCSV} is not lifted, so its CSV is this one at every door; a raw grid's {@code toString}, and the text of a
 * pivot whose columns nothing names, are refused by name here, so at every door (B2b's design audit of 2026-11-25
 * corrected "not lifted, so their text is this one"). No plan-style facet, no hook, no mode. A shape it cannot write is
 * refused by name.
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
        String what = "a relation's toString";
        if (known(rel, what).isLateBound()) {
            throw new com.legend.error.NotImplementedException(what + " over a raw grid (executeInDb): its columns exist"
                    + " only when the plan runs");
        }
        Map<String, Type.Column> byName = columns(rel, what);
        SqlSelect inner = relation.apply(rel);
        List<OutputCol> cols = inner.outputs();
        Render.Lifted lifted = ordered(inner, alias, what);
        StringBuilder header = new StringBuilder("#TDS\n   ");
        SqlExpr line = new SqlExpr.StringLit("   ");
        for (int i = 0; i < cols.size(); i++) {
            OutputCol oc = cols.get(i);
            Type.Column typed = typed(byName, oc, what);
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

    /** {@code tc}, the helper's {@code toCSV(t)} or {@code toCSV(t, renderTdsNull)} with a literal Boolean, as one
     *  scalar subquery; any other form refused by name -- the four-argument {@code toCSV(t, dateTimeFormat, dateFormat,
     *  renderTdsNull)} among them, as the product writes a date by its own text and never ignores what a program
     *  spelled out (the orchestrator's ruling of 2026-11-25, Q-A; register S420). */
    static SqlExpr csv(TypedNativeCall tc, Function<TypedSpec, SqlSelect> relation, String alias) {
        String what = "a relation's toCSV";
        boolean explicitNulls = switch (tc.args().size()) {
            case 1 -> false;
            case 2 -> {
                if (!(tc.args().get(1) instanceof TypedCBoolean b)) {
                    throw new com.legend.error.NotImplementedException(what + " whose renderTdsNull is no literal is"
                            + " not written by the product");
                }
                yield b.value();
            }
            case 4 -> throw new com.legend.error.NotImplementedException(what + " with date formats: the product writes"
                    + " a date by its own text; toCSV(t) or toCSV(t, renderTdsNull)");
            default -> throw new IllegalStateException(what + " of " + tc.args().size() + " arguments");
        };
        return csv(tc.args().get(0), explicitNulls, relation, alias, what);
    }

    /** A TDS's {@code csv} property ({@code tds.pure:19}, a relation's or a {@code #TDS} literal's): lite's own CSV,
     *  written exactly as {@code toCSV(t)} (the user's ruling of 2026-11-25, ".csv in product, yes same as toCSV";
     *  register S405) -- not legend-engine's text, which a last step's {@code csv} at legend-pure's and legend-engine's
     *  doors prints in Java ({@code edge.PrintedRelations}). A value whose columns the compiler does not know is refused
     *  by name, as toCSV's is ({@link #csv(TypedSpec, boolean, Function, String, String)}). */
    static SqlExpr tdsCsv(TypedSpec rel, Function<TypedSpec, SqlSelect> relation, String alias) {
        return csv(rel, false, relation, alias, "a TDS's csv");
    }

    /** {@code rel}'s CSV, as {@link #csv(TypedNativeCall, Function, String)} writes it: one scalar subquery. A value
     *  whose type names no relation schema -- a cast to {@code TDS<Any>} the resolver does not peel (over a chain it
     *  does not anchor: a {@code #TDS} literal's cast, {@code stringToTDS}) -- is refused by name (the review of
     *  d6e94cdfe, 2026-11-25: until then toCSV reached an internal error there). */
    private static SqlExpr csv(TypedSpec rel, boolean explicitNulls, Function<TypedSpec, SqlSelect> relation,
            String alias, String what) {
        known(rel, what);
        SqlSelect inner = relation.apply(rel);
        if (Type.relationSchema(rel.info().type()) instanceof Type.RelationType late && late.isLateBound()
                && inner.outputs().isEmpty()) {
            if (explicitNulls) {
                // the flag quotes a String cell alone, and a raw grid's cells reach the plan as texts whose types the
                // database reports only when the plan runs
                throw new com.legend.error.NotImplementedException(what + " with renderTdsNull over a raw grid"
                        + " (executeInDb): its columns' types exist only when the plan runs");
            }
            return new SqlExpr.ScalarSubquery(gridCsv(inner, alias));
        }
        Map<String, Type.Column> byName = columns(rel, what);
        List<OutputCol> cols = inner.outputs();
        Render.Lifted lifted = ordered(inner, alias, what);
        List<String> names = new ArrayList<>(cols.size());
        SqlExpr line = null;
        for (OutputCol oc : cols) {
            Type.Column typed = typed(byName, oc, what);
            if (oc.type() instanceof SqlType.Tagged) {
                throw new com.legend.error.NotImplementedException(what + " over a mixed column: its cells' kinds"
                        + " differ by row");
            }
            names.add(csvName(oc.name()));
            SqlExpr c = csvCell(SqlExpr.Column.of(alias, oc), typed.type(), explicitNulls);
            line = line == null ? c : Scalars.cat(line, new SqlExpr.StringLit(","), c);
        }
        // each row ends with its own newline (Q-C): no rows, the header line alone
        SqlExpr rows = SqlExpr.Call.of(SqlFn.COALESCE,
                new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG, List.of(Scalars.cat(java.util.Objects.requireNonNull(line),
                        new SqlExpr.StringLit("\n")), new SqlExpr.StringLit("")), false, lifted.keys()),
                new SqlExpr.StringLit(""));
        SqlExpr text = Scalars.cat(new SqlExpr.StringLit(String.join(",", names) + "\n"), rows);
        SqlSelect sel = SqlSelect.starOf(new SqlSource.Subselect(lifted.source(), alias, null))
                .withProjections(List.of(new SqlSelect.Projection(text, "csv",
                        new OutputCol("csv", SqlType.Scalar.VARCHAR, false))));
        return new SqlExpr.ScalarSubquery(sel);
    }

    /** A raw grid's CSV (the planner-driver seam program's S3c, 2026-10-30; moved from the edge with B2b S5b): the header
     *  the grid's reported column names ({@code COLUMNS}), each escaped, joined {@code ,}; each row its cells (the
     *  report's {@code TEXTS}, the database's text of each, read as a String cell: the keyed plan admits an integer's and
     *  a text's columns alone, whose database text is the product's) joined {@code ,} in the grid's order; a newline
     *  after the header and after every row, each row's its own (no rows: the header line alone, Q-C). Nothing is read
     *  at planning. */
    private static SqlSelect gridCsv(SqlSelect grid, String alias) {
        String n = alias + "_n";
        String c = alias + "_c";
        String l = alias + "_l";
        SqlSource.GridReport names = new SqlSource.GridReport(grid, SqlSource.GridReport.Part.COLUMNS, n);
        SqlSource.GridReport texts = new SqlSource.GridReport(grid, SqlSource.GridReport.Part.TEXTS, c);
        SqlExpr name = SqlExpr.Column.of(n, names.outputs(), SqlSource.GridReport.VALUE);
        SqlExpr header = new SqlExpr.ScalarSubquery(SqlSelect.starOf(names).withProjections(List.of(
                new SqlSelect.Projection(SqlExpr.Call.of(SqlFn.COALESCE, new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                        List.of(escaped(name), new SqlExpr.StringLit(",")), false, List.of(new SqlSelect.SortKey(
                                SqlExpr.Column.of(n, names.outputs(), SqlSource.GridReport.ORD), true, null, null))),
                        new SqlExpr.StringLit("")), "header",
                        new OutputCol("header", SqlType.Scalar.VARCHAR, false)))));
        // a cell's text read as a String's (an empty text is a String's: a number's text is never empty)
        SqlExpr text = csvCell(SqlExpr.Column.of(c, texts.outputs(), SqlSource.GridReport.VALUE), Type.Primitive.STRING,
                false);
        SqlExpr row = SqlExpr.Column.of(c, texts.outputs(), SqlSource.GridReport.ROW);
        OutputCol lineOut = new OutputCol("_csv_line", SqlType.Scalar.VARCHAR, false);
        OutputCol rowOut = new OutputCol(SqlSource.GridReport.ROW, SqlType.Scalar.BIGINT, false);
        SqlSelect lines = SqlSelect.starOf(texts).withProjections(List.of(
                new SqlSelect.Projection(row, SqlSource.GridReport.ROW, rowOut),
                new SqlSelect.Projection(new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                        List.of(text, new SqlExpr.StringLit(",")), false, List.of(new SqlSelect.SortKey(
                                SqlExpr.Column.of(c, texts.outputs(), SqlSource.GridReport.ORD), true, null, null))),
                        "_csv_line", lineOut)))
                .withGroupBy(List.of(row));
        SqlExpr nl = new SqlExpr.StringLit("\n");
        SqlExpr rowsJoined = SqlExpr.Call.of(SqlFn.COALESCE, new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                List.of(Scalars.cat(SqlExpr.Column.of(l, List.of(rowOut, lineOut), "_csv_line"), nl),
                        new SqlExpr.StringLit("")), false,
                List.of(new SqlSelect.SortKey(SqlExpr.Column.of(l, List.of(rowOut, lineOut),
                        SqlSource.GridReport.ROW), true, null, null))),
                new SqlExpr.StringLit(""));
        return SqlSelect.starOf(new SqlSource.Subselect(lines, l, null)).withProjections(List.of(
                new SqlSelect.Projection(Scalars.cat(header, nl, rowsJoined), "csv",
                        new OutputCol("csv", SqlType.Scalar.VARCHAR, false))));
    }

    /** {@code rel}'s relation schema; a value whose type names none -- a cast to {@code TDS<Any>} the resolver does not
     *  peel (over a chain it does not anchor: a {@code #TDS} literal's cast, {@code stringToTDS}) -- refused by name, by
     *  toString, toCSV and a TDS's csv alike (the review of d6e94cdfe, 2026-11-25, for the CSV; B2b's design audit of
     *  2026-11-25 for toString, which reached an internal error there). */
    private static Type.RelationType known(TypedSpec rel, String what) {
        Type.RelationType schema = Type.relationSchema(rel.info().type());
        if (schema == null) {
            throw new com.legend.error.NotImplementedException(what + " over a value whose columns the compiler does"
                + " not know (" + rel.info().type() + "): a relation's own columns are written, never a cast's");
        }
        return schema;
    }

    /** {@code rel}'s typed columns by name; a pivot whose columns nothing names refused by name. */
    private static Map<String, Type.Column> columns(TypedSpec rel, String what) {
        Map<String, Type.Column> byName = new HashMap<>();
        if (Type.relationSchema(rel.info().type()) instanceof Type.RelationType rt) {
            if (!rt.dynamicColumns().isEmpty()) {
                throw new com.legend.error.NotImplementedException(what + " over a pivot whose columns nothing names:"
                        + " its columns exist only once the database is asked");
            }
            for (Type.Column col : rt.columns()) {
                byName.put(col.name(), col);
            }
        }
        return byName;
    }

    /** {@code inner}'s order lifted over {@code alias} ({@link Render#liftOrder}); a relation whose plan names no
     *  column, and a DISTINCT or star select ordered by a key that is no column, refused by name. */
    private static Render.Lifted ordered(SqlSelect inner, String alias, String what) {
        if (inner.outputs().isEmpty()) {
            // a pivot's plan whose columns the lowering does not name (the edge's toCSV refused it by name as a dynamic
            // pivot's text until S5b): its columns exist only once the database is asked
            throw new com.legend.error.NotImplementedException(what + " over a relation whose plan names no column:"
                    + " its columns exist only once the database is asked");
        }
        Render.Lifted lifted = Render.liftOrder(inner, alias, what);
        if (lifted == null) {
            throw new com.legend.error.NotImplementedException(what + " over a DISTINCT or star select ordered by a"
                    + " key that is not one of its columns");
        }
        return lifted;
    }

    /** The typed relation column of the output {@code oc}. */
    private static Type.Column typed(Map<String, Type.Column> byName, OutputCol oc, String what) {
        Type.Column typed = byName.get(oc.name());
        if (typed == null) {
            throw new IllegalStateException(what + ": the output column '" + oc.name() + "' has no typed relation"
                    + " column");
        }
        return typed;
    }

    /** A column's name in the CSV header: quoted, its quotes doubled, where it holds a comma, a quote or a line break.
     *  A typed fact of the plan, written at planning (a fold of facts fixed by the types). */
    private static String csvName(String n) {
        return n.contains(",") || n.contains("\"") || n.contains("\n") || n.contains("\r")
                ? "\"" + n.replace("\"", "\"\"") + "\"" : n;
    }

    /** A CSV cell (the user's ruling of 2026-11-25, register S420): a null an empty field, or the word {@code null}
     *  under {@code explicitNulls} ({@code toCSV(t, true)}); a String or a Variant cell quoted under {@code explicitNulls},
     *  its quotes doubled (a Variant's JSON null is {@code "null"}, never the null word: the orchestrator's ruling of
     *  2026-11-25, Q-B); a number's, a Boolean's or a date's text as it is (it never holds a comma, a quote or a line break and is
     *  never empty: a fold of a fact fixed by the type, the review of S5b); any other cell's text quoted where it is
     *  empty or holds a comma, a quote or a line break ({@link #escaped}). */
    private static SqlExpr csvCell(SqlExpr c, Type t, boolean explicitNulls) {
        SqlExpr text = valueText(t, c);
        SqlExpr rendered = PLAIN_TEXT.contains(t) ? text
                : explicitNulls && (t == Type.Primitive.STRING || variant(t)) ? quotedCsv(text)
                : escaped(text);
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, c),
                new SqlExpr.StringLit(explicitNulls ? "null" : ""))), rendered, SqlExpr.Reason.PRINT);
    }

    /** Whether {@code t} is Pure's Variant. */
    private static boolean variant(Type t) {
        return t instanceof Type.ClassType vc && PlatformTypes.isVariant(vc);
    }

    /** The types whose text never holds a comma, a quote or a line break and is never empty. */
    private static final java.util.Set<Type> PLAIN_TEXT = java.util.Set.of(Type.Primitive.INTEGER, Type.Primitive.FLOAT,
            Type.Primitive.DECIMAL, Type.Primitive.NUMBER, Type.Primitive.BOOLEAN, Type.Primitive.DATE,
            Type.Primitive.STRICT_DATE, Type.Primitive.DATE_TIME, Type.Primitive.LATEST_DATE, Type.Primitive.STRICT_TIME);

    /** {@code text} quoted, its quotes doubled, where it is empty (an empty String {@code ""}, never the null's empty
     *  field) or holds a comma, a quote or a line break (RFC 4180's rule, the helper's {@code escapeCSVString}). The
     *  empty test reads the length: MariaDB's and MySQL's PAD SPACE collation holds {@code ' ' = ''} (the review of
     *  S5b's MUST-FIX: a String of spaces was written {@code ""}). */
    private static SqlExpr escaped(SqlExpr text) {
        SqlExpr special = SqlExpr.Call.of(SqlFn.OR,
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Call.of(SqlFn.LENGTH, text), new SqlExpr.IntLit(0)),
                SqlExpr.Call.of(SqlFn.OR, SqlExpr.Call.of(SqlFn.OR, contains(text, ","), contains(text, "\"")),
                        SqlExpr.Call.of(SqlFn.OR, contains(text, "\n"), contains(text, "\r"))));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(special, quotedCsv(text))), text, SqlExpr.Reason.PRINT);
    }

    /** {@code text} quoted, its quotes doubled. */
    private static SqlExpr quotedCsv(SqlExpr text) {
        return Scalars.cat(new SqlExpr.StringLit("\""),
                SqlExpr.Call.of(SqlFn.REPLACE, text, new SqlExpr.StringLit("\""), new SqlExpr.StringLit("\"\"")),
                new SqlExpr.StringLit("\""));
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
        boolean variant = variant(t);
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
