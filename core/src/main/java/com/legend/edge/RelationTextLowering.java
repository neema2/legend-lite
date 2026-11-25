// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.Type;
import com.legend.lowering.RelationTexts;
import com.legend.sql.DateFmt;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * A TDS'S {@code csv} TEXT, LEGEND-ENGINE'S, LOWERED (the separation census's B17, 2026-11-17: moved from the product's
 * {@code lowering.Render}, byte for byte, answering the plan style's {@link RelationTexts} facet): a TDS's {@code csv}
 * property, composed in SQL over the relation's rows. The cell rule ({@code toCSVString}): a NULL cell renders
 * {@code 'TDSNull'}; a date-kinded cell formats {@code yyyy-MM-dd}; a datetime formats then escapes; everything else is
 * {@code toString()->escapeCSVString()} with RFC4180 double-quote escaping. (A relation's {@code toString} and the
 * helper's {@code toCSV} left with stage B's B2b S5a, 2026-11-24, and S5b, 2026-11-25: lite's own texts, computed by the
 * database on every plan style, {@code lowering.ProductRelationText}, their Float and DateTime cells this edge's value
 * texts until S6; legend-pure's text of a last step is the doors' Java printer's, {@code edge.PrintedRelations}. This
 * class goes with S5c.)
 */
final class RelationTextLowering implements RelationTexts {

    @Override
    public SqlExpr csvProperty(com.legend.lowering.Styled styled, com.legend.compiler.spec.typed.TypedPropertyAccess read,
            java.util.function.Function<com.legend.compiler.spec.typed.TypedSpec, SqlSelect> relation, String alias) {
        return relationText(lowerTdsCsvProperty(styled, read, relation, alias), "a TDS's csv");
    }

    /** A relation's {@code text} ({@code what} it is): the text itself (B8, 2026-11-17: the RELATION_TEXT marker the
     *  edge's pass unwrapped is gone with the product's render-time refusal; the product refuses at lowering). */
    private static SqlExpr relationText(SqlExpr text, String what) {
        return text;
    }

    private static SqlExpr cat(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(SqlFn.CONCAT, out, parts[i]);
        }
        return out;
    }

    /** The toCSV default-format pair — the zero-arg format fns or their
     *  folded literal text: the door's lift reads it ({@link PrintedRelations}; the product's toCSV reads the text,
     *  {@code lowering.ProductRelationText}, since S5b). It moves to the lift with S5c. */
    static boolean isDefaultCsvFormat(
            com.legend.compiler.spec.typed.TypedSpec arg, String pattern) {
        if (arg instanceof com.legend.compiler.spec.typed.TypedCString cs) {
            return pattern.equals(cs.value());
        }
        return arg instanceof com.legend.compiler.spec.typed.TypedNativeCall f
                && f.args().isEmpty()
                && (f.callee().qualifiedName().equals(
                        "meta::pure::functions::date::SimpleDateTimeFormat")
                        ? pattern.startsWith("%t{yyyy-MM-dd HH")
                        : f.callee().qualifiedName().equals(
                                "meta::pure::functions::date::ISO8601DateFormat")
                                && pattern.equals("%t{yyyy-MM-dd}"));
    }

    /** The TDS class's {@code csv} property (tds.pure:19) over a relation:
     *  the engine's TDS csv text — header names joined {@code ', '},
     *  rows of ','-joined cells with NULL spelled TDSNull, lines joined
     *  '\n' and NO trailing newline (enumeration golden
     *  testEnumInRelation: {@code 'name, dateOfHire, type, active, firm,
     *  role\nAlice,1983-03-15,CONTRACT,YES,FIRM_A,JUNIOR\n...'}). */
    static SqlExpr lowerTdsCsvProperty(com.legend.lowering.Styled styled,
            com.legend.compiler.spec.typed.TypedPropertyAccess read,
            java.util.function.Function<
                    com.legend.compiler.spec.typed.TypedSpec, SqlSelect> relation,
            String alias) {
        SqlSelect inner = relation.apply(read.source());
        return new SqlExpr.ScalarSubquery(csv(styled, inner,
                typedColumns(inner, read.source()), true, alias, ", ", false));
    }

    /** The typed relation columns of {@code inner}'s outputs, by name. */
    private static List<Type.Column> typedColumns(SqlSelect inner,
            com.legend.compiler.spec.typed.TypedSpec rel) {
        java.util.Map<String, Type.Column> byName = new java.util.HashMap<>();
        if (Type.relationSchema(rel.info().type()) instanceof Type.RelationType rt) {
            for (Type.Column col : rt.columns()) {
                byName.put(col.name(), col);
            }
        }
        return inner.outputs().stream()
                .map(oc -> {
                    Type.Column t = byName.get(oc.name());
                    if (t == null) {
                        throw new IllegalStateException("csv: output column '"
                                + oc.name() + "' has no typed relation column");
                    }
                    return t;
                }).toList();
    }

    /** {@code headerSep}: the header's name separator (',' for toCSV,
     *  ', ' for the TDS csv property); {@code trailingNewline}: toCSV
     *  ends every row with '\n', the csv property does not. */
    private static SqlSelect csv(com.legend.lowering.Styled styled, SqlSelect inner, List<Type.Column> relCols,
            boolean renderTdsNull, String rowAlias, String headerSep,
            boolean trailingNewline) {
        List<OutputCol> cols = inner.outputs();
        inner = withSortOutputs(inner);
        if (cols.size() != relCols.size()) {
            throw new IllegalStateException("a TDS's csv: " + cols.size()
                    + " output columns vs " + relCols.size()
                    + " typed columns");
        }
        // ORDER: the engine renders rows in RESULT order. An ordered
        // inner select keeps its order INSIDE the aggregate; only plain
        // output-column keys can hoist — anything else is a loud wall,
        // never a silently unordered render.
        String aggAlias = rowAlias + "_a";
        if (cols.isEmpty()) {
            throw new IllegalStateException("a TDS's csv over a zero-column relation");
        }
        // the per-row line: cells joined ','
        SqlExpr line = cell(styled, SqlExpr.Column.of(rowAlias, cols.get(0)),
                relCols.get(0), cols.get(0).type(), renderTdsNull);
        for (int i = 1; i < cols.size(); i++) {
            line = cat(line, new SqlExpr.StringLit(","),
                    cell(styled, SqlExpr.Column.of(rowAlias, cols.get(i)),
                            relCols.get(i), cols.get(i).type(),
                            renderTdsNull));
        }
        List<SqlSelect.Projection> rowProjs = new ArrayList<>();
        rowProjs.add(new SqlSelect.Projection(line, "_csv_line",
                new OutputCol("_csv_line", SqlType.Scalar.VARCHAR, false)));
        List<SqlSelect.SortKey> aggOrder = hoistOrder(inner, cols,
                rowAlias, aggAlias, rowProjs);
        // the header line: names through the SAME escape expression
        SqlExpr header = escapeCsv(new SqlExpr.StringLit(cols.get(0).name()));
        for (int i = 1; i < cols.size(); i++) {
            header = cat(header, new SqlExpr.StringLit(headerSep),
                    escapeCsv(new SqlExpr.StringLit(cols.get(i).name())));
        }
        SqlExpr nl = new SqlExpr.StringLit("\n");
        // rows->joinStrings('', '\n', '\n'): string_agg(line, '\n') with
        // the hoisted order; ZERO rows aggregate to NULL -> '' (the
        // engine's empty joinStrings is prefix+suffix)
        SqlExpr rowsJoined = SqlExpr.Call.of(SqlFn.COALESCE,
                new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                        List.of(SqlExpr.Column.of(aggAlias, "_csv_line", SqlType.Scalar.VARCHAR, false, com.legend.sql.OutputCol.Origin.DERIVED), nl),
                        false, aggOrder),
                new SqlExpr.StringLit(""));
        SqlExpr text = trailingNewline ? cat(header, nl, rowsJoined, nl)
                : cat(header, nl, rowsJoined);
        SqlSelect rows = SqlSelect.starOf(
                        new SqlSource.Subselect(inner, rowAlias, null))
                .withProjections(rowProjs);
        return SqlSelect.starOf(new SqlSource.Subselect(rows, aggAlias, null))
                .withProjections(List.of(new SqlSelect.Projection(text, "csv",
                        new OutputCol("csv", SqlType.Scalar.VARCHAR, false))));
    }

    /** The engine {@code s()} cell (s.pure:23-38). */
    /** THE per-type cell PRINT FORMS — one owner (audit 2026-08-19:
     * tdsCell and pctCell were drifting twins; tdsCell lacked the
     * DateTime arm pctCell carried — the third-implementation disease
     * in miniature). Channel-specific QUOTING/ESCAPING stays with each
     * renderer; the SPELLING of a value lives here once. */
    private static SqlExpr dateTimeText(SqlExpr c) {
        // the one owner (docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §4, 2026-10-18): Pure's text of a DateTime read
        // from a database -- 'T', the fraction's digits with trailing zeros trimmed, '+0000' -- on every output
        // (the fixed three millisecond digits, "the engine's own wire spelling", and the value root's own
        // whole-second case are both gone into it)
        return EdgeDateTimeTexts.INSTANCE.dateTimeText(c, null);
    }

    private static SqlExpr strictDateText(SqlExpr c) {
        return SqlExpr.Call.of(SqlFn.STRFTIME, c,
                new SqlExpr.FormatLit(com.legend.sql.DateFmt.DATE));
    }

    /** A cell's text over a tagged column ({@link SqlType.Tagged}, docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a):
     *  {@code CASE WHEN tag = i THEN arm(slot column i, kind i, slot type i) ... END}, the slot columns read beside
     *  the tag's ({@link SqlType.Tagged#slotColumn}). */
    private static SqlExpr perSlot(SqlExpr tag, SqlType.Tagged tg, SlotArm arm) {
        if (!(tag instanceof SqlExpr.Column tc)) {
            throw new IllegalStateException("a tagged cell is read through its column, whose slot columns stand"
                    + " beside it; not through " + tag);
        }
        List<SqlExpr.Case.When> whens = new ArrayList<>();
        for (int i = 0; i < tg.slots().size(); i++) {
            SqlType.Tagged.Slot slot = tg.slots().get(i);
            SqlExpr ref = SqlExpr.Column.of(tc.table(), SqlType.Tagged.slotColumn(tc.name(), i), slot.type(), true,
                    com.legend.sql.OutputCol.Origin.DERIVED);
            whens.add(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, tag, new SqlExpr.IntLit(i)),
                    arm.apply(ref, RelationTexts.kindType(slot.kind()), slot.type())));
        }
        return new SqlExpr.Case(whens, null, SqlExpr.Reason.PRINT);
    }

    @FunctionalInterface
    private interface SlotArm {
        SqlExpr apply(SqlExpr ref, Type kind, SqlType slot);
    }

    /** The DATE family: StrictDate, Date, DateTime, LatestDate. */
    private static boolean dateKind(Type t) {
        return t == Type.Primitive.STRICT_DATE || t == Type.Primitive.DATE || t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.LATEST_DATE;
    }

    /** A slot that carries text: VARCHAR, a temporal's text. */
    private static boolean textSlot(@com.legend.base.Nullable SqlType slot) {
        return slot == SqlType.Scalar.VARCHAR || slot == SqlType.Scalar.TEMPORAL_TEXT;
    }

    /** A relation text's cell as legend-pure PRINTS it: a Float its shortest round-trip text, {@link
     *  SqlFn#FLOAT_SHORTEST_TEXT} (legend-pure's toString of a TDS cell, DecimalFormat "0.0": 0.00001, 25000000.0),
     *  never the in-query cast the edge writes for a Float's toString ({@link SqlFn#FLOAT_TO_TEXT} as legend-engine's
     *  CAST AS VARCHAR, edge.PurePrintForms: 1e-05 on DuckDB, 1.0E-5 on H2). PR #118 fix round, 2026-11-18: commit
     *  237bd8e5f (Phase 5 row 8) applied the in-query cast to every FLOAT_TO_TEXT node, the relation text's cells too. */
    private static SqlExpr printedCell(SqlExpr text) {
        return text instanceof SqlExpr.Call c && c.fn() == SqlFn.FLOAT_TO_TEXT
                ? SqlExpr.Call.of(SqlFn.FLOAT_SHORTEST_TEXT, c.args().get(0)) : text;
    }

    /** {@code inner} with every sort key among its outputs: a key the projection dropped ({@code SELECT a FROM
     *  ... ORDER BY h}, legal SQL whose order the hoist above must carry from outside the row subselect: the
     *  Cypher island's ORDER BY an expression it does not return, 2026-10-17) is projected as a hidden
     *  {@code _sortN} column, which the hoist carries and the rows' rendering, built from the outputs read
     *  before, never shows. The key's type is its expression's typed fact; an untyped key walls, as before. A
     *  star frame (no projections) is left as it is. */
    private static SqlSelect withSortOutputs(SqlSelect inner) {
        if (inner.orderBy().isEmpty() || inner.projections().isEmpty()) {
            return inner;
        }
        List<SqlSelect.Projection> ps = new ArrayList<>(inner.projections());
        List<OutputCol> outs = new ArrayList<>(inner.outputs());
        List<SqlSelect.SortKey> keys = new ArrayList<>();
        int n = 0;
        for (SqlSelect.SortKey k : inner.orderBy()) {
            String out = k.outputName() != null ? k.outputName()
                    : k.expr() instanceof SqlExpr.Column c ? c.name() : null;
            if (out != null && inner.outputs().stream().anyMatch(oc -> oc.name().equals(out))) {
                keys.add(k);
                continue;
            }
            if (!(k.expr().type() instanceof com.legend.sql.TypeFact.Typed t)) {
                throw new com.legend.error.NotImplementedException(
                        "render over an ordered relation whose sort key is"
                        + " not an output column and carries no type");
            }
            String name = "_sort" + n++;
            OutputCol oc = new OutputCol(name, t.type(), t.nullable());
            ps.add(new SqlSelect.Projection(k.expr(), name, oc));
            outs.add(oc);
            keys.add(new SqlSelect.SortKey(k.expr(), k.ascending(), k.nullOrder(), name));
        }
        return n == 0 ? inner : inner.withProjections(ps, outs).withOrderBy(keys);
    }

    /** RESULT-ORDER hoist: an ordered inner select keeps its order
     *  INSIDE the aggregate; only plain output-column keys hoist —
     *  anything else walls loudly, never a silently unordered render.
     *  {@code carry} (when non-null) receives the pass-through
     *  projections/outputs the row subselect must carry. */
    @SuppressWarnings("unchecked")
    private static List<SqlSelect.SortKey> hoistOrder(SqlSelect inner,
            List<OutputCol> cols, String rowAlias, String aggAlias,
            @com.legend.base.Nullable List<SqlSelect.Projection> carry) {
        List<SqlSelect.SortKey> aggOrder = new ArrayList<>();
        int ord = 0;
        for (SqlSelect.SortKey k : inner.orderBy()) {
            String out = k.outputName() != null ? k.outputName()
                    : k.expr() instanceof SqlExpr.Column c ? c.name() : null;
            // the inner's own outputs: a hidden sort column (withSortOutputs) is one, not one of cols
            OutputCol src = out == null ? null : inner.outputs().stream()
                    .filter(oc -> oc.name().equals(out)).findFirst()
                    .orElse(null);
            if (src == null) {
                throw new com.legend.error.NotImplementedException(
                        "render over an ordered relation whose sort key is"
                        + " not an output column");
            }
            String oname = "_ord" + ord++;
            if (carry != null) {
                // E2E audit: the hoist carry transports all four
                // dimensions — pair-native, the slot rides its projection
                carry.add(new SqlSelect.Projection(
                        SqlExpr.Column.of(rowAlias, src), oname,
                        new OutputCol(oname, src.type(), src.nullable())));
            }
            aggOrder.add(new SqlSelect.SortKey(
                    SqlExpr.Column.of(aggAlias, oname, src.type(),
                            src.nullable(),
                            com.legend.sql.OutputCol.Origin.DERIVED),
                    k.ascending(),
                    k.nullOrder(), null));
        }
        return aggOrder;
    }

    /** One cell — the engine's {@code toCSVString} dispatch, typed.
     *  (A count-based render for to-many cells was tried and DELETED:
     *  zero live corpus firings, and its LIST_GET/LIST_LENGTH emissions
     *  violate the carrier-purity tenet — when a to-many cell demand
     *  appears it gets a SEMANTIC node with dialect strategies, per
     *  CARRIER_REDESIGN.md tenet #1. The named residue is
     *  docs/CSV_DIFFERENTIAL.md mechanism 3.) */
    private static SqlExpr cell(com.legend.lowering.Styled styled, SqlExpr c, Type.Column col,
            SqlType slot, boolean renderTdsNull) {
        Type t = col.type();
        SqlExpr rendered;
        if (slot instanceof SqlType.Tagged tg) {
            // a mixed selection's winner: the slot the tag names, its cell by its own kind
            return perSlot(c, tg, (ref, kind, st) -> cell(styled, ref,
                    new Type.Column(col.name(), kind, col.multiplicity()), st, renderTdsNull));
        }
        if (dateKind(t) && textSlot(slot)) {
            // a date carried as its own text (a value root spelled at the precision it has): the text is the cell,
            // a timed one with Pure's '+0000' (the one owner's rule, 2026-10-18)
            rendered = dateTimeTextOfText(c);
        } else if (t == Type.Primitive.STRICT_DATE
                || (t == Type.Primitive.DATE
                        && slot == SqlType.Scalar.DATE)) {
            // date-only branch: format WITHOUT escape (engine
            // formatDateTime escapes only the datetime arm). An ABSTRACT
            // Date over a DATE slot is a StrictDate READ (the engine's
            // relational read of a DATE column yields hasHour=false) —
            // the SLOT KIND is the typed fact, F5.4's rule.
            rendered = SqlExpr.Call.of(SqlFn.STRFTIME, c,
                    new SqlExpr.FormatLit(DateFmt.DATE));
        } else if (t == Type.Primitive.DATE) {
            // ABSTRACT Date over a TIMESTAMP slot: the engine's formatDateTime rule, defined over the value
            // (timed); the JSON wires spell the same (dateText)
            rendered = new SqlExpr.Case(List.of(new SqlExpr.Case.When(timed(c), dateTimeText(c))),
                    strictDateText(c), SqlExpr.Reason.PRINT);
        } else if (t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.LATEST_DATE) {
            // the DateTime kinds: Pure's DateTime text (the one owner, 2026-10-18; the engine's own CSV wrote
            // 'yyyy-MM-dd HH:mm:ss', the fraction dropped: docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §3, gap 4)
            rendered = dateTimeText(c);
        } else {
            rendered = escapeCsv(printedCell(RelationTexts.pureToString(styled, t, c)));
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, c),
                new SqlExpr.StringLit(renderTdsNull ? "TDSNull" : ""))),
                rendered, SqlExpr.Reason.PRINT);
    }

    /** THE RFC4180 escape rule ({@code escapeCSVString}), spelled once:
     *  quote when the text contains comma, quote, LF or CR; embedded
     *  quotes double. */
    private static SqlExpr escapeCsv(SqlExpr s) {
        SqlExpr needs = or(contains(s, ","), or(contains(s, "\""),
                or(contains(s, "\n"), contains(s, "\r"))));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(needs,
                cat(new SqlExpr.StringLit("\""),
                        SqlExpr.Call.of(SqlFn.REPLACE, s,
                                new SqlExpr.StringLit("\""),
                                new SqlExpr.StringLit("\"\"")),
                        new SqlExpr.StringLit("\"")))),
                s, SqlExpr.Reason.PRINT);
    }

    private static SqlExpr contains(SqlExpr s, String needle) {
        return SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, s,
                        new SqlExpr.StringLit(needle)),
                new SqlExpr.IntLit(0));
    }

    private static SqlExpr or(SqlExpr a, SqlExpr b) {
        return SqlExpr.Call.of(SqlFn.OR, a, b);
    }

    /** A date value carried as its own text (a value root spelled at the precision it has: a year, a month, a day,
     *  an hour, a minute, a second or finer; with or without Pure's {@code %}; a literal's print form with its
     *  {@code +0000} already): one with seconds and no zone takes Pure's {@code +0000}, as Pure prints a DateTime;
     *  every other precision is its text, Pure's DateTime literal taking a zone only after seconds (the MongoDB
     *  lane, 2026-10-18: {@code %1973-11-13T23} and {@code %2025-02-10T20:10:20+0000} are read as written). */
    // the zone a written date-time text gets in SQL (register S121, pending: legend-pure writes +0000 from the minute)
    private static SqlExpr dateTimeTextOfText(SqlExpr text) {
        SqlExpr colons = SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, text),
                SqlExpr.Call.of(SqlFn.LENGTH, SqlExpr.Call.of(SqlFn.REPLACE, text, new SqlExpr.StringLit(":"),
                        new SqlExpr.StringLit(""))));
        SqlExpr secondsNoZone = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL, colons, new SqlExpr.IntLit(2)),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.ENDS_WITH, text, new SqlExpr.StringLit("+0000"))));
        return new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(secondsNoZone,
                SqlExpr.Call.of(SqlFn.CONCAT, text, new SqlExpr.StringLit("+0000")))), text, SqlExpr.Reason.PRINT);
    }

    /** Whether a timestamp's time of day is not midnight: the engine's {@code formatDateTime} rule for an ABSTRACT
     *  Date, defined over the value (hasHour) -- a timed value is a DateTime, a date-only one its day. */
    private static SqlExpr timed(SqlExpr c) {
        return SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                SqlExpr.Call.of(SqlFn.STRFTIME, c, new SqlExpr.FormatLit(DateFmt.TIME_ONLY)),
                new SqlExpr.StringLit("00:00:00"));
    }
}
