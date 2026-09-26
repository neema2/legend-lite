// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.DateFmt;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;

/**
 * The RENDER phase (F4.1/F4.2, foundations Phase 4): result text is a
 * PLAN PROJECTION the database executes — Java never touches the value
 * bytes (the graph-fetch precedent, generalized; charter clauses 1–3:
 * headers/envelopes come from typed plan facts, only the LiteralFold-
 * admitted kinds may render in Java, everything else is byte transport).
 *
 * <p>This first form is the engine's {@code meta::relational::tests::
 * csv::toCSV} text (helperFunctions.pure:198-232), constructed in SQL:
 * <pre>
 *   header-names(each toCSVString)->joinStrings(',') + '\n'
 *   + rows(cells joined ',')->joinStrings('', '\n', '\n')
 * </pre>
 * Cell rule ({@code toCSVString}): a NULL cell renders {@code ''}
 * ({@code 'TDSNull'} under renderTdsNull); a date-kinded cell formats
 * {@code yyyy-MM-dd} (no escape — the engine escapes only the datetime
 * branch); a datetime formats {@code yyyy-MM-dd HH:mm:ss} then escapes;
 * everything else is {@code toString()->escapeCSVString()} with
 * RFC4180 double-quote escaping. The escape rule is spelled ONCE below
 * — the header goes through the SAME SQL expression over literal names
 * so no second copy exists in Java.
 */
public final class Render {

    private Render() {
    }

    /** The Lowerer's toCSV dispatch target: arity/format admission +
     *  typed-column collection + the render wrap. The 4-arg overload
     *  accepts only the DEFAULT format pair (SimpleDateTimeFormat /
     *  ISO8601DateFormat, as the zero-arg calls or their folded
     *  '%t{...}' literals); anything else stays loud. */
    static SqlExpr lowerToCsv(
            com.legend.compiler.spec.typed.TypedNativeCall tc,
            java.util.function.Function<
                    com.legend.compiler.spec.typed.TypedSpec, SqlSelect> relation,
            String alias,
            java.util.Map<Integer, Type.RelationType> deferredTds) {
        boolean renderTdsNull = switch (tc.args().size()) {
            case 1 -> false;
            case 2, 4 -> {
                com.legend.compiler.spec.typed.TypedSpec last =
                        tc.args().get(tc.args().size() - 1);
                if (!(last instanceof
                        com.legend.compiler.spec.typed.TypedCBoolean b)) {
                    throw new com.legend.error.NotImplementedException(
                            "toCSV: renderTdsNull must be a literal");
                }
                if (tc.args().size() == 4
                        && (!isDefaultCsvFormat(tc.args().get(1),
                                "%t{yyyy-MM-dd HH:mm:ss}")
                            || !isDefaultCsvFormat(tc.args().get(2),
                                "%t{yyyy-MM-dd}"))) {
                    throw new com.legend.error.NotImplementedException(
                            "toCSV: only the default date formats are lowered");
                }
                yield b.value();
            }
            default -> throw new com.legend.error.NotImplementedException(
                    "toCSV: unexpected arity " + tc.args().size());
        };
        SqlSelect inner = relation.apply(tc.args().get(0));
        // a LATE-BOUND inner (a raw grid: executeInDbToTDS — batch 82) or
        // a pivot inner: the column list exists only at the execution
        // boundary — DEFER the CSV composition exactly like the '#TDS'
        // toString (resolveDeferredTds composes by form)
        if ((inner.outputs().isEmpty() || hasDynamicPivot(inner))
                && Type.relationSchema(tc.args().get(0).info().type())
                        instanceof Type.RelationType drt) {
            int id = deferredTds.size();
            deferredTds.put(id, drt);
            return new SqlExpr.DeferredTdsString(inner, alias, id,
                    SqlExpr.DeferredTdsString.Form.CSV, renderTdsNull);
        }
        java.util.Map<String, Type.Column> byName = new java.util.HashMap<>();
        if (Type.relationSchema(tc.args().get(0).info().type())
                instanceof Type.RelationType rt) {
            for (Type.Column col : rt.columns()) {
                byName.put(col.name(), col);
            }
        }
        List<Type.Column> relCols = inner.outputs().stream()
                .map(oc -> {
                    Type.Column t = byName.get(oc.name());
                    if (t == null) {
                        throw new IllegalStateException("toCSV: output column '"
                                + oc.name() + "' has no typed relation column");
                    }
                    return t;
                }).toList();
        return new SqlExpr.ScalarSubquery(
                csv(inner, relCols, renderTdsNull, alias));
    }

    /** The toCSV default-format pair — the zero-arg format fns or their
     *  folded literal text. */
    private static boolean isDefaultCsvFormat(
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

    /** The whole toCSV text as a one-column scalar select over the
     *  relation plan. {@code colTypes} are the relation's PURE column
     *  types in output order (dates dispatch by KIND — F5.4: the kind
     *  is a typed fact, never a value sniff). */
    static SqlSelect csv(SqlSelect inner, List<Type.Column> relCols,
            boolean renderTdsNull, String rowAlias) {
        return csv(inner, relCols, renderTdsNull, rowAlias, ",", true);
    }

    /** The TDS class's {@code csv} property (tds.pure:19) over a relation:
     *  the engine's TDS csv text — header names joined {@code ', '},
     *  rows of ','-joined cells with NULL spelled TDSNull, lines joined
     *  '\n' and NO trailing newline (enumeration golden
     *  testEnumInRelation: {@code 'name, dateOfHire, type, active, firm,
     *  role\nAlice,1983-03-15,CONTRACT,YES,FIRM_A,JUNIOR\n...'}). */
    /** The csv property's receiver is a relation with a schema (the csv
     * text arm's guard). */
    static boolean csvOverRelation(com.legend.compiler.spec.typed.TypedPropertyAccess read) {
        return Type.relationSchema(read.source().info().type()) != null;
    }

    static SqlExpr lowerTdsCsvProperty(
            com.legend.compiler.spec.typed.TypedPropertyAccess read,
            java.util.function.Function<
                    com.legend.compiler.spec.typed.TypedSpec, SqlSelect> relation,
            String alias) {
        SqlSelect inner = relation.apply(read.source());
        return new SqlExpr.ScalarSubquery(csv(inner,
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
    private static SqlSelect csv(SqlSelect inner, List<Type.Column> relCols,
            boolean renderTdsNull, String rowAlias, String headerSep,
            boolean trailingNewline) {
        List<OutputCol> cols = inner.outputs();
        if (cols.size() != relCols.size()) {
            throw new IllegalStateException("toCSV: " + cols.size()
                    + " output columns vs " + relCols.size()
                    + " typed columns");
        }
        // ORDER: the engine renders rows in RESULT order. An ordered
        // inner select keeps its order INSIDE the aggregate; only plain
        // output-column keys can hoist — anything else is a loud wall,
        // never a silently unordered render.
        String aggAlias = rowAlias + "_a";
        if (cols.isEmpty()) {
            throw new IllegalStateException("toCSV over a zero-column relation");
        }
        // the per-row line: cells joined ','
        SqlExpr line = cell(SqlExpr.Column.of(rowAlias, cols.get(0)),
                relCols.get(0), cols.get(0).type(), renderTdsNull);
        for (int i = 1; i < cols.size(); i++) {
            line = cat(line, new SqlExpr.StringLit(","),
                    cell(SqlExpr.Column.of(rowAlias, cols.get(i)),
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

    /** The PRODUCT CSV wire (E5): RFC 4180 — CRLF line endings, header
     *  of escaped names, NULL cells render empty, every row (last
     *  included) ends CRLF; a zero-row result is the header line only.
     *  Same {@link #cell}/{@link #escapeCsv} owners as toCSV — the
     *  escape rule stays spelled once. */
    static SqlSelect csvWire(SqlSelect inner, List<Type.Column> relCols,
            String rowAlias) {
        List<OutputCol> cols = inner.outputs();
        if (cols.size() != relCols.size()) {
            throw new IllegalStateException("csv wire: " + cols.size()
                    + " output columns vs " + relCols.size()
                    + " typed columns");
        }
        if (cols.isEmpty()) {
            throw new IllegalStateException(
                    "csv wire over a zero-column relation");
        }
        String aggAlias = rowAlias + "_a";
        SqlExpr line = cell(SqlExpr.Column.of(rowAlias, cols.get(0)),
                relCols.get(0), cols.get(0).type(), false);
        for (int i = 1; i < cols.size(); i++) {
            line = cat(line, new SqlExpr.StringLit(","),
                    cell(SqlExpr.Column.of(rowAlias, cols.get(i)),
                            relCols.get(i), cols.get(i).type(), false));
        }
        List<SqlSelect.Projection> rowProjs = new ArrayList<>();
        rowProjs.add(new SqlSelect.Projection(line, "_csv_line",
                new OutputCol("_csv_line", SqlType.Scalar.VARCHAR, false)));
        List<SqlSelect.SortKey> aggOrder = hoistOrder(inner, cols,
                rowAlias, aggAlias, rowProjs);
        SqlExpr header = escapeCsv(new SqlExpr.StringLit(cols.get(0).name()));
        for (int i = 1; i < cols.size(); i++) {
            header = cat(header, new SqlExpr.StringLit(","),
                    escapeCsv(new SqlExpr.StringLit(cols.get(i).name())));
        }
        SqlExpr crlf = new SqlExpr.StringLit("\r\n");
        SqlExpr agg = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                List.of(SqlExpr.Column.of(aggAlias, "_csv_line", SqlType.Scalar.VARCHAR, false, com.legend.sql.OutputCol.Origin.DERIVED), crlf),
                false, aggOrder);
        // an explicit IS NULL dispatch — concat() SKIPS null args, so a
        // coalesce over the concatenation could never see the null
        SqlExpr text = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, agg), cat(header, crlf))),
                cat(header, crlf, agg, crlf));
        SqlSelect rows = SqlSelect.starOf(
                        new SqlSource.Subselect(inner, rowAlias, null))
                .withProjections(rowProjs);
        return SqlSelect.starOf(new SqlSource.Subselect(rows, aggAlias, null))
                .withProjections(List.of(new SqlSelect.Projection(text, "wire",
                        new OutputCol("wire", SqlType.Scalar.VARCHAR, false))));
    }

    /** The per-row product JSON object ({@code json_object} of the
     *  output columns — the DATABASE'S value policy: numbers and
     *  booleans native, dates as their SQL text, strings RFC 8259
     *  escaped by the DB). One VARCHAR {@code _wire_row} column; the
     *  plan's ORDER BY lifts onto the wrapper (output-column keys only,
     *  loud otherwise). */
    static SqlSelect jsonWireRows(SqlSelect inner, String rowAlias) {
        List<OutputCol> cols = inner.outputs();
        List<SqlExpr> kv = new ArrayList<>(cols.size() * 2);
        for (OutputCol oc : cols) {
            kv.add(new SqlExpr.StringLit(oc.name()));
            kv.add(SqlExpr.Column.of(rowAlias, oc));
        }
        SqlExpr obj = new SqlExpr.Cast(new SqlExpr.JsonObject(kv),
                SqlType.Scalar.VARCHAR);
        List<SqlSelect.SortKey> lifted = new ArrayList<>();
        for (SqlSelect.SortKey k : inner.orderBy()) {
            String out = k.outputName() != null ? k.outputName()
                    : k.expr() instanceof SqlExpr.Column c ? c.name() : null;
            OutputCol outCol = out == null ? null : cols.stream()
                    .filter(oc -> oc.name().equals(out)).findFirst()
                    .orElse(null);
            if (outCol == null) {
                throw new com.legend.error.NotImplementedException(
                        "json wire over an ordered relation whose sort key"
                        + " is not an output column");
            }
            lifted.add(new SqlSelect.SortKey(
                    SqlExpr.Column.of(rowAlias, outCol), k.ascending(),
                    k.nullOrder(), null));
        }
        return SqlSelect.starOf(new SqlSource.Subselect(inner, rowAlias, null))
                .withProjections(List.of(new SqlSelect.Projection(obj, "_wire_row",
                        new OutputCol("_wire_row", SqlType.Scalar.VARCHAR, false))))
                .withOrderBy(lifted);
    }

    /** The SNAPSHOT product JSON wire: the whole result as one
     *  {@code [row,row,…]} text — {@code string_agg} over the per-row
     *  objects with hoisted order; ZERO rows are the empty array (the
     *  pinned {@code []} wire). */
    static SqlSelect jsonWire(SqlSelect inner, String rowAlias) {
        List<OutputCol> cols = inner.outputs();
        String aggAlias = rowAlias + "_a";
        List<SqlExpr> kv = new ArrayList<>(cols.size() * 2);
        for (OutputCol oc : cols) {
            kv.add(new SqlExpr.StringLit(oc.name()));
            kv.add(SqlExpr.Column.of(rowAlias, oc));
        }
        SqlExpr obj = new SqlExpr.Cast(new SqlExpr.JsonObject(kv),
                SqlType.Scalar.VARCHAR);
        List<SqlSelect.Projection> rowProjs = new ArrayList<>();
        rowProjs.add(new SqlSelect.Projection(obj, "_wire_row",
                new OutputCol("_wire_row", SqlType.Scalar.VARCHAR, false)));
        List<SqlSelect.SortKey> aggOrder = hoistOrder(inner, cols,
                rowAlias, aggAlias, rowProjs);
        SqlExpr agg = new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                List.of(SqlExpr.Column.of(aggAlias, "_wire_row", SqlType.Scalar.VARCHAR, false, com.legend.sql.OutputCol.Origin.DERIVED),
                        new SqlExpr.StringLit(",")),
                false, aggOrder);
        SqlExpr text = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, agg),
                new SqlExpr.StringLit("[]"))),
                cat(new SqlExpr.StringLit("["), agg,
                        new SqlExpr.StringLit("]")));
        SqlSelect rows = SqlSelect.starOf(
                        new SqlSource.Subselect(inner, rowAlias, null))
                .withProjections(rowProjs);
        return SqlSelect.starOf(new SqlSource.Subselect(rows, aggAlias, null))
                .withProjections(List.of(new SqlSelect.Projection(text, "wire",
                        new OutputCol("wire", SqlType.Scalar.VARCHAR, false))));
    }

    /** The Lowerer's relation-toString dispatch target (engine
     *  toString.pure:19-35): the '#TDS' text form, built by the DB.
     *  typesAndMuls=true stays loud until a demand names it. */
    static SqlExpr lowerToString(
            com.legend.compiler.spec.typed.TypedNativeCall tc,
            java.util.function.Function<
                    com.legend.compiler.spec.typed.TypedSpec, SqlSelect> relation,
            String alias,
            java.util.Map<Integer, Type.RelationType> deferredTds) {
        if (tc.args().size() == 2
                && (!(tc.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCBoolean b)
                        || b.value())) {
            throw new com.legend.error.NotImplementedException(
                    "relation toString(typesAndMuls) is lowered only for"
                    + " the literal-false form");
        }
        SqlSelect inner = relation.apply(tc.args().get(0));
        // DYNAMIC-PIVOT inner: the column list only exists at the
        // execution boundary (DynamicPivot's two-phase discipline) —
        // DEFER the '#TDS' composition; the boundary resolver rebuilds
        // it from the LIMIT-0 probe + this typed schema.
        if (hasDynamicPivot(inner)
                && Type.relationSchema(tc.args().get(0).info().type())
                        instanceof Type.RelationType drt) {
            int id = deferredTds.size();
            deferredTds.put(id, drt);
            return new SqlExpr.DeferredTdsString(inner, alias, id);
        }
        java.util.Map<String, Type.Column> byName = new java.util.HashMap<>();
        if (Type.relationSchema(tc.args().get(0).info().type())
                instanceof Type.RelationType rt) {
            for (Type.Column col : rt.columns()) {
                byName.put(col.name(), col);
            }
        }
        List<Type.Column> relCols = inner.outputs().stream()
                .map(oc -> {
                    Type.Column t = byName.get(oc.name());
                    if (t == null) {
                        throw new IllegalStateException("toString: output"
                                + " column '" + oc.name()
                                + "' has no typed relation column");
                    }
                    return t;
                }).toList();
        return new SqlExpr.ScalarSubquery(
                tdsString(inner, relCols, alias));
    }

    /** Whether the select tree contains a PIVOT — static or dynamic:
     * either way the pivot's output columns are absent from the
     * lowered outputs (witness test_Static_Pivot_Filter, whose '#TDS'
     * lost the pivot columns exactly like the dynamic case), so the
     * composition defers to the boundary probe. */
    private static boolean hasDynamicPivot(SqlQuery q) {
        if (!(q instanceof SqlSelect sel)) {
            return q instanceof com.legend.sql.SqlUnion u
                    && u.branches().stream().anyMatch(Render::hasDynamicPivot);
        }
        return sourceHasDynamicPivot(sel.from());
    }

    private static boolean sourceHasDynamicPivot(SqlSource s) {
        return switch (s) {
            case SqlSource.Pivot p -> true;
            case SqlSource.Subselect sub -> hasDynamicPivot(sub.inner());
            case SqlSource.Join j -> sourceHasDynamicPivot(j.left())
                    || sourceHasDynamicPivot(j.right());
            default -> false;
        };
    }

    /** The WHOLE-PLAN deferred-TDS pass (called by the K-orchestrator
     * after staticize — the dynamic-pivot two-phase discipline's second
     * consumer): every {@link SqlExpr.DeferredTdsString} resolves via
     * the supplied LIMIT-0 probe (a SCHEMA read the exec layer
     * performs; THIS layer owns the composition — invariant 6d: exec
     * never calls the middle-end, so the middle-end hosts the pass and
     * takes the probe as a function). */
    public static SqlQuery resolveAllDeferredTds(SqlQuery plan,
            java.util.Map<Integer, Type.RelationType> registry,
            java.util.function.Function<SqlSelect,
                    com.legend.sql.PlanProbe> probe,
            java.util.function.Function<String, Type> sqlTypeToPure) {
        return new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                if (!(e instanceof SqlExpr.DeferredTdsString d)) {
                    return e;
                }
                com.legend.sql.PlanProbe p = probe.apply(d.inner());
                Type.RelationType schema = java.util.Objects.requireNonNull(
                        registry.get(d.id()), "deferred-TDS id " + d.id()
                                + " has no registered schema");
                return resolveDeferredTds(d, p.outs(), name -> {
                    Type t = schema.pivotColumnType(name);
                    // downstream-rebuilt schemas drop the pivot
                    // templates (filter/select over the pivot) — the
                    // probe's SQL type decodes, the PctTdsWrap pattern
                    return t != null ? t
                            : sqlTypeToPure.apply(String.valueOf(
                                    p.typeNames().get(name)));
                });
            }
        }.rewrite(plan);
    }

    /** THE BOUNDARY RESOLUTION of a deferred relation-toString: the
     * probe's concrete outputs (pivot names included) become the column
     * list; per-column pure types come from the typed schema via
     * {@code colType} (static name match, else the pivot TEMPLATE —
     * {@code Executor.pivotColumnType}'s rule). The inner is wrapped
     * with the concrete projections so {@link #tdsString}'s own
     * outputs-driven composition sees a fully-stamped relation. */
    public static SqlExpr resolveDeferredTds(SqlExpr.DeferredTdsString d,
            List<OutputCol> probed,
            java.util.function.Function<String, Type> colType) {
        String wrapAlias = d.alias() + "_c";
        List<SqlSelect.Projection> projs = new ArrayList<>();
        List<Type.Column> relCols = new ArrayList<>();
        for (OutputCol oc : probed) {
            // ENGINE presentation: quote-wrap separator-bearing pivot
            // names (the REFERENCE stays physical — the projection
            // reads the bare SQL column, labeled with the presented name)
            String presented = Type.RelationType.presentPivotName(oc.name());
            projs.add(new SqlSelect.Projection(
                    SqlExpr.Column.of(wrapAlias, oc), presented,
                    new OutputCol(presented, oc.type(), oc.nullable())));
            Type t = colType.apply(presented);
            relCols.add(new Type.Column(presented, t,
                    com.legend.compiler.element.type.Multiplicity
                            .Bounded.ZERO_ONE));

        }
        SqlSelect concrete = SqlSelect.starOf(
                        new SqlSource.Subselect(d.inner(), wrapAlias, null))
                .withProjections(projs);
        // ROW ORDER: an ordered inner keeps plain-column keys on the
        // wrapper so tdsString's hoist sees them; anything else stays
        // loud in the hoist itself.
        if (!d.inner().orderBy().isEmpty()) {
            List<SqlSelect.SortKey> keys = new ArrayList<>();
            for (SqlSelect.SortKey k : d.inner().orderBy()) {
                String out = k.outputName() != null ? k.outputName()
                        : k.expr() instanceof SqlExpr.Column c
                                ? c.name() : null;
                if (out != null) {
                    // stamp when the key names a probed output; an
                    // unmatched name keeps the exact prior shape
                    OutputCol src = probed.stream()
                            .filter(oc -> oc.name().equals(out))
                            .findFirst().orElse(null);
                    keys.add(new SqlSelect.SortKey(
                            src != null ? SqlExpr.Column.of(wrapAlias, src)
                                    : SqlExpr.Column.derived(wrapAlias, out),
                            k.ascending(), k.nullOrder(), out));
                }
            }
            concrete = concrete.withOrderBy(keys);
        }
        return new SqlExpr.ScalarSubquery(
                d.form() == SqlExpr.DeferredTdsString.Form.CSV
                        ? csv(concrete, relCols, d.renderTdsNull(), d.alias() + "_r")
                        : tdsString(concrete, relCols, d.alias() + "_r"));
    }

    /** The '#TDS' text (engine toString.pure:24-35): {@code #TDS\n} +
     *  {@code '   '} + names (quoted unless simple:
     *  {@code ^[a-zA-Z0-9_]+$} or already quote-bearing) + one
     *  {@code '   '}-prefixed line per row + {@code \n#}; empty
     *  relation = a blank rows segment. Cell = the engine's {@code s()}:
     *  NULL prints {@code null}, a String containing '{'/'[' quotes
     *  with backslash/quote escaping, everything else is the pure print
     *  form. Header names are typed plan facts (charter clause 1). */
    static SqlSelect tdsString(SqlSelect inner, List<Type.Column> relCols,
            String rowAlias) {
        List<OutputCol> cols = inner.outputs();
        String aggAlias = rowAlias + "_a";
        if (cols.isEmpty()) {
            throw new IllegalStateException(
                    "toString over a zero-column relation");
        }
        StringBuilder hdr = new StringBuilder("   ");
        for (int i = 0; i < cols.size(); i++) {
            String name = cols.get(i).name().trim();
            boolean simple = name.startsWith("'")
                    || name.matches("^[a-zA-Z0-9_]+$");
            if (i > 0) {
                hdr.append(',');
            }
            hdr.append(simple ? name : "'" + name + "'");
        }
        SqlExpr line = new SqlExpr.StringLit("   ");
        for (int i = 0; i < cols.size(); i++) {
            SqlExpr c = tdsCell(
                    SqlExpr.Column.of(rowAlias, cols.get(i)),
                    relCols.get(i).type(), cols.get(i).type());
            line = i == 0 ? cat(line, c)
                    : cat(line, new SqlExpr.StringLit(","), c);
        }
        List<SqlSelect.Projection> rowProjs = new ArrayList<>();
        rowProjs.add(new SqlSelect.Projection(line, "_tds_line",
                new OutputCol("_tds_line", SqlType.Scalar.VARCHAR, false)));
        SqlExpr nl = new SqlExpr.StringLit("\n");
        SqlExpr rowsJoined = SqlExpr.Call.of(SqlFn.COALESCE,
                new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                        List.of(SqlExpr.Column.of(aggAlias, "_tds_line", SqlType.Scalar.VARCHAR, false, com.legend.sql.OutputCol.Origin.DERIVED), nl),
                        false, hoistOrder(inner, cols, rowAlias, aggAlias,
                                rowProjs)),
                new SqlExpr.StringLit(""));
        SqlExpr text = cat(new SqlExpr.StringLit("#TDS\n"
                        + hdr + "\n"), rowsJoined,
                new SqlExpr.StringLit("\n#"));
        SqlSelect rows = SqlSelect.starOf(
                        new SqlSource.Subselect(inner, rowAlias, null))
                .withProjections(rowProjs);
        return SqlSelect.starOf(new SqlSource.Subselect(rows, aggAlias, null))
                .withProjections(List.of(new SqlSelect.Projection(text, "tds",
                        new OutputCol("tds", SqlType.Scalar.VARCHAR, false))));
    }

    /** The engine {@code s()} cell (s.pure:23-38). */
    /** THE per-type cell PRINT FORMS — one owner (audit 2026-08-19:
     * tdsCell and pctCell were drifting twins; tdsCell lacked the
     * DateTime arm pctCell carried — the third-implementation disease
     * in miniature). Channel-specific QUOTING/ESCAPING stays with each
     * renderer; the SPELLING of a value lives here once. */
    private static SqlExpr dateTimeText(SqlExpr c) {
        // fixed THREE millisecond digits + '+0000' — the engine's own
        // DateTime wire spelling (measured in the F4.4 attempt)
        return cat(
                SqlExpr.Call.of(SqlFn.STRFTIME, c,
                        new SqlExpr.FormatLit(
                                com.legend.sql.DateFmt.ISO_DOT)),
                SqlExpr.Call.of(SqlFn.SUBSTRING,
                        SqlExpr.Call.of(SqlFn.STRFTIME, c,
                                new SqlExpr.FormatLit(List.of(
                                        com.legend.sql.DateFmt.Part
                                                .SUBSEC_MICRO))),
                        new SqlExpr.IntLit(1), new SqlExpr.IntLit(3)),
                new SqlExpr.StringLit("+0000"));
    }

    private static SqlExpr strictDateText(SqlExpr c) {
        return SqlExpr.Call.of(SqlFn.STRFTIME, c,
                new SqlExpr.FormatLit(com.legend.sql.DateFmt.DATE));
    }

    /** Whether the DATE-family value prints date-only (a DATE slot) or
     * as a full DateTime. */
    private static boolean dateOnly(Type t, @com.legend.base.Nullable SqlType slot) {
        return t == Type.Primitive.STRICT_DATE
                || (t == Type.Primitive.DATE
                        && slot == SqlType.Scalar.DATE);
    }

    private static SqlExpr tdsCell(SqlExpr c, Type t, SqlType slot) {
        boolean variant = t instanceof Type.ClassType vc
                && com.legend.compiler.element.type.PlatformTypes
                        .isVariant(vc);
        SqlExpr rendered;
        if (variant) {
            rendered = cat(new SqlExpr.StringLit("'"),
                    SqlExpr.Call.of(SqlFn.REPLACE,
                            SqlExpr.Call.of(SqlFn.REPLACE,
                                    Scalars.pureToString(t, c),
                                    new SqlExpr.StringLit("\\"),
                                    new SqlExpr.StringLit("\\\\")),
                            new SqlExpr.StringLit("'"),
                            new SqlExpr.StringLit("\\'")),
                    new SqlExpr.StringLit("'"));
        } else if (t == Type.Primitive.STRING) {
            SqlExpr braced = SqlExpr.Call.of(SqlFn.OR,
                    contains(c, "{"), contains(c, "["));
            rendered = new SqlExpr.Case(List.of(new SqlExpr.Case.When(braced,
                    cat(new SqlExpr.StringLit("'"),
                            SqlExpr.Call.of(SqlFn.REPLACE,
                                    SqlExpr.Call.of(SqlFn.REPLACE, c,
                                            new SqlExpr.StringLit("\\"),
                                            new SqlExpr.StringLit("\\\\")),
                                    new SqlExpr.StringLit("'"),
                                    new SqlExpr.StringLit("\\'")),
                            new SqlExpr.StringLit("'")))),
                    c);
        } else if (dateOnly(t, slot)) {
            rendered = strictDateText(c);
        } else if (t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.DATE) {
            // the missing arm the drift hid (witness testExtendMax/
            // MinDate: raw '2026-01-07 00:00:00' where pure prints the
            // ISO+ms+0000 form)
            rendered = dateTimeText(c);
        } else if ((t == Type.Primitive.NUMBER
                        && (slot == null
                                || slot instanceof SqlType.Decimal
                                || slot == SqlType.Scalar.DOUBLE))
                || (t == Type.Primitive.FLOAT
                        && (slot == null
                                || slot instanceof SqlType.Decimal
                                || slot == SqlType.Scalar.DOUBLE
                                || slot == SqlType.Scalar.BIGINT
                                || slot == SqlType.Scalar.INTEGER
                                || slot == SqlType.Scalar.HUGEINT))) {
            // JUDGING_TWO_MODES §1 (numeric charter Rule 2, compiled
            // reference): a Float-DECLARED TDS cell converts to DOUBLE once,
            // here, whatever kind the database computed it in.
            // a Number/Float cell on a FRACTION-KIND wire (a DECIMAL an
            // aggregate collapsed to, or a DOUBLE): the pure print form
            // is the Float spelling — a DECIMAL cast's fixed scale
            // fabricates trailing zeros (81.180 where pure prints 81.18;
            // witness testMax_Floats/Numbers_Relation_Aggregate).
            // INTEGER slots (an all-int Number column) and the VARCHAR
            // mixed-identity carrier keep their own spellings.
            rendered = Scalars.pureToString(Type.Primitive.FLOAT,
                    new SqlExpr.Cast(c, SqlType.Scalar.DOUBLE));
        } else {
            rendered = Scalars.pureToString(t, c);
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, c),
                new SqlExpr.StringLit(variant ? "'null'" : "null"))),
                rendered);
    }

    /** write(rel, accessor)'s observable: the COUNT of rows written
     *  (a TDS-relation accessor destination has no physical table — the
     *  write is vacuous, only the count is observable). */
    static SqlExpr writeCount(SqlSelect src, String alias) {
        SqlSelect count = SqlSelect.starOf(
                        new SqlSource.Subselect(src, alias, null))
                .withProjections(List.of(new SqlSelect.Projection(
                        new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                                List.of(), false, List.of()), null,
                        new OutputCol("count",
                                SqlType.Scalar.BIGINT, false))));
        return new SqlExpr.ScalarSubquery(count);
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
            OutputCol src = out == null ? null : cols.stream()
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
    private static SqlExpr cell(SqlExpr c, Type.Column col,
            SqlType slot, boolean renderTdsNull) {
        Type t = col.type();
        SqlExpr rendered;
        if (t == Type.Primitive.STRICT_DATE
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
            // ABSTRACT Date over a TIMESTAMP slot: the engine's
            // formatDateTime rule is DEFINED OVER THE VALUE (hasHour) —
            // date-only values print yyyy-MM-dd, timed values print the
            // datetime form. The SQL spells the engine's own rule; the
            // one residue (a TRUE midnight DateTime under an abstract
            // Date slot prints date-only) is the F5.4 slot-erasure,
            // identical on the engine's own relational read-back of a
            // DATE-precision-less TIMESTAMP.
            SqlExpr timed = SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                    SqlExpr.Call.of(SqlFn.STRFTIME, c,
                            new SqlExpr.FormatLit(DateFmt.TIME_ONLY)),
                    new SqlExpr.StringLit("00:00:00"));
            rendered = new SqlExpr.Case(List.of(new SqlExpr.Case.When(timed,
                    escapeCsv(SqlExpr.Call.of(SqlFn.STRFTIME, c,
                            new SqlExpr.FormatLit(DateFmt.CSV_DATETIME))))),
                    SqlExpr.Call.of(SqlFn.STRFTIME, c,
                            new SqlExpr.FormatLit(DateFmt.DATE)));
        } else if (t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.LATEST_DATE) {
            // the DateTime kinds: datetime spelling
            rendered = escapeCsv(SqlExpr.Call.of(SqlFn.STRFTIME, c,
                    new SqlExpr.FormatLit(DateFmt.CSV_DATETIME)));
        } else {
            rendered = escapeCsv(Scalars.pureToString(t, c));
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, c),
                new SqlExpr.StringLit(renderTdsNull ? "TDSNull" : ""))),
                rendered);
    }

    /** THE RFC4180 escape rule ({@code escapeCSVString}), spelled once:
     *  quote when the text contains comma, quote, LF or CR; embedded
     *  quotes double. */
    /** E1: the PCT wire TDS text (the deleted
     *  the PCT bridge's deleted formatAsTds, spelled by the DATABASE).
     *  Header = a compile-time constant from the TYPED schema (name
     *  quoting incl. pivot identities, pure type names — Variant FQN —
     *  and data-independent multiplicity); rows '\n'-PREFIXED (no
     *  trailing newline), cells comma-joined with the pure print forms:
     *  DateTime = fixed-3-millis '+0000', StrictDate = yyyy-MM-dd,
     *  Float = the pure minimal-decimal repr (Scalars.floatRepr),
     *  Variant = always-quoted JSON with doubled quotes, SQL NULL =
     *  bare {@code null}, strings quote only when they carry
     *  comma/quote/newline. */
    public static SqlSelect pctTds(SqlSelect inner, List<Type.Column> typedCols,
            String rowAlias) {
        List<OutputCol> cols = inner.outputs();
        java.util.Map<String, Type.Column> byName = new java.util.HashMap<>();
        for (Type.Column c : typedCols) {
            byName.put(c.name(), c);
        }
        // hidden _pct_ord sort carriers ride the outputs for the
        // aggregate's ORDER BY (hoistOrder sees them) but never print
        List<OutputCol> hoistCols = cols;
        cols = cols.stream()
                .filter(oc -> !oc.name().startsWith("_pct_ord")).toList();
        List<Type.Column> relCols = cols.stream().map(oc -> {
            Type.Column t = byName.get(oc.name());
            if (t == null) {
                throw new IllegalStateException("pctTds: output column '"
                        + oc.name() + "' has no typed relation column");
            }
            return t;
        }).toList();
        if (cols.isEmpty()) {
            throw new IllegalStateException("pctTds over a zero-column relation");
        }
        String aggAlias = rowAlias + "_a";
        SqlExpr line = pctCell(SqlExpr.Column.of(rowAlias, cols.get(0)),
                relCols.get(0), cols.get(0).type());
        for (int i = 1; i < cols.size(); i++) {
            line = cat(line, new SqlExpr.StringLit(","),
                    pctCell(SqlExpr.Column.of(rowAlias, cols.get(i)),
                            relCols.get(i), cols.get(i).type()));
        }
        List<SqlSelect.Projection> rowProjs = new ArrayList<>();
        rowProjs.add(new SqlSelect.Projection(line, "_pct_line",
                new OutputCol("_pct_line", SqlType.Scalar.VARCHAR, false)));
        List<SqlSelect.SortKey> aggOrder = hoistOrder(inner, hoistCols,
                rowAlias, aggAlias, rowProjs);
        SqlExpr nl = new SqlExpr.StringLit("\n");
        SqlExpr rowsJoined = SqlExpr.Call.of(SqlFn.COALESCE,
                cat(nl, new SqlAgg.Reducer(SqlAgg.Fn.STRING_AGG,
                        List.of(SqlExpr.Column.of(aggAlias, "_pct_line", SqlType.Scalar.VARCHAR, false, com.legend.sql.OutputCol.Origin.DERIVED), nl),
                        false, aggOrder)),
                new SqlExpr.StringLit(""));
        SqlExpr text = cat(new SqlExpr.StringLit(pctHeader(relCols)),
                rowsJoined);
        SqlSelect rows = SqlSelect.starOf(
                        new SqlSource.Subselect(inner, rowAlias, null))
                .withProjections(rowProjs);
        return SqlSelect.starOf(new SqlSource.Subselect(rows, aggAlias, null))
                .withProjections(List.of(new SqlSelect.Projection(text, "pctTds",
                        new OutputCol("pctTds", SqlType.Scalar.VARCHAR, false))));
    }

    /** The header line — a COMPILE-TIME constant from the typed schema
     *  (Java building a SQL literal is orchestration; every printed
     *  fact is a typed plan fact). */
    static String pctHeader(List<Type.Column> relCols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < relCols.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            Type.Column col = relCols.get(i);
            String colName = col.name();
            if (colName.startsWith("'")) {
                colName = "'" + colName.replace("'", "\\'") + "'";
            } else if (colName.contains("__|__")) {
                colName = "'\\'" + colName + "\\''";
            } else if (!colName.matches("[A-Za-z_][A-Za-z0-9_$]*")) {
                colName = "'" + colName + "'";
            }
            var m = col.multiplicity();
            boolean optional = m instanceof com.legend.compiler.element
                    .type.Multiplicity.Bounded b && b.lower() == 0;
            sb.append(colName).append(':').append(pctTypeName(col.type()))
                    .append(optional ? "[0..1]" : "[1]");
        }
        return sb.toString();
    }

    private static String pctTypeName(Type t) {
        if (t instanceof Type.Primitive p) {
            return p.typeName();
        }
        if (com.legend.compiler.element.type.PlatformTypes.isVariant(t)) {
            // the 5.88 TDS header parser resolves names as pure paths
            // without import scanning — Variant must be fully qualified
            return "meta::pure::metamodel::variant::Variant";
        }
        return t.typeName();
    }

    /** One PCT cell: typed print form, SQL NULL = bare 'null'.
     *
     *  <p>QUOTING is the reader's rule, and the reader is upstream's
     *  {@code stringToTDS} (legend-pure m2-dsl-tds {@code TDSExtension
     *  .makePureCsvSpecs}). At 5.92.0 it reads with {@code quote('\'')}
     *  and {@code escape('\\')} — "emulate Pure String quoting" — where
     *  5.88.0 read RFC-4180 double quotes. So a quoted cell is
     *  {@code '…'} with {@code \} and {@code '} backslash-escaped, the
     *  same spelling the engine's own {@code s()} uses for the '#TDS'
     *  print form ({@link #tdsCell}). Double-quoted cells split on
     *  their commas under the 5.92.0 reader ("Row 2 has too many
     *  columns" — 28 variant tests, batch 1 of the upstream boundary
     *  program, 2026-09-10). */
    private static SqlExpr pctCell(SqlExpr c, Type.Column col, SqlType slot) {
        Type t = col.type();
        SqlExpr rendered;
        if (com.legend.compiler.element.type.PlatformTypes.isVariant(t)) {
            // pure prints VARIANT cells ALWAYS quoted
            rendered = pureQuoted(new SqlExpr.Cast(c, SqlType.Scalar.VARCHAR));
        } else if (dateOnly(t, slot)) {
            rendered = strictDateText(c);
        } else if (t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.DATE) {
            rendered = dateTimeText(c);
        } else if (t == Type.Primitive.FLOAT) {
            rendered = Scalars.pureToString(Type.Primitive.FLOAT, c);
        } else if (t == Type.Primitive.STRING
                || t instanceof Type.EnumType) {
            rendered = pctEscape(new SqlExpr.Cast(c, SqlType.Scalar.VARCHAR));
        } else {
            // Integer/Boolean/Decimal/Number print via the DB's plain
            // VARCHAR cast (NUMBER columns carry mixed-identity print
            // forms already)
            rendered = new SqlExpr.Cast(c, SqlType.Scalar.VARCHAR);
        }
        // EXPLICIT null dispatch on the SOURCE column — DuckDB's
        // concat() SKIPS null arguments (a null DateTime rendered as
        // bare '+0000' through the concat chain), so coalesce over the
        // rendered text is not a null test
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.IS_NULL, c),
                new SqlExpr.StringLit("null"))), rendered);
    }

    /** PCT string cell quoting: only when the value carries a comma, a
     *  single quote, a backslash, or a line break — the characters the
     *  5.92.0 reader ({@code quote('\'')}, {@code escape('\\')}) would
     *  otherwise read as structure. */
    private static SqlExpr pctEscape(SqlExpr s) {
        SqlExpr needs = or(or(contains(s, ","), contains(s, "'")),
                or(contains(s, "\\"), or(contains(s, "\n"), contains(s, "\r"))));
        return new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(needs, pureQuoted(s))), s);
    }

    /** {@code '…'} with backslash and single quote backslash-escaped —
     *  the Pure string spelling upstream's TDS reader (5.92.0+) and the
     *  engine's {@code s()} both use. */
    private static SqlExpr pureQuoted(SqlExpr s) {
        return cat(new SqlExpr.StringLit("'"),
                SqlExpr.Call.of(SqlFn.REPLACE,
                        SqlExpr.Call.of(SqlFn.REPLACE, s,
                                new SqlExpr.StringLit("\\"),
                                new SqlExpr.StringLit("\\\\")),
                        new SqlExpr.StringLit("'"),
                        new SqlExpr.StringLit("\\'")),
                new SqlExpr.StringLit("'"));
    }

    private static SqlExpr escapeCsv(SqlExpr s) {
        SqlExpr needs = or(contains(s, ","), or(contains(s, "\""),
                or(contains(s, "\n"), contains(s, "\r"))));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(needs,
                cat(new SqlExpr.StringLit("\""),
                        SqlExpr.Call.of(SqlFn.REPLACE, s,
                                new SqlExpr.StringLit("\""),
                                new SqlExpr.StringLit("\"\"")),
                        new SqlExpr.StringLit("\"")))),
                s);
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

    private static SqlExpr cat(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(SqlFn.CONCAT, out, parts[i]);
        }
        return out;
    }
}
