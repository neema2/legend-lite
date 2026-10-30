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
        inner = withSortOutputs(inner);
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

    /** A select's order carried outside it: {@code source} the select with its hidden order columns, {@code keys}
     *  the sort keys over {@code rowAlias}, each addressing an output of the source. */
    record Lifted(SqlSelect source, List<SqlSelect.SortKey> keys) {
    }

    /** {@code inner}'s order lifted over the alias {@code rowAlias}: a sort key that is no output (a sort by an
     *  expression, or by a column the select drops; the plan path program's step 6, 2026-10-18) is carried as a
     *  hidden column of the inner select and ordered by outside, never written into the row. Null where a hidden
     *  column would change the rows (a star or a DISTINCT select) -- the caller says what it does then. */
    static @com.legend.base.Nullable Lifted liftOrder(SqlSelect inner, String rowAlias, String what) {
        List<OutputCol> cols = inner.outputs();
        List<SqlSelect.SortKey> lifted = new ArrayList<>();
        List<SqlSelect.Projection> hidden = new ArrayList<>();
        List<OutputCol> hiddenOut = new ArrayList<>();
        for (SqlSelect.SortKey k : inner.orderBy()) {
            String out = k.outputName() != null ? k.outputName()
                    : k.expr() instanceof SqlExpr.Column c ? c.name() : null;
            // the inner's own outputs, as the hoist's: a hidden sort column (withSortOutputs) is one, not one of cols
            OutputCol outCol = out == null ? null : inner.outputs().stream()
                    .filter(oc -> oc.name().equals(out)).findFirst()
                    .orElse(null);
            if (outCol == null) {
                if (!(k.expr().type() instanceof com.legend.sql.TypeFact.Typed typed)) {
                    throw new com.legend.error.NotImplementedException(what + " over an ordered relation whose sort"
                            + " key is not an output column and carries no typed slot");
                }
                if (inner.projections().isEmpty() || inner.distinct()) {
                    return null;
                }
                String oname = "_ord" + hidden.size();
                outCol = new OutputCol(oname, typed.type(), true);
                hidden.add(new SqlSelect.Projection(k.expr(), oname, outCol));
                hiddenOut.add(outCol);
            }
            lifted.add(new SqlSelect.SortKey(
                    SqlExpr.Column.of(rowAlias, outCol), k.ascending(),
                    k.nullOrder(), null));
        }
        SqlSelect source = inner;
        if (!hidden.isEmpty()) {
            List<SqlSelect.Projection> ps = new ArrayList<>(inner.projections());
            ps.addAll(hidden);
            List<OutputCol> outs = new ArrayList<>(inner.outputs());
            outs.addAll(hiddenOut);
            source = inner.withProjections(ps, outs);
        }
        return new Lifted(source, lifted);
    }

    /** {@code inner} as a typed-rows statement (the plan path program's step 7, 2026-10-18): itself, unless a sort
     *  key is no output -- then the select with that key hidden, wrapped once over {@code rowAlias} projecting the
     *  select's own outputs and ordered by the hidden key, as the JSON rows wire carried it (a dialect that emulates
     *  a join by a union, H2's FULL OUTER JOIN, addresses a sort key among the outputs only). Where a hidden column
     *  would change the rows the statement stays as it is. */
    static SqlSelect typedRows(SqlSelect inner, String rowAlias) {
        Lifted l = liftOrder(inner, rowAlias, "typed rows");
        if (l == null || l.source() == inner) {
            return inner;
        }
        List<OutputCol> cols = inner.outputs();
        List<SqlSelect.Projection> projs = new ArrayList<>(cols.size());
        for (OutputCol oc : cols) {
            projs.add(new SqlSelect.Projection(SqlExpr.Column.of(rowAlias, oc), oc.name(), oc));
        }
        return SqlSelect.starOf(new SqlSource.Subselect(l.source(), rowAlias, null))
                .withProjections(projs, cols)
                .withOrderBy(l.keys());
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
                    // probe's SQL type decodes
                    return t != null ? t
                            : sqlTypeToPure.apply(p.typeName(name));
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
        inner = withSortOutputs(inner);
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
        // the one owner (docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §4, 2026-10-18): Pure's text of a DateTime read
        // from a database -- 'T', the fraction's digits with trailing zeros trimmed, '+0000' -- on every output
        // (the fixed three millisecond digits, "the engine's own wire spelling", and the value root's own
        // whole-second case are both gone into it)
        return LiteralSpelling.dateTimeText(c);
    }

    /** Whether a timestamp's time of day is not midnight: the engine's {@code formatDateTime} rule for an ABSTRACT
     *  Date, defined over the value (hasHour) -- a timed value is a DateTime, a date-only one (a parameter bound to a
     *  day; a DATE read into a timestamp) its day. The one residue, a true midnight DateTime under an abstract Date
     *  printing date-only, is F5.4's slot erasure, identical on the engine's own relational read-back. */
    private static SqlExpr timed(SqlExpr c) {
        return LiteralSpelling.timed(c);
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
                    arm.apply(ref, MixedEncoding.kindType(slot.kind()), slot.type())));
        }
        return new SqlExpr.Case(whens, null);
    }

    @FunctionalInterface
    private interface SlotArm {
        SqlExpr apply(SqlExpr ref, Type kind, SqlType slot);
    }

    /** {@code cols} less the slot columns of its tagged columns: the columns a text wire presents (a tagged column's
     *  slots are read through it, {@link #perSlot}). */
    static List<OutputCol> presented(List<OutputCol> cols) {
        java.util.Set<String> slots = new java.util.HashSet<>();
        for (OutputCol c : cols) {
            if (c.type() instanceof SqlType.Tagged tg) {
                for (int i = 0; i < tg.slots().size(); i++) {
                    slots.add(SqlType.Tagged.slotColumn(c.name(), i));
                }
            }
        }
        return slots.isEmpty() ? cols : cols.stream().filter(c -> !slots.contains(c.name())).toList();
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
            // ISO+ms+0000 form): a TDS cell's DateTime text has at least
            // three fraction digits (LiteralSpelling.dateTimeTextTds)
            rendered = LiteralSpelling.dateTimeTextTds(c);
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
    static SqlExpr cell(SqlExpr c, Type.Column col,
            SqlType slot, boolean renderTdsNull) {
        Type t = col.type();
        SqlExpr rendered;
        if (slot instanceof SqlType.Tagged tg) {
            // a mixed selection's winner: the slot the tag names, its cell by its own kind
            return perSlot(c, tg, (ref, kind, st) -> cell(ref,
                    new Type.Column(col.name(), kind, col.multiplicity()), st, renderTdsNull));
        }
        if (dateKind(t) && textSlot(slot)) {
            // a date carried as its own text (a value root spelled at the precision it has): the text is the cell,
            // a timed one with Pure's '+0000' (the one owner's rule, 2026-10-18)
            rendered = LiteralSpelling.dateTimeTextOfText(c);
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
                    strictDateText(c));
        } else if (t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.LATEST_DATE) {
            // the DateTime kinds: Pure's DateTime text (the one owner, 2026-10-18; the engine's own CSV wrote
            // 'yyyy-MM-dd HH:mm:ss', the fraction dropped: docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §3, gap 4)
            rendered = dateTimeText(c);
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
    /** A generated column's name as the engine labels it: the key's text, the separator and the USING's alias
     *  ({@code tail}) between literal single quotes -- the quotes are part of the name on legend-engine's DuckDB SQL
     *  ({@code concat('''', <key>, '''')}), in its interpreter and in the PCT's expected text; lite's physical column
     *  is bare and every carrier presents it this way ({@link Type.RelationType#presentPivotName}; the pivot
     *  homework, docs/PIVOT_UPSTREAM_HOMEWORK_2026_10_18.md). */
    static SqlExpr generatedName(SqlExpr keyText, String tail) {
        return cat(new SqlExpr.StringLit("'"), keyText, new SqlExpr.StringLit(tail + "'"));
    }

    static SqlExpr escapeCsv(SqlExpr s) {
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

    static SqlExpr cat(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(SqlFn.CONCAT, out, parts[i]);
        }
        return out;
    }
}
