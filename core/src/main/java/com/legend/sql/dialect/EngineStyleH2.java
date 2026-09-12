// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The ENGINE's own H2 SQL text &mdash; for byte-exact {@code toSQLString}
 * goldens (transform/fromPure/tests), never for execution. The formatting
 * contract is MATCHED against the engine's output, not invented: one
 * line, lowercase keywords, each SELECT's leftmost source aliased
 * {@code "root"}, every other join source aliased
 * {@code <lowercase-table>_<n>} with a query-global counter (the engine's
 * replaceAliasName pass), source/output aliases double-quoted, physical
 * column names bare, parenthesized ON clauses.
 */
public class EngineStyleH2 extends AnsiSqlRenderer {

    @Override
    protected String listCall(com.legend.sql.SqlFn fn,
            java.util.List<SqlExpr> args) {
        // greatest/least over a LITERAL collection: the engine's H2
        // emission is VARIADIC greatest(a, b, c); EMPTY = greatest(null)
        if ((fn == com.legend.sql.SqlFn.LIST_MAX
                || fn == com.legend.sql.SqlFn.LIST_MIN)
                && args.get(0) instanceof SqlExpr.ArrayLit al) {
            return variadicExtreme(fn == com.legend.sql.SqlFn.LIST_MAX,
                    al.elements());
        }
        if ((fn == com.legend.sql.SqlFn.LIST_MAX
                || fn == com.legend.sql.SqlFn.LIST_MIN)
                && args.get(0) instanceof SqlExpr.NullLit) {
            // the EMPTY collection ([]->cast(@String)) — engine emits
            // greatest(null)
            return variadicExtreme(fn == com.legend.sql.SqlFn.LIST_MAX,
                    java.util.List.of());
        }
        // the mixed-identity carrier's selection recipe
        // ids[list_position(vals, list_max(vals))] renders the SAME
        // variadic form over the ORIGINAL elements (identity == value
        // for the engine's scalar greatest/least)
        if (fn == com.legend.sql.SqlFn.LIST_GET && args.size() == 2
                && args.get(0) instanceof SqlExpr.ArrayLit
                && args.get(1) instanceof SqlExpr.Call pos
                && pos.fn() == com.legend.sql.SqlFn.LIST_POSITION
                && pos.args().size() == 2
                && pos.args().get(0) instanceof SqlExpr.ArrayLit vals
                && pos.args().get(1) instanceof SqlExpr.Call win
                && (win.fn() == com.legend.sql.SqlFn.LIST_MAX
                        || win.fn() == com.legend.sql.SqlFn.LIST_MIN)) {
            // render over the RAW comparables (vals), not the identity
            // encodings (ids) — engine: greatest("root".quantity, 1, 3)
            return variadicExtreme(
                    win.fn() == com.legend.sql.SqlFn.LIST_MAX,
                    vals.elements());
        }
        // a REDUCTION over a LITERAL collection is the engine's infix
        // chain — and([a, b, c]) = 'a and b and c', [x, y]->times() =
        // '(x * y)' (the engine never materializes the list; the
        // greatest/least arm above is the same rule for extremes)
        if (args.size() == 1 && args.get(0) instanceof SqlExpr.ArrayLit lit
                && !lit.elements().isEmpty()) {
            com.legend.sql.SqlFn op = switch (fn) {
                case LIST_BOOL_AND -> com.legend.sql.SqlFn.AND;
                case LIST_BOOL_OR -> com.legend.sql.SqlFn.OR;
                case LIST_PRODUCT -> com.legend.sql.SqlFn.TIMES;
                case LIST_SUM -> com.legend.sql.SqlFn.PLUS;
                default -> null;
            };
            if (op != null) {
                java.util.List<SqlExpr> elems = lit.elements().stream()
                        .map(e -> e instanceof SqlExpr.Cast c
                                && (c.value() instanceof SqlExpr.IntLit
                                        || c.value() instanceof SqlExpr.FloatLit)
                                ? c.value() : e)
                        .toList();
                if (elems.size() == 1) {
                    return expr(elems.get(0), 0);
                }
                SqlExpr chain = new SqlExpr.Call(op, elems);
                // boolean chains are the engine's FLAT text (the AND/OR
                // arms in expr); arithmetic chains parenthesize
                return op == com.legend.sql.SqlFn.AND
                        || op == com.legend.sql.SqlFn.OR
                        ? expr(chain, 0) : expr(chain, Integer.MAX_VALUE);
            }
        }
        // firstNotNull over a literal collection ($set->filter(v | $v !=
        // TDSNull)->first(), tdsExtensions.pure) is the engine's
        // coalesce(a, b, ...)
        if (fn == com.legend.sql.SqlFn.LIST_GET && args.size() == 2
                && args.get(1) instanceof SqlExpr.IntLit one
                && one.value() == 1
                && args.get(0) instanceof SqlExpr.Call flt
                && flt.fn() == com.legend.sql.SqlFn.LIST_FILTER
                && flt.args().size() == 2
                && flt.args().get(0) instanceof SqlExpr.ArrayLit src
                && !src.elements().isEmpty()
                && flt.args().get(1) instanceof SqlExpr.Lambda pred
                && pred.params().size() == 1
                && isNotNullOf(pred.body(), pred.params().get(0))) {
            return expr(new SqlExpr.Call(com.legend.sql.SqlFn.COALESCE,
                    src.elements()), 0);
        }
        return super.listCall(fn, args);
    }

    /** {@code x is not null} in any of its lowered spellings over the
     * lambda's own parameter. */
    private static boolean isNotNullOf(SqlExpr body, String param) {
        if (body instanceof SqlExpr.Call c) {
            java.util.List<SqlExpr> a = c.args();
            return switch (c.fn()) {
                case IS_NOT_NULL -> a.size() == 1 && isParam(a.get(0), param);
                case NOT -> a.size() == 1 && a.get(0) instanceof SqlExpr.Call in
                        && in.fn() == com.legend.sql.SqlFn.IS_NULL
                        && in.args().size() == 1
                        && isParam(in.args().get(0), param);
                case NOT_EQUAL, NULL_SAFE_NOT_EQUAL -> a.size() == 2
                        && ((isParam(a.get(0), param)
                                && a.get(1) instanceof SqlExpr.NullLit)
                            || (isParam(a.get(1), param)
                                && a.get(0) instanceof SqlExpr.NullLit));
                default -> false;
            };
        }
        return false;
    }

    private static boolean isParam(SqlExpr e, String param) {
        return e instanceof SqlExpr.Column col && col.table() == null
                && param.equals(col.name());
    }

    /** Pure round is half-even; the engine's H2 text is the bare
     * {@code round(x[, n])} (the rows verdict judges the value). */
    @Override
    protected String roundHalfEven(java.util.List<SqlExpr> a) {
        return "round(" + expr(a.get(0), 0)
                + (a.size() > 1 ? ", " + expr(a.get(1), 0) : "") + ")";
    }

    /** joinStrings over a LITERAL element list: the engine's H2 emission
     * is one flat {@code concat(el1, el2, ..., <separator args as
     * written>)} — separators appended at the END, never between (the
     * 1-arg form desugars to three {@code ''}). Recognizes the lowered
     * COALESCE(STRING_AGG)+CONCAT wrappers; non-literal collections fall
     * through to the honest wall. */
    /** Engine-TEXT view of checked narrowing: the VERBATIM inner value
     * — the engine translates toOne to processNoOp in SQL (D1; the
     * NULLS-suppression precedent for engine-verbatim views). */
    @Override
    protected String checkedOne(SqlExpr.CheckedOne co, int parentPrec) {
        return expr(co.list(), parentPrec);
    }

    /** Engine-TEXT view of carrier compaction: the VERBATIM inner value
     * — the engine's textual SQL has no compaction (its null-drop is
     * host-side); same rule as {@link #checkedOne}. */
    @Override
    protected String compactList(SqlExpr.CompactList cl, int parentPrec) {
        return expr(cl.list(), parentPrec);
    }

    private @com.legend.Nullable String joinStringsFlat(SqlExpr.Call c) {
        // 4-arg forms: CONCAT(CONCAT(prefix, J), suffix) [list-value arm]
        // or CONCAT(prefix, CONCAT(J, suffix)) [pure-value arm]
        if (c.fn() == com.legend.sql.SqlFn.CONCAT && c.args().size() == 2) {
            if (c.args().get(0) instanceof SqlExpr.Call c0
                    && c0.fn() == com.legend.sql.SqlFn.CONCAT
                    && c0.args().size() == 2
                    && joinedReduction(c0.args().get(1)) instanceof
                            SqlExpr.ReduceCollection rc) {
                return flatConcat(rc, java.util.List.of(c0.args().get(0),
                        rc.extras().get(0), c.args().get(1)));
            }
            if (c.args().get(1) instanceof SqlExpr.Call c1
                    && c1.fn() == com.legend.sql.SqlFn.CONCAT
                    && c1.args().size() == 2
                    && joinedReduction(c1.args().get(0)) instanceof
                            SqlExpr.ReduceCollection rc2) {
                return flatConcat(rc2, java.util.List.of(c.args().get(0),
                        rc2.extras().get(0), c1.args().get(1)));
            }
        }
        // 1/2-arg forms: bare COALESCE(RC, '') — an EMPTY separator is
        // the 1-arg desugar joinStrings(s, '', '', '')
        if (joinedReduction(c) instanceof SqlExpr.ReduceCollection rc) {
            SqlExpr sep = rc.extras().get(0);
            java.util.List<SqlExpr> seps = sep instanceof SqlExpr.StringLit s
                    && s.value().isEmpty()
                    ? java.util.List.of(sep, sep, sep)
                    : java.util.List.of(sep);
            return flatConcat(rc, seps);
        }
        return null;
    }

    /** The COALESCE(ReduceCollection(STRING_AGG, <literal list>, [sep]),
     * '') shape, or null. */
    private static SqlExpr.@com.legend.Nullable ReduceCollection
            joinedReduction(SqlExpr e) {
        if (e instanceof SqlExpr.Call c
                && c.fn() == com.legend.sql.SqlFn.COALESCE
                && c.args().size() == 2
                && c.args().get(0) instanceof SqlExpr.ReduceCollection rc
                && rc.reducer() == com.legend.sql.SqlAgg.Fn.STRING_AGG
                && rc.extras().size() == 1
                && literalElements(rc.collection()) != null) {
            return rc;
        }
        return null;
    }

    /** The underlying literal element list (unwraps the element-text
     * LIST_TRANSFORM), or null for runtime collections. */
    private static java.util.@com.legend.Nullable List<SqlExpr>
            literalElements(SqlExpr coll) {
        if (coll instanceof SqlExpr.ArrayLit al) {
            return al.elements();
        }
        if (coll instanceof SqlExpr.Call c
                && c.fn() == com.legend.sql.SqlFn.LIST_TRANSFORM
                && c.args().get(0) instanceof SqlExpr.ArrayLit al2) {
            return al2.elements();
        }
        return null;
    }

    private String flatConcat(SqlExpr.ReduceCollection rc,
            java.util.List<SqlExpr> seps) {
        java.util.List<SqlExpr> parts = new java.util.ArrayList<>();
        for (SqlExpr e : java.util.Objects.requireNonNull(
                literalElements(rc.collection()))) {
            parts.add(unwrapElementText(e));
        }
        parts.addAll(seps);
        return "concat(" + parts.stream().map(e -> expr(e, 0))
                .collect(java.util.stream.Collectors.joining(", ")) + ")";
    }

    /** Per-element null-text coercion (coalesce(cast(x as varchar),
     * 'TDSNull')) is noise in the engine's flat concat — it spells the
     * raw element. */
    private static SqlExpr unwrapElementText(SqlExpr e) {
        SqlExpr inner = e;
        if (inner instanceof SqlExpr.Call c
                && c.fn() == com.legend.sql.SqlFn.COALESCE
                && c.args().size() == 2
                && c.args().get(1) instanceof SqlExpr.StringLit s
                && com.legend.compiler.element.type.PlatformTypes
                        .TDS_NULL_CELL.equals(s.value())) {
            inner = c.args().get(0);
        }
        return inner instanceof SqlExpr.Cast cast ? cast.value() : inner;
    }

    private String variadicExtreme(boolean max,
            java.util.List<SqlExpr> elems) {
        String name = max ? "greatest" : "least";
        return elems.isEmpty() ? name + "(null)"
                : name + "(" + elems.stream()
                        // LUB-coercion casts around LITERALS are noise in
                        // the engine text (greatest coerces anyway)
                        .map(e -> e instanceof SqlExpr.Cast c
                                && (c.value() instanceof SqlExpr.IntLit
                                        || c.value() instanceof
                                                SqlExpr.FloatLit)
                                ? c.value() : e)
                        .map(e -> expr(e, 0))
                        .collect(java.util.stream.Collectors
                                .joining(", ")) + ")";
    }

    /** The engine connection's {@code quoteIdentifiers} flag: every
     * physical identifier (schema, table, column) renders double-quoted
     * (plan goldens testQuoteIdentifiersFlag*). */
    private final boolean quoteIdentifiers;

    /** The connection's timeZone — a non-default zone wraps DATETIME
     * placeholders in the engine's {@code GMTtoTZ} template
     * (relationalPlanSupportFunctions gate). Null = default. */
    private final @com.legend.Nullable String timeZone;

    public EngineStyleH2() {
        this(false);
    }

    public EngineStyleH2(boolean quoteIdentifiers) {
        this(quoteIdentifiers, null);
    }

    public EngineStyleH2(boolean quoteIdentifiers, @com.legend.Nullable String timeZone) {
        super(Lexicon.ENGINE_STYLE, TypeNames.ANSI, Spellings.DUCKDB);
        this.quoteIdentifiers = quoteIdentifiers;
        this.timeZone = timeZone;
    }

    private String phys(String name) {
        return quoteIdentifiers ? '"' + name + '"' : name;
    }

    private final Map<String, String> renames = new LinkedHashMap<>();
    private final Map<String, SqlSource.Subselect> subselects =
            new LinkedHashMap<>();

    @Override
    public String render(SqlQuery query) {
        for (com.legend.sql.SqlRewriter pass : passes()) {
            query = pass.rewriteRoot(query);
        }
        query = wrapTdsJoinTop(query);
        renames.clear();
        subselects.clear();
        rootConsumed.clear();
        placeholders.clear();
        planQuery(query, new LinkedHashMap<>());
        StringBuilder sb = new StringBuilder();
        query(sb, query, 0);
        return sb.toString();
    }

    /** The engine ISOLATES a user TDS join's result (isolateTdsSelect):
     * the plan spells {@code select <each TDS col quoted> from (select *
     * from (a join b)) as "<group>_0"} — the wrap alias plans PRE-ORDER
     * so it claims the group's _0 and pushes the inner left subselect to
     * _1 (tdsJoinOneDBOneExpression golden). TEXT-channel only: the
     * executed SQL keeps the flat star form (identical rows). Applies to
     * the BARE star-over-join top (every other clause empty — exactly
     * SqlSelect.starOf of a join) with named outputs. */
    private static SqlQuery wrapTdsJoinTop(SqlQuery query) {
        if (query instanceof SqlSelect s
                && s.projections().isEmpty() && !s.distinct()
                && s.from() instanceof SqlSource.Join
                && s.where() == null && s.groupBy().isEmpty()
                && s.having() == null && s.qualify() == null
                && s.orderBy().isEmpty() && s.limit() == null
                && s.offset() == null && !s.outputs().isEmpty()) {
            String wrap = "tdswrap__";
            // placeholder-owned columns spell UNQUOTED (they were never
            // defined with a quoted alias in any in-SQL projection —
            // tdsvar_0.firstName vs "tdsvar_0"."fID" in the goldens)
            java.util.Set<String> phCols = new java.util.HashSet<>();
            java.util.ArrayDeque<SqlSource> srcs = new java.util.ArrayDeque<>();
            srcs.add(s.from());
            while (!srcs.isEmpty()) {
                SqlSource x = srcs.poll();
                if (x instanceof SqlSource.VarSetPlaceholder vp) {
                    vp.outputs().forEach(o -> phCols.add(o.name()));
                }
                if (x instanceof SqlSource.Join jj) {
                    srcs.add(jj.left());
                    srcs.add(jj.right());
                }
            }
            java.util.List<SqlSelect.Projection> cols =
                    s.outputs().stream().map(o ->
                            new SqlSelect.Projection(new SqlExpr.Column(
                                    wrap, phCols.contains(o.name())
                                            ? o.name()
                                            : "\"" + o.name() + "\"",
                                    com.legend.sql.SqlTyping.UNKNOWN,
                                    com.legend.sql.OutputCol.Origin
                                            .DERIVED),
                                    o.name(), o))
                            .toList();
            return new SqlSelect(cols, false,
                    new SqlSource.Subselect(s, wrap, null), null,
                    java.util.List.of(), null, null, java.util.List.of(),
                    null, null, java.util.List.of());
        }
        return query;
    }

    // == the engine's alias plan (replaceAliasName parity, audit 19 F2) ==
    // The engine groups aliases BY LOWERCASED TABLE NAME and numbers
    // 0..n-1 within each group in encounter order; each SELECT's leftmost
    // source renders "root" but still CONSUMES its group's next index
    // (self-join goldens: root + persontable_1). A subselect belongs to
    // its FIRST INNER TABLE's group (goldens: account_info_1, never
    // subselect_0), and its interior plans BEFORE the alias itself.

    private void planQuery(SqlQuery q, Map<String, Integer> groups) {
        planQuery(q, groups, true);
    }

    /** {@code rootScope}: engine reAliasQuery only spells "root" for a
     * root-position scope — a JOINED derived table's interior continues
     * the group numbering (golden: persontable_1 frame, persontable_2/3
     * inside). Union BRANCHES are each their own root scope. */
    private void planQuery(SqlQuery q, Map<String, Integer> groups,
            boolean rootScope) {
        switch (q) {
            case SqlSelect s -> {
                if (!(s.from() instanceof SqlSource.Dual)) {
                    planSource(s.from(), rootScope, groups);
                }
                // correlated EXISTS subqueries join the SAME group
                // numbering (engine reAliasQuery walks filter operations:
                // golden "certificationtable_0" inside the exists)
                if (s.where() != null) {
                    planExprSubqueries(s.where(), groups);
                }
            }
            case SqlUnion u -> u.branches().forEach(
                    b -> planQuery(b, groups, true));
            case com.legend.sql.SqlWith w -> {
                w.ctes().forEach(c -> planQuery(c.query(), groups, true));
                planQuery(w.body(), groups, true);
            }
        }
    }

    private void planExprSubqueries(SqlExpr e, Map<String, Integer> groups) {
        if (e instanceof SqlExpr.Exists ex) {
            planQuery(ex.subquery(), groups, false);
            return;
        }
        for (SqlExpr c : e.children()) {
            planExprSubqueries(c, groups);
        }
    }

    private void planSource(SqlSource src, boolean leftmost,
            Map<String, Integer> groups) {
        switch (src) {
            case SqlSource.Table t -> {
                if (t.alias() != null) {
                    // alias groups take the BARE table name — the engine's
                    // reAliasQuery keys by table, never schema-qualified
                    // (golden "sourceannouncement_0", not
                    // "s.sourceannouncement_0")
                    String group = t.name()
                            .substring(t.name().lastIndexOf('.') + 1)
                            .toLowerCase(Locale.ROOT);
                    if (leftmost) {
                        // reAliasQuery: every 'root' pair in a group
                        // DEDUPES to one — the group spends ONE index on
                        // root no matter how many nested roots it has
                        consumeRootSlot(group, groups);
                        renames.put(t.alias(), "root");
                    } else {
                        renames.put(t.alias(), nextInGroup(group, groups));
                    }
                }
            }
            case SqlSource.Subselect sub -> {
                // a NAMED frame (view-backed target, union frame) groups
                // by its own model identity — orderpnlview_0,
                // unionalias_0 — never the underlying table's group
                String group = sub.frameName() != null
                        && !SqlSource.Subselect.EXISTS_KEYS_FRAME
                                .equals(sub.frameName())
                        ? sub.frameName().toLowerCase(Locale.ROOT)
                        : firstInnerTable(sub.inner());
                // only the DISTINCT materialization keeps the root name
                // (its frame REPLACES the root table); every other frame
                // is group-named — persontable_0, unionalias_0
                boolean rootFrame = leftmost
                        && sub.inner() instanceof SqlSelect ds
                        && ds.distinct();
                if (rootFrame) {
                    consumeRootSlot(group, groups);
                    renames.put(sub.alias(), "root");
                } else {
                    // PRE-ORDER: the alias numbers before its interior
                    // (reAliasQuery traverse pairs the select before its
                    // children — nested union frames count outermost-first)
                    renames.put(sub.alias(), nextInGroup(group, groups));
                }
                planQuery(sub.inner(), groups,
                        !SqlSource.Subselect.EXISTS_KEYS_FRAME
                                .equals(sub.frameName()));
                subselects.put(sub.alias(), sub);
            }
            case SqlSource.Join j -> {
                planSource(j.left(), leftmost, groups);
                planSource(j.right(), false, groups);
            }
            // cross-store plan variable: groups by its VAR NAME (engine
            // reAliasQuery VarSetPlaceHolder arm — tdsvar_0/tdsvar_1)
            case SqlSource.VarSetPlaceholder vp -> {
                renames.put(vp.alias(),
                        nextInGroup(vp.varName().toLowerCase(Locale.ROOT),
                                groups));
                placeholders.add(vp.alias());
            }
            default -> { }
        }
    }

    private static String nextInGroup(String group, Map<String, Integer> groups) {
        int i = groups.merge(group, 1, Integer::sum) - 1;
        return group + "_" + i;
    }

    /** One index per group for ALL its root-named members (reAliasQuery
     * dedupes ('group','root') pairs before numbering). */
    private final Set<String> rootConsumed = new HashSet<>();

    /** VarSetPlaceholder aliases — their column reads spell UNQUOTED
     * (the placeholder's columns were never defined with quoted aliases
     * in any in-SQL projection; engine tdsvar goldens). */
    private final Set<String> placeholders = new HashSet<>();

    private void consumeRootSlot(String group, Map<String, Integer> groups) {
        if (rootConsumed.add(group)) {
            nextInGroup(group, groups);
        }
    }

    /** The group a subselect alias renames into: its leftmost inner
     * table's lowercased name (the engine's traverse rule). */
    private static String firstInnerTable(SqlQuery q) {
        SqlSource src = q instanceof SqlSelect s ? s.from()
                : q instanceof SqlUnion u && !u.branches().isEmpty()
                        ? (u.branches().get(0) instanceof SqlSelect bs
                                ? bs.from() : null)
                        : null;
        while (src instanceof SqlSource.Join j) {
            src = j.left();
        }
        return switch (src) {
            case SqlSource.Table t -> t.name()
                    .substring(t.name().lastIndexOf('.') + 1)
                    .toLowerCase(Locale.ROOT);
            case SqlSource.Subselect sub -> firstInnerTable(sub.inner());
            case SqlSource.VarSetPlaceholder vp ->
                    vp.varName().toLowerCase(Locale.ROOT);
            case null, default -> "subselect";
        };
    }

    /** varPlaceHolderToString prefix/suffix/replace-map per parameter
     * kind (the engine's optional-parameter templates). */
    /** The {@code varPlaceHolderToString} spelling of an optional
     * parameter; a non-default connection timeZone wraps DATETIME
     * placeholders in {@code GMTtoTZ}. */
    /** The {@code (${optionalVarPlaceHolderOperationSelector(name,
     * equalEnumOperationSelector(fn(name), 'col in (...)', 'col = ...'),
     * '0 = 1')})} spelling for {@code rawColumn = enumParam}; null when
     * the expression is not that shape. */
    private @com.legend.Nullable String enumSelector(SqlExpr e) {
        if (!(e instanceof SqlExpr.Call c)
                || c.fn() != com.legend.sql.SqlFn.EQUAL
                || c.args().size() != 2) {
            return null;
        }
        // the ENUM param may sit on EITHER side
        SqlExpr.PlanParam p =
                c.args().get(1) instanceof SqlExpr.PlanParam p1
                        && p1.enumMapFn() != null ? p1
                : c.args().get(0) instanceof SqlExpr.PlanParam p0
                        && p0.enumMapFn() != null ? p0 : null;
        if (p == null) {
            return null;
        }
        SqlExpr other = c.args().get(p == c.args().get(1) ? 0 : 1);
        // a REQUIRED param against an enum LITERAL is a host-vs-host
        // comparison — plain placeholder equality ('\${yesOrNo}' = 'NO'),
        // no store column to select over
        if (other instanceof SqlExpr.StringLit && !p.optional()) {
            return null;
        }
        // the class-prop side may arrive as the mapping's enum DECODE
        // case-chain — the template compares the RAW source column
        // (toSourceValues; the engine never compares decoded names)
        SqlExpr colExpr = decodeSourceColumn(other);
        // the rendered column sits inside the template's single-quoted
        // args — escape like the selector arm at holder-equality does
        // (C2.1: a quote-bearing spelling must not walk out of the arg)
        String col = expr(colExpr != null ? colExpr : other, 4)
                .replace("'", "\\'");
        String pn = p.name() + (p.optional() ? "![]" : "");
        String fn = p.enumMapFn() + "(" + pn + ")";
        return "(${optionalVarPlaceHolderOperationSelector(" + pn
                + ", equalEnumOperationSelector(" + fn + ", '" + col
                + " in (${" + fn + "})', '" + col + " = ${" + fn
                + "}'), '0 = 1')})";
    }

    /** The right-hand literals of an equality or an OR-tree of equalities
     *  over one source (DecodeShapes.conditionSource's shape). */
    private static void collectEqualityRhs(SqlExpr cond, java.util.List<SqlExpr> out) {
        if (cond instanceof SqlExpr.Call c && c.fn() == com.legend.sql.SqlFn.EQUAL
                && c.args().size() == 2) {
            out.add(c.args().get(1));
        } else if (cond instanceof SqlExpr.Call o && o.fn() == com.legend.sql.SqlFn.OR) {
            o.args().forEach(a -> collectEqualityRhs(a, out));
        } else {
            throw new IllegalStateException("decode branch is not an equality: " + cond);
        }
    }

    /** The ONE source expression a literal-decode case chain reads
     * ({@link com.legend.sql.DecodeShapes#sourceExpr}), or null. */
    private static @com.legend.Nullable SqlExpr decodeSourceColumn(SqlExpr e) {
        return com.legend.sql.DecodeShapes.sourceExpr(e).orElse(null);
    }

    private String holder(SqlExpr.PlanParam p) {
        String inner = p.name() + "![]";
        if (p.kind() == SqlExpr.PlanParam.Kind.DATETIME
                && timeZone != null) {
            inner = "GMTtoTZ( \"[" + timeZone + "]\" " + inner + ")";
        }
        return "${varPlaceHolderToString(" + inner + " "
                + holderArgs(p.kind()) + " \"null\")}";
    }

    /** An OPTIONAL plan parameter in an equality — NULL-SAFE on the
     * plan surface (`A is not distinct from B`, dialect-spelled via
     * {@link #nullSafeEq}; upstream #5028 split the doctrine: in-flow
     * execute keeps legacy plain-equals, PLAN surfaces are null-safe).
     * Every kind: the freemarker SELECTOR spellings of the DATE / DATETIME
     * goldens are the LEGACY (H2 1.4.200) halves of the
     * assertEqualsH2Compatible pairs. Null = not this shape. */
    private @com.legend.Nullable String optionalParamEquality(SqlExpr e) {
        if (!(e instanceof SqlExpr.Call oc)
                || oc.fn() != com.legend.sql.SqlFn.EQUAL
                || oc.args().size() != 2) {
            return null;
        }
        SqlExpr l = oc.args().get(0);
        SqlExpr r = oc.args().get(1);
        if (l instanceof SqlExpr.PlanParam lp2 && lp2.optional()
                && r instanceof SqlExpr.PlanParam rp2 && rp2.optional()) {
            // every kind is null-safe on the plan surface — the freemarker
            // SELECTOR form for DATE/DATETIME pairs is the LEGACY
            // (H2 1.4.200) golden of the assertEqualsH2Compatible pairs
            return nullSafeEq(holder(lp2), holder(rp2));
        }
        SqlExpr.PlanParam opt = l instanceof SqlExpr.PlanParam lp
                && lp.optional() ? lp
                : r instanceof SqlExpr.PlanParam rp && rp.optional()
                        ? rp : null;
        if (opt == null) {
            return null;
        }
        SqlExpr other = opt == l ? r : l;
        String otherTx = expr(other, 4);
        return opt == l
                ? nullSafeEq(holder(opt), otherTx)
                : nullSafeEq(otherTx, holder(opt));
    }

    /** NULL-SAFE equality spelling on the plan surface — H2 spells
     * {@code IS NOT DISTINCT FROM}; dialects without it (DB2) expand
     * the OR form. */
    protected String nullSafeEq(String l, String r) {
        return l + " is not distinct from " + r;
    }

    /** The engine's renderCollection template for a MANY plan param —
     * ONE spelling shared by the plain in-collection rendering and the
     * temp-table protocol's falseBlock (processInOperation). */
    public String collectionSplice(SqlExpr.PlanParam cp) {
        // the engine's renderCollection spells each replacement pair with a
        // TRAILING space ({"'" : "''" }); varPlaceHolderToString does not
        return "${renderCollection(" + cp.name() + "![] \",\" "
                + holderArgs(cp.kind()).replace("\"}", "\" }") + " \"null\")}";
    }

    protected String holderArgs(SqlExpr.PlanParam.Kind k) {
        return switch (k) {
            case RAW -> throw new IllegalStateException(
                    "RAW plan params are bare splices, never collections");
            case STRING -> "\"'\" \"'\" {\"'\" : \"''\"}";
            // DATE and DATETIME both carry the TIMESTAMP keyword on the
            // upgraded-H2 plan surface (the bare-quoted DATE args are the
            // legacy goldens of the assertEqualsH2Compatible pairs)
            case DATE, DATETIME -> "\"TIMESTAMP'\" \"'\" {}";
            case FLOAT -> "\"CAST(\" \" AS FLOAT)\" {}";
            case BOOLEAN, ENUM, OTHER -> "\"\" \"\" {}";
        };
    }

    private String rename(String alias) {
        return renames.getOrDefault(alias, alias);
    }

    private static String stripQuotes(String n) {
        return n.length() > 1 && n.startsWith("\"") && n.endsWith("\"")
                ? n.substring(1, n.length() - 1) : n;
    }

    /** Whether the column reads an ALIASED projection of a plain frame —
     * quoted spelling. NAMED frames (view/join frames render bare
     * interiors) and the distinct root materialization stay physical. */
    private boolean quotedFrameRead(SqlExpr.Column c) {
        SqlSource.Subselect sub = subselects.get(c.table());
        if (sub == null) {
            return false;
        }
        // a UNION frame reads its QUOTED output aliases (tds union
        // goldens: "unionalias_0"."lastName") — unlike view frames,
        // whose interiors render bare; holds for the raw union, the
        // re-projection wrapper and the join-isolation select* alike
        if ("unionAlias".equals(sub.frameName())) {
            return sub.inner().outputs() != null && sub.inner().outputs()
                    .stream().anyMatch(o -> c.name().equals(o.name()));
        }
        if (sub.inner() instanceof com.legend.sql.SqlUnion) {
            return false;
        }
        if (sub.frameName() != null
                || !(sub.inner() instanceof SqlSelect is)
                || is.distinct()) {
            return false;
        }
        for (SqlSelect.Projection p : is.projections()) {
            if (c.name().equals(p.alias())) {
                return true;
            }
        }
        return false;
    }

    /** The engine's H2-NEW date spelling has no space ({@code
     * DATE'2005-10-10'} — plan and h2New goldens). */
    @Override
    protected String dateLit(String iso) {
        return "DATE'" + iso + "'";
    }

    @Override
    protected String timestampLit(String iso) {
        // the engine spells the datetime separator as a SPACE
        // (TIMESTAMP'9999-12-31 00:00:00.0000'), never ISO 'T'
        if (iso.length() > 10 && iso.charAt(10) == 'T') {
            iso = iso.substring(0, 10) + ' ' + iso.substring(11);
        }
        // a NON-default connection timeZone CONVERTS datetime constants
        // (engine adjustDate: pure datetimes are GMT; the connection's
        // zone respells them — EST'18:00' prints 13:00)
        if (timeZone != null && !"GMT".equals(timeZone)
                && iso.length() >= 19) {
            try {
                java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(
                        iso.substring(0, 19).replace(' ', 'T'));
                java.time.ZoneId zone = java.time.ZoneId.of(timeZone,
                        java.time.ZoneId.SHORT_IDS);
                java.time.LocalDateTime shifted = ldt
                        .atZone(java.time.ZoneOffset.UTC)
                        .withZoneSameInstant(zone).toLocalDateTime();
                iso = shifted.toString().replace('T', ' ')
                        + iso.substring(19);
                if (iso.length() == 16) {
                    iso = iso + ":00";
                }
            } catch (java.time.DateTimeException ignored) {
                // unknown zone: spell unshifted (loudness lives in the
                // engine parity diff, not a crash)
            }
        }
        return "TIMESTAMP'" + iso + "'";
    }

    /** The engine-style spelling of an alias AFTER a render pass — the
     * plan printer's resultColumns spell the renamed alias ("root", not
     * t0); call only on the instance that rendered the SQL. */
    public String renderedAlias(String alias) {
        return rename(alias);
    }

    // == single-line lowercase clause assembly ==========================

    /** No MIR passes: engine-text goldens have NO QUALIFY spelling — the
     * select wall below stays LOUD rather than inventing one. */
    @Override
    protected java.util.List<com.legend.sql.SqlRewriter> passes() {
        // the engine text has no structural passes EXCEPT the lateral
        // explode: H2 2.1 cannot spell a correlated LATERAL, and the
        // engine's own shape for a per-row literal collection is the
        // decorrelated union (LateralExplodeToUnion)
        // …and the H2 CARRIER STRATEGIES first: a semantic collection
        // node (a collected relation exploded, a filtered collect) must
        // become rows the referee can EXECUTE on H2 — the same rungs
        // the H2 execution dialect runs (text divergence is advisory;
        // rows are the verdict)
        return java.util.List.of(
                new CarrierStrategies(CarrierStrategies.Caps.H2, false),
                new LateralExplodeToUnion());
    }

    @Override
    protected void query(StringBuilder sb, com.legend.sql.SqlQuery q,
            int depth) {
        // engine CTE text: 'with a as (...), b as (...) select ...'
        if (q instanceof com.legend.sql.SqlWith w) {
            sb.append("with ");
            for (int i = 0; i < w.ctes().size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(w.ctes().get(i).name()).append(" as (");
                query(sb, w.ctes().get(i).query(), depth);
                sb.append(')');
            }
            sb.append(' ');
            query(sb, w.body(), depth);
            return;
        }
        // engine union text: one line, lowercase, branches joined inline
        if (q instanceof com.legend.sql.SqlUnion u) {
            String op = u.all() ? " union all " : " union ";
            for (int i = 0; i < u.branches().size(); i++) {
                if (i > 0) {
                    sb.append(op);
                }
                query(sb, u.branches().get(i), depth);
            }
            return;
        }
        super.query(sb, q, depth);
    }

    @Override
    protected void select(StringBuilder sb, SqlSelect s, int depth) {
        if (s.qualify() != null) {
            throw new IllegalStateException(
                    "QUALIFY has no engine-H2 golden spelling");
        }
        sb.append("select ");
        // H2: a bare row cap is TOP; a slice is OFFSET .. FETCH NEXT
        if (s.limit() != null && s.offset() == null) {
            sb.append("top ").append(s.limit()).append(' ');
        }
        if (s.distinct()) {
            sb.append("distinct ");
        }
        // engine reAliasQuery text: an ANONYMOUS DISTINCT-key subselect
        // spells exact self-aliased columns BARE (golden: select distinct
        // "persontable_2".FIRMID); top-level/named frames keep their as
        boolean bareKeys = s.distinct() && anonDistinctDepth > 0;
        sb.append(s.projections().isEmpty() ? "*"
                : s.projections().stream()
                        .map(p -> bareKeys
                                && p.expr() instanceof SqlExpr.Column pc
                                && pc.name().equals(p.alias())
                                ? new SqlSelect.Projection(p.expr(), null,
                                        p.out())
                                : p)
                        .map(this::projection)
                        .collect(Collectors.joining(", ")));
        if (!(s.from() instanceof SqlSource.Dual)) {
            sb.append(" from ");
            source(sb, s.from(), depth);
        }
        if (s.where() != null) {
            sb.append(" where ").append(whereSql(s.where()));
        }
        if (!s.groupBy().isEmpty()) {
            sb.append(" group by ").append(s.groupBy().stream()
                    .map(e -> groupKey(s, e))
                    .collect(Collectors.joining(groupBySeparator())));
        }
        if (s.having() != null) {
            sb.append(" having ").append(expr(s.having(), 0));
        }
        if (!s.orderBy().isEmpty()) {
            sb.append(" order by ").append(s.orderBy().stream()
                    .map(this::sortKey).collect(Collectors.joining(", ")));
        }
        if (s.offset() != null) {
            sb.append(" offset ").append(s.offset()).append(" rows");
            if (s.limit() != null) {
                sb.append(" fetch next ").append(s.limit()).append(" rows only");
            }
        }
    }

    /** The WHERE clause text — DB2-family dialects wrap a top-level
     * conjunction in one extra paren pair. */
    protected String whereSql(SqlExpr w) {
        return expr(w, 0);
    }

    /** A group-by key: a key that is PROJECTED UNDER AN ALIAS spells the
     * quoted OUTPUT alias (the engine's TDS group-by-alias text —
     * {@code group by "prodName"}); a bare same-name key (mapping
     * ~groupBy has no alias) keeps the physical expression. Render-only:
     * the IR keys stay real expressions for the execution dialects. */
    protected String groupKey(SqlSelect s, SqlExpr e) {
        if (e instanceof SqlExpr.Column c && c.table() == null) {
            // a SELF-ALIASED key (ENTITY_ID as ENTITY_ID — the view
            // ~groupBy form) spells the PHYSICAL expression (golden:
            // group by "root".ENTITY_ID); only a RENAMING alias keeps
            // the quoted output name (group by "prodName")
            for (SqlSelect.Projection p : s.projections()) {
                if (c.name().equals(p.outputName())
                        && p.expr() instanceof SqlExpr.Column pc
                        && pc.name().equals(c.name())) {
                    return expr(p.expr(), 0);
                }
            }
            return '"' + c.name() + '"';
        }
        for (SqlSelect.Projection p : s.projections()) {
            if (p.outputName() != null && e.equals(p.expr())) {
                // union-frame reads always spell the quoted OUTPUT name
                // (group by "lastName") — the self-alias physical
                // spelling is a VIEW-frame rule
                boolean unionRead = p.expr() instanceof SqlExpr.Column uc
                        && s.from() instanceof SqlSource.Subselect sub
                        && "unionAlias".equals(sub.frameName())
                        && sub.alias().equals(uc.table());
                if (!unionRead && p.expr() instanceof SqlExpr.Column pc
                        && pc.name().equals(p.outputName())) {
                    return expr(e, 0);
                }
                return '"' + p.outputName().replace("\"", "") + '"';
            }
        }
        return expr(e, 0);
    }

    @Override
    protected void source(StringBuilder sb, SqlSource src, int depth) {
        switch (src) {
            case SqlSource.Table t -> {
                sb.append(quoteIdentifiers
                        ? java.util.Arrays.stream(t.name().split("\\.", -1))
                                .map(x -> '"' + x + '"')
                                .collect(java.util.stream.Collectors
                                        .joining("."))
                        : t.name());
                if (t.alias() != null) {
                    sb.append(" as \"").append(rename(t.alias())).append('"');
                }
            }
            case SqlSource.Subselect sub -> {
                sb.append('(');
                // union frames keep the QUOTED alias interior (tds
                // goldens); only VIEW frames unquote their projections
                boolean viewFrame = sub.frameName() != null
                        && !"unionAlias".equals(sub.frameName())
                        && !SqlSource.Subselect.EXISTS_KEYS_FRAME
                                .equals(sub.frameName());
                boolean anonFrame = SqlSource.Subselect.EXISTS_KEYS_FRAME
                        .equals(sub.frameName());
                if (viewFrame) {
                    frameDepth++;
                }
                if (anonFrame) {
                    anonDistinctDepth++;
                }
                try {
                    query(sb, sub.inner(), depth);
                } finally {
                    if (viewFrame) {
                        frameDepth--;
                    }
                    if (anonFrame) {
                        anonDistinctDepth--;
                    }
                }
                sb.append(") as \"").append(rename(sub.alias())).append('"');
            }
            case SqlSource.VarSetPlaceholder vp -> sb.append("(${")
                    .append(vp.varName()).append("}) as \"")
                    .append(rename(vp.alias())).append('"');
            case SqlSource.Join j -> {
                source(sb, j.left(), depth);
                sb.append(' ')
                        .append(j.kind() == SqlSource.Join.Kind.INNER
                                ? "inner join"
                                : j.kind().sql.toLowerCase(Locale.ROOT))
                        .append(' ');
                source(sb, j.right(), depth);
                if (j.on() != null) {
                    sb.append(" on (").append(expr(j.on(), 0)).append(')');
                }
            }
            default -> super.source(sb, src, depth);
        }
    }

    /** Nesting depth of NAMED frames (view-backed subselects) currently
     * rendering — their projections spell the engine view generator's
     * form: every column aliased, alias UNQUOTED. */
    private int frameDepth;
    /** Anonymous-subselect nesting — the bare DISTINCT-key spelling is
     * scoped to it (same lifecycle as frameDepth). */
    private int anonDistinctDepth;

    /** engine spells the CORRELATED exists form 'exists (select 1
     * from ...)' (buildExistsPredicate emission) — lowercase keyword,
     * literal-1 projection regardless of the subquery's own columns
     * (testProcessingTemporalPropertyQuery golden). */
    private String correlatedExistsSpelling(SqlSelect xs) {
        return "exists (" + inline(xs.withProjections(
                java.util.List.of(new SqlSelect.Projection(
                        new SqlExpr.IntLit(1), null, null)))) + ")";
    }

    /** engine text: an arithmetic op with a MIXED-OPERATOR composite
     * operand wraps itself (testProp3 golden (((1.0*q)/basis)*rate) —
     * TIMES over DIVIDE); SAME-operator chains stay flat (golden
     * quantity + quantity + 3), lone ops stay bare (AGE * AGE).
     * parentPrec >= 7 = the base renderer already wraps. */
    private static boolean mixedOperandArithmetic(SqlExpr.Call ac,
            int parentPrec) {
        return parentPrec < 7
                && (ac.fn() == com.legend.sql.SqlFn.PLUS
                        || ac.fn() == com.legend.sql.SqlFn.MINUS
                        || ac.fn() == com.legend.sql.SqlFn.TIMES)
                && ac.args().stream().anyMatch(x -> {
                    SqlExpr u = x instanceof SqlExpr.Group g ? g.inner() : x;
                    return u instanceof SqlExpr.Call cc
                            && cc.fn() != ac.fn()
                            && (cc.fn() == com.legend.sql.SqlFn.PLUS
                                    || cc.fn() == com.legend.sql.SqlFn.MINUS
                                    || cc.fn() == com.legend.sql.SqlFn.TIMES
                                    || cc.fn() == com.legend.sql.SqlFn.DIVIDE);
                });
    }

    @Override
    protected String projection(SqlSelect.Projection p) {
        // engine text spells a PROJECTED date constant as a PLAIN string
        // ('2015-10-16' as "k_processingDate" —
        // testProcessingTemporalPropertyQuery golden); COMPARISON
        // positions keep the typed DATE'...' literal
        String e = p.expr() instanceof SqlExpr.DateLit dl
                ? "'" + dl.iso() + "'"
                : expr(p.expr(), 0);
        // synthetic scalar-map column: the engine spells a bare map
        // scalar select UNALIASED — this renderer IS the engine-TEXT
        // channel (execution renders through H2/H2Modern, which keep
        // the alias references need); batch 136 retired the
        // TextGoldens thread flag that used to say so
        if (p.alias() != null
                && p.alias().startsWith(com.legend.sql.SqlSelect.SYNTH_MAP_COL)) {
            return e;
        }
        if (frameDepth > 0 && p.outputName() != null) {
            // engine view SQL: '"root".ORDER_ID as ORDER_ID' — always
            // aliased, unquoted
            return e + " as " + p.outputName().replace("\"", "");
        }
        if (p.alias() == null) {
            return e;
        }
        // a PRE-QUOTED alias (the corpus's '"firstName"' spellings)
        // must not double-wrap — the engine prints one quote level
        String a = p.alias();
        if (a.length() > 1 && a.startsWith("\"") && a.endsWith("\"")) {
            return e + " as " + a;
        }
        return e + " as \"" + a + '"';
    }

    /** The row-order pseudo-column rides the SAME alias plan as
     *  ordinary column reads. */
    private String rowOrder(SqlExpr.RowOrder ro) {
        return ro.table() == null ? "rowid"
                : '"' + rename(ro.table()) + "\".rowid";
    }

    /** Engine h2Extension dynaFnToSql: nullSafeEqual =
     *  {@code %s is not distinct from %s} (identical in 1.4.200 and
     *  2.1.214), no parens in the format. */
    private String nullSafeSpelling(SqlExpr.Call bc) {
        String op = bc.fn() == com.legend.sql.SqlFn.NULL_SAFE_EQUAL
                ? " is not distinct from " : " is distinct from ";
        return expr(bc.args().get(0), 4) + op + expr(bc.args().get(1), 4);
    }

    /** Engine H2 text joins group-by keys with {@code ", "}; the DB2
     *  extension spells the bare comma (testGroupByWithJoinDB2 golden). */
    protected String groupBySeparator() {
        return ", ";
    }

    @Override
    protected String expr(SqlExpr e, int parentPrec) {
        // plan-template parameter (engine freemarker): strings are
        // single-quoted with the engine's escape template
        // engine text spells float LITERALS bare (testProp3 golden:
        // basis = 0.0) — cast(%s as float) is the parseFloat DYNAFUNCTION
        // format (h2Extension2_1_214:246) and the Float-typed variant
        // element access (:428), never the literal; the ANSI DOUBLE-cast
        // stays a DuckDB execution idiom
        if (e instanceof SqlExpr.FloatLit f) {
            return String.valueOf(f.value());
        }
        if (e instanceof SqlExpr.DecimalLit d) { // engine H2 decimal spelling (testDecimal)
            return "cast(" + d.value().toPlainString() + " as Decimal(32,16))";
        }
        if (e instanceof SqlExpr.Exists xx
                && xx.subquery() instanceof SqlSelect xs) {
            return correlatedExistsSpelling(xs);
        }
        if (e instanceof SqlExpr.Call ac
                && mixedOperandArithmetic(ac, parentPrec)) {
            return "(" + super.expr(e, 0) + ")";
        }
        if (e instanceof SqlExpr.PlanParam p) {
            // an OPTIONAL parameter spells the varPlaceHolderToString
            // template in EVERY position (comparisons, null guards) —
            // the selector arm below owns only the equality form
            if (p.optional()) {
                return holder(p);
            }
            return switch (p.kind()) {
                case RAW -> "${" + p.name() + "}";
                case STRING -> "'${" + p.name()
                        + "?replace(\"'\", \"''\")}'";
                // h2New spells date-typed placeholders with the type
                // keyword (TIMESTAMP'${reportEndDate.date}'); a non-default
                // connection timeZone wraps DATETIME in GMTtoTZ (the same
                // template the optional holder spells)
                // DATE spells the h2New TIMESTAMP keyword in every
                // position — the bare-quoted '${bd}' is the LEGACY
                // (H2 1.4.200) golden of the assertEqualsH2Compatible
                // pairs (executionPlanTest testTemporalDateVariable*),
                // never the upgraded one this oracle replays
                case DATE -> "TIMESTAMP'${" + p.name() + "}'";
                case DATETIME -> timeZone != null
                        ? "TIMESTAMP'${GMTtoTZ( \"[" + timeZone + "]\" "
                                + p.name() + ")}'"
                        : "TIMESTAMP'${" + p.name() + "}'";
                // enum params spell QUOTED, no escape template
                // ('\${yesOrNo}' = 'NO' — testIfEnumParameterInProject)
                case ENUM -> "'${" + p.name() + "}'";
                case FLOAT, BOOLEAN, OTHER -> "${" + p.name() + "}";
            };
        }
        // ENUM parameter comparison: one selector template covers = and
        // in (equalEnumOperationSelector picks by the mapped value's
        // cardinality); negation spells lowercase not around it
        if (e instanceof SqlExpr.Call nc
                && nc.fn() == com.legend.sql.SqlFn.NOT
                && nc.args().size() == 1) {
            String et = enumSelector(nc.args().get(0));
            if (et != null) {
                return "not " + et;
            }
        }
        String et0 = enumSelector(e);
        if (et0 != null) {
            return et0;
        }
        String optEq = optionalParamEquality(e);
        if (optEq != null) {
            return optEq;
        }
        // a property read THROUGH a plan parameter spells the engine's
        // dotted placeholder ('${reportEndDate.date}' — Allocation-bound
        // instance fields in the terminal's SQL)
        if (e instanceof SqlExpr.StructGet sg) {
            // walk NESTED gets down to the plan param: $var.a.b spells
            // '${var.a.b}' (E2E §4.4 cluster 5 — only one level walked
            // before)
            java.util.ArrayDeque<String> path = new java.util.ArrayDeque<>();
            SqlExpr cur = sg;
            while (cur instanceof SqlExpr.StructGet g2) {
                path.addFirst(g2.field());
                cur = g2.source();
            }
            if (cur instanceof SqlExpr.PlanParam pp) {
                return "'${" + pp.name() + "." + String.join(".", path)
                        + "}'";
            }
            // engine-H2 text has no struct vocabulary — a named wall
            // (SHAPE in the plan branch), not a dialect bug
            throw new UnsupportedOperationException(
                    "plan: struct extraction has no engine-H2 spelling");
        }
        // a QUALIFIED star's frame alias renames exactly like a column
        // read ("root".* — the dated-head nav frame; batch 4: the star
        // was the ONE reference class that bypassed rename())
        if (e instanceof SqlExpr.Star st && st.table() != null) {
            return '"' + rename(st.table()) + "\".*";
        }
        // alias part quoted, physical column bare — "root".FIRSTNAME;
        // reads of a frame's PROJECTED TDS aliases quote the column too
        // ("persontable_0"."firstName" — rename/union frame goldens)
        if (e instanceof SqlExpr.Column c) {
            if (c.table() != null && quotedFrameRead(c)) {
                if (placeholders.contains(c.table())) { // placeholder reads: unquoted col
                    return '"' + rename(c.table()) + "\"." + stripQuotes(c.name());
                }
                return '"' + rename(c.table()) + "\".\"" + c.name() + '"';
            }
            return c.table() == null ? phys(c.name())
                    : '"' + rename(c.table()) + "\"." + phys(c.name());
        }
        if (e instanceof SqlExpr.RowOrder ro) {
            return rowOrder(ro);
        }
        String dd = engineDateDiff(e);
        if (dd != null) {
            return dd;
        }
        // engine boolean text: lowercase keywords, AND groups
        // parenthesized once around the flattened chain —
        // '(x is not null and x > y)' (relative-date goldens)
        if (e instanceof SqlExpr.Call bc) {
            switch (bc.fn()) {
                case NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL -> {
                    return nullSafeSpelling(bc);
                }
                case AND -> {
                    // engine 'and' renders FLAT with no parens at any
                    // arity (extensionDefaults.pure:189) — parens come
                    // only from explicit Group nodes and opposite-operator
                    // nesting (the OR arm below)
                    java.util.List<String> terms = new java.util.ArrayList<>();
                    flattenAnd(bc, terms);
                    return String.join(" and ", terms);
                }
                case OR -> {
                    // and-under-or parenthesizes (the engine's
                    // newAndOrDynaFunctionRelaxedBrackets opposite-operator
                    // group, pureToSQLQuery.pure:5376)
                    java.util.List<String> ops = new java.util.ArrayList<>();
                    for (SqlExpr o : bc.args()) {
                        boolean andLike = o instanceof SqlExpr.Call oc
                                && oc.fn() == com.legend.sql.SqlFn.AND;
                        ops.add(andLike ? "(" + expr(o, 0) + ")"
                                : expr(o, 0));
                    }
                    return String.join(" or ", ops);
                }
                case COALESCE -> {
                    // the null-guarded in() (pure in never returns null;
                    // COALESCE(x in (...), false) is our EXECUTION idiom)
                    // spells the BARE in in engine text — plan-param
                    // templates and literal lists alike; guards may STACK
                    // (in-rule + filter-site), unwrap them all
                    if (bc.args().size() == 2
                            && bc.args().get(1) instanceof SqlExpr.BoolLit bl0
                            && !bl0.value()) {
                        SqlExpr in0 = bc.args().get(0);
                        while (in0 instanceof SqlExpr.Call cc
                                && cc.fn() == com.legend.sql.SqlFn.COALESCE
                                && cc.args().size() == 2
                                && cc.args().get(1)
                                        instanceof SqlExpr.BoolLit bl1
                                && !bl1.value()) {
                            in0 = cc.args().get(0);
                        }
                        if (in0 instanceof SqlExpr.Call ic0
                                && ic0.fn() == com.legend.sql.SqlFn.IN) {
                            return expr(ic0, parentPrec);
                        }
                    }
                }
                case IN -> {
                    // the temp-table IN splice (processInOperation's
                    // over-threshold arm): the engine's fixed
                    // generateTempTableSelectSQLQuery template
                    if (bc.args().size() == 2
                            && bc.args().get(1)
                                    instanceof SqlExpr.TempTableInSplice ts) {
                        String a = ts.tempTableName().toLowerCase(
                                java.util.Locale.ROOT) + "_0";
                        return expr(bc.args().get(0), 4)
                                + " in (select \"" + a
                                + "\".ColumnForStoringInCollection as"
                                + " ColumnForStoringInCollection from "
                                + ts.tempTableName() + " as \"" + a
                                + "\")";
                    }
                    // a COLLECTION-typed plan parameter spells the
                    // engine's renderCollection template — separator ","
                    // plus the SAME per-kind prefix/suffix/escape args as
                    // varPlaceHolderToString (in-collection plan goldens)
                    if (bc.args().size() == 2
                            && bc.args().get(1) instanceof SqlExpr.PlanParam cp) {
                        // the temp-table protocol's wrapper variable —
                        // a bare splice, never a collection template
                        if (cp.kind() == SqlExpr.PlanParam.Kind.RAW) {
                            return expr(bc.args().get(0), 4)
                                    + " in (${" + cp.name() + "})";
                        }
                        // a non-default connection timeZone spells the
                        // TZ-SHIFTING template (renderCollectionWithTz —
                        // tz second, no escape map) over datetime params
                        if (cp.kind() == SqlExpr.PlanParam.Kind.DATETIME
                                && timeZone != null) {
                            // top-of-sql template — quotes UNESCAPED
                            // (only freemarker-nested args escape)
                            return expr(bc.args().get(0), 4)
                                    + " in (${renderCollectionWithTz("
                                    + cp.name() + "![] \"[" + timeZone
                                    + "]\" \",\" \"TIMESTAMP'\" \"'\""
                                    + " \"null\")})";
                        }
                        return expr(bc.args().get(0), 4)
                                + " in (" + collectionSplice(cp) + ")";
                    }
                    // the engine collapses a SINGLETON literal in-list
                    // to equality ('x in ([v])' text = 'x = v')
                    if (bc.args().size() == 2) {
                        return expr(bc.args().get(0), 4) + " = "
                                + expr(bc.args().get(1), 4);
                    }
                    // engine keyword text is lowercase
                    StringBuilder items = new StringBuilder();
                    for (int i = 1; i < bc.args().size(); i++) {
                        if (items.length() > 0) {
                            items.append(", ");
                        }
                        items.append(expr(bc.args().get(i), 0));
                    }
                    return expr(bc.args().get(0), 4) + " in (" + items + ")";
                }
                case IS_NULL -> {
                    return expr(bc.args().get(0), 4) + " is null";
                }
                case IS_NOT_NULL -> {
                    return expr(bc.args().get(0), 4) + " is not null";
                }
                default -> { }
            }
        }
        return super.expr(e, parentPrec);
    }

    private void flattenAnd(SqlExpr e, java.util.List<String> out) {
        if (e instanceof SqlExpr.Call c
                && c.fn() == com.legend.sql.SqlFn.AND) {
            for (SqlExpr a : c.args()) {
                flattenAnd(a, out);
            }
            return;
        }
        // or-under-and parenthesizes (the opposite-operator group,
        // symmetric with the OR arm; engine golden: 'a and (x or y) and
        // b' — the flat join silently REBOUND the chain: '... and x or
        // y and ...' parsed as (… and x) or (y and …)). An explicit
        // Group child wraps ITSELF — no double parens.
        boolean orLike = e instanceof SqlExpr.Call oc
                && oc.fn() == com.legend.sql.SqlFn.OR;
        out.add(orLike ? "(" + expr(e, 0) + ")" : expr(e, 3));
    }

    /** Engine aggregate names are lowercase ({@code sum(}, {@code count(}
     * — every aggregation golden's spelling). */
    @Override
    protected String reducer(com.legend.sql.SqlAgg.Reducer r) {
        // the H2-LENIENT per-group witness spells the BARE expression
        // (view ~groupBy per-row columns — H2 1.x goldens never wrap;
        // our DB-side form is ANY_VALUE, an engine-text-only unwrap)
        if (r.fn() == com.legend.sql.SqlAgg.Fn.ANY_VALUE && r.args().size() == 1) {
            return expr(r.args().get(0), 0);
        }
        // percentile: the engine's inverse-distribution form with the
        // direction spelled explicitly (extensionDefaults.pure:790) —
        // percentile_cont(p) within group (order by v asc)
        if ((r.fn() == com.legend.sql.SqlAgg.Fn.QUANTILE_CONT
                    || r.fn() == com.legend.sql.SqlAgg.Fn.QUANTILE_DISC)
                && r.args().size() == 2 && !r.distinct()
                && r.orderBy().size() <= 1) {
            boolean desc = !r.orderBy().isEmpty()
                    && !r.orderBy().get(0).ascending();
            return (r.fn() == com.legend.sql.SqlAgg.Fn.QUANTILE_CONT
                    ? "percentile_cont(" : "percentile_disc(")
                    + expr(r.args().get(1), 0)
                    + ") within group (order by " + expr(r.args().get(0), 0)
                    + (desc ? " desc" : " asc") + ")";
        }
        String s = super.reducer(r);
        int p = s.indexOf('(');
        return s.substring(0, p).toLowerCase(Locale.ROOT) + s.substring(p);
    }

    /** Engine sort keys spell the direction EXPLICITLY and lowercase
     * ({@code asc}/{@code desc} — every ordered golden's spelling). */
    @Override
    protected String sortKey(com.legend.sql.SqlSelect.SortKey k) {
        // a COLUMN-NAME-keyed sort (or a table-less column key) is an
        // OUTPUT-column reference (TDS ->sort): the engine spells it
        // quoted — `order by "name" asc`
        String e = k.outputName() != null
                ? '"' + k.outputName().replace("\"", "") + '"'
                : k.expr() instanceof SqlExpr.Column c && c.table() == null
                        ? '"' + c.name() + '"'
                        : expr(k.expr(), 0);
        // ENGINE-VERBATIM: the engine never spells a NULLS clause in
        // ORDER BY (every studied golden) — this TEXT channel suppresses
        // the IR's semantic null-order stamp ENTIRELY. Slice 10 made
        // Fold stamp pure's null-is-largest (DESC → NULLS_FIRST), which
        // the old restates-the-default filter printed here, dropping 5
        // corpus rows out of text-matched (h2-exec floor 296 → 291;
        // rows still verified — the EXECUTION renderers keep the stamp,
        // so H2 exec and DuckDB agree). Engine-on-H2 rides H2's default
        // placement instead — the same per-backend upstream divergence
        // class as the index-base fork; drop-in text wins on this
        // surface.
        return e + (k.ascending() ? " asc" : " desc");
    }

    /** ENGINE-VERBATIM, the sortKey suppression's aggregate-internal
     * twin: the engine never spells a NULLS clause inside an ordered
     * aggregate either — the TEXT channel suppresses the IR's semantic
     * null-order stamp (execution renderers keep it; witness the
     * relation wall burn's rescued golden, 2026-08-23). */
    @Override
    protected String aggOrderNullPlacement(com.legend.sql.SqlSelect.SortKey k) {
        return "";
    }

    /** Engine window text is lowercase: {@code sum(...) over (partition
     * by ... order by ...)} (the window-col goldens' spelling). */
    @Override
    protected String windowCall(SqlExpr.WindowCall w) {
        String s = super.windowCall(w);
        return s.replace(" OVER (", " over (")
                .replace("PARTITION BY ", "partition by ")
                .replace("ORDER BY ", "order by ");
    }

    /**
     * dateDiff's composite IR shapes fold BACK to the engine's plain
     * {@code datediff(unit, a, b)} emission. The IR encodes REAL pure's
     * per-unit semantics (truncated elapsed time; Sunday-boundary weeks —
     * PCT-pinned, Scalars.dateDiffExpr); the engine's H2 SQL emits plain
     * DATEDIFF regardless, and the toSQLString goldens pin that TEXT.
     * The shapes (epoch_ms pairs under integer division; the week CASE)
     * are only produced by the dateDiff lowering.
     */
    private @com.legend.Nullable String engineDateDiff(SqlExpr e) {
        // truncated elapsed: (epoch_ms(end) - epoch_ms(start)) // unitMs
        if (e instanceof SqlExpr.Call div
                && div.fn() == com.legend.sql.SqlFn.INT_DIVIDE
                && div.args().size() == 2
                && div.args().get(1) instanceof SqlExpr.IntLit u
                && div.args().get(0) instanceof SqlExpr.Call minus
                && minus.fn() == com.legend.sql.SqlFn.MINUS
                && minus.args().size() == 2
                && minus.args().get(0) instanceof SqlExpr.Call end
                && end.fn() == com.legend.sql.SqlFn.EPOCH_MS
                && minus.args().get(1) instanceof SqlExpr.Call start
                && start.fn() == com.legend.sql.SqlFn.EPOCH_MS) {
            String unit = switch ((int) u.value()) {
                case 3_600_000 -> "hour";
                case 60_000 -> "minute";
                case 1_000 -> "second";
                default -> null;
            };
            if (unit != null) {
                return "datediff(" + unit + ", " + expr(start.args().get(0), 0)
                        + ", " + expr(end.args().get(0), 0) + ")";
            }
        }
        // Sunday-boundary weeks: CASE WHEN date_diff('day', end, start) <= 0
        // THEN forward ELSE backward. The BRANCHES must also match the
        // sundayIndex-difference shape (audit 19 F1): a user-written
        // if(dateDiff(a,b,DAYS) <= 0, |x, |y) lowers to the SAME guard,
        // and folding it would silently delete both branches.
        if (e instanceof SqlExpr.Case cs && cs.whens().size() == 1
                && cs.otherwise() != null
                && cs.whens().get(0).condition() instanceof SqlExpr.Call le
                && le.fn() == com.legend.sql.SqlFn.LESS_EQUAL
                && le.args().size() == 2
                && le.args().get(0) instanceof SqlExpr.Call dayDiff
                && dayDiff.fn() == com.legend.sql.SqlFn.DATE_DIFF
                && dayDiff.args().get(0) instanceof SqlExpr.StringLit day
                && day.value().equals("day")
                && le.args().get(1) instanceof SqlExpr.IntLit zero
                && zero.value() == 0
                && isSundayIndexDifference(cs.whens().get(0).then())
                && isSundayIndexDifference(cs.otherwise())) {
            return "datediff(week, " + expr(dayDiff.args().get(2), 0)
                    + ", " + expr(dayDiff.args().get(1), 0) + ")";
        }
        // GENERIC case spelling (engine text): lowercase keywords —
        // placed BELOW the specialized recognizers (week-diff) that
        // fold whole CASE shapes into engine idioms
        // the mapping's ENUM DECODE chain pushed into the SQL (the plan's
        // PUSH_DOWN_ENUM_TRANSFORM form): the engine spells it FLAT, a
        // multi-source-value branch as `col in (a, b)`, a single one as
        // `col = a`, `else null` (pureToSQLQuery processEnumMapping) — the
        // lowered if-chain nests through its otherwise and ORs its arms
        java.util.Optional<SqlExpr> decodeSrc = e instanceof SqlExpr.Case
                ? com.legend.sql.DecodeShapes.sourceExpr(e) : java.util.Optional.empty();
        java.util.Optional<java.util.List<SqlExpr.Case.When>> decodeArms = decodeSrc.isPresent()
                ? com.legend.sql.DecodeShapes.flattenDecode(e) : java.util.Optional.empty();
        if (decodeSrc.isPresent() && decodeArms.isPresent()) {
            String src = expr(decodeSrc.get(), 4);
            StringBuilder sb = new StringBuilder("case");
            for (SqlExpr.Case.When w : decodeArms.get()) {
                java.util.List<SqlExpr> values = new java.util.ArrayList<>();
                collectEqualityRhs(w.condition(), values);
                sb.append(" when ").append(src);
                if (values.size() == 1) {
                    sb.append(" = ").append(expr(values.get(0), 0));
                } else {
                    sb.append(" in (").append(values.stream().map(v -> expr(v, 0))
                            .collect(java.util.stream.Collectors.joining(", "))).append(')');
                }
                sb.append(" then ").append(expr(w.then(), 0));
            }
            return sb.append(" else null end").toString();
        }
        if (e instanceof SqlExpr.Case c) {
            StringBuilder sb = new StringBuilder("case");
            for (SqlExpr.Case.When w : c.whens()) {
                sb.append(" when ").append(expr(w.condition(), 0))
                        .append(" then ").append(expr(w.then(), 0));
            }
            if (c.otherwise() != null) {
                sb.append(" else ").append(expr(c.otherwise(), 0));
            }
            return sb.append(" end").toString();
        }
        return null;
    }

    /** {@code sundayIndex(d2) - sundayIndex(d1)} where sundayIndex =
     * {@code date_diff('day', DATE '0001-01-07', d) // 7} (Scalars'
     * dateDiff week emission — the only producer of this shape). */
    private static boolean isSundayIndexDifference(SqlExpr e) {
        return e instanceof SqlExpr.Call m
                && m.fn() == com.legend.sql.SqlFn.MINUS
                && m.args().size() == 2
                && isSundayIndex(m.args().get(0))
                && isSundayIndex(m.args().get(1));
    }

    private static boolean isSundayIndex(SqlExpr e) {
        return e instanceof SqlExpr.Call div
                && div.fn() == com.legend.sql.SqlFn.INT_DIVIDE
                && div.args().size() == 2
                && div.args().get(1) instanceof SqlExpr.IntLit seven
                && seven.value() == 7
                && div.args().get(0) instanceof SqlExpr.Call dd
                && dd.fn() == com.legend.sql.SqlFn.DATE_DIFF
                && dd.args().get(1) instanceof SqlExpr.DateLit epoch
                && epoch.iso().equals("0001-01-07");
    }

    // == engine-H2 spellings (each MATCHED against a corpus golden) =====

    /** DurationUnit interval-fn -> H2 dateadd unit keyword. LOWERCASE:
     * the H2Compatible NEW goldens ride the sql-dialect-translation
     * mapToDBUnitType (sqlDialectExtensionDefaults.pure:362 'second'),
     * not the legacy extension's uppercase table — the uppercase
     * dateadd(DAY,...) corpus spellings are firstDayOf* FORMAT literals,
     * never adjust units. */
    private static String dbUnitOf(String unitFn) {
        return switch (unitFn) {
            case "to_years" -> "year";
            case "to_months" -> "month";
            case "to_weeks" -> "week";
            case "to_days" -> "day";
            case "to_hours" -> "hour";
            case "to_minutes" -> "minute";
            case "to_seconds" -> "second";
            case "to_milliseconds" -> "millisecond";
            case "to_microseconds" -> "microsecond";
            default -> throw new IllegalStateException(
                    "no H2 dateadd unit for interval fn '" + unitFn + "'");
        };
    }

    @Override
    protected String call(SqlExpr.Call c, int parentPrec) {
        java.util.List<SqlExpr> a = c.args();
        String flat = joinStringsFlat(c);
        if (flat != null) {
            return flat;
        }
        // H2 digest spelling: rawtohex(hash('SHA-256', x)) — the engine's
        // relational H2 codegen for every HashType
        if (c.fn() == com.legend.sql.SqlFn.MD5
                || c.fn() == com.legend.sql.SqlFn.SHA1
                || c.fn() == com.legend.sql.SqlFn.SHA256) {
            String digest = c.fn() == com.legend.sql.SqlFn.MD5 ? "MD5"
                    : c.fn() == com.legend.sql.SqlFn.SHA1 ? "SHA-1"
                    : "SHA-256";
            return "rawtohex(hash('" + digest + "', "
                    + expr(c.args().get(0), 0) + "))";
        }
        return switch (c.fn()) {
            // engine-H2 spellings (sqlstring goldens): cbrt has no H2
            // native; the trim family spells regexp; pads ride the
            // legend H2 extension with the pad char EXPLICIT
            case CBRT -> "power(" + expr(a.get(0), 0) + ", 1.0/3)";
            // engine spells the FULL substring keyword and indexOf as
            // locate(needle, haystack)
            case SUBSTRING -> "substring(" + a.stream()
                    .map(x -> expr(x, 0))
                    .collect(java.util.stream.Collectors.joining(", "))
                    + ")";
            case STRPOS -> "locate(" + expr(a.get(1), 0) + ", "
                    + expr(a.get(0), 0) + ")";
            case LTRIM -> "regexp_replace(" + expr(a.get(0), 0)
                    + ", '^[ ]+', '')";
            case RTRIM -> "regexp_replace(" + expr(a.get(0), 0)
                    + ", '[ ]+$', '')";
            case LPAD -> "legend_h2_extension_lpad(" + expr(a.get(0), 0)
                    + ", " + expr(a.get(1), 0) + ", "
                    + (a.size() > 2 ? expr(a.get(2), 0) : "' '") + ")";
            case RPAD -> "legend_h2_extension_rpad(" + expr(a.get(0), 0)
                    + ", " + expr(a.get(1), 0) + ", "
                    + (a.size() > 2 ? expr(a.get(2), 0) : "' '") + ")";
            // n-ary concat: nested CONCAT calls SPLICE (the engine emits
            // one flat concat(a, '_', b), never concat(concat(a,'_'),b))
            case CONCAT -> "concat(" + flattenConcat(a).stream()
                    .map(x -> expr(x, 0))
                    .collect(Collectors.joining(", ")) + ")";
            // datediff(<bare unit>, a, b); composite elapsed-time forms
            // built at lowering (weeks/hours/...) have no re-spelling here
            case DATE_DIFF -> a.get(0) instanceof SqlExpr.StringLit u
                    ? "datediff(" + u.value() + ", " + expr(a.get(1), 0)
                            + ", " + expr(a.get(2), 0) + ")"
                    : super.call(c, parentPrec);
            // engine H2 adjust: dateadd(UNIT, n, x) (h2Extension dynaFn
            // 'adjust' + extensionDefaults mapToDBUnitType) — the ANSI
            // base's d + to_days(n) is the DuckDB-executable spelling
            case ADD_INTERVAL -> "dateadd("
                    + dbUnitOf(((SqlExpr.StringLit) a.get(0)).value()) + ", "
                    + expr(a.get(1), 0) + ", " + expr(a.get(2), 0) + ")";
            // milestoning adjust channel: the same lowercase unit — the
            // UPPERCASE dateadd(DAY, ...) corpus spellings are all LEGACY
            // (H2 1.4.200) goldens of assertEqualsH2Compatible pairs
            case ADD_INTERVAL_TEMPORAL -> "dateadd("
                    + dbUnitOf(((SqlExpr.StringLit) a.get(0)).value()) + ", "
                    + expr(a.get(1), 0) + ", " + expr(a.get(2), 0) + ")";
            // engine h2 parseInteger dynaFn golden spelling; execution
            // dialects keep the 64-bit BIGINT cast
            case PARSE_INT -> "cast(" + expr(a.get(0), 0) + " as integer)";
            // bool text keeps the golden cast spelling byte-for-byte
            case BOOL_TO_TEXT -> "cast(" + expr(a.get(0), 0)
                    + " as varchar)";
            case CHR -> "char(" + expr(a.get(0), 0) + ")";
            case LENGTH -> "char_length(" + expr(a.get(0), 0) + ")";
            case REVERSE_STRING -> "legend_h2_extension_reverse_string("
                    + expr(a.get(0), 0) + ")";
            case SPLIT_PART -> "legend_h2_extension_split_part("
                    + a.stream().map(x -> expr(x, 0))
                            .collect(Collectors.joining(", ")) + ")";
            case TODAY -> "cast(now() as date)";
            // enum-by-name temporals: the engine's H2 formatdatetime forms.
            // UNMATCHED formats THROW — falling back to strftime() would
            // leak a DuckDB spelling into engine-H2 golden text (audit 19)
            case STRFTIME -> {
                if (a.size() == 2 && a.get(1) instanceof SqlExpr.FormatLit fl
                        && fl.parts().size() == 1
                        && fl.parts().get(0) instanceof com.legend.sql.DateFmt.Part p) {
                    String java = switch (p) {
                        case MONTH_NAME -> "MMMM";
                        case WEEKDAY_NAME -> "EEEE";
                        default -> null;
                    };
                    if (java != null) {
                        yield "formatdatetime(" + expr(a.get(0), 0) + ", '"
                                + java + "')";
                    }
                }
                throw new IllegalStateException("strftime format has no"
                        + " engine-H2 formatdatetime spelling yet: " + a);
            }
            case TRIM -> a.size() == 1
                    ? "trim(both from " + expr(a.get(0), 0) + ")"
                    : super.call(c, parentPrec);
            case DATE_TRUNC_DAY -> "cast(truncate(" + expr(a.get(0), 0)
                    + ") as date)";
            // contains(x, 'lit') lowers strpos(x, lit) > 0; the engine's
            // H2 spelling is the LIKE form (extensionDefaults 'contains'
            // — m2m2rShowcase golden: description like '%RECEIVE CASH%')
            case GREATER -> a.size() == 2
                    && a.get(0) instanceof SqlExpr.Call sp
                    && sp.fn() == com.legend.sql.SqlFn.STRPOS
                    && sp.args().size() == 2
                    && sp.args().get(1) instanceof SqlExpr.StringLit lit
                    && a.get(1) instanceof SqlExpr.IntLit z
                    && z.value() == 0
                    ? expr(sp.args().get(0), 0) + " like '%"
                            + lit.value().replace("'", "''") + "%'"
                    : super.call(c, parentPrec);
            // extract-part goldens spell the SQL-standard extract form
            // (testToSQLString.pure:368 'extract(doy from ...)'; the
            // engine's spelling of that form: oracleExtension.pure:204)
            case EXTRACT -> a.size() == 2
                    && a.get(0) instanceof SqlExpr.StringLit part
                    && "doy".equals(part.value())
                    ? "extract(doy from " + expr(a.get(1), 0) + ")"
                    : super.call(c, parentPrec);
            // firstDayOf* family: every H2 golden spells the uniform
            // double cast; a TODAY anchor renders bare now() inside
            case DATE_TRUNC -> {
                if (a.size() == 2 && a.get(0) instanceof SqlExpr.StringLit u
                        && Set.of("week", "month", "quarter", "year")
                                .contains(u.value())) {
                    String anchor = a.get(1) instanceof SqlExpr.Call tc
                            && tc.fn() == com.legend.sql.SqlFn.TODAY
                            ? "now()" : expr(a.get(1), 0);
                    yield "cast(cast(date_trunc('" + u.value() + "', "
                            + anchor + ") as timestamp) as date)";
                }
                yield super.call(c, parentPrec);
            }
            // parse-date family: the engine's rule (convertToDateH2) is
            // substring(x, 1, 10) + the Java pattern for ALL date-only
            // formats; datetime formats parse the whole string. UNMATCHED
            // formats THROW rather than leak DuckDB strptime() text.
            // parseDate: the engine's toTimestamp dyna function on H2
            // (h2Extension2_1_214 transformToTimestampH2 — ONE fixed pattern)
            case PARSE_DATE -> "cast(parsedatetime(" + expr(a.get(0), 0)
                    + ", 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS][.SSSSSSSS][.SSSSSSS][.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]') as timestamp)";
            case STRPTIME -> {
                if (a.size() == 2 && a.get(1) instanceof SqlExpr.FormatLit fl) {
                    String java = h2Pattern(fl);
                    if (java != null) {
                        boolean dateOnly = !fl.parts()
                                .contains(com.legend.sql.DateFmt.Part.HOUR2);
                        // the engine's MMMyyyy hack (convertToDateH2 FIXME):
                        // a month-year text parses with a '01' day prepended
                        if (java.equals("MMMyyyy")) {
                            yield "parsedatetime(concat('01', " + expr(a.get(0), 0)
                                    + "), 'ddMMMyyyy')";
                        }
                        yield dateOnly
                                ? "parsedatetime(substring(" + expr(a.get(0), 0)
                                        + ", 1, 10), '" + java + "')"
                                : "parsedatetime(" + expr(a.get(0), 0)
                                        + ", '" + java + "')";
                    }
                }
                throw new IllegalStateException("strptime format has no"
                        + " engine-H2 parsedatetime spelling yet: " + a);
            }
            default -> super.call(c, parentPrec);
        };
    }

    /** TYPED format parts → the Java pattern the engine's parsedatetime
     * takes; null when a part has no mapping (the caller throws — never a
     * silent DuckDB fallback). No format string is ever re-parsed here. */
    private static @com.legend.Nullable String h2Pattern(SqlExpr.FormatLit fl) {
        StringBuilder out = new StringBuilder();
        for (com.legend.sql.DateFmt d : fl.parts()) {
            switch (d) {
                case com.legend.sql.DateFmt.Text t -> out.append(t.s());
                case com.legend.sql.DateFmt.Part p -> {
                    String java = switch (p) {
                        case YEAR4 -> "yyyy";
                        case MONTH2 -> "MM";
                        case MONTH_ABBREV -> "MMM";
                        case DAY2 -> "dd";
                        case HOUR2 -> "HH";
                        case MIN2 -> "mm";
                        case SEC2 -> "ss";
                        default -> null;
                    };
                    if (java == null) {
                        return null;
                    }
                    out.append(java);
                }
            }
        }
        return out.toString();
    }


    @Override
    protected String variantAwareCast(SqlExpr.Cast c) {
        // T4 leg 1: a SYNTH-CONFORMANCE cast is the engine's
        // decode-side coercion made explicit for EXECUTION — the
        // engine's own SQL never spells it, so engine TEXT elides it
        // (the wire-coercion suppression precedent).
        if (c.conform()) {
            return expr(c.value(), 0);
        }
        // The F10 LITERAL marker cast is a LABEL device (the
        // construction-site carrier declaration scalarRoot reads) —
        // never engine text: goldens pin the engine's own spelling
        // (testGreatestLeast caught cast(greatest(...) as varchar)).
        if (c.target() == com.legend.sql.SqlType.Scalar.LITERAL
                || c.target() == com.legend.sql.SqlType.Scalar.TEMPORAL_TEXT
                || (c.target() instanceof com.legend.sql.SqlType.Array la
                        && la.element()
                                == com.legend.sql.SqlType.Scalar.LITERAL)) {
            return expr(c.value(), 0);
        }
        String t = castTypeName(c.target()).toLowerCase(Locale.ROOT);
        // The engine spells casts PER DYNAFUNCTION (audit 19 F3), so only
        // respells that cannot collide survive here:
        // - toDecimal is the ONLY producer of DECIMAL(38, 18) and spells
        //   bare 'decimal'; parseDecimal keeps its declared (p, s).
        // - parseInteger carries SqlType.INTEGER from lowering (no respell;
        //   round/ceiling/floor keep 'bigint', matching their goldens).
        // - 'float': parseFloat's golden spelling. KNOWN LATENT COLLISION:
        //   toFloat shares the DOUBLE IR and the engine spells it 'double
        //   precision' — no corpus H2 golden pins toFloat text today; a
        //   per-dynafunction origin tag is the clean fix when one appears.
        if (t.equals("decimal(38, 18)")) {
            t = "decimal";
        } else if (t.equals("double precision") || t.equals("double")) {
            t = "float";
        }
        return "cast(" + expr(c.value(), 0) + " as " + t + ")";
    }


    @Override
    protected String membership(SqlExpr.Membership m) {
        // engine golden spelling for expression membership:
        // x in (<collection expr>) — ledger cluster 35. A LITERAL
        // collection (a let-bound list, ['a', 'b']->contains(x), an
        // element-text cast of one) spells its elements: x in ('a', 'b')
        // — the engine's in-list; the array literal itself has no H2
        // spelling.
        SqlExpr coll = m.collection();
        while (coll instanceof SqlExpr.Cast c && c.target() instanceof com.legend.sql.SqlType.Array) {
            coll = c.value();
        }
        java.util.List<SqlExpr> elements = literalElements(coll);
        if (elements != null) {
            StringBuilder sb = new StringBuilder(expr(m.needle(), 4)).append(" in (");
            for (int i = 0; i < elements.size(); i++) {
                sb.append(i > 0 ? ", " : "").append(expr(elements.get(i), 0));
            }
            return sb.append(')').toString();
        }
        return expr(m.needle(), 4) + " in (" + expr(coll, 0) + ")";
    }

    /** splitPart = the engine's own H2 extension function (commons split:
     *  adjacent separators collapse, past the end -> NULL) — the golden's spelling. */
    @Override
    protected String splitPartCall(java.util.List<SqlExpr> a) {
        return "legend_h2_extension_split_part(" + expr(a.get(0), 0) + ", "
                + expr(a.get(1), 0) + ", " + expr(a.get(2), 0) + ")";
    }
}
