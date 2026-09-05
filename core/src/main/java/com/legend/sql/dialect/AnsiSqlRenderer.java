package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The ANSI-standard renderer — the base every dialect extends
 * (PHASE_HIJ_LOWERING.md "Dialect architecture"). The IR carries MEANING with
 * Pure conventions; this class renders everything standard SQL can express
 * and exposes GROUPED extension points for the rest:
 *
 * <ul>
 *   <li><b>Lexical</b> — {@link #reservedWords()}, {@link #quoteChar()},
 *       literal forms, {@link #castTypeName}.</li>
 *   <li><b>Idioms</b> — list operations ({@link #foldCall},
 *       {@link #listExists}, {@link #listCall}), variant access
 *       ({@link #variantGet}, {@link #variantCast}), {@link #lambda}.
 *       The base THROWS for these: there is no ANSI spelling, and a silent
 *       approximation is forbidden (the no-fallback rule per dialect).</li>
 *   <li><b>Structural</b> — {@link #appendQualify} (native clause or
 *       self-wrap), {@link #pivotSource}, {@link #asOfJoinClause},
 *       {@link #valuesSource}.</li>
 * </ul>
 *
 * <p>Every {@code throw} below is a capability statement, not a TODO: a
 * dialect that cannot express a construct fails LOUDLY at render time.
 */
public class AnsiSqlRenderer implements SqlDialect {

    private final Lexicon lexicon;
    private final TypeNames typeNames;
    private final Spellings spellings;

    public AnsiSqlRenderer(Lexicon lexicon, TypeNames typeNames, Spellings spellings) {
        this.lexicon = java.util.Objects.requireNonNull(lexicon, "lexicon");
        this.typeNames = java.util.Objects.requireNonNull(typeNames, "typeNames");
        this.spellings = java.util.Objects.requireNonNull(spellings, "spellings");
    }

    private static final Pattern PLAIN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** Infix operators: semantic entry → (sql, precedence). Higher binds tighter. */
    protected record Infix(String sql, int prec) {
    }

    /**
     * A composite arm whose SPELLING expands to operator text: the WALK
     * decides the parens — the expansion is declared WEAKEST-binDING, so
     * any enclosing operator wraps it and no arm ever hand-parenthesizes
     * (remediation T1.6/T3.2: the misbind class is dead structurally, and
     * a new composite arm cannot reintroduce it by forgetting parens).
     */
    protected final String opSpelling(String spelling, int parentPrec) {
        return parentPrec > 0 ? "(" + spelling + ")" : spelling;
    }

    private static final Map<SqlFn, Infix> INFIX = Map.ofEntries(
            Map.entry(SqlFn.OR, new Infix("OR", 1)),
            Map.entry(SqlFn.AND, new Infix("AND", 2)),
            Map.entry(SqlFn.EQUAL, new Infix("=", 4)),
            Map.entry(SqlFn.NOT_EQUAL, new Infix("<>", 4)),
            Map.entry(SqlFn.LESS, new Infix("<", 4)),
            Map.entry(SqlFn.LESS_EQUAL, new Infix("<=", 4)),
            Map.entry(SqlFn.GREATER, new Infix(">", 4)),
            Map.entry(SqlFn.GREATER_EQUAL, new Infix(">=", 4)),
            Map.entry(SqlFn.PLUS, new Infix("+", 5)),
            Map.entry(SqlFn.MINUS, new Infix("-", 5)),
            Map.entry(SqlFn.TIMES, new Infix("*", 6)));

    @Override
    public String render(SqlQuery query) {
        SqlQuery q = query;
        for (com.legend.sql.SqlRewriter pass : passes()) {
            q = pass.rewriteRoot(q);
        }
        StringBuilder sb = new StringBuilder();
        query(sb, q, 0);
        return sb.toString();
    }

    /**
     * This dialect's MIR passes, run at {@code render()} entry — IR
     * rewrites live HERE as named passes, never inside render methods
     * (remediation T3.2; {@code SubselectPrune} is the common-pass model
     * at the lowering exit). The CARRIER STRATEGY pass runs FIRST on
     * every dialect (CARRIER_REDESIGN.md §1): semantic collection nodes
     * become this dialect's emission before any other rewrite sees them.
     */
    protected java.util.List<com.legend.sql.SqlRewriter> passes() {
        CarrierStrategies carriers = new CarrierStrategies(
                CarrierStrategies.Caps.H2);
        return supportsQualify()
                ? java.util.List.of(carriers)
                : java.util.List.of(carriers, new QualifyToSubselect());
    }

    // ==================================================================
    // Queries and clause assembly
    // ==================================================================

    protected void query(StringBuilder sb, SqlQuery q, int depth) {
        switch (q) {
            case com.legend.sql.SqlWith w -> {
                sb.append("WITH ");
                for (int i = 0; i < w.ctes().size(); i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(w.ctes().get(i).name()).append(" AS (");
                    query(sb, w.ctes().get(i).query(), depth + 1);
                    sb.append(')');
                }
                nl(sb, depth);
                query(sb, w.body(), depth);
            }
            case SqlSelect s -> select(sb, s, depth);
            case SqlUnion u -> {
                String op = u.all() ? "UNION ALL" : "UNION";
                for (int i = 0; i < u.branches().size(); i++) {
                    if (i > 0) {
                        nl(sb, depth).append(op);
                        nl(sb, depth);
                    }
                    query(sb, u.branches().get(i), depth);
                }
            }
        }
    }

    protected void select(StringBuilder sb, SqlSelect s, int depth) {
        if (s.qualify() != null && !supportsQualify()) {
            // The QualifyToSubselect PASS owns this rewrite — a QUALIFY
            // reaching the writer means the pass did not run: our bug.
            throw new IllegalStateException("QUALIFY reached a writer without"
                    + " QUALIFY support — the QualifyToSubselect pass must run");
        }
        sb.append("SELECT ");
        if (s.distinct()) {
            sb.append("DISTINCT ");
        }
        if (s.projections().isEmpty()) {
            sb.append("*");
        } else {
            // each projection CARRIES its declared output (outputs-from-
            // projections, SQL-IR slice 2) — the old positional
            // projection↔outputs pairing and its star guard are gone
            for (int i = 0; i < s.projections().size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(projection(s.projections().get(i)));
            }
        }
        if (!(s.from() instanceof SqlSource.Dual)) {
            nl(sb, depth).append("FROM ");
            source(sb, s.from(), depth);
        }
        if (s.where() != null) {
            nl(sb, depth).append("WHERE ").append(expr(s.where(), 0));
        }
        if (!s.groupBy().isEmpty()) {
            nl(sb, depth).append("GROUP BY ")
                    .append(s.groupBy().stream().map(e -> expr(e, 0)).collect(Collectors.joining(", ")));
        }
        if (s.having() != null) {
            nl(sb, depth).append("HAVING ").append(expr(s.having(), 0));
        }
        if (s.qualify() != null) {
            appendQualify(sb, s, depth);
        }
        if (!s.orderBy().isEmpty()) {
            nl(sb, depth).append("ORDER BY ")
                    .append(s.orderBy().stream().map(this::sortKey).collect(Collectors.joining(", ")));
        }
        if (s.limit() != null) {
            nl(sb, depth).append("LIMIT ").append(s.limit());
        }
        if (s.offset() != null) {
            nl(sb, depth).append("OFFSET ").append(s.offset());
        }
    }

    /**
     * Render a {@code sourceUrl} into a complete SELECT (scheme-dispatched).
     * No ANSI spelling exists — the base is a capability statement.
     */
    protected String sourceUrl(String url) {
        throw new DialectCapability("sourceUrl reached a dialect without"
                + " an external-source encoding: " + url);
    }

    /** Whether this dialect has a native QUALIFY clause. ANSI does not. */
    protected boolean supportsQualify() {
        return false;
    }

    /** Emit the native QUALIFY clause (only called when {@link #supportsQualify()}). */
    protected void appendQualify(StringBuilder sb, SqlSelect s, int depth) {
        throw new DialectCapability("QUALIFY reached a dialect without native support");
    }

    /** The base renders the projection's own spelling — an alias-less
     * projection keeps its implicit label (correct where labels fold
     * case-insensitively). A case-sensitive dialect reads the
     * projection's DECLARED output ({@code p.out()}) and labels
     * explicitly — the engine's own convention (every golden aliases
     * every projection). */
    protected String projection(SqlSelect.Projection p) {
        // the synthetic scalar-map marker (PlatformTypes.SYNTH_MAP_COL)
        // stays IN the execution alias — downstream references are built
        // from the (prefixed) row type; engine-TEXT renderers drop it
        String e = expr(p.expr(), 0);
        return p.alias() == null ? e : e + " AS " + aliasIdent(p.alias());
    }

    protected String sortKey(SqlSelect.SortKey k) {
        String s = expr(k.expr(), 0) + (k.ascending() ? "" : " DESC");
        if (k.nullOrder() != null) {
            s += k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                    ? " NULLS FIRST" : " NULLS LAST";
        } else {
            // BARE key = engine relational sort semantics (§7 slice-2
            // burn, 2026-09-01): the engine spells no NULLS clause and
            // rides its H2 backend's NULLS-LOW default — ASC nulls
            // first, DESC nulls last (receipts: testPropertyProjection-
            // QueryWithInnerJoinEmbeddedMappingTable's own assert pins
            // ['null','1 the street','5 Park Ave'] ascending; the
            // LL_ORD_COUNT blast radius found exactly 2 ordered-query
            // leniency passes, both ASC-null placement). Execution
            // dialects pin it EXPLICITLY (DuckDB's default is the
            // opposite on ASC); the engine-TEXT channel suppresses all
            // NULLS spelling (EngineStyleH2.sortKey — goldens never
            // spell one). Pure-semantics sorts (null-is-largest, PCT
            // testRange witnesses) arrive STAMPED and take the branch
            // above.
            s += k.ascending() ? " NULLS FIRST" : " NULLS LAST";
        }
        return s;
    }

    // ==================================================================
    // Sources
    // ==================================================================

    protected void source(StringBuilder sb, SqlSource src, int depth) {
        switch (src) {
            case SqlSource.Dual d -> throw new IllegalStateException(
                    "Dual renders as FROM-clause omission — caller bug");
            case SqlSource.Table t -> {
                sb.append(tableName(t.name()));
                if (t.alias() != null) {
                    sb.append(" AS ").append(aliasIdent(t.alias()));
                }
            }
            case SqlSource.Subselect sub -> subselectSource(sb, sub, depth);
            // cross-store plan variable: freemarker splice at execution
            // (engine VarSetPlaceHolder — plan text only; a DuckDB
            // execution reaching this dies loudly at SQL parse)
            case SqlSource.VarSetPlaceholder vp -> sb.append("(${")
                    .append(vp.varName()).append("}) as ")
                    .append(aliasIdent(vp.alias()));
            case SqlSource.Values v -> valuesSource(sb, v);
            // corpus-authored raw SQL as a relation source (Phase 1:
            // the typed executeInDb grid) — carried text, parenthesized
            case SqlSource.RawSql r -> sb.append("(").append(r.sql())
                    .append(") AS ").append(aliasIdent(r.alias()));
            case SqlSource.SourceUrl u -> {
                sb.append("(");
                nl(sb, depth + 1).append(sourceUrl(u.url()));
                nl(sb, depth).append(") AS ").append(aliasIdent(u.alias()));
            }
            case SqlSource.Pivot p -> pivotSource(sb, p, depth);
            case SqlSource.Join j -> {
                source(sb, j.left(), depth);
                nl(sb, depth);
                if (j.kind() == SqlSource.Join.Kind.ASOF_LEFT) {
                    sb.append(asOfJoinClause());
                } else {
                    sb.append(j.kind().sql);
                }
                sb.append(" ");
                source(sb, j.right(), depth);
                if (j.on() != null) {
                    sb.append(" ON ").append(expr(j.on(), 0));
                }
            }
        }
    }

    /** ANSI row-constructor VALUES with column aliases; SQLite overrides (UNION ALL). */
    protected void subselectSource(StringBuilder sb,
            SqlSource.Subselect sub, int depth) {
        sb.append("(");
        nl(sb, depth + 1);
        query(sb, sub.inner(), depth + 1);
        nl(sb, depth).append(") AS ").append(aliasIdent(sub.alias()));
    }

    protected void valuesSource(StringBuilder sb, SqlSource.Values v) {
        sb.append("(VALUES ")
                .append(v.rows().stream()
                        .map(row -> "(" + row.stream().map(e -> expr(e, 0))
                                .collect(Collectors.joining(", ")) + ")")
                        .collect(Collectors.joining(", ")))
                .append(") AS ").append(aliasIdent(v.alias()))
                .append("(")
                .append(v.columns().stream().map(this::aliasIdent).collect(Collectors.joining(", ")))
                .append(")");
    }

    /** Native PIVOT or a CASE-WHEN aggregation rewrite — no ANSI form exists. */
    protected void pivotSource(StringBuilder sb, SqlSource.Pivot p, int depth) {
        throw new DialectCapability("pivot reached a dialect without a PIVOT strategy");
    }

    /** The AS-OF join clause keyword(s); no ANSI form exists. */
    protected String asOfJoinClause() {
        throw new DialectCapability("asOfJoin reached a dialect without an AS-OF strategy");
    }

    // ==================================================================
    // Expressions
    // ==================================================================

    /** The CHECKED-NARROWING spelling (D1, the one semantic node):
     * execution dialects emit pure's toOne size guard; the engine-TEXT
     * subclasses override to the verbatim inner value (processNoOp). */
    protected String checkedOne(SqlExpr.CheckedOne co, int parentPrec) {
        String bound = co.atLeastOnly() ? "[1..*]" : "[1]";
        if (co.scalarCarrier()) {
            // a SCALAR ([0..1]) carrier: NULL is the empty collection —
            // pure raises "Cannot cast a collection of size 0 ..."
            // (multiplicity audit slice 3: the lower bound enforced)
            return expr(new SqlExpr.Case(
                    java.util.List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NULL,
                                    co.list()),
                            SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR,
                                    new SqlExpr.StringLit(
                                            "Cannot cast a collection of"
                                            + " size 0 to multiplicity "
                                            + bound)))),
                    co.list()), parentPrec);
        }
        SqlExpr len = SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_LENGTH,
                co.list());
        SqlExpr sizeErr = SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR,
                SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT,
                        new SqlExpr.StringLit(
                                "Cannot cast a collection of size "),
                        new SqlExpr.Cast(SqlExpr.Call.of(
                                        com.legend.sql.SqlFn.COALESCE, len,
                                        new SqlExpr.IntLit(0)),
                                com.legend.sql.SqlType.Scalar.VARCHAR),
                        new SqlExpr.StringLit(" to multiplicity " + bound)));
        if (co.atLeastOnly()) {
            // toOneMany: at least one — the LIST rides through intact
            return expr(new SqlExpr.Case(
                    java.util.List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(com.legend.sql.SqlFn.OR,
                                    SqlExpr.Call.of(com.legend.sql
                                            .SqlFn.IS_NULL, co.list()),
                                    SqlExpr.Call.of(com.legend.sql
                                                    .SqlFn.EQUAL, len,
                                            new SqlExpr.IntLit(0))),
                            sizeErr)),
                    co.list()), parentPrec);
        }
        // exactly one: size != 1 raises (audit slice 3 — the old guard
        // tested only >1 and let the empty flow), 1 extracts
        return expr(new SqlExpr.Case(
                java.util.List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(com.legend.sql.SqlFn.OR,
                                SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NULL,
                                        co.list()),
                                SqlExpr.Call.of(com.legend.sql.SqlFn.NOT_EQUAL,
                                        len, new SqlExpr.IntLit(1))),
                        sizeErr)),
                SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_GET,
                        co.list(), new SqlExpr.IntLit(1))), parentPrec);
    }

    /** PURE-COLLECTION carrier compaction (semantic node, audit §5
     * value lane): execution dialects strip SQL NULL elements with
     * their list-filter spelling — a pure collection holds no empties,
     * so a NULL in the carrier can only MEAN empty. Engine-TEXT
     * subclasses override to the verbatim inner value (the engine's
     * textual view has no compaction — it drops host-side; the
     * checkedOne/processNoOp precedent). */
    protected String compactList(SqlExpr.CompactList cl, int parentPrec) {
        return expr(SqlExpr.Call.of(
                com.legend.sql.SqlFn.LIST_FILTER, cl.list(),
                new SqlExpr.Lambda(java.util.List.of("x"),
                        SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NOT_NULL,
                                SqlExpr.Column.derived(null, "x")))),
                parentPrec);
    }

    protected String expr(SqlExpr e, int parentPrec) {
        return switch (e) {
            case SqlExpr.Group g -> "(" + expr(g.inner(), 0) + ")";
            case SqlExpr.TempTableInSplice t -> throw new IllegalStateException(
                    "temp-table IN splice '" + t.tempTableName() + "'"
                    + " reached an executable dialect — plan-text"
                    + " vocabulary only");
            case SqlExpr.PlanParam p -> throw new IllegalStateException(
                    "plan parameter '${" + p.name() + "}' reached an"
                    + " executable dialect — plan templates render via the"
                    + " engine-style dialect only");
            case SqlExpr.RowOrder r -> (r.table() == null ? ""
                    : aliasIdent(r.table()) + ".") + rowOrderColumn();
            // the QUALIFIER is structurally always a source ALIAS (the
            // lowerer aliases every FROM source) — it spells with the
            // alias rule; the NAME spells by its ORIGIN (columnName)
            case SqlExpr.Column c -> c.table() == null
                    ? columnName(c) : aliasIdent(c.table()) + "." + columnName(c);
            case SqlExpr.Star s -> s.table() == null ? "*" : aliasIdent(s.table()) + ".*";
            // DuckDB's EXCLUDE spelling (the one PIVOT backend); the dropped
            // names quote UNCONDITIONALLY — the corpus pins the quoted form.
            case SqlExpr.StarExcept se -> (se.table() == null ? "*" : aliasIdent(se.table()) + ".*")
                    + " " + starExceptKeyword() + " (" + se.except().stream()
                            .map(this::starExceptName)
                            .collect(java.util.stream.Collectors.joining(", ")) + ")";
            case SqlExpr.StringLit s -> stringLit(s.value());
            case SqlExpr.FormatLit fl -> stringLit(formatText(fl));
            case SqlExpr.IntLit i -> String.valueOf(i.value());
            // pure Float IS float8 — a BARE decimal literal types as
            // DECIMAL(p,s) in DuckDB and infects every aggregate over it
            case SqlExpr.FloatLit f -> "CAST(" + f.value() + " AS DOUBLE)";
            // a scale-0 DECIMAL-fact literal (a pure d-suffixed integer:
            // 17774d) CASTS so the wire reads DECIMAL — bare digits read
            // INTEGER by magnitude (probed 1.5.0; the (10,3)<>(15,3)
            // times family). HUGEINT-fact big integers and fractional
            // decimals render bare; engine-TEXT renderers intercept
            // upstream with the goldens' own spelling.
            case SqlExpr.DecimalLit d ->
                    d.type() instanceof com.legend.sql.TypeFact.Typed t
                            && t.type() instanceof com.legend.sql.SqlType
                                    .Decimal dd && dd.scale() == 0
                    ? "CAST(" + d.value().toPlainString() + " AS DECIMAL("
                            + dd.precision() + ",0))"
                    : d.value().toPlainString();
            case SqlExpr.BoolLit b -> boolLit(b.value());
            case SqlExpr.NullLit n -> "NULL";
            case SqlExpr.DateLit d -> dateLit(d.iso());
            case SqlExpr.TimestampLit t -> timestampLit(t.iso());
            case SqlExpr.OrderedListAgg ola -> "list(" + expr(ola.value(), 0)
                    + " ORDER BY " + expr(ola.orderBy(), 0) + ")";
            case SqlExpr.ArrayLit a -> arrayLit(a.elements());
            case SqlExpr.StructLit s -> structLit(s);
            case SqlExpr.StructGet g -> structGet(g);
            case SqlExpr.Call c -> call(c, parentPrec);
            case SqlExpr.Case c -> caseExpr(c);
            case SqlExpr.Exists ex -> "EXISTS (" + inline(ex.subquery()) + ")";
            case SqlExpr.ScalarSubquery sq -> "(" + inline(sq.subquery()) + ")";
            // CHECKED NARROWING (the ONE semantic node, D1): execution
            // dialects spell pure's toOne size guard — >1 raises pure's
            // message, 1 extracts, 0/NULL flows the engine-noOp empty.
            // Engine-TEXT renderers override with the verbatim inner
            // value (processNoOp view).
            case SqlExpr.CheckedOne co -> checkedOne(co, parentPrec);
            case SqlExpr.CompactList cl -> compactList(cl, parentPrec);
            case SqlExpr.DeferredTdsString d -> throw new IllegalStateException(
                    "deferred relation-toString reached the renderer — the"
                    + " execution boundary must resolve the dynamic column"
                    + " list first (DeferredTdsString id " + d.id() + ")");
            case SqlExpr.WindowCall w -> windowCall(w);
            case SqlExpr.Lambda l -> lambda(l);
            case SqlExpr.Cast c -> variantAwareCast(c);
            case SqlExpr.FoldCall f -> foldCall(f);
            case SqlExpr.JsonObject j -> jsonObject(j);
            case SqlExpr.JsonArray j -> jsonArray(j);
            case SqlExpr.JsonArrayAgg j -> jsonArrayAgg(j);
            case SqlExpr.ReduceCollection rc -> reduceCollection(rc);
            case SqlExpr.Membership m -> membership(m);
            case SqlAgg.Reducer r -> reducer(r);
        };
    }

    /** The star-exclusion keyword: DuckDB spells EXCLUDE, the SQL
     * dialects with the standard-ish form spell EXCEPT. */
    protected String starExceptKeyword() {
        return "EXCLUDE";
    }

    /** The backend's physical row-order pseudo-column spelling. */
    protected String rowOrderColumn() {
        return "rowid";
    }

    /** Reduce a collection VALUE with a named aggregate — a backend
     * DATA-MODEL capability; the ANSI base has no collection values.
     * The portable route is the CarrierStrategies FUSION into the
     * collecting subselect; a node that survives to rendering here is
     * an honest budget-counted wall. */
    /** Collection membership — backend data-model capability; the
     * portable route is the CarrierStrategies IN-rewrite. */
    protected String membership(SqlExpr.Membership m) {
        throw new DialectCapability("collection membership reached a"
                + " dialect without a list encoding");
    }

    protected String reduceCollection(SqlExpr.ReduceCollection rc) {
        throw new DialectCapability("collection reduction '" + rc.reducer()
                + "' reached a dialect without a list encoding");
    }

    /** DuckDB reference JSON-object constructor: alternating key/value
     * arguments. Dialects with the SQL-standard {@code KEY: VALUE} form
     * override. */
    protected String jsonObject(SqlExpr.JsonObject j) {
        return "json_object(" + j.kv().stream()
                .map(kvE -> expr(kvE, 0)).collect(Collectors.joining(", ")) + ")";
    }

    /** DuckDB reference JSON-array constructor; the SQL-standard
     * {@code JSON_ARRAY} spelling is an override. */
    protected String jsonArray(SqlExpr.JsonArray j) {
        return "json_array(" + j.elements().stream()
                .map(e -> expr(e, 0)).collect(Collectors.joining(", ")) + ")";
    }

    /**
     * COALESCE: an aggregate over ZERO rows is SQL NULL; the graph
     * contract says empty collection = the EMPTY ARRAY.
     * ordered form: json_group_array is a DuckDB MACRO (no ORDER
     * BY) — list() is a real aggregate that takes one, and to_json
     * over the JSON list yields the same array value
     */
    protected String jsonArrayAgg(SqlExpr.JsonArrayAgg j) {
        return j.orderKeys().isEmpty()
                ? "coalesce(json_group_array(" + expr(j.value(), 0) + "), '[]')"
                : "coalesce(to_json(list(" + expr(j.value(), 0)
                        + " ORDER BY " + j.orderKeys().stream()
                                .map(k -> expr(k.expr(), 0)
                                        + (k.desc() ? " DESC" : " ASC")
                                        + " NULLS LAST")
                                .collect(java.util.stream.Collectors
                                        .joining(", "))
                        + ")), '[]')";
    }

    /**
     * ONE exhaustive switch over the {@link SqlFn} vocabulary — javac fails a
     * dialect the moment a semantic function lacks a rendering decision.
     * ANSI-expressible entries render here; idiom entries delegate to the
     * dialect hooks (which THROW in this base).
     */
    protected String call(SqlExpr.Call c, int parentPrec) {
        Infix infix = INFIX.get(c.fn());
        if (infix != null) {
            // NON-COMMUTATIVE ops (-): trailing SAME-precedence operands
            // must parenthesize — 6 - (4 - 5) is not 6 - 4 - 5 (a real
            // wrong-answer bug PCT caught on the minus composition tests).
            // COMPARISONS (prec 4) are NON-ASSOCIATIVE: a nested
            // comparison operand always parenthesizes — bare
            // a = b = TRUE is a type error, (a = b) = TRUE is the value.
            boolean nonCommutative = c.fn() == SqlFn.MINUS;
            boolean nonAssociative = infix.prec() == 4;
            StringBuilder joined = new StringBuilder();
            String pad = infixPad(c.fn());
            for (int i = 0; i < c.args().size(); i++) {
                if (i > 0) {
                    joined.append(pad).append(infix.sql()).append(pad);
                }
                joined.append(expr(c.args().get(i),
                        (i > 0 && nonCommutative) || nonAssociative
                                ? infix.prec() + 1 : infix.prec()));
            }
            return infix.prec() < parentPrec ? "(" + joined + ")" : joined.toString();
        }
        List<SqlExpr> a = c.args();
        // B7 (RaisedErrors): a message WE raise carries the U+001F
        // provenance sentinel at BOTH ends — the Executor funnel
        // extracts between them, removing the driver's transport
        // envelope from OUR OWN text only; native errors never match.
        if (c.fn() == SqlFn.ERROR) {
            // an optional SECOND arg is the raising call's source span
            // ('line:col', a literal — PureSql.raise): it rides INSIDE
            // the envelope behind a U+001E divider so RaisedErrors can
            // hand assertError the position and production text stays
            // clean (the funnel strips the whole envelope)
            String position = a.size() > 1
                    ? expr(a.get(1), 0) + " || chr(30) || " : "";
            return spellings.fnNames().get(SqlFn.ERROR) + "(chr(31) || "
                    + position + "(" + expr(a.get(0), 0) + ") || chr(31))";
        }
        // PURE spellings are DATA (Spellings row): name(args), nothing else.
        String plain = spellings.fnNames().get(c.fn());
        if (plain != null) {
            return fn(plain, a);
        }
        return switch (c.fn()) {
            case AND, OR, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL,
                 PLUS, MINUS, TIMES ->
                    throw new IllegalStateException("infix operator fell through: " + c.fn());
            // NULL-IGNORING flat concat — the node's semantics are the
            // engine's (H2 CONCAT / DuckDB concat both skip NULL args):
            // a LEFT-JOIN-missed operand yields the other side, never
            // NULL. The '||' spelling propagates NULL — a row-value
            // divergence on join misses (testQualifierWithVariableArg).
            case JSON_MERGE_PATCH -> "json_merge_patch("
                    + a.stream().map(x -> expr(x, 0))
                            .collect(java.util.stream.Collectors.joining(", "))
                    + ")";
            case CONCAT -> "concat(" + flattenConcat(a).stream()
                    .map(x -> expr(x, 0))
                    .collect(java.util.stream.Collectors.joining(", ")) + ")";
            // never flattened into an enclosing concat (see SqlFn)
            case CONCAT_JOIN -> "concat(" + a.stream()
                    .map(x -> expr(x, 0))
                    .collect(java.util.stream.Collectors.joining(", ")) + ")";
            case NOT -> {
                String inner = "NOT " + expr(a.get(0), 3);
                yield 3 < parentPrec ? "(" + inner + ")" : inner;
            }
            case NEGATE -> "-" + expr(a.get(0), 7);
            case HASH -> hashSigned(a);
            case IS_NULL -> expr(a.get(0), 4) + " IS NULL";
            case IS_NOT_NULL -> expr(a.get(0), 4) + " IS NOT NULL";
            case IN -> expr(a.get(0), 4) + " IN (" + list(a.subList(1, a.size())) + ")";
            case IS_DISTINCT -> "(" + expr(a.get(0), 4) + " IS DISTINCT FROM "
                    + expr(a.get(1), 4) + ")";
            // the SEMANTIC null-safe (in)equality nodes (engine
            // nullSafeEqual/nullSafeNotEqual DynaFunctions) — dialects
            // re-spell; execution backends use the native form
            case NULL_SAFE_EQUAL -> "(" + expr(a.get(0), 4)
                    + " IS NOT DISTINCT FROM " + expr(a.get(1), 4) + ")";
            case NULL_SAFE_NOT_EQUAL -> "(" + expr(a.get(0), 4)
                    + " IS DISTINCT FROM " + expr(a.get(1), 4) + ")";
            // MUST-honor semantics (PHASE_HIJ_LOWERING.md):
            // operands render ABOVE TIMES precedence: a composite child
            // ((2*t)/(1+p)) must parenthesize or SQL re-associates it
            case DIVIDE -> "((1.0 * " + expr(a.get(0), 7) + ") / " + expr(a.get(1), 7) + ")";
            case MOD -> "MOD(MOD(" + expr(a.get(0), 0) + ", " + expr(a.get(1), 0) + ") + "
                    + expr(a.get(1), 0) + ", " + expr(a.get(1), 0) + ")";
            case REM -> "MOD(" + expr(a.get(0), 0) + ", " + expr(a.get(1), 0) + ")";
            // Math — ANSI/portable spellings; ROUND is banker's (dialect maps).
            case PI -> "pi()";
            case CEILING -> "CAST(ceil(" + expr(a.get(0), 0) + ") AS BIGINT)";
            case FLOOR -> "CAST(floor(" + expr(a.get(0), 0) + ") AS BIGINT)";
            case ROUND -> roundHalfEven(a);
            // Pure's divide-with-scale is BigDecimal HALF_UP — plain SQL
            // ROUND (half away from zero) says exactly that.
            case ROUND_HALF_UP -> fn("ROUND", a);
            // Runtime assertion: raises with the message when evaluated
            // (guards that must fail LOUD, never clamp).
            // floor WITHOUT the BIGINT cast (FLOOR casts — overflows at
            // 1e18): fraction-free tests over the full double range.
            case SIGN -> "CAST(sign(" + expr(a.get(0), 0) + ") AS BIGINT)";
            case XOR -> {
                String x = expr(a.get(0), 3);
                String y = expr(a.get(1), 3);
                // the OR-chain misbinds under an enclosing AND — the WALK
                // wraps it (opSpelling), never this arm by hand
                yield opSpelling("(" + x + " AND NOT " + y + ") OR (NOT " + x
                        + " AND " + y + ")", parentPrec);
            }
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(c.fn(), a);
            // Strings
            // MATCHES is the PARTIAL regexp test (regexpLike's SQL
            // semantics); pure matches() is REGEXP_FULL_MATCH (the engine
            // anchors ^...$).
            case MAP_EMPTY -> "MAP {}";
            case BIT_NOT -> "xor(" + expr(a.get(0), 0) + ", -1)";   // ~x without negation overflow at MIN_LONG
            // the PAD CHAR is optional in Pure; SQL requires it — ' '.
            case LPAD -> fn("lpad", a.size() == 2
                    ? List.of(a.get(0), a.get(1), new SqlExpr.StringLit(" ")) : a);
            case RPAD -> fn("rpad", a.size() == 2
                    ? List.of(a.get(0), a.get(1), new SqlExpr.StringLit(" ")) : a);
            // the || concat misbinds under +/comparison — walk-wrapped
            case UC_FIRST -> opSpelling("upper(substr(" + expr(a.get(0), 0)
                    + ", 1, 1)) || substr(" + expr(a.get(0), 0) + ", 2)", parentPrec);
            case LC_FIRST -> opSpelling("lower(substr(" + expr(a.get(0), 0)
                    + ", 1, 1)) || substr(" + expr(a.get(0), 0) + ", 2)", parentPrec);
            case ENCODE_BASE64 -> "to_base64(CAST(" + expr(a.get(0), 0) + " AS BLOB))";
            // pure generateGuid : String[1] — the CONTRACT is text, so
            // the emission conforms (bare uuid() wires UUID; §4bZ-V C
            // adjudication: fix-emitter, the CEILING pattern)
            case GUID -> "CAST(uuid() AS VARCHAR)";
            // Temporal
            case TODAY -> "current_date";
            case NOW -> "now()";
            case DATE_TRUNC_DAY -> "CAST(" + expr(a.get(0), 0) + " AS DATE)";
            // DAY-GRAINED truncation delivers a DATE (§8.3a carrier
            // burn, dialect-owned per the single-compiler tenet: the
            // SEMANTIC fact is pure's firstDayOf*(Date):Date; whether
            // a cast is needed to honor it is THIS backend's idiom —
            // this engine's date_trunc returns TIMESTAMP. The
            // engine-TEXT channel never sees this arm: EngineStyleH2
            // owns its own verbatim DATE_TRUNC spelling, golden text
            // spells whatever each engine dialect spells.)
            case DATE_TRUNC -> a.get(0) instanceof SqlExpr.StringLit part
                    && switch (part.value()) {
                        case "month", "year", "week", "quarter" -> true;
                        default -> false;
                    }
                    ? "CAST(" + fn("date_trunc", a) + " AS DATE)"
                    : fn("date_trunc", a);
            // make_timestamp wants DOUBLE seconds.
            case MAKE_TIMESTAMP -> a.size() == 6
                    ? "make_timestamp(" + a.subList(0, 5).stream()
                            .map(x -> expr(x, 0)).collect(Collectors.joining(", "))
                            + ", CAST(" + expr(a.get(5), 0) + " AS DOUBLE))"
                    : fn("make_timestamp", a);           // (part, value)
            // (unitFn literal, amount, date) — the unit FUNCTION NAME rides
            // as a string literal and renders bare: d + to_years(n).
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> opSpelling(expr(a.get(2), 5) + " + "
                    + ((SqlExpr.StringLit) a.get(0)).value()
                    + "(" + expr(a.get(1), 0) + ")", parentPrec);              // (part, d1, d2)               // (zone, ts) — ICU
            // Week buckets align to the Monday ON/BEFORE the epoch
            // (1969-12-29 — real pure's origin, PCT-pinned); every other
            // unit aligns to the 1970 epoch.
            case TIME_BUCKET -> "time_bucket("
                    + ((SqlExpr.StringLit) a.get(0)).value()
                    + "(" + expr(a.get(1), 0) + "), " + expr(a.get(2), 0)
                    + ("to_weeks".equals(((SqlExpr.StringLit) a.get(0)).value())
                            ? ", TIMESTAMP '1969-12-29 00:00:00'"
                            : ", TIMESTAMP '1970-01-01 00:00:00'")
                    + ")";
            case FROM_EPOCH_MS -> "epoch_ms(CAST(" + expr(a.get(0), 0) + " AS BIGINT))";
            case INT_DIVIDE -> "(" + expr(a.get(0), 6) + " // " + expr(a.get(1), 6) + ")";
            // decode(blob) — a CAST of the blob to VARCHAR ESCAPES quotes and
            // non-printables (\x22), never the text itself (batch 72b)
            case DECODE_BASE64 -> "decode(from_base64(" + expr(a.get(0), 0) + "))";
            case CURRENT_USER_FN -> "current_user";
            // Lists (dialect-owned; base throws like the lambda family)
            case LIST_ZIP, LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX,
                 LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT,
                 LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN, REPEAT_VALUE,
                 LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, LIST_BOOL_AND, LIST_BOOL_OR,
                 LIST_REVERSE, TYPEOF ->
                    listCall(c.fn(), a);
            case TO_VARIANT -> variantConstruct(a);
            // boolean text: the reference cast spelling (semantic node —
            // dialects with a diverging bool print override)
            case BOOL_TO_TEXT -> "CAST(" + expr(a.get(0), 0) + " AS VARCHAR)";
            // Idiom points — no ANSI spelling; the dialect decides or dies.
            case UNNEST -> unnestProjection(a);
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET,
                 LIST_POSITION ->
                    listCall(c.fn(), a);
            case STRUCT_INSERT -> structInsert(a);
            case LIST_EXISTS -> listExists(a);
            case ALL_DISTINCT -> allDistinct(a);
            case LIST_FOR_ALL -> listForAll(a);
            // 64-bit parse (PCT Long.MIN/MAX round-trips)
            case PARSE_INT -> "CAST(" + expr(a.get(0), 0) + " AS BIGINT)";
            // parseDate(text): the ISO text as a timestamp (the semantic
            // node; the engine-style H2 spells its parsedatetime idiom)
            case PARSE_DATE -> "CAST(" + expr(a.get(0), 0) + " AS TIMESTAMP)";
            case VARIANT_ELEMENTS -> variantElements(a);
            case VARIANT_GET -> variantGet(a);
            // Not a spelling row, not a coded rule: LOUD. Exhaustiveness is
            // pinned by SpellingsTest.everySqlFnClassified (a new SqlFn must
            // be classified there as data or code).
            default -> throw new IllegalStateException(
                    c.fn() + " has no spelling row and no rendering rule");
        };
    }

    // ---- idiom extension points (base = capability statement, loud) ----

    /** Pure hashCode is Integer[1] — SIGNED 64-bit. A dialect whose
     * native hash is unsigned (DuckDB UBIGINT) conforms by
     * reinterpreting cast; the value stays bijective. */
    protected String hashSigned(List<SqlExpr> a) {
        throw new DialectCapability("signed 64-bit hashCode reached a dialect without a spelling");
    }

    /** Pure ROUND is HALF-EVEN (banker's) — every dialect must honor it. */
    protected String roundHalfEven(List<SqlExpr> a) {
        throw new DialectCapability("banker's ROUND reached a dialect without a spelling");
    }

    protected String bitOp(SqlFn fnName, List<SqlExpr> a) {
        throw new DialectCapability(fnName + " reached a dialect without bit-op support");
    }

    /** Construct a variant (JSON) value from any value. */
    protected String variantConstruct(List<SqlExpr> a) {
        throw new DialectCapability("toVariant reached a dialect without JSON support");
    }

    /** Fold with PURE (element, accumulator) lambda; the encoding is the dialect's. */
    protected String foldCall(SqlExpr.FoldCall f) {
        throw new DialectCapability("fold reached a dialect without a fold encoding");
    }

    /**
     * exists/forAll over a collection value. The expansion MUST honor Pure's
     * empty-collection semantics: {@code exists([]) = false},
     * {@code forAll([]) = true}.
     */
    protected String listExists(List<SqlExpr> args) {
        throw new DialectCapability("collection exists reached a dialect"
                + " without a list-predicate encoding");
    }

    /** 1-arg collection isDistinct (D6): true iff no duplicate
     * elements; empty and singleton are trivially true. */
    protected String allDistinct(List<SqlExpr> args) {
        throw new DialectCapability("collection isDistinct reached a"
                + " dialect without a list encoding");
    }

    /** Contract includes Pure's empty-collection semantics: {@code forAll([]) = true}. */
    protected String listForAll(List<SqlExpr> args) {
        throw new DialectCapability("collection forAll reached a dialect"
                + " without a list-predicate encoding");
    }

    /** map/filter/concat/contains over list values. */
    protected String listCall(SqlFn fn, List<SqlExpr> args) {
        throw new DialectCapability(fn + " reached a dialect without a list encoding");
    }

    /** Explode a collection into rows, aligned with sibling projections. */
    protected String unnestProjection(List<SqlExpr> args) {
        throw new DialectCapability("UNNEST reached a dialect without an unnest placement");
    }

    /** The elements of a variant (JSON) array value. */
    protected String variantElements(List<SqlExpr> args) {
        throw new DialectCapability("variant navigation reached a dialect without JSON support");
    }

    /** JSON access ({@code v -> key}). */
    protected String variantGet(List<SqlExpr> args) {
        throw new DialectCapability("variant navigation reached a dialect without JSON support");
    }

    /** struct_insert(s, 'name', v) — a struct with one field appended;
     * only struct-capable dialects render it. */
    protected String structInsert(List<SqlExpr> args) {
        throw new DialectCapability("struct_insert reached a dialect without struct support");
    }

    /** Lambda expression — only dialects with lambda-capable functions render these. */
    protected String lambda(SqlExpr.Lambda l) {
        throw new DialectCapability("a lambda reached a dialect without lambda support");
    }

    /**
     * CAST rendering; a dialect may route a variant-access value through its
     * text-extraction idiom first (DuckDB {@code ->>}). Base: plain CAST.
     */
    protected String variantAwareCast(SqlExpr.Cast c) {
        // The temporal-text marker cast is a LABEL device (§4bZ-V B3):
        // the value is already the precision-faithful text — the cast
        // exists to carry the fact and NEVER renders, on any dialect
        if (c.target() == com.legend.sql.SqlType.Scalar.TEMPORAL_TEXT
                || c.target() == com.legend.sql.SqlType.Scalar.DECIMAL_TEXT) {
            return expr(c.value(), 0);
        }
        return "CAST(" + expr(c.value(), 0) + " AS "
                + castTypeName(c.target()) + ")";
    }

    // ---- window / aggregate / case (ANSI) ----

    protected String caseExpr(SqlExpr.Case c) {
        StringBuilder sb = new StringBuilder("CASE");
        for (SqlExpr.Case.When w : c.whens()) {
            sb.append(" WHEN ").append(expr(w.condition(), 0))
                    .append(" THEN ").append(expr(w.then(), 0));
        }
        if (c.otherwise() != null) {
            sb.append(" ELSE ").append(expr(c.otherwise(), 0));
        }
        return sb.append(" END").toString();
    }

    protected String windowCall(SqlExpr.WindowCall w) {
        String fnText = switch (w.fn()) {
            case SqlAgg.Reducer r -> reducer(r);
            case SqlAgg.RankingFn r -> r.fn() + "(" + list(r.args()) + ")";
            case SqlAgg.ValueFn v -> v.fn() + "(" + list(v.args()) + ")";
        };
        StringBuilder over = new StringBuilder();
        if (!w.partitionBy().isEmpty()) {
            over.append("PARTITION BY ").append(list(w.partitionBy()));
        }
        if (!w.orderBy().isEmpty()) {
            if (over.length() > 0) {
                over.append(" ");
            }
            over.append("ORDER BY ").append(w.orderBy().stream()
                    .map(this::sortKey).collect(Collectors.joining(", ")));
        }
        if (w.frame() != null) {
            over.append(" ").append(w.frame().kind()).append(" BETWEEN ")
                    .append(bound(w.frame().from())).append(" AND ").append(bound(w.frame().to()));
        }
        return fnText + " OVER (" + over + ")";
    }

    protected String bound(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> "UNBOUNDED PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> p.n() + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> "CURRENT ROW";
            case SqlExpr.WindowCall.Frame.Bound.Following f -> f.n() + " FOLLOWING";
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> "UNBOUNDED FOLLOWING";
            // DuckDB interval spelling; DurationUnit names (DAYS, MONTHS...)
            // are valid interval units as-is.
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p ->
                    "INTERVAL " + p.n() + " " + p.unit() + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing f ->
                    "INTERVAL " + f.n() + " " + f.unit() + " FOLLOWING";
        };
    }

    protected String reducer(SqlAgg.Reducer r) {
        String args = r.args().isEmpty() ? "*" : list(r.args());
        // ORDER-SENSITIVE aggregation (SQL standard <sort specification
        // list> inside the aggregate: string_agg(x, sep ORDER BY k))
        String order = r.orderBy().isEmpty() ? "" : " ORDER BY "
                + r.orderBy().stream()
                        .map(k -> expr(k.expr(), 0)
                                + (k.ascending() ? " ASC" : " DESC")
                                + aggOrderNullPlacement(k))
                        .collect(java.util.stream.Collectors.joining(", "));
        return r.fn() + "(" + (r.distinct() ? "DISTINCT " : "") + args
                + order + ")";
    }

    /** A key with DECLARED null placement keeps it inside the aggregate
     * (pure null-largest sorts hoisted into toString — witness PCT
     * testRange_..._WithOrderByDESC: DESC NULLS FIRST died here and
     * nulls sank to the backend default); legacy keys carry none. The
     * ENGINE-TEXT channel overrides to suppress — the engine never
     * spells a NULLS clause (the sortKey suppression's
     * aggregate-internal twin). */
    protected String aggOrderNullPlacement(SqlSelect.SortKey k) {
        return k.nullOrder() == null ? ""
                : k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                        ? " NULLS FIRST" : " NULLS LAST";
    }

    // ==================================================================
    // Lexical extension points
    // ==================================================================

    /** Reserved words forcing quotes even when plainly spelled (lowercase). */
    protected final Set<String> reservedWords() {
        return lexicon.reservedWords();
    }

    protected final char quoteChar() {
        return lexicon.quoteChar();
    }

    protected String stringLit(String value) {
        // a raw NUL byte in the STATEMENT TEXT kills the SQL lexer
        // ("unterminated quoted string") even though the VARCHAR value
        // domain holds NUL fine (user-verified 2026-08-22: chr(0)
        // concatenates, lengths, and compares exactly) — the spelling
        // splices chr(0) between quoted segments. -1 keeps trailing
        // empty segments so 'a\0' round-trips.
        if (value.indexOf('\u0000') >= 0) {
            String[] parts = value.split("\u0000", -1);
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    sb.append(" || chr(0) || ");
                }
                sb.append('\'').append(parts[i].replace("'", "''"))
                        .append('\'');
            }
            return sb.append(')').toString();
        }
        return "'" + value.replace("'", "''") + "'";
    }

    protected String boolLit(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    protected String dateLit(String iso) {
        return "DATE '" + iso + "'";
    }

    protected String timestampLit(String iso) {
        return "TIMESTAMP '" + iso + "'";
    }

    protected String arrayLit(List<SqlExpr> elements) {
        throw new DialectCapability("an array literal reached a dialect without array support");
    }

    protected String structLit(SqlExpr.StructLit s) {
        throw new DialectCapability("a struct literal reached a dialect without struct support");
    }

    protected String structGet(SqlExpr.StructGet g) {
        throw new DialectCapability("a struct extraction reached a dialect without struct support");
    }

    /** SQL type → CAST spelling: scalar LEAVES from {@link TypeNames}
     * (absence loud), composite RULES structural and shared. */
    protected final String castTypeName(com.legend.sql.SqlType t) {
        return switch (t) {
            case com.legend.sql.SqlType.Scalar s -> {
                String n = typeNames.scalarNames().get(s);
                if (n == null) {
                    throw new IllegalStateException(s + " cast reached a"
                            + " dialect without " + s + " support");
                }
                yield n;
            }
            case com.legend.sql.SqlType.Decimal d ->
                    "DECIMAL(" + d.precision() + ", " + d.scale() + ")";
            case com.legend.sql.SqlType.Array a -> castTypeName(a.element()) + "[]";
            case com.legend.sql.SqlType.Map m ->
                    "MAP(" + castTypeName(m.key()) + ", " + castTypeName(m.value()) + ")";
            case com.legend.sql.SqlType.Struct st -> {
                if (!typeNames.structSupport()) {
                    throw new DialectCapability(
                            "a STRUCT type reached a dialect without struct support");
                }
                yield "STRUCT(" + st.fields().stream()
                        .map(fl -> ident(fl.name()) + " " + castTypeName(fl.type()))
                        .collect(Collectors.joining(", ")) + ")";
            }
        };
    }

    /** The DATE-FORMAT spelling — this renderer family's voice is DuckDB
     * strftime codes; a dialect with its own vocabulary overrides (or
     * consumes {@link SqlExpr.FormatLit} parts in its call arms and never
     * lets one reach here). EXHAUSTIVE: a new part is a compile error. */
    protected String formatText(SqlExpr.FormatLit fl) {
        StringBuilder out = new StringBuilder();
        for (com.legend.sql.DateFmt p : fl.parts()) {
            out.append(switch (p) {
                case com.legend.sql.DateFmt.Text t -> t.s();
                case com.legend.sql.DateFmt.Part part -> switch (part) {
                    case YEAR4 -> "%Y";
                    case MONTH2 -> "%m";
                    case DAY2 -> "%d";
                    case HOUR2 -> "%H";
                    case MIN2 -> "%M";
                    case SEC2 -> "%S";
                    case SUBSEC_MICRO -> "%f";
                    case SUBSEC_NANO -> "%n";
                    case SUBSEC_MIN -> "%g";
                    case MONTH_ABBREV -> "%b";
                    case MONTH_NAME -> "%B";
                    case WEEKDAY_NAME -> "%A";
                    case HOUR12 -> "%I";
                    case HOUR12_NOPAD -> "%-I";
                    case AMPM -> "%p";
                };
            });
        }
        return out.toString();
    }

    /** Spacing around an infix operator — dialect texts differ (the
     * engine's DB2 dynafunction templates print arithmetic TIGHT). */
    protected String infixPad(com.legend.sql.SqlFn fn) {
        return " ";
    }

    protected String fn(String spelling, List<SqlExpr> args) {
        return spelling + "(" + list(args) + ")";
    }

    protected String list(List<SqlExpr> es) {
        return es.stream().map(e -> expr(e, 0)).collect(Collectors.joining(", "));
    }

    /**
     * A subquery rendered inline (EXISTS / scalar position): SINGLE-LINE mode
     * — {@link #nl} emits a space instead of a newline while set. Structural,
     * never text post-processing (collapsing rendered text would corrupt
     * whitespace inside string LITERALS).
     */
    protected String inline(SqlQuery q) {
        boolean previous = inlineMode;
        inlineMode = true;
        try {
            StringBuilder sb = new StringBuilder();
            query(sb, q, 0);
            return sb.toString();
        } finally {
            inlineMode = previous;
        }
    }

    /** When set, clause separators render as single spaces (see {@link #inline}). */
    private boolean inlineMode;

    /** Quote ONLY when necessary (the lean tenet), per this dialect's rules. */
    /**
     * A table name may be schema-qualified (hr.EMPLOYEES): each part quotes
     * UNCONDITIONALLY — the engine's emission for schema tables, pinned by
     * the corpus ("hr"."EMPLOYEES").
     */
    protected String tableName(String name) {
        int dot = name.indexOf('.');
        if (dot <= 0) {
            return ident(name);
        }
        char q = quoteChar();
        return q + name.substring(0, dot) + q + "." + q + name.substring(dot + 1) + q;
    }

    /** COLUMN-NAME spelling at a reference — DIALECT-owned. The base
     * spells every name via {@link #ident} (correct for
     * case-insensitive engines). A case-sensitive dialect dispatches
     * on the reference's ORIGIN: a DERIVED name (the query invented
     * it) quotes like its alias definition; a PHYSICAL name spells as
     * the DDL spelled it; an origin-less reference WALLS rather than
     * guess. */
    protected String columnName(SqlExpr.Column c) {
        return ident(c.name());
    }

    /** EXCEPT/EXCLUDE-list name spelling — DIALECT-owned: the base
     * keeps the UNCONDITIONAL quote (DuckDB's EXCLUDE form, corpus-
     * pinned); the H2 dialect spells via {@link #ident} so the names
     * match every other reference in the same statement on a
     * case-sensitive session (PCT witness: EXCEPT ("country") vs bare
     * _tds0.country in one SELECT). */
    protected String starExceptName(String name) {
        return quoteChar() + name + quoteChar();
    }

    /** ALIAS/label positions ({@code AS x}, VALUES column lists) —
     * default = {@link #ident}. The H2 dialect quotes these
     * UNCONDITIONALLY, the engine's own convention (every golden
     * spells {@code as "root"}, {@code as "legalName"}): on a
     * case-sensitive session a bare alias uppercases in result-set
     * LABELS, breaking every label-reading consumer (witness: PCT
     * dynamic-pivot minted-name decode saw 'ID' for 'id'). */
    protected String aliasIdent(String name) {
        return ident(name);
    }

    protected String ident(String name) {
        if (PLAIN.matcher(name).matches() && !reservedWords().contains(name.toLowerCase())) {
            return name;
        }
        char q = quoteChar();
        // a QUOTE-BEARING identity ('"date"' — quoted store declaration)
        // is already its own spelling — but ONLY when its interior is a
        // valid quoted body (quote chars appear as doubled pairs); a
        // stray interior quote would walk out of the identifier (C2.1)
        if (name.length() > 1 && name.charAt(0) == q
                && name.charAt(name.length() - 1) == q
                && !name.substring(1, name.length() - 1)
                        .replace("" + q + q, "")
                        .contains(String.valueOf(q))) {
            return name;
        }
        return q + name.replace(String.valueOf(q), String.valueOf(q) + q) + q;
    }

    protected StringBuilder nl(StringBuilder sb, int depth) {
        return inlineMode ? sb.append(" ")
                : sb.append("\n").append("  ".repeat(depth));
    }

    /** Nested CONCAT calls splice into ONE flat argument list (the engine
     * emits concat(a, '_', b), never concat(concat(a, '_'), b)). */
    protected static java.util.List<SqlExpr> flattenConcat(java.util.List<SqlExpr> a) {
        java.util.List<SqlExpr> out = new java.util.ArrayList<>();
        for (SqlExpr e : a) {
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.CONCAT) {
                out.addAll(flattenConcat(c.args()));
            } else {
                out.add(e);
            }
        }
        return out;
    }

}
