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
public abstract class AnsiSqlRenderer implements SqlDialect {

    private final Lexicon lexicon;

    /** A dialect over its own lexicon ({@code LEXICON}, a constant of its class: the dialect program's Phase 1a). */
    protected AnsiSqlRenderer(Lexicon lexicon) {
        this.lexicon = java.util.Objects.requireNonNull(lexicon, "lexicon");
    }

    private static final Pattern PLAIN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** Infix operators: semantic entry → (sql, precedence). Higher binds tighter. */
    protected record Infix(String sql, int prec) {
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
        return write(query).text();
    }

    @Override
    public RenderedStatement renderStatement(SqlQuery query) {
        return write(query).statement();
    }

    /** {@inheritDoc} This dialect's own {@link #render}, in the single-line mode {@link #inline} writes a subquery
     *  in. */
    @Override
    public final String renderInline(SqlQuery query) {
        boolean previous = inlineMode;
        inlineMode = true;
        try {
            return render(query);
        } finally {
            inlineMode = previous;
        }
    }

    /** The query after this dialect's passes, written. */
    private SqlWriter write(SqlQuery query) {
        return query(newWriter(), passed(query), 0);
    }

    /** The query after this dialect's passes. */
    private SqlQuery passed(SqlQuery query) {
        SqlQuery q = query;
        for (com.legend.sql.SqlRewriter pass : renderPasses()) {
            q = pass.rewriteRoot(q);
        }
        return q;
    }

    /**
     * This dialect's MIR passes, run at {@code render()} entry — IR
     * rewrites live HERE as named passes, never inside render methods
     * (remediation T3.2; {@code SubselectPrune} is the common-pass model
     * at the lowering exit). The CARRIER STRATEGY pass runs FIRST on
     * every dialect (CARRIER_REDESIGN.md §1): semantic collection nodes
     * become this dialect's emission before any other rewrite sees them,
     * over the dialect's own capabilities ({@code CAPS}).
     */
    protected abstract java.util.List<com.legend.sql.SqlRewriter> passes();

    /** What {@code render()} runs: this dialect's {@link #passes()}, then the STORED READS LAST
     *  (docs/STORE_TYPES_HOMEWORK_2026_10_02.md, 4.3) -- every source the passes introduced is in
     *  scope, and every reference they built reads what the store holds. One owner, so no
     *  dialect's pass list can leave the reads out. */
    protected final java.util.List<com.legend.sql.SqlRewriter> renderPasses() {
        java.util.List<com.legend.sql.SqlRewriter> ps = new java.util.ArrayList<>();
        // a Float's print forms, expanded before every other pass unless this dialect spells them itself
        if (!spellsFloatPrint()) {
            ps.add(new FloatPrintText.Expansion(bindsFloatPrint()));
        }
        // a Float's literal spelling, expanded before every other pass unless this dialect spells it itself
        if (!spellsFloatCanon()) {
            ps.add(new FloatCanonText.Expansion(bindsFloatText()));
        }
        ps.addAll(passes());
        ps.add(new StoredReads(this::readsStored));
        return java.util.List.copyOf(ps);
    }

    /** A HUGEINT-fact integer literal (past a signed BIGINT): bare, the database types it by its magnitude. */
    protected String hugeIntLiteral(java.math.BigDecimal v) {
        return v.toPlainString();
    }

    /** Whether this dialect writes {@link com.legend.sql.SqlFn#FLOAT_CANON} itself; else it is expanded before every
     *  other pass ({@link FloatCanonText.Expansion}) to the portable spelling over the double's text. */
    protected boolean spellsFloatCanon() {
        return false;
    }

    /** Whether this dialect writes {@link com.legend.sql.SqlFn#FLOAT_PRINT} itself; else it is expanded before every
     *  other pass ({@link FloatPrintText.Expansion}) to the portable spelling over the double's text. */
    protected boolean spellsFloatPrint() {
        return false;
    }

    /** Whether the print forms' expansion reads the value and its text from a one-row derived table, each written once
     *  (a dialect whose double text is a long expression and whose derived tables read the row around them). */
    protected boolean bindsFloatPrint() {
        return false;
    }

    /** Whether that expansion reads the double's text from a one-row derived table, written once (a dialect whose
     *  double text is a long expression and whose derived tables read the row around them). */
    protected boolean bindsFloatText() {
        return false;
    }

    /** Whether this dialect reads a column STORED as {@code t} other than as the database holds
     *  it ({@link SqlExpr.StoredRead}). Base: a type Pure cannot name ({@code OTHER},
     *  {@code DISTINCT}) is a Pure String, read as text; every other type is read as held -- a
     *  nested value ({@code JSON}, {@code ARRAY}, {@code OBJECT}) included, as the database
     *  holds it (ruled 2026-10-02). */
    protected boolean readsStored(com.legend.sql.SqlDdl.ColumnType t) {
        return readsAsText(t);
    }

    /** The read of a column whose stored type {@link #readsStored} names: base, as text. The
     *  column renders as any reference does here (the dialect's aliasing and quoting). */
    protected SqlWriter storedRead(SqlWriter writer, SqlExpr.StoredRead r) {
        if (readsAsText(r.stored())) {
            return writer.append("CAST(").expr(r.column(), 0).append(" AS ")
                    .append(castTypeName(com.legend.sql.SqlType.Scalar.VARCHAR)).append(")");
        }
        throw new DialectCapability("this dialect reads a column stored as " + r.stored()
                + " as the database holds it: a stored read of it is a pass defect");
    }

    /** {@code s}'s projections with each star over {@code alias} spelled as {@code source}'s own columns (an empty
     *  projection list is a star), for a dialect that puts helper columns beside a source under its alias: a counted
     *  MODE's counts, or a quantile's ranks (MariaDB's, MySQL's and BigQuery's). A star there would answer the helpers
     *  too, and a typed-rows statement answers its plan's columns alone (2026-10-27, the result boundary program:
     *  measured on the MariaDB, MySQL and BigQuery lanes, the window mode, median and percentile). A star with
     *  exceptions over that source is refused by name. */
    protected static List<SqlSelect.Projection> starsAsSourceColumns(SqlSelect s, SqlSource source, String alias,
            String product) {
        List<SqlSelect.Projection> spelled = new java.util.ArrayList<>();
        List<SqlSelect.Projection> projections = s.projections().isEmpty()
                ? List.of(new SqlSelect.Projection(new SqlExpr.Star(null), null, null)) : s.projections();
        for (SqlSelect.Projection p : projections) {
            if (p.expr() instanceof SqlExpr.Star st && (st.table() == null || st.table().equals(alias))) {
                for (com.legend.sql.OutputCol oc : source.outputs()) {
                    spelled.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, oc), oc.name(), oc));
                }
            } else if (p.expr() instanceof SqlExpr.StarExcept se && (se.table() == null || se.table().equals(alias))) {
                throw new DialectCapability("a star with exceptions over a source with helper columns beside it"
                        + " reached " + product);
            } else {
                spelled.add(p);
            }
        }
        return spelled;
    }

    /** A type Pure cannot name: a Pure String, read as text on every dialect. */
    protected static boolean readsAsText(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case OTHER, DISTINCT -> true;
                case BIGINT, SMALLINT, TINYINT, INTEGER, FLOAT, DOUBLE, REAL, BIT, TIMESTAMP, DATE, TIME,
                        TIMESTAMP_NS, JSON, VARCHAR, ARRAY, OBJECT -> false;
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized ignored -> false;
            case com.legend.sql.SqlDdl.ColumnType.Scaled ignored -> false;
        };
    }

    // ==================================================================
    // Queries and clause assembly
    // ==================================================================

    protected SqlWriter query(SqlWriter writer, SqlQuery q, int depth) {
        return switch (q) {
            case com.legend.sql.SqlWith w -> {
                writer.append(w.recursive() ? "WITH RECURSIVE " : "WITH ");
                for (int i = 0; i < w.ctes().size(); i++) {
                    if (i > 0) {
                        writer.append(", ");
                    }
                    writer.append(cteName(w.ctes().get(i).name())).append(cteAs(w.ctes().get(i)));
                    query(writer, w.ctes().get(i).query(), depth + 1);
                    writer.append(')');
                }
                nl(writer, depth);
                yield query(writer, w.body(), depth);
            }
            case SqlSelect s -> select(writer, s, depth);
            case SqlUnion u -> {
                String op = u.all() ? "UNION ALL" : "UNION";
                for (int i = 0; i < u.branches().size(); i++) {
                    if (i > 0) {
                        nl(writer, depth).append(op);
                        nl(writer, depth);
                    }
                    query(writer, u.branches().get(i), depth);
                }
                yield writer;
            }
        };
    }

    protected SqlWriter select(SqlWriter writer, SqlSelect s, int depth) {
        if (s.qualify() != null && !supportsQualify()) {
            // The QualifyToSubselect PASS owns this rewrite — a QUALIFY
            // reaching the writer means the pass did not run: our bug.
            throw new IllegalStateException("QUALIFY reached a writer without"
                    + " QUALIFY support — the QualifyToSubselect pass must run");
        }
        writer.append("SELECT ");
        if (s.distinct()) {
            writer.append("DISTINCT ");
        }
        if (s.projections().isEmpty()) {
            writer.append("*");
        } else {
            // each projection CARRIES its declared output (outputs-from-
            // projections, SQL-IR slice 2) — the old positional
            // projection↔outputs pairing and its star guard are gone
            projections(writer, s.projections());
        }
        if (!(s.from() instanceof SqlSource.Dual)) {
            nl(writer, depth).append("FROM ");
            source(writer, s.from(), depth);
        }
        if (s.where() != null) {
            nl(writer, depth).append("WHERE ");
            writer.expr(s.where(), 0);
        }
        if (!s.groupBy().isEmpty()) {
            nl(writer, depth).append("GROUP BY ");
            writer.list(s.groupBy());
        }
        if (s.having() != null) {
            nl(writer, depth).append("HAVING ");
            writer.expr(s.having(), 0);
        }
        if (s.qualify() != null) {
            appendQualify(writer, s, depth);
        }
        if (!s.orderBy().isEmpty()) {
            nl(writer, depth).append("ORDER BY ").join(s.orderBy(), ", ", this::sortKey);
        }
        if (s.limit() != null) {
            nl(writer, depth).append("LIMIT ").append(s.limit());
        }
        if (s.offset() != null) {
            nl(writer, depth).append("OFFSET ").append(s.offset());
        }
        return writer;
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
    protected SqlWriter appendQualify(SqlWriter writer, SqlSelect s, int depth) {
        throw new DialectCapability("QUALIFY reached a dialect without native support");
    }

    /** The base renders the projection's own spelling — an alias-less
     * projection keeps its implicit label (correct where labels fold
     * case-insensitively). A case-sensitive dialect reads the
     * projection's DECLARED output ({@code p.out()}) and labels
     * explicitly — the engine's own convention (every golden aliases
     * every projection). */
    /** A column reference, qualified by its source's alias when it has one. */
    protected String columnRef(SqlExpr.Column c) {
        return c.table() == null ? columnName(c) : aliasIdent(c.table()) + "." + columnName(c);
    }

    protected SqlWriter projection(SqlWriter writer, SqlSelect.Projection p) {
        // the synthetic scalar-map marker (PlatformTypes.SYNTH_MAP_COL)
        // stays IN the execution alias — downstream references are built
        // from the (prefixed) row type; engine-TEXT renderers drop it
        projectionValue(writer, p);
        if (p.out() != null && p.out().origin() == com.legend.sql.OutputCol.Origin.PER_KEY) {
            return perKeyName(writer.append(" AS "), p.out().name());
        }
        if (p.alias() != null) {
            return writer.append(" AS ").append(aliasIdent(p.alias()));
        }
        String label = implicitLabel(p);
        return label == null ? writer : writer.append(" AS ").append(label);
    }

    /** A projection's value, before its label. */
    protected SqlWriter projectionValue(SqlWriter writer, SqlSelect.Projection p) {
        if (p.expr() instanceof SqlExpr.NullLit && p.out() != null && typedNullSlot(p.out().type())) {
            // THE SLOT IS THE WIRE: a NULL projected under a typed slot
            // spells its type — a bare NULL is typed by its use when the
            // select is inlined and INTEGER by default when it is
            // materialized (leg 3.4 step 2: a frame CTE's NULL column)
            return writer.append("CAST(NULL AS ").append(castTypeName(p.out().type())).append(")");
        }
        return writer.expr(p.expr(), 0);
    }

    /**
     * A select's projections, comma-separated; a run of PER-KEY projections ({@link com.legend.sql.OutputCol.Origin#PER_KEY},
     * a dynamic pivot's generated columns on its probe plan, 2026-10-20) is one per-key region the runner writes once
     * per key, after the select's other projections: led by a comma when one precedes it, and with no key nothing in
     * its place (a select with no other projection has no column without a key, which the runner refuses by name).
     */
    protected SqlWriter projections(SqlWriter writer, List<SqlSelect.Projection> ps) {
        return projections(writer, ps, (w, i) -> projection(w, ps.get(i)));
    }

    /** {@link #projections(SqlWriter, List)}, each projection written by {@code each} from its position: a dialect
     *  that writes some projections its own way writes them here, so its per-key run is still one region (Snowflake's
     *  select wrote its projections one by one, and its dynamic pivots were refused a per-key name outside a region,
     *  measured on its lane 2026-10-21). */
    protected SqlWriter projections(SqlWriter writer, List<SqlSelect.Projection> ps,
            java.util.function.BiConsumer<SqlWriter, Integer> each) {
        int run = -1;
        for (int i = 0; i < ps.size(); i++) {
            if (perKey(ps.get(i))) {
                run = i;
                break;
            }
        }
        List<Integer> at = java.util.stream.IntStream.range(0, ps.size()).boxed().toList();
        if (run < 0) {
            return writer.join(at, ", ", each);
        }
        for (int i = run; i < ps.size(); i++) {
            if (!perKey(ps.get(i))) {
                throw new IllegalStateException("a per-key projection run is followed by '" + ps.get(i).alias()
                        + "': the run is written last, after the select's other projections");
            }
        }
        List<Integer> fixed = at.subList(0, run);
        List<Integer> perKey = at.subList(run, ps.size());
        writer.join(fixed, ", ", each);
        return writer.region(fixed.isEmpty() ? "" : ", ", ", ", fixed.isEmpty() ? null : "",
                w -> w.join(perKey, ", ", each));
    }

    private static boolean perKey(SqlSelect.Projection p) {
        return p.out() != null && p.out().origin() == com.legend.sql.OutputCol.Origin.PER_KEY;
    }

    /** A per-key name ({@link com.legend.sql.OutputCol.Origin#PER_KEY}): delimited, the key's position written into it by
     *  the runner after its {@code _pk} ({@code "_pk3__u0"} for {@code _u0}); the dialect's quote
     *  doubled inside. Refused outside a per-key region. */
    protected SqlWriter perKeyName(SqlWriter writer, String stem) {
        String q = String.valueOf(quoteChar());
        return writer.append(q + "_pk").counter().append(("_" + stem).replace(q, q + q) + q);
    }

    /** The label an alias-less projection spells, or null for none. Base: a STORED READ keeps
     *  its column's own label, spelled as the reference is -- the name the bare column labeled
     *  itself with before the read wrapped it, on every database's identifier folding. */
    protected @com.legend.base.Nullable String implicitLabel(SqlSelect.Projection p) {
        return p.expr() instanceof SqlExpr.StoredRead r ? columnName(r.column()) : null;
    }

    /** The scalar slot types a typed NULL spells; the label carriers
     * (LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT, JSON) keep a bare NULL. */
    private static boolean typedNullSlot(com.legend.sql.SqlType t) {
        return t == com.legend.sql.SqlType.Scalar.BOOLEAN || t == com.legend.sql.SqlType.Scalar.INTEGER
                || t == com.legend.sql.SqlType.Scalar.BIGINT || t == com.legend.sql.SqlType.Scalar.HUGEINT
                || t == com.legend.sql.SqlType.Scalar.DOUBLE || t == com.legend.sql.SqlType.Scalar.VARCHAR
                || t == com.legend.sql.SqlType.Scalar.DATE || t == com.legend.sql.SqlType.Scalar.TIMESTAMP
                || t == com.legend.sql.SqlType.Scalar.TIMESTAMPTZ;
    }

    protected SqlWriter sortKey(SqlWriter writer, SqlSelect.SortKey k) {
        writer.expr(k.expr(), 0).append(k.ascending() ? "" : " DESC");
        if (k.nullOrder() != null) {
            return writer.append(k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                    ? " NULLS FIRST" : " NULLS LAST");
        } else {
            // BARE key = engine relational sort semantics. Since 4.145.0
            // (batch 8) the engine's printer has ONE canonical null
            // placement for a sort without an explicit NullOrder —
            // dbExtension.pure NullOrderingSupport.processSortItem:
            // DESC -> NULLS FIRST, ASC -> NULLS LAST, i.e. NULL IS LARGEST
            // — spelled as a clause wherever the dialect's native order
            // differs, and left bare where it matches (H2 2.x is
            // nullsHighWithClauseSupport, so the corpus goldens still spell
            // none while their row asserts moved: testGroupBy.pure's eleven
            // desc sorts now put the null group FIRST). That is the same
            // placement the Pure-language sorts stamp (Fold.sortNulls): the
            // two-spec split of §7 slice-2 (2026-09-01) closed UPSTREAM.
            // Until 4.138.2 the bare key rode H2 1.4's nulls-low default
            // (ASC nulls first, DESC nulls last) and the execution dialects
            // pinned that explicitly; the engine-TEXT channel still spells
            // no clause (EngineStyleH2.sortKey — goldens never do).
            return writer.append(k.ascending() ? " NULLS LAST" : " NULLS FIRST");
        }
    }

    // ==================================================================
    // Sources
    // ==================================================================

    protected SqlWriter source(SqlWriter writer, SqlSource src, int depth) {
        return switch (src) {
            case SqlSource.Dual d -> throw new IllegalStateException(
                    "Dual renders as FROM-clause omission — caller bug");
            case SqlSource.Table t -> {
                // a tabular function is CALLED (upstream: schema.fn())
                writer.append(tableName(t.name(), t.quoting())).append(t.call() ? "()" : "");
                if (t.alias() != null) {
                    writer.append(" AS ").append(aliasIdent(t.alias()));
                }
                yield writer;
            }
            case SqlSource.Cte c -> writer.append(cteName(c.name())).append(" AS ").append(aliasIdent(c.alias()));
            case SqlSource.Subselect sub -> subselectSource(writer, sub, depth);
            // cross-store plan variable: freemarker splice at execution
            // (engine VarSetPlaceHolder — plan text only; a DuckDB
            // execution reaching this dies loudly at SQL parse)
            case SqlSource.VarSetPlaceholder vp -> writer.append("(${")
                    .append(vp.varName()).append("}) as ")
                    .append(aliasIdent(vp.alias()));
            case SqlSource.Values v -> valuesSource(writer, v);
            // authored raw SQL as a relation source (the typed executeInDb grid) — carried text, parenthesized, in
            // this session's language (raw: the engine's H2 text translated, any other as written)
            case SqlSource.RawSql r -> writer.append("(").append(raw(r.sql(), r.writtenFor()))
                    .append(") AS ").append(aliasIdent(r.alias()));
            // a catalog grid of the session's database: where it keeps its catalog is the dialect's
            case SqlSource.Catalog c -> catalog(writer.append("("), c).append(") AS ").append(aliasIdent(c.alias()));
            case SqlSource.SourceUrl u -> {
                writer.append("(");
                nl(writer, depth + 1).append(sourceUrl(u.url()));
                yield nl(writer, depth).append(") AS ").append(aliasIdent(u.alias()));
            }
            case SqlSource.Pivot p -> pivotSource(writer, p, depth);
            case SqlSource.Join j -> {
                source(writer, j.left(), depth);
                nl(writer, depth);
                writer.append(joinKeyword(j.kind())).append(" ");
                source(writer, j.right(), depth);
                SqlExpr on = j.on();
                if (on != null) {
                    writer.append(" ON ").expr(joinCondition(j.kind(), on), 0);
                }
                yield writer;
            }
        };
    }

    /** A CTE's name, at its head and wherever a {@link SqlSource.Cte} reads it: as written. A dialect whose unquoted
     *  names fold spells it as it spells a table's, so a reference written as a table ({@code VerdictSql}'s) is the
     *  same name (Snowflake, 2026-10-21). */
    protected String cteName(String name) {
        return name;
    }

    /** A join's ON as the dialect writes it: as it is; a dialect whose planner a form of it defeats writes an
     *  equivalent one (DuckDB). */
    protected SqlExpr joinCondition(SqlSource.Join.Kind kind, SqlExpr on) {
        return on;
    }

    /** ANSI row-constructor VALUES with column aliases; SQLite overrides (UNION ALL). */
    /** The {@code AS (} of a CTE head; a dialect with an evaluate-once
     * keyword spells it for a materialized CTE (DuckDB); the default has
     * none (H2 re-evaluates a CTE per reference). */
    protected String cteAs(com.legend.sql.SqlWith.Cte c) {
        return " AS (";
    }

    protected SqlWriter subselectSource(SqlWriter writer,
            SqlSource.Subselect sub, int depth) {
        writer.append("(");
        nl(writer, depth + 1);
        query(writer, sub.inner(), depth + 1);
        return nl(writer, depth).append(") AS ").append(aliasIdent(sub.alias()));
    }

    protected SqlWriter valuesSource(SqlWriter writer, SqlSource.Values v) {
        writer.append("(VALUES ");
        for (int r = 0; r < v.rows().size(); r++) {
            if (r > 0) {
                writer.append(", ");
            }
            writer.append("(").list(v.rows().get(r)).append(")");
        }
        return writer.append(") AS ").append(aliasIdent(v.alias())).append("(")
                .append(v.columns().stream().map(this::aliasIdent).collect(Collectors.joining(", "))).append(")");
    }

    /** Native PIVOT or a CASE-WHEN aggregation rewrite — no ANSI form exists. */
    protected SqlWriter pivotSource(SqlWriter writer, SqlSource.Pivot p, int depth) {
        throw new DialectCapability("pivot reached a dialect without a PIVOT strategy");
    }

    /** A join's keywords (the dialect program's Phase 2, 2026-10-15: until then the IR's join kind carried its SQL):
     *  the ANSI forms, and an AS-OF join's own ({@link #asOfJoinClause}). */
    protected String joinKeyword(SqlSource.Join.Kind kind) {
        return switch (kind) {
            case INNER -> "JOIN";
            case LEFT -> "LEFT OUTER JOIN";
            case RIGHT -> "RIGHT OUTER JOIN";
            case FULL -> "FULL OUTER JOIN";
            case CROSS -> "CROSS JOIN";
            case CROSS_LATERAL -> "CROSS JOIN LATERAL";
            case ASOF_LEFT -> asOfJoinClause();
            case LEFT_LATERAL -> "LEFT JOIN LATERAL";
        };
    }

    /** The AS-OF join clause keyword(s); no ANSI form exists. */
    protected String asOfJoinClause() {
        throw new DialectCapability("asOfJoin reached a dialect without an AS-OF strategy");
    }

    // ==================================================================
    // Expressions
    // ==================================================================

    /** A tagged value (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a): its root projects it as its tag and slots, and
     *  a rule consuming it reads a channel, so no execution dialect writes it. The engine-TEXT renderers override:
     *  legend-engine has no tags, and its text of a mixed selection is the plain selection over the comparables
     *  (2026-10-26: the corpus's {@code sqlstring::testGreatestLeast}, {@code greatest([$t.quantity, 1, 3])} in a
     *  project column, whose expected text is {@code greatest("root".quantity, 1, 3)}). */
    protected SqlWriter tagged(SqlWriter writer, SqlExpr.Tagged t, int parentPrec) {
        throw new IllegalStateException("a tagged value reached the renderer: a mixed"
                + " max, min, greatest, least or mode is returned, compared or printed, and any other consumer"
                + " of it is not lowered (" + t.candidates().size() + " candidates of kinds "
                + t.slots().stream().map(com.legend.sql.SqlType.Tagged.Slot::kind).toList() + ")");
    }

    /** The CHECKED-NARROWING spelling (D1, the one semantic node):
     * execution dialects emit pure's toOne size guard; the engine-TEXT
     * subclasses override to the verbatim inner value (processNoOp). */
    protected SqlWriter checkedOne(SqlWriter writer, SqlExpr.CheckedOne co, int parentPrec) {
        String bound = co.atLeastOnly() ? "[1..*]" : "[1]";
        if (co.scalarCarrier()) {
            // a SCALAR ([0..1]) carrier: NULL is the empty collection —
            // pure raises "Cannot cast a collection of size 0 ..."
            // (multiplicity audit slice 3: the lower bound enforced)
            return writer.expr(new SqlExpr.Case(
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
            return writer.expr(new SqlExpr.Case(
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
        return writer.expr(new SqlExpr.Case(
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
    protected SqlWriter compactList(SqlWriter writer, SqlExpr.CompactList cl, int parentPrec) {
        return writer.expr(SqlExpr.Call.of(
                com.legend.sql.SqlFn.LIST_FILTER, cl.list(),
                new SqlExpr.Lambda(java.util.List.of("x"),
                        SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NOT_NULL,
                                SqlExpr.Column.derived(null, "x")))),
                parentPrec);
    }

    /** An expression, written: a leaf is spelled as text; a sub-expression is written into the same writer, so a
     *  parameter anywhere below is bound where its placeholder is written. */
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        return switch (e) {
            case SqlExpr.Group g -> writer.append("(").expr(g.inner(), 0).append(")");
            case SqlExpr.TempTableInSplice t -> throw new IllegalStateException(
                    "temp-table IN splice '" + t.tempTableName() + "'"
                    + " reached an executable dialect — plan-text"
                    + " vocabulary only");
            // a plan parameter is BOUND where it is written: a statement (renderStatement) lists it; text
            // (render) refuses it
            case SqlExpr.PlanParam p -> placeholder(writer, oneValue(p));
            case SqlExpr.KeyCell k -> writer.bindKey(k.cell());
            case SqlExpr.RowOrder r -> writer.append((r.table() == null ? ""
                    : aliasIdent(r.table()) + ".") + rowOrderColumn());
            // the QUALIFIER is structurally always a source ALIAS (the
            // lowerer aliases every FROM source) — it spells with the
            // alias rule; the NAME spells by its ORIGIN (columnName)
            case SqlExpr.Column c when c.origin() == com.legend.sql.OutputCol.Origin.PER_KEY ->
                    perKeyName(c.table() == null ? writer : writer.append(aliasIdent(c.table()) + "."), c.name());
            case SqlExpr.Column c -> writer.append(columnRef(c));
            case SqlExpr.StoredRead r -> storedRead(writer, r);
            case SqlExpr.Star s -> writer.append(s.table() == null ? "*" : aliasIdent(s.table()) + ".*");
            // DuckDB's EXCLUDE spelling (the one PIVOT backend); the dropped
            // names quote UNCONDITIONALLY — the corpus pins the quoted form.
            case SqlExpr.StarExcept se -> writer.append((se.table() == null ? "*" : aliasIdent(se.table()) + ".*")
                    + " " + starExceptKeyword() + " (" + se.except().stream()
                            .map(this::starExceptName)
                            .collect(java.util.stream.Collectors.joining(", ")) + ")");
            case SqlExpr.StringLit s -> writer.append(stringLit(s.value()));
            case SqlExpr.FormatLit fl -> writer.append(stringLit(formatText(fl)));
            case SqlExpr.DatePartLit p -> throw new IllegalStateException("a date part (" + p.part()
                    + ") reached the expression writer: the arm of its call spells it");
            case SqlExpr.JsonKindLit k -> writer.append(stringLit(jsonKindName(k.kind())));
            case SqlExpr.TypeNameLit t -> writer.append(stringLit(typeofName(t.scalar())));
            case SqlExpr.IntLit i -> writer.append(String.valueOf(i.value()));
            // NUMERIC CHARTER Rule 1 (docs/NUMERIC_CHARTER_2026_09_17.md): a
            // Float literal renders BARE in the plain Float spelling — the
            // engine's own literal processor (extensionDefaults.pure:134,
            // format '%s'); the database types it DECIMAL and every
            // expression over it stays in the database's own kind; the
            // declared kind converts ONCE at the root select (Rule 2).
            // (Retired: `CAST(x AS DOUBLE)`, 6975118a6 — double arithmetic
            // everywhere: 55.00000000000001 for 55.0.)
            case SqlExpr.FloatLit f -> writer.append(floatLiteral(f.value()));
            // a scale-0 DECIMAL-fact literal (a pure d-suffixed integer:
            // 17774d) CASTS so the wire reads DECIMAL — bare digits read
            // INTEGER by magnitude (probed 1.5.0; the (10,3)<>(15,3)
            // times family). HUGEINT-fact big integers and fractional
            // decimals render bare; engine-TEXT renderers intercept
            // upstream with the goldens' own spelling.
            case SqlExpr.DecimalLit d ->
                    writer.append(d.type() instanceof com.legend.sql.TypeFact.Typed t
                            && t.type() instanceof com.legend.sql.SqlType
                                    .Decimal dd && dd.scale() == 0
                    ? "CAST(" + d.value().toPlainString() + " AS DECIMAL("
                            + dd.precision() + ",0))"
                    : d.type() instanceof com.legend.sql.TypeFact.Typed h
                            && h.type() == com.legend.sql.SqlType.Scalar.HUGEINT
                    ? hugeIntLiteral(d.value())
                    : d.value().toPlainString());
            case SqlExpr.BoolLit b -> writer.append(boolLit(b.value()));
            case SqlExpr.NullLit n -> writer.append("NULL");
            case SqlExpr.DateLit d -> writer.append(dateLit(d.iso()));
            case SqlExpr.TimestampLit t -> writer.append(timestampLit(t.iso()));
            case SqlExpr.TimeLit t -> writer.append(timeLit(t.iso()));
            case SqlExpr.OrderedListAgg ola -> writer.append("list(").expr(ola.value(), 0).append(" ORDER BY ")
                    .expr(ola.orderBy(), 0).append(")");
            case SqlExpr.ArrayLit a -> arrayLit(writer, a.elements());
            case SqlExpr.StructLit s -> structLit(writer, s);
            case SqlExpr.StructGet g -> structGet(writer, g);
            case SqlExpr.Call c -> call(writer, c, parentPrec);
            case SqlExpr.Case c -> caseExpr(writer, c);
            case SqlExpr.Exists ex -> {
                writer.append("EXISTS (");
                inline(writer, ex.subquery());
                yield writer.append(")");
            }
            case SqlExpr.InSubquery i -> {
                writer.expr(i.value(), 4).append(" IN (");
                inline(writer, i.subquery());
                yield writer.append(")");
            }
            case SqlExpr.CheckedDefects ignored -> throw new DialectCapability(
                    "nested checked defects reached a dialect without list lambdas");
            case SqlExpr.CheckedChildValue ignored -> throw new DialectCapability(
                    "a checked child's value reached a dialect without list lambdas");
            case SqlExpr.Quantified q -> {
                writer.expr(q.value(), 4);
                writer.append(" ").append(java.util.Objects.requireNonNull(INFIX.get(q.comparison()),
                        "quantified comparison must be an infix operator: " + q.comparison()).sql())
                        .append(" ").append(q.quantifier().toString()).append(" (");
                inline(writer, q.subquery());
                yield writer.append(")");
            }
            case SqlExpr.ScalarSubquery sq -> {
                writer.append("(");
                inline(writer, sq.subquery());
                yield writer.append(")");
            }
            // CHECKED NARROWING (the ONE semantic node, D1): execution
            // dialects spell pure's toOne size guard — >1 raises pure's
            // message, 1 extracts, 0/NULL flows the engine-noOp empty.
            // Engine-TEXT renderers override with the verbatim inner
            // value (processNoOp view).
            case SqlExpr.CheckedOne co -> checkedOne(writer, co, parentPrec);
            case SqlExpr.CompactList cl -> compactList(writer, cl, parentPrec);
            // a value whose kind the database decides (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a): its root
            // projects it as its tag and slots, and a rule consuming it reads a channel; no dialect writes it
            case SqlExpr.Tagged t -> tagged(writer, t, parentPrec);
            // a list of such values (§5a's step B, track L): its root unnests its channels into rows, and a rule
            // consuming it reads its channels; no dialect writes it
            case SqlExpr.TaggedList tl -> throw new IllegalStateException("a tagged list reached the renderer: a"
                    + " mixed list is returned, sorted, deduplicated, searched, indexed, mapped or printed, and any"
                    + " other consumer of it is not lowered (channels of kinds "
                    + tl.slots().stream().map(com.legend.sql.SqlType.Tagged.Slot::kind).toList() + ")");
            case SqlExpr.DeferredTdsString d -> throw new IllegalStateException(
                    "deferred relation-toString reached the renderer — the"
                    + " execution boundary must resolve the dynamic column"
                    + " list first (DeferredTdsString id " + d.id() + ")");
            case SqlExpr.WindowCall w -> windowCall(writer, w);
            case SqlExpr.Lambda l -> lambda(writer, l);
            // a cast to the tagged carrier is the identity: the column holds the tag, an index, and the label says
            // what it indexes (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a), as a LITERAL label rides on text
            case SqlExpr.Cast c -> c.target() instanceof com.legend.sql.SqlType.Tagged
                    ? writer.expr(c.value(), parentPrec) : variantAwareCast(writer, c);
            case SqlExpr.FoldCall f -> foldCall(writer, f);
            case SqlExpr.JsonObject j -> jsonObject(writer, j);
            case SqlExpr.JsonArray j -> jsonArray(writer, j);
            case SqlExpr.JsonArrayAgg j -> jsonArrayAgg(writer, j);
            case SqlExpr.ReduceCollection rc -> reduceCollection(writer, rc);
            case SqlExpr.Membership m -> m.collection() instanceof SqlExpr.PlanParam p
                    ? anyOf(writer, m.needle(), p, parentPrec) : membership(writer, m);
            case SqlAgg.Reducer r -> reducer(writer, r);
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

    /** Collection membership — backend data-model capability; the
     * portable route is the CarrierStrategies IN-rewrite. */
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        throw new DialectCapability("collection membership reached a"
                + " dialect without a list encoding [collection: "
                + m.collection().getClass().getSimpleName()
                + (m.collection() instanceof SqlExpr.Call c ? " " + c.fn() : "") + "]");
    }

    /** Reduce a collection VALUE with a named aggregate — a backend
     * DATA-MODEL capability; the ANSI base has no collection values.
     * The portable route is the CarrierStrategies FUSION into the
     * collecting subselect; a node that survives to rendering here is
     * an honest budget-counted wall. */
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        throw new DialectCapability("collection reduction '" + rc.reducer()
                + "' reached a dialect without a list encoding");
    }

    /** DuckDB reference JSON-object constructor: alternating key/value
     * arguments. Dialects with the SQL-standard {@code KEY: VALUE} form
     * override. */
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        return writer.append("json_object(").list(j.kv()).append(")");
    }

    /** DuckDB reference JSON-array constructor; the SQL-standard
     * {@code JSON_ARRAY} spelling is an override. */
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.append("json_array(").list(j.elements()).append(")");
    }

    /**
     * COALESCE: an aggregate over ZERO rows is SQL NULL; the graph
     * contract says empty collection = the EMPTY ARRAY.
     * ordered form: json_group_array is a DuckDB MACRO (no ORDER
     * BY) — list() is a real aggregate that takes one, and to_json
     * over the JSON list yields the same array value
     */
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        return j.orderKeys().isEmpty()
                ? writer.append("coalesce(json_group_array(").expr(j.value(), 0).append("), '[]')")
                : writer.append("coalesce(to_json(list(").expr(j.value(), 0).append(" ORDER BY ")
                        .join(j.orderKeys(), ", ", (w, k) -> w.expr(k.expr(), 0)
                                .append(k.desc() ? " DESC" : " ASC").append(" NULLS LAST"))
                        .append(")), '[]')");
    }

    /**
     * A function call. Each concrete dialect decides every {@link SqlFn} in its own exhaustive switch, its whole table:
     * each function's spelling, the named helper that writes it, or its refusal (DialectVocabularyTest; the dialect
     * program's Phase 1, 2026-10-13). The base spells none: the method is abstract, so a dialect that does not decide
     * is a compile error.
     */
    protected abstract SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec);

    // ---- the helpers a dialect's switch calls by name: a spelling several databases share, written once (helpers
    // implement, the switch decides; none is reached unless a dialect names it)

    /** An infix operator (OR, AND, the comparisons, {@code + - *}), ANSI-spelled, parenthesized by precedence. */
    protected final SqlWriter infix(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        Infix infix = java.util.Objects.requireNonNull(INFIX.get(c.fn()), () -> c.fn() + " is no infix operator");
        // NON-COMMUTATIVE ops (-): trailing SAME-precedence operands
        // must parenthesize — 6 - (4 - 5) is not 6 - 4 - 5 (a real
        // wrong-answer bug PCT caught on the minus composition tests).
        // COMPARISONS (prec 4) are NON-ASSOCIATIVE: a nested
        // comparison operand always parenthesizes — bare
        // a = b = TRUE is a type error, (a = b) = TRUE is the value.
        // + and * too (IQ island, 2026-10-17, the differential fuzzer's fifth round): a trailing operand of the same
        // precedence is its own value -- w + (2000 - 2020) is not w + 2000 - 2020, which overflows a BIGINT w that the
        // sum fits, and a DOUBLE's sum rounds by its order; a run's operands (one call's) stay in their written order
        boolean trailingOwnValue = c.fn() == SqlFn.MINUS || c.fn() == SqlFn.PLUS || c.fn() == SqlFn.TIMES;
        boolean nonAssociative = infix.prec() == 4;
        boolean wrap = infix.prec() < parentPrec;
        String pad = infixPad(c.fn());
        if (wrap) {
            writer.append("(");
        }
        for (int i = 0; i < c.args().size(); i++) {
            if (i > 0) {
                writer.append(pad).append(infix.sql()).append(pad);
            }
            writer.expr(c.args().get(i),
                    (i > 0 && trailingOwnValue) || nonAssociative
                            ? infix.prec() + 1 : infix.prec());
        }
        if (wrap) {
            writer.append(")");
        }
        return writer;
    }

    /**
     * B7 (RaisedErrors): a raise through the function {@code fn}, its message enveloped. A message WE raise carries
     * the U+001F provenance sentinel at BOTH ends — the Executor funnel extracts between them, removing the driver's
     * transport envelope from OUR OWN text only; native errors never match. An optional SECOND arg is the raising
     * call's source span ('line:col', a literal — PureSql.raise): it rides INSIDE the envelope behind a U+001E divider
     * so RaisedErrors can hand assertError the position and production text stays clean (the funnel strips the whole
     * envelope).
     */
    protected final SqlWriter raiseEnvelope(SqlWriter writer, List<SqlExpr> a, String fn) {
        writer.append(fn + "(chr(31) || ");
        if (a.size() > 1) {
            writer.expr(a.get(1), 0).append(" || chr(30) || ");
        }
        return writer.append("(").expr(a.get(0), 0).append(") || chr(31))");
    }

    /**
     * NULL-IGNORING flat concat, where the database's {@code concat} skips NULL arguments (H2 CONCAT and DuckDB concat
     * both do): a LEFT-JOIN-missed operand yields the other side, never NULL. The '||' spelling propagates NULL — a
     * row-value divergence on join misses (testQualifierWithVariableArg). Nested concats splice into one call.
     */
    protected final SqlWriter concatFlat(SqlWriter writer, List<SqlExpr> a) {
        return concatOf(writer, flattenConcat(a));
    }

    /** {@code concat} of the arguments, at most {@link #maxFunctionArguments} to one call: past them, a concat of
     *  the concats of runs of them (the database's concat never yields NULL, so the nesting keeps its value). */
    private SqlWriter concatOf(SqlWriter writer, List<SqlExpr> args) {
        int max = maxFunctionArguments();
        if (args.size() <= max) {
            return writer.append("concat(").list(args).append(")");
        }
        int run = (args.size() + max - 1) / max;
        writer.append("concat(");
        for (int from = 0; from < args.size(); from += run) {
            if (from > 0) {
                writer.append(", ");
            }
            concatOf(writer, args.subList(from, Math.min(args.size(), from + run)));
        }
        return writer.append(")");
    }

    /** The most arguments one function call takes on this database: no limit in the base; Postgres's
     *  is 100 ({@code FUNC_MAX_ARGS}). */
    protected int maxFunctionArguments() {
        return Integer.MAX_VALUE;
    }

    /** {@code NOT x}, parenthesized under an operator that binds tighter. */
    protected final SqlWriter notPrefix(SqlWriter writer, List<SqlExpr> a, int parentPrec) {
        if (3 < parentPrec) {
            writer.append("(");
        }
        writer.append("NOT ").expr(a.get(0), 3);
        if (3 < parentPrec) {
            writer.append(")");
        }
        return writer;
    }

    /** {@code -x}. */
    protected final SqlWriter negation(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("-").expr(a.get(0), 7);
    }

    /** {@code x IS NULL}. */
    protected final SqlWriter isNullTest(SqlWriter writer, List<SqlExpr> a) {
        return writer.expr(a.get(0), 4).append(" IS NULL");
    }

    /** {@code x IS NOT NULL}. */
    protected final SqlWriter isNotNullTest(SqlWriter writer, List<SqlExpr> a) {
        return writer.expr(a.get(0), 4).append(" IS NOT NULL");
    }

    /** {@code x IN (a, b, ...)}; a plan parameter as the WHOLE list is one array, bound once ({@link #anyOf}). */
    protected final SqlWriter inList(SqlWriter writer, List<SqlExpr> a, int parentPrec) {
        if (a.size() == 2 && a.get(1) instanceof SqlExpr.PlanParam p) {
            return anyOf(writer, a.get(0), p, parentPrec);
        }
        return writer.expr(a.get(0), 4).append(" IN (").list(a.subList(1, a.size())).append(")");
    }

    /** {@code (x IS DISTINCT FROM y)}: the null-safe inequality (the engine's nullSafeNotEqual DynaFunction). */
    protected final SqlWriter distinctFrom(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("(").expr(a.get(0), 4).append(" IS DISTINCT FROM ").expr(a.get(1), 4).append(")");
    }

    /** {@code (x IS NOT DISTINCT FROM y)}: the null-safe equality (the engine's nullSafeEqual DynaFunction). */
    protected final SqlWriter notDistinctFrom(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("(").expr(a.get(0), 4).append(" IS NOT DISTINCT FROM ").expr(a.get(1), 4).append(")");
    }

    /**
     * MUST-honor semantics (PHASE_HIJ_LOWERING.md): Pure's divide(Number, Number) IS a Float — the division itself is
     * a DOUBLE division, so both operands cast BEFORE it (a cast after would keep the operands' arithmetic: integers
     * truncate, decimals divide exactly on H2 — 36-digit NUMERIC — and round to a double the engine's double division
     * need not reach; stress corpus 2026-09-16: 936 notionalPerRiskPoint rows). The former `1.0 *` promotion only
     * dodged integer truncation.
     */
    protected final SqlWriter divideAsDoubles(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("(CAST(").expr(a.get(0), 0).append(" AS DOUBLE) / CAST(").expr(a.get(1), 0)
                .append(" AS DOUBLE))");
    }

    /** Pure's mod: the remainder with the divisor's sign, {@code MOD(MOD(x, y) + y, y)}. */
    protected final SqlWriter modFloored(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("MOD(MOD(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(") + ")
                .expr(a.get(1), 0).append(", ").expr(a.get(1), 0).append(")");
    }

    /** Pure's rem: the remainder with the dividend's sign, {@code MOD(x, y)}. */
    protected final SqlWriter modTruncated(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("MOD(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
    }

    /** {@code CAST(fn(x) AS BIGINT)}: ceiling, floor and sign are Integers in Pure. */
    protected final SqlWriter bigintOf(SqlWriter writer, String fn, List<SqlExpr> a) {
        return writer.append("CAST(").append(fn).append("(").expr(a.get(0), 0).append(") AS BIGINT)");
    }

    /** {@code CAST(x AS type)}, the type as the calling dialect spells it. */
    protected final SqlWriter castAs(SqlWriter writer, SqlExpr x, String type) {
        return writer.append("CAST(").expr(x, 0).append(" AS ").append(type).append(")");
    }

    /**
     * Boolean xor as {@code (x AND NOT y) OR (NOT x AND y)}: the OR-chain misbinds under an enclosing AND — the WALK
     * wraps it ({@link #op}), never this helper by hand; each operand is WRITTEN twice.
     */
    protected final SqlWriter xorAsAndOr(SqlWriter writer, List<SqlExpr> a, int parentPrec) {
        return op(writer, parentPrec, () -> writer.append("(").expr(a.get(0), 3).append(" AND NOT ")
                .expr(a.get(1), 3).append(") OR (NOT ").expr(a.get(0), 3).append(" AND ").expr(a.get(1), 3)
                .append(")"));
    }

    /** {@code lpad}/{@code rpad}: the PAD CHAR is optional in Pure; SQL requires it — ' '. */
    protected final SqlWriter padWithSpace(SqlWriter writer, String fn, List<SqlExpr> a) {
        return writer.append(fn).append("(").list(a.size() == 2
                ? List.of(a.get(0), a.get(1), new SqlExpr.StringLit(" ")) : a).append(")");
    }

    /** {@code lpad}/{@code rpad} where the length is a 32-bit INTEGER (DuckDB's, Postgres's), which neither narrows from a
     *  BIGINT implicitly: a length that is no literal, a computed Pure Integer (a BIGINT), cast to INTEGER (IQ island,
     *  2026-10-15: a CHAR(n) padded to n UTF-8 bytes, rpad(s, length(s) + n - bytes(s))). */
    protected final SqlWriter padWithSpaceToInteger(SqlWriter writer, String fn, List<SqlExpr> a) {
        if (a.get(1) instanceof SqlExpr.IntLit) {
            return padWithSpace(writer, fn, a);
        }
        writer.append(fn).append("(").expr(a.get(0), 0).append(", CAST(").expr(a.get(1), 0).append(" AS INTEGER), ");
        return writer.expr(a.size() == 2 ? new SqlExpr.StringLit(" ") : a.get(2), 0).append(")");
    }

    /** toUpperFirstCharacter / toLowerFirstCharacter: {@code fn(substr(x, 1, 1)) || substr(x, 2)}; the {@code ||}
     *  concat misbinds under +/comparison — walk-wrapped. */
    protected final SqlWriter firstCharCased(SqlWriter writer, String fn, List<SqlExpr> a, int parentPrec) {
        return op(writer, parentPrec, () -> writer.append(fn).append("(substr(").expr(a.get(0), 0)
                .append(", 1, 1)) || substr(").expr(a.get(0), 0).append(", 2)"));
    }

    /**
     * DAY-GRAINED truncation delivers a DATE (§8.3a carrier burn, dialect-owned per the single-compiler tenet: the
     * SEMANTIC fact is pure's firstDayOf*(Date):Date; whether a cast is needed to honor it is the backend's idiom —
     * DuckDB's date_trunc returns TIMESTAMP). The engine-TEXT channel spells its own.
     */
    protected final SqlWriter dateTruncDateGrained(SqlWriter writer, List<SqlExpr> a, String spelledPart) {
        SqlWriter.Piece trunc = w -> w.append("date_trunc(").append(spelledPart).append(", ").expr(a.get(1), 0)
                .append(")");
        return switch (part(a.get(0))) {
            case MONTH, YEAR, WEEK, QUARTER -> writer.append("CAST(").piece(trunc).append(" AS DATE)");
            case DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK ->
                    writer.piece(trunc);
        };
    }

    /** A JSON kind's name as this dialect's {@code JSON_TYPE} answers it (the dialect program's Phase 2: until
     *  2026-10-15 the IR carried DuckDB's names). */
    protected abstract String jsonKindName(com.legend.sql.JsonKind kind);

    /** A type's name as this dialect's {@code TYPEOF} answers it. */
    protected abstract String typeofName(com.legend.sql.SqlType.Scalar type);

    /** The part a date call's first argument names ({@link SqlExpr.DatePartLit}): the call's arm spells it. */
    protected static com.legend.sql.DatePart part(SqlExpr e) {
        if (e instanceof SqlExpr.DatePartLit p) {
            return p.part();
        }
        throw new IllegalStateException("a date call's first argument is its part, not " + e
                + ": the lowering's bug");
    }

    /** A Float's literal spelling: the portable decomposition over the double's text (a dialect whose double text is
     *  long spells it once, its own way). */
    protected final SqlWriter floatCanonByText(SqlWriter writer, List<SqlExpr> a, int parentPrec) {
        return writer.expr(FloatCanonText.of(a.get(0)), parentPrec);
    }

    /** A Float's print form ({@code c} a {@link com.legend.sql.SqlFn#FLOAT_PRINT} call): the portable decomposition
     *  over the double's text (a dialect whose double text is long spells it once, its own way). */
    protected final SqlWriter floatPrintByText(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        return writer.expr(FloatPrintText.of(c.args().get(0)), parentPrec);
    }

    // ---- idiom extension points (base = capability statement, loud) ----

    /** Pure hashCode is Integer[1] — SIGNED 64-bit. A dialect whose
     * native hash is unsigned (DuckDB UBIGINT) conforms by
     * reinterpreting cast; the value stays bijective. */
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        throw new DialectCapability("signed 64-bit hashCode reached a dialect without a spelling");
    }

    /** Pure ROUND is HALF-EVEN (banker's) — every dialect must honor it. */
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        throw new DialectCapability("banker's ROUND reached a dialect without a spelling");
    }

    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        throw new DialectCapability(fnName + " reached a dialect without bit-op support");
    }

    /** Construct a variant (JSON) value from any value. */
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        throw new DialectCapability("toVariant reached a dialect without JSON support");
    }

    /** Fold with PURE (element, accumulator) lambda; the encoding is the dialect's. */
    protected SqlWriter foldCall(SqlWriter writer, SqlExpr.FoldCall f) {
        throw new DialectCapability("fold reached a dialect without a fold encoding");
    }

    /**
     * exists/forAll over a collection value. The expansion MUST honor Pure's
     * empty-collection semantics: {@code exists([]) = false},
     * {@code forAll([]) = true}.
     */
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("collection exists reached a dialect"
                + " without a list-predicate encoding");
    }

    /** 1-arg collection isDistinct (D6): true iff no duplicate
     * elements; empty and singleton are trivially true. */
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("collection isDistinct reached a"
                + " dialect without a list encoding");
    }

    /** Contract includes Pure's empty-collection semantics: {@code forAll([]) = true}. */
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("collection forAll reached a dialect"
                + " without a list-predicate encoding");
    }

    /** map/filter/concat/contains over list values. */
    protected SqlWriter listCall(SqlWriter writer, SqlFn fn, List<SqlExpr> args) {
        throw new DialectCapability(fn + " reached a dialect without a list encoding");
    }

    /** Pure's splitPart (non-empty tokens, 1-based, NULL past the end). */
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("PURE_SPLIT_PART reached a dialect without a spelling");
    }

    /** Explode a collection into rows, aligned with sibling projections. */
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("UNNEST reached a dialect without an unnest placement");
    }

    /** The elements of a variant (JSON) array value. */
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("variant navigation reached a dialect without JSON support");
    }

    /** JSON access ({@code v -> key}). */
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("variant navigation reached a dialect without JSON support");
    }

    /** struct_insert(s, 'name', v) — a struct with one field appended;
     * only struct-capable dialects render it. */
    protected SqlWriter structInsert(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("struct_insert reached a dialect without struct support");
    }

    /** Lambda expression — only dialects with lambda-capable functions render these. */
    protected SqlWriter lambda(SqlWriter writer, SqlExpr.Lambda l) {
        throw new DialectCapability("a lambda reached a dialect without lambda support");
    }

    /**
     * CAST rendering; a dialect may route a variant-access value through its
     * text-extraction idiom first (DuckDB {@code ->>}). Base: plain CAST.
     */
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        // The temporal-text marker cast is a LABEL device (§4bZ-V B3):
        // the value is already the precision-faithful text — the cast
        // exists to carry the fact and NEVER renders, on any dialect
        if (c.target() == com.legend.sql.SqlType.Scalar.TEMPORAL_TEXT
                || c.target() == com.legend.sql.SqlType.Scalar.DECIMAL_TEXT) {
            return writer.expr(c.value(), 0);
        }
        return writer.append("CAST(").expr(c.value(), 0).append(" AS ").append(castTypeName(c.target())).append(")");
    }

    // ---- window / aggregate / case (ANSI) ----

    protected SqlWriter caseExpr(SqlWriter writer, SqlExpr.Case c) {
        writer.append("CASE");
        for (SqlExpr.Case.When w : c.whens()) {
            writer.append(" WHEN ").expr(w.condition(), 0).append(" THEN ").expr(w.then(), 0);
        }
        if (c.otherwise() != null) {
            writer.append(" ELSE ").expr(c.otherwise(), 0);
        }
        return writer.append(" END");
    }

    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        switch (w.fn()) {
            case SqlAgg.Reducer r -> reducer(writer, r);
            case SqlAgg.RankingFn r -> writer.append(aggregateName(r.fn())).append("(").list(r.args()).append(")");
            case SqlAgg.ValueFn v -> writer.append(aggregateName(v.fn())).append("(").list(v.args()).append(")");
        }
        return over(writer, w);
    }

    /** A window's {@code OVER (...)}: its partition, order and frame. */
    protected final SqlWriter over(SqlWriter writer, SqlExpr.WindowCall w) {
        writer.append(" ").append(keyword("OVER")).append(" (");
        if (!w.partitionBy().isEmpty()) {
            writer.append(keyword("PARTITION BY")).append(" ").list(w.partitionBy());
        }
        if (!w.orderBy().isEmpty()) {
            if (!w.partitionBy().isEmpty()) {
                writer.append(" ");
            }
            writer.append(keyword("ORDER BY")).append(" ").join(w.orderBy(), ", ", this::sortKey);
        }
        if (w.frame() != null) {
            writer.append(" ").append(keyword(w.frame().kind().toString())).append(" ").append(keyword("BETWEEN"))
                    .append(" ").append(bound(w.frame().from())).append(" ").append(keyword("AND")).append(" ")
                    .append(bound(w.frame().to()));
        }
        return writer.append(")");
    }

    /** A keyword of the window and aggregate syntax ({@code OVER}, {@code ORDER BY}, {@code DISTINCT} ...) as this
     *  dialect writes it: as written here. The legacy engine-text printer writes legend-engine's lowercase (its
     *  SQL dialect translation's {@code keyword()}, upper-case keywords off). */
    protected String keyword(String keyword) {
        return keyword;
    }

    /** An aggregate, ranking or value function's name as this dialect writes it: its {@link SqlAgg.Fn} name here.
     *  The legacy engine-text printer writes legend-engine's H2 names. */
    protected String aggregateName(SqlAgg.Fn fn) {
        return fn.toString();
    }

    protected String bound(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> keyword("UNBOUNDED PRECEDING");
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> p.n() + " " + keyword("PRECEDING");
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> keyword("CURRENT ROW");
            case SqlExpr.WindowCall.Frame.Bound.Following f -> f.n() + " " + keyword("FOLLOWING");
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> keyword("UNBOUNDED FOLLOWING");
            // DuckDB interval spelling; DurationUnit names (DAYS, MONTHS...)
            // are valid interval units as-is.
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p ->
                    keyword("INTERVAL") + " " + p.n() + " " + p.unit() + " " + keyword("PRECEDING");
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing f ->
                    keyword("INTERVAL") + " " + f.n() + " " + f.unit() + " " + keyword("FOLLOWING");
        };
    }

    /** Each dialect names every aggregate itself (the dialect program's Phase 1b step 3, 2026-10-13): the base spells
     *  none. */
    protected abstract SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r);

    /** An aggregate as most databases write it, by the name its dialect gives it:
     *  {@code name([DISTINCT ]args[ ORDER BY keys])}, {@code *} for no argument. */
    protected final SqlWriter aggregate(SqlWriter writer, String name, SqlAgg.Reducer r) {
        writer.append(name).append("(").append(r.distinct() ? keyword("DISTINCT") + " " : "");
        if (r.args().isEmpty()) {
            writer.append("*");
        } else {
            writer.list(r.args());
        }
        // ORDER-SENSITIVE aggregation (SQL standard <sort specification
        // list> inside the aggregate: string_agg(x, sep ORDER BY k))
        if (!r.orderBy().isEmpty()) {
            writer.append(" ").append(keyword("ORDER BY")).append(" ")
                    .join(r.orderBy(), ", ", this::aggregateOrderKey);
        }
        return writer.append(")");
    }

    /** {@inheritDoc} The engine's H2 text is the one translated language: its connection declares
     *  {@link RawSqlBoundary#ENGINE_TEXT}. */
    @Override
    public final String raw(String statement, @com.legend.base.Nullable String writtenFor) {
        return RawSqlBoundary.ENGINE_TEXT.equals(writtenFor) ? fromEngineText(statement) : statement;
    }

    /** A catalog grid's query ({@link SqlSource.Catalog}): where this database keeps its catalog and how it is read,
     *  its columns named by the source's outputs. A dialect whose database is proven to answer it writes it; any other
     *  refuses it (the dialect program's Phase 3, 2026-10-16). */
    protected SqlWriter catalog(SqlWriter writer, SqlSource.Catalog c) {
        throw new DialectCapability("a " + c.grid() + " catalog grid (fetchDb*MetaData) reached a dialect without a"
                + " catalog reading");
    }

    /** A lowering-internal pseudo-aggregate ({@link SqlAgg.Fn#marker}) reached a renderer: the lowering's bug. */
    protected static IllegalStateException loweringMarker(SqlAgg.Reducer r) {
        return new IllegalStateException("lowering marker " + r.fn() + " reached the renderer");
    }

    /** A window function as an aggregate: it is a ranking or value function, which {@code windowCall} writes. */
    protected static IllegalStateException windowFunction(SqlAgg.Reducer r) {
        return new IllegalStateException(r.fn() + " is a window function: windowCall writes it");
    }

    /** A key of an aggregate's own ordering: the expression, its direction, its declared null placement. */
    protected final SqlWriter aggregateOrderKey(SqlWriter writer, SqlSelect.SortKey k) {
        return writer.expr(k.expr(), 0).append(" ").append(keyword(k.ascending() ? "ASC" : "DESC"))
                .append(aggOrderNullPlacement(k));
    }

    /** A key with DECLARED null placement keeps it inside the aggregate
     * (pure null-largest sorts hoisted into toString — witness PCT
     * testRange_..._WithOrderByDESC: DESC NULLS FIRST died here and
     * nulls sank to the backend default); legacy keys carry none. The
     * ENGINE-TEXT channel overrides to suppress: the engine spells a NULLS
     * clause only for a query's explicit emptyFirst()/emptyLast(), which
     * the IR cannot yet tell from pure's own null order (PARK-18; the
     * sortKey suppression's aggregate-internal twin). */
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

    /** A time of day, {@code TIME 'HH:mm:ss[.ffffff]'} (DuckDB, Postgres, H2, MariaDB, Trino, Snowflake, BigQuery
     *  spell it so); a dialect without the type refuses. */
    protected String timeLit(String iso) {
        return "TIME '" + iso + "'";
    }

    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        throw new DialectCapability("an array literal reached a dialect without array support");
    }

    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit s) {
        throw new DialectCapability("a struct literal reached a dialect without struct support");
    }

    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        throw new DialectCapability("a struct extraction reached a dialect without struct support");
    }

    /** A scalar type's name: each concrete dialect decides every scalar in its own switch, a name or a refusal (the
     *  dialect program's Phase 1, 2026-10-13; DialectVocabularyTest). */
    protected abstract String scalarTypeName(com.legend.sql.SqlType.Scalar s);

    /** SQL type → CAST spelling: the scalar leaves the dialect's, the composite rules structural and shared. */

    protected final String castTypeName(com.legend.sql.SqlType t) {
        return switch (t) {
            case com.legend.sql.SqlType.Scalar s -> scalarTypeName(s);
            case com.legend.sql.SqlType.Decimal d ->
                    "DECIMAL(" + d.precision() + ", " + d.scale() + ")";
            case com.legend.sql.SqlType.Array a -> arrayTypeName(castTypeName(a.element()));
            case com.legend.sql.SqlType.Map m -> mapTypeName(castTypeName(m.key()), castTypeName(m.value()));
            case com.legend.sql.SqlType.Struct st -> structTypeName(st);
            case com.legend.sql.SqlType.Tagged tg -> throw new IllegalStateException("a tagged value is never cast"
                    + " or stored: its tag and slots are its query's columns"
                    + " (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a)");
        };
    }

    /** A list type's spelling, its element spelled: {@code T[]} (DuckDB, Postgres); a dialect with the
     *  {@code ARRAY<T>} form overrides (Databricks, 2026-10-11). */
    protected String arrayTypeName(String element) {
        return element + "[]";
    }

    /** A map type's spelling: {@code MAP(K, V)}; the {@code MAP<K, V>} form is an override. */
    protected String mapTypeName(String key, String value) {
        return "MAP(" + key + ", " + value + ")";
    }

    /** A struct type's spelling, or its refusal where the database has none: each dialect decides. */
    protected abstract String structTypeName(com.legend.sql.SqlType.Struct st);

    /** A struct type's fields, each its name (spelled) and its type (spelled), for a dialect's {@link
     *  #structTypeName}. */
    protected final List<java.util.Map.Entry<String, String>> structFields(com.legend.sql.SqlType.Struct st) {
        return st.fields().stream()
                .map(fl -> java.util.Map.entry(ident(fl.name()), castTypeName(fl.type()))).toList();
    }

    /** The DATE-FORMAT spelling: each dialect writes its own vocabulary (or consumes {@link SqlExpr.FormatLit} parts
     *  in its call arms and never lets one reach here). */
    protected abstract String formatText(SqlExpr.FormatLit fl);

    /** A date format in strftime codes (DuckDB's; SQLite's and MariaDB's strftime helpers read them too), for a
     *  dialect's {@link #formatText}. EXHAUSTIVE: a new part is a compile error. */
    protected final String strftimeText(SqlExpr.FormatLit fl) {
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

    /**
     * A plan parameter's placeholder: the value bound bare, typed by the database from the bound value (DuckDB and
     * Postgres do, every type answering as its literal does: docs/execution-plan-boundary-2026-10-05/probes/
     * literal-results.txt) -- but a date-time, which is cast to a TYPE HOLE the runner fills with the type of the
     * value's literal ({@link #holeType}) and passed as its text: the drivers pass no digit finer than a microsecond
     * alike (DuckDB's cuts it, Postgres's rounds it; probes/timestamp-results.txt). A dialect whose database types a
     * parameter otherwise writes it typed.
     */
    protected SqlWriter placeholder(SqlWriter writer, SqlExpr.PlanParam p) {
        com.legend.sql.ValueTyping byValue = p.byValue();
        if (byValue != null && byValue.kinds().stream().anyMatch(k -> k == com.legend.sql.ValueKind.DATE_TIME
                || k == com.legend.sql.ValueKind.DATE_TIME_NANOS)) {
            java.util.Map<com.legend.sql.ValueKind, RenderedStatement.TypeSpelling> types =
                    new java.util.EnumMap<>(com.legend.sql.ValueKind.class);
            for (com.legend.sql.ValueKind kind : byValue.kinds()) {
                types.put(kind, holeType(kind));
            }
            return writer.append("CAST(").bind(scalarBind(p)).append(" AS ").typeHole(types, byValue.absent())
                    .append(")");
        }
        return writer.bind(scalarBind(p));
    }

    /** The type this dialect's database gives a literal of a value of {@code kind}, for a placeholder cast to its value's
     *  type: here, a date and a date-time's (every other value is bound bare); a date-time finer than a microsecond is
     *  each dialect's own. */
    protected RenderedStatement.TypeSpelling holeType(com.legend.sql.ValueKind kind) {
        return switch (kind) {
            case DATE -> new RenderedStatement.TypeSpelling("DATE", RenderedStatement.Digits.NONE);
            case DATE_TIME -> new RenderedStatement.TypeSpelling("TIMESTAMP", RenderedStatement.Digits.NONE);
            case DATE_TIME_NANOS -> throw new DialectCapability("a date-time finer than a microsecond: this dialect"
                    + " names no type its literal has");
            case INTEGER, DECIMAL, FLOATING -> throw new DialectCapability("a value of kind " + kind + " is bound"
                    + " bare here, typed by the database from the value");
        };
    }

    /**
     * A plan parameter bound as ONE value — an optional one's absence a null, an enumeration's its name (compared with a
     * mapped column through a value table, {@code EnumValueTables}). What one value cannot carry is refused by name,
     * never bound as something it is not: a RAW splice and a parameter carrying the legacy printer's enumeration-mapping
     * function (both plan-template vocabulary). A list is bound whole, as one array, by {@link #anyOf}
     * (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2).
     */
    protected static RenderedStatement.Bind scalarBind(SqlExpr.PlanParam p) {
        if (p.kind() == SqlExpr.PlanParam.Kind.RAW) {
            throw new DialectCapability("plan parameter '" + p.name() + "' is RAW: it splices plan text, never a"
                    + " bound value");
        }
        if (p.enumMapFn() != null) {
            throw new DialectCapability("enum plan parameter '" + p.name() + "' carries the legacy printer's mapping"
                    + " function (a value table, a plan template's): never a bound value");
        }
        return new RenderedStatement.Bind(p.name(), new RenderedStatement.Binding.One(nullType(p), null));
    }

    /** The JDBC type an absent value of {@code p} is bound as: its type's; for a parameter its value types, its absent
     *  kind's. Refused by name for a parameter of neither (a legacy plan's template parameter). */
    protected static String nullType(SqlExpr.PlanParam p) {
        if (p.type() instanceof com.legend.sql.TypeFact.Typed t) {
            return jdbcType(t.type(), p.name());
        }
        com.legend.sql.ValueTyping byValue = p.byValue();
        if (byValue == null) {
            throw new DialectCapability("plan parameter '" + p.name() + "' has no type and is not typed by its value:"
                    + " not bound");
        }
        return switch (byValue.absent()) {
            case INTEGER -> "BIGINT";
            case DECIMAL -> "DECIMAL";
            case FLOATING -> "DOUBLE";
            case DATE -> "DATE";
            case DATE_TIME, DATE_TIME_NANOS -> "TIMESTAMP";
        };
    }

    /** {@code t}'s JDBC type name (a {@code java.sql.JDBCType}'s), for a parameter's null. */
    private static String jdbcType(com.legend.sql.SqlType t, String parameter) {
        return switch (t) {
            case com.legend.sql.SqlType.Scalar scalar -> switch (scalar) {
                case BOOLEAN -> "BOOLEAN";
                case INTEGER -> "INTEGER";
                case BIGINT -> "BIGINT";
                case DOUBLE -> "DOUBLE";
                case VARCHAR -> "VARCHAR";
                case DATE -> "DATE";
                case TIMESTAMP -> "TIMESTAMP";
                case TIME -> "TIME";
                case HUGEINT, TIMESTAMPTZ, JSON, TEMPORAL_TEXT, DECIMAL_TEXT -> throw new DialectCapability(
                        "plan parameter '" + parameter + "' is typed " + scalar + ", which no parameter is bound as");
            };
            case com.legend.sql.SqlType.Decimal d -> "DECIMAL";
            case com.legend.sql.SqlType.Array a -> throw new DialectCapability("plan parameter '" + parameter
                    + "' is a list: bound as one array, never as one value");
            case com.legend.sql.SqlType.Map m -> throw new DialectCapability("plan parameter '" + parameter
                    + "' is a map, which no parameter is bound as");
            case com.legend.sql.SqlType.Struct st -> throw new DialectCapability("plan parameter '" + parameter
                    + "' is a struct, which no parameter is bound as");
            case com.legend.sql.SqlType.Tagged tg -> throw new DialectCapability("plan parameter '" + parameter
                    + "' is a tagged value, which no parameter is bound as");
        };
    }

    /**
     * {@code needle = ANY(?)}: a list parameter bound as ONE array of its element type, written bare on every database —
     * each answers it as the literal {@code IN (...)} a let writes, the empty list included, and H2 reads a CAST inside
     * {@code ANY(...)} as its boolean aggregate (docs/execution-plan-boundary-2026-10-05/probes/list-results.txt) — and
     * parenthesized under an operator that binds tighter ({@code =} does not chain). A parameter that is not a list of
     * a named element type (a scalar's, a plan template's) is refused by name.
     */
    private SqlWriter anyOf(SqlWriter writer, SqlExpr needle, SqlExpr.PlanParam list, int parentPrec) {
        if (!(list.type() instanceof com.legend.sql.TypeFact.Typed t
                && t.type() instanceof com.legend.sql.SqlType.Array array
                && array.element() instanceof com.legend.sql.SqlType.Scalar element)) {
            throw new DialectCapability("plan parameter '" + list.name() + "' is not a list parameter (an array of a"
                    + " named element type): it is not bound as a whole list");
        }
        boolean wrap = 4 < parentPrec;
        if (wrap) {
            writer.append("(");
        }
        writer.expr(needle, 5).append(" = ANY(").bind(new RenderedStatement.Bind(list.name(),
                        new RenderedStatement.Binding.Array(element.name())))
                .append(")");
        return wrap ? writer.append(")") : writer;
    }

    /** A parameter written where ONE value goes: a list parameter is bound only as a whole list ({@code in},
     *  {@code contains}: {@link #anyOf}); any other use of it is refused by name, never bound as something it is not. */
    private static SqlExpr.PlanParam oneValue(SqlExpr.PlanParam p) {
        if (p.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof com.legend.sql.SqlType.Array) {
            throw new DialectCapability("list parameter '" + p.name() + "' is bound only as a whole list (in,"
                    + " contains); written where one value goes, it is not bound");
        }
        return p;
    }

    /** A writer for this dialect: its {@link SqlWriter#expr} writes a sub-expression in this dialect's spelling. */
    protected final SqlWriter newWriter() {
        return new SqlWriter(this::expr);
    }

    /**
     * A composite arm whose SPELLING expands to operator text: the WALK
     * decides the parens — the expansion is declared WEAKEST-binding, so
     * any enclosing operator wraps it and no arm ever hand-parenthesizes
     * (remediation T1.6/T3.2: the misbind class is dead structurally, and
     * a new composite arm cannot reintroduce it by forgetting parens).
     * Writes {@code body}, parenthesized when an enclosing operator binds tighter.
     */
    protected final SqlWriter op(SqlWriter writer, int parentPrec, Runnable body) {
        if (parentPrec > 0) {
            writer.append("(");
        }
        body.run();
        if (parentPrec > 0) {
            writer.append(")");
        }
        return writer;
    }

    /**
     * A subquery written inline (EXISTS / scalar position), a parameter inside it bound in place: SINGLE-LINE mode
     * — {@link #nl} emits a space instead of a newline while set. Structural,
     * never text post-processing (collapsing rendered text would corrupt
     * whitespace inside string LITERALS).
     */
    protected final SqlWriter inline(SqlWriter writer, SqlQuery q) {
        boolean previous = inlineMode;
        inlineMode = true;
        try {
            query(writer, q, 0);
        } finally {
            inlineMode = previous;
        }
        return writer;
    }

    /** When set, clause separators render as single spaces (see {@link #inline}). */
    private boolean inlineMode;

    /** Quote ONLY when necessary (the lean tenet), per this dialect's rules. */
    /**
     * A table name may be schema-qualified (hr.EMPLOYEES): each part quotes
     * UNCONDITIONALLY — the engine's emission for schema tables, pinned by
     * the corpus ("hr"."EMPLOYEES").
     */
    protected String tableName(String name, com.legend.sql.NameQuoting quoting) {
        int dot = name.indexOf('.');
        if (dot <= 0) {
            // a name the store declared quoted ({@code Table "Orders"}) keeps its case: delimited
            return quoting.table() ? delimited(name) : ident(name);
        }
        return delimited(name.substring(0, dot)) + "." + delimited(name.substring(dot + 1));
    }

    /** COLUMN-NAME spelling at a reference — DIALECT-owned. The base
     * spells every name via {@link #ident} (correct for
     * case-insensitive engines). A case-sensitive dialect dispatches
     * on the reference's ORIGIN: a DERIVED name (the query invented
     * it) quotes like its alias definition; a PHYSICAL name spells as
     * the DDL spelled it; an origin-less reference WALLS rather than
     * guess. */
    protected String columnName(SqlExpr.Column c) {
        return c.origin() == com.legend.sql.OutputCol.Origin.PHYSICAL_QUOTED
                ? delimited(c.name())
                : ident(c.name());
    }

    /** {@code name} as a DELIMITED identifier: always quoted, the quote
     *  character doubled inside ({@code a"b} spells {@code "a""b"}). */
    protected String delimited(String name) {
        String q = String.valueOf(quoteChar());
        return q + name.replace(q, q + q) + q;
    }

    /** EXCEPT/EXCLUDE-list name spelling — DIALECT-owned: the base
     * keeps the UNCONDITIONAL quote (DuckDB's EXCLUDE form, corpus-
     * pinned); the H2 dialect spells via {@link #ident} so the names
     * match every other reference in the same statement on a
     * case-sensitive session (PCT witness: EXCEPT ("country") vs bare
     * _tds0.country in one SELECT). */
    protected String starExceptName(String name) {
        return delimited(name);
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

    // ---- DDL (2026-09-16): the store's declared shape, rendered here like
    // a query. ONE identifier rule per dialect (the query rule, ident()),
    // ONE type spelling (ddlType); the engine-text renderer overrides the
    // deltas its goldens pin. Keys and nullability are the STORE's
    // declarations and always render.

    @Override
    public String render(com.legend.sql.SqlDdl ddl) {
        return switch (ddl) {
            case com.legend.sql.SqlDdl.CreateTable ct -> {
                // a TEMPORARY table: the one ANSI spelling every target here accepts
                // (H2: CREATE [LOCAL] TEMPORARY TABLE; DuckDB: CREATE TEMPORARY TABLE)
                StringBuilder sb = new StringBuilder(ct.temporary() ? "Create Temporary Table " : "Create Table ")
                        .append(ddlQualified(ct.schema(), ct.table(), ct.quoting())).append("(");
                boolean first = true;
                for (com.legend.sql.SqlDdl.Column col : ct.columns()) {
                    if (!first) {
                        sb.append(ddlColumnSeparator());
                    }
                    first = false;
                    sb.append(ddlIdentifier(col.name(), col.declaredQuoted()))
                            .append(' ').append(ddlType(col.type()))
                            .append(ddlNullability(col.primaryKey() || col.notNull()));
                }
                java.util.List<String> pks = ct.columns().stream()
                        .filter(com.legend.sql.SqlDdl.Column::primaryKey)
                        .map(col -> ddlKeyIdentifier(col.name(), col.declaredQuoted()))
                        .toList();
                if (!pks.isEmpty()) {
                    // the engine joins the pk names with a bare comma
                    // (translateCreateTableStatementDefault)
                    sb.append(", PRIMARY KEY(").append(String.join(",", pks)).append(')');
                }
                yield sb.append(");").toString();
            }
            case com.legend.sql.SqlDdl.DropTable dt ->
                    "Drop table if exists " + ddlQualified(dt.schema(), dt.table(), dt.quoting()) + ";";
            case com.legend.sql.SqlDdl.CreateSchema cs ->
                    "Create Schema if not exists " + storedName(cs.schema(), cs.quoted()) + ";";
            case com.legend.sql.SqlDdl.DropSchema ds ->
                    "Drop schema if exists " + storedName(ds.schema(), ds.quoted()) + " cascade;";
        };
    }

    @Override
    public String render(com.legend.sql.SqlDml dml) {
        return switch (dml) {
            case com.legend.sql.SqlDml.InsertValues iv -> {
                SqlWriter writer = newWriter().append("INSERT INTO ").append(ddlQualified(iv.schema(), iv.table(), iv.quoting()))
                        .append(dmlColumns(iv.columns(), iv.quotedColumns())).append(" VALUES ");
                for (int r = 0; r < iv.rows().size(); r++) {
                    writer.append(r == 0 ? "(" : ", (").list(iv.rows().get(r)).append(")");
                }
                RenderedStatement insert = writer.append(";").statement();
                if (!insert.binds().isEmpty()) {
                    throw new DialectCapability("plan parameters " + insert.binds() + " in a DML row: a row holds"
                            + " values, and a DML statement is text");
                }
                yield insert.sql();
            }
            case com.legend.sql.SqlDml.InsertFromTable it -> "INSERT INTO "
                    + ddlQualified(it.schema(), it.table(), it.quoting()) + dmlColumns(it.columns(), it.quotedColumns())
                    + " SELECT * FROM " + ident(it.source()) + ";";
            case com.legend.sql.SqlDml.DeleteAll da ->
                    "DELETE FROM " + ddlQualified(da.schema(), da.table(), da.quoting()) + ";";
            case com.legend.sql.SqlDml.CreateTempTableAs ct -> tempTableAs(ct).text();
            case com.legend.sql.SqlDml.InsertSelect is -> insertSelect(is).text();
            case com.legend.sql.SqlDml.Update u -> update(u).text();
            case com.legend.sql.SqlDml.DeleteWhere dw -> deleteWhere(dw).text();
            case com.legend.sql.SqlDml.Merge m -> merge(m).text();
        };
    }

    @Override
    public RenderedStatement renderStatement(com.legend.sql.SqlDml dml) {
        return switch (dml) {
            // a seed's rows, a copy, a table emptied: setup's statements, which bind no value (their text, as this
            // dialect renders it)
            case com.legend.sql.SqlDml.InsertValues iv -> new RenderedStatement(render(iv), List.of());
            case com.legend.sql.SqlDml.InsertFromTable it -> new RenderedStatement(render(it), List.of());
            case com.legend.sql.SqlDml.DeleteAll da -> new RenderedStatement(render(da), List.of());
            case com.legend.sql.SqlDml.CreateTempTableAs ct -> tempTableAs(ct).statement();
            case com.legend.sql.SqlDml.InsertSelect is -> insertSelect(is).statement();
            case com.legend.sql.SqlDml.Update u -> update(u).statement();
            case com.legend.sql.SqlDml.DeleteWhere dw -> deleteWhere(dw).statement();
            case com.legend.sql.SqlDml.Merge m -> merge(m).statement();
        };
    }

    // ------------------------------------------------------------------ the store writes (docs/WRITES_DESIGN_2026_10_18.md §4)

    /** A query's rows into a table: refused unless the dialect's database is measured to run
     *  {@link #standardInsertSelect}, as every spelling is its dialect's (D1). Measured on DuckDB, H2 and Postgres 16,
     *  2026-10-18 (the writes' leg 2); the others refuse by name until each is. */
    protected SqlWriter insertSelect(com.legend.sql.SqlDml.InsertSelect is) {
        throw unmeasuredWrite("an INSERT of a query's rows");
    }

    /** An UPDATE of a table's rows: refused unless measured ({@link #standardUpdate}). */
    protected SqlWriter update(com.legend.sql.SqlDml.Update u) {
        throw unmeasuredWrite("an UPDATE");
    }

    /** A DELETE of the rows a condition holds for: refused unless measured ({@link #standardDeleteWhere}). */
    protected SqlWriter deleteWhere(com.legend.sql.SqlDml.DeleteWhere dw) {
        throw unmeasuredWrite("a DELETE ... WHERE");
    }

    /** An upsert by key: refused unless measured ({@link #standardMerge}). */
    protected SqlWriter merge(com.legend.sql.SqlDml.Merge m) {
        throw unmeasuredWrite("a MERGE");
    }

    private DialectCapability unmeasuredWrite(String what) {
        return new DialectCapability(what + " on " + getClass().getSimpleName() + ": no spelling measured on its"
                + " database (the store writes run on DuckDB, H2 and Postgres, the writes design's leg 2)");
    }

    /** The standard {@code INSERT INTO t (columns) query}, the query written as this dialect writes one, its passes
     *  applied. */
    protected final SqlWriter standardInsertSelect(com.legend.sql.SqlDml.InsertSelect is) {
        SqlWriter writer = newWriter().append("INSERT INTO ").append(ddlQualified(is.schema(), is.table(), is.quoting()))
                .append(dmlColumns(is.columns(), is.quotedColumns())).append(" ");
        return query(writer, passed(is.query()), 0).append(";");
    }

    /** The standard {@code UPDATE t AS alias SET c = value, ... [WHERE condition]} (an assigned column unqualified,
     *  as every measured database requires; the values and the condition read the row through the alias). */
    protected final SqlWriter standardUpdate(com.legend.sql.SqlDml.Update u) {
        SqlSelect read = storedReadsOver(u.table(), u.alias(), u.storedTypes(),
                u.assignments().stream().map(com.legend.sql.SqlDml.Update.Assignment::value).toList(), u.where());
        SqlWriter writer = newWriter().append("UPDATE ").append(ddlQualified(u.schema(), u.table(), u.quoting()))
                .append(" AS ").append(aliasIdent(u.alias())).append(" SET ");
        for (int i = 0; i < u.assignments().size(); i++) {
            com.legend.sql.SqlDml.Update.Assignment a = u.assignments().get(i);
            writer.append(i == 0 ? "" : ", ").append(storedName(a.column(), a.quoted())).append(" = ")
                    .expr(read.projections().get(i).expr(), 0);
        }
        if (read.where() != null) {
            writer.append(" WHERE ").expr(read.where(), 0);
        }
        return writer.append(";");
    }

    /** The standard {@code DELETE FROM t AS alias [WHERE condition]}. */
    protected final SqlWriter standardDeleteWhere(com.legend.sql.SqlDml.DeleteWhere dw) {
        SqlSelect read = storedReadsOver(dw.table(), dw.alias(), dw.storedTypes(), List.of(), dw.where());
        SqlWriter writer = newWriter().append("DELETE FROM ").append(ddlQualified(dw.schema(), dw.table(), dw.quoting()))
                .append(" AS ").append(aliasIdent(dw.alias()));
        if (read.where() != null) {
            writer.append(" WHERE ").expr(read.where(), 0);
        }
        return writer.append(";");
    }

    /** An UPDATE's or a DELETE's expressions in the scope of their one table, read as a query reads it: the
     *  {@link StoredReads} pass over {@code SELECT values FROM table AS alias WHERE where} (a column this dialect reads
     *  other than as held is read so at every reference, as in a query; the other passes rewrite a query's shape and
     *  have nothing to rewrite in a column's value or a row's condition), its projections and condition read back in
     *  order. */
    private SqlSelect storedReadsOver(String table, String alias,
            java.util.Map<String, com.legend.sql.SqlDdl.ColumnType> storedTypes, List<SqlExpr> values,
            @com.legend.base.Nullable SqlExpr where) {
        List<SqlSelect.Projection> ps = new java.util.ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            ps.add(new SqlSelect.Projection(values.get(i), "__v" + i, null));
        }
        if (ps.isEmpty()) {
            ps.add(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__v", null));
        }
        SqlSelect scope = SqlSelect.starOf(new com.legend.sql.SqlSource.Table(table, alias, List.of(), false, storedTypes,
                com.legend.sql.NameQuoting.BARE)).withProjections(ps).withWhere(where);
        return (SqlSelect) new StoredReads(this::readsStored).rewriteRoot(scope);
    }

    /** The standard {@code MERGE INTO t AS alias USING (query) AS source ON ... WHEN MATCHED THEN UPDATE SET ... WHEN
     *  NOT MATCHED THEN INSERT (...) VALUES (...)}: each column read from the source as a query reads a derived
     *  table's column ({@link #columnRef} under the query output's origin: a stored column's name folded where its
     *  database folds it), written into the table's column as its store declared it. */
    protected final SqlWriter standardMerge(com.legend.sql.SqlDml.Merge m) {
        String target = aliasIdent(m.alias());
        java.util.function.Function<String, String> read = name -> columnRef(SqlExpr.Column.of(m.sourceAlias(),
                m.query().outputs().stream().filter(o -> o.name().equals(name)).findFirst()
                        .orElseThrow(() -> new IllegalStateException("a MERGE's source has no column '" + name + "' "
                                + m.query().outputs()))));
        SqlWriter writer = newWriter().append("MERGE INTO ").append(ddlQualified(m.schema(), m.table(), m.quoting()))
                .append(" AS ").append(target).append(" USING (");
        query(writer, passed(m.query()), 0).append(") AS ").append(aliasIdent(m.sourceAlias())).append(" ON ");
        for (int i = 0; i < m.keys().size(); i++) {
            String k = m.keys().get(i);
            writer.append(i == 0 ? "" : " AND ").append(target).append(".").append(storedName(k, m.quotedColumns()
                    .contains(k))).append(" = ").append(read.apply(k));
        }
        java.util.List<String> others = m.columns().stream().filter(c -> !m.keys().contains(c)).toList();
        if (!others.isEmpty()) {
            writer.append(" WHEN MATCHED THEN UPDATE SET ");
            for (int i = 0; i < others.size(); i++) {
                String c = others.get(i);
                writer.append(i == 0 ? "" : ", ").append(storedName(c, m.quotedColumns().contains(c))).append(" = ")
                        .append(read.apply(c));
            }
        }
        writer.append(" WHEN NOT MATCHED THEN INSERT").append(dmlColumns(m.columns(), m.quotedColumns()))
                .append(" VALUES (");
        for (int i = 0; i < m.columns().size(); i++) {
            writer.append(i == 0 ? "" : ", ").append(read.apply(m.columns().get(i)));
        }
        return writer.append(");");
    }

    /** A temporary table holding a query's rows (the test-data generator's): refused unless the dialect's database is
     *  measured to run {@link #createTemporaryTableAs}, as every spelling is its dialect's (D1). Until the PR audit of
     *  2026-10-16 every dialect inherited the standard spelling. */
    protected SqlWriter tempTableAs(com.legend.sql.SqlDml.CreateTempTableAs ct) {
        throw new DialectCapability("a temporary table from a query (CREATE TEMPORARY TABLE ... AS) on "
                + getClass().getSimpleName() + ": no spelling measured on its database (the test-data generator's"
                + " sessions are H2, DuckDB and SQLite)");
    }

    /** The standard {@code CREATE TEMPORARY TABLE t AS <query>}, for a dialect whose database runs it: the query
     *  written as this dialect writes a query, its passes applied, each plan parameter bound where it is written. */
    protected final SqlWriter createTemporaryTableAs(com.legend.sql.SqlDml.CreateTempTableAs ct) {
        return createTemporaryTableAs(ct, false);
    }

    /** {@link #createTemporaryTableAs(com.legend.sql.SqlDml.CreateTempTableAs)}; {@code named}: its columns named
     *  ({@code CREATE TEMPORARY TABLE t ("a", ...) AS <query>}), each delimited, when the statement names them. */
    protected final SqlWriter createTemporaryTableAs(com.legend.sql.SqlDml.CreateTempTableAs ct, boolean named) {
        return createTemporaryTableAs(ct, named, "CREATE TEMPORARY TABLE ", "");
    }

    /** {@link #createTemporaryTableAs(com.legend.sql.SqlDml.CreateTempTableAs, boolean)}, the statement opened by
     *  {@code create} and {@code options} written before its {@code AS}. The table's name is spelled as a query
     *  reads a bare name ({@link #tableName}), so the statement that makes it, the queries that read it and its
     *  drop name one table (PR #67's review, 2026-10-22: Postgres made {@code "legend_stage_rowsA"} and read
     *  {@code legend_stage_rowsa}). */
    protected final SqlWriter createTemporaryTableAs(com.legend.sql.SqlDml.CreateTempTableAs ct, boolean named,
            String create, String options) {
        SqlWriter head = newWriter().append(create).append(tableName(ct.table(), com.legend.sql.NameQuoting.BARE));
        if (named && !ct.columns().isEmpty()) {
            head.append(" (").append(String.join(", ", ct.columns().stream().map(this::delimited).toList()))
                    .append(")");
        }
        return query(head.append(options).append(" AS "), passed(ct.query()), 0).append(";");
    }

    /** {@code " (a, b)"}, each column a {@link #storedName} (as the queries reference it), or empty for every
     *  column. */
    private String dmlColumns(java.util.List<String> columns, java.util.Set<String> quoted) {
        return columns.isEmpty() ? "" : " (" + String.join(", ",
                columns.stream().map(c -> storedName(c, quoted.contains(c))).toList()) + ")";
    }

    /** {@code schema.table}, each name spelled as a query references it ({@link #storedName}: a declared-quoted one
     *  delimited, its case kept; a bare one by {@link #physicalName}, a reserved or unusual name quoted), so the
     *  statement that creates or fills a table and the query that reads it name one table; the default schema spells
     *  bare. */
    private String ddlQualified(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting) {
        return schema == null || schema.isEmpty() || "default".equals(schema)
                ? storedName(table, quoting.table())
                : storedName(schema, quoting.schema()) + "." + storedName(table, quoting.table());
    }

    /** {@inheritDoc} A declared-quoted name delimited by this dialect's quote character, its case kept; a bare one by
     *  {@link #physicalName}. */
    @Override
    public String storedName(String name, boolean declaredQuoted) {
        return declaredQuoted ? delimited(name) : physicalName(name);
    }

    /** A store column's declared type as this database spells it in DDL: every dialect's whole table (2026-10-15, the
     *  dialect program's owner guard: until then H2's spelling was the base, and each dialect overrode a few). */
    protected abstract String ddlType(com.legend.sql.SqlDdl.ColumnType t);

    /** A column identifier in DDL: the dialect's ONE identifier rule
     *  ({@link #ident}); a declared-quoted name is delimited by this dialect's quote character, its case kept (a
     *  {@code "} on every database wrote a string, not a name, on Databricks: the PR #39 audit, 2026-10-14). */
    protected String ddlIdentifier(String name, boolean declaredQuoted) {
        return declaredQuoted ? delimited(name) : ident(name);
    }

    /** {@inheritDoc} The DDL's column rule ({@link #ddlIdentifier}). */
    @Override
    public String insertColumnName(String name, boolean declaredQuoted) {
        return ddlIdentifier(name, declaredQuoted);
    }

    /** A key-list identifier ({@code PRIMARY KEY(...)}): the column rule. */
    protected String ddlKeyIdentifier(String name, boolean declaredQuoted) {
        return ddlIdentifier(name, declaredQuoted);
    }

    protected String ddlColumnSeparator() {
        return ", ";
    }

    /** A DDL column's nullability: {@code NOT NULL}, or the engine's explicit {@code NULL}. */
    protected String ddlNullability(boolean notNull) {
        return notNull ? " NOT NULL" : " NULL";
    }

    @Override
    public String physicalName(String name) {
        return ident(name);
    }

    protected String ident(String name) {
        if (PLAIN.matcher(name).matches() && !reservedWords().contains(name.toLowerCase(java.util.Locale.ROOT))) {
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

    protected SqlWriter nl(SqlWriter writer, int depth) {
        return inlineMode ? writer.append(" ")
                : writer.append("\n").append("  ".repeat(depth));
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

    /** NUMERIC CHARTER Rule 1: a Float literal's bare spelling — the pure
     * Float's own text (a point always present, never E-notation), so the
     * database types it DECIMAL, never INTEGER (1e18 spells
     * {@code 1000000000000000000.0}) and never DOUBLE. */
    protected String floatLiteral(double v) {
        return plainFloat(v);
    }

    static String plainFloat(double v) {
        // the numeric charter's Rule 1, one owner (SqlTyping.floatDecimal; the runner binds a Float by it): plain
        // digits, or an extreme magnitude in exponent form
        java.math.BigDecimal plain = com.legend.sql.SqlTyping.floatDecimal(v);
        return plain != null ? plain.toPlainString() : Double.toString(v);
    }
}
