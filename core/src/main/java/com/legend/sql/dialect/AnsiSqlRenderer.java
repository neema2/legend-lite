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

    /** The query after this dialect's passes, written. */
    private SqlWriter write(SqlQuery query) {
        SqlQuery q = query;
        for (com.legend.sql.SqlRewriter pass : renderPasses()) {
            q = pass.rewriteRoot(q);
        }
        SqlWriter writer = newWriter();
        return query(writer, q, 0);
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

    /** What {@code render()} runs: this dialect's {@link #passes()}, then the STORED READS LAST
     *  (docs/STORE_TYPES_HOMEWORK_2026_10_02.md, 4.3) -- every source the passes introduced is in
     *  scope, and every reference they built reads what the store holds. One owner, so no
     *  dialect's pass list can leave the reads out. */
    protected final java.util.List<com.legend.sql.SqlRewriter> renderPasses() {
        java.util.List<com.legend.sql.SqlRewriter> ps = new java.util.ArrayList<>(passes());
        ps.add(new StoredReads(this::readsStored));
        return java.util.List.copyOf(ps);
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

    /** A type Pure cannot name: a Pure String, read as text on every dialect. */
    protected static boolean readsAsText(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case OTHER, DISTINCT -> true;
                case BIGINT, SMALLINT, TINYINT, INTEGER, FLOAT, DOUBLE, REAL, BIT, TIMESTAMP, DATE,
                        JSON, VARCHAR, ARRAY, OBJECT -> false;
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
                writer.append("WITH ");
                for (int i = 0; i < w.ctes().size(); i++) {
                    if (i > 0) {
                        writer.append(", ");
                    }
                    writer.append(w.ctes().get(i).name()).append(cteAs(w.ctes().get(i)));
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
            writer.join(s.projections(), ", ", this::projection);
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
        if (p.expr() instanceof SqlExpr.NullLit && p.out() != null && typedNullSlot(p.out().type())) {
            // THE SLOT IS THE WIRE: a NULL projected under a typed slot
            // spells its type — a bare NULL is typed by its use when the
            // select is inlined and INTEGER by default when it is
            // materialized (leg 3.4 step 2: a frame CTE's NULL column)
            writer.append("CAST(NULL AS ").append(castTypeName(p.out().type())).append(")");
        } else {
            writer.expr(p.expr(), 0);
        }
        if (p.alias() != null) {
            return writer.append(" AS ").append(aliasIdent(p.alias()));
        }
        String label = implicitLabel(p);
        return label == null ? writer : writer.append(" AS ").append(label);
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
                writer.append(tableName(t.name())).append(t.call() ? "()" : "");
                if (t.alias() != null) {
                    writer.append(" AS ").append(aliasIdent(t.alias()));
                }
                yield writer;
            }
            case SqlSource.Cte c -> writer.append(c.name()).append(" AS ").append(aliasIdent(c.alias()));
            case SqlSource.Subselect sub -> subselectSource(writer, sub, depth);
            // cross-store plan variable: freemarker splice at execution
            // (engine VarSetPlaceHolder — plan text only; a DuckDB
            // execution reaching this dies loudly at SQL parse)
            case SqlSource.VarSetPlaceholder vp -> writer.append("(${")
                    .append(vp.varName()).append("}) as ")
                    .append(aliasIdent(vp.alias()));
            case SqlSource.Values v -> valuesSource(writer, v);
            // corpus-authored raw SQL as a relation source (Phase 1:
            // the typed executeInDb grid) — carried text, parenthesized
            case SqlSource.RawSql r -> writer.append("(").append(r.sql())
                    .append(") AS ").append(aliasIdent(r.alias()));
            case SqlSource.SourceUrl u -> {
                writer.append("(");
                nl(writer, depth + 1).append(sourceUrl(u.url()));
                yield nl(writer, depth).append(") AS ").append(aliasIdent(u.alias()));
            }
            case SqlSource.Pivot p -> pivotSource(writer, p, depth);
            case SqlSource.Join j -> {
                source(writer, j.left(), depth);
                nl(writer, depth);
                if (j.kind() == SqlSource.Join.Kind.ASOF_LEFT) {
                    writer.append(asOfJoinClause());
                } else {
                    writer.append(j.kind().sql);
                }
                writer.append(" ");
                source(writer, j.right(), depth);
                if (j.on() != null) {
                    writer.append(" ON ").expr(j.on(), 0);
                }
                yield writer;
            }
        };
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
            case SqlExpr.RowOrder r -> writer.append((r.table() == null ? ""
                    : aliasIdent(r.table()) + ".") + rowOrderColumn());
            // the QUALIFIER is structurally always a source ALIAS (the
            // lowerer aliases every FROM source) — it spells with the
            // alias rule; the NAME spells by its ORIGIN (columnName)
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
                    : d.value().toPlainString());
            case SqlExpr.BoolLit b -> writer.append(boolLit(b.value()));
            case SqlExpr.NullLit n -> writer.append("NULL");
            case SqlExpr.DateLit d -> writer.append(dateLit(d.iso()));
            case SqlExpr.TimestampLit t -> writer.append(timestampLit(t.iso()));
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
            case SqlExpr.DeferredTdsString d -> throw new IllegalStateException(
                    "deferred relation-toString reached the renderer — the"
                    + " execution boundary must resolve the dynamic column"
                    + " list first (DeferredTdsString id " + d.id() + ")");
            case SqlExpr.WindowCall w -> windowCall(writer, w);
            case SqlExpr.Lambda l -> lambda(writer, l);
            case SqlExpr.Cast c -> variantAwareCast(writer, c);
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
     * ONE switch over the {@link SqlFn} vocabulary: ANSI-expressible entries
     * render here; idiom entries delegate to the dialect hooks (which THROW
     * in this base). Its last arm throws for an unclassified function, so
     * javac does not check its cases: SpellingsTest.everySqlFnClassified
     * does (every SqlFn a spelling row or a rule here).
     */
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
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
                        (i > 0 && nonCommutative) || nonAssociative
                                ? infix.prec() + 1 : infix.prec());
            }
            if (wrap) {
                writer.append(")");
            }
            return writer;
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
            writer.append(spellings.fnNames().get(SqlFn.ERROR) + "(chr(31) || ");
            if (a.size() > 1) {
                writer.expr(a.get(1), 0).append(" || chr(30) || ");
            }
            return writer.append("(").expr(a.get(0), 0).append(") || chr(31))");
        }
        // PURE spellings are DATA (Spellings row): name(args), nothing else.
        String plain = spellings.fnNames().get(c.fn());
        if (plain != null) {
            return writer.append(plain).append("(").list(a).append(")");
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
            case JSON_MERGE_PATCH -> writer.append("json_merge_patch(").list(a).append(")");
            case CONCAT -> writer.append("concat(").list(flattenConcat(a)).append(")");
            // never flattened into an enclosing concat (see SqlFn)
            case CONCAT_JOIN -> writer.append("concat(").list(a).append(")");
            case NOT -> {
                if (3 < parentPrec) {
                    writer.append("(");
                }
                writer.append("NOT ").expr(a.get(0), 3);
                if (3 < parentPrec) {
                    writer.append(")");
                }
                yield writer;
            }
            case NEGATE -> writer.append("-").expr(a.get(0), 7);
            case HASH -> hashSigned(writer, a);
            case IS_NULL -> writer.expr(a.get(0), 4).append(" IS NULL");
            case IS_NOT_NULL -> writer.expr(a.get(0), 4).append(" IS NOT NULL");
            case IN -> {
                // a plan parameter as the WHOLE list: one array, bound once
                if (a.size() == 2 && a.get(1) instanceof SqlExpr.PlanParam p) {
                    yield anyOf(writer, a.get(0), p, parentPrec);
                }
                yield writer.expr(a.get(0), 4).append(" IN (").list(a.subList(1, a.size())).append(")");
            }
            case IS_DISTINCT_FROM -> writer.append("(").expr(a.get(0), 4).append(" IS DISTINCT FROM ").expr(a.get(1), 4)
                    .append(")");
            // the SEMANTIC null-safe (in)equality nodes (engine
            // nullSafeEqual/nullSafeNotEqual DynaFunctions) — dialects
            // re-spell; execution backends use the native form
            case NULL_SAFE_EQUAL -> writer.append("(").expr(a.get(0), 4).append(" IS NOT DISTINCT FROM ")
                    .expr(a.get(1), 4).append(")");
            case NULL_SAFE_NOT_EQUAL -> writer.append("(").expr(a.get(0), 4).append(" IS DISTINCT FROM ")
                    .expr(a.get(1), 4).append(")");
            // MUST-honor semantics (PHASE_HIJ_LOWERING.md): Pure's
            // divide(Number, Number) IS a Float — the division itself is a
            // DOUBLE division, so both operands cast BEFORE it (a cast
            // after would keep the operands' arithmetic: integers truncate,
            // decimals divide exactly on H2 — 36-digit NUMERIC — and round
            // to a double the engine's double division need not reach;
            // stress corpus 2026-09-16: 936 notionalPerRiskPoint rows).
            // The former `1.0 *` promotion only dodged integer truncation.
            case DIVIDE -> writer.append("(CAST(").expr(a.get(0), 0).append(" AS DOUBLE) / CAST(").expr(a.get(1), 0)
                    .append(" AS DOUBLE))");
            case MOD -> writer.append("MOD(MOD(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(") + ")
                    .expr(a.get(1), 0).append(", ").expr(a.get(1), 0).append(")");
            case REM -> writer.append("MOD(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
            // Math — ANSI/portable spellings; ROUND is banker's (dialect maps).
            case PI -> writer.append("pi()");
            case CEILING -> writer.append("CAST(ceil(").expr(a.get(0), 0).append(") AS BIGINT)");
            case FLOOR -> writer.append("CAST(floor(").expr(a.get(0), 0).append(") AS BIGINT)");
            case ROUND -> roundHalfEven(writer, a);
            // Pure's divide-with-scale is BigDecimal HALF_UP — plain SQL
            // ROUND (half away from zero) says exactly that.
            case ROUND_HALF_UP -> writer.append("ROUND(").list(a).append(")");
            // Runtime assertion: raises with the message when evaluated
            // (guards that must fail LOUD, never clamp).
            // floor WITHOUT the BIGINT cast (FLOOR casts — overflows at
            // 1e18): fraction-free tests over the full double range.
            case SIGN -> writer.append("CAST(sign(").expr(a.get(0), 0).append(") AS BIGINT)");
            case XOR -> // the OR-chain misbinds under an enclosing AND — the WALK
                    // wraps it (op), never this arm by hand; each operand is WRITTEN twice
                    op(writer, parentPrec, () -> writer.append("(").expr(a.get(0), 3).append(" AND NOT ")
                            .expr(a.get(1), 3).append(") OR (NOT ").expr(a.get(0), 3).append(" AND ").expr(a.get(1), 3)
                            .append(")"));
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(writer, c.fn(), a);
            // Strings
            // MATCHES is the PARTIAL regexp test (regexpLike's SQL
            // semantics); pure matches() is REGEXP_FULL_MATCH (the engine
            // anchors ^...$).
            case MAP_EMPTY -> writer.append("MAP {}");
            // ~x without negation overflow at MIN_LONG
            case BIT_NOT -> writer.append("xor(").expr(a.get(0), 0).append(", -1)");
            // the PAD CHAR is optional in Pure; SQL requires it — ' '.
            case LPAD -> writer.append("lpad(").list(a.size() == 2
                    ? List.of(a.get(0), a.get(1), new SqlExpr.StringLit(" ")) : a).append(")");
            case RPAD -> writer.append("rpad(").list(a.size() == 2
                    ? List.of(a.get(0), a.get(1), new SqlExpr.StringLit(" ")) : a).append(")");
            // the || concat misbinds under +/comparison — walk-wrapped
            case UC_FIRST -> op(writer, parentPrec, () -> writer.append("upper(substr(").expr(a.get(0), 0)
                    .append(", 1, 1)) || substr(").expr(a.get(0), 0).append(", 2)"));
            case LC_FIRST -> op(writer, parentPrec, () -> writer.append("lower(substr(").expr(a.get(0), 0)
                    .append(", 1, 1)) || substr(").expr(a.get(0), 0).append(", 2)"));
            case ENCODE_BASE64 -> writer.append("to_base64(CAST(").expr(a.get(0), 0).append(" AS BLOB))");
            // pure generateGuid : String[1] — the CONTRACT is text, so
            // the emission conforms (bare uuid() wires UUID; §4bZ-V C
            // adjudication: fix-emitter, the CEILING pattern)
            case GUID -> writer.append("CAST(uuid() AS VARCHAR)");
            // Temporal
            case TODAY -> writer.append("current_date");
            case NOW -> writer.append("now()");
            case DATE_TRUNC_DAY -> writer.append("CAST(").expr(a.get(0), 0).append(" AS DATE)");
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
                    ? writer.append("CAST(").function("date_trunc", a).append(" AS DATE)")
                    : writer.function("date_trunc", a);
            // make_timestamp wants DOUBLE seconds.
            case MAKE_TIMESTAMP -> a.size() == 6
                    ? writer.append("make_timestamp(").list(a.subList(0, 5)).append(", CAST(").expr(a.get(5), 0)
                            .append(" AS DOUBLE))")
                    : writer.function("make_timestamp", a);           // (part, value)
            // (unitFn literal, amount, date) — the unit FUNCTION NAME rides
            // as a string literal and renders bare: d + to_years(n).
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> op(writer, parentPrec, () -> writer.expr(a.get(2), 5)
                    .append(" + ").append(((SqlExpr.StringLit) a.get(0)).value()).append("(").expr(a.get(1), 0)
                    .append(")"));
            // Week buckets align to the Monday ON/BEFORE the epoch
            // (1969-12-29 — real pure's origin, PCT-pinned); every other
            // unit aligns to the 1970 epoch.
            case TIME_BUCKET -> {
                writer.append("time_bucket(").append(((SqlExpr.StringLit) a.get(0)).value()).append("(")
                        .expr(a.get(1), 0).append("), ").expr(a.get(2), 0);
                writer.append(("to_weeks".equals(((SqlExpr.StringLit) a.get(0)).value())
                            ? ", TIMESTAMP '1969-12-29 00:00:00'"
                            : ", TIMESTAMP '1970-01-01 00:00:00'"));
                yield writer.append(")");
            }
            case FROM_EPOCH_MS -> writer.append("epoch_ms(CAST(").expr(a.get(0), 0).append(" AS BIGINT))");
            case INT_DIVIDE -> writer.append("(").expr(a.get(0), 6).append(" // ").expr(a.get(1), 6).append(")");
            // decode(blob) — a CAST of the blob to VARCHAR ESCAPES quotes and
            // non-printables (\x22), never the text itself (batch 72b)
            case DECODE_BASE64 -> writer.append("decode(from_base64(").expr(a.get(0), 0).append("))");
            case CURRENT_USER_FN -> writer.append("current_user");
            // Lists (dialect-owned; base throws like the lambda family)
            case LIST_ZIP, LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX,
                 LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT,
                 LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN, REPEAT_VALUE,
                 LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, LIST_BOOL_AND, LIST_BOOL_OR,
                 LIST_REVERSE, TYPEOF -> listCall(writer, c.fn(), a);
            case TO_VARIANT -> variantConstruct(writer, a);
            // boolean text: the reference cast spelling (semantic node —
            // dialects with a diverging bool print override)
            case BOOL_TO_TEXT -> writer.append("CAST(").expr(a.get(0), 0).append(" AS VARCHAR)");
            // Idiom points — no ANSI spelling; the dialect decides or dies.
            case UNNEST -> unnestProjection(writer, a);
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET,
                 LIST_POSITION -> listCall(writer, c.fn(), a);
            case STRUCT_INSERT -> structInsert(writer, a);
            case PURE_SPLIT_PART -> splitPartCall(writer, a);
            case LIST_EXISTS -> listExists(writer, a);
            case ALL_DISTINCT -> allDistinct(writer, a);
            case LIST_FOR_ALL -> listForAll(writer, a);
            // 64-bit parse (PCT Long.MIN/MAX round-trips)
            case PARSE_INT -> writer.append("CAST(").expr(a.get(0), 0).append(" AS BIGINT)");
            // parseDate(text): the ISO text as a timestamp (the semantic
            // node; the engine-style H2 spells its parsedatetime idiom)
            case PARSE_DATE -> writer.append("CAST(").expr(a.get(0), 0).append(" AS TIMESTAMP)");
            case VARIANT_ELEMENTS -> variantElements(writer, a);
            case VARIANT_GET -> variantGet(writer, a);
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

    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        writer.append(aggregateName(r.fn())).append("(").append(r.distinct() ? keyword("DISTINCT") + " " : "");
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

    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        throw new DialectCapability("an array literal reached a dialect without array support");
    }

    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit s) {
        throw new DialectCapability("a struct literal reached a dialect without struct support");
    }

    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
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

    /**
     * A plan parameter's placeholder: the value bound bare, typed by the database from the bound value (DuckDB and
     * Postgres do, every type answering as its literal does: docs/execution-plan-boundary-2026-10-05/probes/
     * literal-results.txt). A dialect whose database types a parameter otherwise writes it typed.
     */
    protected SqlWriter placeholder(SqlWriter writer, SqlExpr.PlanParam p) {
        return writer.bind(scalarBind(p));
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
        return new RenderedStatement.Bind(p.name(), null);
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
        writer.expr(needle, 5).append(" = ANY(").bind(new RenderedStatement.Bind(list.name(), element.name()))
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
    protected String tableName(String name) {
        int dot = name.indexOf('.');
        if (dot <= 0) {
            return ident(name);
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
                        .append(ddlQualified(ct.schema(), ct.table())).append("(");
                boolean first = true;
                for (com.legend.sql.SqlDdl.Column col : ct.columns()) {
                    if (!first) {
                        sb.append(ddlColumnSeparator());
                    }
                    first = false;
                    sb.append(ddlIdentifier(col.name(), col.declaredQuoted()))
                            .append(' ').append(ddlType(col.type()))
                            .append(col.primaryKey() || col.notNull() ? " NOT NULL" : " NULL");
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
                    "Drop table if exists " + ddlQualified(dt.schema(), dt.table()) + ";";
            case com.legend.sql.SqlDdl.CreateSchema cs ->
                    "Create Schema if not exists " + physicalName(cs.schema()) + ";";
            case com.legend.sql.SqlDdl.DropSchema ds ->
                    "Drop schema if exists " + physicalName(ds.schema()) + " cascade;";
        };
    }

    @Override
    public String render(com.legend.sql.SqlDml dml) {
        return switch (dml) {
            case com.legend.sql.SqlDml.InsertValues iv -> {
                SqlWriter writer = newWriter().append("INSERT INTO ").append(ddlQualified(iv.schema(), iv.table()))
                        .append(dmlColumns(iv.columns())).append(" VALUES ");
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
                    + ddlQualified(it.schema(), it.table()) + dmlColumns(it.columns())
                    + " SELECT * FROM " + ident(it.source()) + ";";
            case com.legend.sql.SqlDml.DeleteAll da ->
                    "DELETE FROM " + ddlQualified(da.schema(), da.table()) + ";";
        };
    }

    /** {@code " (a, b)"} by the identifier rule, or empty for every column. */
    private String dmlColumns(java.util.List<String> columns) {
        return columns.isEmpty() ? "" : " (" + String.join(", ",
                columns.stream().map(this::ident).toList()) + ")";
    }

    /** {@code schema.table}, each name spelled as a query references it ({@link #physicalName}: a reserved or
     *  unusual name quoted), so the statement that creates or fills a table and the query that reads it name one
     *  table; the default schema spells bare. */
    private String ddlQualified(@com.legend.base.Nullable String schema, String table) {
        return schema == null || schema.isEmpty() || "default".equals(schema)
                ? physicalName(table) : physicalName(schema) + "." + physicalName(table);
    }

    /** A store column's declared type, spelled for this target (the H2
     *  base; a dialect overrides the few it spells otherwise). */
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        return DdlSpelling.h2Type(t);
    }

    /** A column identifier in DDL: the dialect's ONE identifier rule
     *  ({@link #ident}); a declared-quoted name keeps its quotes. */
    protected String ddlIdentifier(String name, boolean declaredQuoted) {
        return declaredQuoted ? '"' + name + '"' : ident(name);
    }

    /** A key-list identifier ({@code PRIMARY KEY(...)}): the column rule. */
    protected String ddlKeyIdentifier(String name, boolean declaredQuoted) {
        return ddlIdentifier(name, declaredQuoted);
    }

    protected String ddlColumnSeparator() {
        return ", ";
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
        double a = Math.abs(v);
        if (a >= 1e15 || (a > 0 && a < 1e-6)) {
            // an extreme magnitude in exponent form — Double.MAX_VALUE spelled
            // plain is 309 digits (the 2-ULP leniency's finite check)
            return Double.toString(v);
        }
        String s = java.math.BigDecimal.valueOf(v).toPlainString();
        return s.contains(".") ? s : s + ".0";
    }
}
