// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DateFmt;
import com.legend.sql.DatePart;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;

import static com.legend.sql.dialect.ExplosionMarkers.explodedRead;
import static com.legend.sql.dialect.ExplosionMarkers.isMarker;
import static com.legend.sql.dialect.ExplosionMarkers.markerColumn;
import static com.legend.sql.dialect.ExplosionMarkers.nullList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The MariaDB EXECUTION dialect (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13): a sibling of {@link SQLite}
 * and {@link BigQuery} over {@link AnsiSqlRenderer}. Every spelling here was measured on MariaDB 11.4 (the pinned
 * server, tools/mariadb) through MariaDB Connector/J before it was trusted; the platform's semantics are DuckDB's.
 *
 * <p>What differs from DuckDB, measured:
 * <ul>
 *   <li>the session decides the dialect: {@code ANSI_QUOTES} (a double-quoted name is an identifier, not a string),
 *       {@code PIPES_AS_CONCAT}, {@code NO_BACKSLASH_ESCAPES}, the zone UTC, utf8mb4 with its binary collation (case-
 *       and accent-sensitive comparison and LIKE, as DuckDB's). The JSON functions answer that collation, so a NO PAD
 *       one would mix with theirs ("Illegal mix of collations", measured): it is PAD SPACE, ' ' equal to '', and an
 *       empty text is tested by its length;</li>
 *   <li>no array, struct or map type: a list, a struct, a map and a Variant are JSON text; a list operation is a
 *       correlated subquery over {@code JSON_TABLE(xs, '$[*]' ...)} (a table function's argument correlates, measured;
 *       an outer reference inside a derived table or a recursive CTE does not, "Unknown table", so a fold, a reduction
 *       and an edit distance are a recursive CTE only over values that read no row ({@link #closed});</li>
 *   <li>no LATERAL, FULL OUTER JOIN, QUALIFY, NULLS FIRST/LAST (NULL sorts first ascending), FILTER clause, typed
 *       NULL-safe IS DISTINCT FROM ({@code <=>} instead) or VALUES alias list;</li>
 *   <li>a boolean is 1 or 0; a Float literal is a DOUBLE only with an exponent ({@code 1.5} is a DECIMAL); a DECIMAL
 *       is exact (65 digits, scale 38);</li>
 *   <li>a SELECT raises no error of its own text: SIGNAL runs only in a stored program, a conversion in a SELECT is a
 *       warning, and an XPath error is raised when the statement is prepared; a raise keeps its message in a session
 *       variable and fails at run time ({@link #raise});</li>
 *   <li>regular expressions are PCRE ({@code REGEXP}, {@code REGEXP_SUBSTR}, {@code REGEXP_REPLACE}, which replaces
 *       every match); base64, MD5, SHA-1 and SHA-2 are native.</li>
 * </ul>
 *
 * <p>Every {@link SqlFn} and {@link SqlAgg.Fn} is decided HERE by an exhaustive switch: its spelling, a shared
 * helper of the base named in its arm, or a {@link DialectCapability} wall.
 */
public class MariaDb extends AnsiSqlRenderer {

    /** MariaDB (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §13): double quotes under the session's
     *  ANSI_QUOTES; MariaDB's reserved words. The {@code MariaDb} dialect quotes EVERY identifier, so the list decides
     *  no spelling there. */
    public static final Lexicon LEXICON = new Lexicon('"', Set.of(
            "accessible", "add", "all", "alter", "analyze", "and", "as", "asc", "asensitive", "before", "between",
            "bigint", "binary", "blob", "both", "by", "call", "cascade", "case", "change", "char", "character", "check",
            "collate", "column", "condition", "constraint", "continue", "convert", "create", "cross", "current_date",
            "current_role", "current_time", "current_timestamp", "current_user", "cursor", "database", "databases",
            "day_hour", "day_microsecond", "day_minute", "day_second", "dec", "decimal", "declare", "default",
            "delayed", "delete", "desc", "describe", "deterministic", "distinct", "distinctrow", "div", "double",
            "drop", "dual", "each", "else", "elseif", "enclosed", "escaped", "except", "exists", "exit", "explain",
            "false", "fetch", "float", "for", "force", "foreign", "from", "fulltext", "general", "grant", "group",
            "having", "high_priority", "if", "ignore", "in", "index", "infile", "inner", "inout", "insensitive",
            "insert", "int", "integer", "intersect", "interval", "into", "is", "iterate", "join", "key", "keys",
            "kill", "leading", "leave", "left", "like", "limit", "lines", "load", "localtime", "localtimestamp",
            "lock", "long", "loop", "match", "minus", "mod", "modifies", "natural", "not", "null", "numeric", "offset",
            "on", "optimize", "option", "or", "order", "out", "outer", "over", "partition", "precision", "primary",
            "procedure", "range", "read", "reads", "real", "recursive", "references", "regexp", "release", "rename",
            "repeat", "replace", "require", "restrict", "return", "returning", "revoke", "right", "rlike", "row_number",
            "rows", "schema", "schemas", "select", "sensitive", "separator", "set", "show", "signal", "smallint",
            "spatial", "specific", "sql", "starting", "straight_join", "table", "terminated", "then", "to",
            "trailing", "trigger", "true", "undo", "union", "unique", "unlock", "unsigned", "update", "usage", "use",
            "using", "values", "varchar", "when", "where", "while", "window", "with", "write", "xor", "zerofill"));

    /** MariaDB (2026-10-11): the portable strategies over its JSON lists (a JSON_TABLE argument correlates,
     *  measured); no FULL OUTER JOIN (measured: a syntax error), so it is emulated, as on H2. */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(false, true, true, false);

    public MariaDb() {
        super(LEXICON);
    }

    /** A dialect of the family ({@link MySql}): MariaDB's spellings where the two agree, its own hooks where not. */
    protected MariaDb(Lexicon lexicon) {
        super(lexicon);
    }

    // ==================================================================
    // The family's hooks: where MySQL spells it otherwise (MySql)
    // ==================================================================

    /** A JSON text as a JSON value, normalized: {@code JSON_COMPACT} (a value read from a column or a table function is
     *  text, which a JSON function would quote, measured). */
    protected SqlWriter.Piece compact(SqlWriter.Piece json) {
        return w -> w.append("JSON_COMPACT(").piece(json).append(")");
    }

    /** {@code JSON_ARRAYAGG([DISTINCT] value [ORDER BY order] [LIMIT 1])}: the values as one JSON array in the order's
     *  order, NULL over no rows; {@code firstOnly} when only its first element is read. */
    protected SqlWriter.Piece arrayAgg(boolean distinct, SqlWriter.Piece value, SqlWriter.@com.legend.base.Nullable Piece order,
            boolean firstOnly) {
        return w -> {
            w.append("JSON_ARRAYAGG(").append(distinct ? "DISTINCT " : "").piece(value);
            if (order != null) {
                w.append(" ORDER BY ").piece(order);
                if (firstOnly) {
                    w.append(" LIMIT 1");
                }
            }
            w.append(")");
        };
    }

    /** {@code '$[' || index || ']'}: a JSON path to a 0-based index (a negative one from the end, measured). */
    protected SqlWriter.Piece indexPath(SqlWriter.Piece index) {
        return w -> w.append("CONCAT('$[', ").piece(index).append(", ']')");
    }

    /** A regular expression replacement's reference to group {@code n}: a backslash and the number (PCRE's). */
    protected String groupRef(int n) {
        return "\\" + n;
    }

    /** The database's name, as an error names it. */
    protected String product() {
        return "MariaDB";
    }

    /** {@code JSON_TABLE(json, spec) AS alias}: a table function over a JSON value, its argument any expression (it
     *  correlates, measured). */
    protected SqlWriter.Piece jsonTable(SqlWriter.Piece json, String spec, String alias) {
        return w -> w.append("JSON_TABLE(").piece(json).append(", " + spec + ") AS " + alias);
    }

    /** A JSON scalar's SQL value at {@code path}, a string unquoted, a JSON null NULL: {@code JSON_VALUE}. */
    protected SqlWriter.Piece jsonText(SqlWriter.Piece json, SqlWriter.Piece path) {
        return w -> w.append("JSON_VALUE(").piece(json).append(", ").piece(path).append(")");
    }

    /** {@link #jsonText(SqlWriter.Piece, SqlWriter.Piece)} of SQL text: a column's JSON at a path's text. */
    protected String jsonText(String json, String path) {
        return "JSON_VALUE(" + json + ", " + path + ")";
    }

    /** A JSON_TABLE column of booleans: BOOLEAN (MySQL's is a BIT there, measured). */
    protected String booleanColumn() {
        return "BOOLEAN";
    }

    /** Whether a JSON value reaches the driver as bytes (MySQL's JSON type does, measured), so a root column of a list,
     *  struct, map or Variant is delivered as its text. */
    protected boolean jsonIsBinary() {
        return false;
    }

    /** Whether a recursive CTE or a derived table may read an outer row (MariaDB's may not: "Unknown column"). */
    protected boolean correlatesDerived() {
        return false;
    }

    // ==================================================================
    // Session and structure
    // ==================================================================

    /** The engine's H2 text is refused here: MariaDB has no translation of it (decision D2: raw text runs on the
     *  database its connection declares; only the engine's H2 text is translated, for H2, DuckDB and Postgres). */
    @Override
    public String fromEngineText(String statement) {
        throw new DialectCapability("the engine's H2 text reached MariaDB, which has no translation of it");
    }

    /** This database's name for each scalar type, a refusal where it has none (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN, INTEGER, BIGINT -> "SIGNED";
            case HUGEINT -> "DECIMAL(38,0)";
            case DOUBLE -> "DOUBLE";
            case VARCHAR, JSON, LITERAL, TEMPORAL_TEXT -> "CHAR";
            case DATE -> "DATE";
            case TIMESTAMP, TIMESTAMPTZ -> "DATETIME(6)";
            case TIME -> "TIME(6)";
            case DECIMAL_TEXT -> 
                    throw new IllegalStateException(s + " cast reached a dialect without " + s + " support");
        };
    }

    /** No STRUCT type here: a struct cast is refused. */
    @Override
    protected String structTypeName(com.legend.sql.SqlType.Struct st) {
        throw new DialectCapability("a STRUCT type reached a dialect without struct support");
    }

    /** A date format's parts: the base's strftime codes, {@code strftimeText} (its own strftime helper spells the
     *  parts it formats itself; the dialect program's Phase 1, 2026-10-13: each dialect declares its date-format
     *  vocabulary). */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        return strftimeText(fl);
    }

    /** The session the dialect's spellings assume (measured: without ANSI_QUOTES {@code "name"} is the string
     *  'name'); recursion and GROUP_CONCAT lengths past their small defaults; and no condition pushed into a derived
     *  table (measured on 11.4.13: a filter on a JSON value of a UNION derived table, pushed into its branches, kept
     *  no row it should). */
    @Override
    public List<String> sessionSetup() {
        return List.of("SET SESSION optimizer_switch = 'condition_pushdown_for_derived=off'","SET SESSION sql_mode = 'ANSI_QUOTES,PIPES_AS_CONCAT,NO_BACKSLASH_ESCAPES,STRICT_ALL_TABLES,"
                + "NO_ENGINE_SUBSTITUTION'", "SET SESSION time_zone = '+00:00'", "SET NAMES utf8mb4 COLLATE utf8mb4_bin",
                "SET SESSION group_concat_max_len = 1073741824", "SET SESSION max_recursive_iterations = 100000000",
                "SET SESSION div_precision_increment = 30");
    }

    /** A Float's literal spelling is this dialect's own, the value read once ({@link #floatLayout}): the portable one
     *  reads the value's text some fifty times, each read a long expression here (2026-10-11). */
    @Override
    protected boolean spellsFloatCanon() {
        return true;
    }

    /** No native pivot: a static pivot is emulated by the carrier strategies, so a dynamic one is discovered first. */
    @Override
    public boolean needsStaticPivot() {
        return true;
    }

    /** The portable carrier strategies, star exclusions spelled out, the substring start clamp, explosions placed as
     *  markers in FROM (a JSON_TABLE), QUALIFY as a wrapping subselect, and a constant GROUP BY or ORDER BY key as an
     *  expression (an integer one is a position). A repeated subexpression is NOT bound once here (CommonSubexpressions,
     *  SQLite's): its binding would be a correlated table function nested per reuse, and MariaDB's planner grew past
     *  11 GB on ten of them (measured, the server killed). */
    @Override
    protected List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CAPS), new StarExceptToColumns(),
                new SubstringClamp(), new ExplosionMarkers.LateralUnnestAsMarker(),
                new ExplosionMarkers.SelectListUnnestAsLateral(), new ExplosionMarkers.StarOverExplosionUnwrapped(),
                new QualifyToSubselect(true), new ConstantKeysAsExpressions());
    }

    @Override
    protected String starExceptKeyword() {
        throw new DialectCapability("a star exclusion reached " + product() + " unexpanded (StarExceptToColumns)");
    }

    @Override
    protected String asOfJoinClause() {
        throw new DialectCapability("an as-of join reached " + product() + ", which has none (the carrier strategies emulate"
                + " the witnessed shapes)");
    }

    @Override
    protected SqlWriter pivotSource(SqlWriter writer, SqlSource.Pivot p, int depth) {
        throw new DialectCapability("a pivot reached " + product() + " unemulated: it has no PIVOT");
    }

    /** Every identifier quoted (ANSI_QUOTES): a column name folds no case, but a keyword would end it. */
    @Override
    protected String ident(String name) {
        if (name.length() > 1 && name.charAt(0) == '"' && name.endsWith("\"")
                && !name.substring(1, name.length() - 1).replace("\"\"", "").contains("\"")) {
            return name;
        }
        return delimited(name);
    }

    /** A UNION branch with an ORDER BY or LIMIT is parenthesized ({@code (SELECT ... LIMIT 1) UNION ALL ...}). */
    @Override
    protected SqlWriter query(SqlWriter writer, SqlQuery q, int depth) {
        if (pendingRoot()) {
            rootSelects.pop();
            rootSelects.push(selectsOf(q));
        }
        if (!(q instanceof SqlUnion u)) {
            return super.query(writer, q, depth);
        }
        String op = u.all() ? "UNION ALL" : "UNION";
        for (int i = 0; i < u.branches().size(); i++) {
            if (i > 0) {
                nl(writer, depth).append(op);
                nl(writer, depth);
            }
            SqlQuery b = u.branches().get(i);
            boolean wrap = !(b instanceof SqlSelect s) || !s.orderBy().isEmpty() || s.limit() != null
                    || s.offset() != null;
            if (wrap) {
                writer.append("(");
            }
            query(writer, b, depth);
            if (wrap) {
                writer.append(")");
            }
        }
        return writer;
    }

    /** The explosion markers in scope of the select being written: alias, then each exploded column's list. */
    private final Deque<Map<String, Map<String, SqlExpr>>> laterals = new ArrayDeque<>();

    /** Root values written as their text ({@link #jsonIsBinary}), by identity. */
    private final java.util.Set<SqlExpr> asText = java.util.Collections.newSetFromMap(new IdentityHashMap<>());

    /** The labels of the statement being rendered, as planned (its root's outputs before this dialect's passes). */
    private final Deque<List<com.legend.sql.OutputCol>> roots = new ArrayDeque<>();

    /** The statement's root selects once its root is written (the root, a union's branches, a WITH's body), or the
     *  empty map while the root is still to come. */
    private final Deque<Map<SqlSelect, Boolean>> rootSelects = new ArrayDeque<>();

    private boolean pendingRoot() {
        Map<SqlSelect, Boolean> top = rootSelects.peek();
        return top != null && top.isEmpty();
    }

    private static Map<SqlSelect, Boolean> selectsOf(SqlQuery q) {
        Map<SqlSelect, Boolean> out = new IdentityHashMap<>();
        collectSelects(q, out);
        if (out.isEmpty()) {
            out.put(new SqlSelect(List.of(), false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null,
                    null, List.of()), Boolean.FALSE);
        }
        return out;
    }

    private static void collectSelects(SqlQuery q, Map<SqlSelect, Boolean> into) {
        switch (q) {
            case SqlSelect s -> into.put(s, Boolean.TRUE);
            case SqlUnion u -> u.branches().forEach(b -> collectSelects(b, into));
            case com.legend.sql.SqlWith w -> collectSelects(w.body(), into);
        }
    }

    @Override
    public String render(SqlQuery query) {
        roots.push(query.outputs());
        rootSelects.push(new IdentityHashMap<>());
        try {
            return super.render(query);
        } finally {
            roots.pop();
            rootSelects.pop();
        }
    }

    @Override
    public RenderedStatement renderStatement(SqlQuery query) {
        roots.push(query.outputs());
        rootSelects.push(new IdentityHashMap<>());
        try {
            return super.renderStatement(query);
        } finally {
            roots.pop();
            rootSelects.pop();
        }
    }

    /**
     * A select that only explodes lists (an explosion marker) is its reads over the marker's positions, in order; the
     * markers of a FROM are in scope of its column reads, and a star over one is spelled out. A root select's values
     * are delivered as their labels say ({@link #delivered}). An OFFSET takes a LIMIT here.
     */
    @Override
    protected SqlWriter select(SqlWriter writer, SqlSelect s, int depth) {
        if (isMarker(s)) {
            String alias = "__u" + depth;
            boolean aligned = s.outputs().size() == s.projections().size();
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (int i = 0; i < s.projections().size(); i++) {
                SqlSelect.Projection p = s.projections().get(i);
                SqlType slot = aligned ? s.outputs().get(i).type() : p.out() != null ? p.out().type() : null;
                ps.add(new SqlSelect.Projection(explodedRead(alias, p, slot), markerColumn(p), p.out()));
            }
            SqlSelect.SortKey position = new SqlSelect.SortKey(SqlExpr.Column.derived(alias, "key"), true, null, null);
            SqlSelect over = new SqlSelect(ps, false, new SqlSource.Subselect(s, alias, null), null, List.of(), null,
                    null, List.of(position), null, null, List.of());
            Map<SqlSelect, Boolean> top = rootSelects.peek();
            if (top != null && top.containsKey(s)) {
                top.put(over, Boolean.TRUE);
            }
            return select(writer, over, depth);
        }
        Map<SqlExpr, List<SqlExpr>> counts = new LinkedHashMap<>();
        SqlSelect counted = modesCounted(s, counts);
        if (counted != s) {
            Map<SqlSelect, Boolean> top = rootSelects.peek();
            if (top != null && top.containsKey(s)) {
                top.put(counted, top.get(s));
            }
            s = counted;
        }
        Map<String, Map<String, SqlExpr>> scope = new LinkedHashMap<>();
        lateralsOf(s.from(), scope);
        SqlSelect spelled = delivered(scope.isEmpty() ? s : starsSpelled(s));
        laterals.push(scope);
        modeCounts.push(counts);
        try {
            return super.select(writer, spelled.offset() != null && spelled.limit() == null
                    ? spelled.withLimit(Long.MAX_VALUE) : spelled, depth);
        } finally {
            modeCounts.pop();
            laterals.pop();
        }
    }

    /** Per select being written, each counted MODE in it and the column counting its value's rows
     *  ({@link #modesCounted}). */
    private final java.util.Deque<Map<SqlExpr, List<SqlExpr>>> modeCounts = new java.util.ArrayDeque<>();

    /** Whether a whole partition's median or quantile is a native window function ({@code PERCENTILE_CONT(q) WITHIN
     *  GROUP (ORDER BY x) OVER (...)}); else it is read from each row's rank in its partition ({@link #modesCounted}). */
    protected boolean windowQuantilesNative() {
        return true;
    }

    /** Whether a GROUPED mode is counted too ({@link #modesCounted}); a windowed one always is. MariaDB reads a group's
     *  values through {@code JSON_TABLE} over their array; MySQL takes no aggregate as a table function's argument
     *  ("Incorrect arguments to JSON_TABLE", measured). */
    protected boolean groupedModesCounted() {
        return false;
    }

    /**
     * {@code s}, its FROM given a count per MODE (2026-10-11, as BigQuery's): a mode is no window function here, and
     * MySQL's grouped one reads its group through no table function. A mode is the most frequent non-NULL value; each
     * one's value is counted over its group's rows (a grouped mode: the select's GROUP BY keys) or its partition's (a
     * window mode), {@code COUNT(v) OVER (PARTITION BY keys, v)}, in a select over the FROM under the FROM's own alias, so
     * every reference of {@code s} still reads it. The mode is then the value of the most counted row, the least on a
     * tie, written where it is ({@link #modeArm}). An ordered or framed window's mode is not counted (its window
     * call refuses it).
     */
    private SqlSelect modesCounted(SqlSelect s, Map<SqlExpr, List<SqlExpr>> counts) {
        Map<SqlExpr, List<SqlExpr>> modes = new LinkedHashMap<>();
        List<SqlExpr> roots = new ArrayList<>();
        s.projections().forEach(p -> roots.add(p.expr()));
        if (s.having() != null) {
            roots.add(s.having());
        }
        if (s.qualify() != null) {
            roots.add(s.qualify());
        }
        for (SqlExpr e : roots) {
            collectModes(e, s.groupBy(), modes);
        }
        if (modes.isEmpty()) {
            return s;
        }
        SqlSource from = s.from();
        String alias = from.alias();
        if (from instanceof SqlSource.Join || alias == null || from.outputs().isEmpty()) {
            throw new DialectCapability("a MODE over a " + from.getClass().getSimpleName() + " reached " + product()
                    + ": its counts are a select over one aliased source");
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (com.legend.sql.OutputCol oc : from.outputs()) {
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, oc), oc.name(), oc));
        }
        int k = 0;
        for (var m : modes.entrySet()) {
            String name = "__mc" + k++;
            SqlAgg.Reducer r = m.getKey() instanceof SqlExpr.WindowCall w ? (SqlAgg.Reducer) w.fn()
                    : (SqlAgg.Reducer) m.getKey();
            if (r.fn() != SqlAgg.Fn.MODE) {
                // a quantile: the row's rank by its value in its partition (NULLs last, the quantile's direction),
                // and the partition's count of values
                SqlExpr v = r.args().get(0);
                boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
                ps.add(new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()),
                        m.getValue(), List.of(new SqlSelect.SortKey(v, !desc, SqlSelect.SortKey.NullOrder.NULLS_LAST,
                                null)), null), name + "r", null));
                ps.add(new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                        List.of(v), false, List.of()), m.getValue(), List.of(), null), name + "n", null));
                counts.put(m.getKey(), List.of(SqlExpr.Column.derived(alias, name + "r"),
                        SqlExpr.Column.derived(alias, name + "n")));
                continue;
            }
            List<SqlExpr> partition = new ArrayList<>(m.getValue());
            partition.add(modeValue(m.getKey()));
            ps.add(new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.Reducer(SqlAgg.Fn.COUNT,
                    List.of(modeValue(m.getKey())), false, List.of()), partition, List.of(), null), name, null));
            counts.put(m.getKey(), List.of(SqlExpr.Column.derived(alias, name)));
        }
        SqlSelect countedRows = new SqlSelect(ps, false, from, null, List.of(), null, null, List.of(), null, null,
                List.of());
        return s.withFrom(new SqlSource.Subselect(countedRows, alias, null));
    }

    /** Each MODE under {@code e} to count: a grouped reducer with the select's keys (where {@link #groupedModesCounted}),
     *  an unordered, unframed window call with its partition. */
    private void collectModes(SqlExpr e, List<SqlExpr> groupBy, Map<SqlExpr, List<SqlExpr>> out) {
        if (e instanceof SqlAgg.Reducer r && r.fn() == SqlAgg.Fn.MODE) {
            if (groupedModesCounted()) {
                out.put(r, groupBy);
            }
            return;
        }
        if (e instanceof SqlExpr.WindowCall w && w.fn() instanceof SqlAgg.Reducer r && r.fn() == SqlAgg.Fn.MODE) {
            if (w.orderBy().isEmpty() && w.frame() == null) {
                out.put(w, w.partitionBy());
            }
            return;
        }
        if (e instanceof SqlExpr.WindowCall w && w.fn() instanceof SqlAgg.Reducer r && !windowQuantilesNative()
                && (r.fn() == SqlAgg.Fn.MEDIAN || r.fn() == SqlAgg.Fn.QUANTILE_CONT || r.fn() == SqlAgg.Fn.QUANTILE_DISC)
                && w.orderBy().isEmpty() && w.frame() == null) {
            out.put(w, w.partitionBy());
            return;
        }
        for (SqlExpr c : e.children()) {
            collectModes(c, groupBy, out);
        }
    }

    /** The value a mode counts. */
    private SqlExpr modeValue(SqlExpr mode) {
        SqlAgg.Reducer r = mode instanceof SqlExpr.WindowCall w ? (SqlAgg.Reducer) w.fn() : (SqlAgg.Reducer) mode;
        if (r.args().size() != 1 || r.distinct()) {
            throw new DialectCapability("a mode of " + r.args().size() + " arguments reached " + product());
        }
        return r.args().get(0);
    }

    /** A counted mode where it is written: the value of its most counted row, the least value on a tie (a NULL's
     *  count is 0, so it is last), NULL when every value is -- over a window, {@code FIRST_VALUE(v) OVER (PARTITION BY ..
     *  ORDER BY count DESC, v)} over the whole partition; grouped, the first of the group's values in that order. */
    private SqlWriter modeArm(SqlWriter writer, SqlExpr mode) {
        List<SqlExpr> counted = modeCounts.isEmpty() ? null : modeCounts.peek().get(mode);
        SqlExpr count = counted == null ? null : counted.get(0);
        if (count == null) {
            throw new IllegalStateException("a MODE reached " + product() + "'s writer uncounted: modesCounted did not"
                    + " run");
        }
        SqlExpr v = modeValue(mode);
        if (mode instanceof SqlExpr.WindowCall w) {
            writer.append("FIRST_VALUE(").expr(v, 0).append(") OVER (");
            if (!w.partitionBy().isEmpty()) {
                writer.append("PARTITION BY ").list(w.partitionBy()).append(" ");
            }
            return writer.append("ORDER BY ").expr(count, 0).append(" DESC, ").expr(v, 0)
                    .append(" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)");
        }
        SqlWriter.Piece first = arrayAgg(false, json(v), o -> o.expr(count, 0).append(" DESC, ").expr(v, 0), true);
        return writer.piece(valueAt(first, w -> w.append("'$[0]'"), typeOf(v)));
    }

    /**
     * A root select's values as their labels deliver them: a Float as a DOUBLE (an integer division or an average is
     * a DECIMAL here), a boolean as a comparison's 1 or 0. A sort key naming a rewritten column is its original
     * expression (ORDER BY reads an alias first).
     */
    private SqlSelect delivered(SqlSelect s) {
        Map<SqlSelect, Boolean> top = rootSelects.peek();
        List<com.legend.sql.OutputCol> labels = roots.peek();
        if (top == null || !Boolean.TRUE.equals(top.get(s)) || labels == null
                || labels.size() != s.projections().size()) {
            return s;
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        Map<String, SqlExpr> moved = new LinkedHashMap<>();
        for (int i = 0; i < s.projections().size(); i++) {
            SqlSelect.Projection p = s.projections().get(i);
            SqlExpr value = deliveredValue(p.expr(), labels.get(i).type());
            SqlType label = labels.get(i).type();
            if (jsonIsBinary() && (nested(label)) && !(value instanceof SqlExpr.Star)
                    && !(value instanceof SqlExpr.StarExcept) && !(value instanceof SqlExpr.NullLit)) {
                asText.add(value);
            }
            if (value != p.expr() && p.outputName() != null) {
                moved.put(p.outputName(), p.expr());
            }
            ps.add(value == p.expr() ? p : new SqlSelect.Projection(value, p.outputName(), p.out()));
        }
        if (moved.isEmpty()) {
            return s;
        }
        List<SqlSelect.SortKey> order = new ArrayList<>();
        for (SqlSelect.SortKey k : s.orderBy()) {
            order.add(k.expr() instanceof SqlExpr.Column c && c.table() == null && moved.containsKey(c.name())
                    ? new SqlSelect.SortKey(java.util.Objects.requireNonNull(moved.get(c.name()), "moved"),
                            k.ascending(), k.nullOrder(), k.outputName())
                    : k);
        }
        return new SqlSelect(ps, s.distinct(), s.from(), s.where(), s.groupBy(), s.having(), s.qualify(), order,
                s.limit(), s.offset(), s.outputs());
    }

    private static SqlExpr deliveredValue(SqlExpr e, SqlType label) {
        if (e instanceof SqlExpr.NullLit || e instanceof SqlExpr.Star || e instanceof SqlExpr.StarExcept) {
            return e;
        }
        if (label == SqlType.Scalar.DOUBLE && typeOf(e) != SqlType.Scalar.DOUBLE) {
            return new SqlExpr.Cast(e, SqlType.Scalar.DOUBLE);
        }
        // a value that claims no type (an empty list's element: a JSON_TABLE text column, or JSON_VALUE of a NULL,
        // which is binary) is its label's type
        if (typeOf(e) == null && (label instanceof SqlType.Decimal || label == SqlType.Scalar.BIGINT
                || label == SqlType.Scalar.INTEGER || label == SqlType.Scalar.HUGEINT
                || label == SqlType.Scalar.BOOLEAN || label == SqlType.Scalar.DATE
                || label == SqlType.Scalar.TIMESTAMP || label == SqlType.Scalar.VARCHAR)) {
            return new SqlExpr.Cast(e, label);
        }
        return e;
    }

    /** {@code s} with each star over a FROM holding a marker spelled out: each other source's columns, then each
     *  exploded column (the marker's positions are a column of their own, which no star may carry). */
    private static SqlSelect starsSpelled(SqlSelect s) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        if (s.projections().isEmpty()) {
            spell(s.from(), null, ps);
        }
        for (SqlSelect.Projection p : s.projections()) {
            if (p.expr() instanceof SqlExpr.Star st) {
                spell(s.from(), st.table(), ps);
            } else {
                ps.add(p);
            }
        }
        return new SqlSelect(ps, s.distinct(), s.from(), s.where(), s.groupBy(), s.having(), s.qualify(), s.orderBy(),
                s.limit(), s.offset(), s.outputs());
    }

    private static void spell(SqlSource src, @com.legend.base.Nullable String table, List<SqlSelect.Projection> into) {
        if (src instanceof SqlSource.Join j) {
            spell(j.left(), table, into);
            spell(j.right(), table, into);
        } else if (src instanceof SqlSource.Subselect sub && isMarker(sub.inner())
                && (table == null || table.equals(sub.alias()))) {
            for (SqlSelect.Projection p : ((SqlSelect) sub.inner()).projections()) {
                into.add(new SqlSelect.Projection(explodedRead(sub.alias(), p, p.out() != null ? p.out().type() : null),
                        markerColumn(p), p.out()));
            }
        } else if (!(src instanceof SqlSource.Dual) && (table == null || table.equals(src.alias()))) {
            into.add(new SqlSelect.Projection(new SqlExpr.Star(src.alias()), null, null));
        }
    }

    /** The markers' exploded columns, by alias, anywhere in a FROM. */
    private static void lateralsOf(SqlSource src, Map<String, Map<String, SqlExpr>> into) {
        if (src instanceof SqlSource.Join j) {
            lateralsOf(j.left(), into);
            lateralsOf(j.right(), into);
        } else if (src instanceof SqlSource.Subselect sub && sub.inner() instanceof SqlSelect m && isMarker(m)) {
            Map<String, SqlExpr> cols = new LinkedHashMap<>();
            for (SqlSelect.Projection p : m.projections()) {
                cols.put(markerColumn(p), ((SqlExpr.Call) p.expr()).args().get(0));
            }
            into.put(sub.alias(), cols);
        }
    }

    /** An exploded column's read, its list's element at the marker's position, or null for any other column. */
    private SqlWriter.@com.legend.base.Nullable Piece explodedColumn(SqlExpr.Column c) {
        for (Map<String, Map<String, SqlExpr>> scope : laterals) {
            if (c.table() != null) {
                Map<String, SqlExpr> cols = scope.get(c.table());
                if (cols != null && cols.containsKey(c.name())) {
                    return element(java.util.Objects.requireNonNull(cols.get(c.name()), "list"), c.table());
                }
            } else {
                List<Map.Entry<String, Map<String, SqlExpr>>> owners = scope.entrySet().stream()
                        .filter(e -> e.getValue().containsKey(c.name())).toList();
                if (owners.size() == 1) {
                    return element(java.util.Objects.requireNonNull(owners.get(0).getValue().get(c.name()), "list"),
                            owners.get(0).getKey());
                }
            }
        }
        return null;
    }

    /** {@code xs[position]}: the list's element at the marker's 0-based position (NULL past its end). */
    private SqlWriter.Piece element(SqlExpr xs, String position) {
        if (nullList(xs)) {
            return w -> w.append("NULL");
        }
        return elementAt(xs, w -> w.append("(" + aliasIdent(position) + ".\"key\" - 1)"));
    }

    /** A marker's positions: {@code JSON_TABLE} over its longest list (DuckDB's parallel UNNEST pads the shorter with
     *  NULL; a NULL list is none), its {@code key} the 0-based position; a lateral one joins its left side correlated
     *  (a table function's argument correlates, measured; LEFT JOIN keeps an empty list's row). Any other lateral
     *  derived table is refused: MariaDB has no LATERAL. */
    @Override
    protected SqlWriter source(SqlWriter writer, SqlSource src, int depth) {
        if (src instanceof SqlSource.Join j && j.right() instanceof SqlSource.Subselect sub && isMarker(sub.inner())
                && (j.kind() == SqlSource.Join.Kind.CROSS_LATERAL || j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)) {
            if (j.on() != null && !(j.on() instanceof SqlExpr.BoolLit b && b.value())) {
                throw new DialectCapability("a lateral explosion with a join condition reached " + product());
            }
            source(writer, j.left(), depth);
            nl(writer, depth).append(j.kind() == SqlSource.Join.Kind.LEFT_LATERAL ? "LEFT JOIN " : "CROSS JOIN ")
                    .piece(positions((SqlSelect) sub.inner(), aliasIdent(sub.alias())));
            return j.kind() == SqlSource.Join.Kind.LEFT_LATERAL ? writer.append(" ON TRUE") : writer;
        }
        if (src instanceof SqlSource.Subselect sub && isMarker(sub.inner())) {
            return writer.piece(positions((SqlSelect) sub.inner(), aliasIdent(sub.alias())));
        }
        if (src instanceof SqlSource.Join j && (j.kind() == SqlSource.Join.Kind.CROSS_LATERAL
                || j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)) {
            if (correlatesDerived()) {
                return super.source(writer, src, depth);   // a database with LATERAL (MySql)
            }
            if (!(j.right() instanceof SqlSource.Subselect sub)) {
                throw new DialectCapability("a lateral join to a " + j.right().getClass().getSimpleName()
                        + " reached " + product() + ", which has no LATERAL");
            }
            boolean left = j.kind() == SqlSource.Join.Kind.LEFT_LATERAL;
            boolean always = j.on() == null || j.on() instanceof SqlExpr.BoolLit b && b.value();
            source(writer, j.left(), depth);
            nl(writer, depth).append(left ? "LEFT JOIN " : always ? "CROSS JOIN " : "JOIN ")
                    .piece(lateralRows(sub, depth)).append(" AS ").append(aliasIdent(sub.alias()));
            if (left || !always) {
                writer.append(" ON ");
                if (always) {
                    writer.append("TRUE");
                } else {
                    writer.expr(java.util.Objects.requireNonNull(j.on(), "on"), 0);
                }
            }
            return writer;
        }
        return super.source(writer, src, depth);
    }

    /**
     * A lateral subquery's rows, for a database with no LATERAL: its rows as a JSON array of objects, read back by
     * {@code JSON_TABLE} as typed columns (a table function's argument correlates with the FROM before it, measured;
     * LEFT JOIN keeps a row with none). A plain select is written inline -- its FROM and WHERE under the aggregate,
     * since an outer reference inside a derived table is refused here ("Unknown table", measured); any other is a
     * derived table, which must then read nothing outside it.
     */
    private SqlWriter.Piece lateralRows(SqlSource.Subselect sub, int depth) {
        List<com.legend.sql.OutputCol> outs = sub.inner().outputs();
        if (outs.isEmpty()) {
            throw new DialectCapability("a lateral subquery of undeclared columns reached " + product());
        }
        return w -> {
            w.append("JSON_TABLE(coalesce((SELECT JSON_ARRAYAGG(JSON_OBJECT(");
            if (sub.inner() instanceof SqlSelect s && inlineable(s)) {
                Map<String, Map<String, SqlExpr>> scope = new LinkedHashMap<>();
                lateralsOf(s.from(), scope);
                laterals.push(scope);
                try {
                    List<SqlExpr> values = lateralValues(s, outs);
                    for (int i = 0; i < outs.size(); i++) {
                        com.legend.sql.OutputCol oc = outs.get(i);
                        SqlExpr v = values.get(i);
                        w.append(i > 0 ? ", " : "").append(stringLit(oc.name())).append(", ")
                                .piece(jsonOf(x -> x.expr(v, 0), oc.type()));
                    }
                    w.append(")");
                    if (!s.orderBy().isEmpty()) {
                        w.append(" ORDER BY ").join(s.orderBy(), ", ", this::orderKey);
                    }
                    w.append(") FROM ");
                    source(w, s.from(), depth + 1);
                    if (s.where() != null) {
                        w.append(" WHERE ").expr(s.where(), 0);
                    }
                } finally {
                    laterals.pop();
                }
            } else {
                for (int i = 0; i < outs.size(); i++) {
                    com.legend.sql.OutputCol oc = outs.get(i);
                    w.append(i > 0 ? ", " : "").append(stringLit(oc.name())).append(", ")
                            .piece(jsonOf(x -> x.append(columnRef(SqlExpr.Column.of("__l", oc))), oc.type()));
                }
                w.append(")) FROM (");
                nl(w, depth + 1);
                query(w, sub.inner(), depth + 1);
                nl(w, depth).append(") AS __l");
            }
            w.append("), JSON_ARRAY()), '$[*]' COLUMNS (");
            for (int i = 0; i < outs.size(); i++) {
                com.legend.sql.OutputCol oc = outs.get(i);
                w.append(i > 0 ? ", " : "").append(aliasIdent(oc.name())).append(" " + columnType(oc.type()) + " PATH ")
                        .append(memberPath(oc.name()));
            }
            w.append("))");
        };
    }

    /** A select whose rows can be aggregated in place: one FROM, a WHERE, nothing that shapes its rows. */
    private static boolean inlineable(SqlSelect s) {
        return !s.distinct() && s.groupBy().isEmpty() && s.having() == null && s.qualify() == null
                && s.limit() == null && s.offset() == null && !(s.from() instanceof SqlSource.Dual);
    }

    /** Each output's value in {@code s}: its projection's, or a star's column (no projection is a star over the
     *  FROM, its one source's alias qualifying the column). */
    private List<SqlExpr> lateralValues(SqlSelect s, List<com.legend.sql.OutputCol> outs) {
        List<SqlExpr> values = new ArrayList<>();
        if (s.projections().isEmpty()) {
            String table = s.from() instanceof SqlSource.Join ? null : s.from().alias();
            for (com.legend.sql.OutputCol oc : outs) {
                values.add(SqlExpr.Column.of(table, oc));
            }
            return values;
        }
        boolean aligned = s.projections().size() == outs.size()
                && s.projections().stream().noneMatch(p -> p.expr() instanceof SqlExpr.Star);
        if (aligned) {
            s.projections().forEach(p -> values.add(p.expr()));
            return values;
        }
        if (s.projections().size() == 1 && s.projections().get(0).expr() instanceof SqlExpr.Star st) {
            for (com.legend.sql.OutputCol oc : outs) {
                values.add(SqlExpr.Column.of(st.table(), oc));
            }
            return values;
        }
        throw new DialectCapability("a lateral subquery whose projections do not match its outputs reached " + product());
    }

    /** {@code JSON_TABLE(<longest list>, '$[*]' COLUMNS ("key" FOR ORDINALITY)) AS alias}: its key the 1-based
     *  ordinal ({@link #element} reads the position before it). */
    private SqlWriter.Piece positions(SqlSelect m, String alias) {
        List<SqlExpr> lists = m.projections().stream().map(p -> ((SqlExpr.Call) p.expr()).args().get(0))
                .filter(xs -> !nullList(xs)).toList();
        return jsonTable(w -> {
            if (lists.isEmpty()) {
                w.append("NULL");
            } else if (lists.size() == 1) {
                w.expr(lists.get(0), 0);
            } else {
                // the longest list (its positions are every list's)
                w.append("CASE");
                for (int i = 0; i < lists.size() - 1; i++) {
                    SqlExpr xs = lists.get(i);
                    w.append(" WHEN ");
                    for (int j = 0; j < lists.size(); j++) {
                        if (j != i) {
                            w.append(j > (i == 0 ? 1 : 0) ? " AND " : "").append("coalesce(JSON_LENGTH(")
                                    .expr(xs, 0).append("), 0) >= coalesce(JSON_LENGTH(").expr(lists.get(j), 0)
                                    .append("), 0)");
                        }
                    }
                    w.append(" THEN ").expr(xs, 0);
                }
                w.append(" ELSE ").expr(lists.get(lists.size() - 1), 0).append(" END");
            }
        }, "'$[*]' COLUMNS (\"key\" FOR ORDINALITY)", alias);
    }

    /** No VALUES alias list: the rows are a union of one-row selects naming their columns (the first row names them).
     *  No rows is one row of NULLs, filtered. */
    @Override
    protected SqlWriter valuesSource(SqlWriter writer, SqlSource.Values v) {
        writer.append("(");
        if (v.rows().isEmpty()) {
            writer.append("SELECT ");
            for (int i = 0; i < v.columns().size(); i++) {
                writer.append(i > 0 ? ", " : "").append("NULL AS ").append(aliasIdent(v.columns().get(i)));
            }
            return writer.append(" FROM DUAL WHERE FALSE) AS ").append(aliasIdent(v.alias()));
        }
        for (int r = 0; r < v.rows().size(); r++) {
            writer.append(r > 0 ? " UNION ALL SELECT " : "SELECT ");
            List<SqlExpr> row = v.rows().get(r);
            for (int i = 0; i < row.size(); i++) {
                writer.append(i > 0 ? ", " : "").expr(row.get(i), 0);
                if (r == 0) {
                    writer.append(" AS ").append(aliasIdent(v.columns().get(i)));
                }
            }
        }
        return writer.append(") AS ").append(aliasIdent(v.alias()));
    }

    // ==================================================================
    // Lexical: identifiers, literals, types
    // ==================================================================

    /** A quote doubled (NO_BACKSLASH_ESCAPES: a backslash is itself, to the server). The driver's own scan of a
     *  prepared statement reads a backslash as escaping the next character whatever the session says (measured:
     *  after {@code '\\'} it takes the closing quote as escaped, and a later brace for a JDBC escape, "unknown escape
     *  sequence"), so a text with an odd run of backslashes before a quote spells each backslash
     *  {@code CHAR(92 USING utf8mb4)}. */
    @Override
    protected String stringLit(String value) {
        String body = value.replace("'", "''");
        if (!oddBackslashesBeforeQuote(body + "'")) {
            return "'" + body + "'";
        }
        String[] parts = value.split("\\\\", -1);
        StringBuilder sb = new StringBuilder("CONCAT(");
        for (int i = 0; i < parts.length; i++) {
            sb.append(i > 0 ? ", CHAR(92 USING utf8mb4), " : "").append('\'').append(parts[i].replace("'", "''"))
                    .append('\'');
        }
        return sb.append(')').toString();
    }

    private static boolean oddBackslashesBeforeQuote(String text) {
        int run = 0;
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            if (ch == '\\') {
                run++;
            } else {
                if (ch == '\'' && run % 2 == 1) {
                    return true;
                }
                run = 0;
            }
        }
        return false;
    }

    @Override
    protected String dateLit(String iso) {
        return "DATE '" + iso + "'";
    }

    /** A date-time literal at microsecond precision (MariaDB keeps six digits). */
    @Override
    protected String timestampLit(String iso) {
        return "CAST('" + iso.replace('T', ' ') + "' AS DATETIME(6))";
    }

    /** A Float literal is a DOUBLE only with an exponent ({@code 1.5} is a DECIMAL, measured); no infinity, no NaN. */
    @Override
    protected String floatLiteral(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) {
            throw new DialectCapability("a " + v + " literal reached " + product() + ", whose DOUBLE has no infinity or NaN");
        }
        String s = Double.toString(v);
        return s.contains("E") ? s : s + "E0";
    }

    @Override
    protected String arrayTypeName(String element) {
        return "LONGTEXT";
    }

    @Override
    protected String mapTypeName(String key, String value) {
        return "LONGTEXT";
    }

    /** Alias-less projections label EXPLICITLY from the declared output: MariaDB labels an expression by its text. */
    @Override
    protected @com.legend.base.Nullable String implicitLabel(SqlSelect.Projection p) {
        com.legend.sql.OutputCol out = p.out();
        return out != null ? aliasIdent(out.name()) : super.implicitLabel(p);
    }

    /** NULL sorts first ascending here, and there is no NULLS FIRST/LAST: the placement is a key of its own before
     *  the value, in the value's direction -- {@code (x IS NULL)} or {@code (x IS NOT NULL)} -- since MySQL's
     *  GROUP_CONCAT takes a key that folds to a constant (a column with no NULL) as the direction of the keys after it
     *  (measured: {@code (x IS NULL), x DESC} sorted ascending there). */
    @Override
    protected SqlWriter sortKey(SqlWriter writer, SqlSelect.SortKey k) {
        boolean nullsFirst = k.nullOrder() != null ? k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                : !k.ascending();
        String dir = k.ascending() ? "" : " DESC";
        if (!(k.expr() instanceof SqlExpr.IntLit) && !(k.expr() instanceof SqlExpr.StringLit)) {
            writer.append("(").expr(k.expr(), 4).append(nullsFirst == k.ascending() ? " IS NOT NULL)" : " IS NULL)")
                    .append(dir + ", ");
        }
        return writer.expr(k.expr(), 0).append(dir);
    }

    @Override
    protected String aggOrderNullPlacement(SqlSelect.SortKey k) {
        return "";
    }

    /** An aggregate's own ORDER BY key, its NULL placement a key of its own. */
    private SqlWriter orderKey(SqlWriter writer, SqlSelect.SortKey k) {
        return sortKey(writer, k);
    }

    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        if (!asText.isEmpty() && asText.remove(e)) {
            writer.append("CAST(");
            expr(writer, e, 0);
            return writer.append(" AS CHAR)");
        }
        return switch (e) {
            case SqlExpr.ArrayLit a -> writer.append("JSON_ARRAY(").join(a.elements(), ", ", (w, x) -> w.piece(json(x)))
                    .append(")");
            case SqlExpr.BoolLit b -> writer.append(b.value() ? "TRUE" : "FALSE");
            // DuckDB's list(v ORDER BY k): ascending, NULLs last
            case SqlExpr.OrderedListAgg ola -> writer.piece(arrayAgg(false, json(ola.value()), w -> orderKey(w,
                    new SqlSelect.SortKey(ola.orderBy(), true, SqlSelect.SortKey.NullOrder.NULLS_LAST, null)), false));
            case SqlExpr.Membership m when m.collection() instanceof SqlExpr.PlanParam p -> throw listParameter(p);
            case SqlExpr.Column c when explodedColumn(c) != null ->
                    writer.piece(java.util.Objects.requireNonNull(explodedColumn(c), "exploded"));
            default -> super.expr(writer, e, parentPrec);
        };
    }

    /** A list plan parameter: MariaDB binds no array. */
    private DialectCapability listParameter(SqlExpr.PlanParam p) {
        return new DialectCapability("list plan parameter '" + p.name() + "' reached " + product() + ", which binds no array");
    }

    // ==================================================================
    // JSON: how a list, a struct, a map and a Variant hold a value
    // ==================================================================

    private static @com.legend.base.Nullable SqlType typeOf(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
    }

    /** The element type of a list, or null for a value that is no list. */
    private static @com.legend.base.Nullable SqlType listElement(SqlExpr e) {
        return typeOf(e) instanceof SqlType.Array a ? a.element() : null;
    }

    private static boolean nested(@com.legend.base.Nullable SqlType t) {
        return t instanceof SqlType.Array || t instanceof SqlType.Struct || t instanceof SqlType.Map
                || t == SqlType.Scalar.JSON;
    }

    /** A JSON value as its text, compact as Pure and DuckDB print JSON: a JSON value is text here, already so. */
    protected SqlWriter.Piece jsonAsText(SqlWriter.Piece json) {
        return json;
    }

    /** A value as a JSON container holds it ({@link #jsonOf}). */
    private SqlWriter.Piece json(SqlExpr x) {
        SqlType t = typeOf(x);
        if (t == null && (x instanceof SqlExpr.ArrayLit || x instanceof SqlExpr.StructLit)) {
            return compact(w -> w.expr(x, 0));
        }
        if (t == null && x instanceof SqlExpr.BoolLit) {
            t = SqlType.Scalar.BOOLEAN;
        }
        return jsonOf(w -> w.expr(x, 0), t);
    }

    /** A value of type {@code t} as a JSON container holds it: a boolean as JSON true or false (it is 1 or 0 here,
     *  and only a comparison's own result is serialized as a JSON boolean, measured), a list, struct, map or Variant as
     *  its JSON ({@code JSON_COMPACT}: a value read from a column or a table function is text, which a JSON function
     *  would quote, measured), anything else as it is. */
    private SqlWriter.Piece jsonOf(SqlWriter.Piece v, @com.legend.base.Nullable SqlType t) {
        if (t == SqlType.Scalar.BOOLEAN) {
            return w -> w.append("JSON_EXTRACT(CASE WHEN ").piece(v).append(" THEN 'true' WHEN NOT (").piece(v)
                    .append(") THEN 'false' END, '$')");
        }
        if (nested(t)) {
            return compact(v);
        }
        return v;
    }

    /** The {@code JSON_TABLE} column type an element of type {@code t} is read as: a scalar by its own type (a string
     *  in the binary collation), a nested value or a Variant as JSON (its text). */
    private String columnType(@com.legend.base.Nullable SqlType t) {
        if (t instanceof SqlType.Decimal d) {
            return "DECIMAL(" + Math.min(d.precision(), 65) + "," + Math.min(d.scale(), 38) + ")";
        }
        if (t == null || !(t instanceof SqlType.Scalar s)) {
            return nested(t) ? "JSON" : "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";
        }
        return switch (s) {
            case BOOLEAN -> booleanColumn();
            case INTEGER, BIGINT -> "BIGINT";
            case HUGEINT -> "DECIMAL(38,0)";
            case DOUBLE -> "DOUBLE";
            case DATE -> "DATE";
            case TIMESTAMP, TIMESTAMPTZ -> "DATETIME(6)";
            case TIME -> "TIME(6)";
            case JSON -> "JSON";
            case VARCHAR, LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT -> "LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin";
        };
    }

    /** A JSON text's value at {@code path} as a value of type {@code t}: a nested value or a Variant its JSON
     *  ({@code JSON_EXTRACT}), a scalar its SQL value ({@code JSON_VALUE}, a string unquoted) cast to its type. */
    private SqlWriter.Piece valueAt(SqlWriter.Piece json, SqlWriter.Piece path, @com.legend.base.Nullable SqlType t) {
        if (nested(t)) {
            return w -> w.append("JSON_EXTRACT(").piece(json).append(", ").piece(path).append(")");
        }
        SqlWriter.Piece text = jsonText(json, path);
        return scalarOfText(text, t);
    }

    /** A JSON scalar's text as a value of type {@code t}. A JSON boolean's {@code JSON_VALUE} is {@code '1'} or
     *  {@code '0'} (measured), a JSON string's its text. */
    private SqlWriter.Piece scalarOfText(SqlWriter.Piece text, @com.legend.base.Nullable SqlType t) {
        if (t == SqlType.Scalar.BOOLEAN) {
            return w -> w.append("(CASE ").piece(text).append(" WHEN '1' THEN TRUE WHEN 'true' THEN TRUE WHEN '0' THEN"
                    + " FALSE WHEN 'false' THEN FALSE END)");
        }
        if (t instanceof SqlType.Decimal || t instanceof SqlType.Scalar s && s != SqlType.Scalar.VARCHAR
                && s != SqlType.Scalar.LITERAL && s != SqlType.Scalar.TEMPORAL_TEXT
                && s != SqlType.Scalar.DECIMAL_TEXT) {
            String type = t instanceof SqlType.Decimal d ? columnType(d) : switch ((SqlType.Scalar) t) {
                case INTEGER, BIGINT -> "SIGNED";
                case HUGEINT -> "DECIMAL(38,0)";
                case DOUBLE -> "DOUBLE";
                case DATE -> "DATE";
                case TIMESTAMP, TIMESTAMPTZ -> "DATETIME(6)";
                case TIME -> "TIME(6)";
                case BOOLEAN, JSON, VARCHAR, LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT -> "CHAR";
            };
            return w -> w.append("CAST(").piece(text).append(" AS " + type + ")");
        }
        return text;
    }

    /** The element of {@code xs} at a 0-based {@code index} (NULL past either end). */
    private SqlWriter.Piece elementAt(SqlExpr xs, SqlWriter.Piece index) {
        return valueAt(w -> w.expr(xs, 0), indexPath(index), listElement(xs));
    }

    // ==================================================================
    // Lists: JSON arrays, lambda-less -- each operation a correlated subquery over JSON_TABLE(xs)
    // ==================================================================

    /** A lambda parameter (or an element's or index's name) as its references spell it. */
    private String param(String name) {
        return columnRef(SqlExpr.Column.derived(null, name));
    }

    /** {@code JSON_TABLE(xs, '$[*]' COLUMNS (o FOR ORDINALITY [, i FOR ORDINALITY], e <type> PATH '$')) AS e__t}: each
     *  element bound to {@code e} as its type reads, its 1-based position to {@code o} (and to {@code i}, DuckDB's
     *  index parameter, when named). A correlated table function inside a scalar subquery, measured; a NULL list is no
     *  rows. */
    private SqlWriter.Piece elements(SqlExpr xs, String e, String o, @com.legend.base.Nullable String i) {
        String type = columnType(listElement(xs));
        String spec = "'$[*]' COLUMNS (" + param(o) + " FOR ORDINALITY" + (i != null ? ", " + param(i)
                + " FOR ORDINALITY" : "") + ", " + param(e) + " " + type + " PATH '$')";
        return jsonTable(w -> w.expr(xs, 0), spec, param(e + "__t"));
    }

    /** {@code body}, or NULL for a NULL list (DuckDB's list functions answer NULL there). */
    private static SqlWriter.Piece unlessNull(SqlExpr xs, SqlWriter.Piece body) {
        return w -> w.append("(CASE WHEN ").expr(xs, 4).append(" IS NULL THEN NULL ELSE ").piece(body).append(" END)");
    }

    private SqlExpr.Lambda lambdaOf(SqlExpr e, SqlFn fn) {
        if (e instanceof SqlExpr.Lambda l && !l.params().isEmpty()) {
            return l;
        }
        throw new DialectCapability(fn + " without a lambda reached " + product());
    }

    /** The position alias of a lambda's element: unique per lambda, so an inner list operation never shadows it. */
    private static String offsetOf(SqlExpr.Lambda l) {
        return l.params().get(0) + "__o";
    }

    private static @com.legend.base.Nullable String indexOf(SqlExpr.Lambda l) {
        return l.params().size() > 1 ? l.params().get(1) : null;
    }

    /** A lambda's body over elements of {@code element} type: a field read off a struct parameter ({@code p.first},
     *  as DuckDB spells it -- a lambda parameter is no table, so a column qualified by one is a field) becomes a field
     *  read of the JSON object, for every parameter. */
    private static SqlExpr lambdaBody(SqlExpr.Lambda l, @com.legend.base.Nullable SqlType element) {
        SqlExpr body = l.body();
        for (int i = 0; i < l.params().size(); i++) {
            body = structFields(body, l.params().get(i), i == 0 ? element : null);
        }
        return body;
    }

    private static SqlExpr structFields(SqlExpr body, String param, @com.legend.base.Nullable SqlType struct) {
        if (body instanceof SqlExpr.Column c && param.equals(c.table())) {
            SqlExpr source = struct instanceof SqlType.Struct ? SqlExpr.Column.of(null, param, struct, true,
                    com.legend.sql.OutputCol.Origin.DERIVED) : SqlExpr.Column.derived(null, param);
            return new SqlExpr.StructGet(source, c.name(), c.type());
        }
        List<SqlExpr> kids = body.children();
        if (kids.isEmpty()) {
            return body;
        }
        List<SqlExpr> mapped = new ArrayList<>(kids.size());
        boolean changed = false;
        for (SqlExpr k : kids) {
            SqlExpr m = structFields(k, param, struct);
            changed |= m != k;
            mapped.add(m);
        }
        return changed ? body.withChildren(mapped) : body;
    }

    /** {@code (SELECT JSON_ARRAYAGG(select ORDER BY o) FROM elements [WHERE where])}, the empty array over no rows
     *  (JSON_ARRAYAGG of none is NULL, measured), NULL for a NULL list: the elements kept in list order. */
    private SqlWriter.Piece overElements(SqlExpr xs, String e, String o, @com.legend.base.Nullable String i,
            SqlWriter.Piece select, SqlWriter.@com.legend.base.Nullable Piece where) {
        return unlessNull(xs, w -> {
            w.append("coalesce((SELECT ").piece(arrayAgg(false, select, v -> v.append(param(o)), false))
                    .append(" FROM ").piece(elements(xs, e, o, i));
            if (where != null) {
                w.append(" WHERE ").piece(where);
            }
            w.append("), JSON_ARRAY())");
        });
    }

    /** {@code (SELECT agg(e) FROM elements)}: a list reduced as rows (an empty or NULL list reduces over no rows).
     *  MODE is the most frequent non-NULL element, the first to occur among ties (DuckDB's list_mode). */
    private SqlWriter.Piece overRows(SqlExpr xs, SqlAgg.Fn fn, List<SqlExpr> extras) {
        String e = param("__e");
        String o = param("__o");
        if (fn == SqlAgg.Fn.MODE) {
            if (!extras.isEmpty()) {
                throw new DialectCapability("a mode with arguments reached " + product());
            }
            return w -> w.append("(SELECT " + e + " FROM ").piece(elements(xs, "__e", "__o", null))
                    .append(" WHERE " + e + " IS NOT NULL GROUP BY " + e + " ORDER BY count(*) DESC, min(" + o
                            + ") LIMIT 1)");
        }
        SqlExpr el = listElement(xs) == null ? SqlExpr.Column.derived(null, "__e") : SqlExpr.Column.param("__e", xs);
        List<SqlExpr> args = new ArrayList<>();
        args.add(el);
        args.addAll(extras);
        List<SqlSelect.SortKey> order = fn == SqlAgg.Fn.STRING_AGG || fn == SqlAgg.Fn.LIST
                ? List.of(new SqlSelect.SortKey(SqlExpr.Column.derived(null, "__o"), true, null, null)) : List.of();
        SqlAgg.Reducer r = new SqlAgg.Reducer(fn, args, false, order);
        return w -> {
            w.append("(SELECT ");
            reducer(w, r);
            w.append(" FROM ").piece(elements(xs, "__e", "__o", null)).append(")");
        };
    }

    /** A reduction whose value is one of the list's elements ({@code min}, {@code max}, {@code mode}): over a list whose
     *  elements claim no type (an empty list's NULL), the JSON_TABLE column is text, so the value is cast to the call's
     *  own type -- the type its column declares. */
    private SqlWriter.Piece elementReduction(SqlExpr.Call c, SqlAgg.Fn fn) {
        SqlWriter.Piece r = overRows(c.args().get(0), fn, List.of());
        String type = listElement(c.args().get(0)) == null ? castTarget(typeOf(c)) : null;
        return type == null ? r : w -> w.append("CAST(").piece(r).append(" AS " + type + ")");
    }

    @Override
    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        return writer.append("JSON_ARRAY(").join(elements, ", ", (w, x) -> w.piece(json(x))).append(")");
    }

    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit st) {
        return writer.append("JSON_OBJECT(").join(st.fields(), ", ",
                (w, f) -> w.append(stringLit(f.name())).append(", ").piece(json(f.value()))).append(")");
    }

    /** A JSON path to a member by name: {@code $."name"}. */
    private String memberPath(String name) {
        return stringLit("$.\"" + name.replace("\\", "\\\\").replace("\"", "\\\"") + "\"");
    }

    /** A field of a JSON object, read as its type ({@link #valueAt}). */
    @Override
    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        SqlType type = typeOf(g);
        if (type == null && typeOf(g.source()) instanceof SqlType.Struct struct) {
            for (SqlType.Struct.Field f : struct.fields()) {
                if (f.name().equals(g.field())) {
                    type = f.type();
                }
            }
        }
        return writer.piece(valueAt(w -> w.expr(g.source(), 0), w -> w.append(memberPath(g.field())), type));
    }

    /** DuckDB's struct_insert: the struct with the field set (a new one last). */
    @Override
    protected SqlWriter structInsert(SqlWriter writer, List<SqlExpr> args) {
        if (!(args.get(1) instanceof SqlExpr.StringLit name)) {
            throw new DialectCapability("struct_insert of a field that is not a literal reached " + product());
        }
        return writer.append("JSON_SET(").expr(args.get(0), 0).append(", ").append(memberPath(name.value()))
                .append(", ").piece(json(args.get(2))).append(")");
    }

    /** exists([]) is false: Pure's empty-collection semantics. */
    @Override
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "max", "FALSE", SqlFn.LIST_EXISTS);
    }

    /** forAll([]) is true. */
    @Override
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "min", "TRUE", SqlFn.LIST_FOR_ALL);
    }

    private SqlWriter listPredicate(SqlWriter writer, List<SqlExpr> args, String agg, String empty, SqlFn fn) {
        SqlExpr.Lambda l = lambdaOf(args.get(1), fn);
        SqlExpr body = lambdaBody(l, listElement(args.get(0)));
        return writer.append("coalesce((SELECT " + agg + "(CASE WHEN ").expr(body, 0).append(" THEN 1 WHEN NOT (")
                .expr(body, 0).append(") THEN 0 END) FROM ")
                .piece(elements(args.get(0), l.params().get(0), offsetOf(l), indexOf(l))).append("), " + empty + ")");
    }

    /** No duplicates iff the distinct count is the count (a NULL element is no distinct value, as DuckDB's
     *  len(list_distinct(x)) = len(x)); an empty or NULL list is distinct. */
    @Override
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> args) {
        String e = param("__e");
        return writer.append("coalesce((SELECT count(*) = count(DISTINCT " + e + ") FROM ")
                .piece(elements(args.get(0), "__e", "__o", null)).append("), TRUE)");
    }

    /** DuckDB's list_contains: NULL for a NULL list or needle; else found or not -- a NULL element never makes an
     *  absent needle NULL. A nested value compares as its JSON text. */
    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        SqlType el = listElement(m.collection());
        String e = param("__e");
        SqlWriter.Piece needle = nested(el) ? compact(w -> w.expr(m.needle(), 0)) : w -> w.expr(m.needle(), 5);
        SqlWriter.Piece element = nested(el) ? compact(w -> w.append(e)) : w -> w.append(e);
        return writer.append("(CASE WHEN ").expr(m.collection(), 4).append(" IS NULL OR ").expr(m.needle(), 4)
                .append(" IS NULL THEN NULL ELSE EXISTS(SELECT 1 FROM ").piece(elements(m.collection(), "__e", "__eo",
                        null)).append(" WHERE ").piece(element).append(" = ").piece(needle).append(") END)");
    }

    @Override
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        return writer.piece(overRows(rc.collection(), rc.reducer(), rc.extras()));
    }

    @Override
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("an unnest in a select list reached " + product() + ", which has no set-returning"
                + " projection (an explode is placed in FROM as a JSON_TABLE)");
    }

    /**
     * Pure's fold, {@code (element, acc)}, as a {@code WITH RECURSIVE} over the elements in order: the accumulator
     * starts at the init, each step applies the body to the next element, and the last row's accumulator is the fold
     * (an empty or NULL list folds to the init). A recursive CTE takes no outer reference (measured: "Unknown column"),
     * so a fold over a row's list, or whose lambda reads a row, is a wall. The anchor's column is read through a typed
     * {@code JSON_TABLE}: a CTE's column takes the anchor's type, and a literal's is too narrow to grow (measured: ''
     * is CHAR(0), "Data too long"; 0 is an INT, "Out of range").
     */
    @Override
    protected SqlWriter foldCall(SqlWriter writer, SqlExpr.FoldCall f) {
        if (!correlatesDerived() && (!closed(f.source(), Set.of()) || !closed(f.init(), Set.of())
                || !closed(f.lambda(), Set.of()))) {
            throw new DialectCapability("a fold that reads a row reached " + product() + ", whose recursive CTE cannot read an"
                    + " outer column (measured: Unknown column)");
        }
        SqlType accType = typeOf(f) != null ? typeOf(f) : typeOf(f.init());
        if (accType == null) {
            throw new DialectCapability("a fold whose accumulator claims no type reached " + product() + ", whose recursive CTE"
                    + " types its column by its first row");
        }
        SqlExpr.Lambda l = f.lambda();
        SqlExpr body = lambdaBody(l, listElement(f.source()));
        return writer.piece(accumulation(f.source(), l.params().get(0), l.params().get(1), accType,
                w -> w.append("SELECT 0, \"__a\".\"a\" FROM ").piece(jsonTable(v -> v.append("JSON_ARRAY(")
                        .piece(json(f.init())).append(")"), "'$[*]' COLUMNS (\"a\" " + columnType(accType) + " PATH '$')",
                        "\"__a\"")),
                body));
    }

    /** DuckDB's list_reduce, {@code (acc, x)}: the first element starts the accumulator and the lambda runs over the
     *  rest; an empty list reduces to NULL here (DuckDB raises). A recursive walk, as {@link #foldCall}'s. */
    private SqlWriter.Piece listReduce(SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.Lambda l) || l.params().size() != 2) {
            throw wall(SqlFn.LIST_REDUCE, "a reduction lambda of another shape");
        }
        if (!correlatesDerived() && (!closed(a.get(0), Set.of()) || !closed(l, Set.of()))) {
            throw wall(SqlFn.LIST_REDUCE, "a reduction that reads a row needs a recursive CTE over an outer column"
                    + " (measured: Unknown column)");
        }
        SqlType el = listElement(a.get(0));
        SqlType accType = typeOf(c) != null ? typeOf(c) : el;
        if (accType == null) {
            throw wall(SqlFn.LIST_REDUCE, "a reduction whose accumulator claims no type");
        }
        String acc = l.params().get(0);
        String x = l.params().get(1);
        SqlExpr body = lambdaBody(new SqlExpr.Lambda(List.of(x, acc), l.body()), el);
        return accumulation(a.get(0), x, acc, accType, w -> w.append("SELECT 1, \"__a\".\"a\" FROM ")
                .piece(jsonTable(v -> v.expr(a.get(0), 0), "'$[0]' COLUMNS (\"a\" " + columnType(accType) + " PATH '$')",
                        "\"__a\"")), body);
    }

    /** {@code (WITH RECURSIVE __f(__i, acc) AS (<anchor> UNION ALL <step>) <last acc>)}: the step joins the element
     *  at position {@code __i + 1} (1-based) and applies {@code body}, reading the element as {@code x} and the
     *  accumulator as {@code acc}. */
    private SqlWriter.Piece accumulation(SqlExpr xs, String x, String acc, SqlType accType, SqlWriter.Piece anchor,
            SqlExpr body) {
        String spec = "'$[*]' COLUMNS (\"__k\" FOR ORDINALITY, " + param(x) + " " + columnType(listElement(xs))
                + " PATH '$')";
        return w -> w.append("(WITH RECURSIVE \"__f\"(\"__i\", ").append(param(acc)).append(") AS (").piece(anchor)
                .append(" UNION ALL SELECT \"__f\".\"__i\" + 1, ").expr(body, 0).append(" FROM \"__f\" JOIN ")
                .piece(jsonTable(v -> v.expr(xs, 0), spec, "\"__x\"")).append(" ON \"__x\".\"__k\" ="
                        + " \"__f\".\"__i\" + 1) SELECT ").append(param(acc))
                .append(" FROM \"__f\" ORDER BY \"__i\" DESC LIMIT 1)");
    }

    /** Whether {@code e} reads no column but the {@code bound} names (the parameters of the lambdas around it), and
     *  holds no subquery: a value a recursive CTE can hold, since a CTE takes no outer reference. */
    private static boolean closed(SqlExpr e, Set<String> bound) {
        if (e instanceof SqlExpr.Lambda l) {
            Set<String> inner = new java.util.HashSet<>(bound);
            inner.addAll(l.params());
            return closed(l.body(), inner);
        }
        if (e instanceof SqlExpr.Column c) {
            return bound.contains(c.table() != null ? c.table() : c.name());
        }
        if (e instanceof SqlExpr.ScalarSubquery || e instanceof SqlExpr.Exists || e instanceof SqlExpr.InSubquery
                || e instanceof SqlExpr.Quantified) {
            return false;
        }
        return e.children().stream().allMatch(k -> closed(k, bound));
    }

    /** Pure's splitPart: the non-empty tokens, 1-based, NULL past the end: the token with n - 1 non-empty tokens
     *  before it (no OFFSET of an expression here). */
    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece parts = split(a.get(0), a.get(1));
        String cols = "'$[*]' COLUMNS (\"__o\" FOR ORDINALITY, \"__v\" " + columnType(SqlType.Scalar.VARCHAR)
                + " PATH '$')";
        return writer.append("(CASE WHEN ").expr(a.get(2), 4).append(" > 0 THEN (SELECT __t.\"__v\" FROM ")
                .piece(jsonTable(parts, cols, "__t")).append(" WHERE __t.\"__v\" <> '' AND (SELECT count(*) FROM ")
                .piece(jsonTable(parts, cols, "__u")).append(" WHERE __u.\"__v\" <> '' AND __u.\"__o\" < __t.\"__o\") = ")
                .expr(a.get(2), 6).append(" - 1) END)");
    }

    /** The Levenshtein distance of two texts, by characters: the edit-distance table a row at a time, each row a JSON
     *  array its cells are appended to, in one recursive walk over the cells (SQLite's walk). A recursive CTE takes no
     *  outer reference, so two texts that read a row are a wall; the anchor's columns are typed through a
     *  {@code JSON_TABLE} (a literal's type is too narrow to grow, measured). */
    private SqlWriter.Piece levenshtein(SqlExpr x, SqlExpr y) {
        if (!correlatesDerived() && (!closed(x, Set.of()) || !closed(y, Set.of()))) {
            throw wall(SqlFn.LEVENSHTEIN, "an edit distance of texts that read a row needs a recursive CTE over an outer"
                    + " column (measured: Unknown column)");
        }
        String at = jsonText("__lv.\"p\"", "CONCAT('$[', __lv.\"j\" + 1, ']')");
        String cell = "LEAST(" + at + " + 1, " + jsonText("__lv.\"c\"", "CONCAT('$[', __lv.\"j\", ']')") + " + 1, "
                + jsonText("__lv.\"p\"", "CONCAT('$[', __lv.\"j\", ']')") + " + (SUBSTRING(__lv.\"a\", __lv.\"i\", 1) <>"
                + " SUBSTRING(__lv.\"b\", __lv.\"j\" + 1, 1)))";
        String more = "__lv.\"j\" < CHAR_LENGTH(__lv.\"b\")";
        String text = columnType(SqlType.Scalar.VARCHAR);
        return w -> w.append("(WITH RECURSIVE __lv(\"a\", \"b\", \"i\", \"j\", \"p\", \"c\") AS (SELECT __s.\"a\","
                + " __s.\"b\", 0, 0, __s.\"p\", __s.\"c\" FROM JSON_TABLE(JSON_ARRAY(").expr(x, 0).append(", ").expr(y, 0)
                .append(", JSON_ARRAY(), JSON_ARRAY(0)), '$' COLUMNS (\"a\" " + text + " PATH '$[0]', \"b\" " + text
                        + " PATH '$[1]', \"p\" JSON PATH '$[2]', \"c\" JSON PATH '$[3]')) AS __s UNION ALL SELECT"
                        + " __lv.\"a\", __lv.\"b\", CASE WHEN " + more + " THEN __lv.\"i\" ELSE __lv.\"i\" + 1 END, CASE"
                        + " WHEN " + more + " THEN __lv.\"j\" + 1 ELSE 0 END, CASE WHEN " + more + " THEN __lv.\"p\" ELSE"
                        + " __lv.\"c\" END, CASE WHEN " + more + " THEN JSON_ARRAY_APPEND(__lv.\"c\", '$', CASE WHEN"
                        + " __lv.\"i\" = 0 THEN __lv.\"j\" + 1 ELSE " + cell + " END) ELSE JSON_ARRAY(__lv.\"i\" + 1) END"
                        + " FROM __lv WHERE NOT (__lv.\"j\" = CHAR_LENGTH(__lv.\"b\") AND __lv.\"i\" ="
                        + " CHAR_LENGTH(__lv.\"a\"))) SELECT CAST(" + jsonText("\"c\"", "CONCAT('$[', \"j\", ']')") + " AS SIGNED)"
                        + " FROM __lv WHERE \"i\" = CHAR_LENGTH(\"a\") AND \"j\" = CHAR_LENGTH(\"b\"))");
    }

    /** {@code JSON_TABLE} over {@code n} rows ({@code n} at least one), its ordinal {@code alias."__n"}: a correlated
     *  row generator (a recursive CTE could not correlate). */
    private SqlWriter.Piece rows(SqlWriter.Piece n, String alias) {
        return jsonTable(w -> w.append("CONCAT('[', REPEAT('0,', (").piece(n).append(") - 1), '0]')"),
                "'$[*]' COLUMNS (\"__n\" FOR ORDINALITY)", alias);
    }

    /**
     * DuckDB's string_split as a JSON array of the parts: the text's JSON string with each (JSON-escaped) delimiter a
     * string boundary; the empty delimiter splits into characters, the empty text into no parts.
     */
    private SqlWriter.Piece split(SqlExpr s, SqlExpr d) {
        return w -> w.append("(CASE WHEN ").expr(s, 4).append(" IS NULL OR ").expr(d, 4)
                .append(" IS NULL THEN NULL WHEN CHAR_LENGTH(").expr(s, 0).append(") = 0 THEN JSON_ARRAY() WHEN CHAR_LENGTH(")
                .expr(d, 0).append(") = 0 THEN (SELECT ").piece(arrayAgg(false, v -> v.append("SUBSTRING(").expr(s, 0)
                        .append(", __g.\"__n\", 1)"), v -> v.append("__g.\"__n\""), false))
                .append(" FROM ").piece(rows(v -> v.append("CHAR_LENGTH(").expr(s, 0).append(")"), "__g"))
                .append(") ELSE ").piece(compact(v -> v.append("CONCAT('[', REPLACE(JSON_QUOTE(").expr(s, 0)
                        .append("), SUBSTRING(JSON_QUOTE(").expr(d, 0).append("), 2, CHAR_LENGTH(JSON_QUOTE(").expr(d, 0)
                        .append(")) - 2), '\",\"'), ']')"))).append(" END)");
    }

    // ==================================================================
    // Variant: JSON text
    // ==================================================================

    private static boolean isJson(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.JSON;
    }

    private static boolean isText(SqlExpr e) {
        return e instanceof SqlExpr.StringLit || typeOf(e) == SqlType.Scalar.VARCHAR;
    }

    private static boolean isDouble(SqlExpr e) {
        return e instanceof SqlExpr.FloatLit || typeOf(e) == SqlType.Scalar.DOUBLE;
    }

    /** A value as a Variant's JSON text: a string quoted, a boolean true or false, a nested value its JSON, a scalar
     *  its JSON ({@code JSON_EXTRACT(JSON_ARRAY(x), '$[0]')}). */
    private SqlWriter.Piece variantOf(SqlExpr x) {
        SqlType t = typeOf(x);
        if (t == SqlType.Scalar.JSON) {
            return w -> w.expr(x, 0);
        }
        if (nested(t) || x instanceof SqlExpr.ArrayLit || x instanceof SqlExpr.StructLit) {
            return compact(w -> w.expr(x, 0));
        }
        if (t == SqlType.Scalar.BOOLEAN || x instanceof SqlExpr.BoolLit) {
            return jsonOf(w -> w.expr(x, 0), SqlType.Scalar.BOOLEAN);
        }
        return w -> w.append("JSON_EXTRACT(JSON_ARRAY(").expr(x, 0).append("), '$[0]')");
    }

    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        if (a.size() != 1) {
            throw new DialectCapability("toVariant of " + a.size() + " arguments reached " + product());
        }
        return writer.piece(variantOf(a.get(0)));
    }

    /** A member by key, or a 0-based index (negative from the end; past either end NULL): its JSON text. */
    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        SqlExpr key = args.get(1);
        if (key instanceof SqlExpr.StringLit root && "$".equals(root.value())) {
            return writer.expr(args.get(0), 0);
        }
        if (isText(key)) {
            return writer.append("JSON_EXTRACT(").expr(args.get(0), 0).append(", CONCAT('$.\"', REPLACE(")
                    .expr(key, 0).append(", '\"', '\\\"'), '\"'))");
        }
        return writer.append("JSON_EXTRACT(").expr(args.get(0), 0).append(", ")
                .piece(indexPath(w -> w.expr(key, 0))).append(")");
    }

    /** A Variant array's elements, each a Variant: the array's JSON is the list (NULL for a non-array). */
    @Override
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("(CASE WHEN JSON_TYPE(").expr(args.get(0), 0).append(") = 'ARRAY' THEN ")
                .expr(args.get(0), 0).append(" END)");
    }

    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        SqlType target = c.target();
        SqlExpr v = c.value();
        if (target == SqlType.Scalar.TEMPORAL_TEXT || target == SqlType.Scalar.DECIMAL_TEXT) {
            return writer.expr(v, 0);
        }
        if (v instanceof SqlExpr.NullLit) {
            return writer.append("NULL");
        }
        if (target == SqlType.Scalar.JSON) {
            return isJson(v) ? writer.expr(v, 0)
                    : isText(v) ? writer.piece(compact(w -> w.expr(v, 0))) : writer.piece(variantOf(v));
        }
        if (isJson(v)) {
            return writer.piece(fromVariant(v, target));
        }
        if (target instanceof SqlType.Array arr) {
            SqlType from = listElement(v);
            // a text element as LITERAL is the text itself, as the scalar cast below writes it (LITERAL is VARCHAR
            // physically): the mixed sort's ordered ids (2026-10-13, PR #34's review: element-wise, it wrote the
            // aggregate twice)
            if (from == null || from.equals(arr.element())
                    || from == SqlType.Scalar.VARCHAR && arr.element() == SqlType.Scalar.LITERAL) {
                return writer.expr(v, 0);
            }
            // element-wise
            return writer.piece(overElements(v, "__c", "__co", null,
                    jsonOf(castTo(w -> w.append(param("__c")), from, arr.element()), arr.element()), null));
        }
        if (target instanceof SqlType.Struct || target instanceof SqlType.Map) {
            return writer.expr(v, 0);
        }
        if (target == SqlType.Scalar.VARCHAR || target == SqlType.Scalar.LITERAL) {
            return writer.piece(asText(v));
        }
        return writer.piece(castTo(w -> w.expr(v, 0), typeOf(v), target));
    }

    /** A Variant's value as {@code to}: a scalar by its SQL value ({@code JSON_VALUE}: a string unquoted), as text a
     *  member's scalar text and a whole value's JSON; a list re-held element by element. */
    private SqlWriter.Piece fromVariant(SqlExpr v, SqlType to) {
        // a member read as text is its scalar text (a string unquoted); a whole value as text is its JSON (DuckDB's
        // CAST(json AS VARCHAR) keeps a string's quotes)
        boolean member = v instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET;
        if ((to == SqlType.Scalar.VARCHAR || to == SqlType.Scalar.LITERAL) && !member) {
            return jsonAsText(w -> w.expr(v, 0));
        }
        if (to == SqlType.Scalar.VARCHAR || to == SqlType.Scalar.LITERAL) {
            return w -> w.append("(CASE WHEN JSON_TYPE(").expr(v, 0).append(") = 'STRING' THEN JSON_UNQUOTE(")
                    .expr(v, 0).append(") ELSE ").piece(jsonAsText(x -> x.expr(v, 0))).append(" END)");
        }
        if (to == SqlType.Scalar.BOOLEAN) {
            // a JSON boolean's JSON_VALUE is '1' or '0' (measured; MySQL's 'true' or 'false')
            SqlWriter.Piece text = jsonText(w -> w.expr(v, 0), w -> w.append("'$'"));
            return w -> w.append("(CASE JSON_TYPE(").expr(v, 0).append(") WHEN 'BOOLEAN' THEN ").piece(text)
                    .append(" IN ('1', 'true') WHEN 'STRING' THEN CASE lower(JSON_UNQUOTE(").expr(v, 0)
                    .append(")) WHEN 'true' THEN TRUE WHEN 'false' THEN FALSE END WHEN 'INTEGER' THEN ").piece(text)
                    .append(" <> 0 END)");
        }
        if (to instanceof SqlType.Array arr) {
            SqlWriter.Piece each = arr.element() == SqlType.Scalar.JSON ? w -> w.append(param("__j"))
                    : scalarOfText(w -> w.append(jsonText(param("__j"), "'$'")), arr.element());
            return w -> w.append("(CASE WHEN JSON_TYPE(").expr(v, 0).append(") = 'ARRAY' THEN coalesce((SELECT ")
                    .piece(arrayAgg(false, jsonOf(each, arr.element()), x -> x.append(param("__jo")), false))
                    .append(" FROM ").piece(jsonTable(x -> x.expr(v, 0), "'$[*]' COLUMNS (" + param("__jo")
                            + " FOR ORDINALITY, " + param("__j") + " JSON PATH '$')", "__jt"))
                    .append("), JSON_ARRAY()) END)");
        }
        if (to instanceof SqlType.Map || to instanceof SqlType.Struct) {
            return w -> w.expr(v, 0);
        }
        return scalarOfText(jsonText(w -> w.expr(v, 0), w -> w.append("'$'")), to);
    }

    /** {@code value} (of type {@code from}, when known) cast to {@code to}, each cast decided here. */
    private SqlWriter.Piece castTo(SqlWriter.Piece value, @com.legend.base.Nullable SqlType from, SqlType to) {
        if (to instanceof SqlType.Decimal d) {
            return w -> w.append("CAST(").piece(value).append(" AS " + columnType(d) + ")");
        }
        if (!(to instanceof SqlType.Scalar s)) {
            return value;
        }
        return switch (s) {
            case VARCHAR, LITERAL -> textOf(value, from);
            case BOOLEAN -> from == SqlType.Scalar.VARCHAR || from == SqlType.Scalar.JSON
                    ? w -> w.append("(CASE lower(").piece(value).append(") WHEN 'true' THEN TRUE WHEN 'false' THEN FALSE")
                            .append(" END)")
                    : w -> w.append("(").piece(value).append(" <> 0)");
            // DuckDB rounds a double to an integer, half away from zero (MariaDB's double CAST rounds half to even)
            case INTEGER, BIGINT -> from == SqlType.Scalar.DOUBLE
                    ? w -> w.append("CAST(sign(").piece(value).append(") * floor(abs(").piece(value)
                            .append(") + 0.5) AS SIGNED)")
                    : w -> w.append("CAST(").piece(value).append(" AS SIGNED)");
            case HUGEINT -> w -> w.append("CAST(").piece(value).append(" AS DECIMAL(38,0))");
            case DOUBLE -> w -> w.append("CAST(").piece(value).append(" AS DOUBLE)");
            case DATE -> w -> w.append("CAST(").piece(value).append(" AS DATE)");
            case TIMESTAMP -> from == SqlType.Scalar.VARCHAR
                    ? w -> w.append("CAST(REPLACE(").piece(value).append(", 'T', ' ') AS DATETIME(6))")
                    : w -> w.append("CAST(").piece(value).append(" AS DATETIME(6))");
            // a zoned date-time is held as its naive UTC date-time: its offset applied at the cast
            case TIMESTAMPTZ -> from == SqlType.Scalar.VARCHAR ? utcOf(value)
                    : w -> w.append("CAST(").piece(value).append(" AS DATETIME(6))");
            case JSON -> w -> w.append("JSON_EXTRACT(JSON_ARRAY(").piece(value).append("), '$[0]')");
            case TIME -> w -> w.append("CAST(").piece(value).append(" AS TIME(6))");
            case TEMPORAL_TEXT, DECIMAL_TEXT -> value;
        };
    }

    /** A value as text, as DuckDB's CAST to VARCHAR prints it. */
    protected SqlWriter.Piece asText(SqlExpr e) {
        if (e instanceof SqlExpr.StringLit) {
            return w -> w.expr(e, 0);
        }
        SqlType t = e instanceof SqlExpr.FloatLit ? SqlType.Scalar.DOUBLE
                : e instanceof SqlExpr.BoolLit ? SqlType.Scalar.BOOLEAN : typeOf(e);
        return textOf(w -> w.expr(e, 0), t);
    }

    /** DuckDB's text of a value of {@code type}: a Float its shortest round-trip digits as DuckDB lays them out
     *  ({@link #floatText}), a boolean true or false, a date-time without trailing fraction zeros, anything else its
     *  CAST (a DECIMAL keeps its scale's digits, as DuckDB's). */
    private SqlWriter.Piece textOf(SqlWriter.Piece value, @com.legend.base.Nullable SqlType type) {
        if (type == SqlType.Scalar.DOUBLE) {
            return floatText(value);
        }
        if (type == SqlType.Scalar.BOOLEAN) {
            return w -> w.append("(CASE WHEN ").piece(value).append(" THEN 'true' WHEN NOT (").piece(value)
                    .append(") THEN 'false' END)");
        }
        if (type == SqlType.Scalar.TIMESTAMP || type == SqlType.Scalar.TIMESTAMPTZ) {
            return w -> w.append("(CASE WHEN MICROSECOND(").piece(value).append(") = 0 THEN DATE_FORMAT(").piece(value)
                    .append(", '%Y-%m-%d %H:%i:%s') ELSE TRIM(TRAILING '0' FROM DATE_FORMAT(").piece(value)
                    .append(", '%Y-%m-%d %H:%i:%s.%f')) END)");
        }
        return w -> w.append("CAST(").piece(value).append(" AS CHAR)");
    }

    /**
     * A double's text as DuckDB prints it (Python's repr: the shortest round-trip digits, fixed notation for a decimal
     * exponent from -4 to 15 with at least one fraction digit, else {@code d.ddde+XX}). MariaDB's own CAST gives the
     * shortest digits (measured: 0.30000000000000004) laid out its own way; its digits and exponent are read back and
     * laid out again, the value bound once through a one-row JSON_TABLE (a derived table could not correlate).
     */
    private SqlWriter.Piece floatText(SqlWriter.Piece value) {
        return floatLayout(value, false);
    }

    /** {@link #floatText}, or with {@code canon} Pure's literal spelling ({@code SqlFn.FLOAT_CANON}): fixed point at
     *  every exponent (Pure never writes one) and every zero {@code 0.0}. */
    private SqlWriter.Piece floatLayout(SqlWriter.Piece value, boolean canon) {
        // __f.t: |v| as MariaDB's text; __f.n: v < 0
        String t = "__f.\"t\"";
        String mant = "SUBSTRING_INDEX(" + t + ", 'e', 1)";
        String ip = "SUBSTRING_INDEX(" + t + ", '.', 1)";
        String fp = "CASE WHEN INSTR(" + t + ", '.') > 0 THEN SUBSTRING_INDEX(" + t + ", '.', -1) ELSE '' END";
        // the significant digits, and the decimal exponent of the first
        String digits = "TRIM(TRAILING '0' FROM CASE WHEN INSTR(" + t + ", 'e') > 0 THEN REPLACE(" + mant + ", '.', '')"
                + " ELSE TRIM(LEADING '0' FROM CONCAT(" + ip + ", " + fp + ")) END)";
        String exp = "CASE WHEN INSTR(" + t + ", 'e') > 0 THEN CAST(SUBSTRING_INDEX(" + t + ", 'e', -1) AS SIGNED)"
                + " WHEN " + ip + " <> '0' THEN CHAR_LENGTH(" + ip + ") - 1"
                + " ELSE -(CHAR_LENGTH(" + fp + ") - CHAR_LENGTH(TRIM(LEADING '0' FROM " + fp + ")) + 1) END";
        String d = "__g.\"d\"";
        String x = "__g.\"x\"";
        String fixed = "CASE WHEN " + x + " >= 0 THEN CONCAT(RPAD(LEFT(" + d + ", " + x + " + 1), " + x + " + 1, '0'),"
                + " '.', CASE WHEN CHAR_LENGTH(" + d + ") > " + x + " + 1 THEN SUBSTRING(" + d + ", " + x + " + 2)"
                + " ELSE '0' END) ELSE CONCAT('0.', REPEAT('0', -" + x + " - 1), " + d + ") END";
        String sci = "CONCAT(LEFT(" + d + ", 1), CASE WHEN CHAR_LENGTH(" + d + ") > 1 THEN CONCAT('.', SUBSTRING(" + d
                + ", 2)) ELSE '' END, 'e', CASE WHEN " + x + " < 0 THEN '-' ELSE '+' END, LPAD(ABS(" + x + "), 2, '0'))";
        String zero = canon ? "'0.0'" : "CASE WHEN __f.\"n\" THEN '-0.0' ELSE '0.0' END";
        String layout = canon ? fixed : "CASE WHEN " + x + " BETWEEN -4 AND 15 THEN " + fixed + " ELSE " + sci + " END";
        return w -> w.append("(SELECT CASE WHEN __f.\"t\" IS NULL THEN NULL WHEN __f.\"t\" = '0' THEN " + zero)
                .append(" ELSE CONCAT(CASE WHEN __f.\"n\" THEN '-' ELSE '' END, (SELECT " + layout + " FROM ")
                .piece(jsonTable(v -> v.append("JSON_ARRAY(" + digits + ", " + exp + ")"),
                        "'$' COLUMNS (\"d\" VARCHAR(64) PATH '$[0]', \"x\" INT PATH '$[1]')", "__g"))
                .append(")) END FROM ").piece(jsonTable(v -> v.append("JSON_ARRAY(CAST(ABS(").piece(value)
                        .append(") AS CHAR), ").piece(value).append(" < 0)"),
                        "'$' COLUMNS (\"t\" VARCHAR(64) PATH '$[0]', \"n\" " + booleanColumn() + " PATH '$[1]')", "__f"))
                .append(")");
    }

    // ==================================================================
    // Scalar functions -- every SqlFn decided here
    // ==================================================================

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            // ---- each function as this dialect spells it, by name (the dialect program's Phase 1, D1, 2026-10-13:
            // an exhaustive switch; a spelling several databases share is a named helper of the base)
            // the ANSI operators
            case AND, OR, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES -> infix(writer, c, parentPrec);
            case EQUAL, NOT_EQUAL -> equalArm(writer, c, parentPrec);
            // the shared spellings (the base's named helpers)
            case NOT -> notPrefix(writer, c.args(), parentPrec);
            case IS_NULL -> isNullTest(writer, c.args());
            case IS_NOT_NULL -> isNotNullTest(writer, c.args());
            case XOR -> xorAsAndOr(writer, c.args(), parentPrec);
            case UC_FIRST -> firstCharCased(writer, "upper", c.args(), parentPrec);
            case LC_FIRST -> firstCharCased(writer, "lower", c.args(), parentPrec);
            // the hooks: this dialect's own (or the base's refusal)
            case HASH -> hashSigned(writer, c.args());
            case ROUND -> roundHalfEven(writer, c.args());
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(writer, c.fn(), c.args());
            case STRUCT_INSERT -> structInsert(writer, c.args());
            case UNNEST -> unnestProjection(writer, c.args());
            case PURE_SPLIT_PART -> splitPartCall(writer, c.args());
            case LIST_EXISTS -> listExists(writer, c.args());
            case LIST_FOR_ALL -> listForAll(writer, c.args());
            case ALL_DISTINCT -> allDistinct(writer, c.args());
            case TO_VARIANT -> variantConstruct(writer, c.args());
            case VARIANT_GET -> variantGet(writer, c.args());
            case VARIANT_ELEMENTS -> variantElements(writer, c.args());
            // this database's plain spellings: name(args)
            case ABS -> writer.function("abs", c.args());
            case ATAN -> writer.function("atan", c.args());
            case ATAN2 -> writer.function("atan2", c.args());
            case COS -> writer.function("cos", c.args());
            case COT -> writer.function("cot", c.args());
            case SIN -> writer.function("sin", c.args());
            case TAN -> writer.function("tan", c.args());
            case DEGREES -> writer.function("degrees", c.args());
            case RADIANS -> writer.function("radians", c.args());
            case EXP -> writer.function("exp", c.args());
            case LN -> writer.function("ln", c.args());
            case LOG10 -> writer.function("log10", c.args());
            case SQRT -> writer.function("sqrt", c.args());
            case POW -> writer.function("POW", c.args());
            case LOWER -> writer.function("lower", c.args());
            case UPPER -> writer.function("upper", c.args());
            case REPLACE -> writer.function("replace", c.args());
            case TRANSLATE -> throw wall(c.fn(), product() + " has no translate function");
            case REVERSE_STRING -> writer.function("reverse", c.args());
            case FLOOR_RAW -> writer.function("floor", c.args());
            case STRPOS -> writer.function("INSTR", c.args());
            case LIST_LENGTH, JSON_ARRAY_LENGTH -> writer.function("JSON_LENGTH", c.args());
            case IN -> in(writer, c, parentPrec);
            // a Pure integer (no CAST to BIGINT here: SIGNED)
            case CEILING -> writer.append("CAST(CEIL(").expr(a.get(0), 0).append(") AS SIGNED)");
            case FLOOR -> writer.append("CAST(FLOOR(").expr(a.get(0), 0).append(") AS SIGNED)");
            case SIGN -> writer.append("CAST(SIGN(").expr(a.get(0), 0).append(") AS SIGNED)");
            // PI() is shown at six digits (measured: 3.141593): its DOUBLE in full
            case PI -> writer.append("(PI() + 0.0E0)");
            // the NULL-safe comparison is <=> here (no IS [NOT] DISTINCT FROM)
            case NULL_SAFE_EQUAL -> writer.append("(").expr(a.get(0), 4).append(" <=> ").expr(a.get(1), 4).append(")");
            case IS_DISTINCT_FROM, NULL_SAFE_NOT_EQUAL -> writer.append("(NOT (").expr(a.get(0), 4).append(" <=> ")
                    .expr(a.get(1), 4).append("))");
            // no hyperbolic functions: their definitions
            case COSH -> writer.append("((EXP(").expr(a.get(0), 0).append(") + EXP(-(").expr(a.get(0), 0)
                    .append("))) / 2)");
            case SINH -> writer.append("((EXP(").expr(a.get(0), 0).append(") - EXP(-(").expr(a.get(0), 0)
                    .append("))) / 2)");
            case TANH -> writer.append("((EXP(2 * (").expr(a.get(0), 0).append(")) - 1) / (EXP(2 * (")
                    .expr(a.get(0), 0).append(")) + 1))");
            // DuckDB's ascii: the first character's code point (ASCII() is a byte here), 0 for ''
            case ASCII_CODE -> writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL THEN NULL WHEN CHAR_LENGTH(")
                    .expr(a.get(0), 0).append(") = 0 THEN 0 ELSE CAST(CONV(HEX(CONVERT(LEFT(").expr(a.get(0), 0)
                    .append(", 1) USING utf32)), 16, 10) AS SIGNED) END)");
            case CHR -> writer.append("CONVERT(CHAR(").expr(a.get(0), 0).append(" USING utf32) USING utf8mb4)");
            // outside [-1, 1] MariaDB answers NULL (measured), DuckDB raises
            case ACOS -> domainGuarded(writer, c, "ACOS");
            case ASIN -> domainGuarded(writer, c, "ASIN");
            case COALESCE -> a.size() == 1 ? writer.expr(a.get(0), parentPrec) : writer.function("coalesce", a);
            // never "--x": that is a comment
            case NEGATE -> writer.append("-(").expr(a.get(0), 0).append(")");
            case ERROR -> writer.piece(raise(a, typeOf(c)));

            // ---- arithmetic
            // Pure's divide is a DOUBLE division; x / 0 is NULL here (DuckDB's answers +-Infinity)
            case DIVIDE -> writer.append("(CAST(").expr(a.get(0), 0).append(" AS DOUBLE) / CAST(").expr(a.get(1), 0)
                    .append(" AS DOUBLE))");
            case MOD, REM -> writer.piece(remainder(c.fn(), a));
            // DuckDB's // truncates toward zero, NULL for a zero divisor: DIV
            case INT_DIVIDE -> intDivide(writer, c);
            case ROUND_HALF_UP -> roundHalfUp(writer, a);
            case CBRT -> writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" < 0 THEN -POW(-(").expr(a.get(0), 0)
                    .append("), 1.0E0 / 3) ELSE POW(").expr(a.get(0), 0).append(", 1.0E0 / 3) END)");
            case BIT_NOT -> writer.append("CAST(~ ").expr(a.get(0), 7).append(" AS SIGNED)");
            case PARSE_INT -> writer.piece(parseInt(a.get(0)));
            case BOOL_TO_TEXT -> writer.piece(textOf(w -> w.expr(a.get(0), 0), SqlType.Scalar.BOOLEAN));
            // a Float's literal spelling, the value read once ({@link #floatLayout}): the portable decomposition reads
            // the text some fifty times, each read this dialect's long text of a double (545 KB for a five-element
            // mixed list, measured 2026-10-11)
            case FLOAT_CANON -> writer.piece(floatLayout(w -> w.expr(a.get(0), 0), true));
            // NULL-skipping, as DuckDB's (MariaDB's GREATEST/LEAST are NULL for any NULL argument)
            case GREATEST -> writer.piece(nullSkipping("GREATEST", a));
            case LEAST -> writer.piece(nullSkipping("LEAST", a));

            // ---- strings
            // NULL-skipping, as DuckDB's concat (CONCAT is NULL for any NULL argument here; CONCAT_WS skips them)
            case CONCAT -> writer.append("CONCAT_WS('', ").join(flattenConcat(a), ", ", (w, x) -> w.piece(asText(x)))
                    .append(")");
            case CONCAT_JOIN -> writer.append("CONCAT_WS('', ").join(a, ", ", (w, x) -> w.piece(asText(x))).append(")");
            case LENGTH -> writer.append("CHAR_LENGTH(").piece(asText(a.get(0))).append(")");
            case LEFT, RIGHT -> writer.piece(leftRight(c.fn(), a));
            case LPAD -> pad(writer, "LPAD", a);
            case RPAD -> pad(writer, "RPAD", a);
            // a NULL (a cast one is written bare) is cast to text: REPEAT of a bare NULL is binary (measured)
            case REPEAT_STR -> (a.get(0) instanceof SqlExpr.NullLit
                    || a.get(0) instanceof SqlExpr.Cast n && n.value() instanceof SqlExpr.NullLit
                    ? writer.append("REPEAT(CAST(").expr(a.get(0), 0).append(" AS CHAR), ")
                    : writer.append("REPEAT(").expr(a.get(0), 0).append(", ")).expr(a.get(1), 0).append(")");
            case STARTS_WITH -> writer.append("(LEFT(").expr(a.get(0), 0).append(", CHAR_LENGTH(").expr(a.get(1), 0)
                    .append(")) = ").expr(a.get(1), 5).append(")");
            case ENDS_WITH -> writer.append("(RIGHT(").expr(a.get(0), 0).append(", CHAR_LENGTH(").expr(a.get(1), 0)
                    .append(")) = ").expr(a.get(1), 5).append(")");
            // DuckDB's trim family: spaces, or any of the given characters
            case TRIM, LTRIM, RTRIM -> writer.piece(trim(c.fn(), a));
            case SUBSTRING -> writer.append("SUBSTRING(").list(a).append(")");
            case SPLIT -> writer.piece(split(a.get(0), a.get(1)));
            // DuckDB's split_part: 1-based, negative from the end, out of range ''
            case SPLIT_PART -> writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL OR ").expr(a.get(1), 4)
                    .append(" IS NULL THEN NULL WHEN ").expr(a.get(2), 4).append(" = 0 THEN '' ELSE coalesce(")
                    .piece(valueAt(split(a.get(0), a.get(1)), indexPath(w -> w.append("CASE WHEN ").expr(a.get(2), 4)
                            .append(" < 0 THEN ").expr(a.get(2), 0).append(" ELSE ").expr(a.get(2), 6)
                            .append(" - 1 END")), SqlType.Scalar.VARCHAR)).append(", '') END)");
            // PCRE: REGEXP searches (a binary collation: case-sensitive); a full match anchors both ends
            case MATCHES -> writer.append("(").expr(a.get(0), 4).append(" REGEXP ").piece(pattern(a, c.fn(), false))
                    .append(")");
            case REGEXP_FULL_MATCH -> writer.append("(").expr(a.get(0), 4).append(" REGEXP ")
                    .piece(pattern(a, c.fn(), true)).append(")");
            case REGEXP_EXTRACT -> writer.piece(regexpExtract(a));
            case REGEXP_EXTRACT_ALL -> writer.piece(regexpExtractAll(a));
            case REGEXP_REPLACE -> writer.piece(regexpReplace(a));
            // TO_BASE64 breaks its lines every 76 characters (measured); FROM_BASE64 wants the padding
            case ENCODE_BASE64 -> writer.append("REPLACE(TO_BASE64(").expr(a.get(0), 0)
                    .append("), CHAR(10 USING utf8mb4), '')");
            case DECODE_BASE64 -> writer.append("CONVERT(FROM_BASE64(CONCAT(").expr(a.get(0), 0)
                    .append(", REPEAT('=', MOD(4 - MOD(CHAR_LENGTH(").expr(a.get(0), 0)
                    .append("), 4), 4)))) USING utf8mb4)");
            case MD5 -> writer.append("MD5(").expr(a.get(0), 0).append(")");
            case SHA1 -> writer.append("SHA1(").expr(a.get(0), 0).append(")");
            case SHA256 -> writer.append("SHA2(").expr(a.get(0), 0).append(", 256)");
            case LEVENSHTEIN -> writer.piece(levenshtein(a.get(0), a.get(1)));
            case JARO_WINKLER -> throw wall(c.fn(), product() + " has no string-distance function");
            // UUID() is of the UUID type (11.x): its text
            case GUID -> writer.append("CAST(UUID() AS CHAR)");
            case RANDOM -> writer.append("RAND()");
            case FORMAT -> format(writer, a);
            case CURRENT_USER_FN -> writer.append("SUBSTRING_INDEX(CURRENT_USER(), '@', 1)");

            // ---- temporal
            case TODAY -> writer.append("UTC_DATE()");
            case NOW -> writer.append("UTC_TIMESTAMP(6)");
            case DATE_TRUNC_DAY -> writer.append("DATE(").expr(a.get(0), 0).append(")");
            case PARSE_DATE -> writer.piece(parsedDate(a.get(0)));
            case EXTRACT -> writer.piece(extract(a));
            case DATE_TRUNC -> writer.piece(dateTrunc(a));
            case MAKE_DATE -> writer.append("STR_TO_DATE(CONCAT_WS('-', ").list(a).append("), '%Y-%m-%d')");
            case MAKE_TIMESTAMP -> makeTimestamp(writer, a, c);
            // past the years 1000-9999 a date function answers NULL (measured): raised, never a NULL value
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> writer.append("(CASE WHEN ").expr(a.get(1), 4)
                    .append(" IS NULL OR ").expr(a.get(2), 4).append(" IS NULL THEN NULL ELSE coalesce(")
                    .piece(addInterval(a, c)).append(", ").piece(raise(List.of(new SqlExpr.StringLit(
                            "a date past the year 9999 has no " + product() + " value")), typeOf(c))).append(") END)");
            case DATE_DIFF -> writer.piece(dateDiff(a));
            case TIME_BUCKET -> writer.piece(timeBucket(a));
            // a zoned date-time is held as its naive UTC date-time: UTC's wall time is the instant's, either way
            case TIMEZONE -> timezone(writer, c, parentPrec);
            // TIMESTAMP(date, time): the date-time at that time of that day
            case DATE_AT_TIME -> writer.function("TIMESTAMP", c.args());
            case EPOCH_SECONDS -> writer.append("(").piece(micros(a.get(0))).append(" / 1000000)");
            case EPOCH_NS, FROM_EPOCH_NS -> throw new DialectCapability("a nanosecond epoch: MariaDB keeps"
                    + " microseconds (the Cypher island's nanosecond leg is DuckDB's)");
            case EPOCH_MS -> writer.append("(").piece(micros(a.get(0))).append(" DIV 1000)");
            case FROM_EPOCH_SECONDS -> writer.append("(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL (")
                    .expr(a.get(0), 0).append(") * 1000000 MICROSECOND)");
            case FROM_EPOCH_MS -> writer.append("(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL (").expr(a.get(0), 0)
                    .append(") * 1000 MICROSECOND)");
            case DAYNAME -> writer.append("DAYNAME(").expr(a.get(0), 0).append(")");
            case MONTHNAME -> writer.append("MONTHNAME(").expr(a.get(0), 0).append(")");
            case STRFTIME -> strftimeArm(writer, c);
            case STRPTIME -> throw wall(c.fn(), "a date parsed by a DuckDB format has no measured " + product() + " spelling");

            // ---- lists (JSON arrays; lambdas as correlated subqueries)
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION, LIST_DISTINCT, LIST_APPEND,
                 LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_BOOL_AND, LIST_BOOL_OR,
                 LIST_SORT, LIST_SORT_DESC, LIST_TAIL, LIST_INIT, LIST_SLICE, LIST_REVERSE, LIST_FLATTEN, LIST_ZIP,
                 LIST_PRODUCT, LIST_REDUCE, RANGE_FN, REPEAT_VALUE, TYPEOF -> lists(writer, c);

            // ---- maps: JSON objects (text keys)
            case MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_KEYS, MAP_VALUES, MAP_CONCAT ->
                    maps(writer, c);

            // ---- variant
            // in DuckDB's json_type vocabulary, which the lowering compares against
            case JSON_TYPE -> writer.append("(CASE JSON_TYPE(").expr(a.get(0), 0)
                    .append(") WHEN 'NULL' THEN 'NULL' WHEN 'STRING' THEN 'VARCHAR' WHEN 'BOOLEAN' THEN 'BOOLEAN'")
                    .append(" WHEN 'INTEGER' THEN 'BIGINT' WHEN 'DOUBLE' THEN 'DOUBLE' WHEN 'ARRAY' THEN 'ARRAY'")
                    .append(" WHEN 'OBJECT' THEN 'OBJECT' END)");
            case JSON_PRETTY -> writer.append("JSON_DETAILED(").expr(a.get(0), 0).append(")");
            case JSON_MERGE_PATCH -> writer.append("JSON_MERGE_PATCH(").list(a).append(")");
        };
    }

    /** The {@code ACOS} and {@code ASIN} arms of {@link #call}: undefined outside [-1, 1], raised. */
    private SqlWriter domainGuarded(SqlWriter writer, SqlExpr.Call c, String fn) {
        List<SqlExpr> a = c.args();
        return writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL THEN NULL ELSE coalesce(").append(fn)
                .append("(").expr(a.get(0), 0).append("), ")
                .piece(raise(List.of(new SqlExpr.StringLit(c.fn() + " is undefined outside [-1,1]")),
                        SqlType.Scalar.DOUBLE))
                .append(") END)");
    }

    /** The {@code LPAD} and {@code RPAD} arms of {@link #call}: a space by default. */
    private SqlWriter pad(SqlWriter writer, String fn, List<SqlExpr> a) {
        return writer.append(fn).append("(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(", ")
                .piece(a.size() > 2 ? w -> w.expr(a.get(2), 0) : w -> w.append("' '")).append(")");
    }

    /** The {@code EQUAL}, {@code NOT_EQUAL} arm of {@link #call}. */
    private SqlWriter equalArm(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 && (composite(a.get(0)) || composite(a.get(1)))) {
            return writer.append("(").piece(compact(w -> w.expr(a.get(0), 0)))
                    .append(c.fn() == SqlFn.EQUAL ? " = " : " <> ").piece(compact(w -> w.expr(a.get(1), 0)))
                    .append(")");
        }
        return infix(writer, c, parentPrec);
    }

    /** The {@code IN} arm of {@link #call}. */
    private SqlWriter in(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 && a.get(1) instanceof SqlExpr.PlanParam p) {
            throw listParameter(p);
        }
        return inList(writer, a, parentPrec);
    }

    /** The {@code INT_DIVIDE} arm of {@link #call}. */
    private SqlWriter intDivide(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.stream().anyMatch(MariaDb::isDouble)) {
            throw wall(c.fn(), "an integer division of a Float");
        }
        return writer.append("(").expr(a.get(0), 6).append(" DIV ").expr(a.get(1), 7).append(")");
    }

    /** The {@code TIMEZONE} arm of {@link #call}. */
    private SqlWriter timezone(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (!(a.get(0) instanceof SqlExpr.StringLit zone) || !UTC_ZONES.contains(zone.value())) {
            throw wall(c.fn(), product() + " knows no named time zone but UTC without its zone tables");
        }
        return writer.expr(a.get(1), parentPrec);
    }

    /** The {@code STRFTIME} arm of {@link #call}. */
    private SqlWriter strftimeArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit fl)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        return writer.piece(strftime(a.get(0), fl));
    }

    private static boolean composite(SqlExpr e) {
        SqlType t = typeOf(e);
        return t instanceof SqlType.Struct || t instanceof SqlType.Array || t instanceof SqlType.Map;
    }

    /** A raised error: SIGNAL runs only in a stored program, and a SELECT's conversion failures are warnings here; an
     *  XPath error carries its text but is raised when the statement is prepared (a constant XPath is compiled then,
     *  and any other is refused, measured), branch taken or not. So the message is kept in the session's user
     *  variable {@code @legend_raise} and a run-time error follows, raised only when evaluated (a CASE branch not
     *  taken raises nothing, measured): a scalar subquery of two rows, "Subquery returns more than 1 row". The
     *  platform reads the message back from the session it failed on ({@code exec.MariaDbConnector.raisedMessage}). */
    private SqlWriter.Piece raise(List<SqlExpr> a) {
        return raise(a, null);
    }

    /** {@link #raise(List)} typed as {@code slot}: its subquery is an integer, which a CASE or a COALESCE beside a
     *  date-time or a text would turn to text (measured: a date-time sum came back as its text). */
    private SqlWriter.Piece raise(List<SqlExpr> a, @com.legend.base.Nullable SqlType slot) {
        SqlWriter.Piece raised = raised(a);
        String type = castTarget(slot);
        return type == null ? raised : w -> w.append("CAST(").piece(raised).append(" AS " + type + ")");
    }

    /** The CAST target a value of type {@code t} is (null for none). */
    private @com.legend.base.Nullable String castTarget(@com.legend.base.Nullable SqlType t) {
        if (t instanceof SqlType.Decimal d) {
            return columnType(d);
        }
        if (!(t instanceof SqlType.Scalar s)) {
            return null;
        }
        return switch (s) {
            case BOOLEAN, INTEGER, BIGINT -> "SIGNED";
            case HUGEINT -> "DECIMAL(38,0)";
            case DOUBLE -> "DOUBLE";
            case DATE -> "DATE";
            case TIMESTAMP, TIMESTAMPTZ -> "DATETIME(6)";
            case TIME -> "TIME(6)";
            case VARCHAR, LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT, JSON -> "CHAR";
        };
    }

    /** A CASE whose branch is error() raises in the CASE's own type ({@link #raise(List, SqlType)}). */
    @Override
    protected SqlWriter caseExpr(SqlWriter writer, SqlExpr.Case c) {
        SqlType slot = typeOf(c);
        writer.append("CASE");
        for (SqlExpr.Case.When w : c.whens()) {
            writer.append(" WHEN ").expr(w.condition(), 0).append(" THEN ").piece(branch(w.then(), slot));
        }
        if (c.otherwise() != null) {
            writer.append(" ELSE ").piece(branch(java.util.Objects.requireNonNull(c.otherwise(), "else"), slot));
        }
        return writer.append(" END");
    }

    private SqlWriter.Piece branch(SqlExpr e, @com.legend.base.Nullable SqlType slot) {
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.ERROR) {
            return raise(c.args(), slot != null ? slot : typeOf(c));
        }
        return w -> w.expr(e, 0);
    }

    private SqlWriter.Piece raised(List<SqlExpr> a) {
        return twoRows(w -> {
            w.append("CONCAT(CHAR(31 USING utf8mb4), ");
            if (a.size() > 1) {
                w.expr(a.get(1), 0).append(", CHAR(30 USING utf8mb4), ");
            }
            w.expr(a.get(0), 0).append(", CHAR(31 USING utf8mb4))");
        });
    }

    /** A scalar subquery of two rows that keeps {@code message} in {@code @legend_raise} as it runs, so it fails
     *  ("Subquery returns more than 1 row") only where it is reached: its rows a table function's. */
    protected SqlWriter.Piece twoRows(SqlWriter.Piece message) {
        return w -> w.append("(SELECT 1 FROM JSON_TABLE(JSON_ARRAY(@legend_raise := ").piece(message)
                .append(", 0), '$[*]' COLUMNS (\"x\" LONGTEXT PATH '$')) AS __r)");
    }

    /** Pure's mod (the divisor's sign) and rem (the dividend's): MOD is the remainder here, the dividend's sign. */
    private static SqlWriter.Piece remainder(SqlFn fn, List<SqlExpr> a) {
        SqlWriter.Piece rem = w -> w.append("MOD(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
        if (fn == SqlFn.REM) {
            return rem;
        }
        return w -> w.append("MOD(").piece(rem).append(" + ").expr(a.get(1), 6).append(", ").expr(a.get(1), 0)
                .append(")");
    }

    /** DuckDB's round: half away from zero; a double by DuckDB's own algorithm, {@code round(x * 10^n) / 10^n} with
     *  the half away from zero (MariaDB rounds a double's half to even, measured: ROUND(2.5E0) is 2); a DECIMAL's
     *  ROUND is already half away from zero. */
    private SqlWriter roundHalfUp(SqlWriter writer, List<SqlExpr> a) {
        if (!isDouble(a.get(0))) {
            return writer.append("ROUND(").list(a).append(")");
        }
        SqlWriter.Piece scale = a.size() > 1 ? w -> w.append("POW(10, ").expr(a.get(1), 0).append(")")
                : w -> w.append("1");
        return writer.append("(SIGN(").expr(a.get(0), 0).append(") * FLOOR(ABS(").expr(a.get(0), 0).append(") * ")
                .piece(scale).append(" + 0.5) / ").piece(scale).append(")");
    }

    /** DuckDB's 64-bit parse: an optional sign and digits, else an error (a CAST reads 'abc' as 0 here). */
    private SqlWriter.Piece parseInt(SqlExpr s) {
        return w -> w.append("(CASE WHEN ").expr(s, 4).append(" IS NULL THEN NULL WHEN TRIM(").expr(s, 0)
                .append(") REGEXP '^[+-]?[0-9]+$' THEN CAST(TRIM(").expr(s, 0).append(") AS SIGNED) ELSE ")
                .piece(raise(List.of(SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("Could not convert string '"),
                        s, new SqlExpr.StringLit("' to INT64"))), SqlType.Scalar.BIGINT)).append(" END)");
    }

    /** GREATEST/LEAST over the arguments, each replaced by the next non-NULL one when it is NULL: NULL only when every
     *  argument is. */
    private static SqlWriter.Piece nullSkipping(String fn, List<SqlExpr> a) {
        if (a.size() == 1) {
            return w -> w.expr(a.get(0), 0);
        }
        return w -> {
            w.append(fn).append("(");
            for (int i = 0; i < a.size(); i++) {
                w.append(i > 0 ? ", coalesce(" : "coalesce(").expr(a.get(i), 0);
                for (int j = 0; j < a.size(); j++) {
                    if (j != i) {
                        w.append(", ").expr(a.get(j), 0);
                    }
                }
                w.append(")");
            }
            w.append(")");
        };
    }

    /** DuckDB's left/right: a negative count is all but that many. */
    private static SqlWriter.Piece leftRight(SqlFn fn, List<SqlExpr> a) {
        if (fn == SqlFn.LEFT) {
            return w -> w.append("(CASE WHEN ").expr(a.get(1), 4).append(" < 0 THEN LEFT(").expr(a.get(0), 0)
                    .append(", GREATEST(CHAR_LENGTH(").expr(a.get(0), 0).append(") + ").expr(a.get(1), 6)
                    .append(", 0)) ELSE LEFT(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(") END)");
        }
        return w -> w.append("(CASE WHEN ").expr(a.get(1), 4).append(" < 0 THEN RIGHT(").expr(a.get(0), 0)
                .append(", GREATEST(CHAR_LENGTH(").expr(a.get(0), 0).append(") + ").expr(a.get(1), 6)
                .append(", 0)) ELSE RIGHT(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(") END)");
    }

    /** DuckDB's trim, ltrim, rtrim: spaces, or every leading/trailing character of the given set (a regular
     *  expression's class: MariaDB's TRIM removes a string, not a set). */
    private SqlWriter.Piece trim(SqlFn fn, List<SqlExpr> a) {
        if (a.size() == 1) {
            String name = fn == SqlFn.TRIM ? "TRIM" : fn == SqlFn.LTRIM ? "LTRIM" : "RTRIM";
            return w -> w.append(name + "(").expr(a.get(0), 0).append(")");
        }
        String chars = literal(a.get(1), "a trim's character set");
        StringBuilder cls = new StringBuilder("[");
        for (char ch : chars.toCharArray()) {
            cls.append("\\]^-[".indexOf(ch) >= 0 ? "\\" + ch : String.valueOf(ch));
        }
        cls.append("]");
        String p = fn == SqlFn.TRIM ? "^" + cls + "+|" + cls + "+$" : fn == SqlFn.LTRIM ? "^" + cls + "+" : cls + "+$";
        return w -> w.append("REGEXP_REPLACE(").expr(a.get(0), 0).append(", " + stringLit(p) + ", '')");
    }

    /** printf, through MariaDB's SFORMAT (its fmt codes): each conversion of a literal format translated (a literal
     *  brace doubled), a {@code %s} argument as its text. */
    protected SqlWriter format(SqlWriter writer, List<SqlExpr> a) {
        String f = literal(a.get(0), "a format");
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("%([-+ #0]*)([0-9]*)(?:\\.([0-9]+))?([a-zA-Z%])")
                .matcher(f);
        StringBuilder out = new StringBuilder();
        List<SqlWriter.Piece> args = new ArrayList<>();
        int arg = 1;
        int last = 0;
        while (m.find()) {
            out.append(f.substring(last, m.start()).replace("{", "{{").replace("}", "}}"));
            last = m.end();
            char conversion = m.group(4).charAt(0);
            if (conversion == '%') {
                out.append('%');
                continue;
            }
            String flags = m.group(1);
            String precision = m.group(3);
            String spec = (flags.contains("-") ? "<" : "") + (flags.contains("+") ? "+" : "")
                    + (flags.contains("0") ? "0" : "") + m.group(2) + (precision != null ? "." + precision : "")
                    + switch (conversion) {
                        case 'd', 'i' -> "d";
                        case 'f', 'F' -> "f";
                        case 'e', 'E', 'g', 'G', 'x', 'X', 'o' -> String.valueOf(conversion);
                        case 's' -> "";
                        default -> throw new DialectCapability("printf conversion %" + conversion + " reached " + product());
                    };
            out.append(spec.isEmpty() ? "{}" : "{:" + spec + "}");
            if (arg < a.size()) {
                SqlExpr x = a.get(arg);
                args.add(conversion == 's' ? asText(x) : w -> w.expr(x, 0));
            }
            arg++;
        }
        out.append(f.substring(last).replace("{", "{{").replace("}", "}}"));
        writer.append("SFORMAT(" + stringLit(out.toString()));
        for (SqlWriter.Piece p : args) {
            writer.append(", ").piece(p);
        }
        return writer.append(")");
    }

    // ---- regular expressions: PCRE, literal patterns

    private String patternOf(List<SqlExpr> a, SqlFn fn) {
        if (!(a.get(1) instanceof SqlExpr.StringLit p)
                || a.size() > 2 && fn != SqlFn.REGEXP_EXTRACT && fn != SqlFn.REGEXP_EXTRACT_ALL) {
            throw wall(fn, "a pattern that is not a literal, or options beside it");
        }
        return p.value();
    }

    private SqlWriter.Piece pattern(List<SqlExpr> a, SqlFn fn, boolean full) {
        String p = patternOf(a, fn);
        return w -> w.append(stringLit(full ? "^(?:" + p + ")\\z" : p));
    }

    private int group(List<SqlExpr> a) {
        if (a.size() < 3) {
            return 0;
        }
        if (a.get(2) instanceof SqlExpr.IntLit g && g.value() >= 0 && g.value() <= 9) {
            return (int) g.value();
        }
        throw new DialectCapability("a regexp group that is not a literal 0-9 reached " + product());
    }

    /** DuckDB's regexp_extract: the leftmost match's group ('' when none matches; NULL for NULL). A group's text is
     *  the text rewritten to it by an anchored pattern: a lazy prefix to the leftmost match, the rest consumed. */
    private SqlWriter.Piece regexpExtract(List<SqlExpr> a) {
        String p = patternOf(a, SqlFn.REGEXP_EXTRACT);
        int g = group(a);
        if (g == 0) {
            return w -> w.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL THEN NULL ELSE REGEXP_SUBSTR(")
                    .expr(a.get(0), 0).append(", " + stringLit(p) + ") END)");
        }
        return w -> w.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL THEN NULL WHEN ").expr(a.get(0), 4)
                .append(" REGEXP " + stringLit(p) + " THEN REGEXP_REPLACE(").expr(a.get(0), 0)
                .append(", " + stringLit("^[\\s\\S]*?(?:" + p + ")[\\s\\S]*\\z") + ", " + stringLit(groupRef(g))
                        + ") ELSE '' END)");
    }

    /**
     * DuckDB's regexp_extract_all: each non-overlapping match's group, left to right; NULL for NULL. The k-th match is
     * the one after k - 1 matches, each a lazy prefix and a match: {@code ^(?:[\s\S]*?(?:p)){k-1}[\s\S]*?(p)}, a
     * pattern built per row (a dynamic pattern is compiled per row here); k runs over a correlated row generator, as
     * far as the text has matches. The k-th's groups follow the repeated ones (the last repeat's): group g is
     * {@code \(m + 1 + g)} for a pattern of m groups.
     */
    private SqlWriter.Piece regexpExtractAll(List<SqlExpr> a) {
        String p = patternOf(a, SqlFn.REGEXP_EXTRACT_ALL);
        int g = group(a);
        int ref = captureGroups(p) + 1 + g;
        if (ref > 9) {
            throw new DialectCapability("regexp_extract_all of group " + g + " of a pattern of " + captureGroups(p)
                    + " groups reached " + product() + " (a replacement names groups 1-9)");
        }
        String repeat = stringLit("^(?:[\\s\\S]*?(?:" + p + ")){");
        SqlWriter.Piece k = w -> w.append("__k.\"__n\"");
        SqlWriter.Piece kth = w -> w.append("REGEXP_REPLACE(").expr(a.get(0), 0).append(", CONCAT(" + repeat + ", ")
                .piece(k).append(" - 1, " + stringLit("}[\\s\\S]*?(" + p + ")[\\s\\S]*\\z") + "), "
                        + stringLit(groupRef(ref)) + ")");
        return w -> w.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL THEN NULL ELSE coalesce((SELECT ")
                .piece(arrayAgg(false, kth, k, false)).append(" FROM ")
                .piece(rows(v -> v.append("CHAR_LENGTH(").expr(a.get(0), 0).append(") + 1"), "__k"))
                .append(" WHERE ").expr(a.get(0), 4).append(" REGEXP CONCAT(" + repeat + ", ").piece(k)
                .append(", '}')), JSON_ARRAY()) END)");
    }

    /** The capturing groups of a pattern: each '(' not escaped, not in a class, not a '(?' group (but a named one). */
    private static int captureGroups(String p) {
        int n = 0;
        boolean inClass = false;
        for (int i = 0; i < p.length(); i++) {
            char ch = p.charAt(i);
            if (ch == '\\') {
                i++;
            } else if (inClass) {
                inClass = ch != ']';
            } else if (ch == '[') {
                inClass = true;
            } else if (ch == '(' && (i + 1 >= p.length() || p.charAt(i + 1) != '?' || p.startsWith("(?<", i)
                    && i + 3 < p.length() && p.charAt(i + 3) != '=' && p.charAt(i + 3) != '!' || p.startsWith("(?P<", i))) {
                n++;
            }
        }
        return n;
    }

    /**
     * DuckDB's regexp_replace(s, p, r[, options]): the first match unless the options say 'g' (MariaDB's replaces
     * each). The first is the only match of the pattern anchored after a lazy prefix, its groups one place on (the
     * prefix is group 1, the match group 2): {@code \0} is group 2, {@code \N} group N + 2.
     */
    protected SqlWriter.Piece regexpReplace(List<SqlExpr> a) {
        String p = literal(a.get(1), "a regexp_replace pattern");
        String r = literal(a.get(2), "a regexp_replace replacement");
        String options = a.size() > 3 ? literal(a.get(3), "regexp_replace options") : "";
        boolean global = false;
        String flags = "";
        for (char o : options.toCharArray()) {
            switch (o) {
                case 'g' -> global = true;
                case 'i' -> flags = "(?i)";
                case 'c' -> flags = "";
                default -> throw new DialectCapability("regexp_replace option '" + o + "' reached " + product());
            }
        }
        if (global) {
            String pat = flags + p;
            return w -> w.append("REGEXP_REPLACE(").expr(a.get(0), 0).append(", " + stringLit(pat) + ", "
                    + stringLit(r) + ")");
        }
        StringBuilder shifted = new StringBuilder("\\1");
        for (int i = 0; i < r.length(); i++) {
            char ch = r.charAt(i);
            if (ch == '\\' && i + 1 < r.length() && Character.isDigit(r.charAt(i + 1))) {
                int k = r.charAt(++i) - '0';
                if (k + 2 > 9) {
                    throw new DialectCapability("regexp_replace of group " + k + " (first match only) reached " + product());
                }
                shifted.append('\\').append(k + 2);
            } else {
                shifted.append(ch);
            }
        }
        String pat = flags + "^([\\s\\S]*?)(" + p + ")";
        return w -> w.append("REGEXP_REPLACE(").expr(a.get(0), 0).append(", " + stringLit(pat) + ", "
                + stringLit(shifted.toString()) + ")");
    }

    private DialectCapability wall(SqlFn fn, String why) {
        return new DialectCapability(fn + " reached " + product() + ": " + why);
    }

    protected String literal(SqlExpr e, String what) {
        if (e instanceof SqlExpr.StringLit s) {
            return s.value();
        }
        throw new DialectCapability(what + " that is not a literal reached " + product());
    }

    // ---- list calls

    private SqlWriter lists(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String e = param("__e");
        String o = param("__o");
        return switch (c.fn()) {
            case LIST_FILTER -> {
                SqlExpr.Lambda l = lambdaOf(a.get(1), c.fn());
                String x = l.params().get(0);
                SqlType el = listElement(a.get(0));
                yield writer.piece(overElements(a.get(0), x, offsetOf(l), indexOf(l), jsonOf(w -> w.append(param(x)),
                        el), w -> w.expr(lambdaBody(l, el), 0)));
            }
            case LIST_TRANSFORM -> {
                SqlExpr.Lambda l = lambdaOf(a.get(1), c.fn());
                SqlExpr body = lambdaBody(l, listElement(a.get(0)));
                yield writer.piece(overElements(a.get(0), l.params().get(0), offsetOf(l), indexOf(l), json(body),
                        null));
            }
            // NULL ++ xs is xs, as DuckDB's list_concat; JSON_MERGE_PRESERVE appends arrays
            case LIST_CONCAT -> {
                writer.append("(CASE WHEN ");
                for (int i = 0; i < a.size(); i++) {
                    writer.append(i > 0 ? " AND " : "").expr(a.get(i), 4).append(" IS NULL");
                }
                writer.append(" THEN NULL ELSE JSON_MERGE_PRESERVE(");
                for (int i = 0; i < a.size(); i++) {
                    writer.append(i > 0 ? ", " : "").append("coalesce(").expr(a.get(i), 0).append(", JSON_ARRAY())");
                }
                yield writer.append(") END)");
            }
            // 1-based, negative from the end, 0 and out of range NULL (DuckDB's list_extract)
            case LIST_GET -> {
                SqlWriter.Piece at = elementAt(a.get(0), w -> w.append("CASE WHEN ").expr(a.get(1), 4)
                        .append(" < 0 THEN ").expr(a.get(1), 0).append(" ELSE ").expr(a.get(1), 6).append(" - 1 END"));
                // a list whose elements claim no type (an empty list's NULL): JSON_VALUE of a NULL is binary, so
                // the element is cast to the call's own type
                String type = listElement(a.get(0)) == null ? castTarget(typeOf(c)) : null;
                writer.append("(CASE WHEN ").expr(a.get(1), 4).append(" = 0 THEN NULL ELSE ");
                if (type == null) {
                    writer.piece(at);
                } else {
                    writer.append("CAST(").piece(at).append(" AS " + type + ")");
                }
                yield writer.append(" END)");
            }
            // DuckDB's list_position: 1-based, NULL when absent, a NULL needle found by IS
            case LIST_POSITION -> writer.append("(SELECT min(" + o + ") FROM ")
                    .piece(elements(a.get(0), "__e", "__o", null)).append(" WHERE " + e + " <=> ").expr(a.get(1), 5)
                    .append(")");
            // DuckDB's list_distinct drops NULLs; the first occurrence's order
            case LIST_DISTINCT -> {
                SqlType el = listElement(a.get(0));
                String e2 = param("__e2");
                yield writer.piece(overElements(a.get(0), "__e", "__o", null, jsonOf(v -> v.append(e), el),
                        w -> w.append(e + " IS NOT NULL AND NOT EXISTS(SELECT 1 FROM ")
                                .piece(elements(a.get(0), "__e2", "__o2", null))
                                .append(" WHERE " + e2 + " = " + e + " AND " + param("__o2") + " < " + o + ")")));
            }
            // list_append(NULL, v) is [v]
            case LIST_APPEND -> writer.append("JSON_ARRAY_APPEND(coalesce(").expr(a.get(0), 0).append(", JSON_ARRAY()),"
                    + " '$', ").piece(json(a.get(1))).append(")");
            case LIST_SUM -> writer.piece(overRows(a.get(0), SqlAgg.Fn.SUM, List.of()));
            case LIST_MIN -> writer.piece(elementReduction(c, SqlAgg.Fn.MIN));
            case LIST_MAX -> writer.piece(elementReduction(c, SqlAgg.Fn.MAX));
            case LIST_AVG -> writer.piece(overRows(a.get(0), SqlAgg.Fn.AVG, List.of()));
            case LIST_MEDIAN -> writer.piece(overRows(a.get(0), SqlAgg.Fn.MEDIAN, List.of()));
            case LIST_MODE -> writer.piece(elementReduction(c, SqlAgg.Fn.MODE));
            case LIST_BOOL_AND -> writer.piece(overRows(a.get(0), SqlAgg.Fn.BOOL_AND, List.of()));
            case LIST_BOOL_OR -> writer.piece(overRows(a.get(0), SqlAgg.Fn.BOOL_OR, List.of()));
            // ascending / descending, NULLs last both ways (DuckDB's list_sort / list_reverse_sort); a struct by its
            // fields in declared order
            case LIST_SORT, LIST_SORT_DESC -> {
                boolean asc = c.fn() == SqlFn.LIST_SORT;
                SqlType el = listElement(a.get(0));
                yield writer.piece(unlessNull(a.get(0), w -> w.append("coalesce((SELECT ")
                        .piece(arrayAgg(false, jsonOf(v -> v.append(e), el), elementSort(e, el, asc), false))
                        .append(" FROM ").piece(elements(a.get(0), "__e", "__o", null)).append("), JSON_ARRAY())")));
            }
            case LIST_REVERSE -> writer.piece(unlessNull(a.get(0), w -> w.append("coalesce((SELECT ")
                    .piece(arrayAgg(false, jsonOf(v -> v.append(e), listElement(a.get(0))), v -> v.append(o + " DESC"),
                            false)).append(" FROM ").piece(elements(a.get(0), "__e", "__o", null))
                    .append("), JSON_ARRAY())")));
            case LIST_TAIL -> writer.piece(overElements(a.get(0), "__e", "__o", null,
                    jsonOf(w -> w.append(e), listElement(a.get(0))), w -> w.append(o + " > 1")));
            case LIST_INIT -> writer.piece(overElements(a.get(0), "__e", "__o", null,
                    jsonOf(w -> w.append(e), listElement(a.get(0))), w -> w.append(o + " < JSON_LENGTH(")
                            .expr(a.get(0), 0).append(")")));
            // DuckDB's list_slice: 1-based, both ends inclusive, a negative end from the end
            case LIST_SLICE -> writer.piece(overElements(a.get(0), "__e", "__o", null,
                    jsonOf(w -> w.append(e), listElement(a.get(0))), w -> w.append(o + " >= ")
                            .piece(sliceEnd(a.get(0), a.get(1))).append(" AND " + o + " <= ")
                            .piece(sliceEnd(a.get(0), a.get(2)))));
            case LIST_FLATTEN -> {
                SqlType inner = listElement(a.get(0)) instanceof SqlType.Array arr ? arr.element() : null;
                yield writer.piece(unlessNull(a.get(0), w -> w.append("coalesce((SELECT ")
                        .piece(arrayAgg(false, jsonOf(v -> v.append("__f.\"v\""), inner),
                                v -> v.append("__f.\"o\", __f.\"i\""), false)).append(" FROM ")
                        .piece(jsonTable(v -> v.expr(a.get(0), 0), "'$[*]' COLUMNS (\"o\" FOR ORDINALITY,"
                                + " NESTED PATH '$[*]' COLUMNS (\"i\" FOR ORDINALITY, \"v\" " + columnType(inner)
                                + " PATH '$'))", "__f")).append(" WHERE __f.\"i\" IS NOT NULL), JSON_ARRAY())")));
            }
            case LIST_ZIP -> writer.piece(zip(a, c));
            case LIST_REDUCE -> writer.piece(listReduce(c));
            case LIST_PRODUCT -> throw wall(c.fn(), "a list's cartesian product has no " + product() + " spelling");
            // DuckDB's range: [start, stop) by step (start 0, step 1 by default)
            case RANGE_FN -> range(writer, a);
            case REPEAT_VALUE -> writer.append("(CASE WHEN ").expr(a.get(1), 4).append(" > 0 THEN (SELECT JSON_ARRAYAGG(")
                    .piece(json(a.get(0))).append(") FROM ").piece(rows(w -> w.expr(a.get(1), 0), "__g"))
                    .append(") ELSE JSON_ARRAY() END)");
            case TYPEOF -> throw wall(c.fn(), "its type names are DuckDB's");
            default -> throw new IllegalStateException("not a list call: " + c.fn());
        };
    }

    /** DuckDB's list_zip: a struct per position of the longest list, a shorter one's field NULL there; the fields
     *  named as the result's struct type names them. */
    private SqlWriter.Piece zip(List<SqlExpr> a, SqlExpr.Call c) {
        if (!(listElement(c) instanceof SqlType.Struct st) || st.fields().size() != a.size()) {
            throw wall(c.fn(), "a zip of unknown struct type");
        }
        SqlWriter.Piece object = w -> {
            w.append("JSON_OBJECT(");
            for (int i = 0; i < a.size(); i++) {
                int at = i;
                w.append(i > 0 ? ", " : "").append(stringLit(st.fields().get(i).name())).append(", ")
                        .piece(jsonOf(elementAt(a.get(i), v -> v.append("__z.\"__n\" - 1")), st.fields().get(at).type()));
            }
            w.append(")");
        };
        return w -> {
            w.append("coalesce((SELECT ").piece(arrayAgg(false, object, v -> v.append("__z.\"__n\""), false))
                    .append(" FROM ").piece(rows(v -> {
                v.append("GREATEST(");
                for (int i = 0; i < a.size(); i++) {
                    v.append(i > 0 ? ", " : "").append("coalesce(JSON_LENGTH(").expr(a.get(i), 0).append("), 0)");
                }
                v.append(")");
            }, "__z")).append(" WHERE __z.\"__n\" <= GREATEST(");
            for (int i = 0; i < a.size(); i++) {
                w.append(i > 0 ? ", " : "").append("coalesce(JSON_LENGTH(").expr(a.get(i), 0).append("), 0)");
            }
            w.append(")), JSON_ARRAY())");
        };
    }

    /** DuckDB's range: [start, stop) by step, a row per value. */
    private SqlWriter range(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece start = a.size() == 1 ? w -> w.append("0") : w -> w.expr(a.get(0), 0);
        SqlExpr last = a.get(a.size() == 1 ? 0 : 1);
        SqlWriter.Piece stop = w -> w.expr(last, 0);
        SqlWriter.Piece step = a.size() > 2 ? w -> w.expr(a.get(2), 0) : w -> w.append("1");
        SqlWriter.Piece count = w -> w.append("CEIL(((").piece(stop).append(") - (").piece(start).append(")) / (")
                .piece(step).append("))");
        return writer.append("(CASE WHEN ((").piece(stop).append(") - (").piece(start).append(")) * (").piece(step)
                .append(") > 0 THEN (SELECT ").piece(arrayAgg(false, v -> v.append("(").piece(start)
                        .append(") + (__g.\"__n\" - 1) * (").piece(step).append(")"), v -> v.append("__g.\"__n\""), false))
                .append(" FROM ").piece(rows(count, "__g"))
                .append(") ELSE JSON_ARRAY() END)");
    }

    /** A slice bound, 1-based: a negative one counts from the end. */
    private static SqlWriter.Piece sliceEnd(SqlExpr xs, SqlExpr i) {
        return w -> w.append("(CASE WHEN ").expr(i, 4).append(" < 0 THEN JSON_LENGTH(").expr(xs, 0)
                .append(") + ").expr(i, 6).append(" + 1 ELSE ").expr(i, 0).append(" END)");
    }

    /** The ORDER BY of a list's elements, NULLs last: a struct by its fields in declared order. */
    private SqlWriter.Piece elementSort(String x, @com.legend.base.Nullable SqlType el, boolean asc) {
        if (el instanceof SqlType.Struct st) {
            return w -> {
                for (int i = 0; i < st.fields().size(); i++) {
                    SqlType.Struct.Field f = st.fields().get(i);
                    String field = jsonText(x, memberPath(f.name()));
                    w.append(i > 0 ? ", " : "").piece(elementSortOf(scalarOfText(v -> v.append(field), f.type()), asc));
                }
            };
        }
        return elementSortOf(w -> w.append(x), asc);
    }

    /** One sort key, NULLs last, its NULL placement in its direction ({@link #sortKey}). */
    private static SqlWriter.Piece elementSortOf(SqlWriter.Piece x, boolean asc) {
        return asc ? w -> w.append("(").piece(x).append(" IS NULL), ").piece(x)
                : w -> w.append("(").piece(x).append(" IS NOT NULL) DESC, ").piece(x).append(" DESC");
    }

    // ---- map calls: JSON objects (text keys)

    private SqlWriter maps(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlType value = typeOf(c) instanceof SqlType.Map m ? m.value()
                : typeOf(a.isEmpty() ? c : a.get(0)) instanceof SqlType.Map m2 ? m2.value() : null;
        return switch (c.fn()) {
            case MAP_EMPTY -> writer.append("JSON_OBJECT()");
            case MAP_FROM_LISTS -> {
                SqlType v = listElement(a.get(1));
                yield writer.append("(SELECT JSON_OBJECTAGG(__k.\"k\", ").piece(jsonOf(w -> w.append("__v.\"v\""), v))
                        .append(") FROM ").piece(jsonTable(x -> x.expr(a.get(0), 0), "'$[*]' COLUMNS (\"o\" FOR ORDINALITY,"
                                + " \"k\" " + columnType(SqlType.Scalar.VARCHAR) + " PATH '$')", "__k"))
                        .append(" LEFT JOIN ").piece(jsonTable(x -> x.expr(a.get(1), 0), "'$[*]' COLUMNS (\"o\" FOR"
                                + " ORDINALITY, \"v\" " + columnType(v) + " PATH '$')", "__v"))
                        .append(" ON __v.\"o\" = __k.\"o\")");
            }
            // DuckDB's map_from_entries: each entry a struct of (key, value), by position whatever their names
            case MAP_FROM_ENTRIES -> {
                if (!(listElement(a.get(0)) instanceof SqlType.Struct entry) || entry.fields().size() != 2) {
                    throw wall(c.fn(), "entries of " + listElement(a.get(0)));
                }
                SqlType vt = entry.fields().get(1).type();
                yield writer.append("(SELECT JSON_OBJECTAGG(__e.\"k\", ").piece(jsonOf(w -> w.append("__e.\"v\""), vt))
                        .append(") FROM ").piece(jsonTable(x -> x.expr(a.get(0), 0), "'$[*]' COLUMNS (\"k\" "
                                + columnType(SqlType.Scalar.VARCHAR) + " PATH " + memberPath(entry.fields().get(0).name())
                                + ", \"v\" " + columnType(vt) + " PATH " + memberPath(entry.fields().get(1).name())
                                + ")", "__e")).append(")");
            }
            // DuckDB's map_extract: a list of the key's value, empty when absent
            case MAP_EXTRACT -> writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL THEN NULL WHEN")
                    .append(" JSON_CONTAINS_PATH(").expr(a.get(0), 0).append(", 'one', CONCAT('$.\"', ").expr(a.get(1), 0)
                    .append(", '\"')) THEN JSON_ARRAY(JSON_EXTRACT(").expr(a.get(0), 0).append(", CONCAT('$.\"', ")
                    .expr(a.get(1), 0).append(", '\"'))) ELSE JSON_ARRAY() END)");
            case MAP_KEYS -> writer.piece(unlessNull(a.get(0), w -> w.append("JSON_KEYS(").expr(a.get(0), 0)
                    .append(")")));
            case MAP_VALUES -> writer.piece(unlessNull(a.get(0), w -> w.append("coalesce(JSON_EXTRACT(")
                    .expr(a.get(0), 0).append(", '$.*'), JSON_ARRAY())")));
            // the later map's keys win, as DuckDB's map_concat
            case MAP_CONCAT -> writer.append("JSON_MERGE_PATCH(").list(a).append(")");
            default -> throw new IllegalStateException("not a map call: " + c.fn());
        };
    }

    // ---- temporal helpers: a date is a DATE, a date-time a DATETIME(6) (microseconds)

    private static final Set<String> UTC_ZONES = Set.of("UTC", "GMT", "Etc/UTC", "Z");

    /** A date-time text ('T' or a space between date and time) with an offset ('Z', '+HH:MM', '+HHMM') as its UTC
     *  date-time: the offset taken off in minutes (CONVERT_TZ converts only inside TIMESTAMP's range); one with none as
     *  it is. NULL for a text that is no date-time (a CAST's failure is NULL here). */
    private static SqlWriter.Piece utcOf(SqlWriter.Piece text) {
        SqlWriter.Piece t = w -> w.append("REPLACE(TRIM(").piece(text).append("), 'T', ' ')");
        SqlWriter.Piece len = w -> w.append("CHAR_LENGTH(").piece(t).append(")");
        java.util.function.BiFunction<SqlWriter.Piece, Integer, SqlWriter.Piece> shifted = (off, cut) -> w -> w
                .append("(CAST(LEFT(").piece(t).append(", ").piece(len).append(" - " + cut + ") AS DATETIME(6)) - INTERVAL")
                .append(" (CASE LEFT(").piece(off).append(", 1) WHEN '-' THEN -1 ELSE 1 END) * (CAST(SUBSTRING(")
                .piece(off).append(", 2, 2) AS SIGNED) * 60 + CAST(RIGHT(").piece(off).append(", 2) AS SIGNED)) MINUTE)");
        SqlWriter.Piece off6 = w -> w.append("RIGHT(").piece(t).append(", 6)");
        SqlWriter.Piece off5 = w -> w.append("RIGHT(").piece(t).append(", 5)");
        return w -> w.append("(CASE WHEN ").piece(t).append(" LIKE '%Z' THEN CAST(LEFT(").piece(t).append(", ").piece(len)
                .append(" - 1) AS DATETIME(6)) WHEN ").piece(t).append(" REGEXP '[+-][0-9]{2}:[0-9]{2}$' THEN ")
                .piece(shifted.apply(off6, 6)).append(" WHEN ").piece(len).append(" > 10 AND ").piece(t)
                .append(" REGEXP '[0-9][+-][0-9]{4}$' THEN ").piece(shifted.apply(off5, 5)).append(" ELSE CAST(")
                .piece(t).append(" AS DATETIME(6)) END)");
    }

    /** Pure's parseDate of a text: its date-time ({@link #utcOf}); a text that is no date raises. */
    private SqlWriter.Piece parsedDate(SqlExpr x) {
        return w -> w.append("(CASE WHEN ").expr(x, 4).append(" IS NULL THEN NULL ELSE coalesce(")
                .piece(utcOf(v -> v.expr(x, 0))).append(", ").piece(raise(List.of(SqlExpr.Call.of(SqlFn.CONCAT,
                        new SqlExpr.StringLit("Could not parse '"), x, new SqlExpr.StringLit("' as a date"))),
                        SqlType.Scalar.TIMESTAMP))
                .append(") END)");
    }

    /** A date-time's microseconds since the epoch (exact, any year: TIMESTAMPDIFF, not UNIX_TIMESTAMP's range). */
    private static SqlWriter.Piece micros(SqlExpr t) {
        return w -> w.append("TIMESTAMPDIFF(MICROSECOND, TIMESTAMP '1970-01-01 00:00:00', ").expr(t, 0).append(")");
    }

    /** A date-time of the given microseconds since the epoch. */
    private static SqlWriter.Piece fromMicros(SqlWriter.Piece us) {
        return w -> w.append("(TIMESTAMP '1970-01-01 00:00:00' + INTERVAL (").piece(us).append(") MICROSECOND)");
    }

    /** A date part of a date, a Pure integer: the ISO day of the week Monday 1 to Sunday 7, the week the ISO week. */
    private SqlWriter.Piece extract(List<SqlExpr> a) {
        DatePart part = part(a.get(0));
        SqlExpr t = a.get(1);
        if (part == DatePart.YEAR) {
            return yearOf(t);
        }
        String fn = switch (part) {
            case YEAR -> "YEAR(";
            case QUARTER -> "QUARTER(";
            case MONTH -> "MONTH(";
            case DAY -> "DAYOFMONTH(";
            case HOUR -> "HOUR(";
            case MINUTE -> "MINUTE(";
            case SECOND -> "SECOND(";
            case DAY_OF_YEAR -> "DAYOFYEAR(";
            case MICROSECOND -> "MICROSECOND(";
            case WEEK, MILLISECOND, NANOSECOND, ISO_DAY_OF_WEEK -> null;
        };
        if (fn != null) {
            return w -> w.append(fn).expr(t, 0).append(")");
        }
        return switch (part) {
            case WEEK -> w -> w.append("WEEK(").expr(t, 0).append(", 3)");
            case MILLISECOND -> w -> w.append("(MICROSECOND(").expr(t, 0).append(") DIV 1000)");
            case ISO_DAY_OF_WEEK -> w -> w.append("(WEEKDAY(").expr(t, 0).append(") + 1)");
            case NANOSECOND -> throw new DialectCapability("date part 'nanosecond' has no measured " + product()
                    + " spelling");
            case YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND, DAY_OF_YEAR, MICROSECOND ->
                    throw new IllegalStateException("named: " + part);
        };
    }

    /** A date's year, an integer. */
    protected SqlWriter.Piece yearOf(SqlExpr t) {
        return w -> w.append("YEAR(").expr(t, 0).append(")");
    }

    /** The base's contract: year/quarter/month/week deliver a DATE, finer parts a date-time; DuckDB's week starts on
     *  Monday. */
    private SqlWriter.Piece dateTrunc(List<SqlExpr> a) {
        DatePart part = part(a.get(0));
        SqlExpr t = a.get(1);
        return switch (part) {
            case YEAR -> w -> w.append("MAKEDATE(YEAR(").expr(t, 0).append("), 1)");
            case QUARTER -> w -> w.append("(MAKEDATE(YEAR(").expr(t, 0).append("), 1) + INTERVAL (QUARTER(").expr(t, 0)
                    .append(") - 1) QUARTER)");
            case MONTH -> w -> w.append("(DATE(").expr(t, 0).append(") - INTERVAL (DAYOFMONTH(").expr(t, 0)
                    .append(") - 1) DAY)");
            case WEEK -> w -> w.append("(DATE(").expr(t, 0).append(") - INTERVAL WEEKDAY(").expr(t, 0).append(") DAY)");
            case DAY -> w -> w.append("CAST(DATE(").expr(t, 0).append(") AS DATETIME(6))");
            case HOUR -> w -> w.append("CAST(DATE_FORMAT(").expr(t, 0).append(", '%Y-%m-%d %H:00:00') AS DATETIME(6))");
            case MINUTE -> w -> w.append("CAST(DATE_FORMAT(").expr(t, 0)
                    .append(", '%Y-%m-%d %H:%i:00') AS DATETIME(6))");
            case SECOND -> w -> w.append("CAST(DATE_FORMAT(").expr(t, 0)
                    .append(", '%Y-%m-%d %H:%i:%s') AS DATETIME(6))");
            case MILLISECOND -> w -> w.append("(CAST(").expr(t, 0).append(" AS DATETIME(6)) - INTERVAL MOD(MICROSECOND(")
                    .expr(t, 0).append("), 1000) MICROSECOND)");
            case MICROSECOND, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("date_trunc part '"
                    + part.name().toLowerCase(Locale.ROOT) + "' has no measured " + product() + " spelling");
        };
    }

    /** A date plus an interval (MariaDB's month arithmetic clamps a month past its end, as DuckDB's: 2020-01-31 + 1
     *  month is 2020-02-29, measured); a date plus whole days is a date. Milliseconds and finer are microseconds. */
    private SqlWriter.Piece addInterval(List<SqlExpr> a, SqlExpr.Call c) {
        DatePart unit = part(a.get(0));
        SqlExpr n = a.get(1);
        SqlExpr t = a.get(2);
        String[] u = switch (unit) {
            case YEAR -> new String[] {"YEAR", "1"};
            case MONTH -> new String[] {"MONTH", "1"};
            case WEEK -> new String[] {"WEEK", "1"};
            case DAY -> new String[] {"DAY", "1"};
            case HOUR -> new String[] {"HOUR", "1"};
            case MINUTE -> new String[] {"MINUTE", "1"};
            case SECOND -> new String[] {"SECOND", "1"};
            case MILLISECOND -> new String[] {"MICROSECOND", "1000"};
            case MICROSECOND -> new String[] {"MICROSECOND", "1"};
            case QUARTER, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("interval unit " + unit
                    + " has no " + product() + " spelling");
        };
        return w -> w.append("(").expr(t, 6).append(" + INTERVAL (").expr(n, 0).append(")" + ("1".equals(u[1]) ? ""
                : " * " + u[1]) + " " + u[0] + ")");
    }

    /** DuckDB's date_diff counts BOUNDARIES crossed (23:59:59 to 00:00 is a day, 01-31 to 02-01 a month); its week is
     *  whole days over 7, truncated. */
    private SqlWriter.Piece dateDiff(List<SqlExpr> a) {
        DatePart part = part(a.get(0));
        SqlExpr from = a.get(1);
        SqlExpr to = a.get(2);
        return switch (part) {
            case YEAR -> w -> w.append("(YEAR(").expr(to, 0).append(") - YEAR(").expr(from, 0).append("))");
            case QUARTER -> w -> w.append("((YEAR(").expr(to, 0).append(") * 4 + QUARTER(").expr(to, 0)
                    .append(")) - (YEAR(").expr(from, 0).append(") * 4 + QUARTER(").expr(from, 0).append(")))");
            case MONTH -> w -> w.append("((YEAR(").expr(to, 0).append(") * 12 + MONTH(").expr(to, 0)
                    .append(")) - (YEAR(").expr(from, 0).append(") * 12 + MONTH(").expr(from, 0).append(")))");
            case WEEK -> w -> w.append("(DATEDIFF(").expr(to, 0).append(", ").expr(from, 0).append(") DIV 7)");
            case DAY -> w -> w.append("DATEDIFF(").expr(to, 0).append(", ").expr(from, 0).append(")");
            case HOUR -> boundaries(from, to, 3600000000L);
            case MINUTE -> boundaries(from, to, 60000000L);
            case SECOND -> boundaries(from, to, 1000000L);
            case MILLISECOND -> boundaries(from, to, 1000L);
            case MICROSECOND -> boundaries(from, to, 1L);
            case NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("date_diff part '"
                    + part.name().toLowerCase(Locale.ROOT) + "' has no measured " + product() + " spelling");
        };
    }

    /** The unit boundaries between two date-times: each floored to the unit, in microseconds. */
    private SqlWriter.Piece boundaries(SqlExpr from, SqlExpr to, long unit) {
        return w -> w.append("(FLOOR(").piece(micros(to)).append(" / " + unit + ") - FLOOR(").piece(micros(from))
                .append(" / " + unit + "))");
    }

    /** DuckDB's time_bucket with the base's origins (1970-01-01; weeks the Monday 1969-12-29): fixed units binned in
     *  microseconds, floored; months and years by the month index since 1970-01, floored to the bucket's size. */
    private SqlWriter.Piece timeBucket(List<SqlExpr> a) {
        DatePart unit = part(a.get(0));
        SqlExpr n = a.get(1);
        SqlExpr t = a.get(2);
        Long micros = intervalMicros(unit);
        if (micros != null) {
            long origin = unit == DatePart.WEEK ? -3L * 86400000000L : 0L;
            SqlWriter.Piece size = w -> w.append("(").expr(n, 6).append(" * " + micros + ")");
            SqlWriter.Piece m = w -> w.append("(").piece(micros(t)).append(" - (" + origin + "))");
            return fromMicros(w -> w.append("(FLOOR(").piece(m).append(" / ").piece(size).append(") * ").piece(size)
                    .append(" + (" + origin + "))"));
        }
        int months = unit == DatePart.MONTH ? 1 : unit == DatePart.YEAR ? 12 : 0;
        if (months == 0) {
            throw new DialectCapability("a " + unit + " time bucket has no " + product() + " spelling");
        }
        SqlWriter.Piece k = w -> w.append("((YEAR(").expr(t, 0).append(") - 1970) * 12 + MONTH(").expr(t, 0)
                .append(") - 1)");
        SqlWriter.Piece size = w -> w.append("(").expr(n, 6).append(" * " + months + ")");
        return w -> w.append("CAST(DATE '1970-01-01' + INTERVAL (FLOOR(").piece(k).append(" / ").piece(size)
                .append(") * ").piece(size).append(") MONTH AS DATETIME(6))");
    }

    /** A fixed-length unit's length in microseconds; null for a calendar one. */
    private static @com.legend.base.Nullable Long intervalMicros(DatePart unit) {
        return switch (unit) {
            case WEEK -> 7L * 86400000000L;
            case DAY -> 86400000000L;
            case HOUR -> 3600000000L;
            case MINUTE -> 60000000L;
            case SECOND -> 1000000L;
            case MILLISECOND -> 1000L;
            case MICROSECOND -> 1L;
            case YEAR, QUARTER, MONTH, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> null;
        };
    }

    private SqlWriter makeTimestamp(SqlWriter writer, List<SqlExpr> a, SqlExpr.Call c) {
        if (a.size() != 6) {
            throw wall(c.fn(), "make_timestamp of " + a.size() + " arguments");
        }
        return writer.append("(STR_TO_DATE(CONCAT_WS('-', ").list(a.subList(0, 3)).append("), '%Y-%m-%d') + INTERVAL ")
                .expr(a.get(3), 6).append(" HOUR + INTERVAL ").expr(a.get(4), 6).append(" MINUTE + INTERVAL ROUND((")
                .expr(a.get(5), 0).append(") * 1000000) MICROSECOND)");
    }

    /** A date-time formatted by DuckDB's strftime parts, through DATE_FORMAT (its codes: %i minutes, %f microseconds,
     *  %h and %l the 12-hour clock, %p AM/PM, %M and %b the month's name, %W the weekday's). */
    private SqlWriter.Piece strftime(SqlExpr value, SqlExpr.FormatLit fl) {
        List<SqlWriter.Piece> pieces = new ArrayList<>();
        StringBuilder run = new StringBuilder();
        for (DateFmt p : fl.parts()) {
            String code = switch (p) {
                case DateFmt.Text t -> t.s().replace("%", "%%");
                case DateFmt.Part part -> switch (part) {
                    case YEAR4 -> "%Y";
                    case MONTH2 -> "%m";
                    case DAY2 -> "%d";
                    case HOUR2 -> "%H";
                    case MIN2 -> "%i";
                    case SEC2 -> "%s";
                    case SUBSEC_MICRO -> "%f";
                    case SUBSEC_NANO -> "%f000";
                    case MONTH_ABBREV -> "%b";
                    case MONTH_NAME -> "%M";
                    case WEEKDAY_NAME -> "%W";
                    case HOUR12 -> "%h";
                    case HOUR12_NOPAD -> "%l";
                    case AMPM -> "%p";
                    case SUBSEC_MIN -> null;
                };
            };
            if (code != null) {
                run.append(code);
                continue;
            }
            if (run.length() > 0) {
                String f = stringLit(run.toString());
                pieces.add(w -> w.append("DATE_FORMAT(").expr(value, 0).append(", " + f + ")"));
                run.setLength(0);
            }
            // DuckDB's %g: milliseconds, three digits
            pieces.add(w -> w.append("LEFT(DATE_FORMAT(").expr(value, 0).append(", '%f'), 3)"));
        }
        if (run.length() > 0 || pieces.isEmpty()) {
            String f = stringLit(run.toString());
            pieces.add(w -> w.append("DATE_FORMAT(").expr(value, 0).append(", " + f + ")"));
        }
        if (pieces.size() == 1) {
            return pieces.get(0);
        }
        return w -> w.append("CONCAT(").join(pieces, ", ", (x, piece) -> x.piece(piece)).append(")");
    }

    // ==================================================================
    // Hooks of the base
    // ==================================================================

    /** Pure's hashCode, a signed 64-bit integer: the first 64 bits of the MD5 of the value's text as JSON (a NULL's
     *  JSON is 'null', so the empty collection hashes too), read signed ({@code CONV} to base -10). */
    @Override
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        writer.append("CAST(CONV(LEFT(MD5(JSON_ARRAY(");
        writer.join(a, ", ", (w, x) -> w.piece(asText(x)));
        return writer.append(")), 16), 16, -10) AS SIGNED)");
    }

    /** Pure round is half-EVEN: a double's ROUND here is already (measured: ROUND(2.5E0) is 2); a DECIMAL's rounds
     *  half away from zero, and a tie that rounded to an odd number is taken back one toward zero. */
    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        if (isDouble(a.get(0))) {
            return writer.append("ROUND(").list(a).append(")");
        }
        SqlWriter.Piece scale = a.size() > 1 ? w -> w.append("POW(10, ").expr(a.get(1), 0).append(")")
                : w -> w.append("1");
        SqlWriter.Piece scaled = w -> w.append("(").expr(a.get(0), 6).append(" * ").piece(scale).append(")");
        SqlWriter.Piece r = w -> w.append("ROUND(").piece(scaled).append(")");
        return writer.append("((CASE WHEN ABS(").piece(scaled).append(" - TRUNCATE(").piece(scaled)
                .append(", 0)) = 0.5 AND MOD(").piece(r).append(", 2) <> 0 THEN ").piece(r).append(" - SIGN(")
                .piece(scaled).append(") ELSE ").piece(r).append(" END) / ").piece(scale).append(")");
    }

    /** 64-bit bit operators: MariaDB's are UNSIGNED (~0 is 18446744073709551615, measured), read back signed; a right
     *  shift is arithmetic (-16 >> 2 is -4, as DuckDB's): a floor division. */
    @Override
    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        SqlWriter.Piece x = w -> w.expr(a.get(0), 7);
        SqlWriter.Piece y = w -> w.expr(a.get(1), 7);
        return switch (fnName) {
            case BIT_AND -> writer.append("CAST(").piece(x).append(" & ").piece(y).append(" AS SIGNED)");
            case BIT_OR -> writer.append("CAST(").piece(x).append(" | ").piece(y).append(" AS SIGNED)");
            case BIT_XOR -> writer.append("CAST(").piece(x).append(" ^ ").piece(y).append(" AS SIGNED)");
            case BIT_SHIFT_LEFT -> writer.append("CAST(").piece(x).append(" << ").piece(y).append(" AS SIGNED)");
            // a floor division by 2^y in integers (a DOUBLE division loses the low bits)
            case BIT_SHIFT_RIGHT -> writer.append("(CASE WHEN ").piece(x).append(" >= 0 THEN ").piece(x)
                    .append(" DIV CAST(1 << ").piece(y).append(" AS SIGNED) ELSE -((-(").piece(x).append(") - 1) DIV CAST(1 << ")
                    .piece(y).append(" AS SIGNED)) - 1 END)");
            default -> throw new IllegalStateException("not a bit op: " + fnName);
        };
    }

    // ==================================================================
    // Aggregates and windows
    // ==================================================================

    private static SqlWriter.Piece aggregateArgs(SqlAgg.Reducer r) {
        return w -> w.append(r.distinct() ? "DISTINCT " : "").list(r.args());
    }

    @Override
    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM -> aggregate(writer, "SUM", r);
            case COUNT -> aggregate(writer, "COUNT", r);
            case MIN -> aggregate(writer, "MIN", r);
            case MAX -> aggregate(writer, "MAX", r);
            // DuckDB's avg is a DOUBLE (an average here is a DECIMAL of four more digits)
            case AVG -> writer.append("AVG(").append(r.distinct() ? "DISTINCT " : "").append("CAST(")
                    .expr(r.args().get(0), 0).append(" AS DOUBLE))");
            // any non-NULL value: the least
            case ANY_VALUE -> writer.append("MIN(").piece(aggregateArgs(r)).append(")");
            case STDDEV_SAMP, STDDEV -> writer.append("STDDEV_SAMP(").piece(aggregateArgs(r)).append(")");
            case VAR_SAMP, VARIANCE -> writer.append("VAR_SAMP(").piece(aggregateArgs(r)).append(")");
            case VAR_POP -> writer.append("VAR_POP(").piece(aggregateArgs(r)).append(")");
            case STDDEV_POP -> writer.append("STDDEV_POP(").piece(aggregateArgs(r)).append(")");
            case COVAR_POP, COVAR_SAMP, CORR -> writer.piece(moments(r));
            case BOOL_AND -> writer.append("MIN(").piece(aggregateArgs(r)).append(")");
            case BOOL_OR -> writer.append("MAX(").piece(aggregateArgs(r)).append(")");
            // GROUP_CONCAT's separator is a literal
            case STRING_AGG -> groupConcat(writer, r);
            case LIST -> listOfValues(writer, r);
            case MEDIAN -> median(writer, r);
            case QUANTILE_CONT, QUANTILE_DISC -> percentile(writer, r);
            // the most frequent non-NULL value, the least among ties: the group's values as a JSON array, grouped by a
            // subquery over it (an outer aggregate inside a correlated table function, measured)
            case MODE -> groupedModesCounted() ? modeArm(writer, r) : modeOverJson(writer, r);
            // the value at the key's greatest (least) row, rows of a NULL key ignored
            case ARG_MAX -> argExtreme(writer, r, false);
            case ARG_MIN -> argExtreme(writer, r, true);
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            // a window function is a ranking or value function, never an aggregate (windowCall writes it)
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
    }

    /** The {@code STRING_AGG} arm of {@link #reducer}: GROUP_CONCAT, its separator a literal. */
    private SqlWriter groupConcat(SqlWriter writer, SqlAgg.Reducer r) {
        SqlExpr sep = r.args().size() > 1 ? r.args().get(1) : new SqlExpr.StringLit(",");
        writer.append("GROUP_CONCAT(").append(r.distinct() ? "DISTINCT " : "").piece(asText(r.args().get(0)));
        if (!r.orderBy().isEmpty()) {
            writer.append(" ORDER BY ").join(r.orderBy(), ", ", this::orderKey);
        }
        return writer.append(" SEPARATOR " + stringLit(literal(sep, "a string_agg separator")) + ")");
    }

    /** The {@code LIST} arm of {@link #reducer}. */
    private SqlWriter listOfValues(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1) {
            throw new DialectCapability("a LIST aggregate of " + r.args().size() + " arguments reached " + product());
        }
        return writer.piece(arrayAgg(r.distinct(), json(r.args().get(0)), r.orderBy().isEmpty() ? null
                : w -> w.join(r.orderBy(), ", ", this::orderKey), false));
    }

    /** The {@code MEDIAN} arm of {@link #reducer}. */
    private SqlWriter median(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1 || r.distinct()) {
            throw new DialectCapability("a median of this shape reached " + product());
        }
        return writer.piece(quantile(SqlAgg.Fn.QUANTILE_CONT, r.args().get(0), w -> w.append("0.5"), r));
    }

    /** The {@code QUANTILE_CONT} and {@code QUANTILE_DISC} arms of {@link #reducer}. */
    private SqlWriter percentile(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 2 || r.distinct() || r.orderBy().size() > 1) {
            throw new DialectCapability(r.fn() + " of this shape reached " + product());
        }
        return writer.piece(quantile(r.fn(), r.args().get(0), w -> w.expr(r.args().get(1), 0), r));
    }

    /** The {@code MODE} arm of {@link #reducer} where the grouped modes are not counted: the group's values as a
     *  JSON array, grouped by a subquery over it. */
    private SqlWriter modeOverJson(SqlWriter writer, SqlAgg.Reducer r) {
        SqlExpr x = r.args().get(0);
        String type = columnType(typeOf(x));
        return writer.append("(SELECT __m.\"v\" FROM JSON_TABLE(JSON_ARRAYAGG(").piece(json(x))
                .append("), '$[*]' COLUMNS (\"v\" " + type + " PATH '$')) AS __m WHERE __m.\"v\" IS NOT NULL")
                .append(" GROUP BY __m.\"v\" ORDER BY count(*) DESC, __m.\"v\" LIMIT 1)");
    }

    /** The value at the key's greatest ({@code asc} false) or least row, rows of a NULL key ignored. */
    private SqlWriter argExtreme(SqlWriter writer, SqlAgg.Reducer r, boolean asc) {
        if (r.args().size() != 2 || r.distinct() || !r.orderBy().isEmpty()) {
            throw new DialectCapability(r.fn() + " of this shape reached " + product());
        }
        SqlExpr v = r.args().get(0);
        SqlExpr k = r.args().get(1);
        SqlWriter.Piece first = arrayAgg(false, json(v), elementSortOf(w -> w.expr(k, 0), asc), true);
        return writer.append("(CASE WHEN count(").expr(k, 0).append(") = 0 THEN NULL ELSE ")
                .piece(valueAt(first, w -> w.append("'$[0]'"), typeOf(v))).append(" END)");
    }

    /** Covariance and correlation over the pairs with both values: their sums, as the moments' definitions (no FILTER
     *  clause here: a CASE keeps a pair's value). */
    private SqlWriter.Piece moments(SqlAgg.Reducer r) {
        SqlExpr y = r.args().get(0);
        SqlExpr x = r.args().get(1);
        SqlWriter.Piece both = w -> w.append("(").expr(x, 4).append(" IS NOT NULL AND ").expr(y, 4)
                .append(" IS NOT NULL)");
        SqlWriter.Piece n = w -> w.append("count(").expr(x, 6).append(" * ").expr(y, 6).append(")");
        SqlWriter.Piece sxy = w -> w.append("sum(CAST(").expr(x, 0).append(" AS DOUBLE) * ").expr(y, 6).append(")");
        SqlWriter.Piece sx = w -> w.append("sum(CASE WHEN ").piece(both).append(" THEN CAST(").expr(x, 0)
                .append(" AS DOUBLE) END)");
        SqlWriter.Piece sy = w -> w.append("sum(CASE WHEN ").piece(both).append(" THEN CAST(").expr(y, 0)
                .append(" AS DOUBLE) END)");
        SqlWriter.Piece sxx = w -> w.append("sum(CASE WHEN ").piece(both).append(" THEN CAST(").expr(x, 0)
                .append(" AS DOUBLE) * ").expr(x, 6).append(" END)");
        SqlWriter.Piece syy = w -> w.append("sum(CASE WHEN ").piece(both).append(" THEN CAST(").expr(y, 0)
                .append(" AS DOUBLE) * ").expr(y, 6).append(" END)");
        SqlWriter.Piece cxy = w -> w.append("(").piece(sxy).append(" - ").piece(sx).append(" * ").piece(sy)
                .append(" / ").piece(n).append(")");
        return switch (r.fn()) {
            case COVAR_POP -> w -> w.append("(CASE WHEN ").piece(n).append(" > 0 THEN ").piece(cxy).append(" / ")
                    .piece(n).append(" END)");
            case COVAR_SAMP -> w -> w.append("(CASE WHEN ").piece(n).append(" > 1 THEN ").piece(cxy).append(" / (")
                    .piece(n).append(" - 1) END)");
            default -> w -> w.append("(CASE WHEN ").piece(n).append(" > 1 THEN ").piece(cxy).append(" / SQRT((")
                    .piece(sxx).append(" - ").piece(sx).append(" * ").piece(sx).append(" / ").piece(n)
                    .append(") * (").piece(syy).append(" - ").piece(sy).append(" * ").piece(sy)
                    .append(" / ").piece(n).append(")) END)");
        };
    }

    /**
     * A grouped quantile (MariaDB's PERCENTILE_CONT/DISC and MEDIAN are window functions only): the group's values
     * sorted, NULLs last ({@code JSON_ARRAYAGG(x ORDER BY (x IS NULL), x)}), and indexed -- continuous: the value at
     * {@code q * (n - 1)} interpolated linearly; discrete: {@code ceil(n * q) - 1} (both DuckDB's).
     */
    private SqlWriter.Piece quantile(SqlAgg.Fn fn, SqlExpr x, SqlWriter.Piece q, SqlAgg.Reducer r) {
        boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
        SqlType t = typeOf(x);
        SqlWriter.Piece sorted = arrayAgg(false, json(x), elementSortOf(w -> w.expr(x, 0), !desc), false);
        SqlWriter.Piece n = w -> w.append("count(").expr(x, 0).append(")");
        java.util.function.Function<SqlWriter.Piece, SqlWriter.Piece> at = i -> valueAt(sorted, indexPath(i),
                fn == SqlAgg.Fn.QUANTILE_DISC ? t : SqlType.Scalar.DOUBLE);
        if (fn == SqlAgg.Fn.QUANTILE_DISC) {
            return at.apply(w -> w.append("GREATEST(CEIL(").piece(n).append(" * (").piece(q).append(")) - 1, 0)"));
        }
        SqlWriter.Piece pos = w -> w.append("((").piece(q).append(") * (").piece(n).append(" - 1))");
        SqlWriter.Piece lo = at.apply(w -> w.append("FLOOR(").piece(pos).append(")"));
        SqlWriter.Piece hi = at.apply(w -> w.append("CEIL(").piece(pos).append(")"));
        return w -> w.append("(").piece(lo).append(" + (").piece(hi).append(" - ").piece(lo).append(") * (")
                .piece(pos).append(" - FLOOR(").piece(pos).append(")))");
    }

    /**
     * A whole partition's median or quantile from each row's rank ({@link #modesCounted}, 2026-10-11: MySQL has no
     * PERCENTILE_CONT): the values at the ranks around the position, as the grouped {@link #quantile} reads its sorted
     * array -- continuous, {@code lo + (hi - lo) * (pos - FLOOR(pos))} at {@code pos = q * (n - 1)}; discrete, the value
     * at {@code GREATEST(CEIL(n * q) - 1, 0)} -- each value spread over the partition by a windowed MAX.
     */
    private SqlWriter.Piece rankedQuantile(SqlAgg.Reducer r, SqlExpr.WindowCall w) {
        List<SqlExpr> counted = modeCounts.isEmpty() ? null : modeCounts.peek().get(w);
        if (counted == null) {
            throw new IllegalStateException("a windowed " + r.fn() + " reached " + product() + "'s writer unranked:"
                    + " modesCounted did not run");
        }
        SqlExpr rank = counted.get(0);
        SqlExpr n = counted.get(1);
        SqlExpr x = r.args().get(0);
        SqlWriter.Piece q = r.fn() == SqlAgg.Fn.MEDIAN ? v -> v.append("0.5") : v -> v.expr(r.args().get(1), 0);
        SqlWriter.Piece over = v -> {
            v.append(" OVER (");
            if (!w.partitionBy().isEmpty()) {
                v.append("PARTITION BY ").list(w.partitionBy());
            }
            v.append(")");
        };
        java.util.function.Function<SqlWriter.Piece, SqlWriter.Piece> at = i -> v -> v.append("MAX(CASE WHEN ")
                .expr(rank, 0).append(" = ").piece(i).append(" + 1 THEN ")
                .piece(r.fn() == SqlAgg.Fn.QUANTILE_DISC ? y -> y.expr(x, 0) : y -> y.append("CAST(").expr(x, 0)
                        .append(" AS DOUBLE)"))
                .append(" END)").piece(over);
        if (r.fn() == SqlAgg.Fn.QUANTILE_DISC) {
            return at.apply(v -> v.append("GREATEST(CEIL(").expr(n, 0).append(" * (").piece(q).append(")) - 1, 0)"));
        }
        SqlWriter.Piece pos = v -> v.append("((").piece(q).append(") * (").expr(n, 0).append(" - 1))");
        SqlWriter.Piece lo = at.apply(v -> v.append("FLOOR(").piece(pos).append(")"));
        SqlWriter.Piece hi = at.apply(v -> v.append("CEIL(").piece(pos).append(")"));
        return v -> v.append("(").piece(lo).append(" + (").piece(hi).append(" - ").piece(lo).append(") * (")
                .piece(pos).append(" - FLOOR(").piece(pos).append(")))");
    }

    private static final Set<SqlAgg.Fn> ORDERED_SETS = Set.of(SqlAgg.Fn.MEDIAN, SqlAgg.Fn.MODE,
            SqlAgg.Fn.QUANTILE_CONT, SqlAgg.Fn.QUANTILE_DISC);

    private static final Set<SqlAgg.Fn> PAIRS = Set.of(SqlAgg.Fn.COVAR_POP, SqlAgg.Fn.COVAR_SAMP, SqlAgg.Fn.CORR);

    /** A window function: MariaDB's are the standard ones, its moments windowed natively; a whole partition's median
     *  and quantiles natively ({@code PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY x) OVER (PARTITION BY ...)}); a
     *  covariance or correlation from window sums. GROUP_CONCAT and JSON_ARRAYAGG are no window functions here
     *  (measured), nor is a mode or an argMax. */
    @Override
    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        switch (w.fn()) {
            case SqlAgg.Reducer r when !windowQuantilesNative() && (r.fn() == SqlAgg.Fn.MEDIAN
                    || r.fn() == SqlAgg.Fn.QUANTILE_CONT || r.fn() == SqlAgg.Fn.QUANTILE_DISC) && w.orderBy().isEmpty()
                    && w.frame() == null -> {
                return writer.piece(rankedQuantile(r, w));
            }
            case SqlAgg.Reducer r when (r.fn() == SqlAgg.Fn.MEDIAN || r.fn() == SqlAgg.Fn.QUANTILE_CONT
                    || r.fn() == SqlAgg.Fn.QUANTILE_DISC) && w.orderBy().isEmpty() && w.frame() == null -> {
                SqlExpr x = r.args().get(0);
                boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
                writer.append(r.fn() == SqlAgg.Fn.QUANTILE_DISC ? "PERCENTILE_DISC(" : "PERCENTILE_CONT(");
                if (r.fn() == SqlAgg.Fn.MEDIAN) {
                    writer.append("0.5");
                } else {
                    writer.expr(r.args().get(1), 0);
                }
                writer.append(") WITHIN GROUP (ORDER BY ").expr(x, 0).append(desc ? " DESC)" : ")");
                return windowOver(writer, w);
            }
            case SqlAgg.Reducer r when r.fn() == SqlAgg.Fn.MODE && w.orderBy().isEmpty() && w.frame() == null -> {
                return modeArm(writer, w);
            }
            case SqlAgg.Reducer r when ORDERED_SETS.contains(r.fn()) ->
                    throw new DialectCapability(r.fn() + " over an ordered or framed window reached " + product());
            case SqlAgg.Reducer r when PAIRS.contains(r.fn()) -> {
                return writer.piece(windowPairs(r, w));
            }
            case SqlAgg.Reducer r when MOMENTS.contains(r.fn()) && integral(r.args().get(0)) -> {
                return writer.piece(windowMoment(r, w));
            }
            case SqlAgg.Reducer r when r.fn() == SqlAgg.Fn.ARG_MAX || r.fn() == SqlAgg.Fn.ARG_MIN
                    || r.fn() == SqlAgg.Fn.STRING_AGG || r.fn() == SqlAgg.Fn.LIST ->
                    throw new DialectCapability("a windowed " + r.fn() + " reached " + product() + ", where it is no window"
                            + " function");
            case SqlAgg.Reducer r when !r.orderBy().isEmpty() -> {
                if (!w.orderBy().isEmpty() || w.frame() != null) {
                    throw new DialectCapability("an ordered " + r.fn() + " over an ordered or framed window reached"
                            + " " + product());
                }
                reducer(writer, new SqlAgg.Reducer(r.fn(), r.args(), r.distinct(), List.of()));
                return windowOver(writer, new SqlExpr.WindowCall(w.fn(), w.partitionBy(), r.orderBy(),
                        new SqlExpr.WindowCall.Frame(SqlExpr.WindowCall.Frame.Kind.ROWS,
                                new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding(),
                                new SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing())));
            }
            case SqlAgg.Reducer r -> reducer(writer, r);
            case SqlAgg.RankingFn r -> writer.append(aggregateName(r.fn())).append("(").list(r.args()).append(")");
            case SqlAgg.ValueFn v -> writer.append(aggregateName(v.fn())).append("(").list(v.args()).append(")");
        }
        return windowOver(writer, w);
    }

    private static final Set<SqlAgg.Fn> MOMENTS = Set.of(SqlAgg.Fn.STDDEV_SAMP, SqlAgg.Fn.STDDEV, SqlAgg.Fn.STDDEV_POP,
            SqlAgg.Fn.VAR_SAMP, SqlAgg.Fn.VARIANCE, SqlAgg.Fn.VAR_POP);

    private static boolean integral(SqlExpr e) {
        SqlType t = typeOf(e);
        return t == SqlType.Scalar.BIGINT || t == SqlType.Scalar.INTEGER || t == SqlType.Scalar.HUGEINT;
    }

    /**
     * A windowed variance or standard deviation of integers from their exact sums over the window: {@code (n Σx² -
     * (Σx)²) / n²} (or {@code / n(n - 1)}) in DECIMAL, then the square root of its double. MariaDB's own windowed
     * moment is a running recurrence over the partition's rows in the order its sort leaves them, which varies (measured:
     * the same statement's z-scores 1.5 in one run and 1.5000000000000002 in another); the sums are exact in any order.
     */
    private SqlWriter.Piece windowMoment(SqlAgg.Reducer r, SqlExpr.WindowCall w) {
        SqlExpr x = r.args().get(0);
        java.util.function.Function<SqlWriter.Piece, SqlWriter.Piece> over = agg -> v -> {
            v.piece(agg);
            windowOver(v, w);
        };
        SqlWriter.Piece n = over.apply(v -> v.append("count(").expr(x, 0).append(")"));
        SqlWriter.Piece sx = over.apply(v -> v.append("sum(CAST(").expr(x, 0).append(" AS DECIMAL(65,0)))"));
        SqlWriter.Piece sxx = over.apply(v -> v.append("sum(CAST(").expr(x, 0).append(" AS DECIMAL(65,0)) * CAST(")
                .expr(x, 0).append(" AS DECIMAL(65,0)))"));
        boolean sample = r.fn() == SqlAgg.Fn.STDDEV_SAMP || r.fn() == SqlAgg.Fn.STDDEV || r.fn() == SqlAgg.Fn.VAR_SAMP
                || r.fn() == SqlAgg.Fn.VARIANCE;
        boolean root = r.fn() == SqlAgg.Fn.STDDEV_SAMP || r.fn() == SqlAgg.Fn.STDDEV || r.fn() == SqlAgg.Fn.STDDEV_POP;
        SqlWriter.Piece variance = v -> {
            v.append("CAST((").piece(sxx).append(" * ").piece(n).append(" - ").piece(sx).append(" * ").piece(sx)
                    .append(") / (").piece(n).append(" * ");
            if (sample) {
                v.append("(").piece(n).append(" - 1)");
            } else {
                v.piece(n);
            }
            v.append(") AS DOUBLE)");
        };
        return v -> {
            v.append("(CASE WHEN ").piece(n).append(sample ? " > 1" : " > 0").append(" THEN ");
            if (root) {
                v.append("SQRT(").piece(variance).append(")");
            } else {
                v.piece(variance);
            }
            v.append(" END)");
        };
    }

    /** A windowed covariance or correlation from its sums over the window: the pairs with both values. */
    private SqlWriter.Piece windowPairs(SqlAgg.Reducer r, SqlExpr.WindowCall w) {
        SqlExpr y = r.args().get(0);
        SqlExpr x = r.args().get(1);
        java.util.function.Function<SqlWriter.Piece, SqlWriter.Piece> over = arg -> v -> {
            v.append("sum(").piece(arg).append(")");
            windowOver(v, w);
        };
        SqlWriter.Piece both = v -> v.append("(CASE WHEN ").expr(x, 4).append(" IS NOT NULL AND ").expr(y, 4)
                .append(" IS NOT NULL THEN 1 END)");
        SqlWriter.Piece n = over.apply(both);
        SqlWriter.Piece sx = over.apply(v -> v.append("CAST(").expr(x, 0).append(" AS DOUBLE) * ").piece(both));
        SqlWriter.Piece sy = over.apply(v -> v.append("CAST(").expr(y, 0).append(" AS DOUBLE) * ").piece(both));
        SqlWriter.Piece sxy = over.apply(v -> v.append("CAST(").expr(x, 0).append(" AS DOUBLE) * ").expr(y, 6)
                .append(" * ").piece(both));
        SqlWriter.Piece sxx = over.apply(v -> v.append("CAST(").expr(x, 0).append(" AS DOUBLE) * ").expr(x, 6)
                .append(" * ").piece(both));
        SqlWriter.Piece syy = over.apply(v -> v.append("CAST(").expr(y, 0).append(" AS DOUBLE) * ").expr(y, 6)
                .append(" * ").piece(both));
        SqlWriter.Piece cxy = v -> v.append("(").piece(sxy).append(" - ").piece(sx).append(" * ").piece(sy)
                .append(" / ").piece(n).append(")");
        SqlWriter.Piece cxx = v -> v.append("GREATEST(").piece(sxx).append(" - ").piece(sx).append(" * ").piece(sx)
                .append(" / ").piece(n).append(", 0.0E0)");
        SqlWriter.Piece cyy = v -> v.append("GREATEST(").piece(syy).append(" - ").piece(sy).append(" * ").piece(sy)
                .append(" / ").piece(n).append(", 0.0E0)");
        return switch (r.fn()) {
            case COVAR_POP -> v -> v.append("(CASE WHEN ").piece(n).append(" > 0 THEN ").piece(cxy).append(" / ")
                    .piece(n).append(" END)");
            case COVAR_SAMP -> v -> v.append("(CASE WHEN ").piece(n).append(" > 1 THEN ").piece(cxy).append(" / (")
                    .piece(n).append(" - 1) END)");
            default -> v -> v.append("(CASE WHEN ").piece(n).append(" > 1 THEN ").piece(cxy).append(" / SQRT(")
                    .piece(cxx).append(" * ").piece(cyy).append(") END)");
        };
    }

    /** A window's {@code OVER (...)}. A RANGE frame with an offset takes ONE numeric key (no INTERVAL offset here,
     *  measured): a date or date-time key is its days since the epoch, the offset in days. */
    private SqlWriter windowOver(SqlWriter writer, SqlExpr.WindowCall w) {
        writer.append(" OVER (");
        boolean any = false;
        if (!w.partitionBy().isEmpty()) {
            writer.append("PARTITION BY ").list(w.partitionBy());
            any = true;
        }
        SqlExpr.WindowCall.Frame f = w.frame();
        boolean days = false;
        if (!w.orderBy().isEmpty()) {
            writer.append(any ? " " : "").append("ORDER BY ");
            boolean interval = f != null && (intervalBound(f.from()) || intervalBound(f.to()));
            boolean range = f != null && f.kind() == SqlExpr.WindowCall.Frame.Kind.RANGE;
            if (interval) {
                if (w.orderBy().size() != 1) {
                    throw new DialectCapability("a RANGE frame over " + w.orderBy().size() + " keys reached " + product());
                }
                SqlSelect.SortKey k = w.orderBy().get(0);
                SqlType kt = typeOf(k.expr());
                if (kt != SqlType.Scalar.DATE && kt != SqlType.Scalar.TIMESTAMP) {
                    throw new DialectCapability("an interval RANGE frame over a " + kt + " key reached " + product());
                }
                days = true;
                writer.append("(").piece(micros(k.expr())).append(" / 86400000000)").append(k.ascending() ? "" : " DESC");
            } else if (range) {
                // a RANGE frame's one key carries no NULL placement key beside it: a NULL is the key past either end
                // (its rows are their own peers, as DuckDB's)
                writer.join(w.orderBy(), ", ", (x, k) -> {
                    boolean nullsFirst = k.nullOrder() != null
                            ? k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST : !k.ascending();
                    x.append("coalesce(").expr(k.expr(), 0).append(nullsFirst == k.ascending() ? ", -1.0E308)" : ", 1.0E308)")
                            .append(k.ascending() ? "" : " DESC");
                });
            } else {
                writer.join(w.orderBy(), ", ", this::sortKey);
            }
            any = true;
        }
        if (f != null) {
            boolean inDays = days;
            writer.append(any ? " " : "").append(f.kind().toString()).append(" BETWEEN ")
                    .append(frameBound(f.from(), inDays)).append(" AND ").append(frameBound(f.to(), inDays));
        }
        return writer.append(")");
    }

    private static boolean intervalBound(SqlExpr.WindowCall.Frame.Bound b) {
        return b instanceof SqlExpr.WindowCall.Frame.Bound.IntervalPreceding
                || b instanceof SqlExpr.WindowCall.Frame.Bound.IntervalFollowing;
    }

    private String frameBound(SqlExpr.WindowCall.Frame.Bound b, boolean days) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> intervalDays(p.n(), p.unit(), days)
                    + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing fo -> intervalDays(fo.n(), fo.unit(), days)
                    + " FOLLOWING";
            default -> bound(b);
        };
    }

    private String intervalDays(long n, String unit, boolean days) {
        Long micros = FRAME_MICROS.get(unit.toUpperCase(Locale.ROOT));
        if (!days || micros == null) {
            throw new DialectCapability("a " + unit + " window frame offset over this key reached " + product());
        }
        return String.valueOf(n * (double) micros / 86400000000.0);
    }

    /** DurationUnit name to microseconds (months and years have no fixed length). */
    private static final Map<String, Long> FRAME_MICROS = Map.of("WEEKS", 7L * 86400000000L, "DAYS", 86400000000L,
            "HOURS", 3600000000L, "MINUTES", 60000000L, "SECONDS", 1000000L, "MILLISECONDS", 1000L);

    // ==================================================================
    // JSON constructors
    // ==================================================================

    @Override
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        writer.append("JSON_OBJECT(");
        List<SqlExpr> kv = j.kv();
        for (int i = 0; i < kv.size(); i++) {
            SqlExpr x = kv.get(i);
            writer.append(i > 0 ? ", " : "").piece(i % 2 == 0 ? w -> w.expr(x, 0) : json(x));
        }
        return writer.append(")");
    }

    @Override
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.append("JSON_ARRAY(").join(j.elements(), ", ", (w, x) -> w.piece(json(x))).append(")");
    }

    /** A JSON array of the values, NULLs kept as JSON nulls; over no rows the empty array (JSON_ARRAYAGG of none is
     *  NULL, measured). */
    @Override
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        SqlWriter.Piece order = j.orderKeys().isEmpty() ? null : v -> v.join(j.orderKeys(), ", ",
                (w, k) -> w.piece(elementSortOf(x -> x.expr(k.expr(), 0), !k.desc())));
        return writer.append("coalesce(").piece(arrayAgg(false, json(j.value()), order, false))
                .append(", JSON_ARRAY())");
    }

    // ==================================================================
    // Plan parameters
    // ==================================================================

    @Override
    protected RenderedStatement.TypeSpelling holeType(com.legend.sql.ValueKind kind) {
        return switch (kind) {
            case DATE -> new RenderedStatement.TypeSpelling("DATE", RenderedStatement.Digits.NONE);
            case DATE_TIME, DATE_TIME_NANOS ->
                    new RenderedStatement.TypeSpelling("DATETIME(6)", RenderedStatement.Digits.NONE);
            case INTEGER, DECIMAL, FLOATING -> super.holeType(kind);
        };
    }

    // ==================================================================
    // Values: what the driver hands back
    // ==================================================================

    /** A boolean arrives as 1 or 0 (a comparison is an integer here); a struct or a map as its JSON text, decoded to
     *  the platform's map of its fields. */
    @Override
    public @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            @com.legend.base.Nullable SqlType type) {
        if (type == SqlType.Scalar.BOOLEAN && jdbcValue instanceof Number n) {
            return n.longValue() != 0;
        }
        if (jdbcValue instanceof String s && (type instanceof SqlType.Struct || type instanceof SqlType.Map)
                && s.startsWith("{")) {
            return com.legend.sql.Json.parse(s);
        }
        return jdbcValue;
    }

    // ==================================================================
    // DDL (test fixtures)
    // ==================================================================

    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT -> "BIGINT";
                case SMALLINT -> "SMALLINT";
                case TINYINT -> "TINYINT";
                case INTEGER -> "INT";
                case BIT -> "BOOLEAN";
                case FLOAT, DOUBLE, REAL -> "DOUBLE";
                case TIMESTAMP -> "DATETIME(6)";
                case DATE -> "DATE";
                case TIME -> "TIME(6)";
                // a nanosecond timestamp column: MariaDB's DATETIME keeps microseconds
                case TIMESTAMP_NS -> throw new DialectCapability("a TIMESTAMP_NS column: MariaDB keeps microseconds");
                case JSON, VARCHAR, OTHER, DISTINCT, ARRAY, OBJECT -> "LONGTEXT";
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> z.kind().toUpperCase(Locale.ROOT).contains("BINARY")
                    ? "LONGBLOB" : "VARCHAR(" + z.size() + ")";
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> "DECIMAL(" + s.precision() + "," + s.scale() + ")";
        };
    }

    /** {@inheritDoc} Its JSON_TYPE answers DuckDB's names (a CASE over JSON_TYPE). */
    @Override
    protected String jsonKindName(com.legend.sql.JsonKind kind) {
        return switch (kind) {
            case OBJECT -> "OBJECT";
            case ARRAY -> "ARRAY";
            case NULL -> "NULL";
            case STRING -> "VARCHAR";
            case INTEGER -> "BIGINT";
            case UNSIGNED_INTEGER -> "UBIGINT";
            case FLOAT -> "DOUBLE";
            case BOOLEAN -> "BOOLEAN";
        };
    }

    /** {@inheritDoc} Refused, as TYPEOF is here (its type names are DuckDB's). */
    @Override
    protected String typeofName(com.legend.sql.SqlType.Scalar type) {
        throw new DialectCapability("typeof reached MariaDB: its type names are DuckDB's");
    }

    /** {@inheritDoc} Its text types and a UUID are text, its DATETIME the naive date-time (2026-10-11). */
    @Override
    public String wireTypeName(@com.legend.base.Nullable String driverTypeName) {
        return WireNames.fold(driverTypeName == null ? null : switch (driverTypeName) {
            case "LONGTEXT", "MEDIUMTEXT", "TEXT", "TINYTEXT", "CHAR", "UUID" -> "VARCHAR";
            case "DATETIME" -> "TIMESTAMP";
            default -> driverTypeName;
        });
    }

    /** {@inheritDoc} Its own types, the platform's by design (2026-10-11): a comparison and a boolean are integers; a
     *  list, a struct, a map and a Variant are JSON text (a JSON function's type is JSON, a column's text); an integer
     *  sum or a big integer is a DECIMAL of scale 0 (SUM of BIGINT, measured), or an unsigned BIGINT (a bit
     *  operation's); a NULL literal's column claims no type. */
    @Override
    public boolean deliversOwn(SqlType label, String wire) {
        boolean integral = label == SqlType.Scalar.BIGINT || label == SqlType.Scalar.INTEGER
                || label == SqlType.Scalar.HUGEINT;
        boolean composite = label instanceof SqlType.Array || label instanceof SqlType.Struct
                || label instanceof SqlType.Map || label == SqlType.Scalar.JSON;
        return wire.equals("NULL")
                || label == SqlType.Scalar.BOOLEAN && (wire.equals("INTEGER") || wire.equals("BIGINT")
                        || wire.equals("TINYINT"))
                || integral && (wire.startsWith("DECIMAL(") && wire.endsWith(",0)") || wire.equals("BIGINT UNSIGNED"))
                || label instanceof SqlType.Decimal d && wire.startsWith("DECIMAL(")
                        && wire.endsWith("," + d.scale() + ")")
                || composite && (wire.equals("JSON") || wire.equals("VARCHAR"));
    }
}
