// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlType;
import com.legend.sql.TypeFact;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The MongoDB target (docs/MONGODB_PLAN_2026_10_16.md, 2026-10-16; decisions D1, D2, D5, D7, D10, D11): the one query
 * compiler's render target whose text is an {@code aggregate} command -- a pipeline in MongoDB's Extended JSON --
 * rather than SQL. It extends the base renderer as every target does, so the dialect program's guards read it, and
 * overrides the writing of a query ({@link MongoDbPipeline}: the stages) and of an expression (here: MongoDB's
 * aggregation operators), keeping the base's compositions that are pure IR (the checked narrowing, the compaction).
 *
 * <p>What this dialect decides, in its own tables:
 * <ul>
 *   <li>every function ({@link #call}) and every aggregate ({@link #reducer}): a spelling, an emulation over
 *       MongoDB's operators, or a refusal by name -- never a fallback;</li>
 *   <li>SQL's three-valued comparisons over BSON's total ones (D7): an operator whose answer MongoDB defines over
 *       null is written through a null guard ({@link MongoJson#guarded}) where an operand may be null;</li>
 *   <li>a plan parameter as a marker document the connector replaces in place ({@code {"$legendParam": "?"}}),
 *       bound by its place among the markers as every dialect's parameters are (a per-key region's copies are
 *       copies of its text, so a marker names no index: 2026-10-20);</li>
 *   <li>an aggregate as a {@code $group} accumulator and the read of its field ({@link #accumulator}): the native
 *       accumulators where MongoDB has one, else the group's values pushed and reduced at the read (a distinct or
 *       ordered aggregate, a median, a quantile, a string aggregate);</li>
 *   <li>a raised error as a conversion that cannot succeed, its message between the platform's U+001F sentinels, so
 *       the connector extracts it as the executor extracts a SQL raise.</li>
 * </ul>
 * No server-side JavaScript is written (D11), and nothing from MongoDB's own documentation or code is copied.
 */
public final class MongoDb extends AnsiSqlRenderer {

    /** No quoting: a field or collection name is spelled as a JSON string, never delimited. */
    public static final Lexicon LEXICON = new Lexicon('"', Set.of());

    /** Lists are native BSON arrays; a correlated explosion, a JSON carrier and a native full join are not shaped. */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(true, false, false, false);

    /** The field a group stage computes an aggregate into, by its index ({@code __agg0}); the projection reads it
     *  through the read {@link #accumulator} registers. */
    static final String AGGREGATE_MARKER = "__agg";

    /** The field a lookup stage computes a subquery into, by its index ({@code __sq0}); the expression that held the
     *  subquery reads it through the read the pipeline registers ({@link #subqueryReads}). */
    static final String SUBQUERY_MARKER = "__sq";
    /** A window function's field, set by a {@code $setWindowFields} stage and read by the clauses after it. */
    static final String WINDOW_MARKER = "__w";

    /** A group key's read after the group stage ({@code _id.k0}). */
    private static final String GROUP_KEY = "_id.k";

    /** A plan parameter's place in a command: the marker document the connector replaces by the value of the slot at
     *  its place among the markers (the connector's {@code MongoDbStatements}), as SQL's {@code ?}. */
    private static final String MARKER = "{\"$legendParam\": \"?\"}";

    private static final String RAISE_SENTINEL = "\u001f";
    private static final String POSITION_DIVIDER = "\u001e";

    private static final String G0 = "\"$$g0\"";
    private static final String G1 = "\"$$g1\"";
    private static final String G2 = "\"$$g2\"";
    private static final String THIS = "\"$$this\"";
    private static final String VALUE = "\"$$value\"";

    private static final List<String> DAY_NAMES = List.of("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday");
    private static final List<String> MONTH_NAMES = List.of("January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December");

    /** The aggregates with no native accumulator, or whose exact answer needs the group's values: pushed into an
     *  array by the group stage and reduced at the read. */
    private static final Set<SqlAgg.Fn> PUSHED = Set.of(SqlAgg.Fn.VAR_SAMP, SqlAgg.Fn.VAR_POP, SqlAgg.Fn.VARIANCE,
            SqlAgg.Fn.MEDIAN, SqlAgg.Fn.STRING_AGG, SqlAgg.Fn.QUANTILE_CONT, SqlAgg.Fn.QUANTILE_DISC, SqlAgg.Fn.MODE,
            SqlAgg.Fn.CORR, SqlAgg.Fn.COVAR_SAMP, SqlAgg.Fn.COVAR_POP, SqlAgg.Fn.ARG_MAX, SqlAgg.Fn.ARG_MIN);
    /** The aggregates over two columns, pushed as pairs ({@code {"x": .., "y": ..}}): the statistics, and the value
     *  at the greatest or least key. */
    private static final Set<SqlAgg.Fn> BIVARIATE = Set.of(SqlAgg.Fn.CORR, SqlAgg.Fn.COVAR_SAMP, SqlAgg.Fn.COVAR_POP,
            SqlAgg.Fn.ARG_MAX, SqlAgg.Fn.ARG_MIN);

    public MongoDb() {
        super(LEXICON);
    }

    // ==================================================================
    // Scope: how a column reference spells inside the pipeline
    // ==================================================================

    /** One level of the pipeline: inside a lookup, the left row's aliases are {@code let} variables and the right
     *  source's rows are flat; at the top, every alias is a field of the current row. */
    private static final class Scope {
        final List<String> variableAliases;
        final @com.legend.base.Nullable String flatAlias;
        /** The reads of this level's markers -- the group stage's aggregates, the lookup stages' subqueries -- each
         *  filled by its stage and read by the clauses after it. */
        final Map<String, SqlWriter.Piece> reads = new java.util.LinkedHashMap<>();
        /** The one alias a select's row is wrapped under while its clauses are written (null over a join, or once
         *  the projection has flattened the row): an unqualified column reads under it, as SQL resolves a column
         *  over one source. */
        @com.legend.base.Nullable String soleAlias;

        Scope(List<String> variableAliases, @com.legend.base.Nullable String flatAlias) {
            this.variableAliases = variableAliases;
            this.flatAlias = flatAlias;
        }
    }

    private final Deque<Scope> scopes = new ArrayDeque<>();

    /** A lambda's parameters in scope, each the pipeline variable it reads as ({@code $$v_x}, or {@code $$this} and
     *  {@code $$value} inside a reduce). */
    private final Deque<Map.Entry<String, String>> lambdaVariables = new ArrayDeque<>();

    /** A lookup's pipeline: the row's aliases become its {@code let} variables; {@code flat} is the alias whose rows
     *  the pipeline sees flat (a join's right source), or null for a subquery's pipeline (its own sources wrapped). */
    void pushLookupScope(List<String> variables, @com.legend.base.Nullable String flat) {
        scopes.push(new Scope(List.copyOf(variables), flat));
    }

    void popScope() {
        scopes.pop();
    }

    /** The alias a select's wrapped row reads unqualified columns under, or null once the row is flat. */
    void soleAlias(@com.legend.base.Nullable String alias) {
        scopes.getFirst().soleAlias = alias;
    }

    void aggregateReads(Map<String, SqlWriter.Piece> reads) {
        replaceReads(AGGREGATE_MARKER, reads);
    }

    void subqueryReads(Map<String, SqlWriter.Piece> reads) {
        replaceReads(SUBQUERY_MARKER, reads);
    }

    void windowReads(Map<String, SqlWriter.Piece> reads) {
        replaceReads(WINDOW_MARKER, reads);
    }

    /** This level's reads of one marker kind replaced: a select's markers are its own, and a nested select's, written
     *  earlier under the same names, are spent. */
    private void replaceReads(String marker, Map<String, SqlWriter.Piece> reads) {
        Scope scope = scopes.getFirst();
        scope.reads.keySet().removeIf(k -> k.startsWith(marker));
        scope.reads.putAll(reads);
    }

    private void reset() {
        scopes.clear();
        scopes.push(new Scope(List.of(), null));
        lambdaVariables.clear();
    }

    // ==================================================================
    // The query: a pipeline
    // ==================================================================

    @Override
    protected SqlWriter query(SqlWriter writer, SqlQuery q, int depth) {
        if (depth != 0) {
            throw new DialectCapability("a nested query reached MongoDb as a subquery: only a relation's own"
                    + " pipeline is written");
        }
        reset();
        return new MongoDbPipeline(this).command(writer, q);
    }

    @Override
    protected java.util.List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CAPS));
    }

    /** {@inheritDoc} A collection's {@code _id} declared {@code OTHER} (an objectId) is read as its hex text. */
    @Override
    protected boolean readsStored(com.legend.sql.SqlDdl.ColumnType t) {
        return t instanceof com.legend.sql.SqlDdl.ColumnType.Plain p
                && p.kind() == com.legend.sql.SqlDdl.ColumnType.Kind.OTHER;
    }

    @Override
    protected SqlWriter storedRead(SqlWriter writer, SqlExpr.StoredRead r) {
        return writer.append("{\"$toString\": ").expr(r.column(), 0).append("}");
    }

    /** {@inheritDoc} The engine's H2 text has no translation into a pipeline. */
    @Override
    public String fromEngineText(String statement) {
        throw new DialectCapability("the engine's H2 text reached MongoDb, which runs no SQL text");
    }

    // ==================================================================
    // Expressions
    // ==================================================================

    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        return switch (e) {
            case SqlExpr.Group g -> writer.expr(g.inner(), 0);
            case SqlExpr.TempTableInSplice t -> throw new IllegalStateException("temp-table IN splice '"
                    + t.tempTableName() + "' reached an executable dialect — plan-text vocabulary only");
            case SqlExpr.PlanParam p -> placeholder(writer, oneValue(p));
            // a dynamic pivot's probe plan: each copy of the per-key region binds its key's text
            case SqlExpr.KeyCell k -> writer.bindKey(MARKER, k.cell());
            case SqlExpr.RowOrder r -> throw new DialectCapability("a physical row order reached MongoDb: a"
                    + " collection's documents have no row order to read");
            case SqlExpr.Column c -> column(writer, c);
            case SqlExpr.StoredRead r -> storedRead(writer, r);
            case SqlExpr.Star s -> throw new IllegalStateException("a star reached the expression writer: the"
                    + " projection stage merges the sources it names");
            case SqlExpr.StarExcept se -> throw new DialectCapability("a star with excluded columns reached MongoDb:"
                    + " no shape yet");
            case SqlExpr.StringLit s -> MongoJson.literal(writer, s.value());
            case SqlExpr.FormatLit fl -> MongoJson.literal(writer, formatText(fl));
            case SqlExpr.DatePartLit p -> throw new IllegalStateException("a date part (" + p.part()
                    + ") reached the expression writer: the arm of its call spells it");
            case SqlExpr.JsonKindLit k -> MongoJson.literal(writer, jsonKindName(k.kind()));
            case SqlExpr.TypeNameLit t -> MongoJson.literal(writer, typeofName(t.scalar()));
            case SqlExpr.IntLit i -> writer.append("{\"$numberLong\": \"").append(i.value()).append("\"}");
            case SqlExpr.FloatLit f -> writer.append("{\"$numberDouble\": \"").append(floatLiteral(f.value()))
                    .append("\"}");
            case SqlExpr.DecimalLit d -> writer.append("{\"$numberDecimal\": \"").append(d.value().toPlainString())
                    .append("\"}");
            case SqlExpr.BoolLit b -> writer.append(b.value() ? "true" : "false");
            case SqlExpr.NullLit n -> writer.append("null");
            case SqlExpr.DateLit d -> writer.append(MongoJson.dateLiteral(d.iso()));
            case SqlExpr.TimestampLit t -> writer.append(MongoJson.dateLiteral(t.iso()));
            case SqlExpr.TimeLit t -> throw new DialectCapability("a time-of-day literal reached MongoDb, which has"
                    + " no time type");
            case SqlExpr.OrderedListAgg ola -> throw new IllegalStateException("an ordered list aggregate reached"
                    + " the expression writer outside a group stage");
            case SqlExpr.ArrayLit a -> arrayLit(writer, a.elements());
            case SqlExpr.StructLit s -> structLit(writer, s);
            case SqlExpr.StructGet g -> structGet(writer, g);
            case SqlExpr.Call c -> call(writer, c, parentPrec);
            case SqlExpr.Case c -> caseExpr(writer, c);
            case SqlExpr.Exists ex -> throw new DialectCapability("an EXISTS subquery reached MongoDb: a correlated"
                    + " subquery has no shape yet");
            case SqlExpr.InSubquery i -> throw new DialectCapability("an IN subquery reached MongoDb: a subquery in"
                    + " expression position has no shape yet");
            case SqlExpr.CheckedDefects ignored -> throw new DialectCapability("nested checked defects reached"
                    + " MongoDb: no shape yet");
            case SqlExpr.CheckedChildValue ignored -> throw new DialectCapability("a checked child's value reached"
                    + " MongoDb: no shape yet");
            case SqlExpr.Quantified q -> throw new IllegalStateException("a quantified comparison reached the"
                    + " expression writer outside the lookup stage that computes its rows (MongoDbPipeline"
                    + ".subqueries)");
            case SqlExpr.ScalarSubquery sq -> throw new DialectCapability("a scalar subquery reached MongoDb: no"
                    + " shape yet");
            case SqlExpr.CheckedOne co -> checkedOne(writer, co, parentPrec);
            case SqlExpr.CompactList cl -> compactList(writer, cl, parentPrec);
            case SqlExpr.DeferredTdsString d -> throw new IllegalStateException("deferred relation-toString reached"
                    + " the renderer — the execution boundary must resolve the dynamic column list first"
                    + " (DeferredTdsString id " + d.id() + ")");
            case SqlExpr.WindowCall w -> windowCall(writer, w);
            case SqlExpr.Lambda l -> lambda(writer, l);
            case SqlExpr.Cast c -> variantAwareCast(writer, c);
            case SqlExpr.FoldCall f -> foldCall(writer, f);
            case SqlExpr.JsonObject j -> jsonObject(writer, j);
            case SqlExpr.JsonArray j -> jsonArray(writer, j);
            case SqlExpr.JsonArrayAgg j -> jsonArrayAgg(writer, j);
            case SqlExpr.ReduceCollection rc -> reduceCollection(writer, rc);
            case SqlExpr.Membership m -> membership(writer, m);
            case SqlAgg.Reducer r -> reducer(writer, r);
        };
    }

    /** A column reference as a path: an aggregate's marker reads the group field; a group key reads {@code _id};
     *  a lambda's parameter is its variable (a field of it, the variable's field); inside a lookup the left row's
     *  aliases are variables and the right source's row is flat; otherwise the row holds each source under its
     *  alias. */
    private SqlWriter column(SqlWriter writer, SqlExpr.Column c) {
        String name = c.name();
        String table = c.table();
        Scope scope = scopes.getFirst();
        if (c.origin() == com.legend.sql.OutputCol.Origin.PER_KEY) {
            return perKeyColumn(writer, scope, table, name);
        }
        if (table == null) {
            if (name.startsWith(AGGREGATE_MARKER) || name.startsWith(SUBQUERY_MARKER)
                    || name.startsWith(WINDOW_MARKER)) {
                SqlWriter.Piece read = scope.reads.get(name);
                if (read == null) {
                    throw new IllegalStateException("marker " + name + " has no read in this scope");
                }
                return writer.piece(read);
            }
            if (name.startsWith(GROUP_KEY)) {
                return writer.append(MongoJson.string("$" + name));
            }
            for (Map.Entry<String, String> v : lambdaVariables) {
                if (v.getKey().equals(name)) {
                    return writer.append(MongoJson.string("$$" + v.getValue()));
                }
            }
            MongoJson.key(name);
            if (scope.soleAlias != null) {
                MongoJson.key(scope.soleAlias);
                return writer.append(MongoJson.path(scope.soleAlias, name));
            }
            return writer.append(MongoJson.path(name));
        }
        MongoJson.key(name);
        for (Map.Entry<String, String> v : lambdaVariables) {
            if (v.getKey().equals(table)) {
                // a lambda parameter's field (a struct element's, a pair's): the variable's field
                return writer.append(MongoJson.string("$$" + v.getValue() + "." + name));
            }
        }
        if (scope.variableAliases.contains(table)) {
            return writer.append(MongoJson.string("$$" + MongoJson.variable(table) + "." + name));
        }
        if (table.equals(scope.flatAlias)) {
            return writer.append(MongoJson.path(name));
        }
        MongoJson.key(table);
        return writer.append(MongoJson.path(table, name));
    }

    /** A per-key column of a dynamic pivot's probe plan ({@code _u0}) read in a copy of its per-key region:
     *  {@code "$_pct0._pk3__u0"} for the fourth key, the runner writing the key's position after {@code _pk}; the row's
     *  path as {@link #column}'s for a source's column (a flat row's own field, else under its alias). */
    private SqlWriter perKeyColumn(SqlWriter writer, Scope scope, @com.legend.base.Nullable String table,
            String name) {
        MongoJson.key(name);
        String under = table != null ? (table.equals(scope.flatAlias) ? null : table) : scope.soleAlias;
        if (under != null) {
            MongoJson.key(under);
            if (scope.variableAliases.contains(under)) {
                throw new DialectCapability("a dynamic pivot's per-key column read inside a lookup reached MongoDb:"
                        + " no shape yet");
            }
        }
        return writer.append("\"$" + (under == null ? "" : under + ".") + "_pk").counter().append("_" + name + "\"");
    }

    /** A plan parameter: a marker the connector replaces in place by the bound value, by its place among the
     *  statement's markers. */
    @Override
    protected SqlWriter placeholder(SqlWriter writer, SqlExpr.PlanParam p) {
        return writer.bind(MARKER, scalarBind(p));
    }

    /** A parameter written where ONE value goes: a list parameter is bound only as a whole list. */
    private static SqlExpr.PlanParam oneValue(SqlExpr.PlanParam p) {
        if (p.type() instanceof TypeFact.Typed t && t.type() instanceof SqlType.Array) {
            throw new DialectCapability("list parameter '" + p.name() + "' is bound only as a whole list (in,"
                    + " contains); written where one value goes, it is not bound");
        }
        return p;
    }

    @Override
    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        return writer.append("[").list(elements).append("]");
    }

    /** A struct is a document: each field by its name. */
    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit s) {
        writer.append("{");
        for (int i = 0; i < s.fields().size(); i++) {
            if (i > 0) {
                writer.append(", ");
            }
            writer.append(MongoJson.key(s.fields().get(i).name())).append(": ").expr(s.fields().get(i).value(), 0);
        }
        return writer.append("}");
    }

    @Override
    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        return writer.append("{\"$getField\": {\"field\": ").append(MongoJson.key(g.field())).append(", \"input\": ")
                .expr(g.source(), 0).append("}}");
    }

    /** A CASE as {@code $switch}: a null condition takes no branch, as SQL's does; no ELSE answers null. */
    @Override
    protected SqlWriter caseExpr(SqlWriter writer, SqlExpr.Case c) {
        writer.append("{\"$switch\": {\"branches\": [");
        for (int i = 0; i < c.whens().size(); i++) {
            if (i > 0) {
                writer.append(", ");
            }
            writer.append("{\"case\": ").expr(c.whens().get(i).condition(), 0).append(", \"then\": ")
                    .expr(c.whens().get(i).then(), 0).append("}");
        }
        writer.append("], \"default\": ");
        if (c.otherwise() == null) {
            writer.append("null");
        } else {
            writer.expr(c.otherwise(), 0);
        }
        return writer.append("}}");
    }

    @Override
    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        throw new IllegalStateException("a window function reached the expression writer outside the"
                + " $setWindowFields stage that computes it (MongoDbPipeline.windows)");
    }

    /**
     * A window function's outputs of a {@code $setWindowFields} stage, and the read of its field: the native window
     * operator where MongoDB has one ({@code $sum}, {@code $avg}, {@code $min}, {@code $max}, {@code $count},
     * {@code $stdDevSamp}, {@code $stdDevPop}, {@code $first}, {@code $last}, {@code $push}; {@code $documentNumber},
     * {@code $rank}, {@code $denseRank}; {@code $shift} for a lag or lead), the window written after it where the
     * operator takes one ({@code window}: the pipeline's, from the call's frame); the group's ordered and distinct
     * reductions pushed and reduced at the read, as the {@code $group} stage does; a percent rank, a cumulative
     * distribution and an ntile from a rank or a document number and the partition's count, read as the SQL
     * definitions say; an nth value from the frame's values. {@code window} writes {@code , "window": {...}} or
     * nothing (the partition entire, $setWindowFields' own default, which a partition's count takes). SUM carries
     * its count of non-null values beside it (S72's fix), as in a group.
     */
    SqlWriter.Piece windowOutput(SqlWriter w, SqlExpr.WindowCall call, String field, SqlWriter.Piece window) {
        String path = MongoJson.string("$" + field);
        String key = MongoJson.string(field);
        switch (call.fn()) {
            case SqlAgg.Reducer r -> {
                if (!pushRoute(r)) {
                    w.append(key).append(": ");
                    reducerOpen(w, r);
                    w.piece(window).append("}");
                    if (r.fn() == SqlAgg.Fn.SUM) {
                        String count = field + "_n";
                        w.append(", ").append(MongoJson.string(count)).append(": {\"$sum\": {\"$cond\": [{\"$eq\": [")
                                .expr(r.args().get(0), 0).append(", null]}, 0, 1]}").piece(window).append("}");
                        return MongoJson.text("{\"$cond\": [{\"$gt\": [" + MongoJson.string("$" + count) + ", 0]}, "
                                + path + ", null]}");
                    }
                    return MongoJson.text(path);
                }
                w.append(key).append(": {\"$push\": ");
                pushedValue(w, r);
                w.piece(window).append("}");
                return pushedRead(r, path);
            }
            case SqlAgg.RankingFn rf -> {
                return switch (rf.fn()) {
                    case ROW_NUMBER -> {
                        w.append(key).append(": {\"$documentNumber\": {}}");
                        yield MongoJson.text(path);
                    }
                    case RANK -> {
                        w.append(key).append(": {\"$rank\": {}}");
                        yield MongoJson.text(path);
                    }
                    case DENSE_RANK -> {
                        w.append(key).append(": {\"$denseRank\": {}}");
                        yield MongoJson.text(path);
                    }
                    case PERCENT_RANK -> {
                        // (rank - 1) / (count - 1), 0 over one row
                        String n = MongoJson.string("$" + field + "_n");
                        w.append(key).append(": {\"$rank\": {}}, ").append(MongoJson.string(field + "_n"))
                                .append(": {\"$count\": {}}");
                        yield MongoJson.text("{\"$cond\": [{\"$lte\": [" + n + ", 1]}, 0.0, {\"$divide\": [{\"$subtract\": ["
                                + path + ", 1]}, {\"$subtract\": [" + n + ", 1]}]}]}");
                    }
                    case CUME_DIST -> {
                        // the rows up to and including the current row's peers, over the partition's count
                        String n = MongoJson.string("$" + field + "_n");
                        w.append(key).append(": {\"$count\": {}").piece(window).append("}, ")
                                .append(MongoJson.string(field + "_n")).append(": {\"$count\": {}}");
                        yield MongoJson.text("{\"$divide\": [" + path + ", " + n + "]}");
                    }
                    case NTILE -> {
                        // SQL's buckets: floor((row - 1) * k / count) + 1, the first buckets taking the remainder
                        String n = MongoJson.string("$" + field + "_n");
                        w.append(key).append(": {\"$documentNumber\": {}}, ").append(MongoJson.string(field + "_n"))
                                .append(": {\"$count\": {}}");
                        SqlExpr k = rf.args().get(0);
                        yield x -> x.append("{\"$add\": [{\"$floor\": {\"$divide\": [{\"$multiply\": [{\"$subtract\": ["
                                + path + ", 1]}, ").expr(k, 0).append("]}, " + n + "]}}, 1]}");
                    }
                    default -> throw new DialectCapability("the window function " + rf.fn() + " reached MongoDb:"
                            + " not shaped yet");
                };
            }
            case SqlAgg.ValueFn vf -> {
                return switch (vf.fn()) {
                    case LAG, LEAD -> {
                        // $shift: the value by rows before (lag) or after (lead) the current one, else the default;
                        // its offset the literal $shift takes (the one the Lowerer passes: literals only)
                        if (vf.args().size() > 1 && !(vf.args().get(1) instanceof SqlExpr.IntLit)) {
                            throw new DialectCapability("a " + vf.fn() + " by a computed offset reached MongoDb,"
                                    + " whose $shift takes a literal");
                        }
                        long by = vf.args().size() > 1 ? ((SqlExpr.IntLit) vf.args().get(1)).value() : 1;
                        w.append(key).append(": {\"$shift\": {\"output\": ").expr(vf.args().get(0), 0)
                                .append(", \"by\": ").append(vf.fn() == SqlAgg.Fn.LAG ? -by : by)
                                .append(", \"default\": ");
                        if (vf.args().size() > 2) {
                            w.expr(vf.args().get(2), 0);
                        } else {
                            w.append("null");
                        }
                        w.append("}}");
                        yield MongoJson.text(path);
                    }
                    case FIRST_VALUE -> {
                        w.append(key).append(": {\"$first\": ").expr(vf.args().get(0), 0).piece(window).append("}");
                        yield MongoJson.text(path);
                    }
                    case LAST_VALUE -> {
                        w.append(key).append(": {\"$last\": ").expr(vf.args().get(0), 0).piece(window).append("}");
                        yield MongoJson.text(path);
                    }
                    case NTH_VALUE -> {
                        w.append(key).append(": {\"$push\": ").expr(vf.args().get(0), 0).piece(window).append("}");
                        SqlExpr n = vf.args().get(1);
                        yield x -> x.append("{\"$arrayElemAt\": [" + path + ", {\"$subtract\": [").expr(n, 0)
                                .append(", 1]}]}");
                    }
                    default -> throw new DialectCapability("the window function " + vf.fn() + " reached MongoDb:"
                            + " not shaped yet");
                };
            }
        }
    }

    /** A native reducer's window operator, opened: {@code {"$sum": <arg>} without its closing brace, so the stage
     *  writes the window inside it. */
    private void reducerOpen(SqlWriter w, SqlAgg.Reducer r) {
        String op = switch (r.fn()) {
            case SUM -> "$sum";
            case AVG -> "$avg";
            case MIN -> "$min";
            case MAX -> "$max";
            case COUNT -> "$count";
            case STDDEV_SAMP, STDDEV -> "$stdDevSamp";
            case STDDEV_POP -> "$stdDevPop";
            case ANY_VALUE -> "$first";
            default -> throw new DialectCapability("the window aggregate " + r.fn() + " reached MongoDb: not shaped"
                    + " yet");
        };
        if (r.fn() == SqlAgg.Fn.COUNT) {
            if (r.args().isEmpty()) {
                w.append("{\"$count\": {}");
            } else {
                // COUNT(x): the non-null values
                w.append("{\"$sum\": {\"$cond\": [{\"$eq\": [").expr(r.args().get(0), 0).append(", null]}, 0, 1]}");
            }
            return;
        }
        w.append("{\"").append(op).append("\": ").expr(r.args().get(0), 0);
    }

    @Override
    protected SqlWriter lambda(SqlWriter writer, SqlExpr.Lambda l) {
        throw new IllegalStateException("a lambda reached the expression writer outside the list operator that"
                + " applies it");
    }

    /** A cast as {@code $convert} to the BSON type the target names; a date to text in Pure's date spelling; a
     *  decimal rounded to its scale; a Variant (JSON) target is the value itself, a BSON value being the variant --
     *  a literal JSON text's value written as the document it spells ({@code $literal}: its fields are not
     *  expressions), a computed text refused by name (MongoDB parses no JSON in a pipeline). */
    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        SqlType target = c.target();
        if (target == SqlType.Scalar.JSON) {
            if (c.value() instanceof SqlExpr.StringLit json) {
                return writer.append("{\"$literal\": ").append(json.value().strip()).append("}");
            }
            if (c.value().type() instanceof TypeFact.Typed t && (t.type() == SqlType.Scalar.VARCHAR
                    || t.type() == SqlType.Scalar.LITERAL)) {
                throw new DialectCapability("a text parsed as a Variant reached MongoDb, whose pipeline parses no"
                        + " JSON");
            }
            return writer.expr(c.value(), 0);
        }
        if (target instanceof SqlType.Decimal d) {
            return writer.append("{\"$round\": [{\"$convert\": {\"input\": ").expr(c.value(), 0)
                    .append(", \"to\": \"decimal\"}}, ").append(d.scale()).append("]}");
        }
        if (target == SqlType.Scalar.TIMESTAMPTZ) {
            // a zoned timestamp is the instant: a text with its offset parses to it (a single-digit month or day
            // padded first, which Pure's parser reads and $convert does not), a date is one already
            boolean fromText = c.value() instanceof SqlExpr.StringLit || c.value().type() instanceof TypeFact.Typed t
                    && (t.type() == SqlType.Scalar.VARCHAR || t.type() == SqlType.Scalar.LITERAL);
            if (!fromText) {
                return writer.append("{\"$convert\": {\"input\": ").expr(c.value(), 0).append(", \"to\": \"date\"}}");
            }
            String pad = "{\"$cond\": [{\"$eq\": [{\"$strLenCP\": %s}, 1]}, {\"$concat\": [\"0\", %s]}, %s]}";
            String cap1 = "{\"$arrayElemAt\": [\"$$m.captures\", 1]}";
            String cap2 = "{\"$arrayElemAt\": [\"$$m.captures\", 2]}";
            return writer.append("{\"$let\": {\"vars\": {\"s\": ").expr(c.value(), 0).append("}, \"in\": {\"$let\":"
                    + " {\"vars\": {\"m\": {\"$regexFind\": {\"input\": \"$$s\", \"regex\":"
                    + " \"^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})(.*)$\"}}}, \"in\": {\"$convert\": {\"input\": {\"$cond\":"
                    + " [{\"$eq\": [\"$$m\", null]}, \"$$s\", {\"$concat\": [{\"$arrayElemAt\": [\"$$m.captures\", 0]},"
                    + " \"-\", " + String.format(java.util.Locale.ROOT, pad, cap1, cap1, cap1) + ", \"-\", "
                    + String.format(java.util.Locale.ROOT, pad, cap2, cap2, cap2) + ", {\"$arrayElemAt\":"
                    + " [\"$$m.captures\", 3]}]}]}, \"to\": \"date\"}}}}}}");
        }
        if (target instanceof SqlType.Array || target instanceof SqlType.Map) {
            // a BSON array or document is untyped: the cast to a list or map type is the lowering's typing of the
            // value, which is itself here (the MongoDB lane's first run, 2026-10-17: 21 list and map cases)
            return writer.expr(c.value(), 0);
        }
        if (!(target instanceof SqlType.Scalar s)) {
            throw new DialectCapability("a cast to " + target + " reached MongoDb: only scalar and decimal targets"
                    + " convert");
        }
        boolean text = s == SqlType.Scalar.VARCHAR || s == SqlType.Scalar.LITERAL;
        if (text && (c.value() instanceof SqlExpr.JsonObject || c.value() instanceof SqlExpr.JsonArray)) {
            // the wire's JSON object as TEXT: written by the pipeline (D10), MongoDB having no JSON writer of its own
            return jsonText(writer, c.value());
        }
        if (text && c.value().type() instanceof TypeFact.Typed t && t.type() == SqlType.Scalar.JSON) {
            if (c.value() instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET) {
                // a Variant element read as text is the element's own string (DuckDb: ->> strips JSON's quoting),
                // any other kind its JSON text
                return writer.append("{\"$let\": {\"vars\": {\"e\": ").expr(c.value(), 0).append("}, \"in\":"
                        + " {\"$cond\": [{\"$eq\": [{\"$type\": \"$$e\"}, \"string\"]}, \"$$e\", ")
                        .piece(x -> cellText(x, SqlExpr.Column.derived(null, "__e"), MongoJson.text("\"$$e\"")))
                        .append("]}}}");
            }
            // a Variant's JSON text: the BSON value written as JSON by its kind at run time (D10)
            return cellText(writer, c.value());
        }
        if (text && temporal(c.value()) != null) {
            return writer.append("{\"$dateToString\": {\"date\": ").expr(c.value(), 0).append(", \"format\": ")
                    .append(MongoJson.string(temporal(c.value()) == SqlType.Scalar.DATE ? "%Y-%m-%d"
                            : "%Y-%m-%dT%H:%M:%S.%L+0000")).append("}}");
        }
        if (text && mayBeNumber(c.value())) {
            return numberText(writer, c.value());
        }
        if (s == SqlType.Scalar.BOOLEAN) {
            // a string converts to true whatever it says ($convert: every non-empty string is true), so a boolean's
            // text is read by its word (a Variant-carried boolean, the MongoDB lane's first run, 2026-10-17)
            return MongoJson.guarded(writer, List.of(c.value()), MongoJson.text("{\"$cond\": [{\"$eq\":"
                    + " [{\"$type\": " + G0 + "}, \"string\"]}, {\"$switch\": {\"branches\": [{\"case\":"
                    + " {\"$eq\": [{\"$toLower\": " + G0 + "}, \"true\"]}, \"then\": true}, {\"case\": {\"$eq\":"
                    + " [{\"$toLower\": " + G0 + "}, \"false\"]}, \"then\": false}], \"default\": "
                    + raiseText("a text that is no boolean reached a cast to Boolean on MongoDb") + "}},"
                    + " {\"$convert\": {\"input\": " + G0 + ", \"to\": \"bool\"}}]}"));
        }
        return writer.append("{\"$convert\": {\"input\": ").expr(c.value(), 0).append(", \"to\": ")
                .append(MongoJson.string(scalarTypeName(s))).append("}}");
    }

    /** Whether the value may be a double or a decimal at run time, so the {@code $type} test decides: typed so (a
     *  bare float literal's fact is the decimal of its digits, the NUMERIC CHARTER's rule for a database that types
     *  the literal, while this target writes it as a double; a HUGEINT is a Decimal128 here); or of no type fact. */
    private static boolean mayBeNumber(SqlExpr e) {
        return !(e.type() instanceof TypeFact.Typed t) || t.type() == SqlType.Scalar.DOUBLE
                || t.type() == SqlType.Scalar.HUGEINT || t.type() instanceof SqlType.Decimal;
    }

    /** The 128 ASCII characters in code order, as a JSON string: the character-code table. */
    private static final String ASCII;

    static {
        StringBuilder table = new StringBuilder();
        for (int c = 0; c < 128; c++) {
            table.append((char) c);
        }
        ASCII = MongoJson.string(table.toString());
    }

    /** The alias under which a number's text is bound once for its spelling ({@link #numberText}). */
    private static final String NUMBER_TEXT = "__nt";

    /**
     * A number's text as Pure spells it, measured on 8.0.17 (the MongoDB line's homework 3, 2026-10-17):
     * {@code $toString} spells a double in scientific notation past 1e15 and below 1e-4 ({@code 1e+20}) and without a
     * fraction when integral ({@code 2}), and a Decimal128 in its own scientific form ({@code 1.00000000000000E+20},
     * {@code 1.5000000000E-8}); Pure never spells an exponent and a Float always carries a fraction digit. So the text
     * is bound once and read as a column of a lookup scope, and the portable spelling ({@link FloatCanonText}) is
     * written over it: a double gets Pure's whole Float spelling; a decimal (a HUGEINT cast, a {@code DECIMAL(38,18)}
     * cast, a Pure Decimal) only its exponent unfolded, and a HUGEINT, an integer, its digits alone.
     */
    private SqlWriter numberText(SqlWriter writer, SqlExpr value) {
        boolean hugeInt = value.type() instanceof TypeFact.Typed t && t.type() == SqlType.Scalar.HUGEINT;
        return MongoJson.guarded(writer, List.of(value), w -> numberTextOf(w, MongoJson.var("g0"), hugeInt));
    }

    /**
     * {@link #numberText} over a value already bound to a variable, which {@code bound} writes, spelled ONCE in the
     * pipeline's own operators (the MariaDb precedent: a dialect whose double text is a long expression spells it its
     * own way): the text's sign, digits and exponent found by one {@code $regexFind} and bound, the point moved as text
     * -- the portable spelling's three shapes ({@link FloatCanonText}: the point before the digits, inside them, or
     * past them) -- so a cast to text costs about a kilobyte wherever the lowering writes one (the MongoDB lane's first
     * run, 2026-10-17: the portable composition rendered to 17 KB per cast, and a print path that casts a value a
     * few hundred times ran the driver out of memory). A double (by its run-time type: a float literal is written as
     * one whatever its fact says, a HUGEINT-fact literal included) gets Pure's Float spelling: every zero {@code 0.0},
     * a fraction digit kept; a decimal its exponent unfolded alone; a HUGEINT, an integer, its digits alone.
     */
    private void numberTextOf(SqlWriter w, SqlWriter.Piece bound, boolean hugeInt) {
        // the shapes over the bound parts: $$s the text, $$g the sign, $$d the digits (the point removed), $$p the
        // point's position among them, $$n their count
        String zeros = "{\"$reduce\": {\"input\": {\"$range\": [0, %s]}, \"initialValue\": \"\", \"in\":"
                + " {\"$concat\": [\"$$value\", \"0\"]}}}";
        String unfolded = "{\"$let\": {\"vars\": {\"g\": {\"$arrayElemAt\": [\"$$m.captures\", 0]}, \"d\":"
                + " {\"$concat\": [{\"$arrayElemAt\": [\"$$m.captures\", 1]}, {\"$ifNull\": [{\"$arrayElemAt\":"
                + " [\"$$m.captures\", 2]}, \"\"]}]}, \"p\": {\"$add\": [{\"$strLenCP\": {\"$arrayElemAt\":"
                + " [\"$$m.captures\", 1]}}, {\"$toInt\": {\"$arrayElemAt\": [\"$$m.captures\", 3]}}]}},"
                + " \"in\": {\"$let\": {\"vars\": {\"n\": {\"$strLenCP\": \"$$d\"}}, \"in\": {\"$concat\": [\"$$g\","
                + " {\"$switch\": {\"branches\": ["
                + "{\"case\": {\"$lte\": [\"$$p\", 0]}, \"then\": {\"$concat\": [\"0.\", "
                + String.format(java.util.Locale.ROOT, zeros, "{\"$subtract\": [0, \"$$p\"]}") + ", \"$$d\"]}},"
                + " {\"case\": {\"$gte\": [\"$$p\", \"$$n\"]}, \"then\": {\"$concat\": [\"$$d\", "
                + String.format(java.util.Locale.ROOT, zeros, "{\"$subtract\": [\"$$p\", \"$$n\"]}") + ", \".0\"]}}],"
                + " \"default\": {\"$concat\": [{\"$substrCP\": [\"$$d\", 0, \"$$p\"]}, \".\", {\"$substrCP\": [\"$$d\","
                + " \"$$p\", {\"$subtract\": [\"$$n\", \"$$p\"]}]}]}}}]}}}}}";
        // the text without an exponent: a double gets its fraction digit (and every zero is 0.0); a decimal is as it is
        String plain = "{\"$cond\": [{\"$eq\": [{\"$type\": " + "BOUND" + "}, \"double\"]}, {\"$cond\": [{\"$regexMatch\":"
                + " {\"input\": \"$$s\", \"regex\": \"^-?0+(\\\\.0+)?$\"}}, \"0.0\", {\"$cond\": [{\"$regexMatch\": {\"input\":"
                + " \"$$s\", \"regex\": \"^-?[0-9]+$\"}}, {\"$concat\": [\"$$s\", \".0\"]}, \"$$s\"]}]}, \"$$s\"]}";
        String spelled = "{\"$let\": {\"vars\": {\"m\": {\"$regexFind\": {\"input\": \"$$s\", \"regex\":"
                + " \"^(-?)([0-9]+)(?:\\\\.([0-9]+))?[eE]([+-]?[0-9]+)$\"}}}, \"in\": {\"$cond\": [{\"$eq\": [\"$$m\","
                + " null]}, " + plain + ", " + unfolded + "]}}}";
        if (hugeInt) {
            // an integer's digits: the unfolded form of a scientific Decimal128 ends in ".0", which an integer's text
            // has not -- a double under a HUGEINT fact (a float literal past the 64-bit edge) keeps its Float spelling
            spelled = "{\"$let\": {\"vars\": {\"t\": " + spelled + "}, \"in\": {\"$cond\": [{\"$or\": [{\"$eq\":"
                    + " [{\"$type\": BOUND}, \"double\"]}, {\"$not\": {\"$regexMatch\": {\"input\": \"$$t\", \"regex\":"
                    + " \"\\\\.0$\"}}}]}, \"$$t\", {\"$substrCP\": [\"$$t\", 0, {\"$subtract\": [{\"$strLenCP\": \"$$t\"},"
                    + " 2]}]}]}}}";
        }
        w.append("{\"$let\": {\"vars\": {\"s\": {\"$toString\": ").piece(bound).append("}}, \"in\": ");
        // the bound value is read again wherever its run-time type decides
        int from = 0;
        for (int at = spelled.indexOf("BOUND"); at >= 0; at = spelled.indexOf("BOUND", from)) {
            w.append(spelled.substring(from, at)).piece(bound);
            from = at + "BOUND".length();
        }
        w.append(spelled.substring(from));
        w.append("}}");
    }

    /** The format a date's text takes in a wire: a date alone, or Pure's date-time to the millisecond. */
    private static String dateFormat(SqlExpr e) {
        return temporal(e) == SqlType.Scalar.DATE ? "%Y-%m-%d" : "%Y-%m-%dT%H:%M:%S.%L+0000";
    }

    /**
     * The JSON text of an object or an array the wire builds ({@code CAST(json_object(...) AS VARCHAR)}): the
     * pipeline writes it as text, key by key, each value's text by its kind at run time -- null, a string escaped and
     * quoted, a date in its format, a number or a boolean as its own text -- and a nested document refused (raised,
     * since the value's kind is the row's).
     */
    private SqlWriter jsonText(SqlWriter writer, SqlExpr value) {
        return switch (value) {
            case SqlExpr.JsonObject j -> objectText(writer, j);
            case SqlExpr.JsonArray a -> arrayText(writer, a.elements());
            default -> throw new IllegalStateException("not a JSON constructor: " + value);
        };
    }

    private SqlWriter objectText(SqlWriter writer, SqlExpr.JsonObject j) {
        writer.append("{\"$concat\": [\"{\"");
        for (int i = 0; i + 1 < j.kv().size(); i += 2) {
            if (!(j.kv().get(i) instanceof SqlExpr.StringLit key)) {
                throw new DialectCapability("a JSON object with a computed key reached MongoDb: a document's field"
                        + " names are spelled");
            }
            writer.append(", ").append(MongoJson.string((i > 0 ? "," : "") + MongoJson.string(key.value()) + ":"))
                    .append(", ");
            cellText(writer, j.kv().get(i + 1));
        }
        return writer.append(", \"}\"]}");
    }

    private SqlWriter arrayText(SqlWriter writer, List<SqlExpr> elements) {
        writer.append("{\"$concat\": [\"[\"");
        for (int i = 0; i < elements.size(); i++) {
            writer.append(i > 0 ? ", \",\", " : ", ");
            cellText(writer, elements.get(i));
        }
        return writer.append(", \"]\"]}");
    }

    /** How deep a nested document's JSON text goes: the writer is unrolled, MongoDB's expressions having no
     *  recursion, one level of text per level of nesting -- eleven BSON levels each, under the server's limit of
     *  two hundred with the expression around it (a Pair collection's text, deep around, overflowed at eight
     *  levels of fifteen) -- and a document nested deeper is refused by a raise. */
    static final int JSON_DEPTH = 6;

    /** One value's JSON text, by its kind at run time: null, a string escaped and quoted, a date in its format
     *  (the value's static kind decides a date alone), a number as Pure spells it, a boolean as its word, and an
     *  array or a document written element by element -- a document's keys escaped and quoted -- to
     *  {@link #JSON_DEPTH} levels. */
    private SqlWriter cellText(SqlWriter writer, SqlExpr value) {
        return cellText(writer, value, x -> x.expr(value, 0));
    }

    /** {@link #cellText(SqlWriter, SqlExpr)} over the value {@code bound} writes, whose kind {@code value} types. */
    private SqlWriter cellText(SqlWriter writer, SqlExpr value, SqlWriter.Piece bound) {
        writer.append("{\"$let\": {\"vars\": {\"v\": ").piece(bound).append("}, \"in\": ");
        jsonLevel(writer, dateFormat(value), JSON_DEPTH);
        return writer.append("}}");
    }

    /** The JSON text of {@code $$v} at one level of nesting. */
    private void jsonLevel(SqlWriter writer, String dateFormat, int depth) {
        writer.append("{\"$switch\":"
                + " {\"branches\": [{\"case\": {\"$eq\": [\"$$v\", null]}, \"then\": \"null\"},"
                + " {\"case\": {\"$eq\": [{\"$type\": \"$$v\"}, \"string\"]}, \"then\": {\"$concat\": [\"\\\"\", "
                + escapedJsonString("\"$$v\"") + ", \"\\\"\"]}},"
                + " {\"case\": {\"$eq\": [{\"$type\": \"$$v\"}, \"date\"]}, \"then\": {\"$concat\": [\"\\\"\","
                + " {\"$dateToString\": {\"date\": \"$$v\", \"format\": ").append(MongoJson.string(dateFormat))
                .append("}}, \"\\\"\"]}},"
                + " {\"case\": {\"$in\": [{\"$type\": \"$$v\"}, [\"object\", \"array\"]]}, \"then\": ");
        if (depth == 0) {
            writer.append(raiseText("a document nested beyond " + JSON_DEPTH + " levels reached MongoDb's JSON"
                    + " writer"));
        } else {
            // the elements -- a document's entries, an array's values -- folded into one text, each written at
            // the next level, a document's keyed, commas between, the kind's brackets around
            String isObject = "{\"$eq\": [{\"$type\": \"$$v\"}, \"object\"]}";
            writer.append("{\"$concat\": [{\"$cond\": [" + isObject + ", \"{\", \"[\"]}, {\"$reduce\": {\"input\":"
                    + " {\"$cond\": [" + isObject + ", {\"$objectToArray\": \"$$v\"}, \"$$v\"]}, \"initialValue\": \"\","
                    + " \"in\": {\"$concat\": [\"$$value\", {\"$cond\": [{\"$eq\": [\"$$value\", \"\"]}, \"\", \",\"]},"
                    + " {\"$cond\": [" + isObject + ", {\"$concat\": [\"\\\"\", " + escapedJsonString("\"$$this.k\"")
                    + ", \"\\\":\"]}, \"\"]}, {\"$let\": {\"vars\": {\"v\": {\"$cond\": [" + isObject
                    + ", \"$$this.v\", \"$$this\"]}}, \"in\": ");
            jsonLevel(writer, "%Y-%m-%dT%H:%M:%S.%L+0000", depth - 1);
            writer.append("}}]}}}, {\"$cond\": [" + isObject + ", \"}\", \"]\"]}]}");
        }
        writer.append("}, {\"case\": {\"$in\": [{\"$type\": \"$$v\"}, [\"double\", \"decimal\"]]}, \"then\": ");
        // a number as Pure spells it (the SQL dialects' JSON writers spell a double with its fraction and no exponent)
        numberTextOf(writer, MongoJson.var("v"), false);
        writer.append("}], \"default\": {\"$toString\": \"$$v\"}}}");
    }

    /** The string {@code s}, with JSON's escapes: a backslash, a quote, a newline, a return, a tab. */
    private static String escapedJsonString(String s) {
        for (String[] r : List.of(new String[] {"\\", "\\\\"}, new String[] {"\"", "\\\""},
                new String[] {"\n", "\\n"}, new String[] {"\r", "\\r"}, new String[] {"\t", "\\t"})) {
            s = "{\"$replaceAll\": {\"input\": " + s + ", \"find\": " + MongoJson.string(r[0]) + ", \"replacement\": "
                    + MongoJson.string(r[1]) + "}}";
        }
        return s;
    }

    /** A raise of a fixed message, as {@link #raise} writes one: the message depends on the row, so the optimizer
     *  cannot fold it ({@link #UNFOLDED}). */
    private static String raiseText(String message) {
        return "{\"$convert\": {\"input\": {\"$concat\": [" + MongoJson.string(RAISE_SENTINEL + message
                + RAISE_SENTINEL) + ", " + UNFOLDED + "]}, \"to\": \"int\"}}";
    }

    /** DATE or TIMESTAMP when {@code e} is typed so; null otherwise. */
    private static SqlType.@com.legend.base.Nullable Scalar temporal(SqlExpr e) {
        return e.type() instanceof TypeFact.Typed t
                && (t.type() == SqlType.Scalar.DATE || t.type() == SqlType.Scalar.TIMESTAMP)
                ? (SqlType.Scalar) t.type() : null;
    }

    /** A fold as {@code $reduce}, the lambda's Pure order (element, accumulator) read as {@code $$this} and
     *  {@code $$value}; the empty (null) collection folds to the initial value. */
    @Override
    protected SqlWriter foldCall(SqlWriter writer, SqlExpr.FoldCall f) {
        List<String> params = f.lambda().params();
        if (params.size() != 2) {
            throw new IllegalStateException("a fold's lambda takes (element, accumulator): " + params);
        }
        writer.append("{\"$reduce\": {\"input\": {\"$ifNull\": [").expr(f.source(), 0)
                .append(", []]}, \"initialValue\": ").expr(f.init(), 0).append(", \"in\": ");
        withLambda(writer, f.lambda(), List.of("this", "value"), x -> x.expr(f.lambda().body(), 0));
        return writer.append("}}");
    }

    /** A JSON object: alternating literal keys and values, as a document. */
    @Override
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        writer.append("{");
        for (int i = 0; i + 1 < j.kv().size(); i += 2) {
            if (i > 0) {
                writer.append(", ");
            }
            if (!(j.kv().get(i) instanceof SqlExpr.StringLit key)) {
                throw new DialectCapability("a JSON object with a computed key reached MongoDb: a document's field"
                        + " names are spelled");
            }
            writer.append(MongoJson.key(key.value())).append(": ").expr(j.kv().get(i + 1), 0);
        }
        return writer.append("}");
    }

    @Override
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.append("[").list(j.elements()).append("]");
    }

    @Override
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        throw new IllegalStateException("a JSON array aggregate reached the expression writer outside a group"
                + " stage");
    }

    /** A collection value reduced by an aggregate: its non-null elements, as the group reads reduce. */
    @Override
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        SqlWriter.Piece nonNull = x -> nonNull(x, y -> y.expr(rc.collection(), 0));
        return arrayReduction(writer, rc.reducer(), nonNull, rc.extras());
    }

    /** {@code needle} in a collection value: null when the needle is null (SQL's answer); a list parameter is bound
     *  whole, as one array. */
    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        if (m.collection() instanceof SqlExpr.PlanParam p) {
            return MongoJson.guarded(writer, List.of(m.needle()), x -> x.append("{\"$in\": [" + G0 + ", ")
                    .bind(MARKER, listBind(p)).append("]}"));
        }
        return MongoJson.guardedOp(writer, "$in", List.of(m.needle(), m.collection()));
    }

    private static RenderedStatement.Bind listBind(SqlExpr.PlanParam p) {
        if (!(p.type() instanceof TypeFact.Typed t && t.type() instanceof SqlType.Array array
                && array.element() instanceof SqlType.Scalar element)) {
            throw new DialectCapability("plan parameter '" + p.name() + "' is not a list parameter (an array of a"
                    + " named element type): it is not bound as a whole list");
        }
        return new RenderedStatement.Bind(p.name(), new RenderedStatement.Binding.Array(element.name()));
    }

    // ==================================================================
    // Functions
    // ==================================================================

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        // every function, each by name: MongoDB's operator, an emulation over its operators, or a refusal
        return switch (c.fn()) {
            // ---- logic: MongoDB's AND and OR read null as false, which is SQL's answer where a predicate decides;
            // NOT answers null for null (D7)
            case AND -> MongoJson.op(writer, "$and", a);
            case OR -> MongoJson.op(writer, "$or", a);
            case NOT -> MongoJson.guardedOp(writer, "$not", a);
            // ---- comparison: three-valued over BSON's total comparisons (D7)
            case EQUAL -> MongoJson.guardedOp(writer, "$eq", a);
            case NOT_EQUAL -> MongoJson.guardedOp(writer, "$ne", a);
            case LESS -> MongoJson.guardedOp(writer, "$lt", a);
            case LESS_EQUAL -> MongoJson.guardedOp(writer, "$lte", a);
            case GREATER -> MongoJson.guardedOp(writer, "$gt", a);
            case GREATER_EQUAL -> MongoJson.guardedOp(writer, "$gte", a);
            case IS_DISTINCT_FROM, NULL_SAFE_NOT_EQUAL -> MongoJson.op(writer, "$ne", a);
            case NULL_SAFE_EQUAL -> MongoJson.op(writer, "$eq", a);
            case XOR -> MongoJson.guardedOp(writer, "$ne", a);
            // ---- arithmetic: MongoDB's operators answer null for a null operand already
            case PLUS -> MongoJson.op(writer, "$add", a);
            case MINUS -> MongoJson.op(writer, "$subtract", a);
            case TIMES -> MongoJson.op(writer, "$multiply", a);
            // Pure's divide answers a Float, every dialect dividing as doubles: $divide keeps two Decimal128s'
            // 34 digits (2026-10-20, testDecimalDivide)
            case DIVIDE -> writer.append("{\"$divide\": [{\"$toDouble\": ").expr(a.get(0), 0)
                    .append("}, {\"$toDouble\": ").expr(a.get(1), 0).append("}]}");
            case MOD -> flooredMod(writer, a);
            case REM -> writer.append("{\"$mod\": [").expr(a.get(0), 0).append(", ").piece(unfolded(a.get(1)))
                    .append("]}");
            case NEGATE -> negated(writer, a);
            case ABS -> MongoJson.op1(writer, "$abs", a.get(0));
            case INT_DIVIDE -> intDivide(writer, a);
            // ---- null and membership
            case IS_NULL -> nullTest(writer, a, true);
            case IS_NOT_NULL -> nullTest(writer, a, false);
            case IN -> inList(writer, a);
            case COALESCE -> coalesce(writer, a);
            // ---- numbers
            case PARSE_INT -> MongoJson.op1(writer, "$toLong", a.get(0));
            case SQRT -> writer.append("{\"$sqrt\": ").piece(unfolded(a.get(0))).append("}");
            case CBRT -> cubeRoot(writer, a);
            case EXP -> MongoJson.op1(writer, "$exp", a.get(0));
            case LN -> MongoJson.op1(writer, "$ln", a.get(0));
            case LOG10 -> MongoJson.op1(writer, "$log10", a.get(0));
            // a power is a Float, as Pure's pow is ($pow of two integers answers an integer)
            case POW -> MongoJson.guarded(writer, a, MongoJson.text("{\"$pow\": [{\"$toDouble\": " + G0 + "}, " + G1
                    + "]}"));
            case PI -> writer.append("{\"$numberDouble\": \"3.141592653589793\"}");
            case SIN -> MongoJson.op1(writer, "$sin", a.get(0));
            case COS -> MongoJson.op1(writer, "$cos", a.get(0));
            case TAN -> MongoJson.op1(writer, "$tan", a.get(0));
            case ASIN -> domainGuarded(writer, "$asin", a);
            case ACOS -> domainGuarded(writer, "$acos", a);
            case ATAN -> MongoJson.op1(writer, "$atan", a.get(0));
            case ATAN2 -> MongoJson.op(writer, "$atan2", a);
            case SINH -> MongoJson.op1(writer, "$sinh", a.get(0));
            case COSH -> MongoJson.op1(writer, "$cosh", a.get(0));
            case TANH -> MongoJson.op1(writer, "$tanh", a.get(0));
            case COT -> cotangent(writer, a);
            case RADIANS -> MongoJson.op1(writer, "$degreesToRadians", a.get(0));
            case DEGREES -> MongoJson.op1(writer, "$radiansToDegrees", a.get(0));
            case CEILING -> longOf(writer, "$ceil", a);
            case FLOOR -> longOf(writer, "$floor", a);
            case FLOOR_RAW -> MongoJson.op1(writer, "$floor", a.get(0));
            case ROUND -> roundHalfEven(writer, a);
            case ROUND_HALF_UP -> roundHalfUp(writer, a);
            case SIGN -> sign(writer, a);
            case GREATEST -> MongoJson.op(writer, "$max", a);
            case LEAST -> MongoJson.op(writer, "$min", a);
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(writer, c.fn(), a);
            case BIT_NOT -> MongoJson.op1(writer, "$bitNot", a.get(0));
            case HASH -> hashSigned(writer, a);
            // ---- strings (a string operator over null errors or answers "", so each is null-guarded)
            case CONCAT -> concatSkippingNull(writer, a);
            case CONCAT_JOIN -> MongoJson.op(writer, "$concat", a);
            case LENGTH -> length(writer, a);
            case UPPER -> MongoJson.guardedOp(writer, "$toUpper", a);
            case LOWER -> MongoJson.guardedOp(writer, "$toLower", a);
            case SUBSTRING -> substring(writer, a);
            case STRPOS -> MongoJson.guarded(writer, a, MongoJson.text("{\"$add\": [{\"$indexOfCP\": [" + G0 + ", "
                    + G1 + "]}, 1]}"));
            case STARTS_WITH -> MongoJson.guarded(writer, a, MongoJson.text("{\"$eq\": [{\"$indexOfCP\": [" + G0
                    + ", " + G1 + "]}, 0]}"));
            case ENDS_WITH -> endsWith(writer, a);
            case MATCHES -> regexMatch(writer, a, false);
            case REGEXP_FULL_MATCH -> regexMatch(writer, a, true);
            case REGEXP_EXTRACT -> regexExtract(writer, a);
            case REGEXP_EXTRACT_ALL -> regexExtractAll(writer, a);
            case REGEXP_REPLACE -> regexReplace(writer, a);
            case LEFT -> MongoJson.guarded(writer, a, MongoJson.text("{\"$substrCP\": [" + G0 + ", 0, {\"$max\":"
                    + " [0, {\"$min\": [" + G1 + ", {\"$strLenCP\": " + G0 + "}]}]}]}"));
            case RIGHT -> rightChars(writer, a);
            case LPAD -> pad(writer, a, true);
            case RPAD -> pad(writer, a, false);
            case TRIM -> trim(writer, "$trim", a);
            case LTRIM -> trim(writer, "$ltrim", a);
            case RTRIM -> trim(writer, "$rtrim", a);
            case REPLACE -> replace(writer, a);
            case SPLIT -> MongoJson.op(writer, "$split", a);
            case SPLIT_PART -> splitPart(writer, a, false);
            case PURE_SPLIT_PART -> splitPartCall(writer, a);
            case TRANSLATE -> throw new DialectCapability("translate reached MongoDb, which has no character-map"
                    + " operator");
            case REVERSE_STRING -> MongoJson.guarded(writer, a, MongoJson.text("{\"$reduce\": {\"input\":"
                    + " {\"$range\": [0, {\"$strLenCP\": " + G0 + "}]}, \"initialValue\": \"\", \"in\":"
                    + " {\"$concat\": [{\"$substrCP\": [" + G0 + ", " + THIS + ", 1]}, " + VALUE + "]}}}"));
            // MongoDB has no character-code operator: the ASCII table spelled as text, a code its position in it
            // (a character beyond it raised by name)
            case ASCII_CODE -> MongoJson.guarded(writer, a, MongoJson.text("{\"$let\": {\"vars\": {\"c\":"
                    + " {\"$indexOfCP\": [" + ASCII + ", {\"$substrCP\": [" + G0 + ", 0, 1]}]}}, \"in\": {\"$cond\":"
                    + " [{\"$lt\": [\"$$c\", 0]}, " + raiseText("a character beyond ASCII reached ascii on MongoDb,"
                            + " which has no character-code operator") + ", \"$$c\"]}}}"));
            case CHR -> MongoJson.guarded(writer, a, MongoJson.text("{\"$cond\": [{\"$or\": [{\"$lt\": [" + G0
                    + ", 0]}, {\"$gte\": [" + G0 + ", 128]}]}, " + raiseText("a code beyond ASCII reached char on"
                            + " MongoDb, which has no code-to-character operator") + ", {\"$substrCP\": [" + ASCII
                    + ", " + G0 + ", 1]}]}"));
            case UC_FIRST -> firstCased(writer, a, "$toUpper");
            case LC_FIRST -> firstCased(writer, a, "$toLower");
            case REPEAT_STR -> MongoJson.guarded(writer, a, MongoJson.text("{\"$reduce\": {\"input\": {\"$range\":"
                    + " [0, {\"$max\": [0, " + G1 + "]}]}, \"initialValue\": \"\", \"in\": {\"$concat\": [" + VALUE
                    + ", " + G0 + "]}}}"));
            // base64 (8.0's conversions between text and binary data): a text's UTF-8 bytes written as base64; the
            // reverse reads base64, padded to a multiple of four first (the Pure function takes unpadded text)
            case ENCODE_BASE64 -> MongoJson.guarded(writer, a, MongoJson.text("{\"$convert\": {\"input\":"
                    + " {\"$convert\": {\"input\": " + G0 + ", \"to\": \"binData\", \"format\": \"utf8\"}},"
                    + " \"to\": \"string\", \"format\": \"base64\"}}"));
            case DECODE_BASE64 -> MongoJson.guarded(writer, a, MongoJson.text("{\"$convert\": {\"input\":"
                    + " {\"$convert\": {\"input\": {\"$concat\": [" + G0 + ", {\"$substrCP\": [\"==\", 0, {\"$mod\":"
                    + " [{\"$subtract\": [4, {\"$mod\": [{\"$strLenCP\": " + G0 + "}, 4]}]}, 4]}]}]}, \"to\":"
                    + " \"binData\", \"format\": \"base64\"}}, \"to\": \"string\", \"format\": \"utf8\"}}"));
            // no string-distance operator: the distances composed over the characters, as Postgres's and SQLite's
            // recursive walks and Databricks's aggregates compose them, here as folds
            case LEVENSHTEIN -> MongoJson.guarded(writer, a, MongoJson.text(levenshtein()));
            case JARO_WINKLER -> MongoJson.guarded(writer, a, MongoJson.text(jaroWinkler()));
            // no uuid operator: thirty-two random hexadecimal digits ($rand, one per step of a reduce), in a
            // uuid's five groups
            // the database's draw in [0, 1), per document it is spelled for (main's RANDOM, 2026-10-18)
            case RANDOM -> writer.append("{\"$rand\": {}}");
            case GUID -> writer.append("{\"$let\": {\"vars\": {\"h\": {\"$reduce\": {\"input\": {\"$range\": [0, 32]},"
                    + " \"initialValue\": \"\", \"in\": {\"$concat\": [\"$$value\", {\"$substrCP\": [\"0123456789abcdef\","
                    + " {\"$toInt\": {\"$floor\": {\"$multiply\": [{\"$rand\": {}}, 16]}}}, 1]}]}}}}, \"in\": {\"$concat\":"
                    + " [{\"$substrCP\": [\"$$h\", 0, 8]}, \"-\", {\"$substrCP\": [\"$$h\", 8, 4]}, \"-\", {\"$substrCP\":"
                    + " [\"$$h\", 12, 4]}, \"-\", {\"$substrCP\": [\"$$h\", 16, 4]}, \"-\", {\"$substrCP\": [\"$$h\", 20,"
                    + " 12]}]}}}");
            case FORMAT -> format(writer, a);
            case MD5, SHA1, SHA256 -> throw new DialectCapability(c.fn() + " reached MongoDb, which has no digest"
                    + " operator");
            case CURRENT_USER_FN -> throw new DialectCapability("currentUserId reached MongoDb, whose pipeline"
                    + " names no user");
            // ---- dates
            case EXTRACT -> extract(writer, a);
            case TODAY -> writer.append("{\"$dateTrunc\": {\"date\": \"$$NOW\", \"unit\": \"day\"}}");
            case NOW -> writer.append("\"$$NOW\"");
            case DATE_TRUNC_DAY -> dateTrunc(writer, a.get(0), com.legend.sql.DatePart.DAY);
            case DATE_TRUNC -> dateTrunc(writer, a.get(1), part(a.get(0)));
            case MAKE_DATE, MAKE_TIMESTAMP -> dateFromParts(writer, a);
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> dateAdd(writer, a);
            case DATE_DIFF -> dateDiff(writer, a);
            case TIME_BUCKET -> timeBucket(writer, a);
            case TIMEZONE -> timezone(writer, a);
            case DATE_AT_TIME -> throw new DialectCapability("a date at a time of day reached MongoDb, which has"
                    + " no time type");
            case EPOCH_NS, FROM_EPOCH_NS -> throw new DialectCapability(c.fn() + " reached MongoDb, whose date"
                    + " holds milliseconds");
            case EPOCH_SECONDS -> writer.append("{\"$divide\": [{\"$toLong\": ").expr(a.get(0), 0)
                    .append("}, 1000]}");
            case EPOCH_MS -> MongoJson.op1(writer, "$toLong", a.get(0));
            case FROM_EPOCH_SECONDS -> writer.append("{\"$toDate\": {\"$multiply\": [").expr(a.get(0), 0)
                    .append(", 1000]}}");
            case FROM_EPOCH_MS -> MongoJson.op1(writer, "$toDate", a.get(0));
            case DAYNAME -> calendarName(writer, a, DAY_NAMES, "$dayOfWeek");
            case MONTHNAME -> calendarName(writer, a, MONTH_NAMES, "$month");
            case STRFTIME -> strftime(writer, a);
            case STRPTIME -> writer.append("{\"$dateFromString\": {\"dateString\": ").expr(a.get(0), 0)
                    .append(", \"format\": ").expr(a.get(1), 0).append("}}");
            case PARSE_DATE -> writer.append("{\"$dateFromString\": {\"dateString\": ").expr(a.get(0), 0)
                    .append("}}");
            // ---- lists (BSON arrays)
            case LIST_FILTER -> listFilter(writer, a);
            case LIST_TRANSFORM -> listMap(writer, a);
            case LIST_CONCAT -> listConcat(writer, a);
            // an element past the end is null (SqlTyping: NULL past the end), where $arrayElemAt answers nothing
            case LIST_GET -> writer.append("{\"$ifNull\": [{\"$arrayElemAt\": [").expr(a.get(0), 0)
                    .append(", {\"$subtract\": [").expr(a.get(1), 0).append(", 1]}]}, null]}");
            case LIST_POSITION -> listPosition(writer, a);
            case LIST_EXISTS -> listExists(writer, a);
            case LIST_FOR_ALL -> listForAll(writer, a);
            case LIST_LENGTH, JSON_ARRAY_LENGTH -> MongoJson.guardedOp(writer, "$size", a);
            case LIST_FLATTEN -> writer.append("{\"$reduce\": {\"input\": ").expr(a.get(0), 0)
                    .append(", \"initialValue\": [], \"in\": {\"$concatArrays\": [" + VALUE + ", " + THIS + "]}}}");
            case ALL_DISTINCT -> allDistinct(writer, a);
            case LIST_ZIP, LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE,
                 LIST_SORT, LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE,
                 REPEAT_VALUE, LIST_BOOL_AND, LIST_BOOL_OR, LIST_REVERSE, TYPEOF -> listCall(writer, c.fn(), a);
            case UNNEST -> unnestProjection(writer, a);
            case STRUCT_INSERT -> structInsert(writer, a);
            // ---- maps (documents)
            case MAP_FROM_LISTS -> writer.append("{\"$arrayToObject\": {\"$zip\": {\"inputs\": [").list(a)
                    .append("]}}}");
            // the entries' pairs (first, second) as the document's fields (k, v); no entry, an empty document
            case MAP_FROM_ENTRIES -> writer.append("{\"$arrayToObject\": {\"$map\": {\"input\": {\"$ifNull\": [")
                    .expr(a.get(0), 0).append(", []]}, \"in\": {\"k\": \"$$this.first\", \"v\": \"$$this.second\"}}}}");
            case MAP_EMPTY -> writer.append("{\"$literal\": {}}");
            case MAP_EXTRACT -> mapExtract(writer, a);
            case MAP_KEYS -> mapSide(writer, a, "k");
            case MAP_VALUES -> mapSide(writer, a, "v");
            case MAP_CONCAT -> MongoJson.op(writer, "$mergeObjects", a);
            // ---- variants (a BSON value is the variant)
            case TO_VARIANT -> variantConstruct(writer, a);
            case VARIANT_ELEMENTS -> variantElements(writer, a);
            case VARIANT_GET -> variantGet(writer, a);
            case JSON_TYPE -> jsonType(writer, a);
            case JSON_MERGE_PATCH -> mergePatch(writer, a);
            case JSON_PRETTY -> throw new DialectCapability("a pretty-printed JSON text reached MongoDb, which has"
                    + " no JSON text operator");
            case BOOL_TO_TEXT -> MongoJson.op1(writer, "$toString", a.get(0));
            case FLOAT_CANON -> floatCanonByText(writer, a, parentPrec);
            case FLOAT_PRINT, FLOAT_PRINT_ENGINE -> floatPrintByText(writer, c, parentPrec);
            // ---- raising
            case ERROR -> raise(writer, a);
        };
    }

    // ---- the arms' helpers, each one spelling

    /** {@code ((a mod b) + b) mod b}: the always-positive modulus. */
    private static SqlWriter flooredMod(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece b = unfolded(a.get(1));
        return writer.append("{\"$mod\": [{\"$add\": [{\"$mod\": [").expr(a.get(0), 0).append(", ")
                .piece(b).append("]}, ").piece(b).append("]}, ").piece(b).append("]}");
    }

    /** A literal operand of an operator that raises on its value (a modulus by zero, the root of a negative, a range
     *  of step zero) read through the row ({@link #UNFOLDED}, its length 0 added): the server's optimizer folds a
     *  constant operator and raises while it plans, before the CASE that guards it runs (measured 8.0: "Failed to
     *  optimize pipeline :: caused by :: can't $mod by zero", where the guard's own message was due; 2026-10-20, the
     *  parity phase). Any other operand as it is. */
    private static SqlWriter.Piece unfolded(SqlExpr x) {
        if (x instanceof SqlExpr.IntLit || x instanceof SqlExpr.FloatLit || x instanceof SqlExpr.DecimalLit) {
            return w -> w.append("{\"$add\": [").expr(x, 0).append(", {\"$strLenCP\": " + UNFOLDED + "}]}");
        }
        return w -> w.expr(x, 0);
    }

    private static SqlWriter negated(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$multiply\": [").expr(a.get(0), 0).append(", -1]}");
    }

    private static SqlWriter intDivide(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$toLong\": {\"$trunc\": [{\"$divide\": [").expr(a.get(0), 0).append(", ")
                .expr(a.get(1), 0).append("]}, 0]}}");
    }

    /** Whether {@code x} is null or missing, by its type: {@code $eq} compares a missing value unequal to null
     *  (measured 8.0: a variant's absent key, {@code {"businessData": {}}}'s {@code signalId}, was not empty; 2026-10-20,
     *  the parity phase), and {@code x} is read once. */
    private static SqlWriter nullTest(SqlWriter writer, List<SqlExpr> a, boolean isNull) {
        writer.append(isNull ? "" : "{\"$not\": [").append("{\"$in\": [{\"$type\": ").expr(a.get(0), 0)
                .append("}, [\"missing\", \"null\"]]}");
        return writer.append(isNull ? "" : "]}");
    }

    /** {@code x IN (a, b, ...)}: null for a null needle. */
    private static SqlWriter inList(SqlWriter writer, List<SqlExpr> a) {
        List<SqlExpr> rest = a.subList(1, a.size());
        return MongoJson.guarded(writer, List.of(a.get(0)),
                x -> x.append("{\"$in\": [" + G0 + ", [").list(rest).append("]]}"));
    }

    /** {@code $ifNull}, which takes two or more arguments; one argument is itself. */
    private static SqlWriter coalesce(SqlWriter writer, List<SqlExpr> a) {
        return a.size() == 1 ? writer.expr(a.get(0), 0) : MongoJson.op(writer, "$ifNull", a);
    }

    /** A cube root with the sign of its argument ({@code $pow} of a negative base errors). */
    private static SqlWriter cubeRoot(SqlWriter writer, List<SqlExpr> a) {
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$cond\": [{\"$lt\": [" + G0 + ", 0]}, {\"$multiply\":"
                + " [-1, {\"$pow\": [{\"$abs\": " + G0 + "}, 0.3333333333333333]}]}, {\"$pow\": [" + G0
                + ", 0.3333333333333333]}]}"));
    }

    /** NaN outside [-1, 1], the engine's answer, where MongoDB's operator errors. */
    private static SqlWriter domainGuarded(SqlWriter writer, String op, List<SqlExpr> a) {
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$cond\": [{\"$and\": [{\"$gte\": [" + G0 + ", -1]},"
                + " {\"$lte\": [" + G0 + ", 1]}]}, {\"" + op + "\": " + G0 + "}, {\"$numberDouble\": \"NaN\"}]}"));
    }

    private static SqlWriter cotangent(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$divide\": [1, {\"$tan\": ").expr(a.get(0), 0).append("}]}");
    }

    /** {@code ceil}/{@code floor} as an integer. */
    private static SqlWriter longOf(SqlWriter writer, String op, List<SqlExpr> a) {
        return writer.append("{\"$toLong\": {\"").append(op).append("\": ").expr(a.get(0), 0).append("}}");
    }

    /** {@code $round} rounds half to even, Pure's banker's rounding. */
    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$round\": [").expr(a.get(0), 0).append(", ")
                .piece(x -> scaleOrZero(x, a)).append("]}");
    }

    private static SqlWriter scaleOrZero(SqlWriter writer, List<SqlExpr> a) {
        return a.size() > 1 ? writer.expr(a.get(1), 0) : writer.append("0");
    }

    /** Half away from zero at a scale: {@code trunc(x * 10^s + sign(x) / 2) / 10^s}. */
    private static SqlWriter roundHalfUp(SqlWriter writer, List<SqlExpr> a) {
        writer.append("{\"$let\": {\"vars\": {\"x\": ").expr(a.get(0), 0).append(", \"f\": {\"$pow\": [10, ");
        scaleOrZero(writer, a);
        return writer.append("]}}, \"in\": {\"$divide\": [{\"$trunc\": [{\"$add\": [{\"$multiply\": [\"$$x\","
                + " \"$$f\"]}, {\"$cond\": [{\"$lt\": [\"$$x\", 0]}, -0.5, 0.5]}]}, 0]}, \"$$f\"]}}}");
    }

    private static SqlWriter sign(SqlWriter writer, List<SqlExpr> a) {
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$cond\": [{\"$gt\": [" + G0 + ", 0]}, 1, {\"$cond\":"
                + " [{\"$lt\": [" + G0 + ", 0]}, -1, 0]}]}"));
    }

    /** {@code $bitAnd}, {@code $bitOr}, {@code $bitXor} over integers; a shift by arithmetic. */
    @Override
    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        return switch (fnName) {
            case BIT_AND -> MongoJson.op(writer, "$bitAnd", a);
            case BIT_OR -> MongoJson.op(writer, "$bitOr", a);
            case BIT_XOR -> MongoJson.op(writer, "$bitXor", a);
            // no shift operator: a shift is the product with, or the floored quotient by, the power of two -- in
            // Decimal128, exact to 34 digits, where a double loses the bits past 2^53
            case BIT_SHIFT_LEFT -> MongoJson.guarded(writer, a, MongoJson.text("{\"$toLong\": {\"$multiply\": [{\"$toDecimal\": "
                    + G0 + "}, {\"$pow\": [{\"$toDecimal\": 2}, " + G1 + "]}]}}"));
            case BIT_SHIFT_RIGHT -> MongoJson.guarded(writer, a, MongoJson.text("{\"$toLong\": {\"$floor\":"
                    + " {\"$divide\": [{\"$toDecimal\": " + G0 + "}, {\"$pow\": [{\"$toDecimal\": 2}, " + G1 + "]}]}}}"));
            default -> throw new IllegalStateException("not a bit operation: " + fnName);
        };
    }

    @Override
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        // the database's own hash, as every lane answers its own (DuckDB's hash is not Java's either): the hashed
        // index key, a 64-bit integer
        return MongoJson.op1(writer, "$toHashedIndexKey", a.get(0));
    }

    /**
     * A list's slice by the platform's bounds ({@code LIST_SLICE(list, begin, end)}: 1-based, both ends inclusive, a
     * negative bound counted from the end -- DuckDB's {@code array_slice}, the lowering's spelling): {@code $slice}
     * takes a 0-based position and a positive count, so the bounds are resolved against the size first and an empty
     * range is the empty array (the MongoDB lane's first run, 2026-10-17).
     */
    private static SqlWriter listSlice(SqlWriter writer, List<SqlExpr> a) {
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$let\": {\"vars\": {\"n\": {\"$size\": " + G0
                + "}}, \"in\": {\"$let\": {\"vars\": {\"b\": {\"$max\": [1, {\"$cond\": [{\"$lt\": [" + G1
                + ", 0]}, {\"$add\": [\"$$n\", " + G1 + ", 1]}, " + G1 + "]}]}, \"e\": {\"$min\": [\"$$n\","
                + " {\"$cond\": [{\"$lt\": [" + G2 + ", 0]}, {\"$add\": [\"$$n\", " + G2 + ", 1]}, " + G2
                + "]}]}}, \"in\": {\"$cond\": [{\"$lt\": [\"$$e\", \"$$b\"]}, [], {\"$slice\": [" + G0
                + ", {\"$subtract\": [\"$$b\", 1]}, {\"$add\": [{\"$subtract\": [\"$$e\", \"$$b\"]}, 1]}]}]}}}}}"));
    }

    /** A null operand is the empty list, as the platform's list concatenation reads it (DuckDB's list_concat; a fold's
     *  empty accumulator), where $concatArrays would answer null (the MongoDB lane's first run, 2026-10-17). */
    private static SqlWriter listConcat(SqlWriter writer, List<SqlExpr> a) {
        writer.append("{\"$concatArrays\": [");
        for (int i = 0; i < a.size(); i++) {
            writer.append(i > 0 ? ", " : "").append("{\"$ifNull\": [").expr(a.get(i), 0).append(", []]}");
        }
        return writer.append("]}");
    }

    /** Null-skipping concat: each argument's null is the empty string, nested concats flattened. */
    private static SqlWriter concatSkippingNull(SqlWriter writer, List<SqlExpr> a) {
        writer.append("{\"$concat\": [");
        List<SqlExpr> flat = flattenConcat(a);
        for (int i = 0; i < flat.size(); i++) {
            if (i > 0) {
                writer.append(", ");
            }
            writer.append("{\"$ifNull\": [").expr(flat.get(i), 0).append(", \"\"]}");
        }
        return writer.append("]}");
    }

    /** The length in code points; a number's, of its text (the corpus spells length() over numbers). */
    private static SqlWriter length(SqlWriter writer, List<SqlExpr> a) {
        boolean text = a.get(0).type() instanceof TypeFact.Typed t && (t.type() == SqlType.Scalar.VARCHAR
                || t.type() == SqlType.Scalar.LITERAL) || a.get(0) instanceof SqlExpr.StringLit;
        // null tested at run time whatever the fact says: $strLenCP of null is the server's error, not SQL's null
        // (the MongoDB lane's first run, 2026-10-17: find() over a lambda's string)
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$cond\": [{\"$eq\": [" + G0 + ", null]}, null, "
                + (text ? "{\"$strLenCP\": " + G0 + "}" : "{\"$strLenCP\": {\"$toString\": " + G0 + "}}")
                + "]}"));
    }

    /**
     * Pure's {@code format} over a literal format: the text between the {@code %s} directives concatenated with each
     * argument's text (a string as it is, a number as Pure spells it, a date in Pure's spelling, a boolean's word, a
     * null's nothing); {@code %%} is a percent sign. The lowering rewrites the directives it can to {@code %s}; one it
     * cannot is refused by name, as a format that is not a literal is. (The MongoDB lane's first run, 2026-10-17:
     * 13 cases.)
     */
    private SqlWriter format(SqlWriter writer, List<SqlExpr> a) {
        if (!(a.get(0) instanceof SqlExpr.StringLit fmt)) {
            throw new DialectCapability("a format that is not a literal reached MongoDb: no printf operator");
        }
        List<String> pieces = new java.util.ArrayList<>();
        // each directive's fraction digits: -1 for %s, -2 for %d, else %.Nf's N
        List<Integer> directives = new java.util.ArrayList<>();
        StringBuilder piece = new StringBuilder();
        String f = fmt.value();
        for (int i = 0; i < f.length(); i++) {
            char c = f.charAt(i);
            if (c != '%') {
                piece.append(c);
                continue;
            }
            if (i + 1 < f.length() && f.charAt(i + 1) == '%') {
                piece.append('%');
                i++;
                continue;
            }
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("%(s|d|\\.([0-9]+)f)")
                    .matcher(f.substring(i));
            if (!m.lookingAt()) {
                throw new DialectCapability("a format directive other than %s, %d and %.Nf reached MongoDb (" + f
                        + "): no printf operator");
            }
            pieces.add(piece.toString());
            piece.setLength(0);
            directives.add(m.group(1).equals("s") ? -1 : m.group(1).equals("d") ? -2 : Integer.parseInt(m.group(2)));
            i += m.end() - 1;
        }
        pieces.add(piece.toString());
        if (pieces.size() - 1 != a.size() - 1) {
            throw new DialectCapability("format reached MongoDb with " + (a.size() - 1) + " arguments for "
                    + (pieces.size() - 1) + " directives (" + f + ")");
        }
        writer.append("{\"$concat\": [").append(MongoJson.string(pieces.get(0)));
        for (int i = 1; i < a.size(); i++) {
            writer.append(", ");
            int d = directives.get(i - 1);
            if (d == -1) {
                formatArgument(writer, a.get(i));
            } else if (d == -2) {
                // %d: the integer's digits
                writer.append("{\"$toString\": {\"$toLong\": ").expr(a.get(i), 0).append("}}");
            } else {
                fixedFraction(writer, a.get(i), d);
            }
            writer.append(", ").append(MongoJson.string(pieces.get(i)));
        }
        return writer.append("]}");
    }

    /** {@code %.Nf}: the number rounded to N fraction digits and spelled with exactly N (Pure's Float spelling of
     *  the rounded double, its fraction cut or padded with zeros; none and no point for N = 0). */
    private void fixedFraction(SqlWriter writer, SqlExpr arg, int digits) {
        // a negative argument rounded to zero keeps its sign (-0.00), as printf spells it
        writer.append("{\"$let\": {\"vars\": {\"f\": ");
        fixedFractionText(writer, arg, digits);
        writer.append("}, \"in\": {\"$cond\": [{\"$and\": [{\"$lt\": [").expr(arg, 0).append(", 0]}, {\"$ne\":"
                + " [{\"$substrCP\": [\"$$f\", 0, 1]}, \"-\"]}]}, {\"$concat\": [\"-\", \"$$f\"]}, \"$$f\"]}}}");
    }

    private void fixedFractionText(SqlWriter writer, SqlExpr arg, int digits) {
        writer.append("{\"$let\": {\"vars\": {\"r\": {\"$round\": [{\"$toDouble\": ").expr(arg, 0).append("}, ")
                .append(digits).append("]}}, \"in\": {\"$let\": {\"vars\": {\"t\": ");
        numberTextOf(writer, MongoJson.var("r"), false);
        writer.append("}, \"in\": {\"$let\": {\"vars\": {\"dot\": {\"$indexOfCP\": [\"$$t\", \".\"]}}, \"in\": ");
        if (digits == 0) {
            writer.append("{\"$substrCP\": [\"$$t\", 0, \"$$dot\"]}");
        } else {
            writer.append("{\"$concat\": [{\"$substrCP\": [\"$$t\", 0, \"$$dot\"]}, \".\", {\"$substrCP\": [{\"$concat\":"
                    + " [{\"$substrCP\": [\"$$t\", {\"$add\": [\"$$dot\", 1]}, {\"$strLenCP\": \"$$t\"}]}, ")
                    .append(MongoJson.string("0".repeat(digits))).append("]}, 0, ").append(digits).append("]}]}");
        }
        writer.append("}}}}}}");
    }

    /** The Levenshtein distance of two texts ({@code $$g0}, {@code $$g1}), by characters: the edit-distance table a
     *  row at a time (Postgres's and SQLite's walk, as folds) -- a fold along the first text whose value is the row
     *  above ({@code p}; the row for no characters is {@code 0..|b|}), each row a fold along the second text appending
     *  one cell, the least of a deletion, an insertion and a substitution; the last row's last cell. */
    private static String levenshtein() {
        String lb = "{\"$strLenCP\": " + G1 + "}";
        String charA = "{\"$substrCP\": [" + G0 + ", {\"$subtract\": [\"$$i\", 1]}, 1]}";
        String charB = "{\"$substrCP\": [" + G1 + ", {\"$subtract\": [\"$$this\", 1]}, 1]}";
        String cell = "{\"$min\": [{\"$add\": [{\"$arrayElemAt\": [\"$$p\", \"$$this\"]}, 1]}, {\"$add\":"
                + " [{\"$arrayElemAt\": [" + VALUE + ", {\"$subtract\": [\"$$this\", 1]}]}, 1]}, {\"$add\":"
                + " [{\"$arrayElemAt\": [\"$$p\", {\"$subtract\": [\"$$this\", 1]}]}, {\"$cond\": [{\"$eq\": [" + charA
                + ", " + charB + "]}, 0, 1]}]}]}";
        String row = "{\"$reduce\": {\"input\": {\"$range\": [1, {\"$add\": [" + lb + ", 1]}]}, \"initialValue\":"
                + " [\"$$i\"], \"in\": {\"$concatArrays\": [" + VALUE + ", [" + cell + "]]}}}";
        String table = "{\"$reduce\": {\"input\": {\"$range\": [1, {\"$add\": [{\"$strLenCP\": " + G0 + "}, 1]}]},"
                + " \"initialValue\": {\"$range\": [0, {\"$add\": [" + lb + ", 1]}]}, \"in\": {\"$let\": {\"vars\":"
                + " {\"p\": " + VALUE + ", \"i\": \"$$this\"}, \"in\": " + row + "}}}}";
        return "{\"$arrayElemAt\": [" + table + ", " + lb + "]}";
    }

    /**
     * Jaro-Winkler similarity of {@code $$g0} and {@code $$g1}, exactly DuckDB's (the algorithm and the order of
     * operations Postgres's and Databricks's compositions spell): each character of the second text in turn takes the
     * first unmatched equal character of the first in its window, a fold along the second text ({@code f} flags the
     * first text's matched characters, {@code tm} is the second's matched ones in order); the transpositions are the
     * first text's matched characters unequal to {@code tm}'s in the same place, a fold along the first; the score in
     * doubles, 0 where no character matched; Winkler's prefix bonus above 0.7.
     */
    private static String jaroWinkler() {
        String lp = "{\"$strLenCP\": " + G0 + "}";
        String lt = "{\"$strLenCP\": " + G1 + "}";
        String window = "{\"$max\": [0, {\"$subtract\": [{\"$toInt\": {\"$floor\": {\"$divide\": [{\"$max\": [" + lp
                + ", " + lt + "]}, 2]}}}, 1]}]}";
        // the flags with the window's character ($$this) set
        String flagged = "{\"$let\": {\"vars\": {\"m\": \"$$this\", \"fv\": \"$$value.f\"}, \"in\": {\"$map\":"
                + " {\"input\": {\"$range\": [0, " + lp + "]}, \"in\": {\"$cond\": [{\"$eq\": [\"$$this\", \"$$m\"]}, 1,"
                + " {\"$arrayElemAt\": [\"$$fv\", \"$$this\"]}]}}}}}";
        String tChar = "{\"$substrCP\": [" + G1 + ", \"$$j\", 1]}";
        // one character of the second text (j, its state s): the window's first unmatched equal character taken
        String take = "{\"$reduce\": {\"input\": {\"$range\": [{\"$max\": [0, {\"$subtract\": [\"$$j\", \"$$b\"]}]},"
                + " {\"$min\": [" + lp + ", {\"$add\": [\"$$j\", \"$$b\", 1]}]}]}, \"initialValue\": {\"f\": \"$$s.f\","
                + " \"tm\": \"$$s.tm\", \"k\": false}, \"in\": {\"$cond\": [{\"$and\": [{\"$not\": [\"$$value.k\"]},"
                + " {\"$eq\": [{\"$arrayElemAt\": [\"$$value.f\", \"$$this\"]}, 0]}, {\"$eq\": [{\"$substrCP\": [" + G0
                + ", \"$$this\", 1]}, " + tChar + "]}]}, {\"f\": " + flagged + ", \"tm\": {\"$concat\": [\"$$value.tm\", "
                + tChar + "]}, \"k\": true}, \"$$value\"]}}}";
        String matched = "{\"$reduce\": {\"input\": {\"$range\": [0, " + lt + "]}, \"initialValue\": {\"f\": {\"$map\":"
                + " {\"input\": {\"$range\": [0, " + lp + "]}, \"in\": 0}}, \"tm\": \"\"}, \"in\": {\"$let\": {\"vars\":"
                + " {\"s\": \"$$value\", \"j\": \"$$this\"}, \"in\": " + take + "}}}}";
        // along the first text: n matched so far, x of them unequal to tm's character in the same place
        String counted = "{\"$reduce\": {\"input\": {\"$range\": [0, " + lp + "]}, \"initialValue\": {\"n\": 0, \"x\": 0},"
                + " \"in\": {\"$cond\": [{\"$eq\": [{\"$arrayElemAt\": [\"$$r.f\", \"$$this\"]}, 1]}, {\"n\": {\"$add\":"
                + " [\"$$value.n\", 1]}, \"x\": {\"$add\": [\"$$value.x\", {\"$cond\": [{\"$eq\": [{\"$substrCP\": [" + G0
                + ", \"$$this\", 1]}, {\"$substrCP\": [\"$$r.tm\", \"$$value.n\", 1]}]}, 0, 1]}]}}, \"$$value\"]}}}";
        String m = "{\"$toDouble\": \"$$c.n\"}";
        String jaro = "{\"$divide\": [{\"$add\": [{\"$add\": [{\"$divide\": [" + m + ", " + lp + "]}, {\"$divide\": [" + m
                + ", " + lt + "]}]}, {\"$divide\": [{\"$subtract\": [" + m + ", {\"$floor\": {\"$divide\": [\"$$c.x\","
                + " 2]}}]}, " + m + "]}]}, 3]}";
        List<String> prefix = new java.util.ArrayList<>();
        for (int k = 1; k <= 4; k++) {
            prefix.add("{\"$cond\": [{\"$and\": [{\"$lte\": [" + k + ", {\"$min\": [" + lp + ", " + lt + "]}]}, {\"$eq\":"
                    + " [{\"$substrCP\": [" + G0 + ", 0, " + k + "]}, {\"$substrCP\": [" + G1 + ", 0, " + k + "]}]}]}, 1, 0]}");
        }
        String bonus = "{\"$multiply\": [{\"$multiply\": [{\"$toDouble\": {\"$add\": [" + String.join(", ", prefix)
                + "]}}, 0.1]}, {\"$subtract\": [1.0, \"$$jw\"]}]}";
        String score = "{\"$let\": {\"vars\": {\"jw\": " + jaro + "}, \"in\": {\"$cond\": [{\"$gt\": [\"$$jw\", 0.7]},"
                + " {\"$add\": [\"$$jw\", " + bonus + "]}, \"$$jw\"]}}}";
        return "{\"$let\": {\"vars\": {\"b\": " + window + "}, \"in\": {\"$let\": {\"vars\": {\"r\": " + matched + "},"
                + " \"in\": {\"$let\": {\"vars\": {\"c\": " + counted + "}, \"in\": {\"$cond\": [{\"$eq\": [\"$$c.n\", 0]},"
                + " 0.0, " + score + "]}}}}}}}";
    }

    /** One {@code %s} argument's text, by its kind at run time. */
    private void formatArgument(SqlWriter writer, SqlExpr arg) {
        writer.append("{\"$let\": {\"vars\": {\"g0\": ").expr(arg, 0).append("}, \"in\": {\"$switch\":"
                + " {\"branches\": [{\"case\": {\"$eq\": [\"$$g0\", null]}, \"then\": \"\"},"
                + " {\"case\": {\"$eq\": [{\"$type\": \"$$g0\"}, \"string\"]}, \"then\": \"$$g0\"},"
                + " {\"case\": {\"$eq\": [{\"$type\": \"$$g0\"}, \"date\"]}, \"then\": {\"$dateToString\":"
                + " {\"date\": \"$$g0\", \"format\": \"%Y-%m-%dT%H:%M:%S.%L+0000\"}}},"
                + " {\"case\": {\"$in\": [{\"$type\": \"$$g0\"}, [\"double\", \"decimal\"]]}, \"then\": ");
        numberTextOf(writer, MongoJson.var("g0"), false);
        writer.append("}], \"default\": {\"$toString\": \"$$g0\"}}}}}");
    }

    /**
     * {@code timeBucket(unit, quantity, date)}: the engine's bucket (DuckDB's {@code time_bucket}, whose origin is
     * 1970-01-01, a week's 1969-12-29, a Monday) by arithmetic, MongoDB's {@code $dateTrunc} binning from its own
     * reference dates: a fixed-width unit floors the milliseconds since the origin to a multiple of the width; a
     * month, a quarter or a year floors the months since 1970-01 to a multiple of the quantity's months. A
     * sub-millisecond unit is refused by name (a BSON date holds milliseconds).
     */
    private SqlWriter timeBucket(SqlWriter writer, List<SqlExpr> a) {
        com.legend.sql.DatePart unit = part(a.get(0));
        Long unitMillis = switch (unit) {
            case WEEK -> 604_800_000L;
            case DAY -> 86_400_000L;
            case HOUR -> 3_600_000L;
            case MINUTE -> 60_000L;
            case SECOND -> 1_000L;
            case MILLISECOND -> 1L;
            case YEAR, QUARTER, MONTH -> null;
            case MICROSECOND, NANOSECOND -> throw new DialectCapability("a time bucket by " + unit + "s reached"
                    + " MongoDb, whose dates hold milliseconds");
            case DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new IllegalStateException("a time bucket by " + unit
                    + ", which is no duration (DateShifts.durationPart)");
        };
        // a literal finer than a millisecond is bucketed at the millisecond: no bucket is finer, so the digits past
        // it never decide one (the one place a BSON date may cut them; MongoJson.dateLiteral refuses elsewhere)
        SqlExpr date = a.get(2) instanceof SqlExpr.TimestampLit t ? new SqlExpr.TimestampLit(toMillisecond(t.iso()))
                : a.get(2);
        List<SqlExpr> args = List.of(a.get(1), date);
        if (unitMillis != null) {
            long origin = unit == com.legend.sql.DatePart.WEEK ? -259_200_000L : 0L;
            return MongoJson.guarded(writer, args, MongoJson.text("{\"$let\": {\"vars\": {\"w\": {\"$multiply\": ["
                    + G0 + ", " + unitMillis + "]}, \"t\": {\"$subtract\": [{\"$toLong\": " + G1 + "}, " + origin
                    + "]}}, \"in\": {\"$toDate\": {\"$add\": [" + origin + ", {\"$multiply\": [{\"$floor\": {\"$divide\":"
                    + " [\"$$t\", \"$$w\"]}}, \"$$w\"]}]}}}}"));
        }
        int perUnit = switch (unit) {
            case YEAR -> 12;
            case QUARTER -> 3;
            case MONTH -> 1;
            default -> throw new IllegalStateException("a time bucket by " + unit + " counts no months");
        };
        return MongoJson.guarded(writer, args, MongoJson.text("{\"$let\": {\"vars\": {\"w\": {\"$multiply\": [" + G0
                + ", " + perUnit + "]}, \"p\": {\"$dateToParts\": {\"date\": " + G1 + "}}}, \"in\": {\"$let\": {\"vars\":"
                + " {\"b\": {\"$multiply\": [{\"$floor\": {\"$divide\": [{\"$add\": [{\"$multiply\": [{\"$subtract\":"
                + " [\"$$p.year\", 1970]}, 12]}, {\"$subtract\": [\"$$p.month\", 1]}]}, \"$$w\"]}}, \"$$w\"]}}, \"in\":"
                + " {\"$let\": {\"vars\": {\"y\": {\"$floor\": {\"$divide\": [\"$$b\", 12]}}}, \"in\": {\"$dateFromParts\":"
                + " {\"year\": {\"$add\": [1970, \"$$y\"]}, \"month\": {\"$add\": [{\"$subtract\": [\"$$b\", {\"$multiply\":"
                + " [\"$$y\", 12]}]}, 1]}, \"day\": 1}}}}}}}}"));
    }

    /** The ISO text with its fraction of a second cut to three digits. */
    private static String toMillisecond(String iso) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(\\.[0-9]{3})[0-9]+").matcher(iso);
        return m.find() ? iso.substring(0, m.start()) + m.group(1) + iso.substring(m.end()) : iso;
    }

    /**
     * {@code timezone(zone, v)}: a BSON date is a UTC instant, the platform's one temporal carrier, so UTC is the
     * value itself; another zone converts an instant (a zoned value: a zoned cast, or UTC pinned on it) to its wall
     * time there, and a wall time there to its instant -- the value's kind decides, as the SQL engines' two
     * overloads do.
     */
    private SqlWriter timezone(SqlWriter writer, List<SqlExpr> a) {
        if (a.get(0) instanceof SqlExpr.StringLit z && z.value().equals("UTC")) {
            return writer.expr(a.get(1), 0);
        }
        boolean instant = instantValued(a.get(1));
        String parts = "{\"$dateToParts\": {\"date\": \"$$d\"" + (instant ? ", \"timezone\": \"$$z\"" : "") + "}}";
        return writer.append("{\"$let\": {\"vars\": {\"d\": ").expr(a.get(1), 0).append(", \"z\": ")
                .expr(a.get(0), 0).append("}, \"in\": {\"$let\": {\"vars\": {\"p\": " + parts + "}, \"in\":"
                        + " {\"$dateFromParts\": {\"year\": \"$$p.year\", \"month\": \"$$p.month\", \"day\": \"$$p.day\","
                        + " \"hour\": \"$$p.hour\", \"minute\": \"$$p.minute\", \"second\": \"$$p.second\","
                        + " \"millisecond\": \"$$p.millisecond\"" + (instant ? "" : ", \"timezone\": \"$$z\"")
                        + "}}}}}}");
    }

    /** Whether the value is an instant: typed as a zoned timestamp, cast to one, or a value UTC is pinned on. */
    private static boolean instantValued(SqlExpr e) {
        return e.type() instanceof TypeFact.Typed t && t.type() == SqlType.Scalar.TIMESTAMPTZ
                || e instanceof SqlExpr.Cast c && c.target() == SqlType.Scalar.TIMESTAMPTZ
                || e instanceof SqlExpr.Call c && c.fn() == SqlFn.TIMEZONE
                        && c.args().get(0) instanceof SqlExpr.StringLit z && z.value().equals("UTC");
    }

    /** 1-based SQL substring, its start and length clamped at zero as the engine's substring is. */
    private static SqlWriter substring(SqlWriter writer, List<SqlExpr> a) {
        String len = a.size() > 2 ? "{\"$max\": [0, " + G2 + "]}" : "{\"$strLenCP\": " + G0 + "}";
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$substrCP\": [" + G0 + ", {\"$max\": [0,"
                + " {\"$subtract\": [" + G1 + ", 1]}]}, " + len + "]}"));
    }

    private static SqlWriter endsWith(SqlWriter writer, List<SqlExpr> a) {
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$let\": {\"vars\": {\"ls\": {\"$strLenCP\": " + G0
                + "}, \"lp\": {\"$strLenCP\": " + G1 + "}}, \"in\": {\"$and\": [{\"$gte\": [\"$$ls\", \"$$lp\"]},"
                + " {\"$eq\": [{\"$substrCP\": [" + G0 + ", {\"$subtract\": [\"$$ls\", \"$$lp\"]}, \"$$lp\"]}, "
                + G1 + "]}]}}}"));
    }

    /** {@code $regexMatch}; a full match anchors the pattern. */
    private static SqlWriter regexMatch(SqlWriter writer, List<SqlExpr> a, boolean full) {
        String regex = full ? "{\"$concat\": [\"^(?:\", " + G1 + ", \")$\"]}" : G1;
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$regexMatch\": {\"input\": " + G0 + ", \"regex\": "
                + regex + "}}"));
    }

    /** The first match, or its group; the empty string when nothing matches (the engine's answer). */
    /** {@code regexp_extract(s, pattern[, group])}: the group's capture, the whole match for group 0 or none. */
    private static SqlWriter regexExtract(SqlWriter writer, List<SqlExpr> a) {
        String found = a.size() > 2 ? "{\"$cond\": [{\"$eq\": [" + G2 + ", 0]}, \"$$m.match\", {\"$ifNull\":"
                + " [{\"$arrayElemAt\": [\"$$m.captures\", {\"$subtract\": [" + G2 + ", 1]}]}, \"\"]}]}"
                : "\"$$m.match\"";
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$let\": {\"vars\": {\"m\": {\"$regexFind\":"
                + " {\"input\": " + G0 + ", \"regex\": " + G1 + "}}}, \"in\": {\"$cond\": [{\"$eq\": [\"$$m\","
                + " null]}, \"\", " + found + "]}}}"));
    }

    private static SqlWriter regexExtractAll(SqlWriter writer, List<SqlExpr> a) {
        String each = a.size() > 2 ? "{\"$cond\": [{\"$eq\": [" + G2 + ", 0]}, \"$$this.match\", {\"$arrayElemAt\":"
                + " [\"$$this.captures\", {\"$subtract\": [" + G2 + ", 1]}]}]}" : "\"$$this.match\"";
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$map\": {\"input\": {\"$regexFindAll\": {\"input\": "
                + G0 + ", \"regex\": " + G1 + "}}, \"in\": " + each + "}}"));
    }

    /**
     * {@code regexp_replace(s, pattern, replacement, 'g' | '')}: MongoDB has no regular-expression replace
     * ({@code $replaceAll} replaces text), so the matches are found ({@code $regexFindAll}, or the first alone) and
     * the text spliced: the stretch before each match kept, the replacement put in its place -- a {@code $N} in it
     * the match's Nth capture -- and the tail after the last match kept.
     */
    private static SqlWriter regexReplace(SqlWriter writer, List<SqlExpr> a) {
        // the fourth argument carries the 'g' option; without it (the three-argument form) the first match alone
        boolean all = a.size() > 3 && a.get(3) instanceof SqlExpr.StringLit flags && flags.value().contains("g");
        String matches = all ? "{\"$regexFindAll\": {\"input\": " + G0 + ", \"regex\": " + G1 + "}}"
                : "{\"$let\": {\"vars\": {\"m\": {\"$regexFind\": {\"input\": " + G0 + ", \"regex\": " + G1 + "}}},"
                        + " \"in\": {\"$cond\": [{\"$eq\": [\"$$m\", null]}, [], [\"$$m\"]]}}}";
        // the replacement's capture references: those a literal replacement spells, else any of $1 to $9
        List<Integer> groups = new ArrayList<>();
        for (int n = 9; n >= 1; n--) {
            if (!(a.get(2) instanceof SqlExpr.StringLit r) || r.value().contains("$" + n)) {
                groups.add(n);
            }
        }
        String replacement = G2;
        for (int n : groups) {
            // the reference under $literal: a text beginning with $ is otherwise a field path
            replacement = "{\"$replaceAll\": {\"input\": " + replacement + ", \"find\": {\"$literal\": \"$" + n + "\"},"
                    + " \"replacement\": {\"$ifNull\": [{\"$arrayElemAt\": [\"$$this.captures\", " + (n - 1) + "]}, \"\"]}}}";
        }
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$let\": {\"vars\": {\"r\": {\"$reduce\": {\"input\": "
                + matches + ", \"initialValue\": {\"t\": \"\", \"p\": 0}, \"in\": {\"t\": {\"$concat\": [\"$$value.t\","
                + " {\"$substrCP\": [" + G0 + ", \"$$value.p\", {\"$subtract\": [\"$$this.idx\", \"$$value.p\"]}]}, "
                + replacement + "]}, \"p\": {\"$add\": [\"$$this.idx\", {\"$strLenCP\": \"$$this.match\"}]}}}}}, \"in\":"
                + " {\"$concat\": [\"$$r.t\", {\"$substrCP\": [" + G0 + ", \"$$r.p\", {\"$subtract\": [{\"$strLenCP\": " + G0
                + "}, \"$$r.p\"]}]}]}}}"));
    }

    private static SqlWriter rightChars(SqlWriter writer, List<SqlExpr> a) {
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$let\": {\"vars\": {\"n\": {\"$max\": [0, {\"$min\":"
                + " [" + G1 + ", {\"$strLenCP\": " + G0 + "}]}]}}, \"in\": {\"$substrCP\": [" + G0
                + ", {\"$subtract\": [{\"$strLenCP\": " + G0 + "}, \"$$n\"]}, \"$$n\"]}}}"));
    }

    /** {@code lpad}/{@code rpad}: the string cut to the length, or the pad text repeated and cut to fill it. */
    private static SqlWriter pad(SqlWriter writer, List<SqlExpr> a, boolean left) {
        String padText = a.size() > 2 ? G2 : "\" \"";
        String fill = "{\"$substrCP\": [{\"$reduce\": {\"input\": {\"$range\": [0, \"$$k\"]}, \"initialValue\": \"\","
                + " \"in\": {\"$concat\": [" + VALUE + ", " + padText + "]}}}, 0, \"$$k\"]}";
        String padded = left ? "{\"$concat\": [" + fill + ", " + G0 + "]}"
                : "{\"$concat\": [" + G0 + ", " + fill + "]}";
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$let\": {\"vars\": {\"k\": {\"$subtract\": [" + G1
                + ", {\"$strLenCP\": " + G0 + "}]}}, \"in\": {\"$cond\": [{\"$lte\": [\"$$k\", 0]}, {\"$substrCP\":"
                + " [" + G0 + ", 0, {\"$max\": [0, " + G1 + "]}]}, " + padded + "]}}}"));
    }

    private static SqlWriter trim(SqlWriter writer, String op, List<SqlExpr> a) {
        writer.append("{\"").append(op).append("\": {\"input\": ").expr(a.get(0), 0);
        if (a.size() > 1) {
            writer.append(", \"chars\": ").expr(a.get(1), 0);
        }
        return writer.append("}}");
    }

    private static SqlWriter replace(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$replaceAll\": {\"input\": ").expr(a.get(0), 0).append(", \"find\": ")
                .expr(a.get(1), 0).append(", \"replacement\": ").expr(a.get(2), 0).append("}}");
    }

    /** The n-th (1-based) part of a split; dropping empty parts for Pure's splitPart. */
    private static SqlWriter splitPart(SqlWriter writer, List<SqlExpr> a, boolean dropEmpty) {
        String parts = dropEmpty ? "{\"$filter\": {\"input\": {\"$split\": [" + G0 + ", " + G1 + "]}, \"cond\":"
                + " {\"$ne\": [" + THIS + ", \"\"]}}}" : "{\"$split\": [" + G0 + ", " + G1 + "]}";
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$let\": {\"vars\": {\"p\": " + parts + "}, \"in\":"
                + " {\"$cond\": [{\"$gt\": [" + G2 + ", {\"$size\": \"$$p\"}]}, null, {\"$arrayElemAt\": [\"$$p\","
                + " {\"$subtract\": [" + G2 + ", 1]}]}]}}}"));
    }

    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> a) {
        return splitPart(writer, a, true);
    }

    private static SqlWriter firstCased(SqlWriter writer, List<SqlExpr> a, String op) {
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$concat\": [{\"" + op + "\": {\"$substrCP\": [" + G0
                + ", 0, 1]}}, {\"$substrCP\": [" + G0 + ", 1, {\"$strLenCP\": " + G0 + "}]}]}"));
    }

    /** A date part: MongoDB's operator for it; a part finer than a millisecond has none. */
    private static SqlWriter extract(SqlWriter writer, List<SqlExpr> a) {
        com.legend.sql.DatePart part = part(a.get(0));
        SqlExpr date = a.get(1);
        return switch (part) {
            case YEAR -> MongoJson.op1(writer, "$year", date);
            case QUARTER -> writer.append("{\"$toLong\": {\"$ceil\": {\"$divide\": [{\"$month\": ").expr(date, 0)
                    .append("}, 3]}}}");
            case MONTH -> MongoJson.op1(writer, "$month", date);
            case WEEK -> MongoJson.op1(writer, "$isoWeek", date);
            case DAY -> MongoJson.op1(writer, "$dayOfMonth", date);
            case HOUR -> MongoJson.op1(writer, "$hour", date);
            case MINUTE -> MongoJson.op1(writer, "$minute", date);
            case SECOND -> MongoJson.op1(writer, "$second", date);
            case MILLISECOND -> MongoJson.op1(writer, "$millisecond", date);
            case DAY_OF_YEAR -> MongoJson.op1(writer, "$dayOfYear", date);
            case ISO_DAY_OF_WEEK -> MongoJson.op1(writer, "$isoDayOfWeek", date);
            case MICROSECOND, NANOSECOND -> throw new DialectCapability("a " + part + " reached MongoDb, whose"
                    + " date holds milliseconds");
        };
    }

    /** A date part as {@code $dateTrunc}'s, {@code $dateAdd}'s and {@code $dateDiff}'s unit; weeks start Monday. */
    private static String unit(com.legend.sql.DatePart part) {
        return switch (part) {
            case YEAR -> "year";
            case QUARTER -> "quarter";
            case MONTH -> "month";
            case WEEK -> "week";
            case DAY -> "day";
            case HOUR -> "hour";
            case MINUTE -> "minute";
            case SECOND -> "second";
            case MILLISECOND -> "millisecond";
            case MICROSECOND, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("a " + part
                    + " unit reached MongoDb, which has no such date unit");
        };
    }

    private static SqlWriter dateTrunc(SqlWriter writer, SqlExpr date, com.legend.sql.DatePart part) {
        return writer.append("{\"$dateTrunc\": {\"date\": ").expr(date, 0).append(", \"unit\": \"")
                .append(unit(part)).append("\", \"startOfWeek\": \"monday\"}}");
    }

    /** {@code (unit, amount, date)}: {@code $dateAdd}. */
    private static SqlWriter dateAdd(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$dateAdd\": {\"startDate\": ").expr(a.get(2), 0).append(", \"unit\": \"")
                .append(unit(part(a.get(0)))).append("\", \"amount\": {\"$toLong\": ").expr(a.get(1), 0)
                .append("}}}");
    }

    /** {@code (unit, start, end)}: {@code $dateDiff}, the unit boundaries crossed. */
    private static SqlWriter dateDiff(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$dateDiff\": {\"startDate\": ").expr(a.get(1), 0).append(", \"endDate\": ")
                .expr(a.get(2), 0).append(", \"unit\": \"").append(unit(part(a.get(0))))
                .append("\", \"startOfWeek\": \"monday\"}}");
    }

    /** {@code (year, month, day[, hour, minute, second])}: {@code $dateFromParts}, a fractional second split into
     *  its milliseconds. */
    private static SqlWriter dateFromParts(SqlWriter writer, List<SqlExpr> a) {
        writer.append("{\"$dateFromParts\": {\"year\": ").expr(a.get(0), 0).append(", \"month\": ").expr(a.get(1), 0)
                .append(", \"day\": ").expr(a.get(2), 0);
        if (a.size() == 6) {
            writer.append(", \"hour\": ").expr(a.get(3), 0).append(", \"minute\": ").expr(a.get(4), 0)
                    .append(", \"second\": {\"$toLong\": {\"$floor\": ").expr(a.get(5), 0)
                    .append("}}, \"millisecond\": {\"$toLong\": {\"$round\": [{\"$multiply\": [{\"$mod\": [")
                    .expr(a.get(5), 0).append(", 1]}, 1000]}, 0]}}");
        } else if (a.size() != 3) {
            throw new IllegalStateException("a date from " + a.size() + " parts");
        }
        return writer.append("}}");
    }

    /** A day's or a month's name, from the calendar operator's 1-based number. */
    private static SqlWriter calendarName(SqlWriter writer, List<SqlExpr> a, List<String> names, String op) {
        writer.append("{\"$arrayElemAt\": [[");
        for (int i = 0; i < names.size(); i++) {
            writer.append(i > 0 ? ", " : "").append(MongoJson.string(names.get(i)));
        }
        return writer.append("], {\"$subtract\": [{\"").append(op).append("\": ").expr(a.get(0), 0)
                .append("}, 1]}]}");
    }

    /** {@code $filter} over the list, the lambda's parameter its variable. */
    private SqlWriter listFilter(SqlWriter writer, List<SqlExpr> a) {
        SqlExpr.Lambda l = lambdaOf(a.get(1));
        if (l.params().size() == 2) {
            // the pairs kept, each read back to its element (a null list staying null)
            writer.append("{\"$let\": {\"vars\": {\"k\": ");
            indexed(writer, "$filter", "cond", a.get(0), l);
            return writer.append("}, \"in\": {\"$cond\": [{\"$eq\": [\"$$k\", null]}, null, {\"$map\": {\"input\":"
                    + " \"$$k\", \"in\": {\"$arrayElemAt\": [\"$$this\", 0]}}}]}}}");
        }
        String v = MongoJson.variable(l.params().get(0));
        writer.append("{\"$filter\": {\"input\": ").expr(a.get(0), 0).append(", \"as\": ").append(MongoJson.string(v))
                .append(", \"cond\": ");
        withLambda(writer, l, List.of(v), x -> x.expr(l.body(), 0));
        return writer.append("}}");
    }

    /** A list operator over a two-parameter lambda (the element, its 1-based position, as the SQL engines' list
     *  lambdas count): the list zipped with its positions, each pair's sides bound to the parameters. */
    private SqlWriter indexed(SqlWriter writer, String op, String bodyKey, SqlExpr list, SqlExpr.Lambda l) {
        String e = MongoJson.variable(l.params().get(0));
        String i = MongoJson.variable(l.params().get(1));
        // a null list stays null, as the SQL engines' list lambdas answer
        writer.append("{\"$let\": {\"vars\": {\"l\": ").expr(list, 0).append("}, \"in\": {\"$cond\": [{\"$eq\":"
                + " [\"$$l\", null]}, null, {\"").append(op).append("\": {\"input\": {\"$zip\": {\"inputs\": [\"$$l\","
                + " {\"$range\": [1, {\"$add\": [{\"$size\": \"$$l\"}, 1]}]}]}}, \"as\": \"p\", \"").append(bodyKey)
                .append("\": {\"$let\": {\"vars\": {").append(MongoJson.string(e))
                .append(": {\"$arrayElemAt\": [\"$$p\", 0]}, ").append(MongoJson.string(i))
                .append(": {\"$arrayElemAt\": [\"$$p\", 1]}}, \"in\": ");
        withLambda(writer, l, List.of(e, i), x -> x.expr(l.body(), 0));
        return writer.append("}}}}]}}}");
    }

    /** {@code $map} over the list. */
    private SqlWriter listMap(SqlWriter writer, List<SqlExpr> a) {
        SqlExpr.Lambda l = lambdaOf(a.get(1));
        if (l.params().size() == 2) {
            return indexed(writer, "$map", "in", a.get(0), l);
        }
        String v = MongoJson.variable(l.params().get(0));
        writer.append("{\"$map\": {\"input\": ").expr(a.get(0), 0).append(", \"as\": ").append(MongoJson.string(v))
                .append(", \"in\": ");
        withLambda(writer, l, List.of(v), x -> x.expr(l.body(), 0));
        return writer.append("}}");
    }

    private static SqlWriter listPosition(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$let\": {\"vars\": {\"i\": {\"$indexOfArray\": [").expr(a.get(0), 0).append(", ")
                .expr(a.get(1), 0).append("]}}, \"in\": {\"$cond\": [{\"$lt\": [\"$$i\", 0]}, null, {\"$add\":"
                        + " [\"$$i\", 1]}]}}}");
    }

    /** {@code exists}: any element true, false over the empty (or null) list. */
    @Override
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> a) {
        return quantified(writer, a, "$anyElementTrue");
    }

    /** {@code forAll}: all elements true, true over the empty (or null) list. */
    @Override
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> a) {
        return quantified(writer, a, "$allElementsTrue");
    }

    private SqlWriter quantified(SqlWriter writer, List<SqlExpr> a, String op) {
        SqlExpr.Lambda l = lambdaOf(a.get(1));
        String v = MongoJson.variable(l.params().get(0));
        writer.append("{\"").append(op).append("\": [{\"$map\": {\"input\": {\"$ifNull\": [").expr(a.get(0), 0)
                .append(", []]}, \"as\": ").append(MongoJson.string(v)).append(", \"in\": ");
        withLambda(writer, l, List.of(v), x -> x.expr(l.body(), 0));
        return writer.append("}}]}");
    }

    @Override
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> a) {
        return MongoJson.guarded(writer, a, MongoJson.text("{\"$eq\": [{\"$size\": " + G0 + "}, {\"$size\":"
                + " {\"$setUnion\": [" + G0 + "]}}]}"));
    }

    /** The list functions beyond the lambda family, each over BSON's array operators. */
    @Override
    protected SqlWriter listCall(SqlWriter writer, SqlFn fn, List<SqlExpr> a) {
        return switch (fn) {
            case LIST_ZIP -> writer.append("{\"$zip\": {\"inputs\": [").list(a).append("]}}");
            case LIST_DISTINCT -> writer.append("{\"$setUnion\": [").expr(a.get(0), 0).append("]}");
            case LIST_APPEND -> writer.append("{\"$concatArrays\": [").expr(a.get(0), 0).append(", [")
                    .expr(a.get(1), 0).append("]]}");
            case LIST_SUM -> MongoJson.op1(writer, "$sum", a.get(0));
            case LIST_MIN -> MongoJson.op1(writer, "$min", a.get(0));
            case LIST_MAX -> MongoJson.op1(writer, "$max", a.get(0));
            case LIST_AVG -> MongoJson.op1(writer, "$avg", a.get(0));
            case LIST_MEDIAN -> arrayReduction(writer, SqlAgg.Fn.MEDIAN, x -> nonNull(x, y -> y.expr(a.get(0), 0)),
                    List.of());
            case LIST_MODE -> arrayReduction(writer, SqlAgg.Fn.MODE, x -> nonNull(x, y -> y.expr(a.get(0), 0)),
                    List.of());
            case LIST_SORT -> writer.append("{\"$sortArray\": {\"input\": ").expr(a.get(0), 0)
                    .append(", \"sortBy\": 1}}");
            case LIST_SORT_DESC -> writer.append("{\"$sortArray\": {\"input\": ").expr(a.get(0), 0)
                    .append(", \"sortBy\": -1}}");
            case LIST_TAIL -> MongoJson.guarded(writer, List.of(a.get(0)), MongoJson.text("{\"$cond\": [{\"$gt\":"
                    + " [{\"$size\": " + G0 + "}, 1]}, {\"$slice\": [" + G0 + ", 1, {\"$subtract\": [{\"$size\": "
                    + G0 + "}, 1]}]}, []]}"));
            case LIST_INIT -> MongoJson.guarded(writer, List.of(a.get(0)), MongoJson.text("{\"$cond\": [{\"$gt\":"
                    + " [{\"$size\": " + G0 + "}, 1]}, {\"$slice\": [" + G0 + ", 0, {\"$subtract\": [{\"$size\": "
                    + G0 + "}, 1]}]}, []]}"));
            // range(n) counts from 0 ($range takes a start)
            case RANGE_FN -> a.size() == 1 ? writer.append("{\"$range\": [0, ").expr(a.get(0), 0).append("]}")
                    : a.size() == 3 ? writer.append("{\"$range\": [").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                            .append(", ").piece(unfolded(a.get(2))).append("]}")
                    : MongoJson.op(writer, "$range", a);
            case LIST_PRODUCT -> writer.append("{\"$reduce\": {\"input\": ").expr(a.get(0), 0)
                    .append(", \"initialValue\": 1, \"in\": {\"$multiply\": [" + VALUE + ", " + THIS + "]}}}");
            case LIST_REDUCE -> listReduce(writer, a);
            case LIST_SLICE -> listSlice(writer, a);
            case REPEAT_VALUE -> writer.append("{\"$map\": {\"input\": {\"$range\": [0, ").expr(a.get(1), 0)
                    .append("]}, \"in\": ").expr(a.get(0), 0).append("}}");
            case LIST_BOOL_AND -> writer.append("{\"$allElementsTrue\": [").expr(a.get(0), 0).append("]}");
            case LIST_BOOL_OR -> writer.append("{\"$anyElementTrue\": [").expr(a.get(0), 0).append("]}");
            case LIST_REVERSE -> MongoJson.op1(writer, "$reverseArray", a.get(0));
            case TYPEOF -> MongoJson.op1(writer, "$type", a.get(0));
            default -> throw new IllegalStateException("not a list call: " + fn);
        };
    }

    /** {@code (list, (accumulator, element) -> ...[, initial])}: {@code $reduce}; without an initial value the
     *  first element is it and the rest are reduced. */
    private SqlWriter listReduce(SqlWriter writer, List<SqlExpr> a) {
        SqlExpr.Lambda l = lambdaOf(a.get(1));
        if (l.params().size() != 2) {
            throw new IllegalStateException("a reduce's lambda takes (accumulator, element): " + l.params());
        }
        if (a.size() > 2) {
            writer.append("{\"$reduce\": {\"input\": ").expr(a.get(0), 0).append(", \"initialValue\": ")
                    .expr(a.get(2), 0).append(", \"in\": ");
            withLambda(writer, l, List.of("value", "this"), x -> x.expr(l.body(), 0));
            return writer.append("}}");
        }
        writer.append("{\"$let\": {\"vars\": {\"l\": ").expr(a.get(0), 0).append("}, \"in\": {\"$cond\": [{\"$lte\":"
                + " [{\"$size\": \"$$l\"}, 1]}, {\"$arrayElemAt\": [\"$$l\", 0]}, {\"$reduce\": {\"input\":"
                + " {\"$slice\": [\"$$l\", 1, {\"$subtract\": [{\"$size\": \"$$l\"}, 1]}]}, \"initialValue\":"
                + " {\"$arrayElemAt\": [\"$$l\", 0]}, \"in\": ");
        withLambda(writer, l, List.of("value", "this"), x -> x.expr(l.body(), 0));
        return writer.append("}}]}}}");
    }

    private static SqlExpr.Lambda lambdaOf(SqlExpr e) {
        if (e instanceof SqlExpr.Lambda l) {
            return l;
        }
        throw new IllegalStateException("a list operator's second argument is a lambda, not " + e);
    }

    /** {@code body} written with the lambda's parameters bound to {@code variables}, in order. */
    private void withLambda(SqlWriter writer, SqlExpr.Lambda l, List<String> variables, SqlWriter.Piece body) {
        if (l.params().size() != variables.size()) {
            throw new IllegalStateException("lambda " + l.params() + " bound to " + variables);
        }
        for (int i = 0; i < variables.size(); i++) {
            lambdaVariables.push(Map.entry(l.params().get(i), variables.get(i)));
        }
        try {
            writer.piece(body);
        } finally {
            for (int i = 0; i < variables.size(); i++) {
                lambdaVariables.pop();
            }
        }
    }

    @Override
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> a) {
        throw new DialectCapability("an unnest reached MongoDb in expression position: an $unwind stage is not"
                + " placed yet");
    }

    /** {@code struct_insert(struct, name, value)}: the document with the field merged in. */
    @Override
    protected SqlWriter structInsert(SqlWriter writer, List<SqlExpr> a) {
        if (!(a.get(1) instanceof SqlExpr.StringLit name)) {
            throw new DialectCapability("a struct insert with a computed field name reached MongoDb");
        }
        return writer.append("{\"$mergeObjects\": [").expr(a.get(0), 0).append(", {")
                .append(MongoJson.key(name.value())).append(": ").expr(a.get(2), 0).append("}]}");
    }

    /** The values under a key: a list of one, or none (the map-extract contract). */
    private static SqlWriter mapExtract(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$map\": {\"input\": {\"$filter\": {\"input\": {\"$objectToArray\": ")
                .expr(a.get(0), 0).append("}, \"cond\": {\"$eq\": [\"$$this.k\", ").expr(a.get(1), 0)
                .append("]}}}, \"in\": \"$$this.v\"}}");
    }

    private static SqlWriter mapSide(SqlWriter writer, List<SqlExpr> a, String side) {
        return writer.append("{\"$map\": {\"input\": {\"$objectToArray\": ").expr(a.get(0), 0)
                .append("}, \"in\": \"$$this.").append(side).append("\"}}");
    }

    /** A BSON value is the variant. */
    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        return writer.expr(a.get(0), 0);
    }

    /** An array value's elements are the array itself. */
    @Override
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> a) {
        return writer.expr(a.get(0), 0);
    }

    /** {@code (value, key)}: the value itself under the JSON path root ({@code $}, the lowering's read of a whole
     *  Variant), a field by its literal name, an element by its 0-based index, or a field by a computed name
     *  through the document's entries. */
    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> a) {
        if (a.get(1) instanceof SqlExpr.StringLit root && root.value().equals("$")) {
            return writer.expr(a.get(0), 0);
        }
        if (a.get(1) instanceof SqlExpr.StringLit key) {
            return writer.append("{\"$getField\": {\"field\": ").append(MongoJson.key(key.value()))
                    .append(", \"input\": ").expr(a.get(0), 0).append("}}");
        }
        if (a.get(1) instanceof SqlExpr.IntLit) {
            return writer.append("{\"$arrayElemAt\": [").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append("]}");
        }
        return writer.append("{\"$arrayElemAt\": [").piece(x -> mapExtract(x, a)).append(", 0]}");
    }

    /** The JSON kind of a value, by the names {@link #jsonKindName} answers. */
    private static SqlWriter jsonType(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$let\": {\"vars\": {\"t\": {\"$type\": ").expr(a.get(0), 0).append("}}, \"in\":"
                + " {\"$switch\": {\"branches\": [{\"case\": {\"$in\": [\"$$t\", [\"int\", \"long\"]]}, \"then\":"
                + " \"INTEGER\"}, {\"case\": {\"$in\": [\"$$t\", [\"double\", \"decimal\"]]}, \"then\": \"FLOAT\"},"
                + " {\"case\": {\"$eq\": [\"$$t\", \"bool\"]}, \"then\": \"BOOLEAN\"}, {\"case\": {\"$eq\": [\"$$t\","
                + " \"string\"]}, \"then\": \"STRING\"}, {\"case\": {\"$eq\": [\"$$t\", \"object\"]}, \"then\":"
                + " \"OBJECT\"}, {\"case\": {\"$eq\": [\"$$t\", \"array\"]}, \"then\": \"ARRAY\"}], \"default\":"
                + " \"NULL\"}}}}");
    }

    /** A merge patch: the patch's fields over the object's, a null-valued field dropped. */
    private static SqlWriter mergePatch(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("{\"$arrayToObject\": {\"$filter\": {\"input\": {\"$objectToArray\": {\"$mergeObjects\":"
                + " [").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append("]}}, \"cond\": {\"$ne\":"
                + " [\"$$this.v\", null]}}}}");
    }

    /**
     * A raise: a conversion that cannot succeed, whose error carries its input -- the message between the platform's
     * U+001F sentinels (the executor's envelope, B7), an optional source position ahead of it behind U+001E -- so
     * the connector extracts the platform's own text from the server's error.
     */
    private static SqlWriter raise(SqlWriter writer, List<SqlExpr> a) {
        writer.append("{\"$convert\": {\"input\": {\"$concat\": [").append(MongoJson.string(RAISE_SENTINEL))
                .append(", ");
        if (a.size() > 1) {
            writer.expr(a.get(1), 0).append(", ").append(MongoJson.string(POSITION_DIVIDER)).append(", ");
        }
        return writer.append("{\"$toString\": ").expr(a.get(0), 0).append("}, ")
                .append(MongoJson.string(RAISE_SENTINEL)).append(", ").append(UNFOLDED)
                .append("]}, \"to\": \"int\"}}");
    }

    /** An empty text that depends on the row ({@code $$ROOT}'s type, cut to nothing): concatenated into a raise so
     *  the server's optimizer cannot fold the conversion of a constant message and raise before any row is read
     *  (measured 8.0: a constant conversion in a branch not taken fails the pipeline's optimization). */
    private static final String UNFOLDED = "{\"$substrCP\": [{\"$type\": \"$$ROOT\"}, 0, 0]}";

    // ==================================================================
    // Aggregates: the group stage's accumulators and their reads
    // ==================================================================

    /** The native accumulator for an aggregate, one argument, unordered and not distinct; every other form is
     *  pushed and reduced at the read ({@link #accumulator}). The window-only functions and the lowering's markers
     *  never reach a group stage. */
    @Override
    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM -> accumulate(writer, "$sum", r);
            case COUNT -> countAccumulator(writer, r);
            case AVG -> accumulate(writer, "$avg", r);
            case MIN -> accumulate(writer, "$min", r);
            case MAX -> accumulate(writer, "$max", r);
            case ANY_VALUE -> accumulate(writer, "$first", r);
            case STDDEV_SAMP, STDDEV -> accumulate(writer, "$stdDevSamp", r);
            case STDDEV_POP -> accumulate(writer, "$stdDevPop", r);
            case LIST -> accumulate(writer, "$push", r);
            case BOOL_AND -> accumulate(writer, "$min", r);
            case BOOL_OR -> accumulate(writer, "$max", r);
            case VAR_SAMP, VAR_POP, VARIANCE, MEDIAN, STRING_AGG, QUANTILE_CONT, QUANTILE_DISC, MODE, CORR,
                 COVAR_SAMP, COVAR_POP, ARG_MAX, ARG_MIN -> pushed(writer, r);
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
    }

    /** {@code {"$op": <arg>}}. */
    private static SqlWriter accumulate(SqlWriter writer, String op, SqlAgg.Reducer r) {
        if (r.args().isEmpty()) {
            throw new IllegalStateException(r.fn() + " accumulates no argument");
        }
        return writer.append("{\"").append(op).append("\": ").expr(r.args().get(0), 0).append("}");
    }

    /** {@code count(*)} counts rows; {@code count(x)} the non-null values. */
    private static SqlWriter countAccumulator(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().isEmpty()) {
            return writer.append("{\"$sum\": 1}");
        }
        return writer.append("{\"$sum\": {\"$cond\": [{\"$eq\": [").expr(r.args().get(0), 0)
                .append(", null]}, 0, 1]}}");
    }

    /** The group's values pushed, each with its order keys when the aggregate is ordered. */
    private static SqlWriter pushed(SqlWriter writer, SqlAgg.Reducer r) {
        writer.append("{\"$push\": ");
        pushedValue(writer, r);
        return writer.append("}");
    }

    /** What an aggregate on the push route pushes: its value -- a two-column statistic's pair of values -- with its
     *  order keys when the aggregate is ordered. */
    private static SqlWriter pushedValue(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().isEmpty()) {
            throw new IllegalStateException(r.fn() + " accumulates no argument");
        }
        SqlWriter.Piece value = BIVARIATE.contains(r.fn())
                ? x -> x.append("{\"x\": ").expr(r.args().get(0), 0).append(", \"y\": ").expr(r.args().get(1), 0)
                        .append("}")
                : x -> x.expr(r.args().get(0), 0);
        if (r.orderBy().isEmpty()) {
            return writer.piece(value);
        }
        return pushedWithKeys(writer, value, r.orderBy().stream().map(com.legend.sql.SqlSelect.SortKey::expr)
                .toList());
    }

    /** The read of a pushed aggregate: its values in their keys' order, the nulls dropped (a pair with a null side
     *  dropped whole), made distinct where the aggregate asks, and reduced; a quantile over the value's own
     *  descending order where the reducer orders it so (the Lowerer's DESCENDING percentile). */
    private static SqlWriter.Piece pushedRead(SqlAgg.Reducer r, String path) {
        return pushedRead(r, MongoJson.text(path));
    }

    private static SqlWriter.Piece pushedRead(SqlAgg.Reducer r, SqlWriter.Piece path) {
        List<SqlExpr> extras = BIVARIATE.contains(r.fn()) ? List.of() : r.args().subList(1, r.args().size());
        SqlWriter.Piece values = pushedValues(path, r.orderBy());
        SqlWriter.Piece present = BIVARIATE.contains(r.fn()) ? x -> nonNullPairs(x, values)
                : x -> nonNull(x, values);
        SqlWriter.Piece set = r.distinct() ? x -> distinctInOrder(x, present) : present;
        boolean descending = !r.orderBy().isEmpty()
                && r.orderBy().stream().noneMatch(com.legend.sql.SqlSelect.SortKey::ascending);
        return x -> arrayReduction(x, r.fn(), set, extras, descending);
    }

    /** {@code {"v": value, "k0": key, "n0": <key is null>, ...}}: a pushed value with its order keys and each key's
     *  null flag, so the read's sort places nulls as the SQL engine does (S73's fix, 2026-10-17). */
    private static SqlWriter pushedWithKeys(SqlWriter writer, SqlExpr value, List<SqlExpr> keys) {
        return pushedWithKeys(writer, x -> x.expr(value, 0), keys);
    }

    private static SqlWriter pushedWithKeys(SqlWriter writer, SqlWriter.Piece value, List<SqlExpr> keys) {
        writer.append("{\"v\": ").piece(value);
        for (int i = 0; i < keys.size(); i++) {
            writer.append(", \"k").append(i).append("\": ").expr(keys.get(i), 0).append(", \"n").append(i)
                    .append("\": {\"$eq\": [").expr(keys.get(i), 0).append(", null]}");
        }
        return writer.append("}");
    }

    /** Whether the aggregate is pushed and reduced at the read, rather than accumulated natively. */
    private static boolean pushRoute(SqlAgg.Reducer r) {
        return r.distinct() || !r.orderBy().isEmpty() || PUSHED.contains(r.fn());
    }

    /** A group stage's field for an aggregate ({@code __agg0}); a dynamic pivot's per-key aggregate's field carries
     *  the key's position ({@code __agg0_pk3}), written by the runner in each copy of the per-key region, so its name
     *  and every read of it are written into the statement (a read inside the projection's own copy of that region). */
    record GroupField(String stem, boolean perKey) {
        GroupField {
            MongoJson.key(stem);
        }

        /** The field's name, quoted, with {@code suffix} ({@code _n}: a sum's count beside it). */
        SqlWriter name(SqlWriter w, String suffix) {
            w.append("\"" + stem);
            if (perKey) {
                w.append("_pk").counter();
            }
            return w.append(suffix + "\"");
        }

        /** The read of the field (with {@code suffix}) as a path. */
        SqlWriter.Piece path(String suffix) {
            return x -> {
                x.append("\"$" + stem);
                if (perKey) {
                    x.append("_pk").counter();
                }
                x.append(suffix + "\"");
            };
        }
    }

    /**
     * An aggregate's accumulator, written as the group field's value, and the read of that field the projection
     * writes in the marker's place: the field itself for a native accumulator; for a pushed aggregate, the pushed
     * values sorted by their keys, their nulls dropped, made distinct, and reduced.
     */
    SqlWriter.Piece accumulator(SqlWriter w, SqlExpr node, GroupField field) {
        SqlWriter.Piece path = field.path("");
        switch (node) {
            case SqlAgg.Reducer r -> {
                if (!pushRoute(r)) {
                    reducer(w, r);
                    if (r.fn() == SqlAgg.Fn.SUM) {
                        // SQL's SUM over no non-null value is null, and $sum answers 0: a count of the non-null
                        // values beside the sum decides at the read (S72's fix, 2026-10-17)
                        field.name(w.append(", "), "_n").append(": {\"$sum\": {\"$cond\": [{\"$eq\": [")
                                .expr(r.args().get(0), 0).append(", null]}, 0, 1]}}");
                        SqlWriter.Piece count = field.path("_n");
                        return x -> x.append("{\"$cond\": [{\"$gt\": [").piece(count).append(", 0]}, ").piece(path)
                                .append(", null]}");
                    }
                    return path;
                }
                pushed(w, r);
                return pushedRead(r, path);
            }
            case SqlExpr.OrderedListAgg ola -> {
                w.append("{\"$push\": ");
                pushedWithKeys(w, ola.value(), List.of(ola.orderBy()));
                w.append("}");
                SqlWriter.Piece values = pushedValues(path, List.of(com.legend.sql.SqlSelect.SortKey.asc(
                        ola.orderBy())));
                return x -> arrayReduction(x, SqlAgg.Fn.LIST, values, List.of());
            }
            case SqlExpr.JsonArrayAgg j -> {
                w.append("{\"$push\": ");
                if (j.orderKeys().isEmpty()) {
                    w.expr(j.value(), 0);
                } else {
                    pushedWithKeys(w, j.value(), j.orderKeys().stream().map(SqlExpr.JsonArrayAgg.Key::expr).toList());
                }
                w.append("}");
                List<com.legend.sql.SqlSelect.SortKey> keys = new ArrayList<>();
                for (SqlExpr.JsonArrayAgg.Key k : j.orderKeys()) {
                    keys.add(new com.legend.sql.SqlSelect.SortKey(k.expr(), !k.desc(), null, null));
                }
                SqlWriter.Piece values = pushedValues(path, keys);
                return x -> x.append("{\"$ifNull\": [").piece(values).append(", []]}");
            }
            default -> throw new IllegalStateException("not an aggregate: " + node);
        }
    }

    /** The value the group's field holds when no row reached it: SQL's one row over no input. */
    static String emptyInput(SqlExpr node) {
        return switch (node) {
            case SqlAgg.Reducer r -> pushRoute(r) ? "[]" : r.fn() == SqlAgg.Fn.COUNT ? "0" : "null";
            case SqlExpr.OrderedListAgg ola -> "[]";
            case SqlExpr.JsonArrayAgg j -> "[]";
            default -> throw new IllegalStateException("not an aggregate: " + node);
        };
    }

    /** The pushed values in their keys' order: the array sorted by the keys, each element's value read. */
    private static SqlWriter.Piece pushedValues(SqlWriter.Piece path, List<com.legend.sql.SqlSelect.SortKey> keys) {
        if (keys.isEmpty()) {
            return path;
        }
        // each key's null flag before the key: SQL's placement (ascending puts nulls last, descending first,
        // unless the key says otherwise), which $sortArray's own order (null first) is not
        StringBuilder sortBy = new StringBuilder("{");
        for (int i = 0; i < keys.size(); i++) {
            com.legend.sql.SqlSelect.SortKey k = keys.get(i);
            boolean nullsFirst = k.nullOrder() == null ? !k.ascending()
                    : k.nullOrder() == com.legend.sql.SqlSelect.SortKey.NullOrder.NULLS_FIRST;
            sortBy.append(i > 0 ? ", " : "").append("\"n").append(i).append("\": ").append(nullsFirst ? -1 : 1)
                    .append(", \"k").append(i).append("\": ").append(k.ascending() ? 1 : -1);
        }
        sortBy.append("}");
        String by = sortBy.toString();
        return x -> x.append("{\"$map\": {\"input\": {\"$sortArray\": {\"input\": ").piece(path)
                .append(", \"sortBy\": " + by + "}}, \"in\": \"$$this.v\"}}");
    }

    /** The array without its nulls. */
    private static SqlWriter nonNull(SqlWriter writer, SqlWriter.Piece array) {
        return writer.append("{\"$filter\": {\"input\": {\"$ifNull\": [").piece(array)
                .append(", []]}, \"cond\": {\"$ne\": [" + THIS + ", null]}}}");
    }

    /** The array of pairs without those a null side: a two-column statistic counts the rows where both are
     *  present. */
    private static SqlWriter nonNullPairs(SqlWriter writer, SqlWriter.Piece array) {
        return writer.append("{\"$filter\": {\"input\": {\"$ifNull\": [").piece(array)
                .append(", []]}, \"cond\": {\"$and\": [{\"$ne\": [\"$$this.x\", null]}, {\"$ne\": [\"$$this.y\","
                        + " null]}]}}}");
    }

    /** The array's distinct elements, the first occurrence of each kept in place. */
    private static SqlWriter distinctInOrder(SqlWriter writer, SqlWriter.Piece array) {
        return writer.append("{\"$reduce\": {\"input\": ").piece(array).append(", \"initialValue\": [], \"in\":"
                + " {\"$cond\": [{\"$in\": [" + THIS + ", " + VALUE + "]}, " + VALUE + ", {\"$concatArrays\": ["
                + VALUE + ", [" + THIS + "]]}]}}}");
    }

    /** An aggregate over an array of non-null values: its count, or SQL's null over no value, else its
     *  reduction. */
    private static SqlWriter arrayReduction(SqlWriter writer, SqlAgg.Fn fn, SqlWriter.Piece array,
            List<SqlExpr> extras) {
        return arrayReduction(writer, fn, array, extras, false);
    }

    private static SqlWriter arrayReduction(SqlWriter writer, SqlAgg.Fn fn, SqlWriter.Piece array,
            List<SqlExpr> extras, boolean descending) {
        writer.append("{\"$let\": {\"vars\": {\"a\": ").piece(array).append("}, \"in\": ");
        if (fn == SqlAgg.Fn.COUNT) {
            return writer.append("{\"$size\": \"$$a\"}}}");
        }
        writer.append("{\"$cond\": [{\"$eq\": [{\"$size\": \"$$a\"}, 0]}, null, ");
        reduceArray(writer, fn, extras, descending);
        return writer.append("]}}}");
    }

    /** The reduction of the bound array {@code $$a}, by aggregate; a quantile or a median over the values sorted
     *  the way the reducer orders them (a descending percentile interpolates from the top). */
    private static SqlWriter reduceArray(SqlWriter writer, SqlAgg.Fn fn, List<SqlExpr> extras, boolean descending) {
        String sorted = "{\"$sortArray\": {\"input\": \"$$a\", \"sortBy\": " + (descending ? -1 : 1) + "}}";
        // the pairs' sides and their products, for the two-column statistics
        String xs = "{\"$map\": {\"input\": \"$$a\", \"in\": \"$$this.x\"}}";
        String ys = "{\"$map\": {\"input\": \"$$a\", \"in\": \"$$this.y\"}}";
        String xys = "{\"$map\": {\"input\": \"$$a\", \"in\": {\"$multiply\": [\"$$this.x\", \"$$this.y\"]}}}";
        // the population covariance: E[xy] - E[x]E[y]
        String covarPop = "{\"$subtract\": [{\"$avg\": " + xys + "}, {\"$multiply\": [{\"$avg\": " + xs + "},"
                + " {\"$avg\": " + ys + "}]}]}";
        return switch (fn) {
            case SUM -> writer.append("{\"$sum\": \"$$a\"}");
            case AVG -> writer.append("{\"$avg\": \"$$a\"}");
            case MIN -> writer.append("{\"$min\": \"$$a\"}");
            case MAX -> writer.append("{\"$max\": \"$$a\"}");
            case ANY_VALUE -> writer.append("{\"$arrayElemAt\": [\"$$a\", 0]}");
            case STDDEV_SAMP, STDDEV -> writer.append("{\"$stdDevSamp\": \"$$a\"}");
            case STDDEV_POP -> writer.append("{\"$stdDevPop\": \"$$a\"}");
            case VAR_SAMP, VARIANCE -> writer.append("{\"$pow\": [{\"$stdDevSamp\": \"$$a\"}, 2]}");
            case VAR_POP -> writer.append("{\"$pow\": [{\"$stdDevPop\": \"$$a\"}, 2]}");
            case LIST -> writer.append("\"$$a\"");
            case BOOL_AND -> writer.append("{\"$allElementsTrue\": [\"$$a\"]}");
            case BOOL_OR -> writer.append("{\"$anyElementTrue\": [\"$$a\"]}");
            case MEDIAN -> writer.append("{\"$let\": {\"vars\": {\"s\": " + sorted + ", \"n\": {\"$size\": \"$$a\"}},"
                    + " \"in\": {\"$cond\": [{\"$eq\": [{\"$mod\": [\"$$n\", 2]}, 1]}, {\"$arrayElemAt\": [\"$$s\","
                    + " {\"$toInt\": {\"$divide\": [{\"$subtract\": [\"$$n\", 1]}, 2]}}]}, {\"$avg\":"
                    + " [{\"$arrayElemAt\": [\"$$s\", {\"$toInt\": {\"$subtract\": [{\"$divide\": [\"$$n\", 2]},"
                    + " 1]}}]}, {\"$arrayElemAt\": [\"$$s\", {\"$toInt\": {\"$divide\": [\"$$n\", 2]}}]}]}]}}}");
            case STRING_AGG -> writer.append("{\"$reduce\": {\"input\": \"$$a\", \"initialValue\": null, \"in\":"
                    + " {\"$cond\": [{\"$eq\": [" + VALUE + ", null]}, {\"$toString\": " + THIS + "},"
                    + " {\"$concat\": [" + VALUE + ", ").piece(x -> separator(x, extras))
                    .append(", {\"$toString\": " + THIS + "}]}]}}}");
            case QUANTILE_CONT -> writer.append("{\"$let\": {\"vars\": {\"s\": " + sorted + ", \"p\": {\"$multiply\":"
                    + " [").piece(x -> quantile(x, extras)).append(", {\"$subtract\": [{\"$size\": \"$$a\"}, 1]}]}},"
                    + " \"in\": {\"$let\": {\"vars\": {\"lo\": {\"$arrayElemAt\": [\"$$s\", {\"$toInt\": {\"$floor\":"
                    + " \"$$p\"}}]}, \"hi\": {\"$arrayElemAt\": [\"$$s\", {\"$toInt\": {\"$ceil\": \"$$p\"}}]}},"
                    + " \"in\": {\"$add\": [\"$$lo\", {\"$multiply\": [{\"$subtract\": [\"$$p\", {\"$floor\":"
                    + " \"$$p\"}]}, {\"$subtract\": [\"$$hi\", \"$$lo\"]}]}]}}}}}");
            case QUANTILE_DISC -> writer.append("{\"$arrayElemAt\": [" + sorted + ", {\"$max\": [0, {\"$subtract\":"
                    + " [{\"$toInt\": {\"$ceil\": {\"$multiply\": [").piece(x -> quantile(x, extras))
                    .append(", {\"$size\": \"$$a\"}]}}}, 1]}]}]}");
            case COUNT -> writer.append("{\"$size\": \"$$a\"}");
            // the most frequent value, the smallest among equally frequent ones: the sorted values' runs counted,
            // a longer run taking the lead (Pure's mode: a fold over the sorted numbers, a strictly greater count
            // replacing the leader)
            case MODE -> writer.append("{\"$let\": {\"vars\": {\"m\": {\"$reduce\": {\"input\": " + sorted
                    + ", \"initialValue\": {\"v\": null, \"n\": 0, \"p\": null, \"c\": 0}, \"in\": {\"$let\":"
                    + " {\"vars\": {\"c\": {\"$cond\": [{\"$eq\": [" + THIS + ", \"$$value.p\"]}, {\"$add\":"
                    + " [\"$$value.c\", 1]}, 1]}}, \"in\": {\"$cond\": [{\"$gt\": [\"$$c\", \"$$value.n\"]}, {\"v\": "
                    + THIS + ", \"n\": \"$$c\", \"p\": " + THIS + ", \"c\": \"$$c\"}, {\"v\": \"$$value.v\", \"n\":"
                    + " \"$$value.n\", \"p\": " + THIS + ", \"c\": \"$$c\"}]}}}}}}, \"in\": \"$$m.v\"}}");
            case COVAR_POP -> writer.append(covarPop);
            // the sample covariance scales the population's by n/(n-1); null over one pair, as SQL answers
            case COVAR_SAMP -> writer.append("{\"$cond\": [{\"$lte\": [{\"$size\": \"$$a\"}, 1]}, null, {\"$multiply\":"
                    + " [" + covarPop + ", {\"$divide\": [{\"$size\": \"$$a\"}, {\"$subtract\": [{\"$size\": \"$$a\"},"
                    + " 1]}]}]}]}");
            // the correlation: the population covariance over the product of the population deviations; null
            // where a side does not vary, as SQL answers
            case CORR -> writer.append("{\"$let\": {\"vars\": {\"sx\": {\"$stdDevPop\": " + xs + "}, \"sy\":"
                    + " {\"$stdDevPop\": " + ys + "}}, \"in\": {\"$cond\": [{\"$or\": [{\"$eq\": [\"$$sx\", 0]},"
                    + " {\"$eq\": [\"$$sy\", 0]}]}, null, {\"$divide\": [" + covarPop + ", {\"$multiply\": [\"$$sx\","
                    + " \"$$sy\"]}]}]}}}");
            // the value (x) of the pair with the greatest or the least key (y)
            case ARG_MAX -> writer.append("{\"$let\": {\"vars\": {\"p\": {\"$arrayElemAt\": [{\"$sortArray\": {\"input\":"
                    + " \"$$a\", \"sortBy\": {\"y\": -1}}}, 0]}}, \"in\": \"$$p.x\"}}");
            case ARG_MIN -> writer.append("{\"$let\": {\"vars\": {\"p\": {\"$arrayElemAt\": [{\"$sortArray\": {\"input\":"
                    + " \"$$a\", \"sortBy\": {\"y\": 1}}}, 0]}}, \"in\": \"$$p.x\"}}");
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw new IllegalStateException(
                    "lowering marker " + fn + " reached the renderer");
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw new IllegalStateException("window-only function " + fn + " in a reduction");
        };
    }

    private static SqlWriter separator(SqlWriter writer, List<SqlExpr> extras) {
        return extras.isEmpty() ? writer.append("\"\"") : writer.expr(extras.get(0), 0);
    }

    private static SqlWriter quantile(SqlWriter writer, List<SqlExpr> extras) {
        if (extras.isEmpty()) {
            throw new IllegalStateException("a quantile without its fraction");
        }
        return writer.expr(extras.get(0), 0);
    }

    // ==================================================================
    // Names and types
    // ==================================================================

    /** The BSON type name a cast converts to, a refusal where MongoDB has no such type. */
    @Override
    protected String scalarTypeName(SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN -> "bool";
            case INTEGER, BIGINT -> "long";
            case DOUBLE -> "double";
            case VARCHAR, LITERAL, TEMPORAL_TEXT -> "string";
            case DATE, TIMESTAMP -> "date";
            // a 128-bit integer (the lowering's exact band for integer arithmetic near the 64-bit edge, and the
            // fraction-free spelling of a Float) is a Decimal128 here: 34 exact digits, the one type past 64 bits
            // (the MongoDB line's homework 3, 2026-10-17)
            case HUGEINT -> "decimal";
            case TIMESTAMPTZ -> throw new DialectCapability("a cast to a zoned timestamp reached MongoDb, whose"
                    + " dates are UTC instants");
            case TIME -> throw new DialectCapability("a cast to a time of day reached MongoDb, which has no time"
                    + " type");
            case JSON -> throw new IllegalStateException("a cast to JSON is the value itself on MongoDb");
            case DECIMAL_TEXT -> throw new DialectCapability("a cast to a decimal's text reached MongoDb");
        };
    }

    @Override
    protected String structTypeName(SqlType.Struct st) {
        throw new DialectCapability("a cast to a struct type reached MongoDb, whose documents are untyped");
    }

    /**
     * A date's text by a format: one {@code $dateToString} where every part has a code; a name or a 12-hour part,
     * which has none, composed beside the coded runs -- a weekday's or a month's name by its number, the 12-hour
     * clock and its marker from the hour.
     */
    private SqlWriter strftime(SqlWriter writer, List<SqlExpr> a) {
        if (!(a.get(1) instanceof SqlExpr.FormatLit fl) || fl.parts().stream().noneMatch(MongoDb::composed)) {
            return writer.append("{\"$dateToString\": {\"date\": ").expr(a.get(0), 0).append(", \"format\": ")
                    .expr(a.get(1), 0).append("}}");
        }
        writer.append("{\"$let\": {\"vars\": {\"d\": ").expr(a.get(0), 0).append("}, \"in\": {\"$concat\": [");
        List<com.legend.sql.DateFmt> run = new ArrayList<>();
        boolean first = true;
        for (com.legend.sql.DateFmt p : fl.parts()) {
            if (!composed(p)) {
                run.add(p);
                continue;
            }
            first = codedRun(writer, run, first);
            writer.append(first ? "" : ", ");
            first = false;
            writer.append(switch ((com.legend.sql.DateFmt.Part) p) {
                case WEEKDAY_NAME -> "{\"$arrayElemAt\": [[\"Sunday\", \"Monday\", \"Tuesday\", \"Wednesday\","
                        + " \"Thursday\", \"Friday\", \"Saturday\"], {\"$subtract\": [{\"$dayOfWeek\": \"$$d\"}, 1]}]}";
                case MONTH_NAME -> "{\"$arrayElemAt\": [[\"January\", \"February\", \"March\", \"April\", \"May\","
                        + " \"June\", \"July\", \"August\", \"September\", \"October\", \"November\","
                        + " \"December\"], {\"$subtract\": [{\"$month\": \"$$d\"}, 1]}]}";
                case MONTH_ABBREV -> "{\"$arrayElemAt\": [[\"Jan\", \"Feb\", \"Mar\", \"Apr\", \"May\", \"Jun\","
                        + " \"Jul\", \"Aug\", \"Sep\", \"Oct\", \"Nov\", \"Dec\"], {\"$subtract\": [{\"$month\":"
                        + " \"$$d\"}, 1]}]}";
                case HOUR12 -> "{\"$let\": {\"vars\": {\"h\": {\"$toString\": {\"$add\": [{\"$mod\": [{\"$add\":"
                        + " [{\"$hour\": \"$$d\"}, 11]}, 12]}, 1]}}}, \"in\": {\"$cond\": [{\"$eq\": [{\"$strLenCP\":"
                        + " \"$$h\"}, 1]}, {\"$concat\": [\"0\", \"$$h\"]}, \"$$h\"]}}}";
                case HOUR12_NOPAD -> "{\"$toString\": {\"$add\": [{\"$mod\": [{\"$add\": [{\"$hour\": \"$$d\"}, 11]},"
                        + " 12]}, 1]}}";
                case AMPM -> "{\"$cond\": [{\"$lt\": [{\"$hour\": \"$$d\"}, 12]}, \"AM\", \"PM\"]}";
                // the minimal fraction: the milliseconds' trailing zeros trimmed, one digit kept
                case SUBSEC_MIN -> "{\"$let\": {\"vars\": {\"f\": {\"$rtrim\": {\"input\": {\"$dateToString\": {\"date\":"
                        + " \"$$d\", \"format\": \"%L\"}}, \"chars\": \"0\"}}}, \"in\": {\"$cond\": [{\"$eq\": [\"$$f\","
                        + " \"\"]}, \"0\", \"$$f\"]}}}";
                default -> throw new IllegalStateException("not a composed date part: " + p);
            });
        }
        codedRun(writer, run, first);
        return writer.append("]}}}");
    }

    /** The run of coded parts as one {@code $dateToString}, emptied; whether nothing has been written yet. */
    private boolean codedRun(SqlWriter writer, List<com.legend.sql.DateFmt> run, boolean first) {
        if (run.isEmpty()) {
            return first;
        }
        writer.append(first ? "" : ", ").append("{\"$dateToString\": {\"date\": \"$$d\", \"format\": ")
                .append(MongoJson.string(formatText(new SqlExpr.FormatLit(List.copyOf(run))))).append("}}");
        run.clear();
        return false;
    }

    /** A part {@code $dateToString} has no code for: a name, the 12-hour clock, or the minimal fraction. */
    private static boolean composed(com.legend.sql.DateFmt p) {
        return p instanceof com.legend.sql.DateFmt.Part part && switch (part) {
            case MONTH_ABBREV, MONTH_NAME, WEEKDAY_NAME, HOUR12, HOUR12_NOPAD, AMPM, SUBSEC_MIN -> true;
            default -> false;
        };
    }

    /** {@code $dateToString}'s codes: the date and time parts to the millisecond; a name or a 12-hour part has none
     *  (composed beside the codes by {@link #strftime}). */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        StringBuilder out = new StringBuilder();
        for (com.legend.sql.DateFmt p : fl.parts()) {
            out.append(switch (p) {
                case com.legend.sql.DateFmt.Text t -> t.s().replace("%", "%%");
                case com.legend.sql.DateFmt.Part part -> switch (part) {
                    case YEAR4 -> "%Y";
                    case MONTH2 -> "%m";
                    case DAY2 -> "%d";
                    case HOUR2 -> "%H";
                    case MIN2 -> "%M";
                    case SEC2 -> "%S";
                    // a BSON date holds milliseconds, so its microsecond and nanosecond digits are zeros: the text
                    // is exact (the TDS text's DateTime cell reads the first three of six, 2026-10-17)
                    case SUBSEC_MICRO -> "%L000";
                    case SUBSEC_NANO -> "%L000000";
                    case SUBSEC_MIN, MONTH_ABBREV, MONTH_NAME, WEEKDAY_NAME, HOUR12, HOUR12_NOPAD, AMPM -> throw
                            new IllegalStateException("a date format's " + part + " part reached $dateToString's"
                                    + " codes outside its composition (MongoDb.strftime)");
                };
            });
        }
        return out.toString();
    }

    /** {@inheritDoc} The kind names {@link #call}'s JSON_TYPE answers. */
    @Override
    protected String jsonKindName(com.legend.sql.JsonKind kind) {
        return switch (kind) {
            case OBJECT -> "OBJECT";
            case ARRAY -> "ARRAY";
            case NULL -> "NULL";
            case STRING -> "STRING";
            case INTEGER, UNSIGNED_INTEGER -> "INTEGER";
            case FLOAT -> "FLOAT";
            case BOOLEAN -> "BOOLEAN";
        };
    }

    /** {@inheritDoc} {@code $type}'s name of a date; no other type's name is compared. */
    @Override
    protected String typeofName(SqlType.Scalar type) {
        if (type == SqlType.Scalar.DATE || type == SqlType.Scalar.TIMESTAMP) {
            return "date";
        }
        throw new DialectCapability("a TYPEOF compared with " + type + "'s name reached MongoDb");
    }

    /** A name is a JSON string: no quoting, no reserved words. */
    @Override
    protected String ident(String name) {
        return name;
    }

    @Override
    protected String delimited(String name) {
        return name;
    }

    @Override
    protected String ddlIdentifier(String name, boolean declaredQuoted) {
        return name;
    }

    /** A declared column's BSON type, for the store's own reading; a collection declares no columns to the server. */
    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT, INTEGER, SMALLINT, TINYINT -> "long";
                case FLOAT, DOUBLE, REAL -> "double";
                case BIT -> "bool";
                case TIMESTAMP, DATE -> "date";
                // main's column kinds of 2026-10-17 (the Cypher line's temporal legs): a BSON date keeps milliseconds
                // and no time of day apart from a day
                case TIME -> throw new DialectCapability("a TIME column: MongoDB has no time-of-day type");
                case TIMESTAMP_NS -> throw new DialectCapability("a TIMESTAMP_NS column: MongoDB's dates keep"
                        + " milliseconds");
                case VARCHAR -> "string";
                case JSON, OBJECT -> "object";
                case ARRAY -> "array";
                case OTHER -> "objectId";
                case DISTINCT -> throw DdlSpelling.unspelled(p.kind());
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> "string";
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> "decimal";
        };
    }

    // ==================================================================
    // DDL and DML: commands
    // ==================================================================

    /** A table is a collection: the {@code create} and {@code drop} commands; a schema has no counterpart (a
     *  collection's database is the connection's), nor has a temporary collection. */
    @Override
    public String render(com.legend.sql.SqlDdl ddl) {
        return switch (ddl) {
            case com.legend.sql.SqlDdl.CreateTable ct -> {
                if (ct.temporary()) {
                    throw new DialectCapability("a temporary table reached MongoDb, which has no temporary"
                            + " collection");
                }
                yield "{\"create\": " + MongoJson.string(collection(ct.schema(), ct.table())) + "}";
            }
            case com.legend.sql.SqlDdl.DropTable dt -> "{\"drop\": "
                    + MongoJson.string(collection(dt.schema(), dt.table())) + "}";
            case com.legend.sql.SqlDdl.CreateSchema cs -> throw new DialectCapability("a schema (" + cs.schema()
                    + ") reached MongoDb: a collection's database is the connection's, and has no schema");
            case com.legend.sql.SqlDdl.DropSchema ds -> throw new DialectCapability("a schema (" + ds.schema()
                    + ") reached MongoDb: a collection's database is the connection's, and has no schema");
        };
    }

    private static String collection(@com.legend.base.Nullable String schema, String table) {
        if (schema != null && !schema.isEmpty() && !"default".equals(schema)) {
            throw new DialectCapability("a schema-qualified table (" + schema + "." + table + ") reached MongoDb: a"
                    + " collection has no schema");
        }
        if (table.indexOf('.') >= 0) {
            throw new DialectCapability("a schema-qualified table (" + table + ") reached MongoDb: a collection has"
                    + " no schema");
        }
        return table;
    }

    /** Rows in: the {@code insert} command, each row a document of values; rows out: {@code delete} of every
     *  document. A copy between collections and a temporary collection from a query are not shaped. */
    @Override
    public String render(com.legend.sql.SqlDml dml) {
        return switch (dml) {
            case com.legend.sql.SqlDml.InsertValues iv -> insert(iv);
            case com.legend.sql.SqlDml.InsertFromTable it -> throw new DialectCapability("a copy from a table"
                    + " reached MongoDb: an insert from another collection is not shaped yet");
            case com.legend.sql.SqlDml.DeleteAll da -> "{\"delete\": "
                    + MongoJson.string(collection(da.schema(), da.table())) + ", \"deletes\": [{\"q\": {},"
                    + " \"limit\": 0}]}";
            case com.legend.sql.SqlDml.CreateTempTableAs ct -> tempTableAs(ct);
        };
    }

    private String insert(com.legend.sql.SqlDml.InsertValues iv) {
        if (iv.columns().isEmpty()) {
            throw new DialectCapability("an insert naming no columns reached MongoDb: a document's fields are"
                    + " named");
        }
        SqlWriter writer = newWriter().append("{\"insert\": ").append(MongoJson.string(collection(iv.schema(),
                iv.table()))).append(", \"documents\": [");
        for (int r = 0; r < iv.rows().size(); r++) {
            List<SqlExpr> row = iv.rows().get(r);
            if (row.size() != iv.columns().size()) {
                throw new IllegalStateException("a row of " + row.size() + " values for " + iv.columns().size()
                        + " columns");
            }
            writer.append(r > 0 ? ", {" : "{");
            for (int c = 0; c < row.size(); c++) {
                writer.append(c > 0 ? ", " : "").append(MongoJson.key(iv.columns().get(c))).append(": ");
                value(writer, row.get(c));
            }
            writer.append("}");
        }
        return writer.append("]}").text();
    }

    /** A row's cell as a VALUE in Extended JSON (an inserted document is data, not an expression: a string is a JSON
     *  string here, never {@code $literal}): a literal, or null; anything else is refused by name. */
    private static SqlWriter value(SqlWriter writer, SqlExpr cell) {
        return switch (cell) {
            case SqlExpr.StringLit s -> writer.append(MongoJson.string(s.value()));
            case SqlExpr.IntLit i -> writer.append("{\"$numberLong\": \"").append(i.value()).append("\"}");
            case SqlExpr.FloatLit f -> writer.append("{\"$numberDouble\": \"").append(plainFloat(f.value()))
                    .append("\"}");
            case SqlExpr.DecimalLit d -> writer.append("{\"$numberDecimal\": \"").append(d.value().toPlainString())
                    .append("\"}");
            case SqlExpr.BoolLit b -> writer.append(b.value() ? "true" : "false");
            case SqlExpr.NullLit n -> writer.append("null");
            case SqlExpr.DateLit d -> writer.append(MongoJson.dateLiteral(d.iso()));
            case SqlExpr.TimestampLit t -> writer.append(MongoJson.dateLiteral(t.iso()));
            case SqlExpr.Cast c -> value(writer, c.value());
            default -> throw new DialectCapability("a row's cell is " + cell.getClass().getSimpleName()
                    + ": a MongoDb insert holds values, not expressions");
        };
    }

    /** MongoDB has no temporary collection: a plan's metamodel tables are its pipeline's own documents. */
    /** {@inheritDoc} No: an aggregation pipeline, which a {@code MongoDBConnection} declares (D4). */
    @Override
    public boolean relational() {
        return false;
    }

    @Override
    public boolean temporaryTables() {
        return false;
    }

    /** MongoDB: a dynamic pivot's probe plan reads the keys and then the body, two commands with no transaction
     *  across them on a server that is no replica set (a key written between the two is not the body's: register
     *  S103). */
    @Override
    public KeysSnapshot keysSnapshot() {
        return KeysSnapshot.NONE;
    }

    /** No column count bounds a document; its 16 MB bounds the answer, refused by the server when exceeded. */
    @Override
    public @com.legend.base.Nullable Integer resultColumnLimit() {
        return null;
    }
}
