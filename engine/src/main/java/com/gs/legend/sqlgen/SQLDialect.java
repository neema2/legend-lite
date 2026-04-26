package com.gs.legend.sqlgen;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface defining SQL dialect-specific behavior.
 * Implementations handle differences between database engines.
 */
public interface SQLDialect {

    /**
     * Detects the SQL dialect from a live JDBC connection's metadata.
     *
     * @param conn A live JDBC connection
     * @return The appropriate SQLDialect
     */
    static SQLDialect forConnection(Connection conn) {
        try {
            String product = conn.getMetaData().getDatabaseProductName();
            return switch (product) {
                case "DuckDB" -> DuckDBDialect.INSTANCE;
                case "SQLite" -> SQLiteDialect.INSTANCE;
                default -> DuckDBDialect.INSTANCE;
            };
        } catch (SQLException e) {
            return DuckDBDialect.INSTANCE;
        }
    }

    /**
     * @return The dialect name (e.g., "DuckDB", "SQLite")
     */
    String name();

    /**
     * Quote an identifier (table name, column name, alias).
     * 
     * @param identifier The identifier to quote
     * @return The quoted identifier
     */
    String quoteIdentifier(String identifier);

    /**
     * Quote a string literal value.
     * 
     * @param value The string value to quote
     * @return The quoted string literal
     */
    String quoteStringLiteral(String value);

    /**
     * Format a boolean literal.
     * 
     * @param value The boolean value
     * @return The SQL boolean representation
     */
    String formatBoolean(boolean value);

    /**
     * Format a NULL literal.
     * 
     * @return The SQL NULL representation
     */
    default String formatNull() {
        return "NULL";
    }



    /**
     * Format a DATE literal from Pure's %YYYY-MM-DD format.
     * 
     * @param pureDate The Pure date string (e.g., "%2024-01-15" or
     *                 "%2024-01-15T10:30:00")
     * @return SQL date/timestamp literal (e.g., "DATE '2024-01-15'" or "TIMESTAMP
     *         '2024-01-15 10:30:00'")
     */
    default String formatDate(String pureDate) {
        // Strip the % prefix
        String dateValue = pureDate.startsWith("%") ? pureDate.substring(1) : pureDate;

        // If it contains 'T', it's a DateTime - render as TIMESTAMP
        if (dateValue.contains("T")) {
            int tIdx = dateValue.indexOf('T');
            String datePart = dateValue.substring(0, tIdx);
            String timePart = dateValue.substring(tIdx + 1);

            // Extract timezone suffix if present (e.g., +0000, -0500)
            String tz = "";
            int tzIdx = timePart.lastIndexOf('+');
            if (tzIdx < 0)
                tzIdx = timePart.lastIndexOf('-');
            // Only treat as timezone if it's after the time portion (not a negative sign in
            // time)
            if (tzIdx > 0 && tzIdx >= timePart.indexOf(':')) {
                tz = timePart.substring(tzIdx);
                timePart = timePart.substring(0, tzIdx);
            }

            // Pad partial times: "17" -> "17:00:00", "17:09" -> "17:09:00"
            String[] timeParts = timePart.split(":");
            String hours = timeParts.length > 0 ? timeParts[0] : "00";
            String minutes = timeParts.length > 1 ? timeParts[1] : "00";
            String seconds = timeParts.length > 2 ? timeParts[2] : "00";
            String fullTime = hours + ":" + minutes + ":" + seconds;

            return "TIMESTAMP '" + datePart + " " + fullTime + tz + "'";
        }

        // Handle partial dates - Pure supports YYYY and YYYY-MM formats
        // Standard SQL DATE literals require full YYYY-MM-DD
        String[] parts = dateValue.split("-");
        if (parts.length == 1) {
            // Year only: 2012 -> 2012-01-01
            dateValue = dateValue + "-01-01";
        } else if (parts.length == 2) {
            // Year-month only: 2012-03 -> 2012-03-01
            dateValue = dateValue + "-01";
        }

        return "DATE '" + dateValue + "'";
    }

    /**
     * Format a TIMESTAMP literal from Pure's DateTime format.
     * Delegates to formatDate which already handles DateTime values.
     */
    default String formatTimestamp(String pureDate) {
        return formatDate(pureDate);
    }

    /**
     * Format a TIME literal from Pure's %HH:MM:SS format.
     * 
     * @param pureTime The Pure time string (e.g., "%12:30:00")
     * @return SQL time literal (e.g., "TIME '12:30:00'")
     */
    default String formatTime(String pureTime) {
        // Strip the % prefix and return as TIME literal
        String timeValue = pureTime.startsWith("%") ? pureTime.substring(1) : pureTime;
        return "TIME '" + timeValue + "'";
    }

    // ==================== Struct / Array Rendering ====================

    /**
     * Render an inline struct literal from pre-rendered field name→value pairs.
     * Field values are already rendered SQL expressions (e.g., quoted strings, numbers).
     *
     * @param fields Ordered map of field name → rendered SQL value
     * @return Dialect-specific struct literal
     */
    String renderStructLiteral(java.util.LinkedHashMap<String, String> fields);

    /**
     * Render an array literal from pre-rendered element values.
     *
     * @param elements List of rendered SQL expressions
     * @return Dialect-specific array literal
     */
    String renderArrayLiteral(java.util.List<String> elements);

    /**
     * Render the SQL expression that unnests (flattens) an array into rows.
     * SqlBuilder handles the JOIN structure (LEFT JOIN LATERAL);
     * this method provides only the dialect-specific unnest expression.
     *
     * @param arrayPath SQL expression for the array to unnest
     * @return Dialect-specific unnest expression
     */
    String renderUnnestExpression(String arrayPath);

    // ==================== Scalar Function Rendering ====================

    /**
     * Render a list-contains check.
     *
     * @param listExpr The list/array expression
     * @param elemExpr The element to find
     * @return Dialect-specific contains expression
     */
    String renderListContains(String listExpr, String elemExpr);

    /**
     * Render 1-based list element access. Default ANSI form is
     * {@code LIST_EXTRACT(list, index)}.
     */
    default String renderListExtract(String listExpr, String indexExpr) {
        return "LIST_EXTRACT(" + listExpr + ", " + indexExpr + ")";
    }

    /**
     * Render 1-based inclusive list slice. Default ANSI form is
     * {@code LIST_SLICE(list, from, to)}.
     */
    default String renderListSlice(String listExpr, String fromExpr, String toExpr) {
        return "LIST_SLICE(" + listExpr + ", " + fromExpr + ", " + toExpr + ")";
    }

    /**
     * Render list length. Default ANSI form is {@code LEN(list)}.
     */
    default String renderListLength(String listExpr) {
        return "LEN(" + listExpr + ")";
    }

    /**
     * Map a Pure type name to the SQL type name for CAST expressions.
     *
     * @param pureTypeName Pure type name (e.g., "String", "Integer", "Float")
     * @return SQL type name (e.g., "VARCHAR", "BIGINT", "DOUBLE")
     */
    String sqlTypeName(String pureTypeName);

    /**
     * Render date arithmetic: date + amount * unit.
     *
     * @param dateExpr Compiled date expression
     * @param amount   Compiled amount expression
     * @param unit     SQL interval unit (e.g., "DAY", "MONTH", "YEAR")
     * @return Dialect-specific date add expression
     */
    String renderDateAdd(String dateExpr, String amount, String unit);

    /**
     * Render starts-with check.
     *
     * @param str    String expression
     * @param prefix Prefix expression
     * @return Dialect-specific starts-with expression
     */
    String renderStartsWith(String str, String prefix);

    /**
     * Render ends-with check.
     *
     * @param str    String expression
     * @param suffix Suffix expression
     * @return Dialect-specific ends-with expression
     */
    String renderEndsWith(String str, String suffix);

    /**
     * Render a function call. PlanGenerator uses Pure/abstract function names;
     * this method maps them to dialect-specific SQL.
     * Default: uppercase the name and use standard SQL call syntax: NAME(args).
     *
     * @param pureName Pure/abstract function name (e.g., "listExtract",
     *                 "levenshteinDistance")
     * @param args     Pre-rendered SQL argument expressions
     * @return Dialect-specific function call SQL
     */
    default String renderFunction(String pureName, java.util.List<String> args) {
        // Default: treat the name as a standard SQL function
        return pureName + "(" + String.join(", ", args) + ")";
    }

    /**
     * Render the SELECT * EXCLUDE/EXCEPT clause for excluding columns from star.
     * Dialect-specific: e.g., EXCLUDE(...) or EXCEPT(...).
     *
     * @param columns Pre-rendered column names to exclude
     * @return Dialect-specific clause
     */
    String renderStarExcept(java.util.List<String> columns);

    // ==================== Variant Rendering ====================
    // No defaults — each dialect must implement its own Variant semantics.

    /** Mark a literal value as Variant. */
    String renderVariantLiteral(String expr);

    /** Access a Variant field by key (returns Variant). */
    String renderVariantAccess(String expr, String key);

    /** Access a Variant array element by index (returns Variant). */
    String renderVariantIndex(String expr, int index);

    /** Extract text value from Variant by key (returns string). */
    String renderVariantTextAccess(String expr, String key);

    /** Convert a value to Variant. */
    String renderToVariant(String expr);

    /** Cast Variant to a typed array. */
    String renderVariantArrayCast(String expr, String sqlType);

    /** Cast Variant to a scalar type. */
    String renderVariantScalarCast(String expr, String sqlType);

    /** Cast a value to VARIANT type for type preservation in mixed-type lists. */
    String renderVariantCast(String expr);

    /**
     * Render an interval unit literal for date diff functions.
     *
     * @param unit The interval unit (e.g., "DAY", "MONTH")
     * @return Dialect-specific interval unit expression
     */
    default String renderIntervalUnit(String unit) {
        return "'" + unit + "'";
    }

    // ==================== JSON Rendering ====================

    /**
     * Render a JSON object constructor from pre-rendered key-value pairs.
     * DuckDB: json_object(k1, v1, k2, v2, ...)
     * Snowflake: OBJECT_CONSTRUCT(k1, v1, k2, v2, ...)
     *
     * @param keyValuePairs Alternating key (string literal) and value expressions
     * @return Dialect-specific JSON object expression
     */
    default String renderJsonObject(java.util.List<String> keyValuePairs) {
        return "json_object(" + String.join(", ", keyValuePairs) + ")";
    }

    /**
     * Render a JSON array aggregation that collects rows into a JSON array.
     * DuckDB: json_group_array(expr)
     * Snowflake: ARRAY_AGG(expr)
     * Postgres: json_agg(expr)
     *
     * @param expr Pre-rendered expression to aggregate
     * @return Dialect-specific JSON array aggregation expression
     */
    default String renderJsonArrayAgg(String expr) {
        return "json_group_array(" + expr + ")";
    }

    // ==================== External Data Source ====================

    /**
     * Renders a complete SELECT subquery for an external data source URL.
     * The subquery must produce a single VARIANT column named "data".
     *
     * <p>DuckDB examples:
     * <ul>
     *   <li>{@code data:} URI → {@code SELECT unnest(CAST('...' AS JSON[]))::VARIANT AS "data"}</li>
     *   <li>{@code file:} URI → {@code SELECT json::VARIANT AS "data" FROM read_json_objects('...')}</li>
     * </ul>
     *
     * @param url The data source URL (data: URI, file:, or http:)
     * @return Complete SELECT SQL to be used as a subquery in FROM
     */
    String renderSourceUrl(String url);

    // ====================================================================
    // Codegen entry: pattern-match SqlExpr → SQL string.
    //
    // This is the ONE place SQL is produced from SqlExpr nodes. IR
    // records are pure data (no `toSql` methods, no SQLDialect imports);
    // this default implementation is the canonical codegen pass. Dialects
    // override individual {@code renderXxx} helpers when they diverge from
    // the ANSI / DuckDB-leaning defaults below — they do not override
    // {@code render(SqlExpr)} itself.
    //
    // Per AGENTS.md invariant 3a (IR is data, codegen lives in the
    // Dialect): if you need to add a new SqlExpr variant, add a new arm
    // here, not a `toSql` method on the record.
    // ====================================================================

    default String render(SqlExpr e) {
        return switch (e) {
            // ---- Column references ----
            case SqlExpr.Column c          -> quoteIdentifier(c.table()) + "." + quoteIdentifier(c.column());
            case SqlExpr.ColumnRef c       -> quoteIdentifier(c.name());
            case SqlExpr.Identifier i      -> i.name();
            case SqlExpr.LambdaExpr l      -> renderLambda(l);

            // ---- Literals ----
            case SqlExpr.NumericLiteral n  -> n.value().toString();
            case SqlExpr.DecimalLiteral d  -> d.value().toPlainString();
            case SqlExpr.NullLiteral n     -> "NULL";
            case SqlExpr.CurrentDate cd    -> "CURRENT_DATE";
            case SqlExpr.CurrentTimestamp ct -> "CURRENT_TIMESTAMP";
            case SqlExpr.IntervalLiteral il -> renderIntervalUnit(il.unit());
            case SqlExpr.OrderByTerm o     -> render(o.column()) + " " + o.direction() + " " + o.nullOrder();
            case SqlExpr.StringLiteral s   -> quoteStringLiteral(s.value());
            case SqlExpr.BoolLiteral b     -> formatBoolean(b.value());
            case SqlExpr.TimestampLiteral t -> formatTimestamp(t.value());
            case SqlExpr.DateLiteral d     -> formatDate(d.value());
            case SqlExpr.TimeLiteral t     -> formatTime(t.value());

            // ---- Operators ----
            case SqlExpr.Binary b          -> renderLegacyBinary(b);
            case SqlExpr.Grouped g         -> "(" + render(g.inner()) + ")";
            case SqlExpr.Unary u           -> u.op() + " " + render(u.operand());
            case SqlExpr.BinaryArith b     -> "(" + render(b.left()) + " " + b.op().sql() + " " + render(b.right()) + ")";
            case SqlExpr.StringConcat s    -> "(" + render(s.left()) + " || " + render(s.right()) + ")";
            case SqlExpr.Negate n          -> "(- " + render(n.expr()) + ")";
            case SqlExpr.BinaryCompare b   -> render(b.left()) + " " + b.op().sql() + " " + render(b.right());

            // ---- Lists ----
            case SqlExpr.ListExtract le    -> renderListExtract(render(le.list()), render(le.index()));
            case SqlExpr.ListSlice ls      -> renderListSlice(render(ls.list()), render(ls.from()), render(ls.to()));
            case SqlExpr.ListLength ll     -> renderListLength(render(ll.list()));

            // ---- Boolean ----
            case SqlExpr.And a             -> a.conditions().stream().map(this::render)
                                                  .collect(java.util.stream.Collectors.joining(" AND ", "(", ")"));
            case SqlExpr.Or o              -> o.conditions().stream().map(this::render)
                                                  .collect(java.util.stream.Collectors.joining(" OR ", "(", ")"));
            case SqlExpr.Not n             -> "NOT (" + render(n.expr()) + ")";

            // ---- Function calls / type ops ----
            case SqlExpr.FunctionCall fc   -> renderFunction(fc.name(),
                                                  fc.args().stream().map(this::render).toList());
            case SqlExpr.Cast c            -> "CAST(" + render(c.expr()) + " AS " + sqlTypeName(c.pureTypeName()) + ")";
            case SqlExpr.IntegerDivide id  -> render(id.left()) + " // " + render(id.right());

            // ---- Predicates ----
            case SqlExpr.IsNull n          -> render(n.expr()) + " IS NULL";
            case SqlExpr.IsNotNull n       -> render(n.expr()) + " IS NOT NULL";
            case SqlExpr.In in             -> render(in.expr()) + " IN ("
                                                  + in.values().stream().map(this::render)
                                                       .collect(java.util.stream.Collectors.joining(", "))
                                                  + ")";
            case SqlExpr.Between b         -> render(b.expr()) + " BETWEEN " + render(b.low())
                                                  + " AND " + render(b.high());

            // ---- CASE ----
            case SqlExpr.CaseWhen cw       -> "CASE WHEN " + render(cw.condition())
                                                  + " THEN " + render(cw.thenExpr())
                                                  + " ELSE " + render(cw.elseExpr()) + " END";
            case SqlExpr.SearchedCase sc   -> renderSearchedCase(sc);

            // ---- Dialect-delegated scalar ----
            case SqlExpr.ListContains lc   -> renderListContains(render(lc.list()), render(lc.element()));
            case SqlExpr.StartsWith sw     -> renderStartsWith(render(sw.str()), render(sw.prefix()));
            case SqlExpr.EndsWith ew       -> renderEndsWith(render(ew.str()), render(ew.suffix()));
            case SqlExpr.DateAdd da        -> renderDateAdd(render(da.date()), render(da.amount()), da.unit());
            case SqlExpr.VariantTextExtract v -> renderVariantTextAccess(render(v.expr()), v.key());
            case SqlExpr.FieldAccess fa    -> render(fa.base()) + "." + fa.field();
            case SqlExpr.Unnest u          -> renderUnnestExpression(render(u.array()));
            case SqlExpr.StrPosition sp    -> "POSITION(" + render(sp.substring()) + " IN " + render(sp.string()) + ")";

            // ---- Window ----
            case SqlExpr.WindowSpec ws     -> renderWindowSpec(ws);
            case SqlExpr.WindowCall wc     -> renderWindowCall(wc);

            // ---- Star / qualified star ----
            case SqlExpr.Star s            -> "*";
            case SqlExpr.QualifiedStar qs  -> qs.table() + ".*";

            // ---- Compile-time markers (must not reach codegen) ----
            case SqlExpr.AssociationRef ar -> throw new IllegalStateException(
                    "AssociationRef should be resolved before SQL generation: "
                            + ar.hops() + "." + ar.targetCol());

            // ---- Struct / Array ----
            case SqlExpr.StructLiteral sl  -> {
                var rendered = new java.util.LinkedHashMap<String, String>();
                sl.fields().forEach((k, v) -> rendered.put(k, render(v)));
                yield renderStructLiteral(rendered);
            }
            case SqlExpr.ArrayLiteral al   -> renderArrayLiteral(
                    al.elements().stream().map(this::render).toList());

            // ---- JSON ----
            case SqlExpr.JsonObject jo     -> renderJsonObject(
                    jo.keyValuePairs().stream().map(this::render).toList());
            case SqlExpr.JsonArrayAgg ja   -> renderJsonArrayAgg(render(ja.expr()));

            // ---- Variant ----
            case SqlExpr.VariantLiteral v  -> renderVariantLiteral(render(v.expr()));
            case SqlExpr.VariantAccess v   -> renderVariantAccess(render(v.expr()), v.key());
            case SqlExpr.VariantIndex v    -> renderVariantIndex(render(v.expr()), v.index());
            case SqlExpr.VariantTextAccess v -> renderVariantTextAccess(render(v.expr()), v.key());
            case SqlExpr.ToVariant tv      -> renderToVariant(render(tv.expr()));
            case SqlExpr.VariantArrayCast v -> renderVariantArrayCast(render(v.expr()), v.sqlType());
            case SqlExpr.VariantScalarCast v -> renderVariantScalarCast(render(v.expr()), v.sqlType());
            case SqlExpr.VariantCast vc    -> renderVariantCast(render(vc.expr()));

            // ---- External data source ----
            case SqlExpr.SourceUrl su      -> renderSourceUrl(su.url());
        };
    }

    /**
     * Lambda expression rendering — DuckDB style ({@code s -> body} for
     * single-param, {@code ((y, x) -> body)} for multi-param). Override
     * for dialects with different lambda syntax.
     */
    default String renderLambda(SqlExpr.LambdaExpr l) {
        String body = render(l.body());
        if (l.params().size() == 1) {
            return l.params().get(0) + " -> " + body;
        }
        return "((" + String.join(", ", l.params()) + ") -> " + body + ")";
    }

    /**
     * Legacy stringly-typed binary operator rendering. Kept until Phase
     * 3.3 finishes the {@link SqlExpr.Binary} → {@link SqlExpr.BinaryArith}
     * / {@link SqlExpr.BinaryCompare} migration; arithmetic and string
     * concat ops parenthesise, comparison/logical ops do not.
     */
    default String renderLegacyBinary(SqlExpr.Binary b) {
        String inner = render(b.left()) + " " + b.op() + " " + render(b.right());
        return switch (b.op()) {
            case "+", "-", "*", "/", "||", "//", "<<", ">>", "&", "|", "^" -> "(" + inner + ")";
            default -> inner;
        };
    }

    /**
     * {@code CASE WHEN c1 THEN r1 [WHEN c2 THEN r2 …] [ELSE e] END}.
     */
    default String renderSearchedCase(SqlExpr.SearchedCase sc) {
        StringBuilder sb = new StringBuilder("CASE");
        for (var br : sc.branches()) {
            sb.append(" WHEN ").append(render(br.condition()))
              .append(" THEN ").append(render(br.result()));
        }
        if (sc.elseExpr() != null) {
            sb.append(" ELSE ").append(render(sc.elseExpr()));
        }
        sb.append(" END");
        return sb.toString();
    }

    /**
     * {@code PARTITION BY … ORDER BY … <frame>} — the inner OVER clause
     * body. Wrapped by {@code WindowFunction} or {@code WindowCall} with
     * the surrounding parens.
     */
    default String renderWindowSpec(SqlExpr.WindowSpec ws) {
        StringBuilder sb = new StringBuilder();
        if (!ws.partitionBy().isEmpty()) {
            sb.append("PARTITION BY ");
            sb.append(ws.partitionBy().stream().map(this::render)
                    .collect(java.util.stream.Collectors.joining(", ")));
        }
        if (!ws.orderBy().isEmpty()) {
            if (!sb.isEmpty()) sb.append(" ");
            sb.append("ORDER BY ");
            sb.append(ws.orderBy().stream().map(this::render)
                    .collect(java.util.stream.Collectors.joining(", ")));
        }
        if (ws.frame() != null && !ws.frame().isEmpty()) {
            if (!sb.isEmpty()) sb.append(" ");
            sb.append(ws.frame());
        }
        return sb.toString();
    }

    /**
     * {@code FUNC(args) OVER (PARTITION BY … ORDER BY … [ROWS|RANGE …])}.
     * The {@code fn} dispatches via the unified {@link #render(com.gs.legend.plan.sql.SqlAggregate)};
     * reducer variants render identically to agg context, ranking and value
     * variants are window-only.
     */
    default String renderWindowCall(SqlExpr.WindowCall wc) {
        String call = render(wc.fn());
        StringBuilder over = new StringBuilder(" OVER (");
        boolean need = false;
        if (!wc.partitionBy().isEmpty()) {
            over.append("PARTITION BY ");
            for (int i = 0; i < wc.partitionBy().size(); i++) {
                if (i > 0) over.append(", ");
                over.append(render(wc.partitionBy().get(i)));
            }
            need = true;
        }
        if (!wc.orderBy().isEmpty()) {
            if (need) over.append(" ");
            over.append("ORDER BY ");
            for (int i = 0; i < wc.orderBy().size(); i++) {
                if (i > 0) over.append(", ");
                over.append(render(wc.orderBy().get(i)));
            }
            need = true;
        }
        if (wc.frame().isPresent()) {
            if (need) over.append(" ");
            over.append(renderWindowFrame(wc.frame().get()));
        }
        over.append(")");
        return call + over.toString();
    }


    /** {@code ROWS|RANGE BETWEEN <start> AND <end>}. ANSI-standard. */
    default String renderWindowFrame(SqlExpr.WindowFrame f) {
        return f.type().name() + " BETWEEN "
                + renderFrameBound(f.start(), /*isStart=*/true)
                + " AND "
                + renderFrameBound(f.end(), /*isStart=*/false);
    }

    // ====================================================================
    // Codegen entry: pattern-match SqlRelation → SQL string.
    //
    // Same role as render(SqlExpr): the ONE place SQL clause structure
    // is produced from SqlRelation nodes. The IR (plan/sql/SqlRelation)
    // is pure data; this default is the canonical codegen pass.
    //
    // Per AGENTS.md invariant 3a: do not add a SQL emission method to
    // SqlRelation records. Add a new arm to render(SqlRelation) here.
    // ====================================================================

    default String render(com.gs.legend.plan.sql.SqlRelation rel) {
        return switch (rel) {
            case com.gs.legend.plan.sql.SqlRelation.SourceExprRel r -> renderSourceExpr(r);
            case com.gs.legend.plan.sql.SqlRelation.TableRef r      -> renderTableRef(r);
            case com.gs.legend.plan.sql.SqlRelation.Values r        -> renderValues(r);
            case com.gs.legend.plan.sql.SqlRelation.Filter r        -> renderFilter(r);
            case com.gs.legend.plan.sql.SqlRelation.Project r       -> renderProject(r);
            case com.gs.legend.plan.sql.SqlRelation.Sort r          -> renderSort(r);
            case com.gs.legend.plan.sql.SqlRelation.Limit r         -> renderLimit(r);
            case com.gs.legend.plan.sql.SqlRelation.Distinct r      -> renderDistinct(r);
            case com.gs.legend.plan.sql.SqlRelation.Rename r        -> renderRename(r);
            case com.gs.legend.plan.sql.SqlRelation.Select r        -> renderSelect(r);
            case com.gs.legend.plan.sql.SqlRelation.SubqueryRel r   -> renderSubqueryRel(r);
            case com.gs.legend.plan.sql.SqlRelation.Extend r        -> renderExtend(r);
            case com.gs.legend.plan.sql.SqlRelation.GroupBy r       -> renderGroupBy(r);
            case com.gs.legend.plan.sql.SqlRelation.Aggregate r     -> renderAggregateRel(r);
            case com.gs.legend.plan.sql.SqlRelation.Union r         -> renderUnion(r);
            case com.gs.legend.plan.sql.SqlRelation.Join r          -> renderJoin(r);
            case com.gs.legend.plan.sql.SqlRelation.Pivot r         -> renderPivot(r);
            case com.gs.legend.plan.sql.SqlRelation.AsOfJoin r      -> renderAsOfJoin(r);
            case com.gs.legend.plan.sql.SqlRelation.Flatten r       -> throw notImpl(r);
            case com.gs.legend.plan.sql.SqlRelation.WithCtes r      -> throw notImpl(r);
        };
    }

    private static UnsupportedOperationException notImpl(com.gs.legend.plan.sql.SqlRelation r) {
        return new UnsupportedOperationException(
                "[plangen] SQLDialect.render: not yet implemented for " + r.getClass().getSimpleName());
    }

    /**
     * {@code SELECT <expr> AS <colAlias>} — a single-column source from a
     * scalar expression (e.g. {@code |1+1}).
     */
    default String renderSourceExpr(com.gs.legend.plan.sql.SqlRelation.SourceExprRel r) {
        String colAlias = r.outputs().isEmpty() ? "result" : r.outputs().get(0).name();
        return "SELECT " + render(r.expr()) + " AS " + quoteIdentifier(colAlias);
    }

    /**
     * {@code SELECT * FROM "TBL" AS "t0"}. Schema-qualified tables emit
     * {@code "schema"."table"}; column-level projection lives in
     * {@code Project}, so a bare TableRef is {@code SELECT *}.
     */
    default String renderTableRef(com.gs.legend.plan.sql.SqlRelation.TableRef r) {
        String qualifiedTable = r.schema() != null
                ? quoteIdentifier(r.schema()) + "." + quoteIdentifier(r.table())
                : quoteIdentifier(r.table());
        return "SELECT * FROM " + qualifiedTable + " AS " + quoteIdentifier(r.alias());
    }

    /**
     * {@code SELECT * FROM (VALUES (...), ...) AS alias("c1","c2",...)}.
     * Outer SELECT-star + inner VALUES works cross-dialect and keeps
     * column aliasing explicit.
     */
    default String renderValues(com.gs.legend.plan.sql.SqlRelation.Values r) {
        StringBuilder sb = new StringBuilder("SELECT * FROM (VALUES ");
        for (int i = 0; i < r.rows().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("(");
            var row = r.rows().get(i);
            for (int c = 0; c < row.size(); c++) {
                if (c > 0) sb.append(", ");
                sb.append(render(row.get(c)));
            }
            sb.append(")");
        }
        sb.append(") AS ").append(quoteIdentifier(r.alias())).append("(");
        for (int i = 0; i < r.columnNames().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(quoteIdentifier(r.columnNames().get(i)));
        }
        return sb.append(")").toString();
    }

    /**
     * {@code <source> WHERE <pred>}. When {@code source} is a bare
     * {@code SELECT} (TableRef, Values, SourceExprRel) we fuse into a
     * single statement; otherwise we wrap as a subquery.
     */
    default String renderFilter(com.gs.legend.plan.sql.SqlRelation.Filter r) {
        return renderAsStatement(r.source()) + " WHERE " + render(r.predicate());
    }

    /**
     * {@code SELECT a AS alias, b AS alias, ... FROM <source>}.
     * Aliased-leaf sources stay inline; composite sources get wrapped.
     */
    default String renderProject(com.gs.legend.plan.sql.SqlRelation.Project r) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < r.projections().size(); i++) {
            if (i > 0) sb.append(", ");
            var p = r.projections().get(i);
            sb.append(render(p.expr())).append(" AS ").append(quoteIdentifier(p.alias()));
        }
        return sb.append(" FROM ").append(renderAsFromItem(r.source())).toString();
    }

    /** {@code <source> ORDER BY k1 DIR, k2 DIR, ...}. */
    default String renderSort(com.gs.legend.plan.sql.SqlRelation.Sort r) {
        StringBuilder sb = new StringBuilder(renderAsStatement(r.source()));
        sb.append(" ORDER BY ");
        for (int i = 0; i < r.keys().size(); i++) {
            if (i > 0) sb.append(", ");
            var k = r.keys().get(i);
            sb.append(render(k.expr())).append(" ").append(k.direction().name());
        }
        return sb.toString();
    }

    /** {@code <source> LIMIT n OFFSET off}. Non-positive fields omitted. */
    default String renderLimit(com.gs.legend.plan.sql.SqlRelation.Limit r) {
        StringBuilder sb = new StringBuilder(renderAsStatement(r.source()));
        if (r.n() >= 0) sb.append(" LIMIT ").append(r.n());
        if (r.offset() > 0) sb.append(" OFFSET ").append(r.offset());
        return sb.toString();
    }

    /** {@code SELECT DISTINCT * FROM <source>}. */
    default String renderDistinct(com.gs.legend.plan.sql.SqlRelation.Distinct r) {
        return "SELECT DISTINCT * FROM " + renderAsFromItem(r.source());
    }

    /**
     * {@code SELECT old1 AS new1, old2 AS new2, <passthrough cols> FROM <source>}.
     * Non-renamed columns emit identity so the outer schema order matches
     * {@code Rename#outputs()}.
     */
    default String renderRename(com.gs.legend.plan.sql.SqlRelation.Rename r) {
        var alias = ensureFromAlias(r.source());
        StringBuilder sb = new StringBuilder("SELECT ");
        var cols = r.source().outputs();
        for (int i = 0; i < cols.size(); i++) {
            if (i > 0) sb.append(", ");
            String name = cols.get(i).name();
            String newName = r.renames().getOrDefault(name, name);
            sb.append(quoteIdentifier(alias)).append(".").append(quoteIdentifier(name));
            if (!newName.equals(name)) {
                sb.append(" AS ").append(quoteIdentifier(newName));
            }
        }
        return sb.append(" FROM ").append(renderAsFromItem(r.source())).toString();
    }

    /** {@code SELECT c1, c2, ... FROM <source>}. */
    default String renderSelect(com.gs.legend.plan.sql.SqlRelation.Select r) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < r.columns().size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(quoteIdentifier(r.columns().get(i)));
        }
        return sb.append(" FROM ").append(renderAsFromItem(r.source())).toString();
    }

    /**
     * {@code SELECT <alias>.*, e1 AS n1, ... FROM <source>}. Extend
     * preserves the source schema in full and appends computed columns.
     */
    default String renderExtend(com.gs.legend.plan.sql.SqlRelation.Extend r) {
        String srcAlias = ensureFromAlias(r.source());
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(quoteIdentifier(srcAlias)).append(".*");
        for (var c : r.cols()) {
            sb.append(", ").append(render(c.expr())).append(" AS ").append(quoteIdentifier(c.name()));
        }
        return sb.append(" FROM ").append(renderAsFromItem(r.source())).toString();
    }

    /** {@code (<inner>) AS alias} — explicit subquery wrapping. */
    default String renderSubqueryRel(com.gs.legend.plan.sql.SqlRelation.SubqueryRel r) {
        return "(" + render(r.inner()) + ") AS " + quoteIdentifier(r.alias());
    }

    /**
     * {@code SELECT key1 AS "alias1", ..., AGG1 AS a1, ... FROM <source>
     * GROUP BY key1, ...}. Each key carries its user-supplied ColSpec
     * alias from the typed AST so downstream operators can reference the
     * grouped columns by name.
     */
    default String renderGroupBy(com.gs.legend.plan.sql.SqlRelation.GroupBy r) {
        StringBuilder sb = new StringBuilder("SELECT ");
        boolean first = true;
        for (var k : r.keys()) {
            if (!first) sb.append(", "); first = false;
            sb.append(render(k.expr())).append(" AS ").append(quoteIdentifier(k.alias()));
        }
        for (var a : r.aggs()) {
            if (!first) sb.append(", "); first = false;
            sb.append(renderAgg(a)).append(" AS ").append(quoteIdentifier(a.alias()));
        }
        sb.append(" FROM ").append(renderAsFromItem(r.source()));
        if (!r.keys().isEmpty()) {
            sb.append(" GROUP BY ");
            for (int i = 0; i < r.keys().size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(render(r.keys().get(i).expr()));
            }
        }
        return sb.toString();
    }

    /** Key-less aggregate → single-row result. */
    default String renderAggregateRel(com.gs.legend.plan.sql.SqlRelation.Aggregate r) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < r.aggs().size(); i++) {
            if (i > 0) sb.append(", ");
            var a = r.aggs().get(i);
            sb.append(renderAgg(a)).append(" AS ").append(quoteIdentifier(a.alias()));
        }
        return sb.append(" FROM ").append(renderAsFromItem(r.source())).toString();
    }

    /** Render one aggregate emission. Dispatches on the typed
     * {@link com.gs.legend.plan.sql.SqlAggregate} variant. */
    default String renderAgg(com.gs.legend.plan.sql.SqlRelation.Agg a) {
        return render(a.fn());
    }

    /**
     * Render a typed {@link com.gs.legend.plan.sql.SqlAggregate}. Single
     * exhaustive switch over the unified hierarchy — reducer variants render
     * the same in agg context and window context (the surrounding
     * {@code OVER (...)} clause is the only difference); ranking and value
     * variants are window-only but share this dispatch path. javac enforces
     * exhaustiveness via the sealed {@code permits} clauses; no
     * {@code default} arms (AGENTS.md invariant 3).
     *
     * <p>Override on a per-dialect basis when the SQL spelling differs
     * (DuckDB {@code QUANTILE_CONT}, {@code ARG_MAX}, etc.).
     *
     * <p>No {@code distinct} flag. {@code COUNT(DISTINCT x)} would be a
     * separate sealed variant when a Pure native targets it.
     */
    default String render(com.gs.legend.plan.sql.SqlAggregate fn) {
        return switch (fn) {
            // -- Reducers (unary) --
            case com.gs.legend.plan.sql.SqlAggregate.CountStar cs           -> "COUNT(*)";
            case com.gs.legend.plan.sql.SqlAggregate.Sum s                  -> unary("SUM", s.expr());
            case com.gs.legend.plan.sql.SqlAggregate.Count c                -> unary("COUNT", c.expr());
            case com.gs.legend.plan.sql.SqlAggregate.Max m                  -> unary("MAX", m.expr());
            case com.gs.legend.plan.sql.SqlAggregate.Min m                  -> unary("MIN", m.expr());
            case com.gs.legend.plan.sql.SqlAggregate.Avg a                  -> unary("AVG", a.expr());
            case com.gs.legend.plan.sql.SqlAggregate.StdDev s               -> unary("STDDEV", s.expr());
            case com.gs.legend.plan.sql.SqlAggregate.Variance v             -> unary("VARIANCE", v.expr());
            case com.gs.legend.plan.sql.SqlAggregate.Product p              -> unary("PRODUCT", p.expr());
            case com.gs.legend.plan.sql.SqlAggregate.Median m               -> unary("MEDIAN", m.expr());
            case com.gs.legend.plan.sql.SqlAggregate.Mode m                 -> unary("MODE", m.expr());
            case com.gs.legend.plan.sql.SqlAggregate.HashCode h             -> unary("HASH", h.expr());
            case com.gs.legend.plan.sql.SqlAggregate.StdDevPopulation s     -> unary("STDDEV_POP", s.expr());
            case com.gs.legend.plan.sql.SqlAggregate.StdDevSample s         -> unary("STDDEV_SAMP", s.expr());
            case com.gs.legend.plan.sql.SqlAggregate.VariancePopulation v   -> unary("VAR_POP", v.expr());
            case com.gs.legend.plan.sql.SqlAggregate.VarianceSample v       -> unary("VAR_SAMP", v.expr());
            case com.gs.legend.plan.sql.SqlAggregate.MaxBy m                ->
                    "MAX_BY(" + render(m.value()) + ", " + render(m.key()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.MinBy m                ->
                    "MIN_BY(" + render(m.value()) + ", " + render(m.key()) + ")";
            // No SQL primitive for weighted average — emit the composite
            // form: (SUM(value * weight) / SUM(weight)). Wrapped in parens
            // so it's safe inside larger expressions.
            case com.gs.legend.plan.sql.SqlAggregate.WeightedAvg w          ->
                    "(SUM(" + render(w.value()) + " * " + render(w.weight())
                            + ") / SUM(" + render(w.weight()) + "))";
            // -- Reducers (multi-operand) --
            case com.gs.legend.plan.sql.SqlAggregate.JoinStrings j          ->
                    "STRING_AGG(" + render(j.expr()) + ", " + render(j.separator()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.PercentileCont p       ->
                    "PERCENTILE_CONT(" + render(p.p()) + ") WITHIN GROUP (ORDER BY " + render(p.expr()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.PercentileDisc p       ->
                    "PERCENTILE_DISC(" + render(p.p()) + ") WITHIN GROUP (ORDER BY " + render(p.expr()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.Corr c                 ->
                    "CORR(" + render(c.x()) + ", " + render(c.y()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.CovarPopulation c      ->
                    "COVAR_POP(" + render(c.x()) + ", " + render(c.y()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.CovarSample c          ->
                    "COVAR_SAMP(" + render(c.x()) + ", " + render(c.y()) + ")";
            // -- Ranking functions (zero-arg, window-only) --
            case com.gs.legend.plan.sql.SqlAggregate.RowNumber r            -> "ROW_NUMBER()";
            case com.gs.legend.plan.sql.SqlAggregate.Rank r                 -> "RANK()";
            case com.gs.legend.plan.sql.SqlAggregate.DenseRank r            -> "DENSE_RANK()";
            case com.gs.legend.plan.sql.SqlAggregate.PercentRank r          -> "PERCENT_RANK()";
            case com.gs.legend.plan.sql.SqlAggregate.CumulativeDistribution r -> "CUME_DIST()";
            // -- Value functions (window-only) --
            case com.gs.legend.plan.sql.SqlAggregate.FirstValue f           -> "FIRST_VALUE(" + render(f.expr()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.LastValue l            -> "LAST_VALUE(" + render(l.expr()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.Lag l                  ->
                    "LAG(" + String.join(", ", l.args().stream().map(this::render).toList()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.Lead l                 ->
                    "LEAD(" + String.join(", ", l.args().stream().map(this::render).toList()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.Ntile n                -> "NTILE(" + render(n.buckets()) + ")";
            case com.gs.legend.plan.sql.SqlAggregate.NthValue n             -> "NTH_VALUE(" + render(n.expr()) + ", " + render(n.n()) + ")";
        };
    }

    /** Helper for the unary {@code FN(expr)} shape. */
    private String unary(String name, SqlExpr expr) {
        return name + "(" + render(expr) + ")";
    }

    /** {@code <left> UNION [ALL] <right>} — both sides are full SELECTs. */
    default String renderUnion(com.gs.legend.plan.sql.SqlRelation.Union r) {
        return render(r.left()) + (r.all() ? " UNION ALL " : " UNION ") + render(r.right());
    }

    /**
     * {@code SELECT * FROM <left> <kind> JOIN <right> [ON <cond>]}. Both
     * sides render as FROM-items so aliases are bound for the ON clause.
     */
    default String renderJoin(com.gs.legend.plan.sql.SqlRelation.Join r) {
        String kind = renderJoinKeyword(r.type(), /*outer=*/false);
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(renderAsFromItem(r.left()))
                .append(" ").append(kind).append(" ")
                .append(renderAsFromItem(r.right()));
        if (r.type() != com.gs.legend.plan.sql.SqlRelation.JoinType.CROSS && r.on() != null) {
            sb.append(" ON ").append(render(r.on()));
        }
        return sb.toString();
    }

    /**
     * {@code PIVOT (<source>) ON <pivotCol> USING AGG(x) AS alias, ...}.
     * DuckDB pivot — grouping keys / pivot values are auto-inferred so we
     * only emit the {@code USING} aggregates.
     */
    default String renderPivot(com.gs.legend.plan.sql.SqlRelation.Pivot r) {
        StringBuilder sb = new StringBuilder("PIVOT (")
                .append(render(r.source()))
                .append(") ON ").append(quoteIdentifier(r.spec().pivotKey()))
                .append(" USING ");
        var aggs = r.spec().aggs();
        for (int i = 0; i < aggs.size(); i++) {
            if (i > 0) sb.append(", ");
            var a = aggs.get(i);
            sb.append(renderAgg(a)).append(" AS ").append(quoteIdentifier(a.alias()));
        }
        return sb.toString();
    }

    /** {@code SELECT * FROM <left> ASOF LEFT JOIN <right> ON <pred>}. */
    default String renderAsOfJoin(com.gs.legend.plan.sql.SqlRelation.AsOfJoin r) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(renderAsFromItem(r.left()))
                .append(" ASOF LEFT JOIN ")
                .append(renderAsFromItem(r.right()));
        if (r.spec().matchPredicate() != null) {
            sb.append(" ON ").append(render(r.spec().matchPredicate()));
        }
        return sb.toString();
    }

    // -------- structural helpers (shared between top-level and FROM-item) --------

    /**
     * Render a relation as a self-contained {@code SELECT} statement so
     * trailing clauses ({@code ORDER BY}, {@code LIMIT}) attach cleanly.
     * FROM-item-shaped sources ({@code SubqueryRel}, {@code Join},
     * {@code TableRef}) need wrapping; others print as a statement already.
     */
    default String renderAsStatement(com.gs.legend.plan.sql.SqlRelation r) {
        if (r instanceof com.gs.legend.plan.sql.SqlRelation.SubqueryRel
                || r instanceof com.gs.legend.plan.sql.SqlRelation.Join
                || r instanceof com.gs.legend.plan.sql.SqlRelation.TableRef) {
            return "SELECT * FROM " + renderAsFromItem(r);
        }
        return render(r);
    }

    /**
     * Render a relation as a {@code FROM}-clause item: aliased-leaf
     * sources pass through unchanged, joins render inline so their
     * left/right aliases stay visible to the outer predicate, everything
     * else gets wrapped in an anonymous subquery.
     */
    default String renderAsFromItem(com.gs.legend.plan.sql.SqlRelation src) {
        if (src instanceof com.gs.legend.plan.sql.SqlRelation.TableRef tr) {
            String q = tr.schema() != null
                    ? quoteIdentifier(tr.schema()) + "." + quoteIdentifier(tr.table())
                    : quoteIdentifier(tr.table());
            return q + " AS " + quoteIdentifier(tr.alias());
        }
        if (src instanceof com.gs.legend.plan.sql.SqlRelation.SubqueryRel sq) {
            return renderSubqueryRel(sq);
        }
        if (src instanceof com.gs.legend.plan.sql.SqlRelation.Join j) {
            return renderJoinBody(j);
        }
        String alias = src.alias() != null ? src.alias() : "_s";
        return "(" + render(src) + ") AS " + quoteIdentifier(alias);
    }

    /** Render a {@link com.gs.legend.plan.sql.SqlRelation.Join} as a bare FROM-clause join chain (no outer SELECT *). */
    default String renderJoinBody(com.gs.legend.plan.sql.SqlRelation.Join j) {
        String kind = renderJoinKeyword(j.type(), /*outer=*/true);
        StringBuilder sb = new StringBuilder()
                .append(renderAsFromItem(j.left()))
                .append(" ").append(kind).append(" ")
                .append(renderAsFromItem(j.right()));
        if (j.type() != com.gs.legend.plan.sql.SqlRelation.JoinType.CROSS && j.on() != null) {
            sb.append(" ON ").append(render(j.on()));
        }
        return sb.toString();
    }

    /**
     * Map join type to SQL keyword. {@code outer=true} emits the
     * {@code OUTER}-qualified spelling used inside FROM-clause join
     * chains; the top-level wrapper omits {@code OUTER} for brevity
     * (both forms are semantically identical in standard SQL).
     */
    default String renderJoinKeyword(com.gs.legend.plan.sql.SqlRelation.JoinType t, boolean outer) {
        return switch (t) {
            case INNER -> "INNER JOIN";
            case LEFT  -> outer ? "LEFT OUTER JOIN"  : "LEFT JOIN";
            case RIGHT -> outer ? "RIGHT OUTER JOIN" : "RIGHT JOIN";
            case FULL  -> outer ? "FULL OUTER JOIN"  : "FULL JOIN";
            case CROSS -> "CROSS JOIN";
        };
    }

    /**
     * Best-effort alias for a relation we need to project columns off of.
     * For a {@code Join} (rendered inline in FROM), walks down the left
     * spine until an aliased leaf — that's the "source row" side of
     * Extend / Rename that the outer {@code *} should pick columns from.
     */
    private static String ensureFromAlias(com.gs.legend.plan.sql.SqlRelation src) {
        com.gs.legend.plan.sql.SqlRelation cur = src;
        while (cur instanceof com.gs.legend.plan.sql.SqlRelation.Join j) cur = j.left();
        return cur.alias() != null ? cur.alias() : "_s";
    }

    /** Window-frame bound: {@code UNBOUNDED PRECEDING} / {@code N FOLLOWING} etc. */
    default String renderFrameBound(SqlExpr.FrameBound b, boolean isStart) {
        return switch (b) {
            case SqlExpr.UnboundedFrameBound u -> isStart ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING";
            case SqlExpr.CurrentRowFrameBound c -> "CURRENT ROW";
            case SqlExpr.OffsetFrameBound o -> {
                if (o.offset() == 0) yield "CURRENT ROW";
                double mag = Math.abs(o.offset());
                String lit = (mag == Math.floor(mag) && !Double.isInfinite(mag))
                        ? String.valueOf((long) mag)
                        : java.math.BigDecimal.valueOf(mag).stripTrailingZeros().toPlainString();
                yield lit + (o.offset() < 0 ? " PRECEDING" : " FOLLOWING");
            }
        };
    }
}
