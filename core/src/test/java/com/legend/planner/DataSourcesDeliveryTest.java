// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.dialect.RenderedStatement;
import com.legend.sql.dialect.SqlDialect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code data:} payload AS EACH DATABASE TAKES IT (the JSON pass, 2026-11-11, the user's rulings: "move everything off
 * temp table", H2 inline "so we can get rid of all the temp table code completely", "Yes on Databricks"): the payload
 * as one bound parameter the database splits -- its measured statement pinned per database, the payload never the
 * statement's text -- or the statement's own rows on H2 and MongoDB, and on Databricks above its parameter limit; a
 * statement length a database bounds refused at planning by name (Trino's); an empty payload no row everywhere; a
 * payload that is no array, or no records, refused by name before any dialect is asked. No database makes a table.
 */
class DataSourcesDeliveryTest {

    /** A payload whose record holds a quote, a backslash and a line break. */
    private static final String PAYLOAD = "data:application/json,[{\"t\":\"it's a\\\\b\"},{\"t\":\"x\\ny\"}]";

    private static final String ARRAY = PAYLOAD.substring(PAYLOAD.indexOf(',') + 1);

    private static SqlSource.SourceUrl source(String url) {
        List<OutputCol> outputs = List.of(new OutputCol("data", SqlType.Scalar.JSON, true, OutputCol.Origin.DERIVED),
                new OutputCol(SqlSource.SourceUrl.PLACE, SqlType.Scalar.BIGINT, false, OutputCol.Origin.DERIVED));
        return new SqlSource.SourceUrl(url, "t0", outputs, SqlSource.SourceUrl.Form.RECORDS);
    }

    private static SqlQuery frame(String url) {
        return SqlSelect.starOf(source(url));
    }

    private static SqlDialect dialect(DatabaseType type) {
        return Databases.dialect(type);
    }

    /** Each database's measured statement for a frame's payload bound as one parameter (the JSON pass's
     *  measurements, 2026-11-11; Snowflake's and BigQuery's round 2's). */
    static final Map<DatabaseType, String> BOUND = Map.ofEntries(
            Map.entry(DatabaseType.Postgres, "SELECT legend_payload.value AS \"data\", CAST(legend_payload.ordinality"
                    + " - 1 AS BIGINT) AS \"u_frame_ord__\" FROM jsonb_array_elements(CAST(? AS JSONB)) WITH ORDINALITY"
                    + " AS legend_payload(value, ordinality)"),
            Map.entry(DatabaseType.DuckDB, "FROM json_each(CAST(? AS JSON)) AS legend_payload"),
            Map.entry(DatabaseType.SQLite, "SELECT CASE legend_payload.type WHEN 'text' THEN"
                    + " json_quote(legend_payload.value) WHEN 'null' THEN 'null' WHEN 'true' THEN 'true' WHEN 'false'"
                    + " THEN 'false' ELSE CAST(legend_payload.value AS TEXT) END AS"),
            Map.entry(DatabaseType.MariaDB, "FROM JSON_TABLE(?, '$[*]' COLUMNS (legend_ord FOR ORDINALITY,"
                    + " legend_element JSON PATH '$')) AS legend_payload"),
            Map.entry(DatabaseType.MySQL, "SELECT JSON_EXTRACT(legend_doc.legend_payload, CONCAT('$[',"
                    + " legend_rows.legend_ord - 1, ']')) AS"),
            Map.entry(DatabaseType.Trino, "FROM UNNEST(CAST(json_parse(?) AS ARRAY(JSON))) WITH ORDINALITY AS"
                    + " legend_payload(legend_element, legend_ord)"),
            Map.entry(DatabaseType.ClickHouse, "FROM (SELECT JSONExtractArrayRaw(?) AS legend_elements) ARRAY JOIN"
                    + " arrayEnumerate(legend_elements) AS legend_index"),
            Map.entry(DatabaseType.Databricks, "FROM variant_explode(parse_json(?))"),
            Map.entry(DatabaseType.Snowflake, "SELECT legend_payload.value AS \"data\", CAST(legend_payload.index AS"
                    + " BIGINT) AS \"u_frame_ord__\" FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?))) legend_payload"),
            Map.entry(DatabaseType.BigQuery, "SELECT legend_payload AS `data`, legend_place AS `u_frame_ord__` FROM"
                    + " UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(?))) AS legend_payload WITH OFFSET AS legend_place"));

    /** The databases whose statement holds the payload's rows itself. */
    static final List<DatabaseType> INLINE = List.of(DatabaseType.H2, DatabaseType.MongoDB);

    @Test
    void eachDatabaseBindsThePayloadAsOneParameterInItsMeasuredStatement() {
        for (var expected : BOUND.entrySet()) {
            SqlDialect dialect = dialect(expected.getKey());
            RenderedStatement written = dialect.renderStatement(DataSources.read(frame(PAYLOAD), dialect));
            assertTrue(written.sql().contains(expected.getValue()), expected.getKey() + ": " + written.sql());
            assertEquals(List.of(new RenderedStatement.Binding.Payload(ARRAY)),
                    written.binds().stream().map(RenderedStatement.Bind::binding).toList(),
                    expected.getKey() + ": the payload is the bound value");
            assertTrue(!written.sql().contains("it's") && !written.sql().toLowerCase(java.util.Locale.ROOT)
                    .contains("legend_data_"), expected.getKey() + ": never the text, never a table: " + written.sql());
            // an empty payload: the empty array bound, no row (measured)
            for (String url : List.of("data:application/json,[]", "data:application/json,")) {
                RenderedStatement empty = dialect.renderStatement(DataSources.read(frame(url), dialect));
                assertEquals(List.of(new RenderedStatement.Binding.Payload("[]")),
                        empty.binds().stream().map(RenderedStatement.Bind::binding).toList(),
                        expected.getKey() + " " + url);
            }
        }
    }

    /** Every database with a dialect is one of the two lists: none makes a table, none refuses a payload. */
    @Test
    void everyDatabaseIsBoundOrInline() {
        for (DatabaseType type : DatabaseType.values()) {
            SqlDialect dialect;
            try {
                dialect = Databases.dialect(type);
            } catch (com.legend.error.NotImplementedException none) {
                continue;   // no dialect at all: no query reaches the database
            }
            if (type == DatabaseType.Deephaven) {
                // 2026-11-14 (main's JSON pass merged into the Deephaven branch, the user to rule): the one database
                // with no delivery, refused by name (DataDeliveryDeclaredTest.assertRefusedOnDeephaven)
                com.legend.sql.dialect.DataDeliveryDeclaredTest.assertRefusedOnDeephaven(dialect);
                continue;
            }
            assertTrue(BOUND.containsKey(type) || INLINE.contains(type), type + " is bound or inline");
            SqlSource read = dialect.deliver(SqlDialect.DataPayload.alone(source(PAYLOAD), List.of("{\"t\":1}")));
            assertEquals(BOUND.containsKey(type), read instanceof SqlSource.SourceUrl, type.name());
        }
    }

    /** H2 and MongoDB write the rows into the statement: an empty payload is the relation's columns and no row -- one
     *  typed NULL row removed by WHERE FALSE (the lowering's empty TDS literal's shape), MongoDB's {@code $match}. */
    @Test
    void anEmptyPayloadIsNoRowOnEveryInlineDatabase() {
        for (DatabaseType type : INLINE) {
            SqlDialect dialect = dialect(type);
            for (String url : List.of("data:application/json,[]", "data:application/json,")) {
                String sql = dialect.render(DataSources.read(frame(url), dialect));
                assertTrue(type == DatabaseType.MongoDB ? sql.contains("$match") : sql.contains("WHERE FALSE"),
                        type + ": " + sql);
            }
            String rows = dialect.render(DataSources.read(frame(PAYLOAD), dialect));
            assertTrue(type == DatabaseType.MongoDB || rows.contains("it''s"), type + ": the rows inline: " + rows);
        }
    }

    /** Databricks binds up to its driver's parameter limit (1 MiB of characters, measured) and writes the rows
     *  inline above it -- VALUES of the elements' texts the select over it parses (the lane's measured form). */
    @Test
    void databricksBindsUpToItsParameterLimitAndWritesTheRowsAbove() {
        SqlDialect databricks = dialect(DatabaseType.Databricks);
        String element = "{\"t\":\"" + "x".repeat(1000) + "\"}";
        int under = 1_048_576 / (element.length() + 1) - 1;
        String small = "data:application/json,[" + String.join(",", java.util.Collections.nCopies(under, element)) + "]";
        RenderedStatement bound = databricks.renderStatement(DataSources.read(frame(small), databricks));
        assertEquals(1, bound.binds().size(), "bound under the limit");
        String large = "data:application/json,[" + String.join(",", java.util.Collections.nCopies(under + 2, element))
                + "]";
        RenderedStatement inline = databricks.renderStatement(DataSources.read(frame(large), databricks));
        assertEquals(List.of(), inline.binds(), "inline above the limit");
        assertTrue(inline.sql().contains("(SELECT parse_json(legend_rows.col1) AS `data`, legend_rows.col2 AS"
                + " `u_frame_ord__` FROM VALUES ('{\"t\":\"xx"), inline.sql().substring(0, 200));
    }

    /**
     * Databricks bounds a statement's parameters TOGETHER (its driver: "the combined size of parameters"), so the
     * statement's payloads are decided against what it binds already (the B checkpoint's correctness audit, C3,
     * 2026-11-11: each was measured alone, so a payload read twice, or two that each fit, bound past the limit and were
     * refused at run): a payload of 600,000 characters read by both arms of a union binds once and is written inline
     * the second time, two different payloads likewise; a statement binding a value only its run knows (a declared
     * parameter, whose size no plan counts) binds none.
     */
    @Test
    void databricksDecidesAStatementsPayloadsTogether() {
        SqlDialect databricks = dialect(DatabaseType.Databricks);
        String element = "{\"t\":\"" + "x".repeat(1000) + "\"}";
        java.util.function.Function<String, String> payload = mark -> "data:application/json,[" + String.join(",",
                java.util.Collections.nCopies(590, element.replace("xx", mark))) + "]";
        for (List<String> urls : List.of(List.of(payload.apply("xx"), payload.apply("xx")),
                List.of(payload.apply("xx"), payload.apply("yy")))) {
            assertTrue(urls.get(0).length() < 1_048_576 && urls.get(0).length() * 2L > 1_048_576, "each fits, two not");
            SqlQuery union = new com.legend.sql.SqlUnion(List.of(frame(urls.get(0)), frame(urls.get(1))), true,
                    frame(urls.get(0)).outputs());
            RenderedStatement written = databricks.renderStatement(DataSources.read(union, databricks));
            assertEquals(1, written.binds().size(), "one bound, the other inline");
            assertTrue(written.sql().contains("FROM VALUES ('{\"t\":\""), "the second inline");
        }
        String small = "data:application/json,[{\"a\":1}]";
        String model = """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
                ###Connection
                RelationalDatabaseConnection s::Conn { store: s::DB; type: Databricks; specification: LocalH2 { }; auth: DefaultH2; }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """;
        com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(model),
                "{a: Integer[1]|sourceUrl('" + small + "')->filter(r|cast(get($r.data, 'a'), @Integer) == $a)"
                        + "->from(s::RT)}").executionPlan("s::RT");
        assertTrue(plan.answerSql().slots().stream().noneMatch(slot -> slot.binding()
                instanceof com.legend.executionplan.ExecutionPlan.Binding.Payload), "a declared parameter beside it:"
                + " the payload inline: " + plan.answerSql().statement());
    }

    /** Databricks' inline rows, pinned exactly (the orchestrator's ask, 2026-11-11: the lane measured the elements'
     *  form, not a frame's): VALUES of the elements' texts, which the select over it parses, a frame's place the
     *  second column -- {@code col1}, {@code col2}, as Spark names a VALUES' columns, aliased in the select. */
    @Test
    void databricksInlineRowsAreItsMeasuredShape() {
        SqlDialect databricks = dialect(DatabaseType.Databricks);
        List<String> cut = List.of("{\"t\":1}", "{\"t\":\"it's\"}");
        String frame = databricks.render(SqlSelect.starOf(databricks.inlineRows(SqlDialect.DataPayload.alone(
                source("data:application/json,[{\"t\":1},{\"t\":\"it's\"}]"), cut))));
        // 2026-11-12 (the total wall's B): the root's columns labeled (AnsiSqlRenderer#labeled)
        assertEquals("SELECT `t0`.`data` AS `data`, `t0`.`u_frame_ord__` AS `u_frame_ord__`\nFROM (SELECT parse_json(legend_rows.col1) AS `data`, legend_rows.col2 AS `u_frame_ord__`"
                + " FROM VALUES ('{\"t\":1}', 0L), ('{\"t\":\"it\\'s\"}', 1L) AS legend_rows(col1, col2)) AS `t0`", frame);
        List<OutputCol> elementOutputs = List.of(new OutputCol("data", SqlType.Scalar.JSON, true,
                OutputCol.Origin.DERIVED));
        String elements = databricks.render(SqlSelect.starOf(databricks.inlineRows(SqlDialect.DataPayload.alone(
                new SqlSource.SourceUrl("data:application/json,[{\"t\":1},{\"t\":\"it's\"}]", "t0", elementOutputs,
                        SqlSource.SourceUrl.Form.ELEMENTS), cut))));
        // 2026-11-12 (the total wall's B): the root's columns labeled (AnsiSqlRenderer#labeled)
        assertEquals("SELECT `t0`.`data` AS `data`\nFROM (SELECT parse_json(legend_rows.col1) AS `data` FROM VALUES ('{\"t\":1}'),"
                + " ('{\"t\":\"it\\'s\"}') AS legend_rows(col1)) AS `t0`", elements);
    }

    /** A large payload's PLAN is about the size of its data (the orchestrator's finding of 2026-11-11: Databricks'
     *  inline plan was 8.7 MB for a 1.09 MB payload, its typed tree carrying every VALUES row again): the payload
     *  once -- a bound one's text the statement's content, an inline one's rows the statement's text, the carried tree
     *  naming it by its digest -- on H2 (inline), Databricks above its parameter limit (inline) and DuckDB (bound). */
    @Test
    void aLargePayloadsPlanIsAboutTheSizeOfItsData() {
        StringBuilder payload = new StringBuilder("[");
        for (int i = 0; i < 12_000; i++) {
            payload.append(i == 0 ? "" : ",").append("{\"id\":").append(i).append(",\"name\":\"row number ")
                    .append(i).append("\",\"note\":\"a b c d e f g h i j k l m n o p q r s t u v w x y z\"}");
        }
        payload.append(']');
        for (DatabaseType type : List.of(DatabaseType.H2, DatabaseType.Databricks, DatabaseType.DuckDB)) {
            String model = """
                    Class s::Item { id: Integer[1]; }
                    ###Relational
                    Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
                    ###Mapping
                    Mapping s::M ( *s::Item: Relational { ~mainTable [s::DB] T id: [s::DB] T.ID } )
                    ###Connection
                    RelationalDatabaseConnection s::Conn { store: s::DB; type: %s; specification: LocalH2 { }; auth: DefaultH2; }
                    ###Runtime
                    Runtime s::RT { mappings: [s::M]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                    """.formatted(type.name());
            com.legend.executionplan.ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(
                    model), "|sourceUrl('data:application/json," + payload + "')->from(s::RT)").executionPlan("s::RT");
            String json = com.legend.executionplan.PlanJson.write(plan);
            // measured 2026-11-11: H2 1.42 MB, Databricks 1.36 MB, DuckDB 1.30 MB for a 1.18 MB payload
            assertEquals(1, json.split("row number 7777\\\\?\"", -1).length - 1, type + ": the payload once in the plan");
            assertTrue(json.length() < 2L * payload.length(), type + ": a plan of " + json.length()
                    + " characters for a payload of " + payload.length());
        }
    }

    /** The PAYLOAD'S ORDER where a statement asks none, pinned on every database (the orchestrator's finding,
     *  2026-11-11): a select of named columns reading an array's elements reads each element's place and is ordered by
     *  it -- the source's place written by each dialect's own split, or its inline rows' index; a sorted, grouped or
     *  aggregated select is as written, its source with no place. */
    @Test
    void anUnsortedSelectOfAPayloadIsOrderedByItsPlaceOnEveryDatabase() {
        List<OutputCol> element = List.of(new OutputCol("data", SqlType.Scalar.JSON, true, OutputCol.Origin.PHYSICAL));
        SqlSource.SourceUrl elements = new SqlSource.SourceUrl(PAYLOAD, "t0", element,
                SqlSource.SourceUrl.Form.ELEMENTS);
        com.legend.sql.SqlExpr data = com.legend.sql.SqlExpr.Column.of("t0", element.get(0));
        SqlSelect unsorted = SqlSelect.starOf(elements).withProjections(List.of(new SqlSelect.Projection(data, "d",
                new OutputCol("d", SqlType.Scalar.JSON, true))));
        SqlSelect sorted = unsorted.withOrderBy(List.of(SqlSelect.SortKey.asc(data)));
        for (DatabaseType type : java.util.stream.Stream.concat(BOUND.keySet().stream(), INLINE.stream()).toList()) {
            SqlDialect dialect = dialect(type);
            SqlQuery ordered = DataSources.read(unsorted, dialect);
            assertTrue(ordered instanceof SqlSelect s && s.orderBy().size() == 1, type + ": ordered: " + ordered);
            String sql = type == DatabaseType.MongoDB ? dialect.render(ordered)
                    : dialect.renderStatement(ordered).sql();
            assertTrue(sql.contains(SqlSource.SourceUrl.PLACE) || sql.contains(
                    SqlSource.SourceUrl.PLACE.toUpperCase(java.util.Locale.ROOT)), type + ": the place read: " + sql);
            assertTrue(type == DatabaseType.MongoDB ? sql.contains("$sort") : sql.contains("ORDER BY"),
                    type + ": ordered by it: " + sql);
            SqlQuery asWritten = DataSources.read(sorted, dialect);
            String sortedSql = type == DatabaseType.MongoDB ? dialect.render(asWritten)
                    : dialect.renderStatement(asWritten).sql();
            assertTrue(!sortedSql.contains(SqlSource.SourceUrl.PLACE) && !sortedSql.contains(
                    SqlSource.SourceUrl.PLACE.toUpperCase(java.util.Locale.ROOT)), type + ": the query's own sort: "
                    + sortedSql);
        }
    }

    /**
     * A LIMIT BELOW THE ROOT picks the payload's FIRST records, pinned on every database (the B checkpoint's tenets
     * audit, M1, 2026-11-11: only the root select was ordered, so {@code ->take(2)->filter(...)} -- an inner
     * {@code LIMIT 2} under a filter -- kept whichever two records the database read first): the limited select, under
     * a filter, a union's arm, a grouping or a common table expression, reads the place and is ordered by it; a filter
     * above it keeps the payload's order, reading the place its subquery exposes, which no star above it sees; a
     * DISTINCT whose LIMIT picks rows keeps the first distinct rows, grouped and ordered by each one's first place.
     */
    @Test
    void aLimitBelowTheRootPicksThePayloadsFirstRecordsOnEveryDatabase() {
        List<OutputCol> element = List.of(new OutputCol("data", SqlType.Scalar.JSON, true, OutputCol.Origin.PHYSICAL));
        com.legend.sql.SqlExpr data = com.legend.sql.SqlExpr.Column.of("t0", element.get(0));
        java.util.function.Function<String, SqlSelect> limited = alias -> SqlSelect.starOf(new SqlSource.SourceUrl(
                PAYLOAD, "t0", element, SqlSource.SourceUrl.Form.ELEMENTS)).withProjections(List.of(
                        new SqlSelect.Projection(data, "d", new OutputCol("d", SqlType.Scalar.JSON, true))))
                .withLimit(2L);
        SqlSelect filtered = SqlSelect.starOf(new SqlSource.Subselect(limited.apply("t0"), "t1", null))
                .withWhere(new com.legend.sql.SqlExpr.BoolLit(true));
        SqlQuery union = new com.legend.sql.SqlUnion(List.of(
                SqlSelect.starOf(new SqlSource.Subselect(limited.apply("t0"), "t1", null)),
                SqlSelect.starOf(new SqlSource.Subselect(limited.apply("t0").withOffset(1L), "t2", null))), true,
                limited.apply("t0").outputs());
        com.legend.sql.SqlExpr d1 = com.legend.sql.SqlExpr.Column.of("t1", limited.apply("t0").outputs().get(0));
        SqlSelect grouped = SqlSelect.starOf(new SqlSource.Subselect(limited.apply("t0"), "t1", null))
                .withProjections(List.of(new SqlSelect.Projection(d1, "d", limited.apply("t0").outputs().get(0))))
                .withGroupBy(List.of(d1));
        SqlQuery cte = new com.legend.sql.SqlWith(List.of(new com.legend.sql.SqlWith.Cte("c", limited.apply("t0"))),
                SqlSelect.starOf(new SqlSource.Cte("c", "c", limited.apply("t0").outputs())));
        SqlSelect distinctFirst = SqlSelect.starOf(new SqlSource.SourceUrl(PAYLOAD, "t0", element,
                SqlSource.SourceUrl.Form.ELEMENTS)).withProjections(List.of(new SqlSelect.Projection(data, "d",
                        new OutputCol("d", SqlType.Scalar.JSON, true)))).withDistinct().withLimit(2L);
        // the planner's MIR: every limited select ordered by its records' place
        SqlSelect inner = (SqlSelect) ((SqlSource.Subselect) ((SqlSelect) DataSources.inPayloadOrder(filtered))
                .from()).inner();
        assertTrue(placeOrdered(inner, false), "the limited select under a filter: " + inner);
        SqlSelect outer = (SqlSelect) DataSources.inPayloadOrder(filtered);
        assertTrue(placeOrdered(outer, false), "the filter above it, in the payload's order: " + outer);
        assertEquals(List.of("d"), outer.outputs().stream().map(OutputCol::name).toList(), "no place in its rows");
        for (SqlQuery arm : ((com.legend.sql.SqlUnion) DataSources.inPayloadOrder(union)).branches()) {
            assertTrue(placeOrdered((SqlSelect) ((SqlSource.Subselect) ((SqlSelect) arm).from()).inner(), false),
                    "a union's arm: " + arm);
            assertEquals(List.of("d"), arm.outputs().stream().map(OutputCol::name).toList(), "no place in its rows");
        }
        assertTrue(placeOrdered((SqlSelect) ((SqlSource.Subselect) ((SqlSelect) DataSources.inPayloadOrder(grouped))
                .from()).inner(), false), "the limited select under a grouping");
        assertTrue(((SqlSelect) DataSources.inPayloadOrder(grouped)).orderBy().isEmpty(),
                "a grouping without a LIMIT asks no order");
        assertTrue(placeOrdered((SqlSelect) ((com.legend.sql.SqlWith) DataSources.inPayloadOrder(cte)).ctes().get(0)
                .query(), false), "a common table expression's limited select");
        SqlSelect firstDistinct = (SqlSelect) DataSources.inPayloadOrder(distinctFirst);
        assertTrue(placeOrdered(firstDistinct, true) && !firstDistinct.distinct()
                && firstDistinct.groupBy().size() == 1, "the first distinct rows: " + firstDistinct);
        // each database's statement: the limited select ordered by the place its dialect reads
        for (DatabaseType type : java.util.stream.Stream.concat(BOUND.keySet().stream(), INLINE.stream()).toList()) {
            SqlDialect dialect = dialect(type);
            // MongoDB has no shape for a WITH (refused by name, DialectCapability)
            for (SqlQuery q : type == DatabaseType.MongoDB ? List.of(filtered, union, grouped, distinctFirst)
                    : List.of(filtered, union, grouped, cte, distinctFirst)) {
                SqlQuery read = DataSources.read(q, dialect);
                String sql = type == DatabaseType.MongoDB ? dialect.render(read) : dialect.renderStatement(read).sql();
                String lower = sql.toLowerCase(java.util.Locale.ROOT);
                assertTrue(lower.contains(SqlSource.SourceUrl.PLACE), type + ": the place read: " + sql);
                assertTrue(type == DatabaseType.MongoDB ? sql.contains("$sort") && sql.contains("$limit")
                        : lower.contains("order by") && (lower.contains("limit") || lower.contains("top")
                                || lower.contains("fetch")), type + ": ordered by it: " + sql);
            }
        }
    }

    /** Whether {@code s} is ordered by its records' place alone (a group's first, {@code MIN}, where {@code grouped}). */
    private static boolean placeOrdered(SqlSelect s, boolean grouped) {
        if (s.orderBy().size() != 1 || !s.orderBy().get(0).ascending()) {
            return false;
        }
        com.legend.sql.SqlExpr e = s.orderBy().get(0).expr();
        if (grouped) {
            if (!(e instanceof com.legend.sql.SqlAgg.Reducer r) || r.fn() != com.legend.sql.SqlAgg.Fn.MIN) {
                return false;
            }
            e = r.args().get(0);
        }
        return e instanceof com.legend.sql.SqlExpr.Column c && c.name().equals(SqlSource.SourceUrl.PLACE);
    }

    /** Trino's statement limit is its server's setting ({@code query.max-length}), the database's to enforce (the B
     *  checkpoint's correctness audit, C5, 2026-11-11: until then a payload whose literal passed 1,000,000 characters
     *  was refused at planning, wrongly on a server configured larger): such a payload is bound as any other, and
     *  Trino's own refusal names it at the seam (DataSourceServersTest). */
    @Test
    void trinoBindsAPayloadPastItsDefaultStatementLimit() {
        SqlDialect trino = dialect(DatabaseType.Trino);
        String element = "{\"t\":\"" + "x".repeat(1000) + "\"}";
        String url = "data:application/json,[" + String.join(",", java.util.Collections.nCopies(1000, element)) + "]";
        RenderedStatement bound = trino.renderStatement(DataSources.read(frame(url), trino));
        assertEquals(1, bound.binds().size(), "bound: " + bound.sql());
    }

    /** A payload that is no array of records is refused by name before any dialect is asked, on every database. */
    @Test
    void aPayloadThatIsNoRecordsIsRefusedByNameEverywhere() {
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite,
                DatabaseType.Postgres, DatabaseType.Trino)) {
            var refused = assertThrows(com.legend.error.NotImplementedException.class,
                    () -> DataSources.read(frame("data:application/json,[{\"t\":1}, 7]"), dialect(type)),
                    type + ": a record that is no object");
            assertTrue(refused.getMessage().contains("payload"), type + ": " + refused.getMessage());
        }
    }
}
