// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlQuery;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Deephaven target's text (docs/DEEPHAVEN_PLAN_2026_10_22.md L2): a query the one compiler lowers renders a batch
 * of Deephaven's table operations, each reading earlier ones by their place; what has no shape yet is refused by name.
 * The query is lowered for a DuckDB runtime (lowering knows no target) and written by the Deephaven dialect; the
 * planner reaches the same target for a Deephaven runtime (DH21: the platform plans, the browser tab sends).
 */
class DeephavenDialectTest {

    private static final String MODEL = """
            ###Relational
            Database risk::Ref
            (
              Table trades (id INTEGER PRIMARY KEY, region VARCHAR(16), country VARCHAR(16), desk VARCHAR(16) NOT NULL,
                            year INTEGER, qty BIGINT, notional DOUBLE, pnl DOUBLE, instrument VARCHAR(16))
              Table instrument (iid VARCHAR(16) PRIMARY KEY, sector VARCHAR(16), ccy VARCHAR(3) NOT NULL)
            )
            ###Connection
            RelationalDatabaseConnection risk::Conn
            {
              store: risk::Ref;
              type: DuckDB;
              specification: DuckDB { };
              auth: Test;
            }
            ###Runtime
            Runtime risk::RT
            {
              mappings: [];
              connections: [ risk::Ref: [ c: risk::Conn ] ];
            }
            """;

    private static final com.legend.compiler.element.ModelContext CTX = Compiler.compileModel(MODEL);

    private static SqlQuery ir(String query) {
        return Compiler.query(CTX, query).lower("risk::RT", false).plan();
    }

    private static String batch(String query) {
        return new Deephaven().render(ir(query));
    }

    private static String ticket(String name) {
        return Base64.getEncoder().encodeToString(("s/" + name).getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void aQueryOnADeephavenRuntimeIsPlannedAsDeephavensTableOperations() {
        String deephaven = MODEL.replace("type: DuckDB;\n  specification: DuckDB { };\n  auth: Test;",
                "type: Deephaven;\n  specification: Deephaven { host: 'localhost'; port: 8443; useTls: true; };\n"
                        + "  auth: PreSharedKey { keyVaultReference: 'deephaven.psk'; };");
        assertTrue(deephaven.contains("type: Deephaven;"), deephaven);
        String query = "#>{risk::Ref.trades}#->groupBy(~[desk], ~[total:x|$x.notional:y|$y->sum()])->sort(~desk->ascending())";
        String planned = Compiler.query(Compiler.compileModel(deephaven), query).plan("risk::RT").sql();
        assertEquals(batch(query), planned);
    }

    @Test
    void aFilteredSortedLimitedSelectIsOneOperationEach() {
        String b = batch("#>{risk::Ref.trades}#->filter(x | $x.region == 'EMEA')->select(~[desk, pnl])"
                + "->sort(~pnl->descending())->limit(10)");
        assertTrue(b.startsWith("{\"ops\": [{\"view\": {\"sourceId\": {\"ticket\": {\"ticket\": \"" + ticket("trades")
                + "\"}}, \"columnSpecs\": [\""), b);
        // the scan renames each column under its alias; the filter is a match the server reads with no compiling (a
        // match keeps no null: no guard)
        assertTrue(b.contains("__region = region"), b);
        assertTrue(b.contains("{\"unstructuredFilter\": {\"sourceId\": {\"batchOffset\": 0}, \"filters\":"
                + " [\"t0__region == `EMEA`\"]}}"), b);
        // pnl may be null: its null flag sorts it first, the platform's order for a descending key; the flag reads the
        // key by its place (a column renamed), so its formula's text is the same for every key of the type: compiled
        // once on a server, not once a column (P1)
        assertTrue(b.contains("\"__k0 = pnl\", \"__n0 = isNull(__k0)\""), b);
        assertTrue(b.contains("{\"columnName\": \"__n0\", \"direction\": \"DESCENDING\"}, {\"columnName\": \"pnl\","
                + " \"direction\": \"DESCENDING\"}"), b);
        assertTrue(b.contains("{\"head\": {"), b);
        assertTrue(b.contains("\"numRows\": \"10\""), b);
        assertTrue(b.endsWith("{\"dropColumns\": {\"sourceId\": {\"batchOffset\": 5}, \"columnNames\": [\"__k0\","
                + " \"__n0\"]}}]}"), b);
    }

    @Test
    void aMembershipOfAColumnThatMayBeNullIsTrueOnlyWhenItMatches() {
        // the lowering's null-safe membership, COALESCE(x IN (...), false), is the membership's TRUE: a match of the
        // list, which keeps no null
        String b = batch("#>{risk::Ref.trades}#->filter(x | $x.region->in(['EMEA', 'APAC']))");
        assertTrue(b.contains("\"filters\": [\"t0__region in `EMEA`, `APAC`\"]"), b);
        // its negation keeps the nulls (a null is in no list): the inverted match, unguarded
        String not = batch("#>{risk::Ref.trades}#->filter(x | !$x.region->in(['EMEA', 'APAC']))");
        assertTrue(not.contains("\"filters\": [\"t0__region not in `EMEA`, `APAC`\"]"), not);
    }

    /** A filter Deephaven reads without compiling (DH25): its conjunction split into the server's list, and each
     *  comparison of a column with a literal bare -- a match or a range filter, a literal's own form (no {@code L},
     *  a plain decimal) -- its null guard a formula of its own that names only its column. What has no bare form is a
     *  formula, as before. */
    @Test
    void aFilterIsWhatTheServerReadsWithoutCompiling() {
        filters("($x.qty > 1000000000) && ($x.desk == 'FX')",
                "\"t0__qty != null\", \"t0__qty > 1000000000\", \"t0__desk == `FX`\"");
        // a literal first is mirrored; a negative float is a plain decimal
        filters("-3.5 < $x.pnl", "\"t0__pnl != null\", \"t0__pnl > -3.5\"");
        filters("$x.pnl <= 0.0", "\"t0__pnl != null\", \"t0__pnl <= 0.0\"");
        // Pure's NOT keeps a null (a null is not greater than 0): "null or at most 0", a formula
        filters("!($x.pnl > 0.0)", "\"(isNull(t0__pnl) || (!isNull(t0__pnl) && t0__pnl <= 0.0))\"");
        // Pure's != is null-safe: a null differs from 'EMEA', and the inverted match keeps it
        filters("$x.region != 'EMEA'", "\"t0__region != `EMEA`\"");
        // a value with a comma (the server splits an `in` list at its commas) and a disjunction stay formulas
        filters("$x.country->in(['UK', 'J,P'])",
                "\"(!isNull(t0__country) && (t0__country == `UK` || t0__country == `J,P`))\"");
        String or = batch("#>{risk::Ref.trades}#->filter(x | ($x.year == 2023) || ($x.desk == 'FX'))");
        assertTrue(or.contains("\"filters\": [\"(") && or.contains(" || "), or);
    }

    /** {@code condition}'s filter list is {@code expected}. */
    private static void filters(String condition, String expected) {
        String b = batch("#>{risk::Ref.trades}#->filter(x | " + condition + ")");
        assertTrue(b.contains("\"filters\": [" + expected + "]"), condition + ": " + b);
    }

    @Test
    void aStarFrameKeepsEverySourceColumnUnderItsOwnName() {
        String b = batch("#>{risk::Ref.trades}#->filter(x | $x.qty > 5)");
        assertTrue(b.contains("\"columnSpecs\": [\"id = t0__id\", \"region = t0__region\""), b);
    }

    @Test
    void aGroupIsOneAggregateOverItsComputedKeysAndArguments() {
        // keys and arguments that are columns as they are, read by their own names: no operation before the group
        // (G1), and the average its column, Deephaven's own average a fraction of any number
        String b = batch("#>{risk::Ref.trades}#->groupBy(~[desk], ~[a: x | $x.pnl : y | $y->average(),"
                + " n: x | $x.instrument : y | $y->distinct()->count(), mx: x | $x.pnl : y | $y->max()])");
        assertTrue(!b.contains("{\"updateView\": {") && !b.contains("1.0 *"), b);
        assertTrue(b.contains("{\"aggregate\": {\"sourceId\": {\"batchOffset\": 0}, \"groupByColumns\": [\"t0__desk\"]"), b);
        assertTrue(b.contains("{\"columns\": {\"spec\": {\"avg\": {}}, \"matchPairs\": [\"__r0=t0__pnl\"]}}"), b);
        assertTrue(b.contains("{\"spec\": {\"countDistinct\": {\"countNulls\": false}}, \"matchPairs\": [\"__r1=t0__instrument\"]"), b);
        assertTrue(b.contains("{\"spec\": {\"max\": {}}"), b);
        // the outputs read the group's columns back
        assertTrue(b.contains("\"desk = t0__desk\""), b);
        // a computed key and a computed argument, each into a column of its own first
        String computed = batch("#>{risk::Ref.trades}#->extend(~[twice:x|$x.id * 2])->groupBy(~[twice], ~[s: x | $x.id + 3 :"
                + " y | $y->sum()])");
        assertTrue(computed.contains("{\"updateView\": {") && computed.contains("\"__a0 = "), computed);
    }

    /** A PIVOT'S CELLS, written with no formula (docs/DEEPHAVEN_PLAN_2026_10_22.md P1): a sum is one aggregation over the
     *  keys and the pivot's column, the cell's rows taken by a match filter and joined back by the keys; the measure the
     *  pivot leaves alone is the group's own aggregate. Nothing the server compiles: no conditional, no typed null. */
    @Test
    void aPivotsCellsAreWrittenWithNoFormula() {
        String b = batch("#>{risk::Ref.trades}#->groupBy(~[region, country], ~['2023__|__total': x | if($x.year =="
                + " 2023, | $x.notional, | []) : y | $y->sum(), total: x | $x.notional : y | $y->sum()])"
                + "->sort([~region->ascending(), ~country->ascending()])");
        assertTrue(!b.contains(" ? ") && !b.contains("NULL_DOUBLE") && !b.contains("__a0"), b);
        // the group's own aggregate: the total
        assertTrue(b.contains("\"groupByColumns\": [\"t0__region\", \"t0__country\"], \"aggregations\": [{\"columns\":"
                + " {\"spec\": {\"sum\": {}}, \"matchPairs\": [\"__r1=t0__notional\"]}}]"), b);
        // the cells' aggregation, over the keys and the pivot's column
        assertTrue(b.contains("\"groupByColumns\": [\"t0__region\", \"t0__country\", \"t0__year\"], \"aggregations\":"
                + " [{\"columns\": {\"spec\": {\"sum\": {}}, \"matchPairs\": [\"__l0=t0__notional\"]}}]"), b);
        // the cell's rows, by a match filter, joined back by the keys
        assertTrue(b.contains("\"filters\": [\"t0__year == 2023\"]"), b);
        assertTrue(b.contains("\"columnsToMatch\": [\"t0__region\", \"t0__country\"], \"columnsToAdd\": [\"__r0=__l0\"]"), b);
        // a name Deephaven cannot hold is the output's place; the plan reads its columns by position
        assertTrue(b.contains("\"__c2 = __r0\""), b);
        assertTrue(b.contains("\"total = __r1\""), b);
    }

    /** A cell's count is the server's count of the rows its match filters take, in the group's aggregate -- 0 where no
     *  row is one of them, as SQL's count -- and a cell over two pivot columns takes both; cells of the same rows are
     *  one join. */
    @Test
    void aPivotsCountsAreCountedWhereAndItsCellsJoinedOnceForTheirRows() {
        String b = batch("#>{risk::Ref.trades}#->groupBy(~[desk], ~["
                + "'EMEA__|__2023__|__n': x | if($x.region == 'EMEA' && $x.year == 2023, | 1, | []) : y | $y->count(),"
                + "'EMEA__|__2023__|__qty': x | if($x.region == 'EMEA' && $x.year == 2023, | $x.qty, | []) : y | $y->max(),"
                + "'EMEA__|__2023__|__pnl': x | if($x.region == 'EMEA' && $x.year == 2023, | $x.pnl, | []) : y | $y->min()])");
        assertTrue(b.contains("{\"countWhere\": {\"columnName\": \"__r0\", \"filters\": [\"t0__region == `EMEA`\","
                + " \"t0__year == 2023\"]}}"), b);
        assertTrue(b.contains("\"groupByColumns\": [\"t0__desk\", \"t0__region\", \"t0__year\"]"), b);
        assertEquals(1, b.split("naturalJoin", -1).length - 1, b);
        assertTrue(b.contains("\"columnsToAdd\": [\"__r1=__l0\", \"__r2=__l1\"]"), b);
        assertTrue(!b.contains(" ? "), b);
        // a cell whose condition guards its pivot column's nulls itself (the lowering's, at times) is the same cell
        String guarded = batch("#>{risk::Ref.trades}#->groupBy(~[desk], ~[a: x | if($x.region->isNotEmpty() &&"
                + " $x.region == 'EMEA', | $x.pnl, | []) : y | $y->average()])");
        assertTrue(!guarded.contains(" ? ") && guarded.contains("\"filters\": [\"t0__region == `EMEA`\"]"), guarded);
    }

    /** What is no cell is written as before: a condition other than columns equal to literals, or cells over
     *  different pivot columns, a conditional formula and its aggregate. */
    @Test
    void aConditionalAggregateNoCellIsAFormula() {
        String ranged = batch("#>{risk::Ref.trades}#->groupBy(~[desk], ~[big: x | if($x.notional > 100.0, | $x.pnl,"
                + " | []) : y | $y->sum()])");
        assertTrue(ranged.contains(" ? ") && ranged.contains("NULL_DOUBLE"), ranged);
        String mixed = batch("#>{risk::Ref.trades}#->groupBy(~[desk], ~["
                + "a: x | if($x.region == 'EMEA', | $x.pnl, | []) : y | $y->sum(),"
                + "b: x | if($x.year == 2023, | $x.pnl, | []) : y | $y->sum()])");
        assertTrue(mixed.contains(" ? ") && !mixed.contains("naturalJoin"), mixed);
    }

    /** A group's unique value (DataCube's aggregate of a column a level does not group by), lowered as COUNT(DISTINCT)
     *  and MAX and a CASE reading them, is Deephaven's one {@code unique} aggregation: no count, no max, no formula. A
     *  unique value with a default of its own is written as lowered (a group of no values answers the default). */
    @Test
    void aUniqueValueIsTheServersOneAggregation() {
        String b = batch("#>{risk::Ref.trades}#->groupBy(~[region], ~[d: x | $x.desk : y | $y->uniqueValueOnly()])");
        assertTrue(b.contains("{\"columns\": {\"spec\": {\"unique\": {\"nonUniqueSentinel\": {\"nullValue\": \"NULL_VALUE\"}}},"
                + " \"matchPairs\": [\"__r0=t0__desk\"]}}"), b);
        assertTrue(!b.contains("countDistinct") && !b.contains("== 1L"), b);
        assertTrue(b.contains("\"d = __r0\""), b);
        String withDefault = batch("#>{risk::Ref.trades}#->groupBy(~[region], ~[d: x | $x.desk : y |"
                + " $y->uniqueValueOnly('many')])");
        assertTrue(withDefault.contains("countDistinct") && !withDefault.contains("\"unique\""), withDefault);
    }

    @Test
    void aGrandTotalKeepsItsOneGroupOverNoRows() {
        String b = batch("#>{risk::Ref.trades}#->groupBy(~[], ~[total: x | $x.notional : y | $y->sum()])");
        assertTrue(b.contains("\"groupByColumns\": []"), b);
        assertTrue(b.contains("\"preserveEmpty\": true"), b);
    }

    @Test
    void aLeftJoinIsANaturalJoinThatRefusesADuplicateKey() {
        String b = batch("#>{risk::Ref.trades}#->join(#>{risk::Ref.instrument}#,"
                + " meta::pure::functions::relation::JoinKind.LEFT, {a, b | $a.instrument == $b.iid})"
                + "->select(~[desk, sector])");
        assertTrue(b.contains("{\"naturalJoin\": {"), b);
        assertTrue(b.contains("\"joinType\": \"ERROR_ON_DUPLICATE\""), b);
        assertTrue(b.contains("\"columnsToMatch\": [\""), b);
    }

    @Test
    void anInnerJoinFiltersARightKeyThatMayBeNull() {
        String b = batch("#>{risk::Ref.instrument}#->join(#>{risk::Ref.trades}#,"
                + " meta::pure::functions::relation::JoinKind.INNER, {a, b | $a.iid == $b.instrument})"
                + "->select(~[sector, desk])");
        assertTrue(b.contains("{\"crossJoin\": {"), b);
        assertTrue(b.contains("{\"unstructuredFilter\": {\"sourceId\": {\"batchOffset\": 1}, \"filters\": [\"")
                && b.contains(" != null\""), b);
    }

    @Test
    void whatHasNoShapeIsRefusedByName() {
        DialectCapability window = assertThrows(DialectCapability.class, () -> batch("#>{risk::Ref.trades}#"
                + "->extend(over(~desk, ~pnl->descending()), ~[r:{p, w, r | $p->rank($w, $r)}])"));
        assertTrue(window.getMessage().contains("reached Deephaven"), window.getMessage());
        DialectCapability string = assertThrows(DialectCapability.class, () -> batch("#>{risk::Ref.trades}#"
                + "->filter(x | $x.region == 'a`b')"));
        assertTrue(string.getMessage().contains("a backtick"), string.getMessage());
    }

    @Test
    void aTableIsCreatedAsAnInputTable() {
        String keyed = new Deephaven().render(new SqlDdl.CreateTable(null, "instrument", com.legend.sql.NameQuoting.BARE,
                List.of(new SqlDdl.Column("id", false, new SqlDdl.ColumnType.Sized("VARCHAR", 16), true, true),
                        new SqlDdl.Column("multiplier", false, new SqlDdl.ColumnType.Plain(
                                SqlDdl.ColumnType.Kind.DOUBLE), false, false))));
        assertEquals("{\"createTable\": \"instrument\", \"batch\": {\"ops\": [{\"emptyTable\": {\"size\": \"0\"}},"
                + " {\"update\": {\"sourceId\": {\"batchOffset\": 0}, \"columnSpecs\": [\"id = (String) null\","
                + " \"multiplier = NULL_DOUBLE\"]}}, {\"createInputTable\": {\"sourceTableId\": {\"batchOffset\": 1},"
                + " \"kind\": {\"inMemoryKeyBacked\": {\"keyColumns\": [\"id\"]}}}}]}}", keyed);
        DialectCapability bytes = assertThrows(DialectCapability.class, () -> new Deephaven().render(
                new SqlDdl.CreateTable(null, "blob", com.legend.sql.NameQuoting.BARE, List.of(new SqlDdl.Column("b",
                        false, new SqlDdl.ColumnType.Sized("VARBINARY", 16), false, false)))));
        assertTrue(bytes.getMessage().contains("column blob.b is VARBINARY(16), which no Deephaven column holds"),
                bytes.getMessage());
        DialectCapability schema = assertThrows(DialectCapability.class, () -> new Deephaven().render(
                new SqlDdl.CreateTable("ref", "t", com.legend.sql.NameQuoting.BARE, List.of())));
        assertTrue(schema.getMessage().contains("schema ref"), schema.getMessage());
    }
}
