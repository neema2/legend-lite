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
        // the scan renames each column under its alias; the filter guards a column that may be null
        assertTrue(b.contains("__region = region"), b);
        assertTrue(b.contains("{\"unstructuredFilter\": {\"sourceId\": {\"batchOffset\": 0}, \"filters\": [\"(!isNull("),
                b);
        assertTrue(b.contains(" == `EMEA`)"), b);
        // pnl may be null: its null flag sorts it first, the platform's order for a descending key
        assertTrue(b.contains("__n0 = isNull(pnl)"), b);
        assertTrue(b.contains("{\"columnName\": \"__n0\", \"direction\": \"DESCENDING\"}, {\"columnName\": \"pnl\","
                + " \"direction\": \"DESCENDING\"}"), b);
        assertTrue(b.contains("{\"head\": {"), b);
        assertTrue(b.contains("\"numRows\": \"10\""), b);
        assertTrue(b.endsWith("{\"dropColumns\": {\"sourceId\": {\"batchOffset\": 5}, \"columnNames\": [\"__n0\"]}}]}"),
                b);
    }

    @Test
    void aMembershipOfAColumnThatMayBeNullIsTrueOnlyWhenItMatches() {
        // the lowering's null-safe membership, COALESCE(x IN (...), false), is the membership's TRUE
        String b = batch("#>{risk::Ref.trades}#->filter(x | $x.region->in(['EMEA', 'APAC']))");
        assertTrue(b.contains("\"filters\": [\"(!isNull(t0__region) && (t0__region == `EMEA` || t0__region =="
                + " `APAC`))\"]"), b);
    }

    @Test
    void aStarFrameKeepsEverySourceColumnUnderItsOwnName() {
        String b = batch("#>{risk::Ref.trades}#->filter(x | $x.qty > 5)");
        assertTrue(b.contains("\"columnSpecs\": [\"id = t0__id\", \"region = t0__region\""), b);
    }

    @Test
    void aGroupIsOneAggregateOverItsComputedKeysAndArguments() {
        String b = batch("#>{risk::Ref.trades}#->groupBy(~[desk], ~[a: x | $x.pnl : y | $y->average(),"
                + " n: x | $x.instrument : y | $y->distinct()->count(), mx: x | $x.pnl : y | $y->max()])");
        assertTrue(b.contains("{\"updateView\": {"), b);
        assertTrue(b.contains("\"__g0 = "), b);
        assertTrue(b.contains("{\"aggregate\": {"), b);
        assertTrue(b.contains("\"groupByColumns\": [\"__g0\"]"), b);
        assertTrue(b.contains("{\"spec\": {\"avg\": {}}"), b);
        assertTrue(b.contains("{\"spec\": {\"countDistinct\": {\"countNulls\": false}}"), b);
        assertTrue(b.contains("{\"spec\": {\"max\": {}}"), b);
        // the outputs read the group's columns back
        assertTrue(b.contains("\"desk = __g0\""), b);
    }

    @Test
    void aPivotIsAGroupOfConditionalAggregates() {
        String b = batch("#>{risk::Ref.trades}#->groupBy(~[region, country], ~['2023__|__total': x | if($x.year =="
                + " 2023, | $x.notional, | []) : y | $y->sum(), total: x | $x.notional : y | $y->sum()])"
                + "->sort([~region->ascending(), ~country->ascending()])");
        // the condition is a CASE's: taken when TRUE, else the typed null
        assertTrue(b.contains(" ? "), b);
        assertTrue(b.contains("NULL_DOUBLE"), b);
        // a name Deephaven cannot hold is the output's place; the plan reads its columns by position
        assertTrue(b.contains("\"__c2 = __r0\""), b);
        assertTrue(b.contains("\"total = __r1\""), b);
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
        assertTrue(b.contains("{\"unstructuredFilter\": {\"sourceId\": {\"batchOffset\": 1}, \"filters\": [\"!isNull("),
                b);
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
