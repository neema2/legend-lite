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
              Table fills (fid INTEGER PRIMARY KEY, tm TIMESTAMP NOT NULL, ts TIMESTAMP, d DATE, px DOUBLE)
              Table prints (pid INTEGER PRIMARY KEY, sym VARCHAR(8), tm TIMESTAMP NOT NULL, px DOUBLE, sz DOUBLE NOT NULL)
              Table books (bid INTEGER PRIMARY KEY, lim DECIMAL(18,2), used DECIMAL(18,2) NOT NULL)
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
        filters("#>{risk::Ref.trades}#", condition, expected);
    }

    /** {@code condition}'s filter list over {@code source} is {@code expected}. */
    private static void filters(String source, String condition, String expected) {
        String b = batch(source + "->filter(x | " + condition + ")");
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

    // ==================================================================
    // A screen's scalars (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md §2, the stress corpus's TF_TradeScalars)
    // ==================================================================

    /** One formula of {@code source->extend(~r:x|lambda)}: the plan holds it as {@code r = formula}. */
    private static void formulaOver(String source, String key, String lambda, String expected) {
        String b = batch(source + "->extend(~r:x|" + lambda + ")->select(~[" + key + ", r])");
        assertTrue(b.contains("\"r = " + expected.replace("\"", "\\\"") + "\""), b);
    }

    @Test
    void aRoundIsHalfEvenAsJavasRintIs() {
        // Pure's round is half-even (SqlFn's contract): rint, to an Integer the whole number it lands on
        formulaOver(P, "pid", "($x.sz / 4.0)->round()", "((long) rint((t0__sz / 4.0)))");
        // to a scale: the value scaled by the literal power of ten, rint, scaled back (ROUND_EVEN(x, 2)'s two steps)
        formulaOver(P, "pid", "($x.sz / 3.0)->round(2)", "(rint((t0__sz / 3.0) * 100.0) / 100.0)");
    }

    @Test
    void aModIsTheRemainderWithTheDivisorsSign() {
        // Pure's mod is floored (the divisor's sign), its rem truncated (the dividend's, Java's %)
        formulaOver(P, "pid", "$x.pid->mod(3)", "(((t0__pid % 3L) + 3L) % 3L)");
        formulaOver(P, "pid", "$x.pid->rem(3)", "(t0__pid % 3L)");
    }

    @Test
    void aSubstringAndAPlaceAreSqlsReadThroughStringsOwnMethods() {
        String instrument = "#>{risk::Ref.instrument}#";
        // the platform's SUBSTRING (one-based start, a length), each index held to the string's length
        formulaOver(instrument, "iid", "$x.ccy->substring(1, 3)", "t0__ccy.substring((t0__ccy.length() < 0 ?"
                + " t0__ccy.length() : 0), (t0__ccy.length() < 3 ? t0__ccy.length() : 3))");
        // a start below 1 held at 1, as H2 holds it and DuckDB's SubstringClamp does
        formulaOver(instrument, "iid", "$x.ccy->substring(0, 2)", "t0__ccy.substring((t0__ccy.length() < 0 ?"
                + " t0__ccy.length() : 0), (t0__ccy.length() < 2 ? t0__ccy.length() : 2))");
        // a string that may be null stays null
        formulaOver(instrument, "iid", "$x.sector->toOne()->substring(1)",
                "(isNull(t0__sector) ? (String) null : t0__sector.substring((t0__sector.length() < 0 ?"
                        + " t0__sector.length() : 0)))");
        // contains is a place past 0
        formulaOver(instrument, "iid", "$x.ccy->contains('U')", "((t0__ccy.indexOf(`U`) + 1) > 0L)");
    }

    @Test
    void aWholeNumberAsTextIsItsDigitsAndADecimalAsAFloatItsOwnDoubleValue() {
        formulaOver(P, "pid", "$x.pid->toString()", "(`` + t0__pid)");
        // a DECIMAL column is a BigDecimal: its doubleValue (DH33 permits BigDecimal's methods), a null kept null
        String books = "#>{risk::Ref.books}#";
        formulaOver(books, "bid", "$x.used->toFloat()", "(t0__used.doubleValue())");
        formulaOver(books, "bid", "$x.lim->toOne()->toFloat()",
                "(isNull(t0__lim) ? NULL_DOUBLE : t0__lim.doubleValue())");
        // a float's text is Java's where SQL's differs (1.0E7 against 10000000.0): refused by name
        DialectCapability text = assertThrows(DialectCapability.class,
                () -> batch(P + "->extend(~r:x|$x.sz->toString())->select(~[pid, r])"));
        assertTrue(text.getMessage().contains("reached Deephaven"), text.getMessage());
    }

    @Test
    void whatHasNoShapeIsRefusedByName() {
        DialectCapability window = assertThrows(DialectCapability.class, () -> batch("#>{risk::Ref.trades}#"
                + "->extend(over(~desk, ~pnl->descending()), ~[r:{p, w, r | $p->nth($w, $r, 2).id}])"));
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

    // ==================================================================
    // Dates and times (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K1, DH29, DH30)
    // ==================================================================

    private static final String F = "#>{risk::Ref.fills}#";
    private static final String UTC = "timeZone(`UTC`)";

    /** One formula of {@code F->extend(~r:x|lambda)}: the plan holds it as {@code r = formula}. */
    private static void formula(String lambda, String expected) {
        String b = batch(F + "->extend(~r:x|" + lambda + ")->select(~[fid, r])");
        assertTrue(b.contains("\"r = " + expected.replace("\"", "\\\"") + "\""), b);
    }

    @Test
    void aDateTimeLiteralIsTheInstantItsUtcWallClockNames() {
        // Pure's DateTime is UTC wall-clock (DH29): the server parses the ISO instant
        String b = batch(F + "->extend(~r:x|$x.tm > %2026-10-24T09:30:00.000+0000)->select(~[fid, r])");
        assertTrue(b.contains("(t0__tm > parseInstant(`2026-10-24T09:30:00.000Z`))"), b);
        // in a filter, compared with a column, the instant in quotes: a range filter the server reads without
        // compiling (DH25), a nullable column's nulls filtered off first
        filters(F, "$x.tm >= %2026-10-24T09:30:15.250+0000", "\"t0__tm >= '2026-10-24T09:30:15.250Z'\"");
        filters(F, "$x.ts < %2026-06-01T00:00:00.000+0000", "\"t0__ts != null\", \"t0__ts < '2026-06-01T00:00:00.000Z'\"");
        filters(F, "$x.d->toOne() == %2026-10-24", "\"t0__d == '2026-10-24'\"");
        assertEquals("2026-10-24T09:30:00Z", Deephaven.isoInstant("2026-10-24 09:30"));
        assertEquals("1970-01-01T00:00:00Z", Deephaven.isoInstant("1970-01-01 00:00:00"));
        assertThrows(DialectCapability.class, () -> Deephaven.isoInstant("2026-10-24T09:30:00+0100"));
    }

    @Test
    void aTimeBucketIsTheServersLowerBinFromTheEpoch() {
        formula("$x.tm->timeBucket(5, DurationUnit.MINUTES)", "lowerBin(t0__tm, (5L * 60000000000L))");
        formula("$x.tm->timeBucket(1, DurationUnit.HOURS)", "lowerBin(t0__tm, (1L * 3600000000000L))");
        // a week's bucket from its Monday, 1969-12-29: three days before the epoch
        formula("$x.tm->timeBucket(2, DurationUnit.WEEKS)",
                "lowerBin(t0__tm, (2L * 604800000000000L), -259200000000000L)");
        // a date's bucket: its UTC midnight binned, the date of the bin (the lowering casts it back)
        formula("$x.d->toOne()->timeBucket(1, DurationUnit.DAYS)", "toLocalDate(lowerBin(toInstant(atMidnight(t0__d, "
                + UTC + ")), (1L * 86400000000000L)), " + UTC + ")");
        // a calendar unit is none of the server's bins
        DialectCapability months = assertThrows(DialectCapability.class,
                () -> batch(F + "->extend(~r:x|$x.tm->timeBucket(1, DurationUnit.MONTHS))"));
        assertTrue(months.getMessage().contains("DH29"), months.getMessage());
    }

    @Test
    void aDatePartIsReadInUtcAsAWholeNumberAndANullStaysNull() {
        formula("$x.tm->year()", "((long) year(t0__tm, " + UTC + "))");
        formula("$x.tm->hour()", "((long) hourOfDay(t0__tm, " + UTC + ", false))");
        formula("$x.tm->minute()", "((long) minuteOfHour(t0__tm, " + UTC + "))");
        formula("$x.tm->dayOfWeekNumber()", "((long) dayOfWeekValue(t0__tm, " + UTC + "))");
        formula("$x.tm->quarter()", "((long) ((int) ((monthOfYear(t0__tm, " + UTC + ") - 1) / 3) + 1))");
        formula("$x.tm->weekOfYear()", "((long) toLocalDate(t0__tm, " + UTC
                + ").get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR))");
        // a column that may be null: its null is the whole number's null, not the part of a null instant
        formula("$x.ts->toOne()->second()", "(isNull(t0__ts) ? NULL_LONG : (long) secondOfMinute(t0__ts, " + UTC + "))");
        // a date's own parts take no zone; it has no hour
        formula("$x.d->toOne()->monthNumber()", "(isNull(t0__d) ? NULL_LONG : (long) monthOfYear(t0__d))");
    }

    @Test
    void aDayAndATruncationAreTheirUtcStarts() {
        formula("$x.tm->datePart()", "toLocalDate(t0__tm, " + UTC + ")");
        // to a day or finer an instant (the epoch is a UTC midnight)
        formula("$x.tm->firstMinuteOfHour()", "lowerBin(t0__tm, 3600000000000L)");
        // to a week or coarser a date, as the platform's dialects answer it: a week's from its Monday
        formula("$x.tm->firstDayOfWeek()", "toLocalDate(lowerBin(t0__tm, 604800000000000L, -259200000000000L), "
                + UTC + ")");
        formula("$x.tm->firstDayOfMonth()", "(toLocalDate(t0__tm, " + UTC + ").withDayOfMonth(1))");
        // a quarter's first month: the month less its place in the quarter
        formula("$x.tm->firstDayOfQuarter()", "(toLocalDate(t0__tm, " + UTC + ").withDayOfMonth(1).minusMonths(("
                + "toLocalDate(t0__tm, " + UTC + ").getMonthValue() - 1) % 3))");
        // a date that may be null: a method of a null date fails the operation, so its null is guarded
        formula("$x.d->toOne()->firstDayOfYear()", "(isNull(t0__d) ? (java.time.LocalDate) null : t0__d.withDayOfYear(1))");
    }

    @Test
    void anAdjustmentMovesByAFixedLengthOrOnTheUtcCalendar() {
        formula("$x.tm->adjust(5, DurationUnit.MINUTES)", "plus(t0__tm, (5L * 60000000000L))");
        formula("$x.tm->adjust(-3, DurationUnit.DAYS)", "plus(t0__tm, (-3L * 86400000000000L))");
        formula("$x.tm->adjust(2, DurationUnit.MONTHS)", "(toInstant(toZonedDateTime(t0__tm, " + UTC
                + ").plusMonths(2L)))");
        // a date moved by a month: the date of its UTC midnight's move (the lowering casts it back)
        formula("$x.d->toOne()->adjust(1, DurationUnit.YEARS)", "toLocalDate(((isNull(t0__d)) ? (java.time.Instant)"
                + " null : toInstant(toZonedDateTime(toInstant(atMidnight(t0__d, " + UTC + ")), " + UTC
                + ").plusYears(1L))), " + UTC + ")");
    }

    /** A candle's open and close (K3a): the value at the first and the last of an order, the server's sortedFirst
     *  and sortedLast by the order's column; a value or an order that may be null refused, since SQL skips a null
     *  order where the server sorts it first. */
    @Test
    void aValueAtTheFirstOrLastOfAnOrderIsTheServersSortedFirstOrLast() {
        String rm = "meta::pure::functions::math::mathUtility::rowMapper";
        String b = batch(F + "->groupBy(~[d], ~[o:x|" + rm + "($x.tm, $x.fid):y|$y->minBy(), c:x|" + rm
                + "($x.tm, $x.fid):y|$y->maxBy()])");
        assertTrue(b.contains("{\"columns\": {\"spec\": {\"sortedFirst\": {\"columns\": [{\"columnName\": \"t0__fid\"}]}},"
                + " \"matchPairs\": [\"__r0=t0__tm\"]}}"), b);
        assertTrue(b.contains("{\"sortedLast\": {\"columns\": [{\"columnName\": \"t0__fid\"}]}}"), b);
        // a value that may be null: SQL skips the row (DuckDB's arg_min skips a null value and a null order), so its
        // value is made null and its order sorts it out of the way -- the greatest there is for sortedFirst, the
        // server's null (first) for sortedLast; a group every row of which is skipped answers null
        String nullable = batch(F + "->groupBy(~[d], ~[o:x|" + rm + "($x.px, $x.fid):y|$y->minBy(), c:x|" + rm
                + "($x.px, $x.fid):y|$y->maxBy()])");
        assertTrue(nullable.contains("\"__x0 = (isNull(t0__px) || isNull(t0__fid)) ? NULL_DOUBLE : t0__px\", \"__y0 ="
                + " (isNull(t0__px) || isNull(t0__fid)) ? 9223372036854775807L : t0__fid\""), nullable);
        assertTrue(nullable.contains("\"__y1 = (isNull(t0__px) || isNull(t0__fid)) ? NULL_LONG : t0__fid\""), nullable);
        assertTrue(nullable.contains("{\"sortedFirst\": {\"columns\": [{\"columnName\": \"__y0\"}]}}, \"matchPairs\":"
                + " [\"__r0=__x0\"]"), nullable);
        // an order with no greatest value written yet (a decimal) is refused by name
        DialectCapability decimal = assertThrows(DialectCapability.class, () -> batch("#>{risk::Ref.books}#"
                + "->groupBy(~[bid], ~[o:x|" + rm + "($x.bid, $x.lim->toOne()):y|$y->minBy()])"));
        assertTrue(decimal.getMessage().contains("may be null"), decimal.getMessage());
    }

    @Test
    void aModeIsItsOwnAggregationJoinedBackByTheKeys() {
        // the values that are not null counted by the keys and the value, each key's greatest count's value by
        // sortedLast, joined back; the group's other aggregates stay in its one aggregation
        String b = batch("#>{risk::Ref.trades}#->groupBy(~[region], ~[m:x|$x.year:y|$y->mode(), n:x|$x.id:y|$y->count()])");
        assertTrue(b.contains("\"aggregations\": [{\"count\": {\"columnName\": \"__r1\"}}]"), b);
        assertTrue(b.contains("{\"unstructuredFilter\": {\"sourceId\": {\"batchOffset\": 0}, \"filters\": [\"t0__year"
                + " != null\"]}}"), b);
        assertTrue(b.contains("\"groupByColumns\": [\"t0__region\", \"t0__year\"], \"aggregations\": [{\"count\":"
                + " {\"columnName\": \"__m0\"}}]"), b);
        assertTrue(b.contains("{\"sortedLast\": {\"columns\": [{\"columnName\": \"__m0\"}]}}, \"matchPairs\":"
                + " [\"__r0=t0__year\"]"), b);
        assertTrue(b.contains("\"columnsToMatch\": [\"t0__region\"], \"columnsToAdd\": [\"__r0\"]"), b);
    }

    @Test
    void aStatisticTheServerHasNoAggregationForIsAFormulaOverWhatTheGroupGathered() {
        String rm = "meta::pure::functions::math::mathUtility::rowMapper";
        // a correlation and the covariances: the library's cor and cov over the group's two vectors, the pairs where
        // neither is null counted; the server's cov is the population's, the sample's rescaled by n / (n - 1)
        String pair = batch(P + "->groupBy(~[sym], ~[c:x|" + rm + "($x.px, $x.sz):y|$y->corr(), s:x|" + rm
                + "($x.px, $x.sz):y|$y->covarSample(), p:x|" + rm + "($x.px, $x.sz):y|$y->covarPopulation()])");
        assertTrue(pair.contains("{\"columns\": {\"spec\": {\"group\": {}}, \"matchPairs\": [\"__v0=t0__px\","
                + " \"__w0=t0__sz\"]}}, {\"countWhere\": {\"columnName\": \"__n0\", \"filters\": [\"t0__px != null\","
                + " \"t0__sz != null\"]}}"), pair);
        assertTrue(pair.contains("{\"update\": {\"sourceId\": {\"batchOffset\": 1}, \"columnSpecs\": [\"__r0 = (__n0 < 2"
                + " ? NULL_DOUBLE : cor(__v0, __w0))\", \"__r1 = (__n1 < 2 ? NULL_DOUBLE : cov(__v1, __w1) * __n1 / (__n1 -"
                + " 1))\", \"__r2 = (__n2 == 0 ? NULL_DOUBLE : cov(__v2, __w2))\"]}}"), pair);
        // the population variance and deviation: the server's sample variance rescaled, 0 of one value
        String spread = batch(P + "->groupBy(~[sym], ~[v:x|$x.px:y|$y->variancePopulation(),"
                + " d:x|$x.px:y|$y->stdDevPopulation()])");
        assertTrue(spread.contains("\"__r0 = (__n0 == 0 ? NULL_DOUBLE : __n0 == 1 ? 0.0 : __v0 * (__n0 - 1) / __n0)\""),
                spread);
        assertTrue(spread.contains("{\"columns\": {\"spec\": {\"var\": {}}, \"matchPairs\": [\"__v1=t0__px\"]}}"),
                spread);
        // the interpolated percentile: the group's vector sorted (its nulls first), two places read, weighted
        String pct = batch(P + "->groupBy(~[sym], ~[p:x|$x.sz:y|$y->percentile(0.9)])");
        assertTrue(pct.contains("\"__c0 = count(__v0)\", \"__o0 = len(__v0) - __c0\", \"__k0 = 0.9 * (__c0 - 1)\","
                + " \"__s0 = sort(__v0)\", \"__r0 = (__c0 == 0 ? NULL_DOUBLE : nth(__o0 + (long) floor(__k0), __s0) +"
                + " (nth(__o0 + (long) ceil(__k0), __s0) - nth(__o0 + (long) floor(__k0), __s0)) * (__k0 -"
                + " floor(__k0)))\""), pct);
    }

    @Test
    void theTimeNowIsRefusedByNameUntilALiveWindowIsDesigned() {
        for (String now : List.of("now()", "today()")) {
            DialectCapability e = assertThrows(DialectCapability.class,
                    () -> batch(F + "->extend(~r:x|" + now + ")"));
            assertTrue(e.getMessage().contains("DH30"), e.getMessage());
        }
    }

    // ==================================================================
    // Windows (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md K6)
    // ==================================================================

    private static final String P = "#>{risk::Ref.prints}#";

    /** How many of {@code b}'s operations are {@code op}. */
    private static int count(String b, String op) {
        return b.split("\\{\"" + op + "\": \\{", -1).length - 1;
    }

    /** A running figure along an order: the rows sorted by it, the server's cumulative operation grouped by the
     *  partition; SQL's default frame counts a row's ties with it, so each tie takes the figure at the last of them. */
    @Test
    void aRunningFigureIsTheServersUpdateByAndATieTakesItsLastFigure() {
        String b = batch(P + "->extend(over(~sym, ~tm->ascending()), ~vol:{p,w,r|$r.sz}:y|$y->plus())->select(~[pid, vol])");
        assertTrue(b.contains("{\"sort\": {\"sourceId\": {\"batchOffset\": 0}, \"sorts\": [{\"columnName\": \"t0__tm\","
                + " \"direction\": \"ASCENDING\"}]}}"), b);
        assertTrue(b.contains("{\"updateBy\": {\"sourceId\": {\"batchOffset\": 1}, \"operations\": [{\"column\": {\"spec\":"
                + " {\"sum\": {}}, \"matchPairs\": [\"__x1=t0__sz\"]}}], \"groupByColumns\": [\"t0__sym\"]}}"), b);
        assertTrue(b.contains("{\"aggregate\": {\"sourceId\": {\"batchOffset\": 2}, \"groupByColumns\": [\"t0__sym\","
                + " \"t0__tm\"], \"aggregations\": [{\"columns\": {\"spec\": {\"last\": {}}, \"matchPairs\":"
                + " [\"__w0=__x1\"]}}]}}"), b);
        assertTrue(b.contains("\"columnsToMatch\": [\"t0__sym\", \"t0__tm\"], \"columnsToAdd\": [\"__w0\"]"), b);
        assertTrue(b.contains("\"vol = __w0\""), b);
        // ROWS from the partition's start is the rows as they are: no tie to take
        String rows = batch(P + "->extend(over(~sym, ~pid->ascending(), rows(unbounded(), 0)), ~v:{p,w,r|$r.sz}:y|"
                + "$y->plus())->select(~[pid, v])");
        assertEquals(0, count(rows, "aggregate"), rows);
        // a count of a value that may be null counts where it is not; a running average is the running sum over it
        String avg = batch(P + "->extend(over(~sym, ~pid->ascending()), ~a:{p,w,r|$r.px}:y|$y->average())"
                + "->select(~[pid, a])");
        assertTrue(avg.contains("{\"countWhere\": {\"resultColumn\": \"__x3\", \"filters\": [\"t0__px != null\"]}}"), avg);
        assertTrue(avg.contains("\"__x1 = (__x3 == 0 ? NULL_DOUBLE : (double) __x2 / __x3)\""), avg);
        // a row's number counts a column of ones, ties or none
        String rn = batch(P + "->extend(over(~sym, ~tm->ascending()), ~rn:{p,w,r|$p->rowNumber($r)})->select(~[pid, rn])");
        assertTrue(rn.contains("\"__x0 = 1L\"") && rn.contains("\"matchPairs\": [\"__w1=__x0\"]"), rn);
        assertEquals(0, count(rn, "aggregate"), rn);
    }

    /** A key that may be null sorts after its null flag, as a select's does: nulls first descending. */
    @Test
    void anOrderKeyThatMayBeNullSortsItsNullsWhereThePlatformsOrderPutsThem() {
        String b = batch(P + "->extend(over(~sym, [~px->descending(), ~pid->ascending()]), ~rn:{p,w,r|$p->rowNumber($r)})"
                + "->select(~[pid, rn])");
        assertTrue(b.contains("\"__x0 = isNull(t0__px)\""), b);
        assertTrue(b.contains("\"sorts\": [{\"columnName\": \"__x0\", \"direction\": \"DESCENDING\"}, {\"columnName\":"
                + " \"t0__px\", \"direction\": \"DESCENDING\"}, {\"columnName\": \"t0__pid\", \"direction\": \"ASCENDING\"}]"), b);
    }

    /** A frame of rows is the server's rolling operation over ticks (the row itself one of those behind it); a RANGE of
     *  time along one date-time is one over its nanoseconds; LAG and LEAD the one row a window that far away holds. */
    @Test
    void aFrameOfRowsOrOfTimeIsTheServersRollingOperation() {
        String rows = batch(P + "->extend(over(~sym, ~pid->ascending(), rows(-2, 0)), ~m:{p,w,r|$r.px}:y|$y->average())"
                + "->select(~[pid, m])");
        assertTrue(rows.contains("{\"rollingAvg\": {\"reverseWindowScale\": {\"ticks\": {\"ticks\": 3}},"
                + " \"forwardWindowScale\": {\"ticks\": {\"ticks\": 0}}}}"), rows);
        String before = batch(P + "->extend(over(~sym, ~pid->ascending(), rows(-2, -1)), ~b:{p,w,r|$r.sz}:y|$y->plus())"
                + "->select(~[pid, b])");
        assertTrue(before.contains("{\"rollingSum\": {\"reverseWindowScale\": {\"ticks\": {\"ticks\": 3}},"
                + " \"forwardWindowScale\": {\"ticks\": {\"ticks\": -1}}}}"), before);
        String time = batch(P + "->extend(over(~sym, ~tm->ascending(), _range(-1, DurationUnit.MINUTES, 30,"
                + " DurationUnit.SECONDS)), ~v:{p,w,r|$r.sz}:y|$y->plus())->select(~[pid, v])");
        assertTrue(time.contains("{\"rollingSum\": {\"reverseWindowScale\": {\"time\": {\"column\": \"t0__tm\", \"nanos\":"
                + " \"60000000000\"}}, \"forwardWindowScale\": {\"time\": {\"column\": \"t0__tm\", \"nanos\":"
                + " \"30000000000\"}}}}"), time);
        String shifted = batch(P + "->extend(over(~sym, ~pid->ascending()), ~[a:{p,w,r|$p->lag($r).px},"
                + " b:{p,w,r|$p->lead($r, 2).sym}])->select(~[pid, a, b])");
        assertTrue(shifted.contains("{\"rollingGroup\": {\"reverseWindowScale\": {\"ticks\": {\"ticks\": 2}},"
                + " \"forwardWindowScale\": {\"ticks\": {\"ticks\": -1}}}}"), shifted);
        assertTrue(shifted.contains("{\"rollingGroup\": {\"reverseWindowScale\": {\"ticks\": {\"ticks\": -1}},"
                + " \"forwardWindowScale\": {\"ticks\": {\"ticks\": 2}}}}"), shifted);
        // the group's one value read by the library (DH33: a vector's own methods are not the validator's)
        assertTrue(shifted.contains(" = first(__x") && shifted.contains(" = firstObj(__x"), shifted);
    }

    /** Windows over one partition and order share their sort and their updateBy; over a whole partition, its
     *  aggregate, joined back. */
    @Test
    void windowsOverOneOrderAreOnePassOfTheServers() {
        String b = batch(P + "->extend(over(~sym, ~pid->ascending()), ~[a:{p,w,r|$r.sz}:y|$y->plus(),"
                + " c:{p,w,r|$r.px}:y|$y->max()])->extend(over(~sym, ~pid->ascending()), ~prev:{p,w,r|$p->lag($r).px})"
                + "->extend(over(~sym, ~pid->ascending(), rows(-4, 0)), ~m:{p,w,r|$r.px}:y|$y->average())"
                + "->select(~[pid, a, c, prev, m])");
        assertEquals(1, count(b, "sort"), b);
        assertEquals(1, count(b, "updateBy"), b);
        assertEquals(1, count(b, "aggregate"), b);
        String whole = batch(P + "->extend(over(~sym), ~[t:{p,w,r|$r.sz}:y|$y->plus(), n:{p,w,r|$r.px}:y|$y->count()])"
                + "->select(~[pid, t, n])");
        assertTrue(whole.contains("{\"aggregate\": {\"sourceId\": {\"batchOffset\": 0}, \"groupByColumns\": [\"t0__sym\"],"
                + " \"aggregations\": [{\"columns\": {\"spec\": {\"sum\": {}}, \"matchPairs\": [\"__w0=t0__sz\"]}},"
                + " {\"countWhere\": {\"columnName\": \"__w1\", \"filters\": [\"t0__px != null\"]}}]}}"), whole);
        assertTrue(whole.contains("\"columnsToMatch\": [\"t0__sym\"], \"columnsToAdd\": [\"__w0\", \"__w1\"]"), whole);
        assertEquals(0, count(whole, "sort"), whole);
    }

    @Test
    void aWindowWithNoShapeYetIsRefusedByName() {
        for (String w : List.of(
                // a month is no fixed length of time
                "over(~sym, ~tm->ascending(), _range(-1, DurationUnit.MONTHS, 0, DurationUnit.MONTHS)), ~v:{p,w,r|$r.sz}:y|$y->plus()",
                // a frame open after the row
                "over(~sym, ~pid->ascending(), rows(0, unbounded())), ~v:{p,w,r|$r.sz}:y|$y->plus()",
                // a RANGE of numbers
                "over(~sym, ~sz->ascending(), _range(-10, 0)), ~v:{p,w,r|$r.sz}:y|$y->plus()")) {
            DialectCapability e = assertThrows(DialectCapability.class, () -> batch(P + "->extend(" + w + ")"));
            assertTrue(e.getMessage().contains("reached Deephaven"), e.getMessage());
        }
    }

    /** A filter over a window -- the top of each group, SQL's QUALIFY -- reads the column the window was computed
     *  into, with no compiling (DH25). */
    @Test
    void aTopOfEachGroupIsItsRowNumberFilteredWithNoCompiling() {
        String b = batch(P + "->extend(over(~sym, [~sz->descending(), ~pid->ascending()]), ~rn:{p,w,r|$p->rowNumber($r)})"
                + "->filter(x|$x.rn <= 2)->select(~[pid, sym, rn])");
        assertTrue(b.contains("{\"unstructuredFilter\": {\"sourceId\": {\"batchOffset\": 3}, \"filters\": [\"__w1 <= 2\"]}}"), b);
    }

    /** A rank is the row number of the first of its ties, read across each tie; a dense rank the running count of the
     *  ties begun; percent rank, cumulative distribution and NTILE figures of those and the partition's count; the
     *  first and the last value the partition's, a tie's, or a rolling group's. */
    @Test
    void aRankAndAFirstOrLastValueAreReadAcrossTiesAndPartitions() {
        String b = batch(P + "->extend(over(~sym, ~px->descending()), ~[rk:{p,w,r|$p->rank($w, $r)},"
                + " dr:{p,w,r|$p->denseRank($w, $r)}, pr:{p,w,r|$p->percentRank($w, $r)},"
                + " cd:{p,w,r|$p->cumulativeDistribution($w, $r)}])->select(~[pid, rk, dr, pr, cd])");
        // one row number, the first and the last of it across each tie, the partition's count
        assertEquals(1, count(b, "sort"), b);
        assertTrue(b.contains("{\"aggregate\": {\"sourceId\": {\"batchOffset\": 3}, \"groupByColumns\": [\"t0__sym\", \"__x0\","
                + " \"t0__px\"], \"aggregations\": [{\"columns\": {\"spec\": {\"first\": {}}, \"matchPairs\": [\"__x3=__x2\"]}},"
                + " {\"columns\": {\"spec\": {\"last\": {}}, \"matchPairs\": [\"__x4=__x2\"]}}]}}"), b);
        assertTrue(b.contains("{\"count\": {\"columnName\": \"__x5\"}}"), b);
        assertTrue(b.contains("\"__x8 = (__x2 == __x3) ? 1L : 0L\""), b);
        assertTrue(b.contains("\"__w9 = (__x5 == 1 ? 0.0 : (double) (__x3 - 1) / (__x5 - 1))\""), b);
        assertTrue(b.contains("\"__w10 = (double) __x4 / __x5\""), b);
        String ntile = batch(P + "->extend(over(~sym, ~pid->ascending()), ~n:{p,w,r|$p->ntile($r, 3)})->select(~[pid, n])");
        assertTrue(ntile.contains(" % 3"), ntile);
        String values = batch(P + "->extend(over(~sym, ~tm->ascending()), ~[f:{p,w,r|$p->first($w, $r).px},"
                + " l:{p,w,r|$p->last($w, $r).px}])->select(~[pid, f, l])");
        // the first to the row is the partition's first; the last to the row, the last of its ties
        assertTrue(values.contains("\"groupByColumns\": [\"t0__sym\"], \"aggregations\": [{\"columns\": {\"spec\": {\"first\":"
                + " {}}, \"matchPairs\": [\"__w0=t0__px\"]}}]"), values);
        assertTrue(values.contains("\"groupByColumns\": [\"t0__sym\", \"t0__tm\"], \"aggregations\": [{\"columns\": {\"spec\":"
                + " {\"last\": {}}, \"matchPairs\": [\"__w1=t0__px\"]}}]"), values);
        String rolling = batch(P + "->extend(over(~sym, ~pid->ascending(), rows(-1, 1)), ~f:{p,w,r|$p->first($w, $r).sym})"
                + "->select(~[pid, f])");
        assertTrue(rolling.contains("{\"rollingGroup\": {\"reverseWindowScale\": {\"ticks\": {\"ticks\": 2}},"
                + " \"forwardWindowScale\": {\"ticks\": {\"ticks\": 1}}}}") && rolling.contains(" = firstObj(__x"), rolling);
    }
}
