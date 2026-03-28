package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.sqlgen.DuckDBDialect;
import com.gs.legend.sqlgen.SQLDialect;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for AsOfJoinChecker — Relation API {@code asOfJoin()}.
 *
 * <p>
 * asOfJoin is a temporal/inequality join: the match condition
 * ({@code $l.time > $r.time})
 * finds the closest right row that satisfies the condition. DuckDB's ASOF JOIN
 * implements this natively.
 *
 * <ul>
 * <li>Match-only: 3-param {@code asOfJoin(left, right, matchCondition)}</li>
 * <li>Match + key: 4-param
 * {@code asOfJoin(left, right, matchCondition, keyCondition)}</li>
 * <li>Match operators: {@code >}, {@code >=}, {@code <}, {@code <=}</li>
 * <li>Prefix variants: with/without column deduplication</li>
 * <li>Chained operations: asOfJoin→filter, asOfJoin→select,
 * asOfJoin→extend</li>
 * <li>Edge cases: empty tables, no match, single row, closest match, NULL
 * verification</li>
 * </ul>
 */
public class AsOfJoinCheckerTest extends AbstractDatabaseTest {

    @Override
    protected String getDatabaseType() {
        return "DuckDB";
    }

    @Override
    protected SQLDialect getDialect() {
        return DuckDBDialect.INSTANCE;
    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:duckdb:";
    }

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(getJdbcUrl());
        setupPureModel();
        setupDatabase();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null)
            connection.close();
    }

    // ==================== Match-only (3-param) ====================

    @Nested
    @DisplayName("asOfJoin(left, right, matchCondition)")
    class MatchOnly {

        @Test
        @DisplayName("basic time-based asOf join with timestamps — verifies matched values")
        void testBasicAsOfJoinTimestamps() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        trade_id, trade_time:DateTime
                        1, %2024-01-15T10:30:00
                        2, %2024-01-15T11:30:00
                        3, %2024-01-15T12:30:00
                    #->asOfJoin(
                        #TDS
                            quote_id, quote_time:DateTime, price
                            A, %2024-01-15T10:15:00, 100
                            B, %2024-01-15T10:45:00, 102
                            C, %2024-01-15T11:15:00, 105
                            D, %2024-01-15T12:00:00, 108
                            E, %2024-01-15T12:45:00, 110
                        #,
                        {t, q | $t.trade_time > $q.quote_time}
                    )""");
            assertEquals(3, result.rowCount(), "One row per trade");
            // trade_id=1 (10:30) > closest quote_time → 10:15 → price=100
            // trade_id=2 (11:30) > closest quote_time → 11:15 → price=105
            // trade_id=3 (12:30) > closest quote_time → 12:00 → price=108
            var byTrade = collectResults(result, "trade_id", "price");
            assertEquals(100L, ((Number) byTrade.get(1)).longValue(), "trade_id=1 → price=100");
            assertEquals(105L, ((Number) byTrade.get(2)).longValue(), "trade_id=2 → price=105");
            assertEquals(108L, ((Number) byTrade.get(3)).longValue(), "trade_id=3 → price=108");
        }

        @Test
        @DisplayName("basic integer-based asOf join — verifies matched values")
        void testBasicAsOfJoinIntegers() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 10
                        2, 20
                        3, 30
                    #->asOfJoin(
                        #TDS
                            ts, value
                            5, 100
                            15, 200
                            25, 300
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            assertEquals(3, result.rowCount());
            // id=1 (et=10) > ts=5 → value=100
            // id=2 (et=20) > ts=15 → value=200
            // id=3 (et=30) > ts=25 → value=300
            var byId = collectResults(result, "id", "value");
            assertEquals(100L, ((Number) byId.get(1)).longValue());
            assertEquals(200L, ((Number) byId.get(2)).longValue());
            assertEquals(300L, ((Number) byId.get(3)).longValue());
        }

        @Test
        @DisplayName("asOf join with no matching right rows — left preserved with NULLs")
        void testAsOfJoinNoMatch() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 1
                    #->asOfJoin(
                        #TDS
                            ts, value
                            100, 999
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            // event_time=1 < ts=100, so no match → left row preserved with NULLs
            assertEquals(1, result.rowCount());
            int valueIdx = columnIndex(result, "value");
            assertNull(result.rows().get(0).get(valueIdx), "Unmatched row should have NULL value");
        }

        @Test
        @DisplayName("asOf join against empty right table")
        void testAsOfJoinEmptyRight() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time:Integer
                        1, 10
                        2, 20
                    #->asOfJoin(
                        #TDS
                            ts:Integer, value
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            // DuckDB ASOF JOIN with empty right returns no rows
            assertEquals(0, result.rowCount(), "ASOF JOIN with empty right produces no matches");
        }

        @Test
        @DisplayName("asOf join single row each side — match verified")
        void testAsOfJoinSingleRowMatch() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 20
                    #->asOfJoin(
                        #TDS
                            ts, value
                            10, 42
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            assertEquals(1, result.rowCount());
            int valueIdx = columnIndex(result, "value");
            assertEquals(42L, ((Number) result.rows().get(0).get(valueIdx)).longValue(),
                    "Single match should yield value=42");
        }

        @Test
        @DisplayName("asOf join single row each side — no match gives NULLs")
        void testAsOfJoinSingleRowNoMatch() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 5
                    #->asOfJoin(
                        #TDS
                            ts, value
                            10, 42
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            assertEquals(1, result.rowCount(), "Left row preserved with NULL right cols");
            int tsIdx = columnIndex(result, "ts");
            int valueIdx = columnIndex(result, "value");
            assertNull(result.rows().get(0).get(tsIdx), "No match → ts is NULL");
            assertNull(result.rows().get(0).get(valueIdx), "No match → value is NULL");
        }
    }

    // ==================== Match operators: >=, <, <= ====================

    @Nested
    @DisplayName("asOfJoin() match operator variants")
    class MatchOperators {

        @Test
        @DisplayName(">= operator — includes exact match")
        void testGreaterThanEqualOperator() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 10
                        2, 20
                        3, 30
                    #->asOfJoin(
                        #TDS
                            ts, value
                            10, 100
                            20, 200
                            25, 300
                        #,
                        {l, r | $l.event_time >= $r.ts}
                    )""");
            assertEquals(3, result.rowCount());
            // id=1 (et=10) >= ts → exact match ts=10, value=100
            // id=2 (et=20) >= ts → exact match ts=20, value=200
            // id=3 (et=30) >= ts → closest ts<=30 is ts=25, value=300
            var byId = collectResults(result, "id", "value");
            assertEquals(100L, ((Number) byId.get(1)).longValue(), "id=1: exact match ts=10");
            assertEquals(200L, ((Number) byId.get(2)).longValue(), "id=2: exact match ts=20");
            assertEquals(300L, ((Number) byId.get(3)).longValue(), "id=3: closest ts=25");
        }

        @Test
        @DisplayName(">= with timestamps — includes exact time match")
        void testGreaterThanEqualTimestamps() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        trade_id, trade_time:DateTime
                        1, %2024-01-15T10:00:00
                        2, %2024-01-15T11:00:00
                    #->asOfJoin(
                        #TDS
                            quote_time:DateTime, price
                            %2024-01-15T10:00:00, 100
                            %2024-01-15T10:30:00, 110
                        #,
                        {t, q | $t.trade_time >= $q.quote_time}
                    )""");
            assertEquals(2, result.rowCount());
            var byTrade = collectResults(result, "trade_id", "price");
            // trade_id=1 (10:00) >= quote_time → exact match at 10:00 → price=100
            assertEquals(100L, ((Number) byTrade.get(1)).longValue(), "Exact timestamp match");
            // trade_id=2 (11:00) >= quote_time → closest is 10:30 → price=110
            assertEquals(110L, ((Number) byTrade.get(2)).longValue(), "Closest timestamp match");
        }

        @Test
        @DisplayName("< operator — left time before right time")
        void testLessThanOperator() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 5
                        2, 15
                        3, 25
                    #->asOfJoin(
                        #TDS
                            ts, value
                            10, 100
                            20, 200
                            30, 300
                        #,
                        {l, r | $l.event_time < $r.ts}
                    )""");
            assertEquals(3, result.rowCount());
            // ASOF with < picks the closest right row where ts > event_time
            // id=1 (et=5) < ts → closest ts>5 is ts=10, value=100
            // id=2 (et=15) < ts → closest ts>15 is ts=20, value=200
            // id=3 (et=25) < ts → closest ts>25 is ts=30, value=300
            var byId = collectResults(result, "id", "value");
            assertEquals(100L, ((Number) byId.get(1)).longValue(), "id=1: closest ts=10");
            assertEquals(200L, ((Number) byId.get(2)).longValue(), "id=2: closest ts=20");
            assertEquals(300L, ((Number) byId.get(3)).longValue(), "id=3: closest ts=30");
        }

        @Test
        @DisplayName("<= operator — includes exact match from above")
        void testLessThanEqualOperator() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 10
                        2, 20
                        3, 25
                    #->asOfJoin(
                        #TDS
                            ts, value
                            10, 100
                            20, 200
                            30, 300
                        #,
                        {l, r | $l.event_time <= $r.ts}
                    )""");
            assertEquals(3, result.rowCount());
            // ASOF with <= picks the closest right row where ts >= event_time
            // id=1 (et=10) <= ts → exact match ts=10, value=100
            // id=2 (et=20) <= ts → exact match ts=20, value=200
            // id=3 (et=25) <= ts → closest ts>=25 is ts=30, value=300
            var byId = collectResults(result, "id", "value");
            assertEquals(100L, ((Number) byId.get(1)).longValue(), "id=1: exact match ts=10");
            assertEquals(200L, ((Number) byId.get(2)).longValue(), "id=2: exact match ts=20");
            assertEquals(300L, ((Number) byId.get(3)).longValue(), "id=3: closest ts=30");
        }
    }

    // ==================== Match + Key (4-param with two lambdas)
    // ====================

    @Nested
    @DisplayName("asOfJoin(left, right, matchCondition, keyCondition)")
    class MatchWithKey {

        @Test
        @DisplayName("asOf join with symbol key — timestamp match — verifies per-symbol matching")
        void testAsOfJoinWithKeyConditionTimestamps() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        trade_id, symbol, trade_time:DateTime
                        1, AAPL, %2024-01-15T10:30:00
                        2, MSFT, %2024-01-15T10:30:00
                        3, AAPL, %2024-01-15T11:30:00
                    #->asOfJoin(
                        #TDS
                            quote_id, quote_symbol, quote_time:DateTime, price
                            A, AAPL, %2024-01-15T10:00:00, 180
                            B, MSFT, %2024-01-15T10:00:00, 350
                            C, AAPL, %2024-01-15T11:00:00, 182
                            D, MSFT, %2024-01-15T11:00:00, 355
                        #,
                        {t, q | $t.trade_time > $q.quote_time},
                        {t, q | $t.symbol == $q.quote_symbol}
                    )""");
            assertEquals(3, result.rowCount());
            var byTrade = collectResults(result, "trade_id", "price");
            // trade_id=1 AAPL 10:30 → closest AAPL quote before 10:30 = A(10:00) →
            // price=180
            assertEquals(180L, ((Number) byTrade.get(1)).longValue(), "AAPL trade matched AAPL quote");
            // trade_id=2 MSFT 10:30 → closest MSFT quote before 10:30 = B(10:00) →
            // price=350
            assertEquals(350L, ((Number) byTrade.get(2)).longValue(), "MSFT trade matched MSFT quote");
            // trade_id=3 AAPL 11:30 → closest AAPL quote before 11:30 = C(11:00) →
            // price=182
            assertEquals(182L, ((Number) byTrade.get(3)).longValue(), "AAPL trade matched later AAPL quote");
        }

        @Test
        @DisplayName("asOf join with key — integer time match — verifies closest within key group")
        void testAsOfJoinWithKeyConditionIntegers() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        trade_id, symbol, trade_time
                        1, AAPL, 15
                        2, GOOG, 20
                    #->asOfJoin(
                        #TDS
                            quote_symbol, quote_time, price
                            AAPL, 10, 150
                            AAPL, 12, 152
                            GOOG, 10, 2800
                            GOOG, 18, 2810
                        #,
                        {t, q | $t.trade_time > $q.quote_time},
                        {t, q | $t.symbol == $q.quote_symbol}
                    )""");
            assertEquals(2, result.rowCount());
            var byTrade = collectResults(result, "trade_id", "price");
            // trade_id=1 AAPL t=15 → closest AAPL quote_time<15 is 12 → price=152
            assertEquals(152L, ((Number) byTrade.get(1)).longValue(), "AAPL picks closest quote at t=12");
            // trade_id=2 GOOG t=20 → closest GOOG quote_time<20 is 18 → price=2810
            assertEquals(2810L, ((Number) byTrade.get(2)).longValue(), "GOOG picks closest quote at t=18");
        }

        @Test
        @DisplayName("asOf join with key — no match in key group → NULLs")
        void testAsOfJoinKeyNoMatch() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        trade_id, symbol, trade_time
                        1, MSFT, 5
                    #->asOfJoin(
                        #TDS
                            quote_symbol, quote_time, price
                            AAPL, 10, 150
                            GOOG, 10, 2800
                        #,
                        {t, q | $t.trade_time > $q.quote_time},
                        {t, q | $t.symbol == $q.quote_symbol}
                    )""");
            assertEquals(1, result.rowCount(), "Left row preserved, no MSFT quotes");
            int priceIdx = columnIndex(result, "price");
            assertNull(result.rows().get(0).get(priceIdx), "No MSFT quotes → price is NULL");
        }
    }

    // ==================== Prefix / Column Deduplication ====================

    @Nested
    @DisplayName("asOfJoin() prefix / deduplication")
    class PrefixDeduplication {

        @Test
        @DisplayName("overlapping columns — match+key+prefix (5-param) — verifies prefixed values")
        void testAsOfJoinOverlappingWithPrefix() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        symbol, ts, value
                        AAPL, 10, 100
                        AAPL, 20, 200
                    #->asOfJoin(
                        #TDS
                            symbol, ts, value
                            AAPL, 5, 50
                            AAPL, 15, 150
                        #,
                        {l, r | $l.ts > $r.ts},
                        {l, r | $l.symbol == $r.symbol},
                        'right'
                    )""");
            assertEquals(2, result.rowCount());
            var colNames = result.columns().stream().map(c -> c.name()).toList();
            assertTrue(colNames.contains("ts"), "Left ts kept");
            assertTrue(colNames.contains("right_ts"), "Right ts prefixed");
            assertTrue(colNames.contains("value"), "Left value kept");
            assertTrue(colNames.contains("right_value"), "Right value prefixed");
            // Verify actual matched values: left ts=10 > right ts=5 → right_value=50
            int rightValIdx = columnIndex(result, "right_value");
            int leftTsIdx = columnIndex(result, "ts");
            // Find which row has left ts=10 vs ts=20
            for (var row : result.rows()) {
                long ts = ((Number) row.get(leftTsIdx)).longValue();
                if (ts == 10)
                    assertEquals(50L, ((Number) row.get(rightValIdx)).longValue());
                if (ts == 20)
                    assertEquals(150L, ((Number) row.get(rightValIdx)).longValue());
            }
        }

        @Test
        @DisplayName("non-overlapping schemas — no prefix needed")
        void testAsOfJoinNonOverlapping() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 10
                        2, 20
                    #->asOfJoin(
                        #TDS
                            ts, value
                            5, 100
                            15, 200
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            assertEquals(2, result.rowCount());
            assertEquals(4, result.columns().size(), "id, event_time, ts, value");
            var byId = collectResults(result, "id", "value");
            assertEquals(100L, ((Number) byId.get(1)).longValue());
            assertEquals(200L, ((Number) byId.get(2)).longValue());
        }

        @Test
        @DisplayName("duplicate columns without prefix throws compile error")
        void testAsOfJoinDuplicateColumnsThrows() {
            assertThrows(Exception.class, () -> executeRelation("""
                    #TDS
                        ts, value
                        10, 100
                    #->asOfJoin(
                        #TDS
                            ts, value
                            5, 50
                        #,
                        {l, r | $l.ts > $r.ts}
                    )"""),
                    "Duplicate cols without prefix should throw");
        }

        @Test
        @DisplayName("partial overlap — only overlapping columns get prefix")
        void testAsOfJoinPartialOverlapPrefix() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        symbol, ts, name
                        AAPL, 10, Alice
                        AAPL, 20, Bob
                    #->asOfJoin(
                        #TDS
                            symbol, ts, score
                            AAPL, 5, 90
                            AAPL, 15, 85
                        #,
                        {l, r | $l.ts > $r.ts},
                        {l, r | $l.symbol == $r.symbol},
                        'right'
                    )""");
            var colNames = result.columns().stream().map(c -> c.name()).toList();
            assertTrue(colNames.contains("ts"), "Left ts kept");
            assertTrue(colNames.contains("right_ts"), "Right ts prefixed");
            assertTrue(colNames.contains("name"), "Left name not prefixed");
            assertTrue(colNames.contains("score"), "Right score not prefixed (no overlap)");
            // Verify matched score values
            int tsIdx = columnIndex(result, "ts");
            int scoreIdx = columnIndex(result, "score");
            for (var row : result.rows()) {
                long ts = ((Number) row.get(tsIdx)).longValue();
                if (ts == 10)
                    assertEquals(90L, ((Number) row.get(scoreIdx)).longValue(), "ts=10 → score=90");
                if (ts == 20)
                    assertEquals(85L, ((Number) row.get(scoreIdx)).longValue(), "ts=20 → score=85");
            }
        }

        @Test
        @DisplayName("match+key with prefix (5-param variant)")
        void testAsOfJoinMatchKeyWithPrefix() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        symbol, ts, qty
                        AAPL, 15, 100
                        GOOG, 20, 200
                    #->asOfJoin(
                        #TDS
                            symbol, ts, price
                            AAPL, 10, 150
                            GOOG, 18, 2810
                        #,
                        {t, q | $t.ts > $q.ts},
                        {t, q | $t.symbol == $q.symbol},
                        'q'
                    )""");
            assertEquals(2, result.rowCount());
            var colNames = result.columns().stream().map(c -> c.name()).toList();
            assertTrue(colNames.contains("symbol"), "Left symbol kept");
            assertTrue(colNames.contains("q_symbol"), "Right symbol prefixed");
            // Verify price matched correctly
            int symbolIdx = columnIndex(result, "symbol");
            int priceIdx = columnIndex(result, "price");
            for (var row : result.rows()) {
                String sym = row.get(symbolIdx).toString();
                if ("AAPL".equals(sym))
                    assertEquals(150L, ((Number) row.get(priceIdx)).longValue());
                if ("GOOG".equals(sym))
                    assertEquals(2810L, ((Number) row.get(priceIdx)).longValue());
            }
        }
    }

    // ==================== Chained Operations ====================

    @Nested
    @DisplayName("asOfJoin() chained with other operations")
    class ChainedOperations {

        @Test
        @DisplayName("asOfJoin then filter — verifies filtered values")
        void testAsOfJoinThenFilter() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 10
                        2, 20
                        3, 30
                    #->asOfJoin(
                        #TDS
                            ts, value
                            5, 100
                            15, 200
                            25, 300
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )->filter(x | $x.value > 150)""");
            assertEquals(2, result.rowCount());
            var byId = collectResults(result, "id", "value");
            // id=1(value=100) filtered out, id=2(value=200) and id=3(value=300) remain
            assertFalse(byId.containsKey(1), "id=1 with value=100 should be filtered out");
            assertEquals(200L, ((Number) byId.get(2)).longValue());
            assertEquals(300L, ((Number) byId.get(3)).longValue());
        }

        @Test
        @DisplayName("asOfJoin then select — verifies selected columns and values")
        void testAsOfJoinThenSelect() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 10
                        2, 20
                    #->asOfJoin(
                        #TDS
                            ts, value
                            5, 100
                            15, 200
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )->select(~[id, value])""");
            assertEquals(2, result.columns().size());
            var colNames = result.columns().stream().map(c -> c.name()).toList();
            assertTrue(colNames.contains("id"));
            assertTrue(colNames.contains("value"));
            var byId = collectResults(result, "id", "value");
            assertEquals(100L, ((Number) byId.get(1)).longValue());
            assertEquals(200L, ((Number) byId.get(2)).longValue());
        }

        @Test
        @DisplayName("filter then asOfJoin — only active rows participate")
        void testFilterThenAsOfJoin() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time, active
                        1, 10, 1
                        2, 20, 0
                        3, 30, 1
                    #->filter(x | $x.active == 1)
                    ->asOfJoin(
                        #TDS
                            ts, value
                            5, 100
                            15, 200
                            25, 300
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            assertEquals(2, result.rowCount(), "Only active rows (id=1,3) participate");
            var byId = collectResults(result, "id", "value");
            assertTrue(byId.containsKey(1));
            assertTrue(byId.containsKey(3));
            assertFalse(byId.containsKey(2), "Inactive id=2 filtered out before join");
            assertEquals(100L, ((Number) byId.get(1)).longValue());
            assertEquals(300L, ((Number) byId.get(3)).longValue());
        }

        @Test
        @DisplayName("asOfJoin then sort — verifies sort order with values")
        void testAsOfJoinThenSort() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 30
                        2, 10
                        3, 20
                    #->asOfJoin(
                        #TDS
                            ts, value
                            5, 100
                            15, 200
                            25, 300
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )->sort(~event_time->ascending())""");
            assertEquals(3, result.rowCount());
            int etIdx = columnIndex(result, "event_time");
            int valIdx = columnIndex(result, "value");
            // Sorted by event_time ascending: 10, 20, 30
            assertEquals(10, ((Number) result.rows().get(0).get(etIdx)).intValue());
            assertEquals(20, ((Number) result.rows().get(1).get(etIdx)).intValue());
            assertEquals(30, ((Number) result.rows().get(2).get(etIdx)).intValue());
            // Verify corresponding values
            assertEquals(100L, ((Number) result.rows().get(0).get(valIdx)).longValue(), "et=10 → value=100");
            assertEquals(200L, ((Number) result.rows().get(1).get(valIdx)).longValue(), "et=20 → value=200");
            assertEquals(300L, ((Number) result.rows().get(2).get(valIdx)).longValue(), "et=30 → value=300");
        }

        @Test
        @DisplayName("asOfJoin then extend — computes derived column from join result")
        void testAsOfJoinThenExtend() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time, qty
                        1, 10, 5
                        2, 20, 3
                    #->asOfJoin(
                        #TDS
                            ts, price
                            5, 100
                            15, 200
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )->extend(~total: x | $x.qty * $x.price)""");
            assertEquals(2, result.rowCount());
            var byId = collectResults(result, "id", "total");
            // id=1: qty=5 * price=100 = 500
            assertEquals(500L, ((Number) byId.get(1)).longValue());
            // id=2: qty=3 * price=200 = 600
            assertEquals(600L, ((Number) byId.get(2)).longValue());
        }
    }

    // ==================== Edge Cases ====================

    @Nested
    @DisplayName("asOfJoin() edge cases")
    class EdgeCases {

        @Test
        @DisplayName("custom lambda param names")
        void testCustomLambdaParamNames() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 10
                        2, 20
                    #->asOfJoin(
                        #TDS
                            ts, value
                            5, 100
                            15, 200
                        #,
                        {trade, quote | $trade.event_time > $quote.ts}
                    )""");
            assertEquals(2, result.rowCount());
            var byId = collectResults(result, "id", "value");
            assertEquals(100L, ((Number) byId.get(1)).longValue());
            assertEquals(200L, ((Number) byId.get(2)).longValue());
        }

        @Test
        @DisplayName("both sides empty → empty result")
        void testBothSidesEmpty() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time:Integer
                    #->asOfJoin(
                        #TDS
                            ts:Integer, value
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            assertTrue(result.rows().isEmpty());
        }

        @Test
        @DisplayName("multiple right matches — picks closest, verifies value")
        void testMultipleMatchesPicksClosest() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time:DateTime
                        1, %2024-01-15T12:00:00
                    #->asOfJoin(
                        #TDS
                            ts:DateTime, value
                            %2024-01-15T10:00:00, 100
                            %2024-01-15T11:00:00, 200
                            %2024-01-15T11:30:00, 300
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            assertEquals(1, result.rowCount());
            int valueIdx = columnIndex(result, "value");
            assertEquals(300, ((Number) result.rows().get(0).get(valueIdx)).intValue(),
                    "ASOF should pick closest match (ts=11:30, value=300)");
        }

        @Test
        @DisplayName("all left rows match same right row — all get same value")
        void testAllMatchSameRightRow() throws SQLException {
            var result = executeRelation("""
                    #TDS
                        id, event_time
                        1, 20
                        2, 30
                        3, 40
                    #->asOfJoin(
                        #TDS
                            ts, value
                            10, 999
                        #,
                        {l, r | $l.event_time > $r.ts}
                    )""");
            assertEquals(3, result.rowCount(), "All 3 rows match the single right row");
            int valueIdx = columnIndex(result, "value");
            for (var row : result.rows()) {
                assertEquals(999L, ((Number) row.get(valueIdx)).longValue(),
                        "Every row should get value=999");
            }
        }
    }

    // ==================== Utilities ====================

    /** Collects results into a map keyed by the group column value. */
    private <K> Map<K, Object> collectResults(
            ExecutionResult result, String keyCol, String valCol) {
        int keyIdx = columnIndex(result, keyCol);
        int valIdx = columnIndex(result, valCol);
        Map<K, Object> map = new HashMap<>();
        for (var row : result.rows()) {
            @SuppressWarnings("unchecked")
            K key = (K) row.get(keyIdx);
            map.put(key, row.get(valIdx));
        }
        return map;
    }

    /** Finds column index by name. */
    private int columnIndex(ExecutionResult result, String name) {
        for (int i = 0; i < result.columns().size(); i++) {
            if (name.equals(result.columns().get(i).name()))
                return i;
        }
        throw new AssertionError("Column '" + name + "' not found in " +
                result.columns().stream().map(c -> c.name()).toList());
    }
}
