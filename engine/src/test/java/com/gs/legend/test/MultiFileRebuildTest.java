package com.gs.legend.test;

import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.PlanExecutor;
import com.gs.legend.model.ParseCache;
import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.plan.PlanGenerator;
import com.gs.legend.plan.SingleExecutionPlan;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration test for multi-file rebuild with ParseCache.
 *
 * Simulates the real editor workflow:
 *   1. Load N separate "files" (source strings), build model, run queries
 *   2. MODIFY some files (add new classes/properties, remove existing ones)
 *   3. Rebuild model from ALL files — unchanged files hit cache, changed files re-parse
 *   4. Run queries that exercise the adds AND verify the removes are truly gone
 *   5. Assert cache behavior: only changed files caused new parse entries
 */
@DisplayName("Multi-File Rebuild Integration")
class MultiFileRebuildTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        ParseCache.global().clear();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    // ==================== File Sources (V1 — initial state) ====================

    static final String FILE_ENUMS = """
            Enum trade::Status { OPEN, CLOSED, PENDING, CANCELLED }
            Enum trade::Side { BUY, SELL }
            """;

    static final String FILE_CLASSES_V1 = """
            Class trade::Trader
            {
                name: String[1];
                desk: String[1];
                active: Boolean[1];
            }

            Class trade::Trade
            {
                tradeId: String[1];
                notional: Float[1];
                status: trade::Status[1];
                side: trade::Side[1];
            }

            Class trade::Instrument
            {
                ticker: String[1];
                name: String[1];
                currency: String[1];
            }
            """;

    static final String FILE_ASSOCIATIONS_V1 = """
            Association trade::Trade_Trader
            {
                trader: trade::Trader[1];
                trades: trade::Trade[*];
            }

            Association trade::Trade_Instrument
            {
                instrument: trade::Instrument[1];
                instrumentTrades: trade::Trade[*];
            }
            """;

    static final String FILE_STORE_V1 = """
            Database store::TradeDB
            (
                Table T_TRADER (ID INT PRIMARY KEY, NAME VARCHAR(100), DESK VARCHAR(50), ACTIVE BOOLEAN)
                Table T_TRADE (ID INT PRIMARY KEY, TRADE_ID VARCHAR(50), NOTIONAL DOUBLE, STATUS VARCHAR(20), SIDE VARCHAR(10), TRADER_ID INT, INSTRUMENT_ID INT)
                Table T_INSTRUMENT (ID INT PRIMARY KEY, TICKER VARCHAR(20), NAME VARCHAR(100), CURRENCY VARCHAR(10))
                Join Trade_Trader(T_TRADE.TRADER_ID = T_TRADER.ID)
                Join Trade_Instrument(T_TRADE.INSTRUMENT_ID = T_INSTRUMENT.ID)
            )
            """;

    static final String FILE_MAPPING_V1 = """
            Mapping trade::TradeMapping
            (
                trade::Trader: Relational
                {
                    ~mainTable [store::TradeDB] T_TRADER
                    name: [store::TradeDB] T_TRADER.NAME,
                    desk: [store::TradeDB] T_TRADER.DESK,
                    active: [store::TradeDB] T_TRADER.ACTIVE
                }

                trade::Trade: Relational
                {
                    ~mainTable [store::TradeDB] T_TRADE
                    tradeId: [store::TradeDB] T_TRADE.TRADE_ID,
                    notional: [store::TradeDB] T_TRADE.NOTIONAL,
                    status: [store::TradeDB] T_TRADE.STATUS,
                    side: [store::TradeDB] T_TRADE.SIDE
                }

                trade::Instrument: Relational
                {
                    ~mainTable [store::TradeDB] T_INSTRUMENT
                    ticker: [store::TradeDB] T_INSTRUMENT.TICKER,
                    name: [store::TradeDB] T_INSTRUMENT.NAME,
                    currency: [store::TradeDB] T_INSTRUMENT.CURRENCY
                }

                trade::Trade_Trader: Relational { AssociationMapping (
                    trader: [store::TradeDB]@Trade_Trader,
                    trades: [store::TradeDB]@Trade_Trader
                ) }

                trade::Trade_Instrument: Relational { AssociationMapping (
                    instrument: [store::TradeDB]@Trade_Instrument,
                    instrumentTrades: [store::TradeDB]@Trade_Instrument
                ) }
            )
            """;

    static final String FILE_RUNTIME = """
            RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
            Runtime trade::RT { mappings: [ trade::TradeMapping ]; connections: [ store::TradeDB: [ environment: store::Conn ] ]; }
            """;

    // ==================== File Sources (V2 — modified) ====================

    /** V2: ADD email property to Trader, ADD rating property to Trade */
    static final String FILE_CLASSES_V2 = """
            Class trade::Trader
            {
                name: String[1];
                desk: String[1];
                active: Boolean[1];
                email: String[1];
            }

            Class trade::Trade
            {
                tradeId: String[1];
                notional: Float[1];
                status: trade::Status[1];
                side: trade::Side[1];
                rating: Integer[1];
            }

            Class trade::Counterparty
            {
                cpId: String[1];
                legalName: String[1];
                country: String[1];
            }
            """;
    // NOTE: Instrument class REMOVED from V2 — it no longer exists

    /** V2: REMOVE Trade_Instrument association, ADD Trade_Counterparty */
    static final String FILE_ASSOCIATIONS_V2 = """
            Association trade::Trade_Trader
            {
                trader: trade::Trader[1];
                trades: trade::Trade[*];
            }

            Association trade::Trade_Counterparty
            {
                counterparty: trade::Counterparty[1];
                cpTrades: trade::Trade[*];
            }
            """;

    /** V2: Store gains T_COUNTERPARTY, loses Trade_Instrument join */
    static final String FILE_STORE_V2 = """
            Database store::TradeDB
            (
                Table T_TRADER (ID INT PRIMARY KEY, NAME VARCHAR(100), DESK VARCHAR(50), ACTIVE BOOLEAN, EMAIL VARCHAR(200))
                Table T_TRADE (ID INT PRIMARY KEY, TRADE_ID VARCHAR(50), NOTIONAL DOUBLE, STATUS VARCHAR(20), SIDE VARCHAR(10), TRADER_ID INT, CP_ID INT, RATING INT)
                Table T_COUNTERPARTY (ID INT PRIMARY KEY, CP_ID VARCHAR(50), LEGAL_NAME VARCHAR(200), COUNTRY VARCHAR(50))
                Join Trade_Trader(T_TRADE.TRADER_ID = T_TRADER.ID)
                Join Trade_Counterparty(T_TRADE.CP_ID = T_COUNTERPARTY.ID)
            )
            """;

    /** V2: Mapping reflects new schema — no Instrument, adds Counterparty + new columns */
    static final String FILE_MAPPING_V2 = """
            Mapping trade::TradeMapping
            (
                trade::Trader: Relational
                {
                    ~mainTable [store::TradeDB] T_TRADER
                    name: [store::TradeDB] T_TRADER.NAME,
                    desk: [store::TradeDB] T_TRADER.DESK,
                    active: [store::TradeDB] T_TRADER.ACTIVE,
                    email: [store::TradeDB] T_TRADER.EMAIL
                }

                trade::Trade: Relational
                {
                    ~mainTable [store::TradeDB] T_TRADE
                    tradeId: [store::TradeDB] T_TRADE.TRADE_ID,
                    notional: [store::TradeDB] T_TRADE.NOTIONAL,
                    status: [store::TradeDB] T_TRADE.STATUS,
                    side: [store::TradeDB] T_TRADE.SIDE,
                    rating: [store::TradeDB] T_TRADE.RATING
                }

                trade::Counterparty: Relational
                {
                    ~mainTable [store::TradeDB] T_COUNTERPARTY
                    cpId: [store::TradeDB] T_COUNTERPARTY.CP_ID,
                    legalName: [store::TradeDB] T_COUNTERPARTY.LEGAL_NAME,
                    country: [store::TradeDB] T_COUNTERPARTY.COUNTRY
                }

                trade::Trade_Trader: Relational { AssociationMapping (
                    trader: [store::TradeDB]@Trade_Trader,
                    trades: [store::TradeDB]@Trade_Trader
                ) }

                trade::Trade_Counterparty: Relational { AssociationMapping (
                    counterparty: [store::TradeDB]@Trade_Counterparty,
                    cpTrades: [store::TradeDB]@Trade_Counterparty
                ) }
            )
            """;

    // ==================== Database Setup ====================

    private void setupV1Tables() throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("DROP TABLE IF EXISTS T_TRADE");
            s.execute("DROP TABLE IF EXISTS T_INSTRUMENT");
            s.execute("DROP TABLE IF EXISTS T_TRADER");
            s.execute("DROP TABLE IF EXISTS T_COUNTERPARTY");
            s.execute("CREATE TABLE T_TRADER (ID INT PRIMARY KEY, NAME VARCHAR(100), DESK VARCHAR(50), ACTIVE BOOLEAN)");
            s.execute("CREATE TABLE T_TRADE (ID INT PRIMARY KEY, TRADE_ID VARCHAR(50), NOTIONAL DOUBLE, STATUS VARCHAR(20), SIDE VARCHAR(10), TRADER_ID INT, INSTRUMENT_ID INT)");
            s.execute("CREATE TABLE T_INSTRUMENT (ID INT PRIMARY KEY, TICKER VARCHAR(20), NAME VARCHAR(100), CURRENCY VARCHAR(10))");
            s.execute("INSERT INTO T_TRADER VALUES (1, 'Alice', 'EQ', true), (2, 'Bob', 'FI', true), (3, 'Charlie', 'EQ', false)");
            s.execute("INSERT INTO T_INSTRUMENT VALUES (1, 'AAPL', 'Apple Inc', 'USD'), (2, 'GOOGL', 'Alphabet', 'USD')");
            s.execute("INSERT INTO T_TRADE VALUES (1, 'T001', 1000000.0, 'OPEN', 'BUY', 1, 1), (2, 'T002', 500000.0, 'CLOSED', 'SELL', 1, 2), (3, 'T003', 750000.0, 'OPEN', 'BUY', 2, 1), (4, 'T004', 250000.0, 'PENDING', 'SELL', 3, 2)");
        }
    }

    private void setupV2Tables() throws SQLException {
        try (Statement s = conn.createStatement()) {
            // Drop V1 tables and recreate with V2 schema
            s.execute("DROP TABLE IF EXISTS T_TRADE");
            s.execute("DROP TABLE IF EXISTS T_INSTRUMENT");
            s.execute("DROP TABLE IF EXISTS T_TRADER");
            s.execute("CREATE TABLE T_TRADER (ID INT PRIMARY KEY, NAME VARCHAR(100), DESK VARCHAR(50), ACTIVE BOOLEAN, EMAIL VARCHAR(200))");
            s.execute("CREATE TABLE T_TRADE (ID INT PRIMARY KEY, TRADE_ID VARCHAR(50), NOTIONAL DOUBLE, STATUS VARCHAR(20), SIDE VARCHAR(10), TRADER_ID INT, CP_ID INT, RATING INT)");
            s.execute("CREATE TABLE T_COUNTERPARTY (ID INT PRIMARY KEY, CP_ID VARCHAR(50), LEGAL_NAME VARCHAR(200), COUNTRY VARCHAR(50))");
            s.execute("INSERT INTO T_TRADER VALUES (1, 'Alice', 'EQ', true, 'alice@gs.com'), (2, 'Bob', 'FI', true, 'bob@gs.com')");
            s.execute("INSERT INTO T_COUNTERPARTY VALUES (1, 'CP001', 'Goldman Sachs', 'US'), (2, 'CP002', 'JP Morgan', 'US'), (3, 'CP003', 'Deutsche Bank', 'DE')");
            s.execute("INSERT INTO T_TRADE VALUES (1, 'T001', 1000000.0, 'OPEN', 'BUY', 1, 1, 5), (2, 'T002', 500000.0, 'CLOSED', 'SELL', 1, 2, 3), (3, 'T003', 750000.0, 'OPEN', 'BUY', 2, 3, 4)");
        }
    }

    // ==================== Helpers ====================

    /**
     * Builds a PureModelBuilder by calling addSource() once per file,
     * exactly like the real editor workflow where each .pure file is
     * a separate source string.
     */
    private PureModelBuilder buildModel(String... files) {
        var builder = new PureModelBuilder();
        for (String file : files) {
            builder.addSource(file);
        }
        return builder;
    }

    /**
     * Executes a query against a pre-built model using the real pipeline:
     * PlanGenerator.generate → PlanExecutor.execute
     */
    private ExecutionResult execute(PureModelBuilder model, String query) throws SQLException {
        SingleExecutionPlan plan = PlanGenerator.generate(model, query, "trade::RT");
        return PlanExecutor.execute(plan, conn);
    }

    /** Extracts all values from a named column as strings. */
    private List<String> col(ExecutionResult result, String colName) {
        int idx = -1;
        for (int i = 0; i < result.columns().size(); i++) {
            if (result.columns().get(i).name().equals(colName)) { idx = i; break; }
        }
        if (idx < 0) throw new IllegalArgumentException("No column: " + colName);
        int fi = idx;
        return result.rows().stream()
                .map(r -> r.get(fi) == null ? null : r.get(fi).toString())
                .collect(Collectors.toList());
    }

    // ==================== Tests ====================

    @Test
    @DisplayName("V1: per-file addSource builds and queries execute correctly")
    void v1_fullModelWorks() throws SQLException {
        setupV1Tables();
        var model = buildModel(FILE_ENUMS, FILE_CLASSES_V1, FILE_ASSOCIATIONS_V1,
                FILE_STORE_V1, FILE_MAPPING_V1, FILE_RUNTIME);

        // Cache: 6 files should produce exactly 6 cache entries
        assertEquals(6, ParseCache.global().size(),
                "6 files should produce 6 cache entries");

        // Query traders — verify exact names and desks
        var traders = execute(model,
                "trade::Trader.all()->project(~[name:t|$t.name, desk:t|$t.desk])");
        assertEquals(3, traders.rows().size(), "Should have 3 traders");
        assertEquals(Set.of("Alice", "Bob", "Charlie"), Set.copyOf(col(traders, "name")));
        assertEquals(Set.of("EQ", "FI"), Set.copyOf(col(traders, "desk")));

        // Query trades — verify IDs and sides
        var trades = execute(model,
                "trade::Trade.all()->project(~[tradeId:t|$t.tradeId, side:t|$t.side])");
        assertEquals(4, trades.rows().size(), "Should have 4 trades");
        assertEquals(Set.of("T001", "T002", "T003", "T004"), Set.copyOf(col(trades, "tradeId")));
        assertEquals(Set.of("BUY", "SELL"), Set.copyOf(col(trades, "side")));

        // Query instruments — verify tickers and currencies
        var instruments = execute(model,
                "trade::Instrument.all()->project(~[ticker:i|$i.ticker, name:i|$i.name, currency:i|$i.currency])");
        assertEquals(2, instruments.rows().size(), "Should have 2 instruments");
        assertEquals(Set.of("AAPL", "GOOGL"), Set.copyOf(col(instruments, "ticker")));
        assertTrue(col(instruments, "currency").stream().allMatch("USD"::equals),
                "All instruments should be USD");

        // Association navigation: trade → trader — verify join correctness
        var tradeTraders = execute(model,
                "trade::Trade.all()->project(~[tradeId:t|$t.tradeId, traderName:t|$t.trader.name])");
        assertEquals(4, tradeTraders.rows().size());
        var traderNames = col(tradeTraders, "traderName");
        assertEquals(Set.of("Alice", "Bob", "Charlie"), Set.copyOf(traderNames));
        // Alice has 2 trades (T001, T002)
        assertEquals(2, traderNames.stream().filter("Alice"::equals).count(),
                "Alice should have 2 trades");

        // Association navigation: trade → instrument — verify join correctness
        var tradeInstruments = execute(model,
                "trade::Trade.all()->project(~[tradeId:t|$t.tradeId, ticker:t|$t.instrument.ticker])");
        assertEquals(4, tradeInstruments.rows().size());
        assertEquals(Set.of("AAPL", "GOOGL"), Set.copyOf(col(tradeInstruments, "ticker")));

        // Filter: active traders only — verify exactly Alice and Bob
        var activeTraders = execute(model,
                "trade::Trader.all()->filter(t|$t.active == true)->project(~[name:t|$t.name])");
        assertEquals(2, activeTraders.rows().size(), "Should have 2 active traders");
        assertEquals(Set.of("Alice", "Bob"), Set.copyOf(col(activeTraders, "name")));

        System.out.println("V1: All 6 queries passed with value assertions (6 files, 6 addSource calls)");
    }

    @Test
    @DisplayName("V2 rebuild: add/remove classes, properties, associations — queries reflect changes")
    void v2_rebuildWithChanges() throws SQLException {
        setupV1Tables();

        // --- Phase 1: Build V1 per-file and warm the cache ---
        ParseCache.global().clear();

        var v1Model = buildModel(FILE_ENUMS, FILE_CLASSES_V1, FILE_ASSOCIATIONS_V1,
                FILE_STORE_V1, FILE_MAPPING_V1, FILE_RUNTIME);

        var v1Result = execute(v1Model,
                "trade::Instrument.all()->project(~[ticker:i|$i.ticker])");
        assertEquals(2, v1Result.rows().size(), "V1 should have instruments");

        int cacheAfterV1 = ParseCache.global().size();
        assertEquals(6, cacheAfterV1, "V1 should cache exactly 6 files");
        System.out.println("Cache after V1 build: " + cacheAfterV1 + " entries (one per file)");

        // --- Phase 2: Rebuild with V2 files (per-file addSource) ---
        // Changed: FILE_CLASSES_V2, FILE_ASSOCIATIONS_V2, FILE_STORE_V2, FILE_MAPPING_V2
        // Unchanged: FILE_ENUMS, FILE_RUNTIME — these should hit cache
        setupV2Tables();

        int cacheBeforeV2 = ParseCache.global().size();
        long t0 = System.nanoTime();
        var v2Builder = buildModel(FILE_ENUMS, FILE_CLASSES_V2, FILE_ASSOCIATIONS_V2,
                FILE_STORE_V2, FILE_MAPPING_V2, FILE_RUNTIME);
        long rebuildMs = (System.nanoTime() - t0) / 1_000_000;
        int cacheAfterV2 = ParseCache.global().size();

        System.out.println("V2 rebuild: " + rebuildMs + " ms");
        System.out.println("Cache before V2: " + cacheBeforeV2 + ", after V2: " + cacheAfterV2);
        int newEntries = cacheAfterV2 - cacheBeforeV2;
        System.out.println("New cache entries: " + newEntries
                + " (expected: 4 changed files, 2 cache hits)");
        assertEquals(4, newEntries,
                "Should have exactly 4 new cache entries for the 4 changed files");

        // --- Verify V2 model structure ---

        // NEW class: Counterparty exists
        assertTrue(v2Builder.findClass("trade::Counterparty").isPresent(),
                "V2 should have Counterparty class");

        // REMOVED class: Instrument gone
        assertTrue(v2Builder.findClass("trade::Instrument").isEmpty(),
                "V2 should NOT have Instrument class (removed)");

        // NEW property: Trader.email
        var traderClass = v2Builder.findClass("trade::Trader").orElseThrow();
        assertTrue(traderClass.properties().stream().anyMatch(p -> p.name().equals("email")),
                "V2 Trader should have email property");

        // NEW property: Trade.rating
        var tradeClass = v2Builder.findClass("trade::Trade").orElseThrow();
        assertTrue(tradeClass.properties().stream().anyMatch(p -> p.name().equals("rating")),
                "V2 Trade should have rating property");

        // --- Execute V2 queries against V2 data ---

        // Query new Counterparty class — verify exact values
        var cps = execute(v2Builder, "trade::Counterparty.all()->project(~[cpId:c|$c.cpId, legalName:c|$c.legalName, country:c|$c.country])");
        assertEquals(3, cps.rows().size(), "Should have 3 counterparties");
        assertEquals(Set.of("CP001", "CP002", "CP003"), Set.copyOf(col(cps, "cpId")));
        assertEquals(Set.of("Goldman Sachs", "JP Morgan", "Deutsche Bank"),
                Set.copyOf(col(cps, "legalName")));
        assertEquals(Set.of("US", "DE"), Set.copyOf(col(cps, "country")));

        // Query new property: Trader.email — verify exact emails
        var traderEmails = execute(v2Builder,
                "trade::Trader.all()->project(~[name:t|$t.name, email:t|$t.email])");
        assertEquals(2, traderEmails.rows().size());
        assertEquals(Set.of("alice@gs.com", "bob@gs.com"), Set.copyOf(col(traderEmails, "email")));
        assertEquals(Set.of("Alice", "Bob"), Set.copyOf(col(traderEmails, "name")));

        // Query new property: Trade.rating — verify values
        var tradeRatings = execute(v2Builder,
                "trade::Trade.all()->project(~[tradeId:t|$t.tradeId, rating:t|$t.rating])");
        assertEquals(3, tradeRatings.rows().size());
        assertEquals(Set.of("T001", "T002", "T003"), Set.copyOf(col(tradeRatings, "tradeId")));
        assertEquals(Set.of("5", "3", "4"), Set.copyOf(col(tradeRatings, "rating")));

        // Association navigation: trade → counterparty (NEW) — verify join values
        var tradeCps = execute(v2Builder,
                "trade::Trade.all()->project(~[tradeId:t|$t.tradeId, cpName:t|$t.counterparty.legalName])");
        assertEquals(3, tradeCps.rows().size());
        assertEquals(Set.of("Goldman Sachs", "JP Morgan", "Deutsche Bank"),
                Set.copyOf(col(tradeCps, "cpName")));

        // Filter on new property: high-rated trades — verify exactly which ones
        var highRated = execute(v2Builder,
                "trade::Trade.all()->filter(t|$t.rating >= 4)->project(~[tradeId:t|$t.tradeId, rating:t|$t.rating])");
        assertEquals(2, highRated.rows().size(), "Should have 2 trades with rating >= 4");
        assertEquals(Set.of("T001", "T003"), Set.copyOf(col(highRated, "tradeId")));

        // --- Verify REMOVED things fail in V2 ---

        // Instrument class removed
        assertThrows(Exception.class, () ->
                        execute(v2Builder, "trade::Instrument.all()->project(~[ticker:i|$i.ticker])"),
                "V2 should fail on Instrument.all() — class was removed");

        // Trade_Instrument association removed — instrument navigation should fail
        assertThrows(Exception.class, () ->
                        execute(v2Builder, "trade::Trade.all()->project(~[tradeId:t|$t.tradeId, ticker:t|$t.instrument.ticker])"),
                "V2 should fail on trade.instrument — association was removed");

        // REMOVED property check: V2 Trader should NOT have 'active' removed... wait, it still has active.
        // But Instrument class is fully gone, so check its properties don't leak
        assertFalse(v2Builder.findClass("trade::Instrument").isPresent(),
                "Instrument class should be completely absent from V2 model");

        System.out.println("V2: All add/remove queries verified");
    }

    @Test
    @DisplayName("Cache correctness: unchanged files produce identical parse results")
    void cacheIdentity() {
        ParseCache.global().clear();

        // Parse all V1 files individually
        var enumParsed1 = ParseCache.global().getOrParse(FILE_ENUMS);
        var runtimeParsed1 = ParseCache.global().getOrParse(FILE_RUNTIME);
        int sizeAfterV1 = ParseCache.global().size();

        // Now parse V2 files (4 changed, 2 unchanged)
        ParseCache.global().getOrParse(FILE_CLASSES_V2);
        ParseCache.global().getOrParse(FILE_ASSOCIATIONS_V2);
        ParseCache.global().getOrParse(FILE_STORE_V2);
        ParseCache.global().getOrParse(FILE_MAPPING_V2);

        // Re-parse unchanged files — must return same object (identity, not equality)
        var enumParsed2 = ParseCache.global().getOrParse(FILE_ENUMS);
        var runtimeParsed2 = ParseCache.global().getOrParse(FILE_RUNTIME);

        assertSame(enumParsed1, enumParsed2, "Enum file unchanged — must be same object");
        assertSame(runtimeParsed1, runtimeParsed2, "Runtime file unchanged — must be same object");

        // V2 should have added exactly 4 new entries (the changed files)
        int newEntries = ParseCache.global().size() - sizeAfterV1;
        assertEquals(4, newEntries,
                "Should have exactly 4 new cache entries for the 4 changed files");

        System.out.println("Cache identity verified: 2 unchanged files hit cache, 4 changed files re-parsed");
    }

    @Test
    @DisplayName("Three-pass edit simulation: V1 → V2 → V3 (revert partial changes)")
    void threePassEditSimulation() throws SQLException {
        String[] v1Files = { FILE_ENUMS, FILE_CLASSES_V1, FILE_ASSOCIATIONS_V1,
                FILE_STORE_V1, FILE_MAPPING_V1, FILE_RUNTIME };
        String[] v2Files = { FILE_ENUMS, FILE_CLASSES_V2, FILE_ASSOCIATIONS_V2,
                FILE_STORE_V2, FILE_MAPPING_V2, FILE_RUNTIME };

        // --- V1 ---
        setupV1Tables();
        var v1Model = buildModel(v1Files);

        var v1Instruments = execute(v1Model,
                "trade::Instrument.all()->project(~[ticker:i|$i.ticker])");
        assertEquals(2, v1Instruments.rows().size());

        // --- V2 (Instrument removed) ---
        setupV2Tables();
        var v2Model = buildModel(v2Files);

        assertThrows(Exception.class, () ->
                execute(v2Model, "trade::Instrument.all()->project(~[ticker:i|$i.ticker])"));

        // --- V3: Revert to V1 (Instrument restored) ---
        // All 6 V1 files were cached in the V1 build above.
        // Going back should be 6 cache hits, 0 re-parses.
        setupV1Tables();
        int cacheBefore = ParseCache.global().size();

        long t0 = System.nanoTime();
        var v3Model = buildModel(v1Files);
        long revertMs = (System.nanoTime() - t0) / 1_000_000;

        assertEquals(cacheBefore, ParseCache.global().size(),
                "Reverting to V1 should not add new cache entries (all 6 files cached)");
        System.out.println("V3 revert (6 cache hits, 0 re-parses): " + revertMs + " ms");

        // Instrument should be back
        var v3Instruments = execute(v3Model,
                "trade::Instrument.all()->project(~[ticker:i|$i.ticker])");
        assertEquals(2, v3Instruments.rows().size(), "V3 revert should restore instruments");

        System.out.println("Three-pass edit simulation passed: V1 → V2 (remove) → V3 (revert)");
    }

    @Test
    @DisplayName("Broken file recovery: syntax error in one file does not poison cache or block recovery")
    void brokenFileRecovery() throws SQLException {
        // --- Phase 1: Build V1 successfully, warm the cache ---
        setupV1Tables();
        var v1Model = buildModel(FILE_ENUMS, FILE_CLASSES_V1, FILE_ASSOCIATIONS_V1,
                FILE_STORE_V1, FILE_MAPPING_V1, FILE_RUNTIME);
        assertEquals(6, ParseCache.global().size(), "V1 should cache 6 files");

        // Verify V1 works
        var traders = execute(v1Model,
                "trade::Trader.all()->project(~[name:t|$t.name])");
        assertEquals(3, traders.rows().size());

        // Snapshot cache references for unchanged files
        var enumCached = ParseCache.global().getOrParse(FILE_ENUMS);
        var runtimeCached = ParseCache.global().getOrParse(FILE_RUNTIME);
        var assocCached = ParseCache.global().getOrParse(FILE_ASSOCIATIONS_V1);
        var storeCached = ParseCache.global().getOrParse(FILE_STORE_V1);
        var mappingCached = ParseCache.global().getOrParse(FILE_MAPPING_V1);
        int cacheBeforeBroken = ParseCache.global().size();

        // --- Phase 2: User introduces a syntax error in the classes file ---
        String BROKEN_CLASSES = """
                Class trade::Trader
                {
                    name: String[1];
                    desk String[1];
                }
                """;
        // ^^^^^ missing colon after "desk" — parse error

        // Rebuild should fail
        assertThrows(Exception.class, () ->
                        buildModel(FILE_ENUMS, BROKEN_CLASSES, FILE_ASSOCIATIONS_V1,
                                FILE_STORE_V1, FILE_MAPPING_V1, FILE_RUNTIME),
                "Broken syntax should cause build to fail");

        // Cache should NOT have grown — broken source must not be cached
        assertEquals(cacheBeforeBroken, ParseCache.global().size(),
                "Broken file should not add a cache entry");

        // Unchanged files must still be the same cached objects (not poisoned)
        assertSame(enumCached, ParseCache.global().getOrParse(FILE_ENUMS),
                "Enum file cache should survive broken rebuild");
        assertSame(runtimeCached, ParseCache.global().getOrParse(FILE_RUNTIME),
                "Runtime file cache should survive broken rebuild");
        assertSame(assocCached, ParseCache.global().getOrParse(FILE_ASSOCIATIONS_V1),
                "Association file cache should survive broken rebuild");
        assertSame(storeCached, ParseCache.global().getOrParse(FILE_STORE_V1),
                "Store file cache should survive broken rebuild");
        assertSame(mappingCached, ParseCache.global().getOrParse(FILE_MAPPING_V1),
                "Mapping file cache should survive broken rebuild");

        // --- Phase 3: User fixes the file ---
        // Use the original V1 classes (already cached from Phase 1)
        int cacheBeforeFix = ParseCache.global().size();
        var fixedModel = buildModel(FILE_ENUMS, FILE_CLASSES_V1, FILE_ASSOCIATIONS_V1,
                FILE_STORE_V1, FILE_MAPPING_V1, FILE_RUNTIME);

        // No new cache entries — all 6 files were already cached before the break
        assertEquals(cacheBeforeFix, ParseCache.global().size(),
                "Fix should hit cache for all 6 files (no new entries)");

        // Queries work again
        var fixedTraders = execute(fixedModel,
                "trade::Trader.all()->project(~[name:t|$t.name])");
        assertEquals(3, fixedTraders.rows().size());
        assertEquals(Set.of("Alice", "Bob", "Charlie"), Set.copyOf(col(fixedTraders, "name")));

        System.out.println("Broken file recovery passed: error → no cache poison → fix → full recovery");
    }
}
