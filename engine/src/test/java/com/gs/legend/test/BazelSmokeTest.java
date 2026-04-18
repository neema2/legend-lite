package com.gs.legend.test;

import com.gs.legend.model.PureModelBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Bazel smoke test corpus — see docs/BAZEL_IMPLEMENTATION_PLAN.md §1.2 and §4.7.
 *
 * <p>Two-project regression canary for the Bazel cross-project dependency work.
 * Each phase (A: typeFqn, B: element serialization, C: lazy loading + validateElement)
 * must keep these tests green.
 *
 * <p>Phase A baseline (this file): load both projects into a combined builder and assert
 * cross-project refs resolve end-to-end using the current eager loader. Phase C will add
 * the lazy-loading scenarios enumerated in §4.7 of the implementation plan.
 */
class BazelSmokeTest {

    private static final String REFDATA_MODEL = "bazel_smoke/refdata/model.pure";
    private static final String REFDATA_IMPL = "bazel_smoke/refdata/impl.pure";
    private static final String TRADING_MODEL = "bazel_smoke/trading/model.pure";
    private static final String TRADING_IMPL = "bazel_smoke/trading/impl.pure";

    @Test
    @DisplayName("Smoke corpus parses and cross-project refs resolve (Phase A baseline)")
    void corpusParsesWithCrossProjectRefs() {
        PureModelBuilder builder = new PureModelBuilder()
                .addSource(loadResource(REFDATA_MODEL))
                .addSource(loadResource(REFDATA_IMPL))
                .addSource(loadResource(TRADING_MODEL))
                .addSource(loadResource(TRADING_IMPL));

        // --- refdata project: all expected elements present ---
        assertNotNull(builder.getClass("refdata::Categorized"), "refdata::Categorized must be registered");
        assertNotNull(builder.getClass("refdata::Region"), "refdata::Region must be registered");
        assertNotNull(builder.getClass("refdata::Sector"), "refdata::Sector must be registered");
        assertTrue(builder.findEnum("refdata::Rating").isPresent(), "refdata::Rating enum must be registered");
        assertTrue(builder.getAssociation("refdata::SectorRegion").isPresent(),
                "refdata::SectorRegion association must be registered");
        assertFalse(builder.findFunction("refdata::formatSector").isEmpty(),
                "refdata::formatSector function must be registered");

        // --- trading project: all expected elements present ---
        assertNotNull(builder.getClass("trading::Trade"), "trading::Trade must be registered");
        assertNotNull(builder.getClass("trading::InternalTrade"),
                "trading::InternalTrade must be registered");
        assertTrue(builder.getAssociation("trading::TradeRegion").isPresent(),
                "trading::TradeRegion association must be registered");
        assertFalse(builder.findFunction("trading::tradeSummary").isEmpty(),
                "trading::tradeSummary function must be registered");
        assertFalse(builder.findFunction("trading::sectorRegionCode").isEmpty(),
                "trading::sectorRegionCode function must be registered");

        // --- cross-project refs: class-typed property resolves to refdata FQN ---
        var trade = builder.getClass("trading::Trade");
        var sectorProp = trade.findProperty("sector", builder);
        assertTrue(sectorProp.isPresent(), "trading::Trade.sector property must exist");
        assertTrue(sectorProp.get().type() instanceof com.gs.legend.model.m3.Type.ClassType,
                "trading::Trade.sector must be a ClassType (not a primitive or enum)");
        assertEquals("refdata::Sector", sectorProp.get().typeFqn(),
                "trading::Trade.sector must resolve to refdata::Sector (cross-project)");

        // --- cross-project refs: enum-typed property (kind preserved as EnumRef) ---
        var ratingProp = trade.findProperty("rating", builder);
        assertTrue(ratingProp.isPresent(), "trading::Trade.rating property must exist");
        assertTrue(ratingProp.get().type() instanceof com.gs.legend.model.m3.Type.EnumType,
                "trading::Trade.rating must be an EnumType (enum-vs-class distinction preserved)");
        assertEquals("refdata::Rating", ratingProp.get().typeFqn(),
                "trading::Trade.rating must resolve to refdata::Rating");

        // --- cross-project refs: superclass resolves across projects ---
        var internalTrade = builder.getClass("trading::InternalTrade");
        assertFalse(internalTrade.superClassFqns().isEmpty(),
                "trading::InternalTrade must have a resolved superclass FQN");
        assertEquals("refdata::Categorized", internalTrade.superClassFqns().get(0),
                "trading::InternalTrade superclass must resolve to refdata::Categorized");

        // --- cross-project refs: inherited property reachable through superclass chain ---
        // PureClass.findProperty walks the superclass chain via ctx.findClass.
        var categoryInherited = internalTrade.findProperty("category", builder);
        assertTrue(categoryInherited.isPresent(),
                "trading::InternalTrade.category (inherited from refdata::Categorized) must be reachable");

        // --- cross-project refs: profile stereotype resolved from short name via import ---
        // trading::Trade declares <<RefDataProfile.rootEntity>> — a short reference to a
        // profile defined in refdata. NameResolver must canonicalize the profile FQN so
        // downstream consumers see "refdata::RefDataProfile", not "RefDataProfile".
        assertEquals(1, trade.stereotypes().size(),
                "trading::Trade must carry one stereotype (RefDataProfile.rootEntity)");
        var stereotype = trade.stereotypes().get(0);
        assertEquals("refdata::RefDataProfile", stereotype.profileName(),
                "Cross-project stereotype profile must canonicalize to refdata::RefDataProfile FQN");
        assertEquals("rootEntity", stereotype.stereotypeName());

        // --- cross-project refs: profile tagged value resolved from short name via import ---
        assertEquals(1, trade.taggedValues().size(),
                "trading::Trade must carry one tagged value (RefDataProfile.description)");
        var taggedValue = trade.taggedValues().get(0);
        assertEquals("refdata::RefDataProfile", taggedValue.profileName(),
                "Cross-project taggedValue profile must canonicalize to refdata::RefDataProfile FQN");
        assertEquals("description", taggedValue.tagName());
        assertEquals("A financial trade", taggedValue.value());

        // --- cross-project refs: database include resolved from short name via import ---
        // trading::TradingDB declares `include RefDB` — NameResolver must canonicalize the
        // included DB FQN to refdata::RefDB in the stored DatabaseDefinition.includes list.
        var tradingDb = builder.getDatabaseDefinition("trading::TradingDB");
        assertNotNull(tradingDb, "trading::TradingDB must be registered");
        assertEquals(java.util.List.of("refdata::RefDB"), tradingDb.includes(),
                "trading::TradingDB.includes must canonicalize to refdata::RefDB FQN");

        // --- cross-project refs: mapping include resolved from short name via import ---
        // trading::TradingMapping declares `include RefMapping` — NameResolver must
        // canonicalize the included-mapping FQN to refdata::RefMapping.
        var tradingMapping = builder.getMappingDefinition("trading::TradingMapping");
        assertNotNull(tradingMapping, "trading::TradingMapping must be registered");
        assertEquals(1, tradingMapping.includes().size(),
                "trading::TradingMapping must declare one include (refdata::RefMapping)");
        assertEquals("refdata::RefMapping",
                tradingMapping.includes().get(0).includedMappingPath(),
                "Mapping include path must canonicalize to refdata::RefMapping FQN");

        // --- cross-project refs: runtime mappings list resolved from short name ---
        // trading::TradingRuntime declares `mappings: [TradingMapping]` — canonicalize.
        var tradingRuntime = builder.getRuntime("trading::TradingRuntime");
        assertNotNull(tradingRuntime, "trading::TradingRuntime must be registered");
        assertEquals(java.util.List.of("trading::TradingMapping"), tradingRuntime.mappings(),
                "Runtime mappings list must canonicalize to FQN");

        // --- cross-project refs: runtime connection bindings (keys and values) ---
        // trading::TradingRuntime declares `connections: [RefDB: [c1: RefConn]]`.
        // Both the store key and the connection value must canonicalize to refdata::* FQNs.
        assertEquals(1, tradingRuntime.connectionBindings().size(),
                "TradingRuntime must have one connection binding");
        var bindingEntry = tradingRuntime.connectionBindings().entrySet().iterator().next();
        assertEquals("refdata::RefDB", bindingEntry.getKey(),
                "Runtime connection binding key (store FQN) must canonicalize");
        assertEquals("refdata::RefConn", bindingEntry.getValue(),
                "Runtime connection binding value (connection FQN) must canonicalize");

        // --- cross-project refs: connection storeName canonicalized ---
        var refConn = builder.getConnection("refdata::RefConn");
        assertNotNull(refConn, "refdata::RefConn must be registered");
        assertEquals("refdata::RefDB", refConn.storeName(),
                "ConnectionDefinition.storeName must be FQN");
    }

    private static String loadResource(String path) {
        try (InputStream is = BazelSmokeTest.class.getClassLoader().getResourceAsStream(path)) {
            assertNotNull(is, "Resource not found on classpath: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read resource " + path, e);
        }
    }
}
