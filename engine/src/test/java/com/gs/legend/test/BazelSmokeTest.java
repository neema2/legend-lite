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
    private static final String TRADING_MODEL = "bazel_smoke/trading/model.pure";
    private static final String TRADING_IMPL = "bazel_smoke/trading/impl.pure";

    @Test
    @DisplayName("Smoke corpus parses and cross-project refs resolve (Phase A baseline)")
    void corpusParsesWithCrossProjectRefs() {
        PureModelBuilder builder = new PureModelBuilder()
                .addSource(loadResource(REFDATA_MODEL))
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
        var sectorProp = trade.findProperty("sector");
        assertTrue(sectorProp.isPresent(), "trading::Trade.sector property must exist");
        assertTrue(sectorProp.get().typeRef() instanceof com.gs.legend.model.m3.TypeRef.ClassRef,
                "trading::Trade.sector must be a ClassRef (not a primitive or enum)");
        assertEquals("refdata::Sector", sectorProp.get().typeFqn(),
                "trading::Trade.sector must resolve to refdata::Sector (cross-project)");

        // --- cross-project refs: enum-typed property (kind preserved as EnumRef) ---
        var ratingProp = trade.findProperty("rating");
        assertTrue(ratingProp.isPresent(), "trading::Trade.rating property must exist");
        assertTrue(ratingProp.get().typeRef() instanceof com.gs.legend.model.m3.TypeRef.EnumRef,
                "trading::Trade.rating must be an EnumRef (enum-vs-class distinction preserved)");
        assertEquals("refdata::Rating", ratingProp.get().typeFqn(),
                "trading::Trade.rating must resolve to refdata::Rating");

        // --- cross-project refs: superclass resolves across projects ---
        var internalTrade = builder.getClass("trading::InternalTrade");
        assertFalse(internalTrade.superClassFqns().isEmpty(),
                "trading::InternalTrade must have a resolved superclass FQN");
        assertEquals("refdata::Categorized", internalTrade.superClassFqns().get(0),
                "trading::InternalTrade superclass must resolve to refdata::Categorized");

        // --- cross-project refs: inherited property reachable through superclass chain ---
        var categoryInherited = internalTrade.findProperty("category");
        assertTrue(categoryInherited.isPresent(),
                "trading::InternalTrade.category (inherited from refdata::Categorized) must be reachable");
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
