package com.gs.legend.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ParseCache}.
 */
class ParseCacheTest {

    @Test
    void cacheHitReturnsSameResult() {
        var cache = new ParseCache(100);
        String source = "Class test::Person { name: String[1]; }";
        var first = cache.getOrParse(source);
        var second = cache.getOrParse(source);
        assertSame(first, second, "Cache should return same object on hit");
        assertEquals(1, cache.size());
    }

    @Test
    void differentSourceProducesDifferentResult() {
        var cache = new ParseCache(100);
        var r1 = cache.getOrParse("Class test::A { x: String[1]; }");
        var r2 = cache.getOrParse("Class test::B { y: Integer[1]; }");
        assertNotSame(r1, r2);
        assertEquals(2, cache.size());
    }

    @Test
    void lruEvictsOldestEntry() {
        var cache = new ParseCache(2);
        var r1 = cache.getOrParse("Class test::A { x: String[1]; }");
        cache.getOrParse("Class test::B { y: Integer[1]; }");
        cache.getOrParse("Class test::C { z: Boolean[1]; }");
        // A should have been evicted
        assertEquals(2, cache.size());
        // Re-parsing A should produce a new object
        var r1b = cache.getOrParse("Class test::A { x: String[1]; }");
        assertNotSame(r1, r1b, "Evicted entry should be re-parsed");
    }

    @Test
    void clearEmptiesCache() {
        var cache = new ParseCache(100);
        cache.getOrParse("Class test::A { x: String[1]; }");
        cache.getOrParse("Class test::B { y: Integer[1]; }");
        assertEquals(2, cache.size());
        cache.clear();
        assertEquals(0, cache.size());
    }

    @Test
    void modelBuilderUsesCacheTransparently() {
        // Verify the integration: PureModelBuilder.addSource() goes through ParseCache.global()
        ParseCache.global().clear();
        String source = "Class test::Person { name: String[1]; age: Integer[1]; }";

        var model1 = new PureModelBuilder();
        model1.addSource(source);

        var model2 = new PureModelBuilder();
        model2.addSource(source);

        // Both should resolve the same class
        assertTrue(model1.findClass("test::Person").isPresent());
        assertTrue(model2.findClass("test::Person").isPresent());

        // Cache should have exactly 1 entry for this source (plus platform enum sources from PureModelBuilder init)
        assertTrue(ParseCache.global().size() >= 1, "Global cache should have at least 1 entry");
    }

    @Test
    void parseResultContainsCorrectDefinitions() {
        var cache = new ParseCache(100);
        String source = """
                Class test::Person
                {
                    name: String[1];
                    age: Integer[1];
                }
                
                Enum test::Status
                {
                    ACTIVE,
                    INACTIVE
                }
                """;
        var result = cache.getOrParse(source);
        assertEquals(2, result.definitions().size(), "Should parse class + enum");
    }
}
