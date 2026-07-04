package com.legend.cache;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the behavioral half of the content-addressing invariant: the store
 * memoizes by content and <strong>cannot return a stale artifact</strong> when
 * the content changes.
 */
final class ContentStoreTest {

    @Test
    void memoizesOncePerKey() {
        ContentStore store = new ContentStore();
        AtomicInteger calls = new AtomicInteger();
        Hash key = Hash.ofUtf8("class A {}");

        String first  = store.getOrCompute(key, () -> { calls.incrementAndGet(); return "compiled-A"; });
        String second = store.getOrCompute(key, () -> { calls.incrementAndGet(); return "compiled-A"; });

        assertEquals("compiled-A", first);
        assertEquals("compiled-A", second);
        assertEquals(1, calls.get(), "compute must run at most once per distinct key");
    }

    /**
     * The core anti-desync guarantee. A compiled artifact is keyed by the hash
     * of its source. When the source changes, the key changes, so the store
     * returns the freshly compiled artifact &mdash; the old one is never served.
     */
    @Test
    void changedContentNeverReturnsStaleArtifact() {
        ContentStore store = new ContentStore();
        Function<String, String> compile = src -> "compiled(" + src + ")";

        String v1src = "class A { x: Integer[1]; }";
        String v2src = "class A { x: Integer[1]; y: Integer[1]; }";

        String r1 = store.getOrCompute(Hash.ofUtf8(v1src), () -> compile.apply(v1src));
        String r2 = store.getOrCompute(Hash.ofUtf8(v2src), () -> compile.apply(v2src));

        assertEquals("compiled(" + v1src + ")", r1);
        assertEquals("compiled(" + v2src + ")", r2,
                "edited content must yield its own artifact, not the previous version's");

        // Re-asking for the original content still returns the original artifact (consistency).
        assertEquals("compiled(" + v1src + ")",
                store.getOrCompute(Hash.ofUtf8(v1src), () -> { throw new AssertionError("should be cached"); }));
    }

    @Test
    void evictionRecomputesToIdenticalValue() {
        ContentStore store = new ContentStore(2);
        // Overflow the bound so the first key is evicted...
        store.getOrCompute(Hash.ofUtf8("a"), () -> "A");
        store.getOrCompute(Hash.ofUtf8("b"), () -> "B");
        store.getOrCompute(Hash.ofUtf8("c"), () -> "C");
        // ...recomputing "a" yields the same value: eviction is safe under content addressing.
        assertEquals("A", store.getOrCompute(Hash.ofUtf8("a"), () -> "A"));
    }

    @Test
    void rejectsNullArtifact() {
        ContentStore store = new ContentStore();
        assertThrows(NullPointerException.class,
                () -> store.getOrCompute(Hash.ofUtf8("x"), () -> null));
    }
}
