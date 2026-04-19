package com.gs.legend.model;

import com.gs.legend.parser.ParseResult;
import com.gs.legend.parser.PureParser;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Content-addressed parse cache for Pure model source.
 *
 * Keyed by the raw source string — identical source always produces identical
 * parse output, so no invalidation is needed. Uses a bounded LRU eviction
 * strategy to keep memory usage predictable.
 *
 * <p>Singleton via {@link #global()}. Thread-safe via synchronized access.
 *
 * <p>Designed for the multi-file editor workflow: when one file changes,
 * all files are re-added to a fresh PureModelBuilder. Unchanged files
 * hit the cache (zero parse cost); only the changed file is re-parsed.
 */
public final class ParseCache {

    private static final ParseCache GLOBAL = new ParseCache(200);

    private final LinkedHashMap<String, ParseResult> cache;

    ParseCache(int maxEntries) {
        this.cache = new LinkedHashMap<>(32, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, ParseResult> eldest) {
                return size() > maxEntries;
            }
        };
    }

    /** Returns the global singleton cache, shared across all PureModelBuilder instances. */
    public static ParseCache global() {
        return GLOBAL;
    }

    /**
     * Returns a cached parse result for the given source, or parses and caches it.
     *
     * @param source Raw Pure source code
     * @return The parse result (definitions + imports)
     */
    public synchronized ParseResult getOrParse(String source) {
        return cache.computeIfAbsent(source, PureParser::parseModelWithImports);
    }

    /** Returns the current number of cached entries. */
    public synchronized int size() {
        return cache.size();
    }

    /** Clears all cached entries. */
    public synchronized void clear() {
        cache.clear();
    }
}
