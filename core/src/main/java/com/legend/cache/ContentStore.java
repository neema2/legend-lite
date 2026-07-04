package com.legend.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * The one sanctioned cache in {@code core/}: a {@link Hash}-keyed memoization
 * table. Generalizes engine's {@code ParseCache} discipline
 * (&ldquo;identical source always produces identical output, so no invalidation
 * is needed&rdquo;) to every compiler phase.
 *
 * <h2>Why this is the <em>only</em> cache</h2>
 *
 * <p>The key type is {@link Hash}, which can only be built from content. So
 * every entry is content-addressed by construction and <strong>cannot desync</strong>:
 * a changed input hashes to a different key, yielding a miss rather than a
 * stale hit. There is no {@code put(name, value)}, no version key, and no
 * mutable &ldquo;current model&rdquo; handle &mdash; the failure modes that let
 * a name-keyed cache rot. {@code ArchitectureTest} (Invariant 3) funnels all
 * caching here so this property holds module-wide.
 *
 * <p>Bounded LRU eviction keeps memory predictable; eviction is safe precisely
 * because entries are content-addressed (an evicted entry simply recomputes to
 * an identical value on the next miss). Thread-safe via coarse synchronization.
 */
public final class ContentStore {

    /** Default bound; matches the order of magnitude of engine's {@code ParseCache} (200). */
    private static final int DEFAULT_MAX_ENTRIES = 1024;

    private final Map<Hash, Object> store;

    public ContentStore() {
        this(DEFAULT_MAX_ENTRIES);
    }

    public ContentStore(int maxEntries) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be >= 1, got " + maxEntries);
        }
        // access-order LinkedHashMap => true LRU; computeIfAbsent counts as access.
        this.store = new LinkedHashMap<>(64, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Hash, Object> eldest) {
                return size() > maxEntries;
            }
        };
    }

    /**
     * Returns the artifact memoized under {@code key}, computing and storing it
     * on a miss. The supplier is invoked at most once per distinct key for as
     * long as the entry is resident.
     *
     * @param key     content hash of the inputs that {@code compute} depends on
     * @param compute pure function producing the artifact; must not return null
     * @param <T>     artifact type (the caller is responsible for using a key
     *                that uniquely determines this type)
     */
    @SuppressWarnings("unchecked")
    public synchronized <T> T getOrCompute(Hash key, Supplier<? extends T> compute) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(compute, "compute");
        return (T) store.computeIfAbsent(key, k ->
                Objects.requireNonNull(compute.get(), () -> "compute returned null for " + k));
    }

    /** Current number of resident entries. */
    public synchronized int size() {
        return store.size();
    }
}
