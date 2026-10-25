// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Values HELD by their users, keyed by content {@link Hash} -- Invariant 3's home for a store whose entries live exactly
 * as long as someone holds them (a compiled model while a planner over it lives: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md
 * §9). Content-keyed like {@link ContentStore} and {@link HandleStore} (the caller hashes the value's DEFINITION, so an
 * edited definition is another entry and never a stale one), with what holding needs and they must not grow:
 *
 * <ul>
 *   <li><b>Counted holders, each its own handle</b>: {@link #hold} builds a key's value the first time and answers each
 *       holder a handle of its own; {@link #release} lets that one handle go (a second release of it does nothing,
 *       whoever else holds the value), and the entry goes with its last handle -- nothing is kept that no one holds, so
 *       the store is bounded by its holders, not by a size or an age.</li>
 *   <li><b>A build outside the lock</b>: a value that takes seconds to build (a model's compile) holds up no one --
 *       neither another key's hold nor a use of a value held already. Two holders racing on one new key may both build;
 *       the first to finish is kept and the other's value dropped (HandleStore.getOrOpenSlowly's rule, with no futures,
 *       so it stays TeaVM's).</li>
 *   <li><b>One user at a time per entry</b>: {@link #with} runs under the entry's own lock, for values not safe to use
 *       from two threads at once (a compiled model's typing caches are plain maps). A use begun before its handle is
 *       released finishes on the value it began with.</li>
 * </ul>
 *
 * <p>GENERIC on purpose, as {@link HandleStore} is: the value's type stays with its owner, never in this package. An
 * instance is its owner's -- a host that holds models makes one; a host that holds nothing (the server) makes none.
 */
public final class HeldStore<T> {

    private final Map<String, Entry<T>> byKey = new HashMap<>();
    /** Each hold's own handle, to the entry it holds. */
    private final Map<Integer, Entry<T>> byHandle = new HashMap<>();
    private final AtomicInteger next = new AtomicInteger(1);

    /** A held value: its key, the value, and how many handles hold it. */
    private static final class Entry<T> {
        private final String key;
        private final T value;
        private final AtomicInteger holders = new AtomicInteger();

        Entry(String key, T value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Hold the value for {@code key}: built by {@code build} the first time the key is held -- outside the store's lock;
     * a build that throws holds nothing -- and the same value held each time after. The answer is this hold's own
     * handle, naming the value to {@link #with} until it is released.
     */
    public int hold(Hash key, Supplier<T> build) {
        String k = key.hex();
        synchronized (this) {
            Entry<T> held = byKey.get(k);
            if (held != null) {
                return handleFor(held);
            }
        }
        T value = build.get();
        synchronized (this) {
            Entry<T> held = byKey.get(k);
            if (held == null) {
                held = new Entry<>(k, value);
                byKey.put(k, held);
            }
            // a build that raced another for the same new key and finished second: its value is dropped
            return handleFor(held);
        }
    }

    /** A new handle for {@code entry}, counted among its holders. Under the store's lock. */
    private int handleFor(Entry<T> entry) {
        int handle = next.getAndIncrement();
        byHandle.put(handle, entry);
        entry.holders.incrementAndGet();
        return handle;
    }

    /**
     * Let {@code handle} go: the value goes with its last handle. A handle not held -- released already, or never held --
     * is let go of already: nothing else changes.
     */
    public synchronized void release(int handle) {
        Entry<T> entry = byHandle.remove(handle);
        if (entry != null && entry.holders.decrementAndGet() == 0) {
            byKey.remove(entry.key);
        }
    }

    /**
     * {@code use} of the value held under {@code handle}, under that entry's own lock. A handle no longer held -- let go
     * of, or never held -- is refused by name.
     */
    public <R> R with(int handle, Function<T, R> use) {
        Entry<T> entry;
        synchronized (this) {
            entry = byHandle.get(handle);
        }
        if (entry == null) {
            throw new IllegalArgumentException("nothing is held under handle " + handle + ": it was let go, or never held");
        }
        synchronized (entry) {
            return use.apply(entry.value);
        }
    }

    /**
     * {@code use} of the value held for {@code key}, under that entry's own lock, when someone holds it; else
     * {@code otherwise}, outside every lock, holding nothing. For a caller that names a value by its content and holds
     * no handle of its own (a request carrying a model's text that a holder has compiled already).
     */
    public <R> R withKeyOr(Hash key, Function<T, R> use, Supplier<R> otherwise) {
        Entry<T> entry;
        synchronized (this) {
            entry = byKey.get(key.hex());
        }
        if (entry == null) {
            return otherwise.get();
        }
        synchronized (entry) {
            return use.apply(entry.value);
        }
    }

    /** How many values are held now (each once, however many handles hold it). */
    public synchronized int size() {
        return byKey.size();
    }
}
