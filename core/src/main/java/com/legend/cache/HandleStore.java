// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

/**
 * Live handles keyed by content {@link Hash} — Invariant 3's home for
 * the ONE cache whose values are stateful handles (JDBC connections)
 * rather than recomputable results. Content-keyed like
 * {@link ContentStore} (the caller hashes the handle's DEFINITION, so
 * an edited definition can never desync onto a stale handle — the
 * engine planCache scar), but with the behaviors handles need that
 * {@code ContentStore} must not grow:
 *
 * <ul>
 *   <li><b>Atomic open</b> ({@code compute}, D5): the old
 *       check-then-act raced — two threads could both open, and the
 *       losing handle leaked unclosed. Under compute there is no loser
 *       to close.</li>
 *   <li><b>Dead-handle replacement</b>: a cached handle the caller's
 *       {@code dead} predicate rejects (e.g. a closed connection) is
 *       replaced in the same atomic step.</li>
 *   <li><b>No eviction</b>: evicting an in-memory connection would
 *       silently drop its tables. Entries live for the process;
 *       distinct content keys are distinct handles BY DESIGN.</li>
 * </ul>
 *
 * <p>GENERIC on purpose: the handle type stays at the caller's
 * chartered seam (java.sql is funnelled to {exec, server, root,
 * testdatagen} — F1.3), never in this package.
 */
public final class HandleStore<T> {

    private final ConcurrentHashMap<String, T> live = new ConcurrentHashMap<>();

    /** Handle opener that may throw the caller's checked exception. */
    @FunctionalInterface
    public interface Open<T, E extends Exception> {
        T open() throws E;
    }

    /**
     * The live handle for {@code key}, opening (or replacing a dead
     * handle) atomically. The open runs under the key's map lock —
     * sanctioned only for fast local opens (in-memory / driver-local),
     * which is all the connection resolver caches.
     */
    public <E extends Exception> T getOrOpen(Hash key, Predicate<T> dead,
            Open<T, E> open) throws E {
        try {
            return live.compute(key.hex(), (k, existing) -> {
                if (existing != null && !dead.test(existing)) {
                    return existing;
                }
                try {
                    return open.open();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new CheckedCarrier(e);
                }
            });
        } catch (CheckedCarrier c) {
            @SuppressWarnings("unchecked")
            E e = (E) c.getCause();
            throw e;
        }
    }

    /** The slow opens, by key: the open each later caller for the key waits for. */
    private final ConcurrentHashMap<String, java.util.concurrent.CompletableFuture<T>> slow = new ConcurrentHashMap<>();

    /**
     * The live handle for {@code key}, its open run OUTSIDE the map's lock — for an open that takes time (a database
     * loaded with its data): the first caller for a key opens it; a later caller for the same key waits for that open;
     * a caller for another key is never held up. A failed open is forgotten, so the next caller opens again; a dead
     * handle is replaced.
     */
    public <E extends Exception> T getOrOpenSlowly(Hash key, Predicate<T> dead, Open<T, E> open) throws E {
        while (true) {
            java.util.concurrent.CompletableFuture<T> mine = new java.util.concurrent.CompletableFuture<>();
            java.util.concurrent.CompletableFuture<T> existing = slow.putIfAbsent(key.hex(), mine);
            if (existing == null) {
                boolean opened = false;
                try {
                    T handle = open.open();
                    mine.complete(handle);
                    opened = true;
                    return handle;
                } finally {
                    if (!opened) {
                        // forgotten, and its waiters released to open in their turn; the failure is this caller's
                        slow.remove(key.hex(), mine);
                        mine.cancel(false);
                    }
                }
            }
            T handle;
            try {
                handle = existing.join();
            } catch (java.util.concurrent.CancellationException e) {
                // the open this caller waited for failed (and was forgotten): this caller opens in its turn
                continue;
            }
            if (!dead.test(handle)) {
                return handle;
            }
            slow.remove(key.hex(), existing);
        }
    }

    /** Carries the caller's checked exception across compute's
     * unchecked boundary; never escapes {@link #getOrOpen}. */
    private static final class CheckedCarrier extends RuntimeException {
        private CheckedCarrier(Exception cause) {
            super(cause);
        }
    }
}
