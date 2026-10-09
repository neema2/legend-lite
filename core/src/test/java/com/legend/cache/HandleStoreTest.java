// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.cache;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * D5 pins for {@link HandleStore}: the properties whose ABSENCE was the
 * audit finding against the old ConnectionResolver map — check-then-act
 * race (two opens, leaked loser), no dead-handle replacement, and
 * name-keyed identity.
 */
class HandleStoreTest {

    private record Handle(int id, boolean dead) {
    }

    @Test
    void sameKeySameHandle() throws Exception {
        HandleStore<Handle> store = new HandleStore<>();
        Hash k = Hash.ofUtf8("a");
        Handle first = store.getOrOpen(k, Handle::dead, () -> new Handle(1, false));
        Handle again = store.getOrOpen(k, Handle::dead, () -> new Handle(2, false));
        assertSame(first, again, "live handle must be reused, not reopened");
    }

    @Test
    void distinctKeysDistinctHandles() throws Exception {
        HandleStore<Handle> store = new HandleStore<>();
        Handle a = store.getOrOpen(Hash.ofUtf8("a"), Handle::dead,
                () -> new Handle(1, false));
        Handle b = store.getOrOpen(Hash.ofUtf8("b"), Handle::dead,
                () -> new Handle(2, false));
        assertNotSame(a, b, "content keys are identity — no cross-key sharing");
    }

    @Test
    void deadHandleIsReplaced() throws Exception {
        HandleStore<Handle> store = new HandleStore<>();
        Hash k = Hash.ofUtf8("a");
        store.getOrOpen(k, Handle::dead, () -> new Handle(1, true));
        Handle replacement = store.getOrOpen(k, Handle::dead,
                () -> new Handle(2, false));
        assertEquals(2, replacement.id(), "a dead handle must be replaced");
    }

    @Test
    void checkedExceptionPropagatesAndCachesNothing() throws Exception {
        HandleStore<Handle> store = new HandleStore<>();
        Hash k = Hash.ofUtf8("a");
        assertThrows(java.io.IOException.class, () -> store.getOrOpen(k,
                Handle::dead, () -> {
                    throw new java.io.IOException("boom");
                }));
        Handle afterFailure = store.getOrOpen(k, Handle::dead,
                () -> new Handle(7, false));
        assertEquals(7, afterFailure.id(), "a failed open must not poison the key");
    }

    /** THE race pin: N threads demanding one key must produce ONE open —
     * the old check-then-act let several through and leaked the losers. */
    @Test
    void concurrentDemandOpensOnce() throws Exception {
        HandleStore<Handle> store = new HandleStore<>();
        Hash k = Hash.ofUtf8("a");
        AtomicInteger opens = new AtomicInteger();
        int threads = 8;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        Thread[] pool = new Thread[threads];
        Handle[] got = new Handle[threads];
        for (int i = 0; i < threads; i++) {
            int slot = i;
            pool[i] = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                    got[slot] = store.getOrOpen(k, Handle::dead,
                            () -> new Handle(opens.incrementAndGet(), false));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            pool[i].start();
        }
        ready.await();
        go.countDown();
        for (Thread t : pool) {
            t.join();
        }
        assertEquals(1, opens.get(), "atomic compute admits exactly one open");
        for (Handle h : got) {
            assertSame(got[0], h, "every demander sees the single opened handle");
        }
    }

    // ---- getOrOpenSlowly (2026-10-09, execution plan step 3: a shared database's setup runs outside the map's lock) --

    /** Waits until each of {@code threads} is parked: in this store, only a demander waiting for an open in progress. */
    private static void awaitParked(Thread... threads) throws InterruptedException {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        for (Thread t : threads) {
            while (t.getState() != Thread.State.WAITING) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError(t.getName() + " never waited: " + t.getState());
                }
                Thread.onSpinWait();
            }
        }
    }

    private static Thread demand(HandleStore<Handle> store, Hash k, AtomicInteger opens, Handle[] got, int slot) {
        Thread t = new Thread(() -> {
            try {
                got[slot] = store.getOrOpenSlowly(k, Handle::dead, () -> new Handle(opens.incrementAndGet(), false));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, "demander-" + slot);
        t.start();
        return t;
    }

    /** One open for a key, however many demand it while it runs: every later demander waits for it, and a demander
     *  of another key is not held up meanwhile. */
    @Test
    void aSlowOpenRunsOnce_itsKeysDemandersWait_andAnotherKeyIsNotHeldUp() throws Exception {
        HandleStore<Handle> store = new HandleStore<>();
        Hash k = Hash.ofUtf8("a");
        AtomicInteger opens = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Handle[] first = new Handle[1];
        Thread opener = new Thread(() -> {
            try {
                first[0] = store.getOrOpenSlowly(k, Handle::dead, () -> {
                    entered.countDown();
                    release.await();
                    return new Handle(opens.incrementAndGet(), false);
                });
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        opener.start();
        entered.await();
        int waiters = 7;
        Handle[] got = new Handle[waiters];
        Thread[] pool = new Thread[waiters];
        for (int i = 0; i < waiters; i++) {
            pool[i] = demand(store, k, opens, got, i);
        }
        awaitParked(pool);
        // the open of "a" is still running: "b" opens at once
        Handle b = store.getOrOpenSlowly(Hash.ofUtf8("b"), Handle::dead, () -> new Handle(100, false));
        assertEquals(100, b.id());
        release.countDown();
        opener.join();
        for (Thread t : pool) {
            t.join();
        }
        assertEquals(1, opens.get(), "one open of the key, however many demanded it");
        for (Handle h : got) {
            assertSame(first[0], h, "every waiter sees the one opened handle");
        }
    }

    /** A failed open is the failing demander's: it is forgotten, and a demander that waited for it opens in its turn. */
    @Test
    void aFailedSlowOpenIsForgotten_andAWaiterOpensInItsTurn() throws Exception {
        HandleStore<Handle> store = new HandleStore<>();
        Hash k = Hash.ofUtf8("a");
        AtomicInteger opens = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Throwable[] failure = new Throwable[1];
        Thread opener = new Thread(() -> {
            try {
                store.getOrOpenSlowly(k, Handle::dead, () -> {
                    entered.countDown();
                    release.await();
                    throw new java.io.IOException("boom");
                });
            } catch (Exception e) {
                failure[0] = e;
            }
        });
        opener.start();
        entered.await();
        Handle[] got = new Handle[1];
        Thread waiter = demand(store, k, opens, got, 0);
        awaitParked(waiter);
        release.countDown();
        opener.join();
        waiter.join();
        assertEquals("boom", failure[0].getMessage(), "the failure is the opener's, as it was thrown");
        assertEquals(1, opens.get(), "the waiter opened in its turn");
        assertEquals(1, got[0].id());
    }

    @Test
    void aSlowlyOpenedDeadHandleIsReplaced() throws Exception {
        HandleStore<Handle> store = new HandleStore<>();
        Hash k = Hash.ofUtf8("a");
        Handle live = store.getOrOpenSlowly(k, Handle::dead, () -> new Handle(1, false));
        assertSame(live, store.getOrOpenSlowly(k, Handle::dead, () -> new Handle(2, false)), "a live handle is reused");
        HandleStore<Handle> dying = new HandleStore<>();
        dying.getOrOpenSlowly(k, Handle::dead, () -> new Handle(1, true));
        assertEquals(2, dying.getOrOpenSlowly(k, Handle::dead, () -> new Handle(2, false)).id(),
                "a dead handle is replaced");
    }
}
