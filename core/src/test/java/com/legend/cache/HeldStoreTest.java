// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** HeldStore: a value built once per content key, held while it has holders, used one thread at a time. */
@DisplayName("HeldStore -- values held by their users, keyed by content")
class HeldStoreTest {

    @Test
    @DisplayName("one build per key while held; built again once its last holder lets it go")
    void builtOncePerKeyWhileHeld() {
        HeldStore<String> store = new HeldStore<>();
        AtomicInteger builds = new AtomicInteger();
        Hash key = Hash.ofUtf8("a definition");
        int a = store.hold(key, () -> "value " + builds.incrementAndGet());
        int b = store.hold(key, () -> "value " + builds.incrementAndGet());
        assertNotEquals(a, b, "each hold its own handle");
        assertEquals(1, builds.get(), "built once while held");
        assertEquals(1, store.size(), "one value, two handles");
        assertNotEquals(a, store.hold(Hash.ofUtf8("another"), () -> "other"), "another key, another entry");
        store.release(a);
        store.release(b);
        int c = store.hold(key, () -> "value " + builds.incrementAndGet());
        assertEquals("value 2", store.with(c, v -> v), "gone with its last holder, built again");
    }

    @Test
    @DisplayName("a value named by its key: the one held, when someone holds it; else the fallback, holding nothing")
    void usedByItsKey() {
        HeldStore<String> store = new HeldStore<>();
        Hash key = Hash.ofUtf8("a definition");
        assertEquals("built for this use", store.withKeyOr(key, v -> v, () -> "built for this use"), "none held");
        assertEquals(0, store.size(), "the fallback holds nothing");
        int a = store.hold(key, () -> "held");
        assertEquals("held", store.withKeyOr(key, v -> v, () -> "built for this use"), "the one held");
        store.release(a);
        assertEquals("built for this use", store.withKeyOr(key, v -> v, () -> "built for this use"), "let go");
    }

    @Test
    @DisplayName("a build that throws holds nothing")
    void aFailedBuildHoldsNothing() {
        HeldStore<String> store = new HeldStore<>();
        assertThrows(IllegalStateException.class, () -> store.hold(Hash.ofUtf8("bad"), () -> {
            throw new IllegalStateException("no");
        }));
        assertEquals(0, store.size());
    }

    @Test
    @DisplayName("a holder that lets its handle go twice takes nothing from another holder")
    void aSecondReleaseTakesNothing() {
        HeldStore<String> store = new HeldStore<>();
        int a = store.hold(Hash.ofUtf8("shared"), () -> "shared");
        int b = store.hold(Hash.ofUtf8("shared"), () -> "shared");
        store.release(a);
        store.release(a);
        assertEquals("shared", store.with(b, v -> v), "b still holds it");
        store.release(b);
        assertEquals(0, store.size());
    }

    @Test
    @DisplayName("a build is made outside the store's lock: a slow one holds up no other key's hold, nor a use")
    void aSlowBuildHoldsUpNoOne() throws Exception {
        HeldStore<String> store = new HeldStore<>();
        int held = store.hold(Hash.ofUtf8("already"), () -> "already");
        CountDownLatch building = new CountDownLatch(1);
        CountDownLatch others = new CountDownLatch(1);
        CompletableFuture<Integer> slow = CompletableFuture.supplyAsync(() -> store.hold(Hash.ofUtf8("slow"), () -> {
            building.countDown();
            try {
                // waits for the other thread's hold and use: under the store's lock, they could never come
                if (!others.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the other key's hold waited for this build");
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            return "slow";
        }));
        building.await(5, TimeUnit.SECONDS);
        int other = store.hold(Hash.ofUtf8("other"), () -> "other");
        assertEquals("already", store.with(held, v -> v));
        others.countDown();
        assertEquals("slow", store.with(slow.get(5, TimeUnit.SECONDS), v -> v));
        assertEquals("other", store.with(other, v -> v));
    }

    @Test
    @DisplayName("a handle let go of is refused by name; letting it go again does nothing")
    void aHandleLetGoIsRefused() {
        HeldStore<String> store = new HeldStore<>();
        int h = store.hold(Hash.ofUtf8("x"), () -> "x");
        store.release(h);
        store.release(h);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> store.with(h, v -> v));
        assertEquals("nothing is held under handle " + h + ": it was let go, or never held", e.getMessage());
    }
}
