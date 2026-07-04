package com.legend.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the content-addressing invariant at the key level: a {@link Hash} is a
 * pure function of content, distinct content yields distinct keys, and there is
 * no way to mint a key from a non-content identity.
 */
final class HashTest {

    @Test
    void sameContentProducesEqualHash() {
        assertEquals(Hash.ofUtf8("class model::Person {}"),
                Hash.ofUtf8("class model::Person {}"),
                "identical content must hash identically (determinism)");
    }

    @Test
    void differentContentProducesDifferentHash() {
        Hash before = Hash.ofUtf8("class model::Person { name: String[1]; }");
        Hash after  = Hash.ofUtf8("class model::Person { name: String[1]; age: Integer[1]; }");
        assertNotEquals(before, after,
                "a one-property edit must change the key (no stale hit downstream)");
    }

    @Test
    void ofUtf8AndOfBytesAgree() {
        assertEquals(Hash.ofUtf8("abc"),
                Hash.of("abc".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    @Test
    void combineIsDeterministic() {
        Hash a = Hash.ofUtf8("A");
        Hash b = Hash.ofUtf8("B");
        assertEquals(Hash.combine(a, b), Hash.combine(a, b));
    }

    @Test
    void combineIsOrderSensitive() {
        Hash a = Hash.ofUtf8("A");
        Hash b = Hash.ofUtf8("B");
        assertNotEquals(Hash.combine(a, b), Hash.combine(b, a),
                "Merkle combination must be order-sensitive");
    }

    @Test
    void hexIsFixed64Width() {
        assertEquals(64, Hash.ofUtf8("anything").hex().length());
    }

    @Test
    void rejectsNonSha256Hex() {
        assertThrows(IllegalArgumentException.class, () -> new Hash("deadbeef"),
                "only a 64-char SHA-256 hex is a valid Hash");
    }
}
