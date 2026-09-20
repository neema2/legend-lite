package com.legend.cache;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Objects;

/**
 * A content hash &mdash; the <strong>only</strong> kind of key permitted in a
 * {@link ContentStore}. SHA-256 of an artifact's canonical bytes, rendered as
 * 64 lowercase hex characters.
 *
 * <h2>Why this type exists</h2>
 *
 * <p>It makes the content-addressing invariant <em>structural</em>: a cache key
 * can only ever be derived from content, because the only ways to construct a
 * {@code Hash} are {@link #of(byte[])} / {@link #ofUtf8(String)} (hash the
 * bytes) and {@link #combine(Hash...)} (hash child hashes). There is
 * deliberately <strong>no</strong> factory that takes a name, an id, a version
 * counter, or a timestamp. A cache built on {@code Hash} therefore cannot go
 * out of sync: changed input &rarr; different key &rarr; a miss, never a stale
 * hit (core/README content-addressing invariant; the explicit rejection of
 * engine's name-keyed {@code MetadataService.planCache}).
 *
 * <h2>{@link #combine} is order-sensitive (Merkle)</h2>
 *
 * <p>{@code combine(a, b) != combine(b, a)}. For an order-insensitive identity
 * (e.g. hashing an unordered <em>set</em> of element hashes into a model
 * identity), sort the parts before combining.
 */
public record Hash(String hex) {

    /** SHA-256 produces 32 bytes = 64 hex characters. */
    private static final int HEX_LENGTH = 64;

    public Hash {
        Objects.requireNonNull(hex, "hex");
        if (hex.length() != HEX_LENGTH) {
            throw new IllegalArgumentException(
                    "hex must be " + HEX_LENGTH + " chars (SHA-256), got " + hex.length());
        }
    }

    /**
     * Hashes raw content bytes.
     *
     * Uses {@link Sha256} rather than {@code MessageDigest}: the
     * algorithm and therefore every hash is identical (pinned by
     * {@code Sha256Test} against the platform), but it needs no crypto
     * provider, so content addressing — and with it the boot-layer
     * cache and the whole compile path — works on a runtime that has
     * no {@code java.security}. That was the last thing keeping the
     * planner off WebAssembly.
     */
    public static Hash of(byte[] content) {
        Objects.requireNonNull(content, "content");
        return new Hash(Sha256.hex(Sha256.digest(content)));
    }

    /** Hashes the UTF-8 bytes of {@code content}. The argument is content, never a name. */
    public static Hash ofUtf8(String content) {
        Objects.requireNonNull(content, "content");
        return of(content.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Merkle combination: hashes the concatenated hex of {@code parts} in order.
     * Order-sensitive (see class javadoc).
     */
    public static Hash combine(Hash... parts) {
        Objects.requireNonNull(parts, "parts");
        StringBuilder sb = new StringBuilder(parts.length * HEX_LENGTH);
        for (Hash p : parts) {
            sb.append(Objects.requireNonNull(p, "part").hex);
        }
        return ofUtf8(sb.toString());
    }

    /** Short form for logs, e.g. {@code Hash(3a7bd3e2f1c0…)}. {@link #equals} still uses the full hex. */
    @Override
    public String toString() {
        return "Hash(" + hex.substring(0, 12) + "\u2026)";
    }
}
