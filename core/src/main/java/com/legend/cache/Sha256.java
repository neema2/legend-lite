// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.cache;

/**
 * SHA-256 (FIPS 180-4), implemented here rather than taken from
 * {@code java.security.MessageDigest}.
 *
 * <h2>Why</h2>
 *
 * <p>This is CONTENT ADDRESSING, not security: the hash names an
 * artifact so a cache can recognise it. Depending on the platform's
 * crypto provider for that made the {@link Hash} type — and therefore
 * the boot-layer cache, and therefore the whole compile path —
 * unavailable on any runtime without one. TeaVM's class library has
 * no {@code java.security}, and it was the LAST thing standing
 * between the planner and a WebAssembly build.
 *
 * <p>The algorithm is unchanged, so every hash this produces is
 * byte-identical to the one {@code MessageDigest} produced before:
 * existing cache entries stay valid, and {@code Sha256Test} pins that
 * equivalence against the platform implementation across a corpus of
 * inputs as well as against the published FIPS vectors.
 *
 * <p>Hand-rolling a cryptographic primitive is normally a poor idea.
 * The case here is narrow and the risk is bounded: SHA-256 is a fully
 * specified function with published test vectors, nothing secret is
 * being protected, and the output is checked against the platform's
 * own implementation on every build.
 */
final class Sha256 {

    /** First 32 bits of the fractional parts of the cube roots of the
     * first 64 primes (FIPS 180-4 §4.2.2). */
    private static final int[] K = {
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
        0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
        0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
        0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
        0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
        0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
        0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    };

    /** First 32 bits of the fractional parts of the square roots of the
     * first 8 primes (FIPS 180-4 §5.3.3). */
    private static final int[] H0 = {
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    };

    private Sha256() {
    }

    /** The 32-byte digest of {@code message}. */
    static byte[] digest(byte[] message) {
        int[] h = H0.clone();

        // Padding (§5.1.1): a 0x80 byte, zeroes, then the length in
        // BITS as a big-endian 64-bit integer.
        long bitLength = (long) message.length * 8;
        int padded = message.length + 1;
        int remainder = padded % 64;
        int zeroes = remainder <= 56 ? 56 - remainder : 120 - remainder;
        byte[] buf = new byte[padded + zeroes + 8];
        System.arraycopy(message, 0, buf, 0, message.length);
        buf[message.length] = (byte) 0x80;
        for (int i = 0; i < 8; i++) {
            buf[buf.length - 1 - i] = (byte) (bitLength >>> (8 * i));
        }

        int[] w = new int[64];
        for (int block = 0; block < buf.length; block += 64) {
            for (int i = 0; i < 16; i++) {
                int j = block + i * 4;
                w[i] = ((buf[j] & 0xff) << 24)
                        | ((buf[j + 1] & 0xff) << 16)
                        | ((buf[j + 2] & 0xff) << 8)
                        | (buf[j + 3] & 0xff);
            }
            for (int i = 16; i < 64; i++) {
                int s0 = Integer.rotateRight(w[i - 15], 7)
                        ^ Integer.rotateRight(w[i - 15], 18)
                        ^ (w[i - 15] >>> 3);
                int s1 = Integer.rotateRight(w[i - 2], 17)
                        ^ Integer.rotateRight(w[i - 2], 19)
                        ^ (w[i - 2] >>> 10);
                w[i] = w[i - 16] + s0 + w[i - 7] + s1;
            }

            int a = h[0];
            int b = h[1];
            int c = h[2];
            int d = h[3];
            int e = h[4];
            int f = h[5];
            int g = h[6];
            int hh = h[7];

            for (int i = 0; i < 64; i++) {
                int s1 = Integer.rotateRight(e, 6)
                        ^ Integer.rotateRight(e, 11)
                        ^ Integer.rotateRight(e, 25);
                int ch = (e & f) ^ (~e & g);
                int temp1 = hh + s1 + ch + K[i] + w[i];
                int s0 = Integer.rotateRight(a, 2)
                        ^ Integer.rotateRight(a, 13)
                        ^ Integer.rotateRight(a, 22);
                int maj = (a & b) ^ (a & c) ^ (b & c);
                int temp2 = s0 + maj;

                hh = g;
                g = f;
                f = e;
                e = d + temp1;
                d = c;
                c = b;
                b = a;
                a = temp1 + temp2;
            }

            h[0] += a;
            h[1] += b;
            h[2] += c;
            h[3] += d;
            h[4] += e;
            h[5] += f;
            h[6] += g;
            h[7] += hh;
        }

        byte[] out = new byte[32];
        for (int i = 0; i < 8; i++) {
            out[i * 4] = (byte) (h[i] >>> 24);
            out[i * 4 + 1] = (byte) (h[i] >>> 16);
            out[i * 4 + 2] = (byte) (h[i] >>> 8);
            out[i * 4 + 3] = (byte) h[i];
        }
        return out;
    }

    /** Lowercase hex, the spelling {@link Hash} stores. */
    static String hex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xff;
            out[i * 2] = Character.forDigit(v >>> 4, 16);
            out[i * 2 + 1] = Character.forDigit(v & 0x0f, 16);
        }
        return new String(out);
    }
}
