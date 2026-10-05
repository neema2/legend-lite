package com.legend.sdlc;

/**
 * SHA-1 (FIPS 180-4), for git object ids. Written out because the WebAssembly build has no
 * {@code java.security.MessageDigest} (wasm/README.md), as {@code core/cache/Sha256} is for SHA-256.
 */
final class Sha1 {
    private Sha1() {}

    /** The 20-byte digest of {@code data}. */
    static byte[] digest(byte[] data) {
        int h0 = 0x67452301, h1 = 0xEFCDAB89, h2 = 0x98BADCFE, h3 = 0x10325476, h4 = 0xC3D2E1F0;
        long bitLength = (long) data.length * 8;
        int padded = ((data.length + 8) / 64 + 1) * 64;
        byte[] m = new byte[padded];
        System.arraycopy(data, 0, m, 0, data.length);
        m[data.length] = (byte) 0x80;
        for (int i = 0; i < 8; i++) m[padded - 1 - i] = (byte) (bitLength >>> (8 * i));
        int[] w = new int[80];
        for (int block = 0; block < padded; block += 64) {
            for (int i = 0; i < 16; i++) {
                int j = block + i * 4;
                w[i] = ((m[j] & 0xff) << 24) | ((m[j + 1] & 0xff) << 16) | ((m[j + 2] & 0xff) << 8) | (m[j + 3] & 0xff);
            }
            for (int i = 16; i < 80; i++) w[i] = Integer.rotateLeft(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
            int a = h0, b = h1, c = h2, d = h3, e = h4;
            for (int i = 0; i < 80; i++) {
                int f;
                int k;
                if (i < 20) { f = (b & c) | (~b & d); k = 0x5A827999; }
                else if (i < 40) { f = b ^ c ^ d; k = 0x6ED9EBA1; }
                else if (i < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8F1BBCDC; }
                else { f = b ^ c ^ d; k = 0xCA62C1D6; }
                int t = Integer.rotateLeft(a, 5) + f + e + k + w[i];
                e = d;
                d = c;
                c = Integer.rotateLeft(b, 30);
                b = a;
                a = t;
            }
            h0 += a; h1 += b; h2 += c; h3 += d; h4 += e;
        }
        byte[] out = new byte[20];
        int[] h = {h0, h1, h2, h3, h4};
        for (int i = 0; i < 5; i++) {
            out[i * 4] = (byte) (h[i] >>> 24);
            out[i * 4 + 1] = (byte) (h[i] >>> 16);
            out[i * 4 + 2] = (byte) (h[i] >>> 8);
            out[i * 4 + 3] = (byte) h[i];
        }
        return out;
    }

    static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(digits[(b >>> 4) & 0xf]).append(digits[b & 0xf]);
        return sb.toString();
    }

    static byte[] unhex(String hex) {
        byte[] out = new byte[hex.length() / 2];
        for (int i = 0; i < out.length; i++) out[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        return out;
    }
}
