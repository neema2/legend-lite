// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/**
 * MD5, SHA-1 and SHA-256 written in SQLite's own SQL (the parity phase, 2026-10-20): SQLite has no digest function, and
 * its integers, bit operators and recursive CTEs compute one exactly. The text's UTF-8 bytes ({@code hex(CAST(x AS
 * BLOB))}) are padded as each digest pads them (a 0x80 byte, zeros to 56 modulo 64, the bit length in eight bytes,
 * little-endian for MD5, big-endian for the SHAs); one walk builds each 64-byte chunk's words (for the SHAs, the message
 * schedule after the chunk's 16); a second walk runs the rounds, one row per round, the chunk's state added in after
 * its last. 32-bit arithmetic is the integer's low 32 bits ({@code & 4294967295}); SQLite has no XOR, so {@code x ^ y}
 * is {@code (x | y) & ~(x & y)}. Measured bit-equal to the JDK's digests on texts of 0 to 200 characters, multi-byte
 * UTF-8 among them; a NULL text has no digest. The database computes every value; this class writes the walks.
 */
final class SqliteDigests {

    private SqliteDigests() {
    }

    private static final String M32 = "4294967295";
    private static final String HEX = "'0123456789ABCDEF'";

    /** MD5's per-round constants (floor(|sin(i + 1)| * 2^32)) and shifts. */
    private static final long[] MD5_K = {
        0xd76aa478L, 0xe8c7b756L, 0x242070dbL, 0xc1bdceeeL, 0xf57c0fafL, 0x4787c62aL, 0xa8304613L, 0xfd469501L,
        0x698098d8L, 0x8b44f7afL, 0xffff5bb1L, 0x895cd7beL, 0x6b901122L, 0xfd987193L, 0xa679438eL, 0x49b40821L,
        0xf61e2562L, 0xc040b340L, 0x265e5a51L, 0xe9b6c7aaL, 0xd62f105dL, 0x02441453L, 0xd8a1e681L, 0xe7d3fbc8L,
        0x21e1cde6L, 0xc33707d6L, 0xf4d50d87L, 0x455a14edL, 0xa9e3e905L, 0xfcefa3f8L, 0x676f02d9L, 0x8d2a4c8aL,
        0xfffa3942L, 0x8771f681L, 0x6d9d6122L, 0xfde5380cL, 0xa4beea44L, 0x4bdecfa9L, 0xf6bb4b60L, 0xbebfbc70L,
        0x289b7ec6L, 0xeaa127faL, 0xd4ef3085L, 0x04881d05L, 0xd9d4d039L, 0xe6db99e5L, 0x1fa27cf8L, 0xc4ac5665L,
        0xf4292244L, 0x432aff97L, 0xab9423a7L, 0xfc93a039L, 0x655b59c3L, 0x8f0ccc92L, 0xffeff47dL, 0x85845dd1L,
        0x6fa87e4fL, 0xfe2ce6e0L, 0xa3014314L, 0x4e0811a1L, 0xf7537e82L, 0xbd3af235L, 0x2ad7d2bbL, 0xeb86d391L};
    private static final int[] MD5_S = {
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9,
        14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15,
        21, 6, 10, 15, 21};

    /** SHA-256's round constants. */
    private static final long[] SHA256_K = {
        0x428a2f98L, 0x71374491L, 0xb5c0fbcfL, 0xe9b5dba5L, 0x3956c25bL, 0x59f111f1L, 0x923f82a4L, 0xab1c5ed5L,
        0xd807aa98L, 0x12835b01L, 0x243185beL, 0x550c7dc3L, 0x72be5d74L, 0x80deb1feL, 0x9bdc06a7L, 0xc19bf174L,
        0xe49b69c1L, 0xefbe4786L, 0x0fc19dc6L, 0x240ca1ccL, 0x2de92c6fL, 0x4a7484aaL, 0x5cb0a9dcL, 0x76f988daL,
        0x983e5152L, 0xa831c66dL, 0xb00327c8L, 0xbf597fc7L, 0xc6e00bf3L, 0xd5a79147L, 0x06ca6351L, 0x14292967L,
        0x27b70a85L, 0x2e1b2138L, 0x4d2c6dfcL, 0x53380d13L, 0x650a7354L, 0x766a0abbL, 0x81c2c92eL, 0x92722c85L,
        0xa2bfe8a1L, 0xa81a664bL, 0xc24b8b70L, 0xc76c51a3L, 0xd192e819L, 0xd6990624L, 0xf40e3585L, 0x106aa070L,
        0x19a4c116L, 0x1e376c08L, 0x2748774cL, 0x34b0bcb5L, 0x391c0cb3L, 0x4ed8aa4aL, 0x5b9cca4fL, 0x682e6ff3L,
        0x748f82eeL, 0x78a5636fL, 0x84c87814L, 0x8cc70208L, 0x90befffaL, 0xa4506cebL, 0xbef9a3f7L, 0xc67178f2L};

    /** The MD5 of {@code text}'s UTF-8 bytes, as 32 lowercase hex digits; NULL for NULL. */
    static SqlWriter.Piece md5(SqlWriter.Piece text) {
        String f = "CASE WHEN r.i < 16 THEN (r.b & r.c) | (~r.b & r.d) WHEN r.i < 32 THEN (r.d & r.b) | (~r.d & r.c)"
                + " WHEN r.i < 48 THEN " + xor(xor("r.b", "r.c"), "r.d") + " ELSE " + xor("r.c", "r.b | ~r.d") + " END";
        String g = "CASE WHEN r.i < 16 THEN r.i WHEN r.i < 32 THEN (5 * r.i + 1) % 16 WHEN r.i < 48 THEN (3 * r.i + 5)"
                + " % 16 ELSE (7 * r.i) % 16 END";
        String t = "(((" + f + ") + r.a + " + at(list(MD5_K), "r.i") + " + " + at("__c.w", g) + ") & " + M32 + ")";
        String[] next = {"r.d", "((r.b + " + rotl(t, at(list(MD5_S), "r.i")) + ") & " + M32 + ")", "r.b", "r.c"};
        StringBuilder out = new StringBuilder();
        for (String v : new String[] {"a", "b", "c", "d"}) {
            out.append(out.length() == 0 ? "" : " || ").append("printf('%02x%02x%02x%02x', r.").append(v)
                    .append("0 & 255, (r.").append(v).append("0 >> 8) & 255, (r.").append(v).append("0 >> 16) & 255, (r.")
                    .append(v).append("0 >> 24) & 255)");
        }
        return rounds(text, false, 16, null, 64, new String[] {"a", "b", "c", "d"},
                new long[] {0x67452301L, 0xefcdab89L, 0x98badcfeL, 0x10325476L}, next, out.toString());
    }

    /** The SHA-1 of {@code text}'s UTF-8 bytes, as 40 lowercase hex digits; NULL for NULL. */
    static SqlWriter.Piece sha1(SqlWriter.Piece text) {
        String f = "CASE WHEN r.i < 20 THEN (r.b & r.c) | (~r.b & r.d) WHEN r.i < 40 THEN " + xor(xor("r.b", "r.c"),
                "r.d") + " WHEN r.i < 60 THEN (r.b & r.c) | (r.b & r.d) | (r.c & r.d) ELSE " + xor(xor("r.b", "r.c"),
                "r.d") + " END";
        String k = "CASE WHEN r.i < 20 THEN 1518500249 WHEN r.i < 40 THEN 1859775393 WHEN r.i < 60 THEN 2400959708"
                + " ELSE 3395469782 END";
        String t = "((" + rotl("r.a", "5") + " + (" + f + ") + r.e + " + k + " + " + at("__c.w", "r.i") + ") & " + M32
                + ")";
        String[] next = {t, "r.a", rotl("r.b", "30"), "r.c", "r.d"};
        return rounds(text, true, 80, (w, j) -> rotl(xor(xor(xor(at(w, j + " - 3"), at(w, j + " - 8")),
                at(w, j + " - 14")), at(w, j + " - 16")), "1"), 80, new String[] {"a", "b", "c", "d", "e"},
                new long[] {0x67452301L, 0xefcdab89L, 0x98badcfeL, 0x10325476L, 0xc3d2e1f0L}, next,
                hexWords(new String[] {"a", "b", "c", "d", "e"}));
    }

    /** The SHA-256 of {@code text}'s UTF-8 bytes, as 64 lowercase hex digits; NULL for NULL. */
    static SqlWriter.Piece sha256(SqlWriter.Piece text) {
        String s1 = xor(xor(rotr("r.e", 6), rotr("r.e", 11)), rotr("r.e", 25));
        String ch = xor("r.e & r.f", "~r.e & r.g");
        String t1 = "(r.h + " + s1 + " + " + ch + " + " + at(list(SHA256_K), "r.i") + " + " + at("__c.w", "r.i") + ")";
        String s0 = xor(xor(rotr("r.a", 2), rotr("r.a", 13)), rotr("r.a", 22));
        String maj = xor(xor("r.a & r.b", "r.a & r.c"), "r.b & r.c");
        String[] next = {"((" + t1 + " + " + s0 + " + " + maj + ") & " + M32 + ")", "r.a", "r.b", "r.c",
            "((r.d + " + t1 + ") & " + M32 + ")", "r.e", "r.f", "r.g"};
        String[] vars = {"a", "b", "c", "d", "e", "f", "g", "h"};
        return rounds(text, true, 64, (w, j) -> {
            String w2 = at(w, j + " - 2");
            String w15 = at(w, j + " - 15");
            String sigma0 = xor(xor(rotr(w15, 7), rotr(w15, 18)), "(" + w15 + " >> 3)");
            String sigma1 = xor(xor(rotr(w2, 17), rotr(w2, 19)), "(" + w2 + " >> 10)");
            return "((" + sigma1 + " + " + at(w, j + " - 7") + " + " + sigma0 + " + " + at(w, j + " - 16") + ") & "
                    + M32 + ")";
        }, 64, vars, new long[] {0x6a09e667L, 0xbb67ae85L, 0x3c6ef372L, 0xa54ff53aL, 0x510e527fL, 0x9b05688cL,
            0x1f83d9abL, 0x5be0cd19L}, next, hexWords(vars));
    }

    /** A schedule word after a chunk's 16: of the words so far ({@code w}, a JSON array) at position {@code j}. */
    private interface Schedule {
        String next(String w, String j);
    }

    /**
     * The walks: {@code __in} the text, {@code __m} its padded bytes' hex, {@code __w}/{@code __c} each chunk's
     * {@code words} words, {@code __r} the rounds ({@code rounds} per chunk; at the row after the last, the state is
     * the chunk's start state plus the round state), the digest {@code out} read off the row after the last chunk.
     */
    private static SqlWriter.Piece rounds(SqlWriter.Piece text, boolean bigEndian, int words,
            @com.legend.base.Nullable Schedule schedule, int rounds, String[] vars, long[] iv, String[] next,
            String out) {
        String word = word("__m.h", "__w.k * 128 + 8 * __w.j", bigEndian);
        String more = schedule == null ? word
                : "CASE WHEN __w.j < 16 THEN " + word + " ELSE " + schedule.next("__w.w", "__w.j") + " END";
        String last = "r.i = " + rounds;
        StringBuilder cols = new StringBuilder();
        StringBuilder start = new StringBuilder();
        StringBuilder step = new StringBuilder("CASE WHEN " + last + " THEN r.k + 1 ELSE r.k END, CASE WHEN " + last
                + " THEN 0 ELSE r.i + 1 END");
        for (int i = 0; i < vars.length; i++) {
            cols.append(", ").append(vars[i]);
            step.append(", CASE WHEN ").append(last).append(" THEN (r.").append(vars[i]).append("0 + r.").append(vars[i])
                    .append(") & ").append(M32).append(" ELSE ").append(next[i]).append(" END");
        }
        for (int i = 0; i < vars.length; i++) {
            cols.append(", ").append(vars[i]).append('0');
            step.append(", CASE WHEN ").append(last).append(" THEN (r.").append(vars[i]).append("0 + r.").append(vars[i])
                    .append(") & ").append(M32).append(" ELSE r.").append(vars[i]).append("0 END");
        }
        for (int pass = 0; pass < 2; pass++) {
            for (long v : iv) {
                start.append(", ").append(v);
            }
        }
        String n = "(length(hex(CAST(__in.v AS BLOB))) / 2)";
        String bits = "(" + n + " * 8)";
        String length;
        if (bigEndian) {
            length = "printf('%016X', " + bits + ")";
        } else {
            StringBuilder le = new StringBuilder("printf('" + "%02X".repeat(8) + "'");
            for (int j = 0; j < 8; j++) {
                le.append(", (").append(bits).append(" >> ").append(8 * j).append(") & 255");
            }
            length = le.append(")").toString();
        }
        String padded = "hex(CAST(__in.v AS BLOB)) || '80' || hex(zeroblob((((55 - " + n + ") % 64) + 64) % 64)) || "
                + length;
        return w -> w.append("(WITH RECURSIVE __in(v) AS (SELECT ").piece(text).append("), __m(h) AS (SELECT " + padded
                + " FROM __in WHERE __in.v IS NOT NULL), __w(k, j, w) AS (SELECT 0, 0, '[]' UNION ALL SELECT CASE WHEN"
                + " __w.j = " + words + " THEN __w.k + 1 ELSE __w.k END, CASE WHEN __w.j = " + words + " THEN 0 ELSE"
                + " __w.j + 1 END, CASE WHEN __w.j = " + words + " THEN '[]' ELSE json_insert(__w.w, '$[#]', " + more
                + ") END FROM __w, __m WHERE __w.j < " + words + " OR (__w.k + 1) * 128 < length(__m.h)), __c(k, w) AS"
                + " (SELECT k, w FROM __w WHERE j = " + words + "), __r(k, i" + cols + ") AS (SELECT 0, 0" + start
                + " UNION ALL SELECT " + step + " FROM __r AS r JOIN __c ON __c.k = r.k) SELECT " + out + " FROM __r AS"
                + " r WHERE r.k = (SELECT count(*) FROM __c) AND EXISTS (SELECT 1 FROM __m))");
    }

    /** The state words as hex, eight digits each, big-endian. */
    private static String hexWords(String[] vars) {
        StringBuilder out = new StringBuilder();
        for (String v : vars) {
            out.append(out.length() == 0 ? "" : " || ").append("printf('%08x', r.").append(v).append("0)");
        }
        return out.toString();
    }

    /** {@code x ^ y} of two 32-bit words. */
    private static String xor(String x, String y) {
        return "(((" + x + ") | (" + y + ")) & ~((" + x + ") & (" + y + ")))";
    }

    private static String rotl(String x, String s) {
        return "((((" + x + ") << (" + s + ")) | ((" + x + ") >> (32 - (" + s + ")))) & " + M32 + ")";
    }

    private static String rotr(String x, int s) {
        return "((((" + x + ") >> " + s + ") | ((" + x + ") << " + (32 - s) + ")) & " + M32 + ")";
    }

    /** Element {@code i} of the JSON array {@code array}. */
    private static String at(String array, String i) {
        return "json_extract(" + array + ", '$[' || (" + i + ") || ']')";
    }

    private static String list(long[] values) {
        StringBuilder out = new StringBuilder("'[");
        for (int i = 0; i < values.length; i++) {
            out.append(i == 0 ? "" : ",").append(values[i]);
        }
        return out.append("]'").toString();
    }

    private static String list(int[] values) {
        StringBuilder out = new StringBuilder("'[");
        for (int i = 0; i < values.length; i++) {
            out.append(i == 0 ? "" : ",").append(values[i]);
        }
        return out.append("]'").toString();
    }

    /** The 32-bit word whose first byte's hex is at offset {@code at} (0-based) of the hex text {@code h}. */
    private static String word(String h, String at, boolean bigEndian) {
        StringBuilder out = new StringBuilder("(");
        for (int j = 0; j < 4; j++) {
            String q = "(" + at + ") + " + (2 * j + 1);
            String b = "((instr(" + HEX + ", substr(" + h + ", " + q + ", 1)) - 1) * 16 + instr(" + HEX + ", substr(" + h
                    + ", " + q + " + 1, 1)) - 1)";
            out.append(j == 0 ? "" : " | ").append("(").append(b).append(" << ").append(bigEndian ? 8 * (3 - j) : 8 * j)
                    .append(")");
        }
        return out.append(")").toString();
    }
}
