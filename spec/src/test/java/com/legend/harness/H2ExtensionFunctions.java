// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.harness;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * JDK-only mirrors of the engine's {@code LegendH2Extensions} SQL functions,
 * registered as H2 aliases on the golden-replay connection (correctness lane
 * C1: golden SQL calling {@code legend_h2_extension_*} previously DECLINED
 * verification — "Function not found"). Semantics copied from the engine
 * class verbatim; null-in-null-out like the originals.
 */
public final class H2ExtensionFunctions {

    private H2ExtensionFunctions() {
    }

    public static @com.legend.Nullable String legend_h2_extension_base64_encode(
            @com.legend.Nullable String s) {
        return s == null ? null
                : Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    public static @com.legend.Nullable String legend_h2_extension_base64_decode(
            @com.legend.Nullable String s) {
        return s == null ? null
                : new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
    }

    public static @com.legend.Nullable String legend_h2_extension_reverse_string(
            @com.legend.Nullable String s) {
        return s == null ? null : new StringBuilder(s).reverse().toString();
    }

    public static @com.legend.Nullable String legend_h2_extension_hash_md5(
            @com.legend.Nullable String s) {
        if (s == null) {
            return null;
        }
        try {
            byte[] d = java.security.MessageDigest.getInstance("MD5")
                    .digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(d.length * 2);
            for (byte b : d) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16))
                        .append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    // ---- Phase 0.6: the engine's remaining string functions (LegendH2Extensions.java) ----

    /** Engine semantics: null in → null; empty pad → the string; longer
     * than the width → truncated to the width; else left-padded. */
    public static @com.legend.Nullable String legend_h2_extension_lpad(
            @com.legend.Nullable String s, Integer width, @com.legend.Nullable String pad) {
        if (s == null) {
            return null;
        }
        if (pad == null || pad.isEmpty()) {
            return s;
        }
        if (s.length() > width) {
            return s.substring(0, width);
        }
        StringBuilder out = new StringBuilder();
        while (out.length() + s.length() < width) {
            out.append(pad);
        }
        return out.substring(0, width - s.length()) + s;
    }

    public static @com.legend.Nullable String legend_h2_extension_rpad(
            @com.legend.Nullable String s, Integer width, @com.legend.Nullable String pad) {
        if (s == null) {
            return null;
        }
        if (pad == null || pad.isEmpty()) {
            return s;
        }
        if (s.length() > width) {
            return s.substring(0, width);
        }
        StringBuilder out = new StringBuilder(s);
        while (out.length() < width) {
            out.append(pad);
        }
        return out.substring(0, width);
    }

    /** Engine semantics (commons StringUtils.split: the token is a SET of
     * separator characters, adjacent separators collapse, 1-based part; a
     * part past the end is null; part < 1 is an error). */
    public static @com.legend.Nullable String legend_h2_extension_split_part(
            @com.legend.Nullable String s, @com.legend.Nullable String token, Integer part) {
        if (part < 1) {
            throw new IllegalArgumentException("Split part must be greater than zero");
        }
        if (s == null) {
            return null;
        }
        java.util.List<String> parts = new java.util.ArrayList<>();
        StringBuilder cur = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            boolean sep = token == null ? Character.isWhitespace(c) : token.indexOf(c) >= 0;
            if (sep) {
                if (cur.length() > 0) {
                    parts.add(cur.toString());
                    cur.setLength(0);
                }
            } else {
                cur.append(c);
            }
        }
        if (cur.length() > 0) {
            parts.add(cur.toString());
        }
        return parts.size() >= part ? parts.get(part - 1) : null;
    }

    /** Levenshtein distance (the engine's commons-text LevenshteinDistance). */
    public static @com.legend.Nullable Integer legend_h2_extension_edit_distance(
            @com.legend.Nullable String a, @com.legend.Nullable String b) {
        if (a == null || b == null) {
            return null;
        }
        int[] prev = new int[b.length() + 1];
        int[] cur = new int[b.length() + 1];
        for (int j = 0; j <= b.length(); j++) {
            prev[j] = j;
        }
        for (int i = 1; i <= a.length(); i++) {
            cur[0] = i;
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                cur[j] = Math.min(Math.min(cur[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
            }
            int[] t = prev;
            prev = cur;
            cur = t;
        }
        return prev[b.length()];
    }

    /** Jaro-Winkler similarity (the engine's commons-text
     * JaroWinklerSimilarity: prefix scale 0.1, prefix up to 4). */
    public static @com.legend.Nullable Double legend_h2_extension_jaro_winkler_similarity(
            @com.legend.Nullable String a, @com.legend.Nullable String b) {
        if (a == null || b == null) {
            return null;
        }
        if (a.isEmpty() && b.isEmpty()) {
            return 1.0;
        }
        if (a.isEmpty() || b.isEmpty()) {
            return 0.0;
        }
        int range = Math.max(0, Math.max(a.length(), b.length()) / 2 - 1);
        boolean[] am = new boolean[a.length()];
        boolean[] bm = new boolean[b.length()];
        int matches = 0;
        for (int i = 0; i < a.length(); i++) {
            int lo = Math.max(0, i - range);
            int hi = Math.min(b.length() - 1, i + range);
            for (int j = lo; j <= hi; j++) {
                if (!bm[j] && a.charAt(i) == b.charAt(j)) {
                    am[i] = true;
                    bm[j] = true;
                    matches++;
                    break;
                }
            }
        }
        if (matches == 0) {
            return 0.0;
        }
        int t = 0;
        int k = 0;
        for (int i = 0; i < a.length(); i++) {
            if (am[i]) {
                while (!bm[k]) {
                    k++;
                }
                if (a.charAt(i) != b.charAt(k)) {
                    t++;
                }
                k++;
            }
        }
        double m = matches;
        double jaro = (m / a.length() + m / b.length() + (m - t / 2.0) / m) / 3.0;
        int prefix = 0;
        for (int i = 0; i < Math.min(4, Math.min(a.length(), b.length())); i++) {
            if (a.charAt(i) == b.charAt(i)) {
                prefix++;
            } else {
                break;
            }
        }
        return jaro + prefix * 0.1 * (1.0 - jaro);
    }

    /** The alias DDL for the replay connection — engine spellings, lite
     *  implementations. */
    public static java.util.List<String> aliases() {
        String cls = H2ExtensionFunctions.class.getName();
        java.util.List<String> out = new java.util.ArrayList<>();
        for (String fn : new String[]{"base64_encode", "base64_decode",
                "reverse_string", "hash_md5", "lpad", "rpad", "split_part",
                "edit_distance", "jaro_winkler_similarity"}) {
            out.add("CREATE ALIAS IF NOT EXISTS legend_h2_extension_" + fn
                    + " FOR \"" + cls + ".legend_h2_extension_" + fn + "\"");
        }
        return out;
    }
}
