// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.values;

/**
 * When two spellings name the SAME column.
 *
 * <h2>Why this exists as one place</h2>
 *
 * <p>A relational identifier KEEPS ITS QUOTES as the wire name
 * ({@code DatabaseProtocolParser.parseIdentifier}, matching the
 * engine's protocol): a Database declaring {@code "total pnl"}
 * carries a column literally named {@code "total pnl"}, quotes
 * included. Every Pure-side reference to it — {@code ~'total pnl'},
 * {@code $x.'total pnl'} — is bare. So any layer comparing the two
 * must normalize, and EVERY layer must normalize the same way.
 *
 * <p>They did not. {@code InferenceKernel} stripped the quotes, so
 * {@code select} and {@code filter} worked; {@code Fold} compared
 * with {@code equals}, so a SORT KEY or a GROUPBY KEY on the same
 * column failed with "cannot be resolved after isolation". A space
 * or a comma in a header was enough to trigger it. It was found by a
 * DataCube stress run over non-ASCII headers and looked like a
 * unicode problem, which it was not — unicode files simply have more
 * columns needing quotes.
 *
 * <p>Two copies of a normalization rule is what let them drift, so
 * there is now one, and both call it.
 */
public final class ColumnNames {

    private ColumnNames() {
    }

    /** Whether two spellings name the same column. */
    public static boolean same(String a, String b) {
        return a.equals(b) || bare(a).equals(bare(b));
    }

    /**
     * A quoted identifier's inner text, unescaped.
     *
     * <p>The escape inside a quoted identifier is the BACKSLASH —
     * {@code Lexer.scanQuotedString} skips two characters after one,
     * and ends the token at the first UNescaped quote. So
     * {@code "a\"b"} is a single token whose raw text still carries
     * the backslash, and decoding it here is what lets a column
     * genuinely called {@code a"b} be named at all. Without the
     * decode the outer quotes came off and the backslash stayed,
     * which matched nothing.
     */
    public static String bare(String name) {
        if (name.length() < 2 || name.charAt(0) != '"'
                || name.charAt(name.length() - 1) != '"') {
            return name;
        }
        String inner = name.substring(1, name.length() - 1);
        if (inner.indexOf('\\') < 0) {
            return inner;
        }
        StringBuilder out = new StringBuilder(inner.length());
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '\\' && i + 1 < inner.length()) {
                out.append(inner.charAt(++i));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}
