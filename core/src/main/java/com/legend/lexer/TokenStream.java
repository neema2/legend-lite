package com.legend.lexer;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable result of lexing &mdash; an indexable sequence of tokens
 * stored as three parallel {@code int[]} arrays for cache locality and
 * low memory footprint.
 *
 * <p>Constructed only by {@link Lexer#tokenize(String)}.
 *
 * <h2>Storage</h2>
 * The arrays {@code types}, {@code starts}, {@code ends} may hold
 * slack capacity beyond {@link #count()} (the last doubling grow in
 * {@link Lexer} typically over-allocates). Only indices {@code 0 ≤ i &lt; count()}
 * are valid. {@code types[i]} holds the ordinal of a {@link TokenType};
 * {@code starts[i]} and {@code ends[i]} are byte offsets into the
 * original source string. Token text is computed on demand by
 * {@code source.substring(starts[i], ends[i])} &mdash; no {@code String}
 * is allocated up front.
 *
 * <p>The arrays are handed off by reference from {@link Lexer} with no
 * {@code Arrays.copyOf} trim &mdash; matches engine's hot-path-and-tail
 * memory profile.
 *
 * <h2>Access patterns</h2>
 * <ul>
 *   <li><strong>Hot paths</strong> (parser, name resolution): use the
 *       positional accessors {@link #type(int)}, {@link #start(int)},
 *       {@link #end(int)}, {@link #textEquals(int, String)}. Zero
 *       allocation per token read.</li>
 *   <li><strong>Tests, debug, error reporting</strong>: use
 *       {@link #at(int)} to materialize a single {@link Token} record,
 *       or {@link #asList()} for all of them. Allocates.</li>
 * </ul>
 */
public final class TokenStream {

    /** Cached {@code TokenType.values()} to avoid array allocation on every {@link #type(int)} call. */
    private static final TokenType[] TOKEN_TYPES = TokenType.values();

    private final String source;
    private final int count;
    private final int[] types;
    private final int[] starts;
    private final int[] ends;

    /** Package-private constructor &mdash; only {@link Lexer} creates {@code TokenStream}s. */
    TokenStream(String source, int count, int[] types, int[] starts, int[] ends) {
        this.source = source;
        this.count = count;
        this.types = types;
        this.starts = starts;
        this.ends = ends;
    }

    /** Number of tokens in the stream. Arrays may have slack capacity beyond this; do not exceed it. */
    public int count() {
        return count;
    }

    /** The original source string that produced this stream. */
    public String source() {
        return source;
    }

    /** Token type at index {@code i}. Zero allocation. */
    public TokenType type(int i) {
        return TOKEN_TYPES[types[i]];
    }

    /** Source start offset (inclusive) of token at index {@code i}. */
    public int start(int i) {
        return starts[i];
    }

    /** Source end offset (exclusive) of token at index {@code i}. */
    public int end(int i) {
        return ends[i];
    }

    /** Verbatim source slice for token at index {@code i}. Allocates one {@code String}. */
    public String text(int i) {
        return source.substring(starts[i], ends[i]);
    }

    /**
     * Zero-allocation text comparison &mdash; checks whether the token at
     * index {@code i} has source text equal to {@code expected}, without
     * materializing a substring. Use on hot paths instead of
     * {@code text(i).equals(expected)}.
     */
    public boolean textEquals(int i, String expected) {
        int start = starts[i];
        int len = ends[i] - start;
        if (len != expected.length()) return false;
        for (int j = 0; j < len; j++) {
            if (source.charAt(start + j) != expected.charAt(j)) return false;
        }
        return true;
    }

    /** Materialize the token at index {@code i} as a {@link Token} record. */
    public Token at(int i) {
        return new Token(type(i), text(i), starts[i], ends[i]);
    }

    /**
     * Materialize all tokens as a {@code List<Token>}. Allocates one
     * {@link Token} record + one substring per token &mdash; not for hot
     * paths. Use for tests, debugging, and serialization.
     */
    public List<Token> asList() {
        List<Token> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(at(i));
        }
        return list;
    }
}
