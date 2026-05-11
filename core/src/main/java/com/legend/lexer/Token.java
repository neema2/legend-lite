package com.legend.lexer;

/**
 * A single token, materialized.
 *
 * <p>Pure-data record &mdash; this is the public, identity-free
 * representation of a token. The {@link Lexer} stores tokens
 * internally as three parallel {@code int[]} arrays inside
 * {@link TokenStream}; {@code Token} instances are allocated on
 * demand (via {@link TokenStream#at(int)} or
 * {@link TokenStream#asList()}) for tests, debugging, and any
 * downstream code that prefers an object API to positional access.
 *
 * @param type  the token classification
 * @param text  the verbatim source slice {@code source.substring(start, end)}
 * @param start byte offset (inclusive) into the original Pure source
 * @param end   byte offset (exclusive) into the original Pure source
 */
public record Token(TokenType type, String text, int start, int end) {}
