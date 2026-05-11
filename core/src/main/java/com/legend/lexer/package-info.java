/**
 * Phase A — text &rarr; tokens.
 *
 * <p>Hand-rolled batch lexer for Pure source.
 *
 * <h2>Public surface</h2>
 * <pre>
 *   TokenStream stream = Lexer.tokenize(pureSource);
 *   for (int i = 0; i &lt; stream.count(); i++) {
 *       TokenType t = stream.type(i);
 *       String text = stream.text(i);
 *       // or: Token token = stream.at(i);  // materialize record
 *   }
 * </pre>
 *
 * <h2>Storage</h2>
 * {@link com.legend.lexer.TokenStream} holds three parallel {@code int[]}
 * arrays internally (type ordinal, source start, source end) for cache
 * locality and low GC pressure on large corpora &mdash; ~10&times; smaller than
 * a {@code List<Token>} for the same token count. {@link com.legend.lexer.Token}
 * records are materialized lazily on demand via {@link com.legend.lexer.TokenStream#at(int)}.
 *
 * <h2>Dependencies</h2>
 * JDK only. No knowledge of grammar above the token level &mdash; the
 * {@code parser/} layer interprets token sequences into
 * {@code parser.element.PackageableElement} and
 * {@code parser.spec.ValueSpecification} trees.
 */
package com.legend.lexer;
