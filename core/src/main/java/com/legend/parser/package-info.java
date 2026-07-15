/**
 * Phase B, C, D &mdash; tokens &rarr; resolved syntax.
 *
 * <p>Drivers in this package:
 * <ul>
 *   <li>{@link com.legend.parser.ElementParser} (B): tokens &rarr;
 *       {@link com.legend.parser.element.PackageableElement} sequence wrapped in
 *       {@link com.legend.parser.ParsedModel}.</li>
 *   <li>{@code SpecParser} (C): tokens &rarr;
 *       {@code parser.spec.ValueSpecification}.</li>
 *   <li>{@code NameResolver} (D, in {@code compiler/}): simple names &rarr; FQNs over both
 *       declarations and specs.</li>
 * </ul>
 *
 * <h2>Public surface</h2>
 * <pre>
 *   ParsedModel model = ElementParser.parse(pureSource);   // text overload
 *   ParsedModel model = ElementParser.parse(tokenStream);  // pre-lexed overload
 * </pre>
 *
 * <h2>Records</h2>
 * Parsed records live in {@link com.legend.parser.element} (declarations)
 * and {@code com.legend.parser.spec} (value specifications). Class names
 * mirror engine's {@code com.gs.legend.model.def} / {@code com.gs.legend.ast}
 * verbatim for parity testing.
 *
 * <h2>Dependencies</h2>
 * Only {@link com.legend.lexer} (upstream) and the JDK.
 */
package com.legend.parser;
