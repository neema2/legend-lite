/**
 * Phase B, C &mdash; tokens &rarr; protocol records.
 *
 * <p>Drivers in this package:
 * <ul>
 *   <li>{@link com.legend.parser.PmcdParser} (B): a text &rarr; its
 *       {@code Protocol.PureModelContextData} (the engine's section walk; each
 *       {@code ###Pure} element through {@link com.legend.parser.ElementParser},
 *       every other section through its grammar).</li>
 *   <li>{@code SpecParser} (C): tokens &rarr;
 *       {@code protocol.spec.ValueSpecification}.</li>
 * </ul>
 *
 * <h2>Public surface</h2>
 * <pre>
 *   Protocol.PureModelContextData records = PmcdParser.parse(pureSource, GrammarLevel.LEGEND_LITE).model();
 *   // three dialect levels — see {@link com.legend.parser.GrammarLevel}
 * </pre>
 * The compiler's model is not the parser's: a text becomes one through the
 * text's door, {@code com.legend.door.ModelFromText}, which hands the records to
 * {@code ModelFromProtocol} (docs/PROTOCOL_PROGRAM_2026_10_05.md, leg 8's last step).
 *
 * <h2>Records</h2>
 * Parsed records live in {@link com.legend.protocol} (declarations)
 * and {@code com.legend.protocol.spec} (value specifications).
 *
 * <h2>Dependencies</h2>
 * The lexer, the protocol records, values, the shared error vocabulary, the
 * overlay seam ({@code spi}) and the JDK (ArchitectureTest, invariant 7c).
 */
package com.legend.parser;
