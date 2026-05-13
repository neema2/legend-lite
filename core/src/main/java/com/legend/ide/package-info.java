/**
 * Dormant home for IDE / language-server infrastructure built on top of
 * the batch compiler.
 *
 * <h2>Status</h2>
 * Currently unused by the batch pipeline. Only the classes in this
 * package and their own tests exercise its code paths. Activated when a
 * future {@code legend-ide} layer (analogous to {@code rust-analyzer}
 * relative to {@code rustc}, or {@code tsserver} relative to {@code tsc})
 * is built on top of the batch compiler.
 *
 * <h2>What lives here</h2>
 * Demand-driven element parsing infrastructure:
 * <ul>
 *   <li>{@link com.legend.ide.ModelIndex} &mdash; FQN &rarr; token range
 *       map produced by a shallow scan.</li>
 *   <li>{@link com.legend.ide.ModelIndexer} &mdash; the shallow scanner;
 *       finds element boundaries without parsing bodies.</li>
 *   <li>{@link com.legend.ide.ModelOrchestrator} &mdash; demand-driven
 *       parser facade. Given an FQN, slices the token range out of the
 *       parent stream and runs {@code ElementParser.parseSingle(slice)}.
 *       Memoizes results.</li>
 * </ul>
 *
 * <h2>What does NOT live here</h2>
 * The batch compiler (parser, name resolver, model builder, type
 * checker, lowerer, dialect, executor) parses eagerly via
 * {@link com.legend.parser.ElementParser#parse(String)} and does not
 * consult this package. Compiler layers must not import
 * {@code com.legend.ide.*}.
 *
 * <h2>Why separate?</h2>
 * Batch compilers (rustc, javac) parse everything up front; their
 * layered invariant is "each step has a single responsibility, reads
 * its input, writes its output, no back-channels". IDE / language
 * servers (rust-analyzer, tsserver) need demand-driven parsing keyed by
 * editor cursor / hover position. The two architectures conflict if
 * fused. Legend-lite chooses to be batch first; the IDE infrastructure
 * waits in this package until a wrapping IDE layer needs it.
 */
package com.legend.ide;
