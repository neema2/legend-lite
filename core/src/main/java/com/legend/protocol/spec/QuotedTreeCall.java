// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.spec;

import java.util.Objects;

/**
 * {@code compileLegendValueSpecification('#{...}#')} with a
 * PARSE-TIME-FOLDABLE string argument — the quote/eval native's literal
 * case, folded where strings die: in the parser.
 *
 * <p>Two faces, exactly the {@link GraphFetchLiteral} contract:
 * <ul>
 *   <li><b>wire</b>: {@link #original()} — the call emits byte-verbatim as
 *       the ordinary {@code AppliedFunction} + string the user wrote (the
 *       engine's wire carries the string OPAQUE too; its
 *       {@code LegendCompile} native parses at runtime);</li>
 *   <li><b>pipeline</b>: {@link #tree()} — the string's parse product
 *       (LEGEND_ENGINE, the level the engine's
 *       {@code PureGrammarParser.parseModel} routing implies), consumed by
 *       {@code GraphFetchChecker} without ever touching the parser.</li>
 * </ul>
 *
 * <p>Non-literal arguments (runtime-composed strings) never fold — they
 * stay plain {@code AppliedFunction}, typed {@code Any[1]} like the
 * engine's native, and wall loudly wherever a static tree is required.
 */
public record QuotedTreeCall(
        AppliedFunction original,
        ValueSpecification tree,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {

    public QuotedTreeCall {
        Objects.requireNonNull(original, "original");
        Objects.requireNonNull(tree, "tree");
    }
}
