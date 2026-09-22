// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.spec;

import java.util.List;
import java.util.Objects;

/**
 * {@code compileLegendGrammar('function my::f(): T[1] { ... }')} with a
 * PARSE-TIME-FOLDABLE string argument whose payload is FUNCTIONS ONLY —
 * the quote/eval native's literal case for grammar text, folded where
 * strings die: in the parser (the same two-faced contract as
 * {@link QuotedTreeCall}).
 *
 * <ul>
 *   <li><b>wire</b>: {@link #original()} — the call emits byte-verbatim
 *       (the engine's wire carries the grammar string OPAQUE; its
 *       {@code LegendCompile} native parses at runtime);</li>
 *   <li><b>pipeline</b>: {@link #functions()} — each parsed function
 *       definition AS ITS LAMBDA (declared parameters + body): the value
 *       {@code compileLegendGrammar(...)} denotes is the element
 *       collection ({@code PackageableElement[*]}), and a function
 *       element's VALUE is its lambda — {@code ->at(i)->cast(
 *       @FunctionDefinition<{...}>)} then feeds the router exactly as a
 *       lambda literal does.</li>
 * </ul>
 *
 * <p>A payload carrying any non-function element (classes, mappings,
 * connections — a MODEL in a string) never folds: it stays the plain
 * {@code AppliedFunction} and walls loudly as the unported native
 * (that route is the compile-once model overlay, a separate leg).
 */
public record QuotedGrammarCall(
        AppliedFunction original,
        List<LambdaFunction> functions,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {

    public QuotedGrammarCall {
        Objects.requireNonNull(original, "original");
        functions = List.copyOf(Objects.requireNonNull(functions, "functions"));
        if (functions.isEmpty()) {
            throw new IllegalArgumentException(
                    "a folded grammar payload carries at least one function");
        }
    }
}
