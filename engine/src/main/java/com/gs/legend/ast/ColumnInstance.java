package com.gs.legend.ast;

/**
 * Sealed family for tilde-column AST nodes — the parser-emitted DSL constructs
 * that denote column references in Pure relation functions.
 *
 * <p>Two shapes:
 * <ul>
 *   <li>{@link ColSpec} — single column ({@code ~name}, {@code ~name:fn}, {@code ~name:fn1:fn2})</li>
 *   <li>{@link ColSpecArray} — list of columns ({@code ~[name1, name2, ...]})</li>
 * </ul>
 *
 * <p>Every site that historically dispatched on
 * {@code ClassInstance ci && "colSpec".equals(ci.type())}
 * or {@code ci.value() instanceof ColSpec} now pattern-matches directly on
 * {@code ColumnInstance} or its subtypes.
 *
 * <p>Type-layer refinements ({@code FuncColSpec}, {@code AggColSpec},
 * {@code FuncColSpecArray}, {@code AggColSpecArray}) remain in {@link com.gs.legend.model.m3.LClass}
 * and dispatch by inspecting {@link ColSpec#function2()} on the same AST shape — they are
 * not separate AST kinds.
 */
public sealed interface ColumnInstance extends ValueSpecification
        permits ColSpec, ColSpecArray {
}
