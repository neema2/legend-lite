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
 * <p>Sites pattern-match directly on {@code ColumnInstance} (the family) or on
 * {@link ColSpec} / {@link ColSpecArray} (specific shapes).
 *
 * <p>Type-layer refinements ({@code FuncColSpec}, {@code AggColSpec},
 * {@code FuncColSpecArray}, {@code AggColSpecArray}) remain in {@link com.gs.legend.model.m3.LClass}
 * and dispatch by inspecting {@link ColSpec#function2()} on the same AST shape — they are
 * not separate AST kinds.
 */
public sealed interface ColumnInstance extends ValueSpecification
        permits ColSpec, ColSpecArray {
}
