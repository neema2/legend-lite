package com.legend.model;


/**
 * Closed set of binary comparison operators that may appear in a relational
 * {@link RelationalOperation.Comparison} node.
 *
 * <p>The parser collapses source-form synonyms onto canonical values so
 * downstream consumers can pattern-match on a finite set:
 * <ul>
 *   <li>{@code =} and {@code ==} both map to {@link #EQ}</li>
 *   <li>{@code !=} and {@code <>} both map to {@link #NEQ}</li>
 * </ul>
 *
 * <p>Per AGENTS.md invariant 4 (no fallbacks): the parser's token mapping
 * throws on any token that is not a comparison &mdash; callers must guard
 * with their own peek check before calling.
 */
public enum ComparisonOp {
    EQ, NEQ, LT, LTE, GT, GTE;

    /** Canonical SQL-style spelling for this op (used by tests and docs). */
    public String symbol() {
        return switch (this) {
            case EQ  -> "=";
            case NEQ -> "<>";
            case LT  -> "<";
            case LTE -> "<=";
            case GT  -> ">";
            case GTE -> ">=";
        };
    }
}
