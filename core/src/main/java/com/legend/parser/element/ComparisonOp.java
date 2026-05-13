package com.legend.parser.element;

import com.legend.lexer.TokenType;

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
 * <p>Per AGENTS.md invariant 4 (no fallbacks): {@link #fromToken(TokenType)}
 * throws on any token that is not a comparison &mdash; callers must guard
 * with their own peek check before calling.
 */
public enum ComparisonOp {
    EQ, NEQ, LT, LTE, GT, GTE;

    /**
     * Map a lexer token to its canonical comparison op.
     *
     * @throws IllegalArgumentException if {@code t} is not a comparison token
     */
    public static ComparisonOp fromToken(TokenType t) {
        return switch (t) {
            case EQUAL, TEST_EQUAL              -> EQ;
            case TEST_NOT_EQUAL, NOT_EQUAL      -> NEQ;
            case LESS_THAN                      -> LT;
            case LESS_OR_EQUAL                  -> LTE;
            case GREATER_THAN                   -> GT;
            case GREATER_OR_EQUAL               -> GTE;
            default -> throw new IllegalArgumentException(
                    "not a comparison token: " + t);
        };
    }

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
