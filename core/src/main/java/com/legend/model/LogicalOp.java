package com.legend.model;

/**
 * Closed set of binary logical connectives used in relational filter and
 * join expressions ({@link RelationalOperation.BooleanOp}).
 *
 * <p>Engine grammar accepts the keywords lowercase ({@code and} / {@code or}).
 * Per AGENTS.md invariant 4 (no fallbacks): {@link #fromKeyword(String)}
 * throws on anything that isn't one of those two &mdash; callers must
 * guard with a peek check before calling.
 */
public enum LogicalOp {
    AND, OR;

    /**
     * Map a source keyword to its op.
     *
     * @throws IllegalArgumentException if {@code keyword} is not {@code "and"}
     *                                  or {@code "or"}
     */
    public static LogicalOp fromKeyword(String keyword) {
        return switch (keyword) {
            case "and" -> AND;
            case "or"  -> OR;
            default -> throw new IllegalArgumentException(
                    "not a logical operator keyword: '" + keyword + "'");
        };
    }

    /** Lowercase keyword spelling as it appears in source. */
    public String keyword() {
        return switch (this) {
            case AND -> "and";
            case OR  -> "or";
        };
    }
}
