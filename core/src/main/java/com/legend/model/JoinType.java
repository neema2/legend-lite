package com.legend.model;

/**
 * Per-hop join type annotation in a {@link JoinChainElement}, written
 * inside parens in mapping property syntax: {@code @J1 > (LEFT) @J2}.
 *
 * <p>Source-form synonyms are folded onto canonical engine values:
 * <ul>
 *   <li>{@code LEFT}, {@code LEFT_OUTER} &rarr; {@link #LEFT_OUTER}</li>
 *   <li>{@code RIGHT}, {@code RIGHT_OUTER} &rarr; {@link #RIGHT_OUTER}</li>
 *   <li>{@code FULL}, {@code FULL_OUTER} &rarr; {@link #FULL_OUTER}</li>
 *   <li>{@code OUTER} &rarr; {@link #OUTER}</li>
 *   <li>{@code INNER} &rarr; {@link #INNER}</li>
 * </ul>
 * The {@code _OUTER} forms are the canonical engine spelling; the short
 * forms exist for source ergonomics. Unknown identifiers throw &mdash;
 * no fallbacks per AGENTS.md invariant 4.
 */
public enum JoinType {
    INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, OUTER;

    /**
     * Map a source-form identifier (case-insensitive) to a canonical
     * {@link JoinType}.
     *
     * @throws IllegalArgumentException if {@code identifier} is not one of
     *         the documented spellings
     */
    public static JoinType fromIdentifier(String identifier) {
        return switch (identifier.toUpperCase()) {
            case "INNER"                   -> INNER;
            case "LEFT", "LEFT_OUTER"      -> LEFT_OUTER;
            case "RIGHT", "RIGHT_OUTER"    -> RIGHT_OUTER;
            case "FULL", "FULL_OUTER"      -> FULL_OUTER;
            case "OUTER"                   -> OUTER;
            default -> throw new IllegalArgumentException(
                    "unknown join type identifier: '" + identifier + "'");
        };
    }
}
