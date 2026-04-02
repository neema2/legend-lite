package com.gs.legend.model.def;

import java.util.Objects;

/**
 * Represents a single element in a multi-hop join chain.
 * 
 * Pure syntax examples:
 * <pre>
 * @JoinName                          → JoinChainElement("JoinName", null, null, false)
 * (INNER) @JoinName                  → JoinChainElement("JoinName", "INNER", null, false)
 * (LEFT_OUTER) [DB2]@JoinName        → JoinChainElement("JoinName", "LEFT_OUTER", "DB2", false)
 * </pre>
 * 
 * Multi-hop chain: {@code @J1 > (INNER) @J2 > @J3}
 * Future strict chain: {@code @J1 >> @J2} (enforces 1-to-1 via scalar subquery)
 * 
 * @param joinName     The join name
 * @param joinType     The join type ("INNER", "LEFT_OUTER", "RIGHT_OUTER", "OUTER") or null for default
 * @param databaseName The database name override for this hop, or null to use the parent's database
 * @param strict       If true, enforce 1-to-1 cardinality (future: use scalar subquery instead of LEFT OUTER JOIN)
 */
public record JoinChainElement(
        String joinName,
        String joinType,
        String databaseName,
        boolean strict
) {
    public JoinChainElement {
        Objects.requireNonNull(joinName, "Join name cannot be null");
    }

    /**
     * Creates a simple join chain element with no join type or database override.
     */
    public static JoinChainElement of(String joinName) {
        return new JoinChainElement(joinName, null, null, false);
    }

    /**
     * Creates a join chain element with a specific join type.
     */
    public static JoinChainElement withType(String joinName, String joinType) {
        return new JoinChainElement(joinName, joinType, null, false);
    }
}
