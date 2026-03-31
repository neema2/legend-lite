package com.gs.legend.model.def;

import java.util.Objects;

/**
 * Represents a single element in a multi-hop join chain.
 * 
 * Pure syntax examples:
 * <pre>
 * @JoinName                          → JoinChainElement("JoinName", null, null)
 * (INNER) @JoinName                  → JoinChainElement("JoinName", "INNER", null)
 * (LEFT_OUTER) [DB2]@JoinName        → JoinChainElement("JoinName", "LEFT_OUTER", "DB2")
 * </pre>
 * 
 * Multi-hop chain: {@code @J1 > (INNER) @J2 > @J3}
 * 
 * @param joinName     The join name
 * @param joinType     The join type ("INNER", "LEFT_OUTER", "RIGHT_OUTER", "OUTER") or null for default
 * @param databaseName The database name override for this hop, or null to use the parent's database
 */
public record JoinChainElement(
        String joinName,
        String joinType,
        String databaseName
) {
    public JoinChainElement {
        Objects.requireNonNull(joinName, "Join name cannot be null");
    }

    /**
     * Creates a simple join chain element with no join type or database override.
     */
    public static JoinChainElement of(String joinName) {
        return new JoinChainElement(joinName, null, null);
    }

    /**
     * Creates a join chain element with a specific join type.
     */
    public static JoinChainElement withType(String joinName, String joinType) {
        return new JoinChainElement(joinName, joinType, null);
    }
}
