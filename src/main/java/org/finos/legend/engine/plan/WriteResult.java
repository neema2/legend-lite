package org.finos.legend.engine.plan;

import java.util.List;
import java.util.Objects;

/**
 * Result of a write (mutation) operation.
 * 
 * @param rowsAffected Number of rows inserted/updated/deleted
 * @param generatedKeys List of generated keys (for INSERT with auto-increment)
 */
public record WriteResult(
        int rowsAffected,
        List<Object> generatedKeys,
        boolean success,
        String message
) {
    public WriteResult {
        Objects.requireNonNull(generatedKeys, "Generated keys cannot be null");
    }
    
    /**
     * Creates a successful write result.
     */
    public static WriteResult success(int rowsAffected, List<Object> generatedKeys) {
        return new WriteResult(rowsAffected, generatedKeys, true, null);
    }
    
    /**
     * Creates a successful write result with no generated keys.
     */
    public static WriteResult success(int rowsAffected) {
        return success(rowsAffected, List.of());
    }
    
    /**
     * Creates a failed write result.
     */
    public static WriteResult failure(String message) {
        return new WriteResult(0, List.of(), false, message);
    }
}
