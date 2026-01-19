package org.finos.legend.engine.serialization;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for discovering and retrieving result serializers.
 * 
 * Built-in serializers (JSON, CSV) are registered automatically.
 * Additional serializers (Arrow, Protobuf, Parquet) can be registered
 * via explicit calls or ServiceLoader.
 * 
 * GraalVM native-image compatible.
 */
public final class SerializerRegistry {

    private static final Map<String, ResultSerializer> SERIALIZERS = new ConcurrentHashMap<>();

    static {
        // Register built-in serializers (no external dependencies)
        register(JsonSerializer.INSTANCE);
        register(CsvSerializer.INSTANCE);
    }

    private SerializerRegistry() {
        // Static utility class
    }

    /**
     * Registers a serializer.
     * Replaces any existing serializer with the same format ID.
     */
    public static void register(ResultSerializer serializer) {
        SERIALIZERS.put(serializer.formatId(), serializer);
    }

    /**
     * Gets a serializer by format ID.
     * 
     * @throws IllegalArgumentException if no serializer is registered for the
     *                                  format
     */
    public static ResultSerializer get(String formatId) {
        ResultSerializer serializer = SERIALIZERS.get(formatId);
        if (serializer == null) {
            throw new IllegalArgumentException("Unknown serialization format: " + formatId +
                    ". Available formats: " + availableFormats());
        }
        return serializer;
    }

    /**
     * Gets a serializer by format ID, or null if not found.
     */
    public static ResultSerializer getOrNull(String formatId) {
        return SERIALIZERS.get(formatId);
    }

    /**
     * Returns the set of available format IDs.
     */
    public static Set<String> availableFormats() {
        return Collections.unmodifiableSet(SERIALIZERS.keySet());
    }

    /**
     * Checks if a format is supported.
     */
    public static boolean isSupported(String formatId) {
        return SERIALIZERS.containsKey(formatId);
    }
}
