package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Map;

/**
 * Represents a runtime definition that binds mappings to connections.
 * 
 * Pure syntax:
 * ```
 * ###Runtime
 * Runtime my::MyRuntime
 * {
 * mappings: [ my::MyMapping ];
 * connections:
 * [
 * store::PersonDb: store::InMemoryDuckDb
 * ];
 * }
 * ```
 */
public record RuntimeDefinition(
        String qualifiedName,
        List<String> mappings,
        Map<String, String> connectionBindings) implements PureDefinition {

    /**
     * Returns the simple name (last part after ::).
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * Returns the package path (everything before the last ::).
     */
    public String packagePath() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }

    /**
     * Gets the connection name for a given store.
     */
    public String getConnectionForStore(String storeName) {
        return connectionBindings.get(storeName);
    }
}
