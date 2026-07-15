package com.legend.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A parsed Pure {@code Runtime} declaration &mdash; binds one or more
 * {@code Mapping}s to a set of store-to-connection bindings.
 *
 * <p>Pure syntax:
 * <pre>
 *   Runtime my::MyRuntime
 *   {
 *     mappings: [ my::MyMapping ];
 *     connections:
 *     [
 *       store::PersonDb: store::InMemoryDuckDb
 *     ];
 *   }
 * </pre>
 *
 * <p>Or with an embedded JSON connection:
 * <pre>
 *   connections: [
 *     ModelStore: [
 *       json: #{ JsonModelConnection { class: ...; url: '...'; } }#
 *     ]
 *   ];
 * </pre>
 *
 * <p>Engine's record exposes a {@code getConnectionForStore(store)} convenience
 * helper and {@code hasJsonConnections()}. Core/'s parser record is pure data;
 * callers can compute those inline.
 *
 * @param qualifiedName       fully qualified runtime name
 * @param mappings            qualified names of bound {@code Mapping}s (declaration order)
 * @param connectionBindings  store qualified name &rarr; connection qualified name
 *                            (only for non-embedded connections)
 * @param jsonConnections     inline {@code JsonModelConnection} bindings parsed
 *                            from {@code #{ ... }#} islands
 */
public record RuntimeDefinition(
        String qualifiedName,
        List<String> mappings,
        Map<String, String> connectionBindings,
        List<JsonModelConnection> jsonConnections) implements PackageableElement {

    public RuntimeDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        mappings = mappings == null ? List.of() : List.copyOf(mappings);
        connectionBindings = connectionBindings == null ? Map.of() : Map.copyOf(connectionBindings);
        jsonConnections = jsonConnections == null ? List.of() : List.copyOf(jsonConnections);
    }
}
