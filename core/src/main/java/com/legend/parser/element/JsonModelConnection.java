package com.legend.parser.element;

import java.util.Objects;

/**
 * A JSON model connection embedded in a {@code Runtime}'s {@code connections}
 * block. Provides inline JSON data as the source for an M2M source class.
 *
 * <p>Pure syntax (embedded form, parsed from inside a {@code #{ ... }#} island):
 * <pre>
 *   connections: [
 *     ModelStore: [
 *       json: #{
 *         JsonModelConnection {
 *           class: model::RawPerson;
 *           url: 'data:application/json,[{"firstName":"John"}]';
 *         }
 *       }#
 *     ]
 *   ];
 * </pre>
 *
 * @param className the source class (qualified name, unresolved)
 * @param url       the data URL ({@code data:} URI, {@code file:}, or {@code http:})
 */
public record JsonModelConnection(String className, String url) {
    public JsonModelConnection {
        Objects.requireNonNull(className, "className cannot be null");
        Objects.requireNonNull(url, "url cannot be null");
    }
}
