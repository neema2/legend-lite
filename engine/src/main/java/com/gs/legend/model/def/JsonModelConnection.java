package com.gs.legend.model.def;

/**
 * Represents a JSON model connection — provides JSON data for an M2M source class.
 *
 * <p>Pure syntax (embedded in Runtime):
 * <pre>
 * connections: [
 *     ModelStore: [
 *         json: #{
 *             JsonModelConnection {
 *                 class: model::RawPerson;
 *                 url: 'data:application/json,[{"firstName":"John"}]';
 *             }
 *         }#
 *     ]
 * ];
 * </pre>
 *
 * <p>Pure data record — no I/O methods. The dialect renders the URL into
 * an inline VARIANT subquery at plan generation time.
 *
 * @param className The source class name (qualified)
 * @param url       The data URL (data: URI, file:, or http:)
 */
public record JsonModelConnection(
        String className,
        String url) {
}