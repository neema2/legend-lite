package com.gs.legend.server;

/**
 * Serialization format for {@link QueryService#execute(String, String, String,
 * java.sql.Connection, java.io.OutputStream, OutputFormat)} output.
 *
 * <p>Describes how rows are formatted on the wire — NOT whether they are
 * materialized or streamed. The choice of snapshot vs streaming is made by
 * calling {@code execute(..., out, fmt)} vs {@code stream(..., out)}.
 *
 * <p>Each constant carries two pieces of metadata:
 * <ul>
 *   <li>{@link #id()} — stable string identifier used by
 *       {@link com.gs.legend.serial.SerializerRegistry} for dispatch.</li>
 *   <li>{@link #contentType()} — the MIME type servers should send with
 *       responses of this format.</li>
 * </ul>
 */
public enum OutputFormat {
    JSON("json", "application/json"),
    CSV("csv", "text/csv");

    private final String id;
    private final String contentType;

    OutputFormat(String id, String contentType) {
        this.id = id;
        this.contentType = contentType;
    }

    public String id() {
        return id;
    }

    public String contentType() {
        return contentType;
    }
}
