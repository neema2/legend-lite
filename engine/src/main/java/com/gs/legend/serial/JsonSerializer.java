package com.gs.legend.serial;

import com.gs.legend.exec.Column;
import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.Row;
import com.gs.legend.util.Json;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * JSON serializer for query results.
 *
 * Produces a JSON array of objects, where each object represents a row
 * with column names as keys.
 *
 * GraalVM native-image compatible (no external dependencies).
 */
public final class JsonSerializer implements ResultSerializer {

    public static final JsonSerializer INSTANCE = new JsonSerializer();

    private JsonSerializer() {
    }

    @Override
    public String formatId() {
        return "json";
    }

    @Override
    public String contentType() {
        return "application/json";
    }

    @Override
    public boolean supportsStreaming() {
        return true;
    }

    @Override
    public void serialize(ExecutionResult result, OutputStream out) throws IOException {
        try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            // Streaming: Json.Writer wraps the response Writer directly, so bytes
            // flow to the OutputStream as rows are emitted — no materialization.
            Json.Writer w = Json.compactWriter(writer);
            List<Column> columns = result.columns();
            w.beginArray();
            for (Row row : result.rows()) {
                writeRow(w, columns, row);
            }
            w.endArray();
        }
    }

    private static void writeRow(Json.Writer w, List<Column> columns, Row row) {
        w.beginObject();
        List<Object> values = row.values();
        for (int i = 0; i < columns.size(); i++) {
            w.name(columns.get(i).name());
            writeValue(w, values.get(i));
        }
        w.endObject();
    }

    private static void writeValue(Json.Writer w, Object value) {
        if (value == null) {
            w.writeNull();
        } else if (value instanceof Boolean b) {
            w.writeBool(b);
        } else if (value instanceof Number n) {
            if (n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte) {
                w.writeLong(n.longValue());
            } else {
                w.writeDouble(n.doubleValue());
            }
        } else {
            // JDBC types (Timestamp, LocalDate, byte[], ...) are emitted as strings;
            // matches pre-convergence behavior.
            w.writeString(value.toString());
        }
    }
}
