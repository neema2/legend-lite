package org.finos.legend.engine.serialization;

import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.execution.Column;
import org.finos.legend.engine.execution.Row;
import org.finos.legend.engine.execution.StreamingResult;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * JSON serializer for query results.
 * 
 * Produces a JSON array of objects, where each object represents a row
 * with column names as keys.
 * 
 * Supports true streaming - rows are written incrementally.
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
    public void serialize(BufferedResult result, OutputStream out) throws IOException {
        try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            writeRows(writer, result.columns(), result.rows().iterator());
        }
    }

    @Override
    public void serializeStreaming(StreamingResult result, OutputStream out) throws IOException {
        try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            writeRows(writer, result.columns(), result.iterator());
        }
    }

    private void writeRows(Writer writer, List<Column> columns, Iterator<Row> rows) throws IOException {
        writer.write('[');

        boolean first = true;
        while (rows.hasNext()) {
            if (!first) {
                writer.write(',');
            }
            first = false;

            writeRow(writer, columns, rows.next());
        }

        writer.write(']');
    }

    private void writeRow(Writer writer, List<Column> columns, Row row) throws IOException {
        writer.write('{');

        List<Object> values = row.values();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                writer.write(',');
            }

            writer.write('"');
            writeEscaped(writer, columns.get(i).name());
            writer.write("\":");
            writeValue(writer, values.get(i));
        }

        writer.write('}');
    }

    private void writeValue(Writer writer, Object value) throws IOException {
        if (value == null) {
            writer.write("null");
        } else if (value instanceof Boolean) {
            writer.write(value.toString());
        } else if (value instanceof Number) {
            writer.write(value.toString());
        } else {
            writer.write('"');
            writeEscaped(writer, value.toString());
            writer.write('"');
        }
    }

    private void writeEscaped(Writer writer, String s) throws IOException {
        if (s == null)
            return;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> writer.write("\\\"");
                case '\\' -> writer.write("\\\\");
                case '\n' -> writer.write("\\n");
                case '\r' -> writer.write("\\r");
                case '\t' -> writer.write("\\t");
                default -> writer.write(c);
            }
        }
    }
}
