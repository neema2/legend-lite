package com.gs.legend.serial;

import com.gs.legend.exec.Column;
import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.Row;

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
            List<Column> columns = result.columns();
            writer.write('[');

            boolean first = true;
            for (Row row : result.rows()) {
                if (!first) {
                    writer.write(',');
                }
                first = false;
                writeRow(writer, columns, row);
            }

            writer.write(']');
        }
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
