package com.gs.legend.serial;
import com.gs.legend.exec.*;
import com.gs.legend.exec.Column;
import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.Row;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * CSV serializer for query results.
 *
 * Produces RFC 4180 compliant CSV with header row.
 * Values containing commas, quotes, or newlines are properly escaped.
 *
 * GraalVM native-image compatible (no external dependencies).
 */
public final class CsvSerializer implements ResultSerializer {

    public static final CsvSerializer INSTANCE = new CsvSerializer();

    private static final char DELIMITER = ',';
    private static final char QUOTE = '"';
    private static final String LINE_ENDING = "\r\n";

    private CsvSerializer() {
    }

    @Override
    public String formatId() {
        return "csv";
    }

    @Override
    public String contentType() {
        return "text/csv";
    }

    @Override
    public boolean supportsStreaming() {
        return true;
    }

    @Override
    public void serialize(ExecutionResult result, OutputStream out) throws IOException {
        try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            writeHeader(writer, result.columns());
            for (Row row : result.rows()) {
                writeRow(writer, row);
            }
        }
    }

    private void writeHeader(Writer writer, List<Column> columns) throws IOException {
        String header = columns.stream()
                .map(col -> escapeField(col.name()))
                .collect(Collectors.joining(String.valueOf(DELIMITER)));
        writer.write(header);
        writer.write(LINE_ENDING);
    }

    private void writeRow(Writer writer, Row row) throws IOException {
        List<Object> values = row.values();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                writer.write(DELIMITER);
            }
            writer.write(formatValue(values.get(i)));
        }
        writer.write(LINE_ENDING);
    }

    private String formatValue(Object value) {
        if (value == null) {
            return "";
        }
        return escapeField(value.toString());
    }

    private String escapeField(String value) {
        if (value == null) {
            return "";
        }

        // Check if escaping is needed
        boolean needsQuoting = value.indexOf(DELIMITER) >= 0
                || value.indexOf(QUOTE) >= 0
                || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0;

        if (!needsQuoting) {
            return value;
        }

        // Escape quotes by doubling them and wrap in quotes
        StringBuilder sb = new StringBuilder(value.length() + 2);
        sb.append(QUOTE);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == QUOTE) {
                sb.append(QUOTE); // Double the quote
            }
            sb.append(c);
        }
        sb.append(QUOTE);
        return sb.toString();
    }
}
