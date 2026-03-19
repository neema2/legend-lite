package com.gs.legend.ast;

import java.util.List;

/**
 * AST node for inline TDS (Tabular Data Set) literals.
 * 
 * Syntax:
 * {@code
 * #TDS
 *   col1:Type1, col2, col3:Type3
 *   val1, val2, val3
 *   val4, val5, val6
 * #
 * }
 *
 * @param columns List of column definitions with names and optional types
 * @param rows    List of data rows, each row is a list of values
 */
public record TdsLiteral(
                List<TdsColumn> columns,
                List<List<Object>> rows) {

        /**
         * Parses a raw TDS literal string into a structured TdsLiteral.
         * Input format: "#TDS\ncol1:Type1, col2\nval1, val2\n#"
         */
        public static TdsLiteral parse(String raw) {
                String content = raw;
                if (content.startsWith("#TDS"))
                        content = content.substring(4);
                if (content.endsWith("#"))
                        content = content.substring(0, content.length() - 1);
                content = content.trim();

                String[] lines = content.split("\n");
                if (lines.length == 0) {
                        return new TdsLiteral(List.of(), List.of());
                }

                // Parse header
                List<TdsColumn> columns = new java.util.ArrayList<>();
                for (String cell : splitCsvLine(lines[0])) {
                        cell = cell.trim();
                        int colonIdx = cell.indexOf(':');
                        if (colonIdx > 0) {
                                columns.add(TdsColumn.of(cell.substring(0, colonIdx).trim(),
                                                cell.substring(colonIdx + 1).trim()));
                        } else {
                                columns.add(TdsColumn.of(cell));
                        }
                }

                // Parse data rows
                List<List<Object>> rows = new java.util.ArrayList<>();
                for (int i = 1; i < lines.length; i++) {
                        String line = lines[i].trim();
                        if (line.isEmpty())
                                continue;
                        List<String> cells = splitCsvLine(line);
                        List<Object> row = new java.util.ArrayList<>();
                        for (String v : cells) {
                                row.add(parseValue(v.trim()));
                        }
                        while (row.size() < columns.size()) {
                                row.add(null);
                        }
                        rows.add(row);
                }

                return new TdsLiteral(columns, rows);
        }

        /** Quote-aware CSV line split: commas inside quotes are not delimiters. */
        private static List<String> splitCsvLine(String line) {
                List<String> result = new java.util.ArrayList<>();
                StringBuilder current = new StringBuilder();
                boolean inQuotes = false;
                for (int i = 0; i < line.length(); i++) {
                        char c = line.charAt(i);
                        if (c == '"') {
                                inQuotes = !inQuotes;
                                current.append(c);
                        } else if (c == ',' && !inQuotes) {
                                result.add(current.toString().trim());
                                current = new StringBuilder();
                        } else {
                                current.append(c);
                        }
                }
                result.add(current.toString().trim());
                return result;
        }

        /** Parse a cell value string to an appropriate typed Object. */
        private static Object parseValue(String value) {
                if (value.isEmpty() || "null".equalsIgnoreCase(value))
                        return null;
                if ("true".equalsIgnoreCase(value))
                        return Boolean.TRUE;
                if ("false".equalsIgnoreCase(value))
                        return Boolean.FALSE;
                // Decimal suffix (e.g., 21d, 41.0D)
                if ((value.endsWith("d") || value.endsWith("D")) && value.length() > 1) {
                        try {
                                return Double.parseDouble(value.substring(0, value.length() - 1));
                        } catch (NumberFormatException e) {
                                /* fall through */ }
                }
                try {
                        return Long.parseLong(value);
                } catch (NumberFormatException e) {
                        /* not int */ }
                try {
                        return Double.parseDouble(value);
                } catch (NumberFormatException e) {
                        /* not num */ }
                // Strip surrounding quotes
                if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
                        // Standard CSV: doubled quotes "" inside are unescaped to "
                        return value.substring(1, value.length() - 1).replace("\"\"", "\"");
                }
                if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
                        return value.substring(1, value.length() - 1);
                }
                return value;
        }

        /**
         * Column definition with name and optional type.
         * If type is null, it's inferred from the data.
         */
        public record TdsColumn(String name, String type) {
                public static TdsColumn of(String name) {
                        return new TdsColumn(name, null);
                }

                public static TdsColumn of(String name, String type) {
                        return new TdsColumn(name, type);
                }

                public boolean isVariant() {
                        return type != null &&
                                        (type.contains("Variant") || type.equalsIgnoreCase("Variant"));
                }
        }

        /**
         * Returns just the column names for backward compatibility.
         */
        public List<String> columnNames() {
                return columns.stream().map(TdsColumn::name).toList();
        }
}
