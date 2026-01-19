package org.finos.legend.engine.plan;

import java.util.List;

/**
 * Describes the schema of query results.
 * 
 * Contains column metadata including name, Pure type, and multiplicity.
 * This is embedded in ExecutionPlan so clients know what to expect.
 */
public record ResultSchema(List<Column> columns) {

    /**
     * A column in the result schema.
     * 
     * @param name       The column name (matches property name from projection)
     * @param pureType   The Pure type (String, Integer, Float, Boolean, Date, etc.)
     * @param lowerBound Multiplicity lower bound (0 or 1)
     * @param upperBound Multiplicity upper bound (1 or null for *)
     */
    public record Column(
            String name,
            String pureType,
            int lowerBound,
            Integer upperBound) {
        public boolean isRequired() {
            return lowerBound >= 1;
        }

        public boolean isMultiple() {
            return upperBound == null || upperBound > 1;
        }

        public String javaType() {
            return switch (pureType) {
                case "String" -> "String";
                case "Integer" -> isRequired() ? "int" : "Integer";
                case "Float" -> isRequired() ? "double" : "Double";
                case "Boolean" -> isRequired() ? "boolean" : "Boolean";
                case "Date", "StrictDate" -> "java.time.LocalDate";
                case "DateTime" -> "java.time.LocalDateTime";
                default -> "Object";
            };
        }
    }

    public int columnCount() {
        return columns.size();
    }

    public Column getColumn(String name) {
        return columns.stream()
                .filter(c -> c.name().equals(name))
                .findFirst()
                .orElse(null);
    }

    public int getColumnIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
