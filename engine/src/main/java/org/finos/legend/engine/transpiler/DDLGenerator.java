package org.finos.legend.engine.transpiler;

import org.finos.legend.pure.dsl.definition.ClassDefinition;

import java.util.stream.Collectors;

/**
 * Generates CREATE TABLE DDL from Pure ClassDefinition.
 * 
 * Convention:
 * - Table: T_ + UPPER_SNAKE_CASE(className)
 * - Column: UPPER_SNAKE_CASE(propertyName) with SQL type
 */
public class DDLGenerator {

    /**
     * Generates CREATE TABLE DDL for a class.
     */
    public static String generateCreateTable(ClassDefinition classDef) {
        String tableName = toTableName(classDef.simpleName());

        String columns = classDef.properties().stream()
                .map(prop -> "    \"" + toColumnName(prop.name()) + "\" " + mapType(prop.type()) + nullability(prop))
                .collect(Collectors.joining(",\n"));

        return "CREATE TABLE IF NOT EXISTS \"" + tableName + "\" (\n" + columns + "\n)";
    }

    /**
     * Generates table name from class name.
     * Person -> T_PERSON
     */
    public static String toTableName(String className) {
        return "T_" + toUpperSnakeCase(className);
    }

    /**
     * Generates column name from property name.
     * firstName -> FIRST_NAME
     */
    public static String toColumnName(String propertyName) {
        return toUpperSnakeCase(propertyName);
    }

    private static String toUpperSnakeCase(String camelCase) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                result.append('_');
            }
            result.append(Character.toUpperCase(c));
        }
        return result.toString();
    }

    private static String mapType(String pureType) {
        return switch (pureType) {
            case "String" -> "VARCHAR";
            case "Integer" -> "INTEGER";
            case "Float", "Number", "Decimal" -> "DOUBLE";
            case "Boolean" -> "BOOLEAN";
            case "Date", "StrictDate" -> "DATE";
            case "DateTime" -> "TIMESTAMP";
            default -> "VARCHAR";
        };
    }

    private static String nullability(ClassDefinition.PropertyDefinition prop) {
        return prop.lowerBound() >= 1 ? " NOT NULL" : "";
    }
}
