package org.finos.legend.engine.plan;

import org.finos.legend.pure.dsl.PureCompileException;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tracks available columns and their types through a relation compilation
 * pipeline.
 *
 * <p>
 * Immutable — every mutation returns a new {@code RelationType}.
 * Column order is preserved (insertion order via {@link LinkedHashMap}).
 *
 * <p>
 * This is the compiler's equivalent of legend-engine's
 * {@code RelationType<(col1:Type1, col2:Type2, ...)>} expressed in Pure's type
 * system.
 *
 * <h3>Usage</h3>
 * 
 * <pre>
 * var rt = RelationType.of(Map.of("val", INTEGER, "str", STRING));
 * rt = rt.withColumn("sum", INTEGER); // extend
 * rt = rt.onlyColumns(List.of("val", "sum")); // project
 * rt.assertHasColumn("val"); // validation
 * </pre>
 *
 * @param columns Ordered map of column name → {@link GenericType}
 */
public record RelationType(Map<String, GenericType> columns) {

    /**
     * Canonical constructor — defensive copy to LinkedHashMap for order
     * preservation.
     */
    public RelationType {
        columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
    }

    // ========== Column Operations ==========

    /**
     * Returns a new RelationType with an additional column.
     * If the column already exists, its type is replaced.
     */
    public RelationType withColumn(String name, GenericType type) {
        var newCols = new LinkedHashMap<>(columns);
        newCols.put(name, type);
        return new RelationType(newCols);
    }

    /**
     * Returns a new RelationType with multiple additional columns.
     */
    public RelationType withColumns(Map<String, GenericType> additionalColumns) {
        var newCols = new LinkedHashMap<>(columns);
        newCols.putAll(additionalColumns);
        return new RelationType(newCols);
    }

    /**
     * Returns a new RelationType containing only the specified columns, in the
     * given order.
     * Used for {@code project} and {@code select}.
     *
     * @throws PureCompileException if any column does not exist
     */
    public RelationType onlyColumns(List<String> names) {
        var newCols = new LinkedHashMap<String, GenericType>();
        for (String name : names) {
            GenericType type = columns.get(name);
            if (type == null) {
                throw new PureCompileException(
                        "Column '" + name + "' does not exist. Available columns: " + columns.keySet());
            }
            newCols.put(name, type);
        }
        return new RelationType(newCols);
    }

    /**
     * Returns a new RelationType with the specified columns removed.
     */
    public RelationType withoutColumns(Set<String> names) {
        var newCols = new LinkedHashMap<>(columns);
        names.forEach(newCols::remove);
        return new RelationType(newCols);
    }

    /**
     * Returns a new RelationType with a column renamed.
     *
     * @throws PureCompileException if the old column does not exist
     */
    public RelationType renameColumn(String oldName, String newName) {
        GenericType type = columns.get(oldName);
        if (type == null) {
            throw new PureCompileException(
                    "Cannot rename: column '" + oldName + "' does not exist. Available: " + columns.keySet());
        }
        // Preserve order: rebuild with rename
        var newCols = new LinkedHashMap<String, GenericType>();
        for (var entry : columns.entrySet()) {
            if (entry.getKey().equals(oldName)) {
                newCols.put(newName, entry.getValue());
            } else {
                newCols.put(entry.getKey(), entry.getValue());
            }
        }
        return new RelationType(newCols);
    }

    /**
     * Merges two RelationTypes (for join, concatenate).
     * If columns collide, the other's type wins.
     */
    public RelationType merge(RelationType other) {
        var newCols = new LinkedHashMap<>(columns);
        newCols.putAll(other.columns());
        return new RelationType(newCols);
    }

    // ========== Validation ==========

    /**
     * Asserts that a column exists in this RelationType.
     *
     * @throws PureCompileException if the column does not exist
     */
    public void assertHasColumn(String name) {
        if (!columns.containsKey(name)) {
            throw new PureCompileException(
                    "Column '" + name + "' does not exist. Available columns: " + columns.keySet());
        }
    }

    /**
     * Asserts that all specified columns exist.
     *
     * @throws PureCompileException listing all missing columns
     */
    public void assertHasColumns(List<String> names) {
        var missing = names.stream()
                .filter(n -> !columns.containsKey(n))
                .toList();
        if (!missing.isEmpty()) {
            throw new PureCompileException(
                    "Columns " + missing + " do not exist. Available columns: " + columns.keySet());
        }
    }

    /**
     * Returns the type of a column, or null if not found.
     */
    public GenericType getColumnType(String name) {
        return columns.get(name);
    }

    /**
     * Returns the type of a column, throwing if not found.
     *
     * @throws PureCompileException if the column does not exist
     */
    public GenericType requireColumnType(String name) {
        GenericType type = columns.get(name);
        if (type == null) {
            throw new PureCompileException(
                    "Column '" + name + "' does not exist. Available columns: " + columns.keySet());
        }
        return type;
    }

    /**
     * Alias for {@link #requireColumnType(String)}.
     * Returns the type of a column, throwing if not found.
     */
    public GenericType requireColumn(String name) {
        return requireColumnType(name);
    }

    public boolean hasColumn(String name) {
        return columns.containsKey(name);
    }

    public int size() {
        return columns.size();
    }

    public List<String> columnNames() {
        return List.copyOf(columns.keySet());
    }

    // ========== Factory Methods ==========

    /**
     * Creates an empty RelationType (no columns).
     */
    public static RelationType empty() {
        return new RelationType(Map.of());
    }

    /**
     * Creates a RelationType from a column name→type map.
     */
    public static RelationType of(Map<String, GenericType> columns) {
        return new RelationType(columns);
    }

    /**
     * Creates a RelationType from parallel lists of names and types.
     */
    public static RelationType of(List<String> names, List<GenericType> types) {
        if (names.size() != types.size()) {
            throw new IllegalArgumentException("Column names and types must have the same size");
        }
        var cols = new LinkedHashMap<String, GenericType>();
        for (int i = 0; i < names.size(); i++) {
            cols.put(names.get(i), types.get(i));
        }
        return new RelationType(cols);
    }

    /**
     * Creates a RelationType with a single column.
     */
    public static RelationType ofSingle(String name, GenericType type) {
        return new RelationType(Map.of(name, type));
    }

    @Override
    public String toString() {
        var sb = new StringBuilder("Relation<(");
        boolean first = true;
        for (var entry : columns.entrySet()) {
            if (!first)
                sb.append(", ");
            sb.append(entry.getKey()).append(":").append(entry.getValue().typeName());
            first = false;
        }
        sb.append(")>");
        return sb.toString();
    }
}
