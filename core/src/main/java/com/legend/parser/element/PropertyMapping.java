package com.legend.parser.element;

import java.util.List;
import java.util.Objects;

/**
 * One property-to-source binding inside a {@link ClassMapping}.
 *
 * <p>Sealed because the five grammar forms have structurally different
 * downstream behavior:
 * <ul>
 *   <li>{@link Column} &mdash; read a value directly from a table column.</li>
 *   <li>{@link EnumeratedColumn} &mdash; read a column and translate the
 *       raw value through a named {@code EnumerationMapping}.</li>
 *   <li>{@link Join} &mdash; navigate an association via a join chain
 *       (the result is an instance of the target class).</li>
 *   <li>{@link JoinTerminalColumn} &mdash; navigate a join chain and then
 *       read a column of the joined table (the result is a primitive
 *       value, not an instance).</li>
 *   <li>{@link Expression} &mdash; compute the value from a structured
 *       relational expression tree (e.g. {@code concat(T.A, ' ', T.B)}).</li>
 * </ul>
 *
 * <p>Each variant captures a genuinely different shape of source data,
 * not just a different field population. Consumers in Phase D (resolution)
 * and downstream (lowering, SQL gen) dispatch on the variant via pattern
 * match.
 *
 * <h2>Deferred to later slices</h2>
 * <ul>
 *   <li><strong>Embedded sub-mappings</strong> ({@code prop ( subProp: ... )}) &mdash;
 *       requires recursive {@code PropertyMapping} composition; deferred.</li>
 *   <li><strong>Inline references</strong> ({@code prop: ClassName[setId]}) &mdash;
 *       reference to another class mapping; deferred.</li>
 *   <li><strong>Otherwise</strong> ({@code prop ( ... ) Otherwise([fallback] join)}) &mdash;
 *       deferred.</li>
 *   <li><strong>Local mapping properties</strong> ({@code +prop: Type[mult]: ...}) &mdash;
 *       deferred.</li>
 * </ul>
 */
public sealed interface PropertyMapping
        permits PropertyMapping.Column,
                PropertyMapping.EnumeratedColumn,
                PropertyMapping.Join,
                PropertyMapping.JoinTerminalColumn,
                PropertyMapping.Expression {

    /** The Pure property name being bound. */
    String propertyName();

    /**
     * Plain column read: {@code propName: [DB] T.COL}.
     *
     * @param propertyName  Pure property name
     * @param database      database the column lives in (always populated:
     *                      either from the explicit {@code [DB]} bracket or
     *                      threaded from the enclosing class mapping's main
     *                      table)
     * @param table         table name
     * @param column        column name
     */
    record Column(String propertyName, String database, String table, String column)
            implements PropertyMapping {
        public Column {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(database, "Database cannot be null");
            Objects.requireNonNull(table, "Table cannot be null");
            Objects.requireNonNull(column, "Column cannot be null");
        }
    }

    /**
     * Column read transformed through a named enumeration mapping:
     * {@code propName: EnumerationMapping enumId : [DB] T.COL}.
     *
     * <p>Separate variant (not a nullable field on {@link Column}) because
     * the value flows through an extra transformation step downstream:
     * the raw column value is looked up in the enum mapping's value table
     * and translated to a typed enum constant. Different return type, different
     * resolution path.
     *
     * @param propertyName     Pure property name
     * @param enumMappingId    name of the {@code EnumerationMapping} declared
     *                         elsewhere in this {@link MappingDefinition} (or
     *                         an included one); resolution validates the
     *                         reference in Phase D
     * @param database         database the column lives in
     * @param table            table name
     * @param column           column name
     */
    record EnumeratedColumn(String propertyName, String enumMappingId,
                            String database, String table, String column)
            implements PropertyMapping {
        public EnumeratedColumn {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(enumMappingId, "Enumeration mapping id cannot be null");
            Objects.requireNonNull(database, "Database cannot be null");
            Objects.requireNonNull(table, "Table cannot be null");
            Objects.requireNonNull(column, "Column cannot be null");
        }
    }

    /**
     * Associative join navigation: {@code propName: [DB] @J1 > @J2 > ...}.
     * Produces an instance of the target class (or a collection thereof,
     * depending on the property multiplicity). No terminal column.
     *
     * @param propertyName  Pure property name
     * @param database      database the joins live in
     * @param joins         the join chain (always non-empty)
     */
    record Join(String propertyName, String database, List<JoinChainElement> joins)
            implements PropertyMapping {
        public Join {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(database, "Database cannot be null");
            Objects.requireNonNull(joins, "Joins cannot be null");
            if (joins.isEmpty()) {
                throw new IllegalArgumentException(
                        "Join property mapping must have at least one join hop");
            }
            joins = List.copyOf(joins);
        }
    }

    /**
     * Join navigation that terminates at a column read:
     * {@code propName: [DB] @J1 > @J2 | T.COL}. Produces the primitive value
     * of the named column on the join-target table &mdash; distinct from
     * {@link Join} (which returns an instance).
     *
     * @param propertyName     Pure property name
     * @param database         database the joins live in
     * @param joins            the join chain (always non-empty)
     * @param terminalColumn   the column read after the pipe; typically a
     *                         {@link RelationalOperation.ColumnRef} but any
     *                         relational expression is permitted
     */
    record JoinTerminalColumn(String propertyName, String database,
                              List<JoinChainElement> joins,
                              RelationalOperation terminalColumn)
            implements PropertyMapping {
        public JoinTerminalColumn {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(database, "Database cannot be null");
            Objects.requireNonNull(joins, "Joins cannot be null");
            Objects.requireNonNull(terminalColumn, "Terminal column cannot be null");
            if (joins.isEmpty()) {
                throw new IllegalArgumentException(
                        "Join-with-terminal property mapping must have at least one join hop");
            }
            joins = List.copyOf(joins);
        }
    }

    /**
     * Property derived from a structured relational expression:
     * {@code propName: concat(T.A, ' ', T.B)}.
     *
     * <p>Used for any property whose source isn't a single column or a
     * pure join navigation &mdash; computed values, conditionals,
     * function calls.
     *
     * @param propertyName  Pure property name
     * @param expression    the value expression
     */
    record Expression(String propertyName, RelationalOperation expression)
            implements PropertyMapping {
        public Expression {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(expression, "Expression cannot be null");
        }
    }
}
