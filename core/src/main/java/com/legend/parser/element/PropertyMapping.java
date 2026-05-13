package com.legend.parser.element;

import com.legend.parser.TypeExpression;

import com.legend.parser.Multiplicity;

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
 * <h2>B.4g additions</h2>
 * <ul>
 *   <li>{@link Embedded} &mdash; nested complex property mapped via inline
 *       sub-properties (no JOIN, columns read from the parent's main table).</li>
 *   <li>{@link InlineEmbedded} &mdash; nested complex property whose body is
 *       a reference to another class mapping by set id.</li>
 *   <li>{@link OtherwiseEmbedded} &mdash; embedded body with a fallback set id
 *       reachable via a join when the embedded data is absent.</li>
 *   <li>{@link LocalProperty} &mdash; a property declared only in the mapping
 *       (not the class), prefixed with {@code +}, carrying its own type and
 *       multiplicity. The binding body is any of the other variants.</li>
 * </ul>
 */
public sealed interface PropertyMapping
        permits PropertyMapping.Column,
                PropertyMapping.EnumeratedColumn,
                PropertyMapping.Join,
                PropertyMapping.JoinTerminalColumn,
                PropertyMapping.Expression,
                PropertyMapping.Embedded,
                PropertyMapping.InlineEmbedded,
                PropertyMapping.OtherwiseEmbedded,
                PropertyMapping.LocalProperty {

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

    /**
     * Embedded sub-mapping: {@code propName ( subProp1: ..., subProp2: ... )}.
     *
     * <p>The property is a complex type whose fields are mapped inline
     * from columns of the parent's main table &mdash; no join is needed.
     * Sub-mappings are themselves any of the {@link PropertyMapping}
     * variants, evaluated against the same scope.
     *
     * @param propertyName       Pure property name (the complex-typed field)
     * @param propertyMappings   per-sub-property bindings
     */
    record Embedded(String propertyName, List<PropertyMapping> propertyMappings)
            implements PropertyMapping {
        public Embedded {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(propertyMappings, "Embedded property mappings cannot be null");
            propertyMappings = List.copyOf(propertyMappings);
        }
    }

    /**
     * Inline embedded reference: {@code propName() Inline[setId]}.
     *
     * <p>The property is mapped by reference to another class mapping
     * declared elsewhere in the same {@link MappingDefinition} (or an
     * included one), identified by its set id. No body is parsed; the
     * empty parens are required by the grammar.
     *
     * @param propertyName  Pure property name
     * @param setId         id of the referenced class mapping
     */
    record InlineEmbedded(String propertyName, String setId)
            implements PropertyMapping {
        public InlineEmbedded {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(setId, "Inline set id cannot be null");
        }
    }

    /**
     * Otherwise-embedded: {@code propName ( subs ) Otherwise ([fallbackSetId]: body)}.
     *
     * <p>Try the embedded sub-mappings first; if the embedded data is
     * absent (e.g. denormalized columns are NULL), fall back to another
     * class mapping reached via the fallback body (typically a join).
     *
     * @param propertyName     Pure property name
     * @param embedded         the primary embedded sub-mappings
     * @param fallbackSetId    id of the fallback class mapping
     * @param fallback         the fallback binding body (typically a
     *                         {@link Join}); evaluated to reach the
     *                         fallback set when the embedded path yields
     *                         no data
     */
    record OtherwiseEmbedded(String propertyName,
                             List<PropertyMapping> embedded,
                             String fallbackSetId,
                             PropertyMapping fallback) implements PropertyMapping {
        public OtherwiseEmbedded {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(embedded, "Embedded sub-mappings cannot be null");
            Objects.requireNonNull(fallbackSetId, "Fallback set id cannot be null");
            Objects.requireNonNull(fallback, "Fallback body cannot be null");
            embedded = List.copyOf(embedded);
        }
    }

    /**
     * Local mapping property: {@code +propName: Type[mult]: body}.
     *
     * <p>A property defined only in the mapping (not the class), used to
     * project additional values that aren't part of the class model.
     * The leading {@code +} is the syntactic marker. The binding body is
     * any of the other {@link PropertyMapping} variants.
     *
     * <p>Multiplicity is stored flat as {@code (lowerBound, upperBound)},
     * with {@code upperBound == null} meaning unbounded ({@code *}) &mdash;
     * same shape as {@code ClassDefinition.PropertyDefinition}.
     *
     * @param propertyName  local property name (without the leading {@code +})
     * @param type          declared property type
     * @param lowerBound    lower multiplicity bound
     * @param upperBound    upper multiplicity bound; {@code null} = unbounded
     * @param body          binding body (Column / Expression / Join / ...)
     */
    record LocalProperty(String propertyName,
                         TypeExpression type,
                         Multiplicity multiplicity,
                         PropertyMapping body) implements PropertyMapping {
        public LocalProperty {
            Objects.requireNonNull(propertyName, "Property name cannot be null");
            Objects.requireNonNull(type, "Local property type cannot be null");
            Objects.requireNonNull(multiplicity, "Local property multiplicity cannot be null");
            Objects.requireNonNull(body, "Local property body cannot be null");
        }
    }
}
