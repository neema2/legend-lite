package com.gs.legend.model.def;

import java.util.List;
import java.util.Objects;

/**
 * Sealed interface representing the different kinds of property mapping values
 * in a relational class mapping.
 * 
 * Replaces the current union-of-nulls pattern in {@link MappingDefinition.PropertyMappingDefinition}
 * where {@code columnReference | joinReference | expressionString} are all nullable fields.
 * 
 * Examples:
 * <pre>
 * // Simple column:     name : [DB] T_PERSON.FIRST_NAME
 * ColumnMapping(ColumnRef("DB", "T_PERSON", "FIRST_NAME"))
 * 
 * // Join reference:    firm : [DB]@PersonFirm
 * JoinMapping("DB", [JoinChainElement("PersonFirm")], null)
 * 
 * // Multi-hop join:    country : [DB]@PersonFirm > @FirmCountry
 * JoinMapping("DB", [JoinChainElement("PersonFirm"), JoinChainElement("FirmCountry")], null)
 * 
 * // DynaFunction:      fullName : concat([DB] T.FIRST, ' ', [DB] T.LAST)
 * ExpressionMapping(FunctionCall("concat", [...]))
 * 
 * // Embedded:          address ( street : T.STREET, city : T.CITY )
 * EmbeddedMapping(null, [PropertyMappingDefinition("street",...), ...])
 * 
 * // Inline:            address () Inline[addressSet]
 * InlineMapping("addressSet")
 * 
 * // Otherwise:         address (...) Otherwise([fallbackSet] : [DB]@AddressJoin)
 * OtherwiseMapping(EmbeddedMapping(...), "fallbackSet", JoinMapping(...))
 * </pre>
 */
public sealed interface PropertyMappingValue {

    /**
     * A simple column reference: {@code [DB] TABLE.COLUMN}.
     * 
     * @param column The column reference expression (a {@link RelationalOperation.ColumnRef})
     */
    record ColumnMapping(RelationalOperation column) implements PropertyMappingValue {
        public ColumnMapping {
            Objects.requireNonNull(column, "Column cannot be null");
        }
    }

    /**
     * A join-based property mapping: {@code [DB]@Join1 > (INNER) @Join2 | terminal}.
     * 
     * The terminal is the column/expression at the end of the join chain (after the pipe).
     * For bare association references like {@code firm : [DB]@PersonFirm}, terminal is null.
     * 
     * @param databaseName Optional database qualifier
     * @param joinChain    The ordered join chain elements
     * @param terminal     Optional terminal expression after the pipe (null for bare join refs)
     */
    record JoinMapping(
            String databaseName,
            List<JoinChainElement> joinChain,
            PropertyMappingValue terminal
    ) implements PropertyMappingValue {
        public JoinMapping {
            Objects.requireNonNull(joinChain, "Join chain cannot be null");
            if (joinChain.isEmpty()) throw new IllegalArgumentException("Join chain cannot be empty");
            joinChain = List.copyOf(joinChain);
        }

        /** Convenience for single-hop join with no terminal. */
        public static JoinMapping single(String databaseName, String joinName) {
            return new JoinMapping(databaseName, List.of(JoinChainElement.of(joinName)), null);
        }
    }

    /**
     * An expression-based mapping using a {@link RelationalOperation} tree.
     * Covers DynaFunctions like {@code concat(T.A, ' ', T.B)}, arithmetic, etc.
     * 
     * @param expression The expression tree
     */
    record ExpressionMapping(RelationalOperation expression) implements PropertyMappingValue {
        public ExpressionMapping {
            Objects.requireNonNull(expression, "Expression cannot be null");
        }
    }

    /**
     * An embedded mapping — inline property-to-column mappings for a nested class.
     * 
     * Pure syntax: {@code address ( street : [DB] T.STREET, city : [DB] T.CITY )}
     * 
     * @param primaryKey Optional primary key columns for deduplication (null if not specified)
     * @param properties The nested property mappings
     */
    record EmbeddedMapping(
            List<RelationalOperation> primaryKey,
            List<MappingDefinition.PropertyMappingDefinition> properties
    ) implements PropertyMappingValue {
        public EmbeddedMapping {
            primaryKey = primaryKey != null ? List.copyOf(primaryKey) : List.of();
            Objects.requireNonNull(properties, "Properties cannot be null");
            properties = List.copyOf(properties);
        }
    }

    /**
     * An inline mapping reference: {@code () Inline[targetSetId]}.
     * 
     * @param targetSetId The set ID of the referenced class mapping
     */
    record InlineMapping(String targetSetId) implements PropertyMappingValue {
        public InlineMapping {
            Objects.requireNonNull(targetSetId, "Target set ID cannot be null");
        }
    }

    /**
     * An otherwise mapping — combines an embedded mapping with a join fallback.
     * 
     * Pure syntax: {@code address (...) Otherwise([fallbackSetId] : [DB]@JoinName)}
     * 
     * @param embedded      The primary embedded mapping
     * @param fallbackSetId The set ID for the fallback mapping
     * @param fallbackJoin  The join-based fallback
     */
    record OtherwiseMapping(
            EmbeddedMapping embedded,
            String fallbackSetId,
            JoinMapping fallbackJoin
    ) implements PropertyMappingValue {
        public OtherwiseMapping {
            Objects.requireNonNull(embedded, "Embedded mapping cannot be null");
            Objects.requireNonNull(fallbackSetId, "Fallback set ID cannot be null");
            Objects.requireNonNull(fallbackJoin, "Fallback join cannot be null");
        }
    }
}
