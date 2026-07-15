package com.legend.model;

import java.util.List;
import java.util.Objects;

/**
 * A parsed enumeration mapping declared inside a {@link MappingDefinition}
 * body. An enumeration mapping translates source values (typically database
 * codes &mdash; strings or integers, or values from another enum) to the
 * canonical enum values used by the logical model.
 *
 * <p>Pure surface syntax:
 * <pre>
 *   model::OrderStatus: EnumerationMapping StatusMapping
 *   {
 *     PENDING:   ['P', 'PEND'],
 *     SHIPPED:   'S',
 *     DELIVERED: ['D', 'DLV'],
 *     CANCELLED: 'C'
 *   }
 * </pre>
 *
 * <p>The {@code mappingId} (the identifier following the
 * {@code EnumerationMapping} keyword) is <strong>optional</strong>; when
 * absent, engine generates a synthetic ID. Class-mapping property
 * mappings refer to enumeration mappings by this ID, e.g.
 * {@code status: EnumerationMapping StatusMapping : ORDERS.STATUS_CODE}.
 *
 * @param enumName       fully-qualified path of the enum being mapped
 * @param mappingId      identifier following {@code EnumerationMapping},
 *                       or {@code null} when omitted in source
 * @param valueMappings  per-enum-value source value bindings; never
 *                       {@code null}, may be empty
 */
public record EnumerationMapping(
        String enumName,
        String mappingId,
        List<EnumValueMapping> valueMappings) {

    public EnumerationMapping {
        Objects.requireNonNull(enumName, "Enum name cannot be null");
        valueMappings = valueMappings == null
                ? List.of()
                : List.copyOf(valueMappings);
    }

    /**
     * One row of the enum mapping: a domain enum value paired with one or
     * more source values that translate to it.
     *
     * @param enumValue     domain enum value name (e.g. {@code "PENDING"})
     * @param sourceValues  source values that resolve to {@code enumValue};
     *                      at least one
     */
    public record EnumValueMapping(String enumValue, List<SourceValue> sourceValues) {
        public EnumValueMapping {
            Objects.requireNonNull(enumValue, "Enum value cannot be null");
            Objects.requireNonNull(sourceValues, "Source values cannot be null");
            if (sourceValues.isEmpty()) {
                throw new IllegalArgumentException(
                        "EnumValueMapping requires at least one source value");
            }
            sourceValues = List.copyOf(sourceValues);
        }
    }

    /**
     * A source value bound to a domain enum value. Sealed because there
     * are exactly three forms in the Pure surface grammar: string literal,
     * integer literal, and a path into another enum (cross-enum mapping).
     */
    public sealed interface SourceValue
            permits SourceValue.StringValue,
                    SourceValue.IntegerValue,
                    SourceValue.EnumRef {

        /** String literal source value: {@code 'PENDING'}. */
        record StringValue(String value) implements SourceValue {
            public StringValue {
                Objects.requireNonNull(value, "String source value cannot be null");
            }
        }

        /** Integer literal source value: {@code 1}, {@code 42}. */
        record IntegerValue(long value) implements SourceValue {}

        /**
         * Cross-enum reference: {@code other::Enum.SOME_VALUE}. The enum
         * path and value name are kept as strings &mdash; resolution to a
         * concrete enum is Phase D's job.
         *
         * @param enumPath        fully-qualified path of the referenced enum
         * @param enumValueName   value name on that enum
         */
        record EnumRef(String enumPath, String enumValueName) implements SourceValue {
            public EnumRef {
                Objects.requireNonNull(enumPath, "Enum path cannot be null");
                Objects.requireNonNull(enumValueName, "Enum value name cannot be null");
            }
        }
    }
}
