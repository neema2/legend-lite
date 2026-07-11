package com.legend.parser.element;

import java.util.Objects;

/**
 * The closed set of canonical relational data types, mirroring engine's
 * {@code meta::relational::metamodel::datatype::CoreDataType} hierarchy
 * (see {@code legend-pure-store-relational/.../relational.pure}). The
 * Java-side name drops engine's {@code Core} prefix because we don't
 * model the sibling {@code DbSpecificDataType} variant (see below).
 *
 * <p>Engine exposes a sibling {@code DbSpecificDataType} (canonical core
 * + raw dialect SQL) for vendor extensions. Pure source has no spelling
 * for that variant &mdash; it's only constructed programmatically by
 * engine. We omit it here until a use case appears.
 *
 * <p>Three field-shape buckets:
 * <ul>
 *   <li><b>nullary</b> &mdash; {@link Bool}, {@link BigInt}, {@link SmallInt},
 *       {@link TinyInt}, {@link Integer_}, {@link Float_}, {@link Double_},
 *       {@link Real}, {@link Bit}, {@link Timestamp}, {@link Date_},
 *       {@link Distinct}, {@link Other}, {@link SemiStructured}.</li>
 *   <li><b>sized</b> &mdash; {@link Varchar}, {@link Char_}, {@link Binary},
 *       {@link Varbinary} each carry a single {@code size} int.</li>
 *   <li><b>precision/scale</b> &mdash; {@link Decimal} and {@link Numeric}
 *       carry {@code precision} and {@code scale}.</li>
 *   <li><b>recursive</b> &mdash; {@link Array} wraps an element {@link RelationalDataType};
 *       {@link Object_} carries key and value {@link RelationalDataType}s.</li>
 * </ul>
 *
 * <p>Variant names that would clash with {@code java.lang} types or Java
 * keywords carry a trailing underscore ({@code Integer_}, {@code Float_},
 * {@code Double_}, {@code Char_}, {@code Date_}, {@code Object_}) and the
 * boolean variant is spelled {@link Bool}. Engine uses the bare names &mdash;
 * the underscore convention is purely a Java-side hygiene measure.
 *
 * <p>{@link #fromName(String)} maps the SQL source identifier (case-insensitive)
 * to a nullary {@link RelationalDataType}. Sized / parameterised variants must be
 * constructed directly via their constructors.
 */
public sealed interface RelationalDataType permits
        RelationalDataType.Bool,
        RelationalDataType.BigInt,
        RelationalDataType.SmallInt,
        RelationalDataType.TinyInt,
        RelationalDataType.Integer_,
        RelationalDataType.Float_,
        RelationalDataType.Double_,
        RelationalDataType.Real,
        RelationalDataType.Bit,
        RelationalDataType.Varchar,
        RelationalDataType.Char_,
        RelationalDataType.Binary,
        RelationalDataType.Varbinary,
        RelationalDataType.Decimal,
        RelationalDataType.Numeric,
        RelationalDataType.Timestamp,
        RelationalDataType.Date_,
        RelationalDataType.Distinct,
        RelationalDataType.Other,
        RelationalDataType.SemiStructured,
        RelationalDataType.Array,
        RelationalDataType.Object_ {

    // ---- Nullary core types ----
    record Bool()           implements RelationalDataType { }
    record BigInt()         implements RelationalDataType { }
    record SmallInt()       implements RelationalDataType { }
    record TinyInt()        implements RelationalDataType { }
    record Integer_()       implements RelationalDataType { }
    record Float_()         implements RelationalDataType { }
    record Double_()        implements RelationalDataType { }
    record Real()           implements RelationalDataType { }
    record Bit()            implements RelationalDataType { }
    record Timestamp()      implements RelationalDataType { }
    record Date_()          implements RelationalDataType { }
    record Distinct()       implements RelationalDataType { }
    record Other()          implements RelationalDataType { }
    record SemiStructured() implements RelationalDataType { }

    // ---- Sized core types ----
    record Varchar(int size)   implements RelationalDataType { }
    record Char_(int size)     implements RelationalDataType { }
    record Binary(int size)    implements RelationalDataType { }
    record Varbinary(int size) implements RelationalDataType { }

    // ---- Precision / scale core types ----
    record Decimal(int precision, int scale) implements RelationalDataType { }
    record Numeric(int precision, int scale) implements RelationalDataType { }

    // ---- Recursive core types ----

    /**
     * Engine's {@code Array} carries an optional element type (multiplicity
     * {@code [0..1]}). We model that as a nullable {@code element} field.
     */
    record Array(RelationalDataType element) implements RelationalDataType { }

    /** Engine's {@code Object} keyed semi-structure carrier. */
    record Object_(RelationalDataType keyType, RelationalDataType valueType) implements RelationalDataType {
        public Object_ {
            Objects.requireNonNull(keyType,   "Object keyType cannot be null");
            Objects.requireNonNull(valueType, "Object valueType cannot be null");
        }
    }

    /**
     * Map a SQL source identifier (case-insensitive) to its nullary
     * canonical {@link RelationalDataType}, mirroring engine's
     * {@code ColumnDataTypeFactory} mapping plus a few obvious extras
     * (BOOLEAN, BIT, REAL, SEMISTRUCTURED).
     *
     * <p>Source synonyms canonicalise:
     * <ul>
     *   <li>{@code INT}, {@code INTEGER} &rarr; {@link Integer_}</li>
     * </ul>
     *
     * @throws IllegalArgumentException if {@code name} is sized/parameterised
     *         (e.g. {@code VARCHAR}, {@code DECIMAL}) &mdash; those must be
     *         constructed via their record constructors after parsing the
     *         parens
     * @throws IllegalArgumentException if {@code name} is not a known type
     */
    static RelationalDataType fromName(String name) {
        return switch (name.toUpperCase()) {
            case "BOOLEAN"        -> new Bool();
            case "BIGINT"         -> new BigInt();
            case "SMALLINT"       -> new SmallInt();
            case "TINYINT"        -> new TinyInt();
            case "INT", "INTEGER" -> new Integer_();
            case "FLOAT"          -> new Float_();
            case "DOUBLE"         -> new Double_();
            case "REAL"           -> new Real();
            case "BIT"            -> new Bit();
            case "TIMESTAMP"      -> new Timestamp();
            case "DATE"           -> new Date_();
            case "DISTINCT"       -> new Distinct();
            case "OTHER"          -> new Other();
            // JSON is the same semi-structured carrier (engine DDL accepts both).
            case "SEMISTRUCTURED", "JSON" -> new SemiStructured();
            // A BARE VARCHAR is unbounded (DuckDB and the engine DDL both
            // accept it); the size only ever feeds the String Pure type.
            case "VARCHAR"        -> new Varchar(Integer.MAX_VALUE);
            case "CHAR", "BINARY", "VARBINARY",
                 "DECIMAL", "NUMERIC", "ARRAY", "OBJECT"
                -> throw new IllegalArgumentException(
                        "type '" + name + "' requires arguments; use its record constructor");
            default -> throw new IllegalArgumentException(
                    "unknown SQL data type: '" + name + "'");
        };
    }
}
