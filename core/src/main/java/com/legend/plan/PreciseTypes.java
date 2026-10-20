// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.model.RelationalDataType;

/**
 * The engine's typing of a RELATION ACCESSOR's columns ({@code #>{db.T}#}):
 * each DDL type becomes a {@code meta::pure::precisePrimitives} subtype
 * (RelationalCompilerExtension.convertTypes — Date stays StrictDate, Bit is
 * TinyInt), and a plan's TDS tuple then spells that pure type's DEFAULT
 * relational type (transform/fromPure/pureToRelational.pure
 * pureTypeToDataTypeMap: Varchar → VARCHAR(1024), Numeric → DECIMAL(10,10)),
 * never the physical column's width — resultColumns keep the physical.
 */
public final class PreciseTypes {

    private PreciseTypes() {
    }

    private static final String PP = "meta::pure::precisePrimitives::";

    /** The precise pure type name of a DDL column type. */
    public static String pureType(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.Integer_ ignored -> PP + "Int";
            case RelationalDataType.Float_ ignored -> PP + "Float4";
            case RelationalDataType.Varchar ignored -> PP + "Varchar";
            case RelationalDataType.Char_ ignored -> PP + "Varchar";
            case RelationalDataType.Decimal ignored -> PP + "Numeric";
            case RelationalDataType.Numeric ignored -> PP + "Numeric";
            case RelationalDataType.Timestamp ignored -> PP + "Timestamp";
            case RelationalDataType.Date_ ignored -> "StrictDate";
            case RelationalDataType.Time ignored -> "StrictTime";
            case RelationalDataType.TimestampNs ignored -> PP + "Timestamp";
            case RelationalDataType.BigInt ignored -> PP + "BigInt";
            case RelationalDataType.SmallInt ignored -> PP + "SmallInt";
            case RelationalDataType.TinyInt ignored -> PP + "TinyInt";
            case RelationalDataType.Double_ ignored -> PP + "Double";
            case RelationalDataType.Bit ignored -> PP + "TinyInt";
            case RelationalDataType.Real ignored -> PP + "Double";
            case RelationalDataType.SemiStructured ignored -> "meta::pure::metamodel::variant::Variant";
            default -> throw new com.legend.error.NotImplementedException(
                    "plan: precise pure type for " + t + " pending");
        };
    }

    /** The default relational spelling of a precise pure type (the
     * engine's pureTypeToDataTypeMap). */
    public static String defaultSpelling(String pureType) {
        return switch (pureType) {
            case PP + "Int", PP + "UInt" -> "INT";
            case PP + "BigInt", PP + "UBigInt" -> "BIGINT";
            case PP + "SmallInt", PP + "USmallInt" -> "SMALLINT";
            case PP + "TinyInt", PP + "UTinyInt" -> "TINYINT";
            case PP + "Varchar" -> "VARCHAR(1024)";
            case PP + "Float4" -> "FLOAT";
            case PP + "Double" -> "DOUBLE";
            case PP + "Numeric" -> "DECIMAL(10,10)";
            case PP + "Timestamp" -> "TIMESTAMP";
            case "StrictDate" -> "DATE";
            default -> throw new com.legend.error.NotImplementedException(
                    "plan: default relational spelling for " + pureType + " pending");
        };
    }
}
