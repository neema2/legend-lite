// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.model.RelationalDataType;

/**
 * The normalizer's SQL-column-type &rarr; pure-kind reader — EXHAUSTIVE
 * (remediation T1.5): null previously meant BOTH "column not found" and
 * "variant unmapped", silently skipping the declared-type coercion.
 * Scalar kinds align with StoreCompiler.columnType; non-scalar carriers
 * answer their OWN names (never matching a declared platform kind, so
 * coercion skips EXPLICITLY). T3.1 will merge this with
 * StoreCompiler.columnType into the one reader.
 */
public final class RelationalKinds {

    private RelationalKinds() {
    }

    public static String pureKindOf(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.Varchar v -> "String";
            case RelationalDataType.Char_ c -> "String";
            case RelationalDataType.BigInt b -> "Integer";
            case RelationalDataType.SmallInt s -> "Integer";
            case RelationalDataType.TinyInt s -> "Integer";
            case RelationalDataType.Integer_ i -> "Integer";
            case RelationalDataType.Float_ f -> "Float";
            case RelationalDataType.Double_ d -> "Float";
            case RelationalDataType.Real r -> "Float";
            case RelationalDataType.Decimal d -> "Decimal";
            case RelationalDataType.Numeric n -> "Decimal";
            case RelationalDataType.Bit b -> "Boolean";
            case RelationalDataType.Timestamp ts -> "DateTime";
            case RelationalDataType.Date_ d -> "StrictDate";
            case RelationalDataType.Time tm -> "StrictTime";
            case RelationalDataType.TimestampNs tn -> "DateTime";
            // EXHAUSTIVE (remediation T1.5): null previously meant BOTH
            // "column not found" and "variant unmapped" — the coercion was
            // silently skipped. Aligned with StoreCompiler.columnType:
            case RelationalDataType.Binary b -> "Byte";
            case RelationalDataType.Varbinary b -> "Byte";
            // aligned with StoreCompiler.columnType: a type Pure cannot name
            // is a String (read as text); a nested value is a Variant
            case RelationalDataType.SemiStructured x -> "Variant";
            case RelationalDataType.Distinct d -> "String";
            case RelationalDataType.Other o -> "String";
            case RelationalDataType.Array a -> "Variant";
            case RelationalDataType.Object_ o -> "Variant";
        };
    }
}
