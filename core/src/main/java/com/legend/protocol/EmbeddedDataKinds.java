// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

/**
 * The kinds of data a model writes ({@code ###Data} elements, a test suite's data), each named once, and the one kind a
 * feature of legend-lite loads today: a relational store's CSV rows (B2e, 2026-11-11, census M17: until then the other
 * kinds were dropped where they were read, a {@code continue} or a skip). Every reader of the data switches over the
 * kinds without a default, so a kind added to the protocol is classified where it is read; a feature that would read a
 * kind it does not load refuses it as not implemented yet, by this name (the user: "not implemented yet"), and when such
 * a feature is built its data takes the model-data path, a load (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §9). Pinned by
 * {@code EmbeddedDataKindsTest}.
 */
public final class EmbeddedDataKinds {

    private EmbeddedDataKinds() {
    }

    /** The kind's name as the grammar spells it, and what it is. */
    public static String name(Protocol.PEmbeddedDataValue value) {
        return switch (value) {
            case Protocol.PRelationalCsvData r -> "Relational (a relational store's CSV rows)";
            case Protocol.PDataReference r -> "Reference (another Data element: " + r.dataElement().path() + ")";
            case Protocol.PModelStoreData m -> "ModelStore (a model store's instances)";
            case Protocol.PExternalFormatData e -> "ExternalFormat (a document of " + e.contentType() + ")";
            case Protocol.PServiceStoreData s -> "ServiceStore (a service store's stubs)";
            case Protocol.PRelationData r -> "Relation (a relation's rows, upstream's RelationElementsData)";
        };
    }

    /** What a feature that does not load {@code value}'s kind says of it: {@code feature}, the kind, not implemented
     *  yet. */
    public static String notImplemented(Protocol.PEmbeddedDataValue value, String feature) {
        return feature + ": " + name(value) + " data is not implemented yet (legend-lite loads a relational store's CSV"
                + " rows alone; census M17)";
    }
}
