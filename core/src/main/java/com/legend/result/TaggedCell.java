// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

/**
 * A tagged column's cell as a decoder hands it to the batch (the result boundary program's track F, 2026-10-29; the
 * track A audit's finding 5, D57): the slot index the column's tag names and that slot's own carrier, read with the
 * slot's type ({@code RowDecode.cells}, {@code JdbcDecode.row}). The batch keeps the two apart -- {@link Batch#get}
 * is the carrier, as every writer reads it (nothing a writer spells changed), {@link Batch#tag} the slot, as the
 * Arrow union needs it (until this record the union inferred the member from the carrier's class and refused two
 * slots of one carrier). A null cell carries no tag.
 */
public record TaggedCell(int slot, Object value) {
    public TaggedCell {
        if (slot < 0) {
            throw new IllegalArgumentException("a tagged cell's slot is an index: " + slot);
        }
        java.util.Objects.requireNonNull(value, "a tagged cell holds a value; a null cell carries no tag");
    }
}
