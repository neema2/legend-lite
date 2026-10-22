// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.util.List;

/**
 * A JDBC driver's own cell types, joined through {@code ServiceLoader} as {@link BulkLoad} is: core compiles
 * against no driver, so what only a driver's classes can recognise lives beside that driver and is found at run
 * time (C3c, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md). A driver with no such types registers
 * nothing.
 */
public interface DriverCells {

    /** {@code cell} as JSON text when it is this driver's own JSON node object, else null. */
    @com.legend.base.Nullable String jsonText(Object cell);

    /** The drivers' cell readers joined to this JVM, found once (the plan path program's step 7, 2026-10-18: the
     *  typed seam reads a JSON cell beside its driver as the legacy executor did). */
    List<DriverCells> JOINED = java.util.stream.StreamSupport.stream(
            java.util.ServiceLoader.load(DriverCells.class).spliterator(), false).toList();

    /** {@code cell} as JSON text when it is some joined driver's own JSON node object, else null. */
    static @com.legend.base.Nullable String jsonTextOf(Object cell) {
        for (DriverCells d : JOINED) {
            String s = d.jsonText(cell);
            if (s != null) {
                return s;
            }
        }
        return null;
    }
}
