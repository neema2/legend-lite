// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * A JDBC driver's own cell types, joined through {@code ServiceLoader} as {@link BulkLoad} is: core compiles
 * against no driver, so what only a driver's classes can recognise lives beside that driver and is found at run
 * time (C3c, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md). A driver with no such types registers
 * nothing.
 */
public interface DriverCells {

    /** {@code cell} as JSON text when it is this driver's own JSON node object, else null. */
    @com.legend.base.Nullable String jsonText(Object cell);
}
