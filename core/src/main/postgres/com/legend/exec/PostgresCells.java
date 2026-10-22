// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import org.postgresql.util.PGobject;

/**
 * Postgres's driver hands a {@code json} or {@code jsonb} cell back as a {@link PGobject} whose value is the JSON
 * text (a struct, a map's entry, a variant); every other PGobject (an interval, a geometry) is not JSON and is left
 * to the decode's own arms. Found by channel B on Postgres (the plan path program's step 9, 2026-10-19): the typed
 * rows refused every instance, pair and map cell, "the driver's value, a org.postgresql.util.PGobject {...}, is not
 * of that kind".
 */
public final class PostgresCells implements DriverCells {

    @Override
    public @com.legend.base.Nullable String jsonText(Object cell) {
        if (cell instanceof PGobject p && ("jsonb".equals(p.getType()) || "json".equals(p.getType()))) {
            return p.getValue();
        }
        return null;
    }
}
