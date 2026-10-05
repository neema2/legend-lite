// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import org.duckdb.JsonNode;

/** DuckDB's driver hands a JSON cell back as its own {@link JsonNode}, whose {@code toString} is the JSON text. */
public final class DuckDbCells implements DriverCells {

    @Override
    public @com.legend.base.Nullable String jsonText(Object cell) {
        return cell instanceof JsonNode node ? node.toString() : null;
    }
}
