// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.List;

/**
 * THE PRODUCT'S H2 SETTINGS ({@link H2Settings}; the audit's J22, RULED by the user 2026-11-17): H2's own defaults,
 * declared ({@code MODE=REGULAR}, {@code DEFAULT_NULL_ORDERING=LOW}: what a bare 2.5.252 database answers, measured);
 * the null placement is written on every sort key ({@code AnsiSqlRenderer.sortKey}), as on every other database.
 */
public final class ProductH2Settings implements H2Settings {

    public static final ProductH2Settings INSTANCE = new ProductH2Settings();

    private static final SqlDialect.DatabaseSettings SETTINGS = H2.settings(List.of(
            new SqlDialect.Setting("MODE", "REGULAR"), new SqlDialect.Setting("DEFAULT_NULL_ORDERING", "LOW")));

    private ProductH2Settings() {
    }

    @Override
    public SqlDialect.DatabaseSettings settings() {
        return SETTINGS;
    }
}
