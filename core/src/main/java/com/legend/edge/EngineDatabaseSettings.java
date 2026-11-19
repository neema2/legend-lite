// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.dialect.SqlDialect;

import java.util.List;

/**
 * legend-engine's database settings, the edge's form of {@link com.legend.sql.dialect.DatabaseSettingsForm} (the audit's
 * J22, 2026-11-17, RULED by the user: the product's database has its own): {@code MODE=LEGACY}, and
 * {@code DEFAULT_NULL_ORDERING=HIGH}, its null placement (null is largest), which its printer leaves bare because the
 * database already orders that way (H2Manager / H2Defaults, 4.145.0). Data only: the dialect writes the read-back.
 */
final class EngineDatabaseSettings implements com.legend.sql.dialect.DatabaseSettingsForm {

    static final EngineDatabaseSettings INSTANCE = new EngineDatabaseSettings();

    private static final List<SqlDialect.Setting> REQUIRED = List.of(new SqlDialect.Setting("MODE", "LEGACY"),
            new SqlDialect.Setting("DEFAULT_NULL_ORDERING", "HIGH"));

    private EngineDatabaseSettings() {
    }

    @Override
    public List<SqlDialect.Setting> required() {
        return REQUIRED;
    }
}
