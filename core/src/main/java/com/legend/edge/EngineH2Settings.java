// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.dialect.H2;
import com.legend.sql.dialect.SqlDialect;

import java.util.List;

/**
 * legend-engine's H2 SETTINGS, the edge's form of {@link com.legend.sql.dialect.H2Settings} (the audit's J22, 2026-11-17,
 * RULED by the user: the product's H2 uses H2's own): {@code MODE=LEGACY}, and {@code DEFAULT_NULL_ORDERING=HIGH}, its
 * null placement (null is largest -- ascending nulls last, descending first), which its printer leaves bare because the
 * database already orders that way (H2Manager / H2Defaults, 4.145.0).
 */
final class EngineH2Settings implements com.legend.sql.dialect.H2Settings {

    static final EngineH2Settings INSTANCE = new EngineH2Settings();

    private static final SqlDialect.DatabaseSettings SETTINGS = H2.settings(List.of(
            new SqlDialect.Setting("MODE", "LEGACY"), new SqlDialect.Setting("DEFAULT_NULL_ORDERING", "HIGH")));

    private EngineH2Settings() {
    }

    @Override
    public SqlDialect.DatabaseSettings settings() {
        return SETTINGS;
    }
}
