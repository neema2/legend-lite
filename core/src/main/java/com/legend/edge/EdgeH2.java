// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.dialect.H2;

import java.util.List;

/**
 * THE EDGE'S H2: lite's H2 dialect on legend-engine's H2 session (the audit's J22, 2026-11-17, RULED by the user, "Yes
 * to the rule": the product's H2 sessions use H2's defaults; legend-engine's quirk stays at the edge, byte-exact).
 */
public final class EdgeH2 extends H2 {

    /** The settings legend-engine's H2 database has (H2Manager / H2Defaults, 4.145.0): {@code MODE=LEGACY}, and
     *  {@code DEFAULT_NULL_ORDERING=HIGH}, its null placement (null is largest -- ascending nulls last, descending
     *  first), which its printer leaves bare because the database already orders that way. Both are the database's,
     *  and their {@code SET} commits an open transaction (measured on 2.5.252), so they are the target's data, written
     *  on the URL of an in-memory database exec opens and read back from a session it did not
     *  ({@code INFORMATION_SCHEMA.SETTINGS} reports each as written). Moved from {@code H2.DATABASE_SETTINGS}. */
    static final DatabaseSettings LEGACY_SETTINGS = differing(List.of(new Setting("MODE", "LEGACY"),
            new Setting("DEFAULT_NULL_ORDERING", "HIGH")));

    /** {@inheritDoc} legend-engine's ({@link #LEGACY_SETTINGS}). */
    @Override
    public DatabaseSettings databaseSettings() {
        return LEGACY_SETTINGS;
    }
}
