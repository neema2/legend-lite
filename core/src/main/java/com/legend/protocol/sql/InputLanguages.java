// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql;

import com.legend.protocol.spec.IslandLanguage;
import com.legend.protocol.sql.iq.IqDialect;
import com.legend.protocol.sql.iq.IqProfile;

/**
 * The input languages a SQL island is written in (the dialect program's D11, 2026-10-18): the registry of
 * {@link InputDialect}s, each the dialect of one {@link IslandLanguage}, which names each one as the database registry
 * names every database. A language read under a profile (IQ's options) is that language's own to read.
 */
public final class InputLanguages {

    private InputLanguages() {
    }

    /** The dialect an island in {@code language} is read in under {@code profile} (null: the language's default), or
     *  null when the language is not SQL (the Cypher island: its own seam) or {@code profile} is not one of its own
     *  ({@link IslandLanguage#unknownProfile}). */
    public static @com.legend.base.Nullable InputDialect of(IslandLanguage language,
            @com.legend.base.Nullable String profile) {
        return switch (language) {
            case SQL -> PostgresDialect.INSTANCE;
            case IQ -> {
                IqProfile named = profile == null ? IqProfile.DEFAULT : IqProfile.named(profile);
                yield named == null ? null : IqDialect.of(named.settings());
            }
            // Cypher's and the timeseries language's own translators read no SQL dialect
            case CYPHER, TS -> null;
        };
    }
}
