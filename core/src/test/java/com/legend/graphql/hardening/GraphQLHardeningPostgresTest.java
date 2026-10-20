// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql.hardening;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;

/** {@link HardeningSuite} on Postgres 16 (the embedded server gate 7P starts): the same corpus and fuzz seeds, judged
 *  by the same reference, through the Postgres dialect ({@code //core:graphql_hardening_postgres_test}). */
@org.junit.jupiter.api.Tag("postgres")
class GraphQLHardeningPostgresTest extends HardeningSuite {

    private static final ReferenceModel ASCII_FOLDING = ShopReference.build(ReferenceModel.CaseFolding.ASCII);

    @Override
    TestBackend backend() {
        return TestBackends.of(DatabaseType.Postgres);
    }

    /** The embedded server runs in the C locale (byte order, for determinism): `lower` and `upper` fold ASCII only,
     *  so `handle` and `mode: insensitive` keep an É (docs/CYPHER_STUDY_2026_10_15.md, the case-folding row). */
    @Override
    ReferenceModel reference() {
        return ASCII_FOLDING;
    }
}
