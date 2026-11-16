// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.protocol.sql.PostgresDialect;
import com.legend.protocol.sql.SqlAst;

/**
 * A class PLANTED to read an island's SQL past the parse doors through an anonymous subclass of a dialect, for one test
 * alone (C5-J's last round's re-check, 2026-11-15): {@code new PostgresDialect(){}.parse(sql)} compiles to a call whose
 * owner is the anonymous class, in the caller's own package, and a rule that counted a dialect's parse only on an owner
 * in the protocol layer let it through. {@code ArchitectureTest.PARSE_BY_THE_DOORS} fails on it. It lives in the
 * dialect package's tests, which name databases by design ({@code DatabaseOwners}): the one dialect a class outside the
 * protocol can extend is named so. A test class: the rule's real check reads the product's classes, never this one.
 */
public final class PlantedDialectSubclassParse {

    private PlantedDialectSubclassParse() {
    }

    /** An island's SQL read by an anonymous dialect, past the door. */
    public static SqlAst.Statement read(String sql) {
        return new PostgresDialect() {
        }.parse(sql);
    }
}
