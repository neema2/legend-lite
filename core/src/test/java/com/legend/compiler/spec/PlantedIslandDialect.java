// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlParser;

/**
 * A class PLANTED to stand for an island dialect with two reads, for one test alone (C5-J's zone fix, 2026-11-15):
 * {@code ArchitectureTest.theParseRulesBiteAPlantedCaller} builds the parse rule with both reads named, and it passes,
 * then with {@link #read} alone named, and it fails on {@link #alsoReads} -- a dialect's reads are named by signature,
 * not by class. A test class: the rule's real check reads the product's classes, never this.
 */
public final class PlantedIslandDialect {

    private PlantedIslandDialect() {
    }

    /** The read the test names in both rules. */
    public static SqlAst.Statement read(String sql) {
        return SqlParser.parse(sql);
    }

    /** The read the second rule leaves unnamed. */
    public static SqlAst.Statement alsoReads(String sql) {
        return SqlParser.parse(sql);
    }
}
