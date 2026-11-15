// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testcases;

import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.InputDialect;
import com.legend.protocol.sql.SqlSyntaxException;
import com.legend.protocol.sql.SqlToPure;

/**
 * A SQL text translated as the compiler's island seam translates it (C5-J's last round, A1, 2026-11-15): read by its
 * dialect through the parse door ({@code door.Parse.sqlIsland}), a text that does not read the translation's refusal
 * of its kind ({@link SqlToPure#refusal}), then translated by the same dialect. {@code SqlToPure} took the text and
 * read it itself until then; the translator takes the tree now, and the tests that hand it a text come here.
 */
public final class SqlText {

    private SqlText() {
    }

    public static ValueSpecification translate(String sql, SqlToPure.Sources sources, InputDialect dialect) {
        com.legend.protocol.sql.SqlAst.Statement statement;
        try {
            statement = com.legend.door.Parse.sqlIsland(sql, dialect);
        } catch (SqlSyntaxException e) {
            throw SqlToPure.refusal(e);
        }
        return SqlToPure.translate(statement, sources, dialect);
    }
}
