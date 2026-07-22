// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * Raw-SQL string utilities for the K-native {@code executeInDb} boundary:
 * a caller-supplied SQL BLOB (engine tests pass several statements in one
 * string) is split into single statements before execution. Dialect
 * adaptation of the statement text itself lives on
 * {@link com.legend.exec.RawSqlBoundary}.
 */
public final class RawSql {

    private RawSql() {
    }

    /** Split a SQL blob into single statements on top-level {@code ;} (string-aware). */
    public static List<String> splitStatements(String sql) {
        List<String> out = new ArrayList<>();
        int start = 0;
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'') {
                i = skipString(sql, i);
                continue;
            }
            if (c == ';') {
                String stmt = sql.substring(start, i).strip();
                if (!stmt.isEmpty()) {
                    out.add(stmt);
                }
                start = i + 1;
            }
            i++;
        }
        String tail = sql.substring(start).strip();
        if (!tail.isEmpty()) {
            out.add(tail);
        }
        return out;
    }

    /** Index just past the string literal opening at {@code i} (SQL {@code ''} doubling handled). */
    private static int skipString(String source, int i) {
        int n = source.length();
        i++;   // opening quote
        while (i < n) {
            char c = source.charAt(i);
            if (c == '\'') {
                if (i + 1 < n && source.charAt(i + 1) == '\'') {
                    i += 2;   // doubled quote = escaped quote inside the literal
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return n;
    }
}
