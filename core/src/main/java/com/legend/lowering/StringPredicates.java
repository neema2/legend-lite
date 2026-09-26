// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.Map;

/**
 * Character-class STRING PREDICATES lowered as one full-regexp match
 * ({@code isAlphaNumeric}; the isDigit/isLetter/isLowerCase family lands
 * here as each is claimed). Pure's bodies walk the string character by
 * character (stringExtension.pure: {@code isAlphaNumeric} = non-empty and
 * every character a digit or a letter); the platform owns the semantics
 * and states them as ONE {@code REGEXP_FULL_MATCH} — each dialect already
 * owns the anchoring spelling (DuckDB {@code regexp_full_match}, H2
 * {@code REGEXP_LIKE('^(?:…)$')}). Split from {@link Scalars} at the
 * shape limit.
 */
final class StringPredicates {

    private StringPredicates() {
    }

    static void register(Map<com.legend.model.FunctionId, Scalars.Rule> rules) {
        for (com.legend.model.FunctionId f : Pure.AT_STRING_IS_ALPHA_NUMERIC) {
            rules.put(f, (n, args) -> SqlExpr.Call.of(SqlFn.REGEXP_FULL_MATCH,
                    args.get(0), new SqlExpr.StringLit("[a-zA-Z0-9]+")));
        }
    }
}
