// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

/**
 * THE RULES AN EDITION'S TABLE MAY NAME ({@link EditionTable#lowersAs}; the edition program's step 5, 2026-11-26): a
 * closed list of the product's other ways to write a scalar call, each named by WHAT IT WRITES and never by who wants
 * it. A door's table maps a call to one of these and the lowering writes it in place of the call's own rule
 * ({@link NamedRules}); the product's own table names none.
 *
 * <p>These are rules, not functions: nothing in the Pure language names one, so no program can call one and no
 * frontend writes one. Until 2026-11-26 the first two were natives of the language
 * ({@code meta::legend::lite::substringAsGiven}, {@code substrAsGiven}) that only the compiler could write, a relic of
 * the door's rewrite of the typed query (A3, deleted 2026-11-25): the table is read where the SQL is produced, so a
 * row needs a rule to name and nothing more.
 *
 * <p>A rule is written for the call its row maps: it reads that call's typed operands as its own rule would.
 */
public enum NamedRule {

    /** The target database's own SUBSTRING over the arguments as given, unchanged: a start of 0 or below, a negative
     *  length, as that database reads them ({@code SqlFn.SUBSTRING_AS_GIVEN}, which every dialect writes as its plain
     *  SUBSTRING and no pass rewrites). Not lite's SQL meaning ({@code SqlFn.SUBSTRING}, a start below 1 raised to 1
     *  where a database reads it otherwise). */
    SUBSTRING_AS_GIVEN,

    /** The target database's own SUBSTRING over Pure's indexes ({@code s, start[, end]}: 0-based, end-exclusive) as
     *  {@code start + 1} and {@code end - start}, the positions SQL arithmetic the database computes, a literal index
     *  included, with no range check. In range that is Pure's answer; out of range it is the database's own cut-short
     *  reading. */
    SUBSTR_AS_GIVEN,

    /** SQL's POSITION: where the second argument first occurs in the first, 1-based, 0 when it does not (the
     *  database's STRPOS; the rule of {@code meta::legend::lite::sqlPosition}). */
    SQL_POSITION,

    /** The operand as it is, with no check that it holds exactly one value: SQL's null passes through (the rule of
     *  {@code meta::legend::lite::trustOne}). */
    TRUST_ONE,

    /** The target database's own ROUND over the arguments as given ({@code SqlFn.ROUND_AS_GIVEN}, which every dialect
     *  writes as its plain ROUND): {@code round(x)} as an Integer, {@code round(x, n)} at {@code n} places, a value
     *  exactly halfway going where that database sends it (away from zero on DuckDB and H2: 16.5 to 17). Not the
     *  product's {@code round}, which sends a half to the even neighbour on every database, as Pure does (16.5 to
     *  16). */
    ROUND_AS_GIVEN
}
