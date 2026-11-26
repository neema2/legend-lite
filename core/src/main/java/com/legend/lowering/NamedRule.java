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

    /** The target database's own ROUND over the arguments as given ({@code SqlFn.ROUND_RAW}, which every dialect
     *  writes as its plain ROUND): {@code round(x)} as an Integer, {@code round(x, n)} at {@code n} places, a value
     *  exactly halfway going where that database sends it (away from zero on DuckDB and H2: 16.5 to 17). Not the
     *  product's {@code round}, which sends a half to the even neighbour on every database, as Pure does (16.5 to
     *  16). */
    ROUND_AS_GIVEN,

    /** A value read as the target database's DECIMAL with no precision written ({@code SqlFn.DECIMAL_RAW}): the
     *  database's own default precision and scale -- three places on DuckDB, none on H2, where 3.8 is 4. Not the
     *  product's {@code toDecimal}, which keeps the value's own digits. */
    CAST_TO_BARE_DECIMAL,

    /** A text read as an Integer by the target database's own cast ({@code SqlFn.CAST_INT}), its refusal the
     *  database's own conversion error. Not the product's {@code parseInteger}, which checks the text against Pure's
     *  grammar first and raises lite's own error. */
    TEXT_CAST_TO_INTEGER,

    /** A text read as a Float by the target database's own cast ({@code SqlFn.CAST_FLOAT}), its refusal the
     *  database's own conversion error. Not the product's {@code parseFloat}, which checks the text first. */
    TEXT_CAST_TO_FLOAT,

    /** Whether a text has no cased letter: {@code LOWER(s) = UPPER(s)}, true of digits, signs, blanks and the empty
     *  text alike; NULL of NULL. */
    TEXT_HAS_NO_CASED_LETTER,

    /** Whether a text is a plain decimal number: an optional minus sign, digits, and optionally a point and digits,
     *  the whole text (a full match of {@code [-]?[0-9]+(\.[0-9]+)?}). No plus sign, no exponent, no blank; NULL of
     *  NULL. */
    TEXT_IS_PLAIN_DECIMAL,

    /** {@code dateDiff} as the target database's own date difference counts the unit ({@code SqlFn.DATE_DIFF}, the
     *  plain function): weeks, hours, minutes and seconds as that database counts them -- boundaries crossed on one,
     *  elapsed whole units on another. Not the product's {@code dateDiff}, which writes Pure's count for those four
     *  units on every database (weeks by Sunday boundaries; hours, minutes and seconds elapsed, truncated). */
    DATE_DIFF_AS_GIVEN,

    /** {@code splitPart} as the target database's own {@code split_part}, its index made 1-based and nothing else
     *  ({@code SqlFn.SPLIT_PART}): an empty part is a part, the separator is a whole text, an index past the parts is
     *  the empty text. Not the product's {@code splitPart}, Pure's (empty parts dropped, an index past them an
     *  error). */
    SPLIT_PART_AS_GIVEN,

    /** A sort with no key and no comparator whose EQUAL NUMBERS OF DIFFERENT TYPES keep the order they were written
     *  in (a stable sort that compares {@code 3} and {@code 3.0} as equal): {@code [3.0, 3, 1]->sort()} is
     *  {@code [1, 3.0, 3]}. Not the product's {@code sort}, which orders equal numbers by their type (Integer, Float,
     *  Decimal). Every other sort is written by its own rule. */
    SORT_TIES_AS_WRITTEN,

    /** {@code max}, {@code min}, {@code greatest} and {@code least} keeping, where several elements hold the winning
     *  value, the element a left fold keeps: over a LIST the last written of them ({@code [3, 3.0]->max()} is the
     *  Float, {@code [3.0, 3]->max()} the Integer), of several ARGUMENTS the first ({@code max(3, 3.0)} is the
     *  Integer). Not the product's, which keeps the element whose type orders it so (the greatest of equal numbers
     *  the Decimal, then the Float; the least the Integer). */
    EXTREME_KEEPS_THE_FOLDS_TIE
}
