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
     *  value, the element the function's own Pure body keeps. A Number or a date list's {@code max} and {@code min}
     *  are a left fold: the LAST written of a tie ({@code [3, 3.0]->max()} and {@code [3, 3.0]->min()} are the Float),
     *  of several ARGUMENTS the first ({@code max(3, 3.0)} is the Integer). The generic {@code max} and {@code min},
     *  and {@code greatest} and {@code least}, which call them, are an end of a stable sort: the greatest the LAST
     *  written of a tie, the least the FIRST ({@code [3, 3.0]->least()} is the Integer). Not the product's, which
     *  keeps the element whose type orders it so (the greatest of equal numbers the Decimal, then the Float; the
     *  least the Integer). */
    EXTREME_KEEPS_THE_FOLDS_TIE,

    /** {@code max} and {@code min} of an Integer and a Float, two arguments, in the type the DATABASE gives the two:
     *  a Float, whichever wins ({@code max} of the columns (3, 3.0) is {@code 3.0}, {@code min} of (2, 2.5) is
     *  {@code 2.0}). What legend-engine answers inside a store query, where it writes the database's own expression
     *  ({@code case when I < F then F else I end}: run 2026-11-26,
     *  docs/records/edition-homework/upstream-step6/answers.md rows S9b and K5); outside one its own Java keeps the
     *  winner in its own type ({@link #EXTREME_KEEPS_THE_FOLDS_TIE}). Written as that CASE with the Integer
     *  converted to a Float, so a NULL argument answers as legend-engine's statement does on each database. For any
     *  other pair of arguments, and for two written values (legend-engine's database answers the winner in its own
     *  type there: run 2026-11-27), it writes {@link #EXTREME_KEEPS_THE_FOLDS_TIE}'s rule (an Integer against a
     *  Decimal inside a store query was not run upstream, so it is not guessed). The user, 2026-11-27: "great to match engine but only if not too
     *  complex and costly". */
    EXTREME_OF_TWO_IN_THE_DATABASES_TYPE,

    /** A comparison ({@code ==}, {@code eq}, {@code contains}, {@code in}, {@code removeDuplicates}, {@code <},
     *  {@code <=}, {@code >}, {@code >=}) AS UPSTREAM COMPARES, at both compatibility doors. It is three things, and
     *  was named for the first alone until 2026-11-27 ({@code NUMBERS_OF_THREE_TYPES}; the architecture audit of step
     *  6's closing batch: the name hid the other two): (1) numbers, below; (2) two dates, the last paragraph; (3) a
     *  door's row REPLACES the product's whole rule for the call ({@code EditionTable.lowersAs}), so the product's
     *  plan of two whole sides ({@code WholeComparisons}) and its refusal of two column types that are never equal
     *  ({@code HeldKinds}) do not run at a door. Splitting the three is step 6B's.
     *
     *  <p>Numbers: an Integer, a Float and a Decimal are THREE TYPES. Inside a mixed list
     *  they are never equal to one another: {@code [3, 'a']->contains(3D)} is false and
     *  {@code [3, 3D]->removeDuplicates()} keeps both. Where the two sides' types are known only when the query runs
     *  (a {@code Number}, an {@code Any}) the comparison is the database's, and nothing is refused. Not the
     *  product's, which calls an Integer and a Decimal of one value equal (two exact numbers; the user's ruling of
     *  2026-11-27) and refuses a comparison of an exact number's column with a Float's ({@code HeldKinds}). A call
     *  that meets neither is written by its own rule, unchanged.
     *
     *  <p>Upstream's equality of two DATES goes with it: a date's written precision is part of its value, so one
     *  built to the hour or the minute is never equal to a full date-time (run 2026-11-27), where the product
     *  compares the two by their instant (register S399). */
    UPSTREAMS_COMPARISON
}