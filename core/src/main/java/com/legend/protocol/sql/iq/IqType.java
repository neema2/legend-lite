// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.sql.InputDialect;

import java.math.BigInteger;
import java.util.Set;

/**
 * What the {@code #IQ{...}#} island knows of a value's IQ type beyond its {@code SqlToPure.Kind}: the type as IQ spells
 * it ({@code SMALLINT}, {@code CHAR(5)}, {@code NUMERIC(10,2)}) -- a FROM source column's declared store type, or the
 * type IQ gives a constant or a computed value (the research's TYPING §2, §3, §8) -- and, for an integer, what the
 * translation knows of its value: the range it lies in ({@code low}..{@code high}; a constant's is its value), and
 * whether the database already computes it in 64 bits ({@code widened}: a BIGINT's always is). The range is the type's
 * own until something narrows it. Opaque to the translator, which carries it in {@code Tx.detail()}.
 *
 * <p>IQ island, 2026-10-14 (review 5's B1): {@code loose}, a value the database may compute wider than 64 bits -- a host
 * variable's (its value written as a literal, which lite's lowering widens near 2^63, a HUGEINT on DuckDB, a NUMERIC(38)
 * on Postgres), and anything computed from one; a constant there is too ({@link #wider}) -- so that a BIGINT result
 * over it is checked by the island, not left to the database's own 64-bit overflow, which never comes.
 *
 * <p>The IQ scale program, 2026-10-25 (the share of a total; measured on IQ 17.1): {@code quotientDigits}, the digits
 * before its point IQ's engine counts a value with as a quotient's divisor, where they are not its type's -- a sum's:
 * IQ computes it in a narrower type than it shows (a window SUM of an INTEGER shows a NUMERIC(20,0) and divides as one
 * of 11 digits: 100.0 * v / SUM(v) OVER () has 12 places); null, its type's.
 */
public record IqType(String spelling, boolean widened, @com.legend.base.Nullable BigInteger low,
        @com.legend.base.Nullable BigInteger high, boolean loose, @com.legend.base.Nullable Integer quotientDigits)
        implements InputDialect.TypeDetail {

    /** IQ's integer types this island computes with, narrowest first: BIT is an integer, 0 or 1, in IQ. TINYINT and
     *  the unsigned types are not among them (their arithmetic widens to unsigned types, refused by name). */
    static final Set<String> INTEGERS = Set.of("BIT", "SMALLINT", "INTEGER", "BIGINT");

    static final BigInteger SMALLINT_MIN = BigInteger.valueOf(Short.MIN_VALUE);
    static final BigInteger SMALLINT_MAX = BigInteger.valueOf(Short.MAX_VALUE);
    static final BigInteger INTEGER_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
    static final BigInteger INTEGER_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
    static final BigInteger BIGINT_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    static final BigInteger BIGINT_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    /** The magnitude past which lite's lowering writes an integer literal as a wider integer (its INT64-edge rule:
     *  above {@code Long.MAX_VALUE >> 2}). */
    static final BigInteger LITERAL_EDGE = BigInteger.valueOf(Long.MAX_VALUE >> 2);

    /** A type with nothing known of its values beyond it: a column's declared type. */
    public IqType(String spelling) {
        this(spelling, false, null, null, false, null);
    }

    /** A type of a value in {@code low..high}, never computed wider than 64 bits. */
    public IqType(String spelling, boolean widened, @com.legend.base.Nullable BigInteger low,
            @com.legend.base.Nullable BigInteger high) {
        this(spelling, widened, low, high, false, null);
    }

    /** A type of a value in {@code low..high}, which the database may compute wider than 64 bits when {@code loose}. */
    public IqType(String spelling, boolean widened, @com.legend.base.Nullable BigInteger low,
            @com.legend.base.Nullable BigInteger high, boolean loose) {
        this(spelling, widened, low, high, loose, null);
    }

    /** This type, divided by as a value of {@code digits} before its point ({@code quotientDigits}). */
    IqType dividingAs(int digits) {
        return new IqType(spelling, widened, low, high, loose, digits);
    }

    /** {@code family} of a value in {@code low..high}, computed in 64 bits by the database when {@code widened}. */
    static IqType integer(String family, BigInteger low, BigInteger high, boolean widened) {
        return new IqType(family, widened, low, high, false);
    }

    /** {@code family} of a value in {@code low..high}, which the database may compute wider than 64 bits when
     *  {@code loose}. */
    static IqType integer(String family, BigInteger low, BigInteger high, boolean widened, boolean loose) {
        return new IqType(family, widened, low, high, loose);
    }

    /** Whether the database may compute this value wider than 64 bits: {@code loose}, or a constant beyond
     *  {@link #LITERAL_EDGE} (its literal widened). */
    boolean wider() {
        return loose || exact() && lo().abs().compareTo(LITERAL_EDGE) > 0;
    }

    /** The type's name without its size: {@code CHAR}, {@code NUMERIC}, ... */
    public String family() {
        int paren = spelling.indexOf('(');
        return (paren < 0 ? spelling : spelling.substring(0, paren)).strip().toUpperCase(java.util.Locale.ROOT);
    }

    /** A sized type's first size: CHAR(n)'s n, NUMERIC(p,s)'s p; -1 for a type written without one. */
    public int size() {
        int open = spelling.indexOf('(');
        if (open < 0) {
            return -1;
        }
        int end = spelling.indexOf(',', open);
        return Integer.parseInt(spelling.substring(open + 1, end < 0 ? spelling.indexOf(')') : end).strip());
    }

    /** Whether this is one of the integer types the island computes with ({@link #INTEGERS}). */
    public boolean isInteger() {
        return INTEGERS.contains(family()) && spelling.indexOf('(') < 0;
    }

    /** An integer type's smallest value. */
    BigInteger min() {
        return switch (family()) {
            case "BIT" -> BigInteger.ZERO;
            case "SMALLINT" -> SMALLINT_MIN;
            case "INTEGER" -> INTEGER_MIN;
            default -> BIGINT_MIN;
        };
    }

    /** An integer type's largest value. */
    BigInteger max() {
        return switch (family()) {
            case "BIT" -> BigInteger.ONE;
            case "SMALLINT" -> SMALLINT_MAX;
            case "INTEGER" -> INTEGER_MAX;
            default -> BIGINT_MAX;
        };
    }

    /** The least value an integer of this type here may hold. */
    BigInteger lo() {
        return low != null ? low : min();
    }

    /** The greatest value an integer of this type here may hold. */
    BigInteger hi() {
        return high != null ? high : max();
    }

    /** Whether the database computes the value in 64 bits already (a BIGINT, or an operation's result that did). */
    boolean wide() {
        return widened || family().equals("BIGINT");
    }

    /** Whether the value is one constant: its range is one value. */
    boolean exact() {
        return low != null && low.equals(high);
    }

    /** The rank of an integer type in IQ's lattice (BIT, SMALLINT, INTEGER, BIGINT), for the larger of two. */
    int rank() {
        return switch (family()) {
            case "BIT" -> 0;
            case "SMALLINT" -> 1;
            case "INTEGER" -> 2;
            case "BIGINT" -> 3;
            default -> throw new IllegalStateException("not an integer type: " + spelling);
        };
    }
}
