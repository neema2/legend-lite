package com.legend.sql;

import java.util.List;

/**
 * One element of a TYPED date-format (remediation T3.2): the format
 * interchange between lowering and dialects is a list of these &mdash;
 * never a C-format string a renderer re-parses. Producers build parts
 * once (pure's format vocabulary parses at the lowering boundary);
 * each dialect SPELLS parts in its own vocabulary (DuckDB strftime
 * codes, H2 formatdatetime patterns, DB2 to_date patterns) with an
 * exhaustive switch &mdash; a new part is a compile error at every
 * speller, an unsupported part a loud wall.
 */
public sealed interface DateFmt {

    /** A format directive (field of the date/time value). */
    enum Part implements DateFmt {
        YEAR4, MONTH2, DAY2, HOUR2, MIN2, SEC2,
        /** Six-digit fractional seconds (DuckDB {@code %f}). */
        SUBSEC_MICRO,
        /** Nine-digit fractional seconds (DuckDB {@code %n}) — the
         * engine's WIRE-DECODE convention for value-read temporals
         * (fromSQLTimestamp {@code %09d}, disagree-9 burn). */
        SUBSEC_NANO,
        /** Minimal fractional seconds — trailing zeros trimmed to
         * engine convention (DuckDB {@code %g}). */
        SUBSEC_MIN,
        /** Abbreviated month name ({@code Jan}). */
        MONTH_ABBREV,
        /** Full month name ({@code January}). */
        MONTH_NAME,
        /** Full weekday name ({@code Saturday}). */
        WEEKDAY_NAME,
        /** Two-digit 12-hour clock (DuckDB {@code %I}). */
        HOUR12,
        /** Unpadded 12-hour clock (DuckDB {@code %-I}). */
        HOUR12_NOPAD,
        /** AM/PM marker (DuckDB {@code %p}). */
        AMPM
    }

    /** Verbatim text between directives. */
    record Text(String s) implements DateFmt {
    }

    // ---- the fixed formats lowering emits ----

    static final List<DateFmt> DATE = List.of(Part.YEAR4, new Text("-"), Part.MONTH2,
            new Text("-"), Part.DAY2);

    static final List<DateFmt> YEAR_MONTH = List.of(Part.YEAR4, new Text("-"), Part.MONTH2);

    static final List<DateFmt> ISO_LOCAL = List.of(Part.YEAR4, new Text("-"), Part.MONTH2,
            new Text("-"), Part.DAY2, new Text("T"), Part.HOUR2,
            new Text(":"), Part.MIN2, new Text(":"), Part.SEC2);

    /** Time-of-day only ({@code HH:mm:ss}) — the RENDER phase's
     *  abstract-Date hasHour test rides it. */
    static final List<DateFmt> TIME_ONLY = List.of(Part.HOUR2,
            new Text(":"), Part.MIN2, new Text(":"), Part.SEC2);

    /** ISO_LOCAL + trailing dot (a subsecond tail is concatenated on). */
    static final List<DateFmt> ISO_DOT = concat(ISO_LOCAL, new Text("."));

    static final List<DateFmt> ISO_MICRO = concat(ISO_LOCAL, new Text("."), Part.SUBSEC_MICRO);

    /** The VALUE-READ wire spelling (disagree-9 burn): a wire temporal
     * cell read into the pure value domain carries NINE subsecond
     * digits — the engine's own decode ({@code %09d}). */
    static final List<DateFmt> ISO_NANO = concat(ISO_LOCAL, new Text("."), Part.SUBSEC_NANO);

    private static List<DateFmt> concat(List<DateFmt> head, DateFmt... tail) {
        java.util.List<DateFmt> out = new java.util.ArrayList<>(head);
        out.addAll(List.of(tail));
        return List.copyOf(out);
    }
}
