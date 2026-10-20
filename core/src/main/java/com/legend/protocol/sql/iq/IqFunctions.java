// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.InputDialect;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlToPure.Kind;
import com.legend.protocol.sql.SqlToPure.Tx;
import com.legend.protocol.sql.SqlTranslationException;
import com.legend.values.PureDateLiteral;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * IQ's scalar functions beyond the NULL family (M1d, docs/IQ_SQL_STUDY_2026_10_13.md §9.6), and CAST and a style-less
 * CONVERT, as {@link IqDialect} reads them: each translated exactly with Pure's existing natives, or refused by name --
 * with the native it would need, or the research's decision it waits for. Java never computes a value here: a rule
 * reads its arguments' types and constants' spellings, and writes Pure the database computes. Measured on SQL
 * Anywhere 17 (the referee of the island's {@code engine=sa}; M1d's probes, kept with the private research), and per
 * engine where IQ's documentation differs (the research's SEMANTICS §2.2, §5):
 * <ul>
 *   <li><b>Strings.</b> {@code LENGTH}, {@code CHAR_LENGTH} and {@code LEN} count characters, trailing blanks too (a
 *       CHAR(n) column under the IQ engine is padded already: D-S1, D-S6); {@code SUBSTRING} and {@code SUBSTR} by
 *       ANSI_SUBSTRING's virtual positions (a start below 1 counts the positions before the string; a negative
 *       length is IQ's error -1090); {@code LEFT} and {@code RIGHT} (a negative count is ''); {@code TRIM},
 *       {@code LTRIM} and {@code RTRIM} of what an ISO-8859-1 database trims (the space, tab, line feed, vertical tab,
 *       form feed, carriage return), or of a constant set; {@code REPLACE}, {@code CHARINDEX}, {@code LOCATE} (a
 *       negative start finds the last occurrence) and {@code PATINDEX} (of a constant pattern, LIKE's parts read
 *       as a regular expression where they are not one literal between %s) case-sensitively, as an IQ database
 *       matches, a search of 255 characters or more NULL;
 *       {@code UPPER}, {@code LOWER}, {@code UCASE}, {@code LCASE} by IQ's ASCII-only fold, and a {@code sqla17}
 *       profile's matching without regard to the ASCII letters' case, both composed of {@code replace} (IQ island,
 *       2026-10-14: the natives' slice);
 *       {@code REPEAT}, {@code REPLICATE} and {@code SPACE} (a negative count is ''); {@code STUFF} (SQL Anywhere's
 *       front end only: D-F3); {@code ASCII}, {@code CHAR}; {@code STRING} (a NULL is '', all NULL is NULL). An integer
 *       argument is read as its digits; any other value IQ would write as text by its own format is refused.</li>
 *   <li><b>Numbers.</b> {@code ABS} (SQL Anywhere's unsigned and NUMERIC(20,0) results; under the IQ engine the
 *       smallest type that holds it, measured on IQ 17.1, where the research's D-F5 raised -158); {@code SIGN}; {@code CEILING}, {@code CEIL} and
 *       {@code FLOOR} (a DOUBLE, in SQL Anywhere's front end: D-F2); {@code ROUND} (half away from zero, in SQL
 *       Anywhere's front end: D-F1) and {@code TRUNCNUM} of integers and NUMERICs, keeping the scale; {@code MOD} and
 *       {@code REMAINDER} as {@code %}; {@code SQRT}, {@code EXP}, {@code LN}, {@code LOG} (natural), {@code LOG10}
 *       (NULL outside their domain in SQL Anywhere's front end, IQ's error under the IQ engine: D-F9); {@code PI}.</li>
 *   <li><b>Dates.</b> {@code GETDATE}, {@code NOW}, {@code CURRENT TIMESTAMP}, {@code TODAY}, {@code CURRENT DATE}:
 *       the statement's time, one value per statement; {@code YEAR}, {@code MONTH}, {@code DAY}, {@code HOUR},
 *       {@code MINUTE}, {@code SECOND}, {@code QUARTER}, {@code DOW}, {@code DATE}; {@code DATEPART} and
 *       {@code DATENAME} (SQL Anywhere's Sunday-based week, weekday 1 on Sunday at the default first day of the week;
 *       the ISO parts in SQL Anywhere's front end only: D-T10); {@code DATEADD} and {@code DATEDIFF} (D-T2, D-T12,
 *       D-T11; IQ island, 2026-10-15, measured on IQ 17.1 for IQ's engine: a DATE moved stays a DATE there, and
 *       DATEDIFF counts whole elapsed units below a day), DATEDIFF by SQL Anywhere's mixed boundary rules in its front
 *       end.</li>
 *   <li><b>NUMERIC division</b> (IQ island, 2026-10-14): SQL Anywhere's exact quotient and remainder
 *       ({@link #quotient}, {@link #remainder}), composed of decimal arithmetic, Pure's {@code rem}, and double
 *       estimates that decimal arithmetic checks; {@link IqDialect} applies them to {@code /} and {@code %}, and an
 *       AVG of a NUMERIC is the same quotient of its SUM by its COUNT at AVG's type.</li>
 *   <li><b>CAST and CONVERT</b> (without a style) to SMALLINT, INTEGER, BIGINT (truncating toward zero, a string by
 *       IQ's strict reading), NUMERIC(p, s) (rounding half away from zero), DOUBLE, CHAR(n), VARCHAR(n) and LONG
 *       VARCHAR (a longer string IQ's error -638 in SQL Anywhere's front end; under the IQ engine D-C2), DATE,
 *       TIMESTAMP, BIT.</li>
 * </ul>
 */
final class IqFunctions {

    private final IqDialect dialect;
    private final IqSettings settings;
    private final IqDialect.@com.legend.base.Nullable Engine engine;

    IqFunctions(IqDialect dialect) {
        this.dialect = dialect;
        this.settings = dialect.settings();
        this.engine = dialect.engine();
    }

    // ------------------------------------------------------------------ the catalog

    /** IQ's functions this class reads, each with how many arguments it takes (else IQ's error -154). */
    private enum Fn {
        LENGTH(1, 1), LEN(1, 1), CHAR_LENGTH(1, 1), SUBSTRING(2, 3), SUBSTR(2, 3), LEFT(2, 2), RIGHT(2, 2),
        TRIM(1, 2), LTRIM(1, 2), RTRIM(1, 2), REPLACE(3, 3), REPEAT(2, 2), REPLICATE(2, 2), SPACE(1, 1),
        CHARINDEX(2, 2), LOCATE(2, 3), PATINDEX(2, 2), STUFF(4, 4), ASCII(1, 1), CHAR(1, 1), LPAD(2, 3), RPAD(2, 3),
        STRING(1, Integer.MAX_VALUE), UPPER(1, 1), LOWER(1, 1), UCASE(1, 1), LCASE(1, 1),
        ABS(1, 1), SIGN(1, 1), CEILING(1, 1), CEIL(1, 1), FLOOR(1, 1), ROUND(2, 2), TRUNCNUM(2, 2), MOD(2, 2),
        REMAINDER(2, 2), SQRT(1, 1), EXP(1, 1), LN(1, 1), LOG(1, 1), LOG10(1, 1), POWER(2, 2), SQUARE(1, 1), PI(0, 0),
        GETDATE(0, 0), NOW(0, 0), TODAY(0, 0), CURRENT_DATE(0, 0, "current date"),
        CURRENT_TIMESTAMP(0, 0, "current timestamp"), YEAR(1, 1), MONTH(1, 1), DAY(1, 1), HOUR(1, 1),
        MINUTE(1, 1), SECOND(1, 1), QUARTER(1, 1), DOW(1, 1), DATE(1, 1), DATEPART(2, 2), DATENAME(2, 2),
        DATEADD(3, 3), DATEDIFF(3, 3), ISNUMERIC(1, 1), YEARS(1, 2), MONTHS(1, 2), WEEKS(1, 2), DAYS(1, 2),
        HOURS(1, 2), MINUTES(1, 2), SECONDS(1, 2), CONVERT_STYLE(3, 3, IqParser.CONVERT_STYLE),
        CONVERT_DATE_STYLE(3, 3, IqParser.CONVERT_DATE_STYLE), STR(1, 3),
        EXTRACT(2, 2, IqParser.EXTRACT), DATEFORMAT(2, 2), DAYNAME(1, 1), MONTHNAME(1, 1), DATETIME(1, 1), YMD(3, 3),
        GREATER(2, 2), LESSER(2, 2), ARGN(2, Integer.MAX_VALUE), REVERSE(1, 1), INSERTSTR(3, 3), BYTE_LENGTH(1, 1),
        TO_CHAR(1, 2),
        REGEXP_SUBSTR(2, 5), DATALENGTH(1, 1);

        final int min;
        final int max;
        final String spelling;

        Fn(int min, int max) {
            this(min, max, null);
        }

        Fn(int min, int max, @com.legend.base.Nullable String spelling) {
            this.min = min;
            this.max = max;
            this.spelling = spelling != null ? spelling : name().toLowerCase(Locale.ROOT);
        }

        /** The function {@code name} names, as IQ matches names (ASCII case folded); null for none. */
        static @com.legend.base.Nullable Fn named(String name) {
            String key = IqLexer.lower(name);
            for (Fn f : values()) {
                if (f.spelling.equals(key)) {
                    return f;
                }
            }
            return null;
        }
    }

    /**
     * IQ island, 2026-10-16 (measured on IQ 17.1; the corpus lane: about 2,900 queries): other dialects' functions IQ
     * has none of -- its error -265, "procedure not found", wherever the call stands. A user function of the name
     * would be answered by IQ, and is no function an island translates, so each is refused naming IQ's error.
     */
    static final java.util.Set<String> NO_SUCH_FUNCTION = java.util.Set.of("string_agg", "concat", "concat_ws", "eomonth", "format",
            "choose", "datefromparts", "date_trunc", "split_part", "age", "getutcdate", "sysdatetime", "json_value",
            "last_day", "nvl", "group_concat", "stdev", "strftime", "translate", "initcap", "quotename", "decode",
            "greatest", "least", "to_date", "string_split",
            // IQ island, 2026-10-17 (the corpus lane's 19; measured on IQ 17.1): SQL Server's, MySQL's and Postgres's
            // too -- DATEDIFF_BIG's part is read as a column there (its -143 for day)
            "var", "varp", "stdevp", "unnest", "substring_index", "splitpart", "parsename", "datediff_big",
            "split_string",
            // IQ island, 2026-10-17 (the TPC-H, TPC-DS and JOB corpus lane's 450; measured on IQ 17.1): PostgreSQL's,
            // Oracle's and MySQL's (IQ reads REGEXP as a search condition, and LPAD, RPAD, GREATER and LESSER)
            "regexp_replace", "regexp_like", "regexp_count", "regexp_instr", "date_part", "random", "array_length",
            "string_to_array", "trunc", "listagg", "instr", "md5", "chr", "nth_value", "position", "array_to_string",
            "julianday");

    /** Whether {@code name} is one of the scalar functions IQ's catalog holds that the island reads ({@link Fn}). */
    static boolean catalogued(String name) {
        return Fn.named(name) != null;
    }

    /** The refusal of a call of {@code name}, one of {@link #NO_SUCH_FUNCTION}. */
    static SqlTranslationException noSuchFunction(String name, SqlAst.Pos pos) {
        return SqlTranslationException.unsupported(noSuchFunctionText(name), pos);
    }

    /** {@link #noSuchFunction}'s text. */
    static String noSuchFunctionText(String name) {
        return name + "(...) is IQ's error -265 (procedure '" + name + "' not found: no function of IQ's, unless a user"
                + " function holds the name), refused before translating";
    }

    /** Other dialects' form of {@code fn} with {@code n} arguments, which IQ raises -154 for (measured on IQ 17.1, as
     *  it plans: over no rows too); null for any other. */
    private static @com.legend.base.Nullable String foreign(Fn fn, int n) {
        return fn == Fn.CHARINDEX && n == 3 ? "CHARINDEX's start (other dialects'; IQ's is LOCATE(s, search, start))"
                : fn == Fn.ROUND && n == 3 ? "ROUND's third argument (other dialects'; IQ's is TRUNCNUM)"
                : fn == Fn.LOG && n == 2 ? "LOG's base (other dialects'; IQ's LOG is the natural one, LOG10 the"
                + " decimal)" : null;
    }

    /** How many arguments {@code f} takes, {min, max}, for a function of this catalog ({@link IqDialect}'s parse checks
     *  the count). Refused by name before translation: a delimited name with a space (only the reader writes
     *  {@code CURRENT DATE}); a constant negative SUBSTRING length (IQ's -1090 wherever computed, under ANSI_SUBSTRING On: {@code
     *  ansiSubstring}). Other dialects' forms IQ raises -154 for (CHARINDEX's start, ROUND's third argument, LOG's
     *  base) are refused by name: the syntax research holds some an accepted form, some a rejected one. IQ island,
     *  2026-10-17 (the corpus lane's 21; measured on IQ 17.1): under the {@code iq17} presets each is IQ's -154, before
     *  a missing column's -143, a conversion's -157 and a grouping's -149. */
    static int[] arity(SqlAst.FunctionCall f, boolean ansiSubstring, boolean iq17) {
        SqlAst.Ident name = f.name().get(f.name().size() - 1);
        Fn fn = Fn.named(name.text());
        if (fn == null || name.quoted() && name.text().indexOf(' ') >= 0) {
            throw SqlTranslationException.unsupported("function " + name.text() + "(...) is not supported yet",
                    f.pos());
        }
        int n = f.args().size();
        String foreign = foreign(fn, n);
        if (foreign != null) {
            if (iq17) {
                throw SqlTranslationException.invalid("wrong number of arguments to " + name.text().toUpperCase(
                        java.util.Locale.ROOT) + ": " + foreign + IqDialect.code(-154, "37505"), f.pos());
            }
            throw SqlTranslationException.unsupported(foreign + " is not supported: IQ raises its error -154", f.pos());
        }
        if ((fn == Fn.SUBSTRING || fn == Fn.SUBSTR) && n == 3 && ansiSubstring
                && signedInteger(f.args().get(2)) instanceof BigInteger len && len.signum() < 0) {
            throw SqlTranslationException.unsupported(name.text() + " of a negative length is IQ's error -1090 wherever"
                    + " it is computed: not supported", f.args().get(2).pos());
        }
        return new int[] {fn.min, fn.max};
    }

    /**
     * Refuses by name, before translation, a CAST or CONVERT of a constant IQ cannot convert -- its error raised
     * wherever the CAST is computed (SQL Anywhere computes it for each row, and so for none of no rows): a string that
     * is no integer (-157) or a number out of an integer type's range (-158); a constant with more whole digits than a
     * NUMERIC(p, s) holds (-158); a string to BIT, or a number other than 0 and 1 (-158); a string that is no ISO date
     * to DATE or TIMESTAMP (-157, or IQ's other spellings, not read); an integer longer than a CHAR or VARCHAR (-157), a
     * string longer than it but for blanks (-638).
     * <p>
     * IQ island, 2026-10-15 (checklist 0.5, measured on IQ 17.1): what only one engine cannot convert is returned, the
     * refusals' texts, for the caller to refuse once the statement's engine is known; null when there is none. Only SQL
     * Anywhere's front end: a string longer than a CHAR or VARCHAR, which IQ's engine cuts (D-C2); a constant only its
     * rounding takes past a NUMERIC's digits, which IQ's engine truncates (D-N6); a string IQ's engine reads as a number
     * and SQL Anywhere's not ('1.5' as an INTEGER). Only IQ's engine: a string it reads as no number, or past the type
     * ('', '1e2'; {@link #iqNumberText}), which SQL Anywhere reads.
     */
    static @com.legend.base.Nullable EngineRefusals constantCast(SqlAst.Expr operand, SqlAst.TypeName type,
            String what, IqSettings settings) {
        String target = type.name();
        List<Integer> sizes = type.modifiers();
        String text = operand instanceof SqlAst.StringLit lit ? lit.value() : null;
        BigDecimal number = numberConstant(operand);
        String problem = null;
        String frontEnd = null;
        String iqEngine = null;
        switch (target) {
            case "SMALLINT", "INTEGER", "BIGINT" -> {
                IqType t = new IqType(target);
                if (text != null) {
                    BigInteger v = integerText(text);
                    frontEnd = v == null ? "the string '" + text + "' is no integer: IQ's error -157" : v.compareTo(
                            t.min()) < 0 || v.compareTo(t.max()) > 0 ? v + " is out of " + target + "'s range: IQ's"
                            + " error -158" : null;
                    BigDecimal d = iqNumberText(text);
                    BigInteger w = d == null ? null : d.setScale(0, java.math.RoundingMode.DOWN).toBigIntegerExact();
                    iqEngine = w == null ? "the string '" + text + "' is no number: IQ's error -1009145" : w.compareTo(
                            t.min()) < 0 || w.compareTo(t.max()) > 0 ? w + " is out of " + target + "'s range: IQ's"
                            + " error -1009145" : null;
                } else if (number != null) {
                    BigInteger v = number.setScale(0, java.math.RoundingMode.DOWN).toBigIntegerExact();
                    if (v.compareTo(t.min()) < 0 || v.compareTo(t.max()) > 0) {
                        problem = v + " is out of " + target + "'s range: IQ's error -158";
                    }
                }
            }
            case "NUMERIC" -> {
                if (!sizes.isEmpty()) {
                    int p = sizes.get(0);
                    int sc = sizes.size() > 1 ? sizes.get(1) : 0;
                    BigDecimal top = new BigDecimal(BigInteger.TEN.pow(p - sc));
                    BigDecimal v = text != null ? IqDialect.numericText(text) : number;
                    BigDecimal w = text != null ? iqNumberText(text) : number;
                    String past = " has more whole digits than a NUMERIC(" + p + "," + sc + ") holds: IQ's error";
                    if (v != null && v.setScale(sc, java.math.RoundingMode.HALF_UP).abs().compareTo(top) >= 0) {
                        frontEnd = v + past + " -158";
                    }
                    if (text != null && w == null) {
                        iqEngine = "the string '" + text + "' is no number: IQ's error -1009145";
                    } else if (w != null && w.setScale(sc, java.math.RoundingMode.DOWN).abs().compareTo(top) >= 0) {
                        iqEngine = w + past + " -1009145";
                    }
                }
            }
            case "BIT" -> {
                if (text != null) {
                    problem = "a string to BIT is not read yet";
                } else if (number != null && number.compareTo(BigDecimal.ZERO) != 0
                        && number.compareTo(BigDecimal.ONE) != 0) {
                    problem = number + " is no BIT: IQ's error -158";
                }
            }
            case "DATE", "TIMESTAMP" -> {
                if (text != null) {
                    // IQ island, 2026-10-14 (M1e): IQ's spellings by the profile's DATE_ORDER (IqDateTime); one
                    // before the year 100 refused here as the reader's
                    String[] parts;
                    try {
                        parts = IqDateTime.read(text, settings, operand.pos());
                    } catch (SqlTranslationException e) {
                        throw com.legend.protocol.sql.SqlSyntaxException.unsupported(String.valueOf(e.getMessage()), operand.pos());
                    }
                    if (parts == null && IqDateTime.timeAlone(text) != null) {
                        // IQ island, 2026-10-16 (measured on IQ 17.1): read on 1900-01-01 by IQ's engine
                        frontEnd = "the string '" + text + "' is a time alone, which SQL Anywhere's front end reads on"
                                + " the current date";
                    } else if (parts == null && IqDateTime.unconvertible(text, settings)) {
                        // IQ island, 2026-10-17 (measured on IQ 17.1): numbers ending in a four-digit year
                        problem = "the string '" + text + "' is no date under DATE_ORDER YMD (IQ's error -157 in SQL"
                                + " Anywhere's front end, -1001006 under IQ's engine)";
                    } else if (parts == null) {
                        problem = "the string '" + text + "' is no date the island reads (IQ's error -157, or a"
                                + " spelling of IQ's date order)";
                    }
                }
            }
            case "CHAR", "VARCHAR" -> {
                if (!sizes.isEmpty()) {
                    int n = sizes.get(0);
                    if (number != null && number.toPlainString().length() > n) {
                        problem = number.toPlainString() + " is longer than " + target + "(" + n + "): IQ's error -157";
                    } else if (text != null && text.length() > n && !text.substring(n).replace(" ", "").isEmpty()) {
                        frontEnd = "'" + text + "' is longer than " + target + "(" + n + "): IQ's error -638";
                    }
                }
            }
            default -> {
            }
        }
        if (problem == null && frontEnd != null && iqEngine != null) {
            // both engines raise
            problem = frontEnd;
        }
        if (problem != null) {
            throw com.legend.protocol.sql.SqlSyntaxException.unsupported(what + " of a constant IQ cannot convert is"
                    + " not supported: " + problem + " wherever it is computed", operand.pos());
        }
        String prefix = what + " of a constant IQ cannot convert is not supported: ";
        return frontEnd == null && iqEngine == null ? null : new EngineRefusals(frontEnd == null ? null : prefix
                + frontEnd + " wherever it is computed in SQL Anywhere's front end", iqEngine == null ? null : prefix
                + iqEngine + " wherever it is computed under IQ's engine");
    }

    /** What one engine alone cannot convert ({@link #constantCast}): the refusal under SQL Anywhere's front end, under
     *  IQ's engine, or null. */
    record EngineRefusals(@com.legend.base.Nullable String frontEnd, @com.legend.base.Nullable String iqEngine) {
    }

    /** A number constant's value, signs applied; null for any other expression. */
    private static @com.legend.base.Nullable BigDecimal numberConstant(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> new BigDecimal(n.value());
            case SqlAst.NumericLit n -> n.value();
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.MINUS -> {
                BigDecimal v = numberConstant(u.operand());
                yield v == null ? null : v.negate();
            }
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.PLUS -> numberConstant(u.operand());
            default -> null;
        };
    }

    /** The rule of the function {@code name} names, or null when this catalog has none. */
    static InputDialect.@com.legend.base.Nullable FunctionRule rule(IqDialect dialect, String name) {
        Fn fn = Fn.named(name);
        if (fn == null) {
            return null;
        }
        return (f, args, at) -> IqDialect.bounded(new IqFunctions(dialect).apply(fn, f, args, at), f);
    }

    private Tx apply(Fn fn, SqlAst.FunctionCall f, List<Tx> args, SqlToPure.At at) {
        String name = f.name().get(f.name().size() - 1).text().toUpperCase(Locale.ROOT);
        if (args.size() < fn.min || args.size() > fn.max) {
            throw SqlTranslationException.invalid("wrong number of arguments to " + name + code(-154, "37505"),
                    f.pos());
        }
        if (engine == IqDialect.Engine.IQ) {
            iqEngineArguments(fn, name, f, args);
        }
        return switch (fn) {
            case LENGTH, LEN, CHAR_LENGTH -> length(name, f, args);
            case SUBSTRING, SUBSTR -> substring(name, f, args);
            case LEFT, RIGHT -> leftRight(fn == Fn.LEFT, name, f, args);
            case TRIM, LTRIM, RTRIM -> trim(fn, name, f, args);
            case REPLACE -> replace(name, f, args, at);
            case REPEAT, REPLICATE -> repeat(name, f, args.get(0), f.args().get(0), args.get(1), f.args().get(1));
            case SPACE -> repeat(name, f, Tx.value(new CString(" "), Kind.STRING, false, new IqType("CHAR(1)")),
                    f, args.get(0), f.args().get(0));
            case CHARINDEX -> locate(name, f, args.get(1), f.args().get(1), args.get(0), f.args().get(0), null, null);
            case LOCATE -> locate(name, f, args.get(0), f.args().get(0), args.get(1), f.args().get(1),
                    args.size() == 3 ? args.get(2) : null, args.size() == 3 ? f.args().get(2) : null);
            case PATINDEX -> patindex(name, f, args);
            case STUFF -> stuff(name, f, args);
            case LPAD, RPAD -> pad(fn == Fn.LPAD, name, f, args);
            case ASCII -> ascii(name, f, args);
            case CHAR -> chr(name, f, args);
            case STRING -> string(f, args);
            // IQ island, 2026-10-14 (natives): the ASCII-only fold, composed of replace
            case UPPER, LOWER, UCASE, LCASE -> foldCase(fn == Fn.UPPER || fn == Fn.UCASE, name, f, args);
            case ABS -> abs(f, args);
            case SIGN -> sign(f, args);
            case CEILING, CEIL, FLOOR -> ceilingFloor(fn == Fn.FLOOR, name, f, args);
            case ROUND, TRUNCNUM -> roundTruncate(fn == Fn.ROUND, name, f, args);
            case MOD, REMAINDER -> mod(name, f, at);
            case SQRT, EXP, LN, LOG, LOG10 -> mathematical(fn, name, f, args);
            case POWER -> {
                if (engine == IqDialect.Engine.IQ) {
                    yield iqPower(name, f, args);
                }
                throw SqlTranslationException.unsupported("POWER(...) is not supported yet in SQL Anywhere's front end:"
                        + " it answers +Infinity (an overflow, a zero to a negative power) or NULL (a negative base to"
                        + " a fraction) where lite's databases raise or answer NaN", f.pos());
            }
            case SQUARE -> square(name, f, args);
            case GREATER, LESSER -> greaterLesser(fn == Fn.GREATER, f, at);
            case ARGN -> argn(f, at);
            case REVERSE -> reverse(name, f, args);
            case TO_CHAR -> toChar(name, f, args);
            case INSERTSTR -> insertstr(name, f, args);
            case BYTE_LENGTH -> byteLength(name, f, args);
            case DATALENGTH -> dataLength(name, f, args);
            case REGEXP_SUBSTR -> regexpSubstr(name, f, args);
            case PI -> Tx.value(SqlToPure.call("pi"), Kind.FLOAT, false, new IqType("DOUBLE"));
            case GETDATE, NOW, CURRENT_TIMESTAMP -> Tx.value(SqlToPure.call("now"), Kind.DATETIME, false,
                    dated("TIMESTAMP", IqDialect.CLOCK_LATEST_YEAR));
            case TODAY, CURRENT_DATE -> Tx.value(SqlToPure.call("today"), Kind.DATE, false, dated("DATE",
                    IqDialect.CLOCK_LATEST_YEAR));
            case YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, QUARTER, DOW -> datePartOf(Part.of(fn.spelling), name, f,
                    args.get(0), f.args().get(0), true, "Sunday");
            case DATE -> dateOf(name, f, args.get(0), f.args().get(0));
            case DATEPART, DATENAME -> datePartCall(fn == Fn.DATENAME, name, f, args);
            case DATEADD -> dateAdd(name, f, args);
            case DATEDIFF -> dateDiff(name, f, args);
            case ISNUMERIC -> isNumeric(f, args);
            case YEARS, MONTHS, WEEKS, DAYS, HOURS, MINUTES, SECONDS -> dateUnit(fn, name, f, args);
            case CONVERT_STYLE -> convertStyle(f, args);
            case CONVERT_DATE_STYLE -> convertDateStyle(f, args, at);
            case STR -> str(f, args);
            case EXTRACT -> extract(f, args);
            case DATEFORMAT -> dateFormat(f, args);
            case DAYNAME, MONTHNAME -> dayMonthName(fn == Fn.DAYNAME, name, f, args);
            case DATETIME -> dateTime(name, f, args);
            case YMD -> ymd(f, args);
        };
    }

    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1): the arguments IQ's engine raises for before computing -- refused
     * before translating, with IQ's code. A number or a date given to a string function that takes only a string
     * (LEFT, RIGHT, SUBSTRING, UPPER, LOWER, the TRIMs, REPLACE, STUFF; its -1009145 "must be binary or character
     * string"; REPEAT, STRING, LENGTH, CHARINDEX, LOCATE and ASCII write it as text, as SQL Anywhere does); and the
     * time of a DATE (HOUR, MINUTE, SECOND, DATEPART and DATENAME of its hour, minute, second, millisecond or
     * microsecond; its -1009145, MILLISECOND's -1001025).
     */
    private static void iqEngineArguments(Fn fn, String name, SqlAst.FunctionCall f, List<Tx> args) {
        boolean stringsOnly = switch (fn) {
            case LEFT, RIGHT, SUBSTRING, SUBSTR, UPPER, LOWER, UCASE, LCASE, TRIM, LTRIM, RTRIM, REPLACE, STUFF,
                 REVERSE -> true;
            default -> false;
        };
        if (stringsOnly && !args.isEmpty() && args.get(0).kind() != Kind.STRING && args.get(0).kind() != Kind.NULL) {
            throw SqlTranslationException.unsupported(name + " of " + kindName(args.get(0).kind()) + " is IQ's error"
                    + " -1009145 under its engine (its argument must be a string), refused before translating",
                    f.args().get(0).pos());
        }
        Part time = switch (fn) {
            case HOUR -> Part.HOUR;
            case MINUTE -> Part.MINUTE;
            case SECOND -> Part.SECOND;
            case DATEPART, DATENAME -> {
                Part p = Part.of(((SqlAst.StringLit) f.args().get(0)).value());
                yield p == Part.HOUR || p == Part.MINUTE || p == Part.SECOND || p == Part.MILLISECOND
                        || p == Part.MICROSECOND ? p : null;
            }
            default -> null;
        };
        int at = fn == Fn.DATEPART || fn == Fn.DATENAME ? 1 : 0;
        if (time != null && args.size() > at && args.get(at).kind() == Kind.DATE) {
            throw SqlTranslationException.unsupported(name + " of a DATE's " + time + " is IQ's error -1009145 under"
                    + " its engine, refused before translating", f.args().get(at).pos());
        }
    }

    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1; the research's D-T10): DATEPART's and DATENAME's calendar parts under
     * IQ's engine -- ISO 8601's rule with weeks from Sunday: CALDAYOFWEEK Sunday 1 to Saturday 7, and week 1 the first
     * week from a Sunday with four days or more in the year (the one holding its first Wednesday), CALYEAROFWEEK that
     * week's year (2026-03-04 is week 9 of 2026, 2026-12-31 week 52, 1900-01-01 week 1). A week's Wednesday decides
     * both, as ISO's Thursday does for weeks from Monday.
     */
    private Tx iqCalendarPart(boolean name, Part part, String fnName, SqlAst.FunctionCall f, List<Tx> args) {
        Tx d = dateArgument(args.get(1), f.args().get(1), fnName);
        IqType type = IqType.integer("INTEGER", BigInteger.valueOf(part.low), BigInteger.valueOf(part.high), true);
        if (d == null) {
            return name ? nullString() : Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, type);
        }
        boolean date = d.kind() == Kind.DATE;
        Tx value = strict(Kind.INTEGER, type, List.of(d), v -> {
            ValueSpecification at = timestamp(v.get(0), date);
            ValueSpecification dow = SqlToPure.call("dayOfWeekNumber", at, new EnumValue(DAY_OF_WEEK, "Sunday"));
            if (part == Part.CALDAYOFWEEK) {
                return dow;
            }
            ValueSpecification wednesday = SqlToPure.call("adjust", at, AppliedFunction.infixRun("minus",
                    List.of(integer(4), dow)), new EnumValue(DURATION_UNIT, "DAYS"));
            return part == Part.CALYEAROFWEEK ? SqlToPure.call("year", wednesday)
                    : AppliedFunction.infixRun("plus", List.of(wholeQuotient(AppliedFunction.infixRun("minus",
                            List.of(SqlToPure.call("dayOfYear", wednesday), integer(1))), 7), integer(1)));
        });
        if (!name) {
            return value;
        }
        Tx text = SqlToPure.strict(Kind.STRING, List.of(value), v -> SqlToPure.call("toString", v.get(0)));
        return Tx.value(text.vs(), Kind.STRING, text.nullable(), varchar());
    }

    /** IQ island, 2026-10-15 (measured on IQ 17.1): {@code DAYNAME(x)} and {@code MONTHNAME(x)}, the English name of x's
     *  weekday or month, a VARCHAR(12). */
    private Tx dayMonthName(boolean day, String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx d = dateArgument(args.get(0), f.args().get(0), name);
        IqType type = new IqType("VARCHAR(12)");
        if (d == null) {
            return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, type);
        }
        boolean date = d.kind() == Kind.DATE;
        return strict(Kind.STRING, type, List.of(d), v -> {
            ValueSpecification at = timestamp(v.get(0), date);
            return day ? names(SqlToPure.call("dayOfWeekNumber", at, new EnumValue(DAY_OF_WEEK, "Sunday")), WEEKDAYS)
                    : names(SqlToPure.call("monthNumber", at), MONTHS);
        });
    }

    /** IQ island, 2026-10-15 (measured on IQ 17.1): {@code DATETIME(x)}, x as a TIMESTAMP -- a date at midnight, a
     *  timestamp itself, a constant string as IQ reads it. */
    private Tx dateTime(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx d = dateArgument(args.get(0), f.args().get(0), name);
        IqType type = new IqType("TIMESTAMP");
        if (d == null) {
            return Tx.value(SqlToPure.nullOf(Kind.DATETIME).vs(), Kind.DATETIME, true, type);
        }
        if (d.kind() == Kind.DATETIME) {
            return Tx.value(d.vs(), Kind.DATETIME, d.nullable(), type);
        }
        return strict(Kind.DATETIME, type, List.of(d), v -> timestamp(v.get(0), true));
    }

    /** IQ island, 2026-10-15 (measured on IQ 17.1): {@code YMD(y, m, d)}, the date m - 1 months and d - 1 days after
     *  January 1 of year y (a month past 12 or a day past the month's last carries on: YMD(2027, 13, 1) is 2028-01-01,
     *  YMD(2026, 2, 31) 2026-03-03); outside the years 1 to 9999 IQ's error. */
    private Tx ymd(SqlAst.FunctionCall f, List<Tx> args) {
        for (int i = 0; i < 3; i++) {
            if (args.get(i).kind() != Kind.NULL) {
                // any integer: a part that leaves the years 1 to 9999 raises below
                integerType(args.get(i), f.args().get(i), "YMD");
            }
        }
        IqType type = new IqType("DATE");
        if (args.stream().anyMatch(a -> a.kind() == Kind.NULL)) {
            return Tx.value(SqlToPure.nullOf(Kind.DATE).vs(), Kind.DATE, true, type);
        }
        return strict(Kind.DATE, type, args, v -> {
            ValueSpecification start = SqlToPure.call("date", v.get(0), integer(1), integer(1), integer(0), integer(0),
                    integer(0));
            ValueSpecification months = SqlToPure.call("adjust", start, AppliedFunction.infixRun("minus",
                    List.of(v.get(1), integer(1))), new EnumValue(DURATION_UNIT, "MONTHS"));
            ValueSpecification moved = typed(SqlToPure.call("adjust", months, AppliedFunction.infixRun("minus",
                    List.of(v.get(2), integer(1))), new EnumValue(DURATION_UNIT, "DAYS")), "DateTime");
            ValueSpecification year = SqlToPure.call("year", moved);
            return ifThen(SqlToPure.call("or", SqlToPure.call("greaterThan", year, integer(9999)), lt(year, integer(1))),
                    raise("StrictDate"), typed(SqlToPure.call("datePart", moved), "StrictDate"));
        });
    }

    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1; the census's Tier 0 dates): {@code DATEFORMAT(x, format)}, a VARCHAR
     * of the format's length, the format a constant read as IQ's date format: a run of one letter is one token, any
     * other character itself (T, q, a space, punctuation). {@code yyyy}, {@code yy}; {@code mm} the month, {@code m} it
     * in one character ({@code *} for two digits), three or more m's its English name cut to as many letters
     * ({@code mmm} mar, {@code Mmm} Mar, {@code MMM} MAR); {@code dd} the day, {@code d} the weekday Sunday 1, three or
     * more d's its name as a month's; {@code jjj} the day of the year; {@code hh} the hour, of the twelve where the
     * format has {@code a} or {@code aa} (a, am or AM, pm), {@code nn} the minute, {@code ss} the second and, after
     * {@code ss.}, as many places of its fraction as s's (cut). A two-letter number (or {@code Jjj}) written first upper
     * then lower drops its leading zeros. Refused by name: a format that is not a constant, a token not measured.
     */
    private Tx dateFormat(SqlAst.FunctionCall f, List<Tx> args) {
        if (!(f.args().get(1) instanceof SqlAst.StringLit lit)) {
            throw SqlTranslationException.unsupported("DATEFORMAT with a format other than a constant string is not"
                    + " supported yet", f.args().get(1).pos());
        }
        String format = lit.value();
        IqType type = new IqType("VARCHAR(" + format.length() + ")");
        Tx d = dateArgument(args.get(0), f.args().get(0), "DATEFORMAT");
        if (d == null) {
            return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, type);
        }
        boolean date = d.kind() == Kind.DATE;
        boolean twelve = false;
        List<String> tokens = new ArrayList<>();
        for (int i = 0; i < format.length(); ) {
            char c = format.charAt(i);
            int j = i;
            if ("ymdjhnsa".indexOf(Character.toLowerCase(c)) >= 0) {
                while (j < format.length() && Character.toLowerCase(format.charAt(j)) == Character.toLowerCase(c)) {
                    j++;
                }
            } else {
                j = i + 1;
            }
            String t = format.substring(i, j);
            twelve |= Character.toLowerCase(c) == 'a';
            tokens.add(t);
            i = j;
        }
        boolean hours12 = twelve;
        List<java.util.function.Function<ValueSpecification, ValueSpecification>> pieces = new ArrayList<>();
        for (int k = 0; k < tokens.size(); k++) {
            String t = tokens.get(k);
            char c = Character.toLowerCase(t.charAt(0));
            int n = t.length();
            boolean letter = "ymdjhnsa".indexOf(c) >= 0;
            if (!letter) {
                pieces.add(x -> new CString(t));
                continue;
            }
            boolean lower = t.equals(t.toLowerCase(Locale.ROOT));
            boolean upper = t.equals(t.toUpperCase(Locale.ROOT));
            boolean capital = !lower && !upper && Character.isUpperCase(t.charAt(0))
                    && t.substring(1).equals(t.substring(1).toLowerCase(Locale.ROOT));
            if (!lower && !upper && !capital) {
                throw dateFormatToken(t, f);
            }
            boolean pad = !capital;
            boolean fraction = c == 's' && k >= 2 && tokens.get(k - 1).equals(".")
                    && tokens.get(k - 2).equalsIgnoreCase("ss");
            pieces.add(switch (c) {
                case 'y' -> {
                    if (n != 4 && n != 2) {
                        throw dateFormatToken(t, f);
                    }
                    yield x -> n == 4 ? lpad(SqlToPure.call("year", x), 4)
                            : lpad(SqlToPure.call("rem", SqlToPure.call("year", x), integer(100)), 2);
                }
                case 'm' -> n >= 3 ? x -> names(SqlToPure.call("monthNumber", x), cased(MONTHS, n, lower, upper))
                        : n == 2 ? x -> number(SqlToPure.call("monthNumber", x), pad)
                        : x -> ifThen(lt(SqlToPure.call("monthNumber", x), integer(10)), SqlToPure.call("toString",
                                SqlToPure.call("monthNumber", x)), new CString("*"));
                case 'd' -> n >= 3 ? x -> names(SqlToPure.call("dayOfWeekNumber", x, new EnumValue(DAY_OF_WEEK,
                                "Sunday")), cased(WEEKDAYS, n, lower, upper))
                        : n == 2 ? x -> number(SqlToPure.call("dayOfMonth", x), pad)
                        : x -> SqlToPure.call("toString", SqlToPure.call("dayOfWeekNumber", x,
                                new EnumValue(DAY_OF_WEEK, "Sunday")));
                case 'j' -> {
                    if (n != 3) {
                        throw dateFormatToken(t, f);
                    }
                    yield x -> pad ? lpad(SqlToPure.call("dayOfYear", x), 3) : SqlToPure.call("toString",
                            SqlToPure.call("dayOfYear", x));
                }
                case 'h' -> {
                    if (n != 2) {
                        throw dateFormatToken(t, f);
                    }
                    yield x -> {
                        ValueSpecification h = SqlToPure.call("hour", x);
                        ValueSpecification shown = hours12 ? ifThen(SqlToPure.call("equal", SqlToPure.call("rem", h,
                                integer(12)), integer(0)), integer(12), SqlToPure.call("rem", h, integer(12))) : h;
                        return number(shown, pad);
                    };
                }
                case 'n' -> {
                    if (n != 2) {
                        throw dateFormatToken(t, f);
                    }
                    yield x -> number(SqlToPure.call("minute", x), pad);
                }
                case 's' -> {
                    if (fraction && n <= 6) {
                        yield x -> SqlToPure.call("substring", lpad(microseconds(x), 6), integer(1), integer(n));
                    }
                    if (n != 2) {
                        throw dateFormatToken(t, f);
                    }
                    yield x -> number(SqlToPure.call("second", x), pad);
                }
                default -> {
                    // a: a or p; aa: am or pm (AA: AM, PM)
                    if (n > 2 || capital) {
                        throw dateFormatToken(t, f);
                    }
                    String am = (n == 1 ? "a" : "am");
                    String pm = (n == 1 ? "p" : "pm");
                    String a = upper ? am.toUpperCase(Locale.ROOT) : am;
                    String p = upper ? pm.toUpperCase(Locale.ROOT) : pm;
                    yield x -> ifThen(lt(SqlToPure.call("hour", x), integer(12)), new CString(a), new CString(p));
                }
            });
        }
        // a DATE's time is midnight: its timestamp reads it so
        boolean isDate = date;
        return strict(Kind.STRING, type, List.of(d), v -> {
            ValueSpecification at = timestamp(v.get(0), isDate);
            List<ValueSpecification> parts = new ArrayList<>();
            for (var piece : pieces) {
                parts.add(piece.apply(at));
            }
            return parts.size() == 1 ? parts.get(0) : AppliedFunction.infixRun("plus", parts);
        });
    }

    private static SqlTranslationException dateFormatToken(String token, SqlAst.FunctionCall f) {
        return SqlTranslationException.unsupported("DATEFORMAT's '" + token + "' is not supported yet: the island reads"
                + " yyyy, yy, m to mmmm..., d to dddd..., jjj, hh, nn, ss, a fraction's s's after ss., a and aa", f.pos());
    }

    /** {@code names} cut to {@code n} letters, in lower case, upper case or as they are (capitalized). */
    private static String[] cased(String[] names, int n, boolean lower, boolean upper) {
        String[] out = new String[names.length];
        for (int i = 0; i < names.length; i++) {
            String cut = names[i].substring(0, Math.min(n, names[i].length()));
            out[i] = lower ? cut.toLowerCase(Locale.ROOT) : upper ? cut.toUpperCase(Locale.ROOT) : cut;
        }
        return out;
    }

    /** An integer's digits, two at least (a zero before one) where {@code pad}. */
    private static ValueSpecification number(ValueSpecification n, boolean pad) {
        return pad ? lpad(n, 2) : SqlToPure.call("toString", n);
    }

    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1): {@code EXTRACT(part FROM x)} -- YEAR, MONTH, DAY, HOUR and MINUTE an
     * INTEGER, as DATEPART reads them; SECOND with its fraction, a NUMERIC(24,7) under IQ's engine (05:06:07.123456 is
     * 7.1234560) and a NUMERIC(8,6) on SQL Anywhere's front end. Under IQ's engine HOUR, MINUTE and SECOND of a DATE (or
     * an untyped NULL) are IQ's error -1009145, refused before translating; the front end reads a date's time as
     * midnight.
     */
    private Tx extract(SqlAst.FunctionCall f, List<Tx> args) {
        Part part = Part.of(((SqlAst.StringLit) f.args().get(0)).value());
        Tx x = args.get(1);
        SqlAst.Expr e = f.args().get(1);
        boolean iq = engine == IqDialect.Engine.IQ;
        boolean time = part == Part.HOUR || part == Part.MINUTE || part == Part.SECOND;
        if (iq && time && (x.kind() == Kind.DATE || x.kind() == Kind.NULL)) {
            // an untyped NULL too (measured: EXTRACT(SECOND FROM NULL) is the error, of YEAR NULL)
            throw SqlTranslationException.unsupported("EXTRACT(" + part + " FROM a DATE) is IQ's error -1009145 under"
                    + " its engine, refused before translating", f.pos());
        }
        if (part != Part.SECOND) {
            return datePartOf(part, "EXTRACT", f, x, e, false, "Sunday");
        }
        int scale = iq ? 7 : 6;
        IqType type = new IqType(iq ? "NUMERIC(24,7)" : "NUMERIC(8,6)");
        Tx d = dateArgument(x, e, "EXTRACT");
        if (d == null) {
            return Tx.value(SqlToPure.nullOf(Kind.DECIMAL).vs(), Kind.DECIMAL, true, type);
        }
        boolean date = d.kind() == Kind.DATE;
        return strict(Kind.DECIMAL, type, List.of(d), v -> atScale(date ? SqlToPure.call("toDecimal", integer(0))
                : AppliedFunction.infixRun("plus", List.of(SqlToPure.call("toDecimal", SqlToPure.call("second",
                        v.get(0))), times(SqlToPure.call("toDecimal", microseconds(v.get(0))),
                        new CDecimal(new BigDecimal("0.000001"))))), scale));
    }

    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1): {@code STR(x [, length [, decimals]])} under IQ's engine -- x's text
     * right-justified in a VARCHAR(length), 10 and 0 by default, cut toward zero (IQ's engine does not round: 2.5 is
     * 2, 99.995 to 2 places 99.99). It shows max(0, min(decimals, length - 1 - w)) places, w the whole part's digits
     * (none below 1) and a minus sign's place: 123.456 in 4 to 2 places is ' 123', 0.5 in 3 to 2 places '0.50', too
     * long. A value that shows as zero is '0', the point and its places' zeros cut to the length, no sign,
     * left-justified ('0.00  ', and '0.' in 2); other text longer than the length is NULL (no asterisks). A length of 0 is a NULL VARCHAR(1), one below 0 or NULL a NULL
     * VARCHAR(255); decimals below 0 or NULL give NULL. Refused by name: SQL Anywhere's front end (it rounds a
     * DOUBLE's binary value half to even and writes asterisks), a DOUBLE (IQ's engine cuts its binary value: 0.3 to 2
     * places is 0.29), a string, a length or decimals that is no integer constant, a length over 255 or decimals over
     * the length (IQ's errors -1001005 and -1009145), a NUMERIC of more than 18 digits before its point.
     */
    private Tx str(SqlAst.FunctionCall f, List<Tx> args) {
        if (engine != IqDialect.Engine.IQ) {
            throw SqlTranslationException.unsupported("STR is supported under IQ's engine only yet: SQL Anywhere's front"
                    + " end rounds a DOUBLE's binary value half to even and writes asterisks where its text does not"
                    + " fit", f.pos());
        }
        int length = 10;
        if (args.size() > 1) {
            if (args.get(1).kind() == Kind.NULL) {
                return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, new IqType("VARCHAR(255)"));
            }
            BigInteger n = signedInteger(f.args().get(1));
            if (n == null) {
                throw SqlTranslationException.unsupported("STR with a length other than an integer constant is not"
                        + " supported yet", f.args().get(1).pos());
            }
            if (n.compareTo(BigInteger.valueOf(255)) > 0) {
                throw SqlTranslationException.unsupported("STR with a length over 255 is IQ's error -1001005 (a string"
                        + " length error), refused before translating", f.args().get(1).pos());
            }
            if (n.signum() <= 0) {
                return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true,
                        new IqType(n.signum() == 0 ? "VARCHAR(1)" : "VARCHAR(255)"));
            }
            length = n.intValue();
        }
        IqType type = new IqType("VARCHAR(" + length + ")");
        int places = 0;
        if (args.size() > 2) {
            BigInteger n = signedInteger(f.args().get(2));
            if (args.get(2).kind() == Kind.NULL || n != null && n.signum() < 0) {
                return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, type);
            }
            if (n == null) {
                throw SqlTranslationException.unsupported("STR with decimals other than an integer constant is not"
                        + " supported yet", f.args().get(2).pos());
            }
            if (n.compareTo(BigInteger.valueOf(length)) > 0) {
                throw SqlTranslationException.unsupported("STR with more decimals than its length is IQ's error"
                        + " -1009145, refused before translating", f.args().get(2).pos());
            }
            places = n.intValue();
        }
        Tx x = args.get(0);
        SqlAst.Expr e = f.args().get(0);
        if (x.kind() == Kind.NULL) {
            return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, type);
        }
        if (x.kind() == Kind.FLOAT) {
            throw SqlTranslationException.unsupported("STR of a DOUBLE is not supported yet: IQ's engine cuts its binary"
                    + " value (0.3 to 2 places is 0.29)", f.pos());
        }
        Digits digits;
        if (decimal(x)) {
            int[] ps = scaleOf(x, e, "STR");
            int whole = ps[0] - ps[1];
            int m = Math.min(places, ps[1]);
            if (whole > 18 || m > 18 || m + ps[1] > 38) {
                throw SqlTranslationException.unsupported("STR of a NUMERIC(" + ps[0] + "," + ps[1] + ") to " + places
                        + " places is not supported yet: its whole part may exceed 18 digits", f.pos());
            }
            digits = new Digits(whole, ps[1], false);
        } else if (x.kind() == Kind.INTEGER && x.detail() instanceof IqType u && u.low() == null
                && (u.spelling().equals("UNSIGNED INT") || u.spelling().equals("UNSIGNED BIGINT"))) {
            // IQ island, 2026-10-17 (the corpus lane: STR(NTILE(100) OVER (...), 3), 5 queries): an unsigned result
            // of IQ's engine (NTILE's, a COUNT's), its digits its type's most
            digits = new Digits(u.spelling().equals("UNSIGNED INT") ? 10 : 20, 0, true);
        } else if (x.kind() == Kind.INTEGER) {
            IqType t = integerType(x, e, "STR");
            int whole = Math.max(t.lo().abs().toString().length(), t.hi().abs().toString().length());
            digits = new Digits(whole, 0, true);
        } else {
            throw SqlTranslationException.unsupported("STR of " + kindName(x.kind()) + " is not supported yet", f.pos());
        }
        int d = places;
        int width = length;
        Tx r = SqlToPure.strict(Kind.STRING, List.of(x), v -> {
            ValueSpecification value = v.get(0);
            ValueSpecification result = null;
            for (int k = 0; k <= d; k++) {
                ValueSpecification branch = strBranch(value, digits, k, width);
                if (k == 0 || result == null) {
                    result = branch;
                    continue;
                }
                // k places where w <= length - 1 - k (the tests run from the most places down)
                ValueSpecification test = digits.wAtMost(value, width - 1 - k);
                result = test instanceof com.legend.protocol.spec.CBoolean c ? c.value() ? branch : result
                        : ifThen(test, branch, result);
            }
            return java.util.Objects.requireNonNull(result);
        });
        return Tx.value(r.vs(), Kind.STRING, true, type);
    }

    /** STR's number: its whole part's most digits, its scale, and whether it is an integer ({@code ofInteger}: never
     *  ABS of one, which overflows at its type's least on DuckDB -- ABS of a SMALLINT -32768). */
    private record Digits(int whole, int scale, boolean ofInteger) {

        /** {@code |x|} of a NUMERIC. */
        ValueSpecification magnitude(ValueSpecification x) {
            return SqlToPure.call("abs", x);
        }

        /** Whether {@code |x| < 10^c}: x's whole part has at most c digits (none below 1). */
        ValueSpecification below(ValueSpecification x, int c) {
            if (c < 0) {
                return new com.legend.protocol.spec.CBoolean(false, null);
            }
            if (c >= whole) {
                return new com.legend.protocol.spec.CBoolean(true, null);
            }
            if (ofInteger) {
                long bound = BigInteger.TEN.pow(c).longValueExact();
                return SqlToPure.call("and", SqlToPure.call("greaterThan", x, integer(-bound)), lt(x, integer(bound)));
            }
            return lt(magnitude(x), integer(BigInteger.TEN.pow(c).longValueExact()));
        }

        /** Whether w <= c: a minus sign's place and the whole part's digits. */
        ValueSpecification wAtMost(ValueSpecification x, int c) {
            ValueSpecification negative = below(x, c - 1);
            ValueSpecification positive = below(x, c);
            if (negative instanceof com.legend.protocol.spec.CBoolean a
                    && positive instanceof com.legend.protocol.spec.CBoolean b && a.value() == b.value()) {
                return a;
            }
            return ifThen(lt(x, integer(0)), negative, positive);
        }

        /** Whether x shows as zero to {@code k} places: |x| < 10^-k. */
        ValueSpecification zero(ValueSpecification x, int k) {
            if (ofInteger) {
                return SqlToPure.call("equal", x, integer(0));
            }
            int m = Math.min(k, scale);
            return lt(magnitude(x), m == 0 ? integer(1) : new CDecimal(pow10(-m)));
        }

        /** The whole part's digits, without a sign. */
        ValueSpecification wholeText(ValueSpecification x) {
            if (!ofInteger) {
                return SqlToPure.call("toString", SqlToPure.call("floor", magnitude(x)));
            }
            ValueSpecification text = SqlToPure.call("toString", x);
            return ifThen(lt(x, integer(0)), SqlToPure.call("substring", text, integer(2), integer(25)), text);
        }

        /** The first {@code k} places' digits, cut toward zero. */
        ValueSpecification placesText(ValueSpecification x, int k) {
            int m = ofInteger ? 0 : Math.min(k, scale);
            String zeros = "0".repeat(k - m);
            if (m == 0) {
                return new CString(zeros);
            }
            ValueSpecification a = magnitude(x);
            ValueSpecification fraction = AppliedFunction.infixRun("minus", List.of(a, SqlToPure.call("toDecimal",
                    SqlToPure.call("floor", a))));
            ValueSpecification digits = SqlToPure.call("lpad", SqlToPure.call("toString", SqlToPure.call("floor",
                    times(fraction, new CDecimal(pow10(m))))), integer(m), new CString("0"));
            return zeros.isEmpty() ? digits : AppliedFunction.infixRun("plus", List.of(digits, new CString(zeros)));
        }
    }

    /** STR's text of {@code x} to {@code k} places in {@code length} characters (see {@link #str}). */
    private static ValueSpecification strBranch(ValueSpecification x, Digits digits, int k, int length) {
        // a value that shows as zero: '0', the point and k zeros, cut to the length (measured: 0.001 in 2 to 2 places
        // is '0.'), left-justified
        String zeroText = "0" + (k > 0 ? "." + "0".repeat(k) : "");
        zeroText = zeroText.substring(0, Math.min(zeroText.length(), length));
        ValueSpecification none = SqlToPure.nullOf(Kind.STRING).vs();
        ValueSpecification zero = new CString(zeroText + " ".repeat(length - zeroText.length()));
        List<ValueSpecification> parts = new ArrayList<>();
        parts.add(ifThen(lt(x, integer(0)), new CString("-"), new CString("")));
        parts.add(digits.wholeText(x));
        if (k > 0) {
            parts.add(new CString("."));
            parts.add(digits.placesText(x, k));
        }
        ValueSpecification text = SqlToPure.call("lpad", AppliedFunction.infixRun("plus", parts), integer(length),
                new CString(" "));
        // too long: with places, only a value below 1 (its '0' has no place in w); without, a whole part too wide
        ValueSpecification tooLong;
        if (k > 0) {
            ValueSpecification belowOne = digits.below(x, 0);
            tooLong = k + 2 > length ? belowOne : k + 3 > length ? SqlToPure.call("and", belowOne, lt(x, integer(0)))
                    : null;
        } else {
            ValueSpecification fits = digits.wAtMost(x, length);
            tooLong = fits instanceof com.legend.protocol.spec.CBoolean c ? c.value() ? null
                    : new com.legend.protocol.spec.CBoolean(true, null) : SqlToPure.call("not", fits);
        }
        ValueSpecification shown = tooLong == null ? text
                : tooLong instanceof com.legend.protocol.spec.CBoolean ? none : ifThen(tooLong, none, text);
        return ifThen(digits.zero(x, k), zero, shown);
    }

    /**
     * {@code CONVERT(DATE | TIMESTAMP, x, style)} (IQ island, 2026-10-17; the corpus lane's {@code CONVERT(DATETIME,
     * '01/01/2012', 103)} and kin, measured on IQ 17.1):
     * <ul>
     *   <li>of a date or a timestamp: under IQ's engine its error -1009145 as it plans ("CONVERT argument does not
     *       match the required data type"); in SQL Anywhere's front end the style is not read, a CONVERT without
     *       it;</li>
     *   <li>of a constant string, read by the style ({@link #styledDate});</li>
     *   <li>of a string the database computes, under IQ's engine, by 20, 21, 120 or 121: {@code yyyy-mm-dd} and a time
     *       {@code hh:mm[:ss]} after one space, as CAST reads them; any other spelling is refused where it is computed
     *       -- IQ rejects most ({@code 2009-11-7}, blanks around: its -1001006) and reads others by rules of its own
     *       (its seconds past 59 carried, its fractions dropped). Another style there, and SQL Anywhere's front end,
     *       are refused by name.</li>
     * </ul>
     */
    private Tx convertDateStyle(SqlAst.FunctionCall f, List<Tx> args, SqlToPure.At at) {
        Tx x = args.get(0);
        SqlAst.Expr operand = f.args().get(0);
        BigInteger written = ((SqlAst.IntegerLit) f.args().get(1)).value();
        boolean toDate = ((SqlAst.IntegerLit) f.args().get(2)).value().signum() > 0;
        Kind kind = toDate ? Kind.DATE : Kind.DATETIME;
        String target = toDate ? "DATE" : "TIMESTAMP";
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(kind).vs(), kind, true, dated(target, null));
        }
        if (x.kind() == Kind.DATE || x.kind() == Kind.DATETIME) {
            if (engine == IqDialect.Engine.IQ) {
                throw SqlTranslationException.invalid("CONVERT of " + kindName(x.kind()) + " to " + target + " with a"
                        + " style: the data type conversion is not possible under IQ's engine (CONVERT argument does"
                        + " not match the required data type)" + code(-1009145, "QFA2E"), f.pos());
            }
            return at.tx(new SqlAst.Cast(operand, new SqlAst.TypeName(IqParser.CONVERT + target, List.of(), f.pos()),
                    f.pos()));
        }
        if (x.kind() != Kind.STRING) {
            throw SqlTranslationException.unsupported("CONVERT of " + kindName(x.kind()) + " to " + target + " with a"
                    + " style is not supported yet", f.pos());
        }
        int style = written.bitLength() > 31 ? -1 : written.intValue();
        if (operand instanceof SqlAst.StringLit s) {
            String[] parts = styledDate(s.value(), style, settings.nearestCentury());
            if (parts == null) {
                throw SqlTranslationException.unsupported("CONVERT of the string '" + s.value() + "' to " + target
                        + " by style " + written + " is not supported: it is no date of that style the island reads"
                        + " (IQ reads some other spellings by rules of its own, and raises -157 for others)", f.pos());
            }
            IqType type = dated(target, Integer.parseInt(parts[0].substring(0, 4)));
            if (toDate) {
                return Tx.value(new CDate(PureDateLiteral.parse(parts[0]), parts[0], null), Kind.DATE, false, type);
            }
            String body = parts[0] + "T" + parts[1];
            return Tx.value(new CDate(PureDateLiteral.parse(body), body, null), Kind.DATETIME, false, type);
        }
        if (engine != IqDialect.Engine.IQ || !ODBC_STYLES.contains(style)) {
            throw SqlTranslationException.unsupported("CONVERT of a string the database computes to " + target + " by"
                    + " style " + written + " is not supported yet" + (engine == IqDialect.Engine.IQ ? ": the island"
                    + " reads styles 20, 21, 120 and 121 there" : " in SQL Anywhere's front end"), f.pos());
        }
        List<String[]> steps = canonicalSteps(true);
        Tx r = SqlToPure.strict(kind, List.of(x), v -> refusedPast(SqlToPure.call("not", SqlToPure.call("matches",
                v.get(0), new CString(ODBC_TEXT))), UNREAD_STYLE_RAISE, readDate(v.get(0), toDate, steps),
                raise(toDate ? "StrictDate" : "DateTime")));
        return Tx.value(r.vs(), kind, r.nullable(), dated(target, null));
    }

    /** The styles whose computed strings {@link #convertDateStyle} reads: yyyy-mm-dd and a time. */
    private static final java.util.Set<Integer> ODBC_STYLES = java.util.Set.of(20, 21, 120, 121);

    /** What {@link #convertDateStyle} reads of a computed string by an {@link #ODBC_STYLES} style. */
    private static final String ODBC_TEXT = "^[0-9]{4}-[0-9]{2}-[0-9]{2}( [0-9]{2}:[0-9]{2}(:[0-9]{2})?)?$";

    /** {@link #convertDateStyle}'s refusal of another spelling, where the database computes the string. */
    static final String UNREAD_STYLE_RAISE = IqDialect.REFUSED_AT_RUN_TIME + "CONVERT by an ODBC style (20, 21, 120,"
            + " 121) of a string other than yyyy-mm-dd and a time hh:mm[:ss] is not supported: IQ rejects most and reads"
            + " others by rules of its own";

    /** {@link #styledDate}'s ODBC spelling, {@code d} a digit: its date alone, with hh:nn, or with hh:nn:ss. */
    private static final String STYLED_ODBC = "dddd-dd-dd dd:dd:dd";

    /**
     * A constant string read as a date by a CONVERT style (IQ island, 2026-10-17, measured on IQ 17.1): its date
     * {@code yyyy-mm-dd} and time {@code hh:nn:ss}, or null where the island does not read it. 1, 101, 10 and 110 read
     * the month first, 3, 103, 4, 104, 5 and 105 the day, 2, 102, 11 and 111 the year: three numbers, the month's and
     * the day's of one or two digits, apart by one of /, - and . twice, blanks around. A style past 100 reads the year as
     * written, of one to four digits ({@code 13/01/012} is the year 12); one below 100 a year of two digits by
     * NEAREST_CENTURY ({@code 13/01/49} is 2049, {@code 13/01/50} 1950), four being IQ's -157. 12 reads {@code yymmdd},
     * 112 {@code yyyymmdd}; 20, 21, 120 and 121 {@code yyyy-mm-dd} and a time {@code hh:nn[:ss]} after a space. A date
     * that does not exist is IQ's -157, as is a time after the day-first styles' date: null, as is any other style.
     */
    static String @com.legend.base.Nullable [] styledDate(String text, int style, int nearest) {
        char order;
        boolean century = style >= 100 || style == 20 || style == 21;
        switch (style) {
            case 1, 10, 101, 110 -> order = 'M';
            case 3, 4, 5, 103, 104, 105 -> order = 'D';
            case 2, 11, 102, 111 -> order = 'Y';
            case 12, 112 -> order = 'C';
            case 20, 21, 120, 121 -> order = 'I';
            default -> {
                return null;
            }
        }
        String time = "00:00:00";
        String y;
        String m;
        String d;
        if (order == 'I') {
            int n = text.length();
            if (n != 10 && n != 16 && n != 19) {
                return null;
            }
            for (int i = 0; i < n; i++) {
                char c = text.charAt(i);
                if (STYLED_ODBC.charAt(i) == 'd' ? c < '0' || c > '9' : c != STYLED_ODBC.charAt(i)) {
                    return null;
                }
            }
            y = text.substring(0, 4);
            m = text.substring(5, 7);
            d = text.substring(8, 10);
            if (n > 10) {
                int hh = Integer.parseInt(text.substring(11, 13));
                int nn = Integer.parseInt(text.substring(14, 16));
                int ss = n == 19 ? Integer.parseInt(text.substring(17, 19)) : 0;
                if (hh > 23 || nn > 59 || ss > 59) {
                    return null;
                }
                time = text.substring(11, 16) + (n == 19 ? text.substring(16) : ":00");
            }
        } else if (order == 'C') {
            int digits = century ? 8 : 6;
            if (text.length() != digits || !text.chars().allMatch(c -> c >= '0' && c <= '9')) {
                return null;
            }
            y = text.substring(0, digits - 4);
            m = text.substring(digits - 4, digits - 2);
            d = text.substring(digits - 2);
        } else {
            // three numbers apart by one of / - . twice, blanks around
            int from = 0;
            int to = text.length();
            while (from < to && text.charAt(from) == ' ') {
                from++;
            }
            while (to > from && text.charAt(to - 1) == ' ') {
                to--;
            }
            String[] numbers = new String[3];
            char separator = 0;
            int i = from;
            for (int k = 0; k < 3; k++) {
                int start = i;
                while (i < to && text.charAt(i) >= '0' && text.charAt(i) <= '9') {
                    i++;
                }
                if (i == start) {
                    return null;
                }
                numbers[k] = text.substring(start, i);
                if (k < 2) {
                    char c = i < to ? text.charAt(i) : 0;
                    if ("/.-".indexOf(c) < 0 || c == 0 || separator != 0 && c != separator) {
                        return null;
                    }
                    separator = c;
                    i++;
                }
            }
            if (i != to) {
                return null;
            }
            y = order == 'Y' ? numbers[0] : numbers[2];
            m = order == 'M' ? numbers[0] : numbers[1];
            d = order == 'D' ? numbers[0] : order == 'M' ? numbers[1] : numbers[2];
        }
        if (m.length() > 2 || d.length() > 2 || (century ? y.length() > 4 : y.length() != 2)) {
            return null;
        }
        int year = Integer.parseInt(y);
        if (!century) {
            year += year < nearest ? 2000 : 1900;
        }
        int month = Integer.parseInt(m);
        int day = Integer.parseInt(d);
        if (year < 1 || month < 1 || month > 12 || day < 1 || day > IqDateTime.daysIn(year, month)) {
            return null;
        }
        return new String[] {String.format(Locale.ROOT, "%04d-%02d-%02d", year, month, day), time};
    }

    /** The months' abbreviations IQ's engine writes in CONVERT's styles (lower case, measured on IQ 17.1). */
    private static final List<String> MONTHS_SHORT = List.of("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug",
            "sep", "oct", "nov", "dec");

    /** The styles {@link #convertStyle} reads, every one IQ's engine writes a date or a timestamp in (measured on IQ
     *  17.1 from -1 to 400 and at 1000; {@link #engineRejects}). */
    private static final java.util.Set<Integer> STYLES = java.util.Set.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 20,
            21, 36, 37, 38, 39, 40, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 120, 121,
            136, 137, 138, 139, 140, 365);

    /** IQ island, 2026-10-17 (the corpus lane's style 126; each style from -1 to 400 and 1000 measured on IQ 17.1):
     *  the styles past which IQ's engine raises -1009145 for a date or a timestamp, as it plans -- every one but
     *  {@link #STYLES}; one beyond them is refused by name. */
    private static boolean engineRejects(int style) {
        return !STYLES.contains(style) && (style >= -1 && style <= 400 || style == 1000);
    }

    /**
     * {@code CONVERT(VARCHAR(n), d, style)} of a DATE or a TIMESTAMP under IQ's engine (IQ island, 2026-10-15; each
     * style measured on IQ 17.1): 1 mm/dd/yy (101 mm/dd/yyyy), 2 yy.mm.dd (102), 3 dd/mm/yy (103), 4 dd.mm.yy (104),
     * 5 dd-mm-yy (105), 10 mm-dd-yy (110), 11 yy/mm/dd (111), 12 yymmdd (112); 20 and 120 yyyy-mm-dd and a timestamp's
     * hh:nn:ss, 21 and 121 its hh:nn:ss.sss; 108 a timestamp's hh:nn:ss (a date's yyyy-mm-dd); 0 and 100 mon dd yyyy
     * and a timestamp's hh:nnAM (the hour of twelve), 106 dd mon yyyy, 107 mon dd, yyyy -- the month's abbreviation in
     * lower case; 2026-10-17, 6 dd mon yy, 7 mon dd, yy, 8 as 108, 9 and 109 mon dd yyyy hh:nn:ss:sssAM, 13 and 113
     * dd mon yyyy hh:nn:ss:sss, 14 and 114 hh:nn:ss:sss (a date's as 108's). A date shows no time in any style.
     * Refused by name: another style, another operand, a target too short for the text, SQL Anywhere's front end
     * (whose styles differ: its month names are capitalized).
     */
    private Tx convertStyle(SqlAst.FunctionCall f, List<Tx> args) {
        if (engine != IqDialect.Engine.IQ) {
            throw SqlTranslationException.unsupported("CONVERT with a style is supported under IQ's engine only yet:"
                    + " SQL Anywhere's front end writes its styles otherwise", f.pos());
        }
        Tx x = args.get(0);
        if (x.kind() == Kind.NULL) {
            return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, new IqType("VARCHAR"));
        }
        if (x.kind() != Kind.DATE && x.kind() != Kind.DATETIME) {
            throw SqlTranslationException.unsupported("CONVERT with a style of " + kindName(x.kind()) + " is not"
                    + " supported yet: only a date's or a timestamp's text is read", f.pos());
        }
        boolean ts = x.kind() == Kind.DATETIME;
        int style = ((SqlAst.IntegerLit) f.args().get(1)).value().intValue();
        int length = ((SqlAst.IntegerLit) f.args().get(2)).value().intValue();
        if (engineRejects(style)) {
            // held: IQ raises it after the statement's names (measured on IQ 17.1, WHERE nosuch = 1 its -143)
            dialect.hold(SqlTranslationException.invalid("CONVERT format style " + style + " is invalid for " + kindName(
                    x.kind()) + " under IQ's engine" + IqDialect.code(-1009145, "QFA2E"), f.args().get(1).pos()));
            return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, new IqType("VARCHAR"));
        }
        List<Object> spec = switch (style) {
            case 1 -> List.of("MM", "/", "DD", "/", "YY");
            case 101 -> List.of("MM", "/", "DD", "/", "YYYY");
            case 2 -> List.of("YY", ".", "MM", ".", "DD");
            case 102 -> List.of("YYYY", ".", "MM", ".", "DD");
            case 3 -> List.of("DD", "/", "MM", "/", "YY");
            case 103 -> List.of("DD", "/", "MM", "/", "YYYY");
            case 4 -> List.of("DD", ".", "MM", ".", "YY");
            case 104 -> List.of("DD", ".", "MM", ".", "YYYY");
            case 5 -> List.of("DD", "-", "MM", "-", "YY");
            case 105 -> List.of("DD", "-", "MM", "-", "YYYY");
            case 10 -> List.of("MM", "-", "DD", "-", "YY");
            case 110 -> List.of("MM", "-", "DD", "-", "YYYY");
            case 11 -> List.of("YY", "/", "MM", "/", "DD");
            case 111 -> List.of("YYYY", "/", "MM", "/", "DD");
            case 12 -> List.of("YY", "MM", "DD");
            case 112 -> List.of("YYYY", "MM", "DD");
            case 20, 120 -> ts ? List.of("YYYY", "-", "MM", "-", "DD", " ", "HH", ":", "NN", ":", "SS")
                    : List.of("YYYY", "-", "MM", "-", "DD");
            case 21, 121 -> ts ? List.of("YYYY", "-", "MM", "-", "DD", " ", "HH", ":", "NN", ":", "SS", ".", "SSS")
                    : List.of("YYYY", "-", "MM", "-", "DD");
            case 108 -> ts ? List.of("HH", ":", "NN", ":", "SS") : List.of("YYYY", "-", "MM", "-", "DD");
            case 0, 100 -> ts ? List.of("MON", " ", "DD", " ", "YYYY", " ", "H12", ":", "NN", "AMPM")
                    : List.of("MON", " ", "DD", " ", "YYYY");
            case 106 -> List.of("DD", " ", "MON", " ", "YYYY");
            case 107 -> List.of("MON", " ", "DD", ", ", "YYYY");
            // IQ island, 2026-10-17 (the corpus lane's style 113; measured on IQ 17.1): 6 dd mon yy, 7 mon dd, yy, 8 as
            // 108; 9 and 109 mon dd yyyy and a timestamp's hh:nn:ss:sss and AM or PM (the hour of twelve), 13 and 113 dd
            // mon yyyy and its hh:nn:ss:sss, 14 and 114 its hh:nn:ss:sss (a date's yyyy-mm-dd)
            case 6 -> List.of("DD", " ", "MON", " ", "YY");
            case 7 -> List.of("MON", " ", "DD", ", ", "YY");
            case 8 -> ts ? List.of("HH", ":", "NN", ":", "SS") : List.of("YYYY", "-", "MM", "-", "DD");
            case 9, 109 -> ts ? List.of("MON", " ", "DD", " ", "YYYY", " ", "H12", ":", "NN", ":", "SS", ":", "SSS",
                    "AMPM") : List.of("MON", " ", "DD", " ", "YYYY");
            case 13, 113 -> ts ? List.of("DD", " ", "MON", " ", "YYYY", " ", "HH", ":", "NN", ":", "SS", ":", "SSS")
                    : List.of("DD", " ", "MON", " ", "YYYY");
            case 14, 114 -> ts ? List.of("HH", ":", "NN", ":", "SS", ":", "SSS") : List.of("YYYY", "-", "MM", "-",
                    "DD");
            // IQ island, 2026-10-17 (measured on IQ 17.1): 36 and 136 a timestamp's hh:nn:ss.ssssss and AM or PM (the
            // hour of twelve), 37 and 137 its hh:nn:ss.ssssss (a date's yyyy-mm-dd); 38 mon dd yy and the first, 39 mon
            // dd yy and the second (138, 139 with yyyy); 40 yy-mm-dd and the second (140 with yyyy); 365 yyyyjjj, the
            // day of the year
            case 36, 136 -> ts ? List.of("H12", ":", "NN", ":", "SS", ".", "SSSSSS", "AMPM") : List.of("YYYY", "-",
                    "MM", "-", "DD");
            case 37, 137 -> ts ? List.of("HH", ":", "NN", ":", "SS", ".", "SSSSSS") : List.of("YYYY", "-", "MM", "-",
                    "DD");
            case 38, 138 -> concat(List.of("MON", " ", "DD", " ", style == 38 ? "YY" : "YYYY"), ts ? List.of(" ",
                    "H12", ":", "NN", ":", "SS", ".", "SSSSSS", "AMPM") : List.of());
            case 39, 139 -> concat(List.of("MON", " ", "DD", " ", style == 39 ? "YY" : "YYYY"), ts ? List.of(" ",
                    "HH", ":", "NN", ":", "SS", ".", "SSSSSS") : List.of());
            case 40, 140 -> concat(List.of(style == 40 ? "YY" : "YYYY", "-", "MM", "-", "DD"), ts ? List.of(" ", "HH",
                    ":", "NN", ":", "SS", ".", "SSSSSS") : List.of());
            case 365 -> List.of("YYYY", "JJJ");
            default -> throw SqlTranslationException.unsupported("CONVERT style " + style + " is not supported yet",
                    f.args().get(1).pos());
        };
        int width = 0;
        for (Object o : spec) {
            String piece = (String) o;
            width += switch (piece) {
                case "YYYY" -> 4;
                case "YY", "MM", "DD", "HH", "NN", "SS", "H12", "AMPM" -> 2;
                case "SSS", "MON", "JJJ" -> 3;
                case "SSSSSS" -> 6;
                default -> piece.length();
            };
        }
        if (length >= 0 && length < width) {
            // IQ island, 2026-10-17 (the corpus lane; measured on IQ 17.1): a style's text longer than the string it
            // is converted to is IQ's error for each row that computes it -- -1009145 under its engine (in a select
            // list, a WHERE, a GROUP BY, a ranked derived table alike; none over no rows), -638 in its front end; NULL
            // stays NULL
            String error = "the text of CONVERT style " + style + " has " + width + " characters, more than the"
                    + " string of " + length + " it is converted to: IQ's error (-1009145 under its engine, -638 in SQL"
                    + " Anywhere's front end)";
            Tx raised = SqlToPure.strict(Kind.STRING, List.of(x), v -> refusedPast(SqlToPure.call("isNotEmpty",
                    v.get(0)), error, new CString(""), new CString("")));
            return Tx.value(raised.vs(), Kind.STRING, raised.nullable(), new IqType("VARCHAR(" + length + ")"));
        }
        Tx r = SqlToPure.strict(Kind.STRING, List.of(x), v -> {
            ValueSpecification d = v.get(0);
            List<ValueSpecification> parts = new ArrayList<>();
            for (Object o : spec) {
                String piece = (String) o;
                parts.add(switch (piece) {
                    case "YYYY" -> lpad(SqlToPure.call("year", d), 4);
                    case "YY" -> lpad(SqlToPure.call("rem", SqlToPure.call("year", d), integer(100)), 2);
                    case "MM" -> lpad(SqlToPure.call("monthNumber", d), 2);
                    case "DD" -> lpad(SqlToPure.call("dayOfMonth", d), 2);
                    case "HH" -> lpad(SqlToPure.call("hour", d), 2);
                    case "NN" -> lpad(SqlToPure.call("minute", d), 2);
                    case "SS" -> lpad(SqlToPure.call("second", d), 2);
                    case "SSS" -> lpad(SqlToPure.call("floor", SqlToPure.call("divide", SqlToPure.call("mod",
                            AppliedFunction.infixRun("plus", List.of(SqlToPure.call("mod", epochMicros(d, false),
                                    integer(1_000_000L)), integer(1_000_000L))), integer(1_000_000L)),
                            integer(1000L))), 3);
                    case "SSSSSS" -> lpad(SqlToPure.call("mod", AppliedFunction.infixRun("plus", List.of(
                            SqlToPure.call("mod", epochMicros(d, false), integer(1_000_000L)), integer(1_000_000L))),
                            integer(1_000_000L)), 6);
                    case "JJJ" -> lpad(SqlToPure.call("dayOfYear", d), 3);
                    case "H12" -> lpad(ifThen(SqlToPure.call("equal", SqlToPure.call("rem", SqlToPure.call("hour",
                            d), integer(12)), integer(0)), integer(12), SqlToPure.call("rem", SqlToPure.call("hour",
                            d), integer(12))), 2);
                    case "AMPM" -> ifThen(lt(SqlToPure.call("hour", d), integer(12)), new CString("AM"),
                            new CString("PM"));
                    case "MON" -> monthShort(SqlToPure.call("monthNumber", d));
                    default -> new CString(piece);
                });
            }
            return AppliedFunction.infixRun("plus", parts);
        });
        return Tx.value(r.vs(), Kind.STRING, r.nullable(), new IqType("VARCHAR(" + width + ")"));
    }

    /** A style's pieces, {@code a}'s then {@code b}'s. */
    private static List<Object> concat(List<Object> a, List<Object> b) {
        List<Object> out = new ArrayList<>(a);
        out.addAll(b);
        return out;
    }

    /** {@code n}'s digits, zeros before them to {@code width}. */
    private static ValueSpecification lpad(ValueSpecification n, int width) {
        return SqlToPure.call("lpad", SqlToPure.call("toString", n), integer(width), new CString("0"));
    }

    /** The month numbered {@code m}'s abbreviation, as IQ's engine's styles write it. */
    private static ValueSpecification monthShort(ValueSpecification m) {
        ValueSpecification out = new CString(MONTHS_SHORT.get(11));
        for (int i = 10; i >= 0; i--) {
            out = ifThen(SqlToPure.call("equal", m, integer(i + 1)), new CString(MONTHS_SHORT.get(i)), out);
        }
        return out;
    }

    /**
     * {@code YEARS(a, b)}, {@code MONTHS}, {@code WEEKS}, {@code DAYS}, {@code HOURS}, {@code MINUTES},
     * {@code SECONDS} of two arguments (IQ island, 2026-10-15; measured on IQ 17.1): with a date or a timestamp
     * second, {@code DATEDIFF(part, a, b)}; with an integer, {@code DATEADD(part, b, a)} -- each as that function is
     * read ({@link #dateDiff}, {@link #dateAdd}). The forms of one argument (DAYS(d), a day number) are refused.
     */
    private Tx dateUnit(Fn fn, String name, SqlAst.FunctionCall f, List<Tx> args) {
        String part = switch (fn) {
            case YEARS -> "year";
            case MONTHS -> "month";
            case WEEKS -> "week";
            case DAYS -> "day";
            case HOURS -> "hour";
            case MINUTES -> "minute";
            case SECONDS -> "second";
            default -> throw new IllegalStateException("no date unit " + fn);
        };
        if (args.size() == 1) {
            if (engine != IqDialect.Engine.IQ) {
                throw SqlTranslationException.unsupported(name + " of one argument (the " + part + "s from SQL"
                        + " Anywhere's own epoch) is not supported in its front end yet", f.pos());
            }
            return sinceYearZero(fn, name, f, args.get(0));
        }
        SqlAst.StringLit unit = new SqlAst.StringLit(part, f.pos());
        Tx partTx = Tx.value(new CString(part), Kind.STRING, false, new IqType("VARCHAR(" + part.length() + ")"));
        Tx second = args.get(1);
        if (second.kind() == Kind.DATE || second.kind() == Kind.DATETIME) {
            SqlAst.FunctionCall diff = new SqlAst.FunctionCall(f.name(), List.of(unit, f.args().get(0),
                    f.args().get(1)), false, false, f.pos());
            return dateDiff(name, diff, List.of(partTx, args.get(0), args.get(1)));
        }
        if (second.kind() == Kind.INTEGER || second.kind() == Kind.NULL) {
            SqlAst.FunctionCall add = new SqlAst.FunctionCall(f.name(), List.of(unit, f.args().get(1),
                    f.args().get(0)), false, false, f.pos());
            return dateAdd(name, add, List.of(partTx, args.get(1), args.get(0)));
        }
        throw SqlTranslationException.unsupported(name + " of a " + kindName(second.kind()) + " second argument is not"
                + " supported yet: IQ converts it by its own rules", f.args().get(1).pos());
    }

    /** The days from 0000-01-01 (proleptic Gregorian) to 1970-01-01, IQ's engine's DAYS of 1970-01-01. */
    private static final long DAYS_TO_1970 = 719_528L;

    /**
     * IQ island, 2026-10-17 (the differential fuzzer's fifth round; measured on IQ 17.1): under IQ's engine {@code
     * DAYS(d)} is the days from 0000-01-01 in the proleptic Gregorian calendar (DAYS of 1970-01-01 is 719528, of
     * 0001-01-01 366), {@code WEEKS(d)} those days / 7 rounded down, {@code YEARS(d)} the year, {@code MONTHS(d)} year *
     * 12 + month - 1, all INTEGERs; {@code HOURS(t)}, {@code MINUTES(t)} and {@code SECONDS(t)} the days' hours,
     * minutes and seconds with the time's own, BIGINTs (a DATE's time is midnight). SQL Anywhere's front end counts
     * from another epoch (DAYS of 2020-01-15 is 737745 there, 737804 here), refused.
     */
    private Tx sinceYearZero(Fn fn, String name, SqlAst.FunctionCall f, Tx x) {
        Tx d = dateArgument(x, f.args().get(0), name);
        boolean big = fn == Fn.HOURS || fn == Fn.MINUTES || fn == Fn.SECONDS;
        IqType type = big ? IqType.integer("BIGINT", BigInteger.ZERO, IqType.BIGINT_MAX, true)
                : IqType.integer("INTEGER", BigInteger.ZERO, IqType.INTEGER_MAX, true);
        if (d == null) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, type);
        }
        boolean date = d.kind() == Kind.DATE;
        return strict(Kind.INTEGER, type, List.of(d), v -> {
            ValueSpecification t = v.get(0);
            ValueSpecification days = AppliedFunction.infixRun("plus", List.of(SqlToPure.call("dateDiff",
                    new CDate(PureDateLiteral.parse("1970-01-01"), "1970-01-01", null), day(t, date),
                    new EnumValue(DURATION_UNIT, "DAYS")), integer(DAYS_TO_1970)));
            ValueSpecification hours = date ? AppliedFunction.infixRun("times", List.of(days, integer(24)))
                    : AppliedFunction.infixRun("plus", List.of(AppliedFunction.infixRun("times", List.of(days,
                            integer(24))), SqlToPure.call("hour", t)));
            ValueSpecification minutes = date ? AppliedFunction.infixRun("times", List.of(hours, integer(60)))
                    : AppliedFunction.infixRun("plus", List.of(AppliedFunction.infixRun("times", List.of(hours,
                            integer(60))), SqlToPure.call("minute", t)));
            return switch (fn) {
                case DAYS -> days;
                case WEEKS -> truncatedQuotient(days, 7);
                case YEARS -> SqlToPure.call("year", t);
                case MONTHS -> minus(months(t, 12), integer(1));
                case HOURS -> hours;
                case MINUTES -> minutes;
                case SECONDS -> date ? AppliedFunction.infixRun("times", List.of(minutes, integer(60)))
                        : AppliedFunction.infixRun("plus", List.of(AppliedFunction.infixRun("times", List.of(minutes,
                                integer(60))), SqlToPure.call("second", t)));
                default -> throw new IllegalStateException("no date unit " + fn);
            };
        });
    }

    /**
     * {@code ISNUMERIC(s)} (IQ island, 2026-10-15; measured on IQ 17.1): 1 where s reads as a number -- blanks
     * around it, an optional sign, digits with an optional point (at least one digit), an optional exponent
     * ({@code ' 7 '}, {@code '+.5'}, {@code '-3e2'}) -- else 0 ({@code ''}, {@code '.'}, {@code '1,000'},
     * {@code '$5'}, and NULL), an INTEGER.
     */
    private Tx isNumeric(SqlAst.FunctionCall f, List<Tx> args) {
        IqType type = IqType.integer("INTEGER", BigInteger.ZERO, BigInteger.ONE, false);
        if (args.get(0).kind() == Kind.NULL) {
            return Tx.value(integer(0), Kind.INTEGER, false, type);
        }
        Tx s = text(args.get(0), f.args().get(0), "ISNUMERIC");
        ValueSpecification v = SqlToPure.valueOf(s);
        ValueSpecification matched = SqlToPure.call("matches", s.nullable() ? SqlToPure.call("toOne", v) : v,
                new CString("^ *[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)? *$"));
        ValueSpecification one = ifThen(matched, integer(1), integer(0));
        ValueSpecification value = s.nullable() ? ifThen(SqlToPure.call("isEmpty", v), integer(0), one) : one;
        return Tx.value(value, Kind.INTEGER, false, type);
    }

    // ------------------------------------------------------------------ strings

    /** {@code LENGTH(s)}, {@code CHAR_LENGTH(s)}, {@code LEN(s)}: its characters, trailing blanks counted (LEN is
     *  LENGTH in IQ: the research's D-S6), an INTEGER; an integer's digits (and sign) in SQL Anywhere's front end, its
     *  storage width under IQ's engine ({@link #iqStorageWidth}). */
    private Tx length(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx width = engine == IqDialect.Engine.IQ ? iqStorageWidth(name, f, args.get(0)) : null;
        if (width != null) {
            return width;
        }
        Tx s = text(args.get(0), f.args().get(0), name);
        return strict(Kind.INTEGER, nonNegative(), List.of(s), v -> SqlToPure.call("length", v.get(0)));
    }

    /**
     * IQ's engine's {@code LENGTH} and {@code BYTE_LENGTH} of a value that is no string (IQ island, 2026-10-15,
     * checklist 1.10, D-S7, measured on IQ 17.1): of an integer column its storage width -- a BIT's 1, a SMALLINT's 2,
     * an INTEGER's 4, a BIGINT's 8 -- where SQL Anywhere's front end counts its digits; null for a string or a NULL.
     * Refused by name: any other such value (a date's and a timestamp's are a storage length that varies with the
     * value: LENGTH of 2026-03-04 05:06:07.123456 is 6, of 1999-12-31 23:59:59.999999 3), an integer that is no column.
     */
    private @com.legend.base.Nullable Tx iqStorageWidth(String name, SqlAst.FunctionCall f, Tx x) {
        if (x.kind() == Kind.STRING || x.kind() == Kind.NULL) {
            return null;
        }
        SqlAst.Expr e = f.args().get(0);
        Integer width = x.kind() == Kind.INTEGER && e instanceof SqlAst.ColumnRef && x.detail() instanceof IqType t
                ? STORAGE_WIDTHS.get(t.family()) : null;
        if (width == null) {
            throw SqlTranslationException.unsupported(name + " of " + kindName(x.kind()) + (x.kind() == Kind.INTEGER
                    ? " that is no column" : "") + " is not supported under IQ's engine yet: it answers the value's"
                    + " storage length (an INTEGER column's 4, a timestamp's varying with the value)", e.pos());
        }
        BigInteger w = BigInteger.valueOf(width);
        long bytes = width;
        return strict(Kind.INTEGER, IqType.integer("INTEGER", w, w, false), List.of(x), v -> integer(bytes));
    }

    /** The integer types' storage widths, in bytes ({@link #iqStorageWidth}). */
    private static final Map<String, Integer> STORAGE_WIDTHS = Map.of("BIT", 1, "SMALLINT", 2, "INTEGER", 4, "BIGINT",
            8);

    /**
     * {@code SUBSTRING(s, start [, length])} (and {@code SUBSTR}) under ANSI_SUBSTRING On (every profile's): the
     * characters at positions start .. start + length - 1 that the string has, where positions below 1 are virtual
     * (measured: {@code ('mnopqrst', 0, 5)} is 'mnop', {@code (-3, 5)} 'm'); without a length, the length is the
     * largest INTEGER (measured: a start of -2147483641 keeps 'mnopq'). A negative length is IQ's error -1090 (for a
     * string that is not NULL); a NULL argument is NULL.
     */
    private Tx substring(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx s = text(args.get(0), f.args().get(0), name);
        Tx start = count(args.get(1), f.args().get(1), name);
        if (engine == IqDialect.Engine.IQ && settings.ansiSubstring()) {
            return substringIq(f, s, start, args.size() == 2 ? null : count(args.get(2), f.args().get(2), name));
        }
        if (!settings.ansiSubstring()) {
            return substringOff(s, start, args.size() == 2 ? null : count(args.get(2), f.args().get(2), name));
        }
        // without a length, SQL Anywhere's is the largest INTEGER (measured: from -2147483641 it is 'abcde')
        Tx len = args.size() == 2 ? Tx.value(integer(Integer.MAX_VALUE), Kind.INTEGER, false,
                IqType.integer("INTEGER", IqType.INTEGER_MAX, IqType.INTEGER_MAX, false))
                : count(args.get(2), f.args().get(2), name);
        boolean negative = len.detail() instanceof IqType t && t.lo().signum() < 0;
        return strict(Kind.STRING, varchar(), List.of(s, start, len), v -> {
            ValueSpecification p = v.get(1);
            ValueSpecification n = v.get(2);
            // a start of 1 or more: substring's own; below 1: the positions from 1 to start + length - 1
            ValueSpecification below = ifThen(lt(AppliedFunction.infixRun("plus", List.of(p, n)), integer(2)),
                    new CString(""), SqlToPure.call("substring", v.get(0), integer(1),
                            AppliedFunction.infixRun("minus", List.of(AppliedFunction.infixRun("plus", List.of(p, n)),
                                    integer(1)))));
            ValueSpecification value = ifThen(lt(p, integer(1)), below, SqlToPure.call("substring", v.get(0), p, n));
            return negative ? ifThen(lt(n, integer(0)), raise("String"), value) : value;
        });
    }

    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1; the census's Tier 1 string functions): {@code SUBSTRING(s, p [, n])}
     * under IQ's engine, which has no ANSI virtual positions before the first: the start is 1 for a p of 0 and L + p +
     * 1 (L the string's length in characters) for a negative one, and the positions from the start to start + n - 1
     * (to the end without n) that lie in 1 .. L are its text, none ''. A p beyond 32,767 either way, or an n beyond
     * 32,767, is NULL (tested first, so the positions' arithmetic stays small); a negative n is IQ's -1090.
     */
    private Tx substringIq(SqlAst.FunctionCall f, Tx s, Tx start, @com.legend.base.Nullable Tx len) {
        boolean negative = len != null && len.detail() instanceof IqType t && t.lo().signum() < 0;
        List<Tx> operands = len == null ? List.of(s, start) : List.of(s, start, len);
        Tx r = strict(Kind.STRING, varchar(), operands, v -> {
            ValueSpecification str = v.get(0);
            ValueSpecification p0 = v.get(1);
            ValueSpecification n = len == null ? null : v.get(2);
            ValueSpecification length = SqlToPure.call("length", str);
            ValueSpecification p = ifThen(gt(p0, integer(0)), p0, ifThen(lt(p0, integer(0)),
                    AppliedFunction.infixRun("plus", List.of(length, p0, integer(1))), integer(1)));
            ValueSpecification hi = n == null ? length : AppliedFunction.infixRun("minus",
                    List.of(AppliedFunction.infixRun("plus", List.of(p, n)), integer(1)));
            ValueSpecification first = ifThen(lt(p, integer(1)), integer(1), p);
            ValueSpecification last = ifThen(gt(hi, length), length, hi);
            ValueSpecification text = ifThen(lt(last, first), new CString(""), SqlToPure.call("substring", str, first,
                    AppliedFunction.infixRun("plus", List.of(AppliedFunction.infixRun("minus", List.of(last, first)),
                            integer(1)))));
            ValueSpecification far = SqlToPure.call("or", gt(p0, integer(32767)), lt(p0, integer(-32767)));
            if (n != null) {
                far = SqlToPure.call("or", far, gt(n, integer(32767)));
            }
            ValueSpecification value = ifThen(far, SqlToPure.nullOf(Kind.STRING).vs(), text);
            return negative ? ifThen(lt(java.util.Objects.requireNonNull(n), integer(0)), raise("String"), value)
                    : value;
        });
        return Tx.value(r.vs(), Kind.STRING, true, varchar());
    }

    /**
     * {@code SUBSTRING(s, start [, length])} under ANSI_SUBSTRING Off (IQ island, 2026-10-14, the options framework;
     * measured on SQL Anywhere 17 over every start from -10 to 10 and length from -3 to 10, alike in both kinds of
     * database): the start p is {@code start} when positive, counted from the end when negative ({@code L + start +
     * 1}, L the string's length), and 0 the first position (the last for a negative length); a positive length takes
     * positions p .. p + length - 1, a negative one the |length| positions ending at p, 0 none, and no length the
     * positions from p to the end; positions outside 1 .. L are none. A NULL argument is NULL; no error.
     */
    private Tx substringOff(Tx s, Tx start, @com.legend.base.Nullable Tx len) {
        List<Tx> operands = len == null ? List.of(s, start) : List.of(s, start, len);
        return strict(Kind.STRING, varchar(), operands, v -> {
            ValueSpecification str = v.get(0);
            ValueSpecification p0 = v.get(1);
            ValueSpecification length = SqlToPure.call("length", str);
            ValueSpecification fromEnd = AppliedFunction.infixRun("plus", List.of(length, p0, integer(1)));
            ValueSpecification n = len == null ? null : v.get(2);
            ValueSpecification p = ifThen(gt(p0, integer(0)), p0, ifThen(lt(p0, integer(0)), fromEnd,
                    n == null ? integer(1) : ifThen(lt(n, integer(0)), length, integer(1))));
            ValueSpecification lo = n == null ? p : ifThen(lt(n, integer(0)),
                    AppliedFunction.infixRun("plus", List.of(p, n, integer(1))), p);
            ValueSpecification hi = n == null ? length : ifThen(lt(n, integer(0)), p,
                    AppliedFunction.infixRun("minus", List.of(AppliedFunction.infixRun("plus", List.of(p, n)),
                            integer(1))));
            ValueSpecification first = ifThen(lt(lo, integer(1)), integer(1), lo);
            ValueSpecification last = ifThen(gt(hi, length), length, hi);
            return ifThen(lt(last, first), new CString(""), SqlToPure.call("substring", str, first,
                    AppliedFunction.infixRun("plus", List.of(AppliedFunction.infixRun("minus", List.of(last, first)),
                            integer(1)))));
        });
    }

    private static ValueSpecification gt(ValueSpecification a, ValueSpecification b) {
        return SqlToPure.call("greaterThan", a, b);
    }

    /** {@code x}, or 1 when it is below 1. */
    private static ValueSpecification atLeastOne(ValueSpecification x) {
        return ifThen(lt(x, integer(1)), integer(1), x);
    }

    /** {@code UPPER(s)}, {@code UCASE(s)}, {@code LOWER(s)}, {@code LCASE(s)}: s with its ASCII letters A-Z (a-z) in
     *  the other case and every other character as it is (measured in both kinds of database: UPPER('é') is 'é',
     *  and a Greek or Cyrillic letter stays as it is), of the argument's type ({@link #asciiFold}). */
    private Tx foldCase(boolean upper, String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx s = text(args.get(0), f.args().get(0), name);
        IqType type = s.detail() instanceof IqType t && (t.family().equals("CHAR") || t.family().equals("VARCHAR"))
                ? t : varchar();
        return strict(Kind.STRING, type, List.of(s), v -> asciiFold(v.get(0), upper));
    }

    /**
     * IQ's case fold: {@code s} with the 26 ASCII letters in upper case ({@code upper}) or lower case, every other
     * character as it is -- IQ folds A-Z only, in an ISO-8859-1 and a UTF-8 database alike (the research's SEMANTICS
     * §3.3; measured). Composed of 26 of Pure's {@code replace}, which both databases compute character for character
     * (measured over Latin-1 and UTF-8 text): Pure's {@code toUpper} and {@code toLower} are the databases' own
     * {@code upper} and {@code lower}, and DuckDB's fold every letter. A constant with no letter to fold is itself.
     */
    static ValueSpecification asciiFold(ValueSpecification s, boolean upper) {
        char first = upper ? 'a' : 'A';
        if (s instanceof CString c) {
            boolean any = false;
            for (int i = 0; i < c.value().length() && !any; i++) {
                char ch = c.value().charAt(i);
                any = ch >= first && ch <= first + 25;
            }
            if (!any) {
                return s;
            }
        }
        ValueSpecification x = s;
        for (int i = 0; i < 26; i++) {
            x = SqlToPure.call("replace", x, new CString(String.valueOf((char) (first + i))),
                    new CString(String.valueOf((char) (first + i + (upper ? -32 : 32)))));
        }
        return x;
    }

    /** IQ's blank padding where it orders strings: {@code s} without its trailing spaces -- U+0020 only (measured: a
     *  tab or a no-break space at the end is a character, and IQ compares as if the trailing spaces were not there,
     *  not as if the shorter string were padded: {@code 'a' < 'a\t'}). One anchored {@code regexpReplace}, which both
     *  databases compute alike, where Pure's {@code rtrim} is DuckDB's (every Unicode space) and Postgres's. */
    static ValueSpecification trailingSpacesDropped(ValueSpecification s) {
        return SqlToPure.call("regexpReplace", s, new CString(" +$"), new CString(""),
                new com.legend.protocol.spec.CBoolean(false));
    }

    /** {@code LEFT(s, n)}, {@code RIGHT(s, n)}: the first or last n characters, all of them when n is more; '' for a
     *  negative n (measured: IQ's, where Pure's left leaves that many off the other end). */
    private Tx leftRight(boolean left, String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx s = text(args.get(0), f.args().get(0), name);
        Tx n = count(args.get(1), f.args().get(1), name);
        return strict(Kind.STRING, varchar(), List.of(s, n), v -> SqlToPure.call(left ? "left" : "right", v.get(0),
                ifThen(lt(v.get(1), integer(0)), integer(0), v.get(1))));
    }

    /** The characters an ISO-8859-1 database's TRIM drops (measured over the 255: the space, and tab through carriage
     *  return), as a regular expression's alternatives: the line feed apart, since Postgres's lowering reads a bracket
     *  expression newline-sensitively. */
    private static final String BLANKS = "(\\n|[\\t-\\r ])";

    /**
     * {@code TRIM(s)}, {@code LTRIM(s)}, {@code RTRIM(s)}: the leading and/or trailing characters an ISO-8859-1
     * database trims -- the space, the tab, the line feed, the vertical tab, the form feed, the carriage return, and no
     * other (measured; DuckDB's trim drops Unicode spaces and no tab, Postgres's the space alone, so neither is used);
     * a {@code sqla17} profile's UTF-8 database trims the same six and no Unicode space (IQ island, 2026-10-14
     * (natives): measured over U+0085, U+00A0, U+1680, U+180E, U+2000-U+200B, U+2028, U+2029, U+202F, U+205F,
     * U+3000, U+FEFF). {@code TRIM(s, set)}: the characters of a constant set, case-sensitively in an IQ database
     * and without regard to the ASCII letters' case in a {@code sqla17} profile's (the research's SEMANTICS §3.3).
     * Refused by name: a set that is not a constant.
     */
    private Tx trim(Fn fn, String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx s = text(args.get(0), f.args().get(0), name);
        String chars;
        if (args.size() == 2) {
            if (args.get(1).kind() == Kind.NULL) {
                return nullString();
            }
            if (!(f.args().get(1) instanceof SqlAst.StringLit set)) {
                throw SqlTranslationException.unsupported(name + " with a set of characters that is not a constant"
                        + " string is not supported yet", f.args().get(1).pos());
            }
            if (set.value().isEmpty()) {
                return strict(Kind.STRING, varchar(), List.of(s), v -> v.get(0));
            }
            chars = setClass(set.value(), !settings.caseSensitive());
        } else {
            chars = BLANKS;
        }
        boolean leading = fn != Fn.RTRIM;
        boolean trailing = fn != Fn.LTRIM;
        String run = chars + "+";
        return strict(Kind.STRING, varchar(), List.of(s), v -> {
            ValueSpecification x = v.get(0);
            if (trailing) {
                x = SqlToPure.call("regexpReplace", x, new CString(run + "$"), new CString(""),
                        new com.legend.protocol.spec.CBoolean(false));
            }
            if (leading) {
                x = SqlToPure.call("regexpReplace", x, new CString("^" + run), new CString(""),
                        new com.legend.protocol.spec.CBoolean(false));
            }
            return x;
        });
    }

    /** A set of characters as a regular expression's alternatives: each character itself, escaped, and with
     *  {@code bothCases} an ASCII letter's other case too; a line feed apart (above). */
    private static String setClass(String set, boolean bothCases) {
        StringBuilder b = new StringBuilder("([");
        boolean newline = false;
        for (int i = 0; i < set.length(); i++) {
            char c = set.charAt(i);
            if (bothCases && asciiLetter(c)) {
                b.append(c).append((char) (c ^ 0x20));
            } else if (c == '\n') {
                newline = true;
            } else if (c < 0x20 || c == 0x7F) {
                String h = Integer.toHexString(c);
                b.append("\\x").append(h.length() == 1 ? "0" : "").append(h);
            } else {
                if ("\\^]-[".indexOf(c) >= 0) {
                    b.append('\\');
                }
                b.append(c);
            }
        }
        b.append("])");
        if (newline) {
            return b.length() == 4 ? "(\\n)" : "(\\n|" + b.substring(1);
        }
        return b.toString();
    }

    /** The longest search string REPLACE, CHARINDEX and LOCATE take: with one of 255 characters or more SQL Anywhere
     *  answers NULL (measured; IQ's manual says above 255). */
    private static final int LONGEST_SEARCH = 254;

    /** {@code REPLACE(s, search, replacement)}: every occurrence, left to right, case-sensitively (an IQ database's);
     *  '' searched is no change; a search of 255 characters or more NULL; a NULL argument NULL. In a {@code sqla17}
     *  profile's database the search matches without regard to the ASCII letters' case (measured: REPLACE('aAbBa',
     *  'a', 'x') is 'xxbBx', REPLACE('éÉe', 'é', 'x') is 'xÉe'): a global {@code regexpReplace} whose pattern spells
     *  each letter of a constant search as a class of its two cases (IQ island, 2026-10-14, natives); refused by name
     *  there: a search with a letter that is no constant, a replacement that is no constant. */
    /** A string type as IQ's engine reads it for {@link #replace}: a CHAR's ({@code fixed}) or a VARCHAR's, of
     *  {@code width} bytes (null where not known). */
    private record Text(boolean fixed, @com.legend.base.Nullable Integer width) {
    }

    /**
     * The type IQ's engine gives string {@code e} (measured on IQ 17.1, 2026-10-17: {@link #replace}), null where not
     * known: a constant a VARCHAR of its length; a column its declared type; a CAST its type; LEFT, RIGHT and SUBSTRING
     * of a CHAR the CHAR, of a VARCHAR a VARCHAR of the lesser of its width and a constant length (0 for a length of
     * 0); the TRIMs, UPPER and LOWER a VARCHAR of their string's width; SPACE and REPEAT by their constant counts;
     * COALESCE, ISNULL, IF and CASE their widest; {@code ||} and STRING their sum.
     */
    private static @com.legend.base.Nullable Text text(SqlAst.Expr e, SqlToPure.At at) {
        return switch (e) {
            case SqlAst.StringLit l -> new Text(false, l.value().getBytes(java.nio.charset.StandardCharsets.UTF_8)
                    .length);
            case SqlAst.ColumnRef r -> at.tx(r).detail() instanceof IqType type && type.size() >= 0
                    && (type.family().equals("CHAR") || type.family().equals("VARCHAR"))
                    ? new Text(type.family().equals("CHAR"), type.size()) : null;
            case SqlAst.Cast c when !c.type().modifiers().isEmpty() && (c.type().name().equalsIgnoreCase("char")
                    || c.type().name().equalsIgnoreCase("varchar")) ->
                    new Text(c.type().name().equalsIgnoreCase("char"), c.type().modifiers().get(0));
            case SqlAst.Binary b when b.op() == SqlAst.BinaryOp.CONCAT -> sum(List.of(b.left(), b.right()), at);
            case SqlAst.Case c -> {
                List<SqlAst.Expr> branches = new ArrayList<>();
                c.whens().forEach(w -> branches.add(w.result()));
                if (c.otherwise() != null) {
                    branches.add(java.util.Objects.requireNonNull(c.otherwise()));
                }
                yield widest(branches, at);
            }
            case SqlAst.FunctionCall f when f.name().size() == 1 -> {
                String fn = f.name().get(0).text().toLowerCase(Locale.ROOT);
                List<SqlAst.Expr> a = f.args();
                yield switch (fn) {
                    case "left", "right" -> a.size() == 2 ? cut(text(a.get(0), at), count(a.get(1))) : null;
                    case "substring", "substr" -> a.size() == 3 ? cut(text(a.get(0), at), count(a.get(2)))
                            : a.size() == 2 ? text(a.get(0), at) : null;
                    case "space" -> a.size() == 1 && count(a.get(0)) != null ? new Text(false, count(a.get(0))) : null;
                    case "repeat", "replicate" -> {
                        Integer n = a.size() == 2 ? count(a.get(1)) : null;
                        Text x = a.size() == 2 ? text(a.get(0), at) : null;
                        yield n == null ? null : n == 0 ? new Text(false, 0) : x == null || x.width() == null ? null
                                : new Text(false, x.width() * n);
                    }
                    case "trim", "ltrim", "rtrim", "upper", "lower", "ucase", "lcase" -> {
                        Text x = a.size() == 1 ? text(a.get(0), at) : null;
                        yield x == null ? null : new Text(false, x.width());
                    }
                    case "coalesce", "isnull", "ifnull" -> widest(a, at);
                    case "string" -> sum(a, at);
                    default -> null;
                };
            }
            default -> null;
        };
    }

    private static @com.legend.base.Nullable Integer count(SqlAst.Expr e) {
        return e instanceof SqlAst.IntegerLit n && n.value().signum() >= 0 && n.value().bitLength() < Integer.SIZE
                ? n.value().intValue() : null;
    }

    /** LEFT's, RIGHT's or SUBSTRING's of {@code x} to {@code n} characters (null when not constant). */
    private static @com.legend.base.Nullable Text cut(@com.legend.base.Nullable Text x,
            @com.legend.base.Nullable Integer n) {
        if (x == null || x.fixed()) {
            return x;
        }
        Integer w = x.width();
        return new Text(false, n == null ? w : n == 0 ? Integer.valueOf(0) : w == null ? null : Math.min(w, n));
    }

    private static @com.legend.base.Nullable Text widest(List<SqlAst.Expr> es, SqlToPure.At at) {
        int most = 0;
        for (SqlAst.Expr x : es) {
            Text t = text(x, at);
            if (t == null || t.width() == null) {
                return null;
            }
            most = Math.max(most, java.util.Objects.requireNonNull(t.width()));
        }
        return es.isEmpty() ? null : new Text(false, most);
    }

    private static @com.legend.base.Nullable Text sum(List<SqlAst.Expr> es, SqlToPure.At at) {
        int total = 0;
        for (SqlAst.Expr x : es) {
            Text t = text(x, at);
            if (t == null || t.width() == null) {
                return null;
            }
            total += java.util.Objects.requireNonNull(t.width());
        }
        return es.isEmpty() ? null : new Text(false, total);
    }

    private Tx replace(String name, SqlAst.FunctionCall f, List<Tx> args, SqlToPure.At at) {
        Tx s = text(args.get(0), f.args().get(0), name);
        Tx search = text(args.get(1), f.args().get(1), name);
        Tx replacement = text(args.get(2), f.args().get(2), name);
        if (!settings.caseSensitive() && !caseFree(f.args().get(1)) && search.kind() != Kind.NULL
                && replacement.kind() != Kind.NULL) {
            if (!(f.args().get(1) instanceof SqlAst.StringLit sought) || sought.value().length() > LONGEST_SEARCH) {
                throw SqlTranslationException.unsupported(name + " in profile " + settings.spelling() + "'s"
                        + " case-insensitive database with a search that is no constant string (of at most "
                        + LONGEST_SEARCH + " characters) is not supported yet", f.args().get(1).pos());
            }
            String with = constantText(f.args().get(2));
            if (with == null) {
                throw SqlTranslationException.unsupported(name + " in profile " + settings.spelling() + "'s"
                        + " case-insensitive database with a replacement that is no constant is not supported yet",
                        f.args().get(2).pos());
            }
            String pattern = caselessPattern(sought.value());
            String rewrite = with.replace("\\", "\\\\");
            return strict(Kind.STRING, varchar(), List.of(s, search, replacement), v -> SqlToPure.call(
                    "regexpReplace", v.get(0), new CString(pattern), new CString(rewrite),
                    new com.legend.protocol.spec.CBoolean(true)));
        }
        Text typed = engine == IqDialect.Engine.IQ ? text(f.args().get(0), at) : null;
        if (typed != null && Integer.valueOf(0).equals(typed.width())) {
            // IQ island, 2026-10-17 (the differential fuzzer's REPLACE('', 'a', ''), and its fifth round's LEFT(ch, 0);
            // measured on IQ 17.1): IQ's engine answers NULL for a string it types with no width ('', LEFT(vc, 0),
            // SPACE(0), TRIM(''), a derived table's '' column), whatever it replaces; '' for a wider one (CAST('' AS
            // VARCHAR(1)), LEFT(vc, id - 1), LEFT(ch, 0) of a CHAR(5), which keeps its width)
            return nullString();
        }
        boolean guard = !shortConstant(f.args().get(1));
        Tx r = strict(Kind.STRING, varchar(), List.of(s, search, replacement), v -> {
            ValueSpecification value = SqlToPure.call("replace", v.get(0), v.get(1), v.get(2));
            return guard ? ifThen(tooLong(v.get(1)), SqlToPure.empty(), value) : value;
        });
        return guard ? Tx.value(r.vs(), Kind.STRING, true, r.detail()) : r;
    }

    /**
     * IQ island, 2026-10-17 (the corpus lane's 38; measured on IQ 17.1): {@code LPAD(s, n [, pad])} and RPAD, IQ's
     * engine's alone (SQL Anywhere's front end raises -134, not implemented): s padded on the left (the right) to n
     * characters with the pad repeated (a space without one), or cut to its first n; '' for an n below 1; NULL for a
     * NULL s or n. Refused by name: a pad that is no constant non-empty string (IQ reads a NULL or '' one as none),
     * and a constant n past 32767 (IQ's -1001005: no string is longer).
     */
    private Tx pad(boolean left, String name, SqlAst.FunctionCall f, List<Tx> args) {
        if (engine != IqDialect.Engine.IQ) {
            throw SqlTranslationException.invalid("Feature '" + name.toLowerCase(Locale.ROOT) + "' not implemented: "
                    + name + " is IQ's engine's alone" + code(-134, "0A000"), f.pos());
        }
        Tx s = text(args.get(0), f.args().get(0), name);
        Tx n = count(args.get(1), f.args().get(1), name);
        String fill = " ";
        if (args.size() == 3) {
            if (!(f.args().get(2) instanceof SqlAst.StringLit lit) || lit.value().isEmpty()) {
                throw SqlTranslationException.unsupported(name + " with a pad that is no constant non-empty string is"
                        + " not supported yet (IQ reads a NULL or an empty one as none)", f.args().get(2).pos());
            }
            fill = lit.value();
        }
        if (f.args().get(1) instanceof SqlAst.IntegerLit width && width.value().compareTo(BigInteger.valueOf(32767)) > 0) {
            throw SqlTranslationException.unsupported(name + " to more than 32767 characters is IQ's error -1001005 (a"
                    + " string length error) where a row computes it, which is not supported", f.args().get(1).pos());
        }
        String padding = fill;
        return strict(Kind.STRING, varchar(), List.of(s, n), v -> ifThen(lt(v.get(1), integer(1)), new CString(""),
                SqlToPure.call(left ? "lpad" : "rpad", v.get(0), v.get(1), new CString(padding))));
    }

    /** Whether {@code c} is one of the 26 ASCII letters, either case. */
    private static boolean asciiLetter(char c) {
        return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z';
    }

    /** Whether {@code e} is a constant whose matching no case can change: a number, or a string without an ASCII
     *  letter. */
    private static boolean caseFree(SqlAst.Expr e) {
        if (e instanceof SqlAst.StringLit lit) {
            for (int i = 0; i < lit.value().length(); i++) {
                if (asciiLetter(lit.value().charAt(i))) {
                    return false;
                }
            }
            return true;
        }
        return e instanceof SqlAst.IntegerLit || e instanceof SqlAst.NullLit;
    }

    /** A constant's text as IQ reads it as a string: a string's own, an integer's digits; null for anything else. */
    private static @com.legend.base.Nullable String constantText(SqlAst.Expr e) {
        return e instanceof SqlAst.StringLit lit ? lit.value() : e instanceof SqlAst.IntegerLit n
                ? n.value().toString() : null;
    }

    /** A constant string as a regular expression matching it without regard to its ASCII letters' case: each letter a
     *  class of its two cases, every other character itself (escaped where a pattern reads it otherwise; a character
     *  outside ASCII is itself, which IQ does not fold either). */
    static String caselessPattern(String text) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (asciiLetter(c)) {
                b.append('[').append((char) (c | 0x20)).append((char) (c & ~0x20)).append(']');
            } else {
                if ("\\.[]{}()*+?^$|".indexOf(c) >= 0) {
                    b.append('\\');
                }
                b.append(c);
            }
        }
        return b.toString();
    }

    /** Whether {@code e} is a constant string IQ searches for (at most {@link #LONGEST_SEARCH} characters). */
    private static boolean shortConstant(SqlAst.Expr e) {
        return e instanceof SqlAst.StringLit s && s.value().length() <= LONGEST_SEARCH;
    }

    private static ValueSpecification tooLong(ValueSpecification search) {
        return SqlToPure.call("greaterThan", SqlToPure.call("length", search), integer(LONGEST_SEARCH));
    }

    /** {@code REPEAT(s, n)}, {@code REPLICATE(s, n)}, {@code SPACE(n)}: n copies; '' for n of 0 or less (measured: not
     *  NULL, as T-SQL's REPLICATE). */
    private Tx repeat(String name, SqlAst.FunctionCall f, Tx s, SqlAst.Expr se, Tx n, SqlAst.Expr ne) {
        Tx text = text(s, se, name);
        Tx times = count(n, ne, name);
        // repeatString is of a String[0..1]: here of a value, so one
        return strict(Kind.STRING, varchar(), List.of(text, times), v -> SqlToPure.call("toOne",
                SqlToPure.call("repeatString", v.get(0), v.get(1))));
    }

    /**
     * {@code CHARINDEX(search, s)} and {@code LOCATE(s, search [, start])}: where search first occurs in s, from 1, or
     * 0; '' is found at 1 (wherever the start); a search of 255 characters or more is NULL. LOCATE's start: from there
     * on when it is positive (0 reads as 1); when negative, the last occurrence in s without its last -start - 1
     * characters (measured). Case-sensitively in an IQ database; without regard to the ASCII letters' case in a
     * {@code sqla17} profile's ({@link #asciiFold}).
     */
    private Tx locate(String name, SqlAst.FunctionCall f, Tx s, SqlAst.Expr se, Tx search, SqlAst.Expr searchExpr,
            @com.legend.base.Nullable Tx start, SqlAst.@com.legend.base.Nullable Expr startExpr) {
        Tx text = text(s, se, name);
        Tx sought = text(search, searchExpr, name);
        boolean guard = !shortConstant(searchExpr);
        List<Tx> operands = new ArrayList<>(List.of(text, sought));
        Tx from = start == null ? null : count(start, java.util.Objects.requireNonNull(startExpr), name);
        if (from != null) {
            operands.add(from);
        }
        // IQ island, 2026-10-14 (natives): a sqla17 profile's database finds the search without regard to the ASCII
        // letters' case (measured: LOCATE('xAbc', 'abc') is 2): both folded, which moves no position
        boolean fold = !settings.caseSensitive();
        Tx r = strict(Kind.INTEGER, nonNegative(), operands, v -> {
            ValueSpecification str = fold ? asciiFold(v.get(0), false) : v.get(0);
            ValueSpecification find = fold ? asciiFold(v.get(1), false) : v.get(1);
            ValueSpecification value;
            if (from == null) {
                value = SqlToPure.call("indexOf", str, find);
            } else {
                ValueSpecification p = v.get(2);
                // from start on: where it occurs in the rest, as a position of s
                ValueSpecification at = atLeastOne(p);
                ValueSpecification rest = SqlToPure.call("indexOf", SqlToPure.call("substring", str, at), find);
                ValueSpecification forward = ifThen(SqlToPure.call("equal", rest, integer(0)), integer(0),
                        AppliedFunction.infixRun("minus", List.of(AppliedFunction.infixRun("plus", List.of(rest, at)),
                                integer(1))));
                // the last occurrence in s less its last -start - 1 characters: the first of the reversed search in
                // the reversed prefix, counted back
                ValueSpecification keep = AppliedFunction.infixRun("plus", List.of(SqlToPure.call("length", str), p,
                        integer(1)));
                ValueSpecification prefix = SqlToPure.call("left", str, ifThen(lt(keep, integer(0)), integer(0),
                        keep));
                ValueSpecification j = SqlToPure.call("indexOf", SqlToPure.call("reverseString", prefix),
                        SqlToPure.call("reverseString", find));
                ValueSpecification backward = ifThen(SqlToPure.call("equal", j, integer(0)), integer(0),
                        AppliedFunction.infixRun("plus", List.of(AppliedFunction.infixRun("minus", List.of(
                                SqlToPure.call("length", prefix), j, SqlToPure.call("length", find))), integer(2))));
                ValueSpecification found = ifThen(lt(p, integer(0)), backward, forward);
                value = ifThen(SqlToPure.call("equal", find, new CString("")), integer(1), found);
            }
            return guard ? ifThen(tooLong(find), SqlToPure.empty(), value) : value;
        });
        return guard ? Tx.value(r.vs(), Kind.INTEGER, true, r.detail()) : r;
    }

    /**
     * {@code PATINDEX} of a pattern with a {@code _}, a {@code [set]} or a {@code %} inside its run. IQ island,
     * 2026-10-17 (the corpus lane: {@code '%[^a-z]%'}, {@code '%[ :,.]%'}, {@code '[^?.]'}; measured on IQ 17.1, its
     * engine's and its front end's alike): a pattern that does not start with {@code %} is LIKE's test, 1 or 0; one
     * that does is the first place from which the rest of the pattern matches the rest of s, 0 where none does
     * ({@code PATINDEX('%a%b', 'xaxbxb')} is 2, {@code PATINDEX('%[^?.]', 'ab?')} 0) -- a regular expression's first
     * match, held to s's end where the pattern does not end with {@code %}, and a {@code _} a line feed too. The
     * pattern's parts are LIKE's ({@link IqDialect#likeParts}, no escape): an empty set or one that descends (IQ finds
     * nothing) is refused as LIKE's is, and so is a set in a case-insensitive database.
     */
    private Tx patternIndex(String name, SqlAst.StringLit lit, SqlAst.FunctionCall f, Tx s, IqType type) {
        List<String> parts = IqDialect.likeParts(lit, null, true, settings.latin1());
        boolean caseless = !settings.caseSensitive();
        for (int i = 1; caseless && i < parts.size(); i += 2) {
            if (parts.get(i).startsWith("[")) {
                throw SqlTranslationException.unsupported(name + "'s [set] classes in a case-insensitive database are"
                        + " not supported yet", lit.pos());
            }
        }
        Tx str = text(s, f.args().get(1), name);
        if (str.kind() == Kind.NULL) {
            return Tx.value(integer(0), Kind.INTEGER, false, type);
        }
        ValueSpecification value = SqlToPure.valueOf(str);
        ValueSpecification x = str.nullable() ? SqlToPure.call("toOne", value) : value;
        ValueSpecification found;
        if (!parts.get(0).isEmpty() || !parts.get(1).equals("%")) {
            found = ifThen(IqDialect.likeTest(x, parts, caseless), integer(1), integer(0));
        } else {
            int first = 0;
            while (parts.get(first).isEmpty() && parts.get(first + 1).equals("%")) {
                first += 2;
            }
            int last = parts.size() - 1;
            boolean open = false;
            while (last - 1 > first && parts.get(last).isEmpty() && parts.get(last - 1).equals("%")) {
                last -= 2;
                open = true;
            }
            StringBuilder regex = new StringBuilder();
            for (int i = first; i <= last; i++) {
                String part = parts.get(i);
                if (i % 2 == 1) {
                    regex.append(part.equals("%") ? "(?:.|\\n)*" : part.equals("_") ? "(?:.|\\n)" : part);
                } else if (caseless) {
                    regex.append(caselessPattern(part));
                } else {
                    for (int k = 0; k < part.length(); k++) {
                        char c = part.charAt(k);
                        if ("\\.[]{}()*+?^$|".indexOf(c) >= 0) {
                            regex.append('\\');
                        }
                        regex.append(c);
                    }
                }
            }
            if (!open) {
                regex.append('$');
            }
            found = AppliedFunction.infixRun("plus", List.of(SqlToPure.call("regexpIndexOf", x,
                    new CString(regex.toString())), integer(1)));
        }
        ValueSpecification result = str.nullable() ? ifThen(SqlToPure.call("isEmpty", value), integer(0), found)
                : found;
        return Tx.value(result, Kind.INTEGER, false, type);
    }

    /**
     * {@code PATINDEX(pattern, s)} of a constant pattern of one literal run between optional %s: where the run first
     * occurs ({@code '%lit%'}), 1 or 0 for {@code 'lit%'}, where it ends s for {@code '%lit'}, 1 or 0 for {@code 'lit'};
     * 0 when either argument is NULL (measured: not NULL). A pattern of %s alone (or '') is 0 in SQL Anywhere and 1 in
     * IQ's manual: the IQ engine's is the research's D-F6, refused. A {@code _}, a {@code [set]} or a % inside the run
     * is read by {@link #patternIndex}. Refused by name: a pattern that is no constant, one over 126 characters (SQL
     * Anywhere's -606, IQ's NULL: D-F7). A {@code sqla17} profile's database matches without regard to the ASCII letters' case
     * ({@link #asciiFold}).
     */
    private Tx patindex(String name, SqlAst.FunctionCall f, List<Tx> args) {
        SqlAst.Expr pe = f.args().get(0);
        Tx s = args.get(1);
        IqType type = IqType.integer("INTEGER", BigInteger.ZERO, IqType.INTEGER_MAX, false);
        if (pe instanceof SqlAst.NullLit) {
            return Tx.value(integer(0), Kind.INTEGER, false, type);
        }
        if (!(pe instanceof SqlAst.StringLit lit)) {
            throw SqlTranslationException.unsupported(name + " with a pattern that is not a constant string is not"
                    + " supported yet", pe.pos());
        }
        String p = lit.value();
        if (p.length() > 126) {
            throw SqlTranslationException.unsupported(name + " with a pattern of more than 126 characters is not"
                    + " supported (SQL Anywhere raises -606, IQ's manual answers NULL: the research's D-F7)", pe.pos());
        }
        int from = 0;
        while (from < p.length() && p.charAt(from) == '%') {
            from++;
        }
        int to = p.length();
        while (to > from && p.charAt(to - 1) == '%') {
            to--;
        }
        if (from == to) {
            if (engine == IqDialect.Engine.IQ) {
                throw SqlTranslationException.unsupported(name + " with a pattern of %s alone (or '') is not supported"
                        + " under the IQ engine: SQL Anywhere answers 0, IQ's manual 1 (the research's D-F6, not yet"
                        + " verified)", pe.pos());
            }
            text(s, f.args().get(1), name);
            return Tx.value(integer(0), Kind.INTEGER, false, type);
        }
        String run = p.substring(from, to);
        if (run.indexOf('%') >= 0 || run.indexOf('_') >= 0 || run.indexOf('[') >= 0) {
            return patternIndex(name, lit, f, s, type);
        }
        boolean anchoredStart = from == 0;
        boolean anchoredEnd = to == p.length();
        Tx str = text(s, f.args().get(1), name);
        if (str.kind() == Kind.NULL) {
            return Tx.value(integer(0), Kind.INTEGER, false, type);
        }
        ValueSpecification value = SqlToPure.valueOf(str);
        ValueSpecification x = str.nullable() ? SqlToPure.call("toOne", value) : value;
        ValueSpecification literal = new CString(run);
        if (!settings.caseSensitive()) {
            // IQ island, 2026-10-14 (natives): without regard to the ASCII letters' case in a sqla17 profile's
            // database (measured: PATINDEX('%BC%', 'abc') is 2), both folded
            x = asciiFold(x, false);
            literal = asciiFold(literal, false);
        }
        ValueSpecification found;
        if (!anchoredStart && !anchoredEnd) {
            found = SqlToPure.call("indexOf", x, literal);
        } else if (anchoredStart && !anchoredEnd) {
            found = ifThen(SqlToPure.call("startsWith", x, literal), integer(1), integer(0));
        } else if (!anchoredStart) {
            found = ifThen(SqlToPure.call("endsWith", x, literal), AppliedFunction.infixRun("plus", List.of(
                    AppliedFunction.infixRun("minus", List.of(SqlToPure.call("length", x), SqlToPure.call("length",
                            literal))), integer(1))), integer(0));
        } else {
            found = ifThen(SqlToPure.call("equal", x, literal), integer(1), integer(0));
        }
        ValueSpecification result = str.nullable() ? ifThen(SqlToPure.call("isEmpty", value), integer(0), found)
                : found;
        return Tx.value(result, Kind.INTEGER, false, type);
    }

    /**
     * {@code STUFF(s, start, length, replacement)}: s with its length characters from start replaced; a NULL
     * replacement deletes them. NULL when s, start or length is NULL, start is below 1 or beyond the string's length
     * plus 1, or length is negative (measured: start = length + 1 appends). IQ island, 2026-10-17 (the differential
     * fuzzer's fifth round; measured on IQ 17.1, the research's D-F3): IQ's engine appends at start = length + 1 too,
     * where its manual says NULL, and a length past 32,767 is NULL there.
     */
    private Tx stuff(String name, SqlAst.FunctionCall f, List<Tx> args) {
        boolean iq = engine == IqDialect.Engine.IQ;
        Tx s = text(args.get(0), f.args().get(0), name);
        Tx start = count(args.get(1), f.args().get(1), name);
        Tx len = count(args.get(2), f.args().get(2), name);
        Tx rep = args.get(3).kind() == Kind.NULL ? Tx.value(new CString(""), Kind.STRING, false, varchar())
                : text(args.get(3), f.args().get(3), name);
        Tx r = strict(Kind.STRING, varchar(), List.of(s, start, len), v -> {
            ValueSpecification str = v.get(0);
            ValueSpecification p = v.get(1);
            ValueSpecification n = v.get(2);
            ValueSpecification replacement = textValue(rep);
            ValueSpecification valid = SqlToPure.call("and", SqlToPure.call("and",
                    SqlToPure.call("greaterThanEqual", p, integer(1)), SqlToPure.call("greaterThanEqual", n,
                            integer(0))), SqlToPure.call("lessThanEqual", p, AppliedFunction.infixRun("plus",
                    List.of(SqlToPure.call("length", str), integer(1)))));
            if (iq) {
                valid = SqlToPure.call("and", valid, SqlToPure.call("lessThanEqual", n, integer(32767)));
            }
            ValueSpecification value = AppliedFunction.infixRun("plus", List.of(SqlToPure.call("left", str,
                    AppliedFunction.infixRun("minus", List.of(p, integer(1)))), replacement,
                    SqlToPure.call("substring", str, AppliedFunction.infixRun("plus", List.of(p, n)))));
            return ifThen(valid, value, SqlToPure.empty());
        });
        return Tx.value(r.vs(), Kind.STRING, true, r.detail());
    }

    /** A string's value, '' for a NULL. */
    private static ValueSpecification textValue(Tx t) {
        ValueSpecification v = SqlToPure.valueOf(t);
        return t.nullable() ? SqlToPure.call("coalesce", v, new CString("")) : v;
    }

    /** {@code ASCII(s)}: its first character's code (an ISO-8859-1 database's byte), 0 for '', a SMALLINT. In a
     *  UTF-8 database (IQ island, 2026-10-17, the corpus lane; measured on IQ 17.1): under IQ's engine the first byte
     *  of the first character's UTF-8, an INTEGER ({@code ASCII('é')} is 195, {@code ASCII('あ')} 227); in SQL
     *  Anywhere's front end a character above 127 is NULL. */
    private Tx ascii(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx s = text(args.get(0), f.args().get(0), name);
        IqType smallint = IqType.integer("SMALLINT", BigInteger.ZERO, BigInteger.valueOf(255), false);
        if (settings.latin1()) {
            return strict(Kind.INTEGER, smallint, List.of(s), v -> SqlToPure.call("ascii", v.get(0)));
        }
        if (engine == IqDialect.Engine.IQ) {
            return strict(Kind.INTEGER, IqType.integer("INTEGER", BigInteger.ZERO, BigInteger.valueOf(255), false),
                    List.of(s), v -> {
                        // the code point's leading UTF-8 byte: 192, 224 or 240 beside its top 5, 4 or 3 bits
                        ValueSpecification c = SqlToPure.call("ascii", v.get(0));
                        return ifThen(lt(c, integer(128)), c, ifThen(lt(c, integer(2048)), lead(192, c, 64), ifThen(
                                lt(c, integer(65536)), lead(224, c, 4096), lead(240, c, 262144))));
                    });
        }
        Tx r = strict(Kind.INTEGER, smallint, List.of(s), v -> ifThen(SqlToPure.call("greaterThan", SqlToPure.call(
                "ascii", v.get(0)), integer(127)), SqlToPure.empty(), SqlToPure.call("ascii", v.get(0))));
        return Tx.value(r.vs(), Kind.INTEGER, true, r.detail());
    }

    /** {@code base + c / unit}, c's leading UTF-8 byte ({@link #ascii}). */
    private static ValueSpecification lead(long base, ValueSpecification c, long unit) {
        return AppliedFunction.infixRun("plus", List.of(integer(base), SqlToPure.wholeQuotient(c, integer(unit))));
    }

    /** {@code CHAR(n)}: the character of code n (an ISO-8859-1 database's), NULL outside 0 .. 255. Refused by name: a
     *  code that may be 0 (a NUL, which lite's databases cannot hold), and above 127 in a {@code sqla17} profile's UTF-8
     *  database (SQL Anywhere answers a lone byte there). IQ island, 2026-10-17 (the differential fuzzer's fifth round;
     *  measured on IQ 17.1): under IQ's engine in a UTF-8 database the byte n modulo 256 (CHAR(256) a NUL, CHAR(-1)
     *  byte 255), refused for a row whose byte is 0 or above 127 (a NUL, a lone byte). */
    private Tx chr(String name, SqlAst.FunctionCall f, List<Tx> args) {
        SqlAst.Expr e = f.args().get(0);
        Tx n = count(args.get(0), e, name);
        if (n.kind() == Kind.NULL) {
            return nullString();
        }
        if (engine == IqDialect.Engine.IQ && !settings.latin1()) {
            BigInteger constant = signedInteger(e);
            int b = constant == null ? -1 : constant.mod(BigInteger.valueOf(256)).intValue();
            if (constant != null && (b == 0 || b > 127)) {
                throw SqlTranslationException.unsupported(name + " of a code whose byte (modulo 256) is 0 or above 127"
                        + " is not supported in a UTF-8 database: IQ's engine answers that byte alone", f.pos());
            }
            if (constant != null) {
                // the byte is the database's to compute (2026-10-18, landing 5's review: compare, not compute); the
                // constant's is read here only to refuse it by name
                return strict(Kind.STRING, varchar(), List.of(n), v -> SqlToPure.call("char", SqlToPure.call("mod",
                        v.get(0), integer(256))));
            }
            String lone = IqDialect.REFUSED_AT_RUN_TIME + name + " of a code whose byte (modulo 256) is 0 or above 127"
                    + " is no character of a UTF-8 database: IQ's engine answers that byte alone";
            return strict(Kind.STRING, varchar(), List.of(n), v -> {
                ValueSpecification byteOf = SqlToPure.call("mod", v.get(0), integer(256));
                return refusedPast(SqlToPure.call("or", SqlToPure.call("equal", byteOf, integer(0)), SqlToPure.call(
                        "greaterThan", byteOf, integer(127))), lone, SqlToPure.call("char", byteOf), SqlToPure.empty());
            });
        }
        IqType t = (IqType) java.util.Objects.requireNonNull(n.detail());
        boolean maybeZero = t.lo().signum() <= 0 && t.hi().signum() >= 0;
        if (maybeZero && signedInteger(e) != null) {
            throw SqlTranslationException.unsupported(name + " of a code that may be 0 is not supported: CHAR(0) is a"
                    + " NUL character, which lite's databases cannot hold", f.pos());
        }
        if (!settings.latin1() && t.hi().compareTo(BigInteger.valueOf(127)) > 0) {
            throw SqlTranslationException.unsupported(name + " of a code above 127 is not supported in profile "
                    + settings.spelling() + "'s UTF-8 database: SQL Anywhere answers a lone byte there", f.pos());
        }
        boolean outside = t.lo().signum() < 0 || t.hi().compareTo(BigInteger.valueOf(255)) > 0;
        // IQ island, 2026-10-17 (the differential fuzzer's fifth round: CHAR(65 + MOD(id, 26))): a computed code that
        // may be 0 is refused for the row that is (a NUL, which lite's databases cannot hold), not before
        String nul = IqDialect.REFUSED_AT_RUN_TIME + name + " of 0 is a NUL character, which lite's databases cannot"
                + " hold";
        Tx r = strict(Kind.STRING, varchar(), List.of(n), v -> {
            ValueSpecification c = SqlToPure.call("char", v.get(0));
            ValueSpecification checked = maybeZero ? refusedPast(SqlToPure.call("equal", v.get(0), integer(0)), nul,
                    c, SqlToPure.empty()) : c;
            return outside ? ifThen(SqlToPure.call("or", lt(v.get(0), integer(0)), SqlToPure.call("greaterThan",
                    v.get(0), integer(255))), SqlToPure.empty(), checked) : checked;
        });
        return outside ? Tx.value(r.vs(), Kind.STRING, true, r.detail()) : r;
    }

    /** {@code STRING(a, ...)}: the values as text, joined; a NULL among them is '', all NULL is NULL (measured). An
     *  integer joins as its digits, a date by the profile's format, under IQ's engine a NUMERIC as its text (IQ
     *  island, 2026-10-15, D-C8, measured on IQ 17.1: '0.01'); a value IQ writes by its own format otherwise (a
     *  DOUBLE, a NUMERIC in SQL Anywhere's front end) is refused. */
    private Tx string(SqlAst.FunctionCall f, List<Tx> args) {
        List<Tx> present = new ArrayList<>();
        for (int i = 0; i < args.size(); i++) {
            Tx a = args.get(i);
            if (a.kind() == Kind.NULL) {
                continue;
            }
            present.add(engine == IqDialect.Engine.IQ && a.kind() == Kind.DECIMAL
                    ? iqNumericAsText(a, f.args().get(i), "STRING") : text(a, f.args().get(i), "STRING"));
        }
        if (present.isEmpty()) {
            return nullString();
        }
        boolean nullable = true;
        List<ValueSpecification> texts = new ArrayList<>();
        ValueSpecification allNull = null;
        for (Tx p : present) {
            nullable &= p.nullable();
            texts.add(textValue(p));
            ValueSpecification isNull = SqlToPure.call("isEmpty", SqlToPure.valueOf(p));
            allNull = allNull == null ? isNull : SqlToPure.call("and", allNull, isNull);
        }
        ValueSpecification joined = texts.size() == 1 ? texts.get(0) : AppliedFunction.infixRun("plus", texts);
        if (!nullable) {
            return Tx.value(joined, Kind.STRING, false, varchar());
        }
        return Tx.value(ifThen(java.util.Objects.requireNonNull(allNull), SqlToPure.empty(), joined), Kind.STRING, true,
                varchar());
    }

    // ------------------------------------------------------------------ numbers

    /**
     * {@code ABS(x)}: in SQL Anywhere's front end, a SMALLINT's an unsigned SMALLINT and an INTEGER's an unsigned
     * INTEGER (no overflow at the smallest value), a BIGINT's a NUMERIC(20,0), a NUMERIC's and a DOUBLE's their own
     * type, a BIT itself (measured). Under the IQ engine (IQ island, 2026-10-16, measured on IQ 17.1, replacing the
     * research's D-F5, which raised IQ's -158 at a type's smallest value): an integer's own type where its absolute
     * value fits it (ABS(-5) a SMALLINT, an unsigned type's always), else the next wider one -- a SMALLINT column's an
     * INTEGER, an INTEGER's a BIGINT, a BIGINT's a NUMERIC(20,0). A string (IQ's NUMERIC conversion) is refused.
     */
    private Tx abs(SqlAst.FunctionCall f, List<Tx> args) {
        Tx x = args.get(0);
        SqlAst.Expr e = f.args().get(0);
        if (x.kind() == Kind.NULL) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, new IqType("UNSIGNED SMALLINT"));
        }
        if (x.kind() == Kind.FLOAT) {
            doubleOnly(x, e, "ABS");
            return strict(Kind.FLOAT, new IqType("DOUBLE"), List.of(x), v -> SqlToPure.call("abs", v.get(0)));
        }
        if (decimal(x)) {
            // an integer constant beyond an INTEGER is IQ's NUMERIC (-2147483648): as a decimal, never Postgres's int4
            boolean integer = x.kind() == Kind.INTEGER;
            scaleOf(x, e, "ABS");
            IqType type = (IqType) java.util.Objects.requireNonNull(x.detail());
            return strict(Kind.DECIMAL, type, List.of(x), v -> SqlToPure.call("abs", integer
                    ? SqlToPure.call("toDecimal", v.get(0)) : v.get(0)));
        }
        if (x.detail() instanceof IqType u && u.family().equals("UNSIGNED BIGINT")) {
            // IQ island, 2026-10-16 (measured on IQ 17.1): a COUNT's, itself
            return x;
        }
        IqType t = integerType(x, e, "ABS");
        BigInteger top = t.lo().abs().max(t.hi().abs());
        BigInteger bottom = t.lo().signum() <= 0 && t.hi().signum() >= 0 ? BigInteger.ZERO
                : t.lo().abs().min(t.hi().abs());
        if (t.family().equals("BIT")) {
            return x;
        }
        if (engine == IqDialect.Engine.IQ) {
            String family = t.family();
            if (top.compareTo(t.max()) > 0) {
                // the next wider type holds it: a BIGINT's a NUMERIC(20,0), computed as a decimal
                if (family.equals("BIGINT")) {
                    return strict(Kind.DECIMAL, new IqType("NUMERIC(20,0)"), List.of(x), v -> SqlToPure.call("abs",
                            SqlToPure.call("toDecimal", v.get(0))));
                }
                family = family.equals("SMALLINT") ? "INTEGER" : "BIGINT";
            }
            boolean widen = !t.wide() && !t.exact();
            Tx r = SqlToPure.strict(Kind.INTEGER, List.of(x), v -> SqlToPure.call("abs", widen
                    ? SqlToPure.call("floor", v.get(0)) : v.get(0)));
            return Tx.value(r.vs(), Kind.INTEGER, r.nullable(), IqType.integer(family, bottom, top, widen || t.wide(),
                    t.wider()));
        }
        if (t.family().equals("BIGINT")) {
            return strict(Kind.DECIMAL, new IqType("NUMERIC(20,0)"), List.of(x), v -> SqlToPure.call("abs",
                    SqlToPure.call("toDecimal", v.get(0))));
        }
        boolean widen = !t.wide() && !t.exact();
        Tx r = SqlToPure.strict(Kind.INTEGER, List.of(x), v -> SqlToPure.call("abs", widen ? SqlToPure.call("floor",
                v.get(0)) : v.get(0)));
        return Tx.value(r.vs(), Kind.INTEGER, r.nullable(), IqType.integer("UNSIGNED " + t.family(), bottom, top,
                true));
    }

    /** {@code SIGN(x)}: -1, 0 or 1, a SMALLINT (of -0.0, 0); under IQ's engine of the argument's own type (IQ island,
     *  2026-10-15, D-F19, measured on IQ 17.1: a BIGINT's a BIGINT, a NUMERIC(10,2)'s -1.00, a DOUBLE's -1.0). */
    private Tx sign(SqlAst.FunctionCall f, List<Tx> args) {
        Tx x = args.get(0);
        if (x.kind() != Kind.NULL) {
            number(x, f.args().get(0), "SIGN");
        }
        if (engine == IqDialect.Engine.IQ && x.kind() != Kind.NULL) {
            SqlAst.Expr e = f.args().get(0);
            if (x.kind() == Kind.FLOAT) {
                return strict(Kind.FLOAT, new IqType("DOUBLE"), List.of(x), v -> SqlToPure.call("toFloat",
                        SqlToPure.call("sign", v.get(0))));
            }
            if (decimal(x)) {
                int[] ps = scaleOf(x, e, "SIGN");
                IqType own = (IqType) java.util.Objects.requireNonNull(x.detail());
                // IQ island, 2026-10-17 (the differential fuzzer; measured on IQ 17.1): a NUMERIC with no digit before
                // its point (0.5 is a NUMERIC(1,1)) cannot hold 1, so a value not 0 is IQ's -1001014 (data conversion
                // failed) in each row that computes it
                boolean holdsNoOne = ps[0] <= ps[1];
                return strict(Kind.DECIMAL, own, List.of(x), v -> {
                    ValueSpecification d = SqlToPure.call("toDecimal", SqlToPure.call("sign", v.get(0)));
                    ValueSpecification shown = ps[1] > 0 ? atScale(d, ps[1]) : d;
                    return holdsNoOne ? ifThen(SqlToPure.call("equal", SqlToPure.call("sign", v.get(0)), integer(0)),
                            shown, raise("Decimal")) : shown;
                });
            }
            IqType t = integerType(x, e, "SIGN");
            return strict(Kind.INTEGER, IqType.integer(t.family(), t.lo().signum() < 0 ? BigInteger.ONE.negate()
                    : BigInteger.ZERO, BigInteger.ONE, false), List.of(x), v -> SqlToPure.call("sign", v.get(0)));
        }
        return strict(Kind.INTEGER, IqType.integer("SMALLINT", BigInteger.ONE.negate(), BigInteger.ONE, false),
                List.of(x), v -> SqlToPure.call("sign", v.get(0)));
    }

    /** 2^52: a DOUBLE of this magnitude or more is a whole number. */
    private static final double WHOLE = 4503599627370496.0;

    /**
     * {@code CEILING(x)}, {@code CEIL(x)}, {@code FLOOR(x)} in SQL Anywhere's front end: the argument as a DOUBLE first
     * (measured: FLOOR of NUMERIC 0.99999999999999999999 is 1), then its whole number, a DOUBLE. Under the IQ engine
     * the result is the argument's own type (the research's D-F2, not yet verified): refused.
     */
    private Tx ceilingFloor(boolean floor, String name, SqlAst.FunctionCall f, List<Tx> args) {
        if (engine == IqDialect.Engine.IQ && args.get(0).kind() != Kind.FLOAT) {
            return iqCeilingFloor(floor, name, f, args.get(0));
        }
        Tx x = asDouble(args.get(0), f.args().get(0), name);
        return strict(Kind.FLOAT, new IqType("DOUBLE"), List.of(x), v -> {
            ValueSpecification d = v.get(0);
            return ifThen(SqlToPure.call("greaterThanEqual", SqlToPure.call("abs", d), new CFloat(WHOLE)), d,
                    SqlToPure.call("toFloat", SqlToPure.call(floor ? "floor" : "ceiling", d)));
        });
    }

    /**
     * {@code CEILING}, {@code FLOOR} under IQ's engine (IQ island, 2026-10-15; the research's D-F2, measured on IQ
     * 17.1): of a NUMERIC(p,s) the whole number at scale s, a NUMERIC(p+1,s) (CEILING of 12345678.99 is 12345679.00);
     * of an integer the integer itself; of a DOUBLE a DOUBLE, as in SQL Anywhere's front end. Refused by name: a
     * NUMERIC of more than 18 digits before its point.
     */
    private Tx iqCeilingFloor(boolean floor, String name, SqlAst.FunctionCall f, Tx x) {
        SqlAst.Expr e = f.args().get(0);
        if (x.kind() == Kind.NULL) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, new IqType("INTEGER"));
        }
        if (!decimal(x)) {
            IqType t = integerType(x, e, name);
            return strict(Kind.INTEGER, t, List.of(x), v -> v.get(0));
        }
        int[] ps = scaleOf(x, e, name);
        if (ps[0] - ps[1] > 18) {
            throw SqlTranslationException.unsupported(name + " of a NUMERIC(" + ps[0] + "," + ps[1] + ") is not"
                    + " supported yet: its whole number may exceed 18 digits", f.pos());
        }
        int s = ps[1];
        return strict(Kind.DECIMAL, new IqType("NUMERIC(" + (ps[0] + 1) + "," + s + ")"), List.of(x), v -> {
            ValueSpecification whole = SqlToPure.call("toDecimal", SqlToPure.call(floor ? "floor" : "ceiling",
                    v.get(0)));
            return s > 0 ? atScale(whole, s) : whole;
        });
    }

    /**
     * {@code ROUND(x, n)} (half away from zero) and {@code TRUNCNUM(x, n)} (toward zero) to n places, n a constant (of
     * at least -18): an integer's a NUMERIC(10,0) (a BIGINT's NUMERIC(20,0)), a NUMERIC's its own type -- a result its
     * precision cannot hold is IQ's error -158, but for a constant (SQL Anywhere types a folded one a digit wider).
     * Refused by name: ROUND under the IQ engine (its type and ROUND_TO_EVEN are the research's D-F1), a DOUBLE (SQL
     * Anywhere rounds its decimal digits, measured: ROUND(2.675e0, 2) is 2.68: a native), a NUMERIC whose scaled value
     * may exceed 18 digits.
     */
    private Tx roundTruncate(boolean round, String name, SqlAst.FunctionCall f, List<Tx> args) {
        // IQ island, 2026-10-15 (the research's D-F1, measured on IQ 17.1): under IQ's engine ROUND of a NUMERIC(p,s) is
        // a NUMERIC(p+1,s), half away from zero (-2.5 is -3, 2.5 is 3), so never past its precision; of an integer an
        // integer, widened as IQ widens (ROUND(2147483647, -1) is the BIGINT 2147483650). TRUNCNUM keeps its type
        boolean iqRound = round && engine == IqDialect.Engine.IQ;
        Tx x = args.get(0);
        SqlAst.Expr e = f.args().get(0);
        BigInteger places = signedInteger(f.args().get(1));
        if (args.get(1).kind() == Kind.NULL) {
            places = null;
        }
        if (places == null && args.get(1).kind() != Kind.NULL) {
            throw SqlTranslationException.unsupported(name + " with a number of places other than an integer constant"
                    + " is not supported yet", f.args().get(1).pos());
        }
        if (x.kind() == Kind.NULL) {
            return Tx.value(SqlToPure.nullOf(Kind.FLOAT).vs(), Kind.FLOAT, true, new IqType("DOUBLE"));
        }
        if (x.kind() == Kind.FLOAT && round) {
            return doubleRound(f, x, places);
        }
        if (x.kind() == Kind.FLOAT) {
            throw SqlTranslationException.unsupported(name + " of a DOUBLE is not supported yet", f.pos());
        }
        int p;
        int s;
        int digits;
        boolean whole;
        if (decimal(x)) {
            int[] ps = scaleOf(x, e, name);
            p = ps[0];
            s = ps[1];
            digits = p - s;
            whole = false;
        } else {
            IqType t = integerType(x, e, name);
            p = t.family().equals("BIGINT") ? 20 : 10;
            s = 0;
            digits = Math.max(t.lo().abs().toString().length(), t.hi().abs().toString().length());
            whole = true;
        }
        IqType type = new IqType("NUMERIC(" + (iqRound ? p + 1 : p) + "," + s + ")");
        if (iqRound && whole) {
            return iqRoundInteger(f, x, places);
        }
        if (!round && whole && engine == IqDialect.Engine.IQ) {
            return iqTruncateInteger(f, x, places);
        }
        if (places == null) {
            return Tx.value(SqlToPure.nullOf(Kind.DECIMAL).vs(), Kind.DECIMAL, true, type);
        }
        if (places.compareTo(BigInteger.valueOf(-18)) < 0) {
            throw SqlTranslationException.unsupported(name + " to fewer than -18 places is not supported yet",
                    f.args().get(1).pos());
        }
        int n = places.min(BigInteger.valueOf(s)).intValue();
        if (n >= s) {
            // no place to drop: the value itself, at its scale
            return strict(Kind.DECIMAL, type, List.of(x), v -> whole ? SqlToPure.call("toDecimal", v.get(0)) : v.get(0));
        }
        // IQ island, 2026-10-15 (the corpus lane: ROUND(COUNT(*) * 100.0 / n, 2), a NUMERIC(30,6) to 2 places): past
        // the 18 digits places() computes in, a NUMERIC is rounded or cut by placesExact, in 38
        boolean exact = digits + n > 18;
        // a value of more digits than placesExact holds (a wide quotient's: ROUND(COUNT(id) * 100.0 / COUNT(*), 2) of
        // IQ's NUMERIC(44,21)) is rounded where it fits, refused at run time where it does not
        int heldDigits = Math.min(digits, 37 - s);
        if (exact && (whole || heldDigits < 1)) {
            throw SqlTranslationException.unsupported(name + " of a NUMERIC(" + p + "," + s + ") to " + n + " places is"
                    + " not supported yet: its scaled value may exceed 18 digits", f.pos());
        }
        String past = exact && heldDigits < digits ? IqDialect.REFUSED_AT_RUN_TIME + name + " of a value of more than "
                + heldDigits + " digits before its point is not supported: its NUMERIC(" + p + "," + s + ") needs more"
                + " than the 38 digits lite's decimals hold" : null;
        // a constant (no column in it) SQL Anywhere folds and types a digit wider (measured): no check; nor under IQ's
        // engine, whose NUMERIC(p+1,s) holds every rounded value
        boolean check = round && readsColumn(e) && !whole && !iqRound;
        return strict(Kind.DECIMAL, type, List.of(x), v -> {
            ValueSpecification value = whole ? SqlToPure.call("toDecimal", v.get(0)) : v.get(0);
            ValueSpecification r = exact ? placesExact(atScale(value, s), s, n, round) : places(value, n, round);
            if (past != null) {
                r = refusedPast(SqlToPure.call("greaterThanEqual", SqlToPure.call("abs", value), new CDecimal(pow10(
                        heldDigits))), past, r, raise("Decimal"));
            }
            if (s > Math.max(n, 0)) {
                r = atScale(r, s);
            }
            if (!check) {
                return r;
            }
            return ifThen(SqlToPure.call("greaterThanEqual", SqlToPure.call("abs", r), new CDecimal(pow10(p - s))),
                    raise("Decimal"), r);
        });
    }

    /**
     * {@code ROUND(x, n)} of a DOUBLE or a REAL (IQ island, 2026-10-15; the corpus lane, measured on IQ 17.1, its engine
     * and its front end alike): the sign of x times floor(|x| * 10^n + 0.5) / 10^n, every step a DOUBLE's -- for n < 0,
     * floor(|x| / 10^-n + 0.5) * 10^-n -- so ROUND(2.675e0, 2) is 2.68 (267.49999999999997 + 0.5 rounds to 268.0) and
     * ROUND(1.005e0, 2) 1.0 (100.49999999999999 + 0.5 does not reach 101), the final step a quotient (ROUND(0.3, 1)
     * is 0.3, not 0.30000000000000004). Of n past 22 places x itself (measured: 23, 308); a magnitude of 2^52 or more
     * is whole already. A DOUBLE's ROUND is a DOUBLE, a REAL's a REAL (the DOUBLE rounded to single precision). Refused
     * by name: n below -22.
     */
    private Tx doubleRound(SqlAst.FunctionCall f, Tx x, @com.legend.base.Nullable BigInteger places) {
        boolean single = real(x);
        if (!single) {
            doubleOnly(x, f.args().get(0), "ROUND");
        }
        IqType type = new IqType(single ? "REAL" : "DOUBLE");
        if (places == null) {
            return Tx.value(SqlToPure.nullOf(Kind.FLOAT).vs(), Kind.FLOAT, true, type);
        }
        if (places.compareTo(BigInteger.valueOf(22)) > 0) {
            return Tx.value(x.vs(), Kind.FLOAT, x.nullable(), type);
        }
        if (places.compareTo(BigInteger.valueOf(-22)) < 0) {
            throw SqlTranslationException.unsupported("ROUND of a DOUBLE to fewer than -22 places is not supported yet",
                    f.args().get(1).pos());
        }
        int n = places.intValue();
        CFloat scale = new CFloat(Math.pow(10, Math.abs(n)));
        return strict(Kind.FLOAT, type, List.of(x), v -> {
            ValueSpecification d = v.get(0);
            ValueSpecification magnitude = SqlToPure.call("abs", d);
            ValueSpecification scaled = n >= 0 ? AppliedFunction.infixRun("times", List.of(magnitude, scale))
                    : SqlToPure.call("divide", magnitude, scale);
            ValueSpecification half = AppliedFunction.infixRun("plus", List.of(scaled, new CFloat(0.5)));
            ValueSpecification whole = ifThen(SqlToPure.call("greaterThanEqual", half, new CFloat(WHOLE)), half,
                    SqlToPure.call("toFloat", SqlToPure.call("floor", half)));
            ValueSpecification back = n >= 0 ? SqlToPure.call("divide", whole, scale)
                    : AppliedFunction.infixRun("times", List.of(whole, scale));
            ValueSpecification signed = ifThen(lt(d, new CFloat(0.0)), SqlToPure.call("minus", back), back);
            ValueSpecification r = n >= 0 ? ifThen(SqlToPure.call("greaterThanEqual", magnitude, new CFloat(WHOLE)), d,
                    signed) : signed;
            return single ? single(r) : r;
        });
    }

    /** ROUND of an integer under IQ's engine (IQ island, 2026-10-15, D-F1, measured on IQ 17.1): the integer itself to
     *  0 places or more; to n < 0 places half away from zero, an integer of 64 bits (IQ widens it). */
    private Tx iqRoundInteger(SqlAst.FunctionCall f, Tx x, @com.legend.base.Nullable BigInteger places) {
        IqType wide = IqType.integer("BIGINT", IqType.BIGINT_MIN, IqType.BIGINT_MAX, true);
        if (places == null) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, wide);
        }
        IqType t = integerType(x, f.args().get(0), "ROUND");
        if (places.signum() >= 0) {
            return strict(Kind.INTEGER, t, List.of(x), v -> v.get(0));
        }
        int n = places.intValue();
        int digits = Math.max(t.lo().abs().toString().length(), t.hi().abs().toString().length());
        if (digits > 18 || n < -18) {
            throw SqlTranslationException.unsupported("ROUND of a BIGINT to " + n + " places is not supported yet: its"
                    + " rounded value may exceed 18 digits", f.pos());
        }
        return strict(Kind.INTEGER, wide, List.of(x), v -> SqlToPure.call("floor", places(SqlToPure.call("toDecimal",
                v.get(0)), n, true)));
    }

    /** TRUNCNUM of an integer under IQ's engine (IQ island, 2026-10-15, D-F19, measured on IQ 17.1): of the integer's
     *  own type (TRUNCNUM(sm, -2) a SMALLINT), toward zero, so never past it. */
    private Tx iqTruncateInteger(SqlAst.FunctionCall f, Tx x, @com.legend.base.Nullable BigInteger places) {
        IqType t = integerType(x, f.args().get(0), "TRUNCNUM");
        if (places == null) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, IqType.integer(t.family(),
                    t.min(), t.max(), false));
        }
        if (places.signum() >= 0) {
            return strict(Kind.INTEGER, t, List.of(x), v -> v.get(0));
        }
        int n = places.intValue();
        int digits = Math.max(t.lo().abs().toString().length(), t.hi().abs().toString().length());
        if (digits + n > 18 || n < -18) {
            throw SqlTranslationException.unsupported("TRUNCNUM of a " + t.family() + " to " + n + " places is not"
                    + " supported yet: its scaled value may exceed 18 digits", f.pos());
        }
        // a 64-bit value now (floor of a decimal), of the integer's type and range
        return strict(Kind.INTEGER, IqType.integer(t.family(), t.lo(), t.hi(), true, t.wider()), List.of(x),
                v -> SqlToPure.call("floor", places(SqlToPure.call("toDecimal", v.get(0)), n, false)));
    }

    /** {@code x} (a decimal) to {@code n} places, half away from zero or toward zero, at scale max(n, 0): exact
     *  decimal arithmetic, its scaled value within 18 digits. */
    static ValueSpecification places(ValueSpecification x, int n, boolean round) {
        ValueSpecification scaled = n == 0 ? x : AppliedFunction.infixRun("times", List.of(x, new CDecimal(pow10(n))));
        ValueSpecification positive = SqlToPure.call("greaterThanEqual", x, integer(0));
        ValueSpecification half = new CDecimal(new BigDecimal("0.5"));
        ValueSpecification whole = round
                ? ifThen(positive, SqlToPure.call("floor", AppliedFunction.infixRun("plus", List.of(scaled, half))),
                        SqlToPure.call("ceiling", AppliedFunction.infixRun("minus", List.of(scaled, half))))
                : ifThen(positive, SqlToPure.call("floor", scaled), SqlToPure.call("ceiling", scaled));
        ValueSpecification back = SqlToPure.call("toDecimal", whole);
        return n == 0 ? back : AppliedFunction.infixRun("times", List.of(back, new CDecimal(pow10(-n))));
    }

    /**
     * {@code x}, a decimal of scale {@code s}, rounded half away from zero ({@code round}) or cut toward zero to
     * {@code n < s} places, at scale {@code s}, exactly in 38 digits (IQ island, 2026-10-15): x and half a unit of the
     * n-th place, less the remainder of that by the unit -- Pure's {@code rem} of two decimals of one scale, exact in both
     * databases ({@link #remainder}), with the dividend's sign, so toward zero.
     */
    static ValueSpecification placesExact(ValueSpecification x, int s, int n, boolean round) {
        BigDecimal unit = pow10(-n);
        ValueSpecification y = x;
        if (round) {
            CDecimal half = new CDecimal(unit.divide(BigDecimal.valueOf(2)).setScale(s));
            y = ifThen(SqlToPure.call("greaterThanEqual", x, integer(0)), AppliedFunction.infixRun("plus", List.of(x,
                    half)), AppliedFunction.infixRun("minus", List.of(x, half)));
        }
        return AppliedFunction.infixRun("minus", List.of(y, SqlToPure.call("rem", y, new CDecimal(unit.setScale(s)))));
    }

    /** {@code value}, but the island's refusal {@code message} raised where {@code test} holds (IQ island,
     *  2026-10-15): Pure's {@code fail} is a Boolean, so it guards the choice of {@code value} over {@code never} (a
     *  value of its type that raises, never reached), which keeps the result of {@code value}'s type. */
    static ValueSpecification refusedPast(ValueSpecification test, String message, ValueSpecification value,
            ValueSpecification never) {
        return ifThen(ifThen(test, SqlToPure.call("fail", new CString(message)), new com.legend.protocol.spec.CBoolean(
                true)), value, never);
    }

    /** 10 to the {@code k}, written with no exponent: 100 for 2, 0.01 for -2. */
    private static BigDecimal pow10(int k) {
        return k >= 0 ? new BigDecimal(BigInteger.TEN.pow(k)) : BigDecimal.ONE.movePointLeft(-k);
    }

    /** {@code x} at scale {@code s} (no fewer places than it has): a 38-digit zero, then a zero of that scale added.
     *  IQ island, 2026-10-14 (natives): the 38-digit zero first -- DuckDB adds two decimals of at most 18 digits in
     *  18 ({@code 756929791864553.5 + 0.00000} overflows there), where Postgres's numeric has no limit. */
    static ValueSpecification atScale(ValueSpecification x, int s) {
        return AppliedFunction.infixRun("plus", List.of(x, SqlToPure.call("toDecimal", new CInteger(0L)),
                new CDecimal(BigDecimal.ZERO.setScale(s))));
    }

    // ------------------------------------------------------------------ exact NUMERIC division (IQ island, 2026-10-14)

    /**
     * SQL Anywhere's NUMERIC quotient {@code x / y}, of IQ's type NUMERIC(p, s) (the research's TYPING §4: p1 + p2 + 1
     * and s1 + p2 - s2 + 1, then the profile's cap; for AVG, AVG's type), where x is a decimal of scale {@code s1} with
     * at most {@code whole1} digits before its point, y one of scale {@code s2} with at most {@code whole2}, neither NULL
     * and y not 0 (the caller raises IQ's -628 first), and |x / y| below 10^(p - s), as IQ's type promises.
     * <p>
     * The value, measured on SQL Anywhere 17 over 3,400 quotients of every shape in both kinds of database (and 129
     * AVGs): the exact quotient cut to ceil((p + 1) / 2) base-100 digits from its first nonzero one, then rounded half
     * away from zero at scale s. That is the quotient rounded half away from zero at scale s -- but truncated there when
     * p is odd, s even and the quotient has all its p - s whole digits (two NUMERIC(1,0)s: 5/3 is 1.66, 2/3 is .67;
     * NUMERIC(2,1) 8.5 / NUMERIC(1,0) 8 is 1.063).
     * <p>
     * Composed of existing natives, exactly: n = |x| * 10^s and d = |y|, so that n / d = |x / y| * 10^s;
     * the rounded quotient is the whole quotient of 2n + d by 2d, the truncated one that of n by d ({@link #whole}); the
     * sign is x's and y's. Every step is a decimal of at most 38 digits, its scale counted (DuckDB's limit): the
     * caller refuses what {@link #quotientFits} says would need more. x and y are 38-digit decimals already (DuckDB
     * multiplies two narrower ones in 64 bits).
     * <p>
     * The value is the one SQL Anywhere shows; as an operand, a CAST, a comparison or a key it reads the quotient cut
     * to base-100 digits, unrounded ({@code (10.0/3)*3} is 10.00), so a caller uses this only where the quotient is
     * shown. AVG of a NUMERIC (measured: 129 AVGs, every one SUM / COUNT by this rule at AVG's NUMERIC(p+19, s+11)):
     * {@code quotient(sum, s, wholeOfSum, count, 0, 19, pAvg, sAvg)}, shown only, as {@code /}.
     */
    static ValueSpecification quotient(ValueSpecification x, int s1, int whole1, ValueSpecification y, int s2,
            int whole2, int p, int s) {
        if (!quotientFits(s1, whole1, s2, whole2, p, s)) {
            throw new IllegalStateException("a quotient of more than 38 digits");
        }
        ValueSpecification ax = SqlToPure.call("abs", x);
        ValueSpecification ay = SqlToPure.call("abs", y);
        ValueSpecification n = s > 0 ? times(ax, new CDecimal(pow10(s))) : ax;
        ValueSpecification d = ay;
        BigDecimal bound = pow10(p);
        ValueSpecification two = new CDecimal(BigDecimal.valueOf(2));
        ValueSpecification m = whole(AppliedFunction.infixRun("plus", List.of(times(two, n), d)), times(two, d), bound);
        if (p % 2 == 1 && s % 2 == 0) {
            // the quotient has all its whole digits when |x| >= |y| * 10^(p - 1 - s)
            int k = p - 1 - s;
            ValueSpecification full = SqlToPure.call("greaterThanEqual", k < 0 ? times(ax, new CDecimal(pow10(-k)))
                    : ax, k > 0 ? times(ay, new CDecimal(pow10(k))) : ay);
            m = ifThen(full, whole(n, d, bound), m);
        }
        ValueSpecification negative = SqlToPure.call("or", SqlToPure.call("and", lt(x, integer(0)),
                SqlToPure.call("greaterThan", y, integer(0))), SqlToPure.call("and",
                SqlToPure.call("greaterThan", x, integer(0)), lt(y, integer(0))));
        BigDecimal unit = pow10(-s);
        return times(m, ifThen(negative, new CDecimal(unit.negate()), new CDecimal(unit)));
    }

    /**
     * IQ's engine's AVG quotient (IQ island, 2026-10-15; measured on IQ 17.1): {@code x / y} cut toward zero at scale
     * {@code s} (24 / 7 is 3.4285714285714285 at scale 16, -11 / 3 is -3.6666666666666666), where x is a decimal of
     * scale {@code s1} (at most s) with at most {@code whole1} digits before its point and y a positive whole number,
     * neither NULL. Exactly: n = |x| * 10^s, so that n / y = |x / y| * 10^s; its whole quotient by y
     * ({@link #whole}) is the value's digits; the sign is x's. x and y are 38-digit decimals already; the caller refuses
     * what {@link #truncatedFits} says would need more than lite's 38 digits.
     */
    static ValueSpecification truncatedQuotient(ValueSpecification x, int s1, int whole1, ValueSpecification y,
            int s) {
        if (!truncatedFits(s1, whole1, s)) {
            throw new IllegalStateException("a quotient of more than 38 digits");
        }
        ValueSpecification ax = SqlToPure.call("abs", x);
        ValueSpecification n;
        if (whole1 + s + s1 <= 38) {
            n = s > 0 ? times(ax, new CDecimal(pow10(s))) : ax;
        } else {
            // IQ island, 2026-10-17 (TPC-H's AVG of a NUMERIC(15,2), IQ's NUMERIC(37,23)): |x| made whole first,
            // |x| * 10^s1 through a BIGINT (exact: at most 18 digits) to a scale-0 decimal, so that n needs
            // whole1 + s digits, not whole1 + s + s1
            ValueSpecification whole = SqlToPure.call("toDecimal", SqlToPure.call("floor",
                    times(ax, new CDecimal(pow10(s1)))));
            n = s > s1 ? times(whole, new CDecimal(pow10(s - s1))) : whole;
        }
        ValueSpecification m = whole(n, y, pow10(whole1 + s));
        BigDecimal unit = pow10(-s);
        return times(m, ifThen(lt(x, integer(0)), new CDecimal(unit.negate()), new CDecimal(unit)));
    }

    /**
     * IQ's engine's NUMERIC quotient (IQ island, 2026-10-15; D-N1, measured on IQ 17.1): {@code x / y} cut toward zero
     * at scale {@code s}, x of scale {@code s1} with at most {@code whole1} digits before its point, y of scale
     * {@code s2}, neither NULL and y not 0 (the caller raises IQ's error first). Exactly: the whole quotient of
     * |x| * 10^s by |y| ({@link #whole}), signed as x / y is. x and y are 38-digit decimals already; the caller
     * refuses what {@link #truncatedDivisionFits} says would need more than lite's 38 digits.
     */
    static ValueSpecification truncatedDivision(ValueSpecification x, int s1, int whole1, ValueSpecification y,
            int s2, int s) {
        ValueSpecification ax = SqlToPure.call("abs", x);
        ValueSpecification ay = SqlToPure.call("abs", y);
        ValueSpecification n = s > 0 ? times(ax, new CDecimal(pow10(s))) : ax;
        ValueSpecification m = whole(n, ay, pow10(whole1 + s + s2));
        ValueSpecification negative = SqlToPure.call("or", SqlToPure.call("and", lt(x, integer(0)),
                SqlToPure.call("greaterThan", y, integer(0))), SqlToPure.call("and",
                SqlToPure.call("greaterThan", x, integer(0)), lt(y, integer(0))));
        BigDecimal unit = pow10(-s);
        return times(m, ifThen(negative, new CDecimal(unit.negate()), new CDecimal(unit)));
    }

    /** The most chunks {@link #longQuotient} divides in (2026-10-17: TPC-DS's AVG of an AVG took 19, and its plan did
     *  not finish in 20 s over empty tables). */
    private static final int LONG_QUOTIENT_CHUNKS = 3;

    /**
     * IQ island, 2026-10-17 (the TPC-H and TPC-DS corpus lanes' 900 AVGs of a NUMERIC whose type IQ's engine makes
     * wider than 38 digits, NUMERIC(p+22, s+21)): {@code x / y} cut toward zero at scale {@code s} by long division, so
     * that no step needs more than 38 digits -- the whole quotient of |x| by y ({@link #whole}), then the places in
     * chunks of at most k digits, each the whole quotient of the rest times 10^k -- with x's sign. x is a decimal of
     * scale {@code s1} with at most {@code whole1} digits before its point, y a whole decimal above 0 of at most
     * {@code wholeY}. A quotient of 38 - s digits or more before its point is refused at run time ({@code refusal}):
     * lite's decimals hold no more, where IQ's type does. Null where a step would still need more than 38 digits.
     */
    static @com.legend.base.Nullable ValueSpecification longQuotient(ValueSpecification x, int s1, int whole1,
            ValueSpecification y, int wholeY, int s, String refusal) {
        // a chunk of at most 13 digits takes whole() one estimate: fewer repeats of the rest (2026-10-17)
        int k = Math.min(38 - wholeY - s1 - 1, 13);
        // each chunk reads the rest the one before it leaves, so the expression grows with their count: past three
        // (a quotient of many places over a dividend of many, an AVG of an AVG) the plan is refused by name
        if (k < 1 || s < s1 || s > 37 || whole1 + s1 > 38 || (s + k - 1) / k > LONG_QUOTIENT_CHUNKS) {
            return null;
        }
        ValueSpecification ax = SqlToPure.call("abs", x);
        ValueSpecification q0 = whole(ax, y, pow10(whole1));
        ValueSpecification rest = AppliedFunction.infixRun("minus", List.of(ax, times(q0, y)));
        ValueSpecification result = q0;
        int done = 0;
        while (done < s) {
            int step = Math.min(k, s - done);
            ValueSpecification a = times(rest, new CDecimal(pow10(step)));
            ValueSpecification q = whole(a, y, pow10(step));
            done += step;
            result = AppliedFunction.infixRun("plus", List.of(result, times(q, new CDecimal(pow10(-done)))));
            rest = AppliedFunction.infixRun("minus", List.of(a, times(q, y)));
        }
        ValueSpecification signed = times(result, ifThen(lt(x, integer(0)), new CDecimal(BigDecimal.ONE.negate()),
                new CDecimal(BigDecimal.ONE)));
        return refusedPast(SqlToPure.call("greaterThanEqual", q0, new CDecimal(pow10(38 - s))), refusal, signed,
                raise("Decimal"));
    }

    /** Whether {@link #truncatedDivision}'s steps fit lite's 38 digits, scales counted. */
    static boolean truncatedDivisionFits(int s1, int whole1, int s2, int whole2, int s) {
        return s <= 38 && whole1 + s + s2 <= 38 && Math.max(whole1 + s + s2, whole2) + 1 + Math.max(s1, s2) <= 38;
    }

    /** Whether {@link #truncatedQuotient}'s steps fit lite's 38 digits, scales counted (n keeps x's scale, unless x
     *  is made whole first, through a BIGINT). */
    static boolean truncatedFits(int s1, int whole1, int s) {
        return s1 <= s && (whole1 + s + s1 <= 38 || whole1 + s <= 38 && whole1 + s1 <= FLOOR_DIGITS);
    }

    /** Whether {@link #quotient}'s steps fit lite's 38 digits, scales counted (n = |x| * 10^s keeps x's scale, and
     *  2n + d, a - q * b the larger one). */
    static boolean quotientFits(int s1, int whole1, int s2, int whole2, int p, int s) {
        int k = p - 1 - s;
        return p <= 38 && Math.max(whole1 + s, whole2) + 1 + Math.max(s1, s2) <= 38
                && (p % 2 == 0 || s % 2 == 1 || whole2 + Math.max(k, 0) + s2 <= 38
                && whole1 + Math.max(-k, 0) + s1 <= 38);
    }

    /**
     * IQ island, 2026-10-15 (windows): {@code x / y} cut toward zero at scale {@code s}, exactly -- the IQ engine's AVG
     * (measured on IQ 17.1: the AVG of 1, 1 and 3 is 1.6666666666666666, at its 16 places) -- for {@code x} a
     * decimal of scale {@code s1} with at most {@code whole1} digits before its point, and {@code y} a whole decimal of
     * at most {@code whole2} digits, above 0: n = |x| * 10^s, its whole quotient by y ({@link #whole}), at scale s, with
     * x's sign. Null where a step would need more than lite's 38 digits.
     */
    static @com.legend.base.Nullable ValueSpecification cutQuotient(ValueSpecification x, int s1, int whole1,
            ValueSpecification y, int whole2, int s) {
        if (whole1 + s + s1 + 1 > 38 || whole2 + s1 + 1 > 38) {
            return null;
        }
        ValueSpecification ax = SqlToPure.call("abs", x);
        ValueSpecification n = s > 0 ? times(ax, new CDecimal(pow10(s))) : ax;
        ValueSpecification m = whole(n, y, pow10(whole1 + s));
        BigDecimal unit = pow10(-s);
        return times(m, ifThen(lt(x, integer(0)), new CDecimal(unit.negate()), new CDecimal(unit)));
    }

    /** Whether {@link #remainder}'s operands at one scale fit lite's 38 digits. */
    static boolean remainderFits(int s1, int whole1, int s2, int whole2) {
        int scale = Math.max(s1, s2);
        return whole1 + scale <= 38 && whole2 + scale <= 38;
    }

    /** How far below the true quotient a biased double estimate may fall, relatively: the bias ({@link #BELOW}) and
     *  the databases' own rounding (a decimal to a double, a double division: a few units in the last place). */
    private static final BigDecimal SLACK = new BigDecimal("6e-14");

    /** The factor each double estimate of a quotient is scaled by before it is cut: 1 - 2^-44, so that no estimate
     *  exceeds the true quotient (the databases' rounding is fifty times smaller). */
    private static final double BELOW = 1 - 0x1p-44;

    /** The largest whole number Pure's {@code floor} of a double gives exactly (a BIGINT): 10^18. */
    private static final int FLOOR_DIGITS = 18;

    /**
     * The whole quotient of {@code a} by {@code b} (decimals, a >= 0, b > 0), below {@code bound}: exactly, from double
     * estimates. An estimate of the rest's quotient, (a - q * b) / b computed in doubles, scaled by {@link #BELOW} and
     * cut to a whole number (to a multiple of 10^c while it may exceed a BIGINT), never exceeds the true one, so q stays
     * at most the quotient and a - q * b at least 0; each estimate leaves at most {@link #SLACK} of the rest plus 10^c
     * unfound, so a quotient of 38 digits takes three, one of 13 or fewer one; the last leaves the quotient or one less,
     * which a comparison of the rest with b settles. The bounds are worked out from the types alone.
     */
    private static ValueSpecification whole(ValueSpecification a, ValueSpecification b, BigDecimal bound) {
        ValueSpecification q = null;
        BigDecimal left = bound;
        while (true) {
            ValueSpecification rest = q == null ? a : AppliedFunction.infixRun("minus", List.of(a, times(q, b)));
            BigDecimal most = left.add(BigDecimal.ONE);
            boolean last = most.multiply(SLACK).compareTo(BigDecimal.ONE) < 0;
            int c = last ? 0 : Math.max(0, most.toBigInteger().toString().length() - FLOOR_DIGITS);
            ValueSpecification estimate = times(SqlToPure.call("divide", rest, b), new CFloat(BELOW));
            if (c > 0) {
                estimate = SqlToPure.call("divide", estimate, new CFloat(Math.pow(10, c)));
            }
            ValueSpecification found = SqlToPure.call("toDecimal", SqlToPure.call("floor", estimate));
            if (c > 0) {
                found = times(found, new CDecimal(pow10(c)));
            }
            q = q == null ? found : AppliedFunction.infixRun("plus", List.of(q, found));
            if (last) {
                break;
            }
            left = most.multiply(SLACK).add(pow10(c)).setScale(0, java.math.RoundingMode.CEILING);
        }
        ValueSpecification rest = AppliedFunction.infixRun("minus", List.of(a, times(q, b)));
        return AppliedFunction.infixRun("plus", List.of(q, ifThen(SqlToPure.call("greaterThanEqual", rest, b),
                new CDecimal(BigDecimal.ONE), new CDecimal(BigDecimal.ZERO))));
    }

    /**
     * SQL Anywhere's NUMERIC remainder {@code x % y}: x - y * the quotient cut toward zero, exactly (the dividend's
     * sign; measured on 274 remainders), of scale max(s1, s2) -- x of scale {@code s1} with at most {@code whole1}
     * digits before its point, y of scale {@code s2} and {@code whole2}, neither NULL, y not 0. Pure's {@code rem} of the
     * two at one scale (both databases' mod is exact for two decimals of one scale; DuckDB's of two scales is a
     * double's); the caller refuses what {@link #remainderFits} says would need more than 38 digits.
     */
    static ValueSpecification remainder(ValueSpecification x, int s1, int whole1, ValueSpecification y, int s2,
            int whole2) {
        if (!remainderFits(s1, whole1, s2, whole2)) {
            throw new IllegalStateException("a remainder of more than 38 digits");
        }
        int scale = Math.max(s1, s2);
        return SqlToPure.call("rem", s1 < scale ? atScale(x, scale) : x, s2 < scale ? atScale(y, scale) : y);
    }

    private static ValueSpecification times(ValueSpecification a, ValueSpecification b) {
        return AppliedFunction.infixRun("times", List.of(a, b));
    }

    /**
     * IQ island, 2026-10-17 (the TPC-H corpus lane's 257 comparisons of a sum with a wide AVG): {@code y}, a decimal of
     * scale {@code sy} with at most {@code wy} digits before its point, at {@code s} places (fewer than sy), rounded up
     * (toward +infinity) or down, exactly: y less its remainder by 10^-s (toward zero), one step away where the
     * remainder points so, at scale s. So a comparison with a value of s places needs no decimal holding both scales.
     */
    static ValueSpecification placesToward(ValueSpecification y, int sy, int wy, int s, boolean up) {
        CDecimal unit = new CDecimal(pow10(-s));
        ValueSpecification r = remainder(y, sy, wy, unit, s, 1);
        ValueSpecification toward = AppliedFunction.infixRun("minus", List.of(y, r));
        ValueSpecification step = ifThen(up ? SqlToPure.call("greaterThan", r, integer(0)) : lt(r, integer(0)), unit,
                new CDecimal(BigDecimal.ZERO));
        return SqlToPure.call("round", AppliedFunction.infixRun(up ? "plus" : "minus", List.of(toward, step)),
                integer(s));
    }

    /** {@code MOD(a, b)} and {@code REMAINDER(a, b)}: IQ's {@code a % b} (the same function: the dividend's sign, a
     *  zero divisor IQ's error -628, IQ's types). */
    private Tx mod(String name, SqlAst.FunctionCall f, SqlToPure.At at) {
        return dialect.binary(new SqlAst.Binary(SqlAst.BinaryOp.MODULO, f.args().get(0), f.args().get(1), f.pos()),
                at);
    }

    /** The largest argument {@code EXP} gives a finite DOUBLE for (exp's of the next DOUBLE overflows). */
    private static final double EXP_LARGEST = 709.782712893384;

    /** The largest argument {@code EXP} gives 0 for (measured: SQL Anywhere's 0.0, where Postgres raises an
     *  underflow). */
    private static final double EXP_ZERO = -745.1332191019412;

    /**
     * {@code SQRT}, {@code EXP}, {@code LN}, {@code LOG} (natural: the research's SEMANTICS §3.6.2) and {@code LOG10}:
     * of the argument as a DOUBLE, a DOUBLE. Outside the domain (a negative root, the logarithm of 0 or less, an
     * exponential past the largest DOUBLE) SQL Anywhere's front end answers NULL (measured) and the IQ engine raises
     * an error (the research's D-F9): never a wrong value either way.
     */
    private Tx mathematical(Fn fn, String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx x = asDouble(args.get(0), f.args().get(0), name);
        boolean iq = engine == IqDialect.Engine.IQ;
        Tx r = strict(Kind.FLOAT, new IqType("DOUBLE"), List.of(x), v -> {
            ValueSpecification d = v.get(0);
            ValueSpecification outside = iq ? raise("Float") : SqlToPure.empty();
            return switch (fn) {
                case SQRT -> ifThen(lt(d, new CFloat(0.0)), outside, SqlToPure.call("sqrt", d));
                case LN, LOG -> ifThen(SqlToPure.call("lessThanEqual", d, new CFloat(0.0)), outside,
                        SqlToPure.call("log", d));
                case LOG10 -> ifThen(SqlToPure.call("lessThanEqual", d, new CFloat(0.0)), outside,
                        SqlToPure.call("log10", d));
                case EXP -> ifThen(SqlToPure.call("greaterThan", d, new CFloat(EXP_LARGEST)), outside,
                        ifThen(SqlToPure.call("lessThanEqual", d, new CFloat(EXP_ZERO)), new CFloat(0.0),
                                SqlToPure.call("exp", d)));
                default -> throw new IllegalStateException("not a function of a DOUBLE: " + fn);
            };
        });
        return Tx.value(r.vs(), Kind.FLOAT, true, r.detail());
    }

    /** The largest DOUBLE: a value past it is an overflow ({@link #iqPower}). */
    private static final double LARGEST = Double.MAX_VALUE;

    /**
     * {@code POWER(x, y)} under IQ's engine (IQ island, 2026-10-15, checklist 1.10, measured on IQ 17.1): x to the power
     * y, both as DOUBLEs, a DOUBLE; NULL where it has no DOUBLE -- a negative x to a power that is no integer, 0 to a
     * negative power, a value past the largest DOUBLE, one that underflows to 0 (a subnormal is kept: 2^-1074 is
     * 4.9e-324). 0 to the power 0 is 1. Each NULL is decided before the power is computed, by y times the logarithm
     * of |x| against the logarithms of the largest DOUBLE and of the smallest subnormal's half (Postgres raises an
     * overflow or an underflow where IQ answers NULL), and checked after (DuckDB answers Infinity and 0 there), so
     * that the tiny band where the logarithms' rounding decides is the database's error or IQ's NULL, never a value.
     * A string is IQ's -1009145 (refused); in SQL Anywhere's front end POWER stays refused.
     */
    private Tx iqPower(String name, SqlAst.FunctionCall f, List<Tx> args) {
        return power(asDouble(args.get(0), f.args().get(0), name), asDouble(args.get(1), f.args().get(1), name));
    }

    /**
     * {@code SQUARE(x)} (IQ island, 2026-10-17, the corpus lane; measured on IQ 17.1, its engine's and its front
     * end's alike): {@code POWER(x, 2)} -- IQ's own error for a string names POWER -- a DOUBLE, NULL past the largest
     * DOUBLE or where it underflows to 0 (SQUARE(1e-162)), a subnormal kept (SQUARE(3e-162) is 1e-323). In SQL
     * Anywhere's front end read in an iq17 profile only: SQL Anywhere 17, the other profiles' judge, answers Infinity
     * and 0 there, as its POWER does (refused by name).
     */
    private Tx square(String name, SqlAst.FunctionCall f, List<Tx> args) {
        if (engine != IqDialect.Engine.IQ && (settings.base() != IqSettings.Base.IQ16 || settings.latin1())) {
            throw SqlTranslationException.unsupported(name + "(...) is not supported yet in SQL Anywhere's front end"
                    + " in profile " + settings.spelling() + ": SQL Anywhere 17 answers +Infinity past the largest"
                    + " DOUBLE and 0 where it underflows, where IQ 17.1 answers NULL", f.pos());
        }
        return power(asDouble(args.get(0), f.args().get(0), name), Tx.value(new CFloat(2.0), Kind.FLOAT, false,
                new IqType("DOUBLE")));
    }

    /** x to the power y, DOUBLEs, as {@link #iqPower} reads it. */
    private Tx power(Tx x, Tx y) {
        Tx r = strict(Kind.FLOAT, new IqType("DOUBLE"), List.of(x, y), v -> {
            ValueSpecification b = v.get(0);
            ValueSpecification e = v.get(1);
            ValueSpecification none = SqlToPure.empty();
            ValueSpecification zero = new CFloat(0.0);
            ValueSpecification fraction = SqlToPure.call("and", lt(SqlToPure.call("abs", e), new CFloat(WHOLE)),
                    SqlToPure.call("not", SqlToPure.call("equal", e, SqlToPure.call("toFloat", SqlToPure.call("floor",
                            e)))));
            ValueSpecification logarithm = times(e, SqlToPure.call("log", SqlToPure.call("abs", b)));
            ValueSpecification power = SqlToPure.call("toFloat", SqlToPure.call("pow", b, e));
            ValueSpecification checked = ifThen(SqlToPure.call("or", SqlToPure.call("greaterThan", SqlToPure.call("abs",
                    power), new CFloat(LARGEST)), SqlToPure.call("equal", power, zero)), none, power);
            return ifThen(SqlToPure.call("equal", b, zero), ifThen(lt(e, zero), none, ifThen(SqlToPure.call("equal", e,
                    zero), new CFloat(1.0), zero)), ifThen(SqlToPure.call("and", lt(b, zero), fraction), none,
                    ifThen(SqlToPure.call("or", SqlToPure.call("greaterThan", logarithm, new CFloat(EXP_LARGEST
                            + 1e-9)), lt(logarithm, new CFloat(EXP_ZERO - 1e-9))), none, checked)));
        });
        return Tx.value(r.vs(), Kind.FLOAT, true, r.detail());
    }

    /**
     * {@code GREATER(a, b)} and {@code LESSER(a, b)} (IQ island, 2026-10-15, checklist 1.10, measured on IQ 17.1): the
     * greater (lesser) of the two, the second where neither is (two strings equal by the collation: 'x  ' and 'x'),
     * NULL where either is; of the type CASE unifies the two to (an INTEGER and a NUMERIC(10,2) a NUMERIC(12,2), a DATE
     * and a TIMESTAMP a TIMESTAMP). Read as that CASE: {@code CASE WHEN a IS NULL OR b IS NULL THEN NULL WHEN a > b
     * THEN a ELSE b END}, so a string beside a number is refused as CASE refuses it (IQ converts it, or raises).
     */
    private Tx greaterLesser(boolean greater, SqlAst.FunctionCall f, SqlToPure.At at) {
        SqlAst.Expr a = f.args().get(0);
        SqlAst.Expr b = f.args().get(1);
        SqlAst.Pos pos = f.pos();
        SqlAst.Expr eitherNull = new SqlAst.Binary(SqlAst.BinaryOp.OR, new SqlAst.IsNull(a, false, pos),
                new SqlAst.IsNull(b, false, pos), pos);
        SqlAst.Expr first = new SqlAst.Binary(greater ? SqlAst.BinaryOp.GT : SqlAst.BinaryOp.LT, a, b, pos);
        return at.tx(new SqlAst.Case(null, List.of(new SqlAst.When(eitherNull, new SqlAst.NullLit(pos)),
                new SqlAst.When(first, a)), b, pos));
    }

    /**
     * {@code ARGN(n, a1, ..., ak)} (IQ island, 2026-10-15, checklist 1.10, measured on IQ 17.1): the n-th of the values
     * after n, NULL for an n that is NULL, below 1 or past k; of the type CASE unifies them to (a SMALLINT and a
     * NUMERIC a NUMERIC, an INTEGER and a DOUBLE a DOUBLE; a string beside a number IQ's -1001020, refused as CASE
     * refuses it). Read as {@code CASE WHEN n = 1 THEN a1 ... WHEN n = k THEN ak END}.
     */
    private Tx argn(SqlAst.FunctionCall f, SqlToPure.At at) {
        SqlAst.Expr n = f.args().get(0);
        List<SqlAst.When> whens = new ArrayList<>();
        for (int i = 1; i < f.args().size(); i++) {
            whens.add(new SqlAst.When(new SqlAst.Binary(SqlAst.BinaryOp.EQ, n, new SqlAst.IntegerLit(BigInteger.valueOf(
                    i), f.pos()), f.pos()), f.args().get(i)));
        }
        return at.tx(new SqlAst.Case(null, whens, null, f.pos()));
    }

    /** {@code TO_CHAR(x [, charset])} (IQ island, 2026-10-17; BIRD's TO_CHAR(date, 'YYYY'), 19 queries, measured on IQ
     *  17.1): SQL Anywhere's conversion of a binary string to text by a character set, not other dialects' format --
     *  a date or a timestamp is IQ's -157 as it plans (no conversion to a binary string), over no rows too; a string
     *  alone is itself, a LONG VARCHAR. Refused by name: a character set, and a number (its bytes). */
    private Tx toChar(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx x = args.get(0);
        if (x.kind() == Kind.DATE || x.kind() == Kind.DATETIME) {
            // held: IQ raises it as it plans, after the statement's names
            dialect.hold(SqlTranslationException.invalid("cannot convert " + kindName(x.kind()) + " to long binary: "
                    + name + " converts a binary string by a character set, not by a format" + code(-157, "53018"),
                    f.pos()));
            return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, new IqType("LONG VARCHAR"));
        }
        if (args.size() == 1 && (x.kind() == Kind.STRING || x.kind() == Kind.NULL)) {
            // a LONG VARCHAR (measured on IQ 17.1: TO_CHAR('abc'))
            return x.kind() == Kind.NULL ? nullString() : Tx.value(x.vs(), Kind.STRING, x.nullable(),
                    new IqType("LONG VARCHAR"));
        }
        throw SqlTranslationException.unsupported(name + " of " + kindName(x.kind()) + (args.size() > 1 ? " by a"
                + " character set" : "") + " is not supported yet: SQL Anywhere's conversion of a binary string",
                f.pos());
    }

    /** {@code REVERSE(s)} (IQ island, 2026-10-15, checklist 1.10, measured on IQ 17.1): its characters in reverse, a
     *  CHAR(n) with its blanks (D-S1); under IQ's engine a value that is no string is its -1009145 (refused before),
     *  in SQL Anywhere's front end an integer is its digits. */
    private Tx reverse(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx s = text(args.get(0), f.args().get(0), name);
        if (s.kind() == Kind.NULL) {
            return nullString();
        }
        return strict(Kind.STRING, s.detail() instanceof IqType t && t.family().equals("VARCHAR") ? t : varchar(),
                List.of(s), v -> SqlToPure.call("reverseString", v.get(0)));
    }

    /**
     * {@code INSERTSTR(p, s, x)} (IQ island, 2026-10-15, checklist 1.10, measured on IQ 17.1): x inserted into s after
     * its p-th character -- at the start for 0, at the end for a negative p or one past s's end; NULL where any argument
     * is or |p| is past 32767. An integer s is its digits.
     */
    private Tx insertstr(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx p = count(args.get(0), f.args().get(0), name);
        Tx s = text(args.get(1), f.args().get(1), name);
        Tx x = text(args.get(2), f.args().get(2), name);
        if (p.kind() == Kind.NULL || s.kind() == Kind.NULL || x.kind() == Kind.NULL) {
            return nullString();
        }
        // no arithmetic on the position, a SMALLINT or an INTEGER (DuckDB adds 1 to a SMALLINT in 16 bits, Postgres's
        // left takes no BIGINT): at the end where it is negative or past the characters, else around the rest's
        Tx r = strict(Kind.STRING, varchar(), List.of(p, s, x), v -> {
            ValueSpecification at = v.get(0);
            ValueSpecification str = v.get(1);
            ValueSpecification ins = v.get(2);
            ValueSpecification length = SqlToPure.call("length", str);
            ValueSpecification inside = AppliedFunction.infixRun("plus", List.of(SqlToPure.call("left", str, at), ins,
                    SqlToPure.call("right", str, AppliedFunction.infixRun("minus", List.of(length, at)))));
            return ifThen(SqlToPure.call("or", SqlToPure.call("greaterThan", at, integer(32767)), lt(at, integer(-32767))),
                    SqlToPure.empty(), ifThen(SqlToPure.call("or", lt(at, integer(0)), SqlToPure.call(
                            "greaterThanEqual", at, length)), AppliedFunction.infixRun("plus", List.of(str, ins)),
                            inside));
        });
        return Tx.value(r.vs(), Kind.STRING, true, r.detail());
    }

    /**
     * {@code REGEXP_SUBSTR(s, pattern [, start [, occurrence]])} (IQ island, 2026-10-15, checklist 1.10, measured on IQ
     * 17.1): the first match of the pattern in s from the start-th character on (0 reads as 1; a negative start counts
     * from the end, -1 the last character), NULL where it has none, where the start is past either end (even for a
     * pattern that matches nothing), or where an argument is NULL; an occurrence of 0 is NULL. A pattern's {@code ^}
     * anchors at the start, its {@code .} and a negated class match a line break, its {@code $} the very end.
     * <p>
     * Read only where every database means what IQ means, refused by name otherwise: a constant pattern of
     * single-character atoms -- a character, an escaped metacharacter, {@code .}, a bracket class of characters and
     * ranges -- each with a greedy quantifier or none, {@code ^} first and {@code $} last ({@link #simplePattern});
     * alternation and groups (IQ matches leftmost-longest, DuckDB's RE2 leftmost-first), the backslash classes and the
     * POSIX classes (IQ's are Unicode's, RE2's ASCII's) and a lazy quantifier (IQ's -1135) are refused. An occurrence
     * past 1 is refused too: IQ counts the next from one character past the last match's start ('a1b22c333''s third
     * [0-9]+ is '2'). A CHAR(n)'s match is a CHAR(n), unpadded, as IQ types it; an integer is its digits.
     */
    private Tx regexpSubstr(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx s = text(args.get(0), f.args().get(0), name);
        if (!settings.caseSensitive()) {
            throw SqlTranslationException.unsupported(name + " in a case-insensitive database is not supported yet",
                    f.pos());
        }
        if (args.get(1).kind() == Kind.NULL) {
            return nullString();
        }
        if (!(f.args().get(1) instanceof SqlAst.StringLit lit)) {
            throw SqlTranslationException.unsupported(name + " with a pattern that is no constant is not supported yet",
                    f.args().get(1).pos());
        }
        String pattern = simplePattern(lit.value(), name, lit.pos());
        if (args.size() > 4) {
            throw SqlTranslationException.unsupported(name + " with an escape character is not supported yet",
                    f.args().get(4).pos());
        }
        if (args.size() > 3) {
            SqlAst.Expr occurrence = f.args().get(3);
            if (args.get(3).kind() == Kind.NULL || occurrence instanceof SqlAst.IntegerLit z && z.value().signum() == 0) {
                return nullString();
            }
            if (!(occurrence instanceof SqlAst.IntegerLit one && one.value().equals(BigInteger.ONE))) {
                throw SqlTranslationException.unsupported(name + " of an occurrence other than 1 is not supported: IQ"
                        + " counts the next from one character past the last match's start", occurrence.pos());
            }
        }
        Tx start = args.size() > 2 ? count(args.get(2), f.args().get(2), name)
                : Tx.value(integer(1), Kind.INTEGER, false, IqType.integer("INTEGER", BigInteger.ONE, BigInteger.ONE,
                        false));
        if (start.kind() == Kind.NULL || s.kind() == Kind.NULL) {
            return nullString();
        }
        // of s's type (a CHAR(n)'s match unpadded, as IQ answers it)
        Tx r = strict(Kind.STRING, s.detail() instanceof IqType t && (t.family().equals("VARCHAR")
                || t.family().equals("CHAR")) ? t : varchar(), List.of(s, start), v -> {
                    ValueSpecification str = v.get(0);
                    ValueSpecification p = v.get(1);
                    ValueSpecification length = SqlToPure.call("length", str);
                    // the start as a position from 1: a negative one from the end
                    ValueSpecification from = ifThen(SqlToPure.call("greaterThan", p, integer(0)), p, ifThen(
                            SqlToPure.call("equal", p, integer(0)), integer(1), AppliedFunction.infixRun("plus",
                                    List.of(length, p, integer(1)))));
                    ValueSpecification match = SqlToPure.call("first", SqlToPure.call("regexpExtract",
                            SqlToPure.call("substring", str, from), new CString("(?s)" + pattern),
                            new com.legend.protocol.spec.CBoolean(false)));
                    return ifThen(SqlToPure.call("or", lt(from, integer(1)), SqlToPure.call("greaterThan", from,
                            length)), SqlToPure.empty(), match);
                });
        return Tx.value(r.vs(), Kind.STRING, true, r.detail());
    }

    /** The REGEXP_SUBSTR pattern {@code p} as RE2, Postgres and Java read it alike, for the subset {@link #regexpSubstr}
     *  reads: the pattern itself; refused by name otherwise. */
    static String simplePattern(String p, String name, SqlAst.Pos pos) {
        String meta = ".[]()*+?{}|^$\\";
        int i = 0;
        int n = p.length();
        while (i < n) {
            char c = p.charAt(i);
            if (c == '^' && i == 0 || c == '$' && i == n - 1) {
                i++;
                continue;
            }
            // an atom
            if (c == '\\') {
                if (i + 1 >= n || meta.indexOf(p.charAt(i + 1)) < 0 && p.charAt(i + 1) != '-' && p.charAt(i + 1) != '/') {
                    throw refusedPattern(name, pos, "a backslash class or escape (IQ's classes are Unicode's)");
                }
                i += 2;
            } else if (c == '[') {
                int j = i + 1;
                if (j < n && p.charAt(j) == '^') {
                    j++;
                }
                if (j >= n || p.charAt(j) == ']') {
                    throw refusedPattern(name, pos, "an empty class or one that starts with ']'");
                }
                while (j < n && p.charAt(j) != ']') {
                    char k = p.charAt(j);
                    if (k == '[' || k == '\\') {
                        throw refusedPattern(name, pos, "a POSIX class or an escape in a bracket class");
                    }
                    j++;
                }
                if (j >= n) {
                    throw refusedPattern(name, pos, "a class without its ']' (IQ's error -1135)");
                }
                i = j + 1;
            } else if (meta.indexOf(c) >= 0 && c != '.') {
                throw refusedPattern(name, pos, "'" + c + "' (alternation, a group, a quantifier with nothing before"
                        + " it, or an anchor inside: IQ matches leftmost-longest, DuckDB leftmost-first)");
            } else {
                i++;
            }
            // its quantifier, greedy
            if (i < n && (p.charAt(i) == '*' || p.charAt(i) == '+' || p.charAt(i) == '?')) {
                i++;
            } else if (i < n && p.charAt(i) == '{') {
                int j = i + 1;
                boolean comma = false;
                boolean digits = false;
                while (j < n && (Character.isDigit(p.charAt(j)) && p.charAt(j) < 128 || p.charAt(j) == ',' && !comma)) {
                    comma |= p.charAt(j) == ',';
                    digits |= p.charAt(j) != ',';
                    j++;
                }
                if (j >= n || p.charAt(j) != '}' || !digits) {
                    throw refusedPattern(name, pos, "a brace that is no quantifier");
                }
                i = j + 1;
            }
            if (i < n && (p.charAt(i) == '?' || p.charAt(i) == '+' || p.charAt(i) == '*' || p.charAt(i) == '{')) {
                throw refusedPattern(name, pos, "a lazy, possessive or doubled quantifier (IQ's error -1135)");
            }
        }
        return p;
    }

    private static SqlTranslationException refusedPattern(String name, SqlAst.Pos pos, String what) {
        return SqlTranslationException.unsupported(name + " with " + what + " in its pattern is not supported", pos);
    }

    /** {@code BYTE_LENGTH(s)} (IQ island, 2026-10-15, checklist 1.10, measured on IQ 17.1): s's length in bytes -- in a
     *  UTF-8 database each character's ({@link #utf8Units}), in ISO-8859-1 its characters --, an INTEGER; an integer
     *  column's storage width under IQ's engine ({@link #iqStorageWidth}). Refused by name: any other value that is no
     *  string (IQ answers its storage width: a NUMERIC(10,2)'s 8, a DATE's 4). */
    private Tx byteLength(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx s = args.get(0);
        IqType type = IqType.integer("INTEGER", BigInteger.ZERO, IqType.INTEGER_MAX, false);
        if (s.kind() == Kind.NULL) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, type);
        }
        if (s.kind() != Kind.STRING) {
            Tx width = engine == IqDialect.Engine.IQ ? iqStorageWidth(name, f, s) : null;
            if (width == null) {
                throw SqlTranslationException.unsupported(name + " of " + kindName(s.kind()) + " is not supported yet:"
                        + " IQ answers its storage width", f.args().get(0).pos());
            }
            return width;
        }
        boolean utf8 = !settings.latin1();
        return strict(Kind.INTEGER, type, List.of(s), v -> SqlToPure.call("length", utf8 ? utf8Units(v.get(0))
                : v.get(0)));
    }

    /**
     * {@code DATALENGTH(x)} (IQ island, 2026-10-15; the corpus lane, measured on IQ 17.1): a string's length in bytes,
     * as {@link #byteLength} (a CHAR(n) column's n, its value blank padded under IQ's engine); under IQ's engine a
     * column of any other type its storage width -- BIT 1, SMALLINT 2, INTEGER and DATE 4, BIGINT, DOUBLE and TIMESTAMP
     * 8, a NUMERIC of at most 4 digits 2, of at most 9 4, of at most 18 8; NULL for NULL. Refused by name: a value
     * that is no string and no column (IQ types a computed integer by its data: DATALENGTH(id + 1) is 2), a NUMERIC
     * of more digits.
     */
    private Tx dataLength(String name, SqlAst.FunctionCall f, List<Tx> args) {
        Tx x = args.get(0);
        if (x.kind() == Kind.STRING || x.kind() == Kind.NULL) {
            return byteLength(name, f, args);
        }
        SqlAst.Expr e = f.args().get(0);
        Integer width = null;
        if (engine == IqDialect.Engine.IQ && e instanceof SqlAst.ColumnRef && x.detail() instanceof IqType t) {
            String family = t.family();
            width = STORAGE_WIDTHS.get(family);
            if (width == null && (x.kind() == Kind.DECIMAL || family.equals("NUMERIC")) && t.size() > 0) {
                width = t.size() <= 4 ? 2 : t.size() <= 9 ? 4 : t.size() <= 18 ? 8 : null;
            } else if (width == null) {
                width = FIXED_WIDTHS.get(family);
            }
        }
        if (width == null) {
            throw SqlTranslationException.unsupported(name + " of " + kindName(x.kind()) + " that is no column, or of"
                    + " this type, is not supported yet: IQ answers its storage width", e.pos());
        }
        BigInteger w = BigInteger.valueOf(width);
        long bytes = width;
        return strict(Kind.INTEGER, IqType.integer("INTEGER", w, w, false), List.of(x), v -> integer(bytes));
    }

    /** The other fixed types' storage widths under IQ's engine, in bytes ({@link #dataLength}). */
    private static final Map<String, Integer> FIXED_WIDTHS = Map.of("DOUBLE", 8, "DATE", 4, "TIMESTAMP", 8);

    // ------------------------------------------------------------------ dates

    /** The months' and the weekdays' names, as DATENAME writes them. */
    private static final String[] MONTHS = {"January", "February", "March", "April", "May", "June", "July", "August",
        "September", "October", "November", "December"};
    private static final String[] WEEKDAYS = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
        "Saturday"};

    private static final String DURATION_UNIT = "meta::pure::functions::date::DurationUnit";
    private static final String DAY_OF_WEEK = "meta::pure::functions::date::DayOfWeek";

    /** A date argument: a DATE or a TIMESTAMP, or a constant string in ISO spelling (IQ reads many more spellings,
     *  by the profile's date order: refused by name; under IQ's engine a time alone, on 1900-01-01); null for a
     *  NULL. */
    private @com.legend.base.Nullable Tx dateArgument(Tx x, SqlAst.Expr e, String name) {
        if (x.kind() == Kind.NULL) {
            return null;
        }
        if (x.kind() == Kind.DATE || x.kind() == Kind.DATETIME) {
            return x;
        }
        if (x.kind() == Kind.INTEGER && (name.equals("DATEADD") || name.equals("DATEDIFF"))) {
            // IQ island, 2026-10-15 (the corpus lane: T-SQL's DATEADD(dd, DATEDIFF(dd, 0, GETDATE()), 0) in 374 of
            // SEDE's and SQLStorm's queries; measured on IQ 17.1): under IQ's engine an integer there is its error
            // -1001011 whenever the statement runs (no row need reach it); in its front end the integer 0 is -157
            if (engine == IqDialect.Engine.IQ) {
                throw SqlTranslationException.unsupported(name + " of an integer where a date is read is IQ's error"
                        + " -1001011 under its engine (an argument must be a DATE or a TIMESTAMP), refused before"
                        + " translating", e.pos());
            }
            BigInteger zero = signedInteger(e);
            if (zero != null && zero.signum() == 0) {
                throw SqlTranslationException.invalid("cannot convert 0 to a date" + IqDialect.code(-157, "53018"),
                        e.pos());
            }
        }
        if (x.kind() == Kind.STRING && e instanceof SqlAst.StringLit s) {
            String[] parts = isoParts(s.value(), e);
            String alone = parts == null && engine == IqDialect.Engine.IQ ? IqDateTime.timeAlone(s.value()) : null;
            if (alone != null) {
                // IQ island, 2026-10-16 (measured on IQ 17.1: DATE, DATETIME, DATEPART, YEAR, DATEADD, DATEDIFF,
                // DATEFORMAT, DAYNAME): a time alone, on 1900-01-01 under IQ's engine
                parts = new String[] {"1900-01-01", alone};
            }
            if (parts != null) {
                String body = parts[0] + "T" + parts[1];
                return Tx.value(new CDate(PureDateLiteral.parse(body), body, null), Kind.DATETIME, false,
                        dated("TIMESTAMP", Integer.parseInt(parts[0].substring(0, 4))));
            }
        }
        if (x.kind() == Kind.STRING && engine == IqDialect.Engine.IQ) {
            // IQ island, 2026-10-17 (BIRD's DATE(SUBSTR(s, 1, 10)); measured on IQ 17.1): DATE, DATEPART,
            // YEAR and DATEADD read a computed string as CAST to TIMESTAMP reads it (stringDate), their -1001006 alike
            return stringDate(x, false, dated("TIMESTAMP", null));
        }
        throw SqlTranslationException.unsupported(name + " of " + kindName(x.kind()) + " is not supported yet: IQ"
                + " converts it to a date by its own rules (a constant string in a spelling of IQ's the island reads is"
                + " read)", e.pos());
    }

    /** {@code YEAR}, {@code MONTH}, {@code DAY}, {@code HOUR}, {@code MINUTE}, {@code SECOND}, {@code QUARTER},
     *  {@code DOW} (Sunday 1: SQL Anywhere's never moves; IQ's moves with DATE_FIRST_DAY_OF_WEEK, no profile's), and
     *  the DATEPART parts below: a SMALLINT (QUARTER an INTEGER). */
    private Tx datePartOf(Part part, String name, SqlAst.FunctionCall f, Tx x, SqlAst.Expr e, boolean small,
            String weekStart) {
        Tx d = dateArgument(x, e, name);
        String family = small && part != Part.QUARTER ? "SMALLINT" : "INTEGER";
        if (d == null) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, new IqType(family));
        }
        boolean date = d.kind() == Kind.DATE;
        IqType type = IqType.integer(family, BigInteger.valueOf(part.low), BigInteger.valueOf(part.high), true);
        return strict(Kind.INTEGER, type, List.of(d), v -> partValue(part, v.get(0), date, weekStart));
    }

    /**
     * The date parts, as the reader names them (its lowercase full name: {@code yy} is {@code year}), with the values
     * each takes. The CAL parts are ISO 8601's, DOW is Sunday 1; TZOFFSET only reaches a refusal.
     */
    private enum Part {
        YEAR(1, 9999), QUARTER(1, 4), MONTH(1, 12), WEEK(1, 54), DAY(1, 31), DAYOFYEAR(1, 366), WEEKDAY(1, 7),
        HOUR(0, 23), MINUTE(0, 59), SECOND(0, 59), MILLISECOND(0, 999), MICROSECOND(0, 999_999),
        CALYEAROFWEEK(1, 9999), CALWEEKOFYEAR(1, 54), CALDAYOFWEEK(1, 7), DOW(1, 7), TZOFFSET(0, 0);

        final long low;
        final long high;

        Part(long low, long high) {
            this.low = low;
            this.high = high;
        }

        /** The part the reader's lowercase {@code name} names. */
        static Part of(String name) {
            for (Part p : values()) {
                if (p.name().toLowerCase(Locale.ROOT).equals(name)) {
                    return p;
                }
            }
            throw new IllegalStateException("no date part " + name);
        }

        boolean calendar() {
            return this == CALYEAROFWEEK || this == CALWEEKOFYEAR || this == CALDAYOFWEEK;
        }
    }

    /** A date part's value of {@code d} (a DATE when {@code date}, its time midnight); the weekday counted from
     *  {@code weekStart} (a DayOfWeek's name: Sunday's but for DATEPART's under first_day_of_week). */
    private static ValueSpecification partValue(Part part, ValueSpecification d, boolean date, String weekStart) {
        return switch (part) {
            case YEAR -> SqlToPure.call("year", d);
            case QUARTER -> SqlToPure.call("quarterNumber", d);
            case MONTH -> SqlToPure.call("monthNumber", d);
            case DAY -> SqlToPure.call("dayOfMonth", d);
            case DAYOFYEAR -> SqlToPure.call("dayOfYear", d);
            // a week from Monday or Sunday is Pure's own; from any other day, the Sunday-based number moved round
            case WEEKDAY -> weekStart.equals("Sunday") || weekStart.equals("Monday")
                    ? SqlToPure.call("dayOfWeekNumber", d, new EnumValue(DAY_OF_WEEK, weekStart))
                    : AppliedFunction.infixRun("plus", List.of(SqlToPure.call("mod", AppliedFunction.infixRun("plus",
                            List.of(SqlToPure.call("dayOfWeekNumber", d, new EnumValue(DAY_OF_WEEK, "Sunday")),
                                    integer(6 - weekDay(weekStart)))), integer(7)), integer(1)));
            case DOW -> SqlToPure.call("dayOfWeekNumber", d, new EnumValue(DAY_OF_WEEK, "Sunday"));
            // weeks from Sunday, week 1 the one holding 1 January: (day of the year - 1 + Sunday-based weekday of
            // 1 January - 1) / 7 + 1
            case WEEK -> AppliedFunction.infixRun("plus", List.of(wholeQuotient(
                    AppliedFunction.infixRun("plus", List.of(SqlToPure.call("dayOfYear", d), SqlToPure.call(
                            "dayOfWeekNumber", SqlToPure.call("firstDayOfYear", d), new EnumValue(DAY_OF_WEEK,
                                    "Sunday")), integer(-2))), 7), integer(1)));
            case HOUR -> date ? integer(0) : SqlToPure.call("hour", d);
            case MINUTE -> date ? integer(0) : SqlToPure.call("minute", d);
            case SECOND -> date ? integer(0) : SqlToPure.call("second", d);
            case MILLISECOND -> date ? integer(0) : wholeQuotient(microseconds(d), 1000);
            case MICROSECOND -> date ? integer(0) : microseconds(d);
            case CALDAYOFWEEK -> SqlToPure.call("dayOfWeekNumber", d);
            case CALWEEKOFYEAR -> SqlToPure.call("weekOfYear", d);
            // the ISO year: the year of the week's Thursday
            case CALYEAROFWEEK -> SqlToPure.call("year", SqlToPure.call("adjust", d, AppliedFunction.infixRun(
                    "minus", List.of(integer(4), SqlToPure.call("dayOfWeekNumber", d))), new EnumValue(DURATION_UNIT,
                    "DAYS")));
            case TZOFFSET -> throw new IllegalStateException("no value of TZOFFSET");
        };
    }

    /** The microseconds of a timestamp's second: its epoch microseconds modulo a million, from 0. */
    private static ValueSpecification microseconds(ValueSpecification d) {
        ValueSpecification epoch = SqlToPure.call("toEpochValue", d, new EnumValue(DURATION_UNIT, "MICROSECONDS"));
        CInteger million = integer(1_000_000);
        return SqlToPure.call("mod", AppliedFunction.infixRun("plus", List.of(SqlToPure.call("mod", epoch, million),
                million)), million);
    }

    /** {@code DATEPART(part, x)}, an INTEGER, and {@code DATENAME(part, x)}, a VARCHAR: the month's and the weekday's
     *  names, any other part's digits. The ISO parts (cdw, cwk, cyr) are SQL Anywhere's; IQ's manual's are
     *  Sunday-based (the research's D-T10): refused under the IQ engine. TZOFFSET is refused. */
    private Tx datePartCall(boolean name, String fnName, SqlAst.FunctionCall f, List<Tx> args) {
        Part part = Part.of(((SqlAst.StringLit) f.args().get(0)).value());
        if (part == Part.TZOFFSET) {
            throw SqlTranslationException.unsupported(fnName + "(TZOFFSET, ...) is not supported: lite reads no time"
                    + " zones", f.pos());
        }
        if (part.calendar() && engine == IqDialect.Engine.IQ) {
            return iqCalendarPart(name, part, fnName, f, args);
        }
        // IQ island, 2026-10-14 (the options framework): DATEPART's weekday counts from first_day_of_week (1 Monday
        // to 7 Sunday; measured on SQL Anywhere 17: no other part moves, nor DATENAME's weekday, nor DOW); under the IQ
        // engine, which documents DATE_FIRST_DAY_OF_WEEK instead, a value other than 7 is refused by name
        int first = settings.firstDayOfWeek();
        if (!name && part == Part.WEEKDAY && first != 7 && engine == IqDialect.Engine.IQ) {
            throw SqlTranslationException.unsupported(fnName + "(WEEKDAY, ...) under first_day_of_week " + first
                    + " is not supported under the IQ engine: IQ documents DATE_FIRST_DAY_OF_WEEK for it (the"
                    + " research's D-T9, not yet verified)", f.pos());
        }
        Tx value = datePartOf(part, fnName, f, args.get(1), f.args().get(1), false, name ? "Sunday"
                : WEEK_STARTS[first - 1]);
        if (!name) {
            return value;
        }
        if (value.kind() == Kind.NULL || SqlToPure.isNullConstant(value)) {
            return nullString();
        }
        boolean month = part == Part.MONTH;
        boolean weekday = part == Part.WEEKDAY;
        Tx r = SqlToPure.strict(Kind.STRING, List.of(value), v -> month ? names(v.get(0), MONTHS)
                : weekday ? names(v.get(0), WEEKDAYS) : SqlToPure.call("toString", v.get(0)));
        return Tx.value(r.vs(), Kind.STRING, r.nullable(), varchar());
    }

    /** {@code day}'s number as first_day_of_week numbers it: Monday 1 to Sunday 7. */
    private static int weekDay(String day) {
        for (int i = 0; i < WEEK_STARTS.length; i++) {
            if (WEEK_STARTS[i].equals(day)) {
                return i + 1;
            }
        }
        throw new IllegalArgumentException("no day " + day);
    }

    /** The days first_day_of_week names, 1 to 7, as Pure's DayOfWeek names them. */
    private static final String[] WEEK_STARTS = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
        "Sunday"};

    /** The name of number {@code n} (from 1) among {@code names}. */
    private static ValueSpecification names(ValueSpecification n, String[] names) {
        ValueSpecification v = new CString(names[names.length - 1]);
        for (int i = names.length - 2; i >= 0; i--) {
            v = ifThen(SqlToPure.call("equal", n, integer(i + 1)), new CString(names[i]), v);
        }
        return v;
    }

    /** {@code DATE(x)}: a timestamp's date, a date itself. */
    private Tx dateOf(String name, SqlAst.FunctionCall f, Tx x, SqlAst.Expr e) {
        Tx d = dateArgument(x, e, name);
        if (d == null) {
            return Tx.value(SqlToPure.nullOf(Kind.DATE).vs(), Kind.DATE, true, new IqType("DATE"));
        }
        if (d.kind() == Kind.DATE) {
            return d;
        }
        return strict(Kind.DATE, dated("DATE", latestYear(d)), List.of(d), v -> typed(SqlToPure.call("datePart",
                v.get(0)), "StrictDate"));
    }

    /** A timestamp of {@code d} (a DATE: its midnight). */
    /** IQ island, 2026-10-15: the TIMESTAMP of date {@code d}'s midnight. */
    static ValueSpecification midnight(ValueSpecification d) {
        return timestamp(d, true);
    }

    private static ValueSpecification timestamp(ValueSpecification d, boolean date) {
        return date ? SqlToPure.call("date", SqlToPure.call("year", d), SqlToPure.call("monthNumber", d),
                SqlToPure.call("dayOfMonth", d), integer(0), integer(0), integer(0)) : d;
    }

    /**
     * {@code DATEADD(part, n, x)} in SQL Anywhere's front end: a TIMESTAMP, x moved by n parts (a month's end clamped:
     * '2026-01-31' plus a month is 2026-02-28; a quarter is three months, a week seven days, a day of the year or a
     * weekday a day). Refused by name: under the IQ engine (its result keeps a DATE: the research's D-T2, D-T12), a
     * MILLISECOND (SQL Anywhere cuts the timestamp to milliseconds first: its defect, D-T3), the CAL parts (no-ops in
     * SQL Anywhere), TZOFFSET, a count that is no integer.
     */
    private Tx dateAdd(String name, SqlAst.FunctionCall f, List<Tx> args) {
        boolean iq = engine == IqDialect.Engine.IQ;
        Part part = Part.of(((SqlAst.StringLit) f.args().get(0)).value());
        if (iq && (part == Part.DAYOFYEAR || part == Part.WEEKDAY)) {
            throw iqDatePart(name, part, f);
        }
        String unit;
        long times = 1;
        switch (part) {
            case YEAR -> unit = "YEARS";
            case QUARTER -> {
                unit = "MONTHS";
                times = 3;
            }
            case MONTH -> unit = "MONTHS";
            case WEEK -> {
                unit = "DAYS";
                times = 7;
            }
            case DAY, DAYOFYEAR, WEEKDAY -> unit = "DAYS";
            case HOUR -> unit = "HOURS";
            case MINUTE -> unit = "MINUTES";
            case SECOND -> unit = "SECONDS";
            case MICROSECOND -> unit = "MICROSECONDS";
            case MILLISECOND -> throw SqlTranslationException.unsupported(name + "(MILLISECOND, ...) is not supported:"
                    + " SQL Anywhere cuts the timestamp to milliseconds first (its defect; the research's D-T3)",
                    f.pos());
            default -> throw SqlTranslationException.unsupported(name + "(" + part + ", ...) is not supported",
                    f.pos());
        }
        Tx n = args.get(1);
        IqType count = null;
        if (n.kind() != Kind.NULL) {
            count = integerType(n, f.args().get(1), name);
            // IQ island, 2026-10-14 (review 3's B4, measured): SQL Anywhere wraps some counts past IQ's range
            // instead of raising -158 (DAY 1,500,000,000 is 2598-05-12, WEEK 200,000,000 is 2408-03-25, QUARTER
            // 2,000,000,000 is 6036-09-01, YEAR -2,147,483,648 leaves the date as it is): its defect, refused by name
            BigInteger bound = WRAPS.get(part);
            if (bound != null && (count.hi().compareTo(bound) > 0 || count.lo().compareTo(bound.negate()) < 0)) {
                // IQ island, 2026-10-17 (the corpus lane: DATEADD(WEEK, DATEDIFF(WEEK, ...), ...), 10 queries): a
                // count whose type may pass the bound, a computed one's, is refused by name where the row computes
                // one past it; its others are read
                String past = IqDialect.REFUSED_AT_RUN_TIME + name + "(" + part + ", n, ...) with a count past " + bound
                        + " is not supported: SQL Anywhere wraps such a count rather than raising its -158 (its defect)";
                Tx checked = SqlToPure.strict(Kind.INTEGER, List.of(n), v -> refusedPast(SqlToPure.call("or",
                        SqlToPure.call("greaterThan", v.get(0), SqlToPure.integer(bound)), lt(v.get(0),
                        SqlToPure.integer(bound.negate()))), past, v.get(0), IqDialect.raise()));
                count = IqType.integer(count.family(), bound.negate(), bound, false);
                n = Tx.value(checked.vs(), Kind.INTEGER, checked.nullable(), count);
            }
        }
        Tx d = dateArgument(args.get(2), f.args().get(2), name);
        IqType type = new IqType("TIMESTAMP");
        if (d == null || n.kind() == Kind.NULL) {
            return Tx.value(SqlToPure.nullOf(Kind.DATETIME).vs(), Kind.DATETIME, true, type);
        }
        // IQ island, 2026-10-14 (review 4's M2, measured): from the year 7911 on SQL Anywhere 17 drops a timestamp's
        // hours and minutes (9999-12-31 22:30 plus an hour is 9999-12-31 00:00, 2026-01-01 10:30 plus 6000 years
        // 8026-01-01 00:00; and past 9999-12-31 it answers rather than raising): its defect, the study's §2.5. A result
        // that may reach those years with a time of day -- a part below a day, or a timestamp that may not be midnight
        // -- is refused by name; the latest year a date may reach is carried in its type (IqType.high)
        Integer latest = latestYear(d);
        int end = latest == null ? 9999 : latest + forwardYears(part, times, java.util.Objects.requireNonNull(count));
        boolean subDay = part == Part.HOUR || part == Part.MINUTE || part == Part.SECOND
                || part == Part.MICROSECOND;
        // a column's date of no known year is a stored one (a computed date carries its year): the parity schema holds
        // none past 7910 (the study's §2.5), and lite's answer there is the calendar's
        boolean stored = latest == null && f.args().get(2) instanceof SqlAst.ColumnRef;
        if (!stored && end > IqDialect.LAST_TIMED_YEAR && (subDay || !midnight(d))) {
            throw SqlTranslationException.unsupported(name + "(" + part + ", ...) that may reach a timestamp after the"
                    + " year " + IqDialect.LAST_TIMED_YEAR + " is not supported: SQL Anywhere 17 drops the hours and"
                    + " minutes of such a timestamp (its defect), so no answer of it judges lite's", f.pos());
        }
        type = dated("TIMESTAMP", Math.min(end, 9999));
        boolean date = d.kind() == Kind.DATE;
        long by = times;
        if (iq && date) {
            // IQ island, 2026-10-15 (D-T2, D-T12, measured on IQ 17.1): under IQ's engine a DATE moved stays a DATE --
            // by a month, the month's end clamped as SQL Anywhere's is; by hours, the day the time reaches (DATEADD(
            // HOUR, 25, '2026-03-04') is 2026-03-05)
            IqType day = dated("DATE", Math.min(end, 9999));
            String u = unit;
            return strict(Kind.DATE, day, List.of(n, d), v -> {
                ValueSpecification moved = typed(SqlToPure.call("adjust", timestamp(v.get(1), true), by == 1
                        ? v.get(0) : AppliedFunction.infixRun("times", List.of(v.get(0), integer(by))),
                        new EnumValue(DURATION_UNIT, u)), "DateTime");
                ValueSpecification year = SqlToPure.call("year", moved);
                return ifThen(SqlToPure.call("or", SqlToPure.call("greaterThan", year, integer(9999)), lt(year,
                        integer(1))), raise("StrictDate"), typed(SqlToPure.call("datePart", moved), "StrictDate"));
            });
        }
        // adjust is of a Date: here of a DateTime, so one; a result outside IQ's years 1 to 9999 is IQ's -158
        // (IQ island, 2026-10-14, review 3's B4, measured: DATEADD(DAY, 1, '9999-12-31'), DATEADD(YEAR, -2026,
        // '2026-01-01'))
        return strict(Kind.DATETIME, type, List.of(n, d), v -> {
            ValueSpecification moved = typed(SqlToPure.call("adjust", timestamp(v.get(1), date), by == 1 ? v.get(0)
                    : AppliedFunction.infixRun("times", List.of(v.get(0), integer(by))), new EnumValue(DURATION_UNIT,
                    unit)), "DateTime");
            ValueSpecification year = SqlToPure.call("year", moved);
            return ifThen(SqlToPure.call("or", SqlToPure.call("greaterThan", year, integer(9999)), lt(year,
                    integer(1))), raise("DateTime"), moved);
        });
    }

    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1): date arithmetic under IQ's engine. A DATE or a TIMESTAMP plus or
     * minus a number moves by the number's whole days, cut toward zero ({@code dt + 1.9} is the next day, {@code ts -
     * 0.25} the same timestamp), and keeps its type; a date or a timestamp minus another is the INTEGER of the days
     * between their dates, a time of day not read (05:06 minus 23:00 the day before is 1). A result outside the years 1
     * to 9999 is IQ's -1001010. Refused by name: a number minus a date, two dates added, a date multiplied or divided,
     * an untyped NULL (each IQ's -1009145), a day count that may pass an INTEGER (IQ wraps it).
     */
    Tx dateArithmetic(SqlAst.Binary b, Tx l, Tx r) {
        boolean ld = l.kind() == Kind.DATE || l.kind() == Kind.DATETIME;
        boolean rd = r.kind() == Kind.DATE || r.kind() == Kind.DATETIME;
        if (b.op() == SqlAst.BinaryOp.MINUS && ld && rd) {
            boolean lDate = l.kind() == Kind.DATE;
            boolean rDate = r.kind() == Kind.DATE;
            IqType days = IqType.integer("INTEGER", IqType.INTEGER_MIN, IqType.INTEGER_MAX, true);
            return strict(Kind.INTEGER, days, List.of(l, r), v -> SqlToPure.call("dateDiff", day(v.get(1), rDate),
                    day(v.get(0), lDate), new EnumValue(DURATION_UNIT, "DAYS")));
        }
        boolean minus = b.op() == SqlAst.BinaryOp.MINUS;
        if (!(b.op() == SqlAst.BinaryOp.PLUS && ld != rd || minus && ld)) {
            throw SqlTranslationException.unsupported("this " + (minus ? "-" : b.op() == SqlAst.BinaryOp.PLUS ? "+"
                    : b.op().name()) + " of a date is IQ's error -1009145 under its engine (a date moves by a number"
                    + " of days, or two dates subtract), refused before translating", b.pos());
        }
        Tx d = ld ? l : r;
        Tx n = ld ? r : l;
        SqlAst.Expr ne = ld ? b.right() : b.left();
        if (n.kind() == Kind.NULL) {
            throw SqlTranslationException.unsupported("a date and an untyped NULL is IQ's error -1009145 under its"
                    + " engine, refused before translating", ne.pos());
        }
        boolean whole = n.kind() == Kind.INTEGER;
        if (whole) {
            IqType t = integerType(n, ne, minus ? "-" : "+");
            if (t.lo().compareTo(IqType.INTEGER_MIN) < 0 || t.hi().compareTo(IqType.INTEGER_MAX) > 0) {
                throw SqlTranslationException.unsupported("a date moved by a day count that may pass an INTEGER is not"
                        + " supported: IQ wraps such a count", ne.pos());
            }
        } else if (n.kind() != Kind.DECIMAL && n.kind() != Kind.NUMBER && n.kind() != Kind.FLOAT) {
            throw SqlTranslationException.unsupported("a date moved by " + kindName(n.kind()) + " is not supported"
                    + " yet", ne.pos());
        }
        boolean date = d.kind() == Kind.DATE;
        IqType type = dated(date ? "DATE" : "TIMESTAMP", null);
        return strict(d.kind(), type, List.of(d, n), v -> {
            ValueSpecification count = whole ? v.get(1) : ifThen(SqlToPure.call("greaterThanEqual", v.get(1),
                    integer(0)), SqlToPure.call("floor", v.get(1)), SqlToPure.call("ceiling", v.get(1)));
            ValueSpecification moved = typed(SqlToPure.call("adjust", timestamp(v.get(0), date), minus
                    ? SqlToPure.call("minus", count) : count, new EnumValue(DURATION_UNIT, "DAYS")), "DateTime");
            ValueSpecification year = SqlToPure.call("year", moved);
            return ifThen(SqlToPure.call("or", SqlToPure.call("greaterThan", year, integer(9999)), lt(year,
                    integer(1))), raise(date ? "StrictDate" : "DateTime"), date ? typed(SqlToPure.call("datePart",
                    moved), "StrictDate") : moved);
        });
    }

    /** The latest year {@code d} (a DATE or a TIMESTAMP) may be in: a constant's own, else the one its type carries
     *  ({@link #dated}); null when it is not known. IQ island, 2026-10-14 (review 4's M2). */
    static @com.legend.base.Nullable Integer latestYear(Tx d) {
        if (SqlToPure.isNullConstant(d)) {
            // no value: no year
            return 1;
        }
        if (d.vs() instanceof CDate c && c.written() != null && java.util.Objects.requireNonNull(c.written()).length()
                >= 4) {
            return Integer.parseInt(java.util.Objects.requireNonNull(c.written()).substring(0, 4));
        }
        if (d.detail() instanceof IqType t && (t.family().equals("DATE") || t.family().equals("TIMESTAMP"))
                && t.high() != null) {
            return java.util.Objects.requireNonNull(t.high()).intValue();
        }
        return null;
    }

    /** A DATE or TIMESTAMP type whose values are in the year {@code latest} at the latest (null: not known). */
    static IqType dated(String spelling, @com.legend.base.Nullable Integer latest) {
        return new IqType(spelling, false, null, latest == null ? null : BigInteger.valueOf(latest));
    }

    /** Whether {@code d}'s time of day is midnight: a DATE's, or a timestamp constant's written so. */
    private static boolean midnight(Tx d) {
        if (d.kind() == Kind.DATE) {
            return true;
        }
        if (!(d.vs() instanceof CDate c) || c.written() == null) {
            return false;
        }
        String w = java.util.Objects.requireNonNull(c.written());
        for (int i = 10; i < w.length(); i++) {
            char ch = w.charAt(i);
            if (ch != 'T' && ch != ':' && ch != '.' && ch != '0') {
                return false;
            }
        }
        return true;
    }

    /** How many years at most {@code count} (of {@code times} {@code part}s each) moves a date forward. */
    private static int forwardYears(Part part, long times, IqType count) {
        BigInteger hi = count.hi().multiply(BigInteger.valueOf(times));
        if (hi.signum() <= 0) {
            return 0;
        }
        // the parts of a common year (a count of days of the shortest year moves the furthest)
        long perYear = switch (part) {
            case YEAR -> 1;
            case QUARTER, MONTH -> 12;
            case WEEK, DAY, DAYOFYEAR, WEEKDAY -> 365;
            case HOUR -> 8_760;
            case MINUTE -> 525_600;
            case SECOND -> 31_536_000;
            case MICROSECOND -> 31_536_000_000_000L;
            default -> throw new IllegalStateException("DATEADD reads no part " + part);
        };
        BigInteger[] qr = hi.divideAndRemainder(BigInteger.valueOf(perYear));
        BigInteger years = qr[1].signum() == 0 ? qr[0] : qr[0].add(BigInteger.ONE);
        return years.min(BigInteger.valueOf(10_000)).intValue();
    }

    /** The counts DATEADD's parts take before SQL Anywhere wraps one (measured; review 3's B4): a count beyond is
     *  refused. A MONTH, an HOUR, a MINUTE, a SECOND and a MICROSECOND of any INTEGER raise or answer rightly. */
    private static final java.util.Map<Part, BigInteger> WRAPS = java.util.Map.of(
            Part.DAY, BigInteger.valueOf(1_000_000_000L), Part.DAYOFYEAR, BigInteger.valueOf(1_000_000_000L),
            Part.WEEKDAY, BigInteger.valueOf(1_000_000_000L), Part.WEEK, BigInteger.valueOf(100_000_000L),
            Part.QUARTER, BigInteger.valueOf(1_500_000_000L), Part.YEAR, BigInteger.valueOf(2_147_483_647L));

    /**
     * {@code DATEDIFF(part, a, b)} in SQL Anywhere's front end (the research's SEMANTICS §2.2, measured): b - a in
     * boundaries crossed for YEAR, QUARTER, MONTH, WEEK (Sundays), DAY (midnights), MINUTE, MILLISECOND and
     * MICROSECOND; HOUR is the minute boundaries over 60 and SECOND the millisecond boundaries over 1000, truncated;
     * DAYOFYEAR is the difference of the two days of the year (2025-12-31 to 2026-01-01 is -364).
     * An INTEGER from YEAR to DAY, a BIGINT from HOUR down. Refused by name: under the IQ engine (whole elapsed units
     * and INTEGER there: the research's D-T11, D-T5), the weekday and CAL parts (SQL Anywhere answers 0), TZOFFSET.
     */
    private Tx dateDiff(String name, SqlAst.FunctionCall f, List<Tx> args) {
        boolean iq = engine == IqDialect.Engine.IQ;
        Part part = Part.of(((SqlAst.StringLit) f.args().get(0)).value());
        if (iq && (part == Part.DAYOFYEAR || part == Part.WEEKDAY)) {
            throw iqDatePart(name, part, f);
        }
        boolean big = switch (part) {
            case YEAR, QUARTER, MONTH, WEEK, DAY, DAYOFYEAR -> false;
            case HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND -> true;
            default -> throw SqlTranslationException.unsupported(name + "(" + part + ", ...) is not supported (SQL"
                    + " Anywhere answers 0 for the weekday and CAL parts)", f.pos());
        };
        Tx a = dateArgument(args.get(1), f.args().get(1), name);
        Tx b = dateArgument(args.get(2), f.args().get(2), name);
        IqType type = big ? IqType.integer("BIGINT", IqType.BIGINT_MIN, IqType.BIGINT_MAX, true)
                : IqType.integer("INTEGER", IqType.INTEGER_MIN, IqType.INTEGER_MAX, true);
        if (a == null || b == null) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, type);
        }
        boolean aDate = a.kind() == Kind.DATE;
        boolean bDate = b.kind() == Kind.DATE;
        return strict(Kind.INTEGER, type, List.of(a, b), v -> {
            ValueSpecification x = v.get(0);
            ValueSpecification y = v.get(1);
            return switch (part) {
                case YEAR -> minus(SqlToPure.call("year", y), SqlToPure.call("year", x));
                case QUARTER -> minus(months(y, 4), months(x, 4));
                case MONTH -> minus(months(y, 12), months(x, 12));
                case DAY -> SqlToPure.call("dateDiff", day(x, aDate), day(y, bDate), new EnumValue(DURATION_UNIT,
                        "DAYS"));
                // measured: the days of the year's difference, not the days'
                case DAYOFYEAR -> minus(SqlToPure.call("dayOfYear", y), SqlToPure.call("dayOfYear", x));
                // Sundays crossed: the days between the Sundays on or before each, in weeks
                case WEEK -> exactQuotient(AppliedFunction.infixRun("plus", List.of(minus(SqlToPure.call("dateDiff",
                        day(x, aDate), day(y, bDate), new EnumValue(DURATION_UNIT, "DAYS")), sunday(y)), sunday(x))),
                        7);
                // IQ island, 2026-10-15 (D-T11, measured on IQ 17.1): under IQ's engine the parts below a day count
                // whole elapsed units, cut toward zero (04:30:30 to 06:30:00 is 1 hour, 05:05:59.9 to 05:06:59.8 no
                // minute); the parts from a day up count boundaries crossed, as SQL Anywhere's front end does
                case MINUTE -> iq ? elapsed(x, aDate, y, bDate, 60_000_000L) : boundaries(x, aDate, y, bDate,
                        60_000_000L);
                case HOUR -> iq ? elapsed(x, aDate, y, bDate, 3_600_000_000L)
                        : truncatedQuotient(boundaries(x, aDate, y, bDate, 60_000_000L), 60);
                case MILLISECOND -> iq ? elapsed(x, aDate, y, bDate, 1000L) : boundaries(x, aDate, y, bDate, 1000L);
                case SECOND -> iq ? elapsed(x, aDate, y, bDate, 1_000_000L)
                        : truncatedQuotient(boundaries(x, aDate, y, bDate, 1000L), 1000);
                case MICROSECOND -> minus(epochMicros(y, bDate), epochMicros(x, aDate));
                default -> throw new IllegalStateException("no DATEDIFF part " + part);
            };
        });
    }

    private static ValueSpecification minus(ValueSpecification a, ValueSpecification b) {
        return AppliedFunction.infixRun("minus", List.of(a, b));
    }

    /** year * 12 + month (per = 12), or year * 4 + quarter (per = 4). */
    private static ValueSpecification months(ValueSpecification d, int per) {
        return AppliedFunction.infixRun("plus", List.of(AppliedFunction.infixRun("times", List.of(
                SqlToPure.call("year", d), integer(per))), SqlToPure.call(per == 12 ? "monthNumber" : "quarterNumber",
                d)));
    }

    /** A date's day, as a DATE. */
    private static ValueSpecification day(ValueSpecification d, boolean date) {
        return date ? d : typed(SqlToPure.call("datePart", d), "StrictDate");
    }

    /** {@code d}'s weekday counted from Sunday, 0 to 6: the days since the Sunday on or before it. */
    private static ValueSpecification sunday(ValueSpecification d) {
        return minus(SqlToPure.call("dayOfWeekNumber", d, new EnumValue(DAY_OF_WEEK, "Sunday")), integer(1));
    }

    /** {@code d}'s microseconds from the epoch. */
    private static ValueSpecification epochMicros(ValueSpecification d, boolean date) {
        return SqlToPure.call("toEpochValue", timestamp(d, date), new EnumValue(DURATION_UNIT, "MICROSECONDS"));
    }

    /** The boundaries of {@code unit} microseconds crossed from a to b: (b - a) less the two remainders' difference,
     *  a multiple of unit, divided by it. */
    private static ValueSpecification boundaries(ValueSpecification a, boolean aDate, ValueSpecification b,
            boolean bDate, long unit) {
        ValueSpecification ea = epochMicros(a, aDate);
        ValueSpecification eb = epochMicros(b, bDate);
        return exactQuotient(minus(minus(eb, ea), minus(remainder(eb, unit), remainder(ea, unit))), unit);
    }

    /** The whole units of {@code unit} microseconds elapsed from a to b, cut toward zero (IQ's engine's DATEDIFF of a
     *  part below a day: IQ island, 2026-10-15, measured on IQ 17.1). */
    private static ValueSpecification elapsed(ValueSpecification a, boolean aDate, ValueSpecification b,
            boolean bDate, long unit) {
        return truncatedQuotient(minus(epochMicros(b, bDate), epochMicros(a, aDate)), unit);
    }

    /** DAYOFYEAR and WEEKDAY in DATEADD and DATEDIFF under IQ's engine: its error -1001015 (measured on IQ 17.1). */
    private static SqlTranslationException iqDatePart(String name, Part part, SqlAst.FunctionCall f) {
        return SqlTranslationException.invalid(name + "(" + part + ", ...) is IQ's error under its engine"
                + IqDialect.code(-1001015, "QFA15"), f.pos());
    }

    /** {@code x} modulo {@code n}, from 0 to n - 1 whatever x's sign. */
    private static ValueSpecification remainder(ValueSpecification x, long n) {
        CInteger by = integer(n);
        return SqlToPure.call("mod", AppliedFunction.infixRun("plus", List.of(SqlToPure.call("mod", x, by), by)), by);
    }

    /**
     * {@code x / n} for an {@code x} that is a multiple of {@code n} whose quotient is below 2^52: the double quotient's
     * nearest integer, exact (x's double is within 2^-53 of x relatively, so the quotient within far less than a half
     * of its integer).
     */
    private static ValueSpecification exactQuotient(ValueSpecification x, long n) {
        return SqlToPure.call("round", SqlToPure.call("divide", x, integer(n)));
    }

    /** {@code x / n} rounded down, for a non-negative {@code x} below 2^52 (the double quotient's floor: exact, a
     *  quotient that is no integer lying at least 1/n from one). */
    private static ValueSpecification wholeQuotient(ValueSpecification x, long n) {
        return SqlToPure.call("floor", SqlToPure.call("divide", x, integer(n)));
    }

    /** {@code x / n} truncated toward zero, for an {@code x} below 2^52 in magnitude (as {@link #wholeQuotient}). */
    private static ValueSpecification truncatedQuotient(ValueSpecification x, long n) {
        ValueSpecification q = SqlToPure.call("divide", x, integer(n));
        return ifThen(SqlToPure.call("greaterThanEqual", x, integer(0)), SqlToPure.call("floor", q),
                SqlToPure.call("ceiling", q));
    }

    // ------------------------------------------------------------------ CAST and CONVERT

    /**
     * {@code CAST(x AS type)}, and {@code CONVERT(type, x)} without a style (they differ only where a date becomes
     * text: CONVERT writes style 0, refused; under the IQ engine the research's D-C6). Measured on SQL Anywhere 17:
     * <ul>
     *   <li>to SMALLINT, INTEGER, BIGINT: an integer as itself, a NUMERIC or a DOUBLE truncated toward zero, a string
     *       by IQ's strict reading (blanks around it, an optional sign, digits; nothing but blanks 0; anything else IQ's
     *       error -157); out of the type IQ's error -158;</li>
     *   <li>to NUMERIC(p, s) (DECIMAL; NUMERIC(p); an integer's NUMERIC(10,0) or (20,0) and a NUMERIC's own without a
     *       precision): rounded half away from zero to s places, more than p - s whole digits IQ's error -158; under
     *       the IQ engine truncated toward zero, a DOUBLE too, and without a precision the options' NUMERIC(126,38)
     *       (D-N6, {@link #toNumeric});</li>
     *   <li>to DOUBLE: an integer, a NUMERIC of at most 15 digits, a constant numeric string;</li>
     *   <li>to CHAR(n), VARCHAR(n), LONG VARCHAR: a string as itself -- longer than n IQ's error -638 in SQL Anywhere's
     *       front end, unless the rest is blanks (measured), and under the IQ engine cut to n bytes (D-C2,
     *       {@link #toText}); under the IQ engine a CHAR(n) is padded to n bytes (D-S1); an integer its digits (too
     *       long: -157, in both engines); a NUMERIC IQ's text ({@code -.50}; {@code -0.50} under the IQ engine, D-C8);
     *       a DATE yyyy-mm-dd;</li>
     *   <li>to DATE, TIMESTAMP (DATETIME): a date, a timestamp, a constant string in ISO spelling;</li>
     *   <li>to BIT, in SQL Anywhere's front end: 0 or 1 (a number truncated first), anything else -158.</li>
     * </ul>
     * Refused by name: what IQ converts by rules not read yet (a non-constant string to a number or a date, a DOUBLE to
     * a NUMERIC in SQL Anywhere's front end, which rounds its decimal digits, and under the IQ engine to more than 18
     * digits, a DOUBLE to text), and the types the island does not read (REAL, TINYINT, unsigned, TIME, binary,
     * national).
     */
    Tx cast(SqlAst.Cast c, SqlToPure.At at) {
        String typeName = c.type().name();
        boolean convert = typeName.startsWith(IqParser.CONVERT);
        String target = convert ? typeName.substring(IqParser.CONVERT.length()) : typeName;
        String what = convert ? "CONVERT" : "CAST";
        List<Integer> sizes = c.type().modifiers();
        Tx x = at.tx(c.operand());
        if (engine == IqDialect.Engine.IQ && x.kind() == Kind.INTEGER && x.detail() instanceof IqType t
                && t.spelling().equals("UNSIGNED BIGINT")) {
            // IQ island, 2026-10-15 (the corpus lane: CAST(COUNT(*) AS ...) in SQLStorm's and BIRD's queries; measured
            // on IQ 17.1): a COUNT converts as the whole number of rows it is (to INTEGER 7, to DECIMAL(10,2) 7.00, to
            // VARCHAR(10) '7', to CHAR(3) '7  ')
            x = new Tx(x.vs(), x.kind(), x.nullable(), x.whenFalse(), IqDialect.countRange(t), x.guard(), x.body());
        }
        if (target.equals("NCHAR") || target.equals("NVARCHAR")) {
            // IQ island, 2026-10-15 (the corpus lane: CAST(... AS NVARCHAR(n)) in 136 of SEDE's and SQLStorm's queries;
            // measured on IQ 17.1): IQ's engine converts to no national type
            if (engine == IqDialect.Engine.IQ) {
                throw SqlTranslationException.unsupported(what + " to " + target + " is IQ's error -1001030 under its"
                        + " engine (a cast to a national type is not supported), refused before translating", c.pos());
            }
            throw SqlTranslationException.unsupported(what + " to " + target + " in SQL Anywhere's front end is not"
                    + " supported yet", c.pos());
        }
        if (engine == IqDialect.Engine.IQ && target.equals("LONG VARCHAR") && readsColumn(c.operand())) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): IQ's engine converts no column's value to LONG VARCHAR
            // (or TEXT): its -1001006, of a string, a number and a timestamp alike
            throw SqlTranslationException.unsupported(what + " of a column to LONG VARCHAR is IQ's error -1001006 under"
                    + " its engine (no conversion), refused before translating", c.pos());
        }
        if (real(x) && !target.equals("REAL")) {
            // IQ island, 2026-10-15 (measured on IQ 17.1): a REAL converts as the DOUBLE of its value does (to INTEGER
            // toward zero, to NUMERIC(10,4) -.1000 for -0.1's single precision)
            x = new Tx(x.vs(), x.kind(), x.nullable(), x.whenFalse(), new IqType("DOUBLE"), x.guard(), x.body());
        }
        Tx result = switch (target) {
            case "REAL" -> toReal(x, c, what);
            case "SMALLINT", "INTEGER", "BIGINT" -> toInteger(target, x, c, what);
            case "NUMERIC" -> toNumeric(sizes, x, c, what);
            case "DOUBLE" -> toDouble(x, c, what);
            case "CHAR", "VARCHAR", "LONG VARCHAR" -> toText(target, sizes, x, c, what, convert);
            case "DATE", "TIMESTAMP" -> toDate(target.equals("DATE"), x, c, what);
            case "BIT" -> toBit(x, c, what);
            default -> throw SqlTranslationException.unsupported(what + " to " + target + " is not supported yet"
                    + (target.equals("REAL") ? " (a single-precision float)" : target.contains("INT")
                    ? " (an unsigned or TINYINT type)" : ""), c.pos());
        };
        return IqDialect.bounded(result, c);
    }

    private static boolean isNull(Tx x) {
        return x.kind() == Kind.NULL || SqlToPure.isNullConstant(x);
    }

    /** CAST to an integer type: see {@link #cast}. */
    private Tx toInteger(String family, Tx x, SqlAst.Cast c, String what) {
        IqType target = new IqType(family);
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, target);
        }
        boolean bigint = family.equals("BIGINT");
        switch (x.kind()) {
            case INTEGER -> {
                IqType t = integerType(x, c.operand(), what);
                boolean inside = t.lo().compareTo(target.min()) >= 0 && t.hi().compareTo(target.max()) <= 0;
                if (inside) {
                    return Tx.value(x.vs(), Kind.INTEGER, x.nullable(), IqType.integer(family, t.lo(), t.hi(),
                            t.wide(), t.wider()));
                }
                return checkedRange(family, x, false, v -> v);
            }
            case DECIMAL, FLOAT -> {
                if (x.kind() == Kind.FLOAT) {
                    doubleOnly(x, c.operand(), what);
                } else {
                    scaleOf(x, c.operand(), what);
                }
                // toward zero; a value beyond a BIGINT is the database's own error
                return checkedRange(family, x, true, v -> ifThen(SqlToPure.call("greaterThanEqual", v,
                        integer(0)), SqlToPure.call("floor", v), SqlToPure.call("ceiling", v)));
            }
            case STRING -> {
                if (engine == IqDialect.Engine.IQ) {
                    return iqStringToInteger(family, x, c, what);
                }
                if (c.operand() instanceof SqlAst.StringLit s) {
                    BigInteger v = integerText(s.value());
                    if (v != null && v.bitLength() < Long.SIZE) {
                        return checkedRange(family, Tx.value(SqlToPure.integer(v), Kind.INTEGER, false,
                                IqType.integer("BIGINT", v, v, true)), false, w -> w);
                    }
                    if (v != null) {
                        throw SqlTranslationException.unsupported(what + " of a string beyond a BIGINT's range is not"
                                + " supported", c.operand().pos());
                    }
                    // IQ's error -157 where it is computed
                    Tx r = SqlToPure.strict(Kind.INTEGER, List.of(x), w -> raise("Integer"));
                    return Tx.value(r.vs(), Kind.INTEGER, true, target);
                }
                // IQ's reading (measured): a string that begins or ends with a space loses its white space at both
                // ends; what is left is an optional sign and digits (none: 0)
                Tx parsed = SqlToPure.strict(Kind.INTEGER, List.of(x), v -> {
                    ValueSpecification str = v.get(0);
                    ValueSpecification valid = SqlToPure.call("or", SqlToPure.call("or",
                            SqlToPure.call("matches", str, new CString("^ " + BLANKS + "*" + SIGNED + BLANKS + "*$")),
                            SqlToPure.call("matches", str, new CString("^" + BLANKS + "*" + SIGNED + BLANKS + "* $"))),
                            SqlToPure.call("matches", str, new CString("^" + SIGNED + "$")));
                    ValueSpecification bare = SqlToPure.call("regexpReplace", str, new CString(BLANKS + "+"),
                            new CString(""), new com.legend.protocol.spec.CBoolean(true));
                    return ifThen(valid, ifThen(SqlToPure.call("matches", bare, new CString("^[+-]?$")), integer(0),
                            SqlToPure.call("parseInteger", bare)), raise("Integer"));
                });
                Tx wide = Tx.value(parsed.vs(), Kind.INTEGER, parsed.nullable(), IqType.integer("BIGINT",
                        IqType.BIGINT_MIN, IqType.BIGINT_MAX, true));
                return bigint ? Tx.value(wide.vs(), Kind.INTEGER, wide.nullable(), IqType.integer(family,
                        IqType.BIGINT_MIN, IqType.BIGINT_MAX, true)) : checkedRange(family, wide, false, v -> v);
            }
            default -> throw SqlTranslationException.unsupported(what + " of " + kindName(x.kind()) + " to " + family
                    + " is not supported yet (IQ reads a date as the integer yyyymmdd)", c.pos());
        }
    }

    /** {@code build} of x's value, an integer, checked against {@code family}'s range (IQ's -158) unless a BIGINT
     *  (the database raises its own). */
    private Tx checkedRange(String family, Tx x, boolean widened, Function<ValueSpecification, ValueSpecification>
            build) {
        IqType target = new IqType(family);
        boolean check = !family.equals("BIGINT");
        boolean narrow = x.detail() instanceof IqType t && t.isInteger() && !t.wide() && !t.exact();
        Tx r = SqlToPure.strict(Kind.INTEGER, List.of(x), v -> {
            ValueSpecification value = build.apply(narrow ? SqlToPure.call("floor", v.get(0)) : v.get(0));
            return check ? ifThen(SqlToPure.call("or", lt(value, SqlToPure.integer(target.min())),
                    SqlToPure.call("greaterThan", value, SqlToPure.integer(target.max()))), raise("Integer"), value)
                    : value;
        });
        BigInteger lo = target.min();
        BigInteger hi = target.max();
        if (x.detail() instanceof IqType t && t.isInteger()) {
            lo = lo.max(t.lo());
            hi = hi.min(t.hi());
            if (lo.compareTo(hi) > 0) {
                lo = target.min();
                hi = target.max();
            }
        }
        return Tx.value(r.vs(), Kind.INTEGER, r.nullable(), IqType.integer(family, lo, hi, true,
                x.detail() instanceof IqType t && t.wider()));
    }

    /** An optional sign and digits, as a regular expression. */
    private static final String SIGNED = "[+-]?[0-9]*";

    /** IQ's reading of a string as an integer (measured): a string that begins or ends with a space loses the white
     *  space (the space, tab through carriage return) at both ends, any other keeps it; what is left is an optional
     *  sign and digits, no digit 0; null for any other spelling (a point, an exponent, a sign apart from its digits). */
    static @com.legend.base.Nullable BigInteger integerText(String text) {
        int start = 0;
        int end = text.length();
        if (!text.isEmpty() && (text.charAt(0) == ' ' || text.charAt(end - 1) == ' ')) {
            while (start < end && blank(text.charAt(start))) {
                start++;
            }
            while (end > start && blank(text.charAt(end - 1))) {
                end--;
            }
        }
        int i = start;
        if (i < end && (text.charAt(i) == '+' || text.charAt(i) == '-')) {
            i++;
        }
        if (i == end) {
            return BigInteger.ZERO;
        }
        for (int k = i; k < end; k++) {
            if (text.charAt(k) < '0' || text.charAt(k) > '9') {
                return null;
            }
        }
        String digits = text.substring(i, end);
        BigInteger v = new BigInteger(digits);
        return text.charAt(start) == '-' ? v.negate() : v;
    }

    private static boolean blank(char c) {
        return c == ' ' || c >= '\t' && c <= '\r';
    }

    /** IQ's engine's reading of a string as a number (IQ island, 2026-10-15, checklist 0.5, measured on IQ 17.1): the
     *  white space at both ends (the space, tab through carriage return) dropped, then an optional sign and digits with
     *  at most one point among or around them, one digit at least; no exponent ('1e2' is IQ's -1009145, as '' is).
     *  Null for any other spelling. */
    static @com.legend.base.Nullable BigDecimal iqNumberText(String text) {
        int start = 0;
        int end = text.length();
        while (start < end && blank(text.charAt(start))) {
            start++;
        }
        while (end > start && blank(text.charAt(end - 1))) {
            end--;
        }
        int i = start < end && (text.charAt(start) == '+' || text.charAt(start) == '-') ? start + 1 : start;
        int digits = 0;
        int points = 0;
        for (int k = i; k < end; k++) {
            char ch = text.charAt(k);
            if (ch >= '0' && ch <= '9') {
                digits++;
            } else if (ch == '.' && points == 0) {
                points++;
            } else {
                return null;
            }
        }
        return digits == 0 ? null : new BigDecimal(text.substring(start, end));
    }

    /** {@link #iqNumberText}'s reading in Pure: {@code s} with its white space at both ends dropped. */
    private static ValueSpecification iqNumberTrimmed(ValueSpecification s) {
        return replaceAll(s, "^" + BLANKS + "+|" + BLANKS + "+$", "");
    }

    /** {@code t} (trimmed) without its point and the digits after it: its sign and whole digits. */
    private static ValueSpecification iqNumberWhole(ValueSpecification t) {
        return replaceAll(t, "[.][0-9]*$", "");
    }

    /** Whether {@code t} (trimmed) is a number by {@link #iqNumberText}'s reading. */
    private static ValueSpecification iqNumberValid(ValueSpecification t) {
        return SqlToPure.call("matches", t, new CString("^[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)$"));
    }

    /**
     * CAST of a string to an integer under IQ's engine (IQ island, 2026-10-15, checklist 0.5, measured on IQ 17.1): the
     * string read as a number ({@link #iqNumberText}) and cut toward zero ('1.5' is 1, '-.9' 0, ' 12. ' 12); any other
     * spelling, '' among them, and a value out of the type are IQ's error -1009145. A constant IQ reads is its value; one
     * it cannot is refused before translating ({@link #constantCast}).
     */
    private Tx iqStringToInteger(String family, Tx x, SqlAst.Cast c, String what) {
        if (c.operand() instanceof SqlAst.StringLit lit) {
            BigDecimal d = iqNumberText(lit.value());
            if (d == null) {
                throw SqlTranslationException.unsupported(what + " of the string '" + lit.value() + "' to " + family
                        + " is IQ's error -1009145 under its engine (no number), refused", c.operand().pos());
            }
            BigInteger v = d.setScale(0, java.math.RoundingMode.DOWN).toBigIntegerExact();
            if (v.bitLength() >= Long.SIZE) {
                throw SqlTranslationException.unsupported(what + " of a string beyond a BIGINT's range is not"
                        + " supported", c.operand().pos());
            }
            return checkedRange(family, Tx.value(SqlToPure.integer(v), Kind.INTEGER, false, IqType.integer("BIGINT", v,
                    v, true)), false, w -> w);
        }
        // the digits before the point, with the sign: none, or a sign alone, is 0
        Tx parsed = SqlToPure.strict(Kind.INTEGER, List.of(x), v -> {
            ValueSpecification t = iqNumberTrimmed(v.get(0));
            ValueSpecification whole = iqNumberWhole(t);
            return ifThen(iqNumberValid(t), ifThen(SqlToPure.call("matches", whole, new CString("^[+-]?$")),
                    integer(0), SqlToPure.call("parseInteger", whole)), raise("Integer"));
        });
        Tx wide = Tx.value(parsed.vs(), Kind.INTEGER, parsed.nullable(), IqType.integer("BIGINT", IqType.BIGINT_MIN,
                IqType.BIGINT_MAX, true));
        return family.equals("BIGINT") ? wide : checkedRange(family, wide, false, v -> v);
    }

    /**
     * CAST of a string that is no constant to NUMERIC(p, s) under IQ's engine (IQ island, 2026-10-15, checklist 0.5,
     * measured on IQ 17.1): the string read as a number ({@link #iqNumberText}), cut toward zero to s places -- its
     * text cut, so that the database's cast, which rounds, has nothing to round ('1.555' is 1.55) -- and more than p - s
     * whole digits, or any other spelling ('' among them), IQ's error -1009145 (the database's own, past the digits).
     */
    private Tx iqStringToNumeric(Tx x, int p, int s, IqType type, SqlAst.Cast c) {
        return strict(Kind.DECIMAL, type, List.of(x), v -> {
            ValueSpecification t = iqNumberTrimmed(v.get(0));
            // the sign and digits before the point, the point, s digits after it, and a 0 (so that '1.' and '.' read)
            ValueSpecification cut = ifThen(SqlToPure.call("contains", t, new CString(".")), AppliedFunction.infixRun(
                    "plus", List.of(SqlToPure.call("left", t, AppliedFunction.infixRun("plus", List.of(SqlToPure.call(
                    "length", iqNumberWhole(t)), integer(s + 1L)))), new CString("0"))), t);
            return ifThen(iqNumberValid(t), SqlToPure.call("parseDecimal", cut, integer(p), integer(s)),
                    raise("Decimal"));
        });
    }

    /** CAST to NUMERIC: see {@link #cast}. Under IQ's engine (IQ island, 2026-10-15; the research's D-N6, measured on
     *  IQ 17.1) a narrower scale truncates toward zero -- a column's, a constant's, a string constant's alike (2.675 is
     *  2.67 at two places, 999.5 is 999 as a NUMERIC(3,0)) -- and without a precision the type is the PRECISION and
     *  SCALE options' (a NUMERIC(126,38): CAST(it AS NUMERIC) too). */
    private Tx toNumeric(List<Integer> sizes, Tx x, SqlAst.Cast c, String what) {
        boolean iq = engine == IqDialect.Engine.IQ;
        if (x.kind() == Kind.FLOAT) {
            if (iq) {
                return iqDoubleToNumeric(sizes, x, c, what);
            }
            throw SqlTranslationException.unsupported(what + " of a DOUBLE to NUMERIC is not supported yet: SQL Anywhere"
                    + " rounds its decimal digits (2.675e0 is 2.68 at two places), the IQ engine truncates (the"
                    + " research's D-N6)", c.pos());
        }
        // the source's own NUMERIC
        int sp;
        int ss;
        Tx value = x;
        boolean whole = false;
        if (isNull(x)) {
            sp = 1;
            ss = 0;
        } else if (x.kind() == Kind.STRING) {
            if (!(c.operand() instanceof SqlAst.StringLit s)) {
                if (iq && !sizes.isEmpty()) {
                    int p = sizes.get(0);
                    int sc = sizes.size() > 1 ? sizes.get(1) : 0;
                    return iqStringToNumeric(x, p, sc, new IqType("NUMERIC(" + p + "," + sc + ")"), c);
                }
                throw SqlTranslationException.unsupported(what + " of a string that is not a constant to NUMERIC is not"
                        + " supported yet: " + (iq ? "without a precision it is a NUMERIC(" + settings.precision() + ","
                        + settings.scale() + "), the PRECISION and SCALE options': write a precision"
                        : "SQL Anywhere's front end reads it by its own rules"), c.operand().pos());
            }
            BigDecimal d = iq ? iqNumberText(s.value()) : IqDialect.numericText(s.value());
            if (d == null) {
                throw SqlTranslationException.unsupported(what + " of the string '" + s.value() + "' to NUMERIC is not"
                        + " supported: " + (iq ? "IQ's error -1009145 under its engine (no number)" : "IQ reads it as a"
                        + " NUMERIC by rules not read for this spelling yet"), c.pos());
            }
            BigDecimal exact = IqTypes.atScale(d);
            if (exact.precision() > 38 || exact.scale() > 38) {
                throw SqlTranslationException.unsupported(what + " of a string of more than 38 digits is not"
                        + " supported", c.pos());
            }
            IqType t = IqTypes.ofNumericConstant(exact);
            value = Tx.value(new CDecimal(exact), Kind.DECIMAL, false, t);
            sp = t.size();
            ss = scaleOf(value, c.operand(), what)[1];
        } else if (decimal(x)) {
            int[] ps = scaleOf(x, c.operand(), what);
            sp = ps[0];
            ss = ps[1];
        } else if (x.kind() == Kind.INTEGER) {
            IqType t = integerType(x, c.operand(), what);
            sp = t.family().equals("BIGINT") ? 20 : 10;
            ss = 0;
            whole = true;
        } else {
            throw SqlTranslationException.unsupported(what + " of " + kindName(x.kind()) + " to NUMERIC is not supported"
                    + " yet", c.pos());
        }
        // without a precision: an integer's NUMERIC(10,0) or (20,0), a NUMERIC's own, a string's the profile's
        // PRECISION and SCALE (measured: CAST('1' AS DECIMAL) is a NUMERIC(126,38) in an IQ database); under IQ's
        // engine every source's the options' (measured on IQ 17.1)
        boolean options = x.kind() == Kind.STRING || iq;
        int p = sizes.isEmpty() ? options ? settings.precision() : sp : sizes.get(0);
        int s = sizes.isEmpty() ? options ? settings.scale() : ss : sizes.size() > 1 ? sizes.get(1) : 0;
        if (p > 38) {
            throw SqlTranslationException.unsupported(what + " to a NUMERIC of more than 38 digits is not supported:"
                    + " lite's decimals hold 38" + (sizes.isEmpty() ? " (without a precision it is a NUMERIC(" + p
                    + "," + s + "), the PRECISION and SCALE options': write a precision)" : ""), c.pos());
        }
        IqType type = new IqType("NUMERIC(" + p + "," + s + ")");
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(Kind.DECIMAL).vs(), Kind.DECIMAL, true, type);
        }
        int n = s;
        boolean round = n < ss;
        // IQ island, 2026-10-17 (the corpus lane's TPC-H and TPC-DS: CAST(SUM(...) AS DECIMAL(12,2)) of a NUMERIC(32,4)):
        // past the 18 digits places() computes in, rounded or cut by placesExact in 38, as ROUND is; a value of more
        // digits than that holds is refused at run time
        boolean exact = round && sp - ss + n > 18;
        int heldDigits = Math.min(sp - ss, 37 - ss);
        if (exact && heldDigits < 1) {
            throw SqlTranslationException.unsupported(what + " of a NUMERIC(" + sp + "," + ss + ") to " + n + " places"
                    + " is not supported yet: its scaled value may exceed 18 digits", c.pos());
        }
        String past = exact && heldDigits < sp - ss ? IqDialect.REFUSED_AT_RUN_TIME + what + " of a value of more than "
                + heldDigits + " digits before its point is not supported: its NUMERIC(" + sp + "," + ss + ") needs"
                + " more than the 38 digits lite's decimals hold" : null;
        // can the value need more whole digits than p - s? (rounding up may add one; IQ's engine truncates)
        boolean check = sp - ss + (round && !iq ? 1 : 0) > p - s;
        boolean fromWhole = whole;
        int fromScale = ss;
        return strict(Kind.DECIMAL, type, List.of(value), v -> {
            ValueSpecification d = fromWhole ? SqlToPure.call("toDecimal", v.get(0)) : v.get(0);
            ValueSpecification r = !round ? (s == fromScale ? d : atScale(d, s)) : !exact ? places(d, n, !iq)
                    : SqlToPure.call("round", placesExact(d, fromScale, n, !iq), integer(n));
            if (past != null) {
                r = refusedPast(SqlToPure.call("greaterThanEqual", SqlToPure.call("abs", d), new CDecimal(pow10(
                        heldDigits))), past, r, raise("Decimal"));
            }
            if (!check) {
                return r;
            }
            return ifThen(SqlToPure.call("greaterThanEqual", SqlToPure.call("abs", r), new CDecimal(pow10(p - s))),
                    raise("Decimal"), r);
        });
    }

    /**
     * CAST of a DOUBLE to NUMERIC(p, s) under IQ's engine (IQ island, 2026-10-15; the research's D-N6, measured on IQ
     * 17.1): a NUMERIC of at most 18 digits is a scaled 64-bit integer there, so the value is the DOUBLE times 10^s,
     * a DOUBLE product, cut toward zero -- 0.29e0 is .28 at two places (28.999999999999996), 0.15e0 is .15 (15.0
     * exactly), 2.3e0 is 2.3 at one -- and a product of 10^p or more is IQ's error -1009145. Refused by name: more
     * than 18 digits, which IQ's engine converts from the DOUBLE's decimal expansion instead (0.15e0 is .14 there).
     */
    private Tx iqDoubleToNumeric(List<Integer> sizes, Tx x, SqlAst.Cast c, String what) {
        doubleOnly(x, c.operand(), what);
        int p = sizes.isEmpty() ? settings.precision() : sizes.get(0);
        int s = sizes.isEmpty() ? settings.scale() : sizes.size() > 1 ? sizes.get(1) : 0;
        if (p > 18) {
            throw SqlTranslationException.unsupported(what + " of a DOUBLE to a NUMERIC of more than 18 digits is not"
                    + " supported yet: IQ's engine converts it from the DOUBLE's decimal expansion (0.15e0 is .14 at two"
                    + " places), not read yet" + (sizes.isEmpty() ? " (without a precision it is a NUMERIC(" + p + ","
                    + s + "), the PRECISION and SCALE options')" : ""), c.pos());
        }
        IqType type = new IqType("NUMERIC(" + p + "," + s + ")");
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(Kind.DECIMAL).vs(), Kind.DECIMAL, true, type);
        }
        return strict(Kind.DECIMAL, type, List.of(x), v -> {
            ValueSpecification y = s == 0 ? v.get(0) : AppliedFunction.infixRun("times", List.of(v.get(0),
                    new CFloat(pow10(s).doubleValue())));
            ValueSpecification cut = SqlToPure.call("toDecimal", ifThen(SqlToPure.call("greaterThanEqual", y,
                    integer(0)), SqlToPure.call("floor", y), SqlToPure.call("ceiling", y)));
            ValueSpecification r = s == 0 ? cut : atScale(AppliedFunction.infixRun("times", List.of(cut,
                    new CDecimal(pow10(-s)))), s);
            return ifThen(SqlToPure.call("greaterThanEqual", SqlToPure.call("abs", y), new CFloat(pow10(p)
                    .doubleValue())), raise("Decimal"), r);
        });
    }

    /** CAST to DOUBLE: see {@link #cast}. */
    private Tx toDouble(Tx x, SqlAst.Cast c, String what) {
        IqType type = new IqType("DOUBLE");
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(Kind.FLOAT).vs(), Kind.FLOAT, true, type);
        }
        if (x.kind() == Kind.STRING) {
            if (!(c.operand() instanceof SqlAst.StringLit s) || IqDialect.numericText(s.value()) == null) {
                throw SqlTranslationException.unsupported(what + " of a string that is not a constant number to DOUBLE"
                        + " is not supported yet: IQ reads it by its own rules", c.operand().pos());
            }
            BigDecimal d = IqTypes.atScale(java.util.Objects.requireNonNull(IqDialect.numericText(s.value())));
            if (d.precision() > 15 || d.scale() > 38) {
                // IQ island, 2026-10-17 (the corpus lane's Public BI, cast('21.280000000000001' as double): a double's
                // seventeen-digit spelling): the database parses the digits, to the nearest DOUBLE, where a NUMERIC's
                // conversion may miss it by a unit in the last place; a magnitude past a DOUBLE's is refused
                if (d.signum() != 0 && (d.abs().compareTo(new BigDecimal("1e308")) >= 0
                        || d.abs().compareTo(new BigDecimal("1e-307")) <= 0)) {
                    throw SqlTranslationException.unsupported(what + " of a string past a DOUBLE's magnitude is not"
                            + " supported yet", c.operand().pos());
                }
                return Tx.value(SqlToPure.call("parseFloat", new CString(d.toPlainString())), Kind.FLOAT, false,
                        type);
            }
            return Tx.value(SqlToPure.call("toFloat", new CDecimal(d)), Kind.FLOAT, false, type);
        }
        Tx d = asDouble(x, c.operand(), what);
        return Tx.value(d.vs(), Kind.FLOAT, d.nullable(), type);
    }

    /**
     * CAST to REAL, FLOAT or FLOAT(1) to FLOAT(24) (IQ island, 2026-10-15; the corpus lane, measured on IQ 17.1): IQ's
     * single precision, held as the DOUBLE of its value ({@link #single}): an integer, a NUMERIC or a DOUBLE rounded to
     * the nearest single-precision value (16777217 is 16777216, 1.1 is 1.100000023841858); a value beyond the largest
     * one is IQ's error, a magnitude below the smallest normal one 0. A string is refused by name.
     */
    private Tx toReal(Tx x, SqlAst.Cast c, String what) {
        IqType type = new IqType("REAL");
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(Kind.FLOAT).vs(), Kind.FLOAT, true, type);
        }
        if (real(x)) {
            return x;
        }
        if (x.kind() == Kind.STRING) {
            if (engine != IqDialect.Engine.IQ) {
                throw SqlTranslationException.unsupported(what + " of a string to REAL is not supported in SQL"
                        + " Anywhere's front end yet: it reads it by its own rules", c.operand().pos());
            }
            return stringToReal(x, type);
        }
        Tx d = asDouble(x, c.operand(), what);
        Tx r = SqlToPure.strict(Kind.FLOAT, List.of(d), v -> single(v.get(0)));
        return Tx.value(r.vs(), Kind.FLOAT, r.nullable(), type);
    }

    /** IQ's engine's spelling of a float: blanks or tabs around a sign, digits with one point (a digit on one side),
     *  an exponent of digits ({@link #stringToReal}). */
    private static final String FLOAT_TEXT = "^[ \\t]*[+-]?([0-9]+[.]?[0-9]*|[.][0-9]+)([eE][+-]?[0-9]+)?[ \\t]*$";

    /**
     * IQ island, 2026-10-17 (the corpus lane's JOB, 26 queries; measured on IQ 17.1): under IQ's engine a string read
     * as a REAL is its float spelling ({@link #FLOAT_TEXT}: ' 7.5 ', '+.5', '5.', '-.5E+2', a tab around it) rounded to
     * single precision ('1.555' is 1.5549999475479126), '' or blanks alone 0.0, any other spelling IQ's -1009145 ('.',
     * '1e', 'inf', '1,5') raised for its row; past the largest REAL IQ answers Infinity, refused for its row.
     */
    private static Tx stringToReal(Tx x, IqType type) {
        String past = IqDialect.REFUSED_AT_RUN_TIME + "a string past the largest REAL read as one: IQ answers Infinity";
        Tx r = SqlToPure.strict(Kind.FLOAT, List.of(x), v -> {
            ValueSpecification bare = SqlToPure.call("trim", SqlToPure.call("replace", v.get(0), new CString("\t"),
                    new CString("")));
            ValueSpecification parsed = SqlToPure.call("parseFloat", bare);
            ValueSpecification read = refusedPast(SqlToPure.call("greaterThan", SqlToPure.call("abs", parsed),
                    new CFloat(SINGLE_MAX)), past, single(parsed), raise("Float"));
            return ifThen(SqlToPure.call("equal", bare, new CString("")), new CFloat(0.0), ifThen(SqlToPure.call(
                    "matches", v.get(0), new CString(FLOAT_TEXT)), read, raise("Float")));
        });
        return Tx.value(r.vs(), Kind.FLOAT, r.nullable(), type);
    }

    /** Whether {@code t} is IQ's REAL: a DOUBLE whose value single precision holds. */
    static boolean real(Tx t) {
        return t.kind() == Kind.FLOAT && t.detail() instanceof IqType type && type.family().equals("REAL");
    }

    /** The largest single-precision value, and the smallest normal one. */
    private static final double SINGLE_MAX = Float.MAX_VALUE;
    private static final double SINGLE_MIN = Float.MIN_NORMAL;

    /**
     * {@code x}, a DOUBLE, as IQ's REAL holds it (IQ island, 2026-10-15; measured on IQ 17.1): rounded to the nearest
     * value of 24 significant bits, ties to even, by Veltkamp's split -- c = x * (2^29 + 1), then c - (c - x), plain
     * double arithmetic, which every database computes alike (checked against Java's own conversion to float over
     * 2.9 million values, 500,000 exact ties among them); beyond the largest single-precision value IQ's error (3.4028235E38
     * is -158, the double before rounding compared), below the smallest normal one 0 (IQ keeps no subnormal: 1E-40 is
     * 0).
     */
    static ValueSpecification single(ValueSpecification x) {
        ValueSpecification c = AppliedFunction.infixRun("times", List.of(x, new CFloat(536870913.0)));
        ValueSpecification rounded = AppliedFunction.infixRun("minus", List.of(c, AppliedFunction.infixRun("minus",
                List.of(c, x))));
        ValueSpecification magnitude = SqlToPure.call("abs", x);
        return ifThen(SqlToPure.call("greaterThan", magnitude, new CFloat(SINGLE_MAX)), raise("Float"),
                ifThen(lt(magnitude, new CFloat(SINGLE_MIN)), new CFloat(0.0), rounded));
    }

    /**
     * CAST to a character type: see {@link #cast}. Under IQ's engine (IQ island, 2026-10-15, measured on IQ 17.1): a
     * longer string is cut to n bytes, never inside a character (the research's D-C2: 'Résumé' is 'Ré' as a
     * VARCHAR(3) in a UTF-8 database, whatever STRING_RTRUNCATION says); a CHAR(n) is padded to n bytes, a CHAR
     * without a length to 255; a VARCHAR's length in characters, {@code (n CHAR)}, cuts at n characters, a CHAR's is
     * IQ's error -1001030; a NUMERIC is written with its 0 before the point (D-C8: '-0.50'); a NUMERIC, a date or a
     * timestamp whose text is longer than n is IQ's error -1009145 for that row (D-C1).
     */
    private Tx toText(String target, List<Integer> sizes, Tx x, SqlAst.Cast c, String what, boolean convert) {
        boolean iq = engine == IqDialect.Engine.IQ;
        boolean characters = sizes.size() > 1;
        if (iq && characters && !target.equals("VARCHAR")) {
            throw SqlTranslationException.unsupported(what + " to a CHAR of a length in characters, (n CHAR), is IQ's"
                    + " error -1001030 under its engine (the CHARACTER-LENGTH semantics it does not support), refused"
                    + " before translating", c.pos());
        }
        // IQ's engine reads CHAR without a length as a CHAR(255) (measured: CAST(vc AS CHAR) is padded to 255 bytes)
        int n = target.equals("LONG VARCHAR") || sizes.isEmpty() ? iq && target.equals("CHAR") ? 255 : -1
                : sizes.get(0);
        boolean pad = target.equals("CHAR") && n > 0 && iq;
        // a UTF-8 database's lengths are bytes, but a length in characters
        boolean utf8 = !settings.latin1() && !characters;
        IqType type = new IqType(n < 0 ? (target.equals("LONG VARCHAR") ? "VARCHAR" : target) : target + "(" + n + ")");
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, type);
        }
        Tx text;
        switch (x.kind()) {
            case STRING -> {
                // a UTF-8 database's lengths are bytes (unless the type counts characters: CHAR(n CHAR))
                boolean bytes = !settings.latin1() && sizes.size() < 2;
                int longest = bytes && c.operand() instanceof SqlAst.StringLit lit
                        ? lit.value().getBytes(java.nio.charset.StandardCharsets.UTF_8).length : longest(x, c.operand());
                if (n < 0 || longest >= 0 && longest <= n) {
                    text = x;
                } else if (iq) {
                    // cut to n bytes (D-C2): in ISO-8859-1 n characters
                    text = SqlToPure.strict(Kind.STRING, List.of(x), v -> utf8 ? utf8Prefix(v.get(0), n)
                            : SqlToPure.call("left", v.get(0), integer(n)));
                } else {
                    if (!settings.latin1()) {
                        throw SqlTranslationException.unsupported(what + " of a string that may be longer than " + n
                                + " to " + target + "(" + n + ") is not supported in profile " + settings.spelling()
                                + "'s UTF-8 database yet: its lengths are bytes", c.pos());
                    }
                    Tx r = SqlToPure.strict(Kind.STRING, List.of(x), v -> {
                        ValueSpecification s = v.get(0);
                        ValueSpecification rest = SqlToPure.call("substring", s, integer(n + 1));
                        return ifThen(SqlToPure.call("lessThanEqual", SqlToPure.call("length", s), integer(n)), s,
                                ifThen(SqlToPure.call("equal", SqlToPure.call("replace", rest, new CString(" "),
                                        new CString("")), new CString("")), SqlToPure.call("left", s, integer(n)),
                                        raise("String")));
                    });
                    text = r;
                }
            }
            case INTEGER -> {
                IqType t = integerType(x, c.operand(), what);
                int digits = Math.max(t.lo().toString().length(), t.hi().toString().length());
                if (n < 0 || digits <= n) {
                    text = SqlToPure.strict(Kind.STRING, List.of(x), v -> SqlToPure.call("toString", v.get(0)));
                } else {
                    text = SqlToPure.strict(Kind.STRING, List.of(x), v -> ifThen(SqlToPure.call("greaterThan",
                            SqlToPure.call("length", SqlToPure.call("toString", v.get(0))), integer(n)),
                            raise("String"), SqlToPure.call("toString", v.get(0))));
                }
            }
            case DECIMAL -> {
                int[] ps = scaleOf(x, c.operand(), what);
                int longest = ps[0] + (ps[1] > 0 ? 2 : 1);
                if (iq) {
                    text = iqNumericText(x, ps, n, c, what);
                    break;
                }
                if (n >= 0 && n < longest) {
                    throw SqlTranslationException.unsupported(what + " of a NUMERIC(" + ps[0] + "," + ps[1] + ") to "
                            + target + "(" + n + ") is not supported yet: SQL Anywhere cuts its text, the IQ engine"
                            + " raises (the research's D-C1)", c.pos());
                }
                // IQ's text: as the database writes it, but no 0 before the point of a value between -1 and 1
                text = SqlToPure.strict(Kind.STRING, List.of(x), v -> ifThen(SqlToPure.call("and", SqlToPure.call("and",
                        SqlToPure.call("greaterThan", v.get(0), integer(-1)), lt(v.get(0), integer(1))),
                        SqlToPure.call("not", SqlToPure.call("equal", v.get(0), integer(0)))),
                        SqlToPure.call("replace", SqlToPure.call("toString", v.get(0)), new CString("0."),
                                new CString(".")), SqlToPure.call("toString", v.get(0))));
            }
            case DATE -> {
                // IQ island, 2026-10-15 (D-C6, measured on IQ 17.1): under IQ's engine CONVERT without a style writes
                // the DATE_FORMAT option's form, as CAST does ('2026-03-04')
                if (convert && engine != IqDialect.Engine.IQ) {
                    throw SqlTranslationException.unsupported("CONVERT of a date to text without a style is not"
                            + " supported yet: " + (engine == IqDialect.Engine.IQ ? "under the IQ engine it writes the"
                            + " DATE_FORMAT option's form (the research's D-C6, not yet verified)" : "SQL Anywhere writes"
                            + " style 0 (Mar 04 2026 12:00AM), a format not read yet"), c.pos());
                }
                if (n >= 0 && n < 10 && !iq) {
                    throw SqlTranslationException.unsupported(what + " of a DATE to " + target + "(" + n + ") is not"
                            + " supported yet: SQL Anywhere cuts its text, the IQ engine raises (the research's D-C1)",
                            c.pos());
                }
                // IQ island, 2026-10-14 (review 5's M4): in the profile's DATE_FORMAT, each part with its zeros (a year
                // below 1000 too), as || and STRING write it (IqDateTime, measured)
                text = IqDateTime.text(x, settings, what, c.pos());
                if (iq && n >= 0 && n < IqDateTime.length(false, settings)) {
                    text = raisedPastLength(text, n, c);
                }
            }
            // IQ island, 2026-10-14 (M1e): a timestamp in the profile's TIMESTAMP_FORMAT (IqDateTime, measured)
            case DATETIME -> {
                // IQ island, 2026-10-15 (D-C6, measured on IQ 17.1): as a DATE's, the TIMESTAMP_FORMAT's form
                if (convert && engine != IqDialect.Engine.IQ) {
                    throw SqlTranslationException.unsupported("CONVERT of a timestamp to text without a style is not"
                            + " supported yet: " + (engine == IqDialect.Engine.IQ ? "under the IQ engine it writes the"
                            + " TIMESTAMP_FORMAT option's form (the research's D-C6, not yet verified)" : "SQL Anywhere"
                            + " writes style 0 (Mar 04 2026 05:06AM), a format not read yet"), c.pos());
                }
                int length = IqDateTime.length(true, settings);
                if (n >= 0 && n < length && !iq) {
                    throw SqlTranslationException.unsupported(what + " of a TIMESTAMP to " + target + "(" + n + ") is not"
                            + " supported yet: SQL Anywhere cuts its text, the IQ engine raises (the research's D-C1)",
                            c.pos());
                }
                text = IqDateTime.text(x, settings, what, c.pos());
                if (iq && n >= 0 && n < length) {
                    text = raisedPastLength(text, n, c);
                }
            }
            default -> throw SqlTranslationException.unsupported(what + " of " + kindName(x.kind()) + " to text is not"
                    + " supported yet: IQ writes it by its own format", c.pos());
        }
        if (pad) {
            // to n bytes: in a UTF-8 database n less the bytes beyond one a character the text has
            Tx padded = SqlToPure.strict(Kind.STRING, List.of(text), v -> SqlToPure.call("rpad", v.get(0),
                    utf8 && x.kind() == Kind.STRING ? AppliedFunction.infixRun("minus", List.of(AppliedFunction
                            .infixRun("plus", List.of(SqlToPure.call("length", v.get(0)), integer(n))),
                            SqlToPure.call("length", utf8Units(v.get(0))))) : integer(n), new CString(" ")));
            return Tx.value(padded.vs(), Kind.STRING, padded.nullable(), type);
        }
        return Tx.value(text.vs(), Kind.STRING, text.nullable(), type);
    }

    /**
     * A NUMERIC as text under IQ's engine (IQ island, 2026-10-15, measured on IQ 17.1): as the database writes it, at
     * its scale, with its 0 before the point (D-C8: 0.01 is '0.01', -0.50 '-0.50', where SQL Anywhere's front end writes
     * '.01'); longer than n, IQ's error -1009145 for that row (D-C1). Refused by name: a NUMERIC that reads no column
     * ({@link #iqNumericAsText}).
     */
    private Tx iqNumericText(Tx x, int[] ps, int n, SqlAst.Cast c, String what) {
        Tx text = iqNumericAsText(x, c.operand(), what);
        int longest = ps[0] + (ps[1] > 0 ? 2 : 1) + (ps[1] == ps[0] ? 1 : 0);
        return n >= 0 && n < longest ? raisedPastLength(text, n, c) : text;
    }

    /**
     * A NUMERIC as IQ's engine writes it as text, in a CAST, {@code ||}, {@code +} of strings and STRING alike (IQ
     * island, 2026-10-15, D-C8, measured on IQ 17.1): as the database writes it, at its scale, its 0 before the point
     * ('0.01', '-0.50'). Refused by name: one that reads no column (IQ's engine writes a constant's digits by rules not
     * read yet: 10.50 is '10.5', 0.0 '0').
     */
    static Tx iqNumericAsText(Tx x, SqlAst.Expr e, String what) {
        if (!readsColumn(e)) {
            throw SqlTranslationException.unsupported(what + " of a NUMERIC constant as text is not supported under IQ's"
                    + " engine yet: it writes a constant's digits by rules not read yet (10.50 is '10.5', 0.0 is '0')",
                    e.pos());
        }
        int s = scaleOf(x, e, what)[1];
        Tx text = SqlToPure.strict(Kind.STRING, List.of(x), v -> SqlToPure.call("toString", s > 0 ? atScale(v.get(0), s)
                : v.get(0)));
        return Tx.value(text.vs(), Kind.STRING, text.nullable(), varchar());
    }

    /** {@code text} (ASCII), IQ's error -1009145 where it is longer than {@code n} (the research's D-C1). */
    private Tx raisedPastLength(Tx text, int n, SqlAst.Cast c) {
        return SqlToPure.strict(Kind.STRING, List.of(text), v -> ifThen(SqlToPure.call("greaterThan",
                SqlToPure.call("length", v.get(0)), integer(n)), raise("String"), v.get(0)));
    }

    /**
     * {@code s}'s UTF-8 bytes as units (IQ island, 2026-10-15): each character becomes as many characters as it has
     * bytes, the last an {@code E}, the others {@code x} -- so the units' length is s's length in bytes, and n units
     * hold whole the characters whose {@code E} they hold. Four replaces by code point range: one byte below U+0080,
     * two below U+0800, three to U+FFFF, four beyond.
     */
    private static ValueSpecification utf8Units(ValueSpecification s) {
        ValueSpecification w = replaceAll(s, "[\u0001-\u007f]", "E");
        w = replaceAll(w, "[\u0080-\u07ff]", "xE");
        w = replaceAll(w, "[\u0800-\uffff]", "xxE");
        return replaceAll(w, "[^xE]", "xxxE");
    }

    /** The longest start of {@code s} of at most {@code n} UTF-8 bytes, no character cut: as many characters as the
     *  first n of its units ({@link #utf8Units}) hold {@code E}s. */
    private static ValueSpecification utf8Prefix(ValueSpecification s, int n) {
        ValueSpecification units = SqlToPure.call("left", utf8Units(SqlToPure.call("left", s, integer(n))), integer(n));
        return SqlToPure.call("left", s, SqlToPure.call("length", replaceAll(units, "x", "")));
    }

    /** Every match of {@code regex} in {@code s} replaced by {@code by}. */
    private static ValueSpecification replaceAll(ValueSpecification s, String regex, String by) {
        return SqlToPure.call("regexpReplace", s, new CString(regex), new CString(by),
                new com.legend.protocol.spec.CBoolean(true));
    }

    /** The most characters a string value may have, by its type: a CHAR(n)'s or VARCHAR(n)'s n, a constant's length;
     *  -1 when not known. */
    private static int longest(Tx x, SqlAst.Expr e) {
        if (e instanceof SqlAst.StringLit s) {
            return s.value().length();
        }
        if (x.detail() instanceof IqType t && (t.family().equals("CHAR") || t.family().equals("VARCHAR"))) {
            return t.size();
        }
        return -1;
    }

    /** CAST to DATE or TIMESTAMP: see {@link #cast}. */
    private Tx toDate(boolean toDate, Tx x, SqlAst.Cast c, String what) {
        Kind kind = toDate ? Kind.DATE : Kind.DATETIME;
        // IQ island, 2026-10-14 (review 4's M2): a date's latest year carried through
        IqType type = dated(toDate ? "DATE" : "TIMESTAMP", x.kind() == Kind.DATE || x.kind() == Kind.DATETIME
                ? latestYear(x) : null);
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(kind).vs(), kind, true, type);
        }
        if (x.kind() == Kind.STRING && c.operand() instanceof SqlAst.StringLit s) {
            String[] parts = isoParts(s.value(), c.operand());
            String alone = parts == null && engine == IqDialect.Engine.IQ ? IqDateTime.timeAlone(s.value()) : null;
            if (alone != null) {
                // IQ island, 2026-10-16 (measured on IQ 17.1): a time alone, on 1900-01-01 under IQ's engine
                parts = new String[] {"1900-01-01", alone};
            }
            if (parts == null) {
                if (s.value().isEmpty() || looksIso(s.value())) {
                    // '' and an ISO spelling of no day: IQ's error -157 where it is computed
                    return Tx.value(SqlToPure.call("toOne", SqlToPure.call("cast", SqlToPure.empty(),
                            SqlToPure.typeArg(toDate ? "StrictDate" : "DateTime"))), kind, false, type);
                }
                throw SqlTranslationException.unsupported(what + " of the string '" + s.value() + "' to a date is not"
                        + " supported: it is no date spelling the island reads yet (IqDateTime)", c.pos());
            }
            type = dated(toDate ? "DATE" : "TIMESTAMP", Integer.parseInt(parts[0].substring(0, 4)));
            if (toDate) {
                return Tx.value(new CDate(PureDateLiteral.parse(parts[0]), parts[0], null), Kind.DATE, false, type);
            }
            String body = parts[0] + "T" + parts[1];
            return Tx.value(new CDate(PureDateLiteral.parse(body), body, null), Kind.DATETIME, false, type);
        }
        if (x.kind() == Kind.DATE) {
            if (toDate) {
                return Tx.value(x.vs(), Kind.DATE, x.nullable(), type);
            }
            return strict(Kind.DATETIME, type, List.of(x), v -> timestamp(v.get(0), true));
        }
        if (x.kind() == Kind.DATETIME) {
            if (!toDate) {
                return Tx.value(x.vs(), Kind.DATETIME, x.nullable(), type);
            }
            return strict(Kind.DATE, type, List.of(x), v -> typed(SqlToPure.call("datePart", v.get(0)),
                    "StrictDate"));
        }
        if (x.kind() == Kind.STRING) {
            return stringDate(x, toDate, type);
        }
        throw SqlTranslationException.unsupported(what + " of " + kindName(x.kind()) + " to a date is not supported"
                + " yet: IQ converts it by its own rules (an integer as yyyymmdd, a string by its date order)",
                c.pos());
    }

    /**
     * IQ island, 2026-10-15 (measured on IQ 17.1; the census's Tier 0 string-to-date conversion -- BI data keeps dates
     * as text): a string that is not a constant read as a date or a timestamp where the database computes it, in the
     * spellings {@link IqDateTime#read} reads first in every profile -- {@code yyyy-m-d} (- / or . at each place),
     * {@code yyyymmdd}, then after spaces or a {@code T} a time {@code h:n[:s[.f]]} (six places of the fraction, more
     * cut) -- with blanks around it. Its parts are read as integers and built into the date and time, so a month or a
     * day that does not exist is the database's error, as it is IQ's -157. Any other spelling (a two-digit year, a date
     * in the profile's DATE_ORDER, a year and a month alone) is refused by name where it is computed ({@link
     * #UNREAD_DATE_RAISE}), though IQ reads some of them: never an answer IQ would not give.
     * <p>
     * IQ island, 2026-10-16 (the corpus lane's sede-36359, measured on IQ 17.1): under IQ's engine a time alone,
     * {@code h:n[:s[.f]]} (an ISO {@code T} before it read too), is that time on 1900-01-01 -- as a DATE, 1900-01-01;
     * SQL Anywhere's front end reads it on the current date, so there it stays raised. A time out of range (an hour past
     * 23, a minute or a second past 59) is IQ's error -1001006 for a DATE as for a TIMESTAMP: raised.
     * <p>
     * IQ island, 2026-10-16 (the corpus lane's sede-153489 and sede-485366, measured on IQ 17.1; the spellings
     * {@link IqDateTime#read} reads of a constant): blanks around the date's separators ({@code STR(y) + '-' + STR(m)}
     * pads each number); a month's English name, or a start of it of three letters or more, in any case -- {@code
     * 07 Nov 2009}, {@code 7-November-2009}, {@code Nov 7, 2009}, {@code Nov 7 2009} (CONVERT's styles 100, 106 and 107
     * written back) -- with a time after it; and a time's {@code AM} or {@code PM} (12 AM midnight, 12 PM noon, a PM
     * hour past 12 kept). Another name ({@code Nox}) is refused where it is computed; IQ raises -1001006. These are
     * this reader's spellings: the constant's reader ({@link IqDateTime#read}) differs (2026-10-16, the landing's
     * review), reading DATE_ORDER and two-digit years, not names, blanks around separators or a time alone.
     */
    private Tx stringDate(Tx x, boolean toDate, IqType type) {
        Kind kind = toDate ? Kind.DATE : Kind.DATETIME;
        List<String[]> steps = canonicalSteps(engine == IqDialect.Engine.IQ);
        Tx r = SqlToPure.strict(kind, List.of(x), v -> readDate(v.get(0), toDate, steps));
        return Tx.value(r.vs(), kind, r.nullable(), type);
    }

    /** A string the database computes of constants, compared with a timestamp under IQ's engine: the TIMESTAMP CAST
     *  reads; with a DATE ({@code date}), the DATE ({@link #stringDate}; IqDialect's comparison, IQ island,
     *  2026-10-17). */
    Tx compared(Tx x, boolean date) {
        return stringDate(x, date, dated(date ? "DATE" : "TIMESTAMP", null));
    }

    /** A string's value {@code v} read as {@link #stringDate} reads it, by {@code steps}. */
    private static ValueSpecification readDate(ValueSpecification v, boolean toDate, List<String[]> steps) {
        // each step reads the last once: the string's Pure grows with the steps, not with their groups
        ValueSpecification text = SqlToPure.call("toLower", v);
        for (String[] step : steps) {
            text = SqlToPure.call("regexpReplace", text, new CString(step[0]), new CString(step[1]),
                    new com.legend.protocol.spec.CBoolean(false));
        }
        ValueSpecification at = SqlToPure.call("parseDate", text);
        ValueSpecification value = toDate ? typed(SqlToPure.call("datePart", at), "StrictDate")
                : typed(at, "DateTime");
        if (!toDate) {
            // as the constant's reader refuses it (IqDateTime.read; 2026-10-16, the landing's review): SQL
            // Anywhere drops a timestamp's hours and minutes after the year 7910
            value = refusedPast(SqlToPure.call("matches", text, new CString(PAST_TIMED)), PAST_TIMED_RAISE, value,
                    raise("DateTime"));
        }
        return refusedPast(SqlToPure.call("not", SqlToPure.call("matches", text, new CString(CANONICAL))),
                UNREAD_DATE_RAISE, value, raise(toDate ? "StrictDate" : "DateTime"));
    }

    /** A year of 1 to 9999, four digits. */
    private static final String YEAR = "(?:[1-9][0-9]{3}|0[1-9][0-9]{2}|00[1-9][0-9]|000[1-9])";

    /** The one spelling {@link #stringDate}'s steps leave of every date they read: {@code yyyy-mm-dd}, then a time
     *  {@code hh:nn:ss} of hours below 24, minutes and seconds below 60, and up to six places of a fraction. */
    private static final String CANONICAL = "^" + YEAR + "-[0-9]{2}-[0-9]{2}( ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]"
            + "([.][0-9]{1,6})?)?$";

    /** A {@link #CANONICAL} timestamp after the year {@link IqDialect#LAST_TIMED_YEAR} with hours or minutes (the
     *  whole string: Pure's {@code matches} matches all of it). */
    private static final String PAST_TIMED = "^(?:791[1-9]|79[2-9][0-9]|[89][0-9]{3})-[0-9]{2}-[0-9]{2}"
            + " (?:(?:0[1-9]|[12][0-9]):|[0-9]{2}:(?:0[1-9]|[1-5][0-9])).*$";

    /** {@link #stringDate}'s refusal of a spelling its steps do not read, where the database computes the string
     *  (2026-10-16, the landing's review: a bare error before, though IQ reads some of them). */
    static final String UNREAD_DATE_RAISE = IqDialect.REFUSED_AT_RUN_TIME + "a string in a spelling the island does"
            + " not read as a date yet (IQ reads some: a date in the profile's DATE_ORDER, a two-digit year, a year"
            + " and a month alone, 24:00; it rejects others, a month's name it does not know)";

    /** {@link #PAST_TIMED}'s refusal, where the database computes the string. */
    static final String PAST_TIMED_RAISE = IqDialect.REFUSED_AT_RUN_TIME + "a timestamp after the year "
            + IqDialect.LAST_TIMED_YEAR + " with hours or minutes is not supported: SQL Anywhere 17 drops them there"
            + " (its defect), so no answer of it judges lite's";

    /** The English months' names, January first. */
    private static final List<String> MONTH_NAMES = List.of("january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december");

    /**
     * {@link #stringDate}'s steps, each a regular expression and its replacement (its first match only), over the
     * string in lower case: what they leave of each spelling read is {@link #CANONICAL}, and of any other something
     * else. {@code timeAlone}: a time alone is read on 1900-01-01 (IQ's engine).
     */
    private static List<String[]> canonicalSteps(boolean timeAlone) {
        String sep = " *[-/.] *";
        String ymd = "^([0-9]{4})" + sep + "([0-9]{1,2})" + sep + "([0-9]{1,2})";
        List<String[]> steps = new ArrayList<>();
        steps.add(new String[] {"^ +", ""});
        steps.add(new String[] {" +$", ""});
        // yyyy-m-d, its separators - / or . with blanks around them, then spaces or one T before a time (as the
        // constant's reader; 2026-10-16, the landing's review: any run of spaces and t's was read)
        steps.add(new String[] {ymd + "(?: +|t)([0-9])", "\\1-\\2-\\3 \\4"});
        steps.add(new String[] {ymd + "$", "\\1-\\2-\\3"});
        steps.add(new String[] {"^([0-9]{4})([0-9]{2})([0-9]{2})$", "\\1-\\2-\\3"});
        // a month's name, or a start of it of three letters or more: M and its number (an M the string, lower case
        // here, cannot hold; 2026-10-16, the landing's review: a written m11 was read as November)
        for (int m = 1; m <= MONTH_NAMES.size(); m++) {
            String full = MONTH_NAMES.get(m - 1);
            StringBuilder name = new StringBuilder(full.substring(0, 3));
            for (int i = 3; i < full.length(); i++) {
                name.append("(?:").append(full.charAt(i));
            }
            name.append(")?".repeat(full.length() - 3));
            steps.add(new String[] {"(^|[^a-z])" + name + "([^a-z]|$)", "\\1M" + (m < 10 ? "0" : "") + m + "\\2"});
        }
        // d mon yyyy (spaces or - between), mon d[,] yyyy: then a time after spaces
        steps.add(new String[] {"^([0-9]{1,2})[ -]+M([0-9]{2})[ -]+([0-9]{4})( +|$)", "\\3-\\2-\\1 "});
        steps.add(new String[] {"^M([0-9]{2}) +([0-9]{1,2}),? +([0-9]{4})( +|$)", "\\3-\\1-\\2 "});
        if (timeAlone) {
            steps.add(new String[] {"^t?([0-9]{1,2}:)", "1900-01-01 \\1"});
        }
        steps.add(new String[] {" +$", ""});
        // AM and PM: 12 AM is midnight, 12 PM noon, a PM hour past 12 kept; any other is left (and not read)
        String rest = "(:[0-9:.]*) *";
        steps.add(new String[] {" 12" + rest + "am$", " 00\\1"});
        steps.add(new String[] {" (0?[0-9]|1[01])" + rest + "am$", " \\1\\2"});
        steps.add(new String[] {" (1[2-9]|2[0-3])" + rest + "pm$", " \\1\\2"});
        for (int h = 1; h <= 11; h++) {
            steps.add(new String[] {" 0?" + h + rest + "pm$", " " + (h + 12) + "\\1"});
        }
        // the fraction cut to six places; zeros written before a one-digit part; the seconds when none
        steps.add(new String[] {"([.][0-9]{6})[0-9]+$", "\\1"});
        steps.add(new String[] {"^([0-9]{4})-([0-9])-", "\\1-0\\2-"});
        steps.add(new String[] {"^([0-9]{4}-[0-9]{2})-([0-9])( |$)", "\\1-0\\2\\3"});
        steps.add(new String[] {" ([0-9]):", " 0\\1:"});
        steps.add(new String[] {":([0-9])(:|[.]|$)", ":0\\1\\2"});
        steps.add(new String[] {":([0-9])(:|[.]|$)", ":0\\1\\2"});
        steps.add(new String[] {"^([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2})$", "\\1:00"});
        return List.copyOf(steps);
    }

    /** A constant's date and time as IQ reads it ({@link IqDateTime#read}: a year below 100 refused by name; IQ
     *  island, 2026-10-14: one helper with M1a's comparisons, review 3's B3; M1e: by the profile's DATE_ORDER). */
    private String @com.legend.base.Nullable [] isoParts(String text, SqlAst.Expr e) {
        return IqDateTime.read(text, settings, e.pos());
    }

    /** Whether {@code s} is spelled yyyy-mm-dd... (digits and dashes where ISO has them), whatever its values. */
    private static boolean looksIso(String s) {
        if (s.length() < 10 || s.charAt(4) != '-' || s.charAt(7) != '-') {
            return false;
        }
        for (int i : new int[] {0, 1, 2, 3, 5, 6, 8, 9}) {
            if (s.charAt(i) < '0' || s.charAt(i) > '9') {
                return false;
            }
        }
        return s.length() == 10;
    }

    /** CAST of an integer to BIT in SQL Anywhere's front end: 0 or 1, anything else IQ's -158. Under the IQ engine any
     *  nonzero integer is 1 (the research's SEMANTICS §2.2, not yet verified): refused unless the value is 0 or 1. A
     *  number with a fraction is refused (SQL Anywhere raises -158 for -0.5). */
    private Tx toBit(Tx x, SqlAst.Cast c, String what) {
        IqType type = IqType.integer("BIT", BigInteger.ZERO, BigInteger.ONE, false);
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(Kind.INTEGER).vs(), Kind.INTEGER, true, type);
        }
        if (x.kind() != Kind.INTEGER) {
            // measured: a fraction is no BIT (-0.5 is -158), by rules not read yet
            throw SqlTranslationException.unsupported(what + " of " + kindName(x.kind()) + " to BIT is not supported"
                    + " yet", c.pos());
        }
        IqType t = integerType(x, c.operand(), what);
        if (t.lo().signum() >= 0 && t.hi().compareTo(BigInteger.ONE) <= 0) {
            return Tx.value(x.vs(), Kind.INTEGER, x.nullable(), type);
        }
        if (engine == IqDialect.Engine.IQ) {
            throw SqlTranslationException.unsupported(what + " to BIT of a value other than 0 or 1 is not supported"
                    + " under the IQ engine yet: IQ's manual makes any nonzero integer 1, SQL Anywhere raises -158",
                    c.pos());
        }
        Tx r = SqlToPure.strict(Kind.INTEGER, List.of(x), v -> ifThen(SqlToPure.call("or", lt(v.get(0), integer(0)),
                SqlToPure.call("greaterThan", v.get(0), integer(1))), raise("Integer"), v.get(0)));
        return Tx.value(r.vs(), Kind.INTEGER, r.nullable(), type);
    }

    // ------------------------------------------------------------------ arguments

    /** A string argument: a string as it is, an integer as its digits (and sign); a NULL. Any other
     *  value (IQ writes it as text by its own format) is refused by name. */
    private Tx text(Tx t, SqlAst.Expr e, String fn) {
        if (t.kind() == Kind.NULL || t.kind() == Kind.STRING) {
            return t;
        }
        if (t.kind() == Kind.INTEGER && t.detail() instanceof IqType type && (type.isInteger()
                || type.family().startsWith("UNSIGNED"))) {
            Tx digits = SqlToPure.strict(Kind.STRING, List.of(t), v -> SqlToPure.call("toString", v.get(0)));
            return Tx.value(digits.vs(), Kind.STRING, digits.nullable(), varchar());
        }
        // IQ island, 2026-10-14 (M1e): a date or a timestamp in the profile's DATE_FORMAT or TIMESTAMP_FORMAT
        if (t.kind() == Kind.DATE || t.kind() == Kind.DATETIME) {
            return IqDateTime.text(t, settings, fn, e.pos());
        }
        throw SqlTranslationException.unsupported(fn + " of " + kindName(t.kind()) + " is not supported yet: IQ writes"
                + " it as text by its own format", e.pos());
    }

    /**
     * A count or a position argument: an integer (a SMALLINT, an INTEGER, a BIGINT, a BIT, a constant, one computed in
     * 64 bits), or a NULL. IQ island, 2026-10-17 (the corpus lane: 23 queries, {@code SUBSTRING(s, 2, LEN(s) -
     * 2)}; measured on IQ 17.1): IQ takes a 32-bit one. Past its range IQ is not consistent -- a constant's or a
     * computed value's is its error (-1009145 under its engine, -1001014 in SUBSTRING, -158 in its front end), a BIGINT
     * column's extremes give '' -- so the island refuses such a value by name where the row computes it. Postgres's
     * string functions take a 32-bit integer too, which its dialect narrows a 64-bit one to
     * ({@code StringCountsAsInt}). A value IQ converts (a NUMERIC truncated, a string read) is refused.
     */
    private Tx count(Tx t, SqlAst.Expr e, String fn) {
        if (t.kind() == Kind.NULL) {
            return t;
        }
        if (t.kind() == Kind.INTEGER && t.detail() instanceof IqType type && type.isInteger()) {
            if (type.family().equals("BIGINT") || type.wide() && !type.exact()) {
                String past = IqDialect.REFUSED_AT_RUN_TIME + fn + " with a count or position past 32 bits is not"
                        + " supported: IQ raises its error for a computed one, and reads a BIGINT column's otherwise";
                Tx checked = SqlToPure.strict(Kind.INTEGER, List.of(t), v -> refusedPast(SqlToPure.call("or",
                        SqlToPure.call("greaterThan", v.get(0), integer(Integer.MAX_VALUE)), SqlToPure.call("lessThan",
                        v.get(0), integer(Integer.MIN_VALUE))), past, v.get(0), IqDialect.raise()));
                return Tx.value(checked.vs(), Kind.INTEGER, checked.nullable(), IqType.integer("INTEGER",
                        BigInteger.valueOf(Integer.MIN_VALUE), BigInteger.valueOf(Integer.MAX_VALUE), false));
            }
            return t;
        }
        throw SqlTranslationException.unsupported(fn + " with a count or position of " + kindName(t.kind()) + " is not"
                + " supported yet (IQ converts it to an integer by its own rules)", e.pos());
    }

    /** The IQ integer type of {@code t}: one of the island's integer types, or an unsigned result whose range is
     *  known; anything else refused by name. */
    private static IqType integerType(Tx t, SqlAst.Expr e, String fn) {
        if (t.kind() == Kind.INTEGER && t.detail() instanceof IqType type && (type.isInteger()
                || type.family().startsWith("UNSIGNED") && type.low() != null)) {
            return type;
        }
        throw SqlTranslationException.unsupported(fn + " of " + (t.kind() == Kind.INTEGER && t.detail() instanceof
                IqType type ? "a " + type.spelling() : kindName(t.kind())) + " is not supported yet", e.pos());
    }

    /** Checks a number argument: an integer, a NUMERIC, a DOUBLE; a string (IQ's NUMERIC conversion) refused. */
    private static void number(Tx t, SqlAst.Expr e, String fn) {
        if (t.kind() == Kind.FLOAT) {
            doubleOnly(t, e, fn);
        } else if (!decimal(t)) {
            integerType(t, e, fn);
        }
    }

    /** A DOUBLE, never a REAL (its table is refused, but a REAL could arrive otherwise). */
    private static void doubleOnly(Tx t, SqlAst.Expr e, String fn) {
        if (t.detail() instanceof IqType type && !type.family().equals("DOUBLE")) {
            throw SqlTranslationException.unsupported(fn + " of a " + type.spelling() + " is not supported yet",
                    e.pos());
        }
    }

    private static boolean decimal(Tx t) {
        if (t.detail() instanceof IqType type && (type.family().equals("NUMERIC") || type.family().equals("DECIMAL"))) {
            return true;
        }
        return t.kind() == Kind.DECIMAL || t.kind() == Kind.NUMBER;
    }

    /** A NUMERIC's precision and scale, by its IQ type; refused when not known. */
    private static int[] scaleOf(Tx t, SqlAst.Expr e, String fn) {
        if (t.detail() instanceof IqType type && (type.family().equals("NUMERIC") || type.family().equals("DECIMAL"))
                && type.size() >= 0) {
            String sp = type.spelling();
            int comma = sp.indexOf(',');
            int scale = comma < 0 ? 0 : Integer.parseInt(sp.substring(comma + 1, sp.indexOf(')')).strip());
            return new int[] {type.size(), scale};
        }
        throw SqlTranslationException.unsupported(fn + " of a NUMERIC whose precision and scale are not known is not"
                + " supported", e.pos());
    }

    /** A number argument as a DOUBLE: an integer converted, a NUMERIC of at most 15 digits converted (exact in every
     *  database, as SQL Anywhere's conversion is), a DOUBLE itself, a NULL. */
    private Tx asDouble(Tx x, SqlAst.Expr e, String fn) {
        if (isNull(x)) {
            return Tx.value(SqlToPure.nullOf(Kind.FLOAT).vs(), Kind.FLOAT, true, new IqType("DOUBLE"));
        }
        if (x.kind() == Kind.FLOAT) {
            doubleOnly(x, e, fn);
            return x;
        }
        if (decimal(x)) {
            int[] ps = scaleOf(x, e, fn);
            if (ps[0] > 15) {
                throw SqlTranslationException.unsupported(fn + " of a NUMERIC of more than 15 digits is not supported"
                        + " yet: its DOUBLE may round otherwise in lite's databases", e.pos());
            }
        } else if (!(x.detail() instanceof IqType t && t.spelling().equals("UNSIGNED BIGINT"))) {
            // IQ island, 2026-10-17 (the corpus lane: LOG10(COUNT(x)), 8 queries): a COUNT's UNSIGNED BIGINT
            // under IQ's engine is a DOUBLE as a BIGINT is
            integerType(x, e, fn);
        }
        Tx r = SqlToPure.strict(Kind.FLOAT, List.of(x), v -> SqlToPure.call("toFloat", v.get(0)));
        return Tx.value(r.vs(), Kind.FLOAT, r.nullable(), new IqType("DOUBLE"));
    }

    /** An integer constant's value, signs applied; null for any other expression. */
    private static @com.legend.base.Nullable BigInteger signedInteger(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> n.value();
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.MINUS -> {
                BigInteger v = signedInteger(u.operand());
                yield v == null ? null : v.negate();
            }
            case SqlAst.Unary u when u.op() == SqlAst.UnaryOp.PLUS -> signedInteger(u.operand());
            default -> null;
        };
    }

    /** Whether {@code e} may read a column (every form but constants, signs, arithmetic and calls of them). */
    private static boolean readsColumn(SqlAst.Expr e) {
        return switch (e) {
            case SqlAst.IntegerLit n -> false;
            case SqlAst.NumericLit n -> false;
            case SqlAst.StringLit n -> false;
            case SqlAst.NullLit n -> false;
            case SqlAst.Unary u -> readsColumn(u.operand());
            case SqlAst.Binary b -> readsColumn(b.left()) || readsColumn(b.right());
            case SqlAst.Cast c -> readsColumn(c.operand());
            default -> true;
        };
    }

    // ------------------------------------------------------------------ Pure

    /** A strict function (NULL when any argument is), its string arguments' integers written as their digits. */
    private static Tx strict(Kind kind, IqType type, List<Tx> args, Function<List<ValueSpecification>,
            ValueSpecification> build) {
        Tx r = SqlToPure.strict(kind, args, v -> {
            List<ValueSpecification> values = new ArrayList<>(v);
            return build.apply(values);
        });
        ValueSpecification guard = r.guard();
        ValueSpecification body = r.body();
        if (guard != null && body != null && !mayBeEmpty(body)) {
            // IQ island, 2026-10-15 (the seventh line's merged guards, PR #41; the scale suite's cliff 2): the NULL
            // test carried apart from the body, so a nested strict function ORs it rather than testing this whole value
            // again -- sound only where the body is never empty while the guard is false, which an empty written
            // nowhere in it assures
            return new Tx(r.vs(), kind, r.nullable(), null, type, guard, body);
        }
        return Tx.value(r.vs(), kind, r.nullable(), type);
    }

    /** Whether {@code vs} writes an empty value anywhere (an empty collection, Pure's NULL), so may compute none. */
    private static boolean mayBeEmpty(ValueSpecification vs) {
        java.util.ArrayDeque<ValueSpecification> todo = new java.util.ArrayDeque<>();
        java.util.Set<ValueSpecification> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        todo.push(vs);
        while (!todo.isEmpty()) {
            ValueSpecification x = todo.pop();
            if (!seen.add(x)) {
                continue;
            }
            switch (x) {
                case com.legend.protocol.spec.PureCollection c -> {
                    if (c.values().isEmpty()) {
                        return true;
                    }
                    c.values().forEach(todo::push);
                }
                case com.legend.protocol.spec.AppliedFunction f -> f.parameters().forEach(todo::push);
                case com.legend.protocol.spec.AppliedProperty a -> todo.push(a.receiver());
                case com.legend.protocol.spec.LambdaFunction l -> l.body().forEach(todo::push);
                default -> {
                }
            }
        }
        return false;
    }

    private static IqType nonNegative() {
        return IqType.integer("INTEGER", BigInteger.ZERO, IqType.INTEGER_MAX, false);
    }

    private static IqType varchar() {
        return new IqType("VARCHAR");
    }

    private static Tx nullString() {
        return Tx.value(SqlToPure.nullOf(Kind.STRING).vs(), Kind.STRING, true, varchar());
    }

    private static CInteger integer(long v) {
        return new CInteger(v);
    }

    private static ValueSpecification lt(ValueSpecification a, ValueSpecification b) {
        return SqlToPure.call("lessThan", a, b);
    }

    private static ValueSpecification ifThen(ValueSpecification test, ValueSpecification then,
            ValueSpecification otherwise) {
        return SqlToPure.call("if", test, SqlToPure.thunk(then), SqlToPure.thunk(otherwise));
    }

    /** {@code x} (a Date) as the date type {@code type} it is (Pure's datePart and adjust are of a Date). */
    private static ValueSpecification typed(ValueSpecification x, String type) {
        return SqlToPure.call("cast", x, SqlToPure.typeArg(type));
    }

    /** A value of Pure type {@code type} that raises when computed: an empty value read as one. */
    private static ValueSpecification raise(String type) {
        return SqlToPure.call("toOne", SqlToPure.call("cast", SqlToPure.empty(), SqlToPure.typeArg(type)));
    }

    private static String code(int sqlcode, String sqlstate) {
        return " (IQ: SQLCODE " + sqlcode + ", SQLSTATE " + sqlstate + ")";
    }

    private static String kindName(Kind k) {
        return switch (k) {
            case BOOLEAN -> "a condition";
            case STRING -> "a string";
            case DATE -> "a date";
            case DATETIME, ZONED, ZONED_TIME -> "a timestamp";
            case TIME -> "a time";
            case DURATION -> "a duration";
            case NULL -> "NULL";
            case INTEGER, WHOLE, FLOAT, DECIMAL, NUMBER, INEXACT -> "a number";
            case JSON, OTHER -> "a value of this type";
        };
    }
}
