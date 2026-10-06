// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.builtin;

import com.legend.builtin.DynaFn.Resolution;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * THE PLATFORM'S DECISION for each engine dynafunction, written by hand: how this platform resolves the operator
 * names {@link DynaFn} (generated from the pinned engine alone) carries. An upstream name not listed here is
 * {@link Resolution#UNSUPPORTED}, so a name an upgrade adds arrives unsupported until someone decides it.
 * <ul>
 *   <li>{@link Resolution#PURE}: the engine's operator IS pure's function; its declarations are the engine
 *       surface's FQNs for the name ({@link EngineHandlers#fqnsOf}, the same tier a bare call gets), else the
 *       declared {@link #RESIDUE};</li>
 *   <li>{@link Resolution#SHIM}: an engine-only operator; its {@link Pure.Lite} identity, named here;</li>
 *   <li>{@link Resolution#TRANSLATED}: the mapping translator rewrites the call (its arms, {@code DynaFnArms}).</li>
 * </ul>
 * {@code DynaFnRegistryTest} holds every decision: a PURE name has catalog declarations, a SHIM a registered Lite
 * native, every TRANSLATED name an arm. A decision names a generated member, so it cannot outlive its upstream name.
 */
public final class DynaFnDecisions {

    private DynaFnDecisions() {
    }

    /** A decision: its resolution, and a SHIM's Lite FQN. */
    private record Decision(Resolution resolution, String liteFqn) {
    }

    private static final Decision PURE = new Decision(Resolution.PURE, "");
    private static final Decision TRANSLATED = new Decision(Resolution.TRANSLATED, "");

    private static Decision shim(String liteFqn) {
        return new Decision(Resolution.SHIM, liteFqn);
    }

    /** By member (the engine's operator name), alphabetical. Unlisted: UNSUPPORTED. A decision names a generated
     *  member, so a name an upgrade drops fails to compile here. */
    private static final Map<DynaFn, Decision> BY_MEMBER = Map.ofEntries(
            Map.entry(DynaFn.ABS, PURE),
            Map.entry(DynaFn.ACOS, PURE),
            Map.entry(DynaFn.ADD, TRANSLATED),
            Map.entry(DynaFn.ADJUST, TRANSLATED),
            Map.entry(DynaFn.AND, PURE),
            Map.entry(DynaFn.ASCII, PURE),
            Map.entry(DynaFn.ASIN, PURE),
            Map.entry(DynaFn.ATAN, PURE),
            Map.entry(DynaFn.ATAN2, PURE),
            Map.entry(DynaFn.AVERAGE, PURE),
            Map.entry(DynaFn.BETWEEN, PURE),
            Map.entry(DynaFn.BIT_AND, PURE),
            Map.entry(DynaFn.BIT_NOT, PURE),
            Map.entry(DynaFn.BIT_OR, PURE),
            Map.entry(DynaFn.BIT_SHIFT_LEFT, PURE),
            Map.entry(DynaFn.BIT_SHIFT_RIGHT, PURE),
            Map.entry(DynaFn.BIT_XOR, PURE),
            Map.entry(DynaFn.CASE, TRANSLATED),
            Map.entry(DynaFn.CAST, PURE),
            Map.entry(DynaFn.CBRT, PURE),
            Map.entry(DynaFn.CEILING, PURE),
            Map.entry(DynaFn.CHAR, PURE),
            Map.entry(DynaFn.COALESCE, PURE),
            Map.entry(DynaFn.CONCAT, TRANSLATED),
            Map.entry(DynaFn.CONTAINS, PURE),
            Map.entry(DynaFn.CONVERT_DATE, TRANSLATED),
            Map.entry(DynaFn.CONVERT_DATE_TIME, TRANSLATED),
            Map.entry(DynaFn.CONVERT_TIME_ZONE, TRANSLATED),
            Map.entry(DynaFn.CONVERT_VARCHAR128, TRANSLATED),
            Map.entry(DynaFn.CORR, PURE),
            Map.entry(DynaFn.COS, PURE),
            Map.entry(DynaFn.COSH, PURE),
            Map.entry(DynaFn.COT, PURE),
            Map.entry(DynaFn.COUNT, PURE),
            Map.entry(DynaFn.COVAR_POPULATION, PURE),
            Map.entry(DynaFn.COVAR_SAMPLE, PURE),
            Map.entry(DynaFn.CUMULATIVE_DISTRIBUTION, PURE),
            Map.entry(DynaFn.CURRENT_USER_ID, PURE),
            Map.entry(DynaFn.DATE, PURE),
            Map.entry(DynaFn.DATE_DIFF, PURE),
            Map.entry(DynaFn.DATE_PART, PURE),
            Map.entry(DynaFn.DAY_OF_MONTH, PURE),
            Map.entry(DynaFn.DAY_OF_WEEK, TRANSLATED),
            Map.entry(DynaFn.DAY_OF_WEEK_NUMBER, TRANSLATED),
            Map.entry(DynaFn.DAY_OF_YEAR, PURE),
            Map.entry(DynaFn.DECODE_BASE64, PURE),
            Map.entry(DynaFn.DENSE_RANK, PURE),
            Map.entry(DynaFn.DISTINCT, PURE),
            Map.entry(DynaFn.DIVIDE, PURE),
            Map.entry(DynaFn.DIVIDE_ROUND, shim(Pure.Lite.DIVIDE_ROUND)),
            Map.entry(DynaFn.ENCODE_BASE64, PURE),
            Map.entry(DynaFn.ENDS_WITH, PURE),
            Map.entry(DynaFn.EQUAL, PURE),
            Map.entry(DynaFn.EXISTS, PURE),
            Map.entry(DynaFn.EXP, PURE),
            Map.entry(DynaFn.EXTRACT_FROM_SEMI_STRUCTURED, TRANSLATED),
            Map.entry(DynaFn.FIRST, PURE),
            Map.entry(DynaFn.FIRST_DAY_OF_MONTH, PURE),
            Map.entry(DynaFn.FIRST_DAY_OF_QUARTER, PURE),
            Map.entry(DynaFn.FIRST_DAY_OF_THIS_MONTH, PURE),
            Map.entry(DynaFn.FIRST_DAY_OF_THIS_QUARTER, PURE),
            Map.entry(DynaFn.FIRST_DAY_OF_THIS_YEAR, PURE),
            Map.entry(DynaFn.FIRST_DAY_OF_WEEK, PURE),
            Map.entry(DynaFn.FIRST_DAY_OF_YEAR, PURE),
            Map.entry(DynaFn.FIRST_HOUR_OF_DAY, PURE),
            Map.entry(DynaFn.FIRST_MILLISECOND_OF_SECOND, PURE),
            Map.entry(DynaFn.FIRST_MINUTE_OF_HOUR, PURE),
            Map.entry(DynaFn.FIRST_SECOND_OF_MINUTE, PURE),
            Map.entry(DynaFn.FLOOR, PURE),
            Map.entry(DynaFn.FORMAT_DATE, PURE),
            Map.entry(DynaFn.GENERATE_GUID, PURE),
            Map.entry(DynaFn.GREATER_THAN, shim(Pure.Lite.GREATER_THAN_ANY)),
            Map.entry(DynaFn.GREATER_THAN_EQUAL, shim(Pure.Lite.GREATER_THAN_EQUAL_ANY)),
            Map.entry(DynaFn.GREATEST, PURE),
            Map.entry(DynaFn.GROUP, TRANSLATED),
            Map.entry(DynaFn.HASH_CODE, PURE),
            Map.entry(DynaFn.HOUR, PURE),
            Map.entry(DynaFn.IF, TRANSLATED),
            Map.entry(DynaFn.IN, PURE),
            Map.entry(DynaFn.INDEX_OF, TRANSLATED),
            Map.entry(DynaFn.IS_ALPHA_NUMERIC, PURE),
            Map.entry(DynaFn.IS_DISTINCT, shim(Pure.Lite.IS_DISTINCT_FROM)),
            Map.entry(DynaFn.IS_EMPTY, PURE),
            Map.entry(DynaFn.IS_NOT_EMPTY, PURE),
            Map.entry(DynaFn.IS_NOT_NULL, TRANSLATED),
            Map.entry(DynaFn.IS_NULL, TRANSLATED),
            Map.entry(DynaFn.IS_NUMERIC, shim(Pure.Lite.IS_NUMERIC)),
            Map.entry(DynaFn.JARO_WINKLER_SIMILARITY, PURE),
            Map.entry(DynaFn.JOIN_STRINGS, PURE),
            Map.entry(DynaFn.KEYS, PURE),
            Map.entry(DynaFn.LAG, PURE),
            Map.entry(DynaFn.LAST, PURE),
            Map.entry(DynaFn.LEAD, PURE),
            Map.entry(DynaFn.LEAST, PURE),
            Map.entry(DynaFn.LEFT, PURE),
            Map.entry(DynaFn.LENGTH, PURE),
            Map.entry(DynaFn.LESS_THAN, shim(Pure.Lite.LESS_THAN_ANY)),
            Map.entry(DynaFn.LESS_THAN_EQUAL, shim(Pure.Lite.LESS_THAN_EQUAL_ANY)),
            Map.entry(DynaFn.LEVENSHTEIN_DISTANCE, PURE),
            Map.entry(DynaFn.LOG, PURE),
            Map.entry(DynaFn.LOG10, PURE),
            Map.entry(DynaFn.LPAD, PURE),
            Map.entry(DynaFn.LTRIM, PURE),
            Map.entry(DynaFn.MATCHES, PURE),
            Map.entry(DynaFn.MAX, PURE),
            Map.entry(DynaFn.MAX_BY, PURE),
            Map.entry(DynaFn.MD5, TRANSLATED),
            Map.entry(DynaFn.MEDIAN, PURE),
            Map.entry(DynaFn.MIN, PURE),
            Map.entry(DynaFn.MIN_BY, PURE),
            Map.entry(DynaFn.MINUS, PURE),
            Map.entry(DynaFn.MINUTE, PURE),
            Map.entry(DynaFn.MOD, PURE),
            Map.entry(DynaFn.MODE, PURE),
            Map.entry(DynaFn.MONTH, PURE),
            Map.entry(DynaFn.MONTH_NUMBER, PURE),
            Map.entry(DynaFn.MOST_RECENT_DAY_OF_WEEK, PURE),
            Map.entry(DynaFn.NOT, PURE),
            Map.entry(DynaFn.NOT_EQUAL_ANSI, shim(Pure.Lite.NOT_EQUAL_ANSI)),
            Map.entry(DynaFn.NOW, PURE),
            Map.entry(DynaFn.NTH, PURE),
            Map.entry(DynaFn.NTILE, PURE),
            Map.entry(DynaFn.OBJECT_REFERENCE_IN, PURE),
            Map.entry(DynaFn.OR, PURE),
            Map.entry(DynaFn.PARSE_BOOLEAN, PURE),
            Map.entry(DynaFn.PARSE_DATE, PURE),
            Map.entry(DynaFn.PARSE_DECIMAL, PURE),
            Map.entry(DynaFn.PARSE_FLOAT, PURE),
            Map.entry(DynaFn.PARSE_INTEGER, PURE),
            Map.entry(DynaFn.PERCENT_RANK, PURE),
            Map.entry(DynaFn.PERCENTILE, PURE),
            Map.entry(DynaFn.PLUS, PURE),
            Map.entry(DynaFn.POSITION, TRANSLATED),
            Map.entry(DynaFn.POW, PURE),
            Map.entry(DynaFn.PREVIOUS_DAY_OF_WEEK, PURE),
            Map.entry(DynaFn.QUARTER, PURE),
            Map.entry(DynaFn.QUARTER_NUMBER, PURE),
            Map.entry(DynaFn.RANGE, PURE),
            Map.entry(DynaFn.RANK, PURE),
            Map.entry(DynaFn.REGEXP_COUNT, PURE),
            Map.entry(DynaFn.REGEXP_EXTRACT, PURE),
            Map.entry(DynaFn.REGEXP_INDEX_OF, PURE),
            Map.entry(DynaFn.REGEXP_LIKE, PURE),
            Map.entry(DynaFn.REGEXP_REPLACE, PURE),
            Map.entry(DynaFn.REM, PURE),
            Map.entry(DynaFn.REPEAT_STRING, PURE),
            Map.entry(DynaFn.REPLACE, PURE),
            Map.entry(DynaFn.REVERSE_STRING, PURE),
            Map.entry(DynaFn.RIGHT, PURE),
            Map.entry(DynaFn.ROUND, PURE),
            Map.entry(DynaFn.ROW_NUMBER, PURE),
            Map.entry(DynaFn.RPAD, PURE),
            Map.entry(DynaFn.RTRIM, PURE),
            Map.entry(DynaFn.SECOND, PURE),
            Map.entry(DynaFn.SHA1, TRANSLATED),
            Map.entry(DynaFn.SHA256, TRANSLATED),
            Map.entry(DynaFn.SIGN, PURE),
            Map.entry(DynaFn.SIN, PURE),
            Map.entry(DynaFn.SINH, PURE),
            Map.entry(DynaFn.SIZE, PURE),
            Map.entry(DynaFn.SPLIT_PART, TRANSLATED),
            Map.entry(DynaFn.SQL_FALSE, PURE),
            Map.entry(DynaFn.SQL_NULL, PURE),
            Map.entry(DynaFn.SQL_TRUE, PURE),
            Map.entry(DynaFn.SQRT, PURE),
            Map.entry(DynaFn.STARTS_WITH, PURE),
            Map.entry(DynaFn.STD_DEV_POPULATION, PURE),
            Map.entry(DynaFn.STD_DEV_SAMPLE, PURE),
            Map.entry(DynaFn.SUB, TRANSLATED),
            Map.entry(DynaFn.SUBSTRING, TRANSLATED),
            Map.entry(DynaFn.SUM, PURE),
            Map.entry(DynaFn.TAN, PURE),
            Map.entry(DynaFn.TANH, PURE),
            Map.entry(DynaFn.TIME_BUCKET, PURE),
            Map.entry(DynaFn.TIMES, PURE),
            Map.entry(DynaFn.TO_DECIMAL, PURE),
            Map.entry(DynaFn.TO_FLOAT, PURE),
            Map.entry(DynaFn.TO_LOWER, PURE),
            Map.entry(DynaFn.TO_ONE, PURE),
            Map.entry(DynaFn.TO_STRING, PURE),
            Map.entry(DynaFn.TO_TIMESTAMP, TRANSLATED),
            Map.entry(DynaFn.TO_UPPER, PURE),
            Map.entry(DynaFn.TO_VARIANT, PURE),
            Map.entry(DynaFn.TODAY, PURE),
            Map.entry(DynaFn.TRIM, PURE),
            Map.entry(DynaFn.VALUES, PURE),
            Map.entry(DynaFn.VARIANCE, PURE),
            Map.entry(DynaFn.VARIANCE_POPULATION, PURE),
            Map.entry(DynaFn.VARIANCE_SAMPLE, PURE),
            Map.entry(DynaFn.WEEK_OF_YEAR, PURE),
            Map.entry(DynaFn.YEAR, PURE));

    /** The three engine operators no handler names: the platform's own relational constants, each spelled by its
     *  catalog declaration. A PURE name that is neither on the engine surface nor here has no declarations (the
     *  registry test fails it): membership is a decision, never a by-name match. */
    static final Map<DynaFn, List<String>> RESIDUE = Map.of(
            DynaFn.SQL_NULL, List.of(Pure.SQL_NULL.qualifiedName()),
            DynaFn.SQL_TRUE, List.of(Pure.SQL_TRUE.qualifiedName()),
            DynaFn.SQL_FALSE, List.of(Pure.SQL_FALSE.qualifiedName()));

    static Resolution resolution(DynaFn d) {
        Decision x = BY_MEMBER.get(d);
        return x == null ? Resolution.UNSUPPORTED : x.resolution();
    }

    /** Each member's declarations, computed once, at first use (the engine surface is joined when EngineHandlers
     *  loads). */
    private static final class Declarations {
        static final Map<DynaFn, List<String>> OF;

        static {
            Map<DynaFn, List<String>> of = new EnumMap<>(DynaFn.class);
            for (DynaFn d : DynaFn.values()) {
                Decision x = BY_MEMBER.get(d);
                List<String> fqns = List.of();
                if (x != null && x.resolution() == Resolution.PURE) {
                    fqns = EngineHandlers.fqnsOf(d.dynaName());
                    if (fqns.isEmpty()) {
                        fqns = RESIDUE.getOrDefault(d, List.of());
                    }
                } else if (x != null && x.resolution() == Resolution.SHIM) {
                    fqns = List.of(x.liteFqn());
                }
                of.put(d, fqns);
            }
            OF = Map.copyOf(of);
        }
    }

    static List<String> fqns(DynaFn d) {
        return Declarations.OF.getOrDefault(d, List.of());
    }
}
