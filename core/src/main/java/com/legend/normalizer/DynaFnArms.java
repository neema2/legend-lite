package com.legend.normalizer;

import com.legend.builtin.DynaFn;
import com.legend.builtin.Pure;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * The mapping translator's DECLARED arm facts — public so the registry's test
 * can hold the declarations against the code and the catalog:
 * <ul>
 *   <li>{@link #ARMS}: every engine dynafunction {@code RelOpTranslator} has a
 *       rewriting arm for (the {@code case … when dyna(call) == DynaFn.X}
 *       guards). The test derives the same set from the translator's SOURCE —
 *       every {@code DynaFn.X} member it names — and holds them equal; every
 *       {@code DynaFn.Resolution.TRANSLATED} name is in here, and every name in
 *       here that is not TRANSLATED is PURE (the arm rewrites the engine's EXTRA
 *       shape — {@code and(a, b, c)}, {@code parseDate(s, fmt)} — and pure's own
 *       shape passes through to the catalog).</li>
 *   <li>{@link #LANDINGS}: the {@link Pure.Lite} identities the FORMAT arms
 *       land on — engine vocabulary with a shape pure lacks (a format
 *       argument). With the registry's SHIM rows they ARE
 *       {@code Pure.ENGINE_VOCAB_SHIMS}; the test holds that equality.</li>
 * </ul>
 */
public final class DynaFnArms {

    private DynaFnArms() {
    }

    /** Every engine dynafunction with a translator arm. SQL_NULL since 2026-11-16 (the Studio roundtrip case
     *  STO_Relational-mapping-join-chains-and-distinct): a case's or an if's branch that is the store's NULL is Pure's
     *  {@code []} (RelOpTranslator.branch); anywhere else pure's own {@code sqlNull()} passes through (PURE). */
    public static final Set<DynaFn> ARMS = Collections.unmodifiableSet(EnumSet.of(
            DynaFn.ADD, DynaFn.ADJUST, DynaFn.AND, DynaFn.CASE,
            DynaFn.CONCAT, DynaFn.CONVERT_DATE,
            DynaFn.CONVERT_DATE_TIME, DynaFn.CONVERT_TIME_ZONE,
            DynaFn.CONVERT_VARCHAR128, DynaFn.DAY_OF_WEEK,
            DynaFn.DAY_OF_WEEK_NUMBER, DynaFn.EXTRACT_FROM_SEMI_STRUCTURED,
            DynaFn.GROUP, DynaFn.IF, DynaFn.INDEX_OF,
            DynaFn.IS_NOT_NULL, DynaFn.IS_NULL,
            DynaFn.MD5, DynaFn.OR,
            DynaFn.PARSE_DATE, DynaFn.POSITION,
            DynaFn.SHA1, DynaFn.SHA256, DynaFn.SPLIT_PART, DynaFn.SQL_NULL, DynaFn.SUB,
            DynaFn.SUBSTRING, DynaFn.TO_TIMESTAMP));

    /** The Lite identities the format arms land on. */
    public static final Map<DynaFn, String> LANDINGS = Map.of(
            DynaFn.PARSE_DATE, Pure.Lite.PARSE_DATE_FORMAT,
            DynaFn.CONVERT_DATE, Pure.Lite.CONVERT_DATE_FORMAT,
            DynaFn.CONVERT_DATE_TIME, Pure.Lite.CONVERT_DATE_TIME_FORMAT,
            DynaFn.TO_TIMESTAMP, Pure.Lite.CONVERT_DATE_TIME_FORMAT,
            DynaFn.CONVERT_TIME_ZONE, Pure.Lite.CONVERT_TIME_ZONE_FORMAT);

}
