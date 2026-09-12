// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * THE H2 session — the engine's own, VERBATIM (convergence batch C,
 * user-ratified "converge directly", landed 2026-08-29): H2Defaults
 * from legend-engine-xt-relationalStore-h2-execution-2.1.214 —
 * case-SENSITIVE identifiers, no DATABASE_TO_UPPER override, the
 * engine's NON_KEYWORDS list (incl OVER), MODE=LEGACY. ONE definition
 * shared by every H2 session opener so all targets open IDENTICAL
 * sessions.
 *
 * <p>HISTORY: this constant used to add CASE_INSENSITIVE_IDENTIFIERS
 * + DATABASE_TO_UPPER=false — session-level compensation for OUR OWN
 * spelling skew (create full-quoted vs insert bare; renderer quoting
 * what DDL spelled bare; bare aliases uppercasing in labels). The
 * cure was conform-by-emission end to end: per-target DDL/insert
 * spelling, declared-quote preservation, and the ORIGIN-driven
 * renderer (a column reference knows whether its name is PHYSICAL —
 * DDL-owned, bare-unless-special — or DERIVED — query-invented,
 * quoted like the engine spells every alias). Receipts: all four
 * consumers probed green on THIS session (oracle sweep
 * census-byte-identical; h2 lane 1369; PCT DuckDB + PCT h2modern).
 */
public final class H2Settings {

    private H2Settings() {
    }

    /** JDBC-URL suffix, {@code ;KEY=VALUE} form — the engine's own H2
     *  connection settings (H2Manager / H2Defaults, 4.145.0), so a golden
     *  replayed here orders as it did in the engine's run:
     *  {@code DEFAULT_NULL_ORDERING=HIGH} is the engine's canonical null
     *  placement (null is largest — ASC nulls last, DESC nulls first; batch
     *  8, the eleven testGroupBy.pure desc sorts) which its printer leaves
     *  BARE on H2 2.x because the session already orders that way. */
    // NOT a compile-time constant (String.join, not a literal): a literal
    // would be INLINED into every class that reads it — the spec module's
    // test classes kept the pre-4.145.0 value across a core install until
    // a clean (batch 8: two chains chased a null-ordering divergence that
    // was a stale inlined string)
    public static final String SETTINGS = String.join("",
            ";NON_KEYWORDS=ANY,ASYMMETRIC,AUTHORIZATION,CAST,",
            "CURRENT_PATH,CURRENT_ROLE,DAY,DEFAULT,ELSE,END,HOUR,KEY,",
            "MINUTE,MONTH,SECOND,SESSION_USER,SET,SOME,SYMMETRIC,",
            "SYSTEM_USER,TO,UESCAPE,USER,VALUE,WHEN,YEAR,OVER",
            ";MODE=LEGACY",
            ";DEFAULT_NULL_ORDERING=HIGH");
}
