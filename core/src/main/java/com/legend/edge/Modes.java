// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.mode.Mode;
import com.legend.parser.GrammarLevel;

/**
 * THE COMPATIBILITY EDGE's TWO MODES (S1m of the presentation program, 2026-11-18,
 * docs/records/presentation-program-design-2026-11-18.md §3.1; the product's own is {@link Mode#LITE}). A door chooses
 * one and reads its grammar and its style from it:
 * <ul>
 *   <li>{@link #ENGINE}, legend-engine end to end: its grammar ({@link GrammarLevel#LEGEND_ENGINE}, the one place it is
 *       named outside the parser: {@code ArchitectureTest.legendEnginesGrammarIsTheEdgesAlone}) and pure/v1's style --
 *       the doors of pure/v1, the hosts' edge routes, plan export and import;</li>
 *   <li>{@link #PURE}, legend-pure's meaning and text: lite's grammar (legend-pure's code uses the function types and
 *       generics LEGEND_LITE declares) and the PCT's style -- PCT's channels A and B, and PURE's direct entry.</li>
 * </ul>
 * The corpus is ENGINE by ruling; until its forms move to ENGINE (S5b, measured against its rosters) it chooses PURE,
 * whose style it runs today (the S1m shape note's (iii), 2026-11-18).
 */
public final class Modes {

    private Modes() {
    }

    /** ENGINE: legend-engine's grammar and pure/v1's style ({@link PureText#PURE_V1}). */
    public static final Mode ENGINE = new Mode("legend-engine", GrammarLevel.LEGEND_ENGINE, PureText.PURE_V1);

    /** PURE: lite's grammar and the PCT's style ({@link PureText#UPSTREAM}). */
    public static final Mode PURE = new Mode("legend-pure", GrammarLevel.LEGEND_LITE, PureText.UPSTREAM);
}
