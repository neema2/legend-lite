/**
 * Phase I &mdash; lowering: typed HIR ({@code TypedSpec}) &rarr; SQL IR
 * ({@code com.legend.sql}). PHASE_HIJ_LOWERING.md governs.
 *
 * <p>The lean-SQL tenet is enforced structurally: every relational op asks the
 * ONE {@link com.legend.lowering.Fold} authority whether it can extend the
 * current {@code SqlSelect} (and into which clause slot) or must isolate it as
 * a subselect. Scalar natives dispatch on the RESOLVED overload's identity
 * (which signature Phase G chose), never on name strings.
 */
package com.legend.lowering;
