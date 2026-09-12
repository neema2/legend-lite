// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

/**
 * The engine's execution FEATURE FLAGS — {@code meta::pure::executionPlan::features::Feature}
 * (executionPlanFeature.pure), one closed enum, mirrored here member for member
 * (spec's {@code FeatureFlagParityTest} holds the two equal).
 *
 * <p>A flag is a COMPILE-TIME fact of one query: it rides an
 * {@code ExecutionOptionContext}'s {@code FeatureFlagOption} on the execute /
 * executionPlan call ({@link ExecutionContext#features}), or a runner's
 * defaults ({@code ExecuteOptions.features}), and is consumed in the LOWERING
 * only ({@code Lowerer.withFeatures}): a flag selects an emission for a call,
 * never a different typed tree. Every member states its consumer; a flag with
 * none is LOUD when set, never ignored.
 */
public enum Feature {
    /** The engine transforms enum values in SQL (on) or in Java after the
     *  query (off). The platform only ever transforms in SQL ("Java
     *  orchestrates, the database executes"): both settings select its ONE
     *  behaviour — a documented no-op. */
    PUSH_DOWN_ENUM_TRANSFORM,
    /** Variant-typed inputs to externalize / internalize (engine-internal
     *  programs; no corpus test sets it). No consumer: loud when set. */
    VARIANT_TYPE_AS_INPUT,
    /** Pre-#4900 equality: plain {@code =} where the platform emits
     *  IS NOT DISTINCT FROM. Consumer: the Lowerer's equality rewrite
     *  (NullSemantics.verbatim). The plan-template selector half of the
     *  engine's legacy behaviour is not carried. */
    LEGACY_SQL_NULL_UNSAFE_EQUALS,
    /** Pure's 0-based, end-exclusive substring indexes become SQL's 1-based
     *  start and length (the engine's processSubstr: literals folded,
     *  computed indexes as plus/minus). Consumer: Scalars' flagged rule for
     *  the {@code substring} overloads, selected in place of the verbatim
     *  emission. Off by default — "correcting
     *  substring unconditionally would change the results of existing queries"
     *  (the engine's own words); the engine's testable runner and this
     *  platform's PCT adapter turn it on. */
    CORRECT_SQL_SUBSTRING_INDEXING,
    /** Implicit sorts drop the NULLS clause and ride the database's native
     *  placement. Its only corpus witnesses are printer tests the platform
     *  walls, so no claim: loud when set. */
    USE_DB_NATIVE_IMPLICIT_NULL_ORDERING;

    /** The engine enum's fully qualified name. */
    public static final String FQN = "meta::pure::executionPlan::features::Feature";
}
