// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A dialect method that only SPELLS -- a text, a literal, a name -- which the compatibility edge's dialects
 * ({@code edge.PureText}) may override to write upstream's spelling (phase D, 2026-11-12, the orchestrator's guard: the
 * edge overrides spelling, never a decision). A method the planner asks a decision of (a parameter's type, how data is
 * delivered, the raise read-back, a refusal, a capability) is never one: an edge override of it would make planning
 * itself a mode. {@code EdgeSpellingHookTest} holds every edge override to a method marked so, or
 * {@link EdgeMayOverride}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface SpellingArm {
}
