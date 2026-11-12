// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A dialect method that is not a spelling, which the compatibility edge's dialects may still override, for the
 * {@link #value reason} given (phase D, 2026-11-12, the orchestrator's second named kind): the database CONFIGURATION
 * upstream's SQL ran in (a session's statements, a database's settings). A plan-structure decision (a parameter's type,
 * a delivery, the raise read-back, a refusal) is never marked so. {@code EdgeSpellingHookTest} enforces both kinds.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface EdgeMayOverride {

    /** Why the edge may override it. */
    String value();
}
