// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.base;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * AN OPEN LINE OF THE EDGE SEPARATION CENSUS (the GUARD stream's C.7, the edge audit of 2026-11-17: "open-line classes
 * marked by an annotation, not by name, so a rename cannot close them"): a compatibility class still standing in a
 * product package, its census line the value ({@code "B2"}, {@code "C5"}). The census counts the class and every product
 * class that depends on it ({@code EdgeSeparationTest.COMPAT_CLASSES}); {@code OpenLineMarkedTest} holds the marked
 * classes and that list equal, so renaming a marked class keeps it counted under its new name, and closing its line is
 * an act -- the class moved to the edge or deleted, or this mark removed with its ruling -- never a side effect.
 * Source retention: the mark is read from the source, so no product class depends on this type in its bytecode (the sql
 * layer's {@code ArchitectureTest.sqlLayerIsFullyStandalone} unchanged).
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface OpenLine {

    /** The census line ({@code "B2"}, {@code "C5"}). */
    String value();
}
