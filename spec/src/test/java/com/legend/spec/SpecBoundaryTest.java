// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.spec;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

/**
 * THE UPSTREAM BOUNDARY of the spec module (batch 7c, 2026-09-11): this module
 * reads the pinned checkouts as FILES — it never imports the engine's or
 * pure's Java. The Maven enforcer bans the artifacts; this bans the imports.
 * Only this module's own classes are imported (its test-classes directory),
 * so core's classes on the classpath are not re-judged here.
 */
class SpecBoundaryTest {

    @Test
    void upstreamJavaNeverEntersSpec() {
        // spec's own compiled tests, found where they actually are: Maven's
        // target/test-classes directory, or the jar Bazel packs them into. The
        // relative "target/test-classes" this used to import exists only under
        // Maven — under Bazel it imported nothing, and ArchUnit's "failed to
        // check any classes" guard is what said so (2026-09-22).
        JavaClasses own = new ClassFileImporter().importUrl(
                SpecBoundaryTest.class.getProtectionDomain().getCodeSource().getLocation());
        noClasses()
            .that().resideInAPackage("com.legend..")
            .should().dependOnClassesThat().resideInAnyPackage("org.finos.legend..")
            .as("the upstream boundary: spec reads checkouts as files and imports no org.finos.legend class")
            .check(own);
    }
}
