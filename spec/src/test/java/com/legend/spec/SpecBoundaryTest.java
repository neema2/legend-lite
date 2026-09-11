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
        JavaClasses own = new ClassFileImporter().importPath("target/test-classes");
        noClasses()
            .that().resideInAPackage("com.legend..")
            .should().dependOnClassesThat().resideInAnyPackage("org.finos.legend..")
            .as("the upstream boundary: spec reads checkouts as files and imports no org.finos.legend class")
            .check(own);
    }
}
