// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.SoftReference;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * THE GUARDS' ONE IMPORT OF A JAR LIST (2026-11-19, the checks lane's time): the classes of the jars the declared lists
 * name ({@code -Dlegend.product.jars} and the rest, {@link ConstantPools#jars}), imported by ArchUnit ONCE per set of
 * jars in a JVM and handed to every guard that reads that set.
 *
 * <p>Until then each guard method made its own {@code ClassFileImporter} over the same jars: the product's, the hosts'
 * doors' and the edge's were imported seventeen times in {@code //core:guardrails_edge} (eleven by
 * {@code ArchitectureTest.coreAndTheHostsDoors}, three by SqlFnSortedTest, one each by EdgeOnlyMembersTest,
 * ServiceLoaderSitesTest, PlanStyleSurfaceTest and EdgeOnlyDataTest), the product's and the doors' seven times, the
 * product's and the edge's four; the target ran 447 s of the checks lane's 1049 s, its heap at its cap (main's CI, run
 * 44437840195).
 *
 * <p>The same jars give the same import: what a guard reads here is what its own import read. The key is the SET of
 * jars, so two guards that list the same lists in another order share one import -- which is only the same import when
 * no class is in two of the jars, so that is checked as each set is imported (a class in two jars is refused by name,
 * never imported first-wins). ArchUnit's {@link JavaClasses} is immutable, and a guard filters it ({@code that(..)}) into
 * a view of its own. A set's import is held softly: when the heap needs it the JVM lets an import go, and the next guard
 * that asks for that set imports it again.
 */
final class ImportedJars {

    private static final Map<List<Path>, SoftReference<JavaClasses>> IMPORTED = new HashMap<>();

    private ImportedJars() {
    }

    /** The classes of the jars the declared lists {@code properties} name, imported once per set of jars. */
    static synchronized JavaClasses of(String... properties) {
        List<Path> jars = new ArrayList<>(new TreeSet<>(ConstantPools.jars(properties)));
        SoftReference<JavaClasses> held = IMPORTED.get(jars);
        JavaClasses classes = held == null ? null : held.get();
        if (classes == null) {
            classes = importOf(jars);
            IMPORTED.put(jars, new SoftReference<>(classes));
        }
        return classes;
    }

    private static JavaClasses importOf(List<Path> jars) {
        List<JarFile> files = new ArrayList<>();
        Map<String, Path> owner = new HashMap<>();
        try {
            for (Path jar : jars) {
                JarFile file = new JarFile(jar.toFile());
                files.add(file);
                for (Enumeration<JarEntry> e = file.entries(); e.hasMoreElements(); ) {
                    String name = e.nextElement().getName();
                    if (!name.endsWith(".class") || name.endsWith("module-info.class")) {
                        continue;
                    }
                    Path was = owner.putIfAbsent(name, jar);
                    if (was != null) {
                        throw new IllegalStateException(name + " is in two of the jars a guard reads, " + was + " and "
                                + jar + ": the import would keep one of them, and which would hang on the jars' order");
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(files);
    }
}
