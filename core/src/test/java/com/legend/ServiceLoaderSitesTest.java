// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE SERVICE-LOADER SEAMS, PINNED BY THEIR TYPE (the GUARD stream, step 2's C.10; the edge audit of 2026-11-17, section
 * C.10: "ServiceLoader call sites pinned by SPI type"). A {@code ServiceLoader} joins whatever a jar's
 * {@code META-INF/services} file names: a dependency no import shows, so the edge could reach the product's runtime
 * through one with no class naming it. Two exact pins over the product's, the hosts' and the edge's jars:
 * <ul>
 *   <li>{@link #SITES}: each method that asks {@code java.util.ServiceLoader} for providers, with the class objects it
 *   hands it (the SPI type) -- a new seam, or a seam loading another type, fails;</li>
 *   <li>{@link #PROVIDERS}: each jar's service files, "jar!/SPI" -> its providers -- a provider added (an edge class in a
 *   product jar's file above all) fails.</li>
 * </ul>
 */
@Tag("guardrail")
class ServiceLoaderSitesTest {

    /** "caller#method" -> the class objects its body names beside the loader. Measured 2026-11-17. */
    static final Map<String, Set<String>> SITES = Map.ofEntries(
            // the platform's decision probe (a test-only provider; none ships)
            Map.entry("com.legend.builtin.DecisionProbe#installed", Set.of("com.legend.builtin.DecisionProbe")),
            // exec's connectors beside the JDBC drivers (MongoDB's); the database type named for its refusal
            Map.entry("com.legend.exec.Connectors#loaded", Set.of("com.legend.exec.DatabaseConnector",
                    "com.legend.executionplan.DatabaseType")),
            // exec's driver cells (DuckDB's, Postgres')
            Map.entry("com.legend.exec.DriverCells#<clinit>", Set.of("com.legend.exec.DriverCells")),
            // the parser's section grammars
            Map.entry("com.legend.parser.SectionGrammarRegistry#build", Set.of("com.legend.spi.SectionGrammar")),
            // the test plan's extension codecs: the edge's seam (EdgeCodec, in the edge's jar alone)
            Map.entry("com.legend.testexecutionplan.TestPlanJson$Codecs#load",
                    Set.of("com.legend.testexecutionplan.ExtensionCodec")));

    /** "jar!/META-INF/services/SPI" -> its providers. Measured 2026-11-17. */
    static final Map<String, Set<String>> PROVIDERS = Map.ofEntries(
            Map.entry("libduckdb_cells.jar!/META-INF/services/com.legend.exec.DriverCells",
                    Set.of("com.legend.exec.DuckDbCells")),
            // the edge's one provider, in the edge's jar: the plan's extension codec (census C2's seam)
            Map.entry("libedge.jar!/META-INF/services/com.legend.testexecutionplan.ExtensionCodec",
                    Set.of("com.legend.edge.EdgeCodec")),
            Map.entry("libmongodb_connector.jar!/META-INF/services/com.legend.exec.DatabaseConnector",
                    Set.of("com.legend.exec.MongoDbConnector")),
            Map.entry("libpostgres_cells.jar!/META-INF/services/com.legend.exec.DriverCells",
                    Set.of("com.legend.exec.PostgresCells")),
            // the tab's TeaVM resource supplier (the prelude for the browser)
            Map.entry("libtab.jar!/META-INF/services/org.teavm.classlib.ResourceSupplier",
                    Set.of("planner.PreludeResources")));

    private static Iterable<JavaClass> importJars(String... properties) {
        List<java.util.jar.JarFile> jars = new ArrayList<>();
        try {
            for (String p : properties) {
                for (java.nio.file.Path jar : com.legend.testing.Runfile.listed(p)) {
                    jars.add(new java.util.jar.JarFile(jar.toFile()));
                }
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(jars);
    }

    /** Each method of {@code classes} calling {@code java.util.ServiceLoader}, with the class objects it names. */
    static Map<String, Set<String>> sites(Iterable<JavaClass> classes) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            for (JavaCodeUnit u : c.getCodeUnits()) {
                boolean loads = u.getMethodCallsFromSelf().stream()
                        .anyMatch(call -> call.getTargetOwner().getName().equals("java.util.ServiceLoader"));
                if (loads) {
                    Set<String> objects = new TreeSet<>();
                    u.getReferencedClassObjects().forEach(o -> objects.add(o.getRawType().getName()));
                    out.put(c.getName() + "#" + u.getName(), objects);
                }
            }
        }
        return out;
    }

    /** Each service file of {@code resources} ("jar!/path" -> text): its providers, comments and blanks aside. */
    static Map<String, Set<String>> providers(Map<String, String> resources) {
        Map<String, Set<String>> out = new TreeMap<>();
        resources.forEach((key, text) -> {
            if (key.contains("!/META-INF/services/")) {
                Set<String> names = new TreeSet<>();
                text.lines().map(l -> l.replaceAll("#.*", "").strip()).filter(l -> !l.isEmpty()).forEach(names::add);
                out.put(key, names);
            }
        });
        return out;
    }

    @Test
    void theLoaderSitesArePinnedByTheirType() {
        Map<String, Set<String>> found = sites(importJars("legend.product.jars", "legend.authored.jars",
                "legend.edge.jars"));
        assertEquals(new TreeMap<>(SITES), found, "a ServiceLoader seam added, or loading another type: pin it with"
                + " its reason (a seam the edge can fill joins the product's runtime with no import)");
    }

    @Test
    void theProvidersArePinned() {
        List<java.nio.file.Path> jars = new ArrayList<>(ConstantPools.jars("legend.product.jars", "legend.authored.jars",
                "legend.edge.jars"));
        Map<String, Set<String>> found = providers(ConstantPools.resourcesOfJars(jars));
        assertEquals(new TreeMap<>(PROVIDERS), found, "a service file or provider added: pin it with its reason");
    }

    /** Planted: a loader site found with its type; a service file read with its providers, comments aside. */
    @Test
    void theGuardBites() {
        Map<String, Set<String>> s = sites(new ClassFileImporter().importClasses(PlantedLoader.class));
        assertEquals(Map.of(PlantedLoader.class.getName() + "#load", Set.of(Runnable.class.getName())), s);
        assertEquals(Map.of("planted.jar!/META-INF/services/java.lang.Runnable", Set.of("com.legend.edge.Planted")),
                providers(Map.of("planted.jar!/META-INF/services/java.lang.Runnable",
                        "# a comment\ncom.legend.edge.Planted   # trailing\n\n", "planted.jar!/other.txt", "x")));
    }

    /** A planted seam. */
    static final class PlantedLoader {
        static List<Runnable> load() {
            List<Runnable> out = new ArrayList<>();
            java.util.ServiceLoader.load(Runnable.class).forEach(out::add);
            return out;
        }
    }
}
