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

    /** "caller#method(params)" -> the class objects its body names beside the loader, and its number of loader reaches
     *  (each site: the load, then its iterator or stream). Measured 2026-11-17. */
    static final Map<String, Set<String>> SITES = Map.ofEntries(
            // the platform's decision probe (a test-only provider; none ships)
            Map.entry("com.legend.builtin.DecisionProbe#installed()", Set.of("com.legend.builtin.DecisionProbe",
                    "(loader reaches 2)")),
            // exec's connectors beside the JDBC drivers (MongoDB's); the database type named for its refusal
            Map.entry("com.legend.exec.Connectors#loaded()", Set.of("com.legend.exec.DatabaseConnector",
                    "com.legend.executionplan.DatabaseType", "(loader reaches 2)")),
            // exec's driver cells (DuckDB's, Postgres')
            Map.entry("com.legend.exec.DriverCells#<clinit>()", Set.of("com.legend.exec.DriverCells",
                    "(loader reaches 2)")),
            // the parser's section grammars
            Map.entry("com.legend.parser.SectionGrammarRegistry#build()", Set.of("com.legend.spi.SectionGrammar",
                    "(loader reaches 2)")),
            // the test plan's extension codecs: the edge's seam (EdgeCodec, in the edge's jar alone)
            Map.entry("com.legend.testexecutionplan.TestPlanJson$Codecs#load()",
                    Set.of("com.legend.testexecutionplan.ExtensionCodec", "(loader reaches 2)")),
            // K2's server half (E-P5's 0c78ae0e00, train 17, 2026-11-18; the user's ruling "Product root, edge opt-in"):
            // the shipped server loads the mount --engine names, a MountProvider (HookRulingTest.RULED); the product
            // ships none, the edge server's jar the one (PROVIDERS)
            Map.entry("com.legend.server.LegendHttpServer#configured(java.lang.String)",
                    Set.of("com.legend.server.MountProvider", "(loader reaches 2)")));

    /** "jar!/META-INF/services/SPI" -> its providers. Measured 2026-11-17. */
    static final Map<String, Set<String>> PROVIDERS = Map.ofEntries(
            Map.entry("libduckdb_cells.jar!/META-INF/services/com.legend.exec.DriverCells",
                    Set.of("com.legend.exec.DuckDbCells")),
            // the edge's one provider: the plan's extension codec (census C2's seam). In the edge's test harness's jar
            // since 2026-11-26, the edition program's step 9, slice 1 (EdgeCodec and its services file moved out of the shipped
            // libedge.jar to the testonly //core:edge_harness; the entry moves with the file)
            Map.entry("libedge_harness.jar!/META-INF/services/com.legend.testexecutionplan.ExtensionCodec",
                    Set.of("com.legend.edge.EdgeCodec")),
            // the edge server's one provider, in the opt-in module's jar: legend-engine's mount (K2's server half,
            // E-P5's 0c78ae0e00, train 17, 2026-11-18)
            Map.entry("libedge_server.jar!/META-INF/services/com.legend.server.MountProvider",
                    Set.of("com.legend.edge.EngineMount")),
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

    /** A code unit's key: its class, its name and its parameters' raw types (an overload or a constructor its own). */
    static String key(JavaClass c, JavaCodeUnit u) {
        return c.getName() + "#" + u.getName() + u.getRawParameterTypes().stream().map(JavaClass::getName)
                .collect(java.util.stream.Collectors.joining(", ", "(", ")"));
    }

    /** Each method of {@code classes} reaching {@code java.util.ServiceLoader} by a call or a method reference, with the
     *  class objects it names and the number of its loader reaches (a second load in a pinned site, of a type held in a
     *  field, changes the number: the review of 9702998c08, 2026-11-17). */
    static Map<String, Set<String>> sites(Iterable<JavaClass> classes) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            for (JavaCodeUnit u : c.getCodeUnits()) {
                long reaches = u.getMethodCallsFromSelf().stream()
                        .filter(call -> call.getTargetOwner().getName().equals("java.util.ServiceLoader")).count()
                        + u.getMethodReferencesFromSelf().stream()
                        .filter(ref -> ref.getTargetOwner().getName().equals("java.util.ServiceLoader")).count();
                if (reaches > 0) {
                    Set<String> objects = new TreeSet<>();
                    u.getReferencedClassObjects().forEach(o -> objects.add(o.getRawType().getName()));
                    objects.add("(loader reaches " + reaches + ")");
                    out.put(key(c, u), objects);
                }
            }
        }
        return out;
    }

    /** Each class of {@code pools} that reaches the loader or a proxy past any site this test reads: the loader's name
     *  as a string (a reflective lookup), or java.lang.reflect.Proxy named at all (a style or a provider with no class
     *  file) -- the review of 7c37c88d7d and 9702998c08, 2026-11-17. */
    static Set<String> reflective(Map<String, ConstantPools.Pool> pools) {
        Set<String> out = new TreeSet<>();
        pools.forEach((name, pool) -> {
            boolean loaderByName = pool.strings().stream().anyMatch(t -> t.contains("java.util.ServiceLoader")
                    || t.contains("java/util/ServiceLoader"));
            boolean proxy = pool.utf8().stream().anyMatch(t -> t.contains("java/lang/reflect/Proxy")
                    || t.contains("java.lang.reflect.Proxy"));
            if (loaderByName) {
                out.add(name + " (the loader by name)");
            }
            if (proxy) {
                out.add(name + " (a proxy)");
            }
        });
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

    /** No class of the product's, the hosts' or the edge's jars reaches the loader by name or makes a proxy: a style or
     *  a provider with no class file would pass every pin above. Pinned (empty today) with any reason. */
    static final Set<String> REFLECTIVE = Set.of();

    @Test
    void noLoaderByNameAndNoProxy() {
        assertEquals(REFLECTIVE, reflective(ConstantPools.poolsOfJars(ConstantPools.jars("legend.product.jars",
                "legend.authored.jars", "legend.edge.jars"))), "a class reaches the ServiceLoader by name or makes a"
                + " java.lang.reflect.Proxy: pin it with its reason");
    }

    /** Planted: loader sites found with their type and their reach count -- a call, a method reference, two overloads
     *  each its own key; a service file read with its providers, comments aside; the loader by name and a proxy found. */
    @Test
    void theGuardBites() {
        Map<String, Set<String>> s = sites(new ClassFileImporter().importClasses(PlantedLoader.class));
        String p = PlantedLoader.class.getName();
        assertEquals(Map.of(p + "#load()", Set.of(Runnable.class.getName(), "(loader reaches 2)"),
                p + "#load(int)", Set.of(Comparable.class.getName(), "(loader reaches 1)"),
                p + "#byReference()", Set.of(AutoCloseable.class.getName(), "(loader reaches 1)")), s);
        Map<String, ConstantPools.Pool> pools = Map.of(p, ConstantPools.pool(ConstantPools.bytesOf(PlantedLoader.class)));
        assertEquals(Set.of(p + " (the loader by name)", p + " (a proxy)"), reflective(pools));
        assertEquals(Map.of("planted.jar!/META-INF/services/java.lang.Runnable", Set.of("com.legend.edge.Planted")),
                providers(Map.of("planted.jar!/META-INF/services/java.lang.Runnable",
                        "# a comment\ncom.legend.edge.Planted   # trailing\n\n", "planted.jar!/other.txt", "x")));
    }

    /** Planted seams: two overloads, a method reference, the loader by name, a proxy. */
    @SuppressWarnings({"unused", "rawtypes"})
    static final class PlantedLoader {
        static List<Runnable> load() {
            List<Runnable> out = new ArrayList<>();
            java.util.ServiceLoader.load(Runnable.class).forEach(out::add);
            return out;
        }

        static Object load(int n) {
            return java.util.ServiceLoader.load(Comparable.class);
        }

        static Object byReference() {
            java.util.function.Function<Class<AutoCloseable>, java.util.ServiceLoader<AutoCloseable>> f =
                    java.util.ServiceLoader::load;
            return f.apply(AutoCloseable.class);
        }

        static Object byName() throws ReflectiveOperationException {
            return Class.forName("java.util.ServiceLoader");
        }

        static Object proxy() {
            return java.lang.reflect.Proxy.newProxyInstance(PlantedLoader.class.getClassLoader(),
                    new Class<?>[] {Runnable.class}, (o, m, a) -> null);
        }
    }
}
