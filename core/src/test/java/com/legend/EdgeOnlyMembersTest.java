// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaConstructor;
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaFieldAccess;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A PRODUCT MEMBER ONLY THE EDGE USES IS COUNTED (J40 of the edge audit, the orchestrator's order of 2026-11-18 on the
 * confirmation audit's suspected list: "a product public member whose only callers are com.legend.edge is counted,
 * unless it is a pinned hook"). A public member of a product class -- a method, a constructor, a field -- that the
 * edge uses and nothing else in the product's, the hosts' and the edge's jars uses is the edge's work standing in the
 * product. Each one is an open census line ({@link #OPEN}, by its audit id) until it moves to the edge, or until it is
 * ruled a product API the edge reads ({@link #READ_API}, with the reason, not counted). The two together are exact, so
 * a new one is seen.
 *
 * <p>Not counted:
 * <ul>
 *   <li>a member of a hook type, or of a class implementing one ({@code HookLambdasTest.hookTypes()}: the hook is the
 *       edge's extension point, HookRulingTest's);</li>
 *   <li>a method overriding a supertype's: it is reached by dispatch, never by its own name;</li>
 *   <li>a synthetic or bridge member;</li>
 *   <li>a member nothing uses (dead code is another question).</li>
 * </ul>
 * A use from the member's own class, or from a host's door jar outside {@code com.legend.edge}, is a product use. A
 * compile-time constant is inlined where it is read, so its bytecode shows no reader: a static final String or
 * primitive with none is read from the sources the guard rails declare ({@code Owner.NAME}, or a static import of it).
 * Named limits: a constant read only through a wildcard static import is not seen, and a {@code Name.CONSTANT} in a
 * product file reads as a product use whichever {@code Name} it means (both fail toward counting less).
 */
@Tag("guardrail")
class EdgeOnlyMembersTest {

    /** The members only the edge uses, each an open census line: "owner#member(params)" -> "J40 date (owner: why)".
     *  Measured 2026-11-18. */
    static final Map<String, String> OPEN = Map.ofEntries();

    /** The members only the edge uses that are ruled a product API the edge reads, each with its reason (not counted). */
    static final Map<String, String> READ_API = Map.ofEntries();

    private static final String EDGE = "com.legend.edge.";

    @Test
    void theEdgeOnlyMembersAreCountedOrRuled() throws IOException {
        JavaClasses classes = new ClassFileImporter().importJars(jarFiles(ConstantPools.jars("legend.product.jars",
                "legend.authored.jars", "legend.edge.jars")));
        Set<String> found = found(classes, HookLambdasTest.hookTypes(), sources());
        Set<String> pinned = new TreeSet<>(OPEN.keySet());
        pinned.addAll(READ_API.keySet());
        assertEquals(pinned, found, "a public product member only the edge uses: move it to the edge, or pin it in OPEN"
                + " (counted, with its audit id) or READ_API (ruled, with its reason); one pinned that the product uses"
                + " again, or that went, is deleted");
        assertEquals(Set.of(), intersection(OPEN.keySet(), READ_API.keySet()), "a member counted and ruled at once");
    }

    /** Planted: a product class whose members the edge alone uses -- a static, a constructor, a field, a constant read
     *  in source -- is found; a member the product uses too, an override, a member nothing uses, and a hook's member
     *  are not. */
    @Test
    void theGuardBites() {
        JavaClasses classes = new ClassFileImporter().importClasses(PlantedEdgeOnlyApi.class,
                PlantedEdgeOnlyApi.Hooked.class, PlantedEdgeOnlyApi.Kind.class, com.legend.edge.PlantedEdgeOnlyCaller.class);
        String api = PlantedEdgeOnlyApi.class.getName();
        Map<String, String> sources = Map.of(
                "core/src/test/java/com/legend/edge/PlantedEdgeOnlyCaller.java",
                "class PlantedEdgeOnlyCaller { String c() { return PlantedEdgeOnlyApi.EDGE_CONSTANT; } }",
                "core/src/test/java/com/legend/PlantedEdgeOnlyApi.java",
                "public final class PlantedEdgeOnlyApi { public static final String EDGE_CONSTANT = \"e\";"
                        + " public static final String SHARED_CONSTANT = \"s\"; String own() { return SHARED_CONSTANT; } }");
        assertEquals(Set.of(api + "#edgeOnly()", api + "#viaEdgeOnly()", api + "#viaLambda()", api + "#countdown(int)",
                        api + "#viaDead()", api + "#<init>(java.lang.String)", api + "#edgeField", api + "#EDGE_CONSTANT"),
                found(classes, Set.of(PlantedEdgeOnlyApi.Hook.class.getName()), sources),
                "the edge-only static, constructor, field and source-read constant are found, and the members reached"
                        + " only through an edge-only member or its lambda, or by themselves or dead non-public code besides; the shared"
                        + " method, the"
                        + " override, the unused member, the cycle no edge use enters, the enum's values(), a member a main or a public method also uses, the shared"
                        + " constant and the hook's member are not");
    }

    /** Each public member of a product class in {@code classes} that the edge alone uses, TRANSITIVELY (the
     *  orchestrator's ruling of 2026-11-18, (4): a use from a member that is itself edge-only is the edge's -- VerdictQueries'
     *  fromWrapped, reached only through text(), which only the edge calls). Computed over every code unit and field of
     *  the product classes, private helpers and lambda bodies included, as a greatest fixpoint: start from every unit with
     *  a use, drop each one a product use reaches (an origin neither the edge nor a unit still held, a use of a unit by
     *  itself aside, and a use from a non-public method nothing uses -- dead code's, or a test's -- aside), until none
     *  drops; then keep the units an edge use reaches through held units (a cycle no edge use
     *  enters is no edge's). Reported: the public members of public classes, hooks, overrides, synthetic members and an
     *  enum's compiler-written values() aside (the ruling's (3)). A lambda body's use is its enclosing method's (javac's
     *  lambda$name$n: every method of that name in the class); an anonymous class's methods are reached by dispatch and
     *  are never held, so their uses are the product's (counting less). Named limits: a non-public method reached by
     *  reflection alone reads as dead, so its uses read as nobody's (counting more); a PUBLIC method nothing in the jars
     *  uses reads as an entry point, so its uses are the product's even when it is dead (counting less: fromWrapped's
     *  two-argument overload, which nothing calls, keeps the three-argument one from being counted until it goes). */
    static Set<String> found(JavaClasses classes, Set<String> hooks, Map<String, String> sources) {
        Map<String, List<JavaAccess<?>>> uses = new TreeMap<>();
        Map<String, List<String>> byName = new TreeMap<>();
        for (JavaClass c : classes) {
            if (!c.getName().startsWith("com.legend.") || c.getName().startsWith(EDGE)) {
                continue;
            }
            for (JavaCodeUnit u : c.getCodeUnits()) {
                byName.computeIfAbsent(c.getName() + "#" + u.getName(), k -> new ArrayList<>())
                        .add(ServiceLoaderSitesTest.key(c, u));
                List<JavaAccess<?>> us = new ArrayList<>();
                if (u instanceof JavaMethod m) {
                    if (overrides(c, m) || hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                        continue;
                    }
                    us.addAll(m.getCallsOfSelf());
                    us.addAll(m.getReferencesToSelf());
                } else if (u instanceof JavaConstructor k) {
                    if (hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                        continue;
                    }
                    us.addAll(k.getCallsOfSelf());
                    us.addAll(k.getReferencesToSelf());
                }
                if (!us.isEmpty()) {
                    uses.put(ServiceLoaderSitesTest.key(c, u), us);
                }
            }
            for (JavaField f : c.getFields()) {
                // a field's READS (a write is its maker's: a field the product writes and the edge alone reads is
                // carried for the edge)
                List<JavaAccess<?>> us = new ArrayList<>();
                f.getAccessesToSelf().stream().filter(x -> x.getAccessType() == JavaFieldAccess.AccessType.GET)
                        .forEach(us::add);
                if (!us.isEmpty() && !hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                    uses.put(c.getName() + "#" + f.getName(), us);
                }
            }
        }
        // a plain non-public method nothing in the jars uses: dead, or a test's, so its uses are nobody's. A PUBLIC one is
        // an entry point -- a door a test, a host or a framework calls (measured 2026-11-18: read as dead, they added 240
        // members) -- and so are a constructor, a static initializer, a main, an annotated method (an export a native or
        // JavaScript host calls), an override and a hook's member
        Set<String> quiet = new TreeSet<>();
        for (JavaClass c : classes) {
            if (!c.getName().startsWith("com.legend.") || c.getName().startsWith(EDGE)
                    || hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                continue;
            }
            for (JavaMethod m : c.getMethods()) {
                String key = ServiceLoaderSitesTest.key(c, m);
                if (!uses.containsKey(key) && !m.getModifiers().contains(JavaModifier.PUBLIC) && !overrides(c, m)
                        && m.getAnnotations().isEmpty()
                        && !m.getModifiers().contains(JavaModifier.SYNTHETIC) && !m.getName().startsWith("lambda$")
                        && !(m.getName().equals("main") && m.getRawParameterTypes().size() == 1)) {
                    quiet.add(key);
                }
            }
        }
        // the greatest fixpoint: a held unit's every use is the edge's, its own, a held unit's or a quiet one's
        Set<String> held = new TreeSet<>(uses.keySet());
        boolean dropped = true;
        while (dropped) {
            dropped = false;
            for (String key : new ArrayList<>(held)) {
                for (JavaAccess<?> use : uses.get(key)) {
                    if (!edgeOrHeld(use, key, held, quiet, byName)) {
                        held.remove(key);
                        dropped = true;
                        break;
                    }
                }
            }
        }
        // ... entered by an edge use: a cycle of product units no edge use reaches is nobody's
        Set<String> reached = new TreeSet<>();
        boolean grew = true;
        while (grew) {
            grew = false;
            for (String key : held) {
                if (!reached.contains(key) && uses.get(key).stream().anyMatch(use -> top(use.getOriginOwner().getName())
                        .startsWith(EDGE) || origins(use, byName).stream().anyMatch(reached::contains))) {
                    reached.add(key);
                    grew = true;
                }
            }
        }
        Set<String> out = new TreeSet<>();
        for (JavaClass c : classes) {
            if (!c.getName().startsWith("com.legend.") || c.getName().startsWith(EDGE) || !reachable(c)
                    || hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                continue;
            }
            for (JavaMethod m : c.getMethods()) {
                boolean values = c.isEnum() && m.getName().equals("values") && m.getRawParameterTypes().isEmpty();
                if (open(m.getModifiers()) && !values && reached.contains(ServiceLoaderSitesTest.key(c, m))) {
                    out.add(ServiceLoaderSitesTest.key(c, m));
                }
            }
            for (JavaConstructor k : c.getConstructors()) {
                if (open(k.getModifiers()) && reached.contains(ServiceLoaderSitesTest.key(c, k))) {
                    out.add(ServiceLoaderSitesTest.key(c, k));
                }
            }
            for (JavaField f : c.getFields()) {
                if (!open(f.getModifiers())) {
                    continue;
                }
                String key = c.getName() + "#" + f.getName();
                if (!uses.containsKey(key) && constant(f)) {
                    // a compile-time constant is inlined where it is read: read from the sources (not transitive)
                    if (edgeAloneInSource(c, f.getName(), sources)) {
                        out.add(key);
                    }
                } else if (reached.contains(key)) {
                    out.add(key);
                }
            }
        }
        return out;
    }

    /** Whether {@code use} of the unit {@code key} comes from the edge, from the unit itself, or from units all held or
     *  quiet (nothing uses them). */
    private static boolean edgeOrHeld(JavaAccess<?> use, String key, Set<String> held, Set<String> quiet,
            Map<String, List<String>> byName) {
        if (top(use.getOriginOwner().getName()).startsWith(EDGE)) {
            return true;
        }
        List<String> from = origins(use, byName);
        return !from.isEmpty() && from.stream().allMatch(o -> o.equals(key) || held.contains(o) || quiet.contains(o));
    }

    /** The units a use comes from: its origin, or -- for a lambda body ({@code lambda$name$n}) -- every unit of that name
     *  in the origin's class ({@code new} a constructor, {@code static} the class's initializer). */
    private static List<String> origins(JavaAccess<?> use, Map<String, List<String>> byName) {
        String owner = use.getOriginOwner().getName();
        String name = use.getOrigin().getName();
        java.util.regex.Matcher lambda = Pattern.compile("lambda\\$(.+)\\$\\d+").matcher(name);
        if (lambda.matches()) {
            String enclosing = switch (lambda.group(1)) {
                case "new" -> "<init>";
                case "static" -> "<clinit>";
                default -> lambda.group(1);
            };
            return byName.getOrDefault(owner + "#" + enclosing, List.of());
        }
        return List.of(ServiceLoaderSitesTest.key(use.getOriginOwner(), use.getOrigin()));
    }

    /** Whether {@code c} and every class enclosing it are public: the edge, another package, can reach its members. */
    private static boolean reachable(JavaClass c) {
        for (JavaClass at = c; at != null; at = at.getEnclosingClass().orElse(null)) {
            if (!at.getModifiers().contains(JavaModifier.PUBLIC)) {
                return false;
            }
        }
        return true;
    }

    private static boolean open(Set<JavaModifier> modifiers) {
        return modifiers.contains(JavaModifier.PUBLIC) && !modifiers.contains(JavaModifier.SYNTHETIC)
                && !modifiers.contains(JavaModifier.BRIDGE);
    }

    /** Whether {@code m} overrides or implements a supertype's method (same name, same raw parameters). */
    private static boolean overrides(JavaClass c, JavaMethod m) {
        if (m.getModifiers().contains(JavaModifier.STATIC)) {
            return false;
        }
        List<JavaClass> supers = new ArrayList<>(c.getAllRawSuperclasses());
        supers.addAll(c.getAllRawInterfaces());
        for (JavaClass s : supers) {
            for (JavaMethod sm : s.getMethods()) {
                if (sm.getName().equals(m.getName()) && !sm.getModifiers().contains(JavaModifier.PRIVATE)
                        && !sm.getModifiers().contains(JavaModifier.STATIC)
                        && sm.getRawParameterTypes().stream().map(JavaClass::getName).toList()
                        .equals(m.getRawParameterTypes().stream().map(JavaClass::getName).toList())) {
                    return true;
                }
            }
        }
        // a class outside the import (java.lang.Object's toString, a JDK interface's method) answers too
        return OBJECT_METHODS.contains(m.getName() + m.getRawParameterTypes().stream().map(JavaClass::getName).toList());
    }

    private static final Set<String> OBJECT_METHODS = Set.of("toString[]", "hashCode[]", "equals[java.lang.Object]");

    /** Whether {@code f} may be a compile-time constant: static final, a primitive or a String. */
    private static boolean constant(JavaField f) {
        return f.getModifiers().contains(JavaModifier.STATIC) && f.getModifiers().contains(JavaModifier.FINAL)
                && (f.getRawType().isPrimitive() || f.getRawType().getName().equals("java.lang.String"));
    }

    /** Whether the sources read {@code owner}'s constant {@code name} in the edge alone: {@code Owner.NAME} or a static
     *  import of it outside the owner's file, the bare name inside it beyond its declaration. */
    private static boolean edgeAloneInSource(JavaClass owner, String name, Map<String, String> sources) {
        String ownPath = "/" + top(owner.getName()).replace('.', '/') + ".java";
        Pattern qualified = Pattern.compile("\\b" + Pattern.quote(owner.getSimpleName()) + "\\s*\\.\\s*"
                + Pattern.quote(name) + "\\b");
        Pattern imported = Pattern.compile("import\\s+static\\s+[\\w.$]*\\." + Pattern.quote(owner.getSimpleName())
                + "\\s*\\.\\s*" + Pattern.quote(name) + "\\s*;");
        Pattern bare = Pattern.compile("\\b" + Pattern.quote(name) + "\\b");
        boolean edge = false;
        for (Map.Entry<String, String> e : sources.entrySet()) {
            String text = e.getValue();
            boolean read;
            if (e.getKey().endsWith(ownPath)) {
                read = bare.matcher(text).results().count() > 1;
            } else {
                read = qualified.matcher(text).find() || imported.matcher(text).find();
            }
            if (!read) {
                continue;
            }
            if (e.getKey().contains("/com/legend/edge/")) {
                edge = true;
            } else {
                return false;
            }
        }
        return edge;
    }

    /** The guard rails' declared sources under core's main tree, comments and strings left in (a name in a comment
     *  reads as a use: it fails toward counting nothing). */
    private static Map<String, String> sources() throws IOException {
        Map<String, String> out = new TreeMap<>();
        for (Path p : SourceFiles.under("core/src/main/java/com/legend")) {
            if (p.toString().endsWith(".java")) {
                out.put(SourceFiles.path(p), Files.readString(p));
            }
        }
        return out;
    }

    private static String top(String n) {
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    private static Set<String> intersection(Set<String> a, Set<String> b) {
        Set<String> out = new TreeSet<>(a);
        out.retainAll(b);
        return out;
    }

    private static List<java.util.jar.JarFile> jarFiles(List<Path> jars) throws IOException {
        List<java.util.jar.JarFile> out = new ArrayList<>();
        for (Path jar : jars) {
            out.add(new java.util.jar.JarFile(jar.toFile()));
        }
        return out;
    }
}
