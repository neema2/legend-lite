// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
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
                PlantedEdgeOnlyApi.Hooked.class, com.legend.edge.PlantedEdgeOnlyCaller.class);
        String api = PlantedEdgeOnlyApi.class.getName();
        Map<String, String> sources = Map.of(
                "core/src/test/java/com/legend/edge/PlantedEdgeOnlyCaller.java",
                "class PlantedEdgeOnlyCaller { String c() { return PlantedEdgeOnlyApi.EDGE_CONSTANT; } }",
                "core/src/test/java/com/legend/PlantedEdgeOnlyApi.java",
                "public final class PlantedEdgeOnlyApi { public static final String EDGE_CONSTANT = \"e\";"
                        + " public static final String SHARED_CONSTANT = \"s\"; String own() { return SHARED_CONSTANT; } }");
        assertEquals(Set.of(api + "#edgeOnly()", api + "#<init>(java.lang.String)", api + "#edgeField",
                        api + "#EDGE_CONSTANT"),
                found(classes, Set.of(PlantedEdgeOnlyApi.Hook.class.getName()), sources),
                "the edge-only static, constructor, field and source-read constant are found; the shared method, the"
                        + " override, the unused member, the shared constant and the hook's member are not");
    }

    /** Each public member of a product class in {@code classes} that the edge alone uses (see the class comment). */
    static Set<String> found(JavaClasses classes, Set<String> hooks, Map<String, String> sources) {
        Set<String> out = new TreeSet<>();
        for (JavaClass c : classes) {
            if (!c.getName().startsWith("com.legend.") || c.getName().startsWith(EDGE) || !reachable(c)
                    || hooks.stream().anyMatch(h -> c.isAssignableTo(h))) {
                continue;
            }
            for (JavaMethod m : c.getMethods()) {
                if (open(m.getModifiers()) && !overrides(c, m)) {
                    List<JavaAccess<?>> uses = new ArrayList<>(m.getCallsOfSelf());
                    uses.addAll(m.getReferencesToSelf());
                    if (edgeAlone(uses)) {
                        out.add(ServiceLoaderSitesTest.key(c, m));
                    }
                }
            }
            for (JavaConstructor k : c.getConstructors()) {
                if (open(k.getModifiers())) {
                    List<JavaAccess<?>> uses = new ArrayList<>(k.getCallsOfSelf());
                    uses.addAll(k.getReferencesToSelf());
                    if (edgeAlone(uses)) {
                        out.add(ServiceLoaderSitesTest.key(c, k));
                    }
                }
            }
            for (JavaField f : c.getFields()) {
                if (!open(f.getModifiers())) {
                    continue;
                }
                // a field's READS (a write is its maker's: a field the product writes and the edge alone reads is
                // carried for the edge)
                List<JavaAccess<?>> uses = new ArrayList<>();
                f.getAccessesToSelf().stream().filter(a -> a.getAccessType() == JavaFieldAccess.AccessType.GET)
                        .forEach(uses::add);
                if (uses.isEmpty() && constant(f)) {
                    if (edgeAloneInSource(c, f.getName(), sources)) {
                        out.add(c.getName() + "#" + f.getName());
                    }
                } else if (edgeAlone(uses)) {
                    out.add(c.getName() + "#" + f.getName());
                }
            }
        }
        return out;
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

    /** Whether the uses are the edge's alone: at least one, and none from the product (the member's own class
     *  included). */
    private static boolean edgeAlone(List<JavaAccess<?>> uses) {
        boolean edge = false;
        for (JavaAccess<?> use : uses) {
            String from = top(use.getOriginOwner().getName());
            if (from.startsWith(EDGE)) {
                edge = true;
            } else {
                return false;
            }
        }
        return edge;
    }

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
