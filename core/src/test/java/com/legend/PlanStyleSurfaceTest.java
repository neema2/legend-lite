// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.PlanStyle;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PLAN STYLE'S SURFACE, PINNED (the GUARD stream, step 2's C.9; the edge audit of 2026-11-17, section C.9: "KINDS,
 * TWO_FORMS and PlanStyle's methods pinned exactly; criterion (a) applied to TWO_FORMS; the overrider set pinned"). The
 * plan style is how the edge hands upstream's forms in: a new facet kind, a new two-form kind, a new method on the style
 * or a new class overriding one is a new door. Each is pinned here exactly:
 * <ul>
 *   <li>{@code PlanStyleFacets.KINDS}, the closed list of facet kinds, in order;</li>
 *   <li>criterion (a) on every {@code PlanStyleFacets.TWO_FORMS} kind (HookRulingTest pins the list itself): an interface
 *   of bare signatures -- no default or static method, no field, no nested type -- but for the pinned exceptions
 *   ({@link #TWO_FORM_EXCEPTIONS}), each found by this guard and reported with its reason;</li>
 *   <li>{@link PlanStyle}'s declared methods, each by its modifiers, its default-ness and its signature;</li>
 *   <li>every class of the product's and the edge's jars implementing {@link PlanStyle}, with the style methods it
 *   declares itself (its overrides).</li>
 * </ul>
 */
@Tag("guardrail")
class PlanStyleSurfaceTest {

    /** The facet kinds, in order. */
    static final List<String> KINDS = List.of("com.legend.testplan.EngineTestText", "com.legend.planner.UpstreamPlans",
            "com.legend.lowering.RelationTexts", "com.legend.lowering.PureOnlyTexts", "com.legend.lowering.ResultEnvelopes");

    /** Members of a TWO_FORMS kind that break criterion (a), each with its reason: found 2026-11-17. */
    static final Map<String, String> TWO_FORM_EXCEPTIONS = Map.ofEntries(
            // the product's own form held in the kind type itself: found by this guard 2026-11-17, reported to the
            // orchestrator (move the product's form out of the kind, or rule it), pinned so the kind grows no further
            Map.entry("TestDataPlanner$Printing#CLEAN (field)", "the product's clean timestamp form (L9, census C1"
                    + " 2026-11-17), found 2026-11-17"),
            Map.entry("CrossKindNumbers#Product (nested type)", "the product's NEVER form (census C1 2c, S127,"
                    + " 2026-11-17), found 2026-11-17"));

    /** PlanStyle's declared methods. */
    static final Set<String> STYLE_METHODS = Set.of(
            "private static java.lang.String dateTimeText(java.lang.String)",
            "public abstract F facet(java.lang.Class<F>)",
            "public abstract F facet(java.lang.Class<F>, java.util.function.Supplier<? extends java.lang.RuntimeException>)",
            "public abstract F twoForm(java.lang.Class<F>, F)",
            "public abstract boolean judgesEngineText()",
            "public abstract com.legend.database.PlanStyle$TraceCell traceCell()",
            "public abstract com.legend.sql.dialect.SqlDialect dialect(com.legend.model.ConnectionDefinition$DatabaseType)",
            "public abstract java.lang.String activitySql(com.legend.database.PlanStyle$Activity)",
            "public abstract java.lang.String errorWords(java.lang.RuntimeException)",
            "public abstract java.lang.String graphKeyDate(java.lang.String)",
            "public abstract java.lang.String graphKeyString(java.lang.String)",
            "public abstract java.lang.String siteWords(com.legend.sql.dialect.RaiseSite)",
            "public abstract java.util.Set<com.legend.sql.RaiseKind> plainRaises()");

    /** Every style method by name: each style implements them all today. */
    private static final Set<String> ALL = Set.of("activitySql", "dialect", "errorWords", "facet", "graphKeyDate",
            "graphKeyString", "judgesEngineText", "plainRaises", "siteWords", "traceCell", "twoForm");

    /** Each implementer of PlanStyle -> the style methods it declares. */
    static final Map<String, Set<String>> OVERRIDERS = Map.ofEntries(
            // the product's style (PlanStyle.PRODUCT) and the edge's two (PureText.UPSTREAM, PureText.PURE_V1)
            Map.entry("com.legend.database.PlanStyle$1", ALL),
            Map.entry("com.legend.edge.PureText$1", ALL),
            Map.entry("com.legend.edge.PureText$2", ALL));

    private static String signature(Method m) {
        return m.getName() + Arrays.stream(m.getGenericParameterTypes()).map(java.lang.reflect.Type::getTypeName)
                .collect(Collectors.joining(", ", "(", ")"));
    }

    private static String described(Method m) {
        String mods = Modifier.toString(m.getModifiers() & Modifier.methodModifiers());
        return (m.isDefault() ? "default " : "") + (mods.isEmpty() ? "" : mods + " ")
                + m.getGenericReturnType().getTypeName() + " " + signature(m);
    }

    /** What breaks criterion (a) in {@code kind}: "Kind#member (what)". */
    static List<String> breaksOfA(Class<?> kind) {
        List<String> out = new ArrayList<>();
        String k = kind.getName().substring(kind.getName().lastIndexOf('.') + 1);
        if (!kind.isInterface()) {
            out.add(k + " (no interface)");
        }
        for (Method m : kind.getDeclaredMethods()) {
            if (m.isSynthetic()) {
                continue;
            }
            if (Modifier.isStatic(m.getModifiers())) {
                out.add(k + "#" + signature(m) + " (static method)");
            } else if (m.isDefault() || !Modifier.isAbstract(m.getModifiers())) {
                out.add(k + "#" + signature(m) + " (method with a body)");
            }
        }
        for (Field f : kind.getDeclaredFields()) {
            if (!f.isSynthetic()) {
                out.add(k + "#" + f.getName() + " (field)");
            }
        }
        for (Class<?> n : kind.getDeclaredClasses()) {
            out.add(k + "#" + n.getSimpleName() + " (nested type)");
        }
        return out;
    }

    private static JavaClasses classes(String... properties) {
        List<java.util.jar.JarFile> jars = new ArrayList<>();
        try {
            for (String property : properties) {
                for (java.nio.file.Path jar : com.legend.testing.Runfile.listed(property)) {
                    jars.add(new java.util.jar.JarFile(jar.toFile()));
                }
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(jars);
    }

    /** Each concrete class of {@code classes} assignable to PlanStyle (PlanStyle aside) -> the style's method names it
     *  declares itself. */
    static Map<String, Set<String>> overriders(Iterable<JavaClass> classes) {
        Set<String> styleMethods = Arrays.stream(PlanStyle.class.getMethods()).map(Method::getName)
                .collect(Collectors.toSet());
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            if (c.getName().equals(PlanStyle.class.getName()) || !c.isAssignableTo(PlanStyle.class.getName())) {
                continue;
            }
            Set<String> own = new TreeSet<>();
            c.getMethods().stream().filter(m -> m.getOwner().equals(c) && styleMethods.contains(m.getName())
                    && !m.getModifiers().contains(com.tngtech.archunit.core.domain.JavaModifier.SYNTHETIC)
                    && !m.getModifiers().contains(com.tngtech.archunit.core.domain.JavaModifier.BRIDGE))
                    .forEach(m -> own.add(m.getName()));
            out.put(c.getName(), own);
        }
        return out;
    }

    @Test
    void theFacetKindsArePinnedInOrder() {
        List<String> kinds = com.legend.testplan.PlanStyleFacets.KINDS.stream().map(Class::getName).toList();
        assertEquals(KINDS, kinds, "PlanStyleFacets.KINDS: a kind added or moved is listed here with its ruling");
    }

    @Test
    void eachTwoFormKindMeetsCriterionA() {
        List<String> breaks = new ArrayList<>();
        com.legend.testplan.PlanStyleFacets.TWO_FORMS.forEach(k -> breaks.addAll(breaksOfA(k)));
        assertEquals(new TreeSet<>(TWO_FORM_EXCEPTIONS.keySet()), new TreeSet<>(breaks), "a TWO_FORMS kind breaks"
                + " criterion (a) (signatures only) beyond its pinned exceptions: move the product's form out of the kind,"
                + " or rule it here");
    }

    @Test
    void thePlanStylesMethodsArePinned() {
        Set<String> declared = new TreeSet<>();
        for (Method m : PlanStyle.class.getDeclaredMethods()) {
            if (!m.isSynthetic()) {
                declared.add(described(m));
            }
        }
        assertEquals(new TreeSet<>(STYLE_METHODS), declared, "PlanStyle's methods: one added is a new door for the"
                + " edge's forms -- list it here with its ruling");
    }

    @Test
    void theStylesOverridersArePinned() {
        Map<String, Set<String>> found = overriders(classes("legend.product.jars", "legend.edge.jars",
                "legend.authored.jars"));
        assertEquals(new TreeMap<>(OVERRIDERS), found, "a class implementing PlanStyle, or a style method it declares,"
                + " differs from the pinned set");
    }

    /** Planted: a kind holding a static, a default, a field and a nested type breaks (a) four ways; a planted style that
     *  overrides one method is an overrider of that method alone. */
    @Test
    void theGuardBites() {
        String k = "PlanStyleSurfaceTest$PlantedKind#";
        assertEquals(List.of(k + "twice(int) (method with a body)", k + "of() (static method)", k + "ZERO (field)",
                k + "Product (nested type)"), sorted(breaksOfA(PlantedKind.class), List.of("twice", "of", "ZERO",
                "Product")));
        Map<String, Set<String>> found = overriders(new ClassFileImporter().importClasses(PlantedStyle.class));
        assertEquals(1, found.size());
        assertEquals(Set.of("judgesEngineText"), found.values().iterator().next(), "its one override is read");
    }

    private static List<String> sorted(List<String> breaks, List<String> order) {
        List<String> out = new ArrayList<>();
        for (String o : order) {
            breaks.stream().filter(b -> b.startsWith("PlanStyleSurfaceTest$PlantedKind#" + o)).forEach(out::add);
        }
        return out;
    }

    /** A planted two-form kind breaking (a) four ways. */
    interface PlantedKind {
        int ZERO = 0;

        int one(int x);

        default int twice(int x) {
            return one(x) * 2;
        }

        static PlantedKind of() {
            return x -> x;
        }

        enum Product implements PlantedKind {
            ONLY;

            @Override
            public int one(int x) {
                return x;
            }
        }
    }

    /** A planted style: one method overridden. */
    abstract static class PlantedStyle implements PlanStyle {
        @Override
        public boolean judgesEngineText() {
            return true;
        }
    }
}
