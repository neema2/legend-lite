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
            // (com.legend.lowering.RelationTexts: gone 2026-11-25 with the facet, stage B's B2b S5c; a shrink)
            "com.legend.lowering.PureOnlyTexts", "com.legend.lowering.ResultEnvelopes");

    /** Members of a TWO_FORMS kind that break criterion (a), each with its reason: found 2026-11-17. */
    static final Map<String, String> TWO_FORM_EXCEPTIONS = Map.of();
    // every exception closed 2026-11-17 by train 6 (E-Java's C.9, 6086df9819's train; d-stage 6240a5147): each product form
    // moved out of its kind into its own class -- LiteCrossKindNumbers, LiteDateLiteralTexts, LiteGoldenNullWord,
    // LiteRenderedTextRule, and LoweringForms' PRODUCT field into ProductLoweringForms (TestDataPlanner$Printing#CLEAN
    // closed earlier by K12, testdatagen.LitePrinting): re-cut on d-stage c37207858 by the GUARD stream

    /** PlanStyle's declared methods. */
    static final Set<String> STYLE_METHODS = Set.of(
            "private static java.lang.String dateTimeText(java.lang.String)",
            "public abstract <F> F facet(java.lang.Class<F>)",
            "public abstract <F> F facet(java.lang.Class<F>, java.util.function.Supplier<? extends java.lang.RuntimeException>)",
            "public abstract <F> F twoForm(java.lang.Class<F>, F)",
            "public abstract boolean judgesEngineText()",
            "public abstract com.legend.database.PlanStyle$TraceCell traceCell()",
            "public abstract com.legend.sql.dialect.SqlDialect dialect(com.legend.model.ConnectionDefinition$DatabaseType)",
            // (errorWords: deleted 2026-11-20, stage B's B1: a planned failure carries lite's message and its kinded part as data; a down move)
            "public abstract java.lang.String graphKeyDate(java.lang.String)",
            "public abstract java.lang.String graphKeyString(java.lang.String)");
            // (siteWords(String, List<RaiseSite>), the planner's offer of each statement and its sites to the edge's
            // recorder (K10b, 2026-11-17): deleted, stage B's B1, 2026-11-20, errors as records: the edge words a site from its record after the run; a down move, the user's standing approval and D2 of 2026-11-20 ~17:00)
            // (plainRaises(): gone from the style 2026-11-17, d-int6)

    /** Every style method by name: each style implements them all today. */
    private static final Set<String> ALL = Set.of("dialect", "facet", "graphKeyDate",
            "graphKeyString", "judgesEngineText", "traceCell", "twoForm");

    /** Each implementer of PlanStyle -> the style methods it declares. */
    static final Map<String, Set<String>> OVERRIDERS = Map.ofEntries(
            // the product's style (PlanStyle.PRODUCT) and the edge's (PureText.UPSTREAM, PureText.PURE_V1, and
            // PureText.ENGINE_TESTS below)
            Map.entry("com.legend.database.PlanStyle$1", ALL),
            Map.entry("com.legend.edge.PureText$1", ALL),
            Map.entry("com.legend.edge.PureText$2", ALL),
            // 2026-11-26 (the edition program's step 3): legend-engine's own tests' style (PureText.ENGINE_TESTS, the
            // relational corpus's door) -- UPSTREAM in every method but twoForm's edition, which is legend-engine's
            // tests' (the queries a test's execute()s run are legend-engine's, the test's own code Pure's). A third
            // style because the corpus and PCT are two audiences that shared UPSTREAM until now
            Map.entry("com.legend.edge.PureText$3", ALL));
            // (the edge's recorder of one planning call's raise-site words, K10b of 2026-11-17: deleted, stage B's B1, 2026-11-20, errors as records: the edge words a site from its record after the run; a down move, the user's standing approval and D2 of 2026-11-20 ~17:00)

    private static String signature(Method m) {
        return m.getName() + Arrays.stream(m.getGenericParameterTypes()).map(java.lang.reflect.Type::getTypeName)
                .collect(Collectors.joining(", ", "(", ")"));
    }

    private static String described(Method m) {
        String mods = Modifier.toString(m.getModifiers() & Modifier.methodModifiers());
        // a type variable's bounds (the review of 7c37c88d7d, 2026-11-17: <F> and <F extends X> read alike before)
        String vars = m.getTypeParameters().length == 0 ? "" : Arrays.stream(m.getTypeParameters())
                .map(v -> v.getName() + Arrays.stream(v.getBounds()).map(java.lang.reflect.Type::getTypeName)
                        .filter(b -> !b.equals("java.lang.Object")).map(b -> " extends " + b)
                        .collect(Collectors.joining(" &")))
                .collect(Collectors.joining(", ", "<", "> "));
        return (m.isDefault() ? "default " : "") + (mods.isEmpty() ? "" : mods + " ") + vars
                + m.getGenericReturnType().getTypeName() + " " + signature(m);
    }

    /** What breaks criterion (a) in {@code kind}: "Kind#member (what)", its superinterfaces' breaks behind
     *  "Kind<-Super", and every member of a nested type it holds (the review of 7c37c88d7d, 2026-11-17: a superinterface's
     *  default and an excepted nested type's growth were unseen). */
    static List<String> breaksOfA(Class<?> kind) {
        List<String> out = new ArrayList<>();
        String k = kind.getName().substring(kind.getName().lastIndexOf('.') + 1);
        for (Class<?> sup : kind.getInterfaces()) {
            if (!sup.getName().startsWith("java.")) {
                for (String b : breaksOfA(sup)) {
                    out.add(k + "<-" + b);
                }
            }
        }
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
            for (String m : OpenClassMembersTest.membersOf(n)) {
                out.add(k + "#" + n.getSimpleName() + "#" + m + " (in a nested type)");
            }
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

    /** PlanStyle's whole surface as the open-class walk reads it (the review of 7c37c88d7d, 2026-11-17: its nested types
     *  Activity and TraceCell, its PRODUCT field and that anonymous class were unpinned). Measured 2026-11-17. */
    static final Set<String> STYLE_SURFACE = Set.of(
            "$1 (anonymous or local class)",
            "$1#<init>()",
            "$1#public F facet(java.lang.Class<F>)",
            "$1#public F facet(java.lang.Class<F>, java.util.function.Supplier<? extends java.lang.RuntimeException>)",
            "$1#public F twoForm(java.lang.Class<F>, F)",
            "$1#public boolean judgesEngineText()",
            "$1#public com.legend.database.PlanStyle$TraceCell traceCell()",
            "$1#public com.legend.sql.dialect.SqlDialect dialect(com.legend.model.ConnectionDefinition$DatabaseType)",
            // ($1#errorWords: deleted 2026-11-20, stage B's B1: a planned failure carries lite's message and its kinded part as data; a down move)
            "$1#public java.lang.String graphKeyDate(java.lang.String)",
            "$1#public java.lang.String graphKeyString(java.lang.String)",
            // (siteWords, K10b's offer of a statement and its sites: deleted, stage B's B1, 2026-11-20, errors as records: the edge words a site from its record after the run; a down move, the user's standing approval and D2 of 2026-11-20 ~17:00)
            "TraceCell#private final java.lang.String after",
            "TraceCell#private final java.lang.String before",
            "TraceCell#public <init>(java.lang.String, java.lang.String)",
            "TraceCell#public final boolean equals(java.lang.Object)",
            "TraceCell#public final int hashCode()",
            "TraceCell#public final java.lang.String toString()",
            "TraceCell#public java.lang.String after()",
            "TraceCell#public java.lang.String before()",
            "private static java.lang.String dateTimeText(java.lang.String)",
            "public abstract F facet(java.lang.Class<F>)",
            "public abstract F facet(java.lang.Class<F>, java.util.function.Supplier<? extends java.lang.RuntimeException>)",
            "public abstract F twoForm(java.lang.Class<F>, F)",
            "public abstract boolean judgesEngineText()",
            "public abstract com.legend.database.PlanStyle$TraceCell traceCell()",
            "public abstract com.legend.sql.dialect.SqlDialect dialect(com.legend.model.ConnectionDefinition$DatabaseType)",
            // (errorWords: deleted 2026-11-20, stage B's B1: a planned failure carries lite's message and its kinded part as data; a down move)
            "public abstract java.lang.String graphKeyDate(java.lang.String)",
            "public abstract java.lang.String graphKeyString(java.lang.String)",
            "public static final TraceCell (class)",
            "public static final com.legend.database.PlanStyle PRODUCT",
            "static {}");

    @Test
    void thePlanStylesWholeSurfaceIsPinned() {
        Set<String> surface = OpenClassMembersTest.membersOf(PlanStyle.class);
        assertEquals(new TreeSet<>(STYLE_SURFACE), new TreeSet<>(surface), "PlanStyle's nested types, fields or its"
                + " PRODUCT's anonymous class changed: pin it with its ruling");
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
        List<String> breaks = breaksOfA(PlantedKind.class);
        assertTrue(breaks.containsAll(List.of(k + "twice(int) (method with a body)", k + "of() (static method)",
                k + "ZERO (field)", k + "Product (nested type)", k + "Product#public int one(int) (in a nested type)",
                "PlanStyleSurfaceTest$PlantedKind<-PlanStyleSurfaceTest$PlantedHelper#helper() (method with a body)")),
                "four breaks of its own, a nested type's member, and a superinterface's default: " + breaks);
        Map<String, Set<String>> found = overriders(new ClassFileImporter().importClasses(PlantedStyle.class));
        assertEquals(1, found.size());
        assertEquals(Set.of("judgesEngineText"), found.values().iterator().next(), "its one override is read");
    }

    /** A planted helper a kind extends: its default breaks (a) through the kind. */
    interface PlantedHelper {
        default int helper() {
            return 0;
        }
    }

    /** A planted two-form kind breaking (a) four ways, and a fifth through its superinterface. */
    interface PlantedKind extends PlantedHelper {
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
