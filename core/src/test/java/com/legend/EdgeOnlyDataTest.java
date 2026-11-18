// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * GUARD L-B: THE PLAN'S AND THE MIR'S DATA, EACH PIECE THE PRODUCT'S OWN (the orchestrator's order of 2026-11-18, after
 * J28 -- a MIR variant the edge alone made -- and J31 -- legend-engine's template fields, set and read by the edge alone).
 * Every record component, enum constant and sealed variant of the MIR ({@code com.legend.sql}), the plans
 * ({@code com.legend.executionplan}, {@code com.legend.testexecutionplan}) and the typed tree
 * ({@code com.legend.compiler.spec.typed}) needs a REAL product use:
 * <ul>
 *   <li>a component is READ by product code;</li>
 *   <li>a constant is NAMED by product code (a switch's arm is a case, not a use: javac's switch tables are left out);</li>
 *   <li>a variant is MADE by product code.</li>
 * </ul>
 * Copying is no use: the record's own code (its constructor, its {@code withX}), a {@code SqlRewriter} (it rebuilds
 * every node it walks) and the plans' codecs ({@code MirJson}, {@code PlanJson}, {@code RecordJson}: they carry every
 * field both ways). Each piece with no real product use is reported, the edge's use beside it, and pinned with its
 * ruling ({@link #OPEN}); a new one fails. Named limits: a value-level shape (a component the product always sets to
 * null, a constant it never produces but compares against) is not seen; a constant compiled into a switch through an
 * {@code invokedynamic} label is a case, as a switch table is.
 */
@Tag("guardrail")
class EdgeOnlyDataTest {

    /** The packages whose data is checked (each exactly, not its subpackages). */
    static final Set<String> PACKAGES = Set.of("com.legend.sql", "com.legend.executionplan",
            "com.legend.testexecutionplan", "com.legend.compiler.spec.typed");

    /** The plans' codecs: they carry every field both ways, so their reads and makes are copies. */
    static final Set<String> CODECS = Set.of("com.legend.executionplan.MirJson", "com.legend.executionplan.PlanJson",
            "com.legend.testexecutionplan.RecordJson");

    private static final String EDGE = "com.legend.edge.";

    private static final String REWRITER = "com.legend.sql.SqlRewriter";

    /** Each piece with no real product use, "kind owner#piece: who uses it" -> its ruling. Measured 2026-11-18. */
    static final Map<String, String> OPEN = Map.of();

    enum Who { PRODUCT, EDGE, COPY }

    static String top(String name) {
        return name.contains("$") ? name.substring(0, name.indexOf('$')) : name;
    }

    /** Whose use {@code use} of a piece of {@code owner} is. */
    static Who who(JavaAccess<?> use, JavaClass owner) {
        JavaClass origin = use.getOriginOwner();
        String name = origin.getName();
        if (top(name).startsWith(EDGE)) {
            return Who.EDGE;
        }
        if (name.equals(owner.getName()) || name.startsWith(owner.getName() + "$")
                || origin.isAssignableTo(REWRITER) || CODECS.contains(top(name))) {
            return Who.COPY;
        }
        return Who.PRODUCT;
    }

    /** "kind owner#piece: by the edge" or "...: by none", for each piece of {@code classes} in {@code packages} with no
     *  real product use. */
    static Set<String> found(JavaClasses classes, Set<String> packages) {
        Set<String> out = new TreeSet<>();
        for (JavaClass c : classes) {
            if (!packages.contains(c.getPackageName()) || c.getName().startsWith(EDGE)) {
                continue;
            }
            Class<?> k = c.reflect();
            if (k.isRecord()) {
                for (java.lang.reflect.RecordComponent rc : k.getRecordComponents()) {
                    List<JavaAccess<?>> uses = new ArrayList<>();
                    Optional<JavaMethod> accessor = c.tryGetMethod(rc.getName());
                    accessor.ifPresent(m -> {
                        uses.addAll(m.getCallsOfSelf());
                        uses.addAll(m.getReferencesToSelf());
                    });
                    c.tryGetField(rc.getName()).ifPresent(f -> f.getAccessesToSelf().stream()
                            .filter(a -> a.getAccessType() == JavaFieldAccess.AccessType.GET).forEach(uses::add));
                    report(out, "component " + c.getName() + "#" + rc.getName(), uses, c);
                }
            }
            if (k.isEnum()) {
                for (Object constant : k.getEnumConstants()) {
                    String name = ((Enum<?>) constant).name();
                    JavaField f = c.getField(name);
                    List<JavaAccess<?>> uses = new ArrayList<>();
                    // a switch's table (javac's synthetic $SwitchMap class) names every constant it cases: no use
                    f.getAccessesToSelf().stream().filter(a -> !a.getOriginOwner().getModifiers()
                            .contains(JavaModifier.SYNTHETIC)).forEach(uses::add);
                    report(out, "constant " + c.getName() + "." + name, uses, c);
                }
            }
            if (k.isSealed()) {
                for (Class<?> variant : k.getPermittedSubclasses()) {
                    if (variant.isInterface() || java.lang.reflect.Modifier.isAbstract(variant.getModifiers())
                            || !packages.contains(variant.getPackageName())) {
                        continue;
                    }
                    JavaClass v = classes.get(variant);
                    List<JavaAccess<?>> uses = new ArrayList<>();
                    for (JavaConstructor ctor : v.getConstructors()) {
                        uses.addAll(ctor.getCallsOfSelf());
                        uses.addAll(ctor.getReferencesToSelf());
                    }
                    report(out, "variant " + variant.getName(), uses, v);
                }
            }
        }
        return out;
    }

    private static void report(Set<String> out, String piece, List<JavaAccess<?>> uses, JavaClass owner) {
        boolean product = false;
        boolean edge = false;
        for (JavaAccess<?> use : uses) {
            switch (who(use, owner)) {
                case PRODUCT -> product = true;
                case EDGE -> edge = true;
                case COPY -> { }
            }
        }
        if (!product) {
            out.add(piece + (edge ? ": by the edge" : ": by none"));
        }
    }

    private static List<java.util.jar.JarFile> jarFiles(List<Path> jars) throws IOException {
        List<java.util.jar.JarFile> out = new ArrayList<>();
        for (Path jar : jars) {
            out.add(new java.util.jar.JarFile(jar.toFile()));
        }
        return out;
    }

    @Test
    void everyPieceOfThePlansDataHasAProductUse() throws IOException {
        JavaClasses classes = new ClassFileImporter().importJars(jarFiles(ConstantPools.jars("legend.product.jars",
                "legend.authored.jars", "legend.edge.jars")));
        Set<String> found = found(classes, PACKAGES);
        Map<String, String> byKind = new TreeMap<>();
        found.forEach(f -> byKind.merge(f.substring(f.lastIndexOf(": ") + 2), "1", (a, b) -> String.valueOf(
                Integer.parseInt(a) + 1)));
        assertEquals(new TreeSet<>(OPEN.keySet()), found, "a piece of the plan's or the MIR's data with no real product"
                + " use -- the edge's data in the product (J28's, J31's shape): move it to the edge, delete it, or rule it"
                + " here (" + byKind + ")");
    }

    @Test
    void theGuardBites() {
        JavaClasses planted = new ClassFileImporter().importClasses(com.legend.sql.PlantedEdgeOnlyMir.class,
                com.legend.sql.PlantedEdgeOnlyMir.Node.class, com.legend.sql.PlantedEdgeOnlyMir.Made.class,
                com.legend.sql.PlantedEdgeOnlyMir.Shared.class, com.legend.sql.PlantedEdgeOnlyMir.Kind.class,
                com.legend.edge.PlantedEdgeOnlyMirMaker.class);
        String p = "com.legend.sql.PlantedEdgeOnlyMir$";
        assertEquals(new TreeSet<>(Set.of(
                "variant " + p + "Made: by the edge",
                "component " + p + "Made#text: by the edge",
                "component " + p + "Shared#template: by the edge",
                "constant " + p + "Kind.RAW: by the edge")), found(planted, Set.of("com.legend.sql")),
                "J28's shape (a variant the edge alone makes) and J31's (a field and a constant the edge alone reads)");
    }
}
