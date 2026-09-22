package com.legend.architecture;

import com.legend.compiler.element.TypedClass;
import com.legend.compiler.element.TypedEnum;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Lazy-loading structural guard — Shape 1 (rebuilt for core; the engine-era
 * original scanned {@code com.gs.legend} and died with that module).
 *
 * <p>Core's design keeps long-lived references to user-model elements as FQN
 * STRINGS resolved lazily through {@code ModelContext.findClass}/{@code findEnum}
 * ({@link TypedClass}'s own javadoc: superclasses are FQN strings so
 * cross-project lazy loading works transparently — doc §5). This test makes
 * that a checked property instead of a comment: it walks every compiled class
 * in {@code core/target/classes} and fails if any field outside the allowlist
 * carries a resolved {@link TypedClass} or {@link TypedEnum} reference —
 * directly, or nested inside a Collection, Map, Optional, array, or wildcard
 * bound.
 *
 * <p>If this test fails, <strong>do not</strong> add your class to the
 * allowlist — store the FQN as a {@code String} and resolve lazily at the use
 * site. The allowlist is reserved for the model registry itself (which by
 * definition owns all loaded elements).
 */
class NoEagerTypeReferencesTest {

    private static final Set<Class<?>> FORBIDDEN_TYPES = Set.of(TypedClass.class, TypedEnum.class);

    /**
     * Field-level allowlist, keyed {@code "fully.qualified.ClassName#fieldName"}.
     * Every entry needs a justification. Field-level on purpose: even a
     * legitimate registry holder gets no free pass for NEW fields.
     */
    private static final Set<String> FIELD_ALLOWLIST = Set.of(
            // THE model registry — the authoritative store of compiled user
            // elements for the current ModelContext scope. Every other
            // TypedClass/TypedEnum reference must come from looking up an FQN
            // here via findClass/findEnum.
            "com.legend.compiler.element.PureModelContext#classCache",
            "com.legend.compiler.element.PureModelContext#enumCache"
    );

    @Test
    void noForbiddenTypeFieldsOutsideAllowlist() throws Exception {
        Path location = Paths.get(
                TypedClass.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        // core's compiled classes are a DIRECTORY under Maven (target/classes)
        // and a JAR under Bazel. Walk whichever it is — and count, because a walk
        // over the wrong location finds nothing and a guard that checks nothing
        // passes.
        java.nio.file.FileSystem jar = Files.isDirectory(location)
                ? null : java.nio.file.FileSystems.newFileSystem(location);
        Path classesRoot = jar == null ? location : jar.getPath("/");

        List<String> violations = new ArrayList<>();
        long[] scanned = {0};
        try (Stream<Path> paths = Files.walk(classesRoot)) {
            paths.filter(p -> p.toString().endsWith(".class"))
                    .map(p -> classesRoot.relativize(p).toString()
                            .replace(java.io.File.separatorChar, '/'))
                    .map(rel -> rel.replace('/', '.').replace('\\', '.'))
                    .map(name -> name.substring(0, name.length() - ".class".length()))
                    .filter(fqn -> fqn.startsWith("com.legend."))
                    // Synthetic / lambda classes surface captured-variable
                    // fields that aren't meaningful for this check.
                    .filter(fqn -> !fqn.contains("$$Lambda"))
                    .forEach(fqn -> {
                        scanned[0]++;
                        scanClass(fqn, violations);
                    });
        } finally {
            if (jar != null) {
                jar.close();
            }
        }
        // Floor, in GuardCoverage's sense (package-private there, so inline
        // here): every main source file compiles to at least one class, and
        // core's main-source guards pin 498 files, so fewer than 498 classes
        // means this walked the wrong place. Moves down only with a written
        // justification.
        assertTrue(scanned[0] >= 498, "NoEagerTypeReferencesTest coverage DROPPED: scanned "
                + scanned[0] + " classes under " + location + ", floor 498 — the guard's"
                + " scope rotted; re-point the walk before trusting this guard");

        if (!violations.isEmpty()) {
            fail("Lazy-loading guard: resolved TypedClass / TypedEnum field(s) found.\n"
                    + "These must be stored as FQN strings and resolved lazily via\n"
                    + "ModelContext.findClass / ModelContext.findEnum at use sites.\n"
                    + "Do NOT add your class to the allowlist — fix the field instead.\n\n"
                    + "Violations:\n  " + String.join("\n  ", violations));
        }
    }

    private static void scanClass(String fqn, List<String> violations) {
        Class<?> cls;
        try {
            cls = Class.forName(fqn, false, NoEagerTypeReferencesTest.class.getClassLoader());
        } catch (Throwable t) {
            return; // unloadable — skip rather than fail the guard
        }
        for (Field f : cls.getDeclaredFields()) {
            if (f.isSynthetic()) continue;
            if (FIELD_ALLOWLIST.contains(fqn + "#" + f.getName())) continue;
            Class<?> forbidden = findForbiddenIn(f.getGenericType());
            if (forbidden != null) {
                violations.add(fqn + "#" + f.getName() + " : " + f.getGenericType().getTypeName()
                        + "  (references " + forbidden.getSimpleName() + ")");
            }
        }
    }

    /** Recursively unwraps arrays, parameterized types, wildcard bounds, and
     * generic arrays — every type-system form that can carry the reference. */
    private static Class<?> findForbiddenIn(Type t) {
        if (t instanceof Class<?> c) {
            if (c.isArray()) return findForbiddenIn(c.getComponentType());
            return FORBIDDEN_TYPES.contains(c) ? c : null;
        }
        if (t instanceof ParameterizedType pt) {
            Class<?> direct = findForbiddenIn(pt.getRawType());
            if (direct != null) return direct;
            for (Type arg : pt.getActualTypeArguments()) {
                Class<?> nested = findForbiddenIn(arg);
                if (nested != null) return nested;
            }
            return null;
        }
        if (t instanceof WildcardType wt) {
            for (Type bound : wt.getUpperBounds()) {
                Class<?> nested = findForbiddenIn(bound);
                if (nested != null) return nested;
            }
            for (Type bound : wt.getLowerBounds()) {
                Class<?> nested = findForbiddenIn(bound);
                if (nested != null) return nested;
            }
            return null;
        }
        if (t instanceof GenericArrayType gat) {
            return findForbiddenIn(gat.getGenericComponentType());
        }
        return null;
    }
}
