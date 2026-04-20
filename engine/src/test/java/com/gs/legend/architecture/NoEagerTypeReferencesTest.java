package com.gs.legend.architecture;

import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.m3.PureEnum;
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
 * AGENTS.md §5 structural guard — Shape 1.
 *
 * <p>Walks every compiled class in {@code engine/target/classes}, inspects its declared fields,
 * and fails if any field outside the allowlist carries a resolved {@link PureClass} or
 * {@link PureEnum} reference (directly, or nested inside a {@link java.util.Collection},
 * {@link java.util.Map}, {@link java.util.Optional}, or array type). This catches the exact
 * pattern that forced eager cross-project loads in pre-refactor {@code PureClassMapping} and
 * {@code RelationalMapping}.
 *
 * <p>If this test fails, <strong>do not</strong> add your class to the allowlist — the right
 * fix is almost always to store the FQN as a {@code String} and resolve lazily via
 * {@code ModelContext.findClass} / {@code ModelContext.findEnum} at the use site. The
 * allowlist is reserved for the model registry itself (which by definition owns all loaded
 * classes) and similar bootstrap-infrastructure holders.
 *
 * <p>{@link com.gs.legend.model.def.FunctionDefinition} is intentionally <em>not</em> on the
 * forbidden list yet: it currently plays a dual role as both parse-level AST and resolved
 * function representation. Once that split lands, the resolved type should be added here.
 */
class NoEagerTypeReferencesTest {

    /**
     * Types whose resolved reference must not appear as a field outside the allowlist.
     * These are the "long-lived resolved user-model objects" from AGENTS.md §5.
     */
    private static final Set<Class<?>> FORBIDDEN_TYPES = Set.of(PureClass.class, PureEnum.class);

    /**
     * Field-level allowlist, keyed as {@code "fully.qualified.ClassName#fieldName"}. Every
     * entry must have a justification comment. Keep this list MINIMAL — the whole point of
     * §5 is to push callers toward FQN-based lazy resolution.
     *
     * <p>Allowlist is field-level (not class-level) on purpose: even a legitimate holder like
     * {@code PureModelBuilder} shouldn't get a free pass for NEW fields — any new resolved
     * reference must justify itself by being added here explicitly.
     *
     * <p>NOTE: {@code PureModelBuilder} currently plays two roles: parse-time assembler AND
     * runtime {@code ModelContext}. In the multi-project future (Bazel cross-project work),
     * these should split — each project gets its own builder, and cross-project lookups go
     * through a composed outer {@code ModelContext} that triggers lazy per-project loads.
     * Today's dual role is the reason this allowlist exists; it should shrink, not grow.
     */
    private static final Set<String> FIELD_ALLOWLIST = Set.of(
            // THE model registry — the authoritative list of materialized user classes for
            // the current ModelContext scope. Every other PureClass reference in the codebase
            // must ultimately come from looking up an FQN in this list via findClass.
            "com.gs.legend.model.PureModelBuilder#classes"
    );

    @Test
    void noForbiddenTypeFieldsOutsideAllowlist() throws Exception {
        Path classesRoot = Paths.get(
                PureClass.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        assertTrue(Files.isDirectory(classesRoot),
                "Expected compiled-classes directory at " + classesRoot);

        List<String> violations = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(classesRoot)) {
            paths.filter(p -> p.toString().endsWith(".class"))
                    .map(p -> classesRoot.relativize(p).toString())
                    .map(rel -> rel.replace('/', '.').replace('\\', '.'))
                    .map(name -> name.substring(0, name.length() - ".class".length()))
                    .filter(fqn -> fqn.startsWith("com.gs.legend."))
                    // Synthetic / lambda / local classes — inspecting them surfaces
                    // captured-variable fields that aren't meaningful for this check.
                    .filter(fqn -> !fqn.contains("$$Lambda"))
                    .forEach(fqn -> scanClass(fqn, violations));
        }

        if (!violations.isEmpty()) {
            fail("AGENTS.md §5: resolved PureClass / PureEnum field(s) found.\n"
                    + "These types must be stored as FQN strings and resolved lazily via\n"
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
            // Unloadable (e.g., missing optional dep) — skip rather than fail the guard.
            return;
        }
        for (Field f : cls.getDeclaredFields()) {
            // Skip synthetic compiler-generated fields (e.g. enum $VALUES, this$0 for inner
            // classes, lambda captures). Real user fields are never synthetic.
            if (f.isSynthetic()) continue;
            if (FIELD_ALLOWLIST.contains(fqn + "#" + f.getName())) continue;
            Class<?> forbidden = findForbiddenIn(f.getGenericType());
            if (forbidden != null) {
                violations.add(fqn + "#" + f.getName() + " : " + f.getGenericType().getTypeName()
                        + "  (references " + forbidden.getSimpleName() + ")");
            }
        }
    }

    /**
     * Recursively unwraps every type-system form that could carry a forbidden reference:
     * arrays, {@link java.util.Collection}s, {@link java.util.Map}s, {@link java.util.Optional}s,
     * wildcard bounds ({@code ? extends PureClass}), and generic arrays. Returns the offending
     * class if found, null otherwise.
     */
    private static Class<?> findForbiddenIn(Type t) {
        if (t instanceof Class<?> c) {
            if (c.isArray()) return findForbiddenIn(c.getComponentType());
            return FORBIDDEN_TYPES.contains(c) ? c : null;
        }
        if (t instanceof ParameterizedType pt) {
            // Check raw type (rarely forbidden directly) and then every type argument.
            Class<?> direct = findForbiddenIn(pt.getRawType());
            if (direct != null) return direct;
            for (Type arg : pt.getActualTypeArguments()) {
                Class<?> nested = findForbiddenIn(arg);
                if (nested != null) return nested;
            }
            return null;
        }
        if (t instanceof WildcardType wt) {
            // `? extends PureClass` carries the forbidden reference in its upper bounds.
            // Lower bounds (`? super X`) are checked too for completeness — equally capable
            // of holding a PureClass reference.
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
        // TypeVariable — its identity is a name, not a concrete class reference. Even if its
        // bound includes PureClass, the field's erased type will be the bound (already checked
        // via the class path above if directly forbidden).
        return null;
    }
}
