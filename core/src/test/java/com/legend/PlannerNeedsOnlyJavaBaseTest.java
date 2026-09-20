package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The planner runs on {@code java.base} ALONE.
 *
 * <p>{@code jdeps} on the planner packages reports exactly one module
 * dependency, and the whole pipeline — parse, type, resolve, lower,
 * render — was run under {@code --limit-modules java.base} on
 * 2026-09-20 with one module visible and {@code java.sql} absent,
 * producing correct SQL in 1.4ms warm.
 *
 * <p>That property is worth a guard because it is invisible: the build
 * passes, every test passes, and nothing tells you it is gone until
 * something tries to run the planner somewhere a JDBC driver cannot
 * go — a jlink image without {@code java.sql}, or a WASM target.
 *
 * <p>IT WAS BROKEN BY ONE LINE, and not an obvious one. A
 * {@code catch (java.sql.SQLException)} in {@code Compiler} made the
 * whole class unloadable without the module, PLAN SURFACE INCLUDED,
 * because the JVM verifier resolves catch-clause types at link time.
 * A JDBC type in a method DESCRIPTOR resolves lazily and is harmless;
 * a catch clause is not. So this checks both the packages and, for the
 * mixed-surface classes that keep JDBC method signatures, the catch
 * clauses specifically.
 */
class PlannerNeedsOnlyJavaBaseTest {

    /** Parse text in, SQL out — everything the planner needs. */
    private static final Set<String> PLANNER_PACKAGES = Set.of(
            "parser", "lexer", "compiler", "normalizer", "resolver",
            "lowering", "sql", "model", "protocol", "values", "builtin",
            "error", "plan");

    /**
     * Classes that legitimately take a {@code java.sql.Connection} in a
     * signature AND sit on the plan path. Descriptors are fine; a catch
     * clause is not.
     */
    private static final Set<String> MIXED_SURFACE = Set.of("Compiler.java");

    private static final Pattern JDBC =
            Pattern.compile("\\bjava\\.sql\\.|\\bjavax\\.sql\\.");
    private static final Pattern JDBC_CATCH =
            Pattern.compile("catch\\s*\\([^)]*java\\.sql\\.");

    private static String code(Path p) throws IOException {
        return Files.readString(p)
                .replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("//[^\n]*", "");
    }

    private static List<Path> sources() throws IOException {
        Path root = Path.of("src/main/java/com/legend");
        if (!Files.isDirectory(root)) {
            root = Path.of("core/src/main/java/com/legend");
        }
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".java")).toList();
        }
    }

    @Test
    void noPlannerPackageTouchesJdbc() throws IOException {
        List<Path> all = sources();
        assertTrue(all.size() > 200,
                "census rotted: only " + all.size() + " sources found");

        List<String> strays = new ArrayList<>();
        for (Path p : all) {
            Path rel = p.subpath(p.getNameCount() - 2, p.getNameCount());
            String pkg = rel.getName(0).toString();
            if (!PLANNER_PACKAGES.contains(pkg)) {
                continue;
            }
            if (JDBC.matcher(code(p)).find()) {
                strays.add(pkg + "/" + p.getFileName());
            }
        }
        assertTrue(strays.isEmpty(),
                "the planner must need java.base alone, but these reach"
                + " for JDBC — move the boundary into exec/ as"
                + " JdbcMetadata was: " + strays);
    }

    @Test
    void noPlanPathClassCatchesAJdbcException() throws IOException {
        // The trap that cost this property once already. A descriptor
        // resolves lazily; a catch clause resolves at LINK time and
        // takes the whole class with it.
        List<String> strays = new ArrayList<>();
        for (Path p : sources()) {
            String name = p.getFileName().toString();
            if (!MIXED_SURFACE.contains(name)) {
                continue;
            }
            if (JDBC_CATCH.matcher(code(p)).find()) {
                strays.add(name);
            }
        }
        assertTrue(strays.isEmpty(),
                "a java.sql catch clause makes the PLAN surface require the"
                + " java.sql module — the verifier resolves handler types at"
                + " link time. Delegate to com.legend.exec.JdbcMetadata"
                + " instead: " + strays);
    }
}
