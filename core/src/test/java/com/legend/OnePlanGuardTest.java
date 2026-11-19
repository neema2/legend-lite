// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;
import com.legend.sql.TypeFact;
import com.tngtech.archunit.core.domain.JavaCall;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaFieldAccess;
import com.tngtech.archunit.core.importer.ClassFileImporter;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * G1, REPORT-ONLY (S0 of the presentation program, 2026-11-18; carried into the compatibility program as slice 0.2,
 * docs/records/compat-program-plan-2026-11-19.md on claude/compat-program): no upstream text built as SQL. Every method of the compatibility edge ({@code com.legend.edge..}), and every
 * product method it reaches through a call ArchUnit resolves, that builds text-producing MIR is a site: a
 * {@code StringLit}, a {@code Call} of a function the product's own typing makes text ({@link #textFns}: VARCHAR or
 * JSON, {@code SqlTyping.callType} over string operands), a JSON constructor ({@code JsonObject}, {@code JsonArray},
 * {@code JsonArrayAgg}), or a {@code Cast} to VARCHAR or JSON. Under CP-1 (ruled 2026-11-19) upstream text is the
 * formatters' after the run or the translator's input, never SQL the edge builds, so the program's slices shrink this
 * set toward 0; the census writes every site to {@code one_plan_g1_report.txt} in the test's outputs, the baseline they
 * shrink. It fails only when it cannot see a planted site; it reports until the program's guard phase makes the count
 * a shrink-only ratchet. Dispatch through an interface is followed to its declaration only.
 */
@Tag("guardrail")
class OnePlanGuardTest {

    private static final String SQL_EXPR = "com.legend.sql.SqlExpr$";

    private static final Set<String> JSON_CONSTRUCTORS = Set.of(SQL_EXPR + "JsonObject", SQL_EXPR + "JsonArray",
            SQL_EXPR + "JsonArrayAgg");

    /** The text-producing functions, derived from the product's typing: each {@code SqlFn} a {@code Call} of which,
     *  over string operands at some arity up to four, is typed VARCHAR or JSON. */
    static Set<String> textFns() {
        Set<String> out = new TreeSet<>();
        for (SqlFn fn : SqlFn.values()) {
            for (int n = 0; n <= 4 && !out.contains(fn.name()); n++) {
                SqlExpr[] args = new SqlExpr[n];
                Arrays.fill(args, new SqlExpr.StringLit("x"));
                try {
                    if (SqlExpr.Call.of(fn, args).type() instanceof TypeFact.Typed t
                            && (t.type() == SqlType.Scalar.VARCHAR || t.type() == SqlType.Scalar.JSON)) {
                        out.add(fn.name());
                    }
                } catch (RuntimeException notThisArity) {
                    // a rule refusing this arity or these operands: the function is tried at the next
                }
            }
        }
        return out;
    }

    /** The text-producing MIR {@code u} builds, one entry per kind ({@code StringLit}, {@code Call:FN},
     *  {@code Cast:VARCHAR}, {@code JsonObject}...). */
    static Set<String> builds(JavaCodeUnit u, Set<String> textFns) {
        Set<String> out = new TreeSet<>();
        boolean call = false;
        boolean cast = false;
        for (JavaCall<?> c : u.getCallsFromSelf()) {
            String owner = c.getTargetOwner().getName();
            boolean constructor = c.getName().equals("<init>");
            if (constructor && owner.equals(SQL_EXPR + "StringLit")) {
                out.add("StringLit");
            } else if (constructor && JSON_CONSTRUCTORS.contains(owner)) {
                out.add(owner.substring(SQL_EXPR.length()));
            } else if (owner.equals(SQL_EXPR + "Call") && (constructor || c.getName().equals("of"))) {
                call = true;
            } else if (constructor && owner.equals(SQL_EXPR + "Cast")) {
                cast = true;
            }
        }
        for (JavaFieldAccess f : u.getFieldAccesses()) {
            String owner = f.getTargetOwner().getName();
            if (call && owner.equals("com.legend.sql.SqlFn") && textFns.contains(f.getName())) {
                out.add("Call:" + f.getName());
            } else if (cast && owner.equals("com.legend.sql.SqlType$Scalar")
                    && (f.getName().equals("VARCHAR") || f.getName().equals("JSON"))) {
                out.add("Cast:" + f.getName());
            }
        }
        return out;
    }

    /** Every site reached from the code units of the classes whose names start with {@code root}: {@code
     *  Class.method -> kinds}, through calls into {@code com.legend} that ArchUnit resolves. */
    static Map<String, Set<String>> sites(JavaClasses classes, String root) {
        Set<String> textFns = textFns();
        Map<String, Set<String>> out = new TreeMap<>();
        Deque<JavaCodeUnit> todo = new ArrayDeque<>();
        Set<String> seen = new HashSet<>();
        for (JavaClass c : classes) {
            if (c.getName().startsWith(root)) {
                todo.addAll(c.getCodeUnits());
            }
        }
        while (!todo.isEmpty()) {
            JavaCodeUnit u = todo.pop();
            if (!seen.add(u.getFullName())) {
                continue;
            }
            Set<String> kinds = builds(u, textFns);
            if (!kinds.isEmpty()) {
                out.put(u.getOwner().getName() + "." + u.getName(), kinds);
            }
            for (JavaCall<?> c : u.getCallsFromSelf()) {
                if (c.getTargetOwner().getName().startsWith("com.legend.")) {
                    c.getTarget().resolveMember().ifPresent(todo::push);
                }
            }
        }
        return out;
    }

    private static JavaClasses productAndEdge() {
        List<java.util.jar.JarFile> jars = new ArrayList<>();
        try {
            for (String property : List.of("legend.product.jars", "legend.edge.jars")) {
                for (Path jar : com.legend.testing.Runfile.listed(property)) {
                    jars.add(new java.util.jar.JarFile(jar.toFile()));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(jars);
    }

    @Test
    void everyTextSiteTheEdgeReachesIsReported() throws IOException {
        Map<String, Set<String>> sites = sites(productAndEdge(), "com.legend.edge.");
        StringBuilder report = new StringBuilder("# G1 (report-only, S0): text-producing MIR built by the edge or a"
                + " product method it reaches; text functions: " + textFns() + "\n");
        int edge = 0;
        int product = 0;
        for (Map.Entry<String, Set<String>> e : sites.entrySet()) {
            boolean isEdge = e.getKey().startsWith("com.legend.edge.");
            if (isEdge) {
                edge++;
            } else {
                product++;
            }
            report.append(isEdge ? "edge" : "product").append('\t').append(e.getKey()).append('\t')
                    .append(String.join(",", e.getValue())).append("\tunledgered\n");
        }
        report.append("edge\t").append(edge).append("\nproduct\t").append(product).append('\n');
        System.out.println("[G1] " + edge + " edge methods and " + product + " product methods the edge reaches build"
                + " text-producing MIR (report-only)");
        String dir = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (dir != null) {
            Files.writeString(Path.of(dir, "one_plan_g1_report.txt"), report, StandardCharsets.UTF_8);
        }
        assertFalse(sites.isEmpty(), "the scan sees the edge's text builders: a scan that sees none reads the wrong jars");
    }

    @Test
    void theTextFunctionsAreTheProductsOwn() {
        Set<String> fns = textFns();
        assertTrue(fns.contains("CONCAT") && !fns.contains("ABS"), fns.toString());
    }

    /** A planted edge method, reaching a planted product method that builds a string literal. */
    static final class PlantedEdge {
        static SqlExpr text() {
            return PlantedProduct.text();
        }
    }

    static final class PlantedProduct {
        static SqlExpr text() {
            return new SqlExpr.StringLit("an upstream word");
        }
    }

    @Test
    void aPlantedSiteReachedFromTheEdgeIsSeen() {
        Map<String, Set<String>> sites = sites(new ClassFileImporter().importClasses(PlantedEdge.class,
                PlantedProduct.class), PlantedEdge.class.getName());
        assertTrue(sites.containsKey(PlantedProduct.class.getName() + ".text"), sites.toString());
    }
}
