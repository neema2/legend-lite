// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.test;

import com.legend.model.ParsedModel;
import com.legend.model.FunctionDefinition;
import com.legend.model.ImportScope;
import com.legend.model.PackageableElement;
import com.legend.model.StereotypeApplication;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DISCOVERY of Pure test functions in a compiled model — the product half of
 * what the corpus harness used to do in one file (upstream boundary batch 7a,
 * 2026-09-11; USER: "the harness shell is product surface").
 *
 * <p>Reads the MODEL only, never a file: a test is a function carrying the
 * {@code test} profile's {@code Test} stereotype (legend-pure's own profile,
 * carried by the prelude), a setup is a zero-parameter function carrying
 * {@code BeforePackage}, and the engine's exclusion marks ({@code ToFix},
 * {@code ExcludeAlloy}) are REPORTED on the test case, never applied here —
 * the caller decides what to skip (a corpus harness skips the engine's
 * exclusions; a user's runner may not). The order is the engine suite's own
 * ({@code PureTestBuilder.buildSuite}): package tree first, then name.
 */
public final class PureTests {

    private PureTests() {
    }

    /** One discovered test function with the facts a runner needs. */
    public record TestCase(String fqn, String pkg, FunctionDefinition fn, ImportScope imports,
            boolean toFix, boolean excludeAlloy) {
        /** The engine's own exclusion marks. */
        public boolean excludedByEngine() {
            return toFix || excludeAlloy;
        }
    }

    /** Everything discovery found: the tests (all of them, marks included) and
     *  the {@code BeforePackage} setups by package. */
    public record Discovery(List<TestCase> tests, Map<String, List<String>> setupsByPackage) {
        public Discovery {
            tests = List.copyOf(tests);
            setupsByPackage = Map.copyOf(setupsByPackage);
        }

        /** The tests the engine's own marks do not exclude. */
        public List<TestCase> runnable() {
            return tests.stream().filter(t -> !t.excludedByEngine()).toList();
        }
    }

    /** Discover every test and setup in {@code model}, skipping the elements
     *  named in {@code notTests} (a caller's library sources contribute model
     *  only — their own test functions are not this run's). */
    public static Discovery discover(ParsedModel model, Set<String> notTests) {
        List<TestCase> tests = new ArrayList<>();
        Map<String, List<String>> setupsByPackage = new LinkedHashMap<>();
        for (PackageableElement el : model.elements()) {
            if (!(el instanceof FunctionDefinition f) || notTests.contains(el.qualifiedName())) {
                continue;
            }
            String fqn = f.qualifiedName();
            int cut = fqn.lastIndexOf("::");
            String pkg = cut > 0 ? fqn.substring(0, cut) : "";
            boolean test = false;
            boolean toFix = false;
            boolean excludeAlloy = false;
            boolean setup = false;
            for (StereotypeApplication st : f.stereotypes()) {
                if (!(st.profileName().equals("test")
                        || st.profileName().equals("meta::pure::profiles::test"))) {
                    continue;
                }
                switch (st.stereotypeName()) {
                    case "Test" -> test = true;
                    // the engine's own exclusions (PureTestHelperFramework
                    // satisfiesConditions: !ExcludeAlloy; ToFix is the corpus's
                    // disabled mark); the profile has no "Ignore"
                    case "ToFix" -> toFix = true;
                    case "ExcludeAlloy" -> excludeAlloy = true;
                    case "BeforePackage" -> setup = true;
                    default -> { }
                }
            }
            if (setup && f.parameters().isEmpty()) {
                setupsByPackage.computeIfAbsent(pkg, k -> new ArrayList<>()).add(fqn);
            }
            if (test) {
                List<String> wildcards = new ArrayList<>();
                ImportScope own = model.elementImports().get(fqn);
                if (own != null) {
                    wildcards.addAll(own.wildcards());
                }
                if (!pkg.isEmpty() && !wildcards.contains(pkg)) {
                    wildcards.add(pkg);
                }
                tests.add(new TestCase(fqn, pkg, f, new ImportScope(wildcards), toFix, excludeAlloy));
            }
        }
        tests.sort((a, b) -> engineSuiteOrder(a.fqn(), b.fqn()));
        return new Discovery(tests, setupsByPackage);
    }

    /** {@code PureTestBuilder.buildSuite}'s traversal as a comparator: compare
     *  package segments; at the first divergence sort alphabetically; an
     *  ANCESTOR package's own tests run AFTER its sub-suites (deeper fqn
     *  first); the same package sorts by test name. */
    public static int engineSuiteOrder(String fqnA, String fqnB) {
        String[] a = fqnA.split("::");
        String[] b = fqnB.split("::");
        int i = 0;
        while (i < a.length - 1 && i < b.length - 1 && a[i].equals(b[i])) {
            i++;
        }
        if (i < a.length - 1 && i < b.length - 1) {
            return a[i].compareTo(b[i]);
        }
        if (a.length == b.length) {
            return a[a.length - 1].compareTo(b[b.length - 1]);
        }
        return a.length < b.length ? 1 : -1;
    }
}
