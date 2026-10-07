// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.builtin.Pure;
import com.legend.model.Function;
import com.legend.model.NativeFunctionDefinition;
import com.legend.platform.DeclarationTable;
import com.legend.model.FunctionId;
import com.legend.platform.Implementation;
import com.legend.platform.ImplementationTable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE IMPLEMENTATION TABLE over real declarations (platform architecture
 * untangle, step 2): the catalog, the whole pinned standard library, and every
 * upstream overload at an FQN the catalog declares. Nothing consumes the table
 * yet; this test proves it is total, that every registration names a declared
 * function, and that no two registrations contradict — and prints the table,
 * the platform's implemented surface as one list.
 */
class ImplementationTableTest {

    /** The table over the catalog, the pinned standard library and every upstream overload at a registered FQN. */
    record Built(DeclarationTable table, ImplementationTable impl, int stdlib, int atCatalogFqns) {
    }

    static Built build() throws IOException {
        UpstreamDeclarations upstream = UpstreamDeclarations.load();
        // THE ENGINE SURFACE the platform takes on is exactly what its
        // registrations name: the catalog's FQNs, and every FQN a form, a wall,
        // a walled body or a subsumed program names (tds::extend, createDbConfig)
        Set<String> registeredFqns = new LinkedHashSet<>();
        for (NativeFunctionDefinition n : Pure.all()) {
            registeredFqns.add(n.qualifiedName());
        }
        for (com.legend.platform.CoreFn form : com.legend.platform.CoreFn.values()) {
            registeredFqns.addAll(form.ownedFqns());
        }
        registeredFqns.addAll(Pure.walledNativeFqns());
        registeredFqns.addAll(com.legend.platform.WalledBodies.reasons().keySet());
        for (com.legend.builtin.Subsumed sub : com.legend.builtin.Subsumed.values()) {
            registeredFqns.add(sub.fqn());
        }
        List<Function> declarations = new ArrayList<>(Pure.all());
        int stdlib = 0;
        int atCatalogFqns = 0;
        for (UpstreamDeclarations.Declared d : upstream.all) {
            if (d.stdlib()) {
                declarations.add(d.function());
                stdlib++;
            } else if (registeredFqns.contains(d.function().qualifiedName())) {
                declarations.add(d.function());
                atCatalogFqns++;
            }
        }
        DeclarationTable table = DeclarationTable.of(declarations);
        return new Built(table, ImplementationTable.build(table, com.legend.lowering.PlatformRegistrations.current()),
                stdlib, atCatalogFqns);
    }

    /** Rows per implementation kind (Intrinsic, Form, Body, ...): a measurement, in spec's ratchets.tsv. */
    static Map<String, Integer> kindsOf(ImplementationTable impl) {
        Map<String, Integer> kinds = new java.util.TreeMap<>();
        for (Implementation i : impl.rows().values()) {
            kinds.merge(i.getClass().getSimpleName(), 1, Integer::sum);
        }
        return kinds;
    }

    @Test
    void theTableIsTotalAndEveryRegistrationResolves() throws IOException {
        Built built = build();
        DeclarationTable table = built.table();
        assertEquals(List.of(), table.duplicates(), "two different bodies under one id");
        ImplementationTable impl = built.impl();
        int stdlib = built.stdlib();
        int atCatalogFqns = built.atCatalogFqns();

        Map<String, Integer> kinds = kindsOf(impl);
        List<String> rows = new ArrayList<>();
        rows.add("fqn\tid\tkind\tbodied\tdetail");
        for (var e : impl.rows().entrySet()) {
            Implementation i = e.getValue();
            String kind = i.getClass().getSimpleName();
            Function decl = table.get(e.getKey());
            rows.add(decl.qualifiedName() + "\t" + e.getKey() + "\t" + kind + "\t"
                    + (decl instanceof com.legend.model.FunctionDefinition) + "\t" + detail(i));
        }
        List<String> out = new ArrayList<>();
        out.add("# implementation table — " + java.time.LocalDate.now());
        out.add("# declarations=" + table.size() + " (catalog " + Pure.all().size() + ", stdlib " + stdlib
                + ", engine declarations at registered FQNs " + atCatalogFqns + ") rows=" + impl.rows().size());
        out.add("# kinds " + kinds);
        out.add("# dangling registrations " + impl.dangling().size() + " " + impl.dangling());
        out.add("# conflicts " + impl.conflicts().size() + " " + impl.conflicts());
        out.add("# walled class-member bodies (not function rows) " + impl.memberWalls().size());
        out.addAll(rows);
        Files.createDirectories(com.legend.testing.TestOutputs.dir());
        Files.write(com.legend.testing.TestOutputs.file("implementation-table.tsv"), out);
        out.subList(0, 6).forEach(System.out::println);

        // TOTAL: one row per declaration
        assertEquals(table.size(), impl.rows().size(), "the table is not total");
        // every catalog native is a declaration the table holds
        for (NativeFunctionDefinition n : Pure.all()) {
            assertTrue(impl.of(FunctionId.of(n)) != null, "catalog native with no row: " + FunctionId.of(n));
        }
        // every registration names something declared; none contradicts another
        assertEquals(List.of(), impl.dangling(), "registrations naming nothing declared");
        assertEquals(List.of(), impl.conflicts(), "contradicting registrations");
        // THE KINDS, checked against the generated report (audit 2026-09-25: totality alone lets an empty
        // registration set pass). The measured counts live in ratchets.tsv (//spec:update_ratchets, diff-tested in
        // //:generated), so a registration that lands moves them there, as a reviewed diff (Bazel workplan P2-16, D9)
        assertEquals(SpecRatchets.measuredWithPrefix("implementation.kinds."), kinds,
                "implementation kinds");
    }

    /** The versions of functions the platform implements with no row of their own (refused, NO_ROW), by id. */
    static List<String> unrowed(ImplementationTable impl) {
        List<String> out = new ArrayList<>();
        for (var e : impl.rows().entrySet()) {
            if (e.getValue() instanceof Implementation.Refused r && r.reason() == Implementation.Reason.NO_ROW) {
                out.add(e.getKey().qualified());
            }
        }
        java.util.Collections.sort(out);
        return out;
    }

    /** The upstream versions of functions the platform implements that have no row: shrink-only. A call that reaches
     *  one fails, naming it; each gets a row (a membership line) before the default world takes upstream core whole
     *  (build rebuild Phase 4). 110 when the table became the one authority (2026-10-06, Phase 3): the by-name drops
     *  went, 26 versions got rows (the 14 the reference lane's OVERLOAD rows needed, the 12 Boolean comparisons PCT
     *  calls), and the rest are listed in unrowed-versions.txt. */
    static final int UNROWED_MAX = 110;

    @Test
    void theVersionsWithoutARowOnlyShrink() throws IOException {
        List<String> unrowed = unrowed(build().impl());
        Files.createDirectories(com.legend.testing.TestOutputs.dir());
        Files.write(com.legend.testing.TestOutputs.file("unrowed-versions.txt"), unrowed);
        assertEquals(UNROWED_MAX, unrowed.size(), "versions of functions the platform implements with no row: "
                + unrowed.size() + " (shrink-only; lower UNROWED_MAX with the reason, never raise it) — the list is in"
                + " unrowed-versions.txt");
        assertEquals(SpecRatchets.measured("implementation.unrowed"), unrowed.size(),
                "the unrowed versions moved -- bazel run //spec:update_ratchets");
    }

    private static String detail(Implementation i) {
        return switch (i) {
            case Implementation.Form f -> f.form() + (f.alsoLowered().isEmpty() ? "" : " +" + f.alsoLowered())
                    + (f.alsoFamilies().isEmpty() ? "" : " +" + f.alsoFamilies().stream().map(Class::getSimpleName).toList());
            case Implementation.Intrinsic in -> in.positions()
                    + (in.featureOverrides().isEmpty() ? "" : " under " + in.featureOverrides())
                    + " " + in.families().stream().map(Class::getSimpleName).toList();
            case Implementation.Refused r -> r.reason() + ": " + r.why();
            case Implementation.Body b -> "";
            case Implementation.Unimplemented u -> "";
        };
    }
}
