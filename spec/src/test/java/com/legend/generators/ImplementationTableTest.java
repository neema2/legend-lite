// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.builtin.Pure;
import com.legend.model.Function;
import com.legend.model.NativeFunctionDefinition;
import com.legend.platform.DeclarationTable;
import com.legend.platform.FunctionId;
import com.legend.platform.Implementation;
import com.legend.platform.ImplementationTable;
import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

    @Test
    void theTableIsTotalAndEveryRegistrationResolves() throws IOException {
        UpstreamDeclarations upstream = UpstreamDeclarations.load();
        // THE ENGINE SURFACE the platform takes on is exactly what its
        // registrations name: the catalog's FQNs, and every FQN a form, a wall,
        // a walled body or a subsumed program names (tds::extend, createDbConfig)
        Set<String> registeredFqns = new LinkedHashSet<>();
        for (NativeFunctionDefinition n : Pure.all()) {
            registeredFqns.add(n.qualifiedName());
        }
        for (com.legend.compiler.spec.CoreFn form : com.legend.compiler.spec.CoreFn.values()) {
            registeredFqns.addAll(form.ownedFqns());
        }
        registeredFqns.addAll(Pure.walledNativeFqns());
        registeredFqns.addAll(com.legend.compiler.spec.WalledBodies.reasons().keySet());
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
        ImplementationTable impl = ImplementationTable.build(table);

        Map<String, Integer> kinds = new LinkedHashMap<>();
        List<String> rows = new ArrayList<>();
        rows.add("fqn\tid\tkind\tbodied\tdetail");
        for (var e : impl.rows().entrySet()) {
            Implementation i = e.getValue();
            String kind = i.getClass().getSimpleName();
            kinds.merge(kind, 1, Integer::sum);
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
        Files.createDirectories(Repo.outDir());
        Files.write(Repo.out("implementation-table.tsv"), out);
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
    }

    private static String detail(Implementation i) {
        return switch (i) {
            case Implementation.Form f -> f.form() + (f.alsoLowered().isEmpty() ? "" : " +" + f.alsoLowered())
                    + (f.alsoFamilies().isEmpty() ? "" : " +" + f.alsoFamilies());
            case Implementation.Intrinsic in -> in.positions() + " " + in.families();
            case Implementation.Refused r -> r.reason() + ": " + r.why();
            case Implementation.Body b -> "";
            case Implementation.Unimplemented u -> "";
        };
    }
}
