// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.probe;

import com.legend.builtin.DecisionProbe;
import com.legend.platform.Registrations;
import com.legend.platform.ImplementationTable;
import com.legend.platform.Implementation;
import com.legend.platform.FunctionId;
import com.legend.platform.DeclarationTable;
import com.legend.builtin.Pure;
import com.legend.platform.CoreFn;
import com.legend.model.Function;
import com.legend.model.FunctionDefinition;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * THE SHADOW DIFF (platform architecture untangle, step 3 — a PROBE, deleted
 * at step 4): at the two decision points the tables will own, record what
 * today decides beside what the tables say, and change nothing.
 *
 * <ul>
 * <li>{@link #overloads}: the overload SET the typer chooses from
 *     ({@code FunctionCompiler.functionsAt}) vs {@link DeclarationTable#at}.</li>
 * <li>{@link #pick}: the IMPLEMENTATION lowering takes for a resolved
 *     callee (a scalar rule, a reducer, a window function, an inlined body,
 *     a wall) vs {@link ImplementationTable#of}.</li>
 * <li>{@link #form}: a language form dispatched on a NAME ({@code CoreFn.of})
 *     vs the form that owns that FQN in the registrations.</li>
 * </ul>
 *
 * <p>Bound to the compiler's and the lowering's {@link DecisionProbe} calls by
 * {@code ServiceLoader} (this package reads their registries, so it sits above
 * them). Off unless {@code LL_SHADOW} is set (the {@code LL_*} census convention).
 * Lines go to {@code $TEST_UNDECLARED_OUTPUTS_DIR/shadow.tsv} under Bazel
 * (collected in the target's {@code outputs.zip}), else to the path
 * {@code LL_SHADOW} names, else to stderr. Each distinct line is written once.
 */
public final class Shadow implements DecisionProbe {

    /** The test/query currently compiling — set by harness runners so a line
     * names its witness (the StampCensus.CONTEXT pattern). */
    public static final ThreadLocal<String> CONTEXT =
            ThreadLocal.withInitial(() -> "<unattributed>");

    /** Every distinct line, written once. */
    static final Set<String> SEEN = ConcurrentHashMap.newKeySet();
    /** The declaration table of each model the typer compiles against. */
    static final Map<Object, DeclarationTable> TABLES =
            Collections.synchronizedMap(new WeakHashMap<>());
    /** The table row of each resolved callee seen by the lowering. */
    static final Map<FunctionId, Implementation> ROWS = new ConcurrentHashMap<>();

    private static final class Sink {
        static final Registrations REGISTRATIONS = com.legend.lowering.PlatformRegistrations.current();
        static final ImplementationTable CATALOG =
                ImplementationTable.build(DeclarationTable.of(Pure.all()), REGISTRATIONS);
        static final Map<String, CoreFn> FORM_AT = formAt();
        static final PrintWriter OUT = open();

        private static Map<String, CoreFn> formAt() {
            Map<String, CoreFn> out = new java.util.HashMap<>();
            for (var e : REGISTRATIONS.forms().entrySet()) {
                for (String fqn : e.getValue()) {
                    out.put(fqn, e.getKey());
                }
            }
            return Map.copyOf(out);
        }

        private static PrintWriter open() {
            String dir = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
            String flag = System.getenv("LL_SHADOW");
            Path path = dir != null ? Path.of(dir, "shadow.tsv")
                    : "1".equals(flag) ? null : Path.of(flag);
            if (path == null) {
                return new PrintWriter(System.err, true, StandardCharsets.UTF_8);
            }
            try {
                return new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND), true);
            } catch (IOException e) {
                throw new IllegalStateException("shadow sink " + path, e);
            }
        }
    }

    /** Decision point 1: the overload set today's merge returns at {@code fqn}
     *  against the declaration table over the catalog and {@code model}'s functions. */
    @Override
    public void onOverloads(String fqn, List<Function> today, Object model,
            Stream<Function> modelFunctions) {
        DeclarationTable table = TABLES.computeIfAbsent(model, k -> {
            DeclarationTable t = DeclarationTable.of(Stream.concat(Pure.all().stream(), modelFunctions).toList());
            if (!t.duplicates().isEmpty()) {
                write("DUPLICATES", String.valueOf(System.identityHashCode(model)),
                        String.valueOf(t.duplicates().size()), String.join(",", t.duplicates()), "");
            }
            return t;
        });
        Set<String> todayIds = new TreeSet<>();
        for (Function f : today) {
            todayIds.add(FunctionId.of(f).qualified());
        }
        Set<String> tableIds = new TreeSet<>();
        for (Function f : table.at(fqn)) {
            tableIds.add(FunctionId.of(f).qualified());
        }
        if (todayIds.equals(tableIds)) {
            return;
        }
        List<String> onlyToday = new ArrayList<>(todayIds);
        onlyToday.removeAll(tableIds);
        List<String> onlyTable = new ArrayList<>(tableIds);
        onlyTable.removeAll(todayIds);
        write("OVERLOADS", fqn, String.join(",", onlyToday), String.join(",", onlyTable), "");
    }

    /** Decision point 2: what lowering picked for {@code callee} ({@code today}
     *  names the pick) against the implementation table's row for its id. */
    @Override
    public void onPick(@com.legend.Nullable Function definition, String today) {
        if (definition == null) {
            return;
        }
        FunctionId id = FunctionId.of(definition);
        Implementation row = Sink.CATALOG.of(id);
        if (row == null) {
            row = ROWS.computeIfAbsent(id, k -> {
                List<Function> ds = new ArrayList<>(Pure.all());
                ds.add(definition);
                Implementation r = ImplementationTable.build(DeclarationTable.of(ds), Sink.REGISTRATIONS).of(k);
                return r == null ? new Implementation.Unimplemented() : r;
            });
        }
        String kind = definition instanceof FunctionDefinition ? "body" : "native";
        // a family-implemented native reaching the scalar funnel is the WRONG
        // SITE — the table and today agree on the family, not on the dispatch
        String verdict = today.equals("FAMILY") ? "WRONG-SITE" : agree(today, row) ? "agree" : "DIFFER";
        write("PICK", id.qualified(), today, describe(row) + " <" + kind + ">", verdict);
    }

    /** A form dispatched on the spelled name {@code name}. */
    @Override
    public void onForm(String name, String form) {
        CoreFn owner = Sink.FORM_AT.get(name);
        String owned = owner == null ? "NONE" : owner.name();
        write("FORM", name, form, owned, owned.equals(form) ? "agree" : "DIFFER");
    }

    private static boolean agree(String today, Implementation row) {
        return switch (row) {
            case Implementation.Form f -> today.startsWith("FORM")
                    || f.alsoLowered().stream().anyMatch(p -> p.name().equals(today));
            case Implementation.Intrinsic in -> in.positions().stream().anyMatch(p -> p.name().equals(today))
                    || (today.equals("SCALAR-FEATURE") && in.positions().contains(Implementation.Position.SCALAR));
            case Implementation.Body b -> today.equals("BODY");
            case Implementation.Unimplemented u -> today.equals("UNIMPLEMENTED");
            case Implementation.Refused r -> today.equals("WALLED-BODY") || today.equals("WALLED-NATIVE")
                    || today.equals("SUBSUMED");
        };
    }

    private static String describe(Implementation row) {
        return switch (row) {
            case Implementation.Form f -> "Form(" + f.form() + (f.alsoLowered().isEmpty() ? "" : " also " + f.alsoLowered()) + ")";
            case Implementation.Intrinsic in -> "Intrinsic" + in.positions()
                    + (in.families().isEmpty() ? "" : in.families().stream().map(Class::getSimpleName).toList());
            case Implementation.Body b -> "Body";
            case Implementation.Unimplemented u -> "Unimplemented";
            case Implementation.Refused r -> "Refused(" + r.reason() + ")";
        };
    }

    private static void write(String kind, String subject, String today, String table, String verdict) {
        String line = kind + "\t" + subject + "\t" + today + "\t" + table + "\t" + verdict;
        if (SEEN.add(line)) {
            Sink.OUT.println(line + "\t" + CONTEXT.get());
        }
    }
}
