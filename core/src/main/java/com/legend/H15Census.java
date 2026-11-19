// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedEval;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * PROBE, NOT PRODUCT (the compatibility program's homework H15, 2026-11-19; scratch branch claude/compat-h15-nested
 * only, never merged): every RELATION-TEXT node of every program lite types (TypedQuery.body(), once per program), on
 * its typed tree after the user calls are inlined, classed by where its text goes. The relation-text nodes are the
 * three calls the Lowerer answers by the plan style's RelationTexts facet (TextRefusals.relation's three callers): a
 * relation's {@code toString} (meta::pure::functions::relation::toString), {@code toCSV}
 * (meta::relational::tests::csv::toCSV) and a TDS's {@code csv} property over a relation. Rows go to
 * /tmp/h15/&lt;pid&gt;.tsv; a run without the directory writes nothing.
 */
final class H15Census {

    private static final Path DIR = Path.of("/tmp/h15");

    private H15Census() {
    }

    static void note(TypedQuery q, List<TypedSpec> body) {
        if (!Files.isDirectory(DIR)) {
            return;
        }
        String label = label();
        List<TypedSpec> program = body;
        String inlined = "inlined";
        try {
            program = new com.legend.compiler.spec.UserCallInliner(q.specs()).inlineBody(body);
        } catch (Throwable t) {
            inlined = "raw:" + t.getClass().getSimpleName();
        }
        List<String> rows = new ArrayList<>();
        for (int i = 0; i < program.size(); i++) {
            walk(program, i, program.get(i), new ArrayList<>(), rows, label, inlined);
        }
        StringBuilder out = new StringBuilder();
        out.append("#PROGRAM\t").append(label).append('\t').append(program.size()).append('\t').append(rows.size())
                .append('\t').append(inlined).append('\n');
        for (String r : rows) {
            out.append(r).append('\n');
        }
        write(out.toString());
    }

    private static String label() {
        String current = System.getProperty("legend.test.current");
        String id = System.getProperty("legend.test.id");
        return (current != null ? current : "-") + "\t" + (id != null ? id : "-");
    }

    /** {@code node} and its subtree, {@code path} its ancestors (nearest last), each paired with its child index. */
    private static void walk(List<TypedSpec> program, int stmt, TypedSpec node, List<Step> path, List<String> rows,
            String label, String inlined) {
        String kind = kind(node);
        if (kind != null) {
            rows.add(String.join("\t", "NODE", label, kind, classify(program, stmt, path, 0), chain(path),
                    (stmt + 1) + "/" + program.size(), pos(node), argType(node), inlined));
        }
        List<TypedSpec> kids = node.children();
        for (int k = 0; k < kids.size(); k++) {
            path.add(new Step(node, k));
            walk(program, stmt, kids.get(k), path, rows, label, inlined);
            path.remove(path.size() - 1);
        }
    }

    private record Step(TypedSpec node, int child) {
    }

    private static @com.legend.base.Nullable String kind(TypedSpec node) {
        if (node instanceof TypedNativeCall n) {
            String f = n.callee().qualifiedName();
            if ("meta::pure::functions::relation::toString".equals(f)) {
                return "toString";
            }
            if (PlatformTypes.TO_CSV.equals(f)) {
                return "toCSV";
            }
        }
        if (node instanceof TypedPropertyAccess p && p.property().equals(PlatformTypes.TDS_CSV_PROPERTY)
                && Type.relationSchema(p.source().info().type()) != null) {
            return "csv";
        }
        return null;
    }

    /** Where the text at the end of {@code path} goes: ROOT (the program's or a statement's root, or an assert's
     *  side) or NESTED (the consuming node named); a let followed to its uses. */
    private static String classify(List<TypedSpec> program, int stmt, List<Step> path, int depth) {
        List<String> via = new ArrayList<>();
        for (int i = path.size() - 1; i >= 0; i--) {
            Step s = path.get(i);
            TypedSpec p = s.node();
            if (p instanceof TypedLet let) {
                if (i != 0 || depth > 3) {
                    return tag(via, "NESTED:let-in-expression");
                }
                List<String> uses = uses(program, stmt, let.name(), depth);
                return tag(via, uses.isEmpty() ? "ROOT:unread-let" : "LET(" + let.name() + ")->" + String.join("|",
                        uses));
            }
            if (p instanceof TypedIf) {
                if (s.child() == 0) {
                    return tag(via, "NESTED:if-condition");
                }
                via.add("if-branch");
                continue;
            }
            if (p instanceof TypedCollection) {
                via.add("collection");
                continue;
            }
            if (p instanceof TypedLambda) {
                if (i > 0 && path.get(i - 1).node() instanceof TypedEval) {
                    via.add("eval");
                    i--;
                    continue;
                }
                if (i == 0) {
                    return tag(via, "ROOT:lambda-statement");
                }
                return tag(via, "NESTED:lambda-in:" + describe(path.get(i - 1).node()));
            }
            if (p instanceof TypedNativeCall n
                    && n.callee().qualifiedName().startsWith("meta::pure::functions::asserts::")) {
                return tag(via, "ROOT:assert:" + simple(n.callee().qualifiedName()) + "[" + s.child() + "]");
            }
            return tag(via, "NESTED:" + describe(p));
        }
        return tag(via, stmt == program.size() - 1 ? "ROOT:program" : "ROOT:statement");
    }

    private static String tag(List<String> via, String c) {
        return via.isEmpty() ? c : c + " via " + String.join(">", via);
    }

    /** The classes of each use of the let-bound {@code name} in the statements after {@code stmt}. */
    private static List<String> uses(List<TypedSpec> program, int stmt, String name, int depth) {
        List<String> out = new ArrayList<>();
        for (int j = stmt + 1; j < program.size(); j++) {
            findUses(program, j, program.get(j), name, new ArrayList<>(), out, depth);
        }
        return out;
    }

    private static void findUses(List<TypedSpec> program, int stmt, TypedSpec node, String name, List<Step> path,
            List<String> out, int depth) {
        if (node instanceof TypedVariable v && v.name().equals(name)) {
            out.add(classify(program, stmt, path, depth + 1));
            return;
        }
        List<TypedSpec> kids = node.children();
        for (int k = 0; k < kids.size(); k++) {
            path.add(new Step(node, k));
            findUses(program, stmt, kids.get(k), name, path, out, depth);
            path.remove(path.size() - 1);
        }
    }

    private static String chain(List<Step> path) {
        List<String> out = new ArrayList<>();
        for (int i = path.size() - 1; i >= 0 && out.size() < 6; i--) {
            out.add(describe(path.get(i).node()) + "[" + path.get(i).child() + "]");
        }
        return String.join(" < ", out);
    }

    private static String describe(TypedSpec p) {
        if (p instanceof TypedNativeCall n) {
            return simple(n.callee().qualifiedName());
        }
        if (p instanceof TypedUserCall u) {
            return "user:" + u.callee().qualifiedName();
        }
        if (p instanceof TypedLet l) {
            return "let:" + l.name();
        }
        return p.getClass().getSimpleName();
    }

    private static String simple(String fqn) {
        int at = fqn.lastIndexOf("::");
        return at < 0 ? fqn : fqn.substring(at + 2);
    }

    private static String pos(TypedSpec node) {
        com.legend.protocol.SourceInfo p = node instanceof TypedNativeCall n ? n.pos() : null;
        return p == null ? "-" : p.sourceId() + ":" + p.startLine();
    }

    private static String argType(TypedSpec node) {
        List<TypedSpec> kids = node.children();
        return kids.isEmpty() ? "-" : kids.get(0).info().type().typeName().replace('\t', ' ');
    }

    private static synchronized void write(String text) {
        try {
            Path file = DIR.resolve(ProcessHandle.current().pid() + ".tsv");
            if (!Files.exists(file)) {
                String cmd = System.getProperty("sun.java.command", "?");
                Files.writeString(file, "#CMD\t" + cmd.replace('\t', ' ').replace('\n', ' ') + "\n",
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
            Files.writeString(file, text, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
