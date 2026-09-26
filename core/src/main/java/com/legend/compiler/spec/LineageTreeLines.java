// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE LINEAGE-TREE GOLDEN CANON (compiler layer; the verdict seam calls it) — a statement-root {@code assertEquals(<tree
 * print>, $tree->relationTreeAsString(…))} is REWRITTEN, at compile time, into
 * the ordinary collection assert {@code assertEquals([<line>, …],
 * $tree->relationTreeLines(…))}: the golden print's lines as a String
 * collection literal, ours as the prelude's Pure body over the handle's
 * LineageRows (one text per node, in preorder). The ordinary verdict then
 * judges it — a verdict row in database mode, the host judge in host mode —
 * with no SQL of this arm's own (task #14 leg 1, 2026-09-21; the raw
 * tree-print query this arm once ran, DuckDB-only, is gone).
 *
 * <p>The canon rule on the golden: a tree print is the engine's
 * {@code buildUniqueName(elements, alias = true)} — its join labels spell the
 * engine's decorated SQL ALIASES ({@code _d#N}, {@code _dy<i>}, {@code _m<N>},
 * {@code _l}, {@code _r}, {@code _md}, duplicate counters — pureToSQLQuery.pure
 * buildNodeId), an artifact of its SQL generation the row charter retired; the
 * tree's CONTENT is the engine's own {@code alias = false} form, the relational
 * element's name. Every decorated alias in a label resolves to the node the
 * tree itself declares (longest name first — a node name can prefix another).
 */
public final class LineageTreeLines {

    private static final String ASSERT_EQUALS = "meta::pure::functions::asserts::assertEquals";
    private static final String TREE_AS_STRING =
            "meta::pure::lineage::scanRelations::relationTreeAsString";
    /** The prelude body printing one text per node (SystemMetamodel). */
    static final String TREE_LINES = "meta::lite::lineage::relationTreeLines";

    private static final Pattern NODE = Pattern.compile(
            "^------> \\(([tv])\\) ([^( \\[]+)(?:\\((.*)\\))? \\[([^\\]]*)\\]$");
    private static final String DECORATIONS =
            "(?:_d#\\d+|_dy\\d+|_md|_d\\d+|_d|_m\\d+|_i\\d+|_l|_r|_f|_\\d+|#\\d+)+";

    private LineageTreeLines() {
    }

    /** The assert rewritten over lines, or null when the statement is not the
     * shape (not assertEquals of a tree print against a tree print). */
    public static @com.legend.base.Nullable TypedSpec asLines(TypedSpec bare, List<TypedSpec> letPrefix,
            ModelContext ctx) {
        TypedFunction callee;
        List<TypedSpec> args;
        if (bare instanceof TypedUserCall c) {
            callee = c.callee();
            args = c.args();
        } else if (bare instanceof TypedNativeCall n) {
            callee = n.callee();
            args = n.args();
        } else {
            return null;
        }
        if (args.size() != 2 || !ASSERT_EQUALS.equals(callee.qualifiedName())) {
            return null;
        }
        String golden = spelled(args.get(0), letPrefix);
        if (golden == null || !isTreePrint(golden)) {
            return null;
        }
        TypedSpec print = Lets.bound(args.get(1), letPrefix);
        if (!(print instanceof TypedUserCall p) || !TREE_AS_STRING.equals(p.callee().qualifiedName())
                || p.args().isEmpty() || p.args().size() > 2) {
            return null;
        }
        List<TypedFunction> lines = ctx.findFunction(TREE_LINES);
        if (lines.size() != 1) {
            return null;
        }
        ExprType one = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
        List<TypedSpec> expected = new ArrayList<>();
        for (String line : goldenLines(golden)) {
            expected.add(new TypedCString(line, one));
        }
        TypedSpec withJoin = p.args().size() == 2 ? p.args().get(1)
                : new TypedCBoolean(true, new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
        ExprType many = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ZERO_MANY);
        List<TypedSpec> rewritten = List.of(
                new TypedCollection(expected, new ExprType(Type.Primitive.STRING,
                        new Multiplicity.Bounded(expected.size(), expected.size()))),
                new TypedUserCall(lines.get(0), List.of(p.args().get(0), withJoin), many));
        return bare instanceof TypedUserCall c
                ? new TypedUserCall(callee, rewritten, c.info())
                : new TypedNativeCall(callee, rewritten, ((TypedNativeCall) bare).info(),
                        ((TypedNativeCall) bare).pos());
    }

    public static boolean isTreePrint(String s) {
        return s.startsWith("root\n") && s.contains("------> (");
    }

    /** The print's lines with every decorated alias in a join label resolved
     * to the node name it decorates; blank lines dropped. */
    public static List<String> goldenLines(String print) {
        record Row(String indent, String kind, String name, @com.legend.base.Nullable String label,
                String cols) {
        }
        List<Row> rows = new ArrayList<>();
        Set<String> names = new LinkedHashSet<>();
        for (String raw : print.split("\n")) {
            String line = raw.strip();
            if (line.isEmpty()) {
                continue;
            }
            String indent = raw.substring(0, raw.indexOf(line));
            if (line.equals("root")) {
                rows.add(new Row(indent, "root", "", null, ""));
                continue;
            }
            Matcher m = NODE.matcher(line);
            if (!m.matches()) {
                throw new IllegalStateException("lineage golden line outside the print grammar: " + raw);
            }
            rows.add(new Row(indent, m.group(1), m.group(2), m.group(3), m.group(4)));
            names.add(m.group(2));
        }
        List<String> alts = new ArrayList<>(names);
        alts.sort((a, b) -> a.length() != b.length() ? b.length() - a.length() : a.compareTo(b));
        StringBuilder alt = new StringBuilder("(");
        for (String n : alts) {
            alt.append(Pattern.quote(n)).append('|');
        }
        alt.append("root|unionBase|unionAlias|\"joinleft_\"|\"joinright_\")");
        Pattern decorated = Pattern.compile(alt + DECORATIONS);
        List<String> out = new ArrayList<>(rows.size());
        for (Row r : rows) {
            if (r.kind().equals("root")) {
                out.add(r.indent() + "root");
                continue;
            }
            StringBuilder sb = new StringBuilder(r.indent())
                    .append("------> (").append(r.kind()).append(") ").append(r.name());
            if (r.label() != null) {
                sb.append('(').append(decorated.matcher(r.label()).replaceAll("$1")).append(')');
            }
            out.add(sb.append(" [").append(r.cols()).append(']').toString());
        }
        return out;
    }

    /** The golden as a STRING: spelled inline, through a let, or as a
     * concatenation of literals. */
    private static @com.legend.base.Nullable String spelled(TypedSpec e, List<TypedSpec> lets) {
        if (e instanceof TypedCString s) {
            return s.value();
        }
        if (e instanceof TypedVariable v) {
            TypedLet let = Lets.binding(lets, v.name());
            return let == null ? null : spelled(let.value(), lets);
        }
        List<TypedSpec> parts;
        if (e instanceof TypedNativeCall n
                && com.legend.compiler.element.type.PlatformTypes.isPlus(n.callee().qualifiedName())) {
            parts = n.args().size() == 1 && n.args().get(0) instanceof TypedCollection c
                    ? c.elements() : n.args();
        } else if (e instanceof TypedCollection c) {
            parts = c.elements();
        } else {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (TypedSpec part : parts) {
            String t = spelled(part, lets);
            if (t == null) {
                return null;
            }
            sb.append(t);
        }
        return sb.toString();
    }
}
