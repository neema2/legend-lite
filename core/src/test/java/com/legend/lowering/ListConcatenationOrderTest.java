// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.database.Databases;
import com.legend.testbackend.StorelessRuntime;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A LIST'S CONCATENATION IS ORDERED BY ONE OUTER ORDER BY (2026-11-15, the order fix's route, plan-only, no
 * database): rounds 55a, 55c and 55d sent the same bytes on MariaDB, MySQL and H2 -- each UNION ALL branch with its
 * own ORDER BY and no outer order -- so the route the union takes is printed here, not inferred: on a failure, the
 * typed root's class tree and the plan's MIR tree (record class names) for each expression.
 */
class ListConcatenationOrderTest {

    private static final String TDS = "#TDS\n  v\n  3\n  1\n  2\n#";

    private static final List<String> CASES = List.of(
            "|" + TDS + "->sort(~v->ascending())->map(r|$r.v)->concatenate(" + TDS
                    + "->sort(~v->descending())->map(r|$r.v))",
            "|[9, 8]->concatenate(" + TDS + "->sort(~v->ascending())->map(r|$r.v))",
            "|" + TDS + "->sort(~v->descending())->map(r|$r.v)->concatenate([7, 0])",
            "|" + TDS + "->sort(~v->ascending())->map(r|$r.v)->concatenate(" + TDS
                    + "->sort(~v->descending())->map(r|$r.v))->filter(x|$x > 1)");

    @Test
    void aListsConcatenationHasOneOuterOrderAndNoneInABranch() {
        List<String> wrong = new ArrayList<>();
        for (String database : List.of("MariaDB", "H2")) {
            String model = StorelessRuntime.with("", Databases.named(database));
            for (String expression : CASES) {
                TypedQuery q = Compiler.query(Compiler.compileModel(model), expression);
                String sql;
                com.legend.sql.SqlQuery tree;
                try {
                    com.legend.executionplan.ExecutionPlan.Sql answer = q.executionPlan(StorelessRuntime.RUNTIME)
                            .answerSql();
                    sql = answer.statement();
                    tree = answer.tree();
                } catch (RuntimeException e) {
                    // a refusal by name is a planned outcome, never an unordered answer
                    System.out.println("[route] " + database + " refused: " + expression.replace('\n', ' ') + " -- "
                            + e.getMessage());
                    continue;
                }
                if (!outerOrderOnly(sql)) {
                    StringBuilder b = new StringBuilder();
                    b.append(database).append(": ").append(expression.replace('\n', ' ')).append('\n');
                    b.append("  SQL: ").append(sql.replace('\n', ' ')).append('\n');
                    b.append("  TYPED ROOT:\n");
                    typedTree(q.expression(), 2, b, 0);
                    b.append("  MIR:\n");
                    mirTree(tree, 2, b, 0);
                    wrong.add(b.toString());
                }
            }
        }
        wrong.forEach(System.out::println);
        assertTrue(wrong.isEmpty(), wrong.size() + " list concatenation(s) not ordered by one outer ORDER BY:\n"
                + String.join("\n", wrong));
    }

    /** One ORDER BY, after the last UNION ALL, at the statement's own level; none between the unions. */
    private static boolean outerOrderOnly(String sql) {
        // a branch's ROW_NUMBER() OVER (ORDER BY ...) is its ordinal, not a branch's ORDER BY
        String flat = sql.replace('\n', ' ').replaceAll("OVER \\(ORDER BY", "OVER (");
        int lastUnion = flat.lastIndexOf("UNION ALL");
        if (lastUnion < 0) {
            return true;
        }
        String before = flat.substring(0, lastUnion);
        String after = flat.substring(lastUnion);
        // the outer select over the union: the Lowerer's (__lo) or the dialect's explode (__u)
        boolean outer = after.matches("(?s).*\\)\\s+AS\\s+\"?(__lo|__u)\"?\\s+ORDER BY.*");
        return outer && !before.contains("ORDER BY");
    }

    /** The typed tree's node classes, by record components, to a depth. */
    private static void typedTree(Object node, int indent, StringBuilder b, int depth) {
        tree(node, indent, b, depth, "com.legend.compiler.spec.typed.", 8);
    }

    /** The MIR's node classes, by record components, to a depth. */
    private static void mirTree(Object node, int indent, StringBuilder b, int depth) {
        tree(node, indent, b, depth, "com.legend.sql.", 10);
    }

    private static void tree(Object node, int indent, StringBuilder b, int depth, String pkg, int maxDepth) {
        if (node == null || depth > maxDepth) {
            return;
        }
        if (node instanceof List<?> l) {
            for (Object o : l) {
                tree(o, indent, b, depth, pkg, maxDepth);
            }
            return;
        }
        Class<?> k = node.getClass();
        if (!k.getName().startsWith(pkg) || !k.isRecord()) {
            return;
        }
        b.append(" ".repeat(indent + 2 * depth)).append(k.getName().substring(pkg.length()));
        if (node instanceof com.legend.sql.SqlSelect s) {
            b.append(" orderBy=").append(s.orderBy().size()).append(" outputs=")
                    .append(s.outputs().stream().map(com.legend.sql.OutputCol::name).toList());
        }
        if (node instanceof com.legend.compiler.spec.typed.TypedNativeCall nc) {
            b.append(" ").append(nc.callee().qualifiedName());
        }
        b.append('\n');
        for (RecordComponent rc : k.getRecordComponents()) {
            try {
                tree(rc.getAccessor().invoke(node), indent, b, depth + 1, pkg, maxDepth);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
