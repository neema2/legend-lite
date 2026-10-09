// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.legend.protocol.Composing.convertIdentifier;

/**
 * A relational operation element ({@code ###Relational} expressions: columns, joins, dyna functions,
 * literals) as upstream prints it ({@code HelperRelationalGrammarComposer.renderRelationalOperationElement})
 * -- over the record ({@link Protocol.PRelOp}; the protocol program's leg 2, step 3).
 *
 * @param indentation  the composer context's indentation
 * @param currentDatabase the database being printed: a column of it drops its {@code [db]} pointer
 * @param dynaFunctionNames a mapping prints dyna functions by name; a store prints its operators
 * @param style the model's render style: in PRETTY a store's {@code and} and {@code or} chains break across lines
 */
record RelationalOperations(String indentation, @com.legend.base.Nullable String currentDatabase, boolean dynaFunctionNames,
        PureComposer.Style style) {

    private static final String SELF_JOIN_TABLE = "{target}";

    /** The dyna functions a store prints as infix operators. */
    private static final Map<String, String> INFIX = Map.of(
            "equal", " = ", "greaterThan", " > ", "lessThan", " < ", "greaterThanEqual", " >= ",
            "lessThanEqual", " <= ", "notEqual", " != ", "notEqualAnsi", " <> ");
    /** The dyna functions a store prints as postfix tests. */
    private static final Map<String, String> POSTFIX = Map.of("isNull", " is null", "isNotNull", " is not null");
    private static final Set<String> BOOLEAN = Set.of("and", "or");
    private static final String GROUP = "group";

    /**
     * A mapping's context: no current database, dyna functions by name, and STANDARD in every model -- upstream builds it
     * with {@code RelationalGrammarComposerContext.Builder.newInstance(PureGrammarComposerContext)}, which copies the
     * indentation and not the render style.
     */
    static RelationalOperations mapping(String indentation) {
        return new RelationalOperations(indentation, null, true, PureComposer.Style.STANDARD);
    }

    RelationalOperations indented(int count) {
        return new RelationalOperations(indentation + " ".repeat(count), currentDatabase, dynaFunctionNames, style);
    }

    String render(Protocol.PRelOp op) {
        return render(op, false, 0);
    }

    /** {@code renderRelationalOperationElement(op, context, nested, numTabs)}: inside a group, how deep. */
    private String render(Protocol.PRelOp op, boolean nested, int numTabs) {
        return switch (op) {
            case Protocol.PDynaFunc f -> dynaFunc(f, nested, numTabs);
            case Protocol.PColumnRef c -> column(c);
            case Protocol.PElemtWithJoins e -> elementWithJoins(e);
            case Protocol.PRelLiteralList l -> "[" + joinRendered(l.values(), ", ") + "]";
            case Protocol.PRelLiteral l -> literal(l);
            case Protocol.PRelLambda l -> lambda(l, nested, numTabs);
            case Protocol.PLambdaParam p -> "$" + p.name();
        };
    }

    private String dynaFunc(Protocol.PDynaFunc f, boolean nested, int numTabs) {
        String name = f.funcName();
        List<Protocol.PRelOp> params = f.parameters();
        if (dynaFunctionNames && !GROUP.equals(name)) {
            return byName(name, params);
        }
        if (GROUP.equals(name)) {
            requireParameters(name, params, 1);
            return "(" + render(params.get(0), true, numTabs + 1) + ")";
        }
        if (BOOLEAN.contains(name)) {
            // PRETTY: each operand on its own line, the operator leading it, one tab deeper inside each group
            return joinRendered(params, style == PureComposer.Style.PRETTY
                    ? "\n  " + Composing.tab(nested ? 1 + numTabs : 1) + convertIdentifier(name) + " "
                    : " " + convertIdentifier(name) + " ");
        }
        String postfix = POSTFIX.get(name);
        if (postfix != null) {
            requireParameters(name, params, 1);
            return render(params.get(0)) + postfix;
        }
        String infix = INFIX.get(name);
        if (infix != null) {
            requireParameters(name, params, 2);
            return render(params.get(0)) + infix + render(params.get(1));
        }
        return byName(name, params);
    }

    /** Upstream prints its "Unable to transform operation" comment: there is nothing to print. */
    private static void requireParameters(String name, List<Protocol.PRelOp> params, int count) {
        if (params.size() != count) {
            throw Composing.refused("the dyna function '" + name + "' with " + params.size() + " parameters (upstream cannot print it)");
        }
    }

    private String byName(String name, List<Protocol.PRelOp> params) {
        return convertIdentifier(name) + "(" + joinRendered(params, ", ") + ")";
    }

    private String joinRendered(List<Protocol.PRelOp> ops, String sep) {
        List<String> out = new ArrayList<>();
        for (Protocol.PRelOp op : ops) {
            out.add(render(op));
        }
        return String.join(sep, out);
    }

    /** {@code renderTableAliasColumn}. */
    private String column(Protocol.PColumnRef c) {
        Protocol.PTablePtr table = c.table();
        String db = tableDb(table);
        String schema = table.schema();
        boolean selfJoin = SELF_JOIN_TABLE.equals(table.table()) && SELF_JOIN_TABLE.equals(c.tableAlias());
        return (db != null ? (db.equals(currentDatabase) ? "" : "[" + db + "]") : "")
                + (!"default".equals(schema) && !selfJoin ? schema + "." : "")
                + table.table()
                + "." + c.column();
    }

    /** The relational mapping table pointer's {@code getDb}: its main table's database, else its own. */
    static @com.legend.base.Nullable String tableDb(Protocol.PTablePtr table) {
        return table.mainTableDb() == null ? table.database() : table.mainTableDb();
    }

    private String elementWithJoins(Protocol.PElemtWithJoins e) {
        StringBuilder b = new StringBuilder();
        List<Protocol.PJoinPtr> joins = e.joins();
        if (!joins.isEmpty()) {
            b.append(firstJoinPointer(joins.get(0)));
            if (joins.size() > 1) {
                List<String> rest = new ArrayList<>();
                for (Protocol.PJoinPtr j : joins.subList(1, joins.size())) {
                    rest.add(joinPointer(j));
                }
                b.append(" > ").append(String.join(" > ", rest));
            }
        }
        Protocol.PRelOp element = e.relationalElement();
        if (element != null) {
            b.append(!joins.isEmpty() ? " | " : "").append(render(element));
        }
        return b.toString();
    }

    /** A string quoted; a double as Java prints it; an integer only when it fits an {@code int} (upstream prints
     *  {@code Integer}, {@code Float} and {@code Double} values and no other number). */
    private static String literal(Protocol.PRelLiteral l) {
        return switch (l.value()) {
            case String s -> Composing.convertString(s, true);
            case Double d -> Double.toString(d);
            case Long n when n == n.intValue() -> Long.toString(n);
            default -> throw Composing.refused("a relational literal whose value upstream cannot print: " + l.value());
        };
    }

    /** A relational lambda: one parameter bare, more in parentheses. */
    private String lambda(Protocol.PRelLambda l, boolean nested, int numTabs) {
        List<String> names = l.parameterNames();
        String body = render(l.body(), nested, numTabs);
        return names.size() == 1 ? names.get(0) + " | " + body : "(" + String.join(", ", names) + " | " + body + ")";
    }

    // ---------------------------------------------------------------------
    // Join pointers and filters
    // ---------------------------------------------------------------------

    private static String joinType(Protocol.PJoinPtr j) {
        return "LEFT_OUTER".equals(j.joinType()) ? "OUTER" : String.valueOf(j.joinType());
    }

    private static String dbPointer(Protocol.PJoinPtr j) {
        return j.db() != null ? "[" + j.db() + "]" : "";
    }

    static String firstJoinPointer(Protocol.PJoinPtr j) {
        return dbPointer(j) + (j.joinType() != null ? " (" + joinType(j) + ") " : "") + "@" + convertIdentifier(j.name());
    }

    static String joinPointer(Protocol.PJoinPtr j) {
        return (j.joinType() != null ? "(" + joinType(j) + ") " : "") + dbPointer(j) + "@" + convertIdentifier(j.name());
    }

    /** {@code renderFilterMapping}: a class mapping's or a view's {@code ~filter}; no database drops the joins. */
    static String filterMapping(@com.legend.base.Nullable String db, String name, List<Protocol.PJoinPtr> joins) {
        StringBuilder body = new StringBuilder();
        if (!joins.isEmpty()) {
            Protocol.PJoinPtr first = joins.get(0);
            body.append(dbPointer(first)).append(" ").append(first.joinType() != null ? "(" + joinType(first) + ") " : "")
                    .append("@").append(convertIdentifier(first.name()));
        }
        if (joins.size() > 1) {
            List<String> rest = new ArrayList<>();
            for (Protocol.PJoinPtr j : joins.subList(1, joins.size())) {
                rest.add(joinPointer(j));
            }
            body.append(" > ").append(String.join(" > ", rest));
        }
        body.append(!joins.isEmpty() ? " | " : "").append("[").append(db).append("]");
        return "~filter " + (db != null ? body.toString() : "") + convertIdentifier(name);
    }
}
