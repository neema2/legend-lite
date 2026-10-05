// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;

/**
 * A relational operation element ({@code ###Relational} expressions: columns, joins, dyna functions,
 * literals) as upstream prints it ({@code HelperRelationalGrammarComposer.renderRelationalOperationElement}),
 * standard style.
 *
 * @param indentation  the composer context's indentation
 * @param currentDatabase the database being printed: a column of it drops its {@code [db]} pointer
 * @param dynaFunctionNames a mapping prints dyna functions by name; a store prints its operators
 */
record RelationalOperations(String indentation, @com.legend.base.Nullable String currentDatabase, boolean dynaFunctionNames) {

    private static final String SELF_JOIN_TABLE = "{target}";

    /** The dyna functions a store prints as infix operators. */
    private static final Map<String, String> INFIX = Map.of(
            "equal", " = ", "greaterThan", " > ", "lessThan", " < ", "greaterThanEqual", " >= ",
            "lessThanEqual", " <= ", "notEqual", " != ", "notEqualAnsi", " <> ");
    /** The dyna functions a store prints as postfix tests. */
    private static final Map<String, String> POSTFIX = Map.of("isNull", " is null", "isNotNull", " is not null");
    private static final Set<String> BOOLEAN = Set.of("and", "or");
    private static final String GROUP = "group";

    /** A mapping's context: no current database, dyna functions by name. */
    static RelationalOperations mapping(String indentation) {
        return new RelationalOperations(indentation, null, true);
    }

    RelationalOperations indented(int count) {
        return new RelationalOperations(indentation + " ".repeat(count), currentDatabase, dynaFunctionNames);
    }

    /** The printer of each operation {@code _type}. */
    private static final Map<String, java.util.function.BiFunction<RelationalOperations, Json.Obj, String>> PRINTERS = Map.of(
            "dynaFunc", RelationalOperations::dynaFunc,
            "column", RelationalOperations::column,
            "elemtWithJoins", RelationalOperations::elementWithJoins,
            "literalList", RelationalOperations::literalList,
            "literal", RelationalOperations::literal,
            "relationalLambda", RelationalOperations::lambda,
            "lambdaParameter", (ops, p) -> "$" + p.getString("name"));

    String render(Json.Node node) {
        Json.Obj op = Composing.obj(node, "relational operation");
        var printer = PRINTERS.get(Composing.type(op));
        if (printer == null) {
            throw Composing.refused("no composer rule for a relational operation of _type '" + Composing.type(op) + "'");
        }
        return printer.apply(this, op);
    }

    private String dynaFunc(Json.Obj f) {
        String name = f.getString("funcName");
        List<Json.Node> params = items(f, "parameters");
        if (dynaFunctionNames && !GROUP.equals(name)) {
            return byName(name, params);
        }
        if (GROUP.equals(name)) {
            requireParameters(name, params, 1);
            return "(" + render(params.get(0)) + ")";
        }
        if (BOOLEAN.contains(name)) {
            return joinRendered(params, " " + convertIdentifier(name) + " ");
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
    private static void requireParameters(String name, List<Json.Node> params, int count) {
        if (params.size() != count) {
            throw Composing.refused("the dyna function '" + name + "' with " + params.size() + " parameters (upstream cannot print it)");
        }
    }

    private String byName(String name, List<Json.Node> params) {
        return convertIdentifier(name) + "(" + joinRendered(params, ", ") + ")";
    }

    private String joinRendered(List<Json.Node> nodes, String sep) {
        List<String> out = new ArrayList<>();
        for (Json.Node n : nodes) {
            out.add(render(n));
        }
        return String.join(sep, out);
    }

    /** {@code renderTableAliasColumn}. */
    private String column(Json.Obj c) {
        Json.Obj table = c.getObj("table");
        String db = tableDb(table);
        String schema = str(table, "schema");
        String tableName = table.getString("table");
        boolean selfJoin = SELF_JOIN_TABLE.equals(tableName) && SELF_JOIN_TABLE.equals(str(c, "tableAlias"));
        String column = str(c, "column");
        return (db != null ? (db.equals(currentDatabase) ? "" : "[" + db + "]") : "")
                + (schema != null && !"default".equals(schema) && !selfJoin ? schema + "." : "")
                + tableName
                + (column != null ? "." + column : "");
    }

    /** The relational mapping table pointer's {@code getDb}: its main table's database, else its own. */
    static @com.legend.base.Nullable String tableDb(Json.Obj table) {
        String main = str(table, "mainTableDb");
        return main == null ? str(table, "database") : main;
    }

    private String elementWithJoins(Json.Obj e) {
        StringBuilder b = new StringBuilder();
        List<Json.Obj> joins = objs(e, "joins");
        if (!joins.isEmpty()) {
            b.append(firstJoinPointer(joins.get(0)));
            if (joins.size() > 1) {
                List<String> rest = new ArrayList<>();
                for (Json.Obj j : joins.subList(1, joins.size())) {
                    rest.add(joinPointer(j));
                }
                b.append(" > ").append(String.join(" > ", rest));
            }
        }
        Json.Obj element = objOr(e, "relationalElement");
        if (element != null) {
            b.append(!joins.isEmpty() ? " | " : "").append(render(element));
        }
        return b.toString();
    }

    private String literalList(Json.Obj l) {
        return "[" + joinRendered(items(l, "values"), ", ") + "]";
    }

    private String literal(Json.Obj l) {
        Json.Node v = l.get("value");
        if (v instanceof Json.Obj) {
            return render(v);
        }
        if (v instanceof Json.Str s) {
            return Composing.convertString(s.value(), true);
        }
        if (v instanceof Json.Num n) {
            if (!n.isInteger()) {
                return Double.toString(Composing.doubleOf(n));
            }
            if (n.longValue() == (int) n.longValue()) {
                return Long.toString(n.longValue());
            }
        }
        throw Composing.refused("a relational literal whose value upstream cannot print: " + v);
    }

    /** A relational lambda: one parameter bare, more in parentheses. */
    private String lambda(Json.Obj l) {
        List<String> names = l.getStringArrayOr("parameterNames", List.of());
        String body = render(l.get("body"));
        return names.size() == 1 ? names.get(0) + " | " + body : "(" + String.join(", ", names) + " | " + body + ")";
    }

    // ---------------------------------------------------------------------
    // Join pointers and filters
    // ---------------------------------------------------------------------

    private static String joinType(Json.Obj j) {
        String t = str(j, "joinType");
        return "LEFT_OUTER".equals(t) ? "OUTER" : String.valueOf(t);
    }

    private static String dbPointer(Json.Obj j) {
        String db = str(j, "db");
        return db != null ? "[" + db + "]" : "";
    }

    static String firstJoinPointer(Json.Obj j) {
        return dbPointer(j) + (str(j, "joinType") != null ? " (" + joinType(j) + ") " : "") + "@" + convertIdentifier(j.getString("name"));
    }

    static String joinPointer(Json.Obj j) {
        return (str(j, "joinType") != null ? "(" + joinType(j) + ") " : "") + dbPointer(j) + "@" + convertIdentifier(j.getString("name"));
    }

    /** {@code renderFilterMapping}. */
    static String filterMapping(Json.Obj fm) {
        List<Json.Obj> joins = objs(fm, "joins");
        Json.Obj filter = fm.getObj("filter");
        String db = str(filter, "db");
        StringBuilder body = new StringBuilder();
        if (!joins.isEmpty()) {
            Json.Obj first = joins.get(0);
            body.append(dbPointer(first)).append(" ").append(str(first, "joinType") != null ? "(" + joinType(first) + ") " : "")
                    .append("@").append(convertIdentifier(first.getString("name")));
        }
        if (joins.size() > 1) {
            List<String> rest = new ArrayList<>();
            for (Json.Obj j : joins.subList(1, joins.size())) {
                rest.add(joinPointer(j));
            }
            body.append(" > ").append(String.join(" > ", rest));
        }
        body.append(!joins.isEmpty() ? " | " : "").append("[").append(db).append("]");
        return "~filter " + (db != null ? body.toString() : "") + convertIdentifier(filter.getString("name"));
    }
}
