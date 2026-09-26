package com.legend.warehouse.server;

import com.legend.json.Json;
import com.legend.warehouse.server.duck.Collect;
import com.legend.warehouse.server.duck.Conn;
import com.legend.warehouse.server.duck.DuckException;
import com.legend.warehouse.server.duck.Result;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What a READER may run (owners are not checked): one SELECT, and everything it names from a FROM
 * clause (a table, a view, a table function) or calls (a macro someone deployed) granted to the
 * reader or one of its roles. Allows or denies; never rewrites (docs/SERVER_PROGRAM_2026_09_26.md §3).
 *
 * <p>The database is locked down first ({@code Database#lockDown}: no files, URLs, ATTACH, COPY,
 * extensions), so this check stays small. It reads DuckDB's own parse ({@code json_serialize_sql},
 * which refuses anything but SELECT) and walks it:
 *
 * <ul>
 *   <li>a table or view: granted; an unqualified name inside a CTE's scope is that CTE, scoped as
 *       DuckDB binds it: a CTE's own body does not see its name, but for a recursive CTE's recursive
 *       term (its anchor does not: there the name is whatever it names outside);</li>
 *   <li>a table function: granted, or a harmless generator ({@code range}, {@code generate_series},
 *       {@code unnest}); {@code query} and {@code query_table} never (they reach a table named only
 *       in a string, which no parse shows);</li>
 *   <li>a function call: one of DuckDB's own, but for a few with side effects or that read the
 *       server's settings; anything else (a deployed macro) must be granted;</li>
 *   <li>only structural pieces this check knows: anything else ({@code SHOW}, a new kind in a later
 *       DuckDB) is denied, never passed.</li>
 * </ul>
 */
final class Authorizer {

    /** A statement a reader may not run: why. */
    static final class Denied extends Exception {
        final boolean parse;

        Denied(String message, boolean parse) {
            super(message);
            this.parse = parse;
        }
    }

    private static final Set<String> GENERATORS = Set.of("range", "generate_series", "unnest");
    private static final Set<String> NEVER = Set.of("query", "query_table");
    private static final Set<String> DENIED_FUNCTIONS = Set.of(
            "sleep_ms", "pg_sleep", "nextval", "currval", "setseed", "current_setting", "write_log");
    /**
     * The kinds (a {@code "type"} outside an expression) a reader's SELECT may contain: query nodes,
     * table references, modifiers, sort directions and type details. Anything else is denied.
     */
    private static final Set<String> STRUCTURE = Set.of(
            "SELECT_NODE", "SET_OPERATION_NODE", "RECURSIVE_CTE_NODE", "CTE_NODE",
            "BASE_TABLE", "JOIN", "SUBQUERY", "TABLE_FUNCTION", "EMPTY", "EXPRESSION_LIST", "PIVOT",
            "ORDER_MODIFIER", "LIMIT_MODIFIER", "LIMIT_PERCENT_MODIFIER", "DISTINCT_MODIFIER",
            // an ORDER BY's direction (in a window, an aggregate, a query)
            "ORDER_DEFAULT", "ASCENDING", "DESCENDING",
            // a type's details (a CAST's DECIMAL(p, s), LIST, STRUCT, ENUM, ...): DuckDB's ExtraTypeInfoType
            "GENERIC_TYPE_INFO", "DECIMAL_TYPE_INFO", "STRING_TYPE_INFO", "LIST_TYPE_INFO", "STRUCT_TYPE_INFO",
            "ENUM_TYPE_INFO", "UNBOUND_TYPE_INFO", "USER_TYPE_INFO", "AGGREGATE_STATE_TYPE_INFO", "ARRAY_TYPE_INFO",
            "ANY_TYPE_INFO", "INTEGER_LITERAL_TYPE_INFO", "TEMPLATE_TYPE_INFO", "GEO_TYPE_INFO");

    private final Grants grants;
    /** DuckDB's own functions (duckdb_functions() where internal): what needs no grant. */
    private final Set<String> builtins;

    Authorizer(Grants grants, Set<String> builtins) {
        this.grants = grants;
        this.builtins = Set.copyOf(builtins);
    }

    /** DuckDB's own function names, read from the database (the same set for every catalog). */
    static Set<String> builtins(Conn conn) throws Exception {
        Set<String> out = new HashSet<>();
        try (Result r = conn.execute("SELECT DISTINCT function_name FROM duckdb_functions() WHERE internal")) {
            for (List<Json.Node> row : Collect.json(r, Long.MAX_VALUE)) out.add(Grants.norm(((Json.Str) row.get(0)).value()));
        }
        return out;
    }

    /** Throws {@link Denied} unless {@code user} may run {@code sql} against {@code catalog}. */
    void check(Conn conn, String sql, String catalog, String user) throws Denied, Exception {
        Json.Obj parsed;
        try (Result r = conn.execute("SELECT json_serialize_sql(?::VARCHAR)", sql)) {
            parsed = Json.parseObject(((Json.Str) Collect.json(r, 1).get(0).get(0)).value());
        }
        if (parsed.getBoolOr("error", false)) {
            String m = parsed.getStringOr("error_message", "");
            // DuckDB's kinds: "parser" for SQL it cannot read; "not implemented" for anything but SELECT
            if ("parser".equals(parsed.getStringOr("error_type", ""))) throw new Denied("Parser Error: " + m, true);
            throw new Denied("readers may run a SELECT, and only a SELECT (" + m + ")", false);
        }
        List<Json.Node> statements = parsed.getArr("statements").items();
        if (statements.size() != 1) throw new Denied("readers may run one statement at a time", false);
        new Walk(catalog, grants.principals(user)).node(statements.get(0), Set.of());
    }

    private final class Walk {
        private final String catalog;
        private final Set<String> principals;

        Walk(String catalog, Set<String> principals) {
            this.catalog = catalog;
            this.principals = principals;
        }

        void node(Json.Node n, Set<String> ctes) throws Denied {
            if (n instanceof Json.Arr a) {
                for (Json.Node x : a.items()) node(x, ctes);
                return;
            }
            if (!(n instanceof Json.Obj o)) return;
            Map<String, Json.Node> f = o.fields();
            Set<String> scope = ctes;
            Json.Node cteMap = f.get("cte_map");
            if (cteMap instanceof Json.Obj cm && cm.fields().get("map") instanceof Json.Arr entries) {
                // each CTE sees the ones before it, never itself (a recursive one's term sees itself:
                // below); the body sees all
                Set<String> growing = new HashSet<>(ctes);
                for (Json.Node e : entries.items()) {
                    Json.Obj entry = (Json.Obj) e;
                    String name = Grants.norm(((Json.Str) entry.get("key")).value());
                    Set<String> inner = new HashSet<>(growing);
                    inner.remove(name);
                    node(entry.get("value"), inner);
                    growing.add(name);
                }
                scope = growing;
            }
            if (f.get("type") instanceof Json.Str rt && rt.value().equals("RECURSIVE_CTE_NODE")) {
                // Only the recursive term (right) sees the CTE's own name: in the anchor (left, however
                // deep its own unions go) DuckDB binds that name to whatever it names outside.
                if (!(f.get("cte_name") instanceof Json.Str cte)) throw new Denied("an unreadable recursive CTE", false);
                String self = Grants.norm(cte.value());
                Set<String> outside = new HashSet<>(scope);
                outside.remove(self);
                Set<String> inside = new HashSet<>(scope);
                inside.add(self);
                for (Map.Entry<String, Json.Node> e : f.entrySet()) {
                    if (!e.getKey().equals("cte_map")) node(e.getValue(), e.getKey().equals("right") ? inside : outside);
                }
                return;
            }
            boolean expression = f.containsKey("class");
            Json.Node type = f.get("type");
            if (!expression && type instanceof Json.Str t) {
                String kind = t.value();
                if (!STRUCTURE.contains(kind)) throw new Denied("readers' statements may not use " + kind, false);
                if (kind.equals("BASE_TABLE")) table(o, scope);
                if (kind.equals("TABLE_FUNCTION")) tableFunction(o);
            }
            if (expression && f.get("function_name") instanceof Json.Str fn && !o.getBoolOr("is_operator", false)) {
                function(o, fn.value());
            }
            for (Map.Entry<String, Json.Node> e : f.entrySet()) {
                if (!e.getKey().equals("cte_map")) node(e.getValue(), scope);
            }
        }

        private void table(Json.Obj ref, Set<String> ctes) throws Denied {
            String c = text(ref, "catalog_name"), s = text(ref, "schema_name"), name = text(ref, "table_name");
            if (c.isEmpty() && s.isEmpty() && ctes.contains(Grants.norm(name))) return;   // a CTE in scope
            object(c, s, name, "table or view");
        }

        private void tableFunction(Json.Obj ref) throws Denied {
            if (!(ref.get("function") instanceof Json.Obj fn)) throw new Denied("an unreadable table function", false);
            String c = text(fn, "catalog"), s = text(fn, "schema"), name = Grants.norm(text(fn, "function_name"));
            if (NEVER.contains(name)) throw new Denied(name + "() reads a table named in a string: readers may not call it", false);
            if (c.isEmpty() && s.isEmpty() && GENERATORS.contains(name)) return;
            object(c, s, name, "table function");
        }

        private void function(Json.Obj call, String fn) throws Denied {
            String name = Grants.norm(fn);
            if (DENIED_FUNCTIONS.contains(name)) throw new Denied("readers may not call " + name + "()", false);
            String c = text(call, "catalog"), s = text(call, "schema");
            boolean identity = name.equals("authenticated_user") && (c.isEmpty() || c.equals("system"));
            if (identity || (builtins.contains(name) && (c.isEmpty() || c.equals("system")))) return;
            object(c, s, name, "function");
        }

        /** A named object: granted to one of the reader's principals, or denied. */
        private void object(String c, String s, String name, String what) throws Denied {
            String cat = c.isEmpty() ? catalog : c;
            String schema = s.isEmpty() ? "main" : s;
            if (!grants.canSelect(principals, cat, schema, name)) {
                throw new Denied("no SELECT granted on " + what + " " + cat + "." + schema + "." + name, false);
            }
        }

        private String text(Json.Obj o, String key) {
            return o.get(key) instanceof Json.Str s ? s.value() : "";
        }
    }
}
