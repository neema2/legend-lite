package com.legend.warehouse.server;

import com.legend.server.Json;
import com.legend.warehouse.server.duck.Collect;
import com.legend.warehouse.server.duck.Conn;
import com.legend.warehouse.server.duck.Database;
import com.legend.warehouse.server.duck.DuckException;
import com.legend.warehouse.server.duck.Result;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Who may read what (docs/SERVER_PROGRAM_2026_09_26.md §3): roles, their members (users and other
 * roles), and SELECT grants on objects (a table, a view, a table function or a macro, by catalog,
 * schema and name) or on whole schemas. Kept in the warehouse's system database; read from an
 * in-memory snapshot, replaced whole after every change. Names compare as DuckDB compares
 * identifiers: without case.
 */
public final class Grants {

    /** A SELECT grant: on one object ({@code name} set) or on a schema ({@code name} empty). */
    public record Grant(String catalog, String schema, String name, String grantee) {
    }

    private record Snapshot(Set<String> roles, Map<String, Set<String>> rolesOf, List<Grant> grants) {
    }

    private final Conn conn;
    private volatile Snapshot now;

    public Grants(Database system) throws DuckException {
        conn = system.connect("warehouse");
        conn.execute("""
                CREATE TABLE IF NOT EXISTS security_roles (role VARCHAR PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS security_members (role VARCHAR, member VARCHAR, PRIMARY KEY (role, member));
                CREATE TABLE IF NOT EXISTS security_grants (catalog VARCHAR, schema_name VARCHAR, name VARCHAR,
                  grantee VARCHAR, PRIMARY KEY (catalog, schema_name, name, grantee))""").close();
        now = load();
    }

    static String norm(String identifier) {
        return identifier.toLowerCase(Locale.ROOT);
    }

    // -- reading -------------------------------------------------------------------------------

    /** The user and every role it holds, directly or through other roles. */
    public Set<String> principals(String user) {
        Snapshot s = now;
        Set<String> out = new HashSet<>();
        ArrayDeque<String> todo = new ArrayDeque<>(List.of(norm(user)));
        while (!todo.isEmpty()) {
            String p = todo.pop();
            if (out.add(p)) todo.addAll(s.rolesOf().getOrDefault(p, Set.of()));
        }
        return out;
    }

    /** Whether any of {@code principals} may SELECT from the object (itself granted, or its schema). */
    public boolean canSelect(Set<String> principals, String catalog, String schema, String name) {
        String c = norm(catalog), sc = norm(schema), n = norm(name);
        for (Grant g : now.grants()) {
            if (principals.contains(g.grantee()) && g.catalog().equals(c) && g.schema().equals(sc)
                    && (g.name().isEmpty() || g.name().equals(n))) {
                return true;
            }
        }
        return false;
    }

    /** Whether any of {@code principals} holds any grant in the schema (what the catalog API lists). */
    public boolean canSeeSchema(Set<String> principals, String catalog, String schema) {
        String c = norm(catalog), sc = norm(schema);
        for (Grant g : now.grants()) {
            if (principals.contains(g.grantee()) && g.catalog().equals(c) && g.schema().equals(sc)) return true;
        }
        return false;
    }

    public List<Grant> all() {
        return now.grants();
    }

    public boolean isRole(String name) {
        return now.roles().contains(norm(name));
    }

    // -- changing (owners only; the caller checks) ------------------------------------------------

    public synchronized void createRole(String role) throws DuckException {
        write("INSERT INTO security_roles VALUES (?) ON CONFLICT DO NOTHING", norm(role));
    }

    public synchronized void dropRole(String role) throws DuckException {
        String r = norm(role);
        write("DELETE FROM security_members WHERE role = ? OR member = ?", r, r);
        write("DELETE FROM security_grants WHERE grantee = ?", r);
        write("DELETE FROM security_roles WHERE role = ?", r);
    }

    public synchronized void grantRole(String role, String member) throws DuckException {
        if (!isRole(role)) throw new IllegalArgumentException("no role " + role);
        write("INSERT INTO security_members VALUES (?, ?) ON CONFLICT DO NOTHING", norm(role), norm(member));
    }

    public synchronized void revokeRole(String role, String member) throws DuckException {
        write("DELETE FROM security_members WHERE role = ? AND member = ?", norm(role), norm(member));
    }

    public synchronized void grantSelect(Grant g) throws DuckException {
        write("INSERT INTO security_grants VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                norm(g.catalog()), norm(g.schema()), norm(g.name()), norm(g.grantee()));
    }

    public synchronized void revokeSelect(Grant g) throws DuckException {
        write("DELETE FROM security_grants WHERE catalog = ? AND schema_name = ? AND name = ? AND grantee = ?",
                norm(g.catalog()), norm(g.schema()), norm(g.name()), norm(g.grantee()));
    }

    private void write(String sql, Object... params) throws DuckException {
        conn.execute(sql, params).close();
        now = load();
    }

    private Snapshot load() throws DuckException {
        Set<String> roles = new HashSet<>();
        for (List<Json.Node> r : rows("SELECT role FROM security_roles")) roles.add(str(r.get(0)));
        Map<String, Set<String>> rolesOf = new HashMap<>();
        for (List<Json.Node> r : rows("SELECT role, member FROM security_members")) {
            rolesOf.computeIfAbsent(str(r.get(1)), k -> new HashSet<>()).add(str(r.get(0)));
        }
        List<Grant> grants = new ArrayList<>();
        for (List<Json.Node> r : rows("SELECT catalog, schema_name, name, grantee FROM security_grants")) {
            grants.add(new Grant(str(r.get(0)), str(r.get(1)), str(r.get(2)), str(r.get(3))));
        }
        return new Snapshot(Set.copyOf(roles), Map.copyOf(rolesOf), List.copyOf(grants));
    }

    private List<List<Json.Node>> rows(String sql) throws DuckException {
        try (Result r = conn.execute(sql)) {
            return Collect.json(r, Long.MAX_VALUE);
        } catch (DuckException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("could not read the grants", e);
        }
    }

    private static String str(Json.Node n) {
        return ((Json.Str) n).value();
    }

    public synchronized void close() {
        conn.close();
    }
}
